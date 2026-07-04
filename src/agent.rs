pub mod message;
pub mod tool;
mod tool_observation;

use crate::agent::message::Message;
use crate::agent::tool::ToolExecutor;
use crate::llm::client::{
    LlmClient, LlmRequest, LlmResponse, LlmStreamClient, LlmStreamEvent, LlmStreamSink,
    RUN_CANCELLED, RunContext,
};
use crate::memory::store::MemoryStore;
use anyhow::{Result, bail};

const MAX_TOOL_CALL_ITERATIONS: usize = 5;

pub struct Agent<L, M, T> {
    instructions: String,
    tools: T,
    llm: L,
    memory: M,
}

impl<L, M, T> Agent<L, M, T>
where
    L: LlmClient,
    M: MemoryStore,
    T: ToolExecutor,
{
    pub fn new(llm: L, memory: M, tools: T) -> Self {
        Self {
            instructions: String::new(),
            tools,
            memory,
            llm,
        }
    }

    pub fn set_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = instructions.into();
        self
    }

    pub async fn execute(&self, content: impl Into<String>) -> Result<String> {
        self.execute_with_context(content, &RunContext::new()).await
    }

    pub async fn execute_with_context(
        &self,
        content: impl Into<String>,
        context: &RunContext,
    ) -> Result<String> {
        self.memory.append(Message::user(content.into())).await?;

        for _ in 0..MAX_TOOL_CALL_ITERATIONS {
            context.ensure_not_cancelled()?;
            let request = self.build_llm_request().await?;
            let response = tokio::select! {
                response = self.llm.generate(request, context) => response?,
                _ = context.cancelled() => bail!(RUN_CANCELLED),
            };

            if let Some(answer) = self.record_response(response).await? {
                return Ok(answer);
            }
        }

        bail!("tool call loop exceeded max iterations ({MAX_TOOL_CALL_ITERATIONS})")
    }
}

impl<L, M, T> Agent<L, M, T>
where
    L: LlmClient + LlmStreamClient,
    M: MemoryStore,
    T: ToolExecutor,
{
    pub async fn execute_streaming(
        &self,
        content: impl Into<String>,
        sink: &mut dyn LlmStreamSink,
    ) -> Result<String> {
        self.execute_streaming_with_context(content, sink, &RunContext::new())
            .await
    }

    pub async fn execute_streaming_with_context(
        &self,
        content: impl Into<String>,
        sink: &mut dyn LlmStreamSink,
        context: &RunContext,
    ) -> Result<String> {
        self.memory.append(Message::user(content.into())).await?;

        for _ in 0..MAX_TOOL_CALL_ITERATIONS {
            context.ensure_not_cancelled()?;
            let request = self.build_llm_request().await?;
            let streamed_turn = self.stream_turn(request, context).await?;
            let (response, stream_events) = streamed_turn.into_parts();

            if response.tool_calls.is_empty() {
                let answer = response.content.clone();
                self.record_response(response).await?;
                context.ensure_not_cancelled()?;
                stream_events.commit_to(sink).await?;
                return Ok(answer);
            }

            self.record_response(response).await?;
        }

        bail!("tool call loop exceeded max iterations ({MAX_TOOL_CALL_ITERATIONS})")
    }

    async fn stream_turn(&self, request: LlmRequest, context: &RunContext) -> Result<StreamedTurn> {
        let mut stream_events = StreamEventBuffer::default();
        let response = tokio::select! {
            response = self.llm.stream(request, &mut stream_events, context) => response?,
            _ = context.cancelled() => bail!(RUN_CANCELLED),
        };

        Ok(StreamedTurn {
            response,
            events: stream_events.into_events(),
        })
    }
}

impl<L, M, T> Agent<L, M, T>
where
    M: MemoryStore,
    T: ToolExecutor,
{
    async fn build_llm_request(&self) -> Result<LlmRequest> {
        let mut messages = Vec::new();
        if !self.instructions.is_empty() {
            messages.push(Message::system(self.instructions.clone()));
        }
        messages.extend(self.memory.load_context().await?);

        Ok(LlmRequest {
            messages,
            tools: self.tools.definitions(),
        })
    }

    async fn record_response(&self, response: LlmResponse) -> Result<Option<String>> {
        if response.tool_calls.is_empty() {
            self.memory
                .append(Message::assistant(response.content.clone()))
                .await?;

            return Ok(Some(response.content));
        }

        self.memory
            .append(Message::assistant_tool_calls(
                response.content,
                response.tool_calls.clone(),
            ))
            .await?;

        for tool_call in response.tool_calls {
            let observation = match self
                .tools
                .execute(&tool_call.name, &tool_call.arguments)
                .await
            {
                Ok(result) => tool_observation::success(result),
                Err(error) => tool_observation::failure(&error),
            };

            self.memory
                .append(Message::tool_result(tool_call.id, observation))
                .await?;
        }

        Ok(None)
    }
}

struct StreamedTurn {
    response: LlmResponse,
    events: StreamEvents,
}

impl StreamedTurn {
    fn into_parts(self) -> (LlmResponse, StreamEvents) {
        (self.response, self.events)
    }
}

struct StreamEvents(Vec<LlmStreamEvent>);

impl StreamEvents {
    async fn commit_to(self, sink: &mut dyn LlmStreamSink) -> Result<()> {
        for event in self.0 {
            sink.on_event(event).await?;
        }

        Ok(())
    }
}

#[derive(Default)]
struct StreamEventBuffer {
    events: Vec<LlmStreamEvent>,
}

impl StreamEventBuffer {
    fn into_events(self) -> StreamEvents {
        StreamEvents(self.events)
    }
}

#[async_trait::async_trait]
impl LlmStreamSink for StreamEventBuffer {
    async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()> {
        self.events.push(event);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use tokio::sync::{Mutex, Notify};

    use crate::{
        agent::{
            Agent,
            message::{Message, Role},
            tool::{Tool, ToolExecutor},
        },
        llm::client::{
            LlmClient, LlmRequest, LlmResponse, LlmStreamClient, LlmStreamEvent, LlmStreamSink,
            LlmToolCall, RunContext,
        },
        memory::{in_memory::InMemoryStore, store::MemoryStore},
    };

    #[derive(Clone)]
    struct FakeClient {
        requests: Arc<Mutex<Vec<Vec<Message>>>>,
    }

    #[derive(Clone)]
    struct BlockingClient {
        started: Arc<Notify>,
    }

    #[derive(Clone)]
    struct ScriptedClient {
        requests: Arc<Mutex<Vec<Vec<Message>>>>,
        responses: Arc<Mutex<Vec<LlmResponse>>>,
    }

    impl ScriptedClient {
        fn new(responses: Vec<LlmResponse>) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(responses)),
            }
        }

        async fn requests(&self) -> Vec<Vec<Message>> {
            self.requests.lock().await.clone()
        }
    }

    #[async_trait]
    impl LlmClient for ScriptedClient {
        async fn generate(
            &self,
            llm_request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            self.requests.lock().await.push(llm_request.messages);

            let mut responses = self.responses.lock().await;
            if responses.is_empty() {
                bail!("scripted client has no response left");
            }

            Ok(responses.remove(0))
        }
    }

    #[async_trait]
    impl LlmStreamClient for ScriptedClient {
        async fn stream(
            &self,
            llm_request: LlmRequest,
            sink: &mut dyn LlmStreamSink,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            self.requests.lock().await.push(llm_request.messages);

            let mut responses = self.responses.lock().await;
            if responses.is_empty() {
                bail!("scripted client has no response left");
            }

            let response = responses.remove(0);
            if !response.content.is_empty() {
                sink.on_event(LlmStreamEvent::TextDelta(response.content.clone()))
                    .await?;
            }

            Ok(response)
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<LlmStreamEvent>,
    }

    #[async_trait]
    impl LlmStreamSink for RecordingSink {
        async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()> {
            self.events.push(event);

            Ok(())
        }
    }

    impl FakeClient {
        fn new() -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        async fn requests(&self) -> Vec<Vec<Message>> {
            self.requests.lock().await.clone()
        }
    }

    #[async_trait]
    impl LlmClient for BlockingClient {
        async fn generate(
            &self,
            _llm_request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            self.started.notify_waiters();
            std::future::pending().await
        }
    }

    #[async_trait]
    impl LlmClient for FakeClient {
        async fn generate(
            &self,
            llm_request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            self.requests.lock().await.push(llm_request.messages);

            Ok(LlmResponse {
                content: "answer".to_string(),
                tool_calls: Vec::new(),
            })
        }
    }

    struct NoTools;

    #[async_trait]
    impl ToolExecutor for NoTools {
        fn definitions(&self) -> Vec<Tool> {
            Vec::new()
        }

        async fn execute(&self, _name: &str, _arguments: &str) -> Result<String> {
            unreachable!("no tools are defined")
        }
    }

    enum ToolBehavior {
        Succeed(&'static str),
        Fail(&'static str),
    }

    struct OneTool {
        behavior: ToolBehavior,
    }

    #[async_trait]
    impl ToolExecutor for OneTool {
        fn definitions(&self) -> Vec<Tool> {
            vec![Tool::new("demo", "Demo tool", Default::default())]
        }

        async fn execute(&self, name: &str, _arguments: &str) -> Result<String> {
            assert_eq!(name, "demo");

            match self.behavior {
                ToolBehavior::Succeed(result) => Ok(result.to_string()),
                ToolBehavior::Fail(error) => bail!(error),
            }
        }
    }

    #[tokio::test]
    async fn system_instruction_is_sent_but_not_persisted() -> Result<()> {
        let client = FakeClient::new();
        let memory = InMemoryStore::new();
        let agent = Agent::new(client.clone(), memory, NoTools).set_instructions("system prompt");

        agent.execute("hello").await?;

        let requests = client.requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0][0], Message::system("system prompt"));

        let context = agent.memory.load_context().await?;

        assert_eq!(context.len(), 2);
        assert!(context.iter().all(|message| message.role != Role::System));
        assert_eq!(context[0], Message::user("hello"));
        assert_eq!(context[1], Message::assistant("answer"));

        Ok(())
    }

    #[tokio::test]
    async fn execute_returns_when_context_is_cancelled_during_llm_request() -> Result<()> {
        let started = Arc::new(Notify::new());
        let client = BlockingClient {
            started: started.clone(),
        };
        let agent = Agent::new(client, InMemoryStore::new(), NoTools);
        let context = RunContext::new();
        let task_context = context.clone();

        let task = tokio::spawn(async move {
            agent
                .execute_with_context("hello", &task_context)
                .await
                .expect_err("run should be cancelled")
        });

        started.notified().await;
        context.cancel();

        let error = task.await?;
        assert_eq!(error.to_string(), "run cancelled");

        Ok(())
    }

    #[tokio::test]
    async fn successful_tool_result_is_recorded_as_observation() -> Result<()> {
        let client = ScriptedClient::new(vec![
            LlmResponse {
                content: String::new(),
                tool_calls: vec![LlmToolCall {
                    id: "call_1".to_string(),
                    name: "demo".to_string(),
                    arguments: "{}".to_string(),
                }],
            },
            LlmResponse {
                content: "done".to_string(),
                tool_calls: Vec::new(),
            },
        ]);
        let memory = InMemoryStore::new();
        let agent = Agent::new(
            client.clone(),
            memory,
            OneTool {
                behavior: ToolBehavior::Succeed("tool output"),
            },
        );

        let answer = agent.execute("hello").await?;

        assert_eq!(answer, "done");
        let context = agent.memory.load_context().await?;
        assert_eq!(
            context[2],
            Message::tool_result("call_1", r#"{"ok":true,"result":"tool output"}"#)
        );

        let requests = client.requests().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1][2], context[2]);

        Ok(())
    }

    #[tokio::test]
    async fn failed_tool_result_is_recorded_as_observation_and_loop_continues() -> Result<()> {
        let client = ScriptedClient::new(vec![
            LlmResponse {
                content: String::new(),
                tool_calls: vec![LlmToolCall {
                    id: "call_1".to_string(),
                    name: "demo".to_string(),
                    arguments: "{}".to_string(),
                }],
            },
            LlmResponse {
                content: "recovered".to_string(),
                tool_calls: Vec::new(),
            },
        ]);
        let memory = InMemoryStore::new();
        let agent = Agent::new(
            client.clone(),
            memory,
            OneTool {
                behavior: ToolBehavior::Fail("tool exploded"),
            },
        );

        let answer = agent.execute("hello").await?;

        assert_eq!(answer, "recovered");
        let context = agent.memory.load_context().await?;
        assert_eq!(
            context[2],
            Message::tool_result("call_1", r#"{"ok":false,"error":"tool exploded"}"#)
        );

        let requests = client.requests().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1][2], context[2]);

        Ok(())
    }

    #[tokio::test]
    async fn streaming_discards_text_from_tool_call_iterations() -> Result<()> {
        let client = ScriptedClient::new(vec![
            LlmResponse {
                content: "<eos>".to_string(),
                tool_calls: vec![LlmToolCall {
                    id: "call_1".to_string(),
                    name: "demo".to_string(),
                    arguments: "{}".to_string(),
                }],
            },
            LlmResponse {
                content: "done".to_string(),
                tool_calls: Vec::new(),
            },
        ]);
        let memory = InMemoryStore::new();
        let agent = Agent::new(
            client,
            memory,
            OneTool {
                behavior: ToolBehavior::Succeed("tool output"),
            },
        );
        let mut sink = RecordingSink::default();

        let answer = agent.execute_streaming("hello", &mut sink).await?;

        assert_eq!(answer, "done");
        assert_eq!(
            sink.events,
            vec![LlmStreamEvent::TextDelta("done".to_string())]
        );

        Ok(())
    }
}
