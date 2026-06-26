pub mod message;
pub mod tool;
mod tool_observation;

use crate::agent::message::Message;
use crate::agent::tool::ToolExecutor;
use crate::llm::client::{LlmClient, LlmRequest};
use crate::memory::store::MemoryStore;
use anyhow::{Result, bail};

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
        self.memory.append(Message::user(content.into())).await?;

        for _ in 0..5 {
            let mut messages = Vec::new();
            if !self.instructions.is_empty() {
                messages.push(Message::system(self.instructions.clone()));
            }
            messages.extend(self.memory.load_context().await?);

            let response = self
                .llm
                .generate(LlmRequest {
                    messages,
                    tools: self.tools.definitions(),
                })
                .await?;

            if response.tool_calls.is_empty() {
                self.memory
                    .append(Message::assistant(response.content.clone()))
                    .await?;

                return Ok(response.content);
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
        }

        bail!("tool call loop exceeded max iterations (5)")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use crate::{
        agent::{
            Agent,
            message::{Message, Role},
            tool::{Tool, ToolExecutor},
        },
        llm::client::{LlmClient, LlmRequest, LlmResponse, LlmToolCall},
        memory::{in_memory::InMemoryStore, store::MemoryStore},
    };

    #[derive(Clone)]
    struct FakeClient {
        requests: Arc<Mutex<Vec<Vec<Message>>>>,
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
        async fn generate(&self, llm_request: LlmRequest) -> Result<LlmResponse> {
            self.requests.lock().await.push(llm_request.messages);

            let mut responses = self.responses.lock().await;
            if responses.is_empty() {
                bail!("scripted client has no response left");
            }

            Ok(responses.remove(0))
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
    impl LlmClient for FakeClient {
        async fn generate(&self, llm_request: LlmRequest) -> Result<LlmResponse> {
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
}
