pub mod message;
pub mod tool;

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
        self.memory.save(Message::user(content.into())).await?;

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
                    .save(Message::assistant(response.content.clone()))
                    .await?;

                return Ok(response.content);
            }

            self.memory
                .save(Message::assistant_tool_calls(
                    response.content,
                    response.tool_calls.clone(),
                ))
                .await?;

            for tool_call in response.tool_calls {
                let result = self
                    .tools
                    .execute(&tool_call.name, &tool_call.arguments)
                    .await?;

                self.memory
                    .save(Message::tool_result(tool_call.id, result))
                    .await?;
            }
        }

        bail!("tool call loop exceeeded max iterations (5)")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use crate::{
        agent::{
            Agent,
            message::{Message, Role},
            tool::{Tool, ToolExecutor},
        },
        llm::client::{LlmClient, LlmRequest, LlmResponse},
        memory::{in_memory::InMemoryStore, store::MemoryStore},
    };

    #[derive(Clone)]
    struct FakeClient {
        requests: Arc<Mutex<Vec<Vec<Message>>>>,
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
}
