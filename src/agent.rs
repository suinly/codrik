pub mod message;
pub mod tool;

use crate::agent::message::Message;
use crate::agent::tool::Tool;
use crate::llm::client::{LlmClient, LlmRequest};
use crate::memory::store::MemoryStore;
use anyhow::Result;

pub struct Agent<L, M> {
    instructions: String,
    tools: Vec<Tool>,
    llm: L,
    memory: M,
}

impl<L, M> Agent<L, M>
where
    L: LlmClient,
    M: MemoryStore,
{
    pub fn new(llm: L, memory: M) -> Self {
        Self {
            instructions: String::new(),
            tools: Vec::new(),
            memory,
            llm,
        }
    }

    pub fn set_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = instructions.into();
        self
    }

    pub fn add_tool(mut self, tool: Tool) -> Self {
        self.tools.push(tool);
        self
    }

    pub async fn execute(&self, content: impl Into<String>) -> Result<String> {
        self.memory
            .save(Message::system(self.instructions.clone()))
            .await?;
        self.memory.save(Message::user(content.into())).await?;

        for _ in 0..5 {
            let messages = self.memory.load_context().await?;

            let response = self
                .llm
                .generate(LlmRequest {
                    messages,
                    tools: self.tools.clone(),
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
                    .execute_tool(&tool_call.name, &tool_call.arguments)
                    .await?;

                self.memory
                    .save(Message::tool_result(tool_call.id, result))
                    .await?;
            }
        }

        anyhow::bail!("tool call loop exceeeded max iterations (5)")
    }

    async fn execute_tool(&self, name: &str, _arguments: &str) -> Result<String> {
        match name {
            "hello_world" => Ok("Hello World".to_string()),
            _ => anyhow::bail!("unknown tool: {name}"),
        }
    }
}
