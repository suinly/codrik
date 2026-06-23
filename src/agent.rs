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

                println!(
                    "Tool: `{}` called with arguments `{}` returned: {}",
                    &tool_call.name, &tool_call.arguments, &result
                );

                self.memory
                    .save(Message::tool_result(tool_call.id, result))
                    .await?;
            }
        }

        bail!("tool call loop exceeeded max iterations (5)")
    }
}
