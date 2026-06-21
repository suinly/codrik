pub mod message;
pub mod tool;

use crate::agent::message::Message;
use crate::llm::client::{LlmClient, LlmRequest};
use crate::memory::store::MemoryStore;
use anyhow::Result;

pub struct Agent<L, M> {
    instructions: String,
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
            memory,
            llm,
        }
    }

    pub fn set_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = instructions.into();
        self
    }

    pub async fn execute(&self, content: impl Into<String>) -> Result<String> {
        let instructions = Message::system(self.instructions.clone());
        self.memory.save(instructions).await?;
        self.memory.save(Message::user(content.into())).await?;

        let messages = self.memory.load_context().await?;
        let tools = Vec::new();

        let request = LlmRequest { messages, tools };
        let response = self.llm.generate(request).await?;

        let message = Message::assistant(response.content.clone());
        self.memory.save(message).await?;

        Ok(response.content)
    }
}
