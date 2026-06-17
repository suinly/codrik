use anyhow::Result;

use crate::agent::message::Message;
use crate::llm::client::LlmClient;
use crate::memory::store::MemoryStore;

pub struct AgentLoop<L, M> {
    llm: L,
    memory: M,
}

impl<L, M> AgentLoop<L, M>
where
    L: LlmClient,
    M: MemoryStore,
{
    pub fn new(llm: L, memory: M) -> Self {
        Self { llm, memory }
    }

    pub async fn run(&self, input: impl Into<String>) -> Result<String> {
        self.memory.save(Message::user(input.into())).await?;

        let context = self.memory.load_context().await?;
        let answer = self.llm.generate(context).await?;

        self.memory.save(Message::assistant(answer.clone())).await?;

        Ok(answer)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::message::{Message, Role},
        llm::client::LlmClient,
        memory::{in_memory::InMemoryStore, store::MemoryStore},
    };

    use super::AgentLoop;
    use anyhow::Result;
    use async_trait::async_trait;

    pub struct DummyClient {
        answer: String,
    }

    impl DummyClient {
        pub fn new(answer: impl Into<String>) -> Self {
            Self {
                answer: answer.into(),
            }
        }
    }

    #[async_trait]
    impl LlmClient for DummyClient {
        async fn generate(&self, _messages: Vec<Message>) -> Result<String> {
            Ok(self.answer.clone())
        }
    }

    #[tokio::test]
    async fn agent_returns_llm_answer() -> Result<()> {
        let llm = DummyClient::new("Готово");
        let memory = InMemoryStore::new();

        let agent = AgentLoop::new(llm, memory);

        let result = agent.run("Привет").await?;

        assert_eq!(result, "Готово");

        let messages = agent.memory.load_context().await?;

        assert_eq!(messages[0].role, Role::User);
        assert_eq!(messages[0].content, "Привет");
        assert_eq!(messages[1].role, Role::Assistant);
        assert_eq!(messages[1].content, "Готово");

        Ok(())
    }
}
