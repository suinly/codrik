use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{agent::message::Message, memory::store::MemoryStore};

pub struct InMemoryStore {
    messages: Mutex<Vec<Message>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl MemoryStore for InMemoryStore {
    async fn save(&self, message: Message) -> Result<()> {
        let mut messages = self.messages.lock().await;
        messages.push(message);
        Ok(())
    }

    async fn load_context(&self) -> Result<Vec<Message>> {
        let messages = self.messages.lock().await;
        Ok(messages.clone())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{agent::message::Message, memory::store::MemoryStore};

    use super::InMemoryStore;

    #[tokio::test]
    async fn message_is_saved_in_memory() -> Result<()> {
        let memory = InMemoryStore::new();
        let message = Message::user("Test message");

        memory.save(message.clone()).await?;

        let context = memory.load_context().await?;

        assert_eq!(context, vec![message]);

        Ok(())
    }
}
