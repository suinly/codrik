use anyhow::Result;
use async_trait::async_trait;

use crate::agent::message::Message;

#[async_trait]
pub trait MemoryStore {
    async fn save(&self, message: Message) -> Result<()>;
    async fn load_context(&self) -> Result<Vec<Message>>;
}
