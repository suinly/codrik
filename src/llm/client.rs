use crate::agent::message::Message;

use async_trait::async_trait;

#[async_trait]
pub trait LlmClient {
    async fn generate(&self, messages: Vec<Message>) -> anyhow::Result<String>;
}
