use anyhow::Result;
use async_trait::async_trait;

use crate::agent::{message::Message, tool::Tool};

#[async_trait]
pub trait LlmClient {
    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse>;
}

pub struct LlmRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<Tool>,
}

#[derive(Clone)]
pub struct LlmResponse {
    pub content: String,
}
