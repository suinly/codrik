use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::agent::{message::Message, tool::Tool};

#[async_trait]
pub trait LlmClient {
    async fn generate(&self, llm_request: LlmRequest) -> Result<LlmResponse>;
}

pub struct LlmRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<Tool>,
}

#[derive(Debug, Clone)]
pub struct LlmResponse {
    pub content: String,
    pub tool_calls: Vec<LlmToolCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}
