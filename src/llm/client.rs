use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::agent::{message::Message, tool::Tool};

#[async_trait]
pub trait LlmClient {
    async fn generate(&self, llm_request: LlmRequest) -> Result<LlmResponse>;
}

#[async_trait]
pub trait LlmStreamClient {
    async fn stream(
        &self,
        llm_request: LlmRequest,
        sink: &mut dyn LlmStreamSink,
    ) -> Result<LlmResponse>;
}

#[async_trait]
pub trait LlmStreamSink: Send {
    async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()>;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmStreamEvent {
    TextDelta(String),
    ToolCallDelta(LlmToolCallDelta),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmToolCallDelta {
    pub index: u32,
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}
