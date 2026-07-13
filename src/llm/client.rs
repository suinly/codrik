use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::agent::{message::Message, tool::Tool};

pub const RUN_CANCELLED: &str = "run cancelled";

#[async_trait]
pub trait LlmClient {
    async fn generate(&self, llm_request: LlmRequest, context: &RunContext) -> Result<LlmResponse>;
}

#[async_trait]
pub trait LlmStreamClient {
    async fn stream(
        &self,
        llm_request: LlmRequest,
        sink: &mut dyn LlmStreamSink,
        context: &RunContext,
    ) -> Result<LlmResponse>;
}

#[async_trait]
pub trait LlmStreamSink: Send {
    async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentActivityEvent {
    ModelStepStarted,
    Description(String),
    ToolStarted { name: String },
    ToolFinished { name: String, succeeded: bool },
    Completed,
    Cancelled,
    Failed,
}

#[async_trait]
pub trait AgentActivitySink: Send {
    async fn on_activity(&mut self, event: AgentActivityEvent);
}

pub struct NoopAgentActivitySink;

#[async_trait]
impl AgentActivitySink for NoopAgentActivitySink {
    async fn on_activity(&mut self, _event: AgentActivityEvent) {}
}

#[derive(Clone)]
pub struct LlmRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<Tool>,
}

#[derive(Clone, Default)]
pub struct RunContext {
    cancellation: CancellationToken,
}

impl RunContext {
    pub fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
        }
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub fn ensure_not_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
            anyhow::bail!(RUN_CANCELLED);
        }

        Ok(())
    }
}

pub fn is_run_cancelled_error(error: &anyhow::Error) -> bool {
    error.to_string() == RUN_CANCELLED
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
