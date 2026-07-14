use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::llm::client::RunContext;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub parameters: ToolParameters,
}

impl Tool {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: ToolParameters,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ToolParameters {
    pub properties: BTreeMap<String, ToolParameter>,
    pub required: Vec<String>,
}

impl ToolParameters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn required(mut self, name: impl Into<String>, parameter: ToolParameter) -> Self {
        let name = name.into();
        self.properties.insert(name.clone(), parameter);
        self.required.push(name);
        self
    }

    pub fn optional(mut self, name: impl Into<String>, parameter: ToolParameter) -> Self {
        self.properties.insert(name.into(), parameter);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolParameter {
    pub kind: ToolParameterKind,
    pub description: String,
    pub allowed_values: Vec<String>,
}

impl ToolParameter {
    pub fn string(description: impl Into<String>) -> Self {
        Self {
            kind: ToolParameterKind::String,
            description: description.into(),
            allowed_values: Vec::new(),
        }
    }
    pub fn number(description: impl Into<String>) -> Self {
        Self {
            kind: ToolParameterKind::Number,
            description: description.into(),
            allowed_values: Vec::new(),
        }
    }
    pub fn boolean(description: impl Into<String>) -> Self {
        Self {
            kind: ToolParameterKind::Boolean,
            description: description.into(),
            allowed_values: Vec::new(),
        }
    }

    pub fn string_enum(
        description: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            kind: ToolParameterKind::String,
            description: description.into(),
            allowed_values: values.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolParameterKind {
    String,
    Number,
    Boolean,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecution {
    pub observation: String,
    pub artifacts: Vec<ToolArtifact>,
}

impl ToolExecution {
    pub fn text(observation: impl Into<String>) -> Self {
        Self {
            observation: observation.into(),
            artifacts: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolArtifact {
    File(FileArtifact),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileArtifact {
    pub path: PathBuf,
    pub display_name: String,
    pub media_type: String,
    pub caption: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCapabilities {
    pub retry_safe: bool,
    pub accepts_idempotency_key: bool,
    pub cancellable: bool,
    pub outcome_probe: bool,
    pub compensatable: bool,
    pub requires_approval: bool,
}

impl ToolCapabilities {
    pub fn conservative() -> Self {
        Self {
            retry_safe: false,
            accepts_idempotency_key: false,
            cancellable: false,
            outcome_probe: false,
            compensatable: false,
            requires_approval: false,
        }
    }

    pub fn read_only() -> Self {
        Self {
            retry_safe: true,
            ..Self::conservative()
        }
    }
}

#[derive(Clone)]
pub struct ToolCallContext {
    pub attempt_id: String,
    pub authorized_tools: Vec<String>,
    pub cancellation: RunContext,
}

impl ToolCallContext {
    pub fn legacy(cancellation: RunContext) -> Self {
        Self {
            attempt_id: Uuid::new_v4().to_string(),
            authorized_tools: Vec::new(),
            cancellation,
        }
    }
}

#[async_trait]
pub trait ToolExecutor {
    fn definitions(&self) -> Vec<Tool>;
    fn capabilities(&self, name: &str) -> Option<ToolCapabilities>;

    async fn execute(
        &self,
        name: &str,
        arguments: &str,
        context: &ToolCallContext,
    ) -> Result<ToolExecution>;
}

#[async_trait]
pub trait ToolHandler: Send + Sync {
    fn name(&self) -> &'static str;

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Standard
    }

    fn definition(&self) -> Tool;

    fn capabilities(&self) -> ToolCapabilities {
        ToolCapabilities::conservative()
    }

    async fn execute(&self, arguments: &str) -> Result<String>;

    async fn execute_typed(
        &self,
        arguments: &str,
        _context: &ToolCallContext,
    ) -> Result<ToolExecution> {
        Ok(ToolExecution::text(self.execute(arguments).await?))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolExposure {
    Standard,
    Privileged,
}
