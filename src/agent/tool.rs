use std::collections::BTreeMap;

use anyhow::Result;
use async_trait::async_trait;

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

#[async_trait]
pub trait ToolExecutor {
    fn definitions(&self) -> Vec<Tool>;

    async fn execute(&self, name: &str, arguments: &str) -> Result<String>;
}

#[async_trait]
pub trait ToolHandler: Send + Sync {
    fn name(&self) -> &'static str;

    fn definition(&self) -> Tool;

    async fn execute(&self, arguments: &str) -> Result<String>;
}
