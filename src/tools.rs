use std::collections::HashSet;

mod datetime;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::agent::tool::{Tool, ToolExecutor, ToolHandler};

pub struct ToolRegistry {
    handlers: Vec<Box<dyn ToolHandler>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            handlers: vec![Box::new(datetime::DatetimeTool)],
        }
    }

    pub fn with_allowed_tools(allowed_tools: impl IntoIterator<Item = String>) -> Self {
        let allowed_tools = allowed_tools.into_iter().collect::<HashSet<_>>();
        if allowed_tools.contains("*") {
            return Self::new();
        }

        let handlers = Self::new()
            .handlers
            .into_iter()
            .filter(|handler| allowed_tools.contains(handler.name()))
            .collect();

        Self { handlers }
    }
}

#[async_trait]
impl ToolExecutor for ToolRegistry {
    fn definitions(&self) -> Vec<Tool> {
        self.handlers
            .iter()
            .map(|handler| handler.definition())
            .collect()
    }

    async fn execute(&self, name: &str, arguments: &str) -> Result<String> {
        let Some(handler) = self.handlers.iter().find(|handler| handler.name() == name) else {
            bail!("unknown tool: {name}");
        };

        handler.execute(arguments).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definitions_include_datetime() {
        let tools = ToolRegistry::new().definitions();
        let datetime_name = datetime::DatetimeTool.definition().name;

        assert!(tools.iter().any(|tool| tool.name == datetime_name));
    }

    #[test]
    fn definitions_hide_disallowed_tools() {
        let tools = ToolRegistry::with_allowed_tools(Vec::<String>::new()).definitions();

        assert!(tools.is_empty());
    }

    #[test]
    fn wildcard_allows_all_tools() {
        let tools = ToolRegistry::with_allowed_tools(vec!["*".to_string()]).definitions();
        let datetime_name = datetime::DatetimeTool.definition().name;

        assert!(tools.iter().any(|tool| tool.name == datetime_name));
    }

    #[tokio::test]
    async fn execute_rejects_disallowed_tools() {
        let result = ToolRegistry::with_allowed_tools(Vec::<String>::new())
            .execute("datetime", "{}")
            .await;

        assert_eq!(result.unwrap_err().to_string(), "unknown tool: datetime");
    }
}
