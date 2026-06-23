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
}
