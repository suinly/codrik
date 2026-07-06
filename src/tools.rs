use std::{collections::HashSet, path::PathBuf};

mod bash;
mod bashkit;
mod datetime;
mod web_browser;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::agent::tool::{Tool, ToolExecutor, ToolExposure, ToolHandler};

pub struct ToolRegistry {
    handlers: Vec<Box<dyn ToolHandler>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolRegistryConfig {
    pub bashkit_workspace: Option<PathBuf>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::with_config(ToolRegistryConfig::default())
    }

    pub fn with_config(config: ToolRegistryConfig) -> Self {
        Self {
            handlers: vec![
                Box::new(datetime::DatetimeTool),
                Box::new(bashkit::BashkitTool::new(bashkit::BashkitToolConfig {
                    workspace: config.bashkit_workspace,
                })),
                Box::new(web_browser::WebBrowserTool::new(
                    web_browser::WebBrowserToolConfig::default(),
                )),
                Box::new(bash::BashTool),
            ],
        }
    }

    pub fn with_allowed_tools_and_config(
        allowed_tools: impl IntoIterator<Item = String>,
        config: ToolRegistryConfig,
    ) -> Self {
        let allowed_tools = allowed_tools.into_iter().collect::<HashSet<_>>();
        let handlers = Self::with_config(config)
            .handlers
            .into_iter()
            .filter(|handler| {
                allowed_tools.contains(handler.name())
                    || (allowed_tools.contains("*") && handler.exposure() == ToolExposure::Standard)
            })
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
    fn definitions_include_bashkit() {
        let tools = ToolRegistry::new().definitions();
        let bashkit_name = bashkit::BashkitTool::new(bashkit::BashkitToolConfig::default())
            .definition()
            .name;

        assert!(tools.iter().any(|tool| tool.name == bashkit_name));
    }

    #[test]
    fn definitions_include_privileged_bash() {
        let tools = ToolRegistry::new().definitions();
        let bash_name = bash::BashTool.definition().name;

        assert!(tools.iter().any(|tool| tool.name == bash_name));
    }

    #[test]
    fn definitions_include_web_browser() {
        let tools = ToolRegistry::new().definitions();

        assert!(tools.iter().any(|tool| tool.name == "web_browser"));
    }

    #[test]
    fn definitions_hide_disallowed_tools() {
        let tools =
            ToolRegistry::with_allowed_tools_and_config(Vec::<String>::new(), default_config())
                .definitions();

        assert!(tools.is_empty());
    }

    #[test]
    fn wildcard_allows_standard_tools_only() {
        let tools =
            ToolRegistry::with_allowed_tools_and_config(vec!["*".to_string()], default_config())
                .definitions();
        let datetime_name = datetime::DatetimeTool.definition().name;
        let bashkit_name = bashkit::BashkitTool::new(bashkit::BashkitToolConfig::default())
            .definition()
            .name;
        let bash_name = bash::BashTool.definition().name;

        assert!(tools.iter().any(|tool| tool.name == datetime_name));
        assert!(tools.iter().any(|tool| tool.name == bashkit_name));
        assert!(tools.iter().any(|tool| tool.name == "web_browser"));
        assert!(!tools.iter().any(|tool| tool.name == bash_name));
    }

    #[test]
    fn explicit_bash_grant_allows_privileged_bash() {
        let tools =
            ToolRegistry::with_allowed_tools_and_config(vec!["bash".to_string()], default_config())
                .definitions();

        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "bash");
    }

    #[tokio::test]
    async fn execute_rejects_disallowed_tools() {
        let result =
            ToolRegistry::with_allowed_tools_and_config(Vec::<String>::new(), default_config())
                .execute("datetime", "{}")
                .await;

        assert_eq!(result.unwrap_err().to_string(), "unknown tool: datetime");
    }

    #[test]
    fn renamed_bashkit_tool_is_allowed_by_new_name() {
        let tools = ToolRegistry::with_allowed_tools_and_config(
            vec!["bashkit".to_string()],
            default_config(),
        )
        .definitions();

        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "bashkit");
    }

    #[test]
    fn explicit_bash_grant_does_not_allow_bashkit() {
        let tools =
            ToolRegistry::with_allowed_tools_and_config(vec!["bash".to_string()], default_config())
                .definitions();

        assert!(!tools.iter().any(|tool| tool.name == "bashkit"));
    }

    fn default_config() -> ToolRegistryConfig {
        ToolRegistryConfig::default()
    }
}
