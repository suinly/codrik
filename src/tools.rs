use std::{collections::HashSet, path::PathBuf};

mod bash;
mod bashkit;
mod datetime;
mod send_file;
mod skills;
mod web_browser;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::agent::tool::{
    Tool, ToolCallContext, ToolCapabilities, ToolExecution, ToolExecutor, ToolExposure, ToolHandler,
};
use crate::skills::SkillRegistry;
pub use send_file::FileRoot;

pub struct ToolRegistry {
    handlers: Vec<Box<dyn ToolHandler>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolRegistryConfig {
    pub actor_workspace: Option<PathBuf>,
    pub skill_roots: Vec<crate::skills::SkillRoot>,
    pub file_roots: Vec<FileRoot>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::with_config(ToolRegistryConfig::default())
    }

    pub fn with_config(config: ToolRegistryConfig) -> Self {
        let actor_workspace = config.actor_workspace;
        let skill_registry = SkillRegistry::new(config.skill_roots);
        Self {
            handlers: vec![
                Box::new(datetime::DatetimeTool),
                Box::new(send_file::SendFileTool::new(
                    send_file::SendFileToolConfig {
                        roots: config.file_roots,
                    },
                )),
                Box::new(skills::SkillsListTool::new(skill_registry.clone())),
                Box::new(skills::SkillsReadTool::new(skill_registry.clone())),
                Box::new(skills::SkillsCreateTool::new(skill_registry.clone())),
                Box::new(skills::SkillsUpdateTool::new(skill_registry)),
                Box::new(bashkit::BashkitTool::new(bashkit::BashkitToolConfig {
                    workspace: actor_workspace.clone(),
                })),
                Box::new(web_browser::WebBrowserTool::new(
                    web_browser::WebBrowserToolConfig::default(),
                )),
                Box::new(bash::BashTool::new(bash::BashToolConfig {
                    default_cwd: actor_workspace,
                })),
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

    fn capabilities(&self, name: &str) -> Option<ToolCapabilities> {
        self.handlers
            .iter()
            .find(|handler| handler.name() == name)
            .map(|handler| handler.capabilities())
    }

    async fn execute(
        &self,
        name: &str,
        arguments: &str,
        context: &ToolCallContext,
    ) -> Result<ToolExecution> {
        let Some(handler) = self.handlers.iter().find(|handler| handler.name() == name) else {
            bail!("unknown tool: {name}");
        };

        handler.execute_typed(arguments, context).await
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
    fn tool_capabilities_are_conservative_by_default() {
        let registry = ToolRegistry::new();

        assert!(registry.capabilities("datetime").unwrap().retry_safe);
        assert!(registry.capabilities("send_file").unwrap().retry_safe);
        assert!(!registry.capabilities("bash").unwrap().retry_safe);
        assert!(!registry.capabilities("skills_update").unwrap().retry_safe);
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
        let bash_name = bash::BashTool::default().definition().name;

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
        let bash_name = bash::BashTool::default().definition().name;

        assert!(tools.iter().any(|tool| tool.name == datetime_name));
        assert!(tools.iter().any(|tool| tool.name == bashkit_name));
        assert!(tools.iter().any(|tool| tool.name == "web_browser"));
        assert!(tools.iter().any(|tool| tool.name == "skills_list"));
        assert!(tools.iter().any(|tool| tool.name == "skills_read"));
        assert!(tools.iter().any(|tool| tool.name == "skills_create"));
        assert!(tools.iter().any(|tool| tool.name == "skills_update"));
        let send_file = tools
            .iter()
            .find(|tool| tool.name == "send_file")
            .expect("wildcard should allow send_file");
        assert!(send_file.description.contains("existing file"));
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
    async fn configured_actor_workspace_is_real_bash_default_cwd() {
        let workspace = std::env::current_dir()
            .expect("current dir should exist")
            .join("src");
        let registry = ToolRegistry::with_allowed_tools_and_config(
            vec!["bash".to_string()],
            ToolRegistryConfig {
                actor_workspace: Some(workspace.clone()),
                ..default_config()
            },
        );

        let execution = registry
            .execute(
                "bash",
                r#"{"command":"pwd"}"#,
                &ToolCallContext::legacy(crate::llm::client::RunContext::new()),
            )
            .await
            .expect("bash should execute");
        let result: serde_json::Value =
            serde_json::from_str(&execution.observation).expect("bash observation should be json");

        assert_eq!(
            result["stdout"]
                .as_str()
                .expect("stdout should be a string")
                .trim(),
            workspace.to_string_lossy()
        );
    }

    #[tokio::test]
    async fn execute_rejects_disallowed_tools() {
        let result =
            ToolRegistry::with_allowed_tools_and_config(Vec::<String>::new(), default_config())
                .execute(
                    "datetime",
                    "{}",
                    &ToolCallContext::legacy(crate::llm::client::RunContext::new()),
                )
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
