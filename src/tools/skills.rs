use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    agent::tool::{Tool, ToolHandler, ToolParameter, ToolParameters},
    skills::{Skill, SkillRegistry},
};

pub struct SkillsListTool {
    registry: SkillRegistry,
}

impl SkillsListTool {
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl ToolHandler for SkillsListTool {
    fn name(&self) -> &'static str {
        "skills_list"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "List available local skills with their name, description, and source.",
            ToolParameters::new(),
        )
    }

    async fn execute(&self, _arguments: &str) -> Result<String> {
        serialize_skills(self.registry.list()?)
    }
}

pub struct SkillsReadTool {
    registry: SkillRegistry,
}

impl SkillsReadTool {
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }
}

#[derive(Deserialize)]
struct SkillsReadArguments {
    name: String,
    path: Option<String>,
}

#[async_trait]
impl ToolHandler for SkillsReadTool {
    fn name(&self) -> &'static str {
        "skills_read"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Read a skill file by skill name. Reads SKILL.md by default, or a relative path inside the skill directory.",
            ToolParameters::new()
                .required("name", ToolParameter::string("Skill name to read."))
                .optional(
                    "path",
                    ToolParameter::string(
                        "Optional relative file path inside the skill directory. Defaults to SKILL.md.",
                    ),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: SkillsReadArguments =
            serde_json::from_str(arguments).context("failed to parse skills_read arguments")?;

        self.registry
            .read(&arguments.name, arguments.path.as_deref())
    }
}

pub struct SkillsCreateTool {
    registry: SkillRegistry,
}

impl SkillsCreateTool {
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }
}

#[derive(Deserialize)]
struct SkillsCreateArguments {
    name: String,
    description: String,
    body: String,
}

#[async_trait]
impl ToolHandler for SkillsCreateTool {
    fn name(&self) -> &'static str {
        "skills_create"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Create a local skill in the first writable skill root and return the created skill summary.",
            ToolParameters::new()
                .required("name", ToolParameter::string("New skill name."))
                .required(
                    "description",
                    ToolParameter::string("Short description of when the skill should be used."),
                )
                .required("body", ToolParameter::string("Markdown body for SKILL.md.")),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: SkillsCreateArguments =
            serde_json::from_str(arguments).context("failed to parse skills_create arguments")?;
        let skill =
            self.registry
                .create(&arguments.name, &arguments.description, &arguments.body)?;

        serialize_skill(skill)
    }
}

pub struct SkillsUpdateTool {
    registry: SkillRegistry,
}

impl SkillsUpdateTool {
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }
}

#[derive(Deserialize)]
struct SkillsUpdateArguments {
    name: String,
    description: String,
    body: String,
}

#[async_trait]
impl ToolHandler for SkillsUpdateTool {
    fn name(&self) -> &'static str {
        "skills_update"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Update an existing local user skill in a writable skill root and return the updated skill summary.",
            ToolParameters::new()
                .required("name", ToolParameter::string("Existing user skill name."))
                .required(
                    "description",
                    ToolParameter::string(
                        "Updated short description of when the skill should be used.",
                    ),
                )
                .required(
                    "body",
                    ToolParameter::string("Updated Markdown body for SKILL.md."),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: SkillsUpdateArguments =
            serde_json::from_str(arguments).context("failed to parse skills_update arguments")?;
        let skill =
            self.registry
                .update(&arguments.name, &arguments.description, &arguments.body)?;

        serialize_skill(skill)
    }
}

#[derive(Serialize)]
struct SkillSummary {
    name: String,
    description: String,
    source: String,
}

impl From<Skill> for SkillSummary {
    fn from(skill: Skill) -> Self {
        Self {
            name: skill.name,
            description: skill.description,
            source: skill.source,
        }
    }
}

fn serialize_skill(skill: Skill) -> Result<String> {
    serde_json::to_string(&SkillSummary::from(skill)).context("failed to serialize skill")
}

fn serialize_skills(skills: Vec<Skill>) -> Result<String> {
    let skills = skills
        .into_iter()
        .map(SkillSummary::from)
        .collect::<Vec<_>>();
    serde_json::to_string(&skills).context("failed to serialize skills")
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;

    use crate::{
        agent::tool::ToolHandler,
        skills::{SkillRegistry, SkillRoot},
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn list_returns_skills_as_json_array() -> Result<()> {
        let root = temp_root("list")?;
        write_skill(
            &root,
            "tdd",
            "---\nname: tdd\ndescription: Use tests first.\n---\n# TDD\n",
        )?;
        let tool = super::SkillsListTool::new(SkillRegistry::new(vec![SkillRoot::read_only(
            &root, "test",
        )]));

        let result = tool.execute("{}").await?;

        assert_eq!(
            result,
            r#"[{"name":"tdd","description":"Use tests first.","source":"test"}]"#
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_returns_requested_skill_content() -> Result<()> {
        let root = temp_root("read")?;
        write_skill(&root, "tdd", "# TDD\nUse tests first.\n")?;
        fs::create_dir_all(root.join("tdd").join("references"))?;
        fs::write(root.join("tdd").join("references").join("loop.md"), "red\n")?;
        let tool = super::SkillsReadTool::new(SkillRegistry::new(vec![SkillRoot::read_only(
            &root, "test",
        )]));

        let result = tool
            .execute(r#"{"name":"tdd","path":"references/loop.md"}"#)
            .await?;

        assert_eq!(result, "red\n");
        Ok(())
    }

    #[tokio::test]
    async fn create_returns_created_skill_summary() -> Result<()> {
        let root = temp_root("create")?;
        let tool = super::SkillsCreateTool::new(SkillRegistry::new(vec![SkillRoot::writable(
            &root, "user",
        )]));

        let result = tool
            .execute(
                r##"{"name":"release","description":"Release checklist.","body":"# Release\n"}"##,
            )
            .await?;

        assert_eq!(
            result,
            r#"{"name":"release","description":"Release checklist.","source":"user"}"#
        );
        assert_eq!(
            fs::read_to_string(root.join("release").join("SKILL.md"))?,
            "---\nname: release\ndescription: Release checklist.\n---\n\n# Release\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_returns_updated_skill_summary() -> Result<()> {
        let root = temp_root("update")?;
        write_skill(
            &root,
            "release",
            "---\nname: release\ndescription: Old release.\n---\n\n# Old\n",
        )?;
        let tool = super::SkillsUpdateTool::new(SkillRegistry::new(vec![SkillRoot::writable(
            &root, "user",
        )]));

        let result = tool
            .execute(
                r##"{"name":"release","description":"Updated release.","body":"# Updated\n"}"##,
            )
            .await?;

        assert_eq!(
            result,
            r#"{"name":"release","description":"Updated release.","source":"user"}"#
        );
        assert_eq!(
            fs::read_to_string(root.join("release").join("SKILL.md"))?,
            "---\nname: release\ndescription: Updated release.\n---\n\n# Updated\n"
        );
        Ok(())
    }

    fn write_skill(root: &Path, name: &str, content: &str) -> Result<()> {
        let dir = root.join(name);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("SKILL.md"), content)?;
        Ok(())
    }

    fn temp_root(label: &str) -> Result<PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .to_string();
        let path = std::env::temp_dir().join(format!(
            "codrik-tools-skills-{label}-{}-{unique}",
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&path)?;
        Ok(path)
    }
}
