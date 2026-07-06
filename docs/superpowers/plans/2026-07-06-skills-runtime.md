# Skills Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add local `SKILL.md` support that every actor can use through standard tools, including agent-created skills.

**Architecture:** Add a focused `skills` module that owns discovery, frontmatter parsing, safe path resolution, and skill creation under configured roots. Expose that module through standard tool handlers in `src/tools/skills.rs`, then wire skill roots from `src/app.rs` so CLI and gateways both get the same capability. Keep provider, memory, and auth contracts unchanged.

**Tech Stack:** Rust 2024, `serde_json`, `tokio` tests where async tool execution is involved, existing `ToolHandler` / `ToolRegistry` abstractions.

---

## File Structure

- Create `src/skills.rs`: domain model and filesystem-backed registry.
- Create `src/tools/skills.rs`: standard tools `skills_list`, `skills_read`, and `skills_create`.
- Modify `src/tools.rs`: include skill tools in `ToolRegistry`, pass skill roots through `ToolRegistryConfig`, and assert wildcard access.
- Modify `src/app.rs`: configure default roots `~/.codrik/skills` and `.codrik/skills` for CLI and actor agents.
- Modify `src/main.rs`: register the new `skills` module.
- Modify `agent_instructions.md`: teach the model when and how to use skills without exposing irrelevant skill text.
- Modify `README.md`: document local skill layout and the three tools.

## Design Decisions

- Skill tools are `ToolExposure::Standard`, so `"tools": ["*"]` grants them automatically.
- `skills_create` writes only to the user-level root `~/.codrik/skills/<name>/SKILL.md`, not the project root. This avoids silently mutating a checked-out repo from Telegram.
- `skills_read` may read `SKILL.md` and relative files inside a known skill directory. It rejects absolute paths, `..`, missing skill names, and paths outside the resolved skill root.
- `skills_list` returns a compact JSON array with `name`, `description`, and `source`.
- Skill discovery supports two roots, in precedence order:
  1. `.codrik/skills` in the current working directory
  2. `~/.codrik/skills`
- Duplicate skill names are de-duplicated by precedence: the first discovered root wins.
- Frontmatter parsing is intentionally small: a `SKILL.md` may start with YAML-like `---` metadata; only `name:` and `description:` are read. No new YAML dependency is needed for the first slice.

---

### Task 1: Add Skill Registry Model

**Files:**
- Create: `src/skills.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Write failing registry tests**

Add `mod skills;` to `src/main.rs` so the new module can compile once created.

Create `src/skills.rs` with the tests first:

```rust
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRegistry {
    roots: Vec<SkillRoot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    path: PathBuf,
    source: String,
    writable: bool,
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn list_discovers_skill_frontmatter() -> Result<()> {
        let root = temp_root("list-discovers")?;
        write_skill(
            &root,
            "telegram-debug",
            "---\nname: telegram-debug\ndescription: Use when debugging Telegram gateway failures.\n---\n# Telegram Debug\n",
        )?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let skills = registry.list()?;

        assert_eq!(
            skills,
            vec![Skill {
                name: "telegram-debug".to_string(),
                description: "Use when debugging Telegram gateway failures.".to_string(),
                source: "test".to_string(),
            }]
        );
        Ok(())
    }

    #[test]
    fn list_uses_directory_name_when_name_is_missing() -> Result<()> {
        let root = temp_root("list-dir-name")?;
        write_skill(&root, "release", "---\ndescription: Release checklist.\n---\n# Release\n")?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let skills = registry.list()?;

        assert_eq!(skills[0].name, "release");
        assert_eq!(skills[0].description, "Release checklist.");
        Ok(())
    }

    #[test]
    fn list_deduplicates_by_root_precedence() -> Result<()> {
        let project = temp_root("project")?;
        let user = temp_root("user")?;
        write_skill(&project, "tdd", "---\ndescription: Project TDD.\n---\n# TDD\n")?;
        write_skill(&user, "tdd", "---\ndescription: User TDD.\n---\n# TDD\n")?;
        let registry = SkillRegistry::new(vec![
            SkillRoot::read_only(&project, "project"),
            SkillRoot::read_only(&user, "user"),
        ]);

        let skills = registry.list()?;

        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].description, "Project TDD.");
        assert_eq!(skills[0].source, "project");
        Ok(())
    }

    #[test]
    fn read_skill_md_returns_content() -> Result<()> {
        let root = temp_root("read-skill")?;
        write_skill(&root, "tdd", "# TDD\nUse tests first.\n")?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let content = registry.read("tdd", None)?;

        assert_eq!(content, "# TDD\nUse tests first.\n");
        Ok(())
    }

    #[test]
    fn read_relative_reference_stays_inside_skill_directory() -> Result<()> {
        let root = temp_root("read-ref")?;
        write_skill(&root, "tdd", "# TDD\n")?;
        fs::create_dir_all(root.join("tdd").join("references"))?;
        fs::write(root.join("tdd").join("references").join("loop.md"), "red green\n")?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let content = registry.read("tdd", Some("references/loop.md"))?;

        assert_eq!(content, "red green\n");
        Ok(())
    }

    #[test]
    fn read_rejects_path_traversal() -> Result<()> {
        let root = temp_root("reject-traversal")?;
        write_skill(&root, "tdd", "# TDD\n")?;
        fs::write(root.join("secret.md"), "secret")?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let error = registry.read("tdd", Some("../secret.md")).unwrap_err();

        assert!(error.to_string().contains("skill path escapes skill directory"));
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
            "codrik-skills-{label}-{}-{unique}",
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&path)?;
        Ok(path)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
rtk cargo test skills::
```

Expected: compile failures for missing methods such as `SkillRegistry::new`, `SkillRoot::read_only`, `SkillRegistry::list`, and `SkillRegistry::read`.

- [ ] **Step 3: Implement registry**

Replace the non-test body in `src/skills.rs` with this implementation, keeping the tests:

```rust
use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result, bail};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRegistry {
    roots: Vec<SkillRoot>,
}

impl SkillRegistry {
    pub fn new(roots: Vec<SkillRoot>) -> Self {
        Self { roots }
    }

    pub fn list(&self) -> Result<Vec<Skill>> {
        let mut skills = BTreeMap::new();

        for root in &self.roots {
            if !root.path.exists() {
                continue;
            }

            for entry in fs::read_dir(&root.path)
                .with_context(|| format!("failed to read skills root: {}", root.path.display()))?
            {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }

                let skill_dir = entry.path();
                let skill_md = skill_dir.join("SKILL.md");
                if !skill_md.is_file() {
                    continue;
                }

                let fallback_name = entry.file_name().to_string_lossy().to_string();
                let content = fs::read_to_string(&skill_md)
                    .with_context(|| format!("failed to read skill: {}", skill_md.display()))?;
                let metadata = parse_metadata(&content);
                let name = metadata
                    .get("name")
                    .cloned()
                    .unwrap_or(fallback_name);
                if skills.contains_key(&name) {
                    continue;
                }

                skills.insert(
                    name.clone(),
                    Skill {
                        name,
                        description: metadata
                            .get("description")
                            .cloned()
                            .unwrap_or_default(),
                        source: root.source.clone(),
                    },
                );
            }
        }

        Ok(skills.into_values().collect())
    }

    pub fn read(&self, name: &str, relative_path: Option<&str>) -> Result<String> {
        let skill_dir = self
            .find_skill_dir(name)?
            .with_context(|| format!("unknown skill: {name}"))?;
        let relative_path = relative_path.unwrap_or("SKILL.md");
        let path = resolve_inside(&skill_dir, relative_path)?;

        fs::read_to_string(&path)
            .with_context(|| format!("failed to read skill file: {}", path.display()))
    }

    fn find_skill_dir(&self, name: &str) -> Result<Option<PathBuf>> {
        for root in &self.roots {
            let candidate = root.path.join(name).join("SKILL.md");
            if candidate.is_file() {
                return Ok(Some(root.path.join(name)));
            }
        }

        Ok(None)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    path: PathBuf,
    source: String,
    writable: bool,
}

impl SkillRoot {
    pub fn read_only(path: impl Into<PathBuf>, source: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            source: source.into(),
            writable: false,
        }
    }

    pub fn writable(path: impl Into<PathBuf>, source: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            source: source.into(),
            writable: true,
        }
    }
}

fn parse_metadata(content: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    let Some(rest) = content.strip_prefix("---\n") else {
        return metadata;
    };
    let Some((frontmatter, _body)) = rest.split_once("\n---") else {
        return metadata;
    };

    for line in frontmatter.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        metadata.insert(key.trim().to_string(), value.trim().trim_matches('"').to_string());
    }

    metadata
}

fn resolve_inside(root: &Path, relative_path: &str) -> Result<PathBuf> {
    let relative = Path::new(relative_path);
    if relative.is_absolute() {
        bail!("skill path must be relative");
    }
    if relative
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("skill path escapes skill directory");
    }

    Ok(root.join(relative))
}
```

- [ ] **Step 4: Run registry tests**

Run:

```bash
rtk cargo test skills::
```

Expected: all `skills::tests::*` pass.

- [ ] **Step 5: Commit**

```bash
rtk git add src/main.rs src/skills.rs
rtk git commit -m "feat(skills): add local skill registry"
```

---

### Task 2: Add Skill Creation

**Files:**
- Modify: `src/skills.rs`

- [ ] **Step 1: Write failing creation tests**

Add these tests to `src/skills.rs`:

```rust
    #[test]
    fn create_writes_skill_to_writable_root() -> Result<()> {
        let root = temp_root("create-skill")?;
        let registry = SkillRegistry::new(vec![SkillRoot::writable(&root, "user")]);

        registry.create(
            "release-checklist",
            "Use when preparing a release.",
            "# Release Checklist\nRun tests.\n",
        )?;

        let content = fs::read_to_string(root.join("release-checklist").join("SKILL.md"))?;
        assert!(content.contains("name: release-checklist"));
        assert!(content.contains("description: Use when preparing a release."));
        assert!(content.contains("# Release Checklist"));
        Ok(())
    }

    #[test]
    fn create_rejects_unsafe_names() -> Result<()> {
        let root = temp_root("create-rejects")?;
        let registry = SkillRegistry::new(vec![SkillRoot::writable(&root, "user")]);

        let error = registry
            .create("../escape", "Bad", "# Bad\n")
            .unwrap_err();

        assert!(error.to_string().contains("unsafe skill name"));
        Ok(())
    }

    #[test]
    fn create_requires_writable_root() -> Result<()> {
        let root = temp_root("create-no-root")?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "project")]);

        let error = registry.create("tdd", "TDD", "# TDD\n").unwrap_err();

        assert!(error.to_string().contains("no writable skill root configured"));
        Ok(())
    }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
rtk cargo test skills::
```

Expected: compile failure for missing `SkillRegistry::create`.

- [ ] **Step 3: Implement creation**

Add to `impl SkillRegistry`:

```rust
    pub fn create(&self, name: &str, description: &str, body: &str) -> Result<Skill> {
        validate_skill_name(name)?;
        let root = self
            .roots
            .iter()
            .find(|root| root.writable)
            .context("no writable skill root configured")?;
        let dir = root.path.join(name);
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create skill directory: {}", dir.display()))?;
        let content = format!(
            "---\nname: {name}\ndescription: {description}\n---\n\n{}",
            body.trim_start()
        );
        fs::write(dir.join("SKILL.md"), content)
            .with_context(|| format!("failed to write skill: {}", dir.display()))?;

        Ok(Skill {
            name: name.to_string(),
            description: description.to_string(),
            source: root.source.clone(),
        })
    }
```

Add helper:

```rust
fn validate_skill_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.chars().any(char::is_whitespace)
    {
        bail!("unsafe skill name: {name}");
    }

    Ok(())
}
```

- [ ] **Step 4: Run registry tests**

```bash
rtk cargo test skills::
```

Expected: all `skills::tests::*` pass.

- [ ] **Step 5: Commit**

```bash
rtk git add src/skills.rs
rtk git commit -m "feat(skills): create local skills"
```

---

### Task 3: Expose Skill Tools

**Files:**
- Create: `src/tools/skills.rs`
- Modify: `src/tools.rs`

- [ ] **Step 1: Write failing tool tests**

Add `mod skills;` to `src/tools.rs`.

Add this to `ToolRegistryConfig`:

```rust
pub skill_roots: Vec<crate::skills::SkillRoot>,
```

Create `src/tools/skills.rs`:

```rust
use anyhow::{Result, Context};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    agent::tool::{Tool, ToolHandler, ToolParameter, ToolParameters},
    skills::SkillRegistry,
};

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::skills::SkillRoot;

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn list_tool_returns_skills_json() -> Result<()> {
        let root = temp_root("list-tool")?;
        write_skill(&root, "tdd", "---\ndescription: Use TDD.\n---\n# TDD\n")?;
        let tool = SkillsListTool::new(SkillRegistry::new(vec![SkillRoot::read_only(
            &root, "test",
        )]));

        let output = tool.execute("{}").await?;

        assert!(output.contains("\"name\":\"tdd\""));
        assert!(output.contains("\"description\":\"Use TDD.\""));
        Ok(())
    }

    #[tokio::test]
    async fn read_tool_returns_skill_content() -> Result<()> {
        let root = temp_root("read-tool")?;
        write_skill(&root, "tdd", "# TDD\n")?;
        let tool = SkillsReadTool::new(SkillRegistry::new(vec![SkillRoot::read_only(
            &root, "test",
        )]));

        let output = tool.execute(r#"{"name":"tdd"}"#).await?;

        assert_eq!(output, "# TDD\n");
        Ok(())
    }

    #[tokio::test]
    async fn create_tool_writes_skill() -> Result<()> {
        let root = temp_root("create-tool")?;
        let tool = SkillsCreateTool::new(SkillRegistry::new(vec![SkillRoot::writable(
            &root, "user",
        )]));

        let output = tool
            .execute(
                r##"{"name":"release","description":"Use for releases.","body":"# Release\nRun cargo test.\n"}"##,
            )
            .await?;

        assert!(output.contains("\"name\":\"release\""));
        assert!(root.join("release").join("SKILL.md").is_file());
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
            "codrik-skill-tools-{label}-{}-{unique}",
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&path)?;
        Ok(path)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
rtk cargo test tools::skills::
```

Expected: compile failures for missing `SkillsListTool`, `SkillsReadTool`, and `SkillsCreateTool`.

- [ ] **Step 3: Implement skill tool handlers**

Put this implementation above the tests in `src/tools/skills.rs`:

```rust
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    agent::tool::{Tool, ToolHandler, ToolParameter, ToolParameters},
    skills::{Skill, SkillRegistry},
};

#[derive(Clone, Debug)]
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
            "List locally available agent skills. Use this before acting when a skill may apply.",
            ToolParameters::new(),
        )
    }

    async fn execute(&self, _arguments: &str) -> Result<String> {
        let skills: Vec<SkillSummary> = self.registry.list()?.into_iter().map(Into::into).collect();
        serde_json::to_string(&skills).context("failed to serialize skills list")
    }
}

#[derive(Clone, Debug)]
pub struct SkillsReadTool {
    registry: SkillRegistry,
}

impl SkillsReadTool {
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }
}

#[derive(Debug, Deserialize)]
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
            "Read a skill's SKILL.md or a relative reference file inside that skill directory.",
            ToolParameters::new()
                .required("name", ToolParameter::string("Skill name to read."))
                .optional(
                    "path",
                    ToolParameter::string("Relative file path inside the skill directory. Defaults to SKILL.md."),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: SkillsReadArguments =
            serde_json::from_str(arguments).context("failed to parse skills_read arguments")?;
        self.registry.read(&arguments.name, arguments.path.as_deref())
    }
}

#[derive(Clone, Debug)]
pub struct SkillsCreateTool {
    registry: SkillRegistry,
}

impl SkillsCreateTool {
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }
}

#[derive(Debug, Deserialize)]
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
            "Create or update a local user skill under the writable skill root.",
            ToolParameters::new()
                .required("name", ToolParameter::string("Safe skill directory name, for example telegram-debug."))
                .required("description", ToolParameter::string("Short trigger-oriented skill description."))
                .required("body", ToolParameter::string("Markdown body for SKILL.md after frontmatter.")),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: SkillsCreateArguments =
            serde_json::from_str(arguments).context("failed to parse skills_create arguments")?;
        let skill = self
            .registry
            .create(&arguments.name, &arguments.description, &arguments.body)?;
        serde_json::to_string(&SkillSummary::from(skill)).context("failed to serialize created skill")
    }
}

#[derive(Debug, Serialize)]
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
```

- [ ] **Step 4: Wire tools into registry**

In `src/tools.rs`, import `SkillRegistry` and add skill tools to `ToolRegistry::with_config`:

```rust
use crate::{
    agent::tool::{Tool, ToolExecutor, ToolExposure, ToolHandler},
    skills::SkillRegistry,
};
```

Update `ToolRegistryConfig`:

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolRegistryConfig {
    pub bashkit_workspace: Option<PathBuf>,
    pub skill_roots: Vec<crate::skills::SkillRoot>,
}
```

Update handler construction:

```rust
        let skills = SkillRegistry::new(config.skill_roots);
        Self {
            handlers: vec![
                Box::new(datetime::DatetimeTool),
                Box::new(bashkit::BashkitTool::new(bashkit::BashkitToolConfig {
                    workspace: config.bashkit_workspace,
                })),
                Box::new(skills::SkillsListTool::new(skills.clone())),
                Box::new(skills::SkillsReadTool::new(skills.clone())),
                Box::new(skills::SkillsCreateTool::new(skills)),
                Box::new(bash::BashTool),
            ],
        }
```

- [ ] **Step 5: Add wildcard standard tool assertion**

In `src/tools.rs` test `wildcard_allows_standard_tools_only`, add:

```rust
        assert!(tools.iter().any(|tool| tool.name == "skills_list"));
        assert!(tools.iter().any(|tool| tool.name == "skills_read"));
        assert!(tools.iter().any(|tool| tool.name == "skills_create"));
```

- [ ] **Step 6: Run tool tests**

```bash
rtk cargo test tools::
```

Expected: all `tools::*` tests pass.

- [ ] **Step 7: Commit**

```bash
rtk git add src/tools.rs src/tools/skills.rs
rtk git commit -m "feat(tools): expose skill tools"
```

---

### Task 4: Wire Skill Roots Into App Composition

**Files:**
- Modify: `src/app.rs`

- [ ] **Step 1: Write failing app-level tests**

Add tests in `src/app.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_skill_roots_prefer_project_then_user() -> Result<()> {
        let roots = default_skill_roots()?;

        assert_eq!(roots.len(), 2);
        Ok(())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
rtk cargo test app::tests::default_skill_roots_prefer_project_then_user
```

Expected: compile failure for missing `default_skill_roots`.

- [ ] **Step 3: Implement default roots and app wiring**

In `src/app.rs`, import `SkillRoot`:

```rust
    skills::SkillRoot,
```

Add helper:

```rust
fn default_tool_config() -> Result<ToolRegistryConfig> {
    Ok(ToolRegistryConfig {
        bashkit_workspace: None,
        skill_roots: default_skill_roots()?,
    })
}

fn actor_tool_config(actor: &AuthorizedActor) -> Result<ToolRegistryConfig> {
    Ok(ToolRegistryConfig {
        bashkit_workspace: Some(actor_workspace_path(&actor.id)?),
        skill_roots: default_skill_roots()?,
    })
}

fn default_skill_roots() -> Result<Vec<SkillRoot>> {
    Ok(vec![
        SkillRoot::read_only(PathBuf::from(".codrik").join("skills"), "project"),
        SkillRoot::writable(codrik_dir()?.join("skills"), "user"),
    ])
}
```

Update `build_agent_with_memory`:

```rust
    let tools = ToolRegistry::with_config(
        default_tool_config().expect("failed to build default tool config"),
    );
```

Update `build_agent_for_actor`:

```rust
    let tools = ToolRegistry::with_allowed_tools_and_config(actor.tools, actor_tool_config(&actor)?);
```

- [ ] **Step 4: Run app tests**

```bash
rtk cargo test app::
```

Expected: all app tests pass.

- [ ] **Step 5: Commit**

```bash
rtk git add src/app.rs
rtk git commit -m "feat(app): configure skill roots"
```

---

### Task 5: Update Agent Instructions and README

**Files:**
- Modify: `agent_instructions.md`
- Modify: `README.md`

- [ ] **Step 1: Update agent instructions**

Add this section to `agent_instructions.md` after the tool-use guidance:

```markdown
## Skills

- Use `skills_list` before acting when a local skill may apply to the user's request.
- If a listed skill is relevant, call `skills_read` for that skill's `SKILL.md` before taking task actions.
- Follow the loaded skill instructions while respecting higher-priority user instructions.
- Read only the referenced files that are relevant to the current task; do not load unrelated skill references.
- Use `skills_create` when the user asks you to create, write, or save a reusable skill.
- Keep created skills focused: one capability per skill, trigger-oriented description, and concrete steps in `SKILL.md`.
```

- [ ] **Step 2: Update README**

Add a `## Skills` section after the tools/auth section:

````markdown
## Skills

codrik can discover local skills from:

- `.codrik/skills/<name>/SKILL.md` in the current working directory
- `~/.codrik/skills/<name>/SKILL.md`

Project skills take precedence over user skills with the same name. Skills are
available through standard tools, so Telegram actors with `"tools": ["*"]` can
list, read, and create user skills.

The runtime exposes:

- `skills_list`: returns available skill names, descriptions, and sources
- `skills_read`: reads `SKILL.md` or a relative reference file inside a skill
- `skills_create`: writes `~/.codrik/skills/<name>/SKILL.md`

Minimal skill:

```md
---
name: telegram-debug
description: Use when debugging Telegram gateway behavior, auth, sessions, or delivery failures.
---

# Telegram Debug

1. Check gateway logs.
2. Inspect `~/.codrik/users.json`.
3. Verify the active Telegram session.
```
````

- [ ] **Step 3: Run formatting-neutral checks**

```bash
rtk cargo fmt --check
rtk cargo test
```

Expected: format check passes and all tests pass.

- [ ] **Step 4: Commit**

```bash
rtk git add agent_instructions.md README.md
rtk git commit -m "docs(skills): document skill workflow"
```

---

### Task 6: Full Validation

**Files:**
- No new files

- [ ] **Step 1: Run full Rust validation**

```bash
rtk cargo fmt --check
rtk cargo test
rtk cargo check
rtk cargo clippy --all-targets --all-features
```

Expected: all commands exit successfully.

- [ ] **Step 2: Manual CLI smoke test**

Create a temporary user skill root without touching real `~/.codrik`:

```bash
rtk env CODRIK_HOME=/tmp/codrik-skills-smoke cargo run -- "Create a skill named release-checklist with description Use when preparing codrik releases and body # Release Checklist"
```

Expected: the model may need credentials to call the LLM. If no API key/config is available, skip the smoke test and record that only unit/integration checks were run.

- [ ] **Step 3: Inspect final diff**

```bash
rtk git diff --stat
rtk git diff --check
```

Expected: no whitespace errors; diff only touches planned files.

- [ ] **Step 4: Final commit if previous tasks were not committed individually**

```bash
rtk git add src/main.rs src/skills.rs src/tools.rs src/tools/skills.rs src/app.rs agent_instructions.md README.md
rtk git commit -m "feat(skills): add local skill runtime"
```

Expected: use this single commit only if task-level commits were intentionally skipped.

---

## Self-Review

- Spec coverage: discovery, reading, safe reference resolution, standard tool exposure, wildcard access, and agent-created skills are each mapped to tasks.
- Scope check: dynamic plugin packaging, remote marketplaces, versioned skill installation, and external Codex plugin cache compatibility are intentionally out of scope.
- Type consistency: `SkillRegistry`, `SkillRoot`, `Skill`, `skills_list`, `skills_read`, and `skills_create` names are consistent across tasks.
- Placeholder scan passed: the plan contains concrete paths, commands, and implementation snippets for each task.
