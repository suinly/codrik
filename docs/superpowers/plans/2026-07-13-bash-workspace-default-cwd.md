# Bash Workspace Default CWD Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make privileged actor-scoped `bash` commands start in the same host workspace exposed to Bashkit and `send_file`, while keeping explicit `cwd` overrides privileged.

**Architecture:** Introduce a configured `BashTool` with an optional default working directory. Rename the registry's shell workspace field so composition can pass one actor workspace to both shell adapters, create that directory before tool construction, and document the distinct Bashkit filesystem and `send_file` virtual path contracts.

**Tech Stack:** Rust 2024, Tokio process execution, existing `ToolHandler` and `ToolRegistryConfig` abstractions, `cargo test`.

## Global Constraints

- Real `bash` uses relative paths such as `nature_photo.jpg` in the actor workspace.
- `/workspace` remains Bashkit-only.
- `send_file` continues to use virtual paths such as `workspace/nature_photo.jpg`.
- An explicit real-bash `cwd` overrides the configured default and remains unrestricted.
- No command rewriting or server-level `/workspace` symlink.

---

### Task 1: Configurable Bash Default Working Directory

**Files:**
- Modify: `src/tools/bash.rs`

**Interfaces:**
- Produces: `BashToolConfig { pub default_cwd: Option<PathBuf> }`.
- Produces: `BashTool::new(config: BashToolConfig) -> BashTool`.
- Preserves: `ToolHandler` behavior and explicit `cwd` argument semantics.

- [ ] **Step 1: Write failing tests for default cwd, explicit override, and path guidance**

Add tests that construct a configured tool, execute `pwd`, and inspect its definition:

```rust
#[tokio::test]
async fn runs_in_default_cwd() {
    let default_cwd = std::env::current_dir()
        .expect("current dir should exist")
        .join("src");
    let result = BashTool::new(BashToolConfig {
        default_cwd: Some(default_cwd.clone()),
    })
    .execute(r#"{"command":"pwd"}"#)
    .await
    .expect("server bash command should execute");
    let result: Value = serde_json::from_str(&result).expect("result should be valid json");

    assert_eq!(
        result["stdout"].as_str().expect("stdout string").trim(),
        default_cwd.to_string_lossy()
    );
}

#[tokio::test]
async fn explicit_cwd_overrides_default_cwd() {
    let root = std::env::current_dir().expect("current dir should exist");
    let explicit_cwd = root.join("src");
    let result = BashTool::new(BashToolConfig {
        default_cwd: Some(root),
    })
    .execute(&format!(
        r#"{{"command":"pwd","cwd":{}}}"#,
        serde_json::to_string(&explicit_cwd).expect("cwd should serialize")
    ))
    .await
    .expect("server bash command should execute");
    let result: Value = serde_json::from_str(&result).expect("result should be valid json");

    assert_eq!(
        result["stdout"].as_str().expect("stdout string").trim(),
        explicit_cwd.to_string_lossy()
    );
}

#[test]
fn definition_explains_workspace_path_contract() {
    let definition = BashTool::default().definition();

    assert!(definition.description.contains("relative paths"));
    assert!(definition.description.contains("/workspace exists only inside Bashkit"));
    assert!(definition.description.contains("workspace/<path>"));
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `rtk cargo test tools::bash::tests`

Expected: compilation fails because `BashToolConfig`, `BashTool::new`, and `BashTool::default` do not exist.

- [ ] **Step 3: Implement the configured tool and cwd precedence**

Replace the unit struct and pass the selected cwd into execution:

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BashToolConfig {
    pub default_cwd: Option<PathBuf>,
}

#[derive(Default)]
pub struct BashTool {
    config: BashToolConfig,
}

impl BashTool {
    pub fn new(config: BashToolConfig) -> Self {
        Self { config }
    }
}

async fn execute(&self, arguments: &str) -> Result<String> {
    let arguments: BashArguments =
        serde_json::from_str(arguments).context("failed to parse bash tool arguments")?;
    let result = run_bash(arguments, self.config.default_cwd.clone()).await?;

    serde_json::to_string(&result).context("failed to serialize bash command result")
}

// Change the existing helper signature:
-async fn run_bash(arguments: BashArguments) -> Result<BashResult> {
+async fn run_bash(
+    arguments: BashArguments,
+    default_cwd: Option<PathBuf>,
+) -> Result<BashResult> {

// Change the existing cwd selection:
-if let Some(cwd) = arguments.cwd {
+if let Some(cwd) = arguments.cwd.or(default_cwd) {
     command.current_dir(cwd);
 }
```

Update existing tests from `BashTool` to `BashTool::default()`. Change the definition text to explicitly state:

```text
Actor-scoped calls start in the actor workspace, so use relative output paths. /workspace exists only inside Bashkit. A relative file such as report.pdf can be delivered with send_file as workspace/report.pdf.
```

Update the `cwd` parameter description to say that it overrides the configured actor workspace default.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `rtk cargo test tools::bash::tests`

Expected: all bash tool tests pass.

- [ ] **Step 5: Commit Task 1**

```bash
rtk git add src/tools/bash.rs
rtk git commit -m "feat(tools): configure bash default cwd"
```

---

### Task 2: Wire the Actor Workspace into Both Shells

**Files:**
- Modify: `src/tools.rs`
- Modify: `src/app.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: `BashTool::new(BashToolConfig)` from Task 1.
- Produces: `ToolRegistryConfig { pub actor_workspace: Option<PathBuf>, ... }`.
- Preserves: wildcard grants expose Bashkit but not privileged `bash`.

- [ ] **Step 1: Write failing registry and application tests**

In `src/tools.rs`, add a test that executes only explicitly granted bash through a configured registry:

```rust
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

    let execution = registry.execute("bash", r#"{"command":"pwd"}"#).await
        .expect("bash should execute");
    let result: serde_json::Value = serde_json::from_str(&execution.observation)
        .expect("bash observation should be json");

    assert_eq!(
        result["stdout"].as_str().expect("stdout string").trim(),
        workspace.to_string_lossy()
    );
}
```

In `src/app.rs`, add a test using a unique actor ID and verify both the directory and config:

```rust
#[test]
fn actor_tool_config_creates_shared_shell_workspace() -> Result<()> {
    let workspace = temp_root("actor-workspace")?;
    std::fs::remove_dir_all(&workspace)?;

    let config = tool_config_for_actor_workspace(workspace.clone())?;

    assert!(workspace.is_dir());
    assert_eq!(config.actor_workspace, Some(workspace.clone()));
    assert_eq!(config.file_roots[0], FileRoot::new("workspace", &workspace));
    std::fs::remove_dir_all(workspace).ok();
    Ok(())
}
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `rtk cargo test workspace`

Expected: compilation fails because `ToolRegistryConfig::actor_workspace` does not exist and the registry does not configure `BashTool`.

- [ ] **Step 3: Rename and wire the shared workspace**

In `src/tools.rs`, rename `bashkit_workspace` to `actor_workspace`, clone it before consuming config fields, and construct both tools:

```rust
pub struct ToolRegistryConfig {
    pub actor_workspace: Option<PathBuf>,
    pub skill_roots: Vec<crate::skills::SkillRoot>,
    pub file_roots: Vec<FileRoot>,
}

pub fn with_config(config: ToolRegistryConfig) -> Self {
    let skill_registry = SkillRegistry::new(config.skill_roots);
    let actor_workspace = config.actor_workspace;
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
```

In `src/app.rs`, use `actor_workspace: None` for default configs. Extract a testable constructor that creates the directory, and call it for actors:

```rust
fn actor_tool_config(actor: &AuthorizedActor) -> Result<ToolRegistryConfig> {
    let workspace = actor_workspace_path(&actor.id)?;
    tool_config_for_actor_workspace(workspace)
}

fn tool_config_for_actor_workspace(workspace: PathBuf) -> Result<ToolRegistryConfig> {
    std::fs::create_dir_all(&workspace)
        .with_context(|| format!("failed to create actor workspace: {}", workspace.display()))?;
    Ok(ToolRegistryConfig {
        actor_workspace: Some(workspace.clone()),
        skill_roots: default_skill_roots()?,
        file_roots: vec![FileRoot::new("workspace", workspace)],
    })
}
```

Update all `ToolRegistryConfig` literals and `BashTool` definition lookups to use the new constructors and field name.

- [ ] **Step 4: Document the real-bash file flow**

After the privileged bash grant paragraph in `README.md`, add:

```markdown
For actor-scoped runs, real `bash` starts in that actor's workspace. Use a
relative output path such as `report.pdf`, then deliver it with
`send_file("workspace/report.pdf")`. The absolute `/workspace` mount exists
only inside `bashkit`.
```

- [ ] **Step 5: Run focused tests and verify GREEN**

Run: `rtk cargo test tools::tests`

Expected: registry tests pass, including wildcard/privileged exposure tests.

Run: `rtk cargo test app::tests`

Expected: application tests pass, including workspace creation.

- [ ] **Step 6: Commit Task 2**

```bash
rtk git add src/tools.rs src/app.rs README.md
rtk git commit -m "fix(tools): share actor workspace with bash"
```

---

### Task 3: Full Verification

**Files:**
- Verify only; no expected code changes.

**Interfaces:**
- Consumes: completed Task 1 and Task 2 behavior.
- Produces: fresh verification evidence for handoff.

- [ ] **Step 1: Run the complete test suite**

Run: `rtk cargo test`

Expected: all non-ignored tests pass.

- [ ] **Step 2: Run build and static checks**

Run: `rtk cargo check`

Expected: exit code 0 with no new warnings.

Run: `rtk cargo fmt --check`

Expected: exit code 0.

Run: `rtk cargo clippy --all-targets --all-features`

Expected: exit code 0 with no new warnings.

Run: `rtk git diff --check`

Expected: exit code 0.

- [ ] **Step 3: Inspect final repository state**

Run: `rtk git status --short --branch`

Expected: clean working tree on `main`, ahead of `origin/main` by the new commits.
