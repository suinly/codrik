# Skill Deletion and Update Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add confirmed deletion of complete writable user-skill directories and prove that the agent-facing update workflow edits an existing skill without creating a replacement.

**Architecture:** Extend `SkillRegistry` with exact-name, writable-only directory deletion, then expose it through a conservative standard `skills_delete` tool requiring `confirm: true`. Keep create, update, and delete separate; strengthen the bundled `skill-creator` decision workflow and verify mutation behavior through registry, handler, and full `ToolRegistry` tests.

**Tech Stack:** Rust 2024, `anyhow`, `serde`/`serde_json`, Tokio tests, standard-library filesystem APIs.

## Global Constraints

- Delete the complete skill directory, including `references/` and all nested files.
- Delete only the active discovered skill from a writable directory root; project and built-in skills remain read-only.
- Require both an exact skill name and `confirm: true`; missing or false confirmation performs no mutation.
- Keep `skills_create`, `skills_update`, and `skills_delete` as distinct strict operations with no fallback between them.
- Keep webhook `SkillsOnly` execution limited to `skills_list` and `skills_read`.
- Do not add rename, recovery, or individual reference-file mutation support.
- Run every shell command through `rtk`.

## File Structure

- Modify `src/skills.rs`: own writable-skill lookup and complete directory deletion, with focused filesystem tests.
- Modify `src/tools/skills.rs`: define the confirmed `skills_delete` agent tool and handler tests.
- Modify `src/tools.rs`: register and expose the tool; add an end-to-end update workflow test through `ToolRegistry`.
- Modify `src/runtime/model.rs`: explicitly test that `SkillsOnly` rejects deletion.
- Modify `.codrik/builtin-skills/skill-creator/SKILL.md`: teach exact update/delete selection and post-mutation verification.
- Modify `src/skills/builtin.rs` only if its compiled description or explicit asset assertions require synchronization; the included `SKILL.md` content itself is compiled from the existing asset.
- Modify `README.md`: document the five skill tools, writable/read-only boundaries, and confirmed whole-directory deletion.

---

### Task 1: Writable Skill Directory Deletion

**Files:**
- Modify: `src/skills.rs`

**Interfaces:**
- Consumes: existing `SkillRegistry::discover()`, `validate_skill_name(name: &str) -> Result<()>`, `SkillLocation::Directory(PathBuf)`, and `Skill`.
- Produces: `pub fn SkillRegistry::delete(&self, name: &str) -> Result<Skill>`.

- [ ] **Step 1: Write failing registry tests**

Add tests that create a writable `release` skill with `references/checklist.md` and a nested file, plus a neighboring `deploy` skill. Assert that `delete("release")` returns the original summary, removes only `root/release`, and makes subsequent `read("release", None)` fail.

```rust
#[test]
fn delete_removes_complete_writable_skill_directory() -> Result<()> {
    let root = temp_root("delete-writable")?;
    write_skill(
        &root,
        "release",
        "---\nname: release\ndescription: Release checklist.\n---\n# Release\n",
    )?;
    fs::create_dir_all(root.join("release/references/nested"))?;
    fs::write(root.join("release/references/checklist.md"), "check\n")?;
    fs::write(root.join("release/references/nested/details.md"), "details\n")?;
    write_skill(&root, "deploy", "# Deploy\n")?;
    let registry = SkillRegistry::new(vec![SkillRoot::writable(&root, "user")]);

    let deleted = registry.delete("release")?;

    assert_eq!(
        deleted,
        Skill {
            name: "release".into(),
            description: "Release checklist.".into(),
            source: "user".into(),
        }
    );
    assert!(!root.join("release").exists());
    assert!(root.join("deploy/SKILL.md").is_file());
    assert!(registry.read("release", None).is_err());
    Ok(())
}
```

Also add separate tests for unknown names, unsafe names, built-in skills, read-only project skills, and a read-only project skill shadowing an equal-name writable user skill. Each must assert that the writable directory still exists.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `rtk cargo test skills::tests::delete -- --nocapture`

Expected: compilation fails because `SkillRegistry::delete` does not exist.

- [ ] **Step 3: Implement exact-name writable-only deletion**

Add this method beside `update`:

```rust
pub fn delete(&self, name: &str) -> Result<Skill> {
    validate_skill_name(name)?;
    let discovered = self
        .discover()?
        .into_iter()
        .find(|entry| entry.skill.name == name)
        .with_context(|| format!("unknown writable skill: {name}"))?;
    if !discovered.writable {
        bail!("skill is read-only: {name}");
    }

    let SkillLocation::Directory(dir) = discovered.location else {
        bail!("skill is read-only: {name}");
    };
    fs::remove_dir_all(&dir)
        .with_context(|| format!("failed to delete skill directory: {}", dir.display()))?;

    Ok(discovered.skill)
}
```

- [ ] **Step 4: Run registry tests and verify GREEN**

Run: `rtk cargo test skills::tests::delete -- --nocapture`

Expected: all deletion tests pass.

- [ ] **Step 5: Commit the registry behavior**

```text
rtk git add src/skills.rs
rtk git commit -m "feat(skills): delete writable skill directories"
```

### Task 2: Confirmed `skills_delete` Tool

**Files:**
- Modify: `src/tools/skills.rs`
- Modify: `src/tools.rs`
- Modify: `src/runtime/model.rs`

**Interfaces:**
- Consumes: `SkillRegistry::delete(&self, name: &str) -> Result<Skill>` from Task 1 and existing `serialize_skill(Skill)`.
- Produces: `SkillsDeleteTool::new(SkillRegistry)`, tool name `skills_delete`, required JSON fields `name: String` and `confirm: bool`.

- [ ] **Step 1: Write failing handler tests**

Add `SkillsDeleteTool` tests in `src/tools/skills.rs` that assert:

```rust
#[tokio::test]
async fn delete_requires_true_confirmation_without_mutating() -> Result<()> {
    let root = temp_root("delete-confirm")?;
    write_skill(&root, "release", "# Release\n")?;
    let tool = super::SkillsDeleteTool::new(SkillRegistry::new(vec![
        SkillRoot::writable(&root, "user"),
    ]));

    assert!(tool.execute(r#"{"name":"release","confirm":false}"#).await.is_err());
    assert!(tool.execute(r#"{"name":"release"}"#).await.is_err());
    assert!(root.join("release/SKILL.md").is_file());
    Ok(())
}
```

Add a success test using `confirm: true`; assert the JSON result is
`{"name":"release","description":"Release checklist.","source":"user"}` and the directory no longer exists. Add a definition test asserting both fields are required and `confirm` has `ToolParameterKind::Boolean`.

- [ ] **Step 2: Write failing registration and policy assertions**

In `src/tools.rs`, extend wildcard/registration/capability tests to expect `skills_delete`, and assert `retry_safe == false`. In `src/runtime/model.rs::skills_only_is_monotonic_and_read_only`, add:

```rust
assert!(!ExecutionPolicy::SkillsOnly.allows("skills_update"));
assert!(!ExecutionPolicy::SkillsOnly.allows("skills_delete"));
```

- [ ] **Step 3: Run focused tests and verify RED**

Run: `rtk cargo test tools::skills::tests::delete -- --nocapture`

Expected: compilation fails because `SkillsDeleteTool` is not defined.

- [ ] **Step 4: Implement the confirmed handler**

Add the handler after `SkillsUpdateTool`:

```rust
pub struct SkillsDeleteTool {
    registry: SkillRegistry,
}

impl SkillsDeleteTool {
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }
}

#[derive(Deserialize)]
struct SkillsDeleteArguments {
    name: String,
    confirm: bool,
}

#[async_trait]
impl ToolHandler for SkillsDeleteTool {
    fn name(&self) -> &'static str {
        "skills_delete"
    }

    fn definition(&self) -> Tool {
        Tool::new(
            self.name(),
            "Permanently delete an existing local user skill directory and all its files. Requires explicit confirmation.",
            ToolParameters::new()
                .required("name", ToolParameter::string("Exact existing user skill name."))
                .required(
                    "confirm",
                    ToolParameter::boolean("Must be true to confirm permanent deletion."),
                ),
        )
    }

    async fn execute(&self, arguments: &str) -> Result<String> {
        let arguments: SkillsDeleteArguments = serde_json::from_str(arguments)
            .context("failed to parse skills_delete arguments")?;
        if !arguments.confirm {
            anyhow::bail!("skills_delete requires confirm: true");
        }

        serialize_skill(self.registry.delete(&arguments.name)?)
    }
}
```

- [ ] **Step 5: Register the handler and retain read-only policy**

In `ToolRegistry::with_config`, clone the registry for update and pass the final instance to delete:

```rust
Box::new(skills::SkillsUpdateTool::new(skill_registry.clone())),
Box::new(skills::SkillsDeleteTool::new(skill_registry)),
```

Do not change `ExecutionPolicy::allows`; its exact match already excludes the new mutation tool.

- [ ] **Step 6: Run focused tool and policy tests**

Run: `rtk cargo test tools:: -- --nocapture`

Run: `rtk cargo test runtime::model::tests::skills_only -- --nocapture`

Expected: all selected tests pass.

- [ ] **Step 7: Commit the tool contract**

```text
rtk git add src/tools/skills.rs src/tools.rs src/runtime/model.rs
rtk git commit -m "feat(skills): expose confirmed skill deletion"
```

### Task 3: End-to-End Update Regression and Agent Guidance

**Files:**
- Modify: `src/tools.rs`
- Modify: `.codrik/builtin-skills/skill-creator/SKILL.md`
- Modify: `src/skills.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: registered `skills_list`, `skills_read`, `skills_create`, `skills_update`, and `skills_delete` tools.
- Produces: a regression test demonstrating in-place update through `ToolRegistry` and agent instructions that forbid create-as-update fallback.

- [ ] **Step 1: Write the failing end-to-end update regression test**

In `src/tools.rs`, create a temp writable root containing `existing-directory/SKILL.md` whose frontmatter name is `release`. Build a registry allowing the three tools used by the flow, then execute list, update with a complete frontmatter-bearing body, and read:

```rust
let registry = ToolRegistry::with_allowed_tools_and_config(
    vec!["skills_list".into(), "skills_update".into(), "skills_read".into()],
    ToolRegistryConfig {
        skill_roots: vec![SkillRoot::writable(&root, "user")],
        ..ToolRegistryConfig::default()
    },
);
let context = ToolCallContext::legacy(crate::llm::client::RunContext::new());
```

Assert the initial list contains one `release`, update returns the user summary, read returns exactly one normalized frontmatter block with the new description/body, `existing-directory/SKILL.md` contains the result, and the root still contains exactly one skill directory. This specifically verifies lookup by discovered frontmatter name rather than assuming directory name equals public name.

- [ ] **Step 2: Run the regression test against current behavior**

Run: `rtk cargo test tools::tests::update_existing_skill_end_to_end -- --nocapture`

Expected: PASS. If it fails, stop and trace the failing boundary before changing implementation; the test is the reproduction for the previously reported behavior.

- [ ] **Step 3: Strengthen the built-in workflow**

Replace the mutation-selection portion of `.codrik/builtin-skills/skill-creator/SKILL.md` with explicit instructions:

```markdown
2. Call `skills_list` before choosing a mutation. Use the exact listed name and
   source in later calls.
3. If an existing writable user skill owns the capability, call
   `skills_update`. If update fails, report or resolve that error; never create
   a differently named replacement.
4. Call `skills_create` only when no existing skill owns the capability.
5. For deletion, identify the exact writable user skill, obtain explicit user
   confirmation, then call `skills_delete` with `confirm: true`.
6. After create or update, call `skills_read` and verify the persisted file.
   After deletion, call `skills_list` and verify the skill is absent.
```

Keep the remaining writing and review guidance, renumbering it into a coherent single workflow. Update the compiled-skill test in `src/skills.rs` to assert that content includes `skills_delete`, `confirm: true`, and the prohibition on a differently named replacement.

- [ ] **Step 4: Document the public behavior**

In the README skills section, list all five tools and state:

```markdown
`skills_create` creates only new user skills, `skills_update` replaces only an
existing writable user's `SKILL.md`, and `skills_delete` permanently removes
the complete writable user-skill directory only when `confirm` is `true`.
Project and built-in skills remain read-only. Mutation tools never fall back to
a different operation.
```

- [ ] **Step 5: Run focused regression and built-in tests**

Run: `rtk cargo test tools::tests::update_existing_skill_end_to_end -- --nocapture`

Run: `rtk cargo test skills::tests::read_returns_compiled_builtin_skill -- --nocapture`

Expected: both tests pass.

- [ ] **Step 6: Commit the regression coverage and guidance**

```text
rtk git add src/tools.rs .codrik/builtin-skills/skill-creator/SKILL.md src/skills.rs README.md
rtk git commit -m "fix(skills): preserve update intent in skill workflow"
```

### Task 4: Full Verification

**Files:**
- Verify only; modify earlier task files solely to resolve failures caused by this feature.

**Interfaces:**
- Consumes: all behavior from Tasks 1-3.
- Produces: formatting-, test-, build-, and lint-clean feature branch.

- [ ] **Step 1: Format the crate**

Run: `rtk cargo fmt`

Expected: command succeeds.

- [ ] **Step 2: Run the complete test suite**

Run: `rtk cargo test`

Expected: all tests pass.

- [ ] **Step 3: Check compilation**

Run: `rtk cargo check`

Expected: command succeeds without errors.

- [ ] **Step 4: Verify formatting**

Run: `rtk cargo fmt --check`

Expected: command succeeds with no diff.

- [ ] **Step 5: Run Clippy across all targets and features**

Run: `rtk cargo clippy --all-targets --all-features`

Expected: command succeeds without warnings promoted to errors.

- [ ] **Step 6: Check the final diff**

Run: `rtk git diff --check`

Run: `rtk git status --short`

Expected: no whitespace errors; only intended feature files are changed.

- [ ] **Step 7: Commit formatting or verification fixes if needed**

If Step 1 or verification-driven fixes changed tracked files:

```text
rtk git add src/skills.rs src/tools/skills.rs src/tools.rs src/runtime/model.rs .codrik/builtin-skills/skill-creator/SKILL.md README.md
rtk git commit -m "chore(skills): finalize skill mutation verification"
```
