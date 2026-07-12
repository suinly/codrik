# Built-in Skill Creator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compile a read-only `skill-creator` into codrik while preserving project → user → built-in precedence and the existing skill tool contracts.

**Architecture:** Extend `SkillRoot` with a built-in catalog variant so `SkillRegistry` can discover and read filesystem and in-memory assets through one ordered source list. Keep built-in definitions in a focused submodule and compile a repository `SKILL.md` asset with `include_str!`; application wiring adds that source after project and user roots.

**Tech Stack:** Rust 2024, `include_str!`, existing `anyhow`, `serde`, `yaml_serde`, `SkillRegistry`, and `ToolRegistry` abstractions.

## Global Constraints

- Discovery precedence is exactly project → user → built-in; the first matching skill name wins.
- The built-in source label is exactly `built-in`.
- Built-in skills are readable but never writable.
- `skills_list`, `skills_read`, `skills_create`, and `skills_update` schemas remain unchanged.
- The public agent, provider, memory, and tool interfaces remain unchanged.
- The mutation API continues to create or replace only `SKILL.md`; it does not create reference assets.
- All shell commands in this repository must be prefixed with `rtk`.

---

## File Structure

- Create `.codrik/builtin-skills/skill-creator/SKILL.md`: bundled workflow content.
- Create `src/skills/builtin.rs`: static built-in definitions and compiled assets.
- Modify `src/skills.rs`: common source model, discovery, reading, and mutation rules.
- Modify `src/app.rs`: add built-ins after project and user roots and test instruction indexing.
- Modify `README.md`: document bundled skills, precedence, and immutability.

### Task 1: Add a Built-in Skill Source to the Registry

**Files:**
- Create: `src/skills/builtin.rs`
- Modify: `src/skills.rs`

**Interfaces:**
- Consumes: existing `Skill`, `SkillRegistry`, and `SkillRoot` APIs.
- Produces: `pub fn builtin_skill_root() -> SkillRoot`; private `BuiltinSkill` and `BuiltinSkillFile` values consumed by `SkillRootKind::Builtin`.

- [ ] **Step 1: Write failing registry tests**

Add these tests to `src/skills.rs`:

```rust
#[test]
fn list_includes_builtin_skill_creator() -> Result<()> {
    let registry = SkillRegistry::new(vec![builtin_skill_root()]);

    let skills = registry.list()?;

    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name, "skill-creator");
    assert_eq!(skills[0].source, "built-in");
    assert!(skills[0].description.contains("creating"));
    Ok(())
}

#[test]
fn read_returns_compiled_builtin_skill() -> Result<()> {
    let registry = SkillRegistry::new(vec![builtin_skill_root()]);

    let content = registry.read("skill-creator", None)?;

    assert!(content.starts_with("---\nname: skill-creator\n"));
    assert!(content.contains("# Skill Creator"));
    assert!(content.contains("skills_create"));
    assert!(content.contains("skills_update"));
    assert!(content.contains("skills_read"));
    Ok(())
}

#[test]
fn read_rejects_unknown_builtin_asset() -> Result<()> {
    let registry = SkillRegistry::new(vec![builtin_skill_root()]);

    let error = registry
        .read("skill-creator", Some("references/missing.md"))
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "unknown built-in skill asset: references/missing.md"
    );
    Ok(())
}

#[test]
fn read_rejects_builtin_parent_traversal() -> Result<()> {
    let registry = SkillRegistry::new(vec![builtin_skill_root()]);

    let error = registry
        .read("skill-creator", Some("../SKILL.md"))
        .unwrap_err();

    assert_eq!(error.to_string(), "skill path escapes skill directory");
    Ok(())
}

#[test]
fn update_rejects_builtin_skill() -> Result<()> {
    let registry = SkillRegistry::new(vec![builtin_skill_root()]);

    let error = registry
        .update("skill-creator", "Changed.", "# Changed\n")
        .unwrap_err();

    assert_eq!(error.to_string(), "skill is read-only: skill-creator");
    Ok(())
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
rtk cargo test skills::tests::list_includes_builtin_skill_creator
```

Expected: compilation fails because `builtin_skill_root` does not exist.

- [ ] **Step 3: Add the initial compiled asset and catalog types**

Create `.codrik/builtin-skills/skill-creator/SKILL.md` with the smallest content
that proves catalog loading before Task 2 fills in the reviewed workflow:

```markdown
---
name: skill-creator
description: Use when creating, writing, saving, or updating reusable skills.
---

# Skill Creator

Use `skills_create` or `skills_update`, then verify the result with
`skills_read`.
```

Create `src/skills/builtin.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BuiltinSkillFile {
    pub path: &'static str,
    pub content: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BuiltinSkill {
    pub name: &'static str,
    pub description: &'static str,
    pub files: &'static [BuiltinSkillFile],
}

const SKILL_CREATOR_FILES: &[BuiltinSkillFile] = &[BuiltinSkillFile {
    path: "SKILL.md",
    content: include_str!("../../.codrik/builtin-skills/skill-creator/SKILL.md"),
}];

pub(super) const SKILLS: &[BuiltinSkill] = &[BuiltinSkill {
    name: "skill-creator",
    description: "Use when creating, writing, saving, or updating reusable skills.",
    files: SKILL_CREATOR_FILES,
}];
```

- [ ] **Step 4: Generalize `SkillRoot` and discovered reads**

At the top of `src/skills.rs`, declare the submodule and import its types:

```rust
mod builtin;

use builtin::{BuiltinSkill, BuiltinSkillFile};
```

Replace the root and discovered storage definitions with:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
enum SkillLocation {
    Directory(PathBuf),
    Builtin(&'static [BuiltinSkillFile]),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DiscoveredSkill {
    location: SkillLocation,
    writable: bool,
    skill: Skill,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SkillRootKind {
    Directory(PathBuf),
    Builtin(&'static [BuiltinSkill]),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    kind: SkillRootKind,
    source: String,
    writable: bool,
}
```

Keep the existing constructors and add the built-in constructor plus exported default root:

```rust
impl SkillRoot {
    pub fn read_only(path: impl Into<PathBuf>, source: impl Into<String>) -> Self {
        Self {
            kind: SkillRootKind::Directory(path.into()),
            source: source.into(),
            writable: false,
        }
    }

    pub fn writable(path: impl Into<PathBuf>, source: impl Into<String>) -> Self {
        Self {
            kind: SkillRootKind::Directory(path.into()),
            source: source.into(),
            writable: true,
        }
    }

    fn builtin(skills: &'static [BuiltinSkill], source: impl Into<String>) -> Self {
        Self {
            kind: SkillRootKind::Builtin(skills),
            source: source.into(),
            writable: false,
        }
    }
}

pub fn builtin_skill_root() -> SkillRoot {
    SkillRoot::builtin(builtin::SKILLS, "built-in")
}
```

Refactor discovery into two focused helpers. Preserve sorted directory entry behavior:

```rust
fn discover_root(root: &SkillRoot) -> Result<Vec<DiscoveredSkill>> {
    match &root.kind {
        SkillRootKind::Directory(path) => discover_directory(path, root),
        SkillRootKind::Builtin(skills) => Ok(skills
            .iter()
            .map(|builtin| DiscoveredSkill {
                location: SkillLocation::Builtin(builtin.files),
                writable: false,
                skill: Skill {
                    name: builtin.name.to_string(),
                    description: builtin.description.to_string(),
                    source: root.source.clone(),
                },
            })
            .collect()),
    }
}

fn discover_directory(path: &Path, root: &SkillRoot) -> Result<Vec<DiscoveredSkill>> {
    if !path.is_dir() {
        return Ok(Vec::new());
    }
    let mut entries = fs::read_dir(path)
        .with_context(|| format!("failed to read skills root: {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    let mut skills = Vec::new();
    for entry in entries {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let skill_dir = entry.path();
        let skill_md = skill_dir.join("SKILL.md");
        if !skill_md.is_file() {
            continue;
        }
        let content = fs::read_to_string(&skill_md)
            .with_context(|| format!("failed to read skill: {}", skill_md.display()))?;
        let metadata = parse_metadata(&content);
        let fallback_name = entry.file_name().to_string_lossy().to_string();
        skills.push(DiscoveredSkill {
            location: SkillLocation::Directory(skill_dir),
            writable: root.writable,
            skill: Skill {
                name: metadata.name.unwrap_or(fallback_name),
                description: metadata.description.unwrap_or_default(),
                source: root.source.clone(),
            },
        });
    }
    Ok(skills)
}
```

Rewrite `discover()` to apply global first-source-wins deduplication:

```rust
fn discover(&self) -> Result<Vec<DiscoveredSkill>> {
    let mut discovered = Vec::new();
    let mut seen_names = BTreeSet::new();
    for root in &self.roots {
        for skill in discover_root(root)? {
            if seen_names.insert(skill.skill.name.clone()) {
                discovered.push(skill);
            }
        }
    }
    Ok(discovered)
}
```

Refactor `read()` to match the selected location:

```rust
pub fn read(&self, name: &str, relative_path: Option<&str>) -> Result<String> {
    let discovered = self
        .discover()?
        .into_iter()
        .find(|entry| entry.skill.name == name)
        .with_context(|| format!("unknown skill: {name}"))?;
    let relative_path = relative_path.unwrap_or("SKILL.md");

    match discovered.location {
        SkillLocation::Directory(dir) => {
            let path = resolve_inside(&dir, relative_path)?;
            fs::read_to_string(&path)
                .with_context(|| format!("failed to read skill file: {}", path.display()))
        }
        SkillLocation::Builtin(files) => read_builtin_file(files, relative_path),
    }
}
```

Add normalized built-in asset lookup:

```rust
fn read_builtin_file(files: &[BuiltinSkillFile], relative_path: &str) -> Result<String> {
    let path = Path::new(relative_path);
    if path.is_absolute() {
        bail!("skill path must be relative");
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("skill path escapes skill directory");
    }
    files
        .iter()
        .find(|file| file.path == relative_path)
        .map(|file| file.content.to_string())
        .with_context(|| format!("unknown built-in skill asset: {relative_path}"))
}
```

Replace `create()` and `update()` with location-aware implementations:

```rust
pub fn create(&self, name: &str, description: &str, body: &str) -> Result<Skill> {
    validate_skill_name(name)?;
    if self.discover()?.iter().any(|entry| entry.skill.name == name) {
        bail!("skill already exists: {name}");
    }

    let root = self
        .roots
        .iter()
        .find_map(|root| match (&root.kind, root.writable) {
            (SkillRootKind::Directory(path), true) => Some((path, &root.source)),
            _ => None,
        })
        .context("no writable skill root configured")?;
    let skill_dir = root.0.join(name);
    fs::create_dir_all(&skill_dir).with_context(|| {
        format!("failed to create skill directory: {}", skill_dir.display())
    })?;
    write_skill_file(&skill_dir.join("SKILL.md"), name, description, body)?;

    Ok(Skill {
        name: name.to_string(),
        description: description.to_string(),
        source: root.1.clone(),
    })
}

pub fn update(&self, name: &str, description: &str, body: &str) -> Result<Skill> {
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
    write_skill_file(&dir.join("SKILL.md"), name, description, body)?;

    Ok(Skill {
        name: name.to_string(),
        description: description.to_string(),
        source: discovered.skill.source,
    })
}
```

- [ ] **Step 5: Run registry tests**

Run:

```bash
rtk cargo test skills::tests
```

Expected: all `skills::tests` pass, including filesystem path and precedence regressions.

- [ ] **Step 6: Commit the registry abstraction**

```bash
rtk git add src/skills.rs src/skills/builtin.rs .codrik/builtin-skills/skill-creator/SKILL.md
rtk git commit -m "feat(skills): support compiled skill sources"
```

### Task 2: Bundle the Skill Creator Workflow and Wire Its Precedence

**Files:**
- Modify: `.codrik/builtin-skills/skill-creator/SKILL.md`
- Modify: `src/app.rs`

**Interfaces:**
- Consumes: `skills::builtin_skill_root() -> SkillRoot` from Task 1.
- Produces: `default_skill_roots() -> Result<Vec<SkillRoot>>` ordered as project, user, built-in; complete `skill-creator` workflow readable through existing skill tools.

- [ ] **Step 1: Write failing precedence and instruction-index tests**

Update the expected roots in `default_skill_roots_prefer_project_then_user` and rename it:

```rust
#[test]
fn default_skill_roots_order_project_user_then_builtin() -> Result<()> {
    let roots = default_skill_roots()?;

    assert_eq!(
        roots,
        vec![
            SkillRoot::read_only(PathBuf::from(".codrik").join("skills"), "project"),
            SkillRoot::writable(codrik_dir()?.join("skills"), "user"),
            builtin_skill_root(),
        ]
    );
    Ok(())
}
```

Add these tests in `src/app.rs`:

```rust
#[test]
fn default_instructions_index_builtin_skill_creator() -> Result<()> {
    let tool_config = default_tool_config()?;

    let instructions = agent_instructions_for_tool_config(&tool_config);

    assert!(instructions.contains(
        "- skill-creator (built-in): Use when creating, writing, saving, or updating reusable skills."
    ));
    assert!(!instructions.contains("# Skill Creator"));
    Ok(())
}

#[test]
fn project_and_user_skills_override_builtin_by_order() -> Result<()> {
    let project = temp_root("project-builtin-override")?;
    let user = temp_root("user-builtin-override")?;
    write_skill(
        &project,
        "skill-creator",
        "---\nname: skill-creator\ndescription: Project creator.\n---\n# Project\n",
    )?;
    write_skill(
        &user,
        "skill-creator",
        "---\nname: skill-creator\ndescription: User creator.\n---\n# User\n",
    )?;
    let registry = SkillRegistry::new(vec![
        SkillRoot::read_only(&project, "project"),
        SkillRoot::writable(&user, "user"),
        builtin_skill_root(),
    ]);

    let skills = registry.list()?;

    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].source, "project");
    assert_eq!(registry.read("skill-creator", None)?, "---\nname: skill-creator\ndescription: Project creator.\n---\n# Project\n");

    let registry = SkillRegistry::new(vec![
        SkillRoot::writable(&user, "user"),
        builtin_skill_root(),
    ]);
    let skills = registry.list()?;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].source, "user");
    assert_eq!(registry.read("skill-creator", None)?, "---\nname: skill-creator\ndescription: User creator.\n---\n# User\n");
    Ok(())
}
```

Import `builtin_skill_root` in the `crate::skills` use list used by `src/app.rs`.

- [ ] **Step 2: Run app tests and verify the root-order test fails**

Run:

```bash
rtk cargo test app::tests::default_skill_roots_order_project_user_then_builtin
```

Expected: FAIL because `default_skill_roots()` returns only project and user roots.

- [ ] **Step 3: Write the complete bundled `SKILL.md`**

Create `.codrik/builtin-skills/skill-creator/SKILL.md` with:

```markdown
---
name: skill-creator
description: Use when creating, writing, saving, or updating reusable skills.
---

# Skill Creator

Create one focused reusable capability at a time. A skill should tell a future
agent exactly when to load it and how to complete and verify the workflow.

## Workflow

1. Clarify the requested capability, trigger conditions, constraints, and
   observable success criteria before writing the skill.
2. Call `skills_list` and check whether an existing skill already owns the
   capability. Prefer updating the active user skill over creating a duplicate.
3. Choose a lowercase name without whitespace, `/`, or `\\`. Use a specific
   capability name rather than a broad category.
4. Write a trigger-oriented description. State situations in which the skill
   must be loaded; do not merely summarize its contents.
5. Write `SKILL.md` as imperative instructions. Keep one responsibility, name
   concrete tools and inputs, include safety boundaries, and define verification.
6. Call `skills_create` for a new skill or `skills_update` for an existing
   writable user skill. These tools create or replace only `SKILL.md`.
7. Call `skills_read` for the saved skill. Verify the persisted file rather than
   trusting the mutation result.
8. Fix and verify again if any review check fails.

## Review Checks

- Frontmatter contains the intended `name` and trigger-oriented `description`.
- Instructions are complete, internally consistent, and free of placeholders.
- The scope is one capability and does not duplicate an active skill.
- Tool names and supported actions are accurate.
- Risky or irreversible actions require an explicit user confirmation.
- Success can be checked through a concrete command, read, or observable result.

## Reference Files

`skills_read` can load relative files that already exist inside a filesystem
skill. The current skill mutation tools cannot create or update those reference
files. Do not claim that `skills_create` or `skills_update` saved references;
keep required instructions in `SKILL.md` unless another available tool is
explicitly authorized to manage the files.
```

- [ ] **Step 4: Add the built-in root after the user root**

Change `default_skill_roots()` in `src/app.rs` to:

```rust
fn default_skill_roots() -> Result<Vec<SkillRoot>> {
    Ok(vec![
        SkillRoot::read_only(PathBuf::from(".codrik").join("skills"), "project"),
        SkillRoot::writable(codrik_dir()?.join("skills"), "user"),
        builtin_skill_root(),
    ])
}
```

- [ ] **Step 5: Run skill and app tests**

Run:

```bash
rtk cargo test skills::tests
rtk cargo test app::tests
rtk cargo test tools::skills::tests
```

Expected: all three test groups pass. The tools tests confirm unchanged JSON and argument schemas while using the generalized registry.

- [ ] **Step 6: Commit the bundled workflow and application wiring**

```bash
rtk git add .codrik/builtin-skills/skill-creator/SKILL.md src/app.rs src/skills.rs src/skills/builtin.rs
rtk git commit -m "feat(skills): bundle skill creator workflow"
```

### Task 3: Document Built-in Skills and Verify the Release Slice

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: final project → user → built-in registry behavior.
- Produces: user-facing documentation matching runtime behavior.

- [ ] **Step 1: Update the Skills documentation**

Replace the discovery and precedence paragraphs in `README.md` with:

```markdown
codrik discovers skills from these sources, in precedence order:

1. `.codrik/skills/<name>/SKILL.md` in the current working directory
2. `~/.codrik/skills/<name>/SKILL.md`
3. skills compiled into the codrik binary

The first skill with a given name wins, so project skills can override user and
built-in skills, and user skills can override built-ins. Built-in and project
skills are read-only through the skill tools; user skills are writable.

codrik ships with `skill-creator`, a built-in workflow for creating and
reviewing reusable user skills. It is available without installing additional
files and can be overridden by a project or user skill with the same name.
```

Keep the existing tool list and minimal-skill example. Adjust the later sentence about project skills to read:

```markdown
Project and built-in skills are read-only through the skill tools. If a higher
precedence skill hides a user skill with the same name, `skills_update` refuses
to edit the hidden user skill.
```

- [ ] **Step 2: Check formatting and the complete test suite**

Run:

```bash
rtk cargo fmt --check
rtk cargo test
rtk cargo check
rtk cargo clippy --all-targets --all-features
```

Expected: every command exits with status 0 and no warnings from Clippy.

- [ ] **Step 3: Inspect the final diff for scope and accidental secrets**

Run:

```bash
rtk git status --short
rtk git diff --check
rtk git diff --stat
rtk git diff -- . ':(exclude)Cargo.lock'
```

Expected: only the built-in asset, skill registry/catalog, app wiring/tests, and README changes are present; `git diff --check` prints no errors; no API keys or private configuration values appear.

- [ ] **Step 4: Commit documentation and any final test-only corrections**

```bash
rtk git add README.md
rtk git commit -m "docs(skills): document built-in skill precedence"
```

- [ ] **Step 5: Confirm a clean handoff state**

Run:

```bash
rtk git status --short
rtk git log -4 --oneline
```

Expected: status is clean and the recent history contains the focused implementation and documentation commits plus the design commit `55e6223`.
