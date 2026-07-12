# Built-in Skill Creator Design

## Goal

Ship a read-only `skill-creator` skill inside the codrik binary. The skill must
be available without installing files while preserving the existing project and
user skill workflows.

## Scope

This change adds one built-in skill and the registry infrastructure needed to
list and read built-in skill assets. It does not change the public agent,
provider, memory, or tool interfaces. It does not add APIs for creating skill
reference files, importing third-party skills, or updating built-in skills.

## Source model and precedence

`SkillRegistry` will support two source kinds:

- filesystem roots, retaining the existing read-only and writable behavior;
- a built-in, read-only catalog whose content is compiled into the binary.

Discovery order remains the authority for duplicate names:

1. project skills from `.codrik/skills`;
2. user skills from `~/.codrik/skills`;
3. built-in skills.

The first matching name wins. A project or user skill can therefore override a
built-in skill without rebuilding codrik. The built-in source is reported as
`built-in` by `skills_list` and in the compact system-instruction index.

## Components

### Built-in catalog

A focused module at `src/skills/builtin.rs` will declare the catalog.
Skill text will remain in a normal repository asset at
`.codrik/builtin-skills/skill-creator/SKILL.md` and be compiled with
`include_str!`. Keeping content in a Markdown asset makes it reviewable and
allows the catalog to grow to include relative reference assets later.

The catalog exposes metadata discovery and relative-file reading. Its API does
not expose mutation.

### Skill registry

`SkillRegistry` will combine filesystem roots and the built-in catalog behind a
common source abstraction. A discovered skill will carry enough source-specific
information to read its content without assuming that it has a filesystem
directory.

The registry retains insertion-order precedence and name deduplication. Existing
filesystem path validation remains unchanged. Built-in reads accept only assets
registered under the selected skill and reject unknown relative paths.

`create` continues to target the first writable user root. `update` operates on
the active discovered skill and rejects project or built-in skills as read-only.
Consequently, a user skill hidden by an equal-name project skill remains
unmodifiable through `skills_update`, matching current behavior.

### Application wiring

Agent construction will add the built-in catalog after the project and user
sources. No caller should need to know whether a readable skill is stored on
disk or in the binary. The existing `skills_list`, `skills_read`,
`skills_create`, and `skills_update` schemas remain stable.

## Skill creator workflow

The bundled `skill-creator` skill will instruct the agent to:

1. Clarify the skill's purpose, triggers, constraints, and success criteria.
2. Inspect existing skills with `skills_list` to avoid accidental duplication.
3. Choose a safe, descriptive name and a precise trigger-oriented description.
4. Create or update `SKILL.md` through `skills_create` or `skills_update`.
5. Read the persisted result through `skills_read`.
6. Check its frontmatter, completeness, consistency, scope, and ambiguity.
7. Revise and verify again when a check fails.

The instructions will distinguish between references supported by
`skills_read` and the current mutation API, which can only create or replace
`SKILL.md`. It must not claim that reference files can be created through the
skill tools.

## Data flow

During agent construction, the registry discovers all configured sources and
the application appends the compact skill index to the system instructions.
When a task matches skill creation or maintenance, the model loads
`skill-creator` using `skills_read`. The built-in catalog returns the compiled
Markdown. The model then uses the existing mutation tools against the writable
user root and verifies the saved result with `skills_read`.

If a project or user version named `skill-creator` exists, discovery selects it
before the bundled version and all reads use that active version.

## Error handling

- Reading an unknown skill returns the existing `unknown skill` error.
- Reading an unknown built-in relative asset returns
  `unknown built-in skill asset: <path>`.
- Absolute paths and parent traversal remain invalid for filesystem skills;
  built-in reads also accept only normalized registered relative paths.
- Updating the active built-in skill returns `skill is read-only`.
- A malformed built-in asset is a development-time defect covered by tests,
  rather than a runtime filesystem failure.

## Testing

Focused tests will verify that:

- `skill-creator` is listed with source `built-in`;
- its complete compiled `SKILL.md` can be read;
- project and user skills override an equal-name built-in skill;
- an active built-in skill cannot be updated;
- unrelated user skills can still be created and updated;
- the compact system-instruction index includes the built-in skill;
- relative built-in asset reads reject unknown or unsafe paths;
- existing filesystem discovery, precedence, and path-safety tests continue to
  pass.

Before handoff, run `rtk cargo test`, `rtk cargo check`, `rtk cargo fmt --check`,
and `rtk cargo clippy --all-targets --all-features`.
