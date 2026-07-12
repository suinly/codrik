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
3. Choose a lowercase name without whitespace, `/`, or `\`. Use a specific
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
