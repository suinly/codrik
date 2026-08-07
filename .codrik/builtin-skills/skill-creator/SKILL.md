---
name: skill-creator
description: Use when creating, writing, saving, updating, or deleting reusable skills.
---

# Skill Creator

Create one focused reusable capability at a time. A skill should tell a future
agent exactly when to load it and how to complete and verify the workflow.

## Workflow

1. Clarify the requested capability, trigger conditions, constraints, and
   observable success criteria before writing the skill.
2. Call `skills_list` before choosing a mutation. Use the exact listed name and
   source in later calls.
3. If an existing writable user skill owns the capability, call
   `skills_update`. If update fails, report or resolve that error; never create
   a differently named replacement.
4. Call `skills_create` only when no existing skill owns the capability.
5. For deletion, identify the exact writable user skill, obtain explicit user
   confirmation, then call `skills_delete` with `confirm: true`.
6. Choose a lowercase name without whitespace, `/`, or `\`. Use a specific
   capability name rather than a broad category.
7. Write a trigger-oriented description. State situations in which the skill
   must be loaded; do not merely summarize its contents.
8. Write `SKILL.md` as imperative instructions. Keep one responsibility, name
   concrete tools and inputs, include safety boundaries, and define verification.
9. After create or update, call `skills_read` and verify the persisted file.
   After deletion, call `skills_list` and verify that the skill is absent.
10. Fix and verify again if any review check fails.

## Review Checks

- Frontmatter contains the intended `name` and trigger-oriented `description`.
- Instructions are complete, internally consistent, and free of placeholders.
- The scope is one capability and does not duplicate an active skill.
- Tool names and supported actions are accurate.
- Risky or irreversible actions require an explicit user confirmation.
- Success can be checked through a concrete command, read, or observable result.

## Reference Files

`skills_read` can load relative files that already exist inside a filesystem
skill. `skills_create` and `skills_update` cannot create or update those
reference files. `skills_delete` removes them with the complete skill directory.
Do not claim that create or update saved references; keep required instructions
in `SKILL.md` unless another available tool is explicitly authorized to manage
the files.
