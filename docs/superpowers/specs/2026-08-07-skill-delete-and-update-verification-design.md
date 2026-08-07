# Skill Deletion and Update Verification Design

## Goal

Allow an agent to delete an existing user skill safely, and verify the complete
agent-facing workflow for updating an existing skill so an attempted edit does
not silently become creation of a second skill.

## Scope

This change adds one mutation tool, `skills_delete`, and strengthens the tested
contract and built-in guidance for `skills_update`. It does not make project or
built-in skills writable, rename skills, recover deleted skills, or add mutation
support for individual reference files.

## Registry Behavior

`SkillRegistry::delete(name)` will use the same discovery order and exact-name
lookup as `read` and `update`. The active discovered skill must be backed by a
writable directory root. Project skills, built-in skills, and any other
read-only source remain immutable, including when they shadow an equal-name
user skill.

On success, deletion removes the complete skill directory with all contained
files and subdirectories. The caller cannot supply a path. Existing skill-name
validation and discovery resolve the exact deletion target, preventing path
traversal and accidental deletion of a neighboring skill.

The registry returns the deleted skill summary after successful removal. An
unknown name, unsafe name, read-only active skill, or filesystem failure returns
an error and must not be reported as success.

## Agent Tool Contract

Add a standard `skills_delete` tool with these required arguments:

- `name`: the exact existing skill name;
- `confirm`: a boolean that must be `true`.

Missing or false confirmation rejects the call before registry mutation. A
successful call returns the same compact skill summary shape used by create and
update: `name`, `description`, and `source`.

Keep the mutation operations deliberately distinct:

- `skills_create` only creates a new name and rejects duplicates;
- `skills_update` only replaces `SKILL.md` for an existing active writable
  skill and rejects missing or read-only skills;
- `skills_delete` only removes an existing active writable skill directory and
  requires explicit confirmation.

There is no automatic fallback from update to create or from create to update.

## Built-in Skill-Creator Guidance

Revise the bundled `skill-creator` workflow to make the decision sequence
explicit:

1. Call `skills_list` before choosing a mutation.
2. If the requested capability matches an existing writable user skill, use
   `skills_update` with that exact listed name.
3. Do not create a differently named replacement when update fails. Report the
   failure or resolve the exact-name/read-only issue first.
4. Use `skills_create` only when no existing skill owns the capability.
5. After create or update, call `skills_read` and verify persisted content.
6. Before deletion, identify the exact listed user skill and call
   `skills_delete` only with explicit user confirmation.
7. After deletion, call `skills_list` and verify that the skill is absent.

This guidance reduces model-selection errors while the strict tool contracts
continue to protect storage independently of prompt compliance.

## Registration and Policies

Register `skills_delete` alongside the existing standard skill tools. Actors
with the standard `*` grant receive it, while explicitly allowlisted actors must
name it to use it.

Webhook `SkillsOnly` execution remains read-only. It continues to expose only
`skills_list` and `skills_read`; create, update, and delete remain forbidden.
Capability metadata for deletion is conservative and not retry-safe.

## Testing

Registry tests will verify that deletion:

- removes `SKILL.md`, reference files, nested directories, and the containing
  skill directory;
- returns the deleted skill summary;
- rejects unknown, unsafe, project, built-in, and shadowed user skills;
- leaves neighboring skill directories unchanged.

Tool tests will verify argument parsing, required `confirm: true`, successful
summary serialization, and zero filesystem mutation for rejected confirmation.
Tool-registry tests will verify registration, wildcard exposure, conservative
capabilities, and read-only webhook policy behavior.

The update workflow will receive an end-to-end test through the tool registry:
list an existing writable skill, update it using a full `SKILL.md` body that
contains frontmatter, then read it back. The assertions will prove that the
original directory was updated, no second directory was created, frontmatter
was not duplicated, and the listed metadata reflects the update.

Final validation will run:

```text
rtk cargo test
rtk cargo check
rtk cargo fmt --check
rtk cargo clippy --all-targets --all-features
```
