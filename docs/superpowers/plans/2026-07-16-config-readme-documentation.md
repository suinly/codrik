# README Configuration Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the English `README.md` configuration section into an accurate compact reference for every supported `config.yml` field.

**Architecture:** Keep configuration documentation in the existing README rather than creating a second user-facing document. Derive all fields, defaults, validation rules, and path semantics directly from `src/config.rs`, preserving the existing command and runtime-semantics sections.

**Tech Stack:** Markdown, YAML examples, Rust configuration tests, Git diff validation.

## Global Constraints

- Document only fields represented by the current `AppConfig`, `AttachmentConfig`, and `RuntimeConfig` types.
- Keep the documentation in English and inside the existing `README.md`.
- Do not document removed Telegram, gateway, session, or `--stream` configuration.
- State that `runtime` is required by `codrik serve` and `runtime.actor_id` must identify an enabled actor.
- State exact attachment defaults: `max_file_size_mb: 20` and `image_detail: auto`.
- State valid image details exactly: `auto`, `low`, and `high`.
- Explain that a leading `~/` in runtime paths resolves under `CODRIK_HOME`; `$HOME` and embedded `~` are not expanded.
- Recommend default or absolute runtime paths for service-managed execution.

---

### Task 1: Expand the README Configuration Reference

**Files:**
- Modify: `README.md`
- Verify: `src/config.rs`

**Interfaces:**
- Consumes: `AppConfig`, `AttachmentConfig`, `RuntimeConfig`, and `RuntimeConfig::resolve_paths`.
- Produces: the complete user-facing `config.yml` reference in `README.md`.

- [ ] **Step 1: Capture the missing-reference RED**

Run:

```bash
rtk rg -n 'database_path|socket_path|lock_path|artifact_path|image_detail' README.md
```

Expected: the command does not find a complete field reference; the existing README mentions default paths but not the configurable field names or attachment settings.

- [ ] **Step 2: Replace the compact Configuration section**

Keep the existing lookup precedence and minimal example, then add this complete example:

```yaml
api_key: "..."
base_url: "https://api.openai.com/v1"
model: "gpt-5"

attachments:
  max_file_size_mb: 20
  image_detail: auto

runtime:
  actor_id: actor:local:owner
  database_path: /absolute/path/to/runtime.sqlite
  socket_path: /absolute/path/to/codrik.sock
  lock_path: /absolute/path/to/runtime.lock
  artifact_path: /absolute/path/to/artifacts
```

Add a Markdown reference table with these exact rows:

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `api_key` | Yes | None | Provider API key; keep the configuration file private. |
| `base_url` | Yes | None | OpenAI-compatible API base URL. |
| `model` | Yes | None | Model name sent to the configured provider. |
| `attachments.max_file_size_mb` | No | `20` | Maximum accepted attachment size in MiB. |
| `attachments.image_detail` | No | `auto` | Image detail: `auto`, `low`, or `high`. |
| `runtime.actor_id` | For `serve` | None | Enabled actor from `users.json`. |
| `runtime.database_path` | No | `<CODRIK_HOME>/runtime.sqlite` | Durable SQLite database. |
| `runtime.socket_path` | No | `<CODRIK_HOME>/codrik.sock` | Private Unix socket. |
| `runtime.lock_path` | No | `<CODRIK_HOME>/runtime.lock` | Exclusive instance lock. |
| `runtime.artifact_path` | No | `<CODRIK_HOME>/artifacts` | Managed tool-result files. |

Add concise subsections covering:

- `CODRIK_CONFIG` → `./config.yml` → `~/.codrik/config.yml` lookup order.
- `CODRIK_HOME`, default `~/.codrik`, and the fixed
  `<CODRIK_HOME>/client/requests` metadata directory.
- Leading `~/` behavior relative to `CODRIK_HOME`.
- No `$HOME` or embedded-tilde expansion.
- Relative path behavior and the recommendation to use defaults or absolute
  paths in services.
- Actor existence/enabled validation and one-time authorization import.
- Common errors:
  missing runtime section, blank/unknown/disabled actor, unsafe runtime path,
  malformed YAML, and obsolete unsupported fields.

- [ ] **Step 3: Verify documented fields against source**

Run:

```bash
rtk rg -n 'api_key|base_url|model|max_file_size_mb|image_detail|actor_id|database_path|socket_path|lock_path|artifact_path' README.md
rtk sed -n '1,150p' src/config.rs
```

Expected: every supported field appears in README and its documented default matches `src/config.rs`.

- [ ] **Step 4: Run configuration tests**

Run:

```bash
rtk cargo test config::tests
```

Expected: all configuration tests pass.

- [ ] **Step 5: Validate Markdown diff**

Run:

```bash
rtk git diff --check
```

Expected: exit code 0 with no whitespace errors.

- [ ] **Step 6: Commit**

```bash
rtk git add README.md docs/superpowers/plans/2026-07-16-config-readme-documentation.md
rtk git commit -m "docs(config): document config.yml"
```
