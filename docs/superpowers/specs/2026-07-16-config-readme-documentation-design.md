# README Configuration Documentation Design

## Goal

Expand the existing English `Configuration` section in `README.md` into a
compact, accurate reference for the current `config.yml` schema used by
`codrik serve`.

## Audience

Codrik users configuring a local foreground runtime on Linux or macOS. The
reader should not need to inspect Rust source code to discover supported
fields, defaults, path behavior, or actor requirements.

## Structure

The existing `Configuration` section will contain:

1. Configuration lookup precedence:
   `CODRIK_CONFIG`, `./config.yml`, then `~/.codrik/config.yml`.
2. A minimal configuration example.
3. A complete example containing every currently supported field.
4. A field-reference table covering:
   - `api_key`
   - `base_url`
   - `model`
   - `attachments.max_file_size_mb`
   - `attachments.image_detail`
   - `runtime.actor_id`
   - `runtime.database_path`
   - `runtime.socket_path`
   - `runtime.lock_path`
   - `runtime.artifact_path`
5. Runtime path and environment behavior.
6. Actor authorization and validation requirements.
7. A short troubleshooting list for common configuration failures.

## Accuracy Rules

- Document only fields represented by the current `AppConfig`,
  `AttachmentConfig`, and `RuntimeConfig` Rust types.
- `api_key`, `base_url`, and `model` are required.
- `attachments` is optional. Its defaults are:
  - `max_file_size_mb: 20`
  - `image_detail: auto`
- Valid `image_detail` values are `auto`, `low`, and `high`.
- `runtime` is required by `codrik serve`; `runtime.actor_id` must be nonblank.
- The configured actor must exist in `users.json` during first startup and be
  enabled.
- Runtime path defaults are rooted under `CODRIK_HOME`, which defaults to
  `~/.codrik`.
- A configured leading `~/` is resolved relative to `CODRIK_HOME`, matching
  current implementation behavior. `$HOME` and embedded `~` values are not
  expanded.
- Relative configured paths remain relative paths and therefore depend on the
  server process working directory. The documentation should recommend
  defaults or absolute paths for services.
- Client request metadata remains under
  `<CODRIK_HOME>/client/requests` and is not configurable.
- Existing authorization is imported into SQLite once; `users.json` is not
  rewritten by the runtime.
- Do not document removed Telegram, gateway, session, or streaming flags.

## Presentation

- Keep the section concise enough for the main README.
- Prefer one complete YAML block and one compact Markdown table.
- Use warnings only for security- or reliability-relevant behavior:
  protecting `api_key`, using safe runtime directories, and avoiding relative
  service paths.
- Preserve the existing commands and runtime-semantics sections.

## Validation

- Compare every documented field and default against `src/config.rs`.
- Run `cargo test config::tests` after editing README to ensure the referenced
  behavior still matches the tested schema.
- Run `git diff --check` to validate Markdown whitespace.
