# Repository Guidelines

## Project Structure & Module Organization

This is a single Rust 2024 binary crate. Source code lives in `src/`.
Top-level module roots are flat files such as `src/agent.rs`, `src/app.rs`,
`src/config.rs`, `src/interfaces.rs`, `src/llm.rs`, `src/memory.rs`, and
`src/tools.rs`. Submodules live beside them in matching directories, for
example `src/llm/openai.rs`, `src/memory/in_memory.rs`, and
`src/tools/datetime.rs`.

`src/main.rs` is the async entry point and delegates to the CLI interface.
`src/app.rs` is the composition layer that builds the configured agent from the
OpenAI client, in-memory store, and tool registry. Keep new runtime wiring near
`app.rs` or `interfaces/`; keep provider, memory, and tool logic in their
existing modules.

## Build, Test, and Development Commands

Always run shell commands through `rtk` in this workspace.

- `rtk cargo check` validates the crate quickly without producing a final binary.
- `rtk cargo test` runs unit tests, including async `tokio` tests.
- `rtk cargo fmt --check` verifies `rustfmt` formatting.
- `rtk cargo clippy --all-targets --all-features` runs lint checks across tests and all enabled features.
- `rtk cargo run -- "hello"` runs the CLI path with a sample prompt.

## Coding Style & Naming Conventions

Use standard `rustfmt` formatting and Rust naming conventions:
`snake_case` for functions, modules, and variables; `PascalCase` for structs,
enums, traits, and type aliases. Prefer small, typed domain structs over raw
JSON or stringly typed control flow. Keep async boundaries explicit and return
`anyhow::Result` only at application/interface boundaries where context is
useful.

## Testing Guidelines

Place focused unit tests in the module they exercise under `#[cfg(test)]`.
Use `#[tokio::test]` for async behavior, especially memory, LLM, tool, and
agent execution paths. Name tests after observable behavior, for example
`datetime_tool_returns_valid_result` or `memory_store_persists_messages`.
Before handing off changes, run `rtk cargo test`; for broader slices also run
`rtk cargo check`, `rtk cargo fmt --check`, and `rtk cargo clippy --all-targets --all-features`.

## Commit & Pull Request Guidelines

Git history uses Conventional Commits, usually with scopes:
`feat(gateway): add telegram gateway`, `fix(client): update OpenAI client request handling`.
Use concise subjects in the imperative mood and keep each commit focused.

Pull requests should include the purpose, behavior changes, validation commands,
and any linked issue. For CLI or Telegram-facing changes, include a short manual
test transcript or describe the runtime path exercised. Do not include real API
keys or private config values from `codrik.config.yml`.
