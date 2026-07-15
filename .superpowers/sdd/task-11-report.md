# Task 11 Report: IPC Client, Renderer, and Request Metadata

Implemented the local IPC command path without adding Task 12 runtime composition.
The CLI now accepts `serve`, `resume <uuid>`, `cancel <uuid>`, and a prompt,
rejects the legacy gateway/session/stream forms, and routes submit/resume/cancel
operations through one-operation Unix socket connections.

## RED evidence

- Replaced the parser tests first and ran
  `rtk cargo test interfaces::cli::tests::parses_supported_commands`. Compilation
  failed on the missing `Serve`, `Resume`, `Cancel`, and `Submit` variants.
- Added metadata, client, and renderer tests before their definitions. The
  focused metadata run failed with unresolved `RequestMetadataStore`,
  `RequestMetadataState`, `LocalIpcClient`, `LocalRenderer`, and `RenderAction`.
- Added the verified-final integration test before the operation driver. It
  failed on the unresolved `drive_operation` boundary.

## GREEN implementation

- `RequestMetadataStore` persists only `{ request_id, created_at,
  prompt_sha256, state }`. Its four monotonic states are `created`,
  `sent_unconfirmed`, `accepted`, and `terminal`. Writes use a unique mode-0600
  temp file, file fsync, atomic rename, and directory fsync. The leaf directory
  is created/repaired to mode 0700; metadata reads use `O_NOFOLLOW` and reject
  non-regular or incorrectly permissioned files. Stale crash temp files never
  replace the authoritative request file.
- `LocalIpcClient` applies a bounded socket connect and the protocol's bounded
  frame write, sends exactly one operation, and retains the write half only to
  keep the event subscription live. Submit, Resume, Cancel, and final ACK each
  use a fresh connection. Connection/write errors name the socket and suggest
  `codrik serve`.
- `LocalRenderer` selects terminal behavior with `std::io::IsTerminal`.
  Non-TTY output suppresses transient activity and deltas. TTY output renders
  activity and deltas, suppresses later deltas after `StreamGap`, and prints the
  verified authoritative final from its beginning.
- Final delivery holds at most one bundle and rejects manifests outside the
  1,024-delivery, 256-KiB-manifest, and 16-MiB decoded limits. It validates
  request/bundle/delivery IDs, exact chunk counts and indexes, decoded chunk
  bounds, complete delivery sizes and SHA-256 hashes, canonical manifest hash,
  and typed payload kinds before producing authoritative output or ACK
  coordinates.
- The CLI writes final output only after verification, sends ACK on a separate
  Task 10-compatible connection, and only then marks metadata terminal.
  Ctrl-C, EOF, and `ServerShuttingDown` leave metadata nonterminal, close only
  the client connection, and print exactly `codrik resume <id>`. An integration
  test confirms interrupt sends no Cancel frame.

## Verification evidence

- `rtk cargo test runtime::ipc::client::tests` — 2 passed.
- `rtk cargo test interfaces::local_renderer::tests` — 3 passed.
- `rtk cargo test interfaces::request_metadata::tests` — 3 passed.
- `rtk cargo test interfaces::cli::tests` — 4 passed.
- `rtk cargo test` — 399 passed, 1 ignored.
- `rtk cargo check` — passed with the existing pre-composition warning baseline.
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; existing warning
  baseline remains.
- `rtk git diff --check` — passed.

## Self-review and concerns

- Task 10 submission registration and final-delivery snapshot reservations are
  unchanged. The original subscription socket stays open while final ACK uses
  a second, one-operation connection.
- The v1 protocol has no positive ACK response. The client therefore treats an
  orderly EOF after writing ACK as completion; metadata remains nonterminal if
  the ACK connection itself reports a framing/read failure.
- `serve` parsing is present, but its runtime composition intentionally remains
  Task 12. No supervisor, SQLite recovery, observability, `app.rs` wiring, or
  gateway deletion was added here.
