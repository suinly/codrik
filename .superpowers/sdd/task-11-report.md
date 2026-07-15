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
- The review correction began with failing coverage for the missing positive
  ACK response, ACK EOF recovery (which printed no resume command), strict
  manifest delivery order, bounded delivery allocation, escaped JSON payloads,
  interleaved shutdown recovery, the missing metadata lock path, and trailing
  arguments accepted by `update`.

## GREEN implementation

- `RequestMetadataStore` persists only `{ request_id, created_at,
  prompt_sha256, state }`. Its four monotonic states are `created`,
  `sent_unconfirmed`, `accepted`, and `terminal`. A per-request mode-0600 OS
  lock serializes load, monotonic validation, unique temp creation, file fsync,
  atomic rename, and directory fsync across processes. The leaf directory is
  created/repaired to mode 0700; metadata and lock reads use `O_NOFOLLOW` and
  reject non-regular, wrong-owner, or incorrectly permissioned files. Tests
  cover concurrent stale writers, backward transitions, permission failures,
  injected pre-rename failure, cleanup, and preservation of the authoritative
  request file.
- `LocalIpcClient` applies a bounded socket connect and the protocol's bounded
  frame write, sends exactly one operation, and retains the write half only to
  keep the event subscription live. Submit, Resume, Cancel, and final ACK each
  use a fresh connection. The client marks ACK success only after receiving an
  exact matching v1 `AckAccepted { request_id, bundle_id }`; EOF, mismatched
  coordinates, protocol errors, and request errors fail explicitly. A paused
  transport test proves the 30-second frame-write timeout. Connection/write
  errors name the socket and suggest `codrik serve`.
- The server emits `AckAccepted` only after the outbox has durably acknowledged
  the exact bundle. Durable ACK failure emits `RequestError` with code
  `ack_failed` and never emits a positive response. The frozen wire test covers
  the exact v1 JSON shape.
- `LocalRenderer` selects terminal behavior with `std::io::IsTerminal`.
  Non-TTY output suppresses transient activity and deltas. TTY output renders
  activity and deltas, suppresses later deltas after `StreamGap`, and prints the
  verified authoritative final from its beginning.
- Final delivery holds at most one decoded bundle and rejects manifests outside
  the 1,024-delivery, 256-KiB-manifest, and 16-MiB decoded limits. A strict
  cursor requires one `FinalBegin`, manifest delivery order, contiguous chunks,
  no interleaving, and the canonical 192-KiB decoded partition. Base64 decodes
  directly into each delivery buffer while SHA-256 is updated incrementally;
  no chunk vectors, assembled duplicate, or owned payload copy are retained.
  Before output or ACK coordinates are produced, it validates every ID, count,
  decoded size, delivery and manifest hash, allowed kind, exact payload shape,
  and complete bundle state. Borrowed raw JSON values preserve escaped strings
  without copying the payload.
- The CLI writes final output only after verification, sends ACK on a separate
  connection, and only after the matching positive response marks metadata
  terminal. ACK failure leaves metadata nonterminal and prints exactly
  `codrik resume <id>`.
  Ctrl-C, EOF, and `ServerShuttingDown` leave metadata nonterminal, close only
  the client connection, and print exactly `codrik resume <id>`. An integration
  test confirms interrupt sends no Cancel frame.

## Verification evidence

- `rtk cargo test runtime::ipc::protocol::tests` — 20 passed.
- `rtk cargo test runtime::ipc::server::tests` — 24 passed.
- `rtk cargo test runtime::ipc::client::tests` — 5 passed.
- `rtk cargo test interfaces::local_renderer::tests` — 11 passed.
- `rtk cargo test interfaces::request_metadata::tests` — 7 passed.
- `rtk cargo test interfaces::cli::tests` — 5 passed.
- `rtk cargo test` — 417 passed, 1 ignored.
- `rtk cargo check` — passed with the existing pre-composition warning baseline.
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; existing warning
  baseline remains.
- `rtk git diff --check` — passed.

## Self-review and concerns

- Task 10 submission registration and final-delivery snapshot reservations are
  unchanged. The original subscription socket stays open while final ACK uses
  a second, one-operation connection.
- The narrow v1 protocol amendment adds only the explicit `AckAccepted`
  confirmation required to distinguish durable success from EOF. All ACK
  failure paths remain recoverable through the exact resume command and retain
  nonterminal metadata.
- `serve` parsing is present, but its runtime composition intentionally remains
  Task 12. No supervisor, SQLite recovery, observability, `app.rs` wiring, or
  gateway deletion was added here.
