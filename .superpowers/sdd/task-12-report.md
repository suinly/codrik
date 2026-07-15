# Task 12 Report: Supervisor and Production Composition

Implemented the foreground `codrik serve` composition root, fail-closed startup,
transactional recovery, component supervision, two-phase graceful shutdown, and
redacted JSON observability. The local CLI's submit/resume/cancel paths remain IPC
only; the legacy one-shot/session agent composition and polling Telegram interface
were removed.

## RED evidence

The required suites were run separately before production definitions existed:

- `rtk cargo test runtime::supervisor::tests` failed on unresolved
  `Supervisor`, `ServeRuntime`, and `RuntimeLogEvent` definitions.
- `rtk cargo test runtime::sqlite::recovery::tests` failed on the missing
  startup-recovery API and fixture probes.

The failing tests established named unexpected-exit propagation, sibling
cancellation, the exact 30-second forced-stop boundary, redacted typed log shape,
and atomic recovery of expired actor/bundle claims and orphaned running attempts.

The review correction began with additional RED tests for live and stale actor
fences, absent/expired leases, idempotent recovery, auth-marker short-circuiting,
explicit component readiness, system-instruction injection, terminal/recovery log
emission, safe shutdown recovery, and model-future cancellation. The recovery
suite initially failed on the missing fence-aware fixture/probe APIs. The new
runner and composition tests then drove the instruction, logger, and startup seams.

## GREEN implementation

- `app::serve` now performs ordered startup: resolve and validate configuration
  and paths, create the private runtime/artifact directories, acquire the
  instance lock, open/migrate SQLite, import `users.json` once through the
  durable marker, require the configured actor to exist and be enabled,
  revalidate every authority parent, remove a stale socket through the held
  lock descriptor, bind mode-0600 IPC, recover durable claims/attempts, compose
  every runtime component, poll them, and only then emit readiness.
- Production composition builds one configured `RuntimeActor`, actor-scoped
  `ToolRegistry`, `OpenAiClient`, `ArtifactManager`, streaming `ActorRunner`,
  `ActorDispatcher`, `StreamHub`, `OutboxWorker`, and `LocalIpcServer`.
  The exact base and skill-index instructions are injected as one transient
  system message on every model request. They are never checkpointed into the
  durable conversation and therefore cannot accumulate between model steps.
- `ServeRuntime` owns named component tasks. Any component return before a
  shutdown signal is fatal, names the exited component, aborts siblings, and
  returns an error for service-manager restart.
- Normal SIGINT/SIGTERM broadcasts the stop watch. Dispatcher stops acquiring
  after its active quantum; outbox stops claiming after active transmissions;
  IPC stops accepting, marks the connection registry draining, broadcasts
  `ServerShuttingDown`, rejects operations not yet started, and lets recognized
  handlers/final sends/ACKs drain. The supervisor force-drops leftovers at 30
  seconds. A deterministic paused-time test covers the exact deadline, and an
  IPC test proves an ACK already inside the durable boundary commits during
  drain.
- Shutdown recovery immediately releases this process's safe actor leases and
  changes this process's sent-but-unacknowledged bundle claims to
  `failed_retryable`. Actor leases with a persisted `running` tool are retained;
  startup recovery later changes that attempt to `outcome_unknown` and its work
  item to `waiting_for_decision`. Forced model-future drops do not enter the
  dispatcher's persisted failure-recording path.
- Startup recovery runs in one immediate transaction and reports counts for
  expired actor leases, expired bundle claims, and orphaned running attempts.
  A `running` attempt is recovered only when its active run has no actor lease,
  an expired lease at the fresh recovery timestamp, or a lease generation that
  does not match the run fence. A live matching fence is never modified, even if
  another process used a different instance-lock path. Typed recovered
  actor/work/run/attempt/generation coordinates are returned for logging.
- The durable legacy-auth marker is queried before `users.json` is opened. An
  existing marker makes a missing or corrupt legacy file irrelevant; an absent
  marker parses the snapshot and the import transaction rechecks the marker.
  Parse/import failure leaves the marker absent.
- `RuntimeLogEvent` accepts typed correlation IDs, typed component/transition
  enums, and redacted error classes. `StderrRuntimeLogger` writes one JSON object
  per line. Startup paths, schema v2, actor, recovery counts, readiness,
  shutdown, component-terminal state, fifth work failure, and each recovered
  unknown outcome are emitted after their durable commits without prompt,
  model, tool, or outbox payload fields.
- `CliCommand::Serve` loads configuration and enters `app::serve`.
  `CliCommand::Submit` only builds `LocalIpcClient`; there is no client-side
  `Agent` construction path. Legacy Telegram polling modules/files and the
  `interfaces::telegram` export were deleted. No Task 13 installer work was
  included.

## Verification evidence

- `rtk cargo test runtime::supervisor::tests` — 4 passed.
- `rtk cargo test runtime::sqlite::recovery::tests` — 4 passed.
- `rtk cargo test runtime::ipc::server::tests` — 25 passed.
- `rtk cargo test runtime::runner::tests` — 23 passed.
- `rtk cargo test runtime::sqlite::failures::tests` — 8 passed.
- `rtk cargo test runtime::outbox_worker::tests` — 18 passed.
- `rtk cargo test runtime::observability::tests` — 2 passed.
- `rtk cargo test app::tests` — 10 passed.
- `rtk cargo test interfaces::cli::tests` — 6 passed.
- `rtk cargo test` — 406 passed, 1 ignored.
- `rtk cargo check` — passed; the existing crate-wide unused/dead-code warning
  baseline remains after removing the legacy production path.
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; existing warnings
  remain.
- `rtk git diff --check` — passed.

## Decisions and self-review

- The lock remains alive across listener ownership, recovery, all supervised
  tasks, socket unlink, and shutdown recovery. Both startup and shutdown socket
  removal use `InstanceLock::remove_stale_socket`; no bare pathname unlink was
  introduced.
- Readiness uses an explicit per-component first-poll handshake. Each task sends
  ready only after its component future returns `Pending`; a future returning
  `Ok` or `Err` on that poll fails startup before readiness. No scheduler-yield
  heuristic remains.
- IPC shutdown is deliberately two phase. `ServerShuttingDown` no longer closes
  recognized connections itself, because doing so would defeat the specified
  transmission/ACK grace. Dropping the server future at the supervisor deadline
  drops its `JoinSet`, which force-aborts remaining handlers.
- Startup imports legacy authorization only. No legacy session/database
  migration or deletion was added, matching the fresh-database decision ledger.
- Recovery strings persisted in `last_error` are fixed redacted classes
  (`interrupted_delivery`, `shutdown_before_ack`), never external payloads.
- A real private runtime directory/socket/SQLite startup test records every
  production phase in order through readiness. Missing/disabled actors fail
  before stale-socket cleanup. Paused time proves the supervisor remains live at
  29 seconds and force-drops remaining work at exactly 30 seconds.

## Concerns

- End-to-end real-socket/process signal coverage belongs to Task 13; Task 12
  verifies the component boundaries and paused-time races in focused tests.
- Removing the legacy polling composition exposes a large pre-existing dead-code
  warning surface in the old one-shot/session modules. This task deliberately
  did not broaden into deleting non-brief memory/auth compatibility files or
  unrelated dependencies.
