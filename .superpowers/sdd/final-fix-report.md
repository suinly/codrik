# Final Whole-Branch Fix Report

Date: 2026-07-16

Base reviewed: `c09b3836c59e43c6ad4db7e7217a7d6e6e0e0571`

## Outcome

All seven Important findings from `final-review-findings.md` were fixed as one
coherent wave. The existing progress ledger and implementation plan were not
edited.

## RED and GREEN evidence

### 1. 64-stream ACK deadlock

- RED command:
  `rtk cargo test interfaces::cli::tests::verified_final_is_acked_before_metadata_becomes_terminal -- --nocapture`
- RED result: failed after the new deterministic EOF assertion timed out; the
  operation connection was still held while the client opened the ACK
  connection.
- Fix: `drive_operation` now shuts down and drops the verified final operation
  stream before opening the separate ACK connection, retaining only verified
  request/bundle/delivery coordinates.
- GREEN command: the same focused command.
- GREEN result: 1 passed.
- Saturation command:
  `rtk cargo test interfaces::cli::tests::sixty_four_final_streams_release_operation_connections_before_ack -- --nocapture`
- Saturation result: 1 passed; all 64 operation sockets reached EOF before all
  64 ACK connections completed, and every metadata record became terminal.

### 2. Multiplicative final-delivery memory

- RED command:
  `rtk cargo test runtime::ipc::protocol::tests::prepared_bundle_clones_share_canonical_payload_storage -- --nocapture`
- RED result: compile failure because `prepare_bundle` and shared prepared
  storage did not exist.
- Fix: `PreparedBundle` stores canonical payload bytes once in shared `Arc`
  storage and generates base64 chunk frames incrementally. Production socket
  sinks serialize borrowed events, so recipient count does not deep-clone the
  bundle payload. `OutboxWorker` now owns one persistent four-permit semaphore
  used by claimed delivery and delivered replay.
- GREEN commands:
  - `rtk cargo test runtime::ipc::protocol::tests::prepared_bundle_clones_share_canonical_payload_storage -- --nocapture`
  - `rtk cargo test runtime::outbox_worker::tests::delivered_replay_shares_the_four_transmission_budget -- --nocapture`
- GREEN result: both passed. The fifth concurrent replay remained blocked until
  one of the first four released its permit.

### 3. Artifact GC absent from production

- RED command:
  `rtk cargo test app::tests::production_startup_is_ordered_and_ready_only_after_recovery -- --nocapture`
- RED result: failed because the required `ArtifactsCollected` startup phase
  was absent.
- Fix: production runs startup GC before readiness, passes the exact same
  cloned `ArtifactManager` and shared path-lock registry into the runner and
  periodic GC, and supervises the periodic component. Shutdown stops the loop;
  GC authority errors return from the component and stop the supervisor.
- GREEN commands:
  - startup-order command above;
  - `rtk cargo test app::tests::periodic_artifact_gc_propagates_authority_failure -- --nocapture`
  - `rtk cargo test runtime::artifacts::tests`
- GREEN result: 1 startup test, 1 failure-propagation test, and 23 artifact
  tests passed. Existing artifact race tests continue to exercise the shared
  production manager locking behavior.

### 4. Active Resume lacks transient live join

- RED command:
  `rtk cargo test runtime::ipc::server::tests::active_resume_emits_accepted_with_the_durable_work_item -- --nocapture`
- RED result: failed when the resumed client timed out waiting for a published
  live text delta.
- Fix: actor-scoped active/rebound Resume owns and drains a transient
  `StreamSubscription` alongside its durable delivery subscription. A request
  already terminal at resolution creates only the durable delivery
  subscription. Both subscriptions drop on disconnect.
- GREEN result: focused test passed. The full server suite passed 31 tests,
  including pending/delivering replay behavior and subscription cleanup.

### 5. Resume/replay/ACK not actor-scoped

- RED command:
  `rtk cargo test runtime::sqlite::local_ingress::tests::request_resolution_is_scoped_to_the_configured_actor -- --nocapture`
- RED result: compile failure because request resolution accepted no actor.
- Fix: configured `ActorId` now flows through request resolution, replay, and
  `BundleAck`. SQLite checks request ownership, reciprocal bundle ownership,
  delivery route, and outbox actor ownership transactionally. Resume does not
  register a delivery sink until actor-scoped resolution succeeds, preventing
  a cross-actor pending bundle claim.
- GREEN commands:
  - actor-scoped resolution command above;
  - `rtk cargo test runtime::sqlite::bundles::tests::replay_and_ack_reject_another_actor_in_the_same_transaction -- --nocapture`
  - `rtk cargo test runtime::ipc::server::tests::production_handler_cannot_resume_another_actors_request -- --nocapture`
- GREEN result: all passed; the production handler also leaves no delivery
  subscription for the foreign request ID.

### 6. Ambiguous initial Submit errors hide recovery metadata

- RED command:
  `rtk cargo test interfaces::cli::tests::accepted_metadata_write_failure_prints_exact_resume_command -- --nocapture`
- RED result: failed because metadata persistence returned an error without
  writing the recovery command.
- Fix: after metadata creation, initial connect/send, sent-unconfirmed
  persistence, response decoding, rendering, accepted/terminal metadata
  persistence, stream close, and ACK errors all preserve nonterminal metadata
  and print recovery information. Initial submit failures print both
  `request id: <id>` and exact `codrik resume <id>`. No error path sends Cancel.
- GREEN result: focused test passed. EOF, ACK failure, Ctrl-C, and recovery
  tests also remain green.

### 7. Connection handler authority failures swallowed

- RED command:
  `rtk cargo test runtime::ipc::server::tests::server_run_propagates_submit_authority_failure -- --nocapture`
- RED result: failed after 200 ms because the accept loop discarded the failed
  handler result and continued running.
- Fix: the server inspects handler `JoinSet` results while waiting for permits,
  accepting connections, and shutting down. Socket/protocol/client and typed
  `AckRejected` errors remain request/connection scoped. SQLite authority
  errors and handler panics propagate to `LocalIpcServer::run`. Arbitrary ACK
  store errors are no longer converted to `ack_failed`.
- GREEN commands:
  - authority command above;
  - `rtk cargo test runtime::ipc::server::tests::server_run_propagates_connection_handler_panic -- --nocapture`
  - `rtk cargo test runtime::ipc::server::tests::ack_authority_failure_propagates_without_request_error -- --nocapture`
  - `rtk cargo test runtime::ipc::server::tests::ack_validation_rejection_stays_request_scoped -- --nocapture`
- GREEN result: all passed.

## Practical Minor findings

- Shared production `ActorSignals` now wake the dispatcher immediately after
  accepted ingress and successful cancel; the 500 ms poll remains a fallback.
- CLI cancel prints every committed `affected_request_ids` entry.
- Reciprocal request/bundle invariants retain the existing transactional
  verifier. Commit-time deferred reciprocal triggers are not practical in
  SQLite; claim/load and ACK transactions validate both directions and fail
  malformed durable state.
- Dependency seams used by integration acceptance are `#[doc(hidden)]`;
  authorization-store exposure was reduced to crate scope.
- Removed unused `teloxide`, `telegram-markdown-v2`, the legacy Telegram
  session module, and the orphan session-deletion module.
- `Timestamp::plus_millis` now uses saturating arithmetic; its synthetic
  `i64::MAX`/`i64::MIN` test passes.
- Linux/macOS no-follow behavior remains the target contract.

## Verification evidence

Focused suites:

- `rtk cargo test runtime::artifacts::tests` — 23 passed.
- `rtk cargo test runtime::outbox_worker::tests` — 19 passed.
- `rtk cargo test runtime::ipc::server::tests` — 31 passed.
- `rtk cargo test runtime::ipc::client::tests` — 6 passed.
- `rtk cargo test interfaces::local_renderer::tests` — 17 passed.
- `rtk cargo test interfaces::request_metadata::tests` — 12 passed.
- `rtk cargo test interfaces::cli::tests` — 8 passed.
- `rtk cargo test runtime::sqlite::local_ingress::tests` — 10 passed.
- `rtk cargo test runtime::sqlite::bundles::tests` — 17 passed.
- `rtk cargo test runtime::supervisor::tests` — 4 passed.
- `rtk cargo test app::tests` — 12 passed.

Acceptance and installer:

- `rtk cargo test --test serve_runtime -- --nocapture --test-threads=1` —
  17 passed.
- `rtk cargo test --test install_script -- --nocapture --test-threads=1` —
  17 passed.

Full suite:

- `rtk cargo test` — 452 passed, 1 ignored.

Exact ordered gate:

1. `rtk cargo fmt --check` — passed.
2. `rtk cargo test` — 452 passed, 1 ignored.
3. `rtk cargo check` — passed with one existing dead-code warning for the
   future external-gateway identity resolver.
4. `rtk cargo clippy --all-targets --all-features` — 0 errors; existing
   non-denied warnings remain, principally test-only mutex-guard and
   default-construction suggestions.
5. `rtk git diff --check` — passed.

## Decisions and remaining concerns

- Active Resume favors live transient join; terminal Resume remains
  delivery-only.
- The total IPC connection cap remains exactly 64. Deadlock prevention comes
  from releasing operation connections, not weakening the cap.
- The final transmission budget counts bundles/replays, not recipients. One
  prepared immutable payload is shared across a fixed recipient snapshot.
- Authority classification is type/chain based at the server boundary:
  `io::Error`, protocol failures, and `AckRejected` are scoped; other handler
  errors and panics terminate the component.
- No known correctness concern remains from the seven Important findings.
  Existing clippy warnings outside this wave are non-fatal and documented by
  the gate output.

---

## Second whole-branch re-review fix wave

Date: 2026-07-16

### Replay load ordering

- RED command:
  `rtk cargo test runtime::outbox_worker::tests::sixty_four_delivered_replays_do_not_preload_before_transmission_permits -- --nocapture`
- RED result: failed because all 64 full replay bundles loaded before the
  first four sinks obtained transmission slots.
- Fix: replay now performs a lightweight actor-scoped
  `ReplayBundleRef` lookup, acquires the shared transmission permit, reserves
  memory, and only then loads the exact delivered bundle transactionally.
  Sixty-four waiting replays retain only lightweight IDs/metadata.
- GREEN result: 1 passed. Full replay load count remained bounded by the
  active transmission/memory reservations and never approached 64.

### Explicit 64 MiB canonical-memory authority

- RED command:
  `rtk cargo test runtime::outbox_worker::tests::max_bundles_never_reserve_more_than_sixty_four_mibibytes -- --nocapture`
- RED result: compile failure because no explicit memory-budget accounting or
  peak reservation API existed.
- Fix: `OutboxWorker` owns a shared weighted 64 MiB semaphore. Each possible
  max bundle conservatively reserves 32 MiB before its full claimed/replay
  load, covering the overlap between the typed `ResultBundle` and canonical
  `PreparedBundle`. Claim leases continue renewing while waiting for memory.
  The typed bundle is explicitly dropped immediately after preparation; only
  claim/request/retry metadata and the shared prepared payload remain.
  Bundle-state polling no longer performs full payload loads.
- GREEN result: 1 passed. Mixed claimed and replay max bundles reached an
  observed peak of exactly 64 MiB, with at most two full loads before release.

### Independent immutable fan-out

- RED command:
  `rtk cargo test runtime::outbox_worker::tests::fast_recipient_reaches_final_end_without_waiting_for_slow_recipient -- --nocapture`
- RED result: failed because frame-by-frame `join_all` prevented the fast sink
  from advancing past the slow sink's first frame.
- Fix: every fixed-snapshot recipient now owns an independent lazy frame
  iteration/task over one shared `Arc<PreparedBundle>`. Canonical payload bytes
  are shared and no recipient deep-copies a full payload or frame vector.
- GREEN commands:
  - fast-recipient command above;
  - `rtk cargo test runtime::outbox_worker::tests::first_ack_does_not_truncate_another_snapshotted_send -- --nocapture`
- GREEN result: the fast recipient reached `FinalEnd` before the slow
  recipient's 45-second deadline, while the already-started slow send still
  completed after the first ACK.

### Second-wave Minors

- RED command:
  `rtk cargo test interfaces::cli::tests::definitive_missing_request_does_not_print_resume_recovery -- --nocapture`
- RED result: definitive `missing_request` printed a misleading resume command.
- Fix: definitive request rejections (`missing_request`, actor unavailable,
  request conflict, and other non-retryable request errors) no longer print
  resume recovery. Transport/EOF/ACK/shutdown ambiguity still prints recovery;
  `server_busy` remains explicitly recoverable.
- Replay full-load transactions now revalidate reciprocal request ownership,
  configured actor ownership, delivered state, and every joined
  `outbox.actor_id`. A corruption test proves foreign outbox ownership rejects
  replay.
- Removed the obsolete `TelegramConfig` token surface. `AppConfig` now rejects
  unknown legacy `telegram:` configuration while all current API, attachment,
  and runtime configuration remains valid.

### Second-wave verification

Focused:

- `rtk cargo test runtime::outbox_worker::tests` — 21 passed.
- `rtk cargo test runtime::ipc::protocol::tests` — 21 passed.
- `rtk cargo test runtime::sqlite::bundles::tests` — 18 passed.
- `rtk cargo test runtime::ipc::server::tests` — 31 passed.
- `rtk cargo test runtime::ipc::client::tests` — 6 passed.
- `rtk cargo test interfaces::local_renderer::tests` — 18 passed.
- `rtk cargo test interfaces::cli::tests` — 9 passed.
- `rtk cargo test app::tests` — 12 passed.
- `rtk cargo test config::tests` — 7 passed.

Full and acceptance:

- `rtk cargo test` — 458 passed, 1 ignored.
- `rtk cargo test --test serve_runtime -- --nocapture --test-threads=1` —
  17 passed.
- `rtk cargo test --test install_script -- --nocapture --test-threads=1` —
  17 passed.

Second-wave exact ordered gate:

1. `rtk cargo fmt --check` — passed.
2. `rtk cargo test` — 458 passed, 1 ignored.
3. `rtk cargo check` — passed with the existing unused future
   external-gateway identity-resolver warning.
4. `rtk cargo clippy --all-targets --all-features` — 0 errors; 46 existing
   non-denied warnings.
5. `rtk git diff --check` — passed.

Second-wave decisions:

- The four-transmission limit remains, but the weighted memory authority can
  reduce concurrent max-sized bundles below four to preserve the hard 64 MiB
  decoded/canonical ceiling.
- Lightweight replay resolution may run for many waiting clients; no full
  payload load or preparation occurs before both transmission and memory
  authority are held.
- Recipient independence applies only to socket progress. Durable first-ACK
  semantics remain unchanged and never cancel a send already started from the
  fixed snapshot.
