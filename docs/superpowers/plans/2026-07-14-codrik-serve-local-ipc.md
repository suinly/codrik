# Codrik Serve and Local IPC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `codrik serve` the single durable agent runtime and route local prompt, resume, cancel, streaming, and final delivery through an authenticated Unix-socket protocol.

**Architecture:** SQLite remains the authority for actor work, tool outcomes, immutable outbox intents, request/result correlation, and atomic result-bundle delivery state. `codrik serve` composes a fenced dispatcher, best-effort `StreamHub`, bundle outbox worker, managed artifact store, and same-UID Unix-socket server; ordinary CLI commands are thin IPC clients and never instantiate an agent.

**Tech Stack:** Rust 2024, Tokio, tokio-rusqlite/SQLite, Serde JSON, SHA-256, Unix domain sockets, libc peer credentials, fs2 file locks, systemd user services, launchd agents.

## Global Constraints

- Run every shell command through `rtk`.
- Preserve SOLID boundaries: orchestration in `runtime`, persistence in `runtime/sqlite`, provider adaptation in `llm`, CLI rendering in `interfaces`, composition in `app.rs`.
- Linux and macOS are the supported IPC platforms; reject peers whose effective UID differs from the daemon UID.
- Socket frames are 4-byte big-endian length-prefixed JSON with a 1 MiB maximum payload.
- Submit text is nonblank and at most 256 KiB of UTF-8.
- Final chunks contain at most 192 KiB decoded bytes; manifest and decoded bundle limits are 256 KiB and 16 MiB respectively; one bundle contains at most 1,024 deliveries.
- Use at most 64 IPC connections, one subscription per connection, 32 MiB aggregate transient queue bytes, 256 events/512 KiB per subscription, and four concurrent final transmissions.
- Frame-header, frame-body, socket-write, bundle-claim, and ACK limits are respectively 5, 30, 30, 30, and 30 seconds; renew bundle claims every 10 seconds.
- Artifact limits are 256 MiB per file and 2 GiB retained per actor.
- Dispatcher fallback polling is 500 ms; recoverable quantum retries are 1, 2, 4, and 8 seconds, terminal on the fifth consecutive failure.
- Delivery retry delays are 1, 2, 4, 8, then capped at 30 seconds; attempt count alone never makes a result unavailable.
- Do not persist prompt text or response payload in client request metadata.
- Do not promise exactly-once LLM execution after an ambiguous provider/crash boundary.
- Each task follows red-green-refactor, runs focused tests, and ends in one Conventional Commit.

## File and Module Map

- `src/config.rs`: validated runtime paths, actor ID, and protocol/resource defaults.
- `src/runtime/model.rs`: IDs and durable domain enums for requests, bundles, and artifacts.
- `src/runtime/store.rs`: narrow trusted-ingress, artifact, bundle, failure, and recovery traits.
- `src/runtime/migrations/0002_serve.sql`: v2 tables, v1 archive/quarantine, constraints, and indexes.
- `src/runtime/sqlite/{local_ingress,artifacts,bundles,failures,recovery}.rs`: persistence implementations split by responsibility.
- `src/runtime/artifacts.rs`: filesystem staging, hashing, quota checks, and GC coordination.
- `src/runtime/ipc/{protocol,security,server,client}.rs`: framed protocol and Unix transport.
- `src/runtime/stream_hub.rs`: bounded, nonblocking transient fan-out and gap semantics.
- `src/runtime/dispatcher.rs`: continuous configured-actor dispatch and persisted failure backoff.
- `src/runtime/outbox_worker.rs`: bundle claim, encoding, broadcast, ACK, retry, and replay.
- `src/runtime/supervisor.rs`: component lifetime, readiness, signals, and graceful shutdown.
- `src/runtime/observability.rs`: redacted structured runtime events and stderr sink.
- `src/interfaces/cli.rs`: command parsing and IPC-only command dispatch.
- `src/interfaces/local_renderer.rs`: TTY/non-TTY streaming and verified final rendering.
- `src/interfaces/request_metadata.rs`: atomic non-payload client recovery metadata.
- `src/app.rs`: production composition root for `serve` and local clients.
- `scripts/install.sh`: clean-owner bootstrap and service definitions that execute `codrik serve`.
- `tests/serve_runtime.rs`: real-socket, on-disk SQLite end-to-end tests.

---

### Task 1: Runtime Configuration and Domain Types

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/config.rs`
- Modify: `src/runtime/model.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produces: `RuntimeConfig`, `RuntimePaths`, `RequestId`, `CancelId`, `BundleId`, `DeliveryId`, `ArtifactId`, `LocalRequestState`, `BundleState`, and all exact limits used by later tasks.
- Consumes: existing `codrik_dir()` and `AppConfig::load`.

- [ ] **Step 1: Write failing configuration and ID tests**

Add tests proving leading-`~/` and `CODRIK_HOME` expansion, defaults, rejection of blank actor IDs, and no arbitrary shell expansion:

```rust
#[test]
fn runtime_config_defaults_under_codrik_home() -> Result<()> {
    let config: AppConfig = yaml_serde::from_str(
        "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: actor:local:owner\n",
    )?;
    let paths = config.runtime.resolve_paths(Path::new("/tmp/codrik-home"))?;
    assert_eq!(paths.database, PathBuf::from("/tmp/codrik-home/runtime.sqlite"));
    assert_eq!(paths.socket, PathBuf::from("/tmp/codrik-home/codrik.sock"));
    assert_eq!(paths.lock, PathBuf::from("/tmp/codrik-home/runtime.lock"));
    assert_eq!(paths.artifacts, PathBuf::from("/tmp/codrik-home/artifacts"));
    Ok(())
}

#[test]
fn request_ids_reject_non_uuid_strings() {
    assert!(RequestId::parse("not-a-uuid").is_err());
}
```

- [ ] **Step 2: Run tests and verify the missing types fail compilation**

Run separately:

```bash
rtk cargo test config::tests
rtk cargo test runtime::model::tests
```

Expected: FAIL because `RuntimeConfig`, `resolve_paths`, and `RequestId::parse` do not exist.

- [ ] **Step 3: Add dependencies and exact domain definitions**

Add `fs2 = "0.4"` and enable UUID serde. Define new IDs with `new`, `parse`, `as_str`, `Display`, and serde; retain existing string-backed IDs for compatibility. Add:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    pub actor_id: String,
    #[serde(default)] pub database_path: Option<PathBuf>,
    #[serde(default)] pub socket_path: Option<PathBuf>,
    #[serde(default)] pub lock_path: Option<PathBuf>,
    #[serde(default)] pub artifact_path: Option<PathBuf>,
}

pub struct RuntimePaths {
    pub database: PathBuf,
    pub socket: PathBuf,
    pub lock: PathBuf,
    pub artifacts: PathBuf,
    pub client_requests: PathBuf,
}

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_SUBMIT_BYTES: usize = 256 * 1024;
pub const MAX_FINAL_CHUNK_BYTES: usize = 192 * 1024;
pub const MAX_MANIFEST_BYTES: usize = 256 * 1024;
pub const MAX_BUNDLE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BUNDLE_DELIVERIES: usize = 1024;
```

`AppConfig.runtime` is required for `serve`, prompt, resume, and cancel paths but remains `Option<RuntimeConfig>` during parsing so `update` can run without loading runtime configuration. Add `AppConfig::required_runtime()` for the explicit error boundary.

- [ ] **Step 4: Run focused tests**

Run separately:

```bash
rtk cargo test config::tests
rtk cargo test runtime::model::tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add Cargo.toml Cargo.lock src/config.rs src/runtime.rs src/runtime/model.rs
rtk git commit -m "feat(runtime): add serve configuration types"
```

### Task 2: SQLite v2 Migration and Quarantine

**Files:**
- Create: `src/runtime/migrations/0002_serve.sql`
- Modify: `src/runtime/sqlite.rs`

**Interfaces:**
- Consumes: Task 1 domain state names.
- Produces: v2 schema for local requests, artifacts, v2 outbox intents, result bundles, immutable memberships, failure fields, cancellation snapshots, archive, and quarantine.

- [ ] **Step 1: Write failing fresh-v2 and seeded-v1 migration tests**

Create test helpers that open a raw temporary v1 database, seed one row in every old outbox state plus an active run, running tool, pending event, and unmanaged file intent, then reopen through `SqliteRuntimeStore::open`:

```rust
#[tokio::test]
async fn v1_migration_archives_outbox_and_quarantines_active_work() -> Result<()> {
    let path = temp_db_path("v1-quarantine");
    seed_v1_runtime(&path).await?;
    let store = SqliteRuntimeStore::open(&path).await?;
    let probe = store.v2_probe().await?;
    assert_eq!(probe.user_version, 2);
    assert_eq!(probe.archived_outbox, 7);
    assert_eq!(probe.active_runs, 0);
    assert_eq!(probe.pending_events, 0);
    assert_eq!(probe.quarantined_entities, 4);
    Ok(())
}
```

Also assert fresh databases apply v1 then v2 and expose all tables and foreign keys.

- [ ] **Step 2: Run migration tests and verify failure**

Run separately:

```bash
rtk cargo test runtime::sqlite::tests::v1_migration
rtk cargo test runtime::sqlite::tests::fresh_database
```

Expected: FAIL because schema version 2 and v2 tables do not exist.

- [ ] **Step 3: Implement the transactional migration**

Create v2 tables with these authoritative shapes:

```sql
CREATE TABLE local_requests (
    request_id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id),
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id),
    work_item_id TEXT NOT NULL REFERENCES work_items(id),
    prompt_sha256 TEXT NOT NULL CHECK(length(prompt_sha256) = 64),
    state TEXT NOT NULL CHECK(state IN ('active','completed','cancelled','failed_terminal')),
    result_bundle_id TEXT UNIQUE REFERENCES result_bundles(id) DEFERRABLE INITIALLY DEFERRED,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK ((state = 'active') = (result_bundle_id IS NULL))
) STRICT;

CREATE TABLE result_bundles (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE REFERENCES local_requests(request_id),
    delivery_count INTEGER NOT NULL CHECK(delivery_count BETWEEN 1 AND 1024),
    manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256) = 64),
    state TEXT NOT NULL CHECK(state IN ('pending','delivering','delivered','failed_retryable','failed_terminal')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER,
    claim_owner TEXT,
    claim_expires_at INTEGER,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
```

Add `artifacts`, v2 intent-only `outbox`, immutable `outbox_deliveries`, `cancel_targets`, `legacy_outbox_archive`, and `legacy_runtime_quarantine`. Add `failure_count`, `next_attempt_at`, `last_error`, and `cancellation_requested_at` to `work_items`. Rebuild v1 outbox into archive only; terminalize/quarantine nonterminal v1 runtime; update `PRAGMA user_version = 2` last inside one immediate transaction.

- [ ] **Step 4: Run migration tests and inspect integrity**

Run: `rtk cargo test runtime::sqlite::tests`

Expected: PASS, including `PRAGMA foreign_key_check` returning no rows and archive/source count equality.

- [ ] **Step 5: Commit**

```bash
rtk git add src/runtime/migrations src/runtime/sqlite.rs
rtk git commit -m "feat(runtime): migrate durable store to schema v2"
```

### Task 3: Trusted Local Ingress, Correlation, and Cancellation

**Files:**
- Modify: `src/runtime/store.rs`
- Create: `src/runtime/sqlite/local_ingress.rs`
- Modify: `src/runtime/sqlite.rs`
- Modify: `src/runtime/sqlite/ingress.rs`
- Modify: `src/runtime/sqlite/dispatch.rs`

**Interfaces:**
- Produces: `LocalIngressStore::submit_for_actor`, `cancel_for_actor`, `resolve_local_request`, `LocalSubmitOutcome`, `CancelOutcome`, and `LocalRequestRecord`.
- Consumes: `ActorId`, `RequestId`, `WorkItemId`, `BundleId`, Task 2 tables.

- [ ] **Step 1: Write failing trusted-ingress tests**

Cover same-ID/same-hash idempotency, conflicting prompt hash, disabled actor, direct actor ingestion without identity, `local:submit` versus `local:cancel` namespaces, cancellation snapshots, terminal cancel idempotency, and refusal to attach new input after `cancellation_requested_at`:

```rust
#[tokio::test]
async fn cancel_freezes_targets_and_new_submit_uses_new_work_item() -> Result<()> {
    let (store, actor) = authorized_store().await?;
    let request = RequestId::parse("0190f2ef-0000-7000-8000-000000000001")?;
    let first = store.submit_for_actor(&actor, submission(request.clone(), "first"), Timestamp(1)).await?;
    let cancelled = store.cancel_for_actor(&actor, cancel("0190f2ef-0000-7000-8000-000000000002", request.clone()), Timestamp(2)).await?;
    assert_eq!(cancelled.affected_request_ids, vec![request]);
    let second = store.submit_for_actor(&actor, submission(RequestId::new(), "second"), Timestamp(3)).await?;
    assert_ne!(first.work_item_id, second.work_item_id);
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify missing trait methods**

Run: `rtk cargo test runtime::sqlite::local_ingress::tests`

Expected: FAIL because `LocalIngressStore` and its implementation do not exist.

- [ ] **Step 3: Define narrow ingress commands and outcomes**

Add:

```rust
#[derive(Clone)]
pub struct LocalSubmission {
    pub request_id: RequestId,
    pub text: String,
    pub prompt_sha256: String,
}

pub struct LocalCancel {
    pub cancel_id: CancelId,
    pub request_id: RequestId,
}

pub enum LocalSubmitOutcome {
    Accepted { event_id: EventId, work_item_id: WorkItemId, sequence: i64 },
    Duplicate { event_id: EventId, work_item_id: WorkItemId, sequence: i64 },
    Conflict,
    ActorUnavailable,
}

pub struct CancelOutcome {
    pub cancel_id: CancelId,
    pub affected_request_ids: Vec<RequestId>,
    pub already_terminal: bool,
}

pub struct LocalRequestRecord {
    pub request_id: RequestId,
    pub actor_id: ActorId,
    pub work_item_id: WorkItemId,
    pub state: LocalRequestState,
    pub result_bundle_id: Option<BundleId>,
}

#[async_trait]
pub trait LocalIngressStore: Send + Sync {
    async fn submit_for_actor(&self, actor: &ActorId, command: LocalSubmission, now: Timestamp) -> Result<LocalSubmitOutcome>;
    async fn cancel_for_actor(&self, actor: &ActorId, command: LocalCancel, now: Timestamp) -> Result<CancelOutcome>;
    async fn resolve_local_request(&self, id: &RequestId) -> Result<Option<LocalRequestRecord>>;
    async fn load_actor(&self, id: &ActorId) -> Result<Option<RuntimeActor>>;
}
```

Keep identity-resolving `IngressStore::ingest` unchanged for future external gateways. In one immediate submit transaction verify actor enabled, compare prompt hash for duplicates, select only work without cancellation marker, insert `events.gateway = 'local:submit'`, and insert `local_requests`. In one cancel transaction set the marker, create `local:cancel`, snapshot `cancel_targets`, and return the immutable target list.

- [ ] **Step 4: Update dispatch queries for cancellation markers**

Exclude cancellation-marked work from ordinary event attachment while still allowing its control event to wake the active run. Keep Task 3 focused on the durable cancel marker and immutable target snapshot; Task 5 replaces the existing `cancel_run` terminal transition with atomic per-target result bundles after the shared bundle helper exists.

- [ ] **Step 5: Run ingress tests**

Run separately:

```bash
rtk cargo test runtime::sqlite::local_ingress::tests
rtk cargo test runtime::sqlite::dispatch::tests
```

Expected: PASS except the explicitly Task-5-owned cancellation-finalization test is not added until Task 5.

- [ ] **Step 6: Commit**

```bash
rtk git add src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/ingress.rs src/runtime/sqlite/local_ingress.rs src/runtime/sqlite/dispatch.rs
rtk git commit -m "feat(runtime): add trusted local ingress"
```

### Task 4: Managed Artifact Store

**Files:**
- Create: `src/runtime/artifacts.rs`
- Create: `src/runtime/sqlite/artifacts.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produces: `ArtifactManager::stage_execution`, `ArtifactStore`, `ManagedArtifact`, `DurableToolExecution`, and `collect_garbage`.
- Consumes: raw `ToolExecution`/`FileArtifact`, actor ID, attempt ID, runtime artifact path.

- [ ] **Step 1: Write failing filesystem and race tests**

Test regular-file copy, symlink rejection, 256 MiB preflight, actor quota, content-addressed dedupe, expired staging cleanup, active lease protection, and DB-reference-before-file impossibility:

```rust
#[tokio::test]
async fn gc_does_not_remove_artifact_with_live_staging_lease() -> Result<()> {
    let fixture = ArtifactFixture::new().await?;
    let staged = fixture.manager.begin_stage(&fixture.actor, &fixture.source, Timestamp(10)).await?;
    fixture.manager.collect_garbage(Timestamp(11)).await?;
    assert!(staged.managed_path.exists());
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify failure**

Run separately:

```bash
rtk cargo test runtime::artifacts::tests
rtk cargo test runtime::sqlite::artifacts::tests
```

Expected: FAIL because artifact modules and traits do not exist.

- [ ] **Step 3: Implement the two-phase artifact lifecycle**

Define:

```rust
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn begin_staging(&self, command: BeginArtifact, now: Timestamp) -> Result<ArtifactLease>;
    async fn renew_staging(&self, lease: &ArtifactLease, until: Timestamp) -> Result<ArtifactLease>;
    async fn commit_staged_execution(&self, run: &AttachedRun, attempt: &AttemptId, execution: DurableToolExecution, leases: &[ArtifactLease], now: Timestamp) -> Result<()>;
    async fn claim_expired_staging(&self, now: Timestamp, limit: usize) -> Result<Vec<ExpiredArtifact>>;
}

pub struct ManagedArtifact {
    pub id: ArtifactId,
    pub managed_path: PathBuf,
    pub display_name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256: String,
    pub caption: Option<String>,
}

pub struct DurableToolExecution {
    pub observation: String,
    pub artifacts: Vec<ManagedArtifact>,
}
```

`ArtifactManager` opens source files without retaining symlinks, copies in bounded chunks, renews the DB staging lease, hashes, fsyncs, renames, fsyncs the parent, then calls `commit_staged_execution`. That store method verifies every lease, changes each artifact to `referenced`, and persists the successful attempt outcome in one immediate SQLite transaction. Convert raw executions to `DurableToolExecution { observation, artifacts: Vec<ManagedArtifact> }`; keep the existing failure/cancellation `finish_attempt` path for outcomes with no successful artifacts.

- [ ] **Step 4: Implement quota-aware GC**

GC claims only expired staging rows, rechecks ownership/state immediately before unlink, and deletes orphan files only when older than one hour and absent from a second DB lookup. Enforce actor retained bytes in the staging transaction and return a known tool error on quota exhaustion.

- [ ] **Step 5: Run focused tests**

Run separately:

```bash
rtk cargo test runtime::artifacts::tests
rtk cargo test runtime::sqlite::artifacts::tests
rtk cargo test runtime::runner::tests
```

Expected: PASS, including recovered successful tool outcomes containing only managed paths and hashes.

- [ ] **Step 6: Commit**

```bash
rtk git add src/runtime.rs src/runtime/artifacts.rs src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/artifacts.rs src/runtime/runner.rs
rtk git commit -m "feat(runtime): persist immutable tool artifacts"
```

### Task 5: Atomic Finalization and Result Bundles

**Files:**
- Create: `src/runtime/sqlite/bundles.rs`
- Modify: `src/runtime/sqlite/checkpoint.rs`
- Modify: `src/runtime/sqlite/outbox.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/model.rs`
- Modify: `src/runtime/runner.rs`

**Interfaces:**
- Produces: `BundleStore`, `ResultBundle`, `BundleManifest`, `FinalPayload`, claim/renew/fail/ack/replay APIs, and atomic completion/cancel/failure bundle creation.
- Consumes: managed artifacts from Task 4 and local-request routes from Task 3.

- [ ] **Step 1: Write failing bundle invariant tests**

Cover one immutable intent shared across request bundles, contiguous ordinals, terminal request/non-null bundle invariant, exact manifest ACK, stale ACK from retryable, no ACK from pending, malformed terminal transition, more than 32 memberships, and 16 MiB/256 KiB/1,024 limits.

```rust
#[tokio::test]
async fn oversized_result_is_replaced_atomically_by_terminal_error() -> Result<()> {
    let fixture = FinalizeFixture::with_two_requests().await?;
    let oversized = "x".repeat(MAX_BUNDLE_BYTES + 1);
    fixture.finalize_text(oversized).await?;
    for request in fixture.requests() {
        let stored = fixture.store.resolve_local_request(request).await?.unwrap();
        assert_eq!(stored.state, LocalRequestState::FailedTerminal);
        assert_eq!(fixture.store.bundle_payloads(stored.bundle_id()).await?.len(), 1);
    }
    assert_eq!(fixture.store.original_intent_count().await?, 0);
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify failure**

Run separately:

```bash
rtk cargo test runtime::sqlite::bundles::tests
rtk cargo test runtime::sqlite::checkpoint::tests::local_
```

Expected: FAIL because v2 bundle operations are missing.

- [ ] **Step 3: Define the bundle store API**

```rust
#[async_trait]
pub trait BundleStore: Send + Sync {
    async fn claim_ready_bundles(&self, owner: &str, request_ids: &[RequestId], now: Timestamp, until: Timestamp, limit: usize) -> Result<Vec<ClaimedBundle>>;
    async fn renew_bundle(&self, claim: &BundleClaim, now: Timestamp, until: Timestamp) -> Result<BundleClaim>;
    async fn load_bundle(&self, id: &BundleId) -> Result<ResultBundle>;
    async fn acknowledge_bundle(&self, ack: BundleAck, now: Timestamp) -> Result<AckOutcome>;
    async fn fail_bundle_retryable(&self, claim: &BundleClaim, error: &str, next_attempt: Timestamp, now: Timestamp) -> Result<()>;
    async fn replay_bundle(&self, request: &RequestId) -> Result<Option<ResultBundle>>;
}

#[derive(Clone)]
pub enum FinalPayload {
    Text { text: String },
    File { artifact: ManagedArtifact },
    TerminalError { code: String, message: String },
}

pub struct BundleManifest {
    pub entries: Vec<BundleManifestEntry>,
    pub sha256: String,
}

pub struct BundleManifestEntry {
    pub delivery_id: DeliveryId,
    pub payload_kind: String,
    pub decoded_bytes: usize,
    pub sha256: String,
    pub chunk_count: usize,
}

pub struct ResultBundle {
    pub id: BundleId,
    pub request_id: RequestId,
    pub state: BundleState,
    pub manifest: BundleManifest,
    pub deliveries: Vec<(DeliveryId, FinalPayload)>,
}

pub struct BundleClaim {
    pub bundle_id: BundleId,
    pub owner: String,
    pub expires_at: Timestamp,
}

pub struct ClaimedBundle {
    pub claim: BundleClaim,
    pub bundle: ResultBundle,
}

pub struct BundleAck {
    pub request_id: RequestId,
    pub bundle_id: BundleId,
    pub delivery_ids: Vec<DeliveryId>,
}

pub enum AckOutcome { Delivered, AlreadyDelivered }
```

`BundleAck` contains request ID, bundle ID, and the exact set of delivery IDs. SQL validates manifest equality and route ownership in one immediate transaction.

- [ ] **Step 4: Rebuild finalization around immutable intents and bundles**

Extract one private `create_terminal_bundles(transaction, request_ids, payloads, terminal_state, now)` helper used by successful finalization, cancellation, fifth-failure terminalization, and oversize replacement. Validate canonical payload/manifest sizes before inserting original intents. Gather managed file artifacts from durable tool outcomes and emit typed file intents.

- [ ] **Step 5: Remove the v1 `OutboxStore` drain API**

Delete `pending_outbox`, `mark_outbox_delivered`, `LocalKernel::drain_outbox`, and row-level `OutboxState` control flow. Keep `OutboxPayload` as immutable intent data, updated so file payloads reference `ArtifactId`, managed path, size, and hash.

- [ ] **Step 6: Run bundle and kernel tests**

Run separately:

```bash
rtk cargo test runtime::sqlite::bundles::tests
rtk cargo test runtime::sqlite::checkpoint::tests
rtk cargo test runtime::service::tests
rtk cargo test runtime::runner::tests
```

Expected: PASS with old drain tests removed and all terminal paths producing bundles.

- [ ] **Step 7: Commit**

```bash
rtk git add src/runtime/model.rs src/runtime/store.rs src/runtime/runner.rs src/runtime/service.rs src/runtime/sqlite.rs src/runtime/sqlite/bundles.rs src/runtime/sqlite/checkpoint.rs src/runtime/sqlite/outbox.rs
rtk git commit -m "feat(runtime): finalize runs into result bundles"
```

### Task 6: Versioned Framed IPC Protocol

**Files:**
- Create: `src/runtime/ipc.rs`
- Create: `src/runtime/ipc/protocol.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produces: `ClientRequest`, `ServerEvent`, `FinalManifestEntry`, `FrameReader`, `FrameWriter`, `ProtocolErrorCode`, canonical payload encoding, and bundle chunking.
- Consumes: request/bundle/delivery IDs and Task 5 final payloads.

- [ ] **Step 1: Write failing protocol tests**

Test every request/event round trip, big-endian prefix, invalid UTF-8/JSON/version/UUID, zero/oversized/incomplete frames, 5/30 second read deadlines with paused Tokio time, 192 KiB chunking, canonical manifest hash, and guaranteed sub-1-MiB encoded frames.

```rust
#[tokio::test]
async fn encoded_final_chunks_never_exceed_frame_limit() -> Result<()> {
    let payload = vec![b'x'; MAX_BUNDLE_BYTES];
    let frames = encode_bundle(bundle_with_text(payload))?;
    assert!(frames.iter().all(|frame| serde_json::to_vec(frame).unwrap().len() <= MAX_FRAME_BYTES));
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify failure**

Run: `rtk cargo test runtime::ipc::protocol::tests`

Expected: FAIL because protocol types and codecs do not exist.

- [ ] **Step 3: Implement strict serde envelopes and framing**

Use `#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]` bodies inside `{ version: 1, body }`. Define Submit, Resume, AckFinal, Cancel, Accepted, CancelAccepted, Activity, TextDelta, StreamGap, FinalBegin, FinalChunk, FinalEnd, RequestError, ProtocolError, and ServerShuttingDown exactly as the design spec. `FrameReader` rejects length before allocating and applies separate header/body deadlines.

- [ ] **Step 4: Implement canonical final encoding**

Serialize each typed payload to canonical structs with deterministic field order, hash decoded bytes incrementally, split before base64, compute the canonical manifest hash, and reject limit violations with `BundleLimitError` for Task 5 replacement.

- [ ] **Step 5: Run protocol tests**

Run: `rtk cargo test runtime::ipc::protocol::tests`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/runtime.rs src/runtime/ipc.rs src/runtime/ipc/protocol.rs
rtk git commit -m "feat(ipc): add framed local protocol"
```

### Task 7: Nonblocking StreamHub and Streaming Runner

**Files:**
- Create: `src/runtime/stream_hub.rs`
- Modify: `src/runtime/runner.rs`
- Modify: `src/runtime/service.rs`
- Modify: `src/runtime.rs`
- Modify: `src/llm/client.rs`

**Interfaces:**
- Produces: `StreamHub`, `StreamSubscription`, `RuntimeEventPublisher`, and an `ActorRunner` using `LlmStreamClient`.
- Consumes: attached request IDs from local ingress and existing `OpenAiClient::stream`.

- [ ] **Step 1: Write failing hub and runner tests**

Test fan-out to multiple requests, bounded event/byte queues, one reserved `StreamGap`, text suppression after a gap, activity continuation, subscription replacement prohibition, disconnect isolation, and a slow subscriber not failing model execution.

```rust
#[tokio::test]
async fn overflow_emits_one_gap_and_suppresses_later_text() {
    let hub = StreamHub::with_limits(2, 16, 64);
    let mut sub = hub.subscribe(request("r1")).unwrap();
    hub.publish_text(&[request("r1")], "0123456789");
    hub.publish_text(&[request("r1")], "overflow");
    hub.publish_text(&[request("r1")], "ignored");
    assert_eq!(sub.drain_types().await, vec!["text_delta", "stream_gap"]);
}
```

- [ ] **Step 2: Run tests and verify failure**

Run separately:

```bash
rtk cargo test runtime::stream_hub::tests
rtk cargo test runtime::runner::tests::stream_
```

Expected: FAIL because `StreamHub` and streaming runner integration do not exist.

- [ ] **Step 3: Implement nonblocking hub**

Store per-subscription bounded channels plus byte counters and a gap/suppress flag. `publish_text`/`publish_activity` are synchronous best-effort methods returning `()`; they never await a subscriber and never surface backpressure as a runner error. Track a global 32 MiB budget and per-subscription 256-event/512-KiB budgets.

```rust
pub trait RuntimeEventPublisher: Send + Sync {
    fn publish_text(&self, requests: &[RequestId], delta: &str);
    fn publish_activity(&self, requests: &[RequestId], event: AgentActivityEvent);
}
```

- [ ] **Step 4: Convert the runner to streaming**

Change `ActorRunner<L>` to require `LlmStreamClient`. Add request IDs to `AttachedRun`. Provide a `RuntimeLlmSink` that forwards only text deltas to `RuntimeEventPublisher`; `ToolCallDelta` remains provider accumulation detail. Publish model/tool activity around existing runner steps. Preserve the returned complete `LlmResponse` as the authoritative checkpoint/finalization input.

- [ ] **Step 5: Run focused tests**

Run separately:

```bash
rtk cargo test runtime::stream_hub::tests
rtk cargo test runtime::runner::tests
rtk cargo test runtime::service::tests
```

Expected: PASS; scripted streaming models emit deltas while finalization still stores the full text.

- [ ] **Step 6: Commit**

```bash
rtk git add src/runtime.rs src/runtime/stream_hub.rs src/runtime/runner.rs src/runtime/service.rs src/llm/client.rs
rtk git commit -m "feat(runtime): stream transient agent progress"
```

### Task 8: Persisted Dispatcher Failures and Continuous Loop

**Files:**
- Create: `src/runtime/dispatcher.rs`
- Create: `src/runtime/sqlite/failures.rs`
- Create: `src/runtime/sqlite/retry.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite.rs`
- Modify: `src/runtime/sqlite/dispatch.rs`
- Modify: `src/runtime/runner.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produces: `ActorDispatcher`, `FailureStore`, `QuantumReport`, `QuantumProgress`, and `QuantumFailure`.
- Consumes: configured actor ID, `ActorSignals`, fenced runner, Task 5 terminal bundle helper.

- [ ] **Step 1: Write failing deterministic backoff tests**

Use `ManualClock` and scripted runner results to prove 1/2/4/8 second scheduling, fifth-failure terminal bundles for every incorporated request, reset only on real progress, 500 ms lost-notification polling, one configured actor, three bounded `SQLITE_BUSY` retries at 10/25/50 ms, malformed work blocking without a busy loop, and authority errors terminating the component.

```rust
#[tokio::test(start_paused = true)]
async fn fifth_recoverable_failure_terminalizes_incorporated_requests() -> Result<()> {
    let fixture = DispatcherFixture::recoverable_failures(5).await?;
    fixture.run_until_idle().await?;
    assert_eq!(fixture.work_state().await?, WorkItemState::FailedTerminal);
    assert!(fixture.all_requests_have_error_bundles().await?);
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify failure**

Run separately:

```bash
rtk cargo test runtime::dispatcher::tests
rtk cargo test runtime::sqlite::failures::tests
```

Expected: FAIL because dispatcher/failure APIs do not exist.

- [ ] **Step 3: Define failure classification and store transitions**

```rust
pub enum QuantumProgress {
    None,
    ModelCheckpoint,
    KnownToolOutcome,
    Finalized,
}

pub struct QuantumReport {
    pub work_item_id: Option<WorkItemId>,
    pub outcome: RunOnceOutcome,
    pub progress: QuantumProgress,
}

pub enum QuantumFailure {
    RecoverableWork { work_item_id: WorkItemId, message: String },
    AuthorityUnavailable(anyhow::Error),
}

#[async_trait]
pub trait QuantumRunner: Send + Sync {
    async fn run_quantum(&self, actor: &ActorId, owner: &str)
        -> std::result::Result<QuantumReport, QuantumFailure>;
}

#[async_trait]
pub trait FailureStore: Send + Sync {
    async fn record_failure(&self, work: &WorkItemId, error: &str, now: Timestamp) -> Result<FailureDisposition>;
    async fn record_progress(&self, work: &WorkItemId, now: Timestamp) -> Result<()>;
}

pub enum FailureDisposition {
    RetryAt(Timestamp),
    Terminalized,
}
```

Make runner outcomes report whether new model output, known tool outcome, or finalization committed after the most recent failure. Do not reset on replayed incorporation.

Add `sqlite::retry::call_with_busy_retry` around immediate authority transactions. Retry only SQLite busy/locked codes after 10, 25, and 50 ms; propagate I/O, corruption, unsupported schema, and exhausted busy errors for dispatcher classification. A malformed persisted inbound/checkpoint payload atomically marks its work item blocked and emits a diagnostic instead of entering this retry path.

- [ ] **Step 4: Implement the continuous dispatcher**

Wait on `ActorSignals` and a 500 ms interval, acquire only `runtime.actor_id`, run one configured worker initially, honor `next_attempt_at`, and isolate recoverable work errors. Return `Err` from `run()` on DB I/O/corruption/unsupported schema so the supervisor restarts the process.

- [ ] **Step 5: Run dispatcher tests**

Run separately:

```bash
rtk cargo test runtime::dispatcher::tests
rtk cargo test runtime::sqlite::failures::tests
rtk cargo test runtime::runner::tests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/runtime.rs src/runtime/dispatcher.rs src/runtime/runner.rs src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/dispatch.rs src/runtime/sqlite/failures.rs src/runtime/sqlite/retry.rs
rtk git commit -m "feat(runtime): dispatch durable actor work continuously"
```

### Task 9: Result-Bundle Outbox Worker

**Files:**
- Create: `src/runtime/outbox_worker.rs`
- Modify: `src/runtime/stream_hub.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produces: `OutboxWorker`, `BundleDeliverySink`, `DeliveryRegistry`, and full replay/ACK behavior.
- Consumes: `BundleStore`, Task 6 encoder, active subscriptions, system clock.

- [ ] **Step 1: Write failing worker state-machine tests**

Cover no-subscriber/no-attempt, whole-bundle claim, 32-bundle batch, 10-second renewal, exact ACK, disconnect retry, expiry recovery, stale ACK, read-only delivered replay, recipient snapshot, late subscriber replay, non-truncating first ACK, malformed terminal failure, and delivery retry schedule.

```rust
#[tokio::test(start_paused = true)]
async fn worker_renews_claim_during_slow_transmission() -> Result<()> {
    let fixture = WorkerFixture::slow_bundle(Duration::from_secs(45)).await?;
    fixture.start();
    tokio::time::advance(Duration::from_secs(31)).await;
    assert_eq!(fixture.claim_owner().await?, Some(fixture.worker_id()));
    assert!(fixture.renew_count().await? >= 3);
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify failure**

Run: `rtk cargo test runtime::outbox_worker::tests`

Expected: FAIL because `OutboxWorker` does not exist.

- [ ] **Step 3: Implement subscription-aware delivery**

`DeliveryRegistry` reports subscribed request IDs and snapshots connection sinks. `OutboxWorker` polls on registry notification plus 500 ms fallback, claims at most 32 bundles, holds a semaphore of four transmissions, renews claims every 10 seconds, and sends all frames from `FinalBegin` through `FinalEnd` to the snapshot.

Use these boundaries so persistence never depends on sockets:

```rust
#[async_trait]
pub trait BundleDeliverySink: Send + Sync {
    async fn send(&self, event: ServerEvent) -> Result<()>;
}

pub trait DeliveryRegistry: Send + Sync {
    fn subscribed_request_ids(&self) -> Vec<RequestId>;
    fn snapshot(&self, request: &RequestId) -> Vec<Arc<dyn BundleDeliverySink>>;
    fn subscribe_changes(&self) -> watch::Receiver<u64>;
}
```

- [ ] **Step 4: Implement ACK and retry coordination**

Route `AckFinal` to `BundleStore::acknowledge_bundle`. First valid ACK changes durable state but does not cancel other send futures. If every sink fails before ACK, record retryable with 1/2/4/8/30-second delay. Delivered resume uses `replay_bundle` without claims or state changes.

- [ ] **Step 5: Run worker tests**

Run separately:

```bash
rtk cargo test runtime::outbox_worker::tests
rtk cargo test runtime::sqlite::bundles::tests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/runtime.rs src/runtime/outbox_worker.rs src/runtime/stream_hub.rs src/runtime/store.rs
rtk git commit -m "feat(runtime): deliver durable result bundles"
```

### Task 10: Unix Socket Security, Instance Lock, and IPC Server

**Files:**
- Create: `src/runtime/instance_lock.rs`
- Create: `src/runtime/ipc/security.rs`
- Create: `src/runtime/ipc/server.rs`
- Modify: `src/runtime/ipc.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produces: `InstanceLock`, `LocalIpcServer`, `SubmissionRegistry`, `AuthorizedUnixStream`, and request handler dispatch.
- Consumes: protocol, trusted ingress, stream/delivery registries, runtime paths.

- [ ] **Step 1: Write failing security and server tests**

Test exclusive lock, stale socket removal only under lock, mode 0700/0600, unsafe/writable/symlink parent rejection, peer UID accept/reject through an injectable credential reader, 64-connection cap, one operation per connection, slow/incomplete frames, and submit/resume race joining the in-flight registry.

```rust
#[tokio::test]
async fn resume_waits_for_inflight_submit_before_reporting_missing() -> Result<()> {
    let fixture = IpcFixture::pause_submit_before_commit().await?;
    let submit = fixture.spawn_submit("r1", "hello");
    let resume = fixture.spawn_resume("r1");
    fixture.allow_submit_commit();
    assert!(matches!(resume.await??, ServerEvent::Accepted { .. }));
    submit.await??;
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify failure**

Run separately:

```bash
rtk cargo test runtime::instance_lock::tests
rtk cargo test runtime::ipc::security::tests
rtk cargo test runtime::ipc::server::tests
```

Expected: FAIL because lock/security/server modules do not exist.

- [ ] **Step 3: Implement fail-closed path and peer security**

Use `fs2::FileExt::try_lock_exclusive` for the OS-managed lock. Validate effective UID ownership and permissions with `symlink_metadata`; bind under restrictive umask and set socket mode before accept. Read Linux `SO_PEERCRED` or macOS `getpeereid` behind a small `PeerCredentials` trait so tests do not require a second real UID.

- [ ] **Step 4: Implement the server and submission registry**

Acquire a 64-permit connection semaphore before spawning a handler. Register a fully decoded Submit before its SQLite future begins; Resume awaits the matching watch receiver before querying durable state. Submit registers stream/delivery subscriptions before calling trusted ingress, sends `Accepted` first, then forwards transient/final events. Resume only registers/wakes delivery. Cancel sends `CancelAccepted`. ACK delegates to the outbox worker/store and closes after success.

- [ ] **Step 5: Run server tests**

Run separately:

```bash
rtk cargo test runtime::instance_lock::tests
rtk cargo test runtime::ipc::security::tests
rtk cargo test runtime::ipc::server::tests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/runtime.rs src/runtime/instance_lock.rs src/runtime/ipc.rs src/runtime/ipc/security.rs src/runtime/ipc/server.rs
rtk git commit -m "feat(ipc): serve authenticated local connections"
```

### Task 11: IPC Client, Renderer, and Request Metadata

**Files:**
- Create: `src/runtime/ipc/client.rs`
- Create: `src/interfaces/local_renderer.rs`
- Create: `src/interfaces/request_metadata.rs`
- Modify: `src/runtime/ipc.rs`
- Modify: `src/interfaces.rs`
- Modify: `src/interfaces/cli.rs`

**Interfaces:**
- Produces: `LocalIpcClient::{submit,resume,cancel}`, `LocalRenderer`, `RequestMetadataStore`, and new `CliCommand` variants.
- Consumes: framed protocol, runtime socket/client paths.

- [ ] **Step 1: Replace CLI parser tests first**

Delete legacy gateway/session/stream variants and add exact parsing/rejection tests:

```rust
#[test]
fn parses_supported_commands() -> Result<()> {
    const UUID: &str = "0190f2ef-0000-7000-8000-000000000001";
    assert_eq!(parse(["serve"])? , CliCommand::Serve);
    assert_eq!(parse(["resume", UUID])?, CliCommand::Resume(RequestId::parse(UUID)?));
    assert_eq!(parse(["cancel", UUID])?, CliCommand::Cancel(RequestId::parse(UUID)?));
    assert_eq!(parse(["hello"])? , CliCommand::Submit("hello".into()));
    assert!(parse(["gateway", "telegram"]).is_err());
    assert!(parse(["--session", "x", "hello"]).is_err());
    assert!(parse(["--stream", "hello"]).is_err());
    Ok(())
}
```

- [ ] **Step 2: Run parser tests and verify failure**

Run: `rtk cargo test interfaces::cli::tests::parses_supported_commands`

Expected: FAIL with old command variants.

- [ ] **Step 3: Implement atomic request metadata**

Store `{ request_id, created_at, prompt_sha256, state }` with states `created`, `sent_unconfirmed`, `accepted`, `terminal` using temp-file, fsync, rename, 0700 directory, and 0600 file. Never write prompt or response. Add crash/permission tests and a recovery-command formatter.

- [ ] **Step 4: Implement client and verified renderer**

`LocalIpcClient` connects to the configured socket, applies write deadlines, sends one operation, and exposes a stream of server events. `LocalRenderer` uses `std::io::IsTerminal`: TTY renders spinner/deltas, stops text after `StreamGap`, then prints verified authoritative text from the beginning; non-TTY suppresses deltas. Buffer at most one 16 MiB bundle, verify all chunks/hash/manifest before output, ACK, then mark metadata terminal.

- [ ] **Step 5: Implement Ctrl-C/EOF behavior**

Select on protocol events and `tokio::signal::ctrl_c()`. Ctrl-C closes only the client connection and prints `codrik resume <id>`; it never sends Cancel. EOF or `ServerShuttingDown` keeps metadata nonterminal and prints the same recovery command. Daemon-unavailable errors name the socket and `codrik serve`.

- [ ] **Step 6: Run client/renderer/CLI tests**

Run separately:

```bash
rtk cargo test runtime::ipc::client::tests
rtk cargo test interfaces::local_renderer::tests
rtk cargo test interfaces::request_metadata::tests
rtk cargo test interfaces::cli::tests
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
rtk git add src/runtime/ipc.rs src/runtime/ipc/client.rs src/interfaces.rs src/interfaces/cli.rs src/interfaces/local_renderer.rs src/interfaces/request_metadata.rs
rtk git commit -m "feat(cli): route local commands through IPC"
```

### Task 12: Supervisor and Production Composition

**Files:**
- Create: `src/runtime/supervisor.rs`
- Create: `src/runtime/observability.rs`
- Create: `src/runtime/sqlite/recovery.rs`
- Modify: `src/runtime.rs`
- Modify: `src/app.rs`
- Modify: `src/interfaces/cli.rs`
- Modify: `src/interfaces.rs`
- Delete: `src/interfaces/telegram.rs`
- Delete: `src/interfaces/telegram/activity_status.rs`
- Delete: `src/interfaces/telegram/commands.rs`
- Delete: `src/interfaces/telegram/files.rs`
- Delete: `src/interfaces/telegram/format.rs`
- Delete: `src/interfaces/telegram/run_coordinator.rs`
- Modify: `src/main.rs`

**Interfaces:**
- Produces: `app::serve(config)`, `ServeRuntime`, startup recovery report, and graceful shutdown.
- Consumes: every runtime component from Tasks 1-11.

- [ ] **Step 1: Write failing startup/shutdown tests**

Test ordered fail-closed startup, configured enabled actor, auth import once, stale socket under lock, expired bundle/actor claims, orphaned running tools, component unexpected-exit propagation, shutdown notice, no failure-count increment on model cancellation, safe lease release, 30-second grace expiry, and structured logs that contain correlation IDs but never prompts/model text/tool or outbox payloads.

```rust
#[tokio::test(start_paused = true)]
async fn unexpected_component_exit_stops_siblings_and_returns_error() -> Result<()> {
    let fixture = SupervisorFixture::dispatcher_exits();
    let result = fixture.run().await;
    assert!(result.unwrap_err().to_string().contains("dispatcher exited"));
    assert!(fixture.ipc_cancelled());
    assert!(fixture.outbox_cancelled());
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify failure**

Run separately:

```bash
rtk cargo test runtime::supervisor::tests
rtk cargo test runtime::sqlite::recovery::tests
```

Expected: FAIL because production supervisor and recovery do not exist.

- [ ] **Step 3: Implement startup recovery and readiness**

Implement the exact startup order from the spec: validate paths, acquire lock, migrate, import legacy auth once, verify configured actor enabled, remove stale socket, bind securely, recover expired claims/orphan attempts, then start components and log readiness. Recovery returns counts for structured logs.

- [ ] **Step 4: Compose the production runner in `app.rs`**

Load `RuntimeActor`, build `OpenAiClient`, actor-scoped `ToolRegistry`, `ArtifactManager`, streaming `ActorRunner`, `ActorDispatcher`, `StreamHub`, `OutboxWorker`, and `LocalIpcServer`. Remove `run_once*`, session composition used only by legacy CLI, the `interfaces::telegram` module export, and the polling Telegram implementation files; retain reusable provider/tool builders needed by the durable runner.

- [ ] **Step 5: Implement graceful shutdown**

On SIGINT/SIGTERM stop accepts/acquisitions, broadcast `ServerShuttingDown`, wait up to 30 seconds for quanta/transmissions/ACK, cancel unfinished model futures without failure increments, release safe actor leases, mark sent-unacked bundles retryable, leave running tools for unknown recovery, remove socket while holding lock, then exit.

- [ ] **Step 6: Add redacted structured observability**

Define a serializable `RuntimeLogEvent` containing optional component, actor ID, work item ID, run ID, request ID, attempt ID, outbox ID, delivery ID, lease generation, transition, latency, and error class. `StderrRuntimeLogger` writes one JSON object per line. Constructors accept typed IDs and redacted error classes only; they expose no prompt/model/tool/outbox payload parameters. Emit startup paths/schema/actor/recovery counts, readiness, terminal failures, and unknown outcomes.

- [ ] **Step 7: Run supervisor and app tests**

Run separately:

```bash
rtk cargo test runtime::supervisor::tests
rtk cargo test runtime::sqlite::recovery::tests
rtk cargo test app::tests
rtk cargo test interfaces::cli::tests
```

Expected: PASS and no code path from `CliCommand::Submit` constructs an `Agent`.

- [ ] **Step 8: Commit**

```bash
rtk git add src/main.rs src/app.rs src/interfaces src/runtime.rs src/runtime/supervisor.rs src/runtime/observability.rs src/runtime/sqlite.rs src/runtime/sqlite/recovery.rs
rtk git commit -m "feat(runtime): supervise the serve process"
```

### Task 13: Installer, End-to-End Recovery, and Final Verification

**Files:**
- Modify: `scripts/install.sh`
- Create: `tests/install_script.rs`
- Create: `tests/serve_runtime.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: clean-install local owner, `codrik serve` systemd/launchd definitions, and complete acceptance coverage.
- Consumes: production binary and all prior tasks.

- [ ] **Step 1: Write failing installer tests**

Add textual/golden tests proving systemd `ExecStart=<bin> serve`, launchd arguments contain only `serve`, old gateway services are replaced, runtime config is written, existing `users.json` remains byte-for-byte unchanged, existing authorization requires an explicit actor-ID prompt, and clean absent/empty authorization creates `actor:local:owner` with `tools: ["*"]` only.

- [ ] **Step 2: Run installer tests and verify failure**

Run: `rtk cargo test --test install_script`

Expected: FAIL because installer still writes gateway services and no runtime owner/config.

- [ ] **Step 3: Update installer service and bootstrap behavior**

Rename service helpers and labels from gateway-specific names to `codrik.service` / `com.suinly.codrik`. Generate `codrik serve` foreground services. On clean interactive install only, create mode-0700 runtime directory, mode-0600 `users.json` with local owner, and `runtime.actor_id: actor:local:owner`. When authorization already exists, ask for and write an explicit actor ID while leaving `users.json` byte-for-byte unchanged; when keeping an old config without `runtime.actor_id`, print the exact YAML the user must add and do not start a broken service. Remove polling gateway prompts and service commands.

- [ ] **Step 4: Add real-socket acceptance tests**

In `tests/serve_runtime.rs`, use a temporary runtime root, real Unix socket, on-disk SQLite, scripted streaming LLM, deterministic clock, and spawned supervisor. Cover all 17 acceptance scenarios from the design spec, including duplicate Submit, conflict, disconnect/resume, multi-request bundle, crash boundaries, lock exclusion, SIGTERM, orphan tools, fifth failure, disabled actor, pre-Accepted race, more than 32 deliveries, multi-frame text, cancel, and malicious slow clients.

- [ ] **Step 5: Run acceptance tests**

Run: `rtk cargo test --test serve_runtime -- --nocapture`

Expected: PASS with every spawned daemon and socket cleaned up by test guards.

- [ ] **Step 6: Update user documentation**

Document only:

```text
codrik serve
codrik "question"
codrik resume <request-id>
codrik cancel <request-id>
codrik update
```

Explain foreground service ownership, socket/config defaults, Ctrl-C disconnect semantics, at-least-once local display after lost ACK, and possible repeated LLM call after an ambiguous crash. Remove session, `--stream`, and `gateway telegram` instructions.

- [ ] **Step 7: Run complete repository verification**

Run in this order:

```bash
rtk cargo fmt --check
rtk cargo test
rtk cargo check
rtk cargo clippy --all-targets --all-features
rtk git diff --check
```

Expected: every command exits 0; test output has zero failures.

- [ ] **Step 8: Perform the manual foreground transcript**

With a temporary config and scripted/local provider, record:

```text
$ rtk codrik serve
runtime ready actor=actor:local:owner ...

$ rtk codrik "hello"
Agent: <streamed text>

^C
Resume with: codrik resume <request-id>

$ rtk codrik resume <request-id>
Agent: <authoritative final text>
```

Also verify a second `codrik serve` fails without deleting the first socket and `codrik cancel <request-id>` produces a terminal cancellation bundle.

- [ ] **Step 9: Commit**

```bash
rtk git add scripts/install.sh tests/install_script.rs tests/serve_runtime.rs README.md
rtk git commit -m "feat(runtime): ship the serve workflow"
```

## Final Review Checklist

- [ ] Every supported CLI prompt path uses IPC and fails clearly without the daemon.
- [ ] No legacy session or polling-gateway command remains.
- [ ] Same-UID peer checks and unsafe-parent checks run before request parsing.
- [ ] Submit/Resume cannot race a false missing request.
- [ ] StreamHub never blocks or fails the runner and emits one guaranteed gap.
- [ ] Every terminal local request has exactly one immutable result bundle.
- [ ] Bundle ACK is exact, route-scoped, idempotent, and at-least-once.
- [ ] Managed artifacts cannot be referenced before durable bytes or deleted by concurrent GC.
- [ ] V1 work without local routes cannot resume or deliver silently.
- [ ] Recoverable failures persist across restart and terminalize on the fifth failure.
- [ ] Graceful shutdown preserves all ambiguous external outcomes conservatively.
- [ ] Installer launches only `codrik serve` and never rewrites existing authorization.
- [ ] Full automated verification and the manual transcript pass.
