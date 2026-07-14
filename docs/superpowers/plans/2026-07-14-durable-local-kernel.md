# Durable Local Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable local vertical slice that durably accepts authenticated actor input, serializes work with fenced SQLite leases, checkpoints model/tool progress, and emits final output only through an outbox.

**Architecture:** Add a `runtime` module whose coarse-grained transaction traits own the normative operations from the design; the final `RuntimeStore` is their composite bound. `SqliteRuntimeStore` runs bundled SQLite on a dedicated `tokio-rusqlite` thread, while `ActorRunner` consumes one audience-compatible work item at a time through fenced leases. Existing one-shot and Telegram paths remain compatibility paths; only the new kernel may claim crash-safe execution.

**Tech Stack:** Rust 2024, Tokio 1.52, async-trait 0.1, serde/serde_json, tokio-rusqlite 0.7.0 with bundled SQLite, UUID 1.23 with v4 generation, existing `LlmClient` and `ToolExecutor` abstractions, `cargo test`.

## Global Constraints

- Run every shell command through `rtk`.
- Follow test-driven development: add one focused failing behavior, verify RED, implement the minimum, then verify GREEN.
- SQLite is authoritative for new runtime actors, identities, events, work items, runs, attempts, recent context, leases, and outbox records.
- Existing session files are not read, imported, changed, or deleted.
- Existing `users.json` is imported once for authorization and remains untouched afterward.
- One actor may own multiple waiting work items, but only one fenced decision step may execute for that actor.
- Every runner mutation must validate `(actor_id, lease_generation)` in the same transaction.
- Mailbox order comes from a per-actor database sequence, never timestamps.
- Only audience-compatible events may attach to the same run.
- Tool execution commits `prepared -> running` before invocation; an orphaned `running` attempt becomes `outcome_unknown`.
- The durable path never calls `LlmStreamSink` or sends a file directly; it writes typed outbox intents.
- Telegram webhooks, recurring schedules, cross-channel linking, long-term summaries, and removal of legacy execution paths are outside this plan.

## File Structure

- `src/runtime.rs`: module exports and runtime-wide traits.
- `src/runtime/model.rs`: typed IDs, clocks, audiences, event/work/run/attempt/outbox domain values.
- `src/runtime/store.rs`: coarse transaction traits, their composite bound, and request/result structs.
- `src/runtime/sqlite.rs`: connection setup, migration runner, and shared call helper.
- `src/runtime/sqlite/ingress.rs`: actor import and ingress transaction implementation.
- `src/runtime/sqlite/dispatch.rs`: fenced acquisition, attachment, cancellation lookup, and lease release.
- `src/runtime/sqlite/checkpoint.rs`: message checkpoints, attempt transitions, finalization, and recovery.
- `src/runtime/sqlite/outbox.rs`: outbox inspection/claim transitions needed by the fake delivery slice.
- `src/runtime/migrations/0001_runtime.sql`: first authoritative schema and constraints.
- `src/runtime/signals.rs`: in-process wake/cancel notification after durable ingress.
- `src/runtime/runner.rs`: bounded actor decision loop and durable tool orchestration.
- `src/runtime/service.rs`: authenticated local ingress, dispatch-once, and fake-delivery-facing API.
- `src/auth.rs`: read-only legacy authorization snapshot export.
- `src/agent/message.rs`: serde support for durable recent context.
- `src/agent/tool.rs`: tool execution capabilities and durable call context.
- `src/tools.rs` plus tool modules: conservative capability declarations and new call signature.
- `tests/durable_local_kernel.rs`: cross-component recovery and end-to-end behavior.

---

### Task 1: Runtime Domain Model and SQLite Schema

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/main.rs`
- Create: `src/runtime.rs`
- Create: `src/runtime/model.rs`
- Create: `src/runtime/store.rs`
- Create: `src/runtime/sqlite.rs`
- Create: `src/runtime/migrations/0001_runtime.sql`

**Interfaces:**
- Produces: `ActorId`, `EventId`, `WorkItemId`, `RunId`, `AttemptId`, `OutboxId`, `Timestamp`.
- Produces: `Audience`, `EventKind`, `EventState`, `WorkItemState`, `RunState`, `AttemptState`, `OutboxState`.
- Produces: shared transaction request/result structs and `SqliteRuntimeStore::open` / `open_in_memory`.

- [ ] **Step 1: Add a failing schema test**

Create `src/runtime/sqlite.rs` with the test first:

```rust
#[cfg(test)]
mod tests {
    use super::SqliteRuntimeStore;

    #[tokio::test]
    async fn migration_creates_runtime_tables_and_enables_foreign_keys() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let (foreign_keys, tables) = store.schema_probe().await.unwrap();

        assert!(foreign_keys);
        for table in [
            "actors", "identities", "events", "work_items", "actor_leases",
            "runs", "run_events", "recent_messages", "tool_attempts", "outbox",
        ] {
            assert!(tables.contains(&table.to_string()), "missing {table}");
        }
    }
}
```

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test runtime::sqlite::tests::migration_creates_runtime_tables_and_enables_foreign_keys`

Expected: compilation fails because the runtime module and SQLite dependencies do not exist.

- [ ] **Step 3: Add dependencies and module roots**

Add to `Cargo.toml`:

```toml
tokio-rusqlite = { version = "0.7.0", features = ["bundled"] }
uuid = { version = "1.23.4", features = ["v4"] }
```

Add `mod runtime;` to `src/main.rs`. In `src/runtime.rs` declare:

```rust
pub mod model;
pub mod sqlite;
pub mod store;
```

- [ ] **Step 4: Define typed runtime values**

In `src/runtime/model.rs`, define string ID newtypes with a local macro and the initial enums:

```rust
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(String);

        impl $name {
            pub fn new() -> Self { Self(Uuid::new_v4().to_string()) }
            pub fn from_string(value: impl Into<String>) -> Self { Self(value.into()) }
            pub fn as_str(&self) -> &str { &self.0 }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(ActorId);
id_type!(EventId);
id_type!(WorkItemId);
id_type!(RunId);
id_type!(AttemptId);
id_type!(OutboxId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn plus_millis(self, millis: i64) -> Self { Self(self.0 + millis) }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Audience {
    ActorPrivate,
    ConversationScoped { address: String },
    Shareable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind { UserMessage, CancelRequested, ExternalCompletion }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventState { Pending, Processing, Completed, Cancelled, FailedTerminal, Blocked }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkItemState { Ready, Waiting, Completed, Cancelled, FailedTerminal, BlockedUnknownOutcome, WaitingForDecision }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState { Active, Completed, Cancelled, FailedTerminal }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptState { Prepared, Running, Succeeded, FailedKnown, OutcomeUnknown, CancelledKnown, WaitingForDecision }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxState { Pending, Delivering, Delivered, FailedRetryable, FailedTerminal, OutcomeUnknown, AcknowledgedDuplicate }
```

- [ ] **Step 5: Define shared transaction values**

In `src/runtime/store.rs`, define requests around transaction boundaries, not row CRUD:

```rust
use crate::agent::message::Message;
use super::model::*;

#[derive(Clone, Debug)]
pub struct ActorLease {
    pub actor_id: ActorId,
    pub owner_id: String,
    pub generation: i64,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug)]
pub struct AttachedRun {
    pub lease: ActorLease,
    pub work_item_id: WorkItemId,
    pub run_id: RunId,
    pub observed_sequence: i64,
    pub source_event_ids: Vec<EventId>,
    pub audience: Audience,
    pub messages: Vec<Message>,
}

```

Each later task adds one complete coarse transaction trait together with its SQLite implementation. Task 8 combines them into `RuntimeStore`; do not add methods before the task that implements them.

- [ ] **Step 6: Create the first migration**

Create `src/runtime/migrations/0001_runtime.sql` with `STRICT` tables and constraints. Use these columns and unique keys exactly:

```sql
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runtime_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE actors (
    id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    tools_json TEXT NOT NULL,
    next_mailbox_sequence INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE identities (
    provider TEXT NOT NULL,
    subject TEXT NOT NULL,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    username TEXT,
    PRIMARY KEY (provider, subject)
) STRICT;

CREATE TABLE work_items (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('interactive', 'external')),
    audience_kind TEXT NOT NULL CHECK (audience_kind IN ('actor_private', 'conversation_scoped', 'shareable')),
    audience_address TEXT,
    state TEXT NOT NULL CHECK (state IN ('ready', 'waiting', 'completed', 'cancelled', 'failed_terminal', 'blocked_unknown_outcome', 'waiting_for_decision')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK ((audience_kind = 'conversation_scoped') = (audience_address IS NOT NULL))
) STRICT;

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT REFERENCES work_items(id),
    mailbox_sequence INTEGER NOT NULL,
    gateway TEXT NOT NULL,
    external_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    audience_kind TEXT NOT NULL,
    audience_address TEXT,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'completed', 'cancelled', 'failed_terminal', 'blocked')),
    run_id TEXT REFERENCES runs(id),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (actor_id, mailbox_sequence),
    UNIQUE (gateway, external_id),
    CHECK ((audience_kind = 'conversation_scoped') = (audience_address IS NOT NULL))
) STRICT;

CREATE TABLE actor_leases (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    owner_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
) STRICT;

CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('active', 'completed', 'cancelled', 'failed_terminal')),
    lease_generation INTEGER NOT NULL,
    observed_sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX one_active_run_per_work_item
ON runs(work_item_id) WHERE state = 'active';

CREATE TABLE run_events (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id) ON DELETE CASCADE,
    incorporated INTEGER NOT NULL DEFAULT 0 CHECK (incorporated IN (0, 1)),
    PRIMARY KEY (run_id, event_id)
) STRICT;

CREATE TABLE recent_messages (
    id INTEGER PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    audience_kind TEXT NOT NULL,
    audience_address TEXT,
    message_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE tool_attempts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'running', 'succeeded', 'failed_known', 'outcome_unknown', 'cancelled_known', 'waiting_for_decision')),
    outcome_json TEXT,
    observation_checkpointed INTEGER NOT NULL DEFAULT 0 CHECK (observation_checkpointed IN (0, 1)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (run_id, tool_call_id)
) STRICT;

CREATE TABLE outbox (
    id TEXT PRIMARY KEY,
    intent_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    intent_class TEXT NOT NULL,
    audience_kind TEXT NOT NULL,
    audience_address TEXT,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'delivering', 'delivered', 'failed_retryable', 'failed_terminal', 'outcome_unknown', 'acknowledged_duplicate')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    claim_owner TEXT,
    claim_expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE INDEX ready_events ON events(actor_id, state, kind, mailbox_sequence);
CREATE INDEX ready_outbox ON outbox(state, created_at);
```

- [ ] **Step 7: Implement connection setup and migration**

Implement `SqliteRuntimeStore` using `tokio_rusqlite::Connection`. On every open, execute `PRAGMA foreign_keys = ON`, `PRAGMA busy_timeout = 5000`, and the included migration. File databases additionally use `PRAGMA journal_mode = WAL`.

```rust
#[derive(Clone)]
pub struct SqliteRuntimeStore {
    connection: tokio_rusqlite::Connection,
}

impl SqliteRuntimeStore {
    pub async fn open(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let connection = tokio_rusqlite::Connection::open(path).await?;
        Self::initialize(connection, true).await
    }

    pub async fn open_in_memory() -> anyhow::Result<Self> {
        let connection = tokio_rusqlite::Connection::open_in_memory().await?;
        Self::initialize(connection, false).await
    }
}
```

- [ ] **Step 8: Verify GREEN and commit**

Run: `rtk cargo test runtime::sqlite::tests`

Expected: the schema test passes.

Commit:

```bash
rtk git add Cargo.toml Cargo.lock src/main.rs src/runtime.rs src/runtime
rtk git commit -m "feat(runtime): add durable SQLite schema"
```

---

### Task 2: Legacy Authorization Import and Identity Resolution

**Files:**
- Modify: `src/auth.rs`
- Modify: `src/runtime/store.rs`
- Create: `src/runtime/sqlite/ingress.rs`
- Modify: `src/runtime/sqlite.rs`

**Interfaces:**
- Produces: `LegacyAuthorizationSnapshot` and `AuthorizationStore::snapshot()`.
- Produces: `RuntimeAuthorizationStore::import_legacy_authorization` and `resolve_identity`.
- Guarantees: import marker and actors/identities/tools commit atomically and repeated import is a no-op.

- [ ] **Step 1: Write failing auth import tests**

Add in `src/runtime/sqlite/ingress.rs`:

```rust
#[tokio::test]
async fn legacy_authorization_import_is_atomic_and_idempotent() {
    let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
    let snapshot = LegacyAuthorizationSnapshot {
        version: 1,
        actors: vec![LegacyActor {
            id: "actor:telegram:123".into(),
            enabled: true,
            tools: vec!["*".into(), "bash".into()],
            identities: vec![LegacyIdentity {
                provider: "telegram".into(),
                subject: "123".into(),
                username: Some("owner".into()),
            }],
        }],
    };

    assert_eq!(store.import_legacy_authorization(snapshot.clone(), Timestamp(10)).await.unwrap(), ImportOutcome::Imported);
    assert_eq!(store.import_legacy_authorization(snapshot, Timestamp(20)).await.unwrap(), ImportOutcome::AlreadyImported);
    assert_eq!(store.resolve_identity("telegram", "123").await.unwrap().unwrap().tools, vec!["*", "bash"]);
}
```

Add a second test with two actors claiming the same `(provider, subject)` and expect the entire import to fail with no `legacy_auth_imported` marker.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test runtime::sqlite::ingress::tests::legacy_authorization_import`

Expected: compilation fails because snapshot and import interfaces do not exist.

- [ ] **Step 3: Expose a read-only legacy snapshot**

In `src/auth.rs`, add crate-private DTOs that do not expose mutable file storage:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LegacyAuthorizationSnapshot {
    pub version: u32,
    pub actors: Vec<LegacyActor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LegacyActor {
    pub id: String,
    pub enabled: bool,
    pub tools: Vec<String>,
    pub identities: Vec<LegacyIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LegacyIdentity {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}

impl AuthorizationStore {
    pub(crate) async fn snapshot(&self) -> anyhow::Result<LegacyAuthorizationSnapshot> {
        let users = self.read_users().await?;
        Ok(LegacyAuthorizationSnapshot::from(users))
    }
}
```

Conversion clones existing actor IDs, enabled state, tool grants, and every stored identity. It never writes `users.json`.

- [ ] **Step 4: Implement the single import transaction**

Define the complete transaction trait with its DTOs:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeActor {
    pub id: ActorId,
    pub enabled: bool,
    pub tools: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportOutcome { Imported, AlreadyImported }

#[async_trait]
pub trait RuntimeAuthorizationStore: Send + Sync {
    async fn import_legacy_authorization(&self, snapshot: LegacyAuthorizationSnapshot, now: Timestamp) -> Result<ImportOutcome>;
    async fn resolve_identity(&self, provider: &str, subject: &str) -> Result<Option<RuntimeActor>>;
}
```

In one `TransactionBehavior::Immediate` transaction:

1. return `AlreadyImported` when `runtime_metadata.key = 'legacy_auth_imported'` exists;
2. insert every actor and identity;
3. serialize deduplicated tools into `tools_json`;
4. insert the marker only after all rows succeed;
5. commit.

- [ ] **Step 5: Verify GREEN and commit**

Run: `rtk cargo test runtime::sqlite::ingress::tests::legacy_authorization_import`

Expected: atomic, duplicate-identity, and idempotency tests pass.

Commit:

```bash
rtk git add src/auth.rs src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/ingress.rs
rtk git commit -m "feat(runtime): import actor authorization"
```

---

### Task 3: Durable Ingress and Mailbox Sequencing

**Files:**
- Modify: `src/runtime/model.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/ingress.rs`

**Interfaces:**
- Produces: `NewInboundEvent`, `IngressOutcome`, and `IngressStore::ingest`.
- Guarantees: identity resolution, deduplication, per-actor sequence allocation, event insertion, and work-item selection are one transaction.

- [ ] **Step 1: Write failing ingress tests**

Add tests that import one actor, then submit these inputs:

```rust
let first = NewInboundEvent::text(
    "local", "event-1", "local", "owner", Audience::ActorPrivate, "first",
);
let duplicate = first.clone();
let second = NewInboundEvent::text(
    "local", "event-2", "local", "owner", Audience::ActorPrivate, "second",
);

assert_eq!(store.ingest(first, Timestamp(100)).await.unwrap().sequence(), Some(1));
assert!(matches!(store.ingest(duplicate, Timestamp(101)).await.unwrap(), IngressOutcome::Duplicate { sequence: 1, .. }));
assert_eq!(store.ingest(second, Timestamp(102)).await.unwrap().sequence(), Some(2));
```

Add tests proving an unknown/disabled identity is rejected without incrementing the actor sequence, and a group audience receives a different work item from actor-private input.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test runtime::sqlite::ingress::tests::ingress`

Expected: compilation fails because ingress types and transaction are missing.

- [ ] **Step 3: Add ingress request types**

In `src/runtime/store.rs`:

```rust
#[derive(Clone, Debug)]
pub struct NewInboundEvent {
    pub gateway: String,
    pub external_id: String,
    pub identity_provider: String,
    pub identity_subject: String,
    pub kind: EventKind,
    pub audience: Audience,
    pub payload_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngressOutcome {
    Accepted { event_id: EventId, work_item_id: WorkItemId, sequence: i64 },
    Duplicate { event_id: EventId, sequence: i64 },
    Unauthorized,
}

impl IngressOutcome {
    pub fn sequence(&self) -> Option<i64> {
        match self {
            Self::Accepted { sequence, .. } | Self::Duplicate { sequence, .. } => Some(*sequence),
            Self::Unauthorized => None,
        }
    }
}

#[async_trait]
pub trait IngressStore: Send + Sync {
    async fn ingest(&self, event: NewInboundEvent, now: Timestamp) -> Result<IngressOutcome>;
}
```

`NewInboundEvent::text` serializes `{"type":"text","text":...}` with `serde_json`, rather than interpolating JSON manually.

- [ ] **Step 4: Implement ingress as one immediate transaction**

The implementation must:

1. check `(gateway, external_id)` first and return the stored ID/sequence on duplicate;
2. resolve an enabled identity;
3. choose the current ready/waiting interactive work item with an exactly equal audience, or create one;
4. increment `actors.next_mailbox_sequence` with `UPDATE ... RETURNING`;
5. insert the pending event with that sequence;
6. mark the work item `ready` unless it is terminal;
7. commit.

Use `Audience::ActorPrivate` as `('actor_private', NULL)` and require `conversation_scoped` to have a non-empty address.

- [ ] **Step 5: Verify GREEN and commit**

Run: `rtk cargo test runtime::sqlite::ingress::tests`

Expected: sequencing, deduplication, authorization, and audience work-item tests pass.

Commit:

```bash
rtk git add src/runtime/model.rs src/runtime/store.rs src/runtime/sqlite/ingress.rs
rtk git commit -m "feat(runtime): persist sequenced inbound events"
```

---

### Task 4: Fenced Actor Acquisition and Audience-Compatible Attachment

**Files:**
- Modify: `src/runtime/store.rs`
- Create: `src/runtime/sqlite/dispatch.rs`
- Modify: `src/runtime/sqlite.rs`

**Interfaces:**
- Implements: `acquire_ready_actor`, `renew_lease`, `attach_next_run`, and `release_lease`.
- Produces: `StaleLease` as a typed transaction error.
- Guarantees: generation fencing, one selected work item, compatible event attachment, and preserved attachment after expiry.

- [ ] **Step 1: Write failing fencing tests**

Create two owners against one store:

```rust
let lease_1 = store.acquire_ready_actor("worker-1", Timestamp(100), Timestamp(110)).await.unwrap().unwrap();
let lease_2 = store.acquire_ready_actor("worker-2", Timestamp(111), Timestamp(121)).await.unwrap().unwrap();

assert_eq!(lease_2.generation, lease_1.generation + 1);
let error = store.attach_next_run(&lease_1, 8, Timestamp(112)).await.unwrap_err();
assert!(error.downcast_ref::<StaleLease>().is_some());
```

Add an attachment test with two actor-private events and one group event. The first attached run contains only actor-private source IDs. After release/reacquisition the group event attaches to a different work item/run. Add a crash test proving an active run and its `run_events` remain attached after lease expiry.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test runtime::sqlite::dispatch::tests`

Expected: fails because dispatch implementation and stale-fence checks do not exist.

- [ ] **Step 3: Implement fenced acquisition**

Define the typed error in `src/runtime/store.rs`:

```rust
#[derive(Debug)]
pub struct StaleLease;

impl std::fmt::Display for StaleLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("stale actor lease") }
}

impl std::error::Error for StaleLease {}

#[async_trait]
pub trait DispatchStore: Send + Sync {
    async fn acquire_ready_actor(&self, owner: &str, now: Timestamp, lease_until: Timestamp) -> Result<Option<ActorLease>>;
    async fn renew_lease(&self, lease: &ActorLease, now: Timestamp, lease_until: Timestamp) -> Result<ActorLease>;
    async fn attach_next_run(&self, lease: &ActorLease, max_events: usize, now: Timestamp) -> Result<Option<AttachedRun>>;
    async fn release_lease(&self, lease: &ActorLease) -> Result<()>;
}
```

Use an immediate transaction and deterministic actor selection ordered by the oldest pending event sequence, then actor ID. For an absent/expired lease, upsert:

```sql
INSERT INTO actor_leases(actor_id, generation, owner_id, expires_at)
VALUES (?1, 1, ?2, ?3)
ON CONFLICT(actor_id) DO UPDATE SET
  generation = actor_leases.generation + 1,
  owner_id = excluded.owner_id,
  expires_at = excluded.expires_at
WHERE actor_leases.expires_at <= ?4
RETURNING generation
```

An unexpired lease owned by another worker makes that actor ineligible. Renewal by the same owner keeps its generation.

- [ ] **Step 4: Implement attachment and fence verification**

At the start of every runner transaction, execute:

```sql
SELECT 1 FROM actor_leases
WHERE actor_id = ?1 AND owner_id = ?2 AND generation = ?3 AND expires_at > ?4
```

Return `StaleLease` unless exactly one row exists. Resume an existing active run before creating another run for that work item. Otherwise select one ready work item and at most `max_events` pending events with the same encoded audience, ordered by cancellation priority, user priority, then mailbox sequence. Insert `run_events`, set those events to `processing`, and update only that run's high-water mark.

- [ ] **Step 5: Implement guarded release**

`renew_lease` updates `expires_at` only for the exact current `(actor_id, owner_id, generation)` row and returns `StaleLease` otherwise. `release_lease` deletes only that same exact row. Releasing a stale lease is a no-op; it must never remove a newer lease.

- [ ] **Step 6: Verify GREEN and commit**

Run: `rtk cargo test runtime::sqlite::dispatch::tests`

Expected: fencing, compatibility, run resumption, and guarded-release tests pass.

Commit:

```bash
rtk git add src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/dispatch.rs
rtk git commit -m "feat(runtime): fence actor dispatch"
```

---

### Task 5: Fenced Message Checkpoints, Finalization, and Outbox

**Files:**
- Modify: `src/agent/message.rs`
- Modify: `src/runtime/store.rs`
- Create: `src/runtime/sqlite/checkpoint.rs`
- Create: `src/runtime/sqlite/outbox.rs`
- Modify: `src/runtime/sqlite.rs`

**Interfaces:**
- Produces: serde round-trip for `Message`, `Role`, `MessagePart`, and `Attachment`.
- Implements: recent context, checkpoint, finalization, and outbox inspection/claim transitions.
- Guarantees: finalization fails on newer compatible input and completes only incorporated source events.

- [ ] **Step 1: Write failing serialization and finalization tests**

In `src/agent/message.rs`, add a serde round-trip test covering assistant tool calls and an attachment.

In `checkpoint.rs`, attach a run, checkpoint a user and assistant-tool-call message, ingest a newer compatible user event, and assert:

```rust
assert_eq!(
    store.finalize_run(command, Timestamp(200)).await.unwrap(),
    FinalizeOutcome::Preempted { newest_sequence: 2 }
);
assert!(store.pending_outbox().await.unwrap().is_empty());
```

Then attach the newer event, mark all current `run_events` incorporated, finalize with a text intent, and assert one unique outbox row plus completed source events. Add a stale-lease finalization test that leaves every row unchanged.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test runtime::sqlite::checkpoint::tests`

Expected: compilation fails because messages are not serializable and checkpoint/finalize methods are absent.

- [ ] **Step 3: Add serde derives without changing wire behavior**

Derive `Serialize`/`Deserialize` on `Message`, `Role`, `MessagePart`, `Attachment`, and any nested local message types. Preserve existing enum names with explicit `#[serde(tag = "type", rename_all = "snake_case")]` on `MessagePart`; do not change LLM adapter encoding.

- [ ] **Step 4: Implement fenced checkpoints**

`checkpoint_run` validates the fence, verifies each message belongs to the run's audience, serializes it, inserts it into `recent_messages`, and marks only the corresponding `run_events.incorporated = 1`. Use an explicit request containing `incorporated_event_ids` rather than inferring incorporation from all attached events:

```rust
pub struct CheckpointRun {
    pub run: AttachedRun,
    pub incorporated_event_ids: Vec<EventId>,
    pub checkpointed_attempt_ids: Vec<AttemptId>,
    pub messages: Vec<Message>,
}
```

Define and implement:

```rust
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn checkpoint_run(&self, command: CheckpointRun, now: Timestamp) -> Result<()>;
    async fn finalize_run(&self, command: FinalizeRun, now: Timestamp) -> Result<FinalizeOutcome>;
}
```

- [ ] **Step 5: Implement finalization atomically**

In one fenced transaction:

1. query for a newer compatible `CancelRequested` or `UserMessage` beyond `observed_sequence`;
2. return `Preempted` without mutations when found;
3. require every source event listed for completion to have `incorporated = 1`;
4. insert final recent messages;
5. insert outbox intents with unique `intent_key` using `INSERT ... ON CONFLICT DO NOTHING`;
6. complete only incorporated run events;
7. complete the run and work item;
8. commit.

Define typed outbox payloads:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutboxPayload {
    Text { text: String },
    File { path: PathBuf, display_name: String, media_type: String, caption: Option<String> },
}

#[derive(Clone, Debug)]
pub struct NewOutboxIntent {
    pub id: OutboxId,
    pub intent_key: String,
    pub intent_class: String,
    pub audience: Audience,
    pub payload: OutboxPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxRecord {
    pub id: OutboxId,
    pub intent_key: String,
    pub payload: OutboxPayload,
    pub state: OutboxState,
    pub attempt_count: i64,
}

#[derive(Clone, Debug)]
pub struct FinalizeRun {
    pub run: AttachedRun,
    pub incorporated_event_ids: Vec<EventId>,
    pub final_messages: Vec<Message>,
    pub outbox: Vec<NewOutboxIntent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizeOutcome {
    Completed,
    Preempted { newest_sequence: i64 },
}
```

- [ ] **Step 6: Add fake-delivery outbox methods**

Add these methods for the local slice:

```rust
#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn pending_outbox(&self) -> Result<Vec<OutboxRecord>>;
    async fn mark_outbox_delivered(&self, id: &OutboxId, now: Timestamp) -> Result<()>;
    async fn mark_outbox_failed_terminal(&self, id: &OutboxId, error: &str, now: Timestamp) -> Result<()>;
}
```

Every mutation checks the current outbox state and increments `attempt_count`; leave gateway-specific unknown retry behavior for the Telegram plan.

- [ ] **Step 7: Verify GREEN and commit**

Run: `rtk cargo test runtime::sqlite::checkpoint::tests`

Run: `rtk cargo test runtime::sqlite::outbox::tests`

Expected: serialization, stale fence, preemption, source-only completion, outbox uniqueness, and delivery transition tests pass.

Commit:

```bash
rtk git add src/agent/message.rs src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/checkpoint.rs src/runtime/sqlite/outbox.rs
rtk git commit -m "feat(runtime): checkpoint runs into outbox"
```

---

### Task 6: Durable Cancellation Signals

**Files:**
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/dispatch.rs`
- Create: `src/runtime/signals.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produces: `ActorSignals::subscribe(actor_id)` and `notify(actor_id, sequence)`.
- Produces: `RuntimeStore::newer_control_event`.
- Guarantees: SQLite remains authoritative; in-process notification only shortens response latency.

- [ ] **Step 1: Write failing signal and durable cancellation tests**

Test that a subscriber receives a higher sequence after durable ingress. Drop the signal registry, recreate it, and prove `newer_control_event(actor, observed_sequence)` still discovers the cancellation from SQLite.

```rust
assert_eq!(
    store.newer_control_event(&lease, 1, Timestamp(300)).await.unwrap(),
    Some(ControlEvent { sequence: 2, kind: EventKind::CancelRequested })
);
```

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test runtime::signals::tests`

Expected: compilation fails because signals and durable control lookup do not exist.

- [ ] **Step 3: Implement actor watch channels**

Use `tokio::sync::watch` keyed by `ActorId` behind a short-held mutex:

```rust
#[derive(Clone, Default)]
pub struct ActorSignals {
    channels: Arc<Mutex<HashMap<ActorId, watch::Sender<i64>>>>,
}

impl ActorSignals {
    pub async fn notify(&self, actor: &ActorId, sequence: i64) {
        let sender = self.sender(actor).await;
        sender.send_replace(sequence);
    }
}
```

Subscribers always query SQLite after wake-up; sequence delivery is a hint, not the source of truth.

- [ ] **Step 4: Implement fenced control lookup**

Add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlEvent {
    pub event_id: EventId,
    pub sequence: i64,
    pub kind: EventKind,
}

#[async_trait]
pub trait ControlStore: Send + Sync {
    async fn newer_control_event(&self, lease: &ActorLease, observed_sequence: i64, now: Timestamp) -> Result<Option<ControlEvent>>;
}
```

The query validates the lease and selects the smallest pending compatible control event above `observed_sequence`, ordering cancellation before user input. It does not change event state.

- [ ] **Step 5: Verify GREEN and commit**

Run: `rtk cargo test runtime::signals::tests`

Run: `rtk cargo test runtime::sqlite::dispatch::tests::durable_cancellation_survives_signal_loss`

Expected: both tests pass.

Commit:

```bash
rtk git add src/runtime.rs src/runtime/signals.rs src/runtime/store.rs src/runtime/sqlite/dispatch.rs
rtk git commit -m "feat(runtime): persist actor cancellation signals"
```

---

### Task 7: Tool Capability and Attempt Contract

**Files:**
- Modify: `src/agent/tool.rs`
- Modify: `src/tools.rs`
- Modify: `src/tools/bash.rs`
- Modify: `src/tools/bashkit.rs`
- Modify: `src/tools/datetime.rs`
- Modify: `src/tools/send_file.rs`
- Modify: `src/tools/skills.rs`
- Modify: `src/tools/web_browser.rs`
- Modify: `src/agent.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/checkpoint.rs`

**Interfaces:**
- Produces: `ToolCapabilities`, `ToolCallContext`, and context-aware `ToolExecutor::execute`.
- Produces: attempt prepare/start/finish/recover store methods.
- Preserves: existing one-shot behavior through `ToolCallContext::legacy()`.

- [ ] **Step 1: Write failing capability and attempt tests**

Add registry tests requiring conservative declarations:

```rust
assert!(registry.capabilities("datetime").unwrap().retry_safe);
assert!(registry.capabilities("send_file").unwrap().retry_safe);
assert!(!registry.capabilities("bash").unwrap().retry_safe);
assert!(!registry.capabilities("skills_update").unwrap().retry_safe);
```

In `checkpoint.rs`, test the durable boundary:

```rust
let attempt = store.prepare_attempt(&run, new_attempt, Timestamp(10)).await.unwrap();
assert_eq!(attempt.state, AttemptState::Prepared);
store.mark_attempt_running(&run, &attempt.id, Timestamp(11)).await.unwrap();
assert_eq!(store.recover_attempt(&attempt.id).await.unwrap(), AttemptRecovery::OutcomeUnknown);
```

Add a test proving a still-`Prepared` attempt returns `AttemptRecovery::MayInvoke`.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test tools::tests::tool_capabilities`

Run: `rtk cargo test runtime::sqlite::checkpoint::tests::attempt`

Expected: compilation fails because capability/context and attempt transitions do not exist.

- [ ] **Step 3: Define orthogonal capabilities and call context**

In `src/agent/tool.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCapabilities {
    pub retry_safe: bool,
    pub accepts_idempotency_key: bool,
    pub cancellable: bool,
    pub outcome_probe: bool,
    pub compensatable: bool,
    pub requires_approval: bool,
}

impl ToolCapabilities {
    pub fn conservative() -> Self { Self { retry_safe: false, accepts_idempotency_key: false, cancellable: false, outcome_probe: false, compensatable: false, requires_approval: false } }
    pub fn read_only() -> Self { Self { retry_safe: true, ..Self::conservative() } }
}

#[derive(Clone, Debug)]
pub struct ToolCallContext {
    pub attempt_id: String,
    pub authorized_tools: Vec<String>,
    pub cancellation: RunContext,
}

impl ToolCallContext {
    pub fn legacy(cancellation: RunContext) -> Self {
        Self { attempt_id: Uuid::new_v4().to_string(), authorized_tools: Vec::new(), cancellation }
    }
}

#[async_trait]
pub trait ToolExecutor {
    fn definitions(&self) -> Vec<Tool>;
    fn capabilities(&self, name: &str) -> Option<ToolCapabilities>;
    async fn execute(&self, name: &str, arguments: &str, context: &ToolCallContext) -> Result<ToolExecution>;
}
```

`ToolHandler` receives default `capabilities() -> ToolCapabilities::conservative()` and `execute_typed(arguments, context)`. Existing handlers may ignore the context but must receive it.

- [ ] **Step 4: Declare initial handler policies**

Use `read_only` for `datetime`, `send_file`, `skills_list`, and `skills_read`. Keep `bash`, `bashkit`, `skills_create`, `skills_update`, and `web_browser` conservative in this slice. Preserve privileged exposure independently from attempt capabilities.

- [ ] **Step 5: Adapt legacy Agent calls**

At each existing call in `src/agent.rs`, construct `ToolCallContext::legacy(context.clone())` with a generated attempt ID. Update every test-local `impl ToolExecutor` in `src/agent.rs` and the registry implementation in `src/tools.rs` to accept the context parameter and expose capabilities. This compatibility path is explicitly not crash-safe; it only preserves compilation and behavior while the durable runner uses persisted IDs.

- [ ] **Step 6: Implement attempt transactions**

Define the request/result types and extend `RuntimeStore`:

```rust
#[derive(Clone, Debug)]
pub struct NewToolAttempt {
    pub id: AttemptId,
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments_json: String,
    pub capabilities: ToolCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolAttempt {
    pub id: AttemptId,
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments_json: String,
    pub capabilities: ToolCapabilities,
    pub state: AttemptState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttemptOutcome {
    Succeeded { execution: ToolExecution },
    FailedKnown { message: String },
    CancelledKnown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptRecovery {
    MayInvoke,
    OutcomeUnknown,
    Terminal(AttemptOutcome),
}

#[async_trait]
pub trait ToolAttemptStore: Send + Sync {
    async fn prepare_attempt(&self, run: &AttachedRun, attempt: NewToolAttempt, now: Timestamp) -> Result<ToolAttempt>;
    async fn mark_attempt_running(&self, run: &AttachedRun, id: &AttemptId, now: Timestamp) -> Result<()>;
    async fn finish_attempt(&self, run: &AttachedRun, id: &AttemptId, outcome: AttemptOutcome, now: Timestamp) -> Result<()>;
    async fn recover_attempt(&self, id: &AttemptId) -> Result<AttemptRecovery>;
    async fn block_unknown_attempt(&self, run: &AttachedRun, id: &AttemptId, now: Timestamp) -> Result<()>;
    async fn unresolved_attempts(&self, run: &AttachedRun) -> Result<Vec<ToolAttempt>>;
}
```

Add serde derives to `ToolExecution`, `ToolArtifact`, and `FileArtifact` so successful outcomes can be persisted without changing their existing field representation.

Every mutation validates the actor fence. `prepare_attempt` is idempotent on `(run_id, tool_call_id)`. `mark_attempt_running` accepts only `Prepared`. `finish_attempt` accepts only `Running` and stores typed success/known failure/cancelled outcome. Recovery maps `Prepared -> MayInvoke`, atomically marks orphaned `Running` as `OutcomeUnknown`, and returns terminal attempts with their stored outcome. `block_unknown_attempt` moves `OutcomeUnknown -> WaitingForDecision` and the work item to `waiting_for_decision` in one fenced transaction. `unresolved_attempts` returns nonterminal attempts plus terminal attempts whose tool observation is not checkpointed, in creation order. The checkpoint that inserts a tool observation also sets `observation_checkpointed = 1` atomically.

- [ ] **Step 7: Verify GREEN and commit**

Run: `rtk cargo test agent::tests`

Run: `rtk cargo test tools::tests`

Run: `rtk cargo test runtime::sqlite::checkpoint::tests::attempt`

Expected: legacy agent tests still pass, capability declarations match, and attempt transition tests pass.

Commit:

```bash
rtk git add src/agent.rs src/agent/tool.rs src/tools.rs src/tools src/runtime/store.rs src/runtime/sqlite/checkpoint.rs
rtk git commit -m "refactor(tools): add durable attempt context"
```

---

### Task 8: Durable Actor Runner and Recent Context

**Files:**
- Create: `src/runtime/runner.rs`
- Modify: `src/runtime.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/checkpoint.rs`

**Interfaces:**
- Produces: `ActorRunner<L, T, S, C>` and `RunnerLimits`.
- Produces: `ContextStore` and the final composite `RuntimeStore` bound.
- Consumes: `LlmClient`, context-aware `ToolExecutor`, `RuntimeStore`, `Clock`, `ActorSignals`.
- Guarantees: one model/tool step at a time, persisted attempt boundaries, bounded quantum, preemption, and outbox-only final output.

- [ ] **Step 1: Write failing runner tests with scripted dependencies**

Add tests for:

1. one text event -> final LLM response -> one text outbox intent;
2. one tool call -> `Prepared` -> `Running` -> `Succeeded` -> tool observation checkpoint -> final response;
3. cancellation signal during a blocking LLM request cancels `RunContext` and does not create final outbox;
4. user event arriving before finalization produces `Preempted`, attaches the new sequence, and asks the model again;
5. six ready model/tool steps stop at the configured quantum and release the lease with the work item still ready.

Use this constructor in tests:

```rust
let runner = ActorRunner::new(
    store.clone(),
    scripted_llm,
    recording_tools,
    ManualClock::new(1_000),
    signals.clone(),
    RunnerLimits {
        max_events: 8,
        max_model_steps: 4,
        max_tool_steps: 8,
        recent_messages: 64,
        max_wall_time: Duration::from_secs(60),
        lease_duration: Duration::from_secs(30),
        heartbeat_interval: Duration::from_secs(10),
    },
);
```

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test runtime::runner::tests`

Expected: compilation fails because the runner, clock, and orchestration methods do not exist.

- [ ] **Step 3: Add an injectable clock**

In `src/runtime/model.rs`:

```rust
pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}

#[derive(Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock before epoch").as_millis() as i64)
    }
}
```

Add a test-only `ManualClock` backed by `Arc<AtomicI64>`:

```rust
#[cfg(test)]
#[derive(Clone)]
struct ManualClock(Arc<AtomicI64>);

#[cfg(test)]
impl ManualClock {
    fn new(now: i64) -> Self { Self(Arc::new(AtomicI64::new(now))) }
    fn advance(&self, millis: i64) { self.0.fetch_add(millis, Ordering::SeqCst); }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now(&self) -> Timestamp { Timestamp(self.0.load(Ordering::SeqCst)) }
}
```

- [ ] **Step 4: Implement audience-safe recent context**

Define:

```rust
#[async_trait]
pub trait ContextStore: Send + Sync {
    async fn load_recent_context(&self, actor: &ActorId, audience: &Audience, limit: usize) -> Result<Vec<Message>>;
}

pub trait RuntimeStore:
    DispatchStore + CheckpointStore + OutboxStore + ControlStore + ToolAttemptStore + ContextStore
{}

impl<T> RuntimeStore for T where
    T: DispatchStore + CheckpointStore + OutboxStore + ControlStore + ToolAttemptStore + ContextStore
{}
```

`load_recent_context` selects the newest `limit` messages where:

- target `ActorPrivate` includes `actor_private` and `shareable`;
- target `ConversationScoped(address)` includes the same address and `shareable`, but excludes actor-private;
- target `Shareable` includes only `shareable`.

Reverse the descending SQL result before returning it to restore chronological order.

- [ ] **Step 5: Implement one actor quantum**

`ActorRunner::run_once(owner_id)` performs:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunOnceOutcome {
    Idle,
    Completed,
    Yielded,
    Cancelled,
    WaitingForDecision,
}

pub async fn run_once(&self, owner: &str) -> Result<RunOnceOutcome> {
    let now = self.clock.now();
    let Some(lease) = self.store.acquire_ready_actor(owner, now, now.plus_millis(30_000)).await? else {
        return Ok(RunOnceOutcome::Idle);
    };
    let result = self.run_leased(lease.clone()).await;
    self.store.release_lease(&lease).await?;
    result
}
```

`run_leased` attaches one run, loads recent audience-safe messages, prepends current source messages, and performs bounded model steps. For each model tool call:

Before the first model request, checkpoint the attached user messages with their exact `incorporated_event_ids`. This makes finalization's source-event requirement explicit; a crash before that checkpoint merely repeats the model request and does not complete the event.

Before asking the model after a resumed run, call `unresolved_attempts`. Invoke an existing `Prepared` attempt with its original ID, reuse and checkpoint a terminal stored outcome whose tool observation was not yet written, and call `block_unknown_attempt` for `OutcomeUnknown`; return `RunOnceOutcome::WaitingForDecision` without invoking that tool again.

1. load capabilities;
2. persist `Prepared` with the model tool-call ID;
3. commit `Running` immediately before `ToolExecutor::execute`;
4. execute with the persisted attempt ID and run cancellation context;
5. persist the typed outcome;
6. checkpoint assistant tool-call and tool observation messages;
7. recheck durable control input.

For a final response, pass the assistant message only in `FinalizeRun.final_messages`; do not checkpoint it separately. Finalization writes that message and `OutboxPayload::Text` atomically. Convert file artifacts into separate `OutboxPayload::File` intents before finalization; never call a stream sink.

- [ ] **Step 6: Add active cancellation monitoring**

While an LLM request is active, `tokio::select!` it against the actor signal receiver. On a signal, query `newer_control_event`. Cancel the `RunContext` only for `CancelRequested` or a newer compatible user message; then attach durable events before restarting. A lost signal is still observed at the next safe-point query.

Run a heartbeat branch in the same active loop. Every `heartbeat_interval`, call `renew_lease` with `now + lease_duration`; stop immediately on `StaleLease`. A `max_wall_time` deadline ends the current quantum after the next safe checkpoint and releases the lease without completing the work item.

- [ ] **Step 7: Verify GREEN and commit**

Run: `rtk cargo test runtime::runner::tests`

Expected: final-output, tool lifecycle, cancellation, preemption, and quantum tests pass.

Commit:

```bash
rtk git add src/runtime.rs src/runtime/model.rs src/runtime/store.rs src/runtime/runner.rs src/runtime/sqlite/checkpoint.rs
rtk git commit -m "feat(runtime): run fenced actor decisions"
```

---

### Task 9: Local Kernel Service and Crash-Recovery Integration

**Files:**
- Create: `src/runtime/service.rs`
- Modify: `src/runtime.rs`
- Create: `tests/durable_local_kernel.rs`

**Interfaces:**
- Produces: `LocalKernel::submit_text`, `request_cancel`, `run_ready_once`, and `drain_outbox`.
- Produces: deterministic end-to-end and restart evidence without adding a user-facing CLI command.
- Preserves: existing CLI and Telegram commands unchanged.

- [ ] **Step 1: Write the end-to-end integration test**

Create a temporary on-disk SQLite database. Import a local owner identity, construct a scripted LLM returning `done`, and assert:

```rust
let accepted = kernel.submit_text("local-event-1", "hello").await.unwrap();
assert_eq!(accepted.sequence(), Some(1));
assert_eq!(kernel.run_ready_once().await.unwrap(), RunOnceOutcome::Completed);

let outbox = kernel.drain_outbox().await.unwrap();
assert_eq!(outbox.len(), 1);
assert_eq!(outbox[0].payload, OutboxPayload::Text { text: "done".into() });
```

Add a dedupe assertion by resubmitting `local-event-1` and checking that no second run or outbox intent appears.

- [ ] **Step 2: Add crash-boundary integration tests**

Use a fault-injecting `RuntimeStore` decorator with named failpoints:

```rust
enum Failpoint {
    AfterAttachment,
    AfterAttemptPrepared,
    AfterAttemptRunning,
    AfterAttemptSucceeded,
    BeforeFinalizationCommit,
    AfterFinalizationCommit,
}
```

For each failpoint, drop the first kernel, reopen the same database, and assert:

- attachment is resumed rather than duplicated;
- `Prepared` may invoke once;
- orphaned `Running` becomes `OutcomeUnknown` and is not invoked automatically;
- `Succeeded` is reused;
- pre-commit finalization retries once;
- post-commit finalization does not create a second outbox intent.

- [ ] **Step 3: Verify RED**

Run: `rtk cargo test --test durable_local_kernel`

Expected: compilation fails because `LocalKernel` and the integration helpers do not exist.

- [ ] **Step 4: Implement the service facade**

`LocalKernel` owns the configured local actor identity, `ActorSignals`, store, and runner:

```rust
#[async_trait]
pub trait ReadyRunner: Send + Sync {
    fn now(&self) -> Timestamp;
    async fn run_ready_once(&self) -> Result<RunOnceOutcome>;
}

pub struct LocalKernel<S, R> {
    store: S,
    runner: R,
    signals: ActorSignals,
    actor_id: ActorId,
    identity_provider: String,
    identity_subject: String,
}

impl<S, R> LocalKernel<S, R>
where
    S: IngressStore + OutboxStore + Clone,
    R: ReadyRunner,
{
    pub async fn submit_text(&self, external_id: &str, text: &str) -> Result<IngressOutcome> {
        let outcome = self.store.ingest(NewInboundEvent::text(
            "local", external_id, &self.identity_provider, &self.identity_subject, Audience::ActorPrivate, text,
        ), self.runner.now()).await?;
        if let IngressOutcome::Accepted { sequence, .. } = outcome {
            self.signals.notify(&self.actor_id, sequence).await;
        }
        Ok(outcome)
    }
}
```

`request_cancel` persists `CancelRequested` through the same ingress path. `drain_outbox` is a fake delivery adapter: it clones pending payloads, marks their rows delivered, then returns the cloned values. It never assumes actor ID equals identity subject.

- [ ] **Step 5: Implement failpoint decoration in the integration test only**

The decorator implements and delegates every `RuntimeStore` and `IngressStore` method, returning a configured error immediately after the named successful delegate call. Do not add production crash flags or environment variables.

- [ ] **Step 6: Verify GREEN and commit**

Run: `rtk cargo test --test durable_local_kernel -- --nocapture`

Expected: end-to-end, dedupe, fencing, and all restart-boundary cases pass.

Commit:

```bash
rtk git add src/runtime.rs src/runtime/service.rs tests/durable_local_kernel.rs
rtk git commit -m "test(runtime): verify durable local kernel"
```

---

### Task 10: Full Verification and Kernel Documentation

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: completed durable local kernel.
- Produces: fresh repository-wide validation and an explicit statement that the kernel is internal until `codrik serve` lands.

- [ ] **Step 1: Document the internal delivery status**

Add a short `Development status` paragraph to `README.md`:

```markdown
The persistent runtime is being delivered in vertical slices. The durable
local kernel stores actor events, fenced checkpoints, tool attempts, recent
context, and outbox intents in SQLite. It is internal until `codrik serve`
and a webhook gateway are available; existing CLI and Telegram commands keep
their legacy behavior in the meantime.
```

- [ ] **Step 2: Run formatting and unit tests**

Run: `rtk cargo fmt --check`

Expected: exit code 0.

Run: `rtk cargo test`

Expected: all non-ignored tests pass, including `durable_local_kernel`.

- [ ] **Step 3: Run build and lint checks**

Run: `rtk cargo check`

Expected: exit code 0 with no new warnings.

Run: `rtk cargo clippy --all-targets --all-features`

Expected: exit code 0 with no new warnings.

- [ ] **Step 4: Verify dependency and diff hygiene**

Run: `rtk cargo tree -d`

Expected: no unintended second `rusqlite` version introduced by a direct dependency; runtime uses the version selected by `tokio-rusqlite`.

Run: `rtk git diff --check`

Expected: exit code 0.

- [ ] **Step 5: Commit documentation**

```bash
rtk git add README.md
rtk git commit -m "docs(runtime): describe durable kernel status"
```
