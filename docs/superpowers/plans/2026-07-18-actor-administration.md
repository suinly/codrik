# Actor Administration and Multi-Actor Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add supported local actor and tool-permission administration, actor-specific linking, guarded deletion, and concurrent runtime dispatch for every enabled actor.

**Architecture:** SQLite remains authoritative. A focused actor-administration service validates tool names and protects `runtime.actor_id`, while strict IPC exposes it only over the existing owner-checked Unix socket. A manager reconciles one dispatcher task per enabled actor and rebuilds a dispatcher after actor permissions change, so changes apply to the next quantum without interrupting the current one.

**Tech Stack:** Rust 2024, Tokio, async-trait, serde, strict local IPC v1, SQLite/tokio-rusqlite, existing supervisor and artifact manager.

## Global Constraints

- Every shell command is prefixed with `rtk`.
- Use TDD: observe each focused test fail before adding its production behavior.
- Do not add dependencies, roles, permission groups, remote administration, JSON CLI output, YAML actor synchronization, or administrative agent tools.
- SQLite is the only actor authority; the CLI never opens `runtime.sqlite`.
- New actors are enabled with `tools: []`; empty-store bootstrap remains enabled with `tools: ["*"]`.
- `runtime.actor_id` remains the local default and cannot be disabled or deleted.
- `"*"` grants standard tools only; privileged `bash` requires an explicit grant.
- Disable never interrupts an active quantum. Permission changes apply to the next quantum.
- Force deletion requires a disabled, non-default actor with no lease, active run, or unresolved delivery.
- Preserve existing IPC v1 encodings for old request and response variants.
- The known flaky test `runtime::sqlite::gateway::tests::unresolved_chunk_blocks_only_its_own_response_suffix` must be run separately if the full suite trips it; do not change it in this feature.

## File Map

- `src/runtime/store.rs` — actor administration domain records and persistence trait.
- `src/runtime/sqlite/actors.rs` — transactional actor queries and mutations.
- `src/runtime/actor_admin.rs` — default-actor protection, tool-name validation, mutation notifications, and artifact cleanup orchestration.
- `src/runtime/signals.rs` — actor-directory change notification beside existing work signals.
- `src/tools.rs` — authoritative registered tool-name projection.
- `src/runtime/ipc/protocol.rs` — strict typed actor-admin wire requests and responses.
- `src/runtime/ipc/client.rs` — one request/response client method for actor administration and actor-aware link requests.
- `src/runtime/ipc/server.rs` — owner-checked admin dispatch.
- `src/interfaces/cli.rs` — command parsing and human-readable rendering.
- `src/runtime/dispatcher.rs` — enabled-actor task reconciliation.
- `src/llm/client.rs` — shared `Arc<LlmStreamClient>` delegation.
- `src/app.rs` — composition of administration, multi-actor dispatch, and per-actor runners.
- `src/interfaces/telegram/ingress.rs` — reject ingress for disabled actors.
- `src/runtime/migrations/0006_actor_deletion.sql` — scoped append-only deletion marker and trigger rules.
- `src/runtime/sqlite.rs` — schema v6 migration wiring.
- `src/runtime/artifacts.rs` — unreferenced managed-file cleanup.
- `tests/serve_runtime.rs` — multi-actor acceptance path.
- `README.md` — operator documentation.

---

### Task 1: Persist Basic Actor Administration

**Files:**
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/actors.rs`

**Interfaces:**
- Consumes: existing `ActorId`, `RuntimeActor`, `LinkIdentity`, `Timestamp`, and `SqliteRuntimeStore`.
- Produces:
  - `ActorDetails { actor: RuntimeActor, identities: Vec<LinkIdentity>, has_active_work: bool }`
  - `ActorCreateOutcome::{Created(RuntimeActor), Existing(RuntimeActor)}`
  - `ActorMutationOutcome { actor: RuntimeActor, changed: bool }`
  - `ActorAdminStore::{list_actors, actor_details, create_actor, set_actor_enabled, grant_actor_tool, revoke_actor_tool}`

- [ ] **Step 1: Write failing store tests**

Add focused async tests to `src/runtime/sqlite/actors.rs`:

```rust
#[tokio::test]
async fn actor_admin_create_list_and_show_are_stable() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let alice = ActorId::parse_workspace_safe("alice")?;
    assert!(matches!(
        store.create_actor(&alice, Timestamp(10)).await?,
        ActorCreateOutcome::Created(RuntimeActor { enabled: true, ref tools, .. })
            if tools.is_empty()
    ));
    assert!(matches!(
        store.create_actor(&alice, Timestamp(11)).await?,
        ActorCreateOutcome::Existing(_)
    ));
    assert_eq!(store.list_actors().await?[0].id, alice);
    assert_eq!(store.actor_details(&alice).await?.unwrap().identities, vec![]);
    Ok(())
}

#[tokio::test]
async fn actor_admin_enable_and_tools_are_idempotent_and_sorted() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let actor = ActorId::parse_workspace_safe("alice")?;
    store.create_actor(&actor, Timestamp(10)).await?;
    assert!(store.set_actor_enabled(&actor, false).await?.unwrap().changed);
    assert!(!store.set_actor_enabled(&actor, false).await?.unwrap().changed);
    store.grant_actor_tool(&actor, "bash").await?;
    store.grant_actor_tool(&actor, "*").await?;
    assert_eq!(
        store.load_actor(&actor).await?.unwrap().tools,
        vec!["*", "bash"]
    );
    assert!(store.revoke_actor_tool(&actor, "bash").await?.unwrap().changed);
    assert!(!store.revoke_actor_tool(&actor, "bash").await?.unwrap().changed);
    Ok(())
}
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```sh
rtk cargo test runtime::sqlite::actors::tests::actor_admin -- --nocapture
```

Expected: compilation fails because the actor-admin records, trait, and methods do not exist.

- [ ] **Step 3: Add the domain records and store trait**

Add to `src/runtime/store.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorDetails {
    pub actor: RuntimeActor,
    pub identities: Vec<LinkIdentity>,
    pub has_active_work: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActorCreateOutcome {
    Created(RuntimeActor),
    Existing(RuntimeActor),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorMutationOutcome {
    pub actor: RuntimeActor,
    pub changed: bool,
}

#[async_trait]
pub trait ActorAdminStore: Send + Sync {
    async fn list_actors(&self) -> Result<Vec<RuntimeActor>>;
    async fn actor_details(&self, actor: &ActorId) -> Result<Option<ActorDetails>>;
    async fn create_actor(
        &self,
        actor: &ActorId,
        now: Timestamp,
    ) -> Result<ActorCreateOutcome>;
    async fn set_actor_enabled(
        &self,
        actor: &ActorId,
        enabled: bool,
    ) -> Result<Option<ActorMutationOutcome>>;
    async fn grant_actor_tool(
        &self,
        actor: &ActorId,
        tool: &str,
    ) -> Result<Option<ActorMutationOutcome>>;
    async fn revoke_actor_tool(
        &self,
        actor: &ActorId,
        tool: &str,
    ) -> Result<Option<ActorMutationOutcome>>;
}
```

Use the single name `ActorMutationOutcome` for both enabled-state and tool changes; do not introduce parallel result types.

- [ ] **Step 4: Implement transactional SQLite methods**

In `src/runtime/sqlite/actors.rs`, implement `ActorAdminStore` with these rules:

```rust
// create_actor transaction
INSERT OR IGNORE INTO actors(id, enabled, tools_json, created_at)
VALUES (?1, 1, '[]', ?2);

// list ordering
SELECT id, enabled, tools_json FROM actors ORDER BY id;
```

For grant/revoke, read `tools_json`, deserialize to `BTreeSet<String>`, mutate it, serialize the sorted set, and update in the same `TransactionBehavior::Immediate` transaction. Return `None` for an absent actor. `actor_details` orders identities by `(provider, subject)` and computes active work from leases, active runs, and nonterminal work items.

- [ ] **Step 5: Run focused and store tests**

Run:

```sh
rtk cargo test runtime::sqlite::actors -- --nocapture
rtk cargo test runtime::sqlite::tests -- --nocapture
rtk cargo check --tests
rtk git diff --check
```

Expected: all pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/runtime/store.rs src/runtime/sqlite/actors.rs
rtk git commit -m "feat(runtime): persist actor administration"
```

---

### Task 2: Enforce Actor Administration Policy

**Files:**
- Create: `src/runtime/actor_admin.rs`
- Modify: `src/runtime.rs`
- Modify: `src/runtime/signals.rs`
- Modify: `src/tools.rs`

**Interfaces:**
- Consumes: `ActorAdminStore` and records from Task 1, `Clock`, and the existing tool registry.
- Produces:
  - `ActorDirectorySignals::{subscribe, notify}`
  - `ActorAdminCommand` and `ActorAdminResult`
  - `ActorAdministrator` trait
  - `ActorAdministration<S, C>` implementation
  - `ToolRegistry::registered_names() -> BTreeSet<String>`

- [ ] **Step 1: Write failing policy tests**

Create `src/runtime/actor_admin.rs` with tests backed by an in-memory SQLite store:

```rust
#[tokio::test]
async fn administration_protects_default_and_rejects_unknown_tools() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let default = ActorId::parse_workspace_safe("owner")?;
    store.ensure_initial_actor(&default, &["*".into()], Timestamp(1)).await?;
    let admin = ActorAdministration::new(
        store,
        default.clone(),
        BTreeSet::from(["*".into(), "bash".into(), "datetime".into()]),
        ActorDirectorySignals::default(),
        ManualClock::new(2),
    );
    assert!(admin.set_enabled(default.clone(), false).await.is_err());
    assert!(admin.grant(default.clone(), "missing".into()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn committed_mutation_notifies_directory_subscribers() -> Result<()> {
    let signals = ActorDirectorySignals::default();
    let mut changed = signals.subscribe();
    let admin = administration(signals);
    admin.create(ActorId::parse_workspace_safe("alice")?).await?;
    changed.changed().await?;
    Ok(())
}
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```sh
rtk cargo test runtime::actor_admin -- --nocapture
```

Expected: compilation fails because the module and policy types do not exist.

- [ ] **Step 3: Add directory signals and registered tool names**

Add a single global watch channel in `src/runtime/signals.rs`:

```rust
#[derive(Clone)]
pub struct ActorDirectorySignals {
    sender: watch::Sender<u64>,
}

impl Default for ActorDirectorySignals {
    fn default() -> Self {
        Self { sender: watch::channel(0).0 }
    }
}

impl ActorDirectorySignals {
    pub fn subscribe(&self) -> watch::Receiver<u64> { self.sender.subscribe() }
    pub fn notify(&self) { self.sender.send_modify(|value| *value += 1); }
}
```

In `src/tools.rs`, return the actual handler names rather than maintaining a second hard-coded list:

```rust
pub fn registered_names() -> BTreeSet<String> {
    Self::new()
        .handlers
        .iter()
        .map(|handler| handler.name().to_owned())
        .collect()
}
```

The administration constructor adds `"*"` to this set.

- [ ] **Step 4: Implement the policy service**

Define commands/results in `src/runtime/actor_admin.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActorAdminCommand {
    List,
    Show { actor_id: ActorId },
    Create { actor_id: ActorId },
    Enable { actor_id: ActorId },
    Disable { actor_id: ActorId },
    ToolsList { actor_id: ActorId },
    ToolsGrant { actor_id: ActorId, tool: String },
    ToolsRevoke { actor_id: ActorId, tool: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActorAdminResult {
    Actors { actors: Vec<RuntimeActor> },
    Actor { details: ActorDetails, changed: bool },
    Tools { actor_id: ActorId, tools: Vec<String> },
}
```

Derive strict serde support for the nested records used on the wire. Define an async `ActorAdministrator` trait with `execute(command) -> Result<ActorAdminResult>` so the IPC server can use a small fake in tests. `ActorAdministration<S, C>` validates tool names, protects the default actor, calls the store, and notifies only after a committed change. Task 3 adds the delete command and result after deletion exists end to end.

- [ ] **Step 5: Run focused tests**

Run:

```sh
rtk cargo test runtime::actor_admin -- --nocapture
rtk cargo test tools::tests -- --nocapture
rtk cargo check --tests
rtk git diff --check
```

Expected: all pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/runtime.rs src/runtime/actor_admin.rs src/runtime/signals.rs src/tools.rs
rtk git commit -m "feat(runtime): enforce actor administration policy"
```

---

### Task 3: Implement Guarded Actor Deletion

**Files:**
- Create: `src/runtime/migrations/0006_actor_deletion.sql`
- Modify: `src/runtime/sqlite.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/actors.rs`
- Modify: `src/runtime/actor_admin.rs`
- Modify: `src/runtime/artifacts.rs`

**Interfaces:**
- Consumes: Task 2 administration policy and configured artifact root.
- Produces:
  - schema version 6 and `actor_deletions` transaction marker;
  - `ActorDeleteMode::{EmptyOnly, Force}`;
  - `ActorDeleteOutcome::{Deleted { artifact_paths }, NotFound, Nonempty, Busy, UnresolvedDelivery}`;
  - working `ActorAdminCommand::Delete`;
  - unreferenced managed-file GC.

- [ ] **Step 1: Write failing deletion tests**

Add store tests for all gates:

```rust
#[tokio::test]
async fn empty_delete_succeeds_but_nonempty_delete_requires_force() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let empty = ActorId::parse_workspace_safe("empty")?;
    store.create_actor(&empty, Timestamp(10)).await?;
    assert!(matches!(
        store.delete_actor(&empty, ActorDeleteMode::EmptyOnly, Timestamp(20)).await?,
        ActorDeleteOutcome::Deleted { .. }
    ));
    let used = ActorId::parse_workspace_safe("used")?;
    store.create_actor(&used, Timestamp(11)).await?;
    attach_identity(&store, &used).await?;
    assert_eq!(
        store.delete_actor(&used, ActorDeleteMode::EmptyOnly, Timestamp(21)).await?,
        ActorDeleteOutcome::Nonempty
    );
    Ok(())
}

#[tokio::test]
async fn force_delete_rejects_enabled_active_and_unresolved_delivery_actors() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let actor = actor(&store, "alice").await?;
    assert_eq!(
        store.delete_actor(&actor, ActorDeleteMode::Force, Timestamp(20)).await?,
        ActorDeleteOutcome::Busy
    );
    store.set_actor_enabled(&actor, false).await?;
    insert_actor_lease(&store, &actor).await?;
    assert_eq!(
        store.delete_actor(&actor, ActorDeleteMode::Force, Timestamp(21)).await?,
        ActorDeleteOutcome::Busy
    );
    remove_actor_lease(&store, &actor).await?;
    enqueue_unresolved_gateway_delivery(&store, &actor).await?;
    assert_eq!(
        store.delete_actor(&actor, ActorDeleteMode::Force, Timestamp(22)).await?,
        ActorDeleteOutcome::UnresolvedDelivery
    );
    assert!(store.load_actor(&actor).await?.is_some());
    Ok(())
}
```

Define `actor`, `attach_identity`, `insert_actor_lease`, `remove_actor_lease`, and `enqueue_unresolved_gateway_delivery` in the same test module. `actor` calls `create_actor`; the other helpers execute the minimum valid SQL fixture rows in one `store.connection.call`, using the authoritative column values and foreign keys from migrations 1-5. Do not add production fixture APIs.

Add an artifact test that creates a canonical managed file with no database row, runs GC twice using the existing two-check safety pattern, and asserts the file is removed without following a symlink.

- [ ] **Step 2: Run deletion tests and verify RED**

Run:

```sh
rtk cargo test runtime::sqlite::actors::tests::force_delete -- --nocapture
rtk cargo test runtime::artifacts::tests::gc_removes_unreferenced_managed_file -- --nocapture
```

Expected: compilation fails because deletion types, schema support, and GC behavior do not exist.

- [ ] **Step 3: Add schema v6**

Create `0006_actor_deletion.sql`:

```sql
CREATE TABLE actor_deletions (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    requested_at INTEGER NOT NULL
) STRICT;
```

Replace only the append-only delete triggers that block actor-owned purge. Their normal branch remains unchanged, and their deletion branch permits a row only when its owning actor exists in `actor_deletions`. For example, the `outbox_deliveries` delete guard resolves ownership through `outbox`:

```sql
DROP TRIGGER outbox_deliveries_are_immutable_on_delete;
CREATE TRIGGER outbox_deliveries_are_immutable_on_delete
BEFORE DELETE ON outbox_deliveries
WHEN NOT EXISTS (
    SELECT 1
    FROM outbox
    JOIN actor_deletions ON actor_deletions.actor_id = outbox.actor_id
    WHERE outbox.id = OLD.outbox_id
)
BEGIN
    SELECT RAISE(ABORT, 'outbox_deliveries are append-only');
END;
```

Wire `ACTOR_DELETION_MIGRATION`, `migrate_to_v6`, versions `0..=5`, and `RUNTIME_SCHEMA_VERSION = 6` in `src/runtime/sqlite.rs`. Add migration tests proving ordinary immutable deletes still fail and marked actor purge succeeds.

- [ ] **Step 4: Implement transactional deletion**

Add to `ActorAdminStore`:

```rust
async fn delete_actor(
    &self,
    actor: &ActorId,
    mode: ActorDeleteMode,
    now: Timestamp,
) -> Result<ActorDeleteOutcome>;
```

Inside one immediate transaction:

1. Load actor; return `NotFound` if absent.
2. For `EmptyOnly`, reject any identity, event, work item, local request, delivery, memory row, or artifact as `Nonempty`.
3. For `Force`, reject `enabled = 1`, any actor lease, active run, or nonterminal work item as `Busy`.
4. Reject pending/claimed/retryable/outcome-unknown local or gateway deliveries as `UnresolvedDelivery`.
5. Collect actor artifact paths.
6. Insert the `actor_deletions` marker.
7. Delete actor-owned child rows in foreign-key order, then delete the actor.
8. Return committed artifact paths.

Do not disable foreign keys or drop triggers at runtime.

- [ ] **Step 5: Complete service deletion and artifact cleanup**

Extend both wire-domain enums:

```rust
// ActorAdminCommand
Delete { actor_id: ActorId, force: bool },

// ActorAdminResult
Deleted { actor_id: ActorId },
```

`ActorAdministration::execute(Delete)` rejects the configured default before calling the store. `force: false` maps to `EmptyOnly`; `force: true` maps to `Force`. `EmptyOnly` maps `Nonempty` to a message that names `disable` and `--force`. `Force` returns the exact failed precondition. After `Deleted`, validate every returned artifact path under the configured artifact root and attempt unlink. Return success after the database commit even if unlink fails; log cleanup failure without exposing the full path.

Extend artifact GC using the existing safe directory traversal and two-database-check pattern. Only regular files under the managed root whose canonical artifact name has no database row are eligible.

- [ ] **Step 6: Run migration, deletion, and artifact tests**

Run:

```sh
rtk cargo test runtime::sqlite::tests -- --nocapture
rtk cargo test runtime::sqlite::actors -- --nocapture
rtk cargo test runtime::artifacts -- --nocapture
rtk cargo test runtime::actor_admin -- --nocapture
rtk cargo check --tests
rtk git diff --check
```

Expected: all pass and schema version assertions expect 6.

- [ ] **Step 7: Commit**

```sh
rtk git add src/runtime/migrations/0006_actor_deletion.sql src/runtime/sqlite.rs src/runtime/store.rs src/runtime/sqlite/actors.rs src/runtime/actor_admin.rs src/runtime/artifacts.rs
rtk git commit -m "feat(runtime): add guarded actor deletion"
```

---

### Task 4: Expose Actor Administration Through Strict IPC

**Files:**
- Modify: `src/runtime/ipc/protocol.rs`
- Modify: `src/runtime/ipc/client.rs`
- Modify: `src/runtime/ipc/server.rs`
- Modify: `src/runtime/identity_link.rs`

**Interfaces:**
- Consumes: `ActorAdministrator`, `ActorAdminCommand`, and `ActorAdminResult` from Tasks 2-3.
- Produces:
  - `ClientRequestBody::ActorAdmin { request_id, command }`;
  - `ServerEventBody::ActorAdminResult { request_id, result }`;
  - optional `actor_id` on `IssueLinkCode`;
  - `LocalIpcClient::{actor_admin, issue_link_code_for}`;
  - `LocalIpcServer::with_actor_administrator`.

- [ ] **Step 1: Write failing strict-wire tests**

In `protocol.rs`, freeze representative JSON:

```rust
assert_eq!(
    serde_json::to_string(&ClientRequest::new(ClientRequestBody::ActorAdmin {
        request_id: request_id(),
        command: ActorAdminCommand::ToolsGrant {
            actor_id: ActorId::parse_workspace_safe("alice")?,
            tool: "bash".into(),
        },
    }))?,
    r#"{"version":1,"body":{"type":"actor_admin","request_id":"0190f2ef-0000-7000-8000-000000000001","command":{"action":"tools_grant","actor_id":"alice","tool":"bash"}}}"#
);
```

Assert that old `issue_link_code` JSON remains byte-for-byte unchanged when `actor_id` is `None`, and that unknown fields/actions are rejected.

Add server/client round-trip tests with a recording fake administrator. Confirm a non-owner peer is rejected before the administrator is called.

- [ ] **Step 2: Run IPC tests and verify RED**

Run:

```sh
rtk cargo test runtime::ipc::protocol::tests::actor_admin -- --nocapture
rtk cargo test runtime::ipc::server::tests::actor_admin -- --nocapture
```

Expected: compilation fails because IPC variants and handlers do not exist.

- [ ] **Step 3: Add strict protocol variants**

Add:

```rust
ActorAdmin {
    request_id: RequestId,
    command: ActorAdminCommand,
},
IssueLinkCode {
    request_id: RequestId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_id: Option<ActorId>,
},
```

and:

```rust
ActorAdminResult {
    request_id: RequestId,
    result: ActorAdminResult,
},
```

Update semantic validation so every actor-admin request and result has a valid UUID request ID, nonblank tool name, and already validated `ActorId`. Preserve the old omitted `actor_id` link encoding.

- [ ] **Step 4: Implement client and server dispatch**

Add one client operation:

```rust
pub async fn actor_admin(
    &self,
    request_id: RequestId,
    command: ActorAdminCommand,
) -> Result<ActorAdminResult>;
```

It opens one operation connection, sends the request, accepts only the matching `ActorAdminResult`, maps matching `RequestError`, and rejects EOF or unrelated responses.

The server stores `Option<Arc<dyn ActorAdministrator>>`. `with_actor_administrator` installs it. The handler calls `execute` only after existing peer authorization and frame validation. Missing administration returns a request error rather than panicking.

For link issuance, choose `actor_id.unwrap_or_else(|| self.actor.clone())`, load it through the administrator, and reject missing or disabled actors before calling the existing identity-link manager. Add an `issue_link_code_for(request_id, Option<ActorId>)` client method; retain `issue_link_code(request_id)` as the backward-compatible `None` wrapper.

- [ ] **Step 5: Run IPC tests**

Run outside the filesystem sandbox when local socket tests report `Operation not permitted`:

```sh
rtk cargo test runtime::ipc::protocol -- --nocapture
rtk cargo test runtime::ipc::client -- --nocapture
rtk cargo test runtime::ipc::server -- --nocapture
rtk cargo check --tests
rtk git diff --check
```

Expected: all pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/runtime/ipc/protocol.rs src/runtime/ipc/client.rs src/runtime/ipc/server.rs src/runtime/identity_link.rs
rtk git commit -m "feat(ipc): expose actor administration"
```

---

### Task 5: Add the Actor Administration CLI

**Files:**
- Modify: `src/interfaces/cli.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: `LocalIpcClient::actor_admin`, `issue_link_code_for`, and IPC domain types from Task 4.
- Produces: the exact CLI surface approved in the design.

- [ ] **Step 1: Write failing parser and renderer tests**

Extend `parses_supported_commands` with all approved forms and rejection cases:

```rust
assert_eq!(parse(&["actors", "list"])?, CliCommand::Actors(ActorAdminCommand::List));
assert_eq!(
    parse(&["actors", "tools", "grant", "alice", "bash"])?,
    CliCommand::Actors(ActorAdminCommand::ToolsGrant {
        actor_id: ActorId::parse_workspace_safe("alice")?,
        tool: "bash".into(),
    })
);
assert_eq!(
    parse(&["actors", "delete", "alice", "--force"])?,
    CliCommand::Actors(ActorAdminCommand::Delete {
        actor_id: ActorId::parse_workspace_safe("alice")?,
        force: true,
    })
);
assert!(parse(&["actors", "delete", "alice", "--unknown"]).is_err());
assert!(parse(&["actors", "tools", "grant", "alice"]).is_err());
```

Add exact output tests for a sorted actor list, `show`, tools list, changed/unchanged mutation, and deletion. Output must not expose identity subjects; `show` prints provider and username when present.

- [ ] **Step 2: Run CLI tests and verify RED**

Run:

```sh
rtk cargo test interfaces::cli::tests::parses_supported_commands -- --nocapture
rtk cargo test interfaces::cli::tests::renders_actor -- --nocapture
```

Expected: compilation fails because nested actor commands and renderers do not exist.

- [ ] **Step 3: Implement parsing and execution**

Add:

```rust
enum CliCommand {
    // existing variants
    Actors(ActorAdminCommand),
    Link(Option<ActorId>),
}
```

Parse exact positional forms only. `--force` is valid only as the final argument of `actors delete`. Do not add a generic argument parser dependency.

Execution calls `local_client()?.actor_admin(RequestId::new(), command)` and renders the returned result. `codrik link` sends `None`; `codrik link alice` sends `Some(alice)`. Render tool grants in stable order and quote `*` in README shell examples to prevent shell glob expansion.

- [ ] **Step 4: Document commands**

Add an `Actor administration` section to README with:

```sh
codrik actors create alice
codrik actors tools grant alice '*'
codrik actors tools grant alice bash
codrik link alice
codrik actors disable alice
codrik actors delete alice --force
```

State that create is enabled with no tools, `runtime.actor_id` is protected, disable lets active work finish, and force deletion is permanent.

- [ ] **Step 5: Run CLI and documentation tests**

```sh
rtk cargo test interfaces::cli -- --nocapture
rtk cargo test --test install_script -- --nocapture
rtk cargo check --tests
rtk git diff --check
```

Expected: all pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/interfaces/cli.rs README.md
rtk git commit -m "feat(cli): manage runtime actors"
```

---

### Task 6: Reconcile One Dispatcher per Enabled Actor

**Files:**
- Modify: `src/runtime/dispatcher.rs`
- Modify: `src/runtime/signals.rs`

**Interfaces:**
- Consumes: `ActorAdminStore::list_actors`, `ActorDirectorySignals`, existing `ActorDispatcher`, and a caller-provided async dispatcher factory.
- Produces: `ActorDispatcherManager<S>` with `run_with(shutdown, make_dispatcher)`.

- [ ] **Step 1: Write failing paused-time manager tests**

Add tests in `dispatcher.rs` using an in-memory actor store and a recording async factory:

```rust
#[tokio::test(start_paused = true)]
async fn manager_runs_enabled_actors_independently_and_stops_disabled_actor() -> Result<()> {
    let harness = ManagerHarness::new([actor("alice", &[]), actor("bob", &[])]).await?;
    let manager = harness.spawn_manager();
    harness.wait_started("alice").await;
    harness.wait_started("bob").await;
    assert_eq!(harness.running(), BTreeSet::from(["alice".into(), "bob".into()]));
    harness.set_enabled("bob", false).await?;
    harness.directory.notify();
    harness.wait_stopped("bob").await;
    assert_eq!(harness.running(), BTreeSet::from(["alice".into()]));
    harness.shutdown();
    manager.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn tool_change_restarts_dispatcher_only_after_current_quantum() -> Result<()> {
    let harness = ManagerHarness::new([actor("alice", &[])]).await?;
    let manager = harness.spawn_manager();
    let first = harness.wait_started("alice").await;
    harness.grant("alice", "bash").await?;
    harness.directory.notify();
    assert!(!first.is_stopped());
    first.finish_current_quantum();
    let second = harness.wait_for_start_count("alice", 2).await;
    assert_eq!(second.actor.tools, vec!["bash"]);
    harness.shutdown();
    manager.await??;
    Ok(())
}
```

`ManagerHarness` is test-only and contains an in-memory store, directory signals, a global shutdown sender, and a factory that records actor snapshots and blocks on a per-run `Notify`. Keep it inside `dispatcher.rs`; production code gets no harness abstractions.

- [ ] **Step 2: Run manager tests and verify RED**

Run:

```sh
rtk cargo test runtime::dispatcher::tests::manager_ -- --nocapture
rtk cargo test runtime::dispatcher::tests::tool_change_ -- --nocapture
```

Expected: compilation fails because `ActorDispatcherManager` does not exist.

- [ ] **Step 3: Implement minimal task reconciliation**

Add:

```rust
pub struct ActorDispatcherManager<S> {
    store: S,
    directory: ActorDirectorySignals,
}

impl<S> ActorDispatcherManager<S>
where
    S: ActorAdminStore + Clone + Send + Sync + 'static,
{
    pub async fn run_with<F, Fut>(
        self,
        mut shutdown: watch::Receiver<bool>,
        make_dispatcher: F,
    ) -> Result<()>
    where
        F: Fn(RuntimeActor, watch::Receiver<bool>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static;
}
```

Maintain a `HashMap<ActorId, RunningDispatcher>` containing the actor snapshot, per-task shutdown sender, and join handle. Reconcile on startup, directory notification, and a 500 ms interval. Start enabled actors absent from the map. For a disabled/missing/changed snapshot, send shutdown and wait for that task to finish before starting its replacement. A dispatcher error propagates; a requested shutdown returning `Ok(())` does not. Global shutdown signals every child and waits for them.

Do not add a scheduler or persistent dispatcher table.

- [ ] **Step 4: Run dispatcher tests**

```sh
rtk cargo test runtime::dispatcher -- --nocapture
rtk cargo check --tests
rtk git diff --check
```

Expected: all pass.

- [ ] **Step 5: Commit**

```sh
rtk git add src/runtime/dispatcher.rs src/runtime/signals.rs
rtk git commit -m "feat(runtime): supervise enabled actor dispatchers"
```

---

### Task 7: Compose the Multi-Actor Runtime

**Files:**
- Modify: `src/llm/client.rs`
- Modify: `src/app.rs`
- Modify: `src/interfaces/telegram/ingress.rs`
- Modify: `tests/serve_runtime.rs`

**Interfaces:**
- Consumes: administration service, IPC wiring, CLI commands, and dispatcher manager from Tasks 1-6.
- Produces: fully hot multi-actor `codrik serve` behavior.

- [ ] **Step 1: Write failing runtime tests**

Add an app-level test with two actors and an injected LLM:

```rust
#[tokio::test]
async fn serve_dispatches_enabled_second_actor_with_its_tools() -> Result<()> {
    let harness = MultiActorServeHarness::new().await?;
    harness.create_actor("alice").await?;
    harness.grant("alice", "datetime").await?;
    harness.revoke("owner", "*").await?;
    harness.submit("owner", "owner request").await?;
    harness.submit("alice", "alice request").await?;
    harness.wait_for_completed("owner").await?;
    harness.wait_for_completed("alice").await?;
    assert_eq!(harness.tool_names_for("owner"), Vec::<String>::new());
    assert_eq!(harness.tool_names_for("alice"), vec!["datetime"]);
    harness.shutdown().await?;
    Ok(())
}
```

`MultiActorServeHarness` is test-only composition around the existing injected-clock/injected-LLM serve seams. It records LLM requests keyed by actor ID and submits directly through `IngressStore`; do not add a production test endpoint.

Add an ingress test resolving a disabled linked actor and assert the update is not ingested or signalled. Add an acceptance scenario that creates Alice through IPC, grants a tool, issues `link alice`, redeems the code through Telegram ingress, completes one request, disables Alice, and proves a later update is rejected.

- [ ] **Step 2: Run runtime tests and verify RED**

Run:

```sh
rtk cargo test app::tests::serve_dispatches_enabled_second_actor_with_its_tools -- --nocapture
rtk cargo test interfaces::telegram::ingress::tests::disabled_actor -- --nocapture
rtk cargo test --test serve_runtime actor_administration -- --nocapture
```

Expected: tests fail because serve still constructs one fixed dispatcher and Telegram accepts a disabled resolution.

- [ ] **Step 3: Share the LLM client without cloning its state**

In `src/llm/client.rs` add the standard delegation:

```rust
#[async_trait]
impl<T> LlmStreamClient for Arc<T>
where
    T: LlmStreamClient + Send + Sync + ?Sized,
{
    async fn stream(
        &self,
        request: LlmRequest,
        sink: &mut dyn LlmStreamSink,
        context: &RunContext,
    ) -> Result<LlmResponse> {
        (**self).stream(request, sink, context).await
    }
}
```

- [ ] **Step 4: Wire administration and dispatcher factory in app composition**

In `serve_at_until_with_hooks`:

1. Preserve empty-store bootstrap and configured-actor enabled verification.
2. Create one `ActorDirectorySignals`.
3. Create `ActorAdministration` with the store, configured actor, registered tool names, artifact root, logger, signals, and clock.
4. Install it on `LocalIpcServer`.
5. Wrap the injected LLM in `Arc`.
6. Replace the single dispatcher component with `ActorDispatcherManager`.

The manager factory receives a `RuntimeActor` snapshot and builds exactly the existing actor-specific objects:

```rust
let tool_config = tool_config_for_actor_workspace(
    actor_workspace_path_in(&home, actor.id.as_str())?,
)?;
let instructions = agent_instructions_for_tool_config(&tool_config);
let tools = ToolRegistry::with_allowed_tools_and_config(actor.tools.clone(), tool_config);
let runner = ActorRunner::new(
    llm.clone(),
    tools,
    signals.clone(),
    events.clone(),
    RunnerLimits::default(),
    artifacts.clone(),
)
.with_system_instructions(instructions)
.with_logger(logger.clone())
.with_boundary_hooks(hooks.clone());
ActorDispatcher::new(
    actor.id.clone(),
    format!("dispatcher-{}-{}", std::process::id(), actor.id),
    signals.clone(),
    runner,
    clock.clone(),
)
.run_with_shutdown(actor_shutdown)
.await
```

Keep the component name `dispatcher`; do not expose one supervisor component per actor.

- [ ] **Step 5: Reject disabled gateway ingress**

In Telegram ingress, after identity resolution and before durable ingest:

```rust
if !actor.enabled {
    self.enqueue_response(update_id, route, "This actor is disabled.").await?;
    return Ok(TelegramIngressOutcome::CommandHandled);
}
```

The store's local ingress already returns `ActorUnavailable` for disabled actors; retain that durable check.

- [ ] **Step 6: Run runtime and acceptance tests**

Run outside the sandbox if socket tests require it:

```sh
rtk cargo test app::tests -- --nocapture
rtk cargo test interfaces::telegram -- --nocapture
rtk cargo test --test serve_runtime -- --nocapture
rtk cargo check --tests
rtk git diff --check
```

Expected: all pass.

- [ ] **Step 7: Commit**

```sh
rtk git add src/llm/client.rs src/app.rs src/interfaces/telegram/ingress.rs tests/serve_runtime.rs
rtk git commit -m "feat(runtime): serve all enabled actors"
```

---

### Task 8: Final Documentation and Verification

**Files:**
- Modify: `README.md`
- Modify: `tests/install_script.rs`

**Interfaces:**
- Consumes: completed feature.
- Produces: operator-ready documentation and final verification evidence.

- [ ] **Step 1: Complete README behavior documentation**

Ensure README states:

- all enabled actors are served concurrently;
- `runtime.actor_id` is the local default and protected;
- actor creation is enabled with no tools;
- `"*"` excludes privileged `bash`;
- permission changes apply on the next run;
- disable allows active work to finish;
- empty delete versus irreversible guarded `--force`;
- `codrik link [actor-id]` behavior.

- [ ] **Step 2: Add active-documentation assertions**

Extend `active_documentation_has_no_users_json_instructions`:

```rust
assert!(readme.contains("codrik actors tools grant alice bash"));
assert!(readme.contains("codrik link alice"));
assert!(readme.contains("next run"));
assert!(readme.contains("--force"));
```

- [ ] **Step 3: Run feature verification**

```sh
rtk cargo test runtime::actor_admin -- --nocapture
rtk cargo test runtime::sqlite::actors -- --nocapture
rtk cargo test runtime::dispatcher -- --nocapture
rtk cargo test runtime::ipc -- --nocapture
rtk cargo test interfaces::cli -- --nocapture
rtk cargo test interfaces::telegram -- --nocapture
rtk cargo test --test install_script -- --nocapture
rtk cargo test --test serve_runtime -- --nocapture
```

Expected: all pass.

- [ ] **Step 4: Run whole-repository verification**

```sh
rtk cargo test
rtk cargo check
rtk cargo fmt --check
rtk cargo clippy --all-targets --all-features
rtk git diff --check
rtk git status --short
```

Expected: tests, check, and formatting pass; clippy has zero errors. If the known SQLite gateway test flakes, run it alone and rerun the remainder with:

```sh
rtk cargo test runtime::sqlite::gateway::tests::unresolved_chunk_blocks_only_its_own_response_suffix -- --nocapture
rtk cargo test -- --skip runtime::sqlite::gateway::tests::unresolved_chunk_blocks_only_its_own_response_suffix
```

- [ ] **Step 5: Commit documentation**

```sh
rtk git add README.md tests/install_script.rs
rtk git commit -m "docs(runtime): document actor administration"
```

- [ ] **Step 6: Request final code review**

Review the full implementation against `docs/superpowers/specs/2026-07-18-actor-administration-design.md`. Fix every Critical and Important finding, rerun the affected focused tests, and repeat the final verification before handing off.
