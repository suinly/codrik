# SQLite Actor Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make SQLite the only actor authorization source, create the first configured actor automatically, and remove `users.json` from runtime, installer, tests, and documentation.

**Architecture:** Add a focused `ActorStore` abstraction backed by an immediate SQLite transaction. Startup validates the configured actor ID, bootstraps it only when the actors table is empty, then performs the existing enabled-actor check. Legacy JSON authorization and its import schema are removed after runtime tests have moved to SQLite-native fixtures.

**Tech Stack:** Rust 2024, Tokio, tokio-rusqlite/SQLite, shell installer tests, Markdown.

## Global Constraints

- SQLite is the only source of truth for actors, identities, enabled state, and tool authorization.
- The first actor uses trimmed `runtime.actor_id`, `enabled = true`, `tools = ["*"]`, no identities, and the injected runtime timestamp.
- Bootstrap creates an actor only when the actors table is empty.
- A nonempty database never gains another actor through bootstrap.
- No interactive confirmation is added.
- `users.json`, legacy authorization import, and its hidden installer commands are removed.
- No migration for an old SQLite database is added.
- Shell commands in this repository must be prefixed with `rtk`.
- Implement every behavior test-first and commit each task separately.

---

### Task 1: Add the SQLite-Native Actor Store

**Files:**
- Modify: `src/runtime/model.rs`
- Modify: `src/runtime/store.rs`
- Create: `src/runtime/sqlite/actors.rs`
- Modify: `src/runtime/sqlite.rs`
- Modify: `src/runtime/sqlite/ingress.rs`
- Modify: `src/runtime/sqlite/local_ingress.rs`
- Modify: `src/runtime/ipc/server.rs`

**Interfaces:**
- Produces:

```rust
impl ActorId {
    pub fn parse_workspace_safe(value: &str) -> anyhow::Result<Self>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorBootstrapOutcome {
    Created,
    AlreadyInitialized,
}

#[async_trait]
pub trait ActorStore: Send + Sync {
    async fn ensure_initial_actor(
        &self,
        id: &ActorId,
        tools: &[String],
        now: Timestamp,
    ) -> anyhow::Result<ActorBootstrapOutcome>;

    async fn load_actor(&self, id: &ActorId)
        -> anyhow::Result<Option<RuntimeActor>>;

    async fn resolve_identity(
        &self,
        provider: &str,
        subject: &str,
    ) -> anyhow::Result<Option<RuntimeActor>>;
}
```

- Preserves temporarily: `RuntimeAuthorizationStore` and legacy import types
  so later tasks can migrate fixtures without mixing the bootstrap behavior
  with the mechanical cleanup. Remove `resolve_identity` from that legacy
  trait; it now belongs only to `ActorStore`.
- Changes: `LocalIngressStore` no longer owns `load_actor`; the method moves to `ActorStore`.

- [ ] **Step 1: Write failing actor ID validation tests**

Add focused tests in `src/runtime/model.rs`:

```rust
#[test]
fn workspace_actor_ids_trim_valid_values() -> anyhow::Result<()> {
    let actor = ActorId::parse_workspace_safe("  actor:local:owner  ")?;
    assert_eq!(actor.as_str(), "actor:local:owner");
    Ok(())
}

#[test]
fn workspace_actor_ids_reject_unsafe_values() {
    for value in ["", "   ", ".", "..", "actor/owner", r"actor\owner"] {
        assert!(
            ActorId::parse_workspace_safe(value).is_err(),
            "accepted unsafe actor id: {value:?}"
        );
    }
}
```

- [ ] **Step 2: Run the validation tests and verify RED**

Run:

```sh
rtk cargo test runtime::model::tests::workspace_actor_ids -- --nocapture
```

Expected: compilation fails because `ActorId::parse_workspace_safe` does not exist.

- [ ] **Step 3: Implement the actor ID constructor**

Add a dedicated `impl ActorId` after the ID macro invocations:

```rust
impl ActorId {
    pub fn parse_workspace_safe(value: &str) -> anyhow::Result<Self> {
        let value = value.trim();
        if value.is_empty()
            || value == "."
            || value == ".."
            || value.contains('/')
            || value.contains('\\')
        {
            anyhow::bail!("unsafe actor id for workspace path: {value}");
        }
        Ok(Self::from_string(value))
    }
}
```

- [ ] **Step 4: Verify actor ID tests are GREEN**

Run:

```sh
rtk cargo test runtime::model::tests::workspace_actor_ids -- --nocapture
```

Expected: both tests pass.

- [ ] **Step 5: Write failing SQLite bootstrap tests**

In the new `src/runtime/sqlite/actors.rs`, add tests for the intended API:

```rust
#[tokio::test]
async fn empty_store_bootstraps_enabled_actor_with_tools_and_timestamp() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let actor = ActorId::parse_workspace_safe(" actor:local:owner ")?;

    assert_eq!(
        store
            .ensure_initial_actor(&actor, &["*".to_string()], Timestamp(42))
            .await?,
        ActorBootstrapOutcome::Created
    );
    assert_eq!(
        store.load_actor(&actor).await?,
        Some(RuntimeActor {
            id: actor,
            enabled: true,
            tools: vec!["*".to_string()],
        })
    );
    assert_eq!(store.actor_created_at_for_test("actor:local:owner").await?, 42);
    Ok(())
}

#[tokio::test]
async fn bootstrap_is_idempotent_for_the_same_actor() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let actor = ActorId::parse_workspace_safe("actor:local:owner")?;

    assert_eq!(
        store.ensure_initial_actor(&actor, &["*".into()], Timestamp(1)).await?,
        ActorBootstrapOutcome::Created
    );
    assert_eq!(
        store.ensure_initial_actor(&actor, &["bash".into()], Timestamp(2)).await?,
        ActorBootstrapOutcome::AlreadyInitialized
    );
    assert_eq!(store.load_actor(&actor).await?.unwrap().tools, vec!["*"]);
    assert_eq!(store.actor_created_at_for_test(actor.as_str()).await?, 1);
    Ok(())
}

#[tokio::test]
async fn nonempty_store_does_not_bootstrap_a_different_actor() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let owner = ActorId::parse_workspace_safe("actor:local:owner")?;
    let typo = ActorId::parse_workspace_safe("actor:local:typo")?;
    store.ensure_initial_actor(&owner, &["*".into()], Timestamp(1)).await?;

    assert_eq!(
        store.ensure_initial_actor(&typo, &["*".into()], Timestamp(2)).await?,
        ActorBootstrapOutcome::AlreadyInitialized
    );
    assert!(store.load_actor(&typo).await?.is_none());
    assert_eq!(store.actor_count_for_test().await?, 1);
    Ok(())
}
```

Add the test-only probes in the same module:

```rust
#[cfg(test)]
impl SqliteRuntimeStore {
    async fn actor_count_for_test(&self) -> Result<i64> {
        self.connection
            .call(|connection| {
                connection.query_row("SELECT COUNT(*) FROM actors", [], |row| row.get(0))
            })
            .await
            .map_err(|error| anyhow::anyhow!("failed to count actors: {error}"))
    }

    async fn actor_created_at_for_test(&self, id: &str) -> Result<i64> {
        let id = id.to_string();
        self.connection
            .call(move |connection| {
                connection.query_row(
                    "SELECT created_at FROM actors WHERE id = ?1",
                    [id],
                    |row| row.get(0),
                )
            })
            .await
            .map_err(|error| anyhow::anyhow!("failed to load actor creation time: {error}"))
    }
}
```

- [ ] **Step 6: Run bootstrap tests and verify RED**

Run:

```sh
rtk cargo test runtime::sqlite::actors::tests -- --nocapture
```

Expected: compilation fails because `ActorStore` and `ensure_initial_actor` do not exist.

- [ ] **Step 7: Implement `ActorStore` atomically**

In `src/runtime/store.rs`, add `ActorBootstrapOutcome` and `ActorStore`. Remove
`load_actor` from `LocalIngressStore`.

In `src/runtime/sqlite/actors.rs`, implement:

```rust
#[async_trait]
impl ActorStore for SqliteRuntimeStore {
    async fn ensure_initial_actor(
        &self,
        id: &ActorId,
        tools: &[String],
        now: Timestamp,
    ) -> Result<ActorBootstrapOutcome> {
        let id = ActorId::parse_workspace_safe(id.as_str())?;
        let tools_json = serde_json::to_string(tools)?;
        self.connection
            .call(move |connection| -> Result<ActorBootstrapOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let count =
                    transaction.query_row("SELECT COUNT(*) FROM actors", [], |row| row.get::<_, i64>(0))?;
                if count != 0 {
                    return Ok(ActorBootstrapOutcome::AlreadyInitialized);
                }
                transaction.execute(
                    "INSERT INTO actors(id, enabled, tools_json, created_at)
                     VALUES (?1, 1, ?2, ?3)",
                    tokio_rusqlite::params![id.as_str(), tools_json, now.0],
                )?;
                transaction.commit()?;
                Ok(ActorBootstrapOutcome::Created)
            })
            .await
            .map_err(|error| anyhow::anyhow!("failed to bootstrap runtime actor: {error}"))
    }

    async fn load_actor(&self, id: &ActorId) -> Result<Option<RuntimeActor>> {
        let id = id.clone();
        self.connection
            .call(move |connection| -> Result<Option<RuntimeActor>> {
                let actor = connection
                    .query_row(
                        "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                        [id.as_str()],
                        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?;
                actor
                    .map(|(enabled, tools_json)| {
                        Ok(RuntimeActor {
                            id,
                            enabled,
                            tools: serde_json::from_str(&tools_json)?,
                        })
                    })
                    .transpose()
            })
            .await
            .map_err(super::map_call_error)
    }

    async fn resolve_identity(
        &self,
        provider: &str,
        subject: &str,
    ) -> Result<Option<RuntimeActor>> {
        let provider = provider.to_string();
        let subject = subject.to_string();
        self.connection
            .call(move |connection| -> Result<Option<RuntimeActor>> {
                let row = connection
                    .query_row(
                        "SELECT actors.id, actors.enabled, actors.tools_json
                         FROM identities
                         JOIN actors ON actors.id = identities.actor_id
                         WHERE identities.provider = ?1 AND identities.subject = ?2",
                        tokio_rusqlite::params![provider, subject],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, bool>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((actor_id, enabled, tools_json)) = row else {
                    return Ok(None);
                };
                Ok(Some(RuntimeActor {
                    id: ActorId::from_string(actor_id),
                    enabled,
                    tools: serde_json::from_str(&tools_json)?,
                }))
            })
            .await
            .map_err(|error| anyhow::anyhow!("failed to resolve runtime identity: {error}"))
    }
}
```

Register `mod actors;` in `src/runtime/sqlite.rs`. Remove `load_actor` from the
SQLite `LocalIngressStore` implementation and from the four test doubles in
`src/runtime/ipc/server.rs`.

- [ ] **Step 8: Verify the actor-store slice**

Run:

```sh
rtk cargo test runtime::sqlite::actors::tests -- --nocapture
rtk cargo test runtime::ipc::server -- --nocapture
rtk cargo check
```

Expected: all commands pass.

- [ ] **Step 9: Commit**

```sh
rtk git add src/runtime/model.rs src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/actors.rs src/runtime/sqlite/ingress.rs src/runtime/sqlite/local_ingress.rs src/runtime/ipc/server.rs
rtk git commit -m "feat(runtime): add SQLite actor bootstrap"
```

---

### Task 2: Bootstrap the Configured Actor During `serve`

**Files:**
- Modify: `src/app.rs`
- Modify: `tests/serve_runtime.rs`

**Interfaces:**
- Consumes: `ActorId::parse_workspace_safe`, `ActorStore::ensure_initial_actor`, `ActorStore::load_actor`.
- Changes startup trace: replace `StartupPhase::AuthImported` with `StartupPhase::ActorBootstrapped`.
- Keeps legacy import code temporarily unused; Task 3 deletes it and its fixtures.

- [ ] **Step 1: Write a failing startup test for a clean runtime**

Update the in-process harness in `tests/serve_runtime.rs` so it creates no
`users.json`. Add:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_runtime_bootstraps_configured_actor_without_users_file() -> Result<()> {
    let harness = InjectedHarness::start(vec![InjectedReply::Text("ready".into())]).await?;
    assert!(!harness.root.join("users.json").exists());

    let connection = tokio_rusqlite::Connection::open(&harness.database).await?;
    let actor = connection
        .call(|db| {
            db.query_row(
                "SELECT enabled, tools_json FROM actors WHERE id = ?1",
                [ACTOR],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?)),
            )
        })
        .await?;
    assert_eq!(actor, (true, r#"["*"]"#.to_string()));
    drop(harness);
    Ok(())
}
```

- [ ] **Step 2: Run the clean startup test and verify RED**

Run:

```sh
rtk cargo test --test serve_runtime clean_runtime_bootstraps_configured_actor_without_users_file -- --nocapture
```

Expected: startup fails with
`configured runtime actor actor:local:owner does not exist`.

- [ ] **Step 3: Replace legacy import with bootstrap**

In `src/app.rs`:

```rust
let actor_id = ActorId::parse_workspace_safe(&runtime.actor_id)?;
store
    .ensure_initial_actor(&actor_id, &["*".to_string()], clock.now())
    .await?;
trace.record(StartupPhase::ActorBootstrapped);
let actor = store.load_actor(&actor_id).await?.with_context(|| {
    format!("configured runtime actor {actor_id} does not exist")
})?;
```

Remove the `AuthorizationStore` import from startup and stop constructing the
actor ID with unchecked `ActorId::from_string`.

Change `actor_workspace_path_in` to call `ActorId::parse_workspace_safe` so the
same validation owns both startup and workspace behavior:

```rust
fn actor_workspace_path_in(home: &Path, actor_id: &str) -> Result<PathBuf> {
    let actor_id = ActorId::parse_workspace_safe(actor_id)?;
    Ok(home.join("workspaces").join(actor_id.as_str()))
}
```

- [ ] **Step 4: Update startup-order and clock tests**

In `src/app.rs`:

- delete `legacy_auth_marker_is_checked_before_reading_corrupt_file`;
- delete `failed_legacy_auth_parse_does_not_set_marker`;
- remove all `users.json` setup from clean startup tests;
- expect `ActorBootstrapped` after `Migrated`;
- assert the injected clock is the actor `created_at`.

- [ ] **Step 5: Add the nonempty-database safety test**

Seed a database with `actor:local:owner`, configure
`actor:local:typo`, run `serve_with_dependencies`, and assert:

```rust
assert!(error.to_string().contains(
    "configured runtime actor actor:local:typo does not exist"
));
assert_eq!(actor_count(&database).await?, 1);
```

This test must seed SQLite directly through `ActorStore`, not through
`users.json`.

- [ ] **Step 6: Verify startup behavior**

Run:

```sh
rtk cargo test app::tests -- --nocapture
rtk cargo test --test serve_runtime -- --nocapture
```

Expected: all app and serve-runtime tests pass, including disabled-actor
behavior seeded directly in SQLite.

- [ ] **Step 7: Commit**

```sh
rtk git add src/app.rs tests/serve_runtime.rs
rtk git commit -m "feat(runtime): bootstrap configured actor on serve"
```

---

### Task 3: Remove JSON Authorization and Legacy Runtime Fixtures

**Files:**
- Delete: `src/auth.rs`
- Modify: `src/lib.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/actors.rs`
- Modify: `src/runtime/sqlite/ingress.rs`
- Modify: `src/runtime/sqlite.rs`
- Modify: `src/runtime/migrations/0001_runtime.sql`
- Modify tests in:
  - `src/runtime/artifacts.rs`
  - `src/runtime/ipc/server.rs`
  - `src/runtime/runner.rs`
  - `src/runtime/service.rs`
  - `src/runtime/sqlite/checkpoint.rs`
  - `src/runtime/sqlite/dispatch.rs`
  - `src/runtime/sqlite/failures.rs`
  - `src/runtime/sqlite/ingress.rs`
  - `src/runtime/sqlite/local_ingress.rs`

**Interfaces:**
- Removes: `RuntimeAuthorizationStore`, `ImportOutcome`,
  `LegacyAuthorizationSnapshot`, `LegacyActor`, `LegacyIdentity`, and
  `pub mod auth`.
- Retains: `ActorStore`, including `resolve_identity`.
- Adds test-only SQLite helpers:

```rust
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestIdentity {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}

#[cfg(test)]
impl SqliteRuntimeStore {
    pub(crate) async fn seed_actor_for_test(
        &self,
        actor: RuntimeActor,
        identities: Vec<TestIdentity>,
        created_at: Timestamp,
    ) -> Result<()>;
}
```

- [ ] **Step 1: Write a failing schema test**

Extend `fresh_database_applies_v1_then_v2_with_foreign_key_integrity`:

```rust
assert!(!tables.contains(&"runtime_metadata".to_string()));
```

Add a source-tree guard in an appropriate test:

```rust
assert!(!include_str!("../lib.rs").contains("pub mod auth;"));
```

- [ ] **Step 2: Run the schema test and verify RED**

Run:

```sh
rtk cargo test runtime::sqlite::tests::fresh_database_applies_v1_then_v2_with_foreign_key_integrity -- --nocapture
```

Expected: failure because `runtime_metadata` still exists.

- [ ] **Step 3: Add SQLite-native test fixtures**

Implement `TestIdentity` and `seed_actor_for_test` under `#[cfg(test)]` in
`src/runtime/sqlite/actors.rs`. The helper must:

1. insert the supplied actor with its explicit enabled state and tools;
2. insert each identity referencing that actor;
3. do both within one immediate transaction;
4. never be compiled into production builds.

Use this helper when a test needs disabled actors, multiple actors, explicit
tool lists, or gateway identity resolution. Use `ensure_initial_actor` in tests
that need only one enabled actor with no identities.

- [ ] **Step 4: Convert every legacy snapshot fixture**

For each file listed above, replace patterns like:

```rust
store
    .import_legacy_authorization(
        LegacyAuthorizationSnapshot {
            version: 1,
            actors: vec![LegacyActor {
                id: "actor-1".into(),
                enabled: true,
                tools: vec!["*".into()],
                identities: vec![LegacyIdentity {
                    provider: "telegram".into(),
                    subject: "1".into(),
                    username: None,
                }],
            }],
        },
        Timestamp(1),
    )
    .await?;
```

with:

```rust
store
    .seed_actor_for_test(
        RuntimeActor {
            id: ActorId::from_string("actor-1"),
            enabled: true,
            tools: vec!["*".into()],
        },
        vec![TestIdentity {
            provider: "telegram".into(),
            subject: "1".into(),
            username: None,
        }],
        Timestamp(1),
    )
    .await?;
```

Delete tests whose only subject is import atomicity, import idempotence, or the
legacy marker. Preserve identity-resolution and unauthorized-ingress tests
using SQLite-native fixtures.

- [ ] **Step 5: Remove the legacy production code and schema**

- Delete `src/auth.rs`.
- Remove `pub mod auth;` from `src/lib.rs`.
- Delete `RuntimeAuthorizationStore` and `ImportOutcome` from
  `src/runtime/store.rs`.
- Delete the legacy trait implementation and marker probes from
  `src/runtime/sqlite/ingress.rs`.
- Remove `CREATE TABLE runtime_metadata` from
  `src/runtime/migrations/0001_runtime.sql`.

Do not add schema version 3 or a migration that drops the old table.

- [ ] **Step 6: Prove no legacy symbols remain**

Run:

```sh
rtk rg -n "users\\.json|AuthorizationStore|LegacyAuthorization|LegacyActor|LegacyIdentity|legacy_auth_imported|RuntimeAuthorizationStore|ImportOutcome|runtime_metadata" src
```

Expected: no matches.

- [ ] **Step 7: Verify the runtime suite**

Run:

```sh
rtk cargo test runtime:: -- --nocapture
rtk cargo check
```

Expected: all runtime tests pass and production code compiles without the auth
module.

- [ ] **Step 8: Commit**

```sh
rtk git add src/lib.rs src/runtime src/app.rs
rtk git rm src/auth.rs
rtk git commit -m "refactor(runtime): remove JSON authorization"
```

---

### Task 4: Remove `users.json` from the Installer and CLI

**Files:**
- Modify: `src/interfaces/cli.rs`
- Modify: `scripts/install.sh`
- Modify: `tests/install_script.rs`

**Interfaces:**
- Removes CLI commands:
  - `__installer_validate <config> <users>`;
  - `__installer_has_actors <users>`;
  - `__installer_validate_actor <users> <actor>`.
- Adds one config-only validator:

```text
codrik __installer_validate_config <config>
```

It loads the production YAML parser, validates `runtime.actor_id`, and prints
the trimmed actor ID. It never opens runtime state.

- [ ] **Step 1: Rewrite installer tests for the new contract**

Replace authorization-oriented tests in `tests/install_script.rs` with:

```rust
fn run_validator(config: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_codrik"))
        .arg("__installer_validate_config")
        .arg(config)
        .output()
        .unwrap()
}

#[test]
fn installer_validator_uses_production_config_parser() {
    let root = temp_dir();
    let config = root.join("config.yml");
    for yaml in [
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: actor:existing:7\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: \" actor:existing:7 \"\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: 'actor:existing:7'\n",
    ] {
        fs::write(&config, yaml).unwrap();
        let output = run_validator(&config);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8(output.stdout).unwrap(), "actor:existing:7\n");
    }
    for yaml in [
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: true\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: 7\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: null\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: '   '\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: '../owner'\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: first\n  actor_id: second\n",
    ] {
        fs::write(&config, yaml).unwrap();
        assert!(!run_validator(&config).status.success(), "accepted: {yaml}");
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn clean_install_writes_owner_config_without_users_json() {
    let root = temp_dir();
    let config_dir = root.join("config");
    let runtime_dir = root.join("runtime");
    let service = root.join("service-started");
    let output = run_library(
        r#"
is_interactive() { return 0; }
ask_yes_no() { return 0; }
ask_secret() { printf '%s\n' test-key; }
ask() { printf '%s\n' "$2"; }
SERVICE_MARKER="$4"
install_serve_service() { touch "$SERVICE_MARKER"; }
capture_install_state "$2" "$3"
configure_codrik "$2"
maybe_install_serve_service /opt/codrik
"#,
        &[&config_dir, &runtime_dir, &service],
    );
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let config = fs::read_to_string(config_dir.join("config.yml")).unwrap();
    assert!(config.contains("actor_id: actor:local:owner"));
    assert!(!runtime_dir.join("users.json").exists());
    assert!(service.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn kept_valid_config_allows_service_without_authorization_file() {
    let root = temp_dir();
    let config_dir = root.join("config");
    let runtime_dir = root.join("runtime");
    let service = root.join("service-started");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&runtime_dir).unwrap();
    let original = b"api_key: old\nbase_url: https://example.test/v1\nmodel: old\nruntime:\n  actor_id: 'actor:existing:7'\n";
    fs::write(config_dir.join("config.yml"), original).unwrap();
    let output = run_library(
        r#"
is_interactive() { return 0; }
ask_yes_no() { case "$1" in *Overwrite*) return 1 ;; *) return 0 ;; esac; }
SERVICE_MARKER="$4"
install_serve_service() { touch "$SERVICE_MARKER"; }
capture_install_state "$2" "$3"
configure_codrik "$2"
maybe_install_serve_service /opt/codrik
"#,
        &[&config_dir, &runtime_dir, &service],
    );
    assert!(output.status.success());
    assert_eq!(fs::read(config_dir.join("config.yml")).unwrap(), original);
    assert!(service.exists());
    assert!(!runtime_dir.join("users.json").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn kept_invalid_config_blocks_service() {
    let root = temp_dir();
    let config_dir = root.join("config");
    let runtime_dir = root.join("runtime");
    let service = root.join("service-started");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&runtime_dir).unwrap();
    let original = b"api_key: old\nruntime:\n  actor_id: '   '\n";
    fs::write(config_dir.join("config.yml"), original).unwrap();
    let output = run_library(
        r#"
is_interactive() { return 0; }
ask_yes_no() { case "$1" in *Overwrite*) return 1 ;; *) return 0 ;; esac; }
SERVICE_MARKER="$4"
install_serve_service() { touch "$SERVICE_MARKER"; }
capture_install_state "$2" "$3"
configure_codrik "$2"
maybe_install_serve_service /opt/codrik
"#,
        &[&config_dir, &runtime_dir, &service],
    );
    assert!(output.status.success());
    assert_eq!(fs::read(config_dir.join("config.yml")).unwrap(), original);
    assert!(!service.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn installer_source_contains_no_users_json_or_authorization_helpers() {
    for removed in [
        "users.json",
        "authorization_has_actors",
        "bootstrap_or_select_actor",
        "__installer_has_actors",
        "__installer_validate_actor",
    ] {
        assert!(!INSTALLER.contains(removed), "legacy installer text remains: {removed}");
    }
}
```

- [ ] **Step 2: Run installer tests and verify RED**

Run:

```sh
rtk cargo test --test install_script -- --nocapture
```

Expected: failures because the installer still creates and validates
`users.json`.

- [ ] **Step 3: Simplify the hidden CLI validation command**

In `src/interfaces/cli.rs`, replace the three installer variants with:

```rust
InstallerValidateConfig { config: PathBuf }
```

Dispatch:

```rust
CliCommand::InstallerValidateConfig { config } => {
    let config = AppConfig::load(config)?;
    let actor = ActorId::parse_workspace_safe(
        &config.required_runtime()?.actor_id,
    )?;
    println!("{actor}");
    Ok(())
}
```

Parse only:

```rust
"__installer_validate_config" => {
    let config = PathBuf::from(args.next().context("missing config path")?);
    Self::InstallerValidateConfig { config }
}
```

Remove `AuthorizationStore` imports and make unsupported internal commands
fail explicitly:

```rust
_ if command.starts_with("__installer_") => {
    bail!("unknown internal command: {command}")
}
```

Update CLI parser tests to assert this error for each removed command.

- [ ] **Step 4: Simplify `scripts/install.sh`**

Delete:

- `authorization_has_actors`;
- `installer_validate_actor`;
- `bootstrap_or_select_actor`;
- authorization guidance text;
- all `users_file` arguments and permission handling.

Change `installer_validate_config` to call:

```sh
"$validator" __installer_validate_config "$config_file"
```

For a new config, set:

```sh
actor_id="actor:local:owner"
```

For a kept config, set `CONFIGURED_RUNTIME_READY=1` only when the config-only
validator succeeds. Keep existing bytes unchanged.

Change the function prologue from two parameters to one:

```sh
configure_codrik() {
  config_dir="$1"
}
```

Change the main-script call to:

```sh
configure_codrik "$config_dir"
```

- [ ] **Step 5: Verify installer and CLI tests**

Run:

```sh
rtk cargo test interfaces::cli::tests -- --nocapture
rtk cargo test --test install_script -- --nocapture
```

Expected: all tests pass and no test creates `users.json`.

- [ ] **Step 6: Prove installer references are gone**

Run:

```sh
rtk rg -n "users\\.json|authorization_has_actors|bootstrap_or_select_actor|__installer_has_actors|__installer_validate_actor" scripts/install.sh src/interfaces/cli.rs
```

Expected: no matches.

- [ ] **Step 7: Commit**

```sh
rtk git add src/interfaces/cli.rs scripts/install.sh tests/install_script.rs
rtk git commit -m "refactor(installer): remove authorization file bootstrap"
```

---

### Task 5: Update Documentation and Run Full Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-14-codrik-serve-local-ipc-design.md`
- Modify: `docs/superpowers/plans/2026-07-14-codrik-serve-local-ipc.md`

**Interfaces:**
- Documentation states that `runtime.actor_id` names the actor selected by
  `serve`.
- An empty SQLite database creates that actor automatically with `tools: ["*"]`.
- A nonempty database never auto-creates a missing configured actor.
- Historical design and plan documents receive a short supersession note
  rather than having their original decision history silently rewritten.

- [ ] **Step 1: Add a failing documentation guard**

Add a test to `tests/install_script.rs` or an app-level documentation test:

```rust
#[test]
fn active_documentation_has_no_users_json_instructions() {
    let readme = include_str!("../README.md");
    assert!(!readme.contains("users.json"));
    assert!(readme.contains("automatically creates the first actor"));
    assert!(readme.contains(r#"tools: ["*"]"#));
}
```

- [ ] **Step 2: Run the documentation guard and verify RED**

Run:

```sh
rtk cargo test --test install_script active_documentation_has_no_users_json_instructions -- --nocapture
```

Expected: failure because README still documents `users.json`.

- [ ] **Step 3: Update README**

Replace installation and actor authorization text with:

```markdown
On a clean install, the installer writes `runtime.actor_id:
actor:local:owner`. The first `codrik serve` run automatically creates that
enabled actor in SQLite with standard-tool authorization `tools: ["*"]`.
```

Update the configuration table:

```markdown
| `runtime.actor_id` | For `serve` | None | Actor selected by the runtime; automatically created only when the actors table is empty. |
```

Replace “Actor authorization” with “Actor bootstrap” and document the
nonempty-database typo protection. Remove all legacy import errors.

- [ ] **Step 4: Add supersession notes to historical documents**

At the top of both 2026-07-14 documents, add:

```markdown
> Superseded for actor bootstrap by
> `docs/superpowers/specs/2026-07-16-sqlite-actor-bootstrap-design.md`.
> `users.json` and legacy authorization import are no longer implemented.
```

- [ ] **Step 5: Verify documentation and source cleanup**

Run:

```sh
rtk cargo test --test install_script active_documentation_has_no_users_json_instructions -- --nocapture
rtk rg -n "users\\.json|legacy_auth_imported|AuthorizationStore|LegacyAuthorization" README.md src scripts
```

Expected: documentation test passes and the search returns no matches.

- [ ] **Step 6: Run full verification**

Run:

```sh
rtk cargo fmt --check
rtk cargo check
rtk cargo test
rtk cargo clippy --all-targets --all-features -- -D warnings
rtk git diff --check
```

Expected: every command succeeds with no warnings.

- [ ] **Step 7: Commit**

```sh
rtk git add README.md tests/install_script.rs docs/superpowers/specs/2026-07-14-codrik-serve-local-ipc-design.md docs/superpowers/plans/2026-07-14-codrik-serve-local-ipc.md
rtk git commit -m "docs(runtime): document automatic actor bootstrap"
```

- [ ] **Step 8: Record the local reset handoff**

Do not delete user data automatically during tests or implementation. In the
final handoff, state that the approved no-migration development reset is:

```sh
rtk rm -f ~/.codrik/runtime.sqlite ~/.codrik/runtime.sqlite-wal ~/.codrik/runtime.sqlite-shm
```

Run it only if the user explicitly asks the implementation session to perform
the destructive reset.
