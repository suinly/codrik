# Identity Linking Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add short-lived one-time identity-link codes backed by SQLite, expose issuance through local IPC and `codrik link`, and provide a gateway-ready redemption service.

**Architecture:** Runtime schema v3 stores only code hashes and per-identity failure windows. A transactional `IdentityLinkStore` is wrapped by an `IdentityLinkService` that owns normalization, hashing, random generation, collision retries, and public outcomes. Local IPC depends on an object-safe linking manager; gateways can later use the same manager without depending on SQLite.

**Tech Stack:** Rust 2024, Tokio, tokio-rusqlite/SQLite, SHA-256, getrandom 0.3, Unix-socket IPC, serde.

## Global Constraints

- Link codes contain exactly eight symbols from `23456789ABCDEFGHJKMNPQRSTUVWXYZ`.
- Display codes as `ABCD-EFGH`; accept grouped, ungrouped, lowercase, and ASCII whitespace.
- Codes expire after exactly 10 minutes and are invalid when `now >= expires_at`.
- One active code exists per actor; reissue atomically revokes the previous code.
- Persist only `SHA256("codrik-identity-link-v1\0" || normalized_code)`.
- Five invalid attempts block one verified `(provider, subject)` for 10 minutes.
- Identity redemption never transfers an identity between actors.
- Linking operations create no actor events, memory, work items, runs, outbox rows, bundles, or client recovery metadata.
- Existing protocol variants remain version 1 and unchanged.
- Telegram/webhook code is outside this plan.
- All shell commands must use `rtk`.
- Follow RED → GREEN → REFACTOR for every behavior and commit each task.

---

### Task 1: Add Runtime Schema Version 3

**Files:**
- Create: `src/runtime/migrations/0003_identity_linking.sql`
- Modify: `src/runtime/sqlite.rs`

**Interfaces:**
- Produces schema tables `identity_link_codes` and `identity_link_attempts`.
- Produces `migrate_to_v3(connection: &mut rusqlite::Connection) -> Result<()>`.
- Changes supported `PRAGMA user_version` from `2` to `3`.

- [ ] **Step 1: Write failing migration tests**

In `src/runtime/sqlite.rs`, add probes and tests:

```rust
#[tokio::test]
async fn fresh_database_applies_identity_linking_schema_v3() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let (foreign_keys, tables) = store.schema_probe().await?;
    assert!(foreign_keys);
    assert!(tables.contains(&"identity_link_codes".to_string()));
    assert!(tables.contains(&"identity_link_attempts".to_string()));
    assert_eq!(store.user_version_for_test().await?, 3);
    Ok(())
}

#[tokio::test]
async fn v2_to_v3_preserves_actor_and_identity_rows() -> Result<()> {
    let db = TempDb::new("identity-link-v3");
    seed_v2_actor_and_identity(db.path()).await?;
    let store = SqliteRuntimeStore::open(db.path()).await?;
    assert_eq!(store.user_version_for_test().await?, 3);
    assert_eq!(store.scalar_for_test("SELECT COUNT(*) FROM actors").await?, 1);
    assert_eq!(store.scalar_for_test("SELECT COUNT(*) FROM identities").await?, 1);
    Ok(())
}
```

`seed_v2_actor_and_identity` must execute migrations 0001 and 0002, set
`user_version = 2`, and insert one actor plus one identity.

- [ ] **Step 2: Verify RED**

Run:

```sh
rtk cargo test runtime::sqlite::tests -- --nocapture
```

Expected: tests fail because schema version 3 and its tables do not exist.

- [ ] **Step 3: Add migration SQL**

Create `src/runtime/migrations/0003_identity_linking.sql`:

```sql
CREATE TABLE identity_link_codes (
    actor_id TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
    code_hash BLOB NOT NULL UNIQUE CHECK(length(code_hash) = 32),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL CHECK(expires_at > created_at)
) STRICT;

CREATE TABLE identity_link_attempts (
    provider TEXT NOT NULL,
    subject TEXT NOT NULL,
    window_started_at INTEGER NOT NULL,
    failure_count INTEGER NOT NULL CHECK(failure_count BETWEEN 1 AND 5),
    blocked_until INTEGER,
    PRIMARY KEY(provider, subject)
) STRICT;
```

- [ ] **Step 4: Wire migration version 3**

Add:

```rust
const IDENTITY_LINKING_MIGRATION: &str =
    include_str!("migrations/0003_identity_linking.sql");

fn migrate_to_v3(connection: &mut rusqlite::Connection) -> Result<()> {
    let transaction =
        connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(IDENTITY_LINKING_MIGRATION)?;
    let violations = transaction
        .prepare("PRAGMA foreign_key_check")?
        .query([])?
        .mapped(|row| row.get::<_, String>(0))
        .count();
    if violations != 0 {
        anyhow::bail!("schema v3 migration left {violations} foreign key violations");
    }
    transaction.execute_batch("PRAGMA user_version = 3;")?;
    transaction.commit()?;
    Ok(())
}
```

Initialization order:

```rust
match version {
    0 => {
        // Apply v1, then v2, then v3.
    }
    1 => {
        migrate_to_v2(connection)?;
        migrate_to_v3(connection)?;
    }
    2 => migrate_to_v3(connection)?,
    3 => {}
    other => anyhow::bail!("unsupported runtime schema version: {other}"),
}
```

- [ ] **Step 5: Verify migration GREEN**

Run:

```sh
rtk cargo test runtime::sqlite::tests -- --nocapture
rtk cargo check
```

Expected: SQLite tests pass and schema version is 3.

- [ ] **Step 6: Commit**

```sh
rtk git add src/runtime/migrations/0003_identity_linking.sql src/runtime/sqlite.rs
rtk git commit -m "feat(identity): add linking schema"
```

---

### Task 2: Implement the Transactional Identity Link Store

**Files:**
- Modify: `src/runtime/store.rs`
- Create: `src/runtime/sqlite/identity_link.rs`
- Modify: `src/runtime/sqlite.rs`

**Interfaces:**
- Produces exactly:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkIdentity {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreLinkCodeReplacement {
    Stored,
    HashCollision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreLinkRedemption {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict { actor_id: ActorId },
}

#[async_trait]
pub trait IdentityLinkStore: Send + Sync {
    async fn replace_link_code(
        &self,
        actor: &ActorId,
        code_hash: [u8; 32],
        created_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<StoreLinkCodeReplacement>;

    async fn redeem_link_code(
        &self,
        identity: LinkIdentity,
        code_hash: Option<[u8; 32]>,
        now: Timestamp,
    ) -> Result<StoreLinkRedemption>;

    async fn collect_expired_link_state(
        &self,
        now: Timestamp,
        limit: usize,
    ) -> Result<usize>;
}
```

- [ ] **Step 1: Write failing issue/replacement tests**

Create `src/runtime/sqlite/identity_link.rs` tests for:

```rust
#[tokio::test]
async fn replacement_revokes_previous_code_for_actor() -> Result<()> {
    let (store, actor) = enabled_actor_store().await?;
    assert_eq!(
        store.replace_link_code(&actor, [1; 32], Timestamp(10), Timestamp(610)).await?,
        StoreLinkCodeReplacement::Stored
    );
    assert_eq!(
        store.replace_link_code(&actor, [2; 32], Timestamp(20), Timestamp(620)).await?,
        StoreLinkCodeReplacement::Stored
    );
    assert_eq!(store.code_hashes_for_test().await?, vec![vec![2; 32]]);
    Ok(())
}

#[tokio::test]
async fn duplicate_hash_for_another_actor_reports_collision() -> Result<()> {
    let store = two_actor_store().await?;
    let actors = store.actor_ids_for_test().await?;
    store.replace_link_code(&actors[0], [7; 32], Timestamp(1), Timestamp(601)).await?;
    assert_eq!(
        store.replace_link_code(&actors[1], [7; 32], Timestamp(2), Timestamp(602)).await?,
        StoreLinkCodeReplacement::HashCollision
    );
    Ok(())
}
```

Also test missing and disabled actors return errors without rows.

- [ ] **Step 2: Verify issue RED**

Run:

```sh
rtk cargo test runtime::sqlite::identity_link::tests -- --nocapture
```

Expected: compilation fails because `IdentityLinkStore` does not exist.

- [ ] **Step 3: Add store domain types and issue implementation**

Add the interfaces to `src/runtime/store.rs`, register
`mod identity_link;`, and implement `replace_link_code` with:

```sql
SELECT enabled FROM actors WHERE id = ?1;

INSERT INTO identity_link_codes(actor_id, code_hash, created_at, expires_at)
VALUES (?1, ?2, ?3, ?4)
ON CONFLICT(actor_id) DO UPDATE SET
    code_hash = excluded.code_hash,
    created_at = excluded.created_at,
    expires_at = excluded.expires_at;
```

Map only the unique `identity_link_codes.code_hash` constraint to
`HashCollision`; propagate every other SQLite failure.

- [ ] **Step 4: Write failing redemption tests**

Add focused tests:

```rust
#[tokio::test]
async fn valid_code_links_identity_once_and_routes_resolution() -> Result<()> {
    let (store, actor) = enabled_actor_store().await?;
    store
        .replace_link_code(&actor, [3; 32], Timestamp(10), Timestamp(610))
        .await?;
    let identity = LinkIdentity {
        provider: "telegram".into(),
        subject: "123".into(),
        username: Some("owner".into()),
    };
    assert_eq!(
        store
            .redeem_link_code(identity, Some([3; 32]), Timestamp(11))
            .await?,
        StoreLinkRedemption::Linked {
            actor_id: actor.clone()
        }
    );
    assert_eq!(
        store.resolve_identity("telegram", "123").await?.unwrap().id,
        actor
    );
    assert_eq!(store.code_count_for_test().await?, 0);
    Ok(())
}

#[tokio::test]
async fn code_is_expired_at_exact_expiry() -> Result<()> {
    let (store, actor) = enabled_actor_store().await?;
    store
        .replace_link_code(&actor, [4; 32], Timestamp(10), Timestamp(610))
        .await?;
    assert_eq!(
        store
            .redeem_link_code(link_identity("123", None), Some([4; 32]), Timestamp(610))
            .await?,
        StoreLinkRedemption::InvalidOrExpired
    );
    assert!(store.resolve_identity("telegram", "123").await?.is_none());
    Ok(())
}

#[tokio::test]
async fn same_actor_redemption_is_idempotent_and_preserves_missing_username() -> Result<()> {
    let (store, actor) = store_with_linked_identity("123", Some("owner")).await?;
    store
        .replace_link_code(&actor, [5; 32], Timestamp(10), Timestamp(610))
        .await?;
    assert_eq!(
        store
            .redeem_link_code(link_identity("123", None), Some([5; 32]), Timestamp(11))
            .await?,
        StoreLinkRedemption::AlreadyLinked {
            actor_id: actor.clone()
        }
    );
    assert_eq!(
        store.identity_username_for_test("telegram", "123").await?,
        Some("owner".into())
    );
    assert_eq!(store.code_count_for_test().await?, 0);
    Ok(())
}

#[tokio::test]
async fn different_actor_identity_conflict_does_not_consume_code() -> Result<()> {
    let (store, code_actor, identity_actor) = conflicting_identity_store().await?;
    store
        .replace_link_code(&code_actor, [6; 32], Timestamp(10), Timestamp(610))
        .await?;
    assert_eq!(
        store
            .redeem_link_code(link_identity("123", None), Some([6; 32]), Timestamp(11))
            .await?,
        StoreLinkRedemption::IdentityConflict {
            actor_id: identity_actor
        }
    );
    assert_eq!(store.code_count_for_test().await?, 1);
    Ok(())
}

#[tokio::test]
async fn fifth_invalid_attempt_blocks_identity_for_ten_minutes() -> Result<()> {
    let (store, _) = enabled_actor_store().await?;
    for now in 0..5 {
        assert_eq!(
            store
                .redeem_link_code(
                    link_identity("attacker", None),
                    None,
                    Timestamp(now),
                )
                .await?,
            StoreLinkRedemption::InvalidOrExpired
        );
    }
    assert_eq!(
        store
            .redeem_link_code(
                link_identity("attacker", None),
                None,
                Timestamp(5),
            )
            .await?,
        StoreLinkRedemption::RateLimited {
            retry_at: Timestamp(600_004)
        }
    );
    Ok(())
}

#[tokio::test]
async fn successful_redemption_clears_failure_state() -> Result<()> {
    let (store, actor) = enabled_actor_store().await?;
    let identity = link_identity("123", Some("owner"));
    store
        .redeem_link_code(identity.clone(), None, Timestamp(1))
        .await?;
    store
        .replace_link_code(&actor, [8; 32], Timestamp(2), Timestamp(602))
        .await?;
    assert!(matches!(
        store
            .redeem_link_code(identity, Some([8; 32]), Timestamp(3))
            .await?,
        StoreLinkRedemption::Linked { .. }
    ));
    assert_eq!(
        store.attempt_count_for_test("telegram", "123").await?,
        0
    );
    Ok(())
}
```

Define `link_identity`, `enabled_actor_store`, `store_with_linked_identity`,
`conflicting_identity_store`, and the direct read-only test probes in the same
module. They must seed actors through the existing test-only
`seed_actors_for_test` helper and must not bypass the production redemption
method for the behavior under test.

- [ ] **Step 5: Verify redemption RED**

Run:

```sh
rtk cargo test runtime::sqlite::identity_link::tests -- --nocapture
```

Expected: issue tests pass; redemption tests fail because the method is not implemented.

- [ ] **Step 6: Implement redemption transaction**

Inside one immediate transaction:

1. Reject blank provider or subject with `bail!`.
2. Load attempt state and return `RateLimited` when `now < blocked_until`.
3. If `code_hash` is `Some`, select code joined to `actors` with
   `actors.enabled = 1` and `expires_at > now`.
4. On no matching code, upsert failure state:
   - reset to count 1 when `now >= window_started_at + 600_000` or a block expired;
   - increment inside the window;
   - on count 5 set `blocked_until = now + 600_000`;
   - return `InvalidOrExpired`.
5. Load identity by provider and subject.
6. Different actor: return `IdentityConflict` without deleting code.
7. Same actor: update username only when `Some`, delete code and attempts,
   return `AlreadyLinked`.
8. Missing identity: insert it, delete code and attempts, return `Linked`.
9. Commit before returning a successful or failed-attempt mutation.

- [ ] **Step 7: Implement bounded cleanup**

Delete at most `limit` code rows with `expires_at <= now`, then at most the
remaining limit attempt rows where:

```sql
(blocked_until IS NOT NULL AND blocked_until <= ?1)
OR (blocked_until IS NULL AND window_started_at + 600000 <= ?1)
```

Return the combined deleted count. `limit == 0` performs no work.

- [ ] **Step 8: Verify store GREEN**

Run:

```sh
rtk cargo test runtime::sqlite::identity_link::tests -- --nocapture
rtk cargo test runtime::sqlite::ingress::tests -- --nocapture
rtk cargo check
```

Expected: all identity-link and ingress tests pass.

- [ ] **Step 9: Commit**

```sh
rtk git add src/runtime/store.rs src/runtime/sqlite.rs src/runtime/sqlite/identity_link.rs
rtk git commit -m "feat(identity): persist linking state"
```

---

### Task 3: Add Code Generation and Identity Linking Service

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `src/runtime/identity_link.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Add dependency: `getrandom = "0.3.4"`.
- Produces:

```rust
pub const LINK_CODE_TTL_MILLIS: i64 = 600_000;
pub const LINK_CODE_ALPHABET: &[u8] =
    b"23456789ABCDEFGHJKMNPQRSTUVWXYZ";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IssuedLinkCode {
    pub code: String,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkRedemption {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict,
}

pub trait LinkCodeGenerator: Send + Sync {
    fn generate(&self) -> Result<String>;
}

#[async_trait]
pub trait IdentityLinkManager: Send + Sync {
    async fn issue_code(&self, actor: &ActorId) -> Result<IssuedLinkCode>;
    async fn redeem_code(&self, identity: LinkIdentity, code: &str)
        -> Result<LinkRedemption>;
    async fn collect_expired(&self, limit: usize) -> Result<usize>;
}
```

- [ ] **Step 1: Write failing normalization and hash tests**

Tests in `src/runtime/identity_link.rs`:

```rust
#[test]
fn normalization_accepts_supported_forms() {
    for raw in ["ABCD-EFGH", "abcdefgh", "  ABCD EFGH\n"] {
        assert_eq!(normalize_link_code(raw).as_deref(), Some("ABCDEFGH"));
    }
}

#[test]
fn normalization_rejects_length_and_alphabet_errors() {
    for raw in ["ABC", "ABCDEFGHI", "ABCD0FGH", "ABCD/FGH"] {
        assert!(normalize_link_code(raw).is_none(), "{raw}");
    }
}

#[test]
fn grouped_code_and_domain_separated_hash_are_stable() {
    assert_eq!(group_link_code("ABCDEFGH"), "ABCD-EFGH");
    assert_eq!(
        hash_link_code("ABCDEFGH"),
        Sha256::digest(b"codrik-identity-link-v1\0ABCDEFGH").into()
    );
}
```

- [ ] **Step 2: Verify normalization RED**

Run:

```sh
rtk cargo test runtime::identity_link::tests -- --nocapture
```

Expected: module and functions are missing.

- [ ] **Step 3: Implement normalization, grouping, and hashing**

Normalization removes only ASCII whitespace and `-`, uppercases ASCII letters,
requires eight symbols, and checks membership in `LINK_CODE_ALPHABET`.

- [ ] **Step 4: Write failing generator and service tests**

Add:

```rust
#[test]
fn system_generator_returns_eight_allowed_symbols() -> Result<()> {
    for _ in 0..128 {
        let code = SystemLinkCodeGenerator.generate()?;
        assert_eq!(code.len(), 8);
        assert!(code.bytes().all(|byte| LINK_CODE_ALPHABET.contains(&byte)));
    }
    Ok(())
}

#[tokio::test]
async fn issue_retries_hash_collision_and_returns_grouped_code() -> Result<()> {
    let store = ScriptedLinkStore::replacements([
        StoreLinkCodeReplacement::HashCollision,
        StoreLinkCodeReplacement::Stored,
    ]);
    let generator = SequenceGenerator::new(["ABCDEFGH", "MNPQRSTU"]);
    let service = IdentityLinkService::new(
        store.clone(),
        ManualClock::new(100),
        generator,
    );
    assert_eq!(
        service.issue_code(&ActorId::from_string("actor")).await?,
        IssuedLinkCode {
            code: "MNPQ-RSTU".into(),
            expires_at: Timestamp(600_100),
        }
    );
    assert_eq!(store.replacement_calls(), 2);
    Ok(())
}

#[tokio::test]
async fn invalid_syntax_redeems_with_no_hash() -> Result<()> {
    let store = ScriptedLinkStore::default();
    let service = test_service(store.clone());
    assert_eq!(
        service
            .redeem_code(link_identity(), "bad/code")
            .await?,
        LinkRedemption::InvalidOrExpired
    );
    assert_eq!(store.last_redeem_hash(), None);
    Ok(())
}
```

Also test five collision outcomes fail without returning a code and store
redemption outcomes map without exposing the conflicting actor ID.

- [ ] **Step 5: Implement unbiased system generation**

Use `getrandom::fill` and rejection sampling. For a 31-symbol alphabet, accept
random bytes below `248` (`31 * 8`) and map `byte % 31` until eight symbols are
filled. Never use direct modulo on all 256 values.

- [ ] **Step 6: Implement `IdentityLinkService<S, C, G>`**

The service stores `store`, `clock`, and `generator`. `issue_code` retries only
`HashCollision` up to five generated values. `redeem_code` passes `None` for
invalid syntax and maps `IdentityConflict { .. }` to public
`LinkRedemption::IdentityConflict`. `collect_expired` delegates with
`clock.now()`.

- [ ] **Step 7: Verify service GREEN**

Run:

```sh
rtk cargo test runtime::identity_link::tests -- --nocapture
rtk cargo check
```

Expected: all service tests pass.

- [ ] **Step 8: Commit**

```sh
rtk git add Cargo.toml Cargo.lock src/runtime.rs src/runtime/identity_link.rs
rtk git commit -m "feat(identity): add linking service"
```

---

### Task 4: Extend Local IPC with Link-Code Issuance

**Files:**
- Modify: `src/runtime/ipc/protocol.rs`
- Modify: `src/runtime/ipc/client.rs`
- Modify: `src/runtime/ipc/server.rs`
- Modify: `src/app.rs`

**Interfaces:**
- Consumes: `IdentityLinkManager`, `IssuedLinkCode`.
- Adds:

```rust
ClientRequestBody::IssueLinkCode { request_id: RequestId }

ServerEventBody::LinkCodeIssued {
    request_id: RequestId,
    code: String,
    expires_at: i64,
}

LocalIpcClient::issue_link_code(
    &self,
    request_id: RequestId,
) -> Result<IssuedLinkCode>
```

- Existing `LocalIpcServer` constructors remain source-compatible.
- Adds builder:

```rust
pub fn with_identity_linking(
    self,
    linking: Arc<dyn IdentityLinkManager>,
) -> Self;
```

- [ ] **Step 1: Write failing protocol tests**

Add round-trip tests for both new variants and malformed JSON tests proving:

- missing `request_id` fails;
- unknown fields fail;
- invalid UUID fails;
- blank or malformed response codes fail validation;
- `expires_at` must be positive.

- [ ] **Step 2: Verify protocol RED**

Run:

```sh
rtk cargo test runtime::ipc::protocol::tests -- --nocapture
```

Expected: compilation fails because variants are missing.

- [ ] **Step 3: Add variants and validation**

Update `validate_client_body`, `validate_server_body`, request UUID validation,
and the server `request_id` helper. Validate response code by calling the
identity-link normalizer and requiring already grouped canonical form.

- [ ] **Step 4: Write failing client issuance tests**

Test a server that receives `IssueLinkCode`, returns `LinkCodeIssued`, and
assert `LocalIpcClient::issue_link_code` returns the typed value. Test
`RequestError`, unexpected response, mismatched request ID, and EOF.

- [ ] **Step 5: Implement client issuance**

Open one operation, close the write half, read one event, require matching
`request_id`, and map the terminal response into `IssuedLinkCode`.

- [ ] **Step 6: Write failing server issuance tests**

Construct a server with a fake `IdentityLinkManager`, send the new request, and
assert:

- manager receives the configured actor;
- response contains its code and expiry;
- manager errors become `RequestError` with code `link_code_failed`;
- a server without linking manager returns `linking_unavailable`;
- no ingress or outbox methods are called.

- [ ] **Step 7: Implement optional server linking manager**

Add `linking: Option<Arc<dyn IdentityLinkManager>>` to server and connection
handler. Existing constructors initialize `None`; the builder sets `Some`.
Handle issuance before submit/resume paths and close the sink after one
terminal response.

- [ ] **Step 8: Wire production service in `app.rs`**

Construct:

```rust
let identity_linking: Arc<dyn IdentityLinkManager> = Arc::new(
    IdentityLinkService::new(
        store.clone(),
        clock.clone(),
        SystemLinkCodeGenerator,
    ),
);
```

Attach it with `.with_identity_linking(identity_linking.clone())`. Change
startup log `schema_version` from `2` to `3`.

- [ ] **Step 9: Verify IPC GREEN**

Run:

```sh
rtk cargo test runtime::ipc::protocol::tests -- --nocapture
rtk cargo test runtime::ipc::client::tests -- --nocapture
rtk cargo test runtime::ipc::server::tests -- --nocapture
rtk cargo test app::tests -- --nocapture
```

Expected: all IPC and app tests pass.

- [ ] **Step 10: Commit**

```sh
rtk git add src/runtime/ipc/protocol.rs src/runtime/ipc/client.rs src/runtime/ipc/server.rs src/app.rs
rtk git commit -m "feat(identity): issue link codes over IPC"
```

---

### Task 5: Add `codrik link`

**Files:**
- Modify: `src/interfaces/cli.rs`
- Modify: `README.md`

**Interfaces:**
- Adds `CliCommand::Link`.
- `codrik link` uses the configured socket and does not instantiate
  `RequestMetadataStore`.
- Output is exactly:

```text
Link code: ABCD-EFGH
Expires in 10 minutes.
In the new channel, send: /link ABCD-EFGH
```

- [ ] **Step 1: Write failing parser and rendering tests**

Add:

```rust
assert_eq!(parse(&["link"])?, CliCommand::Link);
assert!(parse(&["link", "extra"]).is_err());
```

Add an async CLI helper test with a fake IPC server returning a code. Assert
stdout matches the exact three lines and the client request directory remains
absent.

- [ ] **Step 2: Verify CLI RED**

Run:

```sh
rtk cargo test interfaces::cli::tests -- --nocapture
```

Expected: `CliCommand::Link` and the execution helper are missing.

- [ ] **Step 3: Implement link command**

Add:

```rust
async fn link() -> Result<()> {
    let config = AppConfig::load_default()?;
    let paths = config.required_runtime()?.resolve_paths(&codrik_dir()?)?;
    let issued = LocalIpcClient::new(paths.socket)
        .issue_link_code(RequestId::new())
        .await?;
    println!("Link code: {}", issued.code);
    println!("Expires in 10 minutes.");
    println!("In the new channel, send: /link {}", issued.code);
    Ok(())
}
```

For testability, extract a writer-based helper receiving `&LocalIpcClient`.
Do not call `local_context`, because it constructs recovery metadata state.

- [ ] **Step 4: Update README command documentation**

Add:

````markdown
Issue a one-time identity-link code:

```sh
codrik link
```

The code is valid for 10 minutes. Sending `/link CODE` through a supported
private gateway links that verified channel identity to the same actor.
````

- [ ] **Step 5: Verify CLI GREEN**

Run:

```sh
rtk cargo test interfaces::cli::tests -- --nocapture
rtk cargo test --test install_script -- --nocapture
```

Expected: CLI and active-documentation tests pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/interfaces/cli.rs README.md
rtk git commit -m "feat(cli): add identity link command"
```

---

### Task 6: Add Cleanup, Integration Coverage, and Final Verification

**Files:**
- Modify: `src/app.rs`
- Modify: `tests/serve_runtime.rs`
- Modify: `src/runtime/observability.rs`

**Interfaces:**
- Adds a supervised `identity-link-gc` component using the existing 300-second
  garbage-collection interval.
- Uses `IdentityLinkManager::collect_expired(256)`.
- Adds no code, hash, or full subject fields to `RuntimeLogEvent`.

- [ ] **Step 1: Write failing GC and end-to-end tests**

Add app tests proving the link GC loop:

- invokes cleanup after the interval;
- propagates store authority failure;
- exits on shutdown.

Add a `tests/serve_runtime.rs` scenario:

1. Start a clean runtime.
2. Send raw `IssueLinkCode`.
3. Assert canonical response and expiry.
4. Inspect SQLite: one hash row, no plaintext code bytes.
5. Assert counts remain zero for events, work items, runs, outbox,
   result bundles, and local requests.
6. Issue again and assert the first code hash is replaced.

- [ ] **Step 2: Verify integration RED**

Run:

```sh
rtk cargo test app::tests::identity_link_gc -- --nocapture
rtk cargo test --test serve_runtime identity_link -- --nocapture
```

Expected: GC component and integration scenario are missing.

- [ ] **Step 3: Implement supervised cleanup**

At startup, call `identity_linking.collect_expired(256).await?`. Add:

```rust
async fn run_identity_link_gc(
    manager: Arc<dyn IdentityLinkManager>,
    mut shutdown: watch::Receiver<bool>,
    interval: Duration,
) -> Result<()> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(interval) => {
                manager.collect_expired(256).await?;
            }
        }
    }
}
```

Register it as `identity-link-gc` in `ServeRuntime`.

- [ ] **Step 4: Add observability redaction guard**

Extend observability forbidden fields and fixture strings with:

```rust
"link_code",
"code_hash",
"identity_subject",
"/link ABCD-EFGH",
```

No new log payload fields are required.

- [ ] **Step 5: Verify complete feature**

Run:

```sh
rtk cargo fmt --check
rtk cargo check
rtk cargo test
rtk cargo clippy --all-targets --all-features
rtk sh -n scripts/install.sh
rtk git diff --check
```

Expected: all commands exit successfully. Existing repository-wide Clippy
warnings may remain warnings; no new warning may originate from changed code.

- [ ] **Step 6: Manual runtime verification**

Start:

```sh
rtk cargo run -- serve
```

In another terminal run:

```sh
rtk cargo run -- link
```

Expected: a canonical code and three-line instructions. Stop `serve` cleanly.
Verify:

```sh
rtk sqlite3 ~/.codrik/runtime.sqlite \
  "SELECT length(code_hash), expires_at > created_at FROM identity_link_codes;"
```

Expected: `32|1`.

- [ ] **Step 7: Commit**

```sh
rtk git add src/app.rs src/runtime/observability.rs tests/serve_runtime.rs
rtk git commit -m "test(identity): verify linking runtime"
```
