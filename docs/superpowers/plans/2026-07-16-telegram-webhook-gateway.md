# Telegram Webhook Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one private-chat Telegram webhook gateway to `codrik serve`, including identity linking, durable text ingress, best-effort streaming, and durable final text/file delivery.

**Architecture:** Telegram is a thin adapter around new gateway-neutral `DeliveryRoute`, command-idempotency, activity, and durable-delivery boundaries. SQLite remains authoritative; webhook requests return success only after required state commits, while a supervised Telegram worker performs Bot API side effects independently from the actor loop.

**Tech Stack:** Rust 2024, Tokio, SQLite via `tokio-rusqlite`, Axum 0.8, Reqwest 0.13 with multipart streaming, Serde, SHA-256, existing supervisor/dispatcher/runner.

## Global Constraints

- Work directly on the current branch selected by the user; do not create a worktree.
- Run every shell command through `rtk`.
- Follow TDD: add a focused failing test, observe RED, implement the minimum behavior, then observe GREEN.
- Telegram is optional and supports exactly one bot per runtime.
- The embedded listener binds locally; TLS belongs to the reverse proxy.
- Only private, non-bot Telegram text messages are accepted.
- Incoming attachments, groups, channels, callback queries, and multiple bots remain out of scope.
- Telegram private ingress uses `Audience::ActorPrivate`; reply routing is stored separately.
- Link commands never create events, work items, runs, model input, or memory.
- Plaintext link codes are never persisted.
- Webhook body and Telegram API response limits are 1 MiB.
- Webhook connection limit is 64.
- API connect timeout is 5 seconds; text/edit timeout is 30 seconds; upload timeout is 120 seconds.
- Final text chunks contain at most 4096 Unicode scalar values; captions contain at most 1024.
- Streaming edits occur at most once per second and remain best effort.
- Durable delivery uses claim batch 32, global concurrency 4, 30-second claims, and 10-second renewals.
- Retry backoff starts at 1 second and caps at 5 minutes; Telegram `429 retry_after` overrides it.
- Never log bot tokens, webhook secrets, message text, link codes/hashes, full identity subjects, chat addresses, or file contents.

---

## File Map

Create:

- `src/runtime/gateway.rs` — gateway-neutral routes, command keys/outcomes, delivery records, states, claims, and store traits.
- `src/runtime/gateway_activity.rs` — bounded transient activity broadcast and composite runtime publisher.
- `src/runtime/migrations/0004_gateway.sql` — delivery-route, command-ledger, durable-delivery, and streaming-state schema.
- `src/runtime/sqlite/gateway.rs` — SQLite command and delivery operations.
- `src/interfaces/telegram.rs` — module root and composition-facing facade.
- `src/interfaces/telegram/types.rs` — strict minimal Telegram DTOs and command parsing.
- `src/interfaces/telegram/api.rs` — typed Bot API client and error classification.
- `src/interfaces/telegram/webhook.rs` — Axum HTTP boundary and durable update routing.
- `src/interfaces/telegram/delivery.rs` — durable Telegram delivery worker.
- `src/interfaces/telegram/streaming.rs` — best-effort status/text editing.

Modify:

- `Cargo.toml`, `Cargo.lock` — HTTP server, URL parsing, constant-time comparison, multipart streaming.
- `src/config.rs` — optional strict Telegram configuration and validation.
- `src/interfaces.rs` — export Telegram module.
- `src/runtime.rs`, `src/runtime/model.rs`, `src/runtime/store.rs` — export and consume gateway-neutral domain types.
- `src/runtime/sqlite.rs`, `src/runtime/sqlite/identity_link.rs`, `src/runtime/sqlite/ingress.rs`, `src/runtime/sqlite/dispatch.rs`, `src/runtime/sqlite/checkpoint.rs` — schema v4, idempotent link commands, route persistence, final delivery projection.
- `src/runtime/identity_link.rs` — idempotent gateway redemption API.
- `src/runtime/runner.rs`, `src/runtime/stream_hub.rs` — publish local and gateway activity targets.
- `src/app.rs` — Telegram startup reconciliation and supervised components.
- `src/runtime/observability.rs` — Telegram-safe coordinates and redaction guards.
- `README.md` — English configuration, proxy, linking, scope, and troubleshooting documentation.
- `tests/serve_runtime.rs` — full mocked Telegram acceptance path.

---

### Task 1: Add Strict Telegram Configuration and Dependencies

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/config.rs`

**Interfaces:**

- Produces:

```rust
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    pub token: String,
    pub public_url: String,
    #[serde(default = "default_telegram_listen")]
    pub listen: String,
    pub webhook_secret: String,
}

#[derive(Clone)]
pub struct ValidatedTelegramConfig {
    pub token: String,
    pub public_url: url::Url,
    pub listen: std::net::SocketAddr,
    pub webhook_secret: String,
}

impl TelegramConfig {
    pub fn validate(&self) -> anyhow::Result<ValidatedTelegramConfig>;
}
```

- `AppConfig` gains `#[serde(default)] pub telegram: Option<TelegramConfig>`.

- [ ] **Step 1: Write failing configuration tests**

Add tests in `src/config.rs`:

```rust
#[test]
fn telegram_config_defaults_and_validates() -> Result<()> {
    let config: AppConfig = yaml_serde::from_str(
        r#"api_key: key
base_url: https://example.test/v1
model: test
telegram:
  token: bot-token
  public_url: https://agent.example/webhooks/telegram
  webhook_secret: abc_DEF-123
"#,
    )?;
    let telegram = config.telegram.as_ref().unwrap().validate()?;
    assert_eq!(telegram.listen, "127.0.0.1:8080".parse()?);
    assert_eq!(telegram.public_url.as_str(), "https://agent.example/webhooks/telegram");
    Ok(())
}

#[test]
fn telegram_config_rejects_insecure_url_bad_secret_and_unknown_fields() {
    for yaml in [
        "telegram:\n  token: t\n  public_url: http://agent.example/hook\n  webhook_secret: valid",
        "telegram:\n  token: t\n  public_url: https://agent.example/hook\n  webhook_secret: 'bad secret'",
        "telegram:\n  token: t\n  public_url: https://agent.example/hook\n  webhook_secret: valid\n  extra: true",
    ] {
        let document = format!("api_key: key\nbase_url: https://example.test/v1\nmodel: test\n{yaml}\n");
        let invalid = match yaml_serde::from_str::<AppConfig>(&document) {
            Ok(config) => config.telegram.unwrap().validate().is_err(),
            Err(_) => true,
        };
        assert!(invalid);
    }
}
```

- [ ] **Step 2: Verify configuration RED**

Run:

```sh
rtk cargo test config::tests::telegram_config -- --nocapture
```

Expected: compilation fails because `TelegramConfig` and `AppConfig::telegram` do not exist.

- [ ] **Step 3: Add dependencies**

Add:

```toml
axum = "0.8"
subtle = "2.6"
url = "2.5"
```

Change existing dependencies to:

```toml
reqwest = { version = "0.13.4", default-features = false, features = ["json", "multipart", "rustls", "stream"] }
tokio-util = { version = "0.7.17", features = ["io"] }
```

- [ ] **Step 4: Implement strict validation**

Validation must:

```rust
let public_url = url::Url::parse(&self.public_url)?;
if public_url.scheme() != "https"
    || public_url.host_str().is_none()
    || public_url.query().is_some()
    || public_url.fragment().is_some()
{
    bail!("telegram.public_url must be an HTTPS URL without query or fragment");
}
let listen = self.listen.parse::<SocketAddr>()
    .context("telegram.listen must be a socket address")?;
if self.token.trim().is_empty() {
    bail!("telegram.token must not be blank");
}
if self.webhook_secret.is_empty()
    || self.webhook_secret.len() > 256
    || !self.webhook_secret.bytes().all(
        |byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
    )
{
    bail!("telegram.webhook_secret has invalid length or characters");
}
```

Implement a custom `Debug` for `TelegramConfig` and `ValidatedTelegramConfig`
that renders token and secret as `"[REDACTED]"`.

- [ ] **Step 5: Verify configuration GREEN**

Run:

```sh
rtk cargo test config::tests::telegram_config -- --nocapture
rtk cargo test config::tests
rtk cargo fmt --check
```

Expected: all configuration tests pass.

- [ ] **Step 6: Commit**

```sh
rtk git add Cargo.toml Cargo.lock src/config.rs
rtk git commit -m "feat(telegram): add gateway configuration"
```

---

### Task 2: Add Schema v4 and Gateway Domain Boundaries

**Files:**

- Create: `src/runtime/gateway.rs`
- Create: `src/runtime/migrations/0004_gateway.sql`
- Modify: `src/runtime.rs`
- Modify: `src/runtime/model.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite.rs`

**Interfaces:**

- Produces:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRoute {
    pub gateway: String,
    pub address: String,
    pub reply_to_external_id: Option<String>,
    pub max_text_chars: usize,
    pub max_caption_chars: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GatewayCommandKey {
    pub gateway: String,
    pub external_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GatewayCommandOutcome {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GatewayDeliveryState {
    Pending,
    Delivering,
    Delivered,
    FailedRetryable,
    FailedTerminal,
    OutcomeUnknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayDeliveryClaim {
    pub id: GatewayDeliveryId,
    pub owner: String,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedGatewayDelivery {
    pub claim: GatewayDeliveryClaim,
    pub intent_key: String,
    pub source_outbox_id: Option<OutboxId>,
    pub work_item_id: Option<WorkItemId>,
    pub ordinal: usize,
    pub route: DeliveryRoute,
    pub payload: OutboxPayload,
    pub attempt_count: usize,
    pub remote_message_id: Option<String>,
}

pub struct NewGatewayDelivery {
    pub intent_key: String,
    pub source_outbox_id: Option<OutboxId>,
    pub ordinal: usize,
    pub route: DeliveryRoute,
    pub payload: OutboxPayload,
}
```

Add `GatewayDeliveryId` to `src/runtime/model.rs` with the existing `id_type!`
macro. Add `Serialize` and `Deserialize` to `Timestamp`, because persisted
gateway command outcomes contain retry timestamps.

Add store traits:

```rust
#[async_trait]
pub trait GatewayDeliveryStore: Send + Sync {
    async fn enqueue_gateway_delivery(
        &self,
        delivery: NewGatewayDelivery,
        now: Timestamp,
    ) -> Result<GatewayDeliveryId>;

    async fn claim_gateway_deliveries(
        &self,
        gateway: &str,
        owner: &str,
        now: Timestamp,
        claim_until: Timestamp,
        limit: usize,
    ) -> Result<Vec<ClaimedGatewayDelivery>>;

    async fn renew_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        now: Timestamp,
        claim_until: Timestamp,
    ) -> Result<Option<GatewayDeliveryClaim>>;

    async fn complete_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        remote_message_id: Option<String>,
        now: Timestamp,
    ) -> Result<bool>;

    async fn retry_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        next_attempt_at: Timestamp,
        error_class: &str,
        error: &str,
        now: Timestamp,
    ) -> Result<bool>;

    async fn fail_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        state: GatewayDeliveryState,
        error_class: &str,
        error: &str,
        now: Timestamp,
    ) -> Result<bool>;
}
```

- [ ] **Step 1: Write failing migration tests**

Extend SQLite tests in `src/runtime/sqlite.rs`:

```rust
#[tokio::test]
async fn schema_v4_adds_gateway_state_without_losing_v3_rows() -> Result<()> {
    let connection = seeded_v3_connection().await?;
    let store = SqliteRuntimeStore::initialize(connection, false).await?;
    let probe = store.gateway_schema_probe().await?;
    assert_eq!(probe.user_version, 4);
    assert!(probe.event_route_columns);
    assert!(probe.run_route_columns);
    assert!(probe.tables.contains(&"gateway_commands".into()));
    assert!(probe.tables.contains(&"gateway_deliveries".into()));
    assert!(probe.tables.contains(&"gateway_streams".into()));
    assert_eq!(probe.actor_count, 1);
    assert_eq!(probe.identity_count, 1);
    Ok(())
}
```

- [ ] **Step 2: Verify migration RED**

Run:

```sh
rtk cargo test runtime::sqlite::tests::schema_v4 -- --nocapture
```

Expected: test fails because schema version 4 and gateway tables do not exist.

- [ ] **Step 3: Add schema v4**

`0004_gateway.sql` must:

```sql
ALTER TABLE events ADD COLUMN delivery_gateway TEXT;
ALTER TABLE events ADD COLUMN delivery_address TEXT;
ALTER TABLE events ADD COLUMN reply_to_external_id TEXT;
ALTER TABLE events ADD COLUMN delivery_max_text_chars INTEGER;
ALTER TABLE events ADD COLUMN delivery_max_caption_chars INTEGER;

ALTER TABLE runs ADD COLUMN delivery_gateway TEXT;
ALTER TABLE runs ADD COLUMN delivery_address TEXT;
ALTER TABLE runs ADD COLUMN reply_to_external_id TEXT;
ALTER TABLE runs ADD COLUMN delivery_max_text_chars INTEGER;
ALTER TABLE runs ADD COLUMN delivery_max_caption_chars INTEGER;

CREATE TABLE gateway_commands (
    gateway TEXT NOT NULL,
    external_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    outcome_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(gateway, external_id)
) STRICT;

CREATE TABLE gateway_deliveries (
    id TEXT PRIMARY KEY,
    intent_key TEXT NOT NULL UNIQUE,
    source_outbox_id TEXT REFERENCES outbox(id),
    gateway TEXT NOT NULL,
    address TEXT NOT NULL,
    reply_to_external_id TEXT,
    max_text_chars INTEGER NOT NULL CHECK(max_text_chars > 0),
    max_caption_chars INTEGER NOT NULL CHECK(max_caption_chars > 0),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'pending','delivering','delivered','failed_retryable',
        'failed_terminal','outcome_unknown'
    )),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    next_attempt_at INTEGER,
    claim_owner TEXT,
    claim_expires_at INTEGER,
    remote_message_id TEXT,
    error_class TEXT,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(source_outbox_id, gateway, address, ordinal)
) STRICT;

CREATE INDEX ready_gateway_deliveries
ON gateway_deliveries(gateway, state, next_attempt_at, created_at);

CREATE TABLE gateway_streams (
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    gateway TEXT NOT NULL,
    address TEXT NOT NULL,
    remote_message_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active','closed')),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(work_item_id, gateway, address)
) STRICT;
```

Add CHECK constraints in application validation so route fields are either all
present or all absent. Migration v4 preserves all v3 rows with null routes.

- [ ] **Step 4: Add gateway domain types and store traits**

Implement constructors that reject blank gateway/address, zero text/caption
limits, blank intent keys, and `GatewayDeliveryState::Pending` as a terminal
failure target.

- [ ] **Step 5: Verify schema and domain GREEN**

Run:

```sh
rtk cargo test runtime::sqlite::tests::schema_v4 -- --nocapture
rtk cargo test runtime::gateway -- --nocapture
rtk cargo check
```

Expected: schema v4 and gateway type tests pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/runtime.rs src/runtime/model.rs src/runtime/store.rs \
  src/runtime/gateway.rs src/runtime/sqlite.rs \
  src/runtime/migrations/0004_gateway.sql
rtk git commit -m "feat(gateway): add durable delivery schema"
```

---

### Task 3: Implement Idempotent Link Commands and Delivery Store

**Files:**

- Create: `src/runtime/sqlite/gateway.rs`
- Modify: `src/runtime/sqlite.rs`
- Modify: `src/runtime/sqlite/identity_link.rs`
- Modify: `src/runtime/identity_link.rs`
- Modify: `src/runtime/store.rs`

**Interfaces:**

- Extend:

```rust
#[async_trait]
pub trait IdentityLinkManager: Send + Sync {
    async fn issue_code(&self, actor: &ActorId) -> Result<IssuedLinkCode>;
    async fn redeem_code(&self, identity: LinkIdentity, code: &str) -> Result<LinkRedemption>;
    async fn redeem_code_once(
        &self,
        key: GatewayCommandKey,
        identity: LinkIdentity,
        code: &str,
    ) -> Result<LinkRedemption>;
    async fn collect_expired(&self, limit: usize) -> Result<usize>;
}
```

- Extend `IdentityLinkStore` with:

```rust
async fn redeem_link_code_once(
    &self,
    key: GatewayCommandKey,
    identity: LinkIdentity,
    code_hash: Option<[u8; 32]>,
    now: Timestamp,
) -> Result<StoreLinkRedemption>;
```

- [ ] **Step 1: Write failing idempotency tests**

In `src/runtime/sqlite/identity_link.rs`:

```rust
#[tokio::test]
async fn repeated_gateway_link_update_returns_stored_outcome() -> Result<()> {
    let (store, actor) = enabled_actor_store().await?;
    store.replace_link_code(&actor, [7; 32], Timestamp(1), Timestamp(601)).await?;
    let key = GatewayCommandKey {
        gateway: "telegram:bot-1".into(),
        external_id: "42".into(),
    };
    let identity = LinkIdentity {
        provider: "telegram:bot-1".into(),
        subject: "100".into(),
        username: Some("owner".into()),
    };
    let first = store.redeem_link_code_once(
        key.clone(), identity.clone(), Some([7; 32]), Timestamp(2)
    ).await?;
    let repeated = store.redeem_link_code_once(
        key, identity, None, Timestamp(900)
    ).await?;
    assert_eq!(repeated, first);
    assert_eq!(store.gateway_command_count_for_test().await?, 1);
    Ok(())
}
```

In `src/runtime/sqlite/gateway.rs`, add tests for:

- duplicate `intent_key` with identical immutable data returns the same ID;
- duplicate `intent_key` with different route or payload fails;
- claims serialize one address while allowing different addresses;
- renewal and terminal transitions require the current claim;
- retry stores `next_attempt_at`;
- `OutcomeUnknown` cannot be reclaimed.

- [ ] **Step 2: Verify store RED**

Run:

```sh
rtk cargo test repeated_gateway_link_update_returns_stored_outcome -- --nocapture
rtk cargo test runtime::sqlite::gateway -- --nocapture
```

Expected: compilation fails because the new APIs and SQLite implementation are absent.

- [ ] **Step 3: Refactor link redemption transaction**

Extract the current `redeem_link_code` transaction body into one private
function receiving:

```rust
fn redeem_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    identity: &LinkIdentity,
    code_hash: Option<[u8; 32]>,
    now: Timestamp,
) -> Result<StoreLinkRedemption>;
```

`redeem_link_code_once` must:

1. Start an immediate transaction.
2. Read `gateway_commands` by key.
3. If present, decode and return the immutable stored outcome.
4. Otherwise run `redeem_in_transaction`.
5. Store the normalized outcome JSON under kind `identity_link`.
6. Commit.

The stored conflict outcome omits the conflicting actor ID.

- [ ] **Step 4: Implement durable delivery operations**

Use immediate transactions. Claim query requirements:

```sql
state IN ('pending','failed_retryable')
AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
AND NOT EXISTS (
    SELECT 1 FROM gateway_deliveries active
    WHERE active.gateway = gateway_deliveries.gateway
      AND active.address = gateway_deliveries.address
      AND active.state = 'delivering'
)
```

Claims increment `attempt_count`, set owner/expiry, and transition to
`delivering`. Every completion/retry/failure update matches ID, owner, and
claim expiry from `GatewayDeliveryClaim`.

- [ ] **Step 5: Verify store GREEN**

Run:

```sh
rtk cargo test runtime::sqlite::identity_link -- --nocapture
rtk cargo test runtime::sqlite::gateway -- --nocapture
rtk cargo test runtime::identity_link -- --nocapture
```

Expected: all identity-link and gateway delivery tests pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/runtime/store.rs src/runtime/identity_link.rs \
  src/runtime/sqlite.rs src/runtime/sqlite/identity_link.rs \
  src/runtime/sqlite/gateway.rs
rtk git commit -m "feat(gateway): persist idempotent commands and deliveries"
```

---

### Task 4: Persist Delivery Routes and Project Final Results

**Files:**

- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/ingress.rs`
- Modify: `src/runtime/sqlite/dispatch.rs`
- Modify: `src/runtime/sqlite/checkpoint.rs`
- Modify: `src/runtime/runner.rs`

**Interfaces:**

- `NewInboundEvent` gains:

```rust
pub delivery_route: Option<DeliveryRoute>
```

- Preserve existing callers:

```rust
pub fn text(...) -> Result<Self>; // sets delivery_route = None

pub fn text_with_route(
    gateway: impl Into<String>,
    external_id: impl Into<String>,
    identity_provider: impl Into<String>,
    identity_subject: impl Into<String>,
    audience: Audience,
    route: DeliveryRoute,
    text: impl Into<String>,
) -> Result<Self>;
```

- `AttachedRun` gains `pub delivery_route: Option<DeliveryRoute>`.

- [ ] **Step 1: Write failing route tests**

Add tests proving:

```rust
#[tokio::test]
async fn actor_private_events_keep_distinct_reply_routes() -> Result<()> {
    let store = linked_actor_store().await?;
    store.ingest(NewInboundEvent::text_with_route(
        "telegram:bot-1", "1", "telegram:bot-1", "100",
        Audience::ActorPrivate, telegram_route("100", "10"), "first"
    )?, Timestamp(10)).await?;
    store.ingest(NewInboundEvent::text_with_route(
        "telegram:bot-1", "2", "telegram:bot-1", "200",
        Audience::ActorPrivate, telegram_route("200", "20"), "second"
    )?, Timestamp(11)).await?;
    let run = attach_actor_run(&store).await?;
    assert_eq!(run.audience, Audience::ActorPrivate);
    assert_eq!(run.delivery_route, Some(telegram_route("200", "20")));
    Ok(())
}
```

Add finalization tests:

- a run with no route creates semantic outbox only;
- a routed text result creates 4096-character durable chunks;
- chunk intent keys end with stable ordinals;
- a long caption creates text delivery followed by a captionless file;
- repeated finalization does not duplicate deliveries.

- [ ] **Step 2: Verify route RED**

Run:

```sh
rtk cargo test actor_private_events_keep_distinct_reply_routes -- --nocapture
rtk cargo test gateway_delivery_projection -- --nocapture
```

Expected: tests fail because events/runs do not carry routes and finalization does not enqueue gateway deliveries.

- [ ] **Step 3: Persist and select routes**

Ingress stores route columns on events. During `attach_next_run`, choose the
route from the highest-mailbox-sequence incorporated `user_message` with a
non-null route, persist it on the run, and return it in `AttachedRun`.

Active-run resume reads the persisted route; it must not recompute a different
route after restart.

- [ ] **Step 4: Add gateway-neutral text and caption splitting**

Implement:

```rust
pub fn split_unicode(text: &str, max_chars: usize) -> Vec<String> {
    assert!(max_chars > 0);
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        if current.chars().count() == max_chars {
            chunks.push(std::mem::take(&mut current));
        }
        current.push(character);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}
```

Use a running counter rather than recounting `current.chars()` in production.

- [ ] **Step 5: Project final intents in the finalization transaction**

After each immutable outbox intent is inserted:

- if no route exists, do nothing;
- text and terminal errors become chunked text deliveries;
- file captions longer than `max_caption_chars` become preceding text chunks;
- file payload follows with the remaining allowed caption or no caption;
- intent keys are `gateway:<outbox-intent-key>:<ordinal>`;
- every row references `source_outbox_id`.

Local result bundles remain unchanged.

- [ ] **Step 6: Verify route GREEN**

Run:

```sh
rtk cargo test runtime::sqlite::ingress -- --nocapture
rtk cargo test runtime::sqlite::dispatch -- --nocapture
rtk cargo test runtime::sqlite::checkpoint -- --nocapture
rtk cargo test runtime::runner -- --nocapture
```

Expected: route selection and gateway delivery projection tests pass.

- [ ] **Step 7: Commit**

```sh
rtk git add src/runtime/store.rs src/runtime/runner.rs \
  src/runtime/sqlite/ingress.rs src/runtime/sqlite/dispatch.rs \
  src/runtime/sqlite/checkpoint.rs
rtk git commit -m "feat(gateway): route durable actor replies"
```

---

### Task 5: Implement Telegram DTOs and Bot API Client

**Files:**

- Create: `src/interfaces/telegram.rs`
- Create: `src/interfaces/telegram/types.rs`
- Create: `src/interfaces/telegram/api.rs`
- Modify: `src/interfaces.rs`

**Interfaces:**

- Produces:

```rust
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
}

pub enum TelegramInbound {
    Link { code: Option<String>, identity: LinkIdentity, route: DeliveryRoute },
    Text { text: String, identity: LinkIdentity, route: DeliveryRoute },
    Unsupported,
}

#[async_trait]
pub trait TelegramApi: Send + Sync {
    async fn get_me(&self) -> Result<TelegramBot, TelegramApiError>;
    async fn set_webhook(&self, command: SetWebhook) -> Result<(), TelegramApiError>;
    async fn get_webhook_info(&self) -> Result<WebhookInfo, TelegramApiError>;
    async fn send_message(&self, command: SendMessage) -> Result<TelegramMessageRef, TelegramApiError>;
    async fn edit_message_text(&self, command: EditMessageText) -> Result<(), TelegramApiError>;
    async fn send_photo(&self, command: SendFile) -> Result<TelegramMessageRef, TelegramApiError>;
    async fn send_document(&self, command: SendFile) -> Result<TelegramMessageRef, TelegramApiError>;
}
```

- [ ] **Step 1: Write failing DTO tests**

Cover exact parsing:

```rust
#[test]
fn private_link_command_normalizes_bot_suffix() -> Result<()> {
    let update: TelegramUpdate = serde_json::from_value(json!({
        "update_id": 42,
        "message": {
            "message_id": 7,
            "from": {"id": 100, "is_bot": false, "username": "owner"},
            "chat": {"id": 100, "type": "private"},
            "text": "/link@codrik_bot abcd-efgh"
        }
    }))?;
    assert!(matches!(
        update.classify("900", "codrik_bot")?,
        TelegramInbound::Link { code: Some(code), .. } if code == "abcd-efgh"
    ));
    Ok(())
}
```

Also assert group, bot sender, missing text, attachment-only, wrong command
suffix, and non-message updates classify as `Unsupported`.

- [ ] **Step 2: Write failing API classification tests**

With a loopback mock HTTP server, assert:

- `getMe`, `setWebhook`, and `getWebhookInfo` request bodies;
- successful Telegram envelope decoding;
- `429` extracts `retry_after`;
- known `400` is terminal;
- `message is not modified` edit is success;
- response timeout after request body transmission is `OutcomeUnknown` for
  sends and `Retryable` for deterministic edits;
- error formatting contains no token.

- [ ] **Step 3: Verify Telegram client RED**

Run:

```sh
rtk cargo test interfaces::telegram::types -- --nocapture
rtk cargo test interfaces::telegram::api -- --nocapture
```

Expected: compilation fails because Telegram modules do not exist.

- [ ] **Step 4: Implement DTO classification**

Use strict DTO structs with optional fields only where Telegram may omit them.
`classify` constructs:

```rust
DeliveryRoute {
    gateway: format!("telegram:{bot_id}"),
    address: message.chat.id.to_string(),
    reply_to_external_id: Some(message.message_id.to_string()),
    max_text_chars: 4096,
    max_caption_chars: 1024,
}
```

The link command parser accepts exactly `/link` and `/link@<current bot username>`.

- [ ] **Step 5: Implement Bot API client**

Build method URLs internally without exposing them through `Debug` or error
messages. Enforce response-size and timeout constants. Multipart uploads stream
`tokio::fs::File` through `tokio_util::io::ReaderStream`.

Represent error class explicitly:

```rust
pub enum TelegramApiErrorClass {
    Retryable { retry_after: Option<Duration> },
    Terminal,
    OutcomeUnknown,
}
```

- [ ] **Step 6: Verify Telegram client GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::types -- --nocapture
rtk cargo test interfaces::telegram::api -- --nocapture
rtk cargo check
```

Expected: all DTO and Bot API tests pass.

- [ ] **Step 7: Commit**

```sh
rtk git add src/interfaces.rs src/interfaces/telegram.rs \
  src/interfaces/telegram/types.rs src/interfaces/telegram/api.rs
rtk git commit -m "feat(telegram): add Bot API adapter"
```

---

### Task 6: Implement the Authenticated Webhook Ingress

**Files:**

- Create: `src/interfaces/telegram/webhook.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/gateway.rs`

**Interfaces:**

- Produces:

```rust
#[async_trait]
pub trait TelegramIngress: Send + Sync {
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramWebhookOutcome>;
}

pub enum TelegramWebhookOutcome {
    Accepted { actor_id: ActorId, sequence: i64 },
    Duplicate,
    CommandHandled,
    Unsupported,
}

pub struct TelegramWebhookServer<I> {
    listener: tokio::net::TcpListener,
    path: String,
    secret: SecretToken,
    ingress: Arc<I>,
}

pub struct SecretToken(Vec<u8>);

impl SecretToken {
    pub fn new(value: &str) -> Self;
    pub fn matches(&self, candidate: &[u8]) -> bool;
}
```

- [ ] **Step 1: Write failing HTTP boundary tests**

Using a bound loopback listener, assert:

- correct POST and secret reaches ingress;
- wrong path `404`;
- wrong method `405`;
- missing/wrong secret `401`;
- content type mismatch `415`;
- body over 1 MiB `413`;
- malformed JSON `400`;
- valid unsupported update `200`;
- store authority failure `503`;
- handler does not return `200` before an injected commit hook releases.

- [ ] **Step 2: Write failing routing tests**

Use mock stores and managers:

```rust
#[tokio::test]
async fn link_command_is_redeemed_without_agent_ingress() -> Result<()> {
    let harness = TelegramIngressHarness::linked_code();
    let outcome = harness.handle(link_update(42, "ABCD-EFGH")).await?;
    assert_eq!(outcome, TelegramWebhookOutcome::CommandHandled);
    assert_eq!(harness.link_calls(), 1);
    assert_eq!(harness.ingress_calls(), 0);
    assert_eq!(harness.deliveries(), vec!["This channel is now linked."]);
    Ok(())
}
```

Also test:

- `/link` without a code enqueues CLI instructions;
- unlinked ordinary text enqueues linking instructions only;
- linked ordinary text calls `IngressStore::ingest` with actor-private route;
- duplicate update returns success without duplicate delivery;
- accepted ingress notifies `ActorSignals`.

- [ ] **Step 3: Verify webhook RED**

Run:

```sh
rtk cargo test interfaces::telegram::webhook -- --nocapture
```

Expected: compilation fails because webhook server and ingress coordinator do not exist.

- [ ] **Step 4: Add atomic enqueue helpers**

Add a store operation for deterministic system responses:

```rust
async fn enqueue_gateway_response(
    &self,
    key: &GatewayCommandKey,
    route: DeliveryRoute,
    payload: OutboxPayload,
    now: Timestamp,
) -> Result<GatewayDeliveryId>;
```

Its intent key is:

```text
gateway-response:<gateway>:<external-id>
```

Identical repeats return the existing row; immutable mismatches fail.

- [ ] **Step 5: Implement ingress routing**

Order is normative:

1. Classify Telegram update.
2. Return `Unsupported` without persistence for unsupported valid updates.
3. For `/link CODE`, call `redeem_code_once`, then enqueue its fixed response.
4. For `/link` without a code, enqueue CLI instructions.
5. For ordinary text, call routed `IngressStore::ingest`.
6. On `Unauthorized`, enqueue linking instructions.
7. On accepted/duplicate actor ingress, notify the actor only for accepted work.

HTTP secret comparison uses `subtle::ConstantTimeEq`.

- [ ] **Step 6: Verify webhook GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::webhook -- --nocapture
rtk cargo test runtime::sqlite::gateway -- --nocapture
```

Expected: HTTP matrix and durable routing tests pass.

- [ ] **Step 7: Commit**

```sh
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/webhook.rs \
  src/runtime/store.rs src/runtime/sqlite/gateway.rs
rtk git commit -m "feat(telegram): accept authenticated webhooks"
```

---

### Task 7: Implement Durable Telegram Delivery

**Files:**

- Create: `src/interfaces/telegram/delivery.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/runtime/sqlite/gateway.rs`

**Interfaces:**

- Produces:

```rust
pub struct TelegramDeliveryWorker<S, A, C> {
    store: S,
    api: A,
    clock: C,
    gateway: String,
    owner: String,
}

impl<S, A, C> TelegramDeliveryWorker<S, A, C> {
    pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<()>;
    pub async fn run_once(&self) -> Result<usize>;
}
```

- [ ] **Step 1: Write failing worker tests**

Cover:

- text delivery completes with remote message ID;
- first final chunk edits a known active stream message;
- later text chunks use `sendMessage`;
- JPEG/PNG/WebP up to 10 MB use `sendPhoto`;
- other files up to 50 MB use `sendDocument`;
- managed path, regular-file, size, and SHA-256 mismatch fail terminally;
- `429` schedules exact retry time;
- retryable errors use capped exponential backoff;
- terminal `4xx` becomes `FailedTerminal`;
- ambiguous sends become `OutcomeUnknown`;
- per-address serialization and global concurrency four;
- shutdown exits and releases no uncertain claim as retryable.

- [ ] **Step 2: Verify worker RED**

Run:

```sh
rtk cargo test interfaces::telegram::delivery -- --nocapture
```

Expected: compilation fails because `TelegramDeliveryWorker` does not exist.

- [ ] **Step 3: Implement retry calculation**

```rust
fn retry_at(now: Timestamp, attempt: usize, telegram_delay: Option<Duration>) -> Timestamp {
    if let Some(delay) = telegram_delay {
        return now.plus_millis(delay.as_millis().min(i64::MAX as u128) as i64);
    }
    let exponent = attempt.saturating_sub(1).min(18);
    let seconds = 1_u64.checked_shl(exponent as u32).unwrap_or(u64::MAX).min(300);
    now.plus_millis((seconds * 1000) as i64)
}
```

Add bounded deterministic jitter derived from delivery ID, without global RNG.

- [ ] **Step 4: Implement payload delivery**

For each claimed row:

1. Acquire global permit.
2. Acquire address-specific mutex.
3. Renew claim before transport.
4. Choose edit/send/photo/document.
5. Classify result.
6. Commit delivered, retry, terminal, or unknown state under the claim fence.

Truncate stored errors to a bounded diagnostic summary and strip URLs.

- [ ] **Step 5: Verify worker GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::delivery -- --nocapture
rtk cargo test runtime::sqlite::gateway -- --nocapture
```

Expected: all delivery state-machine tests pass.

- [ ] **Step 6: Commit**

```sh
rtk git add src/interfaces/telegram.rs \
  src/interfaces/telegram/delivery.rs src/runtime/sqlite/gateway.rs
rtk git commit -m "feat(telegram): deliver durable replies"
```

---

### Task 8: Add Gateway Activity Streaming

**Files:**

- Create: `src/runtime/gateway_activity.rs`
- Create: `src/interfaces/telegram/streaming.rs`
- Modify: `src/runtime.rs`
- Modify: `src/runtime/stream_hub.rs`
- Modify: `src/runtime/runner.rs`
- Modify: `src/runtime/store.rs`
- Modify: `src/runtime/sqlite/gateway.rs`
- Modify: `src/interfaces/telegram.rs`

**Interfaces:**

- Produces:

```rust
#[derive(Clone, Debug)]
pub struct GatewayActivity {
    pub work_item_id: WorkItemId,
    pub route: DeliveryRoute,
    pub event: GatewayActivityEvent,
}

#[derive(Clone, Debug)]
pub enum GatewayActivityEvent {
    Activity(AgentActivityEvent),
    TextDelta(String),
}

#[derive(Clone)]
pub struct GatewayActivityHub { /* bounded broadcast */ }

pub struct CompositeRuntimeEventPublisher {
    local: Arc<dyn RuntimeEventPublisher>,
    gateway: GatewayActivityHub,
}
```

Change runtime publisher calls to:

```rust
fn publish_text(&self, run: &AttachedRun, delta: &str);
fn publish_activity(&self, run: &AttachedRun, event: AgentActivityEvent);
```

Local publishing uses `run.request_ids`; gateway publishing uses
`run.delivery_route`.

- [ ] **Step 1: Write failing publisher tests**

Assert one runner event is sent to:

- every local request subscription;
- one gateway activity subscriber when a route exists;
- no gateway subscriber when route is absent;
- bounded overflow drops transient events without blocking the runner.

- [ ] **Step 2: Write failing streaming tests**

With a paused Tokio clock and mocked API/store:

- first activity sends `Thinking…`;
- returned message ID is persisted;
- deltas are accumulated;
- edits occur no more than once per second;
- unchanged text is skipped;
- completion closes stream state;
- API failure never fails the actor runner;
- restart with stored stream ID allows final delivery to edit it.

- [ ] **Step 3: Verify streaming RED**

Run:

```sh
rtk cargo test runtime::gateway_activity -- --nocapture
rtk cargo test interfaces::telegram::streaming -- --nocapture
```

Expected: compilation fails because gateway activity and streaming components are absent.

- [ ] **Step 4: Implement composite publishing**

Use a bounded `tokio::sync::broadcast` channel. `publish_*` is synchronous and
uses `send` without awaiting. Dropped/no-receiver events are ignored because
the stream is explicitly best effort.

Update every runner call site and its test doubles to pass `&AttachedRun`.

- [ ] **Step 5: Persist streaming message IDs**

Add:

```rust
#[async_trait]
pub trait GatewayStreamStore: Send + Sync {
    async fn upsert_gateway_stream(
        &self,
        work_item: &WorkItemId,
        route: &DeliveryRoute,
        remote_message_id: &str,
        now: Timestamp,
    ) -> Result<()>;

    async fn resolve_gateway_stream(
        &self,
        work_item: &WorkItemId,
        route: &DeliveryRoute,
    ) -> Result<Option<String>>;

    async fn close_gateway_stream(
        &self,
        work_item: &WorkItemId,
        route: &DeliveryRoute,
        now: Timestamp,
    ) -> Result<()>;
}
```

- [ ] **Step 6: Implement Telegram streaming worker**

Maintain per-work-item buffers and last-edit instants. Use plain text only.
Never propagate Telegram failures into `GatewayActivityHub`, runner, or
dispatcher.

- [ ] **Step 7: Verify streaming GREEN**

Run:

```sh
rtk cargo test runtime::gateway_activity -- --nocapture
rtk cargo test interfaces::telegram::streaming -- --nocapture
rtk cargo test runtime::runner -- --nocapture
rtk cargo test runtime::stream_hub -- --nocapture
```

Expected: local IPC streaming remains intact and Telegram streaming tests pass.

- [ ] **Step 8: Commit**

```sh
rtk git add src/runtime.rs src/runtime/store.rs src/runtime/runner.rs \
  src/runtime/stream_hub.rs src/runtime/gateway_activity.rs \
  src/runtime/sqlite/gateway.rs src/interfaces/telegram.rs \
  src/interfaces/telegram/streaming.rs
rtk git commit -m "feat(telegram): stream actor activity"
```

---

### Task 9: Compose Telegram in `codrik serve`

**Files:**

- Modify: `src/app.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/runtime/observability.rs`

**Interfaces:**

- `TelegramGateway::prepare` performs:

```rust
pub async fn prepare(
    config: ValidatedTelegramConfig,
    store: SqliteRuntimeStore,
    identity_linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    activity: GatewayActivityHub,
    clock: impl Clock,
) -> Result<PreparedTelegramGateway>;
```

Production `prepare` constructs `ReqwestTelegramApi`. Add a
`#[doc(hidden)] prepare_with_api` dependency seam taking `Arc<dyn TelegramApi>`
for startup-order and failure tests.

- `PreparedTelegramGateway` exposes:

```rust
pub fn bot_id(&self) -> &str;
pub fn gateway_name(&self) -> &str;
pub fn webhook(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> impl Future<Output = Result<()>>;
pub fn delivery(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> impl Future<Output = Result<()>>;
pub fn streaming(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> impl Future<Output = Result<()>>;
```

- [ ] **Step 1: Write failing startup tests**

Add app tests:

- no Telegram config starts no Telegram components;
- enabled config binds listener before `setWebhook`;
- startup calls `getMe -> setWebhook -> getWebhookInfo`;
- exact URL, secret, `allowed_updates=["message"]`, and
  `drop_pending_updates=false` are sent;
- mismatched webhook info fails before ready;
- enabled component exit is terminal;
- shutdown stops listener/workers without `deleteWebhook`;
- startup log contains schema version 4 and bot ID but no token/secret.

- [ ] **Step 2: Verify composition RED**

Run:

```sh
rtk cargo test app::tests::telegram -- --nocapture
```

Expected: tests fail because app composition does not construct Telegram.

- [ ] **Step 3: Implement startup reconciliation**

Bind listener first, then call:

```rust
let bot = api.get_me().await?;
api.set_webhook(SetWebhook {
    url: config.public_url.clone(),
    secret_token: config.webhook_secret.clone(),
    allowed_updates: vec!["message".into()],
    drop_pending_updates: false,
}).await?;
let info = api.get_webhook_info().await?;
```

Require `info.url == config.public_url.as_str()` and visible allowed updates
contain only `message`. Configure the Axum router at
`config.public_url.path()`; the listener must not accept the same handler at
other paths.

- [ ] **Step 4: Register supervised components**

When Telegram is enabled:

```rust
service.component("telegram-webhook", telegram.clone().webhook(shutdown_rx.clone()));
service.component("telegram-delivery", telegram.clone().delivery(shutdown_rx.clone()));
service.component("telegram-streaming", telegram.streaming(shutdown_rx.clone()));
```

Compose `CompositeRuntimeEventPublisher` so existing local IPC and Telegram
both receive transient events.

- [ ] **Step 5: Extend safe observability**

Add typed `RuntimeComponent` values for Telegram components and optional
`gateway_delivery_id`, `gateway_update_id`, and `bot_id`. Extend forbidden
fixtures with real-looking token, webhook secret, message, subject, chat ID,
and link code strings.

- [ ] **Step 6: Verify composition GREEN**

Run:

```sh
rtk cargo test app::tests::telegram -- --nocapture
rtk cargo test runtime::observability::tests -- --nocapture
rtk cargo check
```

Expected: optional startup, reconciliation, supervision, and redaction tests pass.

- [ ] **Step 7: Commit**

```sh
rtk git add src/app.rs src/interfaces/telegram.rs src/runtime/observability.rs
rtk git commit -m "feat(telegram): supervise webhook gateway"
```

---

### Task 10: Add Acceptance Coverage and English Documentation

**Files:**

- Modify: `tests/serve_runtime.rs`
- Modify: `README.md`

**Interfaces:**

- No new production interfaces.

- [ ] **Step 1: Write the failing acceptance scenario**

Add `telegram_webhook_links_runs_and_delivers_without_duplicates`:

1. Start a loopback mock Telegram API.
2. Configure `codrik serve` with its public URL and listener.
3. Assert startup reconciliation requests.
4. Issue `codrik link` through local IPC and capture the code.
5. POST `/link CODE` with valid secret.
6. Assert one identity row and no agent work from the command.
7. POST one linked private text update.
8. Assert webhook returns only after the event commits.
9. Let the scripted LLM return a streamed text and managed file.
10. Assert throttled streaming, final text, and file calls.
11. Replay both Telegram updates.
12. Assert one identity command, one event, one work item, and no duplicate
    final gateway deliveries.
13. Restart the runtime between ingress and final delivery and assert pending
    deliveries resume.

- [ ] **Step 2: Verify acceptance RED**

Run:

```sh
rtk cargo test --test serve_runtime telegram_webhook_links_runs_and_delivers_without_duplicates -- --nocapture
```

Expected: the new scenario passes. If it fails, return to the task owning the
failed invariant and rerun that task's focused tests before rerunning acceptance.

- [ ] **Step 3: Document `config.yml` and operations in English**

README must include:

```yaml
telegram:
  token: "..."
  public_url: "https://agent.example.com/webhooks/telegram"
  listen: "127.0.0.1:8080"
  webhook_secret: "..."
```

Document:

- Caddy/Nginx terminates HTTPS and proxies the exact path;
- `codrik serve` automatically registers and verifies the webhook;
- `codrik link` then Telegram `/link CODE`;
- private text-only inbound scope;
- outgoing text/files;
- transient streaming versus durable final guarantees;
- no token/secret values in logs or examples;
- troubleshooting for `401`, `413`, `503`, webhook mismatch, `429`, terminal
  delivery failures, and `outcome_unknown`.

- [ ] **Step 4: Run full verification**

Run:

```sh
rtk cargo fmt --check
rtk cargo check
rtk cargo test
rtk cargo clippy --all-targets --all-features
rtk sh -n scripts/install.sh
rtk git diff --check
```

Expected:

- all tests pass;
- Clippy reports zero errors;
- no new warning originates from changed code;
- installer syntax and diff checks pass.

- [ ] **Step 5: Manual verification**

With a temporary `CODRIK_HOME` and loopback/reverse-proxy test endpoint:

```sh
rtk cargo run -- serve
rtk cargo run -- link
```

POST a signed private Telegram update and inspect SQLite:

```sh
rtk sqlite3 runtime.sqlite \
  "SELECT COUNT(*) FROM gateway_commands;
   SELECT COUNT(*) FROM events;
   SELECT state, COUNT(*) FROM gateway_deliveries GROUP BY state;"
```

Expected:

- one link command row;
- one event for the ordinary text and none for `/link`;
- final deliveries reach `delivered`;
- replaying the updates leaves counts unchanged.

- [ ] **Step 6: Commit**

```sh
rtk git add tests/serve_runtime.rs README.md
rtk git commit -m "test(telegram): verify webhook gateway"
```
