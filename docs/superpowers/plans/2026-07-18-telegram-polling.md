# Telegram Polling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit Telegram long-polling ingress mode that feeds the existing durable actor runtime without requiring a public webhook.

**Architecture:** Configuration selects one typed ingress transport. Webhook and polling both call one transport-neutral `TelegramIngressService`; the existing Reqwest adapter implements separate inbound and outbound API traits, and the supervisor runs exactly one Telegram ingress component.

**Tech Stack:** Rust 2024, Tokio, Reqwest, Serde/YAML, Axum, SQLite-backed durable ingress, Telegram Bot API.

## Global Constraints

- `telegram.mode` accepts `webhook` or `polling`; omission means `webhook`.
- Polling requires only `telegram.token`; retained webhook fields are accepted but ignored.
- Polling calls `deleteWebhook` with `drop_pending_updates: false` before `getUpdates`.
- Polling and webhook never run simultaneously for one configured bot.
- Long poll timeout is 25 seconds, request timeout remains 30 seconds, and batch limit is 100.
- Updates are sorted by update ID and handled sequentially; offset advances only after successful durable ingress.
- Retry delays are 1, 2, 4, 8, 16, then 30 seconds; Telegram `retry_after` takes precedence.
- No automatic fallback, cursor persistence, sidecar, second agent loop, or new dependency.

---

### Task 1: Add Typed Telegram Ingress Configuration

**Files:**
- Modify: `src/config.rs`

**Interfaces:**
- Produces: `TelegramMode`, `ValidatedTelegramIngressConfig`, and `ValidatedTelegramConfig { token, ingress }`.
- Consumes: existing `TelegramConfig::validate()` and `default_telegram_listen()`.

- [ ] **Step 1: Write failing configuration tests**

Add tests beside the existing Telegram config tests:

```rust
#[test]
fn telegram_mode_defaults_to_webhook() -> Result<()> {
    let config: AppConfig = yaml_serde::from_str(
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\ntelegram:\n  token: t\n  public_url: https://agent.example/webhooks/telegram\n  webhook_secret: secret\n",
    )?;
    assert!(matches!(
        config.telegram.unwrap().validate()?.ingress,
        ValidatedTelegramIngressConfig::Webhook { .. }
    ));
    Ok(())
}

#[test]
fn telegram_polling_requires_only_token_and_ignores_webhook_fields() -> Result<()> {
    for yaml in [
        "telegram:\n  token: t\n  mode: polling\n",
        "telegram:\n  token: t\n  mode: polling\n  public_url: not-a-url\n  listen: not-an-address\n  webhook_secret: 'bad secret'\n",
    ] {
        let document = format!("api_key: k\nbase_url: https://example.test/v1\nmodel: m\n{yaml}");
        let config: AppConfig = yaml_serde::from_str(&document)?;
        assert_eq!(
            config.telegram.unwrap().validate()?.ingress,
            ValidatedTelegramIngressConfig::Polling
        );
    }
    Ok(())
}

#[test]
fn telegram_webhook_still_requires_webhook_fields() -> Result<()> {
    let config: AppConfig = yaml_serde::from_str(
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\ntelegram:\n  token: t\n",
    )?;
    assert!(config.telegram.unwrap().validate().is_err());
    Ok(())
}
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```sh
rtk cargo test config::tests::telegram_ -- --nocapture
```

Expected: FAIL because `mode`, `ingress`, and the polling variant do not exist.

- [ ] **Step 3: Implement the typed configuration**

Use these public shapes:

```rust
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TelegramMode {
    #[default]
    Webhook,
    Polling,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidatedTelegramIngressConfig {
    Webhook {
        public_url: url::Url,
        listen: SocketAddr,
        webhook_secret: String,
    },
    Polling,
}

#[derive(Clone)]
pub struct ValidatedTelegramConfig {
    pub token: String,
    pub ingress: ValidatedTelegramIngressConfig,
}
```

Change raw webhook-only strings to `Option<String>` with `#[serde(default)]`, add `mode` with `#[serde(default)]`, and validate URL/listen/secret only inside the webhook branch. Keep token validation and secret-redacting `Debug` output in both modes.

- [ ] **Step 4: Run config tests and verify GREEN**

Run:

```sh
rtk cargo test config::tests::telegram -- --nocapture
```

Expected: all Telegram config tests pass, including unknown-field and secret-redaction checks.

- [ ] **Step 5: Commit**

```sh
rtk git add src/config.rs
rtk git commit -m "feat(config): add Telegram polling mode"
```

---

### Task 2: Separate Durable Ingress from Webhook Transport

**Files:**
- Create: `src/interfaces/telegram/ingress.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/interfaces/telegram/webhook.rs`
- Modify: `src/interfaces/telegram/api.rs`
- Modify: `src/interfaces/telegram/activity.rs`
- Modify: `src/interfaces/telegram/delivery.rs`
- Modify: `tests/serve_runtime.rs`

**Interfaces:**
- Produces: `ingress::{TelegramIngress, TelegramIngressOutcome, TelegramIngressService}` and `api::TelegramIngressApi`.
- Consumes: existing `TelegramUpdate`, store traits, identity linking, actor signals, and outbound `TelegramApi`.

- [ ] **Step 1: Run the existing ingress and Telegram tests as a refactor baseline**

Run:

```sh
rtk cargo test interfaces::telegram::webhook::tests -- --nocapture
rtk cargo test interfaces::telegram::delivery::tests -- --nocapture
rtk cargo test interfaces::telegram::activity::tests -- --nocapture
```

Expected: PASS before moving code.

- [ ] **Step 2: Extract transport-neutral ingress code**

Create `ingress.rs` containing the existing trait, service, outcome enum, and service tests from `webhook.rs`, renaming the outcome to:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramIngressOutcome {
    Accepted { actor_id: ActorId, sequence: i64 },
    Duplicate,
    CommandHandled,
    Unsupported,
}

#[async_trait]
pub trait TelegramIngress: Send + Sync + 'static {
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramIngressOutcome>;
}
```

Register `pub mod ingress;` in `telegram.rs`. Make `webhook.rs` depend only on the extracted trait and update its fake ingress return type. Do not change durable keys, messages, authorization, notification, or route behavior.

- [ ] **Step 3: Split inbound and outbound Bot API traits**

Leave only send/edit/upload methods on `TelegramApi`. Add:

```rust
#[async_trait]
pub trait TelegramIngressApi: Send + Sync {
    async fn get_me(&self) -> Result<TelegramBot, TelegramApiError>;
    async fn set_webhook(&self, command: SetWebhook) -> Result<(), TelegramApiError>;
    async fn get_webhook_info(&self) -> Result<WebhookInfo, TelegramApiError>;
}
```

Implement `TelegramIngressApi` for `ReqwestTelegramApi`. Update startup and acceptance fakes to implement this trait; delete the unrelated startup methods from delivery/activity fakes. Keep outbound method signatures unchanged.

- [ ] **Step 4: Run Telegram tests and verify GREEN**

Run:

```sh
rtk cargo test interfaces::telegram -- --nocapture
rtk cargo test --test serve_runtime telegram_acceptance -- --nocapture
```

Expected: all existing webhook, delivery, activity, and acceptance tests pass without behavior changes.

- [ ] **Step 5: Commit**

```sh
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/ingress.rs src/interfaces/telegram/webhook.rs src/interfaces/telegram/api.rs src/interfaces/telegram/activity.rs src/interfaces/telegram/delivery.rs tests/serve_runtime.rs
rtk git commit -m "refactor(telegram): separate ingress transport"
```

---

### Task 3: Add Polling Bot API Methods

**Files:**
- Modify: `src/interfaces/telegram/api.rs`

**Interfaces:**
- Extends: `TelegramIngressApi`.
- Produces: `DeleteWebhook`, `GetUpdates`, `delete_webhook`, and `get_updates`.

- [ ] **Step 1: Write failing Reqwest adapter tests**

Use the existing local HTTP recorder in `api.rs`. For `deleteWebhook`, return
`{"ok":true,"result":true}` and assert:

```rust
assert_eq!(delete_body, serde_json::json!({"drop_pending_updates": false}));
assert_eq!(updates_body, serde_json::json!({
    "offset": 42,
    "timeout": 25,
    "limit": 100,
    "allowed_updates": ["message"]
}));
assert_eq!(updates[0].update_id, 42);
```

For `getUpdates`, return this Bot API envelope:

```json
{"ok":true,"result":[{"update_id":42}]}
```

- [ ] **Step 2: Run API tests and verify RED**

Run:

```sh
rtk cargo test interfaces::telegram::api::tests::delete_webhook_preserves_pending_updates -- --nocapture
rtk cargo test interfaces::telegram::api::tests::get_updates_posts_long_poll_parameters -- --nocapture
```

Expected: FAIL because the commands and trait methods do not exist.

- [ ] **Step 3: Implement exact request types and methods**

Add:

```rust
#[derive(Clone, Serialize)]
pub struct DeleteWebhook {
    pub drop_pending_updates: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct GetUpdates {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    pub timeout: u64,
    pub limit: u8,
    pub allowed_updates: Vec<String>,
}
```

Extend `TelegramIngressApi`:

```rust
async fn delete_webhook(&self, command: DeleteWebhook) -> Result<(), TelegramApiError>;
async fn get_updates(&self, command: GetUpdates) -> Result<Vec<TelegramUpdate>, TelegramApiError>;
```

Implement both with retry-safe `post_json`; decode `deleteWebhook` as `bool` and `getUpdates` as `Vec<TelegramUpdate>`.

- [ ] **Step 4: Run API tests and verify GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::api::tests -- --nocapture
```

Expected: all API tests pass and tokens remain absent from errors.

- [ ] **Step 5: Commit**

```sh
rtk git add src/interfaces/telegram/api.rs
rtk git commit -m "feat(telegram): add polling Bot API"
```

---

### Task 4: Implement the Supervised Polling Worker

**Files:**
- Create: `src/interfaces/telegram/polling.rs`
- Modify: `src/interfaces/telegram.rs`

**Interfaces:**
- Consumes: `TelegramIngressApi`, `TelegramIngress`, `GetUpdates`, `TelegramApiErrorClass`, and `watch::Receiver<bool>`.
- Produces: `TelegramPollingWorker<A, I>::new(api, ingress)` and `run(shutdown) -> Result<()>`.

- [ ] **Step 1: Write failing worker tests**

Create fakes that queue `getUpdates` results and record request offsets plus handled IDs. Add these observable tests:

```rust
#[tokio::test]
async fn sorts_updates_and_advances_offset_after_each_success() {
    let harness = PollingHarness::new(vec![Ok(vec![update(12), update(10), update(11)])]);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(harness.worker().run(shutdown_rx));
    harness.wait_for_requests(2).await;
    assert_eq!(harness.handled_ids(), vec![10, 11, 12]);
    assert_eq!(harness.requests()[1].offset, Some(13));
    shutdown_tx.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn failed_ingress_retries_without_skipping_update() {
    let harness = PollingHarness::with_ingress_results(
        vec![Ok(vec![update(10), update(11)])],
        vec![Ok(TelegramIngressOutcome::Unsupported), Err(anyhow!("fail"))],
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(harness.worker().run(shutdown_rx));
    harness.wait_for_requests(1).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    harness.wait_for_requests(2).await;
    assert_eq!(harness.requests()[1].offset, Some(11));
    shutdown_tx.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn retry_after_overrides_backoff_and_success_resets_it() {
    let harness = PollingHarness::new(vec![
        Err(retryable(Some(Duration::from_secs(7)))),
        Ok(Vec::new()),
        Err(retryable(None)),
        Ok(Vec::new()),
    ]);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(harness.worker().run(shutdown_rx));
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(harness.requests().len(), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    harness.wait_for_requests(3).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    harness.wait_for_requests(4).await;
    shutdown_tx.send_replace(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_interrupts_blocked_get_updates() {
    let harness = PollingHarness::new(Vec::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(harness.worker().run(shutdown_rx));
    harness.wait_for_requests(1).await;
    shutdown_tx.send_replace(true);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
```

Define `PollingHarness` in the same test module with queued
`VecDeque<Result<Vec<TelegramUpdate>, TelegramApiError>>`, recorded
`Vec<GetUpdates>`, queued ingress results, and a `Notify` signalled after every
request. `update(id)` deserializes `{"update_id": id}`; `retryable(delay)` uses
`TelegramApiError::classified`. When its API result queue is empty,
`get_updates` awaits a never-notified `Notify`, allowing the shutdown branch to
cancel it. `wait_for_requests(count)` loops on the request notification until
the recorded length reaches `count`; it contains no wall-clock sleep.

- [ ] **Step 2: Run polling tests and verify RED**

Run:

```sh
rtk cargo test interfaces::telegram::polling::tests -- --nocapture
```

Expected: FAIL because the module and worker do not exist.

- [ ] **Step 3: Implement the minimal worker**

Define constants:

```rust
const LONG_POLL_TIMEOUT_SECS: u64 = 25;
const UPDATE_LIMIT: u8 = 100;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
```

Each request uses `GetUpdates { offset, timeout: 25, limit: 100, allowed_updates: vec!["message".into()] }`. Sort with `sort_by_key(|update| update.update_id)`. After successful handling use `checked_add(1)` and return an error on overflow. On a failed update, keep its ID as the next offset. Retry only `TelegramApiErrorClass::Retryable`; return terminal errors. Select both the poll future and retry sleep against shutdown.

Keep backoff state local to the worker. Compute seconds as
`1_u64.checked_shl(failures).unwrap_or(u64::MAX).min(30)`, increment after each
failure, and reset after a successful poll response. This yields exactly 1, 2,
4, 8, 16, then 30 seconds.

- [ ] **Step 4: Run polling tests and verify GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::polling::tests -- --nocapture
```

Expected: all ordering, retry, offset, and shutdown tests pass under paused Tokio time.

- [ ] **Step 5: Commit**

```sh
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/polling.rs
rtk git commit -m "feat(telegram): add polling ingress worker"
```

---

### Task 5: Select and Supervise One Telegram Transport

**Files:**
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/app.rs`
- Modify: `tests/serve_runtime.rs`

**Interfaces:**
- Consumes: `ValidatedTelegramIngressConfig`, `TelegramWebhookServer`, and `TelegramPollingWorker`.
- Produces: `PreparedTelegramGateway::ingress(shutdown)` replacing `webhook(shutdown)`.

- [ ] **Step 1: Write failing preparation tests**

Extend the startup fake to record `deleteWebhook`, `getWebhookInfo`, and polling requests. Add:

```rust
#[tokio::test]
async fn prepare_polling_deletes_webhook_without_binding_listener() -> Result<()> {
    let config = ValidatedTelegramConfig {
        token: "secret-token".into(),
        ingress: ValidatedTelegramIngressConfig::Polling,
    };
    let fixture = StartupFixture::new(WebhookInfo {
        url: String::new(),
        allowed_updates: vec!["message".into()],
        pending_update_count: 6,
    }).await?;
    let prepared = fixture.prepare(config).await?;
    assert_eq!(fixture.api.calls(), vec!["getMe", "deleteWebhook", "getWebhookInfo"]);
    assert_eq!(fixture.api.drop_pending_updates(), Some(false));
    assert!(prepared.is_polling());
    Ok(())
}

#[tokio::test]
async fn prepare_polling_rejects_nonempty_webhook_url() -> Result<()> {
    let config = ValidatedTelegramConfig {
        token: "secret-token".into(),
        ingress: ValidatedTelegramIngressConfig::Polling,
    };
    let fixture = StartupFixture::new(WebhookInfo {
        url: "https://agent.example/webhooks/telegram".into(),
        allowed_updates: vec!["message".into()],
        pending_update_count: 0,
    }).await?;
    let error = fixture.prepare(config).await.unwrap_err();
    assert!(error.to_string().contains("reconciliation mismatch"));
    Ok(())
}
```

Define `StartupFixture` in the test module by moving the repeated in-memory
store, manual clock, identity-link manager, signals, activity hub, artifact
root, and `prepare_with_api` call from the two current preparation tests into
one helper. Its API fake records exact method names and the optional
`drop_pending_updates` value. `is_polling()` is a test-only query on the
prepared transport enum.

Keep `prepare_binds_and_reconciles_webhook_in_order` as the webhook regression test, updated only for the typed config shape.

- [ ] **Step 2: Run preparation tests and verify RED**

Run:

```sh
rtk cargo test interfaces::telegram::tests::prepare_polling -- --nocapture
```

Expected: FAIL because preparation always binds and sets a webhook.

- [ ] **Step 3: Implement typed prepared transport**

Add a private enum:

```rust
enum PreparedTelegramIngress {
    Webhook {
        listener: Mutex<Option<TcpListener>>,
        path: String,
        secret: String,
    },
    Polling,
}
```

In `prepare_with_api`, first match configuration only far enough to bind the
webhook listener or select polling. This preserves the existing webhook
guarantee that the listener is bound before Bot API reconciliation while
ensuring polling never binds a listener. Then call `getMe` once, create the
shared ingress service, and finish the selected reconciliation:

- webhook: keep the pre-bound listener, call `setWebhook`, and validate URL plus allowed updates;
- polling: call `deleteWebhook { drop_pending_updates: false }`, then require `getWebhookInfo().url.is_empty()`.

Replace `webhook()` with `ingress()`, dispatching to the webhook server or `TelegramPollingWorker` with the same API and ingress instances.

- [ ] **Step 4: Update composition and acceptance fakes**

In `app.rs`, register:

```rust
service.component("telegram-ingress", {
    let telegram = telegram.clone();
    let shutdown = shutdown_rx.clone();
    async move { telegram.ingress(shutdown).await }
});
```

Update `tests/serve_runtime.rs` fakes for the split API traits and typed webhook config. Preserve the existing webhook acceptance transcript unchanged.

- [ ] **Step 5: Run gateway and serve tests and verify GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::tests -- --nocapture
rtk cargo test --test serve_runtime telegram_acceptance -- --nocapture
rtk cargo test app::tests -- --nocapture
```

Expected: polling preparation tests pass; webhook acceptance and supervisor tests remain green.

- [ ] **Step 6: Commit**

```sh
rtk git add src/interfaces/telegram.rs src/app.rs tests/serve_runtime.rs
rtk git commit -m "feat(telegram): select polling ingress"
```

---

### Task 6: Document and Verify Both Modes

**Files:**
- Modify: `README.md`
- Modify: `tests/install_script.rs`

**Interfaces:**
- Consumes: final `telegram.mode` configuration contract.
- Produces: operator instructions for webhook and polling installations.

- [ ] **Step 1: Write a failing documentation assertion**

Add to the active documentation test:

```rust
assert!(README.contains("mode: polling"));
assert!(README.contains("deleteWebhook"));
assert!(README.contains("Polling and webhook modes are mutually exclusive"));
```

- [ ] **Step 2: Run the documentation test and verify RED**

Run:

```sh
rtk cargo test --test install_script active_documentation -- --nocapture
```

Expected: FAIL because polling documentation is absent.

- [ ] **Step 3: Update README**

Keep the existing webhook example and add:

```yaml
telegram:
  token: "..."
  mode: polling
```

State that polling needs no public URL or listener, startup calls `deleteWebhook` without dropping pending updates, webhook and polling are mutually exclusive, omission defaults to webhook, and switching modes requires restarting `codrik.service` or the launchd agent. Do not add installer flags because `codrik serve` already owns either mode.

- [ ] **Step 4: Run focused and full verification**

Run:

```sh
rtk cargo test --test install_script -- --nocapture
rtk cargo fmt --check
rtk cargo check
rtk proxy cargo test -- --test-threads=1
rtk cargo clippy --all-targets --all-features
rtk git diff --check
```

Expected: documentation tests pass; build and formatting succeed; the full suite has no failures; clippy has no errors beyond the repository's existing warnings.

- [ ] **Step 5: Commit**

```sh
rtk git add README.md tests/install_script.rs
rtk git commit -m "docs(telegram): document polling mode"
```
