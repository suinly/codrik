# Telegram Rich Markdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver durable Telegram text through Rich Messages with a safe plain-text fallback after definitive formatting rejection.

**Architecture:** Extend the Telegram API abstraction with a typed `sendRichMessage` command whose `rich_message.markdown` field carries the original LLM output. The durable delivery worker tries rich delivery first and falls back to ordinary `sendMessage` only after a terminal rejection, preserving the current one-delivery/one-successful-message and retry semantics.

**Tech Stack:** Rust 2024, Tokio, async-trait, serde, reqwest, Telegram Bot API 10.2 Rich Messages, SQLite-backed gateway deliveries.

## Global Constraints

- Run every shell command through `rtk`.
- Use `apply_patch` for source and documentation edits.
- Durable text payloads use `sendRichMessage`; tool activity remains plain `sendMessage`.
- Pass the original assistant Markdown without local MarkdownV2 or HTML conversion.
- Keep the durable Telegram text chunk limit at 4096 characters.
- Fall back to plain text only after `TelegramApiErrorClass::Terminal`.
- Never fall back after retryable or outcome-unknown rich delivery.
- Preserve private-chat `reply_parameters: None`.
- Do not change file transport or caption formatting.
- Do not add a local Markdown parser or new parsing dependency.

---

### Task 1: Add the typed Telegram Rich Message API

**Files:**
- Modify: `src/interfaces/telegram/api.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/interfaces/telegram/activity.rs`
- Modify: `src/interfaces/telegram/delivery.rs`
- Modify: `tests/serve_runtime.rs`

**Interfaces:**
- Consumes: existing `TelegramApi`, `ReqwestTelegramApi::post_json`, and `TelegramMessageRef`.
- Produces:
  - `InputRichMessage { markdown: String }`
  - `SendRichMessage { chat_id: String, rich_message: InputRichMessage }`
  - `TelegramApi::send_rich_message(SendRichMessage) -> Result<TelegramMessageRef, TelegramApiError>`

- [ ] **Step 1: Write the failing Bot API request test**

Add the imports and test in `src/interfaces/telegram/api.rs`:

```rust
use super::{
    InputRichMessage, ReqwestTelegramApi, SendChatAction, SendMessage,
    SendRichMessage, TelegramApi, TelegramApiErrorClass, TelegramChatAction,
};

#[tokio::test]
async fn send_rich_message_posts_original_markdown() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = vec![0_u8; 8192];
        let read = socket.read(&mut request).await?;
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("POST /botsecret-token/sendRichMessage "));
        assert!(request.contains(r#""chat_id":"100""#));
        assert!(request.contains(
            r#""rich_message":{"markdown":"# Title\n\n| A | B |"}"#
        ));
        assert!(!request.contains("parse_mode"));
        assert!(!request.contains("reply_parameters"));
        let body = r#"{"ok":true,"result":{"message_id":88}}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
        anyhow::Ok(())
    });
    let api = ReqwestTelegramApi::with_base_url("secret-token", &base)?;

    let sent = api
        .send_rich_message(SendRichMessage {
            chat_id: "100".into(),
            rich_message: InputRichMessage {
                markdown: "# Title\n\n| A | B |".into(),
            },
        })
        .await?;

    assert_eq!(sent.message_id, 88);
    server.await??;
    Ok(())
}
```

- [ ] **Step 2: Run the test to verify RED**

Run:

```bash
rtk cargo test interfaces::telegram::api::tests::send_rich_message_posts_original_markdown -- --nocapture
```

Expected: compilation fails because `InputRichMessage`, `SendRichMessage`, and
`TelegramApi::send_rich_message` do not exist.

- [ ] **Step 3: Add typed commands and the API method**

In `src/interfaces/telegram/api.rs`, add:

```rust
#[derive(Clone, Serialize)]
pub struct InputRichMessage {
    pub markdown: String,
}

#[derive(Clone, Serialize)]
pub struct SendRichMessage {
    pub chat_id: String,
    pub rich_message: InputRichMessage,
}
```

Extend `TelegramApi`:

```rust
async fn send_rich_message(
    &self,
    command: SendRichMessage,
) -> Result<TelegramMessageRef, TelegramApiError>;
```

Implement it for `ReqwestTelegramApi`:

```rust
async fn send_rich_message(
    &self,
    command: SendRichMessage,
) -> Result<TelegramMessageRef, TelegramApiError> {
    self.post_json("sendRichMessage", &command, false).await
}
```

Add temporary compile-only implementations to every test/mock `TelegramApi`
implementation in:

- `src/interfaces/telegram.rs`
- `src/interfaces/telegram/activity.rs`
- `src/interfaces/telegram/delivery.rs`
- `tests/serve_runtime.rs`

Use this shape until the later tasks add recording behavior:

```rust
async fn send_rich_message(
    &self,
    _command: SendRichMessage,
) -> Result<TelegramMessageRef, TelegramApiError> {
    unreachable!()
}
```

Import `SendRichMessage` alongside the other API command types in each module.

- [ ] **Step 4: Run focused API and compile checks**

Run:

```bash
rtk cargo test interfaces::telegram::api
rtk cargo check
rtk cargo fmt --check
```

Expected: API tests pass and every `TelegramApi` implementation compiles.

- [ ] **Step 5: Commit Task 1**

```bash
rtk git add src/interfaces/telegram/api.rs src/interfaces/telegram.rs src/interfaces/telegram/activity.rs src/interfaces/telegram/delivery.rs tests/serve_runtime.rs
rtk git commit -m "feat(telegram): add rich message API"
```

---

### Task 2: Prefer rich Markdown and fall back only after terminal rejection

**Files:**
- Modify: `src/interfaces/telegram/delivery.rs`

**Interfaces:**
- Consumes:
  - `TelegramApi::send_rich_message`
  - `SendRichMessage`
  - `InputRichMessage`
  - `TelegramApiErrorClass`
- Produces: text delivery behavior that returns exactly one successful
  `TelegramMessageRef`, using plain fallback only after a terminal rich error.

- [ ] **Step 1: Refactor the recording API for independent rich/plain outcomes**

In `src/interfaces/telegram/delivery.rs` tests, replace the shared response
queue with:

```rust
#[derive(Clone, Default)]
struct RecordingApi {
    rich_messages: Arc<Mutex<Vec<(String, String)>>>,
    messages: Arc<Mutex<Vec<(String, String)>>>,
    edits: Arc<Mutex<Vec<(i64, String)>>>,
    files: Arc<Mutex<Vec<(&'static str, PathBuf)>>>,
    rich_responses: Arc<Mutex<VecDeque<TelegramApiErrorClass>>>,
    plain_responses: Arc<Mutex<VecDeque<TelegramApiErrorClass>>>,
    delay: Duration,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}
```

Add helpers:

```rust
fn with_error(class: TelegramApiErrorClass) -> Self {
    let api = Self::default();
    api.plain_responses.lock().unwrap().push_back(class);
    api
}

fn with_rich_error(class: TelegramApiErrorClass) -> Self {
    let api = Self::default();
    api.rich_responses.lock().unwrap().push_back(class);
    api
}

fn with_rich_and_plain_errors(
    rich: TelegramApiErrorClass,
    plain: TelegramApiErrorClass,
) -> Self {
    let api = Self::with_rich_error(rich);
    api.plain_responses.lock().unwrap().push_back(plain);
    api
}
```

Change the mock response helper to accept the method and queue:

```rust
async fn response(
    &self,
    method: &'static str,
    responses: &Mutex<VecDeque<TelegramApiErrorClass>>,
) -> Result<TelegramMessageRef, TelegramApiError> {
    let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
    self.max_active.fetch_max(active, Ordering::SeqCst);
    if !self.delay.is_zero() {
        tokio::time::sleep(self.delay).await;
    }
    self.active.fetch_sub(1, Ordering::SeqCst);
    if let Some(class) = responses.lock().unwrap().pop_front() {
        Err(TelegramApiError::classified(
            class,
            method,
            "injected failure",
        ))
    } else {
        Ok(TelegramMessageRef { message_id: 77 })
    }
}
```

Implement rich recording:

```rust
async fn send_rich_message(
    &self,
    command: SendRichMessage,
) -> Result<TelegramMessageRef, TelegramApiError> {
    self.rich_messages.lock().unwrap().push((
        command.chat_id,
        command.rich_message.markdown,
    ));
    self.response("sendRichMessage", &self.rich_responses).await
}
```

Update `send_message`, `send_photo`, and `send_document` to pass the appropriate
queue to `response`.

- [ ] **Step 2: Write failing delivery tests**

Add these tests:

```rust
#[tokio::test]
async fn final_markdown_uses_rich_message_without_plain_send() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let api = RecordingApi::default();
    let worker = TelegramDeliveryWorker::new(
        store,
        api.clone(),
        ManualClock::new(10),
        "telegram:900",
        "worker-1",
        std::env::temp_dir(),
    );
    let delivery = claimed_text("# Heading\n\n| A | B |")?;

    worker
        .send_payload(&delivery)
        .await
        .expect("rich send succeeds");

    assert_eq!(
        *api.rich_messages.lock().unwrap(),
        vec![("100".into(), "# Heading\n\n| A | B |".into())]
    );
    assert!(api.messages.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn terminal_rich_rejection_falls_back_to_plain_text_once() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let api = RecordingApi::with_rich_error(TelegramApiErrorClass::Terminal);
    let worker = delivery_worker(store, api.clone());
    let delivery = claimed_text("broken **markdown")?;

    worker
        .send_payload(&delivery)
        .await
        .expect("plain fallback succeeds");

    assert_eq!(api.rich_messages.lock().unwrap().len(), 1);
    assert_eq!(
        *api.messages.lock().unwrap(),
        vec![("100".into(), "broken **markdown".into())]
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_rich_failures_never_fall_back() -> Result<()> {
    for class in [
        TelegramApiErrorClass::Retryable { retry_after: None },
        TelegramApiErrorClass::OutcomeUnknown,
    ] {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let api = RecordingApi::with_rich_error(class.clone());
        let worker = delivery_worker(store, api.clone());
        let delivery = claimed_text("**markdown**")?;

        let error = worker.send_payload(&delivery).await.unwrap_err();

        assert!(matches!(error, super::DeliveryError::Api(_)));
        assert!(api.messages.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn plain_fallback_error_controls_delivery_result() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let api = RecordingApi::with_rich_and_plain_errors(
        TelegramApiErrorClass::Terminal,
        TelegramApiErrorClass::OutcomeUnknown,
    );
    let worker = delivery_worker(store, api);
    let delivery = claimed_text("broken **markdown")?;

    let error = worker.send_payload(&delivery).await.unwrap_err();

    assert!(matches!(
        error,
        super::DeliveryError::Api(ref error)
            if error.class() == TelegramApiErrorClass::OutcomeUnknown
    ));
    Ok(())
}
```

Add focused test helpers rather than repeating the full claim:

```rust
fn claimed_text(text: &str) -> Result<ClaimedGatewayDelivery> {
    Ok(ClaimedGatewayDelivery {
        claim: GatewayDeliveryClaim {
            id: GatewayDeliveryId::new(),
            owner: "worker-1".into(),
            expires_at: Timestamp(1_000),
        },
        intent_key: "final:0".into(),
        source_outbox_id: None,
        work_item_id: Some(WorkItemId::new()),
        ordinal: 0,
        route: route("100")?,
        payload: OutboxPayload::Text { text: text.into() },
        attempt_count: 1,
        remote_message_id: None,
    })
}
```

Adapt exact imports to the existing grouped module imports.

Add the worker helper used above:

```rust
fn delivery_worker(
    store: SqliteRuntimeStore,
    api: RecordingApi,
) -> TelegramDeliveryWorker<SqliteRuntimeStore, RecordingApi, ManualClock> {
    TelegramDeliveryWorker::new(
        store,
        api,
        ManualClock::new(10),
        "telegram:900",
        "worker-1",
        std::env::temp_dir(),
    )
}
```

- [ ] **Step 3: Run the tests to verify RED**

Run:

```bash
rtk cargo test interfaces::telegram::delivery::tests::final_markdown_uses_rich_message_without_plain_send -- --nocapture
rtk cargo test interfaces::telegram::delivery::tests::terminal_rich_rejection_falls_back_to_plain_text_once -- --nocapture
rtk cargo test interfaces::telegram::delivery::tests::uncertain_rich_failures_never_fall_back -- --nocapture
```

Expected: tests fail because text delivery still calls only `send_message`.

- [ ] **Step 4: Implement the rich-first delivery helper**

In `TelegramDeliveryWorker`, add:

```rust
async fn send_text(
    &self,
    delivery: &ClaimedGatewayDelivery,
    text: &str,
) -> std::result::Result<TelegramMessageRef, DeliveryError> {
    match self
        .api
        .send_rich_message(SendRichMessage {
            chat_id: delivery.route.address.clone(),
            rich_message: InputRichMessage {
                markdown: text.to_owned(),
            },
        })
        .await
    {
        Ok(message) => Ok(message),
        Err(error) if error.class() == TelegramApiErrorClass::Terminal => self
            .api
            .send_message(SendMessage {
                chat_id: delivery.route.address.clone(),
                text: text.to_owned(),
                reply_parameters: None,
            })
            .await
            .map_err(DeliveryError::Api),
        Err(error) => Err(DeliveryError::Api(error)),
    }
}
```

Replace the text arm in `send_payload` with:

```rust
OutboxPayload::Text { text } => self.send_text(delivery, text).await,
```

Do not change file handling.

- [ ] **Step 5: Run delivery verification**

Run:

```bash
rtk cargo test interfaces::telegram::delivery
rtk cargo check
rtk cargo fmt --check
```

Expected: all delivery tests pass. Existing retry, file, concurrency, and
private no-reply tests remain green.

- [ ] **Step 6: Commit Task 2**

```bash
rtk git add src/interfaces/telegram/delivery.rs
rtk git commit -m "feat(telegram): deliver rich Markdown"
```

---

### Task 3: Cover the runtime path and document Rich Messages

**Files:**
- Modify: `tests/serve_runtime.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: `TelegramApi::send_rich_message` and the rich-first delivery
  behavior from Tasks 1-2.
- Produces: acceptance proof that durable Telegram text reaches
  `sendRichMessage` unchanged after reopening SQLite.

- [ ] **Step 1: Make the Telegram acceptance mock record rich messages**

In `tests/serve_runtime.rs`, add:

```rust
rich_sent: Arc<Mutex<Vec<String>>>,
```

Implement:

```rust
async fn send_rich_message(
    &self,
    command: SendRichMessage,
) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
    self.rich_sent
        .lock()
        .unwrap()
        .push(command.rich_message.markdown);
    Ok(TelegramMessageRef { message_id: 78 })
}
```

Keep `send_message` recording link responses and transient tool statuses.

- [ ] **Step 2: Change the acceptance final to representative Rich Markdown**

Use a final payload that covers the main Telegram Rich Markdown block classes:

```rust
const RICH_FINAL: &str = concat!(
    "# Result\n\n",
    "**Bold** and [link](https://example.com)\n\n",
    "- first\n- second\n\n",
    "| Name | Value |\n| --- | --- |\n| time | 22:45 |\n\n",
    "```rust\nlet ready = true;\n```\n\n",
    "> quoted\n\n",
    "||spoiler|| and $x^2$\n\n",
    "<details><summary>More</summary>Details</details>",
);
```

Set the injected response's `final_text` to `RICH_FINAL`.

Replace the final delivery assertions with:

```rust
    assert!(api.sent.lock().unwrap().is_empty());
    assert_eq!(
        *api.rich_sent.lock().unwrap(),
        vec!["This channel is now linked.", RICH_FINAL]
    );
    assert!(api.reply_message_ids.lock().unwrap().is_empty());
assert!(!api.actions.lock().unwrap().is_empty());
```

The replay and SQLite count assertions remain unchanged.

- [ ] **Step 3: Run the acceptance test to verify behavior**

Run:

```bash
rtk cargo test telegram_webhook_links_runs_and_delivers_without_duplicates -- --nocapture
```

Expected: PASS with the link response and exact assistant Markdown each recorded
once by `send_rich_message`.

- [ ] **Step 4: Update README**

Replace the final-text paragraph in the Telegram section with:

```markdown
Durable Telegram text is delivered through Rich Messages, so supported Rich
Markdown constructs such as headings, lists, tables, fenced code, links,
quotations, spoilers, formulas, and details blocks render natively. Codrik
passes text to Telegram unchanged. If Telegram definitively rejects a rich
message, Codrik sends the same chunk as readable plain text. Retryable or
outcome-unknown rich sends never trigger fallback, avoiding duplicate messages.

The durable Telegram text chunk limit remains 4096 characters. A chunk boundary
may split Markdown syntax; if Telegram rejects that chunk, the plain-text
fallback preserves its content.
```

Keep the existing typing, tool status, no-reply, SQLite durability, and retry
documentation.

- [ ] **Step 5: Run task verification**

Run:

```bash
rtk cargo test telegram_webhook_links_runs_and_delivers_without_duplicates
rtk cargo test interfaces::telegram
rtk cargo fmt --check
rtk git diff --check
```

Expected: all selected tests pass and formatting is clean.

- [ ] **Step 6: Commit Task 3**

```bash
rtk git add tests/serve_runtime.rs README.md
rtk git commit -m "docs(telegram): document rich Markdown"
```

---

### Task 4: Final verification and review

**Files:**
- Review only: all files changed by Tasks 1-3

**Interfaces:**
- Consumes: the complete implementation.
- Produces: verified, reviewed changes ready on `main`.

- [ ] **Step 1: Run focused regression tests**

```bash
rtk cargo test interfaces::telegram::api
rtk cargo test interfaces::telegram::delivery
rtk cargo test telegram_webhook_links_runs_and_delivers_without_duplicates
```

Expected: all focused suites pass.

- [ ] **Step 2: Run full verification without the known concurrent SQLite flake**

Run:

```bash
rtk proxy cargo test -- --test-threads=1
rtk cargo clippy --all-targets --all-features
rtk cargo fmt --check
rtk git diff --check
```

Expected:

- full sequential test suite passes;
- clippy reports no errors (existing repository warnings may remain);
- formatting and diff checks pass.

- [ ] **Step 3: Request independent review**

Ask the reviewer to verify:

- raw Markdown reaches `sendRichMessage` unchanged;
- only terminal rich errors fall back;
- retryable/outcome-unknown errors never issue a second send;
- private replies remain disabled;
- file and activity transports remain plain and unchanged;
- acceptance coverage includes restart-safe durable delivery.

Fix every Critical or Important finding and rerun Step 2.

- [ ] **Step 4: Confirm repository state**

```bash
rtk git status --short
rtk git log -5 --oneline
```

Expected: clean worktree and the three focused implementation commits after the
design and plan commits.
