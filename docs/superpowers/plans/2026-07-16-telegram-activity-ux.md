# Telegram Activity UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Telegram draft replies with complete durable answers, four-second typing actions during model generation, and the established elapsed-time tool status.

**Architecture:** The OpenAI stream accumulator becomes authoritative for finalized streamed text when an OpenAI-compatible provider omits text from `response.completed`. Telegram activity remains transient and gateway-local: it consumes runtime activity events, sends retry-safe typing actions, and owns one best-effort tool-status message per work item. Durable final delivery no longer reads or edits `gateway_streams`.

**Tech Stack:** Rust 2024, Tokio paused-time tests, async-openai Responses API types, Reqwest Telegram Bot API, existing `GatewayActivityHub`, SQLite durable delivery.

## Global Constraints

- Run every shell command through `rtk`.
- Work directly on `main`; do not create a worktree.
- Follow TDD for every production change and observe RED before implementation.
- Telegram private-chat messages never use `reply_parameters`.
- Typing starts immediately and repeats every four seconds only while a model step is generating.
- Text deltas never create or edit Telegram messages.
- Tool status uses the previous Russian copy and timing: two-second description coalescing and ten-second elapsed refresh.
- Telegram activity failures are best effort and never fail the actor runner or durable final delivery.
- Durable final text/files remain idempotent, ordered, and restart-safe.
- Never log message text, bot tokens, webhook secrets, identity subjects, or chat addresses.

---

### Task 1: Recover Complete Responses API Stream Text

**Files:**

- Modify: `src/llm/openai.rs`

**Interfaces:**

- Consumes: `ResponseStreamEvent::ResponseOutputTextDelta`,
  `ResponseStreamEvent::ResponseOutputTextDone`, and `ResponseCompleted`.
- Produces: `StreamAccumulator::into_response() -> Result<LlmResponse>` whose
  `content` falls back to finalized/accumulated stream text only when the
  completed response contains no assistant text.

- [ ] **Step 1: Write failing provider-compatibility tests**

Add focused tests beside `stream_accumulator_emits_deltas_and_uses_completed_response`:

```rust
#[tokio::test]
async fn stream_done_text_fills_empty_completed_response() -> Result<()> {
    let mut accumulator = StreamAccumulator::default();
    let mut sink = RecordingSink::default();
    accumulator.push(stream_event(json!({
        "type": "response.output_text.delta",
        "sequence_number": 1,
        "item_id": "msg_1",
        "output_index": 0,
        "content_index": 0,
        "delta": "Пр"
    }))?, &mut sink).await?;
    accumulator.push(stream_event(json!({
        "type": "response.output_text.done",
        "sequence_number": 2,
        "item_id": "msg_1",
        "output_index": 0,
        "content_index": 0,
        "text": "Привет! Полный ответ.",
        "logprobs": null
    }))?, &mut sink).await?;
    accumulator.push(empty_completed_event(3)?, &mut sink).await?;

    assert_eq!(accumulator.into_response()?.content, "Привет! Полный ответ.");
    Ok(())
}

#[tokio::test]
async fn accumulated_deltas_fill_empty_completed_response_without_done_event() -> Result<()> {
    // Push "hello " and "world" deltas, then an empty response.completed.
    assert_eq!(accumulator.into_response()?.content, "hello world");
}
```

The helper `empty_completed_event(sequence_number)` must build a completed
response containing one assistant message with an empty content array.

- [ ] **Step 2: Verify RED**

Run:

```sh
rtk cargo test llm::openai::tests::stream_done_text -- --nocapture
rtk cargo test llm::openai::tests::accumulated_deltas -- --nocapture
```

Expected: both tests fail because `StreamAccumulator` ignores
`response.output_text.done` and trusts the empty completed response.

- [ ] **Step 3: Implement text accumulation and fallback**

Extend the accumulator:

```rust
#[derive(Default)]
struct StreamAccumulator {
    completed: Option<Response>,
    text: std::collections::BTreeMap<(u32, u32), String>,
}
```

On `ResponseOutputTextDelta`, append to the `(output_index, content_index)`
entry before publishing the delta. On `ResponseOutputTextDone`, replace that
entry with `event.text`. In `into_response`, convert the completed response
normally, then apply:

```rust
let fallback = self.text.into_values().collect::<String>();
if response.content.is_empty() && !fallback.is_empty() {
    response.content = fallback;
}
```

Never overwrite nonempty completed-response content.

- [ ] **Step 4: Verify GREEN**

Run:

```sh
rtk cargo test llm::openai -- --nocapture
rtk cargo test runtime::runner -- --nocapture
```

Expected: provider fallback and existing normal-provider tests pass.

- [ ] **Step 5: Commit**

```sh
rtk git add src/llm/openai.rs
rtk git commit -m "fix(openai): recover finalized stream text"
```

---

### Task 2: Remove Private Reply-To and Add Typing Bot API

**Files:**

- Modify: `src/interfaces/telegram/types.rs`
- Modify: `src/interfaces/telegram/api.rs`
- Modify: Telegram API mocks in `src/interfaces/telegram.rs`,
  `src/interfaces/telegram/delivery.rs`,
  `src/interfaces/telegram/streaming.rs`, and `tests/serve_runtime.rs`

**Interfaces:**

- Produces:

```rust
#[derive(Clone, Serialize)]
pub struct SendChatAction {
    pub chat_id: String,
    pub action: TelegramChatAction,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelegramChatAction {
    Typing,
}

#[async_trait]
pub trait TelegramApi {
    async fn send_chat_action(&self, command: SendChatAction)
        -> Result<(), TelegramApiError>;
}
```

- Private `TelegramUpdate::classify` produces a `DeliveryRoute` whose
  `reply_to_external_id` is always `None`.

- [ ] **Step 1: Write failing route and API tests**

Update the private-text classification test to assert:

```rust
assert_eq!(route.reply_to_external_id, None);
```

Add an HTTP adapter test whose mock server asserts a request to
`/botsecret-token/sendChatAction` and a JSON body containing:

```json
{"chat_id":"100","action":"typing"}
```

- [ ] **Step 2: Verify RED**

Run:

```sh
rtk cargo test interfaces::telegram::types -- --nocapture
rtk cargo test interfaces::telegram::api::tests::send_chat_action -- --nocapture
```

Expected: route test sees the inbound message ID and the API test cannot
compile because typed chat-action support does not exist.

- [ ] **Step 3: Implement private routing and chat action**

Change the private route construction to:

```rust
DeliveryRoute::new(gateway, message.chat.id.to_string(), None, 4096, 1024)?
```

Implement `send_chat_action` using retry-safe `post_json`:

```rust
let _: bool = self.post_json("sendChatAction", &command, true).await?;
Ok(())
```

Add the method to every `TelegramApi` mock. Activity-aware mocks record the
chat action; unrelated mocks may use `unreachable!()`.

- [ ] **Step 4: Verify GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::types -- --nocapture
rtk cargo test interfaces::telegram::api -- --nocapture
rtk cargo check
```

Expected: private routes have no reply-to and typed action serialization passes.

- [ ] **Step 5: Commit**

```sh
rtk git add src/interfaces/telegram/types.rs src/interfaces/telegram/api.rs \
  src/interfaces/telegram.rs src/interfaces/telegram/delivery.rs \
  src/interfaces/telegram/streaming.rs tests/serve_runtime.rs
rtk git commit -m "feat(telegram): add private typing actions"
```

---

### Task 3: Replace Draft Streaming with Typing and Tool Status

**Files:**

- Rewrite: `src/interfaces/telegram/streaming.rs`
- Modify: `src/interfaces/telegram.rs`

**Interfaces:**

- `TelegramStreamingWorker<A>` consumes `GatewayActivity` and owns only
  transient in-memory state.
- Constructor:

```rust
pub fn new(api: A, gateway: impl Into<String>) -> Self;
pub(crate) async fn maintain(&self);
```

- Constants:

```rust
const TYPING_INTERVAL: Duration = Duration::from_secs(4);
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
const STATUS_TICK_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_DESCRIPTION: &str = "Работаю над задачей";
const MAX_STATUS_DESCRIPTION_CHARS: usize = 240;
```

- [ ] **Step 1: Replace old draft tests with failing activity tests**

Using paused Tokio time and a recording `TelegramApi`, cover:

```rust
#[tokio::test(start_paused = true)]
async fn model_step_sends_typing_every_four_seconds_without_messages() {
    worker.handle(model_started()).await;
    assert_eq!(api.actions(), vec!["typing"]);
    tokio::time::advance(Duration::from_secs(4)).await;
    worker.maintain().await;
    assert_eq!(api.actions(), vec!["typing", "typing"]);
    assert!(api.sent_messages().is_empty());
}

#[tokio::test(start_paused = true)]
async fn text_deltas_never_create_or_edit_messages() {
    worker.handle(text_delta("Пр")).await;
    assert!(api.sent_messages().is_empty());
    assert!(api.edits().is_empty());
}

#[tokio::test(start_paused = true)]
async fn tool_run_uses_description_and_updates_elapsed_status() {
    worker.handle(model_started()).await;
    worker.handle(description("Проверяю конфигурацию")).await;
    worker.handle(tool_started("bash")).await;
    assert_eq!(api.sent_messages(), vec!["Проверяю конфигурацию — 0 сек"]);
    tokio::time::advance(Duration::from_secs(10)).await;
    worker.maintain().await;
    assert_eq!(api.edits().last(), Some("Проверяю конфигурацию — 10 сек"));
}
```

Also test default description, two-second description coalescing, success,
cancellation, failure terminal copy, typing restart after `ToolFinished` plus
the next `ModelStepStarted`, and swallowed API failures.

- [ ] **Step 2: Verify RED**

Run:

```sh
rtk cargo test interfaces::telegram::streaming -- --nocapture
```

Expected: old tests/implementation send `Thinking…`, edit text deltas, and
have no chat-action API.

- [ ] **Step 3: Implement the transient activity state machine**

Remove `GatewayStreamStore`, `Clock`, `gateway_streams`, text buffers, and
draft-message fencing from this worker. Maintain a `HashMap<StreamKey,
ActivityState>` containing:

```rust
struct ActivityState {
    route: DeliveryRoute,
    started_at: Instant,
    typing: bool,
    next_typing_at: Instant,
    description: String,
    status_message_id: Option<i64>,
    description_dirty: bool,
    next_description_update_at: Instant,
    next_status_tick_at: Instant,
}
```

Event behavior:

- `ModelStepStarted`: set `typing = true`, send typing immediately, schedule
  the next action in four seconds.
- `TextDelta`: ignore.
- `Description`: stop typing, normalize/store description, and create or dirty
  the tool status.
- `ToolStarted`: stop typing and create the default status if absent.
- `ToolFinished`: leave the existing status visible.
- terminal activity: stop typing, terminal-edit an existing status, and remove
  the state.

The run loop uses a 200 ms maintenance interval to service typing,
description, and elapsed deadlines without blocking the activity publisher.
Every Bot API error is swallowed.

- [ ] **Step 4: Update composition**

Construct the worker in `PreparedTelegramGateway::streaming` as:

```rust
TelegramStreamingWorker::new(self.api.clone(), self.gateway.clone())
    .run(self.activity.subscribe(), shutdown)
    .await
```

The prepared gateway no longer passes SQLite or a clock to transient activity.

- [ ] **Step 5: Verify GREEN**

Run:

```sh
rtk cargo test interfaces::telegram::streaming -- --nocapture
rtk cargo test runtime::gateway_activity -- --nocapture
rtk cargo test runtime::runner -- --nocapture
```

Expected: typing/status tests pass and local event publishing remains intact.

- [ ] **Step 6: Commit**

```sh
rtk git add src/interfaces/telegram/streaming.rs src/interfaces/telegram.rs
rtk git commit -m "feat(telegram): restore typing and tool status"
```

---

### Task 4: Remove Draft Coupling from Durable Final Delivery

**Files:**

- Modify: `src/interfaces/telegram/delivery.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `tests/serve_runtime.rs`
- Modify: `README.md`

**Interfaces:**

- `TelegramDeliveryWorker<S, A, C>` requires only `GatewayDeliveryStore` from
  `S`; it never resolves, claims, or edits a transient stream message.
- Final text always uses `sendMessage`; files continue using `sendPhoto` or
  `sendDocument`.

- [ ] **Step 1: Write failing durable-final regression**

Replace the acceptance provider response with the production-shaped stream:

1. delta `Пр`;
2. done text `Привет! Полный ответ.`;
3. completed response with empty assistant content.

Assert:

```rust
assert!(!api.sent_messages().contains(&"Thinking…".to_string()));
assert!(!api.sent_messages().contains(&"Пр".to_string()));
assert_eq!(
    api.sent_messages()
        .iter()
        .filter(|text| text.as_str() == "Привет! Полный ответ.")
        .count(),
    1
);
assert!(api.reply_message_ids().iter().all(Option::is_none));
```

Keep the existing replay and reopened-store count assertions.

- [ ] **Step 2: Verify RED**

Run:

```sh
rtk cargo test --test serve_runtime \
  telegram_webhook_links_runs_and_delivers_without_duplicates -- --nocapture
```

Expected: the current worker sends `Thinking…`/draft edits and final delivery
still tries to claim/edit `gateway_streams`.

- [ ] **Step 3: Remove stream edit/fencing from delivery**

Delete `lock_gateway_stream`, `claim_gateway_stream_for_final`,
`set_gateway_delivery_retry_safe` calls made only for `editMessageText`, and
the `StreamEditTerminal` fallback branch. Text delivery becomes:

```rust
self.api
    .send_message(SendMessage {
        chat_id: delivery.route.address.clone(),
        text: text.clone(),
        reply_parameters: None,
    })
    .await
```

Keep non-idempotent ambiguity handling, claims, renewals, ordering, file
integrity, and retry backoff unchanged.

Remove `GatewayStreamStore` from `PreparedTelegramGateway` and
`prepare_with_api` generic bounds because neither transient activity nor
durable delivery consumes it after this change.

- [ ] **Step 4: Update README**

Replace draft-streaming documentation with:

- typing action every four seconds during model generation;
- tool-only elapsed status messages;
- durable final answer is the only assistant text message;
- private chats do not use reply-to.

- [ ] **Step 5: Verify GREEN**

Run:

```sh
rtk cargo test --test serve_runtime \
  telegram_webhook_links_runs_and_delivers_without_duplicates -- --nocapture
rtk cargo test interfaces::telegram -- --nocapture
rtk cargo test
rtk cargo clippy --all-targets --all-features
rtk cargo fmt --check
rtk cargo check
rtk sh -n scripts/install.sh
rtk git diff --check
```

Expected: the full suite passes; Clippy has zero errors and no new warning from
the changed code.

- [ ] **Step 6: Commit**

```sh
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/delivery.rs \
  tests/serve_runtime.rs README.md
rtk git commit -m "fix(telegram): deliver complete private replies"
```
