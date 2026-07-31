# Reticulum Airtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Send one `Думаю...` status after five seconds and keep Reticulum assistant replies within 500 Unicode characters.

**Architecture:** The actor runner injects a transient Reticulum-only brevity instruction and bounds final plain assistant text before checkpointing. A focused Reticulum activity worker subscribes to the existing `GatewayActivityHub` and enqueues one best-effort durable status delivery per work item. Existing LXMF bridge, ingress, final outbox, and delivery machinery remain authoritative.

**Tech Stack:** Rust 2024, Tokio broadcast/watch/time, SQLite runtime stores, existing Python LXMF bridge.

## Global Constraints

- Status text is exactly `Думаю...`.
- Delay is exactly five seconds after the first `ModelStepStarted` for a Reticulum-routed work item.
- Send at most one status per work item; never stream, edit, repeat, or expose tool details.
- Final Reticulum assistant text is at most 500 Unicode characters.
- Prefer a sentence boundary within 497 characters; append `...` after shortening.
- Do not truncate tool calls, tool observations, gateway-generated responses, Telegram, or CLI output.
- Status delivery is best effort and receives no automatic retry.
- Add no dependency, configuration field, schema migration, or custom LXMF field.
- Preserve unrelated worktree changes, including the existing uncommitted `src/interfaces/reticulum/bridge.py` fix.
- Run every shell command through `rtk`.

---

## File Structure

- Modify `src/runtime/runner.rs`: route-specific transient model instruction and final response bounding.
- Create `src/interfaces/reticulum/activity.rs`: in-memory five-second status timer and idempotent enqueue.
- Modify `src/interfaces/reticulum.rs`: expose activity execution; apply one-attempt status outcome policy.
- Modify `src/app.rs`: share `GatewayActivityHub` with Reticulum, publish routed activity, supervise worker.
- Modify `README.md`: document the status and 500-character response budget.

---

### Task 1: Reticulum Response Budget

**Files:**
- Modify: `src/runtime/runner.rs:33-55, 328-407`
- Test: `src/runtime/runner.rs` test module

**Interfaces:**
- Consumes: `AttachedRun.delivery_route: Option<DeliveryRoute>` and gateway names prefixed by `reticulum:`.
- Produces: private `bounded_reticulum_response(route: Option<&DeliveryRoute>, text: String) -> String` and `RETICULUM_RESPONSE_INSTRUCTIONS`.

- [ ] **Step 1: Write failing route-policy tests**

Add focused unit cases near existing runner system-instruction tests:

```rust
const RETICULUM_GATEWAY: &str = "reticulum:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[test]
fn reticulum_response_limit_preserves_short_and_bounds_unicode() -> Result<()> {
    let route = DeliveryRoute::new(RETICULUM_GATEWAY, "c".repeat(32), None, 256 * 1024, 1)?;
    assert_eq!(
        bounded_reticulum_response(Some(&route), "Коротко.".into()),
        "Коротко."
    );

    let long = format!("{} Конец второго предложения", "я".repeat(490));
    let bounded = bounded_reticulum_response(Some(&route), long);
    assert!(bounded.chars().count() <= 500);
    assert!(bounded.ends_with("..."));
    assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
    Ok(())
}

#[test]
fn non_reticulum_response_is_not_bounded() -> Result<()> {
    let route = DeliveryRoute::new("telegram:1", "2", None, 4096, 1024)?;
    let text = "x".repeat(700);
    assert_eq!(bounded_reticulum_response(Some(&route), text.clone()), text);
    Ok(())
}
```

Extend the existing scripted LLM runner test to capture two requests around a tool call. Assert every request for a Reticulum route contains a system message with `500 Unicode characters`; run the same setup with a Telegram route and assert that instruction is absent. Assert a 600-character final response is checkpointed and emitted as the same bounded text.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
rtk cargo test runtime::runner::tests::reticulum_response -- --nocapture
```

Expected: compilation failure for missing `bounded_reticulum_response`, then assertion failures until route-specific instruction injection exists.

- [ ] **Step 3: Implement the minimal policy**

Add constants and helpers in `src/runtime/runner.rs`:

```rust
const RETICULUM_GATEWAY_PREFIX: &str = "reticulum:";
const RETICULUM_RESPONSE_CHARS: usize = 500;
const RETICULUM_RESPONSE_PREFIX_CHARS: usize = 497;
const RETICULUM_RESPONSE_INSTRUCTIONS: &str =
    "Reticulum airtime is scarce. Answer directly and completely in at most 500 Unicode characters. Omit preambles, repetition, and optional detail.";

fn is_reticulum_route(route: Option<&crate::runtime::gateway::DeliveryRoute>) -> bool {
    route.is_some_and(|route| route.gateway.starts_with(RETICULUM_GATEWAY_PREFIX))
}

fn bounded_reticulum_response(
    route: Option<&crate::runtime::gateway::DeliveryRoute>,
    text: String,
) -> String {
    if !is_reticulum_route(route) || text.chars().count() <= RETICULUM_RESPONSE_CHARS {
        return text;
    }
    let prefix = text
        .chars()
        .take(RETICULUM_RESPONSE_PREFIX_CHARS)
        .collect::<Vec<_>>();
    let end = prefix
        .iter()
        .rposition(|character| matches!(character, '.' | '!' | '?' | '\n'))
        .map_or(prefix.len(), |index| index + 1);
    format!("{}...", prefix[..end].iter().collect::<String>().trim_end())
}
```

At each LLM request, inject the transient instruction after the existing base system instruction without mutating `messages`:

```rust
if is_reticulum_route(run.delivery_route.as_ref()) {
    request_messages.insert(0, Message::system(RETICULUM_RESPONSE_INSTRUCTIONS));
}
```

Before constructing `FinalizeRun`, bind once and use the same value for memory and outbox:

```rust
let content = bounded_reticulum_response(
    run.delivery_route.as_ref(),
    response.content,
);
// final_messages: Message::assistant(content.clone())
// payload: OutboxPayload::Text { text: content }
```

Do not apply the helper to `Message::assistant_tool_calls`; model content accompanying tool calls remains unchanged because tool protocol integrity outranks airtime optimization.

- [ ] **Step 4: Run focused and runner tests**

Run:

```bash
rtk cargo test runtime::runner::tests::reticulum_response -- --nocapture
rtk cargo test runtime::runner::tests -- --nocapture
```

Expected: all selected tests pass; captured Telegram requests contain no Reticulum instruction; stored and delivered final text match.

- [ ] **Step 5: Commit**

```bash
rtk git add src/runtime/runner.rs
rtk git commit -m "feat(reticulum): bound agent replies"
```

---

### Task 2: Delayed Thinking Status

**Files:**
- Create: `src/interfaces/reticulum/activity.rs`
- Modify: `src/interfaces/reticulum.rs:1-27, 29-249`
- Test: `src/interfaces/reticulum/activity.rs` test module
- Test: `src/interfaces/reticulum.rs` test module

**Interfaces:**
- Consumes: `GatewayActivity`, `GatewayActivityEvent`, `GatewayActivityHub::subscribe()`, `GatewayDeliveryStore::enqueue_gateway_delivery`, `DeliveryRoute`, `Clock`.
- Produces: `ReticulumActivityWorker<S, C>::new(store, clock, gateway)` and `run(activity, shutdown) -> Result<()>`.
- Produces: status intent keys beginning `reticulum-thinking:<local-destination>:`; `PreparedReticulumGateway::activity(shutdown) -> Result<()>`.

- [ ] **Step 1: Write failing activity worker tests**

Create `src/interfaces/reticulum/activity.rs` with test scaffolding using `SqliteRuntimeStore::open_in_memory()`, `ManualClock`, `GatewayActivityHub`, and paused Tokio time. Cover these observable cases:

```rust
#[tokio::test(start_paused = true)]
async fn thinking_status_is_enqueued_once_after_five_seconds() -> Result<()> {
    let store = SqliteRuntimeStore::open_in_memory().await?;
    let clock = ManualClock::new(1_000);
    let gateway = format!("reticulum:{DESTINATION}");
    let worker = ReticulumActivityWorker::new(store.clone(), clock, gateway.clone());
    let route = DeliveryRoute::new(gateway, SOURCE, Some("a".repeat(64)), 256 * 1024, 1)?;
    let work = WorkItemId::new();

    worker.handle(activity(work.clone(), route, AgentActivityEvent::ModelStepStarted)).await;
    tokio::time::advance(Duration::from_millis(4_999)).await;
    worker.maintain().await;
    assert!(claim_reticulum(&store).await?.is_empty());

    tokio::time::advance(Duration::from_millis(1)).await;
    worker.maintain().await;
    let deliveries = claim_reticulum(&store).await?;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].payload, OutboxPayload::Text { text: "Думаю...".into() });

    worker.handle(activity(work, deliveries[0].route.clone(), AgentActivityEvent::ModelStepStarted)).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    worker.maintain().await;
    assert!(claim_reticulum(&store).await?.is_empty());
    Ok(())
}
```

Add separate tests where `Completed`, `Failed`, and `Cancelled` arrive at 4.999 seconds and no delivery appears. Add one event with gateway `telegram:1` and one with a different Reticulum destination; both must be ignored. Test duplicate enqueue safety by calling `maintain()` twice after the deadline.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
rtk cargo test interfaces::reticulum::activity::tests -- --nocapture
```

Expected: compilation failure because `ReticulumActivityWorker` does not exist.

- [ ] **Step 3: Implement the in-memory timer and best-effort enqueue**

Implement one focused worker:

```rust
const THINKING_DELAY: Duration = Duration::from_secs(5);
const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(100);
const THINKING_TEXT: &str = "Думаю...";

struct ActivityState {
    route: DeliveryRoute,
    due_at: Instant,
}

pub struct ReticulumActivityWorker<S, C> {
    store: S,
    clock: C,
    gateway: String,
    states: Mutex<HashMap<WorkItemId, ActivityState>>,
    sent: Mutex<HashSet<WorkItemId>>,
}
```

Behavior:

- Ignore events whose `route.gateway != self.gateway`.
- On the first `ModelStepStarted`, insert state only when the work item is absent and not in `sent`.
- On `Completed`, `Failed`, or `Cancelled`, remove state and `sent` bookkeeping.
- Ignore `Description`, tool events, and `TextDelta`.
- In `maintain`, remove due state before awaiting SQLite, insert the work item into `sent`, then enqueue:

```rust
NewGatewayDelivery::new(
    format!("reticulum-thinking:{}:{work_item_id}", self.gateway),
    None,
    0,
    state.route,
    OutboxPayload::Text { text: THINKING_TEXT.into() },
)
```

Log cosmetic enqueue failure without message text or route address:

```rust
if let Err(error) = self.store.enqueue_gateway_delivery(delivery, self.clock.now()).await {
    eprintln!("reticulum activity: failed to enqueue thinking status: {error:#}");
}
```

The deterministic key provides SQLite idempotency. `run` follows Telegram's broadcast/watch maintenance loop and tolerates `Lagged`.

- [ ] **Step 4: Make status delivery one-attempt**

In `src/interfaces/reticulum.rs`, add:

```rust
const THINKING_INTENT_PREFIX: &str = "reticulum-thinking:";

fn is_thinking_delivery(delivery: &ClaimedGatewayDelivery) -> bool {
    delivery.intent_key.starts_with(THINKING_INTENT_PREFIX)
}
```

In `transition_delivery`, map `Retryable` for a thinking delivery to `fail_gateway_delivery(... FailedTerminal, "reticulum_thinking_failed", ...)` instead of scheduling retry. Keep final replies unchanged. If bridge submission itself errors after the status is claimed, preserve bridge lifecycle failure semantics; an unhealthy shared bridge still fails its supervised component.

Add a focused test creating a thinking delivery, invoking `transition_delivery` with `Retryable`, and asserting `FailedTerminal`; create a normal delivery and assert it remains `FailedRetryable`.

- [ ] **Step 5: Expose activity execution from the prepared gateway**

Add `pub mod activity;`. Store a clone of `GatewayActivityHub` in `PreparedReticulumGateway`, accept it in `prepare`, and expose:

```rust
pub async fn activity(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
    activity::ReticulumActivityWorker::new(
        self.store.clone(),
        self.clock.clone(),
        self.gateway.clone(),
    )
    .run(self.activity.subscribe(), shutdown)
    .await
}
```

Name the field `activity_hub` to avoid collision with the method. Update all direct `prepare` calls in Reticulum tests with `GatewayActivityHub::default()`.

- [ ] **Step 6: Run focused Reticulum tests**

Run:

```bash
rtk cargo test interfaces::reticulum::activity::tests -- --nocapture
rtk cargo test interfaces::reticulum::tests -- --nocapture
rtk cargo test interfaces::reticulum -- --nocapture
```

Expected: all selected tests pass; status retry becomes terminal; final retry remains retryable.

- [ ] **Step 7: Commit**

```bash
rtk git add src/interfaces/reticulum.rs src/interfaces/reticulum/activity.rs
rtk git commit -m "feat(reticulum): send delayed thinking status"
```

---

### Task 3: Runtime Composition and Documentation

**Files:**
- Modify: `src/app.rs:220-300, 354-375`
- Modify: `README.md` Reticulum section
- Test: `src/app.rs` test module
- Test: `tests/install_script.rs` if README assertions require adjustment

**Interfaces:**
- Consumes: `PreparedReticulumGateway::activity`, shared `GatewayActivityHub`.
- Produces: supervised component name `reticulum-activity`; routed activity publication whenever Telegram or Reticulum exists.

- [ ] **Step 1: Write failing composition test**

Extend existing `serve_with_dependencies`/startup test scaffolding with Reticulum configured and Telegram absent. Use a fake bridge that emits `ready`, a scripted LLM blocked beyond five paused seconds, and the in-memory/test runtime paths already used in `app.rs` tests. Assert a Reticulum `Думаю...` gateway delivery appears. This proves `CompositeRuntimeEventPublisher` is active without Telegram and the activity component is supervised.

Also retain a no-gateway case asserting local event publication works unchanged.

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
rtk cargo test app::tests::reticulum_activity -- --nocapture
```

Expected: no status delivery because `app.rs` currently builds `CompositeRuntimeEventPublisher` only for Telegram and starts no Reticulum activity component.

- [ ] **Step 3: Share and publish gateway activity**

Pass `gateway_activity.clone()` into `reticulum::prepare`. Select the publisher when either gateway exists:

```rust
let events: Arc<dyn RuntimeEventPublisher> = if telegram.is_some() || reticulum.is_some() {
    Arc::new(CompositeRuntimeEventPublisher::new(
        hub.clone(),
        gateway_activity.clone(),
    ))
} else {
    hub.clone()
};
```

Keep passing the same hub clone to Telegram. Do not create one hub per gateway.

- [ ] **Step 4: Supervise Reticulum activity independently**

Before moving `reticulum` into the existing component, register both clones:

```rust
if let Some(reticulum) = reticulum {
    service.component("reticulum-activity", {
        let reticulum = reticulum.clone();
        let shutdown = shutdown_rx.clone();
        async move { reticulum.activity(shutdown).await }
    });
    service.component("reticulum", {
        let shutdown = shutdown_rx.clone();
        async move { reticulum.run(shutdown).await }
    });
}
```

- [ ] **Step 5: Document visible behavior**

In the README Reticulum usage section add concise operator-facing text:

```markdown
For agent requests that take longer than five seconds, Codrik sends one
`Думаю...` status before the final reply. Reticulum replies are intentionally
concise and limited to 500 Unicode characters to reduce airtime. CLI and
Telegram replies are unaffected.
```

Do not document tuning because delay, text, and limit are deliberately fixed.

- [ ] **Step 6: Run composition and documentation tests**

Run:

```bash
rtk cargo test app::tests::reticulum_activity -- --nocapture
rtk cargo test --test install_script -- --nocapture
```

Expected: both commands pass.

- [ ] **Step 7: Commit**

```bash
rtk git add src/app.rs README.md tests/install_script.rs
rtk git commit -m "feat(runtime): compose Reticulum activity"
```

---

### Task 4: Final Verification and Bridge Fix Isolation

**Files:**
- Verify: all changed files
- Existing unrelated-to-airtime change: `src/interfaces/reticulum/bridge.py`

**Interfaces:**
- Consumes: completed Tasks 1-3.
- Produces: green workspace with airtime commits separate from the pending bridge identity-discovery fix.

- [ ] **Step 1: Inspect scope before verification**

Run:

```bash
rtk git status --short
rtk git diff --stat
rtk git diff -- src/interfaces/reticulum/bridge.py
```

Confirm airtime commits did not absorb or revert the existing bridge fix. If that bridge fix is still uncommitted, leave it untouched for a separate bugfix commit/review.

- [ ] **Step 2: Run all project gates**

Run:

```bash
rtk python3 src/interfaces/reticulum/bridge.py --self-check
rtk cargo fmt --check
rtk cargo check
rtk cargo test
rtk cargo clippy --all-targets --all-features
```

Expected: every command exits 0; full tests report no failures.

- [ ] **Step 3: Manual LXMF smoke test**

Run:

```bash
rtk cargo run -- serve
```

From a linked Sideband identity:

1. Send a prompt whose model response takes more than five seconds.
2. Verify exactly one `Думаю...` message arrives before the final answer.
3. Verify the final answer contains at most 500 Unicode characters.
4. Send a fast prompt; verify no `Думаю...` message appears.
5. Confirm Telegram and CLI responses retain their existing length behavior.

- [ ] **Step 4: Inspect final history and status**

Run:

```bash
rtk git log --oneline -8
rtk git status --short --branch
```

Expected: three focused airtime commits; only explicitly preserved pre-existing changes remain uncommitted.
