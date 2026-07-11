# Telegram Session Superseding Runs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cancel an in-flight Telegram generation when a newer ordinary message arrives in the same chat/session, persist every message in order, and generate only for the newest message with the complete history.

**Architecture:** Add a Telegram-only coordinator keyed by `(ChatId, session_id)`. Registration cancels the prior `RunContext` and returns a run permit sharing a fair Tokio mutex; handlers acquire that mutex in registration order, so cancelled handlers still append their user message through the existing `Agent` entry point and stop before LLM generation, while the newest handler generates from full history.

**Tech Stack:** Rust 2024, Tokio `Mutex`, `RunContext`/`CancellationToken`, teloxide, existing `Agent` and `FileMemoryStore`, async unit tests.

## Global Constraints

- Scope the behavior to the Telegram gateway; do not change CLI behavior or the public `Agent` interface.
- Key coordination by both Telegram chat ID and resolved session ID.
- Every ordinary message is persisted exactly once and in arrival order; do not combine or debounce messages.
- Telegram commands bypass coordination, do not enter history, and do not cancel active work.
- Cancelled runs emit neither a final Telegram answer nor a gateway error.
- Different chats and different sessions must remain independent.
- Keep all shell commands prefixed with `rtk`.

## File Structure

- Create `src/interfaces/telegram/run_coordinator.rs`: Telegram session key, shared coordinator state, run registration/permit lifecycle, and focused concurrency tests.
- Modify `src/interfaces/telegram.rs`: construct and inject the coordinator, resolve commands before registration, and execute ordinary messages under a run permit.
- Modify tests inside `src/interfaces/telegram.rs`: verify cancellation-result behavior at the gateway boundary.

---

### Task 1: Session-scoped run coordinator

**Files:**
- Create: `src/interfaces/telegram/run_coordinator.rs`
- Modify: `src/interfaces/telegram.rs:15-19`

**Interfaces:**
- Consumes: `crate::llm::client::RunContext`, `teloxide::types::ChatId`, Tokio synchronization primitives.
- Produces: `TelegramRunCoordinator::new()`, `TelegramRunCoordinator::register(chat_id: ChatId, session_id: impl Into<String>) -> TelegramRunPermit`, `TelegramRunPermit::context(&self) -> &RunContext`, `TelegramRunPermit::enter(&self) -> OwnedMutexGuard<()>`, and `TelegramRunPermit::finish(self)`.

- [ ] **Step 1: Add failing coordinator tests**

Create the module declaration in `src/interfaces/telegram.rs`:

```rust
mod run_coordinator;
```

Create `src/interfaces/telegram/run_coordinator.rs` with tests describing the public behavior:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use teloxide::types::ChatId;
    use tokio::sync::Barrier;

    use super::TelegramRunCoordinator;

    #[tokio::test]
    async fn newer_run_cancels_previous_run_in_same_session() {
        let coordinator = TelegramRunCoordinator::new();
        let first = coordinator.register(ChatId(1), "session-a").await;
        let second = coordinator.register(ChatId(1), "session-a").await;

        assert!(first.context().is_cancelled());
        assert!(!second.context().is_cancelled());
    }

    #[tokio::test]
    async fn runs_in_different_keys_do_not_cancel_each_other() {
        let coordinator = TelegramRunCoordinator::new();
        let first_chat = coordinator.register(ChatId(1), "session-a").await;
        let second_chat = coordinator.register(ChatId(2), "session-a").await;
        let second_session = coordinator.register(ChatId(1), "session-b").await;

        assert!(!first_chat.context().is_cancelled());
        assert!(!second_chat.context().is_cancelled());
        assert!(!second_session.context().is_cancelled());
    }

    #[tokio::test]
    async fn runs_enter_session_in_registration_order() {
        let coordinator = TelegramRunCoordinator::new();
        let first = coordinator.register(ChatId(1), "session-a").await;
        let first_guard = first.enter().await;
        let second = coordinator.register(ChatId(1), "session-a").await;
        let barrier = Arc::new(Barrier::new(2));
        let reached = barrier.clone();
        let task = tokio::spawn(async move {
            let _guard = second.enter().await;
            reached.wait().await;
        });

        assert!(!task.is_finished());
        drop(first_guard);
        barrier.wait().await;
        task.await.unwrap();
    }

    #[tokio::test]
    async fn stale_finish_does_not_remove_newer_run() {
        let coordinator = TelegramRunCoordinator::new();
        let first = coordinator.register(ChatId(1), "session-a").await;
        let second = coordinator.register(ChatId(1), "session-a").await;

        first.finish().await;
        let third = coordinator.register(ChatId(1), "session-a").await;

        assert!(second.context().is_cancelled());
        assert!(!third.context().is_cancelled());
    }
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `rtk cargo test interfaces::telegram::run_coordinator::tests -- --nocapture`

Expected: compilation fails because `TelegramRunCoordinator` is not defined.

- [ ] **Step 3: Implement the minimal coordinator**

Implement these concrete types in `src/interfaces/telegram/run_coordinator.rs`:

```rust
use std::{collections::HashMap, sync::Arc};

use teloxide::types::ChatId;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::llm::client::RunContext;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TelegramSessionKey {
    chat_id: ChatId,
    session_id: String,
}

struct SessionRun {
    id: u64,
    context: RunContext,
    execution: Arc<Mutex<()>>,
}

#[derive(Default)]
struct CoordinatorState {
    next_run_id: u64,
    sessions: HashMap<TelegramSessionKey, SessionRun>,
}

#[derive(Clone, Default)]
pub(super) struct TelegramRunCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
}

pub(super) struct TelegramRunPermit {
    coordinator: TelegramRunCoordinator,
    key: TelegramSessionKey,
    id: u64,
    context: RunContext,
    execution: Arc<Mutex<()>>,
}

impl TelegramRunCoordinator {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) async fn register(
        &self,
        chat_id: ChatId,
        session_id: impl Into<String>,
    ) -> TelegramRunPermit {
        let key = TelegramSessionKey { chat_id, session_id: session_id.into() };
        let mut state = self.state.lock().await;
        state.next_run_id = state.next_run_id.wrapping_add(1);
        let id = state.next_run_id;
        let context = RunContext::new();
        let execution = state.sessions.get(&key)
            .map_or_else(|| Arc::new(Mutex::new(())), |run| run.execution.clone());

        if let Some(previous) = state.sessions.insert(
            key.clone(),
            SessionRun { id, context: context.clone(), execution: execution.clone() },
        ) {
            previous.context.cancel();
        }

        TelegramRunPermit {
            coordinator: self.clone(), key, id, context, execution,
        }
    }
}

impl TelegramRunPermit {
    pub(super) fn context(&self) -> &RunContext { &self.context }

    pub(super) async fn enter(&self) -> OwnedMutexGuard<()> {
        self.execution.clone().lock_owned().await
    }

    pub(super) async fn finish(self) {
        let mut state = self.coordinator.state.lock().await;
        if state.sessions.get(&self.key).is_some_and(|run| run.id == self.id) {
            state.sessions.remove(&self.key);
        }
    }
}
```

Keep cleanup explicit through `finish`; do not spawn cleanup work from `Drop`.

- [ ] **Step 4: Run focused tests**

Run: `rtk cargo test interfaces::telegram::run_coordinator::tests -- --nocapture`

Expected: all four coordinator tests pass.

- [ ] **Step 5: Run formatting and commit**

Run: `rtk cargo fmt --check`

Expected: PASS. If it reports formatting changes, run `rtk cargo fmt`, then rerun the check.

Commit:

```bash
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/run_coordinator.rs
rtk git commit -m "feat(telegram): coordinate runs by session"
```

---

### Task 2: Route ordinary Telegram messages through run permits

**Files:**
- Modify: `src/interfaces/telegram.rs:34-227`
- Test: `src/interfaces/telegram.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Consumes: `TelegramRunCoordinator::register`, `TelegramRunPermit::{context, enter, finish}` from Task 1; existing app session execution functions.
- Produces: existing command bypass plus coordinated private/regular message execution and permit ownership through final answer sending.

- [ ] **Step 1: Add a failing permit-lifecycle gateway test**

Add a small test around an extracted completion helper proving that a permit is not finished before answer sending completes. The helper must accept an async send closure so the test needs no Telegram network:

```rust
async fn complete_coordinated_answer<F, Fut>(
    answer: Option<String>,
    permit: TelegramRunPermit,
    send: F,
) -> Result<()>
where
    F: FnOnce(String, RunContext) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    // Implemented after the failing test.
}
```

The test's send closure should register a newer permit before returning and assert that the original send context becomes cancelled. This proves the old registration remains active throughout sending. Existing `commands.rs::tests::parses_session_commands` already covers `/new`, `/sessions`, and `/sessions <id>`; do not duplicate command syntax or add a second parser.

- [ ] **Step 2: Run the lifecycle test and verify it fails**

Run: `rtk cargo test interfaces::telegram::tests::coordinated_permit_remains_registered_until_send_finishes -- --nocapture`

Expected: FAIL because `complete_coordinated_answer` is not implemented.

- [ ] **Step 3: Preserve command bypass and inject the coordinator**

Keep `answer_authorized_message` executing `answer_session_command(...)` before resolving a session or registering a run. Pass `&TelegramRunCoordinator` into the function, but do not pass it into the command handler. Preserve the existing early return:

```rust
if let Some(answer) =
    answer_session_command(session_store, msg.chat.id, text, bot_username).await
{
    return Some(answer);
}
```

Create `TelegramRunCoordinator::new()` once next to `session_store` in `run`, clone it into the `teloxide::repl` closure, and pass it only along the authorized ordinary-message path. Run existing command tests with `rtk cargo test interfaces::telegram::commands::tests` to prove commands still bypass generation.

- [ ] **Step 4: Register after session resolution and execute under the permit**

In both `answer_private_chat` and `answer_regular_chat`, after resolving `session_id`, use this lifecycle:

```rust
let permit = coordinator.register(msg.chat.id, session_id.clone()).await;
let _cancellation_watch = cancel_on_ctrl_c(msg.chat.id, permit.context().clone());
let _execution = permit.enter().await;

let result = app::run_once_with_actor_session_streaming_in_root_and_context(
    text.to_string(),
    run.config,
    run.actor,
    session_store.session_root(msg.chat.id.0),
    session_id,
    &mut stream,
    permit.context(),
)
.await;

stream.finish().await;
let answer = answer_or_gateway_error(msg.chat.id, result, permit.context());
drop(_execution);
(answer, permit)
```

Use the equivalent non-streaming call for regular chats. Construct typing/draft state only after acquiring the execution guard, so queued cancelled messages do not display typing. Move the existing per-update `RunContext` and Ctrl+C watcher out of the top-level `repl` handler; commands and authorization responses do not need a generation context. Return the permit with the optional answer, call `complete_coordinated_answer`, send using `permit.context().clone()`, and call `permit.finish()` only after send success, send failure, or cancellation. Do not create a fresh context for sending. Keep plain command/auth responses on their existing direct-send path without a permit.

- [ ] **Step 5: Add the agent regression test for queued cancelled messages**

In `src/agent.rs`, add a focused test documenting the relied-upon existing behavior:

```rust
#[tokio::test]
async fn already_cancelled_run_records_user_message_without_calling_llm() -> Result<()> {
    let client = FakeClient::new();
    let memory = InMemoryStore::new();
    let agent = Agent::new(client.clone(), memory, NoTools);
    let context = RunContext::new();
    context.cancel();

    let error = agent.execute_with_context("queued", &context).await.unwrap_err();

    assert_eq!(error.to_string(), "run cancelled");
    assert_eq!(agent.memory.load_context().await?, vec![Message::user("queued")]);
    assert!(client.requests().await.is_empty());
    Ok(())
}
```

- [ ] **Step 6: Run focused tests**

Run: `rtk cargo test interfaces::telegram agent::tests::already_cancelled_run_records_user_message_without_calling_llm -- --nocapture`

Expected: Telegram and agent tests pass. In particular, an already-cancelled permit still records its message and performs zero LLM requests.

- [ ] **Step 7: Commit the gateway integration**

```bash
rtk git add src/agent.rs src/interfaces/telegram.rs
rtk git commit -m "feat(telegram): supersede active session generation"
```

---

### Task 3: End-to-end concurrency regression and full verification

**Files:**
- Modify: `src/interfaces/telegram/run_coordinator.rs`
- Modify: `src/interfaces/telegram.rs` tests only if a small extracted executor helper is required for dependency injection.

**Interfaces:**
- Consumes: coordinator and permit lifecycle from Tasks 1–2; `RunContext` cancellation behavior; existing in-memory/fake agent test doubles.
- Produces: regression coverage demonstrating ordered persistence and newest-only generation.

- [ ] **Step 1: Write the failing async scenario test**

Add a test-only runner around the real permit lifecycle. Submit `first` with a fake generation that blocks after persisting, then submit `second`. Assert:

```rust
assert!(first_context.is_cancelled());
assert_eq!(persisted_messages.lock().await.as_slice(), ["first", "second"]);
assert_eq!(generated_requests.lock().await.as_slice(), [vec!["first", "second"]]);
assert_eq!(sent_answers.lock().await.as_slice(), ["answer to second"]);
```

Use `tokio::sync::Notify` to make the timing deterministic. The fake execution closure must follow production order: acquire permit, append its input, return immediately if its context is cancelled, otherwise snapshot history and simulate generation. Do not use sleeps.

- [ ] **Step 2: Run the scenario and verify it fails for any missing integration behavior**

Run: `rtk cargo test interfaces::telegram::run_coordinator::tests::superseding_run_persists_both_messages_and_only_newest_generates -- --nocapture`

Expected before completing the helper/lifecycle wiring: FAIL on ordered persistence, request context, or newest-only answer assertion.

- [ ] **Step 3: Make the smallest lifecycle correction required by the test**

Keep production logic in the coordinator/permit APIs already defined. If test and production duplicate more than the callback body, extract this helper in `run_coordinator.rs`:

```rust
impl TelegramRunPermit {
    pub(super) async fn run<F, Fut, T>(&self, operation: F) -> T
    where
        F: FnOnce(&RunContext) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _execution = self.enter().await;
        operation(self.context()).await
    }
}
```

Then use `permit.run(...)` from both Telegram paths and the test. Keep `finish` explicit after result/draft cleanup.

- [ ] **Step 4: Run the focused concurrency test repeatedly**

Run: `rtk cargo test interfaces::telegram::run_coordinator::tests::superseding_run_persists_both_messages_and_only_newest_generates -- --nocapture`

Expected: PASS with deterministic ordering and no timing sleeps. Run it three times to catch accidental scheduling dependence.

- [ ] **Step 5: Run complete project verification**

Run these commands separately:

```bash
rtk cargo test
rtk cargo check
rtk cargo fmt --check
rtk cargo clippy --all-targets --all-features
```

Expected: every command exits successfully with no test failure, formatting diff, compiler error, or Clippy warning.

- [ ] **Step 6: Review the final diff against scope**

Run:

```bash
rtk git diff HEAD~2 -- src/agent.rs src/interfaces/telegram.rs src/interfaces/telegram/run_coordinator.rs
rtk git status --short
```

Expected: only Telegram coordination/integration, the narrow agent regression test, and any command-classification helper are changed; no API/provider/CLI behavior changes and no unrelated files are present.

- [ ] **Step 7: Commit final regression coverage**

```bash
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/run_coordinator.rs
rtk git commit -m "test(telegram): cover superseding session messages"
```
