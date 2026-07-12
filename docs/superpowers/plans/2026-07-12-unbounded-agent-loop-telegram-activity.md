# Unbounded Agent Loop and Telegram Activity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the agent's fixed tool-round limit and expose human-readable, elapsed-time Telegram activity that users can stop with `/stop` or supersede with a new message.

**Architecture:** The agent owns an unbounded tool loop and publishes semantic best-effort activity events through a new interface-level observer. Telegram adapts those events into one rate-limited message edited by a background task, while `TelegramRunCoordinator` remains the sole authority for cancellation and run replacement.

**Tech Stack:** Rust 2024, Tokio, async-trait, Teloxide, Reqwest, anyhow, existing in-memory scripted test doubles.

## Global Constraints

- Do not introduce a replacement iteration, token, cost, or time budget.
- Do not expose chain-of-thought, raw tool arguments, raw command JSON, or tool output in Telegram.
- Model activity descriptions may contain several natural phrases; Telegram must collapse them into one safe line.
- Status publication is best-effort and must never fail agent execution or final-answer delivery.
- `/stop` only cancels the active run; a normal new message cancels and replaces it.
- Keep final answers separate from the status message.
- Run every shell command through `rtk`.

---

### Task 1: Remove the fixed agent-loop limit

**Files:**
- Modify: `src/agent.rs`

**Interfaces:**
- Consumes: existing `RunContext`, `LlmClient`, `LlmStreamClient`, and `record_response` behavior.
- Produces: regular and streaming execution loops that terminate only on a final response, cancellation, or error.

- [ ] **Step 1: Write failing regression tests**

Add tests in `src/agent.rs` that script six tool-call responses followed by a final response for both execution modes. Build each sequence with distinct call IDs and assert the returned answer is `done` and the client received seven requests. Add a cancellation test whose fake client always returns a tool call, cancels the shared `RunContext` after the sixth request, and asserts `run cancelled`.

```rust
#[tokio::test]
async fn execute_allows_more_than_five_tool_call_rounds() -> Result<()> {
    let client = ScriptedClient::new(tool_rounds_then_answer(6, "done"));
    let agent = Agent::new(client.clone(), InMemoryStore::new(), OneTool {
        behavior: ToolBehavior::Succeed("ok"),
    });

    assert_eq!(agent.execute("hello").await?, "done");
    assert_eq!(client.requests().await.len(), 7);
    Ok(())
}

#[tokio::test]
async fn streaming_allows_more_than_five_tool_call_rounds() -> Result<()> {
    let client = ScriptedClient::new(tool_rounds_then_answer(6, "done"));
    let agent = Agent::new(client.clone(), InMemoryStore::new(), OneTool {
        behavior: ToolBehavior::Succeed("ok"),
    });
    let mut sink = RecordingSink::default();

    assert_eq!(agent.execute_streaming("hello", &mut sink).await?, "done");
    assert_eq!(client.requests().await.len(), 7);
    Ok(())
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
rtk cargo test agent::tests::execute_allows_more_than_five_tool_call_rounds -- --nocapture
rtk cargo test agent::tests::streaming_allows_more_than_five_tool_call_rounds -- --nocapture
```

Expected: both tests fail with `tool call loop exceeded max iterations (5)`.

- [ ] **Step 3: Implement unbounded loops**

Delete `MAX_TOOL_CALL_ITERATIONS`. Replace both `for _ in 0..MAX_TOOL_CALL_ITERATIONS` loops with `loop`. Delete the unreachable `bail!("tool call loop exceeded...")` statements. Preserve cancellation checks at the top of each iteration and the `tokio::select!` around model requests.

- [ ] **Step 4: Verify GREEN**

Run: `rtk cargo test agent::tests -- --nocapture`

Expected: all agent tests pass, including the new six-round and cancellation cases.

- [ ] **Step 5: Commit**

```bash
rtk git add src/agent.rs
rtk git commit -m "refactor(agent): allow unbounded tool rounds"
```

---

### Task 2: Publish semantic agent activity

**Files:**
- Modify: `agent_instructions.md`
- Modify: `src/agent.rs`
- Modify: `src/app.rs`
- Modify: `src/llm/client.rs`

**Interfaces:**
- Consumes: `LlmResponse.content`, tool names, tool execution results, and `RunContext`.
- Produces: `AgentActivityEvent`, `AgentActivitySink`, and Telegram-facing app entry points that accept an activity sink.

- [ ] **Step 1: Define the desired event contract in failing tests**

In `src/agent.rs`, add a recording activity sink and a test with one tool round followed by `done`. Require this exact event order:

```rust
vec![
    AgentActivityEvent::ModelStepStarted,
    AgentActivityEvent::Description("Смотрю структуру проекта\nи нужные файлы".into()),
    AgentActivityEvent::ToolStarted { name: "demo".into() },
    AgentActivityEvent::ToolFinished { name: "demo".into(), succeeded: true },
    AgentActivityEvent::ModelStepStarted,
    AgentActivityEvent::Completed,
]
```

Add a second test where tool execution fails and assert `ToolFinished { succeeded: false }` followed by another model step and `Completed`. Add a cancellation test asserting the last event is `Cancelled`.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test agent::tests::activity -- --nocapture`

Expected: compilation fails because `AgentActivityEvent`, `AgentActivitySink`, and the activity-aware execution method do not exist.

- [ ] **Step 3: Add activity types**

In `src/llm/client.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentActivityEvent {
    ModelStepStarted,
    Description(String),
    ToolStarted { name: String },
    ToolFinished { name: String, succeeded: bool },
    Completed,
    Cancelled,
    Failed,
}

#[async_trait]
pub trait AgentActivitySink: Send {
    async fn on_activity(&mut self, event: AgentActivityEvent);
}

pub struct NoopAgentActivitySink;

#[async_trait]
impl AgentActivitySink for NoopAgentActivitySink {
    async fn on_activity(&mut self, _event: AgentActivityEvent) {}
}
```

The callback intentionally returns `()` so presentation errors cannot propagate into agent execution.

- [ ] **Step 4: Instrument agent execution**

Add `execute_streaming_with_context_and_activity` as the activity-aware primitive. Existing `execute_streaming_with_context` creates `NoopAgentActivitySink` and delegates to it.

At the start of each loop iteration emit `ModelStepStarted`. When a response contains tool calls and non-empty `content`, emit `Description(content.clone())`. Before and after every tool execution emit `ToolStarted` and `ToolFinished`; keep success/failure observation serialization unchanged. Wrap the internal result and emit exactly one terminal event: `Completed` on success, `Cancelled` when the context or error indicates cancellation, otherwise `Failed`.

Refactor `record_response` to accept `&mut dyn AgentActivitySink`; the regular non-streaming path uses `NoopAgentActivitySink` so its public API does not grow Telegram concerns.

- [ ] **Step 5: Wire app entry points**

In `src/app.rs`, add activity-aware variants for actor session execution:

```rust
pub async fn run_once_with_actor_session_streaming_and_activity_in_root_and_context(
    query: String,
    config: AppConfig,
    actor: AuthorizedActor,
    session_root: PathBuf,
    session_id: impl AsRef<str>,
    sink: &mut dyn LlmStreamSink,
    activity: &mut dyn AgentActivitySink,
    context: &RunContext,
) -> Result<String>
```

Keep the existing function as a compatibility wrapper using `NoopAgentActivitySink`.

- [ ] **Step 6: Encourage model-authored progress**

Append a concise instruction to `agent_instructions.md`:

```text
Before requesting tools, briefly tell the user what you are about to investigate or change. The description may be a few natural phrases; do not expose hidden reasoning, secrets, or raw tool arguments.
```

- [ ] **Step 7: Verify GREEN**

Run:

```bash
rtk cargo test agent::tests -- --nocapture
rtk cargo test app::tests -- --nocapture
```

Expected: all selected tests pass and existing final streaming still contains only the final answer.

- [ ] **Step 8: Commit**

```bash
rtk git add agent_instructions.md src/agent.rs src/app.rs src/llm/client.rs
rtk git commit -m "feat(agent): publish run activity events"
```

---

### Task 3: Render one human Telegram status message

**Files:**
- Create: `src/interfaces/telegram/activity_status.rs`
- Modify: `src/interfaces/telegram.rs`

**Interfaces:**
- Consumes: `AgentActivityEvent`, `Bot`, `ChatId`, and Tokio time.
- Produces: `TelegramActivityStatus::start`, an `AgentActivitySink` handle, and `finish(TerminalStatus)`.

- [ ] **Step 1: Write formatter and lifecycle tests**

Create `activity_status.rs` with tests requiring:

```rust
assert_eq!(
    normalize_description("  Проверяю проект\nи запускаю тесты  ", 80),
    "Проверяю проект и запускаю тесты"
);
assert_eq!(format_elapsed(Duration::from_secs(102)), "1 мин 42 сек");
assert_eq!(
    render_status("Проверяю результат", Duration::from_secs(102)),
    "Проверяю результат — 1 мин 42 сек"
);
```

Use a fake API that records `send` and `edit` calls. Assert one message is sent, rapid descriptions are coalesced by the configured update interval, elapsed time causes a later edit, and `finish(Success)` performs a final best-effort edit. Add a fake that returns errors and assert activity callbacks and `finish` still complete normally.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test interfaces::telegram::activity_status::tests -- --nocapture`

Expected: compilation fails because the module and status types do not exist.

- [ ] **Step 3: Implement isolated status worker**

Define:

```rust
const MAX_STATUS_DESCRIPTION_CHARS: usize = 240;
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
const STATUS_TICK_INTERVAL: Duration = Duration::from_secs(10);

pub(super) enum TerminalStatus { Success, Cancelled, Failed }

pub(super) struct TelegramActivityStatus {
    sender: tokio::sync::mpsc::UnboundedSender<StatusCommand>,
    task: JoinHandle<()>,
}
```

The worker sends `Работаю над задачей — 0 сек`, owns the returned `MessageId`, keeps only the newest normalized description, edits no more often than every two seconds, and ticks every ten seconds. Map `Description` to normalized model text, `ModelStepStarted` to the current description, and missing descriptions/tool events to `Работаю над задачей`; do not render tool names or arguments.

Map the agent's `Completed`, `Cancelled`, and `Failed` events to terminal commands. `finish(fallback_terminal)` sends the supplied terminal only when the worker has not already received one, then awaits the worker. This makes the outer Telegram orchestration robust if execution ends before the agent can publish its terminal event. Use the exact terminal wording from the spec. API errors log and disable later nonterminal edits; one terminal edit remains best-effort.

- [ ] **Step 4: Implement Teloxide adapter**

Use an internal async trait for `send_message` and `edit_message_text`, then implement it for a small `BotStatusApi { bot: Bot, chat_id: ChatId }`. This keeps Telegram network behavior behind a deterministic test double.

- [ ] **Step 5: Verify GREEN**

Run: `rtk cargo test interfaces::telegram::activity_status::tests -- --nocapture`

Expected: all formatter, throttling, timer, terminal, and API-failure tests pass under paused Tokio time where applicable.

- [ ] **Step 6: Commit**

```bash
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/activity_status.rs
rtk git commit -m "feat(telegram): show human run activity"
```

---

### Task 4: Add explicit `/stop` cancellation

**Files:**
- Modify: `src/interfaces/telegram/commands.rs`
- Modify: `src/interfaces/telegram/run_coordinator.rs`
- Modify: `src/interfaces/telegram.rs`

**Interfaces:**
- Consumes: chat ID, active session ID, bot username, and coordinator registrations.
- Produces: `is_stop_command` and `TelegramRunCoordinator::cancel(chat_id, session_id) -> bool`.

- [ ] **Step 1: Write failing command and coordinator tests**

Add parser cases for `/stop`, `/stop@this_bot`, and rejection of `/stop@other_bot`. Add coordinator tests:

```rust
#[tokio::test]
async fn cancel_stops_the_current_run_without_registering_a_replacement() {
    let coordinator = TelegramRunCoordinator::new();
    let permit = coordinator.register(ChatId(1), "session-a").await;

    assert!(coordinator.cancel(ChatId(1), "session-a").await);
    assert!(permit.context().is_cancelled());
    assert!(!coordinator.cancel(ChatId(1), "missing").await);
}
```

Add a race test proving a stale permit finishing after `/stop` cannot remove a subsequently registered run.

- [ ] **Step 2: Verify RED**

Run:

```bash
rtk cargo test interfaces::telegram::commands::tests -- --nocapture
rtk cargo test interfaces::telegram::run_coordinator::tests -- --nocapture
```

Expected: compilation fails because the parser and coordinator cancellation method do not exist.

- [ ] **Step 3: Implement command parsing and coordinator cancellation**

Implement `is_stop_command` using the existing command-addressing helpers. Implement coordinator cancellation by looking up the exact `TelegramSessionKey`, cloning its context while holding the state lock, releasing the lock, and calling `cancel()`. Do not insert or remove a run in this method.

- [ ] **Step 4: Route `/stop` before ordinary generation**

In `answer_authorized_message`, handle `/stop` before session commands and before private/group generation. Resolve the active session ID, call the coordinator, and return exactly one of:

```text
Останавливаю текущую задачу
Сейчас нет активной задачи
```

The command must never call an app generation entry point or persist a user conversation message.

- [ ] **Step 5: Verify GREEN**

Run: `rtk cargo test interfaces::telegram -- --nocapture`

Expected: command, cancellation, superseding, and stale-run tests all pass.

- [ ] **Step 6: Commit**

```bash
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/commands.rs src/interfaces/telegram/run_coordinator.rs
rtk git commit -m "feat(telegram): stop active runs on command"
```

---

### Task 5: Wire activity status into all Telegram runs

**Files:**
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/interfaces/telegram/activity_status.rs`
- Modify: `src/app.rs`

**Interfaces:**
- Consumes: the Task 2 activity-aware app function and Task 3 status sink.
- Produces: identical observable status behavior in private and regular chats, with final-answer delivery unchanged.

- [ ] **Step 1: Write failing orchestration tests**

Extract a pure terminal-state selector and test:

```rust
assert_eq!(terminal_status(&Ok("done".into()), false), TerminalStatus::Success);
assert_eq!(terminal_status(&Err(anyhow!(RUN_CANCELLED)), true), TerminalStatus::Cancelled);
assert_eq!(terminal_status(&Err(anyhow!("boom")), false), TerminalStatus::Failed);
```

Extend existing coordinated-run tests with a recording activity sink to prove superseded runs finish as cancelled and only the newest final answer is sent. Add a status API failure case proving the returned app result is still delivered.

- [ ] **Step 2: Verify RED**

Run: `rtk cargo test interfaces::telegram::tests -- --nocapture`

Expected: new tests fail because Telegram execution paths do not create or finish activity status.

- [ ] **Step 3: Use streaming plus activity in both chat paths**

For private chats, retain `TelegramDraftStream` for final text and pass `TelegramActivityStatus` as the activity sink. For regular chats, introduce a `DiscardingStreamSink` for final deltas, call the same streaming-and-activity app entry point, then send the returned final answer normally.

Create status immediately after entering execution ownership. After the app future resolves, derive `TerminalStatus` from the result and `RunContext`, finish the status, then preserve the current `answer_or_gateway_error` and permit-completion behavior. Ensure a superseded run cannot send a final answer.

- [ ] **Step 4: Verify focused behavior**

Run:

```bash
rtk cargo test interfaces::telegram -- --nocapture
rtk cargo test agent::tests -- --nocapture
```

Expected: all tests pass; both chat paths use activity status, `/stop` cancels, and new messages still supersede.

- [ ] **Step 5: Run full verification**

Run:

```bash
rtk cargo fmt --check
rtk cargo check
rtk cargo test
rtk cargo clippy --all-targets --all-features
```

Expected: all commands exit successfully with no warnings introduced by this change.

- [ ] **Step 6: Commit**

```bash
rtk git add src/app.rs src/interfaces/telegram.rs src/interfaces/telegram/activity_status.rs
rtk git commit -m "feat(telegram): wire observable cancellable runs"
```
