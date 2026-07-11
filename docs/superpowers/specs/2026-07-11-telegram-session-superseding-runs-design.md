# Telegram Session Superseding Runs Design

## Goal

When an ordinary Telegram message arrives while the LLM is processing an earlier message in the same Telegram session, the new message must cancel the earlier run, be appended to that session's history, and start a new generation whose context contains both user messages.

## Scope

This behavior belongs to the Telegram gateway and is scoped by Telegram chat and session. CLI execution and the public `Agent` API keep their current behavior. Telegram session commands remain outside the run coordinator: they are neither persisted as user messages nor used to cancel an active generation.

## Architecture

Add a shared Telegram run coordinator created once in `telegram::run` and cloned into each update handler. It owns session entries keyed by both `ChatId` and the resolved session ID. Each entry tracks:

- the cancellation context for the newest registered run;
- a monotonically increasing run identity, used to prevent stale cleanup;
- a session-scoped execution lock, used to serialize access to the file-backed conversation history.

Registration is atomic. Registering a new ordinary message replaces the current run identity and cancellation context, then cancels the displaced context. Runs in different chats or sessions remain independent.

The execution lock is retained across generations for an entry instead of being recreated during replacement. A new run cancels its predecessor immediately, then waits for that predecessor to unwind and release the lock before it appends its message and calls the LLM. This avoids concurrent read-modify-write operations in `FileMemoryStore` without delaying cancellation. The normal cancellation path is expected to unwind promptly because LLM generation already selects on `RunContext::cancelled()`.

## Message Flow

For every authorized ordinary Telegram message:

1. Resolve the active Telegram session as today.
2. Register a new run for `(chat_id, session_id)`, atomically cancelling the previous run for that key.
3. Enqueue the message in the entry's FIFO and ensure its session runner is active.
4. The runner acquires the entry's execution lock and drains messages in arrival order. For an item that has already been superseded, it invokes the existing agent path with an already-cancelled context: `Agent` appends the user message, then exits at its cancellation check before building an LLM request.
5. For the newest queued item, execute the agent with its live registered `RunContext`. The agent appends the incoming message, loads the complete history, and starts generation.
6. On cancellation, suppress gateway errors and final Telegram messages, as today.
7. On completion, remove the active registration only if its run identity still matches. Cleanup from an older run cannot remove a newer registration.

Because the first run appends its user message before it starts its LLM request, its cancellation leaves that message in history. After acquiring the lock, the replacement run appends the new message and builds a request from the complete ordered history. No partial assistant response is persisted: non-streaming responses are recorded only after generation completes, and streaming events are buffered until the turn succeeds.

The per-session FIFO makes every received ordinary message durable in arrival order, even if several messages arrive before the runner gets the execution lock. Each queued item is passed through the existing agent entry point exactly once. Superseded queued items contribute their user message to history but do not call the LLM; only the newest item proceeds past the cancellation check into generation.

## Coordinator Responsibilities

The coordinator owns Telegram concurrency only. It does not format prompts, call providers, or implement memory persistence. Its session runner drains pending inputs in FIFO order into the existing agent/memory path, cancels the active context when a newer input arrives, and starts generation only for the newest input after all earlier inputs have been persisted.

To keep responsibilities explicit, the coordinator exposes a small gateway-facing operation for submitting an ordinary message. Session resolution, authorization, command parsing, typing indicators, drafts, and sending final answers remain in the Telegram interface.

## Commands and Session Changes

Commands use the existing command path before ordinary-message submission. They do not cancel active work and are not written to conversation history. Since the coordinator key includes the resolved session ID, work in distinct sessions does not cancel each other, even within the same Telegram chat.

## Cancellation and Errors

- Superseding cancellation uses the existing `RunContext` and `RUN_CANCELLED` classification.
- Ctrl+C cancellation continues to cancel the affected active run.
- A cancelled run emits no final answer and no gateway error.
- Draft/typing cleanup always runs when a generation exits.
- Persistence or generation failures for the newest run follow the existing Telegram error behavior.
- Failure to persist one queued message stops that session's current drain and reports an error; it must not silently generate without the missing history entry.

## Testing

Focused unit and async tests will verify:

- a new ordinary message cancels an in-flight run for the same chat and session;
- all received messages are persisted once and in arrival order, including messages superseded before generation;
- the newest LLM request contains the earlier and newest user messages;
- only the newest run generates and sends a final answer;
- stale completion cannot clear or replace the newest registration;
- different chats and different sessions do not cancel or serialize one another;
- Telegram commands bypass the coordinator and do not cancel active work;
- cancelled streaming work does not publish buffered output as a final answer;
- existing Ctrl+C and gateway-error behavior remains intact.

## Non-goals

- Changing cancellation semantics for CLI callers.
- Combining or debouncing multiple incoming messages into one history item.
- Persisting partial assistant output from cancelled generations.
- Cancelling work across different Telegram sessions.
- Refactoring provider adapters or changing the public `Agent` interface.
