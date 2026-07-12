# Unbounded Agent Loop and Telegram Activity Design

## Goal

Allow the agent to continue tool-call rounds until it produces a final answer, is cancelled, or fails, while making long-running work observable and explicitly cancellable from Telegram.

## Agent loop

Remove `MAX_TOOL_CALL_ITERATIONS` from both the regular and streaming execution paths. The loop has no iteration counter and continues while the model returns tool calls. It terminates when the model returns a response without tool calls, the `RunContext` is cancelled, or an LLM, memory, tool-orchestration, or stream error ends the run.

This change deliberately does not add a replacement turn, token, or time budget. Existing cancellation checks around model requests remain the safety mechanism for user-controlled termination.

## Activity events

Introduce an agent-level activity event boundary independent of Telegram and independent of the final-answer stream. It reports enough lifecycle information for an interface to describe current work without exposing raw tool arguments or tool output.

The activity lifecycle includes:

- a model step beginning;
- intermediate model text produced during a tool-call round;
- tool execution beginning;
- tool execution completing;
- run completion, cancellation, or failure.

Activity delivery must not change conversation memory or the final answer. Failure to publish a presentation-only status must not fail or cancel agent execution.

## Model-authored descriptions

Agent instructions should encourage the model to briefly describe what it is about to do before requesting tools. The model is not restricted to one sentence or a rigid status schema; it may provide a short, natural description with enough context to be useful.

Text emitted during a tool-call round is treated as an activity-description candidate rather than final-answer text. If the model emits no usable description, the interface falls back to a neutral phrase such as `Работаю над задачей`.

The Telegram renderer converts a candidate into a safe single-line status by collapsing whitespace, removing empty content, and truncating it to a conservative display length. It never includes raw tool-call JSON, raw command arguments, or tool output as a fallback.

## Telegram status message

Each Telegram run creates one ordinary status message that is edited in place. The same mechanism applies to private and non-private chats. Its visible format is a single line:

```text
Проверяю результат — 1 мин 42 сек
```

The status renderer appends elapsed time and rate-limits edits to avoid Telegram API flooding. A periodic refresh updates elapsed time even while one tool is running for a long time, so a live gateway remains visibly active.

Terminal states replace the same message:

- success: `Завершил работу — <elapsed>`;
- cancellation: `Работа остановлена — <elapsed>`;
- failure: `Не удалось завершить работу — <elapsed>`.

The final answer remains a separate Telegram message. Status-send or status-edit failures are logged locally and disable further status edits for that run, but do not affect agent execution or final-answer delivery.

## Cancellation behavior

Add a `/stop` command for authorized Telegram users. It resolves the chat's active session and asks `TelegramRunCoordinator` to cancel its currently registered `RunContext` without registering a replacement run.

- If a run is active, `/stop` cancels it and confirms the request.
- If no run is active, it responds `Сейчас нет активной задачи`.
- A normal new message preserves current superseding behavior: it cancels the previous run in the same chat and session, waits for serialized execution ownership, persists the new user message, and starts the replacement run.

Cancellation remains cooperative through `RunContext`. Model streaming and the loop observe it immediately; a currently executing tool can only stop promptly where its implementation supports cancellation or returns control to the loop.

## Component boundaries

- `agent` owns the unbounded orchestration loop and emits semantic activity lifecycle events.
- `llm` continues to expose response deltas and cancellation-aware model calls.
- `tools` execute tools and do not depend on Telegram formatting.
- `interfaces::telegram` owns wording normalization, elapsed-time formatting, message creation/editing, throttling, and `/stop` responses.
- `TelegramRunCoordinator` remains the authority for active-run lookup, superseding, and explicit cancellation.
- `app` wires the activity observer into both private and regular Telegram execution paths without introducing Telegram dependencies into the agent.

## Error and race handling

The coordinator cancels only a run whose chat/session key is currently registered. Completion from a stale superseded run must not remove or alter a newer registration. `/stop` racing with normal completion may report that cancellation was requested; terminal status derives from the run's actual context/result.

Status updates are cancellation-aware but terminal status gets one best-effort edit after execution resolves. Telegram API errors remain presentation failures and do not become gateway errors returned to the user.

## Testing

Implement test-first coverage for:

- more than five tool-call rounds completing successfully in regular execution;
- more than five tool-call rounds completing successfully in streaming execution;
- an unbounded tool loop terminating through `RunContext` cancellation;
- activity descriptions from tool-call-round text and neutral fallback behavior;
- activity lifecycle ordering around tool execution;
- single-line normalization, truncation, elapsed-time formatting, and edit throttling;
- `/stop` cancelling an active run and reporting an idle session;
- a new message continuing to supersede an active run;
- stale completion not affecting a newer run;
- success, cancellation, and failure terminal status wording;
- status API failure not failing agent execution or final-answer delivery.

## Out of scope

- Persisting activity logs in conversation memory;
- exposing chain-of-thought or raw reasoning;
- displaying raw tool arguments or outputs in Telegram;
- adding token, cost, time, or iteration budgets;
- adding cancellation support inside every individual tool implementation;
- changing CLI progress presentation.
