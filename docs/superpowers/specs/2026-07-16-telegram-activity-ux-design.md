# Telegram Activity UX Design

## Goal

Replace Telegram draft-message streaming with a quieter private-chat experience:

- send the `typing` chat action every four seconds while the model is generating;
- show the established elapsed-time status message only when tools are used;
- deliver the assistant answer only through the durable final-delivery path;
- do not attach private-chat messages to the inbound message with Telegram reply-to;
- recover complete text from OpenAI-compatible providers that finalize text in
  `response.output_text.done`.

## Root Cause of the Truncated `Пр` Reply

The active provider emits an initial `response.output_text.delta`, then puts the
complete answer in `response.output_text.done`. Its `response.completed` output
contains no final text.

Codrik currently forwards the delta to Telegram draft streaming but ignores
`response.output_text.done`. The durable outbox consequently receives an empty
final response, creates no Telegram final chunk, and leaves the initial `Пр`
draft visible.

The provider adapter must accumulate deltas and consume
`response.output_text.done`. The done text is authoritative for its output
content. If the completed response contains no assistant text, the accumulated
or done text becomes the final `LlmResponse` content.

## Private-Chat Delivery

Telegram private-chat routes set `reply_to_external_id` to `None`. Link
responses, activity statuses, final text chunks, and files are therefore sent
as ordinary chat messages without `reply_parameters`.

This changes only Telegram presentation. Durable update idempotency, actor
identity linking, memory scope, delivery ordering, retries, and replay recovery
remain unchanged.

## Typing Lifecycle

The Telegram activity worker maintains transient per-work-item state.

- `ModelStepStarted` starts typing immediately.
- While the model step remains active, `sendChatAction(chat_id, "typing")` is
  called every four seconds.
- `Description`, `ToolStarted`, terminal activity, or shutdown stops the
  current model-step typing loop.
- A later `ModelStepStarted`, including the model step after a tool result,
  starts typing again.
- API failures are ignored because typing is best effort and must never affect
  the actor runner.

No Telegram message is created for model text deltas.

## Tool Status Lifecycle

Tool activity follows the behavior previously implemented in commits
`cbbe0f1` and `990efbf`.

- A status message is created only for a run that reaches `ToolStarted`.
- The default description is `Работаю над задачей`.
- If the model emits a nonblank `Description` with its tool call, that
  normalized text replaces the default description.
- Status text is rendered as `<description> — <elapsed>`.
- Elapsed time is refreshed every ten seconds.
- Description changes are coalesced and applied no more often than every two
  seconds.
- Descriptions are whitespace-normalized and limited to 240 Unicode scalar
  values.
- Terminal activity edits the status to one of:
  - `Завершил работу — <elapsed>`;
  - `Работа остановлена — <elapsed>`;
  - `Не удалось завершить работу — <elapsed>`.

The status message is transient UX. Failure to send or edit it does not affect
tool execution or durable final delivery.

## Gateway Activity Changes

`GatewayActivityEvent::TextDelta` remains available to the gateway-neutral
publisher but Telegram ignores it. Telegram consumes:

- `ModelStepStarted` for typing;
- `Description` for an optional tool-status description;
- `ToolStarted` to activate the tool status;
- `ToolFinished` as informational state without creating a new message;
- `Completed`, `Cancelled`, and `Failed` to stop typing and terminalize an
  existing tool status.

The existing `gateway_streams` table is no longer used for Telegram draft text.
No destructive migration is required; old rows remain inert.

## Bot API

Add typed `sendChatAction` support to `TelegramApi` and
`ReqwestTelegramApi`. The request contains:

```json
{"chat_id":"…","action":"typing"}
```

It is retry-safe, but activity-worker failures are swallowed rather than
persisted.

## Testing

Tests must cover:

- `response.output_text.done` supplies the final answer when
  `response.completed` is empty;
- accumulated deltas are used as a fallback when the done event is absent;
- private Telegram routes contain no reply-to external ID;
- simple model-only runs send typing actions and no status messages;
- typing repeats every four seconds and stops at model-step/tool/terminal
  boundaries;
- tool runs create the default status only at `ToolStarted`;
- a model description replaces the default status;
- elapsed status updates every ten seconds;
- terminal status text matches success, cancellation, and failure;
- Telegram activity API failures do not propagate;
- durable final text is delivered once and is never replaced by a partial
  draft.

## Out of Scope

- Group or channel reply behavior.
- Telegram message threads.
- Persisting typing or tool-status activity.
- Deleting historical draft/status messages.
- Changing actor memory, identity linking, or durable delivery guarantees.
