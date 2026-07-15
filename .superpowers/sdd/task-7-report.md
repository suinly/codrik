# Task 7 Report: Nonblocking StreamHub and Streaming Runner

## Outcome

Implemented the transient streaming slice only. `StreamHub` now fans protocol events out to every subscription for an attached request without awaiting consumers. `ActorRunner` now requires `LlmStreamClient`, forwards only model text deltas through a runtime sink, publishes model/tool activity, and continues to use the complete returned `LlmResponse` for checkpoints and durable finalization.

`AttachedRun` carries the active local request IDs selected from its attached run events. This required the directly related store and SQLite dispatch construction sites in addition to the files named in the brief.

## RED Evidence

Tests were added before production implementation and the required commands were run separately:

- `rtk cargo test runtime::stream_hub::tests` — failed with unresolved imports for `StreamHub` and `RuntimeEventPublisher` (5 compile errors total), which was the expected missing-feature failure.
- `rtk cargo test runtime::runner::tests::stream_` — failed with the same missing streaming hub/publisher API before the runner conversion, again the expected missing-feature failure.

No production implementation existed when these failures were captured.

## GREEN Evidence

Focused verification, run separately after implementation:

- `rtk cargo test runtime::stream_hub::tests` — 4 passed.
- `rtk cargo test runtime::runner::tests` — 7 passed.
- `rtk cargo test runtime::service::tests` — 3 passed.

Full verification:

- `rtk cargo test` — 295 passed, 1 ignored across two suites after the final test coverage addition.
- `rtk cargo fmt --check` — passed.
- `rtk cargo check` — passed; the crate retains its pre-existing dead-code/unused warnings while later serve composition remains unimplemented.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; pre-existing warnings remain.
- `rtk git diff --check` — passed.

## Design Decisions

- A request ID maps to a vector of weak subscription states. Subscribing again never replaces an existing observer; multiple clients can independently observe the same request.
- Each subscription uses a mutex-protected `VecDeque` and `Notify`. Publication is synchronous best-effort and contains no channel send, await, or subscriber-derived error path.
- Normal events may consume at most `event_limit - 1` slots. The last slot is reserved exclusively for one zero-byte `StreamGap`, making the gap deliverable even when normal event capacity is exhausted.
- Byte accounting counts text UTF-8 bytes and variable activity payload bytes (description/tool name). Fixed activity and gap markers count as zero payload bytes but remain bounded by event capacity. Global bytes are reserved atomically and released on receive or subscription destruction.
- Any event/byte/global overflow emits the one gap and permanently suppresses subsequent text for that subscription. Activity is still eligible for delivery when normal capacity becomes available.
- Dropping a subscription deregisters only that observer and releases its queued-byte reservation. It never touches the actor `RunContext`.
- `RuntimeLlmSink` publishes `TextDelta` only. `ToolCallDelta` and other provider stream details are ignored at the runtime boundary. Hub methods return `()`, so a slow or disconnected subscriber cannot fail the runner.
- The runner injects an `Arc<dyn RuntimeEventPublisher>` and publishes model-started, description, tool-started/tool-finished, completed, and cancelled activity around the existing durable steps.
- The streaming model's complete return value remains authoritative. The runner test deliberately emits a partial delta and a provider-only tool-call delta while asserting that durable final text comes from the complete response.

## Coverage and Self-Review

Tests cover multiple subscriptions without replacement, per-request fanout to two attached request IDs, event capacity, byte capacity, global capacity, the reserved single gap, post-gap text suppression, resumed activity delivery, disconnect cleanup/isolation, ignored provider tool-call deltas, slow-subscriber overflow without model failure, and authoritative durable finalization.

Self-review removed an unnecessary fallback change to the public LLM traits, leaving the existing `LlmStreamClient` contract intact and making the runner depend directly on it. No dispatcher, outbox worker, socket server, or later-task composition was added.

## Concerns

- Queue critical sections use short standard mutex locks. They cannot wait on subscriber consumption and hold no lock across I/O or await; “nonblocking” here is best-effort publication independent of subscriber backpressure, as specified.
- `Failed` terminal activity remains owned by the later dispatcher failure path; this task publishes the terminal states the current runner durably determines (`Completed` and `Cancelled`).
