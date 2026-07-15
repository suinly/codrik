# Task 9 Report: Result-Bundle Outbox Worker

Implemented subscription-aware durable bundle delivery without coupling SQLite
to sockets. `OutboxWorker` claims complete bundles only for subscribed request
IDs, snapshots recipients, transmits complete protocol frame sequences with four
bundle transmissions in flight, renews 30-second claims every 10 seconds, and
coordinates durable ACK, bounded retry, terminal failure, and read-only replay.

## RED evidence

- `rtk cargo test runtime::outbox_worker::tests`
- Failed with `unresolved import super::OutboxWorker`, proving the new worker API
  test failed because the production type did not exist.

## GREEN implementation

- Added `BundleDeliverySink`, `DeliveryRegistry`, and generic `OutboxWorker`.
- Registry changes wake the worker immediately, with a 500 ms fallback poll.
- No subscribed request IDs means no claim call and therefore no attempt count
  increment.
- Claims are capped at 32 complete bundles and transmissions are capped at four
  concurrent bundles. Claimed bundles renew while waiting for a transmission
  permit. SQLite returns lightweight claimed references and materializes a full
  bundle only after one of four permits is held, bounding decoded active bundle
  memory. All recipients in one immutable snapshot start their own complete
  `FinalBegin` through `FinalEnd` send.
- Claims renew every 10 seconds during slow writes and the 30-second ACK wait.
  A renewal fenced by a delivered ACK does not cancel other snapshot sends.
- Every-sink failure and ACK timeout use persisted attempt counts for delays of
  1, 2, 4, 8, then 30 seconds. Encoding failures become `failed_terminal`.
- ACK delegates to the exact durable `BundleAck` transaction. Delivered resume
  uses `replay_bundle` and `replay: true` framing without claims or state writes.
- `StreamHub` now supplies delivery snapshots and subscription-change watches.
  Durable membership requires an explicit connection sink whose `send` boundary
  completes the socket write; transient subscriptions retain their existing
  bounded queue/gap behavior. `StreamSubscription::delivery_sink` gives Task 10
  an exact per-connection replay target.
- `ClaimedBundle` exposes the persisted attempt count and `BundleStore` exposes
  a fenced terminal-failure transition; SQLite implements both.

## Verification

- `rtk cargo test runtime::outbox_worker::tests` — 12 passed.
- `rtk cargo test runtime::stream_hub::tests` — 7 passed.
- `rtk cargo test runtime::sqlite::bundles::tests` — 11 passed.
- `rtk cargo test` — 343 passed, 1 ignored.
- `rtk cargo check` — passed (existing dead-code/unused warnings remain).
- `rtk cargo fmt --check` — passed.
- `rtk cargo clippy --all-targets --all-features` — 0 errors; existing warning
  baseline remains.
- `rtk git diff --check` — passed.

## Self-review and concerns

- Independent correction review approved the final state with no remaining
  critical or important findings after queued-lease, socket-boundary, memory,
  fencing, and persistence-error fixes.
- Checked the claim/ACK race: the first ACK clears the claim, renewal detects
  durable `delivered`, and all already-started snapshot sends continue.
- Checked subscribe/drop/send ordering: the hub serializes disconnect with queue
  mutation and notifies the worker on both subscription creation and teardown;
  ACK wait also detects when every original snapshot sink disappears.
- Checked late subscription: it receives no suffix of an in-flight bundle and
  receives a separate full read-only replay.
- Persistence does not reference delivery sinks, sockets, or server types.
- Persistence errors propagate to the worker loop; terminal/retry transitions
  are not silently discarded. Fencing aborts active connection sinks.
- Task 10 server/security composition remains intentionally absent.
- Existing crate-wide warnings are unchanged in policy; Task 9 adds no clippy
  errors. Connection write failure relies on client resume/replay, matching
  at-least-once delivery.
