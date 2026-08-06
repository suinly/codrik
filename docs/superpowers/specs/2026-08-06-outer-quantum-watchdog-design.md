# Post-Attachment Execution Watchdog Design

## Goal

Prevent model or tool execution from keeping an attached run active beyond
`RunnerLimits.max_wall_time`. Expiration must retain lease authority, stop
ephemeral activity, enter existing durable failure accounting, apply retry
backoff, and terminalize after the existing failure limit.

## Problem

The current deadline and heartbeat are both polled inside the LLM stream loop.
A blocked tool is outside that loop. Production also showed an active model
stream continuing to renew its lease and publish typing beyond the intended
deadline without recording a failure.

Wrapping the complete leased quantum in a timeout is unsafe. The lease expires
after 30 seconds while the proposed watchdog expires after 60 seconds, so the
timeout may no longer own authority to record failure. It can also drop an
in-flight SQLite attachment or checkpoint operation before its durable result is
known. Before attachment there is no `FailureFence`, so expiration cannot be
classified as a work failure.

## Design

Keep lease acquisition, run attachment, context loading, and the initial
incorporation checkpoint outside the watchdog. These operations retain their
existing SQLite retry and reconciliation semantics. Start the execution
deadline only after attachment and incorporation have committed and a durable
`FailureFence` exists.

During post-attachment execution, one supervising loop owns:

- the current renewed `ActorLease`;
- the matching `AttachedRun` and `FailureFence` snapshots;
- the heartbeat interval;
- the execution deadline;
- durable control checks.

The supervisor polls heartbeat independently of model and tool futures. Each
successful renewal replaces the lease in all three snapshots. Consequently,
timeout handling and subsequent failure recording use the latest authoritative
generation and expiry, not the lease originally acquired by `run_quantum`.

Only cancellable external execution waits are raced against the deadline:
provider streaming and tool execution. Durable attachment and checkpoint store
calls are awaited normally rather than dropped mid-operation. The same absolute
post-attachment deadline is checked again at each safe execution boundary, so
checkpoint latency does not reset or extend the execution budget.

When the deadline expires, the supervisor drops the active model or tool future
and cancels its `RunContext`. It then checks durable control state with the
latest lease:

1. A newer `CancelRequested` event executes the existing `cancel_run` path,
   publishes `Cancelled`, and does not increment failure count.
2. Any other control state produces
   `model or tool execution exceeded wall-time limit` as an ordinary work error.
3. Existing `record_failure` increments durable failure count, schedules
   1/2/4/8-second backoff, and terminalizes the fifth failure.
4. Existing failed activity and lease cleanup stop Telegram typing and release
   authority.

Ordinary newer user input does not mask an expired execution. It remains durable
for later attachment. Before expiration, existing compatible-input yielding
behavior remains unchanged.

## Authority And Cleanup

Failure classification must use the renewed `FailureFence`. Lease cleanup must
release the latest lease, not the initial lease. A failed renewal is an
`AuthorityUnavailable` result and must not mutate work failure state.

External supervisor cancellation remains distinct. Dropping `run_quantum` from
outside performs no durable failure accounting, preserving safe shutdown
semantics.

## Tests

Add focused paused-time tests proving:

- a blocked LLM is timed out while heartbeat renewals keep the failure fence
  authoritative;
- a blocked tool is timed out by the same post-attachment deadline;
- timeout after a lease-duration boundary records one recoverable failure and
  releases the renewed lease;
- durable cancellation at timeout returns `Cancelled` with zero failures;
- ordinary queued input does not mask timeout and remains pending;
- external supervisor cancellation records no failure;
- existing retry and fifth-failure terminalization tests remain green.

Run runner tests, the full suite, formatting, check, and Clippy.
