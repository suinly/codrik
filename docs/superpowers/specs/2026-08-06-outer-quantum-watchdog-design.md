# Outer Quantum Watchdog Design

## Goal

Guarantee that one actor quantum cannot remain active beyond
`RunnerLimits.max_wall_time`. A wall-time expiration must use the existing
durable failure path, stop Telegram typing activity, apply retry backoff, and
eventually terminalize the work item after the existing failure limit.

## Problem

The current deadline is polled inside the LLM stream loop. Production showed an
active run renewing its lease and publishing typing for minutes while that
deadline never produced a durable failure. Moving the deadline outside
`run_leased` makes the runner, rather than the provider stream loop, own the
wall-time guarantee.

## Design

`run_quantum` creates one Tokio deadline around `run_leased`. If `run_leased`
finishes first, existing completion, yield, cancellation, authority-error, and
failure behavior remains unchanged.

If the deadline expires:

1. Drop the in-flight `run_leased` future, cancelling the LLM/tool operation.
2. Use the failure fence and activity run already attached by `run_leased`.
3. Check durable control state once. If a `CancelRequested` event targets the
   active work item, execute the existing cancellation path and report
   `Cancelled` without incrementing failure count.
4. Otherwise produce `model generation exceeded wall-time limit` as a normal
   work error. Existing `record_failure` increments the durable failure count,
   schedules 1/2/4/8-second backoff, and terminalizes the fifth failure with the
   existing terminal outbox notification.
5. Publish existing `Failed` ephemeral activity and release the actor lease
   through the existing `run_quantum` cleanup path.

Ordinary newer input does not override an expired deadline. It remains durable
and pending for subsequent attachment. Before deadline expiry, existing signal
handling may still yield promptly for compatible newer input.

Remove the inner LLM-loop wall deadline. Keep heartbeat lease renewal and
control-signal handling inside the stream loop.

## Boundaries

The watchdog applies to the complete leased quantum: attachment, recovery,
model streaming, tool execution, artifact staging, checkpoints, and
finalization. It does not replace component-specific timeouts. Authority errors
from the final cancellation check or failure recording remain
`AuthorityUnavailable` and do not mutate work failure state.

Supervisor task cancellation remains distinct: dropping `run_quantum` from
outside still performs no durable failure accounting, preserving safe shutdown
semantics.

## Tests

Add focused paused-time tests proving:

- a blocked LLM exceeds the outer deadline and records one recoverable failure;
- a blocked operation that never reaches the inner LLM select is still timed
  out;
- durable cancellation at the deadline returns `Cancelled` with zero failures;
- unrelated queued input does not mask timeout and remains pending;
- external supervisor cancellation records no failure;
- failure retry and fifth-failure terminalization continue through existing
  `FailureStore` tests.

Run runner tests, full tests, formatting, check, and Clippy.
