# Post-Attachment Execution Watchdog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound post-attachment model and tool execution while continuously renewing the actor lease and routing expiration through durable failure accounting.

**Architecture:** Keep attachment and incorporation checkpointing outside cancellation. After a `FailureFence` exists, supervise model/tool waits with one absolute deadline, heartbeat lease renewal, and durable control checks. Propagate every renewed lease into `AttachedRun`, `FailureFence`, failure recording, and release.

**Tech Stack:** Rust 2024, Tokio, anyhow, async-trait, SQLite runtime store.

## Global Constraints

- No new dependency or public abstraction.
- Start the deadline only after durable attachment and incorporation checkpoint.
- Never drop an in-flight attachment or checkpoint store operation.
- Keep heartbeat renewal active during blocked model and tool execution.
- Use the latest renewed lease for control, failure accounting, and release.
- Preserve supervisor task-abort semantics: external abort records no failure.
- Durable `CancelRequested` at timeout wins without incrementing failure count.
- Ordinary queued input never masks expiration and remains durable.
- Existing failure backoff and fifth-failure terminalization remain authoritative.
- Run all commands through `rtk`.

---

### Task 1: Lease-Renewing Execution Watchdog

**Files:**
- Modify: `src/runtime/runner.rs:193-725,744-834`
- Test: `src/runtime/runner.rs` test module

**Interfaces:**
- Consumes: `DispatchStore::renew_lease`, `ControlStore::newer_control_event`, `CheckpointStore::cancel_run`, `FailureStore::record_failure`.
- Produces: unchanged `QuantumRunner::run_quantum(...) -> Result<QuantumReport, QuantumFailure>` with bounded model/tool waits and an authoritative renewed failure fence.

- [ ] **Step 1: Write the failing lease-authority regression**

Add a paused-time test named
`execution_timeout_renews_lease_before_recording_failure`. Configure
`lease_duration = 30ms`, `heartbeat_interval = 10ms`, and
`max_wall_time = 60ms`. Use `BlockingLlm`, spawn `run_quantum`, await its start,
advance 60ms, then assert `QuantumFailure::RecoverableWork`, durable failure
count `1`, and no current lease. This catches the bug where the deadline
outlives the original lease or failure recording uses its stale snapshot.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```sh
rtk cargo test --lib runtime::runner::tests::execution_timeout_renews_lease_before_recording_failure -- --exact
```

Expected: FAIL because current timeout/heartbeat ownership cannot guarantee a
renewed fence through expiration.

- [ ] **Step 3: Move execution timing and heartbeat ownership above external waits**

Refactor the post-incorporation portion of `run_leased` so one execution
supervisor owns the absolute deadline, heartbeat interval, current lease,
`AttachedRun`, and `FailureFence`. Start it after
`incorporation_committed`. Race both `llm.stream(...)` and
`tools.execute(...)` against heartbeat and the same pinned deadline. On each
heartbeat call:

```rust
let now = self.clock.now();
current_lease = self
    .store
    .renew_lease(
        &current_lease,
        now,
        now.plus_millis(duration_millis(self.limits.lease_duration)?),
    )
    .await?;
run.lease = current_lease.clone();
*failure_fence = Some(FailureFence::from(&run));
*activity_run = Some(run.clone());
```

Await attachment and checkpoint store operations outside cancellable external
waits. Check the absolute deadline again before starting the next external
wait; never create a replacement deadline.

- [ ] **Step 4: Use renewed authority for timeout and cleanup**

On expiration, cancel `RunContext`, query `newer_control_event` with the current
lease, and process only `EventKind::CancelRequested` through the existing
`cancel_run` path. Otherwise return
`anyhow::bail!("model or tool execution exceeded wall-time limit")` to existing
failure classification. Ensure `run_quantum` releases the latest lease snapshot
rather than the initially acquired lease.

- [ ] **Step 5: Verify GREEN for the authority regression**

Run:

```sh
rtk cargo test --lib runtime::runner::tests::execution_timeout_renews_lease_before_recording_failure -- --exact
```

Expected: PASS.

- [ ] **Step 6: Add the failing blocked-tool regression**

Add `tool_execution_obeys_post_attachment_deadline` using one scripted model
tool call and a test executor that notifies then remains pending. Advance paused
time to `max_wall_time`; assert one recoverable failure, failure count `1`, and
released lease. Run it before implementation adjustment and confirm it fails
because current tool execution is not raced against the deadline.

- [ ] **Step 7: Extend the same supervisor to tool execution**

Apply the existing deadline/heartbeat/control wait helper to
`tools.execute(...)`; do not add a second timeout or reset elapsed time. Run:

```sh
rtk cargo test --lib runtime::runner::tests::tool_execution_obeys_post_attachment_deadline -- --exact
rtk cargo test --lib runtime::runner::tests
```

Expected: both commands pass, including cancellation, queued-input, and
external-abort coverage.

- [ ] **Step 8: Run full verification**

Run:

```sh
rtk cargo fmt --check
rtk cargo check
rtk cargo clippy --all-targets --all-features
rtk cargo test
```

Expected: every command exits `0`; full suite has no failures.

- [ ] **Step 9: Commit implementation**

```sh
rtk git add src/runtime/runner.rs
rtk git commit -m "fix(runtime): supervise execution timeout"
```
