# Outer Quantum Watchdog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce `RunnerLimits.max_wall_time` around the complete leased actor quantum and route expiration through durable failure accounting.

**Architecture:** `ActorRunner::run_quantum` owns one Tokio timeout around `run_leased`. On expiration it checks only durable cancellation, otherwise creates the existing recoverable work failure; classification, backoff, terminalization, activity, and lease release remain unchanged. The inner LLM loop retains heartbeat and signal handling but no wall deadline.

**Tech Stack:** Rust 2024, Tokio, anyhow, async-trait, SQLite runtime store.

## Global Constraints

- No new dependency or public abstraction.
- Preserve supervisor task-abort semantics: external abort records no failure.
- Durable `CancelRequested` at timeout wins without incrementing failure count.
- Ordinary queued input never masks an expired quantum and remains durable.
- Existing failure backoff and fifth-failure terminalization remain authoritative.
- Run all commands through `rtk`.

---

### Task 1: Outer Quantum Watchdog

**Files:**
- Modify: `src/runtime/runner.rs:193-205,400-500,720-818`
- Test: `src/runtime/runner.rs` test module

**Interfaces:**
- Consumes: `ActorRunner::run_leased`, `ControlStore::newer_control_event`, `CheckpointStore::cancel_run`, `FailureStore::record_failure`.
- Produces: unchanged `QuantumRunner::run_quantum(...) -> Result<QuantumReport, QuantumFailure>` behavior with a hard outer deadline.

- [ ] **Step 1: Write the failing outer-timeout regression**

Add a paused-time test using a test-only `BlockingIncorporationHook { started: Arc<Notify> }` whose `RuntimeBoundaryHooks::incorporation_committed` notifies `started` and then awaits `std::future::pending()`. Spawn `run_quantum`, await `started.notified()`, advance `max_wall_time`, then assert `QuantumFailure::RecoverableWork`, failure count `1`, and released lease. This hook blocks inside `run_leased` before the inner LLM loop, so the current implementation cannot satisfy the test.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```sh
rtk test cargo test --lib runtime::runner::tests::quantum_watchdog_times_out_before_model_stream -- --exact
```

Expected: failure or hang under a short harness timeout because the current deadline exists only inside the model stream loop.

- [ ] **Step 3: Add one timeout around `run_leased`**

In `run_quantum`, pin `tokio::time::sleep(self.limits.max_wall_time)` and select it against `run_leased`. On expiration, inspect the attached failure fence/activity run. Re-check `newer_control_event`; process only `EventKind::CancelRequested` through `cancel_run`, progress `Finalized`, and `Cancelled` activity. Otherwise return `anyhow::bail!("model generation exceeded wall-time limit")` into the existing classification block.

Delete `wall_deadline` creation from `run_leased` and its branch from the inner LLM stream `tokio::select!`. Keep heartbeat and signal branches unchanged.

- [ ] **Step 4: Verify timeout, cancellation, queued-input, abort semantics**

Run:

```sh
rtk test cargo test --lib runtime::runner::tests
```

Expected: all runner tests pass, including outer timeout, durable cancel at deadline, queued user input at deadline, and supervisor cancellation without failure increment.

- [ ] **Step 5: Run full verification**

Run:

```sh
rtk cargo fmt --check
rtk cargo check
rtk cargo clippy --all-targets --all-features
rtk test cargo test
```

Expected: commands exit `0`; full suite has no failures.

- [ ] **Step 6: Commit implementation**

```sh
rtk git add src/runtime/runner.rs
rtk git commit -m "fix(runtime): enforce outer quantum timeout"
```
