# Reticulum Native Delivery Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Codrik's separate `Думаю...` LXMF message while preserving native LXMF delivery state and the 500-Unicode-character final reply limit.

**Architecture:** Delete the Reticulum activity worker rather than disabling it. Reticulum no longer subscribes to gateway activity or receives a dedicated supervised activity component; Telegram remains the sole consumer of `GatewayActivityHub`. Final Reticulum replies retain their current runner policy and durable delivery path.

**Tech Stack:** Rust 2024, Tokio, SQLite runtime stores, existing Python LXMF bridge.

## Global Constraints

- Delete the separate `Думаю...` LXMF message and all code used only to produce it.
- Preserve final Reticulum reply bounding at 500 Unicode characters.
- Preserve the Reticulum-only transient brevity instruction.
- Preserve durable final delivery and native LXMF delivery status.
- Preserve Telegram activity behavior and CLI output.
- Add no dependency, configuration field, or schema migration.
- Keep historical airtime design and plan documents unchanged; the new native-delivery-status spec supersedes their status-message requirement.
- Run every shell command through `rtk`.

---

## File Structure

- Delete `src/interfaces/reticulum/activity.rs`: the removed synthetic status worker and its tests.
- Modify `src/interfaces/reticulum.rs`: remove activity composition and restore one retry policy for all Reticulum deliveries.
- Modify `src/app.rs`: publish gateway activity only for Telegram and remove Reticulum activity supervision.
- Modify `README.md`: remove `Думаю...` documentation while retaining the 500-character airtime policy.

---

### Task 1: Remove Synthetic Reticulum Status

**Files:**
- Delete: `src/interfaces/reticulum/activity.rs`
- Modify: `src/interfaces/reticulum.rs:1-63,278-320,351-389,392-803`
- Modify: `src/app.rs:41-48,224-307,361-387,886-894`
- Modify: `README.md:368-377`
- Test: `src/app.rs` test module
- Test: `src/interfaces/reticulum.rs` test module
- Test: `src/runtime/runner.rs` existing Reticulum response tests

**Interfaces:**
- Removes: `ReticulumActivityWorker<S, C>`, `PreparedReticulumGateway::activity`, and the `GatewayActivityHub` argument from `reticulum::prepare`.
- Preserves: `reticulum::prepare(config, store, linking, signals, clock, state_dir)` and `bounded_reticulum_response(route, text)` behavior.

- [ ] **Step 1: Change the composition policy test first**

Replace `reticulum_activity_publisher_is_enabled_without_telegram` in `src/app.rs` with:

```rust
#[test]
fn gateway_activity_publisher_is_enabled_only_for_telegram() {
    assert!(publishes_gateway_activity(true));
    assert!(!publishes_gateway_activity(false));
}
```

Do not change `publishes_gateway_activity` yet.

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
rtk cargo test app::tests::gateway_activity_publisher_is_enabled_only_for_telegram -- --nocapture
```

Expected: compilation failure because `publishes_gateway_activity` still accepts two booleans.

- [ ] **Step 3: Remove Reticulum activity composition**

In `src/app.rs`, reduce the policy to Telegram only:

```rust
fn publishes_gateway_activity(telegram: bool) -> bool {
    telegram
}
```

Use it as:

```rust
let events: Arc<dyn RuntimeEventPublisher> = if publishes_gateway_activity(telegram.is_some()) {
    Arc::new(CompositeRuntimeEventPublisher::new(
        hub.clone(),
        gateway_activity.clone(),
    ))
} else {
    hub.clone()
};
```

Remove `gateway_activity.clone()` from the `reticulum::prepare` call. Remove the entire `reticulum-activity` `service.component`; retain the existing `reticulum` component unchanged.

- [ ] **Step 4: Delete the worker and Reticulum-specific policy**

Delete `src/interfaces/reticulum/activity.rs`.

In `src/interfaces/reticulum.rs`:

- remove `pub mod activity;`;
- remove the `GatewayActivityHub` import;
- remove `THINKING_INTENT_PREFIX` and `is_thinking_delivery`;
- remove the `activity` field from `PreparedReticulumGateway`;
- remove `PreparedReticulumGateway::activity`;
- remove `activity_hub: GatewayActivityHub` from `prepare`;
- remove activity receiver initialization;
- remove `GatewayActivityHub::default()` arguments from all direct `prepare` calls;
- delete `retryable_thinking_delivery_is_terminal`.

Restore the `Retryable` transition to one branch for every delivery:

```rust
protocol::BridgeDeliveryOutcome::Retryable => {
    let delay = retry_after_ms.unwrap_or_else(|| {
        let exponent = delivery.attempt_count.saturating_sub(1).min(5);
        1000_u64
            .checked_shl(exponent as u32)
            .unwrap_or(30_000)
            .min(30_000)
    });
    store
        .retry_gateway_delivery(
            &delivery.claim,
            now.plus_millis(delay.min(i64::MAX as u64) as i64),
            "reticulum_retryable",
            "LXMF delivery failed retryably",
            now,
        )
        .await?
}
```

- [ ] **Step 5: Update operator documentation**

Replace the README paragraph about delayed status with:

```markdown
Reticulum replies are intentionally concise and limited to 500 Unicode
characters to reduce airtime. CLI and Telegram replies are unaffected.
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
rtk cargo test app::tests::gateway_activity_publisher_is_enabled_only_for_telegram -- --nocapture
rtk cargo test interfaces::reticulum -- --nocapture
rtk cargo test runtime::runner::tests::reticulum_response -- --nocapture
rtk cargo test --test install_script -- --nocapture
```

Expected: all selected tests pass. Runner tests still prove Reticulum replies are bounded; no activity test module remains.

- [ ] **Step 7: Verify removed and preserved behavior statically**

Run:

```bash
rtk rg 'Думаю|reticulum-thinking|reticulum-activity|ReticulumActivityWorker' src README.md
rtk rg 'RETICULUM_RESPONSE_CHARS|500 Unicode characters' src/runtime/runner.rs README.md
```

Expected: first command exits 1 with no matches; second command finds the runner limit/instruction, runner tests, and README documentation.

- [ ] **Step 8: Run all project gates**

Run:

```bash
rtk python3 src/interfaces/reticulum/bridge.py --self-check
rtk cargo fmt --check
rtk cargo check
rtk cargo test
rtk cargo clippy --all-targets --all-features
```

Expected: every command exits 0; full tests report no failures.

- [ ] **Step 9: Commit**

Inspect scope first:

```bash
rtk git status --short
rtk git diff --stat
rtk git diff -- src/interfaces/reticulum.rs src/app.rs README.md
rtk git log --oneline -10
```

Then commit only implementation files:

```bash
rtk git add src/interfaces/reticulum.rs src/interfaces/reticulum/activity.rs src/app.rs README.md
rtk git commit -m "fix(reticulum): remove thinking status"
```
