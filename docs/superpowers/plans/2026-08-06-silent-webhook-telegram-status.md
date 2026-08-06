# Silent Webhook Telegram Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep Telegram typing indicators for generic webhook runs while suppressing every ephemeral Telegram status message and edit before the durable final notification.

**Architecture:** Propagate trusted `AttachedRun.ingress_source` through ephemeral `GatewayActivity`. Keep the shared publisher complete; apply webhook-specific presentation filtering only in `TelegramActivityWorker`, preserving local activity and all ordinary routed-run behavior.

**Tech Stack:** Rust 2024, Tokio broadcast/activity workers, existing Telegram API adapter and runtime event publisher.

## Global Constraints

- Apply to every generic webhook endpoint; never special-case Grafana or endpoint names.
- Identify webhook runs only by non-null durable `AttachedRun.ingress_source`.
- Webhook model steps continue sending Telegram typing actions.
- Webhook descriptions, tool events, and terminal activity create or edit no Telegram status messages.
- Terminal webhook activity clears ephemeral typing state.
- Durable final outbox delivery, route snapshots, deferred delivery, execution policy, and local/IPC activity remain unchanged.
- Ordinary routed runs retain current typing and status behavior.
- Add no dependencies.
- Run every repository command through `rtk`.

## File Structure

- Modify `src/runtime/gateway_activity.rs`: carry trusted ingress metadata on routed ephemeral activity and test propagation.
- Modify `src/runtime/stream_hub.rs`: copy `AttachedRun.ingress_source` into every routed gateway event while preserving local publication.
- Modify `src/interfaces/telegram/activity.rs`: filter webhook status activity while retaining typing and terminal state cleanup.

---

### Task 1: Suppress Webhook Telegram Status Messages

**Files:**
- Modify: `src/runtime/gateway_activity.rs:10-57,60-185`
- Modify: `src/runtime/stream_hub.rs:39-72`
- Modify: `src/interfaces/telegram/activity.rs:103-177,292-548`

**Interfaces:**
- Changes: `GatewayActivity` gains `pub ingress_source: Option<String>`.
- Changes: `GatewayActivityHub::publish(work_item_id, route, ingress_source, event)` accepts trusted run source metadata.
- Consumes: `AttachedRun.ingress_source` already loaded and checkpoint-fenced by the runtime store.
- Preserves: `RuntimeEventPublisher` signatures and all durable store/outbox interfaces.

- [ ] **Step 1: Write failing gateway metadata propagation tests**

Update the `run` fixture to accept an ingress source, then prove routed events carry it while local subscribers still receive the same event:

```rust
fn run(route: Option<DeliveryRoute>, ingress_source: Option<&str>) -> AttachedRun {
    AttachedRun {
        // existing fields unchanged
        delivery_route: route,
        ingress_source: ingress_source.map(str::to_owned),
        // existing fields unchanged
    }
}

#[tokio::test]
async fn composite_copies_webhook_source_only_to_gateway_metadata() -> Result<()> {
    let local = StreamHub::default();
    let gateway = GatewayActivityHub::with_capacity(2);
    let mut gateway_receiver = gateway.subscribe();
    let composite = CompositeRuntimeEventPublisher::new(Arc::new(local.clone()), gateway);
    let run = run(
        Some(DeliveryRoute::new("telegram:900", "100", None, 4096, 1024)?),
        Some("grafana"),
    );
    let mut local_receiver = local.subscribe(run.request_ids[0].clone()).unwrap();

    composite.publish_activity(&run, AgentActivityEvent::ModelStepStarted);

    assert!(matches!(
        local_receiver.recv().await.unwrap().body,
        ServerEventBody::Activity { .. }
    ));
    assert_eq!(
        gateway_receiver.recv().await?.ingress_source.as_deref(),
        Some("grafana")
    );
    Ok(())
}
```

Update direct `GatewayActivityHub::publish` calls with `None`; update direct `GatewayActivity` literals with `ingress_source: None`.

- [ ] **Step 2: Write failing Telegram webhook activity tests**

Extend the test helper without changing ordinary call sites:

```rust
fn webhook_activity(work_item: &WorkItemId, event: GatewayActivityEvent) -> GatewayActivity {
    GatewayActivity {
        ingress_source: Some("grafana".into()),
        ..activity(work_item, event)
    }
}
```

Add one lifecycle test that proves typing remains visible but status messages never appear:

```rust
#[tokio::test(start_paused = true)]
async fn webhook_tool_run_keeps_typing_without_status_messages() {
    let api = ActivityApi::default();
    let worker = TelegramActivityWorker::new(api.clone(), "telegram:900");
    let work = WorkItemId::new();

    for event in [
        AgentActivityEvent::ModelStepStarted,
        AgentActivityEvent::Description("Использую skill".into()),
        AgentActivityEvent::ToolStarted { name: "skills_read".into() },
        AgentActivityEvent::ToolFinished { name: "skills_read".into(), succeeded: true },
    ] {
        worker
            .handle(webhook_activity(&work, GatewayActivityEvent::Activity(event)))
            .await;
    }

    assert_eq!(api.actions.lock().unwrap().len(), 1);
    assert!(api.sent.lock().unwrap().is_empty());
    assert!(api.edited.lock().unwrap().is_empty());

    tokio::time::advance(Duration::from_secs(4)).await;
    worker.maintain().await;
    assert_eq!(api.actions.lock().unwrap().len(), 2);

    worker
        .handle(webhook_activity(
            &work,
            GatewayActivityEvent::Activity(AgentActivityEvent::Completed),
        ))
        .await;
    tokio::time::advance(Duration::from_secs(4)).await;
    worker.maintain().await;

    assert_eq!(api.actions.lock().unwrap().len(), 2);
    assert!(api.sent.lock().unwrap().is_empty());
    assert!(api.edited.lock().unwrap().is_empty());
}
```

Add a table-driven terminal regression for `Completed`, `Cancelled`, and `Failed`; each must clear state and send/edit nothing. Keep existing ordinary status tests unchanged.

- [ ] **Step 3: Run focused tests to verify RED**

Run:

```bash
rtk cargo test runtime::gateway_activity::tests::composite_copies_webhook_source_only_to_gateway_metadata -- --nocapture
rtk cargo test interfaces::telegram::activity::tests::webhook -- --nocapture
```

Expected: FAIL because `GatewayActivity` has no ingress metadata and Telegram treats webhook activity as ordinary status activity.

- [ ] **Step 4: Propagate trusted ingress metadata**

Add the field and publish argument:

```rust
pub struct GatewayActivity {
    pub work_item_id: WorkItemId,
    pub route: DeliveryRoute,
    pub ingress_source: Option<String>,
    pub event: GatewayActivityEvent,
}

pub fn publish(
    &self,
    work_item_id: WorkItemId,
    route: DeliveryRoute,
    ingress_source: Option<String>,
    event: GatewayActivityEvent,
) {
    let _ = self.sender.send(GatewayActivity {
        work_item_id,
        route,
        ingress_source,
        event,
    });
}
```

In both `CompositeRuntimeEventPublisher` methods, pass `run.ingress_source.clone()` to `gateway.publish`. Do not filter events here; local and gateway publishers continue receiving the full event stream.

- [ ] **Step 5: Filter webhook presentation in Telegram**

After gateway/text checks and key construction, handle webhook events before ordinary `ActivityState` logic:

```rust
if activity.ingress_source.is_some() {
    match activity.event {
        GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted) => {}
        GatewayActivityEvent::Activity(
            AgentActivityEvent::Completed
            | AgentActivityEvent::Cancelled
            | AgentActivityEvent::Failed,
        ) => {
            self.states
                .lock()
                .expect("Telegram activity states poisoned")
                .remove(&key);
            return;
        }
        GatewayActivityEvent::Activity(_) => return,
        GatewayActivityEvent::TextDelta(_) => unreachable!(),
    }
}
```

Let webhook `ModelStepStarted` continue through the existing ordinary branch so it sends typing and creates a typing-only state. Ignored description/tool events must not set `typing = false`; maintenance therefore refreshes typing until the next model step or terminal cleanup. Do not call `ensure_status` or `edit_status` for webhook activity.

- [ ] **Step 6: Run focused and regression tests**

Run:

```bash
rtk cargo test runtime::gateway_activity::tests -- --nocapture
rtk cargo test runtime::stream_hub::tests -- --nocapture
rtk cargo test interfaces::telegram::activity::tests -- --nocapture
rtk cargo test app::tests::webhook_end_to_end_delivers_once_to_latest_telegram_route -- --nocapture
```

Expected: PASS. Ordinary Telegram status tests must retain their existing assertions.

- [ ] **Step 7: Run full verification**

Run:

```bash
rtk cargo fmt --check
rtk cargo check
rtk cargo test
rtk cargo clippy --all-targets --all-features
rtk git diff --check
```

Expected: every command exits 0; existing non-denied Clippy warnings may remain unchanged.

- [ ] **Step 8: Commit**

```bash
rtk git add src/runtime/gateway_activity.rs src/runtime/stream_hub.rs src/interfaces/telegram/activity.rs
rtk git commit -m "fix(webhook): suppress telegram status activity"
```

## Final Review

- [ ] Confirm no endpoint name or Grafana-specific production branch exists.
- [ ] Confirm webhook activity still reaches local/IPC subscribers.
- [ ] Confirm webhook typing repeats while active and stops after every terminal event.
- [ ] Confirm no webhook activity path calls Telegram send/edit status APIs.
- [ ] Confirm durable final and deferred webhook delivery tests remain green.
- [ ] Confirm ordinary Telegram activity behavior is byte-for-byte unchanged in existing tests.
