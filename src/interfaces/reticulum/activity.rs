use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::Duration,
};

use anyhow::Result;
use tokio::{
    sync::{broadcast, watch},
    time::Instant,
};

use crate::{
    llm::client::AgentActivityEvent,
    runtime::{
        gateway::{DeliveryRoute, NewGatewayDelivery},
        gateway_activity::{GatewayActivity, GatewayActivityEvent},
        model::{Clock, WorkItemId},
        store::{GatewayDeliveryStore, OutboxPayload},
    },
};

const THINKING_DELAY: Duration = Duration::from_secs(5);
const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(100);
const THINKING_TEXT: &str = "Думаю...";

struct ActivityState {
    route: DeliveryRoute,
    due_at: Instant,
}

pub struct ReticulumActivityWorker<S, C> {
    store: S,
    clock: C,
    gateway: String,
    states: Mutex<HashMap<WorkItemId, ActivityState>>,
    sent: Mutex<HashSet<WorkItemId>>,
}

impl<S, C> ReticulumActivityWorker<S, C>
where
    S: GatewayDeliveryStore,
    C: Clock,
{
    pub fn new(store: S, clock: C, gateway: impl Into<String>) -> Self {
        Self {
            store,
            clock,
            gateway: gateway.into(),
            states: Mutex::new(HashMap::new()),
            sent: Mutex::new(HashSet::new()),
        }
    }

    pub async fn run(
        &self,
        mut activity: broadcast::Receiver<GatewayActivity>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut maintenance = tokio::time::interval(MAINTENANCE_INTERVAL);
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                received = activity.recv() => match received {
                    Ok(event) => self.handle(event).await,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
                _ = maintenance.tick() => self.maintain().await,
            }
        }
    }

    pub(crate) async fn handle(&self, activity: GatewayActivity) {
        if activity.route.gateway != self.gateway {
            return;
        }
        match activity.event {
            GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted) => {
                if self
                    .sent
                    .lock()
                    .expect("Reticulum activity sent set poisoned")
                    .contains(&activity.work_item_id)
                {
                    return;
                }
                self.states
                    .lock()
                    .expect("Reticulum activity states poisoned")
                    .entry(activity.work_item_id)
                    .or_insert(ActivityState {
                        route: activity.route,
                        due_at: Instant::now() + THINKING_DELAY,
                    });
            }
            GatewayActivityEvent::Activity(
                AgentActivityEvent::Completed
                | AgentActivityEvent::Failed
                | AgentActivityEvent::Cancelled,
            ) => {
                let due = self
                    .states
                    .lock()
                    .expect("Reticulum activity states poisoned")
                    .get(&activity.work_item_id)
                    .is_some_and(|state| Instant::now() >= state.due_at);
                if due {
                    self.maintain().await;
                }
                self.states
                    .lock()
                    .expect("Reticulum activity states poisoned")
                    .remove(&activity.work_item_id);
                self.sent
                    .lock()
                    .expect("Reticulum activity sent set poisoned")
                    .remove(&activity.work_item_id);
            }
            _ => {}
        }
    }

    pub(crate) async fn maintain(&self) {
        let now = Instant::now();
        let due = {
            let mut states = self
                .states
                .lock()
                .expect("Reticulum activity states poisoned");
            let work_items = states
                .iter()
                .filter(|(_, state)| now >= state.due_at)
                .map(|(work_item, _)| work_item.clone())
                .collect::<Vec<_>>();
            work_items
                .into_iter()
                .filter_map(|work_item| states.remove(&work_item).map(|state| (work_item, state)))
                .collect::<Vec<_>>()
        };
        for (work_item, state) in due {
            self.sent
                .lock()
                .expect("Reticulum activity sent set poisoned")
                .insert(work_item.clone());
            let delivery = NewGatewayDelivery::new(
                format!("reticulum-thinking:{}:{work_item}", self.gateway),
                None,
                0,
                state.route,
                OutboxPayload::Text {
                    text: THINKING_TEXT.into(),
                },
            );
            let result = match delivery {
                Ok(delivery) => {
                    self.store
                        .enqueue_gateway_delivery(delivery, self.clock.now())
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                eprintln!("reticulum activity: failed to enqueue thinking status: {error:#}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;

    use super::ReticulumActivityWorker;
    use crate::{
        llm::client::AgentActivityEvent,
        runtime::{
            gateway::DeliveryRoute,
            gateway_activity::{GatewayActivity, GatewayActivityEvent},
            model::{ManualClock, Timestamp, WorkItemId},
            sqlite::SqliteRuntimeStore,
            store::{GatewayDeliveryStore, OutboxPayload},
        },
    };

    const DESTINATION: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SOURCE: &str = "cccccccccccccccccccccccccccccccc";

    fn activity(
        work_item_id: WorkItemId,
        route: DeliveryRoute,
        event: AgentActivityEvent,
    ) -> GatewayActivity {
        GatewayActivity {
            work_item_id,
            route,
            event: GatewayActivityEvent::Activity(event),
        }
    }

    async fn claim_reticulum(
        store: &SqliteRuntimeStore,
    ) -> Result<Vec<crate::runtime::gateway::ClaimedGatewayDelivery>> {
        store
            .claim_gateway_deliveries(
                &format!("reticulum:{DESTINATION}"),
                "test",
                Timestamp(2_000),
                Timestamp(32_000),
                10,
            )
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn thinking_status_is_enqueued_once_after_five_seconds() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let gateway = format!("reticulum:{DESTINATION}");
        let worker =
            ReticulumActivityWorker::new(store.clone(), ManualClock::new(1_000), gateway.clone());
        let route = DeliveryRoute::new(gateway, SOURCE, Some("a".repeat(64)), 256 * 1024, 1)?;
        let work = WorkItemId::new();

        worker
            .handle(activity(
                work.clone(),
                route.clone(),
                AgentActivityEvent::ModelStepStarted,
            ))
            .await;
        tokio::time::advance(Duration::from_millis(4_999)).await;
        worker.maintain().await;
        assert!(claim_reticulum(&store).await?.is_empty());

        tokio::time::advance(Duration::from_millis(1)).await;
        worker.maintain().await;
        let deliveries = claim_reticulum(&store).await?;
        assert_eq!(deliveries.len(), 1);
        assert_eq!(
            deliveries[0].payload,
            OutboxPayload::Text {
                text: "Думаю...".into()
            }
        );

        worker
            .handle(activity(work, route, AgentActivityEvent::ModelStepStarted))
            .await;
        tokio::time::advance(Duration::from_secs(5)).await;
        worker.maintain().await;
        assert!(claim_reticulum(&store).await?.is_empty());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_event_before_deadline_cancels_status() -> Result<()> {
        for terminal in [
            AgentActivityEvent::Completed,
            AgentActivityEvent::Failed,
            AgentActivityEvent::Cancelled,
        ] {
            let store = SqliteRuntimeStore::open_in_memory().await?;
            let gateway = format!("reticulum:{DESTINATION}");
            let worker = ReticulumActivityWorker::new(
                store.clone(),
                ManualClock::new(1_000),
                gateway.clone(),
            );
            let route = DeliveryRoute::new(&gateway, SOURCE, None, 256 * 1024, 1)?;
            let work = WorkItemId::new();
            worker
                .handle(activity(
                    work.clone(),
                    route.clone(),
                    AgentActivityEvent::ModelStepStarted,
                ))
                .await;
            tokio::time::advance(Duration::from_millis(4_999)).await;
            worker.handle(activity(work, route, terminal)).await;
            tokio::time::advance(Duration::from_millis(1)).await;
            worker.maintain().await;
            assert!(claim_reticulum(&store).await?.is_empty());
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_event_after_deadline_preserves_status() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let gateway = format!("reticulum:{DESTINATION}");
        let worker =
            ReticulumActivityWorker::new(store.clone(), ManualClock::new(1_000), gateway.clone());
        let route = DeliveryRoute::new(gateway, SOURCE, None, 256 * 1024, 1)?;
        let work = WorkItemId::new();
        worker
            .handle(activity(
                work.clone(),
                route.clone(),
                AgentActivityEvent::ModelStepStarted,
            ))
            .await;
        tokio::time::advance(Duration::from_millis(5_001)).await;
        worker
            .handle(activity(work, route, AgentActivityEvent::Completed))
            .await;

        assert_eq!(claim_reticulum(&store).await?.len(), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn subscribed_receiver_retains_event_before_worker_starts() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let gateway = format!("reticulum:{DESTINATION}");
        let worker =
            ReticulumActivityWorker::new(store.clone(), ManualClock::new(1_000), gateway.clone());
        let hub = crate::runtime::gateway_activity::GatewayActivityHub::default();
        let receiver = hub.subscribe();
        hub.publish(
            WorkItemId::new(),
            DeliveryRoute::new(gateway, SOURCE, None, 256 * 1024, 1)?,
            GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted),
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn(async move { worker.run(receiver, shutdown_rx).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        shutdown_tx.send(true)?;
        run.await??;

        assert_eq!(claim_reticulum(&store).await?.len(), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn other_gateway_activity_is_ignored() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let gateway = format!("reticulum:{DESTINATION}");
        let worker = ReticulumActivityWorker::new(store.clone(), ManualClock::new(1_000), gateway);
        for route in [
            DeliveryRoute::new("telegram:1", "2", None, 4096, 1024)?,
            DeliveryRoute::new(
                "reticulum:dddddddddddddddddddddddddddddddd",
                SOURCE,
                None,
                256 * 1024,
                1,
            )?,
        ] {
            worker
                .handle(activity(
                    WorkItemId::new(),
                    route,
                    AgentActivityEvent::ModelStepStarted,
                ))
                .await;
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        worker.maintain().await;
        assert!(claim_reticulum(&store).await?.is_empty());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_maintenance_does_not_duplicate_status() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let gateway = format!("reticulum:{DESTINATION}");
        let worker =
            ReticulumActivityWorker::new(store.clone(), ManualClock::new(1_000), gateway.clone());
        worker
            .handle(activity(
                WorkItemId::new(),
                DeliveryRoute::new(gateway, SOURCE, None, 256 * 1024, 1)?,
                AgentActivityEvent::ModelStepStarted,
            ))
            .await;
        tokio::time::advance(Duration::from_secs(5)).await;
        worker.maintain().await;
        worker.maintain().await;
        assert_eq!(claim_reticulum(&store).await?.len(), 1);
        Ok(())
    }
}
