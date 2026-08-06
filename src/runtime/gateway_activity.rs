use tokio::sync::broadcast;

use crate::{
    llm::client::AgentActivityEvent,
    runtime::{gateway::DeliveryRoute, model::WorkItemId},
};

const DEFAULT_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub struct GatewayActivity {
    pub work_item_id: WorkItemId,
    pub route: DeliveryRoute,
    pub ingress_source: Option<String>,
    pub event: GatewayActivityEvent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayActivityEvent {
    Activity(AgentActivityEvent),
    TextDelta(String),
}

#[derive(Clone)]
pub struct GatewayActivityHub {
    sender: broadcast::Sender<GatewayActivity>,
}

impl Default for GatewayActivityHub {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

impl GatewayActivityHub {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "gateway activity capacity must be positive");
        Self {
            sender: broadcast::channel(capacity).0,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<GatewayActivity> {
        self.sender.subscribe()
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
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{GatewayActivityEvent, GatewayActivityHub};
    use crate::{
        llm::client::AgentActivityEvent,
        runtime::{
            gateway::DeliveryRoute,
            ipc::protocol::ServerEventBody,
            model::{ActorId, Audience, RequestId, RunId, Timestamp, WorkItemId},
            store::{ActorLease, AttachedRun},
            stream_hub::{CompositeRuntimeEventPublisher, RuntimeEventPublisher, StreamHub},
        },
    };

    fn run(route: Option<DeliveryRoute>) -> AttachedRun {
        AttachedRun {
            lease: ActorLease {
                actor_id: ActorId::from_string("actor"),
                owner_id: "owner".into(),
                generation: 1,
                expires_at: Timestamp(1_000),
            },
            work_item_id: WorkItemId::new(),
            run_id: RunId::new(),
            observed_sequence: 1,
            source_event_ids: Vec::new(),
            request_ids: vec![RequestId::new()],
            audience: Audience::ActorPrivate,
            delivery_route: route,
            execution_policy: crate::runtime::model::ExecutionPolicy::ActorTools,
            ingress_source: None,
            messages: Vec::new(),
        }
    }

    #[tokio::test]
    async fn bounded_activity_hub_publishes_without_blocking() -> Result<()> {
        let hub = GatewayActivityHub::with_capacity(2);
        let mut receiver = hub.subscribe();
        let route = DeliveryRoute::new("telegram:900", "100", None, 4096, 1024)?;
        let work_item_id = WorkItemId::new();

        hub.publish(
            work_item_id.clone(),
            route.clone(),
            None,
            GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted),
        );
        hub.publish(
            work_item_id.clone(),
            route,
            None,
            GatewayActivityEvent::TextDelta("hello".into()),
        );

        let first = receiver.recv().await?;
        let second = receiver.recv().await?;
        assert_eq!(first.work_item_id, work_item_id);
        assert!(matches!(
            first.event,
            GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted)
        ));
        assert!(matches!(
            second.event,
            GatewayActivityEvent::TextDelta(ref delta) if delta == "hello"
        ));
        Ok(())
    }

    #[test]
    fn publishing_without_receivers_is_best_effort() -> Result<()> {
        let hub = GatewayActivityHub::with_capacity(1);
        hub.publish(
            WorkItemId::new(),
            DeliveryRoute::new("telegram:900", "100", None, 4096, 1024)?,
            None,
            GatewayActivityEvent::Activity(AgentActivityEvent::Completed),
        );
        Ok(())
    }

    #[tokio::test]
    async fn composite_publishes_to_local_and_routed_gateway_subscribers() -> Result<()> {
        let local = StreamHub::default();
        let gateway = GatewayActivityHub::with_capacity(4);
        let mut gateway_receiver = gateway.subscribe();
        let composite =
            CompositeRuntimeEventPublisher::new(std::sync::Arc::new(local.clone()), gateway);
        let run = run(Some(DeliveryRoute::new(
            "telegram:900",
            "100",
            None,
            4096,
            1024,
        )?));
        let mut local_receiver = local.subscribe(run.request_ids[0].clone()).unwrap();

        composite.publish_text(&run, "hello");

        assert!(matches!(
            local_receiver.recv().await.expect("local event").body,
            ServerEventBody::TextDelta { ref delta, .. } if delta == "hello"
        ));
        assert!(matches!(
            gateway_receiver.recv().await?.event,
            GatewayActivityEvent::TextDelta(ref delta) if delta == "hello"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn composite_copies_webhook_source_only_to_gateway_metadata() -> Result<()> {
        let local = StreamHub::default();
        let gateway = GatewayActivityHub::with_capacity(2);
        let mut gateway_receiver = gateway.subscribe();
        let composite =
            CompositeRuntimeEventPublisher::new(std::sync::Arc::new(local.clone()), gateway);
        let mut run = run(Some(DeliveryRoute::new(
            "telegram:900",
            "100",
            None,
            4096,
            1024,
        )?));
        run.ingress_source = Some("grafana".into());
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

    #[test]
    fn composite_skips_gateway_for_unrouted_runs() {
        let gateway = GatewayActivityHub::with_capacity(1);
        let mut receiver = gateway.subscribe();
        let composite = CompositeRuntimeEventPublisher::new(
            std::sync::Arc::new(crate::runtime::stream_hub::NoopRuntimeEventPublisher),
            gateway,
        );

        composite.publish_activity(&run(None), AgentActivityEvent::Completed);

        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }
}
