use anyhow::Result;
use async_trait::async_trait;
use axum::body::Bytes;
use sha2::{Digest, Sha256};

use crate::{
    config::ValidatedWebhookEndpoint,
    runtime::{
        model::Clock,
        observability::{RuntimeComponent, RuntimeLogEvent, RuntimeLogger, RuntimeTransition},
        signals::ActorSignals,
        store::{NewWebhookEvent, WebhookIdempotency, WebhookIngressOutcome, WebhookIngressStore},
    },
};

#[async_trait]
pub trait WebhookIngress: Send + Sync + 'static {
    async fn handle(
        &self,
        endpoint: &ValidatedWebhookEndpoint,
        body: Bytes,
        idempotency_key: Option<&[u8]>,
    ) -> Result<WebhookIngressOutcome>;
}

pub struct WebhookIngressService<S, C> {
    store: S,
    signals: ActorSignals,
    clock: C,
    logger: std::sync::Arc<dyn RuntimeLogger>,
}

impl<S, C> WebhookIngressService<S, C> {
    pub fn new(
        store: S,
        signals: ActorSignals,
        clock: C,
        logger: std::sync::Arc<dyn RuntimeLogger>,
    ) -> Self {
        Self {
            store,
            signals,
            clock,
            logger,
        }
    }
}

#[async_trait]
impl<S, C> WebhookIngress for WebhookIngressService<S, C>
where
    S: WebhookIngressStore + Send + Sync + 'static,
    C: Clock,
{
    async fn handle(
        &self,
        endpoint: &ValidatedWebhookEndpoint,
        body: Bytes,
        idempotency_key: Option<&[u8]>,
    ) -> Result<WebhookIngressOutcome> {
        let now = self.clock.now();
        let data: serde_json::Value = serde_json::from_slice(&body)?;
        let payload_json = serde_json::to_string(&serde_json::json!({
            "type": "webhook",
            "source": endpoint.name,
            "received_at": now.to_rfc3339_utc()?,
            "data": data,
        }))?;
        let idempotency = match idempotency_key {
            Some(key) => WebhookIdempotency::Explicit(Sha256::digest(key).into()),
            None => WebhookIdempotency::Automatic(Sha256::digest(&body).into()),
        };
        let outcome = self
            .store
            .ingest_webhook(
                NewWebhookEvent {
                    endpoint: endpoint.name.clone(),
                    actor_id: endpoint.actor_id.clone(),
                    idempotency,
                    payload_json,
                },
                now,
            )
            .await?;
        let mut log =
            RuntimeLogEvent::transition(RuntimeComponent::Webhook, RuntimeTransition::Accepted);
        log.actor_id = Some(endpoint.actor_id.clone());
        log.webhook_endpoint = Some(endpoint.name.clone());
        match &outcome {
            WebhookIngressOutcome::Accepted {
                event_id: _,
                work_item_id,
                route_snapshotted,
                ..
            } => {
                log.work_item_id = Some(work_item_id.clone());
                log.duplicate = Some(false);
                log.route_snapshotted = Some(*route_snapshotted);
                let _ = self.logger.log(&log);
            }
            WebhookIngressOutcome::Duplicate { .. } => {
                log.duplicate = Some(true);
                let _ = self.logger.log(&log);
            }
            WebhookIngressOutcome::ActorUnavailable => {}
        }
        if let WebhookIngressOutcome::Accepted { sequence, .. } = &outcome {
            self.signals.notify(&endpoint.actor_id, *sequence).await;
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;
    use axum::body::Bytes;

    use super::{WebhookIngress, WebhookIngressService};
    use crate::{
        config::ValidatedWebhookEndpoint,
        runtime::{
            model::{ActorId, EventId, ManualClock, Timestamp, WorkItemId},
            signals::ActorSignals,
            store::{NewWebhookEvent, WebhookIngressOutcome, WebhookIngressStore},
        },
    };

    #[derive(Clone, Default)]
    struct RecordingStore(Arc<Mutex<Option<NewWebhookEvent>>>);

    #[async_trait]
    impl WebhookIngressStore for RecordingStore {
        async fn ingest_webhook(
            &self,
            event: NewWebhookEvent,
            _now: Timestamp,
        ) -> Result<WebhookIngressOutcome> {
            *self.0.lock().unwrap() = Some(event);
            Ok(WebhookIngressOutcome::Accepted {
                event_id: EventId::new(),
                work_item_id: WorkItemId::new(),
                sequence: 1,
                route_snapshotted: false,
            })
        }
    }

    #[tokio::test]
    async fn builds_trusted_envelope_and_persists_exact_key_hash() -> Result<()> {
        let store = RecordingStore::default();
        let actor = ActorId::from_string("owner");
        let service = WebhookIngressService::new(
            store.clone(),
            ActorSignals::default(),
            ManualClock::new(1),
            Arc::new(crate::runtime::observability::NoopRuntimeLogger),
        );
        let endpoint = ValidatedWebhookEndpoint {
            name: "grafana".into(),
            path: "/webhooks/grafana".into(),
            token: "secret".into(),
            actor_id: actor,
        };
        let outcome = service
            .handle(
                &endpoint,
                Bytes::from_static(br#"{"status":"firing"}"#),
                Some(b"event-1"),
            )
            .await?;
        assert!(matches!(outcome, WebhookIngressOutcome::Accepted { .. }));
        let command = store.0.lock().unwrap().clone().unwrap();
        let payload: serde_json::Value = serde_json::from_str(&command.payload_json)?;
        assert_eq!(payload["type"], "webhook");
        assert_eq!(payload["source"], "grafana");
        assert_eq!(payload["received_at"], "1970-01-01T00:00:00.001Z");
        assert_eq!(payload["data"]["status"], "firing");
        assert!(!payload.to_string().contains("event-1"));
        Ok(())
    }
}
