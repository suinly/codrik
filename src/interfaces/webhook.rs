pub mod ingress;
pub mod server;

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    config::ValidatedWebhookConfig,
    interfaces::webhook::{ingress::WebhookIngressService, server::WebhookServer},
    runtime::{
        model::Clock, observability::RuntimeLogger, signals::ActorSignals,
        store::WebhookIngressStore,
    },
};

pub struct PreparedWebhookGateway<S, C> {
    listener: Mutex<Option<TcpListener>>,
    endpoints: Vec<crate::config::ValidatedWebhookEndpoint>,
    ingress: Arc<WebhookIngressService<S, C>>,
}

impl<S, C> PreparedWebhookGateway<S, C>
where
    S: WebhookIngressStore + Send + Sync + 'static,
    C: Clock,
{
    pub async fn run(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<()> {
        let listener = self
            .listener
            .lock()
            .expect("webhook listener poisoned")
            .take()
            .context("webhook listener was already started")?;
        WebhookServer::new(listener, self.endpoints.clone(), self.ingress.clone())?
            .run(shutdown)
            .await
    }
}

pub async fn prepare<S, C>(
    config: ValidatedWebhookConfig,
    store: S,
    signals: ActorSignals,
    clock: C,
    logger: Arc<dyn RuntimeLogger>,
) -> Result<PreparedWebhookGateway<S, C>>
where
    S: WebhookIngressStore + Send + Sync + 'static,
    C: Clock,
{
    let listener = TcpListener::bind(config.listen)
        .await
        .context("failed to bind generic webhook listener")?;
    Ok(PreparedWebhookGateway {
        listener: Mutex::new(Some(listener)),
        endpoints: config.endpoints,
        ingress: Arc::new(WebhookIngressService::new(store, signals, clock, logger)),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use tokio::{net::TcpListener, sync::watch};

    use super::prepare;
    use crate::{
        config::{ValidatedWebhookConfig, ValidatedWebhookEndpoint},
        runtime::{
            model::{ActorId, ManualClock, Timestamp},
            observability::{RuntimeLogEvent, RuntimeLogger},
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
            store::ActorStore,
        },
    };

    #[derive(Default)]
    struct CapturingLogger(Mutex<Vec<String>>);

    impl RuntimeLogger for CapturingLogger {
        fn log(&self, event: &RuntimeLogEvent) -> Result<()> {
            self.0.lock().unwrap().push(serde_json::to_string(event)?);
            Ok(())
        }
    }

    #[tokio::test]
    async fn prepare_fails_when_listener_is_already_bound() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let config = ValidatedWebhookConfig {
            listen: listener.local_addr()?,
            endpoints: Vec::new(),
        };
        assert!(
            prepare(
                config,
                SqliteRuntimeStore::open_in_memory().await?,
                ActorSignals::default(),
                ManualClock::new(1),
                Arc::new(crate::runtime::observability::NoopRuntimeLogger),
            )
            .await
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn durable_ingress_logs_safe_coordinates_without_request_secrets() -> Result<()> {
        let token = "fake-bearer-token-marker";
        let key = "fake-idempotency-key-marker";
        let payload = "fake-payload-marker";
        let unknown_path = "/webhooks/fake-unknown-path-marker";
        let rejected_auth = "fake-rejected-auth-marker";
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("owner");
        store
            .ensure_initial_actor(&actor, &[], Timestamp(0))
            .await?;
        let address = {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            drop(listener);
            address
        };
        let logger = Arc::new(CapturingLogger::default());
        let gateway = Arc::new(
            prepare(
                ValidatedWebhookConfig {
                    listen: address,
                    endpoints: vec![ValidatedWebhookEndpoint {
                        name: "events".into(),
                        path: "/webhooks/events".into(),
                        token: token.into(),
                        actor_id: actor,
                    }],
                },
                store,
                ActorSignals::default(),
                ManualClock::new(1),
                logger.clone(),
            )
            .await?,
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(gateway.run(shutdown_rx));
        let client = reqwest::Client::new();

        let send = |path: &str, bearer: &str, idempotency_key: Option<&str>| {
            let mut request = client
                .post(format!("http://{address}{path}"))
                .header("authorization", format!("Bearer {bearer}"))
                .header("content-type", "application/json")
                .body(format!(r#"{{"marker":"{payload}"}}"#));
            if let Some(key) = idempotency_key {
                request = request.header("idempotency-key", key);
            }
            request
        };
        assert_eq!(
            send("/webhooks/events", token, Some(key))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::ACCEPTED
        );
        assert_eq!(
            send("/webhooks/events", token, Some(key))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::ACCEPTED
        );
        assert_eq!(
            send("/webhooks/events", rejected_auth, None)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            send(unknown_path, token, None).send().await?.status(),
            reqwest::StatusCode::NOT_FOUND
        );

        shutdown_tx.send_replace(true);
        task.await??;
        let logs = logger.0.lock().unwrap();
        assert_eq!(logs.len(), 2);
        assert!(logs[0].contains(r#""webhook_endpoint":"events""#));
        assert!(logs[0].contains(r#""duplicate":false"#));
        assert!(logs[0].contains(r#""route_snapshotted":false"#));
        assert!(logs[0].contains("work_item_id"));
        assert!(logs[1].contains(r#""webhook_endpoint":"events""#));
        assert!(logs[1].contains(r#""duplicate":true"#));
        assert!(!logs[1].contains("work_item_id"));
        let serialized = logs.join("\n");
        for forbidden in [token, key, payload, unknown_path, rejected_auth] {
            assert!(!serialized.contains(forbidden));
        }
        Ok(())
    }
}
