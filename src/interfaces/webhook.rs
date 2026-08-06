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
    use std::sync::Arc;

    use anyhow::Result;
    use tokio::net::TcpListener;

    use super::prepare;
    use crate::{
        config::ValidatedWebhookConfig,
        runtime::{model::ManualClock, signals::ActorSignals, sqlite::SqliteRuntimeStore},
    };

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
}
