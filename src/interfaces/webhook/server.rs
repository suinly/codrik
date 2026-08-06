use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Result, bail};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, OriginalUri, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use subtle::ConstantTimeEq;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
};

use crate::{
    config::ValidatedWebhookEndpoint, interfaces::webhook::ingress::WebhookIngress,
    runtime::store::WebhookIngressOutcome,
};

const MAX_WEBHOOK_BODY_BYTES: usize = 1024 * 1024;
const MAX_WEBHOOK_CONCURRENCY: usize = 64;

struct BearerToken(Vec<u8>);

impl BearerToken {
    fn new(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }

    fn matches(&self, candidate: &[u8]) -> bool {
        candidate.len() == self.0.len() && bool::from(self.0.as_slice().ct_eq(candidate))
    }
}

struct PreparedEndpoint {
    config: ValidatedWebhookEndpoint,
    token: BearerToken,
}

struct ServerState<I> {
    endpoints: BTreeMap<String, PreparedEndpoint>,
    ingress: Arc<I>,
    permits: Arc<Semaphore>,
}

pub struct WebhookServer<I> {
    listener: TcpListener,
    state: Arc<ServerState<I>>,
}

impl<I> WebhookServer<I>
where
    I: WebhookIngress,
{
    pub fn new(
        listener: TcpListener,
        endpoints: Vec<ValidatedWebhookEndpoint>,
        ingress: Arc<I>,
    ) -> Result<Self> {
        let mut prepared = BTreeMap::new();
        for endpoint in endpoints {
            if prepared.contains_key(&endpoint.path) {
                bail!("webhook endpoint paths must be unique");
            }
            prepared.insert(
                endpoint.path.clone(),
                PreparedEndpoint {
                    token: BearerToken::new(&endpoint.token),
                    config: endpoint,
                },
            );
        }
        Ok(Self {
            listener,
            state: Arc::new(ServerState {
                endpoints: prepared,
                ingress,
                permits: Arc::new(Semaphore::new(MAX_WEBHOOK_CONCURRENCY)),
            }),
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut router = Router::new();
        for path in self.state.endpoints.keys() {
            router = router.route(path, post(handle_webhook::<I>));
        }
        let router = router
            .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_BYTES))
            .layer(middleware::from_fn_with_state(
                self.state.clone(),
                limit_concurrency::<I>,
            ))
            .with_state(self.state);
        axum::serve(self.listener, router)
            .with_graceful_shutdown(async move {
                while !*shutdown.borrow() {
                    if shutdown.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await?;
        Ok(())
    }
}

async fn limit_concurrency<I>(
    State(state): State<Arc<ServerState<I>>>,
    request: Request<Body>,
    next: Next,
) -> Response
where
    I: WebhookIngress,
{
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    next.run(request).await
}

async fn handle_webhook<I>(
    State(state): State<Arc<ServerState<I>>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode
where
    I: WebhookIngress,
{
    let Some(endpoint) = state.endpoints.get(uri.path()) else {
        return StatusCode::NOT_FOUND;
    };
    let Some(candidate) = headers
        .get("authorization")
        .map(|value| value.as_bytes())
        .and_then(|value| value.strip_prefix(b"Bearer "))
        .filter(|value| !value.is_empty())
    else {
        return StatusCode::UNAUTHORIZED;
    };
    if !endpoint.token.matches(candidate) {
        return StatusCode::UNAUTHORIZED;
    }
    let content_type = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE;
    }
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return StatusCode::BAD_REQUEST;
    }
    let idempotency_key = match headers.get("idempotency-key") {
        Some(value)
            if value.as_bytes().is_empty()
                || value.as_bytes().len() > 256
                || !value.as_bytes().iter().all(u8::is_ascii_graphic) =>
        {
            return StatusCode::BAD_REQUEST;
        }
        Some(value) => Some(value.as_bytes()),
        None => None,
    };
    match state
        .ingress
        .handle(&endpoint.config, body, idempotency_key)
        .await
    {
        Ok(WebhookIngressOutcome::Accepted { .. } | WebhookIngressOutcome::Duplicate { .. }) => {
            StatusCode::ACCEPTED
        }
        Ok(WebhookIngressOutcome::ActorUnavailable) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;
    use axum::body::Bytes;
    use tokio::{net::TcpListener, sync::watch};

    use super::WebhookServer;
    use crate::{
        config::ValidatedWebhookEndpoint,
        interfaces::webhook::ingress::WebhookIngress,
        runtime::{
            model::{ActorId, EventId, WorkItemId},
            store::WebhookIngressOutcome,
        },
    };

    struct AcceptingIngress;

    #[async_trait]
    impl WebhookIngress for AcceptingIngress {
        async fn handle(
            &self,
            _endpoint: &ValidatedWebhookEndpoint,
            _body: Bytes,
            _idempotency_key: Option<&[u8]>,
        ) -> Result<WebhookIngressOutcome> {
            Ok(WebhookIngressOutcome::Accepted {
                event_id: EventId::new(),
                work_item_id: WorkItemId::new(),
                sequence: 1,
                route_snapshotted: false,
            })
        }
    }

    async fn spawn() -> Result<(
        String,
        watch::Sender<bool>,
        tokio::task::JoinHandle<Result<()>>,
    )> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let endpoint = ValidatedWebhookEndpoint {
            name: "grafana".into(),
            path: "/webhooks/grafana".into(),
            token: "secret".into(),
            actor_id: ActorId::from_string("owner"),
        };
        let server = WebhookServer::new(listener, vec![endpoint], Arc::new(AcceptingIngress))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.run(shutdown_rx));
        Ok((format!("http://{address}"), shutdown_tx, task))
    }

    #[tokio::test]
    async fn authenticates_before_json_parsing_and_returns_bodyless_accepted() -> Result<()> {
        let (base, shutdown, task) = spawn().await?;
        let client = reqwest::Client::new();
        let unauthorized = client
            .post(format!("{base}/webhooks/grafana"))
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await?;
        assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert!(unauthorized.bytes().await?.is_empty());

        let accepted = client
            .post(format!("{base}/webhooks/grafana"))
            .header("authorization", "Bearer secret")
            .header("content-type", "application/json; charset=utf-8")
            .body(r#"{"status":"firing"}"#)
            .send()
            .await?;
        assert_eq!(accepted.status(), reqwest::StatusCode::ACCEPTED);
        assert!(accepted.bytes().await?.is_empty());
        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn exact_paths_and_methods_use_standard_statuses() -> Result<()> {
        let (base, shutdown, task) = spawn().await?;
        let client = reqwest::Client::new();
        assert_eq!(
            client
                .get(format!("{base}/webhooks/grafana"))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            client
                .post(format!("{base}/missing"))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_media_json_and_idempotency_key() -> Result<()> {
        let (base, shutdown, task) = spawn().await?;
        let client = reqwest::Client::new();
        for (content_type, body, key, expected) in [
            (
                "text/plain",
                "{}",
                None,
                reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                "application/json",
                "{",
                None,
                reqwest::StatusCode::BAD_REQUEST,
            ),
            (
                "application/json",
                "{}",
                Some("bad key"),
                reqwest::StatusCode::BAD_REQUEST,
            ),
        ] {
            let mut request = client
                .post(format!("{base}/webhooks/grafana"))
                .header("authorization", "Bearer secret")
                .header("content-type", content_type)
                .body(body);
            if let Some(key) = key {
                request = request.header("idempotency-key", key);
            }
            assert_eq!(request.send().await?.status(), expected);
        }
        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }
}
