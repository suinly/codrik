use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Result, bail};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode},
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

struct BearerToken {
    bytes: [u8; 256],
    len: usize,
}

impl BearerToken {
    fn new(value: &str) -> Self {
        let mut bytes = [0; 256];
        for (index, destination) in bytes.iter_mut().enumerate() {
            *destination = value.as_bytes().get(index).copied().unwrap_or_default();
        }
        Self {
            bytes,
            len: value.len(),
        }
    }

    fn matches(&self, candidate: &[u8]) -> bool {
        let mut bytes = [0; 256];
        for (index, destination) in bytes.iter_mut().enumerate() {
            *destination = candidate.get(index).copied().unwrap_or_default();
        }
        bool::from(self.bytes.ct_eq(&bytes) & self.len.ct_eq(&candidate.len()))
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
    request: Request<Body>,
) -> Response
where
    I: WebhookIngress,
{
    let Some(endpoint) = state.endpoints.get(request.uri().path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(candidate) = request
        .headers()
        .get("authorization")
        .map(|value| value.as_bytes())
        .and_then(|value| value.strip_prefix(b"Bearer "))
        .filter(|value| !value.is_empty())
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !endpoint.token.matches(candidate) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let idempotency_key = match request.headers().get("idempotency-key") {
        Some(value)
            if value.as_bytes().is_empty()
                || value.as_bytes().len() > 256
                || !value.as_bytes().iter().all(u8::is_ascii_graphic) =>
        {
            return StatusCode::BAD_REQUEST.into_response();
        }
        Some(value) => Some(value.as_bytes()),
        None => None,
    };
    let idempotency_key = idempotency_key.map(<[u8]>::to_vec);
    let body = match to_bytes(request.into_body(), MAX_WEBHOOK_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    if serde_json::from_slice::<Box<serde_json::value::RawValue>>(&body).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match state
        .ingress
        .handle(&endpoint.config, body, idempotency_key.as_deref())
        .await
    {
        Ok(WebhookIngressOutcome::Accepted { .. } | WebhookIngressOutcome::Duplicate { .. }) => {
            StatusCode::ACCEPTED.into_response()
        }
        Ok(WebhookIngressOutcome::ActorUnavailable) | Err(_) => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use axum::body::Bytes;
    use tokio::{
        net::TcpListener,
        sync::{Notify, watch},
        time::{Duration, timeout},
    };

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

    #[derive(Default)]
    struct RecordingIngress(Mutex<Vec<Bytes>>);

    #[async_trait]
    impl WebhookIngress for RecordingIngress {
        async fn handle(
            &self,
            _endpoint: &ValidatedWebhookEndpoint,
            body: Bytes,
            _idempotency_key: Option<&[u8]>,
        ) -> Result<WebhookIngressOutcome> {
            self.0.lock().unwrap().push(body);
            Ok(WebhookIngressOutcome::Accepted {
                event_id: EventId::new(),
                work_item_id: WorkItemId::new(),
                sequence: 1,
                route_snapshotted: false,
            })
        }
    }

    struct BlockingIngress {
        active: AtomicUsize,
        entered: Notify,
        release: Notify,
    }

    #[async_trait]
    impl WebhookIngress for BlockingIngress {
        async fn handle(
            &self,
            _endpoint: &ValidatedWebhookEndpoint,
            _body: Bytes,
            _idempotency_key: Option<&[u8]>,
        ) -> Result<WebhookIngressOutcome> {
            self.active.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_waiters();
            self.release.notified().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
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

    async fn spawn_with<I: WebhookIngress>(
        ingress: Arc<I>,
    ) -> Result<(
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
        let server = WebhookServer::new(listener, vec![endpoint], ingress)?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.run(shutdown_rx));
        Ok((format!("http://{address}"), shutdown_tx, task))
    }

    async fn post(
        client: &reqwest::Client,
        base: &str,
        body: impl Into<reqwest::Body>,
    ) -> reqwest::Response {
        client
            .post(format!("{base}/webhooks/grafana"))
            .header("authorization", "Bearer secret")
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap()
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

    #[tokio::test]
    async fn body_limit_accepts_exact_mib_and_rejects_overflow_bodyless_after_auth() -> Result<()> {
        let (base, shutdown, task) = spawn().await?;
        let client = reqwest::Client::new();
        let exact = format!("\"{}\"", "x".repeat(super::MAX_WEBHOOK_BODY_BYTES - 2));
        let accepted = post(&client, &base, exact).await;
        assert_eq!(accepted.status(), reqwest::StatusCode::ACCEPTED);
        assert!(accepted.bytes().await?.is_empty());

        let overflow = format!("\"{}\"", "x".repeat(super::MAX_WEBHOOK_BODY_BYTES - 1));
        let rejected = post(&client, &base, overflow).await;
        assert_eq!(rejected.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
        assert!(rejected.bytes().await?.is_empty());

        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn unauthenticated_oversized_body_is_bodyless_unauthorized() -> Result<()> {
        let (base, shutdown, task) = spawn().await?;
        let response = reqwest::Client::new()
            .post(format!("{base}/webhooks/grafana"))
            .header("content-type", "application/json")
            .body("x".repeat(super::MAX_WEBHOOK_BODY_BYTES + 1))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert!(response.bytes().await?.is_empty());
        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn accepts_every_json_shape_including_unbounded_numbers() -> Result<()> {
        let ingress = Arc::new(RecordingIngress::default());
        let (base, shutdown, task) = spawn_with(ingress.clone()).await?;
        let client = reqwest::Client::new();
        for json in [
            "null",
            "true",
            "false",
            "\"text\"",
            "0",
            "1e400",
            "[]",
            "{}",
            "[null,true,1e400]",
            "{\"huge\":1e400}",
        ] {
            assert_eq!(
                post(&client, &base, json).await.status(),
                reqwest::StatusCode::ACCEPTED,
                "rejected {json}"
            );
        }
        assert_eq!(ingress.0.lock().unwrap().len(), 10);
        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn durable_failure_is_bodyless_service_unavailable() -> Result<()> {
        struct FailingIngress;
        #[async_trait]
        impl WebhookIngress for FailingIngress {
            async fn handle(
                &self,
                _: &ValidatedWebhookEndpoint,
                _: Bytes,
                _: Option<&[u8]>,
            ) -> Result<WebhookIngressOutcome> {
                anyhow::bail!("database unavailable")
            }
        }
        let (base, shutdown, task) = spawn_with(Arc::new(FailingIngress)).await?;
        let response = post(&reqwest::Client::new(), &base, "{}").await;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.bytes().await?.is_empty());
        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn sixty_fifth_request_is_rejected_and_released_permit_is_reused() -> Result<()> {
        let ingress = Arc::new(BlockingIngress {
            active: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let (base, shutdown, task) = spawn_with(ingress.clone()).await?;
        let client = reqwest::Client::new();
        let mut requests = tokio::task::JoinSet::new();
        for _ in 0..super::MAX_WEBHOOK_CONCURRENCY {
            let client = client.clone();
            let base = base.clone();
            requests.spawn(async move { post(&client, &base, "{}").await });
        }
        timeout(Duration::from_secs(5), async {
            while ingress.active.load(Ordering::SeqCst) != super::MAX_WEBHOOK_CONCURRENCY {
                ingress.entered.notified().await;
            }
        })
        .await?;
        let saturated = post(&client, &base, "{}").await;
        assert_eq!(saturated.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert!(saturated.bytes().await?.is_empty());

        ingress.release.notify_one();
        assert_eq!(
            requests.join_next().await.unwrap()?.status(),
            reqwest::StatusCode::ACCEPTED
        );
        let replacement = tokio::spawn({
            let client = client.clone();
            let base = base.clone();
            async move { post(&client, &base, "{}").await }
        });
        timeout(Duration::from_secs(5), async {
            while ingress.active.load(Ordering::SeqCst) != super::MAX_WEBHOOK_CONCURRENCY {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        ingress.release.notify_waiters();
        assert_eq!(replacement.await?.status(), reqwest::StatusCode::ACCEPTED);
        while let Some(request) = requests.join_next().await {
            assert_eq!(request?.status(), reqwest::StatusCode::ACCEPTED);
        }
        shutdown.send_replace(true);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn graceful_shutdown_waits_for_in_flight_request() -> Result<()> {
        let ingress = Arc::new(BlockingIngress {
            active: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let (base, shutdown, mut task) = spawn_with(ingress.clone()).await?;
        let request = tokio::spawn(async move { post(&reqwest::Client::new(), &base, "{}").await });
        timeout(Duration::from_secs(5), ingress.entered.notified()).await?;
        shutdown.send_replace(true);
        assert!(timeout(Duration::from_millis(50), &mut task).await.is_err());
        ingress.release.notify_one();
        assert_eq!(request.await?.status(), reqwest::StatusCode::ACCEPTED);
        task.await??;
        Ok(())
    }
}
