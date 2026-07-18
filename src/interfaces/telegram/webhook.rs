use std::sync::Arc;

use anyhow::{Result, bail};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
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

use crate::interfaces::telegram::{ingress::TelegramIngress, types::TelegramUpdate};

const MAX_WEBHOOK_BODY_BYTES: usize = 1024 * 1024;
const MAX_WEBHOOK_CONCURRENCY: usize = 64;

pub struct SecretToken(Vec<u8>);

impl SecretToken {
    pub fn new(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }

    pub fn matches(&self, candidate: &[u8]) -> bool {
        candidate.len() == self.0.len() && bool::from(self.0.as_slice().ct_eq(candidate))
    }
}

struct WebhookState<I> {
    secret: SecretToken,
    ingress: Arc<I>,
    permits: Arc<Semaphore>,
}

pub struct TelegramWebhookServer<I> {
    listener: TcpListener,
    path: String,
    state: Arc<WebhookState<I>>,
}

impl<I> TelegramWebhookServer<I>
where
    I: TelegramIngress,
{
    pub fn new(
        listener: TcpListener,
        path: impl Into<String>,
        secret: SecretToken,
        ingress: Arc<I>,
    ) -> Result<Self> {
        let path = path.into();
        if !path.starts_with('/') || path.contains('?') || path.contains('#') {
            bail!("Telegram webhook path must be an absolute path");
        }
        Ok(Self {
            listener,
            path,
            state: Arc::new(WebhookState {
                secret,
                ingress,
                permits: Arc::new(Semaphore::new(MAX_WEBHOOK_CONCURRENCY)),
            }),
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let router = Router::new()
            .route(&self.path, post(handle_webhook::<I>))
            .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_BYTES))
            .layer(middleware::from_fn_with_state(
                self.state.clone(),
                limit_webhook_concurrency::<I>,
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

async fn limit_webhook_concurrency<I>(
    State(state): State<Arc<WebhookState<I>>>,
    request: Request<Body>,
    next: Next,
) -> Response
where
    I: TelegramIngress,
{
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    next.run(request).await
}

async fn handle_webhook<I>(
    State(state): State<Arc<WebhookState<I>>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode
where
    I: TelegramIngress,
{
    let Some(candidate) = headers
        .get("x-telegram-bot-api-secret-token")
        .map(|value| value.as_bytes())
    else {
        return StatusCode::UNAUTHORIZED;
    };
    if !state.secret.matches(candidate) {
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
    let update = match serde_json::from_slice::<TelegramUpdate>(&body) {
        Ok(update) => update,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    match state.ingress.handle(update).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::{net::TcpListener, sync::watch};

    use super::{SecretToken, TelegramWebhookServer};
    use crate::interfaces::telegram::{
        ingress::{TelegramIngress, TelegramIngressOutcome},
        types::TelegramUpdate,
    };

    struct UnsupportedIngress;

    #[async_trait]
    impl TelegramIngress for UnsupportedIngress {
        async fn handle(&self, _update: TelegramUpdate) -> Result<TelegramIngressOutcome> {
            Ok(TelegramIngressOutcome::Unsupported)
        }
    }

    #[test]
    fn secret_token_matches_exact_bytes() {
        let secret = SecretToken::new("abc_DEF-123");
        assert!(secret.matches(b"abc_DEF-123"));
        assert!(!secret.matches(b"abc_DEF-124"));
        assert!(!secret.matches(b"abc_DEF-1234"));
    }

    #[tokio::test]
    async fn webhook_requires_secret_and_accepts_valid_unsupported_update() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = TelegramWebhookServer::new(
            listener,
            "/webhooks/telegram",
            SecretToken::new("secret"),
            Arc::new(UnsupportedIngress),
        )?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.run(shutdown_rx));
        let client = reqwest::Client::new();

        let unauthorized = client
            .post(format!("http://{address}/webhooks/telegram"))
            .header("content-type", "application/json")
            .body(r#"{"update_id":1}"#)
            .send()
            .await?;
        assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

        let accepted = client
            .post(format!("http://{address}/webhooks/telegram"))
            .header("content-type", "application/json")
            .header("x-telegram-bot-api-secret-token", "secret")
            .body(r#"{"update_id":1}"#)
            .send()
            .await?;
        assert_eq!(accepted.status(), reqwest::StatusCode::OK);

        shutdown_tx.send_replace(true);
        task.await??;
        Ok(())
    }
}
