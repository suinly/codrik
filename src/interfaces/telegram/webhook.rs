use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
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

use crate::{
    interfaces::telegram::types::{TelegramInbound, TelegramUpdate},
    runtime::{
        gateway::{GatewayCommandKey, NewGatewayDelivery},
        identity_link::{IdentityLinkManager, LinkRedemption},
        model::{ActorId, Clock},
        signals::ActorSignals,
        store::{
            ActorStore, GatewayDeliveryStore, IngressOutcome, IngressStore, NewInboundEvent,
            OutboxPayload,
        },
    },
};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramWebhookOutcome {
    Accepted { actor_id: ActorId, sequence: i64 },
    Duplicate,
    CommandHandled,
    Unsupported,
}

#[async_trait]
pub trait TelegramIngress: Send + Sync + 'static {
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramWebhookOutcome>;
}

pub struct TelegramIngressService<S, C> {
    store: S,
    linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    bot_id: String,
    bot_username: String,
    clock: C,
}

impl<S, C> TelegramIngressService<S, C>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone,
    C: Clock,
{
    pub fn new(
        store: S,
        linking: Arc<dyn IdentityLinkManager>,
        signals: ActorSignals,
        bot_id: impl Into<String>,
        bot_username: impl Into<String>,
        clock: C,
    ) -> Result<Self> {
        let bot_id = bot_id.into();
        let bot_username = bot_username.into();
        if bot_id.trim().is_empty() || bot_username.trim().is_empty() {
            bail!("Telegram bot identity must not be blank");
        }
        Ok(Self {
            store,
            linking,
            signals,
            bot_id,
            bot_username,
            clock,
        })
    }

    async fn enqueue_response(
        &self,
        update_id: i64,
        route: crate::runtime::gateway::DeliveryRoute,
        text: &str,
    ) -> Result<()> {
        self.store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    format!("gateway-response:telegram:{}:{update_id}", self.bot_id),
                    None,
                    0,
                    route,
                    OutboxPayload::Text { text: text.into() },
                )?,
                self.clock.now(),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<S, C> TelegramIngress for TelegramIngressService<S, C>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone + Send + Sync + 'static,
    C: Clock,
{
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramWebhookOutcome> {
        let update_id = update.update_id;
        match update.classify(&self.bot_id, &self.bot_username)? {
            TelegramInbound::Unsupported => Ok(TelegramWebhookOutcome::Unsupported),
            TelegramInbound::Link {
                code,
                identity,
                route,
            } => {
                let text = if let Some(code) = code {
                    match self
                        .linking
                        .redeem_code_once(
                            GatewayCommandKey {
                                gateway: format!("telegram:{}", self.bot_id),
                                external_id: update_id.to_string(),
                            },
                            identity,
                            &code,
                        )
                        .await?
                    {
                        LinkRedemption::Linked { .. } => "This channel is now linked.",
                        LinkRedemption::AlreadyLinked { .. } => "This channel was already linked.",
                        LinkRedemption::InvalidOrExpired => "Invalid or expired link code.",
                        LinkRedemption::RateLimited { .. } => {
                            "Too many failed attempts. Try again later."
                        }
                        LinkRedemption::IdentityConflict => {
                            "This channel is already linked to another actor."
                        }
                    }
                } else {
                    "This channel is not linked. Run `codrik link`, then send `/link CODE` here."
                };
                self.enqueue_response(update_id, route, text).await?;
                Ok(TelegramWebhookOutcome::CommandHandled)
            }
            TelegramInbound::Text {
                text,
                identity,
                route,
            } => {
                let Some(actor) = self
                    .store
                    .resolve_identity(&identity.provider, &identity.subject)
                    .await?
                else {
                    self.enqueue_response(
                        update_id,
                        route,
                        "This channel is not linked. Run `codrik link`, then send `/link CODE` here.",
                    )
                    .await?;
                    return Ok(TelegramWebhookOutcome::CommandHandled);
                };
                match self
                    .store
                    .ingest(
                        NewInboundEvent::text_with_route(
                            format!("telegram:{}", self.bot_id),
                            update_id.to_string(),
                            identity.provider,
                            identity.subject,
                            crate::runtime::model::Audience::ActorPrivate,
                            route,
                            text,
                        )?,
                        self.clock.now(),
                    )
                    .await?
                {
                    IngressOutcome::Accepted { sequence, .. } => {
                        self.signals.notify(&actor.id, sequence).await;
                        Ok(TelegramWebhookOutcome::Accepted {
                            actor_id: actor.id,
                            sequence,
                        })
                    }
                    IngressOutcome::Duplicate { .. } => Ok(TelegramWebhookOutcome::Duplicate),
                    IngressOutcome::Unauthorized => {
                        bail!("Telegram identity became unauthorized during ingress")
                    }
                }
            }
        }
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

    use super::{SecretToken, TelegramIngress, TelegramWebhookOutcome, TelegramWebhookServer};
    use crate::interfaces::telegram::types::TelegramUpdate;
    use crate::runtime::{
        identity_link::{IdentityLinkManager, IdentityLinkService, SystemLinkCodeGenerator},
        model::{ActorId, ManualClock, Timestamp},
        signals::ActorSignals,
        sqlite::SqliteRuntimeStore,
        store::{ActorStore, GatewayDeliveryStore, OutboxPayload},
    };

    struct UnsupportedIngress;

    #[async_trait]
    impl TelegramIngress for UnsupportedIngress {
        async fn handle(&self, _update: TelegramUpdate) -> Result<TelegramWebhookOutcome> {
            Ok(TelegramWebhookOutcome::Unsupported)
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

    #[tokio::test]
    async fn link_command_uses_idempotent_core_and_enqueues_response_without_agent_work()
    -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("owner");
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let manager: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let issued = manager.issue_code(&actor).await?;
        let ingress = super::TelegramIngressService::new(
            store.clone(),
            manager,
            ActorSignals::default(),
            "900",
            "codrik_bot",
            ManualClock::new(20),
        )?;
        let update: TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 42,
            "message": {
                "message_id": 7,
                "from": {"id": 100, "is_bot": false, "username": "owner"},
                "chat": {"id": 100, "type": "private"},
                "text": format!("/link {}", issued.code)
            }
        }))?;
        assert_eq!(
            ingress.handle(update).await?,
            TelegramWebhookOutcome::CommandHandled
        );
        assert_eq!(
            store
                .resolve_identity("telegram:900", "100")
                .await?
                .unwrap()
                .id,
            actor
        );
        let deliveries = store
            .claim_gateway_deliveries("telegram:900", "test", Timestamp(21), Timestamp(51), 10)
            .await?;
        assert_eq!(deliveries.len(), 1);
        assert_eq!(
            deliveries[0].payload,
            OutboxPayload::Text {
                text: "This channel is now linked.".into()
            }
        );
        Ok(())
    }
}
