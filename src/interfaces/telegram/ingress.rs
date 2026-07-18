use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramIngressOutcome {
    Accepted { actor_id: ActorId, sequence: i64 },
    Duplicate,
    CommandHandled,
    Unsupported,
}

#[async_trait]
pub trait TelegramIngress: Send + Sync + 'static {
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramIngressOutcome>;
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
    async fn handle(&self, update: TelegramUpdate) -> Result<TelegramIngressOutcome> {
        let update_id = update.update_id;
        match update.classify(&self.bot_id, &self.bot_username)? {
            TelegramInbound::Unsupported => Ok(TelegramIngressOutcome::Unsupported),
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
                Ok(TelegramIngressOutcome::CommandHandled)
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
                    return Ok(TelegramIngressOutcome::CommandHandled);
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
                        Ok(TelegramIngressOutcome::Accepted {
                            actor_id: actor.id,
                            sequence,
                        })
                    }
                    IngressOutcome::Duplicate { .. } => Ok(TelegramIngressOutcome::Duplicate),
                    IngressOutcome::Unauthorized => {
                        bail!("Telegram identity became unauthorized during ingress")
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;

    use super::{TelegramIngress, TelegramIngressOutcome, TelegramIngressService};
    use crate::{
        interfaces::telegram::types::TelegramUpdate,
        runtime::{
            identity_link::{IdentityLinkManager, IdentityLinkService, SystemLinkCodeGenerator},
            model::{ActorId, ManualClock, Timestamp},
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
            store::{ActorStore, GatewayDeliveryStore, OutboxPayload},
        },
    };

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
        let ingress = TelegramIngressService::new(
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
            TelegramIngressOutcome::CommandHandled
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
