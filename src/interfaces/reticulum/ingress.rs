use std::sync::Arc;

use anyhow::{Result, bail};

use crate::runtime::{
    gateway::{DeliveryRoute, GatewayCommandKey, NewGatewayDelivery},
    identity_link::{IdentityLinkManager, LinkRedemption},
    model::{ActorId, Audience, Clock},
    signals::ActorSignals,
    store::{
        ActorStore, GatewayDeliveryStore, IngressOutcome, IngressStore, LinkIdentity,
        NewInboundEvent, OutboxPayload,
    },
};

const MAX_TEXT_CHARS: usize = 256 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub struct InboundMessage {
    pub message_hash: String,
    pub source: String,
    pub timestamp: f64,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReticulumIngressOutcome {
    Accepted { actor_id: ActorId, sequence: i64 },
    Duplicate,
    CommandHandled,
}

pub struct ReticulumIngressService<S, C> {
    store: S,
    linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    local_destination: String,
    clock: C,
}

impl<S, C> ReticulumIngressService<S, C>
where
    S: ActorStore + IngressStore + GatewayDeliveryStore + Clone + Send + Sync + 'static,
    C: Clock,
{
    pub fn new(
        store: S,
        linking: Arc<dyn IdentityLinkManager>,
        signals: ActorSignals,
        local_destination: impl Into<String>,
        clock: C,
    ) -> Result<Self> {
        let local_destination = local_destination.into();
        validate_destination(&local_destination)?;
        Ok(Self {
            store,
            linking,
            signals,
            local_destination,
            clock,
        })
    }

    pub async fn handle(&self, message: InboundMessage) -> Result<ReticulumIngressOutcome> {
        validate_message(&message)?;
        let gateway = format!("reticulum:{}", self.local_destination);
        let identity = LinkIdentity {
            provider: gateway.clone(),
            subject: message.source.clone(),
            username: None,
        };
        let route = DeliveryRoute::new(gateway.clone(), message.source, None, MAX_TEXT_CHARS, 1)?;
        let trimmed = message.text.trim();
        let (command, argument) = split_command(trimmed);
        if command == "/link" {
            let text = if let Some(code) = argument {
                match self
                    .linking
                    .redeem_code_once(
                        GatewayCommandKey {
                            gateway,
                            external_id: message.message_hash.clone(),
                        },
                        identity,
                        code,
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
            self.enqueue_response(&message.message_hash, route, text)
                .await?;
            return Ok(ReticulumIngressOutcome::CommandHandled);
        }
        let Some(actor) = self
            .store
            .resolve_identity(&identity.provider, &identity.subject)
            .await?
        else {
            self.enqueue_response(
                &message.message_hash,
                route,
                "This channel is not linked. Run `codrik link`, then send `/link CODE` here.",
            )
            .await?;
            return Ok(ReticulumIngressOutcome::CommandHandled);
        };
        if !actor.enabled {
            self.enqueue_response(&message.message_hash, route, "This actor is disabled.")
                .await?;
            return Ok(ReticulumIngressOutcome::CommandHandled);
        }
        match self
            .store
            .ingest(
                NewInboundEvent::text_with_route(
                    format!("reticulum:{}", self.local_destination),
                    message.message_hash,
                    identity.provider,
                    identity.subject,
                    Audience::ActorPrivate,
                    route,
                    message.text,
                )?,
                self.clock.now(),
            )
            .await?
        {
            IngressOutcome::Accepted { sequence, .. } => {
                self.signals.notify(&actor.id, sequence).await;
                Ok(ReticulumIngressOutcome::Accepted {
                    actor_id: actor.id,
                    sequence,
                })
            }
            IngressOutcome::Duplicate { .. } => Ok(ReticulumIngressOutcome::Duplicate),
            IngressOutcome::Unauthorized => {
                bail!("Reticulum identity became unauthorized during ingress")
            }
        }
    }

    async fn enqueue_response(
        &self,
        message_hash: &str,
        route: DeliveryRoute,
        text: &str,
    ) -> Result<()> {
        self.store
            .enqueue_gateway_delivery(
                NewGatewayDelivery::new(
                    format!(
                        "gateway-response:reticulum:{}:{message_hash}",
                        self.local_destination
                    ),
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

fn validate_message(message: &InboundMessage) -> Result<()> {
    validate_hex(&message.message_hash, 64)?;
    validate_destination(&message.source)?;
    if !message.timestamp.is_finite()
        || message.timestamp < 0.0
        || message.text.trim().is_empty()
        || message.text.len() > MAX_TEXT_CHARS
    {
        bail!("invalid Reticulum inbound message");
    }
    Ok(())
}

fn validate_destination(destination: &str) -> Result<()> {
    validate_hex(destination, 32)
}

fn validate_hex(value: &str, length: usize) -> Result<()> {
    if value.len() != length
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("invalid Reticulum hash");
    }
    Ok(())
}

fn split_command(text: &str) -> (&str, Option<&str>) {
    match text.find(char::is_whitespace) {
        Some(index) => {
            let argument = text[index..].trim();
            (&text[..index], (!argument.is_empty()).then_some(argument))
        }
        None => (text, None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;

    use super::{InboundMessage, ReticulumIngressOutcome, ReticulumIngressService};
    use crate::runtime::{
        identity_link::{IdentityLinkManager, IdentityLinkService, SystemLinkCodeGenerator},
        model::{ActorId, ManualClock, Timestamp},
        signals::ActorSignals,
        sqlite::SqliteRuntimeStore,
        store::{ActorStore, GatewayDeliveryStore, OutboxPayload},
    };

    const DESTINATION: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SOURCE: &str = "cccccccccccccccccccccccccccccccc";

    fn message(id: char, text: impl Into<String>) -> InboundMessage {
        InboundMessage {
            message_hash: id.to_string().repeat(64),
            source: SOURCE.into(),
            timestamp: 20.0,
            text: text.into(),
        }
    }

    async fn fixture() -> Result<(
        SqliteRuntimeStore,
        ActorId,
        Arc<dyn IdentityLinkManager>,
        ReticulumIngressService<SqliteRuntimeStore, ManualClock>,
    )> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("owner");
        store
            .ensure_initial_actor(&actor, &[], Timestamp(1))
            .await?;
        let linking: Arc<dyn IdentityLinkManager> = Arc::new(IdentityLinkService::new(
            store.clone(),
            ManualClock::new(10),
            SystemLinkCodeGenerator,
        ));
        let ingress = ReticulumIngressService::new(
            store.clone(),
            linking.clone(),
            ActorSignals::default(),
            DESTINATION,
            ManualClock::new(20),
        )?;
        Ok((store, actor, linking, ingress))
    }

    #[tokio::test]
    async fn link_command_links_source_without_agent_work_and_enqueues_reply() -> Result<()> {
        let (store, actor, linking, ingress) = fixture().await?;
        let code = linking.issue_code(&actor).await?.code;
        assert_eq!(
            ingress
                .handle(message('a', format!("/link {code}")))
                .await?,
            ReticulumIngressOutcome::CommandHandled
        );
        assert_eq!(
            store
                .resolve_identity(&format!("reticulum:{DESTINATION}"), SOURCE)
                .await?
                .unwrap()
                .id,
            actor
        );
        let deliveries = store
            .claim_gateway_deliveries(
                &format!("reticulum:{DESTINATION}"),
                "test",
                Timestamp(21),
                Timestamp(51),
                10,
            )
            .await?;
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].route.address, SOURCE);
        assert_eq!(
            deliveries[0].payload,
            OutboxPayload::Text {
                text: "This channel is now linked.".into()
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn linked_text_uses_message_hash_for_durable_deduplication() -> Result<()> {
        let (store, actor, linking, ingress) = fixture().await?;
        let code = linking.issue_code(&actor).await?.code;
        ingress
            .handle(message('a', format!("/link {code}")))
            .await?;
        let inbound = message('b', "hello");
        assert!(matches!(
            ingress.handle(inbound.clone()).await?,
            ReticulumIngressOutcome::Accepted { .. }
        ));
        assert_eq!(
            ingress.handle(inbound).await?,
            ReticulumIngressOutcome::Duplicate
        );
        Ok(())
    }
}
