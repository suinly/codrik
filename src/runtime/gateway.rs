use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::runtime::{
    model::{ActorId, GatewayDeliveryId, OutboxId, Timestamp, WorkItemId},
    store::OutboxPayload,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRoute {
    pub gateway: String,
    pub address: String,
    pub reply_to_external_id: Option<String>,
    pub max_text_chars: usize,
    pub max_caption_chars: usize,
}

impl DeliveryRoute {
    pub fn new(
        gateway: impl Into<String>,
        address: impl Into<String>,
        reply_to_external_id: Option<String>,
        max_text_chars: usize,
        max_caption_chars: usize,
    ) -> Result<Self> {
        let gateway = gateway.into();
        let address = address.into();
        if gateway.trim().is_empty() {
            bail!("delivery gateway must not be blank");
        }
        if address.trim().is_empty() {
            bail!("delivery address must not be blank");
        }
        if max_text_chars == 0 {
            bail!("delivery text limit must be greater than zero");
        }
        if max_caption_chars == 0 {
            bail!("delivery caption limit must be greater than zero");
        }
        Ok(Self {
            gateway,
            address,
            reply_to_external_id,
            max_text_chars,
            max_caption_chars,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GatewayCommandKey {
    pub gateway: String,
    pub external_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayCommandOutcome {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayDeliveryState {
    Pending,
    Delivering,
    Delivered,
    FailedRetryable,
    FailedTerminal,
    OutcomeUnknown,
}

impl GatewayDeliveryState {
    pub fn is_failure_target(self) -> bool {
        matches!(self, Self::FailedTerminal | Self::OutcomeUnknown)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayDeliveryClaim {
    pub id: GatewayDeliveryId,
    pub owner: String,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedGatewayDelivery {
    pub claim: GatewayDeliveryClaim,
    pub intent_key: String,
    pub source_outbox_id: Option<OutboxId>,
    pub work_item_id: Option<WorkItemId>,
    pub ordinal: usize,
    pub route: DeliveryRoute,
    pub payload: OutboxPayload,
    pub attempt_count: usize,
    pub remote_message_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewGatewayDelivery {
    pub intent_key: String,
    pub source_outbox_id: Option<OutboxId>,
    pub ordinal: usize,
    pub route: DeliveryRoute,
    pub payload: OutboxPayload,
}

impl NewGatewayDelivery {
    pub fn new(
        intent_key: impl Into<String>,
        source_outbox_id: Option<OutboxId>,
        ordinal: usize,
        route: DeliveryRoute,
        payload: OutboxPayload,
    ) -> Result<Self> {
        let intent_key = intent_key.into();
        if intent_key.trim().is_empty() {
            bail!("gateway delivery intent key must not be blank");
        }
        Ok(Self {
            intent_key,
            source_outbox_id,
            ordinal,
            route,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{DeliveryRoute, GatewayDeliveryState, NewGatewayDelivery};
    use crate::runtime::store::OutboxPayload;

    #[test]
    fn delivery_route_requires_address_and_positive_limits() -> Result<()> {
        let route = DeliveryRoute::new("telegram:900", "100", Some("7".into()), 4096, 1024)?;
        assert_eq!(route.gateway, "telegram:900");
        assert_eq!(route.address, "100");
        for (gateway, address, text, caption) in [
            ("", "100", 4096, 1024),
            ("telegram:900", "", 4096, 1024),
            ("telegram:900", "100", 0, 1024),
            ("telegram:900", "100", 4096, 0),
        ] {
            assert!(
                DeliveryRoute::new(gateway, address, None, text, caption).is_err(),
                "accepted invalid route"
            );
        }
        Ok(())
    }

    #[test]
    fn new_delivery_requires_nonblank_key() -> Result<()> {
        let route = DeliveryRoute::new("telegram:900", "100", None, 4096, 1024)?;
        assert!(
            NewGatewayDelivery::new(
                " ",
                None,
                0,
                route,
                OutboxPayload::Text {
                    text: "hello".into(),
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn only_failure_states_are_valid_failure_targets() {
        for state in [
            GatewayDeliveryState::FailedTerminal,
            GatewayDeliveryState::OutcomeUnknown,
        ] {
            assert!(state.is_failure_target());
        }
        for state in [
            GatewayDeliveryState::Pending,
            GatewayDeliveryState::Delivering,
            GatewayDeliveryState::Delivered,
            GatewayDeliveryState::FailedRetryable,
        ] {
            assert!(!state.is_failure_target());
        }
    }
}
