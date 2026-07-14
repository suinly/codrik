use anyhow::Result;
use async_trait::async_trait;

use crate::{agent::message::Message, auth::LegacyAuthorizationSnapshot, runtime::model::*};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeActor {
    pub id: ActorId,
    pub enabled: bool,
    pub tools: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportOutcome {
    Imported,
    AlreadyImported,
}

#[async_trait]
pub trait RuntimeAuthorizationStore: Send + Sync {
    async fn import_legacy_authorization(
        &self,
        snapshot: LegacyAuthorizationSnapshot,
        now: Timestamp,
    ) -> Result<ImportOutcome>;

    async fn resolve_identity(&self, provider: &str, subject: &str)
    -> Result<Option<RuntimeActor>>;
}

#[derive(Clone, Debug)]
pub struct NewInboundEvent {
    pub gateway: String,
    pub external_id: String,
    pub identity_provider: String,
    pub identity_subject: String,
    pub kind: EventKind,
    pub audience: Audience,
    pub payload_json: String,
}

impl NewInboundEvent {
    pub fn text(
        gateway: impl Into<String>,
        external_id: impl Into<String>,
        identity_provider: impl Into<String>,
        identity_subject: impl Into<String>,
        audience: Audience,
        text: impl Into<String>,
    ) -> Result<Self> {
        let payload_json = serde_json::to_string(&serde_json::json!({
            "type": "text",
            "text": text.into(),
        }))?;
        Ok(Self {
            gateway: gateway.into(),
            external_id: external_id.into(),
            identity_provider: identity_provider.into(),
            identity_subject: identity_subject.into(),
            kind: EventKind::UserMessage,
            audience,
            payload_json,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngressOutcome {
    Accepted {
        event_id: EventId,
        work_item_id: WorkItemId,
        sequence: i64,
    },
    Duplicate {
        event_id: EventId,
        sequence: i64,
    },
    Unauthorized,
}

impl IngressOutcome {
    pub fn sequence(&self) -> Option<i64> {
        match self {
            Self::Accepted { sequence, .. } | Self::Duplicate { sequence, .. } => Some(*sequence),
            Self::Unauthorized => None,
        }
    }

    pub fn work_item_id(&self) -> Option<&WorkItemId> {
        match self {
            Self::Accepted { work_item_id, .. } => Some(work_item_id),
            Self::Duplicate { .. } | Self::Unauthorized => None,
        }
    }
}

#[async_trait]
pub trait IngressStore: Send + Sync {
    async fn ingest(&self, event: NewInboundEvent, now: Timestamp) -> Result<IngressOutcome>;
}

#[derive(Clone, Debug)]
pub struct ActorLease {
    pub actor_id: ActorId,
    pub owner_id: String,
    pub generation: i64,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug)]
pub struct AttachedRun {
    pub lease: ActorLease,
    pub work_item_id: WorkItemId,
    pub run_id: RunId,
    pub observed_sequence: i64,
    pub source_event_ids: Vec<EventId>,
    pub audience: Audience,
    pub messages: Vec<Message>,
}
