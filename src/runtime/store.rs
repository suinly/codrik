use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorLease {
    pub actor_id: ActorId,
    pub owner_id: String,
    pub generation: i64,
    pub expires_at: Timestamp,
}

#[derive(Debug)]
pub struct StaleLease;

impl std::fmt::Display for StaleLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stale actor lease")
    }
}

impl std::error::Error for StaleLease {}

#[async_trait]
pub trait DispatchStore: Send + Sync {
    async fn acquire_ready_actor(
        &self,
        owner: &str,
        now: Timestamp,
        lease_until: Timestamp,
    ) -> Result<Option<ActorLease>>;

    async fn renew_lease(
        &self,
        lease: &ActorLease,
        now: Timestamp,
        lease_until: Timestamp,
    ) -> Result<ActorLease>;

    async fn attach_next_run(
        &self,
        lease: &ActorLease,
        max_events: usize,
        now: Timestamp,
    ) -> Result<Option<AttachedRun>>;

    async fn release_lease(&self, lease: &ActorLease) -> Result<()>;
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

#[derive(Clone, Debug)]
pub struct CheckpointRun {
    pub run: AttachedRun,
    pub incorporated_event_ids: Vec<EventId>,
    pub checkpointed_attempt_ids: Vec<AttemptId>,
    pub messages: Vec<Message>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutboxPayload {
    Text {
        text: String,
    },
    File {
        path: PathBuf,
        display_name: String,
        media_type: String,
        caption: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub struct NewOutboxIntent {
    pub id: OutboxId,
    pub intent_key: String,
    pub intent_class: String,
    pub audience: Audience,
    pub payload: OutboxPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxRecord {
    pub id: OutboxId,
    pub intent_key: String,
    pub payload: OutboxPayload,
    pub state: OutboxState,
    pub attempt_count: i64,
}

#[derive(Clone, Debug)]
pub struct FinalizeRun {
    pub run: AttachedRun,
    pub incorporated_event_ids: Vec<EventId>,
    pub final_messages: Vec<Message>,
    pub outbox: Vec<NewOutboxIntent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizeOutcome {
    Completed,
    Preempted { newest_sequence: i64 },
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn checkpoint_run(&self, command: CheckpointRun, now: Timestamp) -> Result<()>;
    async fn finalize_run(&self, command: FinalizeRun, now: Timestamp) -> Result<FinalizeOutcome>;
}

#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn pending_outbox(&self) -> Result<Vec<OutboxRecord>>;
    async fn mark_outbox_delivered(&self, id: &OutboxId, now: Timestamp) -> Result<()>;
    async fn mark_outbox_failed_terminal(
        &self,
        id: &OutboxId,
        error: &str,
        now: Timestamp,
    ) -> Result<()>;
}
