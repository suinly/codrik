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
