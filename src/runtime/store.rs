use crate::{agent::message::Message, runtime::model::*};

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
