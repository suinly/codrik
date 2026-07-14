use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(String);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            pub fn from_string(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(ActorId);
id_type!(EventId);
id_type!(WorkItemId);
id_type!(RunId);
id_type!(AttemptId);
id_type!(OutboxId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn plus_millis(self, millis: i64) -> Self {
        Self(self.0 + millis)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Audience {
    ActorPrivate,
    ConversationScoped { address: String },
    Shareable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    UserMessage,
    CancelRequested,
    ExternalCompletion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventState {
    Pending,
    Processing,
    Completed,
    Cancelled,
    FailedTerminal,
    Blocked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkItemState {
    Ready,
    Waiting,
    Completed,
    Cancelled,
    FailedTerminal,
    BlockedUnknownOutcome,
    WaitingForDecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState {
    Active,
    Completed,
    Cancelled,
    FailedTerminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptState {
    Prepared,
    Running,
    Succeeded,
    FailedKnown,
    OutcomeUnknown,
    CancelledKnown,
    WaitingForDecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxState {
    Pending,
    Delivering,
    Delivered,
    FailedRetryable,
    FailedTerminal,
    OutcomeUnknown,
    AcknowledgedDuplicate,
}
