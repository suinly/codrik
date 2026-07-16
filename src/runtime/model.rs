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

macro_rules! uuid_id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            pub fn parse(value: &str) -> Result<Self, uuid::Error> {
                Uuid::parse_str(value).map(|id| Self(id.to_string()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(&value).map_err(serde::de::Error::custom)
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
id_type!(GatewayDeliveryId);
uuid_id_type!(RequestId);
uuid_id_type!(CancelId);
uuid_id_type!(BundleId);
uuid_id_type!(DeliveryId);
uuid_id_type!(ArtifactId);

impl ActorId {
    pub fn parse_workspace_safe(value: &str) -> anyhow::Result<Self> {
        let value = value.trim();
        if value.is_empty()
            || value == "."
            || value == ".."
            || value.contains('/')
            || value.contains('\\')
        {
            anyhow::bail!("unsafe actor id for workspace path: {value}");
        }
        Ok(Self::from_string(value))
    }
}

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_SUBMIT_BYTES: usize = 256 * 1024;
pub const MAX_FINAL_CHUNK_BYTES: usize = 192 * 1024;
pub const MAX_MANIFEST_BYTES: usize = 256 * 1024;
pub const MAX_BUNDLE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BUNDLE_DELIVERIES: usize = 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn plus_millis(self, millis: i64) -> Self {
        Self(self.0.saturating_add(millis))
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::Timestamp;

    #[test]
    fn plus_millis_saturates_synthetic_overflow() {
        assert_eq!(Timestamp(i64::MAX).plus_millis(1), Timestamp(i64::MAX));
        assert_eq!(Timestamp(i64::MIN).plus_millis(-1), Timestamp(i64::MIN));
    }
}

pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}

#[derive(Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock before epoch")
                .as_millis() as i64,
        )
    }
}

#[derive(Clone)]
pub struct ManualClock(std::sync::Arc<std::sync::atomic::AtomicI64>);

impl ManualClock {
    pub fn new(now: i64) -> Self {
        Self(std::sync::Arc::new(std::sync::atomic::AtomicI64::new(now)))
    }

    pub fn advance(&self, millis: i64) {
        self.0
            .fetch_add(millis, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ActorId, ArtifactId, BundleId, BundleState, CancelId, DeliveryId, LocalRequestState,
        MAX_BUNDLE_BYTES, MAX_BUNDLE_DELIVERIES, MAX_FINAL_CHUNK_BYTES, MAX_FRAME_BYTES,
        MAX_MANIFEST_BYTES, MAX_SUBMIT_BYTES, RequestId, WorkItemState,
    };

    #[test]
    fn workspace_actor_ids_trim_valid_values() -> anyhow::Result<()> {
        let actor = ActorId::parse_workspace_safe("  actor:local:owner  ")?;
        assert_eq!(actor.as_str(), "actor:local:owner");
        Ok(())
    }

    #[test]
    fn workspace_actor_ids_reject_unsafe_values() {
        for value in ["", "   ", ".", "..", "actor/owner", r"actor\owner"] {
            assert!(
                ActorId::parse_workspace_safe(value).is_err(),
                "accepted unsafe actor id: {value:?}"
            );
        }
    }

    #[test]
    fn request_ids_reject_non_uuid_strings() {
        assert!(RequestId::parse("not-a-uuid").is_err());
    }

    #[test]
    fn serve_ids_round_trip_through_text_and_json() -> anyhow::Result<()> {
        fn round_trip<T>(value: T) -> anyhow::Result<()>
        where
            T: serde::Serialize
                + serde::de::DeserializeOwned
                + std::fmt::Display
                + PartialEq
                + std::fmt::Debug,
        {
            let text = value.to_string();
            let json = serde_json::to_string(&value)?;
            let decoded: T = serde_json::from_str(&json)?;
            assert_eq!(decoded, value);
            assert_eq!(json, format!("\"{text}\""));
            Ok(())
        }

        round_trip(RequestId::new())?;
        round_trip(CancelId::new())?;
        round_trip(BundleId::new())?;
        round_trip(DeliveryId::new())?;
        round_trip(ArtifactId::new())?;
        Ok(())
    }

    #[test]
    fn serve_protocol_limits_are_exact() {
        assert_eq!(MAX_FRAME_BYTES, 1024 * 1024);
        assert_eq!(MAX_SUBMIT_BYTES, 256 * 1024);
        assert_eq!(MAX_FINAL_CHUNK_BYTES, 192 * 1024);
        assert_eq!(MAX_MANIFEST_BYTES, 256 * 1024);
        assert_eq!(MAX_BUNDLE_BYTES, 16 * 1024 * 1024);
        assert_eq!(MAX_BUNDLE_DELIVERIES, 1024);
    }

    #[test]
    fn serve_states_use_schema_v2_names() -> anyhow::Result<()> {
        assert_eq!(
            serde_json::to_string(&LocalRequestState::Active)?,
            "\"active\""
        );
        assert_eq!(
            serde_json::to_string(&LocalRequestState::FailedTerminal)?,
            "\"failed_terminal\""
        );
        assert_eq!(
            serde_json::to_string(&BundleState::FailedRetryable)?,
            "\"failed_retryable\""
        );
        assert_eq!(
            serde_json::to_string(&BundleState::FailedTerminal)?,
            "\"failed_terminal\""
        );
        assert_eq!(
            serde_json::to_string(&WorkItemState::BlockedMalformed)?,
            "\"blocked_malformed\""
        );
        assert_ne!(
            WorkItemState::BlockedMalformed,
            WorkItemState::BlockedUnknownOutcome
        );
        Ok(())
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemState {
    Ready,
    Waiting,
    Completed,
    Cancelled,
    FailedTerminal,
    BlockedUnknownOutcome,
    BlockedMalformed,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalRequestState {
    Active,
    Completed,
    Cancelled,
    FailedTerminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleState {
    Pending,
    Delivering,
    Delivered,
    FailedRetryable,
    FailedTerminal,
}
