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

    pub fn to_rfc3339_utc(self) -> anyhow::Result<String> {
        if self.0 < 0 {
            anyhow::bail!("timestamp must not be before Unix epoch");
        }
        let days = self.0 / 86_400_000;
        let day_millis = self.0 % 86_400_000;
        let (year, month, day) = civil_date_from_unix_days(days);
        if year > 9999 {
            anyhow::bail!("timestamp year exceeds RFC3339 four-digit range");
        }
        let hour = day_millis / 3_600_000;
        let minute = day_millis % 3_600_000 / 60_000;
        let second = day_millis % 60_000 / 1_000;
        let millis = day_millis % 1_000;
        Ok(format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
        ))
    }
}

fn civil_date_from_unix_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod timestamp_tests {
    use super::Timestamp;

    #[test]
    fn formats_rfc3339_utc() -> anyhow::Result<()> {
        assert_eq!(Timestamp(0).to_rfc3339_utc()?, "1970-01-01T00:00:00.000Z");
        assert_eq!(Timestamp(1).to_rfc3339_utc()?, "1970-01-01T00:00:00.001Z");
        assert_eq!(
            Timestamp(1_709_164_800_000).to_rfc3339_utc()?,
            "2024-02-29T00:00:00.000Z"
        );
        assert!(Timestamp(-1).to_rfc3339_utc().is_err());
        Ok(())
    }

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
        ActorId, ArtifactId, BundleId, BundleState, CancelId, DeliveryId, ExecutionPolicy,
        LocalRequestState, MAX_BUNDLE_BYTES, MAX_BUNDLE_DELIVERIES, MAX_FINAL_CHUNK_BYTES,
        MAX_FRAME_BYTES, MAX_MANIFEST_BYTES, MAX_SUBMIT_BYTES, RequestId, WorkItemState,
    };

    #[test]
    fn skills_only_is_monotonic_and_read_only() {
        assert_eq!(
            ExecutionPolicy::ActorTools.intersect(ExecutionPolicy::SkillsOnly),
            ExecutionPolicy::SkillsOnly
        );
        assert!(ExecutionPolicy::SkillsOnly.allows("skills_list"));
        assert!(ExecutionPolicy::SkillsOnly.allows("skills_read"));
        assert!(!ExecutionPolicy::SkillsOnly.allows("skills_create"));
        assert!(!ExecutionPolicy::SkillsOnly.allows("datetime"));
    }

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPolicy {
    #[default]
    ActorTools,
    SkillsOnly,
}

impl ExecutionPolicy {
    pub fn intersect(self, other: Self) -> Self {
        if matches!(self, Self::SkillsOnly) || matches!(other, Self::SkillsOnly) {
            Self::SkillsOnly
        } else {
            Self::ActorTools
        }
    }

    pub fn allows(self, name: &str) -> bool {
        matches!(self, Self::ActorTools) || matches!(name, "skills_list" | "skills_read")
    }
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
