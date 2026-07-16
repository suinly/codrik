use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::{
    agent::{message::Message, tool::ToolCapabilities},
    runtime::{gateway::*, model::*},
};

use super::runner::RunOnceOutcome;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuantumProgress {
    None,
    ModelCheckpoint,
    KnownToolOutcome,
    Finalized,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuantumReport {
    pub work_item_id: Option<WorkItemId>,
    pub outcome: RunOnceOutcome,
    pub progress: QuantumProgress,
}

#[derive(Debug)]
pub enum QuantumFailure {
    RecoverableWork { disposition: FailureDisposition },
    AuthorityUnavailable(anyhow::Error),
}

#[async_trait]
pub trait QuantumRunner: Send + Sync {
    async fn run_quantum(
        &self,
        actor: &ActorId,
        owner: &str,
    ) -> std::result::Result<QuantumReport, QuantumFailure>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureDisposition {
    RetryAt(Timestamp),
    Terminalized,
}

#[async_trait]
pub trait FailureStore: Send + Sync {
    async fn record_failure<C: Clock>(
        &self,
        fence: &FailureFence,
        error: &str,
        progress: QuantumProgress,
        clock: &C,
    ) -> Result<FailureDisposition>;

    async fn record_progress<C: Clock>(&self, fence: &FailureFence, clock: &C) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct BeginArtifact {
    pub id: ArtifactId,
    pub actor_id: ActorId,
    pub attempt_id: AttemptId,
    pub managed_path: PathBuf,
    pub display_name: String,
    pub media_type: String,
    pub size: u64,
    pub caption: Option<String>,
    pub owner: String,
    pub lease_until: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactLease {
    pub id: ArtifactId,
    pub actor_id: ActorId,
    pub attempt_id: AttemptId,
    pub managed_path: PathBuf,
    pub owner: String,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredArtifact {
    pub id: ArtifactId,
    pub managed_path: PathBuf,
    pub owner: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferencedArtifact {
    pub id: ArtifactId,
    pub managed_path: PathBuf,
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedArtifact {
    pub id: ArtifactId,
    pub managed_path: PathBuf,
    pub display_name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256: String,
    pub caption: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurableToolExecution {
    pub observation: String,
    pub artifacts: Vec<ManagedArtifact>,
}

#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn begin_staging(&self, command: BeginArtifact, now: Timestamp) -> Result<ArtifactLease>;
    async fn renew_staging(&self, lease: &ArtifactLease, until: Timestamp)
    -> Result<ArtifactLease>;
    async fn commit_staged_execution(
        &self,
        run: &AttachedRun,
        attempt: &AttemptId,
        execution: DurableToolExecution,
        leases: &[ArtifactLease],
        now: Timestamp,
    ) -> Result<()>;
    async fn claim_expired_staging(
        &self,
        now: Timestamp,
        limit: usize,
    ) -> Result<Vec<ExpiredArtifact>>;
    async fn renew_gc_claim(
        &self,
        artifact: &ExpiredArtifact,
        now: Timestamp,
        until: Timestamp,
    ) -> Result<bool>;
    async fn complete_claimed_staging(&self, artifact: &ExpiredArtifact) -> Result<bool>;
    async fn artifact_path_exists(&self, path: &std::path::Path) -> Result<bool>;
    async fn referenced_artifact(
        &self,
        actor: &ActorId,
        sha256: &str,
        size: u64,
    ) -> Result<Option<ReferencedArtifact>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeActor {
    pub id: ActorId,
    pub enabled: bool,
    pub tools: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorBootstrapOutcome {
    Created,
    AlreadyInitialized,
}

#[async_trait]
pub trait ActorStore: Send + Sync {
    async fn ensure_initial_actor(
        &self,
        id: &ActorId,
        tools: &[String],
        now: Timestamp,
    ) -> Result<ActorBootstrapOutcome>;

    async fn load_actor(&self, id: &ActorId) -> Result<Option<RuntimeActor>>;

    async fn resolve_identity(&self, provider: &str, subject: &str)
    -> Result<Option<RuntimeActor>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkIdentity {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreLinkCodeReplacement {
    Stored,
    HashCollision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreLinkRedemption {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict { actor_id: ActorId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreLinkCommandRedemption {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict,
}

#[async_trait]
pub trait IdentityLinkStore: Send + Sync {
    async fn replace_link_code(
        &self,
        actor: &ActorId,
        code_hash: [u8; 32],
        created_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<StoreLinkCodeReplacement>;

    async fn redeem_link_code(
        &self,
        identity: LinkIdentity,
        code_hash: Option<[u8; 32]>,
        now: Timestamp,
    ) -> Result<StoreLinkRedemption>;

    async fn redeem_link_code_once(
        &self,
        key: GatewayCommandKey,
        identity: LinkIdentity,
        code_hash: Option<[u8; 32]>,
        now: Timestamp,
    ) -> Result<StoreLinkCommandRedemption>;

    async fn collect_expired_link_state(&self, now: Timestamp, limit: usize) -> Result<usize>;
}

#[async_trait]
pub trait GatewayDeliveryStore: Send + Sync {
    async fn enqueue_gateway_delivery(
        &self,
        delivery: NewGatewayDelivery,
        now: Timestamp,
    ) -> Result<GatewayDeliveryId>;

    async fn claim_gateway_deliveries(
        &self,
        gateway: &str,
        owner: &str,
        now: Timestamp,
        claim_until: Timestamp,
        limit: usize,
    ) -> Result<Vec<ClaimedGatewayDelivery>>;

    async fn renew_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        now: Timestamp,
        claim_until: Timestamp,
    ) -> Result<Option<GatewayDeliveryClaim>>;

    async fn complete_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        remote_message_id: Option<String>,
        now: Timestamp,
    ) -> Result<bool>;

    async fn retry_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        next_attempt_at: Timestamp,
        error_class: &str,
        error: &str,
        now: Timestamp,
    ) -> Result<bool>;

    async fn fail_gateway_delivery(
        &self,
        claim: &GatewayDeliveryClaim,
        state: GatewayDeliveryState,
        error_class: &str,
        error: &str,
        now: Timestamp,
    ) -> Result<bool>;
}

#[async_trait]
pub trait GatewayStreamStore: Send + Sync {
    async fn upsert_gateway_stream(
        &self,
        work_item: &WorkItemId,
        route: &DeliveryRoute,
        remote_message_id: &str,
        now: Timestamp,
    ) -> Result<()>;

    async fn resolve_gateway_stream(
        &self,
        work_item: &WorkItemId,
        route: &DeliveryRoute,
    ) -> Result<Option<String>>;

    async fn close_gateway_stream(
        &self,
        work_item: &WorkItemId,
        route: &DeliveryRoute,
        now: Timestamp,
    ) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct NewInboundEvent {
    pub gateway: String,
    pub external_id: String,
    pub identity_provider: String,
    pub identity_subject: String,
    pub kind: EventKind,
    pub audience: Audience,
    pub delivery_route: Option<DeliveryRoute>,
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
        Self::text_with_optional_route(
            gateway,
            external_id,
            identity_provider,
            identity_subject,
            audience,
            None,
            text,
        )
    }

    pub fn text_with_route(
        gateway: impl Into<String>,
        external_id: impl Into<String>,
        identity_provider: impl Into<String>,
        identity_subject: impl Into<String>,
        audience: Audience,
        delivery_route: DeliveryRoute,
        text: impl Into<String>,
    ) -> Result<Self> {
        Self::text_with_optional_route(
            gateway,
            external_id,
            identity_provider,
            identity_subject,
            audience,
            Some(delivery_route),
            text,
        )
    }

    fn text_with_optional_route(
        gateway: impl Into<String>,
        external_id: impl Into<String>,
        identity_provider: impl Into<String>,
        identity_subject: impl Into<String>,
        audience: Audience,
        delivery_route: Option<DeliveryRoute>,
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
            delivery_route,
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
pub struct LocalSubmission {
    pub request_id: RequestId,
    pub text: String,
    pub prompt_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalCancel {
    pub cancel_id: CancelId,
    pub request_id: RequestId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalSubmitOutcome {
    Accepted {
        event_id: EventId,
        work_item_id: WorkItemId,
        sequence: i64,
    },
    Duplicate {
        event_id: EventId,
        work_item_id: Option<WorkItemId>,
        sequence: i64,
    },
    Conflict,
    ActorUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOutcome {
    pub cancel_id: CancelId,
    pub affected_request_ids: Vec<RequestId>,
    pub already_terminal: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalRequestRecord {
    pub request_id: RequestId,
    pub actor_id: ActorId,
    pub event_id: EventId,
    pub sequence: i64,
    pub work_item_id: Option<WorkItemId>,
    pub state: LocalRequestState,
    pub result_bundle_id: Option<BundleId>,
    pub result_bundle_state: Option<BundleState>,
}

#[async_trait]
pub trait LocalIngressStore: Send + Sync {
    async fn submit_for_actor(
        &self,
        actor: &ActorId,
        command: LocalSubmission,
        now: Timestamp,
    ) -> Result<LocalSubmitOutcome>;

    async fn cancel_for_actor(
        &self,
        actor: &ActorId,
        command: LocalCancel,
        now: Timestamp,
    ) -> Result<CancelOutcome>;

    async fn resolve_local_request(
        &self,
        actor: &ActorId,
        id: &RequestId,
    ) -> Result<Option<LocalRequestRecord>>;
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

    async fn acquire_ready_actor_for(
        &self,
        actor: &ActorId,
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
    pub request_ids: Vec<RequestId>,
    pub audience: Audience,
    pub delivery_route: Option<DeliveryRoute>,
    pub messages: Vec<Message>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureFence {
    pub lease: ActorLease,
    pub work_item_id: WorkItemId,
    pub run_id: RunId,
}

impl From<&AttachedRun> for FailureFence {
    fn from(run: &AttachedRun) -> Self {
        Self {
            lease: run.lease.clone(),
            work_item_id: run.work_item_id.clone(),
            run_id: run.run_id.clone(),
        }
    }
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
        artifact_id: ArtifactId,
        managed_path: PathBuf,
        display_name: String,
        media_type: String,
        size: u64,
        sha256: String,
        caption: Option<String>,
    },
    TerminalError {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FinalPayload {
    Text { text: String },
    File { artifact: ManagedArtifact },
    TerminalError { code: String, message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BundleManifestEntry {
    pub delivery_id: DeliveryId,
    pub payload_kind: String,
    pub decoded_bytes: usize,
    pub sha256: String,
    pub chunk_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleManifest {
    pub entries: Vec<BundleManifestEntry>,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultBundle {
    pub id: BundleId,
    pub request_id: RequestId,
    pub state: BundleState,
    pub manifest: BundleManifest,
    pub deliveries: Vec<(DeliveryId, FinalPayload)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleClaim {
    pub bundle_id: BundleId,
    pub owner: String,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedBundle {
    pub claim: BundleClaim,
    pub bundle: ResultBundle,
    pub attempt_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedBundleRef {
    pub claim: BundleClaim,
    pub request_id: RequestId,
    pub attempt_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayBundleRef {
    pub actor_id: ActorId,
    pub request_id: RequestId,
    pub bundle_id: BundleId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimedBundleLoad {
    Loaded(ResultBundle),
    FailedTerminal,
    Delivered,
    Fenced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimRenewal {
    Renewed(BundleClaim),
    Fenced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimTransition {
    Applied,
    Fenced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleAck {
    pub actor_id: ActorId,
    pub request_id: RequestId,
    pub bundle_id: BundleId,
    pub delivery_ids: Vec<DeliveryId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckOutcome {
    Delivered,
    AlreadyDelivered,
}

#[derive(Debug)]
pub struct AckRejected(pub String);

impl std::fmt::Display for AckRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AckRejected {}

#[async_trait]
pub trait BundleStore: Send + Sync {
    async fn claim_ready_bundle_refs(
        &self,
        owner: &str,
        request_ids: &[RequestId],
        now: Timestamp,
        until: Timestamp,
        limit: usize,
    ) -> Result<Vec<ClaimedBundleRef>>;
    async fn claim_ready_bundles(
        &self,
        owner: &str,
        request_ids: &[RequestId],
        now: Timestamp,
        until: Timestamp,
        limit: usize,
    ) -> Result<Vec<ClaimedBundle>>;
    async fn renew_bundle(
        &self,
        claim: &BundleClaim,
        now: Timestamp,
        until: Timestamp,
    ) -> Result<ClaimRenewal>;
    async fn load_claimed_bundle(
        &self,
        claim: &BundleClaim,
        now: Timestamp,
    ) -> Result<ClaimedBundleLoad>;
    async fn load_bundle(&self, id: &BundleId) -> Result<ResultBundle>;
    async fn load_bundle_state(&self, id: &BundleId) -> Result<BundleState>;
    async fn acknowledge_bundle(&self, ack: BundleAck, now: Timestamp) -> Result<AckOutcome>;
    async fn fail_bundle_retryable(
        &self,
        claim: &BundleClaim,
        error: &str,
        next_attempt: Timestamp,
        now: Timestamp,
    ) -> Result<ClaimTransition>;
    async fn fail_bundle_terminal(
        &self,
        claim: &BundleClaim,
        error: &str,
        now: Timestamp,
    ) -> Result<()>;
    async fn resolve_replay_bundle(
        &self,
        actor: &ActorId,
        request: &RequestId,
    ) -> Result<Option<ReplayBundleRef>>;
    async fn load_replay_bundle(&self, replay: &ReplayBundleRef) -> Result<ResultBundle>;
}

#[derive(Clone, Debug)]
pub struct NewOutboxIntent {
    pub id: OutboxId,
    pub intent_key: String,
    pub intent_class: String,
    pub audience: Audience,
    pub payload: OutboxPayload,
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
    async fn cancel_run(
        &self,
        run: &AttachedRun,
        control: &ControlEvent,
        now: Timestamp,
    ) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlEvent {
    pub event_id: EventId,
    pub sequence: i64,
    pub kind: EventKind,
}

#[async_trait]
pub trait ControlStore: Send + Sync {
    async fn newer_control_event(
        &self,
        lease: &ActorLease,
        observed_sequence: i64,
        now: Timestamp,
    ) -> Result<Option<ControlEvent>>;
}

#[derive(Clone, Debug)]
pub struct NewToolAttempt {
    pub id: AttemptId,
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments_json: String,
    pub capabilities: ToolCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolAttempt {
    pub id: AttemptId,
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments_json: String,
    pub capabilities: ToolCapabilities,
    pub state: AttemptState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AttemptOutcome {
    Succeeded { execution: DurableToolExecution },
    FailedKnown { message: String },
    CancelledKnown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptRecovery {
    MayInvoke,
    OutcomeUnknown,
    Terminal(AttemptOutcome),
}

#[async_trait]
pub trait ToolAttemptStore: Send + Sync {
    async fn prepare_attempt(
        &self,
        run: &AttachedRun,
        attempt: NewToolAttempt,
        now: Timestamp,
    ) -> Result<ToolAttempt>;
    async fn mark_attempt_running(
        &self,
        run: &AttachedRun,
        id: &AttemptId,
        now: Timestamp,
    ) -> Result<()>;
    async fn finish_attempt(
        &self,
        run: &AttachedRun,
        id: &AttemptId,
        outcome: AttemptOutcome,
        now: Timestamp,
    ) -> Result<()>;
    async fn recover_attempt(&self, id: &AttemptId) -> Result<AttemptRecovery>;
    async fn block_unknown_attempt(
        &self,
        run: &AttachedRun,
        id: &AttemptId,
        now: Timestamp,
    ) -> Result<()>;
    async fn unresolved_attempts(&self, run: &AttachedRun) -> Result<Vec<ToolAttempt>>;
}

#[async_trait]
pub trait ContextStore: Send + Sync {
    async fn load_recent_context(
        &self,
        actor: &ActorId,
        audience: &Audience,
        limit: usize,
    ) -> Result<Vec<Message>>;
}

pub trait RuntimeStore:
    DispatchStore
    + CheckpointStore
    + ControlStore
    + ToolAttemptStore
    + ContextStore
    + ArtifactStore
    + BundleStore
    + FailureStore
{
}

impl<T> RuntimeStore for T where
    T: DispatchStore
        + CheckpointStore
        + ControlStore
        + ToolAttemptStore
        + ContextStore
        + ArtifactStore
        + BundleStore
        + FailureStore
{
}
