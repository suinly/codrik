use anyhow::Result;
use async_trait::async_trait;

use crate::runtime::{
    model::{ActorId, Audience, EventKind, Timestamp},
    runner::{ActorRunner, RunOnceOutcome},
    signals::ActorSignals,
    store::{IngressOutcome, IngressStore, NewInboundEvent, RuntimeStore},
};

#[async_trait]
pub trait ReadyRunner: Send + Sync {
    fn now(&self) -> Timestamp;
    async fn run_ready_once(&self) -> Result<RunOnceOutcome>;
}

#[async_trait]
impl<L, T, S, C> ReadyRunner for ActorRunner<L, T, S, C>
where
    L: crate::llm::client::LlmStreamClient + Send + Sync,
    T: crate::agent::tool::ToolExecutor + Send + Sync,
    S: RuntimeStore + Send + Sync + Clone + 'static,
    C: crate::runtime::model::Clock,
{
    fn now(&self) -> Timestamp {
        self.now()
    }

    async fn run_ready_once(&self) -> Result<RunOnceOutcome> {
        self.run_once("local-kernel").await
    }
}

pub struct LocalKernel<S, R> {
    store: S,
    runner: R,
    signals: ActorSignals,
    actor_id: ActorId,
    identity_provider: String,
    identity_subject: String,
}

impl<S, R> LocalKernel<S, R>
where
    S: IngressStore + Clone,
    R: ReadyRunner,
{
    pub fn new(
        store: S,
        runner: R,
        signals: ActorSignals,
        actor_id: ActorId,
        identity_provider: impl Into<String>,
        identity_subject: impl Into<String>,
    ) -> Self {
        Self {
            store,
            runner,
            signals,
            actor_id,
            identity_provider: identity_provider.into(),
            identity_subject: identity_subject.into(),
        }
    }

    pub async fn submit_text(&self, external_id: &str, text: &str) -> Result<IngressOutcome> {
        self.ingest(NewInboundEvent::text(
            "local",
            external_id,
            &self.identity_provider,
            &self.identity_subject,
            Audience::ActorPrivate,
            text,
        )?)
        .await
    }

    pub async fn request_cancel(&self, external_id: &str) -> Result<IngressOutcome> {
        self.ingest(NewInboundEvent {
            gateway: "local".into(),
            external_id: external_id.into(),
            identity_provider: self.identity_provider.clone(),
            identity_subject: self.identity_subject.clone(),
            kind: EventKind::CancelRequested,
            audience: Audience::ActorPrivate,
            delivery_route: None,
            payload_json: r#"{"type":"cancel"}"#.into(),
        })
        .await
    }

    pub async fn run_ready_once(&self) -> Result<RunOnceOutcome> {
        self.runner.run_ready_once().await
    }

    async fn ingest(&self, event: NewInboundEvent) -> Result<IngressOutcome> {
        let outcome = self.store.ingest(event, self.runner.now()).await?;
        if let IngressOutcome::Accepted { sequence, .. } = &outcome {
            self.signals.notify(&self.actor_id, *sequence).await;
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;

    use crate::{
        agent::tool::{Tool, ToolCallContext, ToolCapabilities, ToolExecution, ToolExecutor},
        llm::client::{
            LlmClient, LlmRequest, LlmResponse, LlmStreamClient, LlmStreamSink, RunContext,
        },
        runtime::{
            artifacts::ArtifactManager,
            model::{ActorId, AttemptId, ManualClock, Timestamp},
            runner::{ActorRunner, RunOnceOutcome, RunnerLimits},
            service::LocalKernel,
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
            store::{
                DispatchStore, IngressOutcome, IngressStore, NewInboundEvent, NewToolAttempt,
                OutboxPayload, ToolAttemptStore,
            },
            stream_hub::NoopRuntimeEventPublisher,
        },
        test_fixtures::{ActorSeed, ActorSeedSet, IdentitySeed},
    };

    fn test_artifacts(
        store: &SqliteRuntimeStore,
        clock: ManualClock,
    ) -> ArtifactManager<SqliteRuntimeStore, ManualClock> {
        ArtifactManager::new(
            std::env::temp_dir().join(format!("codrik-service-test-{}", uuid::Uuid::new_v4())),
            store.clone(),
            clock,
        )
    }

    #[derive(Clone)]
    struct FinalLlm;

    #[async_trait]
    impl LlmClient for FinalLlm {
        async fn generate(
            &self,
            _request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            Ok(LlmResponse {
                content: "done".into(),
                tool_calls: Vec::new(),
            })
        }
    }

    #[async_trait]
    impl LlmStreamClient for FinalLlm {
        async fn stream(
            &self,
            request: LlmRequest,
            _sink: &mut dyn LlmStreamSink,
            context: &RunContext,
        ) -> Result<LlmResponse> {
            self.generate(request, context).await
        }
    }

    #[derive(Clone)]
    struct NoTools;

    #[async_trait]
    impl ToolExecutor for NoTools {
        fn definitions(&self) -> Vec<Tool> {
            Vec::new()
        }

        fn capabilities(&self, _name: &str) -> Option<ToolCapabilities> {
            None
        }

        async fn execute(
            &self,
            _name: &str,
            _arguments: &str,
            _context: &ToolCallContext,
        ) -> Result<ToolExecution> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn durable_local_kernel_deduplicates_input_and_drains_outbox() {
        let path = std::env::temp_dir().join(format!(
            "codrik-local-kernel-{}-{}.sqlite",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let store = SqliteRuntimeStore::open(&path).await.unwrap();
        store
            .seed_actors_for_test(
                ActorSeedSet {
                    actors: vec![ActorSeed {
                        id: "actor:local:1".into(),
                        enabled: true,
                        tools: Vec::new(),
                        identities: vec![IdentitySeed {
                            provider: "local".into(),
                            subject: "owner".into(),
                            username: None,
                        }],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        let signals = ActorSignals::default();
        let runner = ActorRunner::new(
            FinalLlm,
            NoTools,
            signals.clone(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );
        let kernel = LocalKernel::new(
            store,
            runner,
            signals,
            ActorId::from_string("actor:local:1"),
            "local",
            "owner",
        );

        let accepted = kernel.submit_text("event-1", "hello").await.unwrap();
        assert_eq!(accepted.sequence(), Some(1));
        assert_eq!(
            kernel.run_ready_once().await.unwrap(),
            RunOnceOutcome::Completed
        );
        let outbox = kernel.store.outbox_intents().await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].payload,
            OutboxPayload::Text {
                text: "done".into()
            }
        );

        assert!(matches!(
            kernel.submit_text("event-1", "hello").await.unwrap(),
            IngressOutcome::Duplicate { .. }
        ));
        assert_eq!(kernel.run_ready_once().await.unwrap(), RunOnceOutcome::Idle);
        assert_eq!(kernel.store.outbox_intents().await.unwrap().len(), 1);

        drop(kernel);
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn restart_resumes_attached_run_without_duplicate_outbox() {
        let path = std::env::temp_dir().join(format!(
            "codrik-local-restart-{}-{}.sqlite",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let store = authorized_store(&path).await;
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-1",
                    "local",
                    "owner",
                    crate::runtime::model::Audience::ActorPrivate,
                    "hello",
                )
                .unwrap(),
                Timestamp(2),
            )
            .await
            .unwrap();
        let lease = store
            .acquire_ready_actor("crashed-worker", Timestamp(100), Timestamp(110))
            .await
            .unwrap()
            .unwrap();
        let original = store
            .attach_next_run(&lease, 8, Timestamp(101))
            .await
            .unwrap()
            .unwrap();
        drop(store);

        let reopened = SqliteRuntimeStore::open(&path).await.unwrap();
        let runner = ActorRunner::new(
            FinalLlm,
            NoTools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&reopened, ManualClock::new(1_000)),
        );
        assert_eq!(
            runner.run_once("recovery-worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        let outbox = reopened.outbox_intents().await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].intent_key.contains(original.run_id.as_str()));

        drop(runner);
        drop(reopened);
        let after_commit = SqliteRuntimeStore::open(&path).await.unwrap();
        let after_commit_runner = ActorRunner::new(
            FinalLlm,
            NoTools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&after_commit, ManualClock::new(2_000)),
        );
        assert_eq!(
            after_commit_runner
                .run_once("post-commit-worker")
                .await
                .unwrap(),
            RunOnceOutcome::Idle
        );
        assert_eq!(after_commit.outbox_intents().await.unwrap().len(), 1);
        drop(after_commit_runner);
        drop(after_commit);
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn restart_never_reinvokes_orphaned_running_attempt() {
        let path = std::env::temp_dir().join(format!(
            "codrik-local-running-{}-{}.sqlite",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let store = authorized_store(&path).await;
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-1",
                    "local",
                    "owner",
                    crate::runtime::model::Audience::ActorPrivate,
                    "hello",
                )
                .unwrap(),
                Timestamp(2),
            )
            .await
            .unwrap();
        let lease = store
            .acquire_ready_actor("crashed-worker", Timestamp(100), Timestamp(110))
            .await
            .unwrap()
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(101))
            .await
            .unwrap()
            .unwrap();
        let attempt = store
            .prepare_attempt(
                &run,
                NewToolAttempt {
                    id: AttemptId::new(),
                    tool_call_id: "call-1".into(),
                    tool_name: "dangerous".into(),
                    arguments_json: "{}".into(),
                    capabilities: ToolCapabilities::conservative(),
                },
                Timestamp(102),
            )
            .await
            .unwrap();
        store
            .mark_attempt_running(&run, &attempt.id, Timestamp(103))
            .await
            .unwrap();
        drop(store);

        let reopened = SqliteRuntimeStore::open(&path).await.unwrap();
        let runner = ActorRunner::new(
            FinalLlm,
            NoTools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&reopened, ManualClock::new(1_000)),
        );
        assert_eq!(
            runner.run_once("recovery-worker").await.unwrap(),
            RunOnceOutcome::WaitingForDecision
        );
        assert!(reopened.outbox_intents().await.unwrap().is_empty());

        drop(runner);
        drop(reopened);
        tokio::fs::remove_file(path).await.unwrap();
    }

    async fn authorized_store(path: &std::path::Path) -> SqliteRuntimeStore {
        let store = SqliteRuntimeStore::open(path).await.unwrap();
        store
            .seed_actors_for_test(
                ActorSeedSet {
                    actors: vec![ActorSeed {
                        id: "actor:local:1".into(),
                        enabled: true,
                        tools: Vec::new(),
                        identities: vec![IdentitySeed {
                            provider: "local".into(),
                            subject: "owner".into(),
                            username: None,
                        }],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        store
    }
}
