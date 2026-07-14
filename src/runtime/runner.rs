use std::time::Duration;

use anyhow::Result;

use crate::{
    agent::{
        message::Message,
        tool::{ToolCallContext, ToolExecutor},
        tool_observation,
    },
    llm::client::{LlmClient, LlmRequest, LlmToolCall, RunContext},
    runtime::{
        model::{AttemptId, Clock, EventKind, OutboxId},
        signals::ActorSignals,
        store::{
            AttemptOutcome, AttemptRecovery, CheckpointRun, FinalizeOutcome, FinalizeRun,
            NewOutboxIntent, NewToolAttempt, OutboxPayload, RuntimeStore,
        },
    },
};

#[derive(Clone, Debug)]
pub struct RunnerLimits {
    pub max_events: usize,
    pub max_model_steps: usize,
    pub max_tool_steps: usize,
    pub recent_messages: usize,
    pub max_wall_time: Duration,
    pub lease_duration: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for RunnerLimits {
    fn default() -> Self {
        Self {
            max_events: 8,
            max_model_steps: 4,
            max_tool_steps: 8,
            recent_messages: 64,
            max_wall_time: Duration::from_secs(60),
            lease_duration: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunOnceOutcome {
    Idle,
    Completed,
    Yielded,
    Cancelled,
    WaitingForDecision,
}

pub struct ActorRunner<L, T, S, C> {
    store: S,
    llm: L,
    tools: T,
    clock: C,
    signals: ActorSignals,
    limits: RunnerLimits,
}

impl<L, T, S, C> ActorRunner<L, T, S, C>
where
    L: LlmClient + Send + Sync,
    T: ToolExecutor + Send + Sync,
    S: RuntimeStore,
    C: Clock,
{
    pub fn new(
        store: S,
        llm: L,
        tools: T,
        clock: C,
        signals: ActorSignals,
        limits: RunnerLimits,
    ) -> Self {
        Self {
            store,
            llm,
            tools,
            clock,
            signals,
            limits,
        }
    }

    pub async fn run_once(&self, owner: &str) -> Result<RunOnceOutcome> {
        let now = self.clock.now();
        let lease_until = now.plus_millis(duration_millis(self.limits.lease_duration)?);
        let Some(lease) = self
            .store
            .acquire_ready_actor(owner, now, lease_until)
            .await?
        else {
            return Ok(RunOnceOutcome::Idle);
        };
        let result = self.run_leased(&lease).await;
        let release = self.store.release_lease(&lease).await;
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(outcome), Ok(())) => Ok(outcome),
        }
    }

    pub fn now(&self) -> crate::runtime::model::Timestamp {
        self.clock.now()
    }

    async fn run_leased(
        &self,
        lease: &crate::runtime::store::ActorLease,
    ) -> Result<RunOnceOutcome> {
        let mut current_lease = lease.clone();
        let mut heartbeat = tokio::time::interval(self.limits.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let wall_deadline = tokio::time::sleep(self.limits.max_wall_time);
        tokio::pin!(wall_deadline);
        let Some(mut run) = self
            .store
            .attach_next_run(lease, self.limits.max_events, self.clock.now())
            .await?
        else {
            return Ok(RunOnceOutcome::Idle);
        };
        let mut messages = self
            .store
            .load_recent_context(&lease.actor_id, &run.audience, self.limits.recent_messages)
            .await?;
        messages.extend(run.messages.clone());
        self.store
            .checkpoint_run(
                CheckpointRun {
                    run: run.clone(),
                    incorporated_event_ids: run.source_event_ids.clone(),
                    checkpointed_attempt_ids: Vec::new(),
                    messages: run.messages.clone(),
                },
                self.clock.now(),
            )
            .await?;

        let context = RunContext::new();
        for attempt in self.store.unresolved_attempts(&run).await? {
            let recovery = self.store.recover_attempt(&attempt.id).await?;
            let outcome = match recovery {
                AttemptRecovery::MayInvoke => {
                    self.store
                        .mark_attempt_running(&run, &attempt.id, self.clock.now())
                        .await?;
                    let tool_context = ToolCallContext {
                        attempt_id: attempt.id.to_string(),
                        authorized_tools: vec![attempt.tool_name.clone()],
                        cancellation: context.clone(),
                    };
                    let outcome = match self
                        .tools
                        .execute(&attempt.tool_name, &attempt.arguments_json, &tool_context)
                        .await
                    {
                        Ok(execution) => AttemptOutcome::Succeeded { execution },
                        Err(error) => AttemptOutcome::FailedKnown {
                            message: error.to_string(),
                        },
                    };
                    self.store
                        .finish_attempt(&run, &attempt.id, outcome.clone(), self.clock.now())
                        .await?;
                    outcome
                }
                AttemptRecovery::OutcomeUnknown => {
                    if attempt.state != crate::runtime::model::AttemptState::WaitingForDecision {
                        self.store
                            .block_unknown_attempt(&run, &attempt.id, self.clock.now())
                            .await?;
                    }
                    return Ok(RunOnceOutcome::WaitingForDecision);
                }
                AttemptRecovery::Terminal(outcome) => outcome,
            };
            let assistant = Message::assistant_tool_calls(
                "",
                vec![LlmToolCall {
                    id: attempt.tool_call_id.clone(),
                    name: attempt.tool_name,
                    arguments: attempt.arguments_json,
                }],
            );
            let tool_result =
                Message::tool_result(attempt.tool_call_id, observation_for_outcome(&outcome));
            let recovered_messages = vec![assistant, tool_result];
            self.store
                .checkpoint_run(
                    CheckpointRun {
                        run: run.clone(),
                        incorporated_event_ids: Vec::new(),
                        checkpointed_attempt_ids: vec![attempt.id],
                        messages: recovered_messages.clone(),
                    },
                    self.clock.now(),
                )
                .await?;
            messages.extend(recovered_messages);
        }
        let mut signal_receiver = self.signals.subscribe(&lease.actor_id).await;
        let mut tool_steps = 0;
        for _ in 0..self.limits.max_model_steps {
            if let Some(control) = self
                .store
                .newer_control_event(&current_lease, run.observed_sequence, self.clock.now())
                .await?
            {
                return match control.kind {
                    EventKind::CancelRequested => {
                        self.store
                            .cancel_run(&run, &control, self.clock.now())
                            .await?;
                        Ok(RunOnceOutcome::Cancelled)
                    }
                    EventKind::UserMessage => Ok(RunOnceOutcome::Yielded),
                    EventKind::ExternalCompletion => unreachable!(),
                };
            }
            let request = LlmRequest {
                messages: messages.clone(),
                tools: self.tools.definitions(),
            };
            let response = {
                let generation = self.llm.generate(request, &context);
                tokio::pin!(generation);
                loop {
                    tokio::select! {
                        response = &mut generation => break response?,
                        _ = &mut wall_deadline => return Ok(RunOnceOutcome::Yielded),
                        _ = heartbeat.tick() => {
                            let now = self.clock.now();
                            current_lease = self.store
                                .renew_lease(
                                    &current_lease,
                                    now,
                                    now.plus_millis(duration_millis(self.limits.lease_duration)?),
                                )
                                .await?;
                        }
                        changed = signal_receiver.changed() => {
                            if changed.is_err() {
                                continue;
                            }
                            if let Some(control) = self.store
                                .newer_control_event(&current_lease, run.observed_sequence, self.clock.now())
                                .await?
                            {
                                context.cancel();
                                return match control.kind {
                                    EventKind::CancelRequested => {
                                        self.store.cancel_run(&run, &control, self.clock.now()).await?;
                                        Ok(RunOnceOutcome::Cancelled)
                                    }
                                    EventKind::UserMessage => Ok(RunOnceOutcome::Yielded),
                                    EventKind::ExternalCompletion => unreachable!(),
                                };
                            }
                        }
                    }
                }
            };
            if response.tool_calls.is_empty() {
                let intent_key = format!("run:{}:final", run.run_id);
                match self
                    .store
                    .finalize_run(
                        FinalizeRun {
                            run: run.clone(),
                            incorporated_event_ids: run.source_event_ids.clone(),
                            final_messages: vec![Message::assistant(response.content.clone())],
                            outbox: vec![NewOutboxIntent {
                                id: OutboxId::new(),
                                intent_key,
                                intent_class: "interactive_reply".into(),
                                audience: run.audience.clone(),
                                payload: OutboxPayload::Text {
                                    text: response.content,
                                },
                            }],
                        },
                        self.clock.now(),
                    )
                    .await?
                {
                    FinalizeOutcome::Completed => return Ok(RunOnceOutcome::Completed),
                    FinalizeOutcome::Preempted { .. } => {
                        run = self
                            .store
                            .attach_next_run(lease, self.limits.max_events, self.clock.now())
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("preempted run was not resumable"))?;
                        messages = self
                            .store
                            .load_recent_context(
                                &lease.actor_id,
                                &run.audience,
                                self.limits.recent_messages,
                            )
                            .await?;
                        messages.extend(run.messages.clone());
                        self.store
                            .checkpoint_run(
                                CheckpointRun {
                                    run: run.clone(),
                                    incorporated_event_ids: run.source_event_ids.clone(),
                                    checkpointed_attempt_ids: Vec::new(),
                                    messages: run.messages.clone(),
                                },
                                self.clock.now(),
                            )
                            .await?;
                        continue;
                    }
                }
            }

            let assistant =
                Message::assistant_tool_calls(response.content, response.tool_calls.clone());
            let mut checkpoint_messages = vec![assistant.clone()];
            let mut checkpointed_attempt_ids = Vec::new();
            for tool_call in response.tool_calls {
                if tool_steps >= self.limits.max_tool_steps {
                    return Ok(RunOnceOutcome::Yielded);
                }
                tool_steps += 1;
                let capabilities = self
                    .tools
                    .capabilities(&tool_call.name)
                    .ok_or_else(|| anyhow::anyhow!("unknown tool: {}", tool_call.name))?;
                let attempt = self
                    .store
                    .prepare_attempt(
                        &run,
                        NewToolAttempt {
                            id: AttemptId::new(),
                            tool_call_id: tool_call.id.clone(),
                            tool_name: tool_call.name.clone(),
                            arguments_json: tool_call.arguments.clone(),
                            capabilities,
                        },
                        self.clock.now(),
                    )
                    .await?;
                self.store
                    .mark_attempt_running(&run, &attempt.id, self.clock.now())
                    .await?;
                let tool_context = ToolCallContext {
                    attempt_id: attempt.id.to_string(),
                    authorized_tools: Vec::new(),
                    cancellation: context.clone(),
                };
                let (outcome, observation) = match self
                    .tools
                    .execute(&tool_call.name, &tool_call.arguments, &tool_context)
                    .await
                {
                    Ok(execution) => {
                        let observation = tool_observation::success(&execution.observation);
                        (AttemptOutcome::Succeeded { execution }, observation)
                    }
                    Err(error) => {
                        let observation = tool_observation::failure(&error);
                        (
                            AttemptOutcome::FailedKnown {
                                message: error.to_string(),
                            },
                            observation,
                        )
                    }
                };
                self.store
                    .finish_attempt(&run, &attempt.id, outcome, self.clock.now())
                    .await?;
                checkpointed_attempt_ids.push(attempt.id);
                checkpoint_messages.push(Message::tool_result(tool_call.id, observation));
            }
            self.store
                .checkpoint_run(
                    CheckpointRun {
                        run: run.clone(),
                        incorporated_event_ids: Vec::new(),
                        checkpointed_attempt_ids,
                        messages: checkpoint_messages.clone(),
                    },
                    self.clock.now(),
                )
                .await?;
            messages.extend(checkpoint_messages);
        }
        Ok(RunOnceOutcome::Yielded)
    }
}

fn duration_millis(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis()).map_err(Into::into)
}

fn observation_for_outcome(outcome: &AttemptOutcome) -> String {
    match outcome {
        AttemptOutcome::Succeeded { execution } => {
            tool_observation::success(&execution.observation)
        }
        AttemptOutcome::FailedKnown { message } => {
            tool_observation::failure(&anyhow::anyhow!(message.clone()))
        }
        AttemptOutcome::CancelledKnown => {
            tool_observation::failure(&anyhow::anyhow!("tool call cancelled"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::sync::{Mutex, Notify};

    use crate::{
        agent::tool::{Tool, ToolCallContext, ToolCapabilities, ToolExecution, ToolExecutor},
        auth::{LegacyActor, LegacyAuthorizationSnapshot, LegacyIdentity},
        llm::client::{LlmClient, LlmRequest, LlmResponse, LlmToolCall, RunContext},
        runtime::{
            model::{ActorId, Audience, EventKind, ManualClock, Timestamp},
            runner::{ActorRunner, RunOnceOutcome, RunnerLimits},
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
            store::{
                DispatchStore, IngressStore, NewInboundEvent, OutboxPayload, OutboxStore,
                RuntimeAuthorizationStore,
            },
        },
    };

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

    #[derive(Clone)]
    struct ScriptedLlm {
        responses: Arc<Mutex<VecDeque<LlmResponse>>>,
    }

    #[async_trait]
    impl LlmClient for ScriptedLlm {
        async fn generate(
            &self,
            _request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            Ok(self.responses.lock().await.pop_front().unwrap())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingTools {
        attempts: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone)]
    struct InjectingLlm {
        store: SqliteRuntimeStore,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct BlockingLlm {
        started: Arc<Notify>,
    }

    #[async_trait]
    impl LlmClient for BlockingLlm {
        async fn generate(
            &self,
            _request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    #[async_trait]
    impl LlmClient for InjectingLlm {
        async fn generate(
            &self,
            _request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.store
                    .ingest(
                        NewInboundEvent::text(
                            "local",
                            "event-2",
                            "local",
                            "owner",
                            Audience::ActorPrivate,
                            "new context",
                        )?,
                        Timestamp(3),
                    )
                    .await?;
            }
            Ok(LlmResponse {
                content: if call == 0 { "stale" } else { "fresh" }.into(),
                tool_calls: Vec::new(),
            })
        }
    }

    #[async_trait]
    impl ToolExecutor for RecordingTools {
        fn definitions(&self) -> Vec<Tool> {
            vec![Tool::new("datetime", "time", Default::default())]
        }

        fn capabilities(&self, name: &str) -> Option<ToolCapabilities> {
            (name == "datetime").then(ToolCapabilities::read_only)
        }

        async fn execute(
            &self,
            name: &str,
            _arguments: &str,
            context: &ToolCallContext,
        ) -> Result<ToolExecution> {
            assert_eq!(name, "datetime");
            self.attempts.lock().await.push(context.attempt_id.clone());
            Ok(ToolExecution::text("2026-07-14"))
        }
    }

    #[tokio::test]
    async fn text_event_finishes_only_through_outbox() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: "actor:local:1".into(),
                        enabled: true,
                        tools: Vec::new(),
                        identities: vec![LegacyIdentity {
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
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-1",
                    "local",
                    "owner",
                    Audience::ActorPrivate,
                    "hello",
                )
                .unwrap(),
                Timestamp(2),
            )
            .await
            .unwrap();
        let runner = ActorRunner::new(
            store.clone(),
            FinalLlm,
            NoTools,
            ManualClock::new(1_000),
            ActorSignals::default(),
            RunnerLimits::default(),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        let outbox = store.pending_outbox().await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].payload,
            OutboxPayload::Text {
                text: "done".into()
            }
        );
    }

    #[tokio::test]
    async fn tool_call_is_durable_before_final_response() {
        let store = store_with_text().await;
        let llm = ScriptedLlm {
            responses: Arc::new(Mutex::new(VecDeque::from([
                LlmResponse {
                    content: "checking".into(),
                    tool_calls: vec![LlmToolCall {
                        id: "call-1".into(),
                        name: "datetime".into(),
                        arguments: "{}".into(),
                    }],
                },
                LlmResponse {
                    content: "done".into(),
                    tool_calls: Vec::new(),
                },
            ]))),
        };
        let tools = RecordingTools::default();
        let attempts = tools.attempts.clone();
        let runner = ActorRunner::new(
            store.clone(),
            llm,
            tools,
            ManualClock::new(1_000),
            ActorSignals::default(),
            RunnerLimits::default(),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        assert_eq!(attempts.lock().await.len(), 1);
        assert_eq!(store.pending_outbox().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn newer_input_preempts_finalization_and_restarts_model() {
        let store = store_with_text().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = ActorRunner::new(
            store.clone(),
            InjectingLlm {
                store: store.clone(),
                calls: calls.clone(),
            },
            NoTools,
            ManualClock::new(1_000),
            ActorSignals::default(),
            RunnerLimits::default(),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.pending_outbox().await.unwrap()[0].payload,
            OutboxPayload::Text {
                text: "fresh".into()
            }
        );
    }

    #[tokio::test]
    async fn cancellation_signal_interrupts_blocking_model_without_outbox() {
        let store = store_with_text().await;
        let signals = ActorSignals::default();
        let started = Arc::new(Notify::new());
        let runner = ActorRunner::new(
            store.clone(),
            BlockingLlm {
                started: started.clone(),
            },
            NoTools,
            ManualClock::new(1_000),
            signals.clone(),
            RunnerLimits::default(),
        );
        let task = tokio::spawn(async move { runner.run_once("worker").await });
        started.notified().await;
        let accepted = store
            .ingest(
                NewInboundEvent {
                    gateway: "local".into(),
                    external_id: "cancel-1".into(),
                    identity_provider: "local".into(),
                    identity_subject: "owner".into(),
                    kind: EventKind::CancelRequested,
                    audience: Audience::ActorPrivate,
                    payload_json: r#"{"type":"cancel"}"#.into(),
                },
                Timestamp(3),
            )
            .await
            .unwrap();
        signals
            .notify(
                &ActorId::from_string("actor:local:1"),
                accepted.sequence().unwrap(),
            )
            .await;

        assert_eq!(task.await.unwrap().unwrap(), RunOnceOutcome::Cancelled);
        assert!(store.pending_outbox().await.unwrap().is_empty());
        let recovery = ActorRunner::new(
            store,
            FinalLlm,
            NoTools,
            ManualClock::new(1_001),
            signals,
            RunnerLimits::default(),
        );
        assert_eq!(
            recovery.run_once("worker-2").await.unwrap(),
            RunOnceOutcome::Idle
        );
    }

    #[tokio::test]
    async fn model_quantum_yields_and_releases_actor_lease() {
        let store = store_with_text().await;
        let responses = (0..6)
            .map(|index| LlmResponse {
                content: format!("step {index}"),
                tool_calls: vec![LlmToolCall {
                    id: format!("call-{index}"),
                    name: "datetime".into(),
                    arguments: "{}".into(),
                }],
            })
            .collect();
        let mut limits = RunnerLimits::default();
        limits.max_model_steps = 4;
        let runner = ActorRunner::new(
            store.clone(),
            ScriptedLlm {
                responses: Arc::new(Mutex::new(responses)),
            },
            RecordingTools::default(),
            ManualClock::new(1_000),
            ActorSignals::default(),
            limits,
        );

        assert_eq!(
            runner.run_once("worker-1").await.unwrap(),
            RunOnceOutcome::Yielded
        );
        assert!(
            store
                .acquire_ready_actor("worker-2", Timestamp(1_001), Timestamp(2_000))
                .await
                .unwrap()
                .is_some()
        );
    }

    async fn store_with_text() -> SqliteRuntimeStore {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: "actor:local:1".into(),
                        enabled: true,
                        tools: vec!["datetime".into()],
                        identities: vec![LegacyIdentity {
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
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-1",
                    "local",
                    "owner",
                    Audience::ActorPrivate,
                    "hello",
                )
                .unwrap(),
                Timestamp(2),
            )
            .await
            .unwrap();
        store
    }
}
