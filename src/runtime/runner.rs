use std::{sync::Arc, time::Duration};

use anyhow::Result;

use crate::{
    agent::{
        message::Message,
        tool::{ToolCallContext, ToolExecutor},
        tool_observation,
    },
    llm::client::{
        AgentActivityEvent, LlmRequest, LlmStreamClient, LlmStreamEvent, LlmStreamSink,
        LlmToolCall, RunContext,
    },
    runtime::{
        artifacts::ArtifactManager,
        model::{AttemptId, Clock, EventKind, OutboxId},
        signals::ActorSignals,
        store::{
            AttemptOutcome, AttemptRecovery, CheckpointRun, FailureFence, FinalizeOutcome,
            FinalizeRun, NewOutboxIntent, NewToolAttempt, OutboxPayload, QuantumFailure,
            QuantumProgress, QuantumReport, QuantumRunner, RuntimeStore,
        },
        stream_hub::RuntimeEventPublisher,
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
    events: Arc<dyn RuntimeEventPublisher>,
    limits: RunnerLimits,
    artifacts: ArtifactManager<S, C>,
}

impl<L, T, S, C> ActorRunner<L, T, S, C>
where
    L: LlmStreamClient + Send + Sync,
    T: ToolExecutor + Send + Sync,
    S: RuntimeStore + Clone + 'static,
    C: Clock,
{
    pub fn new(
        llm: L,
        tools: T,
        signals: ActorSignals,
        events: Arc<dyn RuntimeEventPublisher>,
        limits: RunnerLimits,
        artifacts: ArtifactManager<S, C>,
    ) -> Self {
        let store = artifacts.store();
        let clock = artifacts.clock();
        Self {
            store,
            llm,
            tools,
            clock,
            signals,
            events,
            limits,
            artifacts,
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
        let mut fence = None;
        let mut progress = QuantumProgress::None;
        let result = self.run_leased(&lease, &mut fence, &mut progress).await;
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
        failure_fence: &mut Option<FailureFence>,
        progress: &mut QuantumProgress,
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
        *failure_fence = Some(FailureFence::from(&run));
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
        let mut tool_steps = 0;
        for attempt in self.store.unresolved_attempts(&run).await? {
            let recovery = self.store.recover_attempt(&attempt.id).await?;
            let outcome = match recovery {
                AttemptRecovery::MayInvoke => {
                    if tool_steps >= self.limits.max_tool_steps {
                        return Ok(RunOnceOutcome::Yielded);
                    }
                    tool_steps += 1;
                    self.store
                        .mark_attempt_running(&run, &attempt.id, self.clock.now())
                        .await?;
                    self.events.publish_activity(
                        &run.request_ids,
                        AgentActivityEvent::ToolStarted {
                            name: attempt.tool_name.clone(),
                        },
                    );
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
                        Ok(execution) => match self
                            .artifacts
                            .stage_execution(&run, &attempt.id, execution)
                            .await
                        {
                            Ok(execution) => AttemptOutcome::Succeeded { execution },
                            Err(error) => AttemptOutcome::FailedKnown {
                                message: error.to_string(),
                            },
                        },
                        Err(error) => AttemptOutcome::FailedKnown {
                            message: error.to_string(),
                        },
                    };
                    if !matches!(outcome, AttemptOutcome::Succeeded { .. }) {
                        self.store
                            .finish_attempt(&run, &attempt.id, outcome.clone(), self.clock.now())
                            .await?;
                    }
                    self.events.publish_activity(
                        &run.request_ids,
                        AgentActivityEvent::ToolFinished {
                            name: attempt.tool_name.clone(),
                            succeeded: matches!(outcome, AttemptOutcome::Succeeded { .. }),
                        },
                    );
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
            let mut recovered_messages = Vec::new();
            if !messages.iter().any(|message| {
                message
                    .tool_calls
                    .iter()
                    .any(|call| call.id == assistant.tool_calls[0].id)
            }) {
                recovered_messages.push(assistant);
            }
            recovered_messages.push(tool_result);
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
            advance_progress(progress, QuantumProgress::KnownToolOutcome);
            messages.extend(recovered_messages);
        }
        let mut signal_receiver = self.signals.subscribe(&lease.actor_id).await;
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
                        advance_progress(progress, QuantumProgress::Finalized);
                        self.events
                            .publish_activity(&run.request_ids, AgentActivityEvent::Cancelled);
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
            self.events
                .publish_activity(&run.request_ids, AgentActivityEvent::ModelStepStarted);
            let response = {
                let mut sink = RuntimeLlmSink {
                    requests: &run.request_ids,
                    publisher: self.events.as_ref(),
                };
                let generation = self.llm.stream(request, &mut sink, &context);
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
                            run.lease = current_lease.clone();
                            *failure_fence = Some(FailureFence::from(&run));
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
                                        advance_progress(progress, QuantumProgress::Finalized);
                                        self.events.publish_activity(
                                            &run.request_ids,
                                            AgentActivityEvent::Cancelled,
                                        );
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
                    FinalizeOutcome::Completed => {
                        advance_progress(progress, QuantumProgress::Finalized);
                        self.events
                            .publish_activity(&run.request_ids, AgentActivityEvent::Completed);
                        return Ok(RunOnceOutcome::Completed);
                    }
                    FinalizeOutcome::Preempted { .. } => {
                        run = self
                            .store
                            .attach_next_run(
                                &current_lease,
                                self.limits.max_events,
                                self.clock.now(),
                            )
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("preempted run was not resumable"))?;
                        *failure_fence = Some(FailureFence::from(&run));
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

            if !response.content.is_empty() {
                self.events.publish_activity(
                    &run.request_ids,
                    AgentActivityEvent::Description(response.content.clone()),
                );
            }

            let assistant =
                Message::assistant_tool_calls(response.content, response.tool_calls.clone());
            let mut prepared = Vec::new();
            for tool_call in response.tool_calls {
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
                prepared.push((tool_call, attempt));
            }
            self.store
                .checkpoint_run(
                    CheckpointRun {
                        run: run.clone(),
                        incorporated_event_ids: Vec::new(),
                        checkpointed_attempt_ids: Vec::new(),
                        messages: vec![assistant.clone()],
                    },
                    self.clock.now(),
                )
                .await?;
            advance_progress(progress, QuantumProgress::ModelCheckpoint);
            messages.push(assistant);

            let mut checkpoint_messages = Vec::new();
            let mut checkpointed_attempt_ids = Vec::new();
            let mut budget_exhausted = false;
            for (tool_call, attempt) in prepared {
                if tool_steps >= self.limits.max_tool_steps {
                    budget_exhausted = true;
                    break;
                }
                tool_steps += 1;
                self.store
                    .mark_attempt_running(&run, &attempt.id, self.clock.now())
                    .await?;
                self.events.publish_activity(
                    &run.request_ids,
                    AgentActivityEvent::ToolStarted {
                        name: tool_call.name.clone(),
                    },
                );
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
                    Ok(execution) => match self
                        .artifacts
                        .stage_execution(&run, &attempt.id, execution)
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
                    },
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
                let succeeded = matches!(outcome, AttemptOutcome::Succeeded { .. });
                self.events.publish_activity(
                    &run.request_ids,
                    AgentActivityEvent::ToolFinished {
                        name: tool_call.name.clone(),
                        succeeded,
                    },
                );
                if !succeeded {
                    self.store
                        .finish_attempt(&run, &attempt.id, outcome, self.clock.now())
                        .await?;
                }
                checkpointed_attempt_ids.push(attempt.id);
                checkpoint_messages.push(Message::tool_result(tool_call.id, observation));
            }
            if !checkpointed_attempt_ids.is_empty() {
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
                advance_progress(progress, QuantumProgress::KnownToolOutcome);
                messages.extend(checkpoint_messages);
            }
            if budget_exhausted {
                return Ok(RunOnceOutcome::Yielded);
            }
        }
        Ok(RunOnceOutcome::Yielded)
    }
}

#[async_trait::async_trait]
impl<L, T, S, C> QuantumRunner for ActorRunner<L, T, S, C>
where
    L: LlmStreamClient + Send + Sync,
    T: ToolExecutor + Send + Sync,
    S: RuntimeStore + Clone + 'static,
    C: Clock,
{
    async fn run_quantum(
        &self,
        actor: &crate::runtime::model::ActorId,
        owner: &str,
    ) -> std::result::Result<QuantumReport, QuantumFailure> {
        let now = self.clock.now();
        let lease_until = match duration_millis(self.limits.lease_duration) {
            Ok(millis) => now.plus_millis(millis),
            Err(error) => return Err(QuantumFailure::AuthorityUnavailable(error)),
        };
        let lease = self
            .store
            .acquire_ready_actor_for(actor, owner, now, lease_until)
            .await
            .map_err(QuantumFailure::AuthorityUnavailable)?;
        let Some(lease) = lease else {
            return Ok(QuantumReport {
                work_item_id: None,
                outcome: RunOnceOutcome::Idle,
                progress: QuantumProgress::None,
            });
        };
        let mut fence = None;
        let mut progress = QuantumProgress::None;
        let result = self.run_leased(&lease, &mut fence, &mut progress).await;
        let classified = match result {
            Ok(outcome) => {
                let bookkeeping = if progress != QuantumProgress::None {
                    if let Some(fence) = fence.as_ref() {
                        self.store.record_progress(fence, &self.clock).await
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                };
                bookkeeping
                    .map_err(QuantumFailure::AuthorityUnavailable)
                    .map(|()| QuantumReport {
                        work_item_id: fence.as_ref().map(|fence| fence.work_item_id.clone()),
                        outcome,
                        progress,
                    })
            }
            Err(error) if fence.is_some() && !authority_error(&error) => self
                .store
                .record_failure(
                    fence.as_ref().expect("checked above"),
                    &error.to_string(),
                    progress,
                    &self.clock,
                )
                .await
                .map_err(QuantumFailure::AuthorityUnavailable)
                .and_then(|disposition| Err(QuantumFailure::RecoverableWork { disposition })),
            Err(error) => Err(QuantumFailure::AuthorityUnavailable(error)),
        };
        let release = self
            .store
            .release_lease(&lease)
            .await
            .map_err(QuantumFailure::AuthorityUnavailable);
        match (classified, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(report), Ok(())) => Ok(report),
        }
    }
}

fn authority_error(error: &anyhow::Error) -> bool {
    crate::runtime::sqlite::is_authority_failure(error)
}

fn advance_progress(current: &mut QuantumProgress, committed: QuantumProgress) {
    fn rank(progress: QuantumProgress) -> u8 {
        match progress {
            QuantumProgress::None => 0,
            QuantumProgress::ModelCheckpoint => 1,
            QuantumProgress::KnownToolOutcome => 2,
            QuantumProgress::Finalized => 3,
        }
    }
    if rank(committed) > rank(*current) {
        *current = committed;
    }
}

struct RuntimeLlmSink<'a> {
    requests: &'a [crate::runtime::model::RequestId],
    publisher: &'a dyn RuntimeEventPublisher,
}

#[async_trait::async_trait]
impl LlmStreamSink for RuntimeLlmSink<'_> {
    async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()> {
        if let LlmStreamEvent::TextDelta(delta) = event {
            self.publisher.publish_text(self.requests, &delta);
        }
        Ok(())
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
    use sha2::Digest;
    use tokio::sync::{Mutex, Notify};

    use crate::{
        agent::tool::{
            FileArtifact, Tool, ToolArtifact, ToolCallContext, ToolCapabilities, ToolExecution,
            ToolExecutor,
        },
        auth::{LegacyActor, LegacyAuthorizationSnapshot, LegacyIdentity},
        llm::client::{
            LlmClient, LlmRequest, LlmResponse, LlmStreamClient, LlmStreamEvent, LlmStreamSink,
            LlmToolCall, LlmToolCallDelta, RunContext,
        },
        runtime::{
            artifacts::{ArtifactManager, TestPause},
            ipc::protocol::{ActivityEvent, ServerEventBody},
            model::{ActorId, AttemptId, Audience, EventKind, ManualClock, RequestId, Timestamp},
            runner::{ActorRunner, RunOnceOutcome, RunnerLimits},
            signals::ActorSignals,
            sqlite::SqliteRuntimeStore,
            store::{
                AttemptOutcome, AttemptRecovery, CheckpointStore, DispatchStore, FailureStore,
                IngressStore, LocalIngressStore, LocalSubmission, NewInboundEvent, OutboxPayload,
                QuantumProgress, QuantumRunner, RuntimeAuthorizationStore, ToolAttemptStore,
            },
            stream_hub::{NoopRuntimeEventPublisher, StreamHub},
        },
    };

    fn test_artifacts(
        store: &SqliteRuntimeStore,
        clock: ManualClock,
    ) -> ArtifactManager<SqliteRuntimeStore, ManualClock> {
        ArtifactManager::new(
            std::env::temp_dir().join(format!("codrik-runner-test-{}", uuid::Uuid::new_v4())),
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

    #[async_trait]
    impl LlmStreamClient for ScriptedLlm {
        async fn stream(
            &self,
            request: LlmRequest,
            _sink: &mut dyn LlmStreamSink,
            context: &RunContext,
        ) -> Result<LlmResponse> {
            self.generate(request, context).await
        }
    }

    #[derive(Clone, Default)]
    struct RecordingTools {
        attempts: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone)]
    struct ArtifactTools {
        source: std::path::PathBuf,
        attempts: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone)]
    struct InjectingLlm {
        store: SqliteRuntimeStore,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone, Default)]
    struct ToolThenErrorLlm {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl LlmClient for ToolThenErrorLlm {
        async fn generate(
            &self,
            _request: LlmRequest,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(LlmResponse {
                    content: "tool first".into(),
                    tool_calls: vec![LlmToolCall {
                        id: "known-before-error".into(),
                        name: "datetime".into(),
                        arguments: "{}".into(),
                    }],
                })
            } else {
                anyhow::bail!("later model failure")
            }
        }
    }

    #[async_trait]
    impl LlmStreamClient for ToolThenErrorLlm {
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
    struct BlockingLlm {
        started: Arc<Notify>,
    }

    #[derive(Clone)]
    struct StreamingFinalLlm {
        deltas: Vec<String>,
        content: String,
    }

    #[async_trait]
    impl LlmStreamClient for StreamingFinalLlm {
        async fn stream(
            &self,
            _request: LlmRequest,
            sink: &mut dyn LlmStreamSink,
            _context: &RunContext,
        ) -> Result<LlmResponse> {
            for delta in &self.deltas {
                sink.on_event(LlmStreamEvent::TextDelta(delta.clone()))
                    .await?;
            }
            sink.on_event(LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                index: 0,
                id: Some("provider-only".into()),
                name: Some("hidden".into()),
                arguments: Some("{}".into()),
            }))
            .await?;
            Ok(LlmResponse {
                content: self.content.clone(),
                tool_calls: Vec::new(),
            })
        }
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
    impl LlmStreamClient for BlockingLlm {
        async fn stream(
            &self,
            request: LlmRequest,
            _sink: &mut dyn LlmStreamSink,
            context: &RunContext,
        ) -> Result<LlmResponse> {
            self.generate(request, context).await
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
    impl LlmStreamClient for InjectingLlm {
        async fn stream(
            &self,
            request: LlmRequest,
            _sink: &mut dyn LlmStreamSink,
            context: &RunContext,
        ) -> Result<LlmResponse> {
            self.generate(request, context).await
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

    #[async_trait]
    impl ToolExecutor for ArtifactTools {
        fn definitions(&self) -> Vec<Tool> {
            vec![Tool::new("datetime", "file", Default::default())]
        }

        fn capabilities(&self, name: &str) -> Option<ToolCapabilities> {
            (name == "datetime").then(ToolCapabilities::read_only)
        }

        async fn execute(
            &self,
            _name: &str,
            _arguments: &str,
            context: &ToolCallContext,
        ) -> Result<ToolExecution> {
            self.attempts.lock().await.push(context.attempt_id.clone());
            Ok(ToolExecution {
                observation: "created report".into(),
                artifacts: vec![ToolArtifact::File(FileArtifact {
                    path: self.source.clone(),
                    display_name: "report.txt".into(),
                    media_type: "text/plain".into(),
                    caption: Some("report".into()),
                })],
            })
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
            FinalLlm,
            NoTools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        let outbox = store.outbox_intents().await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].payload,
            OutboxPayload::Text {
                text: "done".into()
            }
        );
    }

    #[tokio::test]
    async fn stream_fans_out_text_for_attached_requests_but_finalizes_complete_response() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: "actor:local:1".into(),
                        enabled: true,
                        tools: Vec::new(),
                        identities: Vec::new(),
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        let request_id = RequestId::new();
        let second_request_id = RequestId::new();
        let hub = StreamHub::default();
        let mut subscription = hub.subscribe(request_id.clone()).unwrap();
        let mut second_subscription = hub.subscribe(second_request_id.clone()).unwrap();
        store
            .submit_for_actor(
                &ActorId::from_string("actor:local:1"),
                LocalSubmission {
                    request_id,
                    text: "hello".into(),
                    prompt_sha256:
                        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
                },
                Timestamp(2),
            )
            .await
            .unwrap();
        store
            .submit_for_actor(
                &ActorId::from_string("actor:local:1"),
                LocalSubmission {
                    request_id: second_request_id,
                    text: "more context".into(),
                    prompt_sha256:
                        "d0e6e76cb7f5008f6c9ea7c788e43922c120eb68e334b14dc7d34d41dba1c200".into(),
                },
                Timestamp(3),
            )
            .await
            .unwrap();
        let runner = ActorRunner::new(
            StreamingFinalLlm {
                deltas: vec!["partial".into()],
                content: "authoritative final".into(),
            },
            NoTools,
            ActorSignals::default(),
            Arc::new(hub),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        let events = std::iter::from_fn(|| subscription.try_recv()).collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(
            &event.body,
            ServerEventBody::TextDelta { delta, .. } if delta == "partial"
        )));
        assert!(!events.iter().any(|event| matches!(
            &event.body,
            ServerEventBody::Activity {
                event: ActivityEvent::Description { .. },
                ..
            }
        )));
        let second_events =
            std::iter::from_fn(|| second_subscription.try_recv()).collect::<Vec<_>>();
        assert!(second_events.iter().any(|event| matches!(
            &event.body,
            ServerEventBody::TextDelta { delta, .. } if delta == "partial"
        )));
        assert_eq!(
            store.outbox_intents().await.unwrap()[0].payload,
            OutboxPayload::Text {
                text: "authoritative final".into()
            }
        );
    }

    #[tokio::test]
    async fn stream_terminal_full_response_does_not_create_description_or_gap() {
        let (store, request) = store_with_local_text().await;
        let hub = StreamHub::with_limits(8, 16, 64);
        let mut subscription = hub.subscribe(request).unwrap();
        let full_response = "x".repeat(32);
        let runner = ActorRunner::new(
            StreamingFinalLlm {
                deltas: vec!["ok".into()],
                content: full_response.clone(),
            },
            NoTools,
            ActorSignals::default(),
            Arc::new(hub),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        let events = std::iter::from_fn(|| subscription.try_recv()).collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(
            &event.body,
            ServerEventBody::TextDelta { delta, .. } if delta == "ok"
        )));
        assert!(!events.iter().any(|event| matches!(
            &event.body,
            ServerEventBody::Activity {
                event: ActivityEvent::Description { .. },
                ..
            } | ServerEventBody::StreamGap { .. }
        )));
        assert_eq!(
            store.outbox_intents().await.unwrap()[0].payload,
            OutboxPayload::Text {
                text: full_response
            }
        );
    }

    #[tokio::test]
    async fn stream_intermediate_description_precedes_tool_activity_and_final_has_none() {
        let (store, request) = store_with_local_text().await;
        let hub = StreamHub::default();
        let mut subscription = hub.subscribe(request).unwrap();
        let runner = ActorRunner::new(
            ScriptedLlm {
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
            },
            RecordingTools::default(),
            ActorSignals::default(),
            Arc::new(hub),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        let activity = std::iter::from_fn(|| subscription.try_recv())
            .filter_map(|event| match event.body {
                ServerEventBody::Activity { event, .. } => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            activity,
            vec![
                ActivityEvent::ModelStepStarted,
                ActivityEvent::Description {
                    description: "checking".into(),
                },
                ActivityEvent::ToolStarted {
                    name: "datetime".into(),
                },
                ActivityEvent::ToolFinished {
                    name: "datetime".into(),
                    succeeded: true,
                },
                ActivityEvent::ModelStepStarted,
                ActivityEvent::Completed,
            ]
        );
    }

    #[tokio::test]
    async fn stream_overflow_never_fails_authoritative_finalization() {
        let (store, request) = store_with_local_text().await;
        let hub = StreamHub::with_limits(3, 16, 64);
        let mut subscription = hub.subscribe(request).unwrap();
        let runner = ActorRunner::new(
            StreamingFinalLlm {
                deltas: vec!["partial".into(), "overflow".into()],
                content: "authoritative final".into(),
            },
            NoTools,
            ActorSignals::default(),
            Arc::new(hub),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        assert!(
            std::iter::from_fn(|| subscription.try_recv())
                .any(|event| matches!(event.body, ServerEventBody::StreamGap { .. }))
        );
        assert_eq!(
            store.outbox_intents().await.unwrap()[0].payload,
            OutboxPayload::Text {
                text: "authoritative final".into()
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
            llm,
            tools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        assert_eq!(attempts.lock().await.len(), 1);
        assert_eq!(store.outbox_intents().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn production_quantum_reports_finalized_progress() {
        let store = store_with_text().await;
        let runner = ActorRunner::new(
            FinalLlm,
            NoTools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        let report = runner
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker")
            .await
            .unwrap();
        assert_eq!(report.outcome, RunOnceOutcome::Completed);
        assert_eq!(report.progress, QuantumProgress::Finalized);
    }

    #[tokio::test]
    async fn production_quantum_reports_model_checkpoint_before_tool_execution() {
        let store = store_with_text().await;
        let tools = RecordingTools::default();
        let attempts = tools.attempts.clone();
        let mut limits = RunnerLimits::default();
        limits.max_tool_steps = 0;
        let runner = ActorRunner::new(
            ScriptedLlm {
                responses: Arc::new(Mutex::new(VecDeque::from([LlmResponse {
                    content: "checking".into(),
                    tool_calls: vec![LlmToolCall {
                        id: "call-checkpoint".into(),
                        name: "datetime".into(),
                        arguments: "{}".into(),
                    }],
                }]))),
            },
            tools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            limits,
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        let report = runner
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker")
            .await
            .unwrap();
        assert_eq!(report.outcome, RunOnceOutcome::Yielded);
        assert_eq!(report.progress, QuantumProgress::ModelCheckpoint);
        assert!(attempts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn production_quantum_reports_known_tool_outcome() {
        let store = store_with_text().await;
        let mut limits = RunnerLimits::default();
        limits.max_model_steps = 1;
        let runner = ActorRunner::new(
            ScriptedLlm {
                responses: Arc::new(Mutex::new(VecDeque::from([LlmResponse {
                    content: "checking".into(),
                    tool_calls: vec![LlmToolCall {
                        id: "call-known".into(),
                        name: "datetime".into(),
                        arguments: "{}".into(),
                    }],
                }]))),
            },
            RecordingTools::default(),
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            limits,
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        let report = runner
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker")
            .await
            .unwrap();
        assert_eq!(report.outcome, RunOnceOutcome::Yielded);
        assert_eq!(report.progress, QuantumProgress::KnownToolOutcome);
    }

    #[tokio::test]
    async fn production_quantum_reports_none_for_incorporation_replay() {
        let store = store_with_text().await;
        let mut limits = RunnerLimits::default();
        limits.max_model_steps = 0;
        let runner = ActorRunner::new(
            FinalLlm,
            NoTools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            limits,
            test_artifacts(&store, ManualClock::new(1_000)),
        );
        let actor = ActorId::from_string("actor:local:1");

        let initial = runner.run_quantum(&actor, "worker-1").await.unwrap();
        let replay = runner.run_quantum(&actor, "worker-2").await.unwrap();
        assert_eq!(initial.progress, QuantumProgress::None);
        assert_eq!(replay.progress, QuantumProgress::None);
    }

    #[tokio::test]
    async fn zero_tool_budget_never_executes_prepared_calls_during_recovery() {
        let store = store_with_text().await;
        let tools = RecordingTools::default();
        let attempts = tools.attempts.clone();
        let mut initial_limits = RunnerLimits::default();
        initial_limits.max_model_steps = 1;
        initial_limits.max_tool_steps = 0;
        let initial = ActorRunner::new(
            ScriptedLlm {
                responses: Arc::new(Mutex::new(VecDeque::from([LlmResponse {
                    content: "queued".into(),
                    tool_calls: vec![LlmToolCall {
                        id: "zero-budget".into(),
                        name: "datetime".into(),
                        arguments: "{}".into(),
                    }],
                }]))),
            },
            tools.clone(),
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            initial_limits,
            test_artifacts(&store, ManualClock::new(1_000)),
        );
        initial
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker-1")
            .await
            .unwrap();

        let mut recovery_limits = RunnerLimits::default();
        recovery_limits.max_model_steps = 0;
        recovery_limits.max_tool_steps = 0;
        let recovery = ActorRunner::new(
            FinalLlm,
            tools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            recovery_limits,
            test_artifacts(&store, ManualClock::new(1_001)),
        );
        recovery
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker-2")
            .await
            .unwrap();

        assert!(attempts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn prepared_calls_resume_one_per_quantum_with_stable_attempt_ids() {
        let store = store_with_text().await;
        let tools = RecordingTools::default();
        let attempts = tools.attempts.clone();
        let mut initial_limits = RunnerLimits::default();
        initial_limits.max_model_steps = 1;
        initial_limits.max_tool_steps = 1;
        let initial = ActorRunner::new(
            ScriptedLlm {
                responses: Arc::new(Mutex::new(VecDeque::from([LlmResponse {
                    content: "three calls".into(),
                    tool_calls: (0..3)
                        .map(|index| LlmToolCall {
                            id: format!("budget-{index}"),
                            name: "datetime".into(),
                            arguments: "{}".into(),
                        })
                        .collect(),
                }]))),
            },
            tools.clone(),
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            initial_limits,
            test_artifacts(&store, ManualClock::new(1_000)),
        );
        initial
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker-1")
            .await
            .unwrap();
        assert_eq!(attempts.lock().await.len(), 1);

        for (quantum, expected) in [(2, 2), (3, 3), (4, 3)] {
            let mut limits = RunnerLimits::default();
            limits.max_model_steps = 0;
            limits.max_tool_steps = 1;
            let recovery = ActorRunner::new(
                FinalLlm,
                tools.clone(),
                ActorSignals::default(),
                Arc::new(NoopRuntimeEventPublisher),
                limits,
                test_artifacts(&store, ManualClock::new(1_000 + quantum)),
            );
            recovery
                .run_quantum(
                    &ActorId::from_string("actor:local:1"),
                    &format!("worker-{quantum}"),
                )
                .await
                .unwrap();
            assert_eq!(attempts.lock().await.len(), expected);
        }
        let attempts = attempts.lock().await;
        assert_eq!(
            attempts
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }

    async fn seed_four_failures(store: &SqliteRuntimeStore) -> crate::runtime::model::WorkItemId {
        let lease = store
            .acquire_ready_actor("seed", Timestamp(10), Timestamp(1_000))
            .await
            .unwrap()
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(11))
            .await
            .unwrap()
            .unwrap();
        store
            .checkpoint_run(
                crate::runtime::store::CheckpointRun {
                    run: run.clone(),
                    incorporated_event_ids: run.source_event_ids.clone(),
                    checkpointed_attempt_ids: vec![],
                    messages: vec![],
                },
                Timestamp(12),
            )
            .await
            .unwrap();
        for now in 100..104 {
            store
                .record_failure(
                    &crate::runtime::store::FailureFence::from(&run),
                    "seed failure",
                    QuantumProgress::None,
                    &ManualClock::new(now),
                )
                .await
                .unwrap();
        }
        store.release_lease(&lease).await.unwrap();
        run.work_item_id
    }

    #[tokio::test]
    async fn model_checkpoint_then_tool_start_error_records_first_new_failure() {
        let store = store_with_text().await;
        let work = seed_four_failures(&store).await;
        store.fail_next_tool_start_for_test();
        let mut limits = RunnerLimits::default();
        limits.max_model_steps = 1;
        let runner = ActorRunner::new(
            ScriptedLlm {
                responses: Arc::new(Mutex::new(VecDeque::from([LlmResponse {
                    content: "start tool".into(),
                    tool_calls: vec![LlmToolCall {
                        id: "tool-start-fails".into(),
                        name: "datetime".into(),
                        arguments: "{}".into(),
                    }],
                }]))),
            },
            RecordingTools::default(),
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            limits,
            test_artifacts(&store, ManualClock::new(9_000)),
        );

        let failure = runner
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker")
            .await
            .unwrap_err();
        assert!(matches!(
            failure,
            crate::runtime::store::QuantumFailure::RecoverableWork { .. }
        ));
        assert_eq!(
            store.failure_probe_for_test(&work).await.unwrap(),
            (1, "ready".into(), Some(10_000), 0)
        );
    }

    #[tokio::test]
    async fn known_tool_outcome_then_model_error_records_first_new_failure() {
        let store = store_with_text().await;
        let work = seed_four_failures(&store).await;
        let mut limits = RunnerLimits::default();
        limits.max_model_steps = 2;
        let runner = ActorRunner::new(
            ToolThenErrorLlm::default(),
            RecordingTools::default(),
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            limits,
            test_artifacts(&store, ManualClock::new(9_000)),
        );

        let failure = runner
            .run_quantum(&ActorId::from_string("actor:local:1"), "worker")
            .await
            .unwrap_err();
        assert!(matches!(
            failure,
            crate::runtime::store::QuantumFailure::RecoverableWork { .. }
        ));
        assert_eq!(
            store.failure_probe_for_test(&work).await.unwrap(),
            (1, "ready".into(), Some(10_000), 0)
        );
    }

    #[tokio::test]
    async fn composed_runner_and_gc_share_canonical_path_exclusion() {
        let store = store_with_text().await;
        let root =
            std::env::temp_dir().join(format!("codrik-runner-artifact-{}", uuid::Uuid::new_v4()));
        let source = root.join("source.txt");
        let managed = root.join("managed");
        tokio::fs::create_dir_all(&managed).await.unwrap();
        let managed = tokio::fs::canonicalize(managed).await.unwrap();
        tokio::fs::write(&source, b"runner artifact").await.unwrap();
        let content_hash = format!("{:x}", sha2::Sha256::digest(b"runner artifact"));
        let actor_hash = format!("{:x}", sha2::Sha256::digest(b"actor:local:1"));
        let actor_dir = managed.join(actor_hash);
        tokio::fs::create_dir_all(&actor_dir).await.unwrap();
        let canonical = actor_dir.join(&content_hash);
        tokio::fs::write(&canonical, b"runner artifact")
            .await
            .unwrap();
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let llm = ScriptedLlm {
            responses: Arc::new(Mutex::new(VecDeque::from([
                LlmResponse {
                    content: "creating".into(),
                    tool_calls: vec![LlmToolCall {
                        id: "call-file".into(),
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
        let pause = Arc::new(TestPause::new());
        let artifacts = ArtifactManager::new(&managed, store.clone(), ManualClock::new(1_000))
            .with_test_gc_pause(pause.clone());
        let gc_artifacts = artifacts.clone();
        let gc =
            tokio::spawn(async move { gc_artifacts.collect_garbage(Timestamp(i64::MAX)).await });
        pause.wait_until_entered().await;
        let runner = ActorRunner::new(
            llm,
            ArtifactTools {
                source: source.clone(),
                attempts: attempts.clone(),
            },
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            artifacts,
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        pause.resume();
        gc.await.unwrap().unwrap();
        let id = AttemptId::from_string(attempts.lock().await[0].clone());
        let AttemptRecovery::Terminal(AttemptOutcome::Succeeded { execution }) =
            store.recover_attempt(&id).await.unwrap()
        else {
            panic!("missing durable success")
        };
        assert_eq!(execution.artifacts.len(), 1);
        assert!(execution.artifacts[0].managed_path.starts_with(&managed));
        assert_ne!(execution.artifacts[0].managed_path, source);
        assert_eq!(execution.artifacts[0].sha256.len(), 64);
        assert_eq!(execution.artifacts[0].sha256, content_hash);
        let file_intent = store
            .outbox_intents()
            .await
            .unwrap()
            .into_iter()
            .find(|intent| matches!(intent.payload, OutboxPayload::File { .. }))
            .expect("managed artifact should become a typed immutable intent");
        assert!(matches!(
            file_intent.payload,
            OutboxPayload::File {
                artifact_id,
                managed_path,
                size: 15,
                sha256,
                ..
            } if artifact_id == execution.artifacts[0].id
                && managed_path == execution.artifacts[0].managed_path
                && sha256 == content_hash
        ));
        assert_eq!(
            tokio::fs::read(&execution.artifacts[0].managed_path)
                .await
                .unwrap(),
            b"runner artifact"
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn newer_input_preempts_finalization_and_restarts_model() {
        let store = store_with_text().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = ActorRunner::new(
            InjectingLlm {
                store: store.clone(),
                calls: calls.clone(),
            },
            NoTools,
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
        );

        assert_eq!(
            runner.run_once("worker").await.unwrap(),
            RunOnceOutcome::Completed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.outbox_intents().await.unwrap()[0].payload,
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
            BlockingLlm {
                started: started.clone(),
            },
            NoTools,
            signals.clone(),
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_000)),
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
        assert!(store.outbox_intents().await.unwrap().is_empty());
        let recovery = ActorRunner::new(
            FinalLlm,
            NoTools,
            signals,
            Arc::new(NoopRuntimeEventPublisher),
            RunnerLimits::default(),
            test_artifacts(&store, ManualClock::new(1_001)),
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
        let limits = RunnerLimits {
            max_model_steps: 4,
            ..RunnerLimits::default()
        };
        let runner = ActorRunner::new(
            ScriptedLlm {
                responses: Arc::new(Mutex::new(responses)),
            },
            RecordingTools::default(),
            ActorSignals::default(),
            Arc::new(NoopRuntimeEventPublisher),
            limits,
            test_artifacts(&store, ManualClock::new(1_000)),
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

    async fn store_with_local_text() -> (SqliteRuntimeStore, RequestId) {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let actor = ActorId::from_string("actor:local:1");
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: actor.to_string(),
                        enabled: true,
                        tools: vec!["datetime".into()],
                        identities: Vec::new(),
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        let request = RequestId::new();
        store
            .submit_for_actor(
                &actor,
                LocalSubmission {
                    request_id: request.clone(),
                    text: "hello".into(),
                    prompt_sha256: "00".repeat(32),
                },
                Timestamp(2),
            )
            .await
            .unwrap();
        (store, request)
    }
}
