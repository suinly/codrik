use std::time::Duration;

use anyhow::Result;

use crate::runtime::{
    model::{ActorId, Clock},
    signals::ActorSignals,
    store::{FailureDisposition, FailureStore, QuantumFailure, QuantumProgress, QuantumRunner},
};

pub struct ActorDispatcher<R, S, C> {
    actor_id: ActorId,
    owner: String,
    signals: ActorSignals,
    runner: R,
    failures: S,
    clock: C,
}

impl<R, S, C> ActorDispatcher<R, S, C>
where
    R: QuantumRunner,
    S: FailureStore,
    C: Clock,
{
    pub fn new(
        actor_id: ActorId,
        owner: impl Into<String>,
        signals: ActorSignals,
        runner: R,
        failures: S,
        clock: C,
    ) -> Self {
        Self {
            actor_id,
            owner: owner.into(),
            signals,
            runner,
            failures,
            clock,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut signal = self.signals.subscribe(&self.actor_id).await;
        let mut poll = tokio::time::interval(Duration::from_millis(500));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        poll.tick().await;
        loop {
            self.dispatch_ready().await?;
            tokio::select! {
                changed = signal.changed() => {
                    changed.map_err(|_| anyhow::anyhow!("actor signal channel closed"))?;
                }
                _ = poll.tick() => {}
            }
        }
    }

    pub async fn dispatch_ready(&self) -> Result<()> {
        loop {
            match self.runner.run_quantum(&self.actor_id, &self.owner).await {
                Ok(report) => {
                    if report.progress != QuantumProgress::None {
                        if let Some(work) = report.work_item_id.as_ref() {
                            self.failures
                                .record_progress(work, self.clock.now())
                                .await?;
                        }
                    }
                    if matches!(report.outcome, crate::runtime::runner::RunOnceOutcome::Idle) {
                        return Ok(());
                    }
                }
                Err(QuantumFailure::RecoverableWork {
                    work_item_id,
                    message,
                }) => {
                    match self
                        .failures
                        .record_failure(&work_item_id, &message, self.clock.now())
                        .await?
                    {
                        FailureDisposition::RetryAt(_) => return Ok(()),
                        FailureDisposition::Terminalized => continue,
                    }
                }
                Err(QuantumFailure::AuthorityUnavailable(error)) => return Err(error),
            }
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
        time::Duration,
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use crate::runtime::{
        dispatcher::ActorDispatcher,
        model::{ActorId, ManualClock, Timestamp, WorkItemId},
        runner::RunOnceOutcome,
        signals::ActorSignals,
        store::{
            FailureDisposition, FailureStore, QuantumFailure, QuantumProgress, QuantumReport,
            QuantumRunner,
        },
    };

    #[derive(Clone)]
    struct ScriptedRunner {
        calls: Arc<AtomicUsize>,
        actors: Arc<Mutex<Vec<ActorId>>>,
        script: Arc<Mutex<VecDeque<std::result::Result<QuantumReport, QuantumFailure>>>>,
    }

    #[async_trait]
    impl QuantumRunner for ScriptedRunner {
        async fn run_quantum(
            &self,
            actor: &ActorId,
            _: &str,
        ) -> std::result::Result<QuantumReport, QuantumFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.actors.lock().await.push(actor.clone());
            self.script
                .lock()
                .await
                .pop_front()
                .unwrap_or(Ok(QuantumReport {
                    work_item_id: None,
                    outcome: RunOnceOutcome::Idle,
                    progress: QuantumProgress::None,
                }))
        }
    }

    #[derive(Clone, Default)]
    struct RecordingFailures(Arc<Mutex<Vec<(WorkItemId, Timestamp, bool)>>>);

    #[async_trait]
    impl FailureStore for RecordingFailures {
        async fn record_failure(
            &self,
            work: &WorkItemId,
            _: &str,
            now: Timestamp,
        ) -> Result<FailureDisposition> {
            self.0.lock().await.push((work.clone(), now, false));
            Ok(FailureDisposition::RetryAt(now.plus_millis(1_000)))
        }
        async fn record_progress(&self, work: &WorkItemId, now: Timestamp) -> Result<()> {
            self.0.lock().await.push((work.clone(), now, true));
            Ok(())
        }
    }

    fn report(
        outcome: RunOnceOutcome,
        progress: QuantumProgress,
    ) -> std::result::Result<QuantumReport, QuantumFailure> {
        Ok(QuantumReport {
            work_item_id: Some(WorkItemId::from_string("work-1")),
            outcome,
            progress,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn lost_notification_is_recovered_by_500ms_poll_for_only_configured_actor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let actors = Arc::new(Mutex::new(Vec::new()));
        let runner = ScriptedRunner {
            calls: calls.clone(),
            actors: actors.clone(),
            script: Arc::new(Mutex::new(VecDeque::from([report(
                RunOnceOutcome::Idle,
                QuantumProgress::None,
            )]))),
        };
        let dispatcher = ActorDispatcher::new(
            ActorId::from_string("configured"),
            "owner",
            ActorSignals::default(),
            runner,
            RecordingFailures::default(),
            ManualClock::new(0),
        );
        let task = tokio::spawn(async move { dispatcher.run().await });
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(499)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            actors
                .lock()
                .await
                .iter()
                .all(|actor| actor.as_str() == "configured")
        );
        task.abort();
    }

    #[tokio::test]
    async fn real_progress_resets_failure_history_but_replay_does_not() -> Result<()> {
        let failures = RecordingFailures::default();
        let runner = ScriptedRunner {
            calls: Arc::new(AtomicUsize::new(0)),
            actors: Arc::new(Mutex::new(Vec::new())),
            script: Arc::new(Mutex::new(VecDeque::from([
                report(RunOnceOutcome::Yielded, QuantumProgress::None),
                report(RunOnceOutcome::Yielded, QuantumProgress::KnownToolOutcome),
                report(RunOnceOutcome::Idle, QuantumProgress::None),
            ]))),
        };
        ActorDispatcher::new(
            ActorId::from_string("actor"),
            "owner",
            ActorSignals::default(),
            runner,
            failures.clone(),
            ManualClock::new(10),
        )
        .dispatch_ready()
        .await?;
        let records = failures.0.lock().await;
        assert_eq!(records.len(), 1);
        assert!(records[0].2);
        Ok(())
    }

    #[tokio::test]
    async fn authority_error_terminates_dispatcher() {
        let runner = ScriptedRunner {
            calls: Arc::new(AtomicUsize::new(0)),
            actors: Arc::new(Mutex::new(Vec::new())),
            script: Arc::new(Mutex::new(VecDeque::from([Err(
                QuantumFailure::AuthorityUnavailable(anyhow::anyhow!("database corrupt")),
            )]))),
        };
        let error = ActorDispatcher::new(
            ActorId::from_string("actor"),
            "owner",
            ActorSignals::default(),
            runner,
            RecordingFailures::default(),
            ManualClock::new(0),
        )
        .dispatch_ready()
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "database corrupt");
    }
}
