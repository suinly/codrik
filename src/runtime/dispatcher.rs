use std::{collections::HashMap, future::Future, time::Duration};

use anyhow::Result;
use tokio::sync::watch;

use crate::runtime::{
    model::{ActorId, Clock},
    signals::{ActorDirectorySignals, ActorSignals},
    store::{ActorAdminStore, FailureDisposition, QuantumFailure, QuantumRunner, RuntimeActor},
};

pub struct ActorDispatcherManager<S> {
    store: S,
    directory: ActorDirectorySignals,
}

struct RunningDispatcher {
    actor: RuntimeActor,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Default)]
struct RunningDispatchers(HashMap<ActorId, RunningDispatcher>);

impl Drop for RunningDispatchers {
    fn drop(&mut self) {
        for child in self.0.values() {
            child.task.abort();
        }
    }
}

impl<S> ActorDispatcherManager<S>
where
    S: ActorAdminStore + Clone + Send + Sync + 'static,
{
    pub fn new(store: S, directory: ActorDirectorySignals) -> Self {
        Self { store, directory }
    }

    pub async fn run_with<F, Fut>(
        self,
        mut shutdown: watch::Receiver<bool>,
        make_dispatcher: F,
    ) -> Result<()>
    where
        F: Fn(RuntimeActor, watch::Receiver<bool>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let mut directory = self.directory.subscribe();
        let mut poll = tokio::time::interval(Duration::from_millis(500));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        poll.tick().await;
        let mut running = RunningDispatchers::default();
        loop {
            if *shutdown.borrow() {
                stop_all_dispatchers(&mut running).await?;
                return Ok(());
            }
            reconcile_dispatchers(&self.store, &make_dispatcher, &mut running).await?;
            if *shutdown.borrow() {
                stop_all_dispatchers(&mut running).await?;
                return Ok(());
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        stop_all_dispatchers(&mut running).await?;
                        return Ok(());
                    }
                }
                changed = directory.changed() => {
                    changed.map_err(|_| anyhow::anyhow!("actor directory signal channel closed"))?;
                }
                _ = poll.tick() => {}
            }
        }
    }
}

async fn reconcile_dispatchers<S, F, Fut>(
    store: &S,
    make_dispatcher: &F,
    running: &mut RunningDispatchers,
) -> Result<()>
where
    S: ActorAdminStore,
    F: Fn(RuntimeActor, watch::Receiver<bool>) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let mut desired = enabled_actors(store).await?;
    let stale = running
        .0
        .iter()
        .filter_map(|(id, child)| {
            (desired.get(id) != Some(&child.actor) || child.task.is_finished()).then(|| id.clone())
        })
        .collect::<Vec<_>>();
    if !stale.is_empty() {
        let stopping = stale
            .into_iter()
            .map(|id| {
                running
                    .0
                    .remove(&id)
                    .expect("running dispatcher disappeared")
            })
            .collect::<Vec<_>>();
        for child in &stopping {
            child.shutdown.send_replace(true);
        }
        for child in stopping {
            child
                .task
                .await
                .map_err(|error| anyhow::anyhow!("actor dispatcher task failed: {error}"))??;
        }
        desired = enabled_actors(store).await?;
    }
    for (id, actor) in desired {
        if running.0.contains_key(&id) {
            continue;
        }
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(make_dispatcher(actor.clone(), receiver));
        running.0.insert(
            id,
            RunningDispatcher {
                actor,
                shutdown,
                task,
            },
        );
    }
    Ok(())
}

async fn enabled_actors<S: ActorAdminStore>(store: &S) -> Result<HashMap<ActorId, RuntimeActor>> {
    Ok(store
        .list_actors()
        .await?
        .into_iter()
        .filter(|actor| actor.enabled)
        .map(|actor| (actor.id.clone(), actor))
        .collect())
}

async fn stop_all_dispatchers(running: &mut RunningDispatchers) -> Result<()> {
    for child in running.0.values() {
        child.shutdown.send_replace(true);
    }
    for (_, child) in running.0.drain() {
        child
            .task
            .await
            .map_err(|error| anyhow::anyhow!("actor dispatcher task failed: {error}"))??;
    }
    Ok(())
}

pub struct ActorDispatcher<R, C> {
    actor_id: ActorId,
    owner: String,
    signals: ActorSignals,
    runner: R,
    _clock: C,
}

impl<R, C> ActorDispatcher<R, C>
where
    R: QuantumRunner,
    C: Clock,
{
    pub fn new(
        actor_id: ActorId,
        owner: impl Into<String>,
        signals: ActorSignals,
        runner: R,
        clock: C,
    ) -> Self {
        Self {
            actor_id,
            owner: owner.into(),
            signals,
            runner,
            _clock: clock,
        }
    }

    pub async fn run(&self) -> Result<()> {
        self.run_with_shutdown(watch::channel(false).1).await
    }

    pub async fn run_with_shutdown(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut signal = self.signals.subscribe(&self.actor_id).await;
        let mut poll = tokio::time::interval(Duration::from_millis(500));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        poll.tick().await;
        loop {
            self.dispatch_ready_until_shutdown(&shutdown).await?;
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                changed = signal.changed() => {
                    changed.map_err(|_| anyhow::anyhow!("actor signal channel closed"))?;
                }
                _ = poll.tick() => {}
            }
        }
    }

    pub async fn dispatch_ready(&self) -> Result<()> {
        self.dispatch_ready_until_shutdown(&watch::channel(false).1)
            .await
    }

    async fn dispatch_ready_until_shutdown(&self, shutdown: &watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            match self.runner.run_quantum(&self.actor_id, &self.owner).await {
                Ok(report) => {
                    if matches!(report.outcome, crate::runtime::runner::RunOnceOutcome::Idle) {
                        return Ok(());
                    }
                }
                Err(QuantumFailure::RecoverableWork { disposition }) => match disposition {
                    FailureDisposition::RetryAt(_) => return Ok(()),
                    FailureDisposition::Terminalized => continue,
                },
                Err(QuantumFailure::AuthorityUnavailable(error)) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, VecDeque},
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::sync::{Mutex, Notify, watch};

    use crate::runtime::{
        dispatcher::{ActorDispatcher, ActorDispatcherManager},
        model::{ActorId, ManualClock, Timestamp, WorkItemId},
        runner::RunOnceOutcome,
        signals::{ActorDirectorySignals, ActorSignals},
        sqlite::SqliteRuntimeStore,
        store::{
            ActorAdminStore, QuantumFailure, QuantumProgress, QuantumReport, QuantumRunner,
            RuntimeActor,
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
            ManualClock::new(10),
        )
        .dispatch_ready()
        .await?;
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
            ManualClock::new(0),
        )
        .dispatch_ready()
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "database corrupt");
    }

    #[tokio::test(start_paused = true)]
    async fn manager_runs_enabled_actors_independently_and_stops_disabled_actor() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        for id in ["alice", "bob"] {
            store
                .create_actor(&ActorId::parse_workspace_safe(id)?, Timestamp(1))
                .await?;
        }
        let directory = ActorDirectorySignals::default();
        let running = Arc::new(StdMutex::new(BTreeSet::new()));
        let changed = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let manager = ActorDispatcherManager::new(store.clone(), directory.clone());
        let task = {
            let running = running.clone();
            let changed = changed.clone();
            tokio::spawn(manager.run_with(shutdown_rx, move |actor, mut stop| {
                let running = running.clone();
                let changed = changed.clone();
                async move {
                    running.lock().unwrap().insert(actor.id.to_string());
                    changed.notify_waiters();
                    while !*stop.borrow() && stop.changed().await.is_ok() {}
                    running.lock().unwrap().remove(actor.id.as_str());
                    changed.notify_waiters();
                    Ok(())
                }
            }))
        };
        wait_until(&changed, || running.lock().unwrap().len() == 2).await;
        assert_eq!(
            *running.lock().unwrap(),
            BTreeSet::from(["alice".into(), "bob".into()])
        );

        store
            .set_actor_enabled(&ActorId::parse_workspace_safe("bob")?, false)
            .await?;
        directory.notify();
        wait_until(&changed, || !running.lock().unwrap().contains("bob")).await;
        assert_eq!(*running.lock().unwrap(), BTreeSet::from(["alice".into()]));

        shutdown_tx.send(true)?;
        task.await??;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn tool_change_restarts_dispatcher_only_after_current_quantum() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let alice = ActorId::parse_workspace_safe("alice")?;
        store.create_actor(&alice, Timestamp(1)).await?;
        let directory = ActorDirectorySignals::default();
        let starts = Arc::new(StdMutex::new(Vec::<RuntimeActor>::new()));
        let changed = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let manager = ActorDispatcherManager::new(store.clone(), directory.clone());
        let task = {
            let starts = starts.clone();
            let changed = changed.clone();
            let release_first = release_first.clone();
            tokio::spawn(manager.run_with(shutdown_rx, move |actor, mut stop| {
                let starts = starts.clone();
                let changed = changed.clone();
                let release_first = release_first.clone();
                async move {
                    let first = actor.tools.is_empty();
                    starts.lock().unwrap().push(actor);
                    changed.notify_waiters();
                    while !*stop.borrow() && stop.changed().await.is_ok() {}
                    if first {
                        release_first.notified().await;
                    }
                    Ok(())
                }
            }))
        };
        wait_until(&changed, || starts.lock().unwrap().len() == 1).await;

        store.grant_actor_tool(&alice, "bash").await?;
        directory.notify();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(starts.lock().unwrap().len(), 1);

        release_first.notify_one();
        wait_until(&changed, || starts.lock().unwrap().len() == 2).await;
        assert_eq!(starts.lock().unwrap()[1].tools, vec!["bash"]);

        shutdown_tx.send(true)?;
        task.await??;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn manager_propagates_dispatcher_failure() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        store
            .create_actor(&ActorId::parse_workspace_safe("alice")?, Timestamp(1))
            .await?;
        let manager = ActorDispatcherManager::new(store, ActorDirectorySignals::default());
        let task = tokio::spawn(manager.run_with(watch::channel(false).1, |_, _| async {
            anyhow::bail!("dispatcher failed")
        }));

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(500)).await;
        let error = task.await?.unwrap_err();
        assert!(error.to_string().contains("dispatcher failed"));
        Ok(())
    }

    async fn wait_until(changed: &Notify, ready: impl Fn() -> bool) {
        for _ in 0..100 {
            if ready() {
                return;
            }
            let notified = changed.notified();
            if ready() {
                return;
            }
            notified.await;
        }
        panic!("condition was not reached");
    }
}
