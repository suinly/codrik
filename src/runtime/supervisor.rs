use std::{future::Future, pin::Pin, time::Duration};

use anyhow::{Result, anyhow};
use tokio::{sync::oneshot, task::JoinSet};

type ComponentFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

pub struct Supervisor {
    grace: Duration,
}

impl Supervisor {
    pub fn new(grace: Duration) -> Self {
        Self { grace }
    }
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

pub struct ServeRuntime {
    supervisor: Supervisor,
    components: Vec<(&'static str, ComponentFuture)>,
}

impl ServeRuntime {
    pub fn new(supervisor: Supervisor) -> Self {
        Self {
            supervisor,
            components: Vec::new(),
        }
    }

    pub fn component<F>(&mut self, name: &'static str, future: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.components.push((name, Box::pin(future)));
    }

    pub async fn run_until<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        self.run_until_started(shutdown, || Ok(())).await
    }

    pub async fn run_until_started<F, S>(self, shutdown: F, started: S) -> Result<()>
    where
        F: Future<Output = ()>,
        S: FnOnce() -> Result<()>,
    {
        let mut tasks = JoinSet::new();
        let grace = self.supervisor.grace;
        let mut readiness = Vec::new();
        for (name, future) in self.components {
            let (ready_tx, ready_rx) = oneshot::channel();
            readiness.push((name, ready_rx));
            tasks.spawn(async move {
                let mut future = future;
                let mut ready_tx = Some(ready_tx);
                let result = std::future::poll_fn(|context| match future.as_mut().poll(context) {
                    std::task::Poll::Pending => {
                        if let Some(ready) = ready_tx.take() {
                            let _ = ready.send(());
                        }
                        std::task::Poll::Pending
                    }
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
                })
                .await;
                (name, result)
            });
        }
        for (name, ready) in readiness {
            tokio::select! {
                result = ready => {
                    if result.is_err() {
                        let error = anyhow!("{name} exited before readiness");
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return Err(error);
                    }
                }
                completed = tasks.join_next() => {
                    let error = unexpected_exit(completed);
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(error);
                }
            }
        }
        started()?;
        tokio::pin!(shutdown);
        tokio::select! {
            _ = &mut shutdown => Self::drain(grace, &mut tasks).await,
            completed = tasks.join_next() => {
                let error = unexpected_exit(completed);
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                Err(error)
            }
        }
    }

    async fn drain(
        grace_duration: Duration,
        tasks: &mut JoinSet<(&'static str, Result<()>)>,
    ) -> Result<()> {
        let grace = async {
            while let Some(completed) = tasks.join_next().await {
                match completed {
                    Ok((_, Ok(()))) => {}
                    Ok((name, Err(error))) => {
                        return Err(anyhow!("{name} failed during shutdown: {error:#}"));
                    }
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => return Err(anyhow!("runtime shutdown task failed: {error}")),
                }
            }
            Ok(())
        };
        match tokio::time::timeout(grace_duration, grace).await {
            Ok(result) => result,
            Err(_) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                Ok(())
            }
        }
    }
}

fn unexpected_exit(
    completed: Option<std::result::Result<(&'static str, Result<()>), tokio::task::JoinError>>,
) -> anyhow::Error {
    match completed {
        Some(Ok((name, Ok(())))) => anyhow!("{name} exited unexpectedly"),
        Some(Ok((name, Err(error)))) => anyhow!("{name} exited unexpectedly: {error:#}"),
        Some(Err(error)) => anyhow!("runtime component task failed: {error}"),
        None => anyhow!("runtime has no live components"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use anyhow::{Result, bail};

    use super::{ServeRuntime, Supervisor};

    struct CancelMarker(Arc<AtomicBool>);

    impl Drop for CancelMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unexpected_component_exit_stops_siblings_and_returns_error() -> Result<()> {
        let ipc_cancelled = Arc::new(AtomicBool::new(false));
        let outbox_cancelled = Arc::new(AtomicBool::new(false));
        let mut runtime = ServeRuntime::new(Supervisor::new(Duration::from_secs(30)));
        let ipc_marker = CancelMarker(ipc_cancelled.clone());
        runtime.component("ipc", async move {
            let _marker = ipc_marker;
            pending::<()>().await;
            Ok(())
        });
        let outbox_marker = CancelMarker(outbox_cancelled.clone());
        runtime.component("outbox", async move {
            let _marker = outbox_marker;
            pending::<()>().await;
            Ok(())
        });
        runtime.component("dispatcher", async { bail!("dispatcher failed") });

        let error = runtime.run_until(pending()).await.unwrap_err();
        assert!(error.to_string().contains("dispatcher exited"));
        assert!(ipc_cancelled.load(Ordering::SeqCst));
        assert!(outbox_cancelled.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_shutdown_waits_at_most_thirty_seconds() -> Result<()> {
        let mut runtime = ServeRuntime::new(Supervisor::new(Duration::from_secs(30)));
        runtime.component("stuck", pending::<Result<()>>());
        let ready = Arc::new(tokio::sync::Notify::new());
        let observed = ready.clone();
        let task = tokio::spawn(async move {
            runtime
                .run_until_started(async {}, move || {
                    observed.notify_one();
                    Ok(())
                })
                .await
        });
        ready.notified().await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn readiness_requires_every_component_to_return_pending_on_first_poll() -> Result<()> {
        let ready = Arc::new(AtomicBool::new(false));
        let observed = ready.clone();
        let mut runtime = ServeRuntime::new(Supervisor::new(Duration::ZERO));
        runtime.component("live", pending::<Result<()>>());
        let task = tokio::spawn(async move {
            runtime
                .run_until_started(async {}, move || {
                    observed.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
        });
        task.await??;
        assert!(ready.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn component_that_exits_ok_on_first_poll_fails_before_readiness() {
        let ready = Arc::new(AtomicBool::new(false));
        let observed = ready.clone();
        let mut runtime = ServeRuntime::new(Supervisor::default());
        runtime.component("early", async { Ok(()) });
        let error = runtime
            .run_until_started(pending(), move || {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("early exited"));
        assert!(!ready.load(Ordering::SeqCst));
    }
}
