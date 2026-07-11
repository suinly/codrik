use std::{collections::HashMap, sync::Arc};

use teloxide::types::ChatId;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::llm::client::RunContext;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TelegramSessionKey {
    chat_id: ChatId,
    session_id: String,
}

#[derive(Default)]
struct CoordinatorState {
    next_run_id: u64,
    sessions: HashMap<TelegramSessionKey, SessionRun>,
}

struct SessionRun {
    id: u64,
    context: RunContext,
    execution: Arc<Mutex<()>>,
}

#[derive(Clone, Default)]
pub(super) struct TelegramRunCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
}

pub(super) struct TelegramRunPermit {
    coordinator: TelegramRunCoordinator,
    key: TelegramSessionKey,
    id: u64,
    context: RunContext,
    execution: Arc<Mutex<()>>,
}

impl TelegramRunCoordinator {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) async fn register(
        &self,
        chat_id: ChatId,
        session_id: impl Into<String>,
    ) -> TelegramRunPermit {
        let key = TelegramSessionKey {
            chat_id,
            session_id: session_id.into(),
        };
        let mut state = self.state.lock().await;
        state.next_run_id = state.next_run_id.wrapping_add(1);
        let id = state.next_run_id;
        let context = RunContext::new();
        let execution = state
            .sessions
            .get(&key)
            .map_or_else(|| Arc::new(Mutex::new(())), |run| run.execution.clone());
        if let Some(previous) = state.sessions.insert(
            key.clone(),
            SessionRun {
                id,
                context: context.clone(),
                execution: execution.clone(),
            },
        ) {
            previous.context.cancel();
        }

        TelegramRunPermit {
            coordinator: self.clone(),
            key,
            id,
            context,
            execution,
        }
    }
}

impl TelegramRunPermit {
    pub(super) fn context(&self) -> &RunContext {
        &self.context
    }

    pub(super) async fn enter(&self) -> OwnedMutexGuard<()> {
        self.execution.clone().lock_owned().await
    }

    pub(super) async fn finish(self) {
        let mut state = self.coordinator.state.lock().await;
        if state
            .sessions
            .get(&self.key)
            .is_some_and(|run| run.id == self.id)
        {
            state.sessions.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use teloxide::types::ChatId;
    use tokio::sync::{Barrier, oneshot};

    use super::TelegramRunCoordinator;

    #[tokio::test]
    async fn newer_run_cancels_previous_run_in_same_session() {
        let coordinator = TelegramRunCoordinator::new();
        let first = coordinator.register(ChatId(1), "session-a").await;
        let second = coordinator.register(ChatId(1), "session-a").await;

        assert!(first.context().is_cancelled());
        assert!(!second.context().is_cancelled());
    }

    #[tokio::test]
    async fn runs_in_different_keys_do_not_cancel_each_other() {
        let coordinator = TelegramRunCoordinator::new();
        let first_chat = coordinator.register(ChatId(1), "session-a").await;
        let second_chat = coordinator.register(ChatId(2), "session-a").await;
        let second_session = coordinator.register(ChatId(1), "session-b").await;

        assert!(!first_chat.context().is_cancelled());
        assert!(!second_chat.context().is_cancelled());
        assert!(!second_session.context().is_cancelled());
    }

    #[tokio::test]
    async fn runs_enter_session_in_registration_order() {
        let coordinator = TelegramRunCoordinator::new();
        let first = coordinator.register(ChatId(1), "session-a").await;
        let first_guard = first.enter().await;
        let second = coordinator.register(ChatId(1), "session-a").await;
        let barrier = Arc::new(Barrier::new(2));
        let reached = barrier.clone();
        let (entered_tx, mut entered_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = second.enter().await;
            entered_tx.send(()).unwrap();
            reached.wait().await;
        });

        assert!(entered_rx.try_recv().is_err());
        drop(first_guard);
        barrier.wait().await;
        task.await.unwrap();
    }

    #[tokio::test]
    async fn stale_finish_does_not_remove_newer_run() {
        let coordinator = TelegramRunCoordinator::new();
        let first = coordinator.register(ChatId(1), "session-a").await;
        let second = coordinator.register(ChatId(1), "session-a").await;

        first.finish().await;
        let third = coordinator.register(ChatId(1), "session-a").await;

        assert!(second.context().is_cancelled());
        assert!(!third.context().is_cancelled());
    }
}
