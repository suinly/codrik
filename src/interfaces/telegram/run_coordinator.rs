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

    pub(super) async fn cancel(&self, chat_id: ChatId, session_id: impl Into<String>) -> bool {
        let key = TelegramSessionKey {
            chat_id,
            session_id: session_id.into(),
        };
        let context = self
            .state
            .lock()
            .await
            .sessions
            .get(&key)
            .map(|run| run.context.clone());
        let Some(context) = context else {
            return false;
        };
        context.cancel();
        true
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

    pub(super) async fn complete<F, T>(self, operation: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let output = operation.await;
        self.finish().await;
        output
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use teloxide::types::ChatId;
    use tokio::sync::{Barrier, Mutex, Notify, oneshot};

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
    async fn cancel_stops_current_run_without_registering_replacement() {
        let coordinator = TelegramRunCoordinator::new();
        let permit = coordinator.register(ChatId(1), "session-a").await;

        assert!(coordinator.cancel(ChatId(1), "session-a").await);
        assert!(permit.context().is_cancelled());
        assert!(!coordinator.cancel(ChatId(1), "missing").await);
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

    #[tokio::test]
    async fn complete_keeps_run_registered_until_operation_finishes() {
        let coordinator = TelegramRunCoordinator::new();
        let first = coordinator.register(ChatId(1), "session-a").await;
        let first_context = first.context().clone();
        let replacement_coordinator = coordinator.clone();

        first
            .complete(async move {
                let _second = replacement_coordinator
                    .register(ChatId(1), "session-a")
                    .await;
                assert!(first_context.is_cancelled());
            })
            .await;
    }

    #[tokio::test]
    async fn superseding_run_persists_both_messages_and_only_newest_generates() {
        let coordinator = TelegramRunCoordinator::new();
        let messages = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let answers = Arc::new(Mutex::new(Vec::new()));
        let first_persisted = Arc::new(Notify::new());

        let first = coordinator.register(ChatId(1), "session-a").await;
        let first_context = first.context().clone();
        let first_messages = messages.clone();
        let persisted = first_persisted.clone();
        let first_task = tokio::spawn(async move {
            let _execution = first.enter().await;
            first_messages.lock().await.push("first");
            persisted.notify_one();
            first_context.cancelled().await;
            first.finish().await;
        });

        first_persisted.notified().await;
        let second = coordinator.register(ChatId(1), "session-a").await;
        let second_context = second.context().clone();
        let second_messages = messages.clone();
        let second_requests = requests.clone();
        let second_answers = answers.clone();
        let second_task = tokio::spawn(async move {
            let _execution = second.enter().await;
            second_messages.lock().await.push("second");
            assert!(!second_context.is_cancelled());
            second_requests
                .lock()
                .await
                .push(second_messages.lock().await.clone());
            second_answers.lock().await.push("answer to second");
            second.finish().await;
        });

        first_task.await.unwrap();
        second_task.await.unwrap();

        assert_eq!(messages.lock().await.as_slice(), ["first", "second"]);
        assert_eq!(requests.lock().await.as_slice(), [vec!["first", "second"]]);
        assert_eq!(answers.lock().await.as_slice(), ["answer to second"]);
    }
}
