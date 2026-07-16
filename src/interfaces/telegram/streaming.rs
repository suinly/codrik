use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};

use anyhow::Result;
use tokio::sync::{broadcast, watch};

use crate::{
    interfaces::telegram::api::{EditMessageText, ReplyParameters, SendMessage, TelegramApi},
    llm::client::AgentActivityEvent,
    runtime::{
        gateway::DeliveryRoute,
        gateway_activity::{GatewayActivity, GatewayActivityEvent},
        model::{Clock, WorkItemId},
        store::GatewayStreamStore,
    },
};

const EDIT_INTERVAL: Duration = Duration::from_secs(1);

type StreamKey = (WorkItemId, String, String);
type StreamLockMap = HashMap<String, Weak<tokio::sync::Mutex<()>>>;

static STREAM_LOCKS: OnceLock<Mutex<StreamLockMap>> = OnceLock::new();

pub(crate) async fn lock_gateway_stream(
    work_item: &WorkItemId,
    route: &DeliveryRoute,
) -> tokio::sync::OwnedMutexGuard<()> {
    let key = format!(
        "{}\0{}\0{}",
        work_item.as_str(),
        route.gateway,
        route.address
    );
    let lock = {
        let mut locks = STREAM_LOCKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("Telegram stream locks poisoned");
        let lock = locks
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(())));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    };
    lock.lock_owned().await
}

struct StreamState {
    route: DeliveryRoute,
    remote_message_id: Option<i64>,
    buffer: String,
    last_sent: String,
    last_edit: tokio::time::Instant,
}

pub struct TelegramStreamingWorker<S, A, C> {
    store: S,
    api: A,
    clock: C,
    gateway: String,
    streams: Mutex<HashMap<StreamKey, StreamState>>,
}

impl<S, A, C> TelegramStreamingWorker<S, A, C>
where
    S: GatewayStreamStore,
    A: TelegramApi,
    C: Clock,
{
    pub fn new(store: S, api: A, clock: C, gateway: impl Into<String>) -> Self {
        Self {
            store,
            api,
            clock,
            gateway: gateway.into(),
            streams: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run(
        &self,
        mut activity: broadcast::Receiver<GatewayActivity>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut flush = tokio::time::interval(Duration::from_millis(200));
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                },
                received = activity.recv() => match received {
                    Ok(event) => self.handle(event).await,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
                _ = flush.tick() => self.flush_ready().await,
            }
        }
    }

    async fn flush_ready(&self) {
        let keys = self
            .streams
            .lock()
            .expect("stream state poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let state = self
                .streams
                .lock()
                .expect("stream state poisoned")
                .remove(&key);
            let Some(mut state) = state else {
                continue;
            };
            let _guard = lock_gateway_stream(&key.0, &state.route).await;
            if state.last_edit.elapsed() >= EDIT_INTERVAL {
                self.edit(&key.0, &mut state).await;
            }
            self.streams
                .lock()
                .expect("stream state poisoned")
                .insert(key, state);
        }
    }

    pub(crate) async fn handle(&self, activity: GatewayActivity) {
        if activity.route.gateway != self.gateway {
            return;
        }
        let key = (
            activity.work_item_id.clone(),
            activity.route.gateway.clone(),
            activity.route.address.clone(),
        );
        let _guard = lock_gateway_stream(&activity.work_item_id, &activity.route).await;
        let existing = self
            .streams
            .lock()
            .expect("stream state poisoned")
            .remove(&key);
        let mut state = match existing {
            Some(state) => state,
            None => {
                let remote_message_id = self
                    .store
                    .resolve_gateway_stream(&activity.work_item_id, &activity.route)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|value| value.parse::<i64>().ok());
                StreamState {
                    route: activity.route.clone(),
                    remote_message_id,
                    buffer: String::new(),
                    last_sent: String::new(),
                    last_edit: tokio::time::Instant::now()
                        .checked_sub(EDIT_INTERVAL)
                        .unwrap_or_else(tokio::time::Instant::now),
                }
            }
        };

        let terminal = matches!(
            activity.event,
            GatewayActivityEvent::Activity(
                AgentActivityEvent::Completed
                    | AgentActivityEvent::Cancelled
                    | AgentActivityEvent::Failed
            )
        );
        if !terminal {
            self.ensure_message(&activity.work_item_id, &mut state)
                .await;
        }
        if let GatewayActivityEvent::TextDelta(delta) = activity.event {
            state.buffer.push_str(&delta);
            if state.last_edit.elapsed() >= EDIT_INTERVAL {
                self.edit(&activity.work_item_id, &mut state).await;
            }
        }
        if terminal {
            if state.last_edit.elapsed() >= EDIT_INTERVAL {
                self.edit(&activity.work_item_id, &mut state).await;
            }
            let _ = self
                .store
                .close_gateway_stream(&activity.work_item_id, &activity.route, self.clock.now())
                .await;
        } else {
            self.streams
                .lock()
                .expect("stream state poisoned")
                .insert(key, state);
        }
    }

    async fn ensure_message(&self, work_item: &WorkItemId, state: &mut StreamState) {
        if state.remote_message_id.is_some() {
            return;
        }
        let reply_parameters = state
            .route
            .reply_to_external_id
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            .map(|message_id| ReplyParameters { message_id });
        let Ok(message) = self
            .api
            .send_message(SendMessage {
                chat_id: state.route.address.clone(),
                text: "Thinking…".into(),
                reply_parameters,
            })
            .await
        else {
            return;
        };
        state.remote_message_id = Some(message.message_id);
        state.last_sent = "Thinking…".into();
        state.last_edit = tokio::time::Instant::now();
        let _ = self
            .store
            .upsert_gateway_stream(
                work_item,
                &state.route,
                &message.message_id.to_string(),
                self.clock.now(),
            )
            .await;
    }

    async fn edit(&self, work_item: &WorkItemId, state: &mut StreamState) {
        let Some(message_id) = state.remote_message_id else {
            return;
        };
        if !matches!(
            self.store
                .resolve_gateway_stream(work_item, &state.route)
                .await,
            Ok(Some(ref stored)) if stored == &message_id.to_string()
        ) {
            return;
        }
        if state.buffer.is_empty() {
            return;
        }
        let text = state
            .buffer
            .chars()
            .take(state.route.max_text_chars)
            .collect::<String>();
        if text == state.last_sent {
            return;
        }
        if self
            .api
            .edit_message_text(EditMessageText {
                chat_id: state.route.address.clone(),
                message_id,
                text: text.clone(),
            })
            .await
            .is_ok()
        {
            state.last_sent = text;
            state.last_edit = tokio::time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;

    use super::TelegramStreamingWorker;
    use crate::{
        interfaces::telegram::api::{
            EditMessageText, SendFile, SendMessage, SetWebhook, TelegramApi, TelegramApiError,
            TelegramMessageRef, WebhookInfo,
        },
        llm::client::AgentActivityEvent,
        runtime::{
            gateway::DeliveryRoute,
            gateway_activity::{GatewayActivity, GatewayActivityEvent},
            model::{ManualClock, Timestamp, WorkItemId},
            store::GatewayStreamStore,
        },
    };

    #[derive(Clone, Default)]
    struct MemoryStreamStore {
        remote: Arc<Mutex<Option<String>>>,
        closes: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl GatewayStreamStore for MemoryStreamStore {
        async fn upsert_gateway_stream(
            &self,
            _work_item: &WorkItemId,
            _route: &DeliveryRoute,
            remote_message_id: &str,
            _now: Timestamp,
        ) -> Result<()> {
            *self.remote.lock().unwrap() = Some(remote_message_id.into());
            Ok(())
        }

        async fn resolve_gateway_stream(
            &self,
            _work_item: &WorkItemId,
            _route: &DeliveryRoute,
        ) -> Result<Option<String>> {
            Ok(self.remote.lock().unwrap().clone())
        }

        async fn close_gateway_stream(
            &self,
            _work_item: &WorkItemId,
            _route: &DeliveryRoute,
            _now: Timestamp,
        ) -> Result<()> {
            *self.closes.lock().unwrap() += 1;
            *self.remote.lock().unwrap() = None;
            Ok(())
        }

        async fn claim_gateway_stream_for_final(
            &self,
            _work_item: &WorkItemId,
            _route: &DeliveryRoute,
            _now: Timestamp,
        ) -> Result<Option<String>> {
            Ok(self.remote.lock().unwrap().take())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingApi {
        sends: Arc<Mutex<Vec<String>>>,
        edits: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl TelegramApi for RecordingApi {
        async fn get_me(
            &self,
        ) -> std::result::Result<crate::interfaces::telegram::types::TelegramBot, TelegramApiError>
        {
            unreachable!()
        }

        async fn set_webhook(
            &self,
            _command: SetWebhook,
        ) -> std::result::Result<(), TelegramApiError> {
            unreachable!()
        }

        async fn get_webhook_info(&self) -> std::result::Result<WebhookInfo, TelegramApiError> {
            unreachable!()
        }

        async fn send_message(
            &self,
            command: SendMessage,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            self.sends.lock().unwrap().push(command.text);
            Ok(TelegramMessageRef { message_id: 77 })
        }

        async fn send_chat_action(
            &self,
            _command: crate::interfaces::telegram::api::SendChatAction,
        ) -> std::result::Result<(), TelegramApiError> {
            Ok(())
        }

        async fn edit_message_text(
            &self,
            command: EditMessageText,
        ) -> std::result::Result<(), TelegramApiError> {
            self.edits.lock().unwrap().push(command.text);
            Ok(())
        }

        async fn send_photo(
            &self,
            _command: SendFile,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }

        async fn send_document(
            &self,
            _command: SendFile,
        ) -> std::result::Result<TelegramMessageRef, TelegramApiError> {
            unreachable!()
        }
    }

    fn activity(work_item_id: WorkItemId, event: GatewayActivityEvent) -> GatewayActivity {
        GatewayActivity {
            work_item_id,
            route: DeliveryRoute::new("telegram:900", "100", Some("42".into()), 4096, 1024)
                .unwrap(),
            event,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn activity_creates_persisted_stream_and_throttles_edits() -> Result<()> {
        let store = MemoryStreamStore::default();
        let api = RecordingApi::default();
        let worker = TelegramStreamingWorker::new(
            store.clone(),
            api.clone(),
            ManualClock::new(10),
            "telegram:900",
        );
        let work_item = WorkItemId::new();

        worker
            .handle(activity(
                work_item.clone(),
                GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted),
            ))
            .await;
        worker
            .handle(activity(
                work_item.clone(),
                GatewayActivityEvent::TextDelta("hel".into()),
            ))
            .await;
        assert_eq!(*api.sends.lock().unwrap(), vec!["Thinking…"]);
        assert!(api.edits.lock().unwrap().is_empty());
        assert_eq!(*store.remote.lock().unwrap(), Some("77".into()));

        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        worker
            .handle(activity(
                work_item.clone(),
                GatewayActivityEvent::TextDelta("lo".into()),
            ))
            .await;
        assert_eq!(*api.edits.lock().unwrap(), vec!["hello"]);

        worker
            .handle(activity(
                work_item,
                GatewayActivityEvent::Activity(AgentActivityEvent::Completed),
            ))
            .await;
        assert_eq!(*store.closes.lock().unwrap(), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn final_claim_prevents_late_stream_edit() -> Result<()> {
        let store = MemoryStreamStore::default();
        let api = RecordingApi::default();
        let worker = TelegramStreamingWorker::new(
            store.clone(),
            api.clone(),
            ManualClock::new(10),
            "telegram:900",
        );
        let work_item = WorkItemId::new();
        let route = DeliveryRoute::new("telegram:900", "100", Some("42".into()), 4096, 1024)?;

        worker
            .handle(GatewayActivity {
                work_item_id: work_item.clone(),
                route: route.clone(),
                event: GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted),
            })
            .await;
        assert_eq!(
            store
                .claim_gateway_stream_for_final(&work_item, &route, Timestamp(11))
                .await?,
            Some("77".into())
        );
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        worker
            .handle(GatewayActivity {
                work_item_id: work_item,
                route,
                event: GatewayActivityEvent::TextDelta("late partial".into()),
            })
            .await;

        assert!(api.edits.lock().unwrap().is_empty());
        Ok(())
    }
}
