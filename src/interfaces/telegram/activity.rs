use std::{collections::HashMap, sync::Mutex, time::Duration};

use anyhow::Result;
use tokio::{
    sync::{broadcast, watch},
    time::Instant,
};

use crate::{
    interfaces::telegram::api::{
        EditMessageText, SendChatAction, SendMessage, TelegramApi, TelegramChatAction,
    },
    llm::client::AgentActivityEvent,
    runtime::{
        gateway::DeliveryRoute,
        gateway_activity::{GatewayActivity, GatewayActivityEvent},
        model::WorkItemId,
    },
};

const TYPING_INTERVAL: Duration = Duration::from_secs(4);
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
const STATUS_TICK_INTERVAL: Duration = Duration::from_secs(10);
const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_DESCRIPTION: &str = "Работаю над задачей";
const MAX_STATUS_DESCRIPTION_CHARS: usize = 240;

type ActivityKey = (WorkItemId, String, String);

struct ActivityState {
    route: DeliveryRoute,
    started_at: Instant,
    typing: bool,
    next_typing_at: Instant,
    description: String,
    status_message_id: Option<i64>,
    description_dirty: bool,
    next_description_update_at: Instant,
    next_status_tick_at: Instant,
}

impl ActivityState {
    fn new(route: DeliveryRoute, now: Instant) -> Self {
        Self {
            route,
            started_at: now,
            typing: false,
            next_typing_at: now,
            description: DEFAULT_DESCRIPTION.into(),
            status_message_id: None,
            description_dirty: false,
            next_description_update_at: now + STATUS_UPDATE_INTERVAL,
            next_status_tick_at: now + STATUS_TICK_INTERVAL,
        }
    }
}

pub struct TelegramActivityWorker<A> {
    api: A,
    gateway: String,
    states: Mutex<HashMap<ActivityKey, ActivityState>>,
}

impl<A> TelegramActivityWorker<A>
where
    A: TelegramApi,
{
    pub fn new(api: A, gateway: impl Into<String>) -> Self {
        Self {
            api,
            gateway: gateway.into(),
            states: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run(
        &self,
        mut activity: broadcast::Receiver<GatewayActivity>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut maintenance = tokio::time::interval(MAINTENANCE_INTERVAL);
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                _ = maintenance.tick() => self.maintain().await,
            }
        }
    }

    pub(crate) async fn handle(&self, activity: GatewayActivity) {
        if activity.route.gateway != self.gateway {
            return;
        }
        if matches!(activity.event, GatewayActivityEvent::TextDelta(_)) {
            return;
        }
        let key = (
            activity.work_item_id,
            activity.route.gateway.clone(),
            activity.route.address.clone(),
        );
        let now = Instant::now();
        let mut state = self
            .states
            .lock()
            .expect("Telegram activity states poisoned")
            .remove(&key)
            .unwrap_or_else(|| ActivityState::new(activity.route, now));

        let terminal = match activity.event {
            GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted) => {
                state.typing = true;
                self.send_typing(&state.route).await;
                state.next_typing_at = now + TYPING_INTERVAL;
                None
            }
            GatewayActivityEvent::Activity(AgentActivityEvent::Description(description)) => {
                state.typing = false;
                let description = normalize_description(&description, MAX_STATUS_DESCRIPTION_CHARS);
                if !description.is_empty() && description != state.description {
                    state.description = description;
                    state.description_dirty = state.status_message_id.is_some();
                    state.next_description_update_at = now + STATUS_UPDATE_INTERVAL;
                }
                self.ensure_status(&mut state).await;
                None
            }
            GatewayActivityEvent::Activity(AgentActivityEvent::ToolStarted { .. }) => {
                state.typing = false;
                self.ensure_status(&mut state).await;
                None
            }
            GatewayActivityEvent::Activity(AgentActivityEvent::ToolFinished { .. }) => None,
            GatewayActivityEvent::Activity(AgentActivityEvent::Completed) => {
                Some("Завершил работу")
            }
            GatewayActivityEvent::Activity(AgentActivityEvent::Cancelled) => {
                Some("Работа остановлена")
            }
            GatewayActivityEvent::Activity(AgentActivityEvent::Failed) => {
                Some("Не удалось завершить работу")
            }
            GatewayActivityEvent::TextDelta(_) => unreachable!(),
        };

        if let Some(description) = terminal {
            state.typing = false;
            if let Some(message_id) = state.status_message_id {
                let _ = self
                    .api
                    .edit_message_text(EditMessageText {
                        chat_id: state.route.address,
                        message_id,
                        text: render_status(description, state.started_at.elapsed()),
                    })
                    .await;
            }
        } else {
            self.states
                .lock()
                .expect("Telegram activity states poisoned")
                .insert(key, state);
        }
    }

    pub(crate) async fn maintain(&self) {
        let keys = self
            .states
            .lock()
            .expect("Telegram activity states poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let state = self
                .states
                .lock()
                .expect("Telegram activity states poisoned")
                .remove(&key);
            let Some(mut state) = state else {
                continue;
            };
            let now = Instant::now();
            if state.typing && now >= state.next_typing_at {
                self.send_typing(&state.route).await;
                state.next_typing_at = now + TYPING_INTERVAL;
            }
            let description_due =
                state.description_dirty && now >= state.next_description_update_at;
            let elapsed_due = now >= state.next_status_tick_at;
            if state.status_message_id.is_some() && (description_due || elapsed_due) {
                self.edit_status(&state).await;
                if description_due {
                    state.description_dirty = false;
                    state.next_description_update_at = now + STATUS_UPDATE_INTERVAL;
                }
                if elapsed_due {
                    state.next_status_tick_at = now + STATUS_TICK_INTERVAL;
                }
            }
            self.states
                .lock()
                .expect("Telegram activity states poisoned")
                .insert(key, state);
        }
    }

    async fn send_typing(&self, route: &DeliveryRoute) {
        let _ = self
            .api
            .send_chat_action(SendChatAction {
                chat_id: route.address.clone(),
                action: TelegramChatAction::Typing,
            })
            .await;
    }

    async fn ensure_status(&self, state: &mut ActivityState) {
        if state.status_message_id.is_some() {
            return;
        }
        if let Ok(message) = self
            .api
            .send_message(SendMessage {
                chat_id: state.route.address.clone(),
                text: render_status(&state.description, state.started_at.elapsed()),
                reply_parameters: None,
            })
            .await
        {
            state.status_message_id = Some(message.message_id);
            state.description_dirty = false;
            let now = Instant::now();
            state.next_description_update_at = now + STATUS_UPDATE_INTERVAL;
            state.next_status_tick_at = now + STATUS_TICK_INTERVAL;
        }
    }

    async fn edit_status(&self, state: &ActivityState) {
        let Some(message_id) = state.status_message_id else {
            return;
        };
        let _ = self
            .api
            .edit_message_text(EditMessageText {
                chat_id: state.route.address.clone(),
                message_id,
                text: render_status(&state.description, state.started_at.elapsed()),
            })
            .await;
    }
}

fn normalize_description(description: &str, max_chars: usize) -> String {
    description
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max_chars)
        .collect()
}

fn format_elapsed(elapsed: Duration) -> String {
    let total_seconds = elapsed.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    if minutes == 0 {
        format!("{seconds} сек")
    } else {
        format!("{minutes} мин {seconds} сек")
    }
}

fn render_status(description: &str, elapsed: Duration) -> String {
    format!("{description} — {}", format_elapsed(elapsed))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;

    use super::TelegramActivityWorker;
    use crate::{
        interfaces::telegram::api::{
            EditMessageText, SendChatAction, SendFile, SendMessage, SetWebhook, TelegramApi,
            TelegramApiError, TelegramMessageRef, WebhookInfo,
        },
        llm::client::AgentActivityEvent,
        runtime::{
            gateway::DeliveryRoute,
            gateway_activity::{GatewayActivity, GatewayActivityEvent},
            model::WorkItemId,
        },
    };

    #[derive(Clone, Default)]
    struct ActivityApi {
        actions: Arc<Mutex<Vec<String>>>,
        sent: Arc<Mutex<Vec<String>>>,
        edited: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl TelegramApi for ActivityApi {
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
            assert!(command.reply_parameters.is_none());
            self.sent.lock().unwrap().push(command.text);
            Ok(TelegramMessageRef { message_id: 77 })
        }

        async fn send_chat_action(
            &self,
            command: SendChatAction,
        ) -> std::result::Result<(), TelegramApiError> {
            self.actions.lock().unwrap().push(command.chat_id);
            Ok(())
        }

        async fn edit_message_text(
            &self,
            command: EditMessageText,
        ) -> std::result::Result<(), TelegramApiError> {
            self.edited.lock().unwrap().push(command.text);
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

    fn activity(work_item: &WorkItemId, event: GatewayActivityEvent) -> GatewayActivity {
        GatewayActivity {
            work_item_id: work_item.clone(),
            route: DeliveryRoute::new("telegram:900", "100", None, 4096, 1024).unwrap(),
            event,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn model_step_sends_typing_every_four_seconds_without_messages() {
        let api = ActivityApi::default();
        let worker = TelegramActivityWorker::new(api.clone(), "telegram:900");
        let work = WorkItemId::new();
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted),
            ))
            .await;
        assert_eq!(api.actions.lock().unwrap().len(), 1);

        tokio::time::advance(Duration::from_secs(4)).await;
        worker.maintain().await;

        assert_eq!(api.actions.lock().unwrap().len(), 2);
        assert!(api.sent.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn text_deltas_never_create_or_edit_messages() {
        let api = ActivityApi::default();
        let worker = TelegramActivityWorker::new(api.clone(), "telegram:900");
        worker
            .handle(activity(
                &WorkItemId::new(),
                GatewayActivityEvent::TextDelta("Пр".into()),
            ))
            .await;

        assert!(api.sent.lock().unwrap().is_empty());
        assert!(api.edited.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn tool_run_uses_description_and_updates_elapsed_status() {
        let api = ActivityApi::default();
        let worker = TelegramActivityWorker::new(api.clone(), "telegram:900");
        let work = WorkItemId::new();
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::ModelStepStarted),
            ))
            .await;
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::Description(
                    "Проверяю   конфигурацию".into(),
                )),
            ))
            .await;
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::ToolStarted {
                    name: "bash".into(),
                }),
            ))
            .await;
        assert_eq!(
            *api.sent.lock().unwrap(),
            vec!["Проверяю конфигурацию — 0 сек"]
        );
        tokio::time::advance(Duration::from_secs(4)).await;
        worker.maintain().await;
        assert_eq!(api.actions.lock().unwrap().len(), 1);

        tokio::time::advance(Duration::from_secs(6)).await;
        worker.maintain().await;
        assert_eq!(
            api.edited.lock().unwrap().last().map(String::as_str),
            Some("Проверяю конфигурацию — 10 сек")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tool_status_uses_default_and_terminal_success_copy() {
        let api = ActivityApi::default();
        let worker = TelegramActivityWorker::new(api.clone(), "telegram:900");
        let work = WorkItemId::new();
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::ToolStarted {
                    name: "datetime".into(),
                }),
            ))
            .await;
        assert_eq!(
            *api.sent.lock().unwrap(),
            vec!["Работаю над задачей — 0 сек"]
        );
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::Completed),
            ))
            .await;
        assert_eq!(
            api.edited.lock().unwrap().last().map(String::as_str),
            Some("Завершил работу — 0 сек")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn description_change_is_coalesced_for_two_seconds() {
        let api = ActivityApi::default();
        let worker = TelegramActivityWorker::new(api.clone(), "telegram:900");
        let work = WorkItemId::new();
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::ToolStarted {
                    name: "bash".into(),
                }),
            ))
            .await;
        worker
            .handle(activity(
                &work,
                GatewayActivityEvent::Activity(AgentActivityEvent::Description(
                    "Читаю файлы".into(),
                )),
            ))
            .await;

        tokio::time::advance(Duration::from_secs(1)).await;
        worker.maintain().await;
        assert!(api.edited.lock().unwrap().is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        worker.maintain().await;
        assert_eq!(
            api.edited.lock().unwrap().last().map(String::as_str),
            Some("Читаю файлы — 2 сек")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_status_uses_cancelled_and_failed_copy() {
        for (event, expected) in [
            (AgentActivityEvent::Cancelled, "Работа остановлена — 0 сек"),
            (
                AgentActivityEvent::Failed,
                "Не удалось завершить работу — 0 сек",
            ),
        ] {
            let api = ActivityApi::default();
            let worker = TelegramActivityWorker::new(api.clone(), "telegram:900");
            let work = WorkItemId::new();
            worker
                .handle(activity(
                    &work,
                    GatewayActivityEvent::Activity(AgentActivityEvent::ToolStarted {
                        name: "bash".into(),
                    }),
                ))
                .await;
            worker
                .handle(activity(&work, GatewayActivityEvent::Activity(event)))
                .await;
            assert_eq!(
                api.edited.lock().unwrap().last().map(String::as_str),
                Some(expected)
            );
        }
    }
}
