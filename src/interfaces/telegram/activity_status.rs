use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use teloxide::{
    Bot,
    prelude::Requester,
    types::{ChatId, MessageId},
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, interval},
};

use crate::llm::client::{AgentActivityEvent, AgentActivitySink};

const DEFAULT_DESCRIPTION: &str = "Работаю над задачей";
const MAX_STATUS_DESCRIPTION_CHARS: usize = 240;
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
const STATUS_TICK_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminalStatus {
    Success,
    Cancelled,
    Failed,
}

impl TerminalStatus {
    fn description(self) -> &'static str {
        match self {
            Self::Success => "Завершил работу",
            Self::Cancelled => "Работа остановлена",
            Self::Failed => "Не удалось завершить работу",
        }
    }
}

enum StatusCommand {
    Description(String),
    Terminal(TerminalStatus),
}

#[async_trait]
trait StatusApi: Send + Sync {
    async fn send(&self, text: String) -> Result<MessageId>;
    async fn edit(&self, message_id: MessageId, text: String) -> Result<()>;
}

struct BotStatusApi {
    bot: Bot,
    chat_id: ChatId,
}

#[async_trait]
impl StatusApi for BotStatusApi {
    async fn send(&self, text: String) -> Result<MessageId> {
        Ok(self.bot.send_message(self.chat_id, text).await?.id)
    }

    async fn edit(&self, message_id: MessageId, text: String) -> Result<()> {
        self.bot
            .edit_message_text(self.chat_id, message_id, text)
            .await?;
        Ok(())
    }
}

pub(super) struct TelegramActivityStatus {
    sender: mpsc::UnboundedSender<StatusCommand>,
    task: JoinHandle<()>,
}

impl TelegramActivityStatus {
    pub(super) fn start(bot: Bot, chat_id: ChatId) -> Self {
        Self::start_with_api(
            Arc::new(BotStatusApi { bot, chat_id }),
            STATUS_UPDATE_INTERVAL,
            STATUS_TICK_INTERVAL,
        )
    }

    fn start_with_api(
        api: Arc<dyn StatusApi>,
        update_interval: Duration,
        tick_interval: Duration,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_status_worker(
            api,
            receiver,
            update_interval,
            tick_interval,
        ));
        Self { sender, task }
    }

    pub(super) async fn finish(self, fallback: TerminalStatus) {
        let _ = self.sender.send(StatusCommand::Terminal(fallback));
        drop(self.sender);
        let _ = self.task.await;
    }
}

#[async_trait]
impl AgentActivitySink for TelegramActivityStatus {
    async fn on_activity(&mut self, event: AgentActivityEvent) {
        let command = match event {
            AgentActivityEvent::Description(description) => {
                Some(StatusCommand::Description(description))
            }
            AgentActivityEvent::Completed => Some(StatusCommand::Terminal(TerminalStatus::Success)),
            AgentActivityEvent::Cancelled => {
                Some(StatusCommand::Terminal(TerminalStatus::Cancelled))
            }
            AgentActivityEvent::Failed => Some(StatusCommand::Terminal(TerminalStatus::Failed)),
            AgentActivityEvent::ModelStepStarted
            | AgentActivityEvent::ToolStarted { .. }
            | AgentActivityEvent::ToolFinished { .. } => None,
        };
        if let Some(command) = command {
            let _ = self.sender.send(command);
        }
    }
}

async fn run_status_worker(
    api: Arc<dyn StatusApi>,
    mut receiver: mpsc::UnboundedReceiver<StatusCommand>,
    status_update_interval: Duration,
    status_tick_interval: Duration,
) {
    let started_at = Instant::now();
    let mut description = DEFAULT_DESCRIPTION.to_string();
    let mut dirty = false;
    let mut disabled = false;
    let message_id = match api
        .send(render_status(&description, started_at.elapsed()))
        .await
    {
        Ok(message_id) => Some(message_id),
        Err(error) => {
            eprintln!("Telegram activity status send failed: {error:#}");
            disabled = true;
            None
        }
    };
    let mut update_interval = interval(status_update_interval);
    update_interval.tick().await;
    let mut elapsed_interval = interval(status_tick_interval);
    elapsed_interval.tick().await;

    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(StatusCommand::Description(candidate)) => {
                    let normalized = normalize_description(&candidate, MAX_STATUS_DESCRIPTION_CHARS);
                    if !normalized.is_empty() && normalized != description {
                        description = normalized;
                        dirty = true;
                    }
                }
                Some(StatusCommand::Terminal(terminal)) => {
                    if let Some(message_id) = message_id {
                        let text = render_status(terminal.description(), started_at.elapsed());
                        if let Err(error) = api.edit(message_id, text).await {
                            eprintln!("Telegram activity terminal status edit failed: {error:#}");
                        }
                    }
                    break;
                }
                None => break,
            },
            _ = update_interval.tick(), if dirty && !disabled => {
                if let Some(message_id) = message_id {
                    let text = render_status(&description, started_at.elapsed());
                    if let Err(error) = api.edit(message_id, text).await {
                        eprintln!("Telegram activity status edit failed: {error:#}");
                        disabled = true;
                    }
                }
                dirty = false;
            }
            _ = elapsed_interval.tick(), if !disabled => {
                if let Some(message_id) = message_id {
                    let text = render_status(&description, started_at.elapsed());
                    if let Err(error) = api.edit(message_id, text).await {
                        eprintln!("Telegram activity elapsed status edit failed: {error:#}");
                        disabled = true;
                    }
                }
            }
        }
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
    use std::{sync::Arc, time::Duration};

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use teloxide::types::MessageId;
    use tokio::{sync::Mutex, time::sleep};

    use crate::llm::client::{AgentActivityEvent, AgentActivitySink};

    use super::{
        StatusApi, TelegramActivityStatus, TerminalStatus, format_elapsed, normalize_description,
        render_status,
    };

    #[derive(Default)]
    struct FakeStatusApi {
        sent: Mutex<Vec<String>>,
        edited: Mutex<Vec<String>>,
        fail: bool,
    }

    #[async_trait]
    impl StatusApi for FakeStatusApi {
        async fn send(&self, text: String) -> Result<MessageId> {
            if self.fail {
                bail!("send failed");
            }
            self.sent.lock().await.push(text);
            Ok(MessageId(7))
        }

        async fn edit(&self, _message_id: MessageId, text: String) -> Result<()> {
            if self.fail {
                bail!("edit failed");
            }
            self.edited.lock().await.push(text);
            Ok(())
        }
    }

    #[test]
    fn normalizes_description_into_one_bounded_line() {
        assert_eq!(
            normalize_description("  Проверяю проект\nи запускаю тесты  ", 80),
            "Проверяю проект и запускаю тесты"
        );
        assert_eq!(normalize_description("abcdef", 5), "abcde");
    }

    #[test]
    fn renders_human_elapsed_time() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "0 сек");
        assert_eq!(format_elapsed(Duration::from_secs(102)), "1 мин 42 сек");
        assert_eq!(
            render_status("Проверяю результат", Duration::from_secs(102)),
            "Проверяю результат — 1 мин 42 сек"
        );
    }

    #[tokio::test]
    async fn coalesces_activity_and_finishes_the_same_message() {
        let api = Arc::new(FakeStatusApi::default());
        let mut status = TelegramActivityStatus::start_with_api(
            api.clone(),
            Duration::from_millis(5),
            Duration::from_secs(60),
        );
        tokio::task::yield_now().await;
        status
            .on_activity(AgentActivityEvent::Description(
                "  Проверяю проект\nи запускаю тесты  ".to_string(),
            ))
            .await;
        status
            .on_activity(AgentActivityEvent::Description(
                "Проверяю результат".to_string(),
            ))
            .await;
        sleep(Duration::from_millis(10)).await;
        status.finish(TerminalStatus::Success).await;

        assert_eq!(api.sent.lock().await.len(), 1);
        let edits = api.edited.lock().await;
        assert!(
            edits
                .iter()
                .any(|text| text.starts_with("Проверяю результат —"))
        );
        assert!(
            edits
                .last()
                .is_some_and(|text| text.starts_with("Завершил работу —"))
        );
    }

    #[tokio::test]
    async fn api_failure_does_not_escape_activity_status() {
        let api = Arc::new(FakeStatusApi {
            fail: true,
            ..Default::default()
        });
        let mut status = TelegramActivityStatus::start_with_api(
            api,
            Duration::from_millis(1),
            Duration::from_millis(1),
        );
        status
            .on_activity(AgentActivityEvent::Description("Работаю".to_string()))
            .await;
        status.finish(TerminalStatus::Failed).await;
    }
}
