use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Deserialize;
use teloxide::{
    Bot,
    dispatching::{Dispatcher, UpdateFilterExt},
    payloads::SendMessageSetters,
    prelude::Requester,
    types::{ChatAction, ChatId, Message, MessageId, ParseMode, Update},
};
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant, sleep},
};

mod activity_status;
mod commands;
mod format;
mod run_coordinator;

use activity_status::{TelegramActivityStatus, TerminalStatus};
use commands::{
    answer_session_command, is_command_addressed_to_other_bot, is_start_command, is_stop_command,
};
use format::markdown_to_telegram_markdown_v2;
use run_coordinator::TelegramRunCoordinator;

use crate::{
    app,
    auth::{AuthDecision, AuthorizationStore, AuthorizedActor, GatewayIdentity},
    config::AppConfig,
    llm::client::{
        LlmStreamEvent, LlmStreamSink, RUN_CANCELLED, RunContext, is_run_cancelled_error,
    },
    memory::telegram_sessions::TelegramSessionStore,
};

const DRAFT_UPDATE_INTERVAL: Duration = Duration::from_millis(350);
const MAX_DRAFT_TEXT_CHARS: usize = 4096;

pub async fn run(config: AppConfig) -> Result<()> {
    let token = config
        .telegram
        .as_ref()
        .context("telegram config is missing")?
        .token
        .clone();
    let draft_api = TelegramDraftApi::new(token.clone());
    let auth_store = AuthorizationStore::new(crate::config::codrik_dir()?.join("users.json"));
    let session_store = TelegramSessionStore::new(crate::config::codrik_dir()?.join("sessions"));
    let run_coordinator = TelegramRunCoordinator::new();

    let bot = Bot::new(token);
    let me = bot
        .get_me()
        .await
        .context("failed to initialize telegram bot")?;
    eprintln!(
        "Telegram gateway started for @{}",
        me.user.username.as_deref().unwrap_or("<unknown>")
    );
    let bot_username = me.user.username.clone();

    let handler = Update::filter_message().endpoint(move |bot: Bot, msg: Message| {
        let config = config.clone();
        let draft_api = draft_api.clone();
        let auth_store = auth_store.clone();
        let session_store = session_store.clone();
        let bot_username = bot_username.clone();
        let run_coordinator = run_coordinator.clone();

        async move {
            let Some(text) = msg.text() else {
                return Ok(());
            };

            eprintln!("Telegram text received from chat {}", msg.chat.id);

            let Some(identity) = telegram_identity(&msg) else {
                eprintln!(
                    "Telegram message without sender ignored for chat {}",
                    msg.chat.id
                );
                return Ok(());
            };

            if is_command_addressed_to_other_bot(text, bot_username.as_deref()) {
                return Ok(());
            }

            let answer = if is_start_command(text, bot_username.as_deref()) {
                Some(answer_start_command(&auth_store, identity).await)
            } else {
                match auth_store.authorize(&identity).await {
                    Ok(AuthDecision::Authorized(actor)) => {
                        answer_authorized_message(
                            bot.clone(),
                            &session_store,
                            &msg,
                            text,
                            bot_username.as_deref(),
                            draft_api,
                            TelegramAgentRun {
                                config,
                                actor,
                                coordinator: &run_coordinator,
                            },
                        )
                        .await
                    }
                    Ok(AuthDecision::Denied) => Some(denied_message()),
                    Err(error) => {
                        eprintln!(
                            "Telegram authorization failed for chat {}: {error:#}",
                            msg.chat.id
                        );
                        Some(format!("Gateway error: {error:#}"))
                    }
                }
            };

            let Some(answer) = answer else {
                return Ok(());
            };

            let context = RunContext::new();
            if let Err(error) = send_telegram_answer(&bot, msg.chat.id, answer, &context).await {
                eprintln!(
                    "Telegram send_message failed for chat {}: {error:#}",
                    msg.chat.id
                );
                return Err(error);
            }

            Ok(())
        }
    });

    Dispatcher::builder(bot, handler)
        .distribution_function(telegram_update_distribution)
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;

    Ok(())
}

fn telegram_update_distribution(_update: &Update) -> Option<()> {
    None
}

struct TelegramAgentRun<'a> {
    config: AppConfig,
    actor: AuthorizedActor,
    coordinator: &'a TelegramRunCoordinator,
}

async fn answer_authorized_message(
    bot: Bot,
    session_store: &TelegramSessionStore,
    msg: &Message,
    text: &str,
    bot_username: Option<&str>,
    draft_api: TelegramDraftApi,
    run: TelegramAgentRun<'_>,
) -> Option<String> {
    if is_stop_command(text, bot_username) {
        let session_id = match active_session_id_or_error(session_store, msg.chat.id).await {
            Ok(session_id) => session_id,
            Err(error) => return Some(format!("Gateway error: {error:#}")),
        };
        return Some(
            if run.coordinator.cancel(msg.chat.id, session_id).await {
                "Останавливаю текущую задачу"
            } else {
                "Сейчас нет активной задачи"
            }
            .to_string(),
        );
    }

    if let Some(answer) =
        answer_session_command(session_store, msg.chat.id, text, bot_username).await
    {
        return Some(answer);
    }

    if msg.chat.is_private() {
        answer_private_chat(bot, session_store, msg, text, draft_api, run).await
    } else {
        answer_regular_chat(bot, session_store, msg.chat.id, text, run).await
    }
}

async fn answer_private_chat(
    bot: Bot,
    session_store: &TelegramSessionStore,
    msg: &Message,
    text: &str,
    draft_api: TelegramDraftApi,
    run: TelegramAgentRun<'_>,
) -> Option<String> {
    let session_id = match active_session_id_or_error(session_store, msg.chat.id).await {
        Ok(session_id) => session_id,
        Err(error) => return Some(format!("Gateway error: {error:#}")),
    };
    let permit = run
        .coordinator
        .register(msg.chat.id, session_id.clone())
        .await;
    let _cancellation_watch = cancel_on_ctrl_c(msg.chat.id, permit.context().clone());
    let execution = permit.enter().await;
    if permit.context().is_cancelled() {
        let _ = app::run_once_with_actor_session_in_root_and_context(
            text.to_string(),
            run.config,
            run.actor,
            session_store.session_root(msg.chat.id.0),
            session_id,
            permit.context(),
        )
        .await;
        drop(execution);
        permit.finish().await;
        return None;
    }

    let typing = keep_typing(bot.clone(), msg.chat.id);
    let mut activity = TelegramActivityStatus::start(bot.clone(), msg.chat.id);
    let mut stream = TelegramDraftStream::new(
        draft_api,
        msg.chat.id,
        msg.id,
        typing,
        permit.context().clone(),
    );
    let result = app::run_once_with_actor_session_streaming_and_activity_in_root_and_context(
        text.to_string(),
        run.config,
        run.actor,
        session_store.session_root(msg.chat.id.0),
        session_id,
        app::AgentRunSinks {
            output: &mut stream,
            activity: &mut activity,
        },
        permit.context(),
    )
    .await;
    stream.finish().await;
    activity
        .finish(terminal_status(&result, permit.context()))
        .await;

    let answer = answer_or_gateway_error(msg.chat.id, result, permit.context());
    drop(execution);
    let context = permit.context().clone();
    permit
        .complete(async {
            if let Some(answer) = answer
                && let Err(error) = send_telegram_answer(&bot, msg.chat.id, answer, &context).await
            {
                eprintln!(
                    "Telegram send_message failed for chat {}: {error:#}",
                    msg.chat.id
                );
            }
        })
        .await;
    None
}

async fn answer_regular_chat(
    bot: Bot,
    session_store: &TelegramSessionStore,
    chat_id: ChatId,
    text: &str,
    run: TelegramAgentRun<'_>,
) -> Option<String> {
    let session_id = match active_session_id_or_error(session_store, chat_id).await {
        Ok(session_id) => session_id,
        Err(error) => return Some(format!("Gateway error: {error:#}")),
    };
    let permit = run.coordinator.register(chat_id, session_id.clone()).await;
    let _cancellation_watch = cancel_on_ctrl_c(chat_id, permit.context().clone());
    let execution = permit.enter().await;
    if permit.context().is_cancelled() {
        let _ = app::run_once_with_actor_session_in_root_and_context(
            text.to_string(),
            run.config,
            run.actor,
            session_store.session_root(chat_id.0),
            session_id,
            permit.context(),
        )
        .await;
        drop(execution);
        permit.finish().await;
        return None;
    }

    let mut stream = DiscardingStreamSink;
    let mut activity = TelegramActivityStatus::start(bot.clone(), chat_id);
    let result = app::run_once_with_actor_session_streaming_and_activity_in_root_and_context(
        text.to_string(),
        run.config,
        run.actor,
        session_store.session_root(chat_id.0),
        session_id,
        app::AgentRunSinks {
            output: &mut stream,
            activity: &mut activity,
        },
        permit.context(),
    )
    .await;
    activity
        .finish(terminal_status(&result, permit.context()))
        .await;

    let answer = answer_or_gateway_error(chat_id, result, permit.context());
    drop(execution);
    let context = permit.context().clone();
    permit
        .complete(async {
            if let Some(answer) = answer
                && let Err(error) = send_telegram_answer(&bot, chat_id, answer, &context).await
            {
                eprintln!("Telegram send_message failed for chat {chat_id}: {error:#}");
            }
        })
        .await;
    None
}

async fn active_session_id_or_error(
    session_store: &TelegramSessionStore,
    chat_id: ChatId,
) -> Result<String> {
    session_store
        .active_session_id(chat_id.0)
        .await
        .map_err(|error| {
            eprintln!("Telegram active session lookup failed for chat {chat_id}: {error:#}");
            error
        })
}

async fn answer_start_command(
    auth_store: &AuthorizationStore,
    identity: GatewayIdentity,
) -> String {
    match auth_store.start(identity).await {
        Ok(AuthDecision::Authorized(_)) => "Доступ к Кодрику включен. Пиши задачу.".to_string(),
        Ok(AuthDecision::Denied) => {
            "Заявка на доступ сохранена. Попроси владельца включить тебе доступ.".to_string()
        }
        Err(error) => format!("Gateway error: {error:#}"),
    }
}

fn denied_message() -> String {
    "Доступ к Кодрику не выдан. Отправь /start и попроси владельца включить тебе доступ."
        .to_string()
}

fn telegram_identity(msg: &Message) -> Option<GatewayIdentity> {
    telegram_identity_from_sender(
        msg.from
            .as_ref()
            .map(|user| (user.id.0, user.username.clone())),
    )
}

fn telegram_identity_from_sender(sender: Option<(u64, Option<String>)>) -> Option<GatewayIdentity> {
    let (id, username) = sender?;
    Some(GatewayIdentity::new("telegram", id.to_string(), username))
}

fn answer_or_gateway_error(
    chat_id: ChatId,
    result: Result<String>,
    context: &RunContext,
) -> Option<String> {
    match result {
        Ok(answer) => Some(answer),
        Err(error) => {
            if context.is_cancelled() || is_run_cancelled_error(&error) {
                return None;
            }
            eprintln!("Telegram gateway error for chat {chat_id}: {error:#}");
            Some(format!("Gateway error: {error:#}"))
        }
    }
}

fn terminal_status<T>(result: &Result<T>, context: &RunContext) -> TerminalStatus {
    match result {
        Ok(_) => TerminalStatus::Success,
        Err(error) if context.is_cancelled() || is_run_cancelled_error(error) => {
            TerminalStatus::Cancelled
        }
        Err(_) => TerminalStatus::Failed,
    }
}

async fn send_telegram_answer(
    bot: &Bot,
    chat_id: ChatId,
    answer: String,
    context: &RunContext,
) -> Result<(), teloxide::RequestError> {
    let markdown_v2 = match markdown_to_telegram_markdown_v2(&answer) {
        Ok(markdown_v2) => markdown_v2,
        Err(error) => {
            eprintln!(
                "Telegram MarkdownV2 conversion failed for chat {chat_id}; sending plain text: {error:#}"
            );
            return send_plain_telegram_answer(bot, chat_id, answer, context).await;
        }
    };

    let markdown_request = bot
        .send_message(chat_id, markdown_v2)
        .parse_mode(ParseMode::MarkdownV2);
    let markdown_result = tokio::select! {
        result = markdown_request => result,
        _ = context.cancelled() => return Ok(()),
    };

    match markdown_result {
        Ok(_) => Ok(()),
        Err(error) => {
            eprintln!(
                "Telegram MarkdownV2 send_message failed for chat {chat_id}; retrying as plain text: {error:#}"
            );
            send_plain_telegram_answer(bot, chat_id, answer, context).await
        }
    }
}

async fn send_plain_telegram_answer(
    bot: &Bot,
    chat_id: ChatId,
    answer: String,
    context: &RunContext,
) -> Result<(), teloxide::RequestError> {
    if context.is_cancelled() {
        return Ok(());
    }

    tokio::select! {
        result = bot.send_message(chat_id, answer) => result.map(|_| ()),
        _ = context.cancelled() => Ok(()),
    }
}

fn cancel_on_ctrl_c(chat_id: ChatId, context: RunContext) -> CancellationWatch {
    CancellationWatch(tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                context.cancel();
                eprintln!("Telegram run cancelled for chat {chat_id}");
            }
            Err(error) => {
                context.cancel();
                eprintln!("Telegram Ctrl-C listener failed for chat {chat_id}: {error:#}");
            }
        }
    }))
}

struct CancellationWatch(JoinHandle<()>);

impl Drop for CancellationWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn keep_typing(bot: Bot, chat_id: ChatId) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(error) = bot.send_chat_action(chat_id, ChatAction::Typing).await {
                eprintln!("Telegram typing action failed for chat {chat_id}: {error:#}");
            }
            sleep(Duration::from_secs(4)).await;
        }
    })
}

async fn stop_typing(typing: JoinHandle<()>) {
    typing.abort();
    let _ = typing.await;
}

#[derive(Clone)]
struct TelegramDraftApi {
    client: Client,
    token: String,
}

impl TelegramDraftApi {
    fn new(token: String) -> Self {
        Self {
            client: Client::new(),
            token,
        }
    }

    async fn send_message_draft(&self, chat_id: ChatId, draft_id: i32, text: &str) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessageDraft",
            self.token
        );
        let response = self
            .client
            .post(url)
            .json(&TelegramDraftRequest {
                chat_id: chat_id.0,
                draft_id,
                text: truncate_chars(text, MAX_DRAFT_TEXT_CHARS),
            })
            .send()
            .await
            .context("failed to call sendMessageDraft")?;

        let status = response.status();
        let response = response
            .json::<TelegramApiResponse>()
            .await
            .context("failed to decode sendMessageDraft response")?;

        if !response.ok {
            bail!(
                "sendMessageDraft returned {status}: {}",
                response
                    .description
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }

        Ok(())
    }
}

struct TelegramDraftStream {
    api: TelegramDraftApi,
    chat_id: ChatId,
    draft_id: i32,
    text: String,
    last_update: Option<Instant>,
    disabled: bool,
    typing: Option<JoinHandle<()>>,
    context: RunContext,
}

impl TelegramDraftStream {
    fn new(
        api: TelegramDraftApi,
        chat_id: ChatId,
        message_id: MessageId,
        typing: JoinHandle<()>,
        context: RunContext,
    ) -> Self {
        Self {
            api,
            chat_id,
            draft_id: message_id.0.max(1),
            text: String::new(),
            last_update: None,
            disabled: false,
            typing: Some(typing),
            context,
        }
    }

    async fn maybe_update(&mut self) {
        if self.disabled {
            return;
        }

        let should_update = self
            .last_update
            .is_none_or(|updated_at| updated_at.elapsed() >= DRAFT_UPDATE_INTERVAL);

        if should_update {
            self.update_draft().await;
        }
    }

    async fn update_draft(&mut self) {
        self.stop_typing().await;

        if let Err(error) = self.send_draft().await {
            self.disabled = true;
            eprintln!(
                "Telegram sendMessageDraft failed for chat {}: {error:#}",
                self.chat_id
            );
        } else {
            self.last_update = Some(Instant::now());
        }
    }

    async fn send_draft(&self) -> Result<()> {
        tokio::select! {
            result = self.api.send_message_draft(self.chat_id, self.draft_id, &self.text) => result,
            _ = self.context.cancelled() => bail!(RUN_CANCELLED),
        }
    }

    async fn stop_typing(&mut self) {
        let Some(typing) = self.typing.take() else {
            return;
        };

        stop_typing(typing).await;
    }

    async fn finish(mut self) {
        self.stop_typing().await;
    }
}

#[async_trait::async_trait]
impl LlmStreamSink for TelegramDraftStream {
    async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()> {
        if matches!(event, LlmStreamEvent::FileReady(_)) {
            bail!("file output is not configured");
        }
        if let LlmStreamEvent::TextDelta(delta) = event {
            if delta.is_empty() {
                return Ok(());
            }

            self.text.push_str(&delta);
            self.maybe_update().await;
        }

        Ok(())
    }
}

struct DiscardingStreamSink;

#[async_trait::async_trait]
impl LlmStreamSink for DiscardingStreamSink {
    async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()> {
        if matches!(event, LlmStreamEvent::FileReady(_)) {
            bail!("file output is not configured");
        }
        Ok(())
    }
}

#[derive(serde::Serialize)]
struct TelegramDraftRequest<'a> {
    chat_id: i64,
    draft_id: i32,
    text: &'a str,
}

#[derive(Deserialize)]
struct TelegramApiResponse {
    ok: bool,
    description: Option<String>,
}

fn truncate_chars(text: &str, max_chars: usize) -> &str {
    text.char_indices()
        .nth(max_chars)
        .map_or(text, |(index, _)| &text[..index])
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, bail};
    use teloxide::types::{ChatId, Update, UpdateId, UpdateKind};

    use crate::{auth::GatewayIdentity, llm::client::RunContext};

    use super::{
        activity_status::TerminalStatus, answer_or_gateway_error, denied_message,
        telegram_identity_from_sender, telegram_update_distribution, terminal_status,
        truncate_chars,
    };

    #[test]
    fn telegram_updates_are_not_grouped_by_chat() {
        let update = Update {
            id: UpdateId(1),
            kind: UpdateKind::Error(serde_json::json!({})),
        };

        assert_eq!(telegram_update_distribution(&update), None);
    }

    #[test]
    fn telegram_identity_uses_sender_user_id() {
        let identity = telegram_identity_from_sender(Some((123, Some("SomeUser".to_string()))));

        assert_eq!(
            identity,
            Some(GatewayIdentity::new(
                "telegram",
                "123",
                Some("SomeUser".to_string())
            ))
        );
    }

    #[test]
    fn telegram_identity_requires_sender() {
        assert_eq!(telegram_identity_from_sender(None), None);
    }

    #[test]
    fn denied_message_points_to_start_without_technical_details() {
        let message = denied_message();

        assert!(message.contains("/start"));
        assert!(!message.contains("users.json"));
        assert!(!message.contains("~/.codrik"));
    }

    #[test]
    fn cancelled_run_does_not_render_gateway_error() {
        let context = RunContext::new();
        context.cancel();

        let answer = answer_or_gateway_error(ChatId(123), (|| bail!("run cancelled"))(), &context);

        assert_eq!(answer, None);
    }

    #[test]
    fn terminal_status_reflects_success_cancellation_and_failure() {
        let active = RunContext::new();
        assert_eq!(
            terminal_status(&Ok("done".to_string()), &active),
            TerminalStatus::Success
        );
        assert_eq!(
            terminal_status(&Err::<String, _>(anyhow!("boom")), &active),
            TerminalStatus::Failed
        );

        let cancelled = RunContext::new();
        cancelled.cancel();
        assert_eq!(
            terminal_status(&Err::<String, _>(anyhow!("run cancelled")), &cancelled),
            TerminalStatus::Cancelled
        );
    }

    #[test]
    fn truncate_chars_keeps_short_text_unchanged() {
        assert_eq!(truncate_chars("hello", 10), "hello");
    }

    #[test]
    fn truncate_chars_respects_utf8_boundaries() {
        assert_eq!(truncate_chars("привет", 3), "при");
    }
}
