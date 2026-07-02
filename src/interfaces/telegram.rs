use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Deserialize;
use teloxide::{
    Bot,
    prelude::Requester,
    types::{ChatAction, ChatId, Message, MessageId},
};
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant, sleep},
};

mod commands;

use commands::{answer_session_command, is_command_addressed_to_other_bot, is_start_command};

use crate::{
    app,
    auth::{AuthDecision, AuthorizationStore, AuthorizedActor, GatewayIdentity},
    config::AppConfig,
    llm::client::{LlmStreamEvent, LlmStreamSink},
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
    let session_store =
        TelegramSessionStore::new(crate::config::codrik_dir()?.join("telegram-sessions.json"));

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

    teloxide::repl(bot, move |bot: Bot, msg: Message| {
        let config = config.clone();
        let draft_api = draft_api.clone();
        let auth_store = auth_store.clone();
        let session_store = session_store.clone();
        let bot_username = bot_username.clone();

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
                answer_start_command(&auth_store, identity).await
            } else {
                match auth_store.authorize(&identity).await {
                    Ok(AuthDecision::Authorized(actor)) => {
                        if let Some(answer) = answer_session_command(
                            &session_store,
                            msg.chat.id,
                            text,
                            bot_username.as_deref(),
                        )
                        .await
                        {
                            answer
                        } else if msg.chat.is_private() {
                            answer_private_chat(
                                bot.clone(),
                                &session_store,
                                &msg,
                                text,
                                config,
                                actor,
                                draft_api,
                            )
                            .await
                        } else {
                            answer_regular_chat(
                                bot.clone(),
                                &session_store,
                                msg.chat.id,
                                text,
                                config,
                                actor,
                            )
                            .await
                        }
                    }
                    Ok(AuthDecision::Denied) => denied_message(),
                    Err(error) => {
                        eprintln!(
                            "Telegram authorization failed for chat {}: {error:#}",
                            msg.chat.id
                        );
                        format!("Gateway error: {error:#}")
                    }
                }
            };

            if let Err(error) = bot.send_message(msg.chat.id, answer).await {
                eprintln!(
                    "Telegram send_message failed for chat {}: {error:#}",
                    msg.chat.id
                );
                return Err(error);
            }

            Ok(())
        }
    })
    .await;

    Ok(())
}

async fn answer_private_chat(
    bot: Bot,
    session_store: &TelegramSessionStore,
    msg: &Message,
    text: &str,
    config: AppConfig,
    actor: AuthorizedActor,
    draft_api: TelegramDraftApi,
) -> String {
    let session_id = match active_session_id_or_error(session_store, msg.chat.id).await {
        Ok(session_id) => session_id,
        Err(error) => return format!("Gateway error: {error:#}"),
    };
    let typing = keep_typing(bot, msg.chat.id);
    let mut stream = TelegramDraftStream::new(draft_api, msg.chat.id, msg.id, typing);
    let result = app::run_once_with_actor_session_streaming(
        text.to_string(),
        config,
        actor,
        session_id,
        &mut stream,
    )
    .await;
    stream.finish().await;

    answer_or_gateway_error(msg.chat.id, result)
}

async fn answer_regular_chat(
    bot: Bot,
    session_store: &TelegramSessionStore,
    chat_id: ChatId,
    text: &str,
    config: AppConfig,
    actor: AuthorizedActor,
) -> String {
    let session_id = match active_session_id_or_error(session_store, chat_id).await {
        Ok(session_id) => session_id,
        Err(error) => return format!("Gateway error: {error:#}"),
    };
    let typing = keep_typing(bot, chat_id);
    let result =
        app::run_once_with_actor_session(text.to_string(), config, actor, session_id).await;
    stop_typing(typing).await;

    answer_or_gateway_error(chat_id, result)
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

fn answer_or_gateway_error(chat_id: ChatId, result: Result<String>) -> String {
    match result {
        Ok(answer) => answer,
        Err(error) => {
            eprintln!("Telegram gateway error for chat {chat_id}: {error:#}");
            format!("Gateway error: {error:#}")
        }
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
}

impl TelegramDraftStream {
    fn new(
        api: TelegramDraftApi,
        chat_id: ChatId,
        message_id: MessageId,
        typing: JoinHandle<()>,
    ) -> Self {
        Self {
            api,
            chat_id,
            draft_id: message_id.0.max(1),
            text: String::new(),
            last_update: None,
            disabled: false,
            typing: Some(typing),
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
        self.api
            .send_message_draft(self.chat_id, self.draft_id, &self.text)
            .await
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
    use crate::auth::GatewayIdentity;

    use super::{denied_message, telegram_identity_from_sender, truncate_chars};

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
    fn truncate_chars_keeps_short_text_unchanged() {
        assert_eq!(truncate_chars("hello", 10), "hello");
    }

    #[test]
    fn truncate_chars_respects_utf8_boundaries() {
        assert_eq!(truncate_chars("привет", 3), "при");
    }
}
