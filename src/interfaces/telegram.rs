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

use crate::{
    app,
    config::AppConfig,
    llm::client::{LlmStreamEvent, LlmStreamSink},
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

    let bot = Bot::new(token);
    let me = bot
        .get_me()
        .await
        .context("failed to initialize telegram bot")?;
    eprintln!(
        "Telegram gateway started for @{}",
        me.user.username.as_deref().unwrap_or("<unknown>")
    );

    teloxide::repl(bot, move |bot: Bot, msg: Message| {
        let config = config.clone();
        let draft_api = draft_api.clone();

        async move {
            let Some(text) = msg.text() else {
                return Ok(());
            };

            eprintln!("Telegram text received from chat {}", msg.chat.id);

            let answer = if msg.chat.is_private() {
                answer_private_chat(bot.clone(), &msg, text, config, draft_api).await
            } else {
                answer_regular_chat(bot.clone(), msg.chat.id, text, config).await
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
    msg: &Message,
    text: &str,
    config: AppConfig,
    draft_api: TelegramDraftApi,
) -> String {
    let typing = keep_typing(bot, msg.chat.id);
    let mut stream = TelegramDraftStream::new(draft_api, msg.chat.id, msg.id, typing);
    let result = app::run_once_with_session_streaming(
        text.to_string(),
        config,
        session_id(msg.chat.id),
        &mut stream,
    )
    .await;
    stream.finish().await;

    answer_or_gateway_error(msg.chat.id, result)
}

async fn answer_regular_chat(bot: Bot, chat_id: ChatId, text: &str, config: AppConfig) -> String {
    let typing = keep_typing(bot, chat_id);
    let result = app::run_once_with_session(text.to_string(), config, session_id(chat_id)).await;
    stop_typing(typing).await;

    answer_or_gateway_error(chat_id, result)
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

fn session_id(chat_id: ChatId) -> String {
    format!("telegram-chat-{chat_id}")
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
    use super::truncate_chars;

    #[test]
    fn truncate_chars_keeps_short_text_unchanged() {
        assert_eq!(truncate_chars("hello", 10), "hello");
    }

    #[test]
    fn truncate_chars_respects_utf8_boundaries() {
        assert_eq!(truncate_chars("привет", 3), "при");
    }
}
