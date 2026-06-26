use anyhow::{Context, Result};
use teloxide::{
    Bot,
    prelude::Requester,
    types::{ChatAction, Message},
};
use tokio::time::{Duration, sleep};

use crate::{app, config::AppConfig};

pub async fn run(config: AppConfig) -> Result<()> {
    let token = config
        .telegram
        .as_ref()
        .context("telegram config is missing")?
        .token
        .clone();

    let bot = Bot::new(token);

    teloxide::repl(bot, move |bot: Bot, msg: Message| {
        let config = config.clone();

        async move {
            let Some(text) = msg.text() else {
                return Ok(());
            };

            let typing = keep_typing(bot.clone(), msg.chat.id);

            let session_id = format!("telegram-chat-{}", msg.chat.id);
            let answer =
                match app::run_once_with_session(text.to_string(), config, session_id).await {
                    Ok(answer) => answer,
                    Err(error) => format!("Gateway error: {error:#}"),
                };
            typing.abort();
            let _ = typing.await;

            bot.send_message(msg.chat.id, answer).await?;

            Ok(())
        }
    })
    .await;

    Ok(())
}

fn keep_typing(bot: Bot, chat_id: teloxide::types::ChatId) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let _ = bot.send_chat_action(chat_id, ChatAction::Typing).await;
            sleep(Duration::from_secs(4)).await;
        }
    })
}
