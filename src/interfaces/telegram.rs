use anyhow::{Context, Result};
use teloxide::{Bot, prelude::Requester, types::Message};

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

            let answer = match app::run_once_with_config(text.to_string(), config).await {
                Ok(answer) => answer,
                Err(error) => format!("Gateway error: {error:#}"),
            };

            bot.send_message(msg.chat.id, answer).await?;

            Ok(())
        }
    })
    .await;

    Ok(())
}
