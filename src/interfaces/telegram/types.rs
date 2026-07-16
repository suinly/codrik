use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::runtime::{gateway::DeliveryRoute, store::LinkIdentity};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramMessage {
    pub message_id: i64,
    #[serde(rename = "from")]
    pub sender: Option<TelegramUser>,
    pub chat: TelegramChat,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramUser {
    pub id: i64,
    pub is_bot: bool,
    pub username: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TelegramBot {
    pub id: i64,
    pub is_bot: bool,
    pub username: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramInbound {
    Link {
        code: Option<String>,
        identity: LinkIdentity,
        route: DeliveryRoute,
    },
    Text {
        text: String,
        identity: LinkIdentity,
        route: DeliveryRoute,
    },
    Unsupported,
}

impl TelegramUpdate {
    pub fn classify(&self, bot_id: &str, bot_username: &str) -> Result<TelegramInbound> {
        if bot_id.trim().is_empty() || bot_username.trim().is_empty() {
            bail!("Telegram bot identity must not be blank");
        }
        let Some(message) = &self.message else {
            return Ok(TelegramInbound::Unsupported);
        };
        let Some(sender) = &message.sender else {
            return Ok(TelegramInbound::Unsupported);
        };
        let Some(text) = message.text.as_deref().map(str::trim) else {
            return Ok(TelegramInbound::Unsupported);
        };
        if message.chat.kind != "private" || sender.is_bot || text.is_empty() {
            return Ok(TelegramInbound::Unsupported);
        }
        let gateway = format!("telegram:{bot_id}");
        let identity = LinkIdentity {
            provider: gateway.clone(),
            subject: sender.id.to_string(),
            username: sender.username.clone(),
        };
        let route = DeliveryRoute::new(gateway, message.chat.id.to_string(), None, 4096, 1024)?;
        let (command, argument) = split_command(text);
        if command == "/link" || command == format!("/link@{bot_username}") {
            return Ok(TelegramInbound::Link {
                code: argument.map(str::to_owned),
                identity,
                route,
            });
        }
        if command.starts_with("/link@") {
            return Ok(TelegramInbound::Unsupported);
        }
        Ok(TelegramInbound::Text {
            text: text.to_owned(),
            identity,
            route,
        })
    }
}

fn split_command(text: &str) -> (&str, Option<&str>) {
    let split = text
        .char_indices()
        .find(|(_, character)| character.is_whitespace())
        .map(|(index, _)| index);
    match split {
        Some(index) => {
            let argument = text[index..].trim();
            (&text[..index], (!argument.is_empty()).then_some(argument))
        }
        None => (text, None),
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use super::{TelegramInbound, TelegramUpdate};

    #[test]
    fn private_link_command_normalizes_bot_suffix() -> Result<()> {
        let update: TelegramUpdate = serde_json::from_value(json!({
            "update_id": 42,
            "message": {
                "message_id": 7,
                "from": {"id": 100, "is_bot": false, "username": "owner"},
                "chat": {"id": 100, "type": "private"},
                "text": "/link@codrik_bot abcd-efgh"
            }
        }))?;
        assert!(matches!(
            update.classify("900", "codrik_bot")?,
            TelegramInbound::Link { code: Some(code), identity, route }
                if code == "abcd-efgh"
                    && identity.provider == "telegram:900"
                    && identity.subject == "100"
                    && route.address == "100"
                    && route.reply_to_external_id.is_none()
        ));
        Ok(())
    }

    #[test]
    fn private_text_classifies_with_actor_private_route() -> Result<()> {
        let update: TelegramUpdate = serde_json::from_value(json!({
            "update_id": 43,
            "message": {
                "message_id": 8,
                "from": {"id": 100, "is_bot": false},
                "chat": {"id": 100, "type": "private"},
                "text": "hello"
            }
        }))?;
        assert!(matches!(
            update.classify("900", "codrik_bot")?,
            TelegramInbound::Text { text, route, .. }
                if text == "hello"
                    && route.max_text_chars == 4096
                    && route.max_caption_chars == 1024
                    && route.reply_to_external_id.is_none()
        ));
        Ok(())
    }

    #[test]
    fn unsupported_updates_never_classify_as_user_input() -> Result<()> {
        for update in [
            json!({"update_id": 1}),
            json!({"update_id": 2, "message": {
                "message_id": 1,
                "from": {"id": 100, "is_bot": true},
                "chat": {"id": 100, "type": "private"},
                "text": "hello"
            }}),
            json!({"update_id": 3, "message": {
                "message_id": 1,
                "from": {"id": 100, "is_bot": false},
                "chat": {"id": -100, "type": "group"},
                "text": "hello"
            }}),
            json!({"update_id": 4, "message": {
                "message_id": 1,
                "from": {"id": 100, "is_bot": false},
                "chat": {"id": 100, "type": "private"},
                "photo": []
            }}),
            json!({"update_id": 5, "message": {
                "message_id": 1,
                "from": {"id": 100, "is_bot": false},
                "chat": {"id": 100, "type": "private"},
                "text": "/link@another_bot ABCD-EFGH"
            }}),
        ] {
            let update: TelegramUpdate = serde_json::from_value(update)?;
            assert_eq!(
                update.classify("900", "codrik_bot")?,
                TelegramInbound::Unsupported
            );
        }
        Ok(())
    }
}
