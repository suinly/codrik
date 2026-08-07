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
    pub caption: Option<String>,
    #[serde(default)]
    pub photo: Vec<TelegramPhotoSize>,
    pub document: Option<TelegramFileDescriptor>,
    pub video: Option<TelegramFileDescriptor>,
    pub animation: Option<TelegramFileDescriptor>,
    pub audio: Option<TelegramFileDescriptor>,
    pub voice: Option<TelegramFileDescriptor>,
    pub video_note: Option<TelegramFileDescriptor>,
    pub sticker: Option<TelegramFileDescriptor>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramPhotoSize {
    pub file_id: String,
    pub file_unique_id: String,
    pub width: u64,
    pub height: u64,
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramFileDescriptor {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
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
    Attachment {
        caption: Option<String>,
        attachment: TelegramInboundAttachment,
        identity: LinkIdentity,
        route: DeliveryRoute,
    },
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramInboundAttachment {
    pub file_id: String,
    pub file_size: Option<u64>,
    pub display_name: String,
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
        if message.chat.kind != "private" || sender.is_bot {
            return Ok(TelegramInbound::Unsupported);
        }
        let gateway = format!("telegram:{bot_id}");
        let identity = LinkIdentity {
            provider: gateway.clone(),
            subject: sender.id.to_string(),
            username: sender.username.clone(),
        };
        let route = DeliveryRoute::new(gateway, message.chat.id.to_string(), None, 4096, 1024)?;
        if let Some(text) = message
            .text
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
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
            return Ok(TelegramInbound::Text {
                text: text.to_owned(),
                identity,
                route,
            });
        }

        let mut attachments = Vec::new();
        if let Some(photo) = message.photo.iter().max_by_key(|photo| {
            (
                photo.file_size.unwrap_or(0),
                photo.width.saturating_mul(photo.height),
            )
        }) {
            attachments.push(TelegramInboundAttachment {
                file_id: photo.file_id.clone(),
                file_size: photo.file_size,
                display_name: "photo.jpg".into(),
            });
        }
        for (file, fallback) in [
            (message.document.as_ref(), "document.bin"),
            (message.video.as_ref(), "video.mp4"),
            (message.animation.as_ref(), "animation.mp4"),
            (message.audio.as_ref(), "audio.mp3"),
            (message.voice.as_ref(), "voice.ogg"),
            (message.video_note.as_ref(), "video-note.mp4"),
            (message.sticker.as_ref(), "sticker.webp"),
        ] {
            if let Some(file) = file {
                attachments.push(TelegramInboundAttachment {
                    file_id: file.file_id.clone(),
                    file_size: file.file_size,
                    display_name: file
                        .file_name
                        .clone()
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or_else(|| fallback.into()),
                });
            }
        }
        if attachments.len() != 1 || attachments[0].file_id.trim().is_empty() {
            return Ok(TelegramInbound::Unsupported);
        }
        Ok(TelegramInbound::Attachment {
            caption: message
                .caption
                .as_deref()
                .map(str::trim)
                .filter(|caption| !caption.is_empty())
                .map(str::to_owned),
            attachment: attachments.pop().unwrap(),
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

    #[test]
    fn all_supported_media_classify_as_attachments() -> Result<()> {
        for field in [
            "document",
            "video",
            "animation",
            "audio",
            "voice",
            "video_note",
            "sticker",
        ] {
            let mut message = json!({
                "message_id": 7,
                "from": {"id": 100, "is_bot": false},
                "chat": {"id": 100, "type": "private"},
                "caption": "inspect"
            });
            message[field] = json!({
                "file_id": format!("{field}-id"),
                "file_unique_id": format!("{field}-unique"),
                "file_size": 42,
                "file_name": format!("sample.{field}"),
                "mime_type": "application/octet-stream"
            });
            let update: TelegramUpdate = serde_json::from_value(json!({
                "update_id": 42,
                "message": message
            }))?;
            assert!(matches!(
                update.classify("900", "codrik_bot")?,
                TelegramInbound::Attachment { caption: Some(caption), attachment, .. }
                    if caption == "inspect" && attachment.file_id == format!("{field}-id")
            ));
        }
        Ok(())
    }

    #[test]
    fn photo_uses_largest_available_size_without_caption() -> Result<()> {
        let update: TelegramUpdate = serde_json::from_value(json!({
            "update_id": 43,
            "message": {
                "message_id": 8,
                "from": {"id": 100, "is_bot": false},
                "chat": {"id": 100, "type": "private"},
                "caption": "   ",
                "media_group_id": "album-1",
                "photo": [
                    {"file_id":"small","file_unique_id":"s","width":90,"height":90,"file_size":100},
                    {"file_id":"large","file_unique_id":"l","width":900,"height":900,"file_size":1000}
                ]
            }
        }))?;
        assert!(matches!(
            update.classify("900", "codrik_bot")?,
            TelegramInbound::Attachment { caption: None, attachment, .. }
                if attachment.file_id == "large" && attachment.display_name == "photo.jpg"
        ));
        Ok(())
    }
}
