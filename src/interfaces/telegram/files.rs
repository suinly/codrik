use teloxide::types::{FileId, Message};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramIncomingFile {
    pub file_id: FileId,
    pub display_name: String,
}

impl TelegramIncomingFile {
    pub fn from_message(message: &Message) -> Option<Self> {
        if let Some(photo) = message
            .photo()
            .and_then(|photos| photos.iter().max_by_key(|photo| photo.file.size))
        {
            return Some(Self {
                file_id: photo.file.id.clone(),
                display_name: "photo.jpg".to_string(),
            });
        }

        message.document().map(|document| Self {
            file_id: document.file.id.clone(),
            display_name: document
                .file_name
                .clone()
                .unwrap_or_else(|| "document.bin".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use teloxide::types::Message;

    use super::TelegramIncomingFile;

    #[test]
    fn largest_photo_becomes_incoming_file() {
        let message: Message = serde_json::from_value(serde_json::json!({
            "message_id": 1,
            "date": 1,
            "chat": {"id": 1, "type": "private"},
            "caption": "inspect",
            "photo": [
                {"file_id": "small", "file_unique_id": "s", "file_size": 10, "width": 10, "height": 10},
                {"file_id": "largest", "file_unique_id": "l", "file_size": 20, "width": 20, "height": 20}
            ]
        }))
        .expect("photo fixture should deserialize");

        let incoming = TelegramIncomingFile::from_message(&message).expect("photo expected");

        assert_eq!(incoming.file_id.0, "largest");
        assert_eq!(incoming.display_name, "photo.jpg");
        assert_eq!(message.caption(), Some("inspect"));
    }

    #[test]
    fn document_preserves_display_name() {
        let message: Message = serde_json::from_value(serde_json::json!({
            "message_id": 1,
            "date": 1,
            "chat": {"id": 1, "type": "private"},
            "document": {
                "file_id": "document",
                "file_unique_id": "d",
                "file_size": 30,
                "file_name": "report.pdf",
                "mime_type": "application/pdf"
            }
        }))
        .expect("document fixture should deserialize");

        let incoming = TelegramIncomingFile::from_message(&message).expect("document expected");

        assert_eq!(incoming.file_id.0, "document");
        assert_eq!(incoming.display_name, "report.pdf");
    }
}
