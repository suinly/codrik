use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::llm::client::LlmToolCall;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Vec<MessagePart>,
    pub tool_calls: Vec<LlmToolCall>,
    pub tool_call_id: Option<String>,
}

impl Message {
    pub fn user(input: impl Into<UserInput>) -> Self {
        Self {
            role: Role::User,
            content: input.into().into_parts(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: text_parts(content),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn assistant_tool_calls(content: impl Into<String>, tool_calls: Vec<LlmToolCall>) -> Self {
        Self {
            role: Role::Assistant,
            content: text_parts(content),
            tool_calls,
            tool_call_id: None,
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: text_parts(content),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: text_parts(content),
            tool_calls: Vec::new(),
            tool_call_id: Some(tool_call_id.into()),
        }
    }

    pub fn text(&self) -> String {
        text_from_parts(&self.content)
    }
}

fn text_parts(content: impl Into<String>) -> Vec<MessagePart> {
    let content = content.into();
    if content.is_empty() {
        Vec::new()
    } else {
        vec![MessagePart::Text(content)]
    }
}

fn text_from_parts(parts: &[MessagePart]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            MessagePart::Text(text) => Some(text.as_str()),
            MessagePart::Attachment(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub id: String,
    pub relative_path: PathBuf,
    pub display_name: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: String,
}

impl Attachment {
    pub fn new(
        id: impl Into<String>,
        relative_path: impl Into<PathBuf>,
        display_name: impl Into<String>,
        media_type: impl Into<String>,
        size_bytes: u64,
        sha256: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            relative_path: relative_path.into(),
            display_name: display_name.into(),
            media_type: media_type.into(),
            size_bytes,
            sha256: sha256.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum MessagePart {
    Text(String),
    Attachment(Attachment),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserInput {
    parts: Vec<MessagePart>,
}

impl UserInput {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_text(mut self, text: impl Into<String>) -> Self {
        let text = text.into();
        if !text.is_empty() {
            self.parts.push(MessagePart::Text(text));
        }
        self
    }

    pub fn push_attachment(mut self, attachment: Attachment) -> Self {
        self.parts.push(MessagePart::Attachment(attachment));
        self
    }

    pub fn parts(&self) -> &[MessagePart] {
        &self.parts
    }

    pub fn text(&self) -> String {
        text_from_parts(&self.parts)
    }

    fn into_parts(self) -> Vec<MessagePart> {
        self.parts
    }
}

impl From<String> for UserInput {
    fn from(value: String) -> Self {
        Self::new().push_text(value)
    }
}

impl From<&str> for UserInput {
    fn from(value: &str) -> Self {
        Self::new().push_text(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    User,
    Assistant,
    System,
    Tool,
}

#[cfg(test)]
mod tests {
    use crate::llm::client::LlmToolCall;

    use super::{Attachment, Message, MessagePart, UserInput};

    #[test]
    fn user_input_preserves_text_then_attachment() {
        let attachment = Attachment::new(
            "att-1",
            "attachments/att-1.png",
            "screen.png",
            "image/png",
            4,
            "abcd",
        );
        let input = UserInput::new()
            .push_text("inspect this")
            .push_attachment(attachment.clone());

        assert_eq!(
            input.parts(),
            &[
                MessagePart::Text("inspect this".into()),
                MessagePart::Attachment(attachment),
            ]
        );
    }

    #[test]
    fn string_becomes_text_only_user_input() {
        assert_eq!(UserInput::from("hello").text(), "hello");
    }

    #[test]
    fn message_serde_round_trip_preserves_tool_calls_and_attachments() {
        let message = Message::assistant_tool_calls(
            "working",
            vec![LlmToolCall {
                id: "call-1".into(),
                name: "inspect".into(),
                arguments: r#"{"path":"screen.png"}"#.into(),
            }],
        );
        let attached = Message::user(UserInput::new().push_attachment(Attachment::new(
            "att-1",
            "attachments/att-1.png",
            "screen.png",
            "image/png",
            4,
            "abcd",
        )));

        for original in [message, attached] {
            let json = serde_json::to_string(&original).unwrap();
            let decoded: Message = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, original);
        }
    }
}
