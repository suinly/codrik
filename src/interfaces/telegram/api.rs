use std::{fmt, path::PathBuf, time::Duration};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::interfaces::telegram::types::TelegramBot;

const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramApiErrorClass {
    Retryable { retry_after: Option<Duration> },
    Terminal,
    OutcomeUnknown,
}

#[derive(Debug)]
pub struct TelegramApiError {
    class: TelegramApiErrorClass,
    method: &'static str,
    description: String,
}

impl TelegramApiError {
    pub fn class(&self) -> TelegramApiErrorClass {
        self.class.clone()
    }

    #[cfg(test)]
    pub(crate) fn classified(
        class: TelegramApiErrorClass,
        method: &'static str,
        description: impl Into<String>,
    ) -> Self {
        Self {
            class,
            method,
            description: description.into(),
        }
    }
}

impl fmt::Display for TelegramApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Telegram {} failed: {}",
            self.method, self.description
        )
    }
}

impl std::error::Error for TelegramApiError {}

#[derive(Clone, Serialize)]
pub struct SetWebhook {
    pub url: String,
    pub secret_token: String,
    pub allowed_updates: Vec<String>,
    pub drop_pending_updates: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WebhookInfo {
    pub url: String,
    #[serde(default)]
    pub allowed_updates: Vec<String>,
    #[serde(default)]
    pub pending_update_count: usize,
}

#[derive(Clone, Serialize)]
pub struct SendMessage {
    pub chat_id: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_parameters: Option<ReplyParameters>,
}

#[derive(Clone, Serialize)]
pub struct EditMessageText {
    pub chat_id: String,
    pub message_id: i64,
    pub text: String,
}

#[derive(Clone, Serialize)]
pub struct SendChatAction {
    pub chat_id: String,
    pub action: TelegramChatAction,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelegramChatAction {
    Typing,
}

#[derive(Clone, Serialize)]
pub struct ReplyParameters {
    pub message_id: i64,
}

pub struct SendFile {
    pub chat_id: String,
    pub path: PathBuf,
    pub file: tokio::fs::File,
    pub length: u64,
    pub display_name: String,
    pub caption: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramMessageRef {
    pub message_id: i64,
}

#[async_trait]
pub trait TelegramApi: Send + Sync {
    async fn get_me(&self) -> Result<TelegramBot, TelegramApiError>;
    async fn set_webhook(&self, command: SetWebhook) -> Result<(), TelegramApiError>;
    async fn get_webhook_info(&self) -> Result<WebhookInfo, TelegramApiError>;
    async fn send_message(
        &self,
        command: SendMessage,
    ) -> Result<TelegramMessageRef, TelegramApiError>;
    async fn send_chat_action(&self, command: SendChatAction) -> Result<(), TelegramApiError>;
    async fn edit_message_text(&self, command: EditMessageText) -> Result<(), TelegramApiError>;
    async fn send_photo(&self, command: SendFile) -> Result<TelegramMessageRef, TelegramApiError>;
    async fn send_document(
        &self,
        command: SendFile,
    ) -> Result<TelegramMessageRef, TelegramApiError>;
}

#[derive(Clone)]
pub struct ReqwestTelegramApi {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl ReqwestTelegramApi {
    pub fn new(token: impl Into<String>) -> Result<Self, TelegramApiError> {
        Self::with_base_url(token, "https://api.telegram.org")
    }

    pub fn with_base_url(
        token: impl Into<String>,
        base_url: &str,
    ) -> Result<Self, TelegramApiError> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(api_error(
                TelegramApiErrorClass::Terminal,
                "client",
                "bot token is blank",
            ));
        }
        let parsed = url::Url::parse(base_url).map_err(|_| {
            api_error(
                TelegramApiErrorClass::Terminal,
                "client",
                "Bot API base URL is invalid",
            )
        })?;
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|_| {
                api_error(
                    TelegramApiErrorClass::Terminal,
                    "client",
                    "failed to build HTTP client",
                )
            })?;
        Ok(Self {
            client,
            base_url: parsed.as_str().trim_end_matches('/').to_owned(),
            token,
        })
    }

    fn endpoint(&self, method: &str) -> String {
        format!("{}/bot{}/{method}", self.base_url, self.token)
    }

    async fn post_json<T, R>(
        &self,
        method: &'static str,
        command: &T,
        retry_safe: bool,
    ) -> Result<R, TelegramApiError>
    where
        T: Serialize + Sync,
        R: DeserializeOwned,
    {
        let response = self
            .client
            .post(self.endpoint(method))
            .json(command)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|error| {
                api_error(
                    if retry_safe || error.is_connect() {
                        TelegramApiErrorClass::Retryable { retry_after: None }
                    } else {
                        TelegramApiErrorClass::OutcomeUnknown
                    },
                    method,
                    "HTTP transport failed",
                )
            })?;
        decode_response(
            method,
            response,
            if retry_safe {
                TelegramApiErrorClass::Retryable { retry_after: None }
            } else {
                TelegramApiErrorClass::OutcomeUnknown
            },
        )
        .await
    }

    async fn post_file(
        &self,
        method: &'static str,
        field: &'static str,
        command: SendFile,
    ) -> Result<TelegramMessageRef, TelegramApiError> {
        let stream = tokio_util::io::ReaderStream::new(command.file);
        let part = reqwest::multipart::Part::stream_with_length(
            reqwest::Body::wrap_stream(stream),
            command.length,
        )
        .file_name(command.display_name);
        let mut form = reqwest::multipart::Form::new()
            .text("chat_id", command.chat_id)
            .part(field, part);
        if let Some(caption) = command.caption {
            form = form.text("caption", caption);
        }
        let response = self
            .client
            .post(self.endpoint(method))
            .multipart(form)
            .timeout(UPLOAD_TIMEOUT)
            .send()
            .await
            .map_err(|error| {
                api_error(
                    if error.is_connect() {
                        TelegramApiErrorClass::Retryable { retry_after: None }
                    } else {
                        TelegramApiErrorClass::OutcomeUnknown
                    },
                    method,
                    "HTTP transport failed",
                )
            })?;
        decode_response(method, response, TelegramApiErrorClass::OutcomeUnknown).await
    }
}

#[async_trait]
impl TelegramApi for ReqwestTelegramApi {
    async fn get_me(&self) -> Result<TelegramBot, TelegramApiError> {
        self.post_json("getMe", &serde_json::json!({}), true).await
    }

    async fn set_webhook(&self, command: SetWebhook) -> Result<(), TelegramApiError> {
        let _: bool = self.post_json("setWebhook", &command, true).await?;
        Ok(())
    }

    async fn get_webhook_info(&self) -> Result<WebhookInfo, TelegramApiError> {
        self.post_json("getWebhookInfo", &serde_json::json!({}), true)
            .await
    }

    async fn send_message(
        &self,
        command: SendMessage,
    ) -> Result<TelegramMessageRef, TelegramApiError> {
        self.post_json("sendMessage", &command, false).await
    }

    async fn send_chat_action(&self, command: SendChatAction) -> Result<(), TelegramApiError> {
        let _: bool = self.post_json("sendChatAction", &command, true).await?;
        Ok(())
    }

    async fn edit_message_text(&self, command: EditMessageText) -> Result<(), TelegramApiError> {
        match self
            .post_json::<_, TelegramMessageRef>("editMessageText", &command, true)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.description.contains("message is not modified") => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn send_photo(&self, command: SendFile) -> Result<TelegramMessageRef, TelegramApiError> {
        self.post_file("sendPhoto", "photo", command).await
    }

    async fn send_document(
        &self,
        command: SendFile,
    ) -> Result<TelegramMessageRef, TelegramApiError> {
        self.post_file("sendDocument", "document", command).await
    }
}

#[derive(Deserialize)]
struct TelegramEnvelope<T> {
    ok: bool,
    result: Option<T>,
    error_code: Option<u16>,
    description: Option<String>,
    parameters: Option<TelegramErrorParameters>,
}

#[derive(Deserialize)]
struct TelegramErrorParameters {
    retry_after: Option<u64>,
}

async fn decode_response<T: DeserializeOwned>(
    method: &'static str,
    response: reqwest::Response,
    ambiguous_class: TelegramApiErrorClass,
) -> Result<T, TelegramApiError> {
    let status = response.status();
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|_| api_error(ambiguous_class.clone(), method, "response body failed"))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(api_error(
                ambiguous_class.clone(),
                method,
                "response body exceeds limit",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let envelope: TelegramEnvelope<T> = serde_json::from_slice(&bytes).map_err(|_| {
        api_error(
            ambiguous_class.clone(),
            method,
            "response envelope is invalid",
        )
    })?;
    if envelope.ok {
        return envelope.result.ok_or_else(|| {
            api_error(
                ambiguous_class,
                method,
                "successful response omitted result",
            )
        });
    }
    let retry_after = envelope
        .parameters
        .and_then(|parameters| parameters.retry_after)
        .map(Duration::from_secs);
    let code = envelope.error_code.unwrap_or(status.as_u16());
    let class = if code == 429 {
        TelegramApiErrorClass::Retryable { retry_after }
    } else if code >= 500 || status.is_server_error() {
        TelegramApiErrorClass::Retryable { retry_after: None }
    } else {
        TelegramApiErrorClass::Terminal
    };
    Err(api_error(
        class,
        method,
        &envelope
            .description
            .unwrap_or_else(|| "Bot API rejected request".into()),
    ))
}

fn api_error(
    class: TelegramApiErrorClass,
    method: &'static str,
    description: &str,
) -> TelegramApiError {
    TelegramApiError {
        class,
        method,
        description: description.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{
        ReqwestTelegramApi, SendChatAction, SendMessage, TelegramApi, TelegramApiErrorClass,
        TelegramChatAction,
    };

    #[tokio::test]
    async fn get_me_decodes_successful_envelope() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = vec![0_u8; 4096];
            let read = socket.read(&mut request).await?;
            let request = String::from_utf8_lossy(&request[..read]).to_string();
            assert!(request.starts_with("POST /botsecret-token/getMe "));
            let body = r#"{"ok":true,"result":{"id":900,"is_bot":true,"username":"codrik_bot"}}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
            anyhow::Ok(())
        });
        let api = ReqwestTelegramApi::with_base_url("secret-token", &base)?;
        let bot = api.get_me().await?;
        assert_eq!(bot.id, 900);
        assert_eq!(bot.username.as_deref(), Some("codrik_bot"));
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn send_chat_action_posts_typed_typing_action() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = vec![0_u8; 4096];
            let read = socket.read(&mut request).await?;
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("POST /botsecret-token/sendChatAction "));
            assert!(request.contains(r#""chat_id":"100""#));
            assert!(request.contains(r#""action":"typing""#));
            let body = r#"{"ok":true,"result":true}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
            anyhow::Ok(())
        });
        let api = ReqwestTelegramApi::with_base_url("secret-token", &base)?;

        api.send_chat_action(SendChatAction {
            chat_id: "100".into(),
            action: TelegramChatAction::Typing,
        })
        .await?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn telegram_429_exposes_retry_without_leaking_token() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = vec![0_u8; 4096];
            let _ = socket.read(&mut request).await?;
            let body = r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":7}}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
            anyhow::Ok(())
        });
        let api = ReqwestTelegramApi::with_base_url("secret-token", &base)?;
        let error = api.get_me().await.unwrap_err();
        assert_eq!(
            error.class(),
            TelegramApiErrorClass::Retryable {
                retry_after: Some(Duration::from_secs(7))
            }
        );
        assert!(!error.to_string().contains("secret-token"));
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_send_response_is_outcome_unknown() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = vec![0_u8; 4096];
            let _ = socket.read(&mut request).await?;
            let body = r#"{"ok":true,"result":"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
            anyhow::Ok(())
        });
        let api = ReqwestTelegramApi::with_base_url("secret-token", &base)?;

        let error = api
            .send_message(SendMessage {
                chat_id: "100".into(),
                text: "hello".into(),
                reply_parameters: None,
            })
            .await
            .unwrap_err();
        assert_eq!(error.class(), TelegramApiErrorClass::OutcomeUnknown);
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn send_connect_failure_is_retryable() -> Result<()> {
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = probe.local_addr()?;
        drop(probe);
        let api = ReqwestTelegramApi::with_base_url("secret-token", &format!("http://{address}"))?;

        let error = api
            .send_message(SendMessage {
                chat_id: "100".into(),
                text: "hello".into(),
                reply_parameters: None,
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.class(),
            TelegramApiErrorClass::Retryable { retry_after: None }
        );
        Ok(())
    }
}
