use std::{collections::HashSet, path::PathBuf, sync::atomic::Ordering};

use anyhow::{Context, Result, bail};
use async_openai::types::{
    InputSource,
    files::{CreateFileRequest, FileInput, FilePurpose},
    responses::{ImageDetail, InputContent, InputFileContent, InputImageContent, InputTextContent},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use tokio::fs;

use crate::{
    agent::message::{Attachment, MessagePart},
    config::ImageDetailConfig,
    memory::provider_files::{
        ProviderFileKey, ProviderFileRecord, ProviderFileStore, ProviderUploadPurpose,
    },
};

use super::OpenAiClient;

const MAX_DOCUMENT_INPUT_BYTES: u64 = 50 * 1024 * 1024;
pub(super) const FILES_API_UNKNOWN: u8 = 0;
const FILES_API_SUPPORTED: u8 = 1;
const FILES_API_UNAVAILABLE: u8 = 2;

#[derive(Clone, Debug)]
pub struct OpenAiAttachmentContext {
    pub session_dir: PathBuf,
    pub provider_files: ProviderFileStore,
    pub image_detail: ImageDetailConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProviderAttachmentKind {
    Image,
    Document,
    MetadataOnly,
}

#[derive(Debug)]
pub(super) struct ResolvedAttachment {
    pub content: InputContent,
    pub cache_key: Option<ProviderFileKey>,
    pub reused_cache: bool,
}

pub(super) fn classify(media_type: &str) -> ProviderAttachmentKind {
    match media_type {
        "image/png" | "image/jpeg" | "image/webp" | "image/gif" => ProviderAttachmentKind::Image,
        "application/pdf"
        | "text/plain"
        | "text/markdown"
        | "text/csv"
        | "text/html"
        | "text/xml"
        | "application/xml"
        | "application/json"
        | "application/msword"
        | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        | "application/vnd.ms-powerpoint"
        | "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        | "application/vnd.ms-excel"
        | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => {
            ProviderAttachmentKind::Document
        }
        _ => ProviderAttachmentKind::MetadataOnly,
    }
}

pub(super) fn selected_document_ids<'a>(
    parts: impl DoubleEndedIterator<Item = &'a MessagePart>,
) -> HashSet<String> {
    let mut remaining = MAX_DOCUMENT_INPUT_BYTES;
    let mut selected = HashSet::new();

    for part in parts.rev() {
        let MessagePart::Attachment(file) = part else {
            continue;
        };
        if classify(&file.media_type) == ProviderAttachmentKind::Document
            && file.size_bytes <= remaining
        {
            remaining -= file.size_bytes;
            selected.insert(file.id.clone());
        }
    }

    selected
}

pub(super) fn metadata_text(file: &Attachment) -> String {
    format!(
        "[Attached file: name={}, media_type={}, size={} bytes, path={}]",
        file.display_name,
        file.media_type,
        file.size_bytes,
        file.relative_path.display()
    )
}

impl OpenAiClient {
    pub fn with_attachment_context(mut self, context: OpenAiAttachmentContext) -> Self {
        self.attachment_context = Some(context);
        self
    }

    pub async fn delete_file(&self, file_id: &str) -> Result<()> {
        self.client.files().delete(file_id).await?;
        Ok(())
    }

    pub(super) async fn resolve_attachment(
        &self,
        file: &Attachment,
        selected_documents: &HashSet<String>,
    ) -> Result<ResolvedAttachment> {
        let kind = classify(&file.media_type);
        if kind == ProviderAttachmentKind::MetadataOnly
            || (kind == ProviderAttachmentKind::Document && !selected_documents.contains(&file.id))
        {
            return Ok(ResolvedAttachment {
                content: InputContent::InputText(InputTextContent {
                    text: metadata_text(file),
                }),
                cache_key: None,
                reused_cache: false,
            });
        }

        let context = self
            .attachment_context
            .as_ref()
            .context("file attachment requires a session attachment context")?;
        if self.files_api_capability.load(Ordering::Relaxed) == FILES_API_UNAVAILABLE {
            return self.resolve_without_files_api(file, kind, context).await;
        }
        let purpose = match kind {
            ProviderAttachmentKind::Image => ProviderUploadPurpose::Vision,
            ProviderAttachmentKind::Document => ProviderUploadPurpose::UserData,
            ProviderAttachmentKind::MetadataOnly => unreachable!(),
        };
        let key = ProviderFileKey {
            sha256: file.sha256.clone(),
            provider: "openai".to_string(),
            purpose: purpose.clone(),
        };
        let cached = context.provider_files.get(&key).await?;
        let (file_id, reused_cache) = if let Some(record) = cached {
            (record.file_id, true)
        } else {
            let path = safe_attachment_path(&context.session_dir, &file.relative_path).await?;
            let uploaded = match self
                .client
                .files()
                .create(CreateFileRequest {
                    file: FileInput {
                        source: InputSource::Path { path },
                    },
                    purpose: match purpose {
                        ProviderUploadPurpose::Vision => FilePurpose::Vision,
                        ProviderUploadPurpose::UserData => FilePurpose::UserData,
                    },
                    expires_after: None,
                })
                .await
            {
                Ok(uploaded) => {
                    self.files_api_capability
                        .store(FILES_API_SUPPORTED, Ordering::Relaxed);
                    uploaded
                }
                Err(error) if provider_has_no_files_api(&error.to_string()) => {
                    self.files_api_capability
                        .store(FILES_API_UNAVAILABLE, Ordering::Relaxed);
                    return self.resolve_without_files_api(file, kind, context).await;
                }
                Err(error) => return Err(error.into()),
            };
            context
                .provider_files
                .put(ProviderFileRecord {
                    key: key.clone(),
                    file_id: uploaded.id.clone(),
                })
                .await?;
            (uploaded.id, false)
        };

        let content = match kind {
            ProviderAttachmentKind::Image => InputContent::InputImage(InputImageContent {
                detail: image_detail(context.image_detail),
                file_id: Some(file_id),
                image_url: None,
            }),
            ProviderAttachmentKind::Document => InputContent::InputFile(InputFileContent {
                file_id: Some(file_id),
                filename: Some(file.display_name.clone()),
                ..Default::default()
            }),
            ProviderAttachmentKind::MetadataOnly => unreachable!(),
        };

        Ok(ResolvedAttachment {
            content,
            cache_key: Some(key),
            reused_cache,
        })
    }

    async fn resolve_without_files_api(
        &self,
        file: &Attachment,
        kind: ProviderAttachmentKind,
        context: &OpenAiAttachmentContext,
    ) -> Result<ResolvedAttachment> {
        let content = match kind {
            ProviderAttachmentKind::Image => {
                let path = safe_attachment_path(&context.session_dir, &file.relative_path).await?;
                let bytes = fs::read(&path).await.with_context(|| {
                    format!("failed to read image attachment: {}", path.display())
                })?;
                InputContent::InputImage(InputImageContent {
                    detail: image_detail(context.image_detail),
                    file_id: None,
                    image_url: Some(format!(
                        "data:{};base64,{}",
                        file.media_type,
                        STANDARD.encode(bytes)
                    )),
                })
            }
            ProviderAttachmentKind::Document | ProviderAttachmentKind::MetadataOnly => {
                InputContent::InputText(InputTextContent {
                    text: metadata_text(file),
                })
            }
        };

        Ok(ResolvedAttachment {
            content,
            cache_key: None,
            reused_cache: false,
        })
    }

    pub(super) async fn evict_provider_files(&self, keys: &[ProviderFileKey]) -> Result<()> {
        let Some(context) = &self.attachment_context else {
            return Ok(());
        };
        for key in keys {
            context.provider_files.remove(key).await?;
        }
        Ok(())
    }
}

pub(super) fn provider_has_no_files_api(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("404") && message.contains("not found")
}

fn image_detail(detail: ImageDetailConfig) -> ImageDetail {
    match detail {
        ImageDetailConfig::Auto => ImageDetail::Auto,
        ImageDetailConfig::Low => ImageDetail::Low,
        ImageDetailConfig::High => ImageDetail::High,
    }
}

async fn safe_attachment_path(
    session_dir: &std::path::Path,
    relative_path: &std::path::Path,
) -> Result<PathBuf> {
    if relative_path.is_absolute() {
        bail!("attachment path must be relative");
    }
    let canonical_session = fs::canonicalize(session_dir).await.with_context(|| {
        format!(
            "failed to resolve session directory: {}",
            session_dir.display()
        )
    })?;
    let candidate = fs::canonicalize(session_dir.join(relative_path))
        .await
        .with_context(|| format!("failed to resolve attachment: {}", relative_path.display()))?;
    if !candidate.starts_with(&canonical_session) {
        bail!("attachment path escapes the session directory");
    }
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        collections::VecDeque,
        path::PathBuf,
        sync::{Arc, Mutex, atomic::Ordering},
    };

    use anyhow::{Context, Result};
    use tokio::{
        fs,
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::{
        agent::message::{Attachment, Message, MessagePart, UserInput},
        config::ImageDetailConfig,
        llm::client::{LlmClient, LlmRequest, RunContext},
        memory::provider_files::ProviderFileStore,
    };

    use super::{
        FILES_API_UNAVAILABLE, OpenAiAttachmentContext, OpenAiClient, ProviderAttachmentKind,
        classify, metadata_text, provider_has_no_files_api, selected_document_ids,
    };

    fn attachment(id: &str, media_type: &str, size_bytes: u64) -> Attachment {
        Attachment::new(
            id,
            PathBuf::from("attachments").join(format!("{id}.bin")),
            format!("{id}.bin"),
            media_type,
            size_bytes,
            id,
        )
    }

    #[test]
    fn classifies_supported_images_documents_and_unknown_files() {
        assert_eq!(classify("image/png"), ProviderAttachmentKind::Image);
        assert_eq!(
            classify("application/pdf"),
            ProviderAttachmentKind::Document
        );
        assert_eq!(
            classify("application/x-tar"),
            ProviderAttachmentKind::MetadataOnly
        );
    }

    #[test]
    fn selects_newest_documents_within_fifty_megabytes() {
        let parts = [
            MessagePart::Attachment(attachment("old", "application/pdf", 30 * 1024 * 1024)),
            MessagePart::Attachment(attachment("new", "application/pdf", 30 * 1024 * 1024)),
        ];

        let selected = selected_document_ids(parts.iter());

        assert!(selected.contains("new"));
        assert!(!selected.contains("old"));
    }

    #[test]
    fn metadata_fallback_contains_user_visible_file_details() {
        let file = attachment("archive", "application/x-tar", 42);

        let metadata = metadata_text(&file);

        assert!(metadata.contains("archive.bin"));
        assert!(metadata.contains("application/x-tar"));
        assert!(metadata.contains("42 bytes"));
        assert!(metadata.contains("attachments/archive.bin"));
    }

    #[test]
    fn recognizes_plain_text_files_api_404() {
        assert!(provider_has_no_files_api(
            "failed to deserialize api response: content:404 page not found"
        ));
        assert!(!provider_has_no_files_api("rate limit exceeded"));
    }

    #[tokio::test]
    async fn unavailable_files_api_inlines_image_as_data_url() -> Result<()> {
        let session_dir = std::env::temp_dir().join(format!(
            "codrik-openai-inline-image-test-{}",
            std::process::id()
        ));
        fs::remove_dir_all(&session_dir).await.ok();
        fs::create_dir_all(session_dir.join("attachments")).await?;
        fs::write(session_dir.join("attachments/screen.png"), b"png!").await?;
        let file = Attachment::new(
            "image",
            "attachments/screen.png",
            "screen.png",
            "image/png",
            4,
            "image-sha",
        );
        let client = OpenAiClient::new("test", "key", "http://127.0.0.1:1")
            .with_attachment_context(OpenAiAttachmentContext {
                provider_files: ProviderFileStore::new(&session_dir),
                session_dir: session_dir.clone(),
                image_detail: ImageDetailConfig::Auto,
            });
        client
            .files_api_capability
            .store(FILES_API_UNAVAILABLE, Ordering::Relaxed);

        let resolved = client.resolve_attachment(&file, &HashSet::new()).await?;

        assert_eq!(
            serde_json::to_value(resolved.content)?,
            serde_json::json!({
                "type": "input_image",
                "detail": "auto",
                "image_url": "data:image/png;base64,cG5nIQ=="
            })
        );
        fs::remove_dir_all(session_dir).await.ok();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "local mock networking conflicts with concurrent bashkit sandbox tests"]
    async fn image_upload_uses_vision_then_input_image() -> Result<()> {
        let server = MockOpenAi::start(vec![
            r#"{"id":"file-image","object":"file","bytes":4,"created_at":1,"filename":"screen.png","purpose":"vision","status":"processed"}"#,
            r#"{"id":"resp_1","object":"response","created_at":1,"model":"test","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok","annotations":[]}]}]}"#,
        ])
        .await?;
        let session_dir = std::env::temp_dir().join(format!(
            "codrik-openai-attachment-test-{}",
            std::process::id()
        ));
        fs::remove_dir_all(&session_dir).await.ok();
        fs::create_dir_all(session_dir.join("attachments"))
            .await
            .context("create test attachment directory")?;
        fs::write(session_dir.join("attachments/screen.png"), b"png!")
            .await
            .context("write test attachment")?;
        let file = Attachment::new(
            "image",
            "attachments/screen.png",
            "screen.png",
            "image/png",
            4,
            "image-sha",
        );
        let client = OpenAiClient::new("test", "key", server.base_url.clone())
            .with_attachment_context(OpenAiAttachmentContext {
                provider_files: ProviderFileStore::new(&session_dir),
                session_dir: session_dir.clone(),
                image_detail: ImageDetailConfig::Auto,
            });

        client
            .generate(
                LlmRequest {
                    messages: vec![Message::user(UserInput::new().push_attachment(file))],
                    tools: Vec::new(),
                },
                &RunContext::new(),
            )
            .await
            .context("generate response with uploaded image")?;

        let requests = server.requests();
        assert!(
            requests[0].contains("name=\"purpose\""),
            "unexpected upload request: {}",
            requests[0]
        );
        assert!(requests[0].contains("vision"));
        let json: serde_json::Value = serde_json::from_str(
            requests[1]
                .split_once("\r\n\r\n")
                .expect("request should have headers")
                .1,
        )?;
        assert_eq!(
            json["input"][0]["content"][0],
            serde_json::json!({
                "type": "input_image",
                "detail": "auto",
                "file_id": "file-image"
            })
        );

        let cached_server = MockOpenAi::start(vec![
            r#"{"id":"resp_2","object":"response","created_at":1,"model":"test","status":"completed","output":[{"type":"message","id":"msg_2","role":"assistant","status":"completed","content":[{"type":"output_text","text":"cached","annotations":[]}]}]}"#,
        ])
        .await?;
        let cached_file = Attachment::new(
            "image",
            "attachments/screen.png",
            "screen.png",
            "image/png",
            4,
            "image-sha",
        );
        let cached_client = OpenAiClient::new("test", "key", cached_server.base_url.clone())
            .with_attachment_context(OpenAiAttachmentContext {
                provider_files: ProviderFileStore::new(&session_dir),
                session_dir: session_dir.clone(),
                image_detail: ImageDetailConfig::Auto,
            });
        cached_client
            .generate(
                LlmRequest {
                    messages: vec![Message::user(UserInput::new().push_attachment(cached_file))],
                    tools: Vec::new(),
                },
                &RunContext::new(),
            )
            .await
            .context("generate response with cached image")?;
        let cached_requests = cached_server.requests();
        assert_eq!(cached_requests.len(), 1, "cached file must not be uploaded");
        assert!(cached_requests[0].contains("file-image"));

        fs::remove_dir_all(session_dir).await.ok();
        Ok(())
    }

    struct MockOpenAi {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl MockOpenAi {
        async fn start(responses: Vec<&'static str>) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .context("bind mock OpenAI server")?;
            let address = listener.local_addr()?;
            let requests = Arc::new(Mutex::new(Vec::new()));
            let captured = requests.clone();
            tokio::spawn(async move {
                let mut responses = VecDeque::from(responses);
                while let Some(body) = responses.pop_front() {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };
                    let Ok(request) = read_http_request(&mut socket).await else {
                        return;
                    };
                    captured
                        .lock()
                        .expect("request lock poisoned")
                        .push(request);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    if socket.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });

            Ok(Self {
                base_url: format!("http://{address}"),
                requests,
            })
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().expect("request lock poisoned").clone()
        }
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Result<String> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = socket.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            if headers
                .to_ascii_lowercase()
                .contains("transfer-encoding: chunked")
            {
                if bytes[header_end + 4..]
                    .windows(7)
                    .any(|window| window == b"\r\n0\r\n\r\n")
                {
                    break;
                }
                continue;
            }
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if bytes.len() >= header_end + 4 + content_length {
                break;
            }
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}
