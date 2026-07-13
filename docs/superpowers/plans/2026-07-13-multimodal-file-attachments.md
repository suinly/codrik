# Multimodal File Attachments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the gateway-independent agent retain and understand uploaded files through OpenAI Responses/Files APIs and send workspace or session files through an explicit `send_file` tool, with Telegram as the first transport.

**Architecture:** Messages gain ordered typed content, while session-local stores own attachment bytes and provider file-ID metadata. `OpenAiClient` moves from Chat Completions to Responses and resolves supported attachments through the Files API. Tool artifacts cross an agent-output boundary; Telegram owns download and delivery, and an application service coordinates session deletion with remote cleanup.

**Tech Stack:** Rust 2024, Tokio, async-trait, async-openai 0.41.1 (`responses`, `file`), Teloxide 0.17, serde/serde_json, sha2, infer, tempfile-style atomic writes implemented with Tokio filesystem APIs.

## Global Constraints

- Always run shell commands through `rtk`.
- Preserve SOLID module boundaries: agent orchestration, provider wire format, memory persistence, tools, and interfaces remain separate.
- A configured `base_url` must support compatible `/v1/responses` and `/v1/files`; do not retain a Chat Completions fallback.
- Store every session at `<session-root>/<session-id>/messages.json` with attachments under `attachments/` and provider IDs in `provider-files.json`.
- Read legacy `<session-id>.json` histories and migrate them atomically on the next append.
- Default `attachments.max_file_size_mb` is 20 and `attachments.image_detail` is `auto`.
- Pass every retained image to the model; select documents newest-first up to the fixed 50 MB combined provider limit.
- Accept and retain arbitrary files, but expose unsupported provider formats to the model as metadata only.
- Restrict `send_file` to the current actor workspace and current session directory after canonicalization.
- Telegram media groups, CLI attachment input/delivery, archive extraction, local document parsing, File Search, and Chat Completions compatibility are out of scope.
- Implement every behavioral change test-first and keep commits focused.

---

## File Structure

### Create

- `src/memory/attachments.rs` — streaming attachment persistence, content-derived metadata, and relative path creation.
- `src/memory/provider_files.rs` — atomic session-scoped provider file-ID cache.
- `src/tools/send_file.rs` — allowed-root path resolution and typed file artifact creation.
- `src/llm/openai/attachments.rs` — provider MIME classification, newest-first document selection, upload/cache resolution, and stale-ID recovery.
- `src/interfaces/telegram/files.rs` — Telegram attachment extraction/download and `FileReady` delivery.
- `src/app/session_deletion.rs` — remote provider cleanup followed by local session removal.

### Modify

- `Cargo.toml`, `Cargo.lock` — enable async-openai Responses/Files features and add content sniffing.
- `src/main.rs`, `src/memory.rs`, `src/tools.rs`, `src/llm.rs` — register new modules.
- `src/config.rs` — attachment configuration and defaults.
- `src/agent/message.rs` — `UserInput`, `Attachment`, and ordered `MessagePart` values.
- `src/memory/file.rs` — directory layout, typed content serialization, legacy migration, and atomic history writes.
- `src/memory/store.rs` — no provider details; retain the existing message persistence abstraction.
- `src/llm/client.rs` — agent-output events/sink and tool call semantics used by Responses.
- `src/llm/openai.rs` — Responses request/response/stream conversion and session attachment context.
- `src/agent/tool.rs`, all `src/tools/*.rs` handlers — typed `ToolExecution` results.
- `src/agent.rs` — `UserInput` execution and artifact delivery before function output persistence.
- `src/app.rs` — session-scoped attachment/cache composition and agent output wiring.
- `src/interfaces/cli.rs` — consume agent output text and reject file delivery explicitly.
- `src/interfaces/telegram.rs` — accept non-text messages and pass Telegram file sinks into runs.
- `src/interfaces/telegram/commands.rs` — `/sessions delete <id>` parsing and application-service call.
- `src/memory/telegram_sessions.rs` — session directory paths, exclusive deletion, and index removal.
- `README.md` — Responses provider requirement, attachment config, Telegram files, and delete command.

---

### Task 1: Ordered Multimodal Domain Messages and Configuration

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/config.rs`
- Modify: `src/agent/message.rs`
- Modify: `src/agent.rs`
- Modify: `src/memory/in_memory.rs`
- Modify: `src/memory/file.rs`
- Modify: `src/llm/openai.rs`

**Interfaces:**
- Produces: `Attachment`, `MessagePart`, `UserInput`, `Message::user(impl Into<UserInput>)`, `Message::text() -> String`.
- Produces: `AttachmentConfig { max_file_size_mb: u64, image_detail: ImageDetailConfig }` and `AppConfig::attachments` with defaults.

- [ ] **Step 1: Add failing configuration and message tests**

Add tests proving old YAML still loads defaults, explicit values override them, string input stays text-only, and attachment order is retained:

```rust
#[test]
fn attachment_config_defaults_when_omitted() -> Result<()> {
    let config: AppConfig = yaml_serde::from_str(
        "api_key: key\nbase_url: https://example.test/v1\nmodel: test\n",
    )?;
    assert_eq!(config.attachments.max_file_size_mb, 20);
    assert_eq!(config.attachments.image_detail, ImageDetailConfig::Auto);
    Ok(())
}

#[test]
fn user_input_preserves_text_then_attachment() {
    let attachment = Attachment::new(
        "att-1", "attachments/att-1.png", "screen.png",
        "image/png", 4, "abcd",
    );
    let input = UserInput::new()
        .push_text("inspect this")
        .push_attachment(attachment.clone());
    assert_eq!(
        input.parts(),
        &[MessagePart::Text("inspect this".into()), MessagePart::Attachment(attachment)]
    );
}

#[test]
fn string_becomes_text_only_user_input() {
    assert_eq!(UserInput::from("hello").text(), "hello");
}
```

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `rtk cargo test attachment_config_defaults_when_omitted`

Run: `rtk cargo test user_input_preserves_text_then_attachment`

Expected: FAIL because attachment configuration and multimodal message types do not exist.

- [ ] **Step 3: Add dependency features and exact domain types**

Change async-openai features and add MIME sniffing:

```toml
async-openai = { version = "0.41.1", features = ["chat-completion", "responses", "file"] }
bytes = "1.11"
infer = "0.19"
```

Keep `chat-completion` enabled only so Task 1 remains buildable before the adapter migration. Task 3 removes that feature after deleting all Chat types.

Define the domain API in `src/agent/message.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
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
        id: impl Into<String>, relative_path: impl Into<PathBuf>,
        display_name: impl Into<String>, media_type: impl Into<String>,
        size_bytes: u64, sha256: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(), relative_path: relative_path.into(),
            display_name: display_name.into(), media_type: media_type.into(),
            size_bytes, sha256: sha256.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessagePart {
    Text(String),
    Attachment(Attachment),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UserInput { parts: Vec<MessagePart> }

impl UserInput {
    pub fn new() -> Self { Self::default() }
    pub fn push_text(mut self, text: impl Into<String>) -> Self {
        self.parts.push(MessagePart::Text(text.into())); self
    }
    pub fn push_attachment(mut self, attachment: Attachment) -> Self {
        self.parts.push(MessagePart::Attachment(attachment)); self
    }
    pub fn parts(&self) -> &[MessagePart] { &self.parts }
    pub fn text(&self) -> String {
        self.parts.iter().filter_map(|part| match part {
            MessagePart::Text(text) => Some(text.as_str()),
            MessagePart::Attachment(_) => None,
        }).collect::<Vec<_>>().join("\n")
    }
}

impl From<String> for UserInput { fn from(value: String) -> Self { Self::new().push_text(value) } }
impl From<&str> for UserInput { fn from(value: &str) -> Self { Self::new().push_text(value) } }
```

Change `Message.content` to `Vec<MessagePart>`, make text constructors create one `Text` part, and add `Message::text()`. Update agent activity/final-answer reads to call `response.content` only for LLM responses and `Message::text()` only for stored messages. In the still-Chat-based OpenAI adapter, replace direct `message.content` reads with `message.text()` so this commit remains buildable; attachments intentionally do not enter provider requests until Task 4.

Make current file-memory serialization support both old string content and the new ordered parts without changing its path layout yet:

```rust
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum SessionContent {
    LegacyText(String),
    Parts(Vec<SessionPart>),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SessionPart {
    Text { text: String },
    Attachment { attachment: SessionAttachment },
}

#[derive(Serialize, Deserialize)]
struct SessionAttachment {
    id: String,
    relative_path: PathBuf,
    display_name: String,
    media_type: String,
    size_bytes: u64,
    sha256: String,
}
```

Implement lossless `From<Attachment> for SessionAttachment` and `From<SessionAttachment> for Attachment` conversions field-for-field.

Add serde-backed configuration defaults:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct AttachmentConfig {
    #[serde(default = "default_max_file_size_mb")]
    pub max_file_size_mb: u64,
    #[serde(default)]
    pub image_detail: ImageDetailConfig,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetailConfig { #[default] Auto, Low, High }

impl Default for AttachmentConfig {
    fn default() -> Self {
        Self { max_file_size_mb: 20, image_detail: ImageDetailConfig::Auto }
    }
}
```

Add `#[serde(default)] pub attachments: AttachmentConfig` to `AppConfig`.

- [ ] **Step 4: Run focused and crate tests**

Run: `rtk cargo test agent::message`

Run: `rtk cargo test config::tests`

Run: `rtk cargo test memory::in_memory::tests`

Run: `rtk cargo test`

Expected: PASS; existing text-only agent tests compile with calls such as `Message::user("hello")`.

- [ ] **Step 5: Commit**

```bash
rtk git add Cargo.toml Cargo.lock src/config.rs src/agent/message.rs src/agent.rs src/memory/in_memory.rs src/memory/file.rs src/llm/openai.rs
rtk git commit -m "feat(agent): add typed attachment inputs"
```

---

### Task 2: Session Directories, Attachment Persistence, and Provider Cache

**Files:**
- Create: `src/memory/attachments.rs`
- Create: `src/memory/provider_files.rs`
- Modify: `src/memory.rs`
- Modify: `src/memory/file.rs`

**Interfaces:**
- Consumes: `Attachment`, `MessagePart` from Task 1.
- Produces: `FileMemoryStore::session_dir() -> &Path`.
- Produces: `AttachmentStore::new(session_dir, max_bytes)` and `store_stream<S, E>(display_name, stream) -> Result<Attachment>` where `S: Stream<Item = Result<Bytes, E>> + Unpin` and `E: Error + Send + Sync + 'static`.
- Produces: `ProviderFileStore::{get, put, remove, entries}` keyed by `ProviderFileKey`.

- [ ] **Step 1: Write failing storage, migration, and cache tests**

Add async tests for the new layout and byte-derived metadata:

```rust
#[tokio::test]
async fn stores_history_inside_session_directory() -> Result<()> {
    let root = temp_session_root();
    let memory = FileMemoryStore::new(&root, "work")?;
    memory.append(Message::user("hello")).await?;
    assert!(fs::try_exists(root.join("work/messages.json")).await?);
    assert!(!fs::try_exists(root.join("work.json")).await?);
    Ok(())
}

#[tokio::test]
async fn migrates_legacy_file_on_append() -> Result<()> {
    let root = temp_session_root();
    fs::create_dir_all(&root).await?;
    fs::write(root.join("work.json"), r#"[{"role":"user","content":"old"}]"#).await?;
    let memory = FileMemoryStore::new(&root, "work")?;
    memory.append(Message::user("new")).await?;
    assert!(!fs::try_exists(root.join("work.json")).await?);
    assert_eq!(memory.load_context().await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn attachment_store_enforces_actual_stream_size() {
    let store = AttachmentStore::new(temp_session_root(), 3);
    let error = store.store_stream(
        "x.txt",
        stream::iter([Ok::<Bytes, std::io::Error>(Bytes::from_static(b"four"))]),
    ).await.unwrap_err();
    assert!(error.to_string().contains("exceeds 3 bytes"));
}
```

Add a provider cache round-trip test using key `(sha256, normalized base URL, purpose)` and verify atomic JSON persists across a new `ProviderFileStore` instance.

- [ ] **Step 2: Run tests and verify failure**

Run: `rtk cargo test stores_history_inside_session_directory`

Run: `rtk cargo test memory::attachments::tests`

Run: `rtk cargo test memory::provider_files::tests`

Expected: FAIL because the stores and directory layout are absent.

- [ ] **Step 3: Implement session directory paths and atomic writes**

Retain the typed/legacy content representation from Task 1. Set `FileMemoryStore` paths to `legacy_path = root/<id>.json`, `session_dir = root/<id>`, and `path = session_dir/messages.json`. Implement `write_atomic(path, bytes)` as create parent, write `<filename>.tmp`, `sync_all`, then `rename`. On append, read `messages.json` or the legacy file, atomically write the new path, then remove the legacy file only after rename succeeds.

- [ ] **Step 4: Implement attachment and provider-cache stores**

`AttachmentStore::store_stream` must write chunks to `<session>/attachments/.download-<counter>.tmp`, stop once actual bytes exceed the limit, hash with `Sha256`, sniff the completed prefix with `infer`, derive a safe extension from verified MIME, and rename to `attachments/<sha256-prefix>-<counter>.<ext>`. It returns only a session-relative path.

Define cache records exactly:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderUploadPurpose { Vision, UserData }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderFileKey {
    pub sha256: String,
    pub provider: String,
    pub purpose: ProviderUploadPurpose,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderFileRecord { pub key: ProviderFileKey, pub file_id: String }
```

Use one `Arc<Mutex<()>>` per cloned store and the same atomic-write helper for `provider-files.json`.

- [ ] **Step 5: Run storage tests**

Run: `rtk cargo test memory::`

Expected: PASS, including legacy capitalized roles and concurrent history append tests.

- [ ] **Step 6: Commit**

```bash
rtk git add src/memory.rs src/memory/file.rs src/memory/attachments.rs src/memory/provider_files.rs
rtk git commit -m "feat(memory): persist session file attachments"
```

---

### Task 3: Replace Chat Completions with Responses for Text and Functions

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/llm/openai.rs`
- Modify: `src/llm/client.rs`
- Modify: `src/llm.rs`

**Interfaces:**
- Consumes: ordered `MessagePart` but handles text-only parts in this task.
- Preserves: `LlmClient`, `LlmStreamClient`, `LlmResponse`, `LlmStreamEvent`, and `LlmToolCall` agent-facing behavior.
- Defines: `LlmToolCall.id` as the Responses function `call_id`, not the output item ID.

- [ ] **Step 1: Replace Chat request tests with Responses mapping tests**

Add unit tests around pure conversion helpers:

```rust
#[test]
fn tool_result_maps_to_function_call_output_by_call_id() -> Result<()> {
    let item = OpenAiClient::to_response_item(Message::tool_result("call_1", "sunny"))?;
    assert_eq!(serde_json::to_value(item)?, json!({
        "type": "function_call_output", "call_id": "call_1", "output": "sunny"
    }));
    Ok(())
}

#[test]
fn response_function_call_uses_call_id() -> Result<()> {
    let response: Response = serde_json::from_value(json!({
        "id": "resp_1", "object": "response", "created_at": 1,
        "status": "completed", "error": null, "incomplete_details": null,
        "instructions": null, "max_output_tokens": null, "model": "test",
        "output": [{
            "type": "function_call", "id": "item_1", "call_id": "call_1",
            "name": "weather", "arguments": "{}", "status": "completed"
        }],
        "parallel_tool_calls": true, "previous_response_id": null,
        "reasoning": {"effort": null, "summary": null}, "store": false,
        "temperature": 1.0, "text": {"format": {"type": "text"}},
        "tool_choice": "auto", "tools": [], "top_p": 1.0,
        "truncation": "disabled", "usage": null, "metadata": {}
    }))?;
    assert_eq!(OpenAiClient::to_llm_response(response)?.tool_calls[0].id, "call_1");
    Ok(())
}
```

Add stream accumulator tests for `response.output_text.delta`, `response.output_item.added` function calls, `response.function_call_arguments.delta`, and `response.completed`.

- [ ] **Step 2: Run tests and verify failure**

Run: `rtk cargo test llm::openai::tests`

Expected: FAIL because the adapter still creates Chat Completion messages and parses Chat stream chunks.

- [ ] **Step 3: Implement Responses request conversion**

Remove the `chat-completion` async-openai feature so the dependency line becomes:

```toml
async-openai = { version = "0.41.1", features = ["responses", "file"] }
```

Use async-openai Responses types. Build stateless requests with complete local history, `store(false)`, configured instructions, and function tools:

```rust
fn to_openai_request(&self, request: LlmRequest) -> Result<CreateResponse> {
    Ok(CreateResponseArgs::default()
        .model(self.model.clone())
        .instructions(self.instructions_from_system(&request.messages))
        .input(InputParam::Items(self.to_response_items(request.messages)?))
        .tools(request.tools.into_iter().map(Self::to_response_tool).collect())
        .store(false)
        .build()?)
}
```

Map user/assistant text to message items, assistant tool calls to `FunctionToolCall` items, and tool results to `FunctionCallOutputItemParam`. Keep system instructions out of ordinary input items.

- [ ] **Step 4: Implement non-streaming and streaming response parsing**

For non-streaming responses, concatenate `OutputItem::Message` output text and collect `OutputItem::FunctionCall` into `LlmToolCall { id: call_id, name, arguments }`.

For streams, emit `TextDelta` from `ResponseOutputTextDelta`, initialize a partial tool call from `ResponseOutputItemAdded(FunctionCall)`, append arguments from `ResponseFunctionCallArgumentsDelta`, and use `ResponseCompleted` as the authoritative terminal response. Return an error on `ResponseFailed`, `ResponseIncomplete`, or `ResponseError`.

- [ ] **Step 5: Run adapter and agent regression tests**

Run: `rtk cargo test llm::openai`

Run: `rtk cargo test agent::tests`

Expected: PASS; text streaming and multi-round function execution retain existing observable behavior.

- [ ] **Step 6: Commit**

```bash
rtk git add Cargo.toml Cargo.lock src/llm.rs src/llm/client.rs src/llm/openai.rs
rtk git commit -m "refactor(llm): migrate OpenAI adapter to Responses"
```

---

### Task 4: Resolve Attachments through the Files API

**Files:**
- Create: `src/llm/openai/attachments.rs`
- Modify: `src/llm/openai.rs`
- Modify: `src/app.rs`

**Interfaces:**
- Consumes: `Attachment`, `ProviderFileStore`, `ProviderUploadPurpose`, `AttachmentConfig`.
- Produces: `OpenAiAttachmentContext { session_dir, provider_files, image_detail }`.
- Produces: `OpenAiClient::with_attachment_context(context)` and `OpenAiClient::delete_file(file_id)`.
- Test-only: `MockOpenAi { base_url, requests }` backed by `TcpListener`, with `enqueue(status, headers, body)` and `take_requests()` so multipart and JSON requests are asserted without external calls. The same test module defines `completed_text_response`, `client_with_attachment`, `image_attachment`, and `single_user_request` fixture builders used by its tests.

- [ ] **Step 1: Write failing provider selection/cache tests**

Use a local mock HTTP server and assert:

```rust
#[tokio::test]
async fn image_upload_uses_vision_then_input_image() -> Result<()> {
    let server = MockOpenAi::start().await;
    server.enqueue(200, "application/json", r#"{"id":"file-image","object":"file","bytes":4,"created_at":1,"filename":"screen.png","purpose":"vision","status":"processed"}"#).await;
    server.enqueue(200, "application/json", completed_text_response("ok")).await;
    let client = client_with_attachment(&server, image_attachment()).await?;
    client.generate(single_user_request(), &RunContext::new()).await?;
    let requests = server.take_requests().await;
    assert!(requests[0].body_text().contains("name=\"purpose\"\r\n\r\nvision"));
    assert_eq!(requests[1].json()["input"][0]["content"][0], json!({
        "type": "input_image", "file_id": "file-image", "detail": "auto"
    }));
    Ok(())
}

#[tokio::test]
async fn documents_are_selected_newest_first_within_fifty_mb() {
    let selected = select_documents(&[doc("old", 30), doc("new", 30)]);
    assert_eq!(selected.model_file_ids(), ["new"]);
    assert_eq!(selected.metadata_only_ids(), ["old"]);
}
```

Also test `user_data` document uploads, cached ID reuse without a second multipart call, unsupported MIME metadata, and one stale-ID re-upload/retry.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test llm::openai::attachments::tests`

Expected: FAIL because attachment context and Files resolution do not exist.

- [ ] **Step 3: Implement classification and selection**

Define exact provider classes:

```rust
enum ProviderAttachmentKind { Image, Document, MetadataOnly }

const MAX_DOCUMENT_INPUT_BYTES: u64 = 50 * 1024 * 1024;

fn classify(media_type: &str) -> ProviderAttachmentKind {
    match media_type {
        "image/png" | "image/jpeg" | "image/webp" | "image/gif" => ProviderAttachmentKind::Image,
        media_type if SUPPORTED_DOCUMENT_MIME_TYPES.contains(&media_type) => ProviderAttachmentKind::Document,
        _ => ProviderAttachmentKind::MetadataOnly,
    }
}

fn selected_document_ids(parts: &[MessagePart]) -> HashSet<String> {
    let mut remaining = MAX_DOCUMENT_INPUT_BYTES;
    parts.iter().rev().filter_map(|part| match part {
        MessagePart::Attachment(file) if classify(&file.media_type) == ProviderAttachmentKind::Document
            && file.size_bytes <= remaining => {
                remaining -= file.size_bytes;
                Some(file.id.clone())
            }
        _ => None,
    }).collect()
}
```

Use the image allowlist `image/png`, `image/jpeg`, `image/webp`, and non-animated `image/gif`. Build the document allowlist from the MIME/extensions listed in the official File Inputs guide. Everything else becomes metadata text with display name, MIME, size, and relative path.

- [ ] **Step 4: Implement lazy upload and cache resolution**

For each selected supported attachment, resolve the absolute path with `session_dir.join(relative_path)`, verify it remains inside the canonical session directory, look up `ProviderFileKey`, otherwise call:

```rust
let uploaded = client.files().create(CreateFileRequest {
    file: FileInput { source: InputSource::Path { path } },
    purpose: match kind {
        ProviderAttachmentKind::Image => FilePurpose::Vision,
        ProviderAttachmentKind::Document => FilePurpose::UserData,
        ProviderAttachmentKind::MetadataOnly => unreachable!(),
    },
    ..Default::default()
}).await?;
```

Persist the ID before constructing `InputImageContent` or `InputFileContent`. If `/responses` reports a missing/inaccessible cached ID, evict only the referenced records, upload again, and retry once.

- [ ] **Step 5: Wire session context in `app.rs`**

For file-backed runs, construct `ProviderFileStore::new(memory.session_dir())` and pass:

```rust
OpenAiAttachmentContext {
    session_dir: memory.session_dir().to_path_buf(),
    provider_files,
    image_detail: config.attachments.image_detail,
}
```

Text-only in-memory CLI agents construct `OpenAiClient` without attachment context.

- [ ] **Step 6: Run provider and app tests**

Run: `rtk cargo test llm::openai`

Run: `rtk cargo test app::tests`

Expected: PASS with no external network calls.

- [ ] **Step 7: Commit**

```bash
rtk git add src/llm/openai.rs src/llm/openai/attachments.rs src/app.rs
rtk git commit -m "feat(llm): upload model file inputs"
```

---

### Task 5: Typed Tool Artifacts, Agent Output, and `send_file`

**Files:**
- Create: `src/tools/send_file.rs`
- Modify: `src/tools.rs`
- Modify: `src/agent/tool.rs`
- Modify: `src/agent/tool_observation.rs`
- Modify: `src/agent.rs`
- Modify: `src/llm/client.rs`
- Modify: `src/interfaces/cli.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/app.rs`
- Modify: each existing `src/tools/*.rs` handler returning text.

**Interfaces:**
- Produces: `ToolExecution { observation, artifacts }`, `ToolArtifact::File(FileArtifact)`.
- Produces: `AgentOutputEvent::{TextDelta, FileReady}` and `AgentOutputSink`.
- Produces: `SendFileToolConfig { roots: Vec<FileRoot> }` with virtual `workspace/` and session roots.
- Produces: `ToolRegistryConfig.file_roots: Vec<FileRoot>`; session-backed app builders supply both the actor workspace and session directory.
- Test-only: add `temp_send_file_root`, `tool_call_response`, `final_response`, `OneFileArtifactTool`, and `FailingFileSink` beside the existing scripted agent fixtures.

- [ ] **Step 1: Write failing `send_file` and agent-delivery tests**

```rust
#[tokio::test]
async fn send_file_accepts_session_file_and_returns_artifact() -> Result<()> {
    let root = std::env::temp_dir().join(format!("codrik-send-file-{}", std::process::id()));
    fs::create_dir_all(&root).await?;
    fs::write(root.join("report.pdf"), b"pdf").await?;
    let tool = SendFileTool::new(SendFileToolConfig::session(&root));
    let result = tool.execute(r#"{"path":"report.pdf","caption":"Report"}"#).await?;
    assert!(matches!(result.artifacts.as_slice(), [ToolArtifact::File(_)]));
    fs::remove_dir_all(root).await.ok();
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn send_file_rejects_symlink_escaping_roots() -> Result<()> {
    let root = temp_send_file_root("root");
    let outside = temp_send_file_root("outside");
    fs::create_dir_all(&root).await?;
    fs::write(&outside, b"secret").await?;
    std::os::unix::fs::symlink(&outside, root.join("escape"))?;
    let error = SendFileTool::new(SendFileToolConfig::session(&root))
        .execute(r#"{"path":"escape"}"#).await.unwrap_err();
    assert!(error.to_string().contains("outside allowed file roots"));
    fs::remove_dir_all(root).await.ok();
    fs::remove_file(outside).await.ok();
    Ok(())
}

#[tokio::test]
async fn sink_failure_becomes_tool_failure_observation() -> Result<()> {
    let client = ScriptedClient::new(vec![tool_call_response("send_file"), final_response("done")]);
    let agent = Agent::new(client.clone(), InMemoryStore::new(), OneFileArtifactTool);
    let mut sink = FailingFileSink;
    agent.execute_streaming("send it", &mut sink).await?;
    let requests = client.requests().await;
    assert!(requests[1].iter().any(|message| message.text().contains("failed")));
    Ok(())
}
```

Add path traversal, missing file, directory, workspace file, and successful delivery-order tests.

- [ ] **Step 2: Run tests and verify failure**

Run: `rtk cargo test tools::send_file`

Run: `rtk cargo test sink_failure_becomes_tool_failure_observation`

Expected: FAIL because tool results are strings and the output sink is text-only.

- [ ] **Step 3: Introduce typed execution and output contracts**

```rust
pub struct ToolExecution {
    pub observation: String,
    pub artifacts: Vec<ToolArtifact>,
}

pub enum ToolArtifact { File(FileArtifact) }

pub struct FileArtifact {
    pub path: PathBuf,
    pub display_name: String,
    pub media_type: String,
    pub caption: Option<String>,
}

pub enum AgentOutputEvent { TextDelta(String), FileReady(FileArtifact) }

#[async_trait]
pub trait AgentOutputSink: Send {
    async fn on_event(&mut self, event: AgentOutputEvent) -> Result<()>;
}
```

Change `ToolExecutor` and `ToolHandler` to return `Result<ToolExecution>`. Update all existing handlers to return `ToolExecution::text(existing_string)`; do not change their observations.

- [ ] **Step 4: Implement `send_file` root validation**

Parse `{ path, caption }`, map the `workspace/<relative-path>` and `attachments/<relative-path>` virtual prefixes to configured roots, canonicalize the candidate and root, require `candidate.starts_with(root)`, require regular-file metadata, sniff MIME, and return one `FileArtifact`. Register it as a standard tool so wildcard-authorized actors receive it.

- [ ] **Step 5: Deliver artifacts inside the agent loop**

Map buffered LLM text deltas to `AgentOutputEvent::TextDelta`. During tool processing, deliver every artifact before appending the tool-result message:

```rust
let observation = match deliver_artifacts(execution.artifacts, output).await {
    Ok(()) => execution.observation,
    Err(error) => tool_observation::failure(&error),
};
self.memory.append(Message::tool_result(tool_call.id, observation)).await?;
```

The CLI sink prints text deltas and returns `bail!("file output is unsupported by the CLI")` for `FileReady`. Adapt `TelegramDraftStream` and `DiscardingStreamSink` to the new `AgentOutputSink` in this task: both preserve text behavior and return `bail!("file output is not configured")` for file events. Task 6 replaces that temporary Telegram file rejection with real delivery.

- [ ] **Step 6: Run tool, agent, and CLI tests**

Run: `rtk cargo test tools::`

Run: `rtk cargo test agent::tests`

Run: `rtk cargo test interfaces::cli::tests`

Run: `rtk cargo test interfaces::telegram`

Expected: PASS, including existing tools' byte-for-byte observations.

- [ ] **Step 7: Commit**

```bash
rtk git add src/tools.rs src/tools/send_file.rs src/tools/*.rs src/agent.rs src/agent/tool.rs src/agent/tool_observation.rs src/llm/client.rs src/interfaces/cli.rs src/interfaces/telegram.rs src/app.rs
rtk git commit -m "feat(tools): add send file artifacts"
```

---

### Task 6: Telegram File Input and Delivery

**Files:**
- Create: `src/interfaces/telegram/files.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `src/app.rs`

**Interfaces:**
- Consumes: `AttachmentStore`, `UserInput`, `AgentOutputSink`, `FileArtifact`.
- Produces: `TelegramIncomingFile::from_message(&Message)` and `TelegramAgentOutputSink`.
- Test-only: define JSON fixture builders `photo_fixture`, `document_fixture`, and `telegram_message` in `interfaces::telegram::files::tests`.

- [ ] **Step 1: Write failing Telegram extraction and delivery tests**

Use deserialized Telegram update fixtures so tests do not call Telegram:

```rust
#[test]
fn largest_photo_becomes_incoming_file_with_caption() {
    let message = telegram_message(photo_fixture(), Some("inspect"));
    let incoming = TelegramIncomingFile::from_message(&message).unwrap();
    assert_eq!(incoming.caption.as_deref(), Some("inspect"));
    assert_eq!(incoming.file_id, "largest-photo-id");
}

#[test]
fn document_preserves_display_name() {
    let incoming = TelegramIncomingFile::from_message(&document_fixture("report.pdf")).unwrap();
    assert_eq!(incoming.display_name, "report.pdf");
}
```

Test captionless input, text-only input, image artifact choosing photo delivery, and other MIME choosing document delivery through a mock `TelegramFileSender` trait.

- [ ] **Step 2: Run tests and verify failure**

Run: `rtk cargo test interfaces::telegram::files::tests`

Expected: FAIL because Telegram ignores every non-text message.

- [ ] **Step 3: Implement incoming extraction and streaming download**

`TelegramIncomingFile::from_message` chooses the largest `PhotoSize` or the message `Document`. Authorized handling resolves the active session first, calls `bot.get_file(file_id)`, consumes `bot.download_file_stream(&file.path)`, and passes the stream to `AttachmentStore::store_stream`. Build:

```rust
let input = UserInput::new()
    .push_text(message.caption().unwrap_or_default())
    .push_attachment(attachment);
```

When the caption is absent, omit the empty text part rather than storing `Text("")`; use an explicit `match message.caption()` around `push_text`.

Commands remain text-only and are parsed before file download. Unauthorized users must not trigger a download.

- [ ] **Step 4: Implement Telegram agent output sink**

Wrap the existing draft stream and bot/chat context. Forward text deltas unchanged. For `FileReady`, use `InputFile::file(path)`, `send_photo` for provider-supported image MIME, otherwise `send_document`; apply caption only when present. Propagate delivery errors so the agent records a failed function output.

- [ ] **Step 5: Wire private and regular chat runs**

Change `answer_authorized_message`, `answer_private_chat`, `answer_regular_chat`, and app run functions from `String` to `UserInput`. Both private and regular runs receive a Telegram output sink; regular chats still discard text streaming but must deliver file events.

- [ ] **Step 6: Run Telegram and app tests**

Run: `rtk cargo test interfaces::telegram`

Run: `rtk cargo test app::tests`

Expected: PASS; `/start`, `/stop`, `/new`, superseding runs, and text answers retain existing behavior.

- [ ] **Step 7: Commit**

```bash
rtk git add src/interfaces/telegram.rs src/interfaces/telegram/files.rs src/app.rs
rtk git commit -m "feat(telegram): accept and deliver files"
```

---

### Task 7: Inactive Session Deletion with Provider Cleanup

**Files:**
- Create: `src/app/session_deletion.rs`
- Modify: `src/app.rs`
- Modify: `src/memory/telegram_sessions.rs`
- Modify: `src/interfaces/telegram/commands.rs`
- Modify: `src/interfaces/telegram.rs`

**Interfaces:**
- Consumes: `ProviderFileStore::entries`, `OpenAiClient::delete_file`.
- Produces: `TelegramSessionStore::{begin_delete, finish_delete}` and `BeginDeleteResult::{NotFound, Active, Ready}`.
- Produces: `SessionDeletionService::delete(chat_id, session_id) -> Result<SessionDeletionOutcome>`.
- Test-only: define `deletion_fixture_with_ids` and `RecordingDeleter::failing_only` in `app::session_deletion::tests`.

- [ ] **Step 1: Write failing parsing and deletion tests**

```rust
#[test]
fn parses_delete_session_command() {
    assert_eq!(
        TelegramCommand::parse("/sessions delete old", Some("CodrikBot")),
        Some(TelegramCommand::DeleteSession("old".into()))
    );
}

#[tokio::test]
async fn refuses_to_delete_active_session() {
    let store = TelegramSessionStore::new(temp_store_root());
    let active = store.active_session_id(123).await.unwrap();
    assert_eq!(store.begin_delete(123, &active).await.unwrap(), BeginDeleteResult::Active);
}

#[tokio::test]
async fn local_delete_continues_after_partial_remote_failure() -> Result<()> {
    let fixture = deletion_fixture_with_ids(["file-ok", "file-fail"]).await?;
    let deleter = RecordingDeleter::failing_only("file-fail");
    let outcome = fixture.service(&deleter).delete(123, &fixture.inactive_id).await?;
    assert_eq!(outcome.failed_remote_deletions, 1);
    assert!(!fs::try_exists(&fixture.session_dir).await?);
    assert!(!fixture.store.list_sessions(123).await?.iter().any(|s| s.id == fixture.inactive_id));
    Ok(())
}
```

Also test foreign/unknown session, successful removal from `index.json`, local directory removal, and the exact partial-cleanup user message.

- [ ] **Step 2: Run tests and verify failure**

Run: `rtk cargo test refuses_to_delete_active_session`

Run: `rtk cargo test parses_delete_session_command`

Run: `rtk cargo test app::session_deletion::tests`

Expected: FAIL because deletion APIs and command variants are absent.

- [ ] **Step 3: Add exclusive local deletion primitive**

Under `TelegramSessionStore.write_lock`, validate safe ID and chat membership, reject the active ID, mark the record as deleting, and atomically write `index.json` before releasing the lock. `switch_session` must reject deleting records. The deletion service then performs remote cleanup without holding the global store lock and calls `finish_delete` to remove local state:

```rust
pub enum BeginDeleteResult { NotFound, Active, Ready { session_dir: PathBuf } }

pub async fn begin_delete(&self, chat_id: i64, session_id: &str) -> Result<BeginDeleteResult>;
pub async fn finish_delete(&self, chat_id: i64, session_id: &str) -> Result<()>;
```

Add `#[serde(default)] deleting: bool` to `ChatSessionRecord` for backward compatibility. `finish_delete` removes the session directory, removes the marked index record, and atomically rewrites `index.json`. If remote cleanup encounters failures, still call `finish_delete`.

- [ ] **Step 4: Implement application cleanup orchestration**

Define a narrow mockable provider interface:

```rust
#[async_trait]
pub trait ProviderFileDeleter: Send + Sync {
    async fn delete_file(&self, file_id: &str) -> Result<()>;
}

pub struct SessionDeletionOutcome {
    pub failed_remote_deletions: usize,
}
```

Load and deduplicate cached IDs, attempt every delete, count failures, and return the count to the Telegram formatter. Never let a remote failure prevent local deletion.

- [ ] **Step 5: Wire `/sessions delete <id>`**

Pass `SessionDeletionService` into command handling. Return distinct Russian messages for not found, active refusal, full success, and local success with `N` remote cleanup failures. Keep authorization checks before command execution.

- [ ] **Step 6: Run deletion and session regression tests**

Run: `rtk cargo test app::session_deletion`

Run: `rtk cargo test memory::telegram_sessions`

Run: `rtk cargo test interfaces::telegram::commands`

Expected: PASS, including existing session create/list/switch and legacy migration tests.

- [ ] **Step 7: Commit**

```bash
rtk git add src/app.rs src/app/session_deletion.rs src/memory/telegram_sessions.rs src/interfaces/telegram.rs src/interfaces/telegram/commands.rs
rtk git commit -m "feat(telegram): delete inactive file sessions"
```

---

### Task 8: Documentation, Full Verification, and Compatibility Audit

**Files:**
- Modify: `README.md`
- Modify: `agent_instructions.md`
- Test: all crate and integration tests.

**Interfaces:**
- Consumes: all prior tasks.
- Produces: documented provider requirements, configuration, Telegram behavior, and model guidance for `send_file`.

- [ ] **Step 1: Add documentation assertions where existing tests inspect instructions**

Extend the relevant app/tool tests to assert the default agent sees the `send_file` definition only when allowed and that its description says it sends an existing file rather than embedding a path in final text.

- [ ] **Step 2: Update user documentation and model instructions**

Document:

```yaml
attachments:
  max_file_size_mb: 20
  image_detail: auto
```

State that `base_url` must implement Responses and Files APIs, list accepted Telegram photo/document behavior, explain storage at `<chat>/<session>/`, show `/sessions delete <id>`, and state that unsupported binary formats are retained/deliverable but metadata-only for the model.

In `agent_instructions.md`, tell the model to call `send_file` whenever the user requests a generated/existing file and never claim delivery merely by printing a server path.

- [ ] **Step 3: Run formatting**

Run: `rtk cargo fmt --check`

Expected: PASS. If it fails, run `rtk cargo fmt`, inspect the diff, then rerun `rtk cargo fmt --check`.

- [ ] **Step 4: Run the full test suite**

Run: `rtk cargo test`

Expected: PASS with all unit and integration tests.

- [ ] **Step 5: Run compile and lint verification**

Run: `rtk cargo check`

Expected: PASS.

Run: `rtk cargo clippy --all-targets --all-features`

Expected: PASS with no warnings.

- [ ] **Step 6: Audit scope and repository state**

Run: `rtk git status --short`

Expected: only intended README/instruction changes are uncommitted; `.superpowers/` visual-companion artifacts remain untracked and must not be staged.

Run: `rtk git diff --check`

Expected: no whitespace errors.

- [ ] **Step 7: Commit**

```bash
rtk git add README.md agent_instructions.md
rtk git commit -m "docs(agent): document file attachments"
```

- [ ] **Step 8: Final clean verification**

Run: `rtk cargo test`

Run: `rtk cargo check`

Run: `rtk cargo fmt --check`

Run: `rtk cargo clippy --all-targets --all-features`

Expected: every command exits 0. Record these commands and results in the implementation handoff.
