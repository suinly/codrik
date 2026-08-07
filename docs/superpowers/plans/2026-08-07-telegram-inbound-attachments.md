# Telegram Inbound Attachments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Accept every supported Telegram file category as a durable actor input, retain its bytes under `CODRIK_HOME`, and pass provider-supported files to the model.

**Architecture:** Telegram normalizes media metadata, resolves `file_id`, and streams authorized downloads into an actor-scoped content-addressed store. SQLite stores only typed attachment metadata; dispatch rebuilds `UserInput`, while each model run receives its actor attachment root through `RunContext`. Forced actor deletion removes retained bytes after the SQLite transaction commits.

**Tech Stack:** Rust 2024, Tokio, reqwest, async-trait, futures-util, bytes, serde/serde_json, sha2, infer, async-openai Responses/Files API, SQLite via tokio-rusqlite.

## Global Constraints

- Run every shell command through `rtk`.
- Use only Telegram's hosted Bot API at `https://api.telegram.org`.
- Accept `photo`, `document`, `video`, `animation`, `audio`, `voice`, `video_note`, and `sticker` from private non-bot senders.
- Treat every Telegram update, including each media-group member, as one independent user turn.
- Enforce exactly `20_000_000` downloaded bytes; accept the limit, reject any byte above it.
- Store bytes at `<CODRIK_HOME>/attachments/<actor-id>/<sha256>.<extension>` and persist only actor-relative paths.
- Put a non-empty caption before the attachment; attachment-only input is valid.
- Infer MIME from bytes. Treat Telegram names, MIME values, sizes, paths, captions, and content as untrusted.
- Keep unsupported provider formats as metadata-only model content. Do not convert, transcribe, extract, or aggregate media.
- Retain files until `actors delete --force`; no TTL or orphan collector.
- Add no dependency and no SQLite migration.
- Preserve text, `/link`, webhook, polling, output-file, legacy session, and actor-administration behavior.
- Never log or persist bot tokens, credential-bearing URLs, file bytes, or absolute attachment paths.
- Implement behavior test-first; keep commits focused.

---

## File Structure

### Create

- `src/runtime/attachments.rs` - actor-scoped streamed storage, byte limit, MIME inference, content addressing, safe removal.

### Modify

- `src/runtime.rs` - register the runtime attachment module.
- `src/config.rs` - expose fixed `<CODRIK_HOME>/attachments` in `RuntimePaths`.
- `src/app.rs` - create/validate the root; wire Telegram, runners, OpenAI configuration, actor deletion.
- `src/interfaces/telegram/types.rs` - deserialize and normalize all supported media variants.
- `src/interfaces/telegram/api.rs` - add `getFile` and credential-safe streaming download.
- `src/interfaces/telegram.rs` - inject the API and attachment root into shared ingress.
- `src/interfaces/telegram/ingress.rs` - authorize, download, persist, ingest, signal, report user failures.
- `src/interfaces/telegram/polling.rs` - update API test doubles for the expanded trait.
- `src/runtime/store.rs` - add typed attachment event payload construction.
- `src/runtime/sqlite/dispatch.rs` - reconstruct caption-plus-attachment messages durably.
- `src/llm/client.rs` - carry an optional actor attachment root in `RunContext`.
- `src/llm/openai.rs` - pass `RunContext` into request conversion.
- `src/llm/openai/attachments.rs` - resolve files beneath the per-run actor root and keep provider cache actor-local.
- `src/runtime/runner.rs` - configure and propagate the actor attachment root.
- `src/runtime/actor_admin.rs` - remove retained attachments after forced deletion.
- `tests/serve_runtime.rs` - update Telegram API test doubles and add full runtime coverage.
- `README.md` - document Telegram inbound media, fixed hosted limit, storage, retention.

---

### Task 1: Runtime Attachment Root and Store

**Files:**
- Create: `src/runtime/attachments.rs`
- Modify: `src/runtime.rs`
- Modify: `src/config.rs:382-445`
- Modify: `src/app.rs:580-680`

**Interfaces:**
- Consumes: `ActorId::parse_workspace_safe`, `Attachment`, `Stream<Item = Result<Bytes, E>>`.
- Produces: `TELEGRAM_MAX_DOWNLOAD_BYTES`, `RuntimeAttachmentStore::new`, `actor_root`, `store_stream`, `remove_actor`.

- [ ] **Step 1: Add failing fixed-path and storage tests**

Add a `RuntimePaths` assertion in `src/config.rs`:

```rust
assert_eq!(paths.attachments, PathBuf::from("/tmp/codrik-home/attachments"));
```

Create tests in `src/runtime/attachments.rs` using unique directories beneath `std::env::temp_dir()`:

```rust
#[tokio::test]
async fn accepts_exact_telegram_limit_and_rejects_one_more_byte() -> Result<()> {
    let root = temp_root("limit");
    fs::remove_dir_all(&root).await.ok();
    let store = RuntimeAttachmentStore::new(&root);
    let actor = ActorId::parse_workspace_safe("alice")?;

    let accepted = stream::iter([Ok::<_, Infallible>(Bytes::from(vec![
        b'x'; TELEGRAM_MAX_DOWNLOAD_BYTES as usize
    ]))]);
    assert_eq!(
        store.store_stream(&actor, "exact.bin", accepted).await?.size_bytes,
        TELEGRAM_MAX_DOWNLOAD_BYTES
    );

    let rejected = stream::iter([Ok::<_, Infallible>(Bytes::from(vec![
        b'x'; TELEGRAM_MAX_DOWNLOAD_BYTES as usize + 1
    ]))]);
    assert!(store.store_stream(&actor, "large.bin", rejected).await.is_err());
    fs::remove_dir_all(root).await.ok();
    Ok(())
}

#[tokio::test]
async fn stores_verified_actor_relative_content_addressed_file() -> Result<()> {
    let root = temp_root("store");
    fs::remove_dir_all(&root).await.ok();
    let store = RuntimeAttachmentStore::new(&root);
    let actor = ActorId::parse_workspace_safe("alice")?;
    let png = Bytes::from_static(b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR");

    let first = store.store_stream(
        &actor,
        "../screen.png",
        stream::iter([Ok::<_, Infallible>(png.clone())]),
    ).await?;
    let second = store.store_stream(
        &actor,
        "screen.png",
        stream::iter([Ok::<_, Infallible>(png)]),
    ).await?;

    assert_eq!(first, second);
    assert_eq!(first.display_name, "screen.png");
    assert_eq!(first.media_type, "image/png");
    assert!(!first.relative_path.is_absolute());
    let mut entries = fs::read_dir(root.join("alice")).await?;
    assert!(entries.next_entry().await?.is_some());
    assert!(entries.next_entry().await?.is_none());
    fs::remove_dir_all(root).await.ok();
    Ok(())
}
```

Also test partial stream failure cleanup, unknown MIME with safe-extension/`bin` fallback, different-actor separation, unsafe actor rejection, symlinked root/actor rejection on Unix, missing-directory removal, and removal of only the selected actor.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test runtime::attachments`

Run: `rtk cargo test runtime_paths`

Expected: FAIL because `RuntimeAttachmentStore` and `RuntimePaths.attachments` do not exist.

- [ ] **Step 3: Implement the minimal actor-scoped store**

Register `pub mod attachments;` in `src/runtime.rs`. Implement this API:

```rust
pub const TELEGRAM_MAX_DOWNLOAD_BYTES: u64 = 20_000_000;

#[derive(Clone, Debug)]
pub struct RuntimeAttachmentStore {
    root: PathBuf,
}

impl RuntimeAttachmentStore {
    pub fn new(root: impl Into<PathBuf>) -> Self;
    pub fn actor_root(&self, actor: &ActorId) -> Result<PathBuf>;
    pub async fn store_stream<S, E>(
        &self,
        actor: &ActorId,
        display_name: &str,
        stream: S,
    ) -> Result<Attachment>
    where
        S: Stream<Item = std::result::Result<Bytes, E>>,
        E: Display;
    pub async fn remove_actor(&self, actor: &ActorId) -> Result<()>;
}
```

Adapt the existing write loop from `src/memory/attachments.rs`, but use `<root>/<actor>` and return only `<sha256>.<extension>` in `Attachment.relative_path`. Validate root and actor directories with `symlink_metadata`; reject symlinks and non-directories. Re-parse `actor.as_str()` with `ActorId::parse_workspace_safe`. Use generated temp names, `checked_add`, actual-byte enforcement, SHA-256, an 8192-byte inference prefix, `flush`, atomic rename, regular-file verification for existing digest paths, and best-effort temp cleanup on every failure.

Add `attachments: codrik_home.join("attachments")` to `RuntimePaths`. Include it in existing startup directory creation, permission validation, and required-parent checks in `src/app.rs`.

- [ ] **Step 4: Run focused tests**

Run: `rtk cargo test runtime::attachments`

Run: `rtk cargo test config::tests`

Run: `rtk cargo test app::tests`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add src/runtime/attachments.rs src/runtime.rs src/config.rs src/app.rs
rtk git commit -m "feat(runtime): add actor attachment storage"
```

---

### Task 2: Telegram Media Classification

**Files:**
- Modify: `src/interfaces/telegram/types.rs`

**Interfaces:**
- Consumes: existing `LinkIdentity`, `DeliveryRoute`, `/link` parsing.
- Produces: `TelegramInboundAttachment`; `TelegramInbound::Attachment { caption, attachment, identity, route }`.

- [ ] **Step 1: Add failing classification tests**

Add table-driven tests covering every media field:

```rust
#[test]
fn all_supported_media_classify_as_attachments() -> Result<()> {
    for field in [
        "document", "video", "animation", "audio", "voice", "video_note", "sticker",
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
fn photo_uses_largest_available_size() -> Result<()> {
    let update: TelegramUpdate = serde_json::from_value(json!({
        "update_id": 43,
        "message": {
            "message_id": 8,
            "from": {"id": 100, "is_bot": false},
            "chat": {"id": 100, "type": "private"},
            "photo": [
                {"file_id":"small","file_unique_id":"s","width":90,"height":90,"file_size":100},
                {"file_id":"large","file_unique_id":"l","width":900,"height":900,"file_size":1000}
            ]
        }
    }))?;
    assert!(matches!(
        update.classify("900", "codrik_bot")?,
        TelegramInbound::Attachment { caption: None, attachment, .. }
            if attachment.file_id == "large"
    ));
    Ok(())
}
```

Add tests proving an empty caption becomes `None`, a media-only message is accepted, `media_group_id` does not alter classification, and malformed/multiple-media/group/bot updates are unsupported.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test interfaces::telegram::types::tests`

Expected: FAIL because media fields and attachment classification do not exist.

- [ ] **Step 3: Add exact normalized wire types and classification**

Add narrow serde wire structs for Telegram's file-bearing objects, sharing fields only where their JSON shapes agree. Normalize them to:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramInboundAttachment {
    pub file_id: String,
    pub file_size: Option<u64>,
    pub display_name: String,
}
```

Extend `TelegramMessage` with optional `caption`, `photo`, `document`, `video`, `animation`, `audio`, `voice`, `video_note`, and `sticker`. Extend `TelegramInbound`:

```rust
Attachment {
    caption: Option<String>,
    attachment: TelegramInboundAttachment,
    identity: LinkIdentity,
    route: DeliveryRoute,
},
```

Build identity/route after validating private/non-bot sender. Preserve non-empty text and `/link` precedence. For media, require exactly one supported field and a non-blank `file_id`. Select photo by `file_size`, then saturating pixel area. Generate sensible display-name fallbacks such as `photo.jpg`, `voice.ogg`, and `sticker.webp`; do not trust these names for MIME decisions.

- [ ] **Step 4: Run focused tests**

Run: `rtk cargo test interfaces::telegram::types::tests`

Expected: PASS, including existing text and `/link` tests.

- [ ] **Step 5: Commit**

```bash
rtk git add src/interfaces/telegram/types.rs
rtk git commit -m "feat(telegram): classify inbound media"
```

---

### Task 3: Telegram `getFile` and Streaming Download

**Files:**
- Modify: `src/interfaces/telegram/api.rs`
- Modify: `src/interfaces/telegram/polling.rs`
- Modify: `src/interfaces/telegram.rs`
- Modify: `tests/serve_runtime.rs`

**Interfaces:**
- Consumes: existing `ReqwestTelegramApi::post_json`, fixed-error `TelegramApiError` conventions.
- Produces: `GetFile`, `TelegramFile`, `TelegramDownloadStream`, `TelegramIngressApi::get_file`, `download_file`.

- [ ] **Step 1: Add failing API contract tests**

Add tests beside existing mock HTTP tests:

```rust
#[tokio::test]
async fn get_file_posts_file_id_and_decodes_path() -> Result<()> {
    let (base, request) = serve_one_response(
        r#"{"ok":true,"result":{"file_id":"f","file_unique_id":"u","file_size":4,"file_path":"documents/a.bin"}}"#,
    ).await?;
    let api = ReqwestTelegramApi::with_base_url("secret-token", &base)?;
    let file = api.get_file(GetFile { file_id: "f".into() }).await?;
    assert_eq!(file.file_path.as_deref(), Some("documents/a.bin"));
    assert!(request.await?.contains("POST /botsecret-token/getFile"));
    Ok(())
}

#[tokio::test]
async fn download_stream_returns_chunks_without_exposing_credentials() -> Result<()> {
    let (base, request) = serve_one_raw_response(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata").await?;
    let api = ReqwestTelegramApi::with_base_url("secret-token", &base)?;
    let bytes = api.download_file("documents/a.bin").await?
        .try_concat().await?;
    assert_eq!(bytes.as_ref(), b"data");
    assert!(request.await?.starts_with("GET /file/botsecret-token/documents/a.bin"));
    Ok(())
}
```

Also test non-2xx and body-stream errors without token/path leakage; reject empty, absolute, `..`, backslash, query, and fragment paths.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test interfaces::telegram::api::tests`

Expected: FAIL because `get_file` and `download_file` do not exist.

- [ ] **Step 3: Implement API methods and update test doubles**

Add:

```rust
#[derive(Clone, Serialize)]
pub struct GetFile { pub file_id: String }

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TelegramFile {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: Option<u64>,
    pub file_path: Option<String>,
}

pub type TelegramDownloadStream = Pin<
    Box<dyn Stream<Item = Result<Bytes, TelegramApiError>> + Send>
>;
```

Extend `TelegramIngressApi`:

```rust
async fn get_file(&self, command: GetFile) -> Result<TelegramFile, TelegramApiError>;
async fn download_file(
    &self,
    file_path: &str,
) -> Result<TelegramDownloadStream, TelegramApiError>;
```

Implement `get_file` through `post_json("getFile", ..., true)`. Validate the relative Telegram path before constructing `{base_url}/file/bot{token}/{file_path}`. Send GET with existing connect/request timeouts, reject non-success status, then map `response.bytes_stream()` errors to fixed credential-free messages.

Update every `TelegramIngressApi` test double in `src/interfaces/telegram.rs`, `src/interfaces/telegram/polling.rs`, and `tests/serve_runtime.rs`. Defaults should return explicit terminal errors, not panic, so unrelated tests remain diagnosable.

- [ ] **Step 4: Run Telegram tests**

Run: `rtk cargo test interfaces::telegram`

Run: `rtk cargo test --test serve_runtime telegram`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add src/interfaces/telegram/api.rs src/interfaces/telegram/polling.rs src/interfaces/telegram.rs tests/serve_runtime.rs
rtk git commit -m "feat(telegram): download inbound files"
```

---

### Task 4: Durable Attachment Event Contract

**Files:**
- Modify: `src/runtime/store.rs:389-479`
- Modify: `src/runtime/sqlite/dispatch.rs:481-511,786-814`

**Interfaces:**
- Consumes: `Attachment`, `UserInput`, `Message::user`, existing opaque `events.payload_json` column.
- Produces: `NewInboundEvent::attachment_with_route`; deterministic attachment event decoding.

- [ ] **Step 1: Add failing constructor and restart tests**

Add a payload constructor test in `src/runtime/store.rs` and dispatch tests in `src/runtime/sqlite/dispatch.rs`:

```rust
#[tokio::test]
async fn attachment_event_reopens_as_caption_then_attachment() -> Result<()> {
    let path = temp_database("attachment-reopen");
    remove_sqlite_files(&path);
    let store = SqliteRuntimeStore::open(&path).await?;
    let actor = ActorId::from_string("alice");
    store.ensure_initial_actor(&actor, &[], Timestamp(1)).await?;
    let attachment = Attachment::new(
        "a".repeat(64), "a.png", "screen.png", "image/png", 4, "a".repeat(64),
    );
    store.ingest(
        NewInboundEvent::attachment_with_route(
            "telegram:900", "42", "telegram:900", "100",
            Audience::ActorPrivate, route(), Some("inspect".into()), attachment.clone(),
        )?,
        Timestamp(2),
    ).await?;
    drop(store);

    let reopened = SqliteRuntimeStore::open(&path).await?;
    let run = attach_first_run(&reopened, &actor).await?.unwrap();
    assert_eq!(run.messages, vec![Message::user(
        UserInput::new().push_text("inspect").push_attachment(attachment)
    )]);
    remove_sqlite_files(&path);
    Ok(())
}
```

Add attachment-only and malformed absolute/traversal path cases. Malformed persisted payload must follow the existing `blocked_malformed` path.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test attachment_event`

Expected: FAIL because the constructor and decoder case do not exist.

- [ ] **Step 3: Implement typed serialization and decoding**

Define private or crate-visible serde payload types in `src/runtime/store.rs`:

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum InboundUserPayload {
    Text { text: String },
    Attachment { caption: Option<String>, attachment: Attachment },
}
```

Use this enum in the existing text constructor. Add:

```rust
pub fn attachment_with_route(
    gateway: impl Into<String>,
    external_id: impl Into<String>,
    identity_provider: impl Into<String>,
    identity_subject: impl Into<String>,
    audience: Audience,
    delivery_route: DeliveryRoute,
    caption: Option<String>,
    attachment: Attachment,
) -> Result<Self>;
```

Trim caption and store empty as `None`. Validate non-empty attachment ID/name/MIME, lowercase 64-hex SHA-256, relative non-empty path containing only normal components, and `attachment.id == attachment.sha256` for content-addressed runtime files.

In `event_message`, deserialize `attachment`, repeat trust-boundary validation, then build:

```rust
let input = UserInput::new();
let input = match caption.filter(|value| !value.trim().is_empty()) {
    Some(caption) => input.push_text(caption),
    None => input,
};
Ok(Message::user(input.push_attachment(attachment)))
```

Do not check file existence during SQLite decoding.

- [ ] **Step 4: Run focused tests**

Run: `rtk cargo test runtime::store`

Run: `rtk cargo test runtime::sqlite::dispatch`

Expected: PASS; existing text/webhook tests unchanged.

- [ ] **Step 5: Commit**

```bash
rtk git add src/runtime/store.rs src/runtime/sqlite/dispatch.rs
rtk git commit -m "feat(runtime): persist attachment inputs"
```

---

### Task 5: Authorized Telegram Attachment Ingress

**Files:**
- Modify: `src/interfaces/telegram/ingress.rs`
- Modify: `src/interfaces/telegram.rs:121-240`
- Modify: `src/app.rs:320-331`

**Interfaces:**
- Consumes: `TelegramIngressApi::{get_file,download_file}`, `RuntimeAttachmentStore::store_stream`, `NewInboundEvent::attachment_with_route`.
- Produces: shared webhook/polling attachment ingestion; user-facing failure response without actor execution.

- [ ] **Step 1: Add failing ingress tests**

Create a cloneable mock ingress API that records calls and returns configured file metadata/streams. Add tests:

```rust
#[tokio::test]
async fn linked_attachment_downloads_persists_and_signals_once() -> Result<()> {
    let fixture = linked_fixture("attachment-accepted").await?;
    let update = attachment_update(42, "document", Some("inspect"));

    assert!(matches!(
        fixture.ingress.handle(update).await?,
        TelegramIngressOutcome::Accepted { sequence: 1, .. }
    ));
    let run = attach_first_run(&fixture.store, &fixture.actor).await?.unwrap();
    assert!(matches!(
        run.messages[0].content.as_slice(),
        [MessagePart::Text(text), MessagePart::Attachment(file)]
            if text == "inspect" && file.display_name == "sample.bin"
    ));
    assert_eq!(fixture.api.calls(), ["get_file", "download_file"]);
    Ok(())
}
```

Also test attachment-only acceptance; all eight variants through shared classification; unlinked/disabled actors causing zero API calls/files; declared size above `20_000_000` causing no `getFile`; `getFile` size above limit causing no download; missing path, API failure, stream failure, and overflow producing `CommandHandled`, one concise response, no event/signal; duplicate update producing one event/signal; two media-group member updates producing two sequences.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test interfaces::telegram::ingress::tests`

Expected: FAIL because ingress has no attachment branch or injected API/store.

- [ ] **Step 3: Inject dependencies and implement the attachment branch**

Change the service to:

```rust
pub struct TelegramIngressService<S, A, C> {
    store: S,
    api: A,
    attachments: RuntimeAttachmentStore,
    linking: Arc<dyn IdentityLinkManager>,
    signals: ActorSignals,
    bot_id: String,
    bot_username: String,
    clock: C,
}
```

Add `A: TelegramIngressApi + Clone + Send + Sync + 'static` bounds. Add `api` and `RuntimeAttachmentStore` to `new`.

In the attachment branch:

1. Resolve identity and reject unlinked/disabled before any API call.
2. Early reject declared `file_size > TELEGRAM_MAX_DOWNLOAD_BYTES`.
3. Call `get_file`; require a safe `file_path`; reject returned oversized size.
4. Call `download_file` and `store_stream(&actor.id, &display_name, stream)`.
5. Build `attachment_with_route(...).with_latest_telegram_route_tracking()`.
6. Signal only `IngressOutcome::Accepted`; preserve duplicate behavior.

Map remote/content/storage failures to one stable response such as `Could not receive this file. Telegram files must be 20 MB or smaller.` Return `CommandHandled` so webhook does not return 5xx and polling does not retry malformed user content. Propagate SQLite and response-enqueue failures as `Err`.

Add `attachment_root: PathBuf` to `telegram::prepare` and `prepare_with_api`; instantiate `RuntimeAttachmentStore::new(attachment_root)`. Pass `paths.attachments.clone()` from `app.rs` while retaining the separate artifact root.

- [ ] **Step 4: Run shared Telegram and app tests**

Run: `rtk cargo test interfaces::telegram::ingress::tests`

Run: `rtk cargo test interfaces::telegram::tests`

Run: `rtk cargo test app::tests`

Expected: PASS; webhook and polling use the same ingress object.

- [ ] **Step 5: Commit**

```bash
rtk git add src/interfaces/telegram/ingress.rs src/interfaces/telegram.rs src/app.rs
rtk git commit -m "feat(telegram): ingest authorized attachments"
```

---

### Task 6: Per-Actor Provider Attachment Context

**Files:**
- Modify: `src/llm/client.rs:77-108`
- Modify: `src/runtime/runner.rs:126-180,520-545`
- Modify: `src/llm/openai.rs`
- Modify: `src/llm/openai/attachments.rs`
- Modify: `src/app.rs:50-64,455-500`

**Interfaces:**
- Consumes: actor attachment root from `RuntimePaths`; existing OpenAI image/document/cache machinery.
- Produces: `RunContext::with_attachment_root`, `attachment_root`; `ActorRunner::with_attachment_root`.

- [ ] **Step 1: Add failing context and provider tests**

Add `RunContext` unit coverage:

```rust
#[test]
fn run_context_retains_attachment_root() {
    let context = RunContext::new().with_attachment_root("/tmp/attachments/alice");
    assert_eq!(
        context.attachment_root(),
        Some(Path::new("/tmp/attachments/alice"))
    );
}
```

Adapt OpenAI attachment fixtures to pass run context. Add tests proving image maps to `input_image`, PDF maps to `input_file`, video/audio/sticker map to metadata text, missing context errors only when supported bytes are required, actor A/B caches remain separate, and absolute/parent/symlink escapes fail.

Add a runner test with an injected LLM recording `RunContext::attachment_root()`.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test run_context_retains_attachment_root`

Run: `rtk cargo test llm::openai::attachments::tests`

Run: `rtk cargo test runtime::runner::tests`

Expected: FAIL because run context has no attachment root.

- [ ] **Step 3: Thread the actor root through model requests**

Extend `RunContext`:

```rust
#[derive(Clone, Default)]
pub struct RunContext {
    cancellation: CancellationToken,
    attachment_root: Option<PathBuf>,
}

pub fn with_attachment_root(mut self, root: impl Into<PathBuf>) -> Self {
    self.attachment_root = Some(root.into());
    self
}

pub fn attachment_root(&self) -> Option<&Path> {
    self.attachment_root.as_deref()
}
```

Add `attachment_root: Option<PathBuf>` and `ActorRunner::with_attachment_root`. Construct each run context with that root before generation.

Pass `&RunContext` through OpenAI request conversion into `resolve_attachment`. Reduce `OpenAiAttachmentContext` to provider-static configuration:

```rust
pub struct OpenAiAttachmentContext {
    pub image_detail: ImageDetailConfig,
}
```

For every resolved supported attachment, require `run_context.attachment_root()`, create `ProviderFileStore::new(actor_root)`, and resolve `Attachment.relative_path` beneath that root. Reject non-normal path components before canonicalization, then retain the existing canonical confinement check. Metadata-only formats must not require reading the file.

Configure the production client in `serve` with `config.attachments.image_detail`. In dispatcher construction, pass:

```rust
.with_attachment_root(paths.attachments.join(actor.id.as_str()))
```

Capture `paths.attachments` directly in the dispatcher closure rather than reconstructing `CODRIK_HOME` conventions.

- [ ] **Step 4: Run provider and runner tests**

Run: `rtk cargo test llm::openai`

Run: `rtk cargo test runtime::runner::tests`

Run: `rtk cargo test app::tests`

Expected: PASS; existing cancellation behavior remains unchanged.

- [ ] **Step 5: Commit**

```bash
rtk git add src/llm/client.rs src/runtime/runner.rs src/llm/openai.rs src/llm/openai/attachments.rs src/app.rs
rtk git commit -m "feat(client): resolve actor attachments"
```

---

### Task 7: Forced Actor Attachment Cleanup

**Files:**
- Modify: `src/runtime/actor_admin.rs`
- Modify: `src/app.rs:250-285`

**Interfaces:**
- Consumes: `RuntimeAttachmentStore::remove_actor`, `ActorDeleteOutcome`.
- Produces: force-only post-transaction cleanup with explicit failure reporting.

- [ ] **Step 1: Add failing administration tests**

Add tests using separate actor directories:

```rust
#[tokio::test]
async fn force_delete_removes_only_deleted_actor_attachments() -> Result<()> {
    let fixture = admin_fixture("force-attachments").await?;
    fixture.create_disabled_actor("alice").await?;
    fs::create_dir_all(fixture.attachments.join("alice")).await?;
    fs::create_dir_all(fixture.attachments.join("bob")).await?;
    fs::write(fixture.attachments.join("alice/a.bin"), b"a").await?;
    fs::write(fixture.attachments.join("bob/b.bin"), b"b").await?;

    fixture.admin.execute(ActorAdminCommand::Delete {
        actor_id: ActorId::from_string("alice"),
        force: true,
    }).await?;

    assert!(!fs::try_exists(fixture.attachments.join("alice")).await?);
    assert!(fs::try_exists(fixture.attachments.join("bob/b.bin")).await?);
    Ok(())
}
```

Also test disable retains files; failed/non-forced deletion retains files; missing directory succeeds; symlink actor directory is rejected without deleting its target; cleanup failure returns an error after `actor_details` confirms the actor no longer exists.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `rtk cargo test runtime::actor_admin::tests`

Expected: FAIL because administration does not own attachment storage.

- [ ] **Step 3: Add post-commit force cleanup**

Store `RuntimeAttachmentStore` in `ActorAdministration`; add it to `new`. In `ActorDeleteOutcome::Deleted`, keep artifact cleanup and signal behavior, then call `remove_actor(&actor)` only when `force` is true. Add context stating that actor deletion succeeded but attachment cleanup failed. Never restore actor state.

Pass `RuntimeAttachmentStore::new(paths.attachments.clone())` from `app.rs`. Update all test constructors.

- [ ] **Step 4: Run administration and app tests**

Run: `rtk cargo test runtime::actor_admin::tests`

Run: `rtk cargo test app::tests`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add src/runtime/actor_admin.rs src/app.rs
rtk git commit -m "feat(runtime): remove deleted actor attachments"
```

---

### Task 8: End-to-End Runtime Coverage and Documentation

**Files:**
- Modify: `tests/serve_runtime.rs`
- Modify: `README.md:41-101,197-240`

**Interfaces:**
- Consumes: complete Telegram attachment ingress/runtime/provider path.
- Produces: regression proof across startup, polling/webhook-independent ingress, restart, duplicate suppression, and documented operator behavior.

- [ ] **Step 1: Add a failing serve-level attachment test**

Extend the existing `telegram_acceptance` module and its `TelegramApiMock`. Add `file: Arc<Mutex<Option<(TelegramFile, Bytes)>>>`; implement `get_file` by cloning the configured metadata and `download_file` by returning `stream::once` over the configured bytes. Add `requests: Arc<Mutex<Vec<LlmRequest>>>` to the existing `InjectedLlm` and push `request.clone()` at the start of `stream` before consuming its tools.

Add a media update helper beside the existing text `update` helper:

```rust
fn document_update(update_id: i64, message_id: i64) -> serde_json::Value {
    serde_json::json!({
        "update_id": update_id,
        "message": {
            "message_id": message_id,
            "from": {"id": 4242, "is_bot": false, "username": "owner"},
            "chat": {"id": 4242, "type": "private"},
            "caption": "inspect",
            "document": {
                "file_id": "telegram-file",
                "file_unique_id": "telegram-unique",
                "file_name": "sample.png",
                "mime_type": "application/octet-stream",
                "file_size": 16
            }
        }
    })
}
```

Extend `telegram_webhook_links_runs_and_delivers_without_duplicates` rather than creating another server harness:

1. Define `let attachments = root.join("attachments");` and pass it to the expanded `prepare_with_api` before `artifacts`.
2. Configure `api.file` with a `TelegramFile` whose `file_path` is `documents/sample.png` and PNG-signature bytes.
3. POST `document_update(12, 102)` through the existing webhook client after the text update; assert HTTP 200 and that event/work-item counts each increase once.
4. Clone an `InjectedLlm` into the existing `ActorRunner`, call `.with_attachment_root(attachments.join(ACTOR))`, run it, and assert its last recorded request contains `MessagePart::Text("inspect")` followed by one `MessagePart::Attachment` with `image/png`.
5. Assert the attachment exists beneath `attachments.join(ACTOR)` and no absolute path appears in the stored event JSON.
6. Replay the document update with the existing replay loop; assert event/work-item counts and LLM call count do not increase.
7. Reopen `SqliteRuntimeStore` before delivery as the test already does, proving durable replay remains decodable.

Add a forced actor deletion integration assertion only if Task 7's `ActorAdministration` tests cannot exercise production constructor wiring.

- [ ] **Step 2: Run the integration test and verify failure**

Run: `rtk cargo test --test serve_runtime telegram_attachment -- --nocapture`

Expected: FAIL until every production wiring path is complete.

- [ ] **Step 3: Complete wiring gaps and document behavior**

Fix only missing production wiring exposed by the test. In `README.md`, document:

```markdown
### Incoming files

Private Telegram chats accept photos, documents, videos, animations, audio,
voice messages, video notes, and stickers. Each Telegram update is a separate
agent turn; album members are not grouped. Captions precede the file.

Codrik uses Telegram's hosted Bot API and accepts at most 20,000,000 downloaded
bytes per file. Files are retained under
`<CODRIK_HOME>/attachments/<actor-id>/` until that actor is force-deleted.
Provider-supported images and documents are sent to the model; other formats
remain available as metadata and a safe local path.
```

Clarify in the configuration table that `attachments.max_file_size_mb` does not control hosted Telegram downloads; it remains a legacy/session input setting.

- [ ] **Step 4: Run full verification**

Run: `rtk cargo test`

Run: `rtk cargo check`

Run: `rtk cargo fmt --check`

Run: `rtk cargo clippy --all-targets --all-features`

Expected: all commands exit 0 with no warnings introduced by this change.

- [ ] **Step 5: Inspect the final diff for scope and secrets**

Run: `rtk git status --short`

Run: `rtk git diff --check`

Run: `rtk git diff --stat`

Expected: only planned source, test, README, spec, and plan files; no token, credential-bearing URL, downloaded fixture binary, SQLite database, or runtime attachment directory.

- [ ] **Step 6: Commit**

```bash
rtk git add README.md tests/serve_runtime.rs
rtk git commit -m "test(runtime): cover Telegram attachments"
```
