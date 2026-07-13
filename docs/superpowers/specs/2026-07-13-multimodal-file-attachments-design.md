# Multimodal File Attachments Design

## Goal

Allow Codrik to accept, retain, understand, and send files through a gateway-independent agent API. Telegram is the first gateway implementation. The model receives images and supported documents through the OpenAI Responses and Files APIs, while arbitrary unsupported binary files remain available for storage and delivery.

The first release supports one Telegram attachment per update. Telegram media-group aggregation, CLI file input, and provider fallbacks are out of scope.

## Provider requirements

Replace Chat Completions usage in `OpenAiClient` with the Responses API. A configured `base_url` must provide compatible `/v1/responses` and `/v1/files` endpoints. Codrik does not retain a Chat Completions compatibility mode.

OpenAI file inputs support file IDs created by the Files API. Documents intended for model input are uploaded with purpose `user_data` and represented as `input_file`. Images are uploaded with purpose `vision` and represented as `input_image`. The adapter retains existing text streaming and function-calling behavior while translating it to Responses events and items.

Official API behavior and limits used by this design are documented in:

- [File inputs](https://developers.openai.com/api/docs/guides/file-inputs)
- [Images and vision](https://developers.openai.com/api/docs/guides/images-vision)

## Domain model

Introduce a gateway-independent `UserInput` containing ordered text and attachment content. Existing string entry points remain source-compatible by converting strings into text-only `UserInput` values.

An `Attachment` contains:

- a locally generated identifier;
- a path relative to the session directory;
- the original display name;
- a content-derived MIME type;
- the byte size;
- a SHA-256 digest.

Raw bytes and base64 data are not stored in messages. An image is an attachment whose verified MIME type is supported by the provider's image-input contract; there is no separate image-only domain object.

Message content becomes an ordered collection of typed parts so that text and attachments retain their original order. Assistant and tool messages continue to use text content and their existing tool-call fields. Legacy text-only serialized messages must deserialize without migration-specific user action.

## Session storage

Each session is a self-contained directory:

```text
sessions/<chat-id>/<session-id>/
├── messages.json
├── provider-files.json
└── attachments/
    ├── <generated-id>.pdf
    ├── <generated-id>.png
    └── <generated-id>.docx
```

`messages.json` stores conversation messages and attachment metadata with relative paths. `provider-files.json` maps a local attachment digest, provider identity, and upload purpose to a remote file ID. It contains neither file bytes nor credentials.

The provider identity includes the normalized `base_url`. If credentials change while the URL remains the same and a cached ID is inaccessible, stale-ID recovery performs a fresh upload.

Existing `<session-id>.json` files remain readable. On the first subsequent append, Codrik creates the session directory, atomically writes `messages.json`, and removes the legacy file only after the new history is durable. New sessions use the directory layout immediately.

## Incoming Telegram files

Telegram accepts ordinary photos and documents of any MIME type. For each update, the gateway:

1. resolves or creates the active session directory;
2. streams the Telegram download into a temporary file inside that directory;
3. enforces the configured byte limit against bytes actually read;
4. derives MIME type from file content, computes SHA-256, and sanitizes the original name for display only;
5. atomically renames the file to a generated attachment path;
6. constructs one `UserInput` from the caption, if any, and the attachment;
7. starts the agent only after storage succeeds.

An empty caption is valid. A failed or oversized download leaves neither a conversation message nor a final attachment. Temporary-file cleanup is best effort.

The initial Telegram integration does not aggregate media-group updates. Each received attachment becomes its own user turn, while the domain model permits multiple attachments for future gateways and batching support.

## Preparing model input

Provider-supported attachments are uploaded lazily before their first model request and cached by digest, provider identity, and purpose.

- Supported images use Files purpose `vision` and Responses content type `input_image` with `file_id`.
- Supported documents, presentations, spreadsheets, text, and source files use purpose `user_data` and content type `input_file` with `file_id`.
- Unsupported binary formats are not uploaded. The model receives a text representation containing the safe display name, verified MIME type, byte size, and session-relative attachment path.

Every request contains images from the retained conversation history. Documents are considered newest first; Codrik includes the newest documents that fit within the provider's 50 MB combined document-input limit. Older supported documents remain visible as metadata-only text parts. A single supported document exceeding the provider's per-file limit is also represented as metadata rather than uploaded.

The application-level incoming limit is configured independently and defaults to 20 MB, so the default cannot create an individually oversized provider document. Configuration may raise the storage limit, but it does not override provider request limits.

If a cached remote file ID is rejected as missing, the adapter removes that cache entry, uploads the local attachment once, and retries the model request once. Other upload or response failures end the run without discarding the stored user message or local attachment; a future turn may retry the upload.

Concurrent work against one session serializes updates to `messages.json` and `provider-files.json`. Two callers must not create duplicate cached uploads for the same digest/purpose tuple.

## Outgoing files

Add a `send_file(path, caption)` tool. The path may identify a regular file inside either:

- the current authorized actor's workspace; or
- the current session directory, including its attachments.

The tool resolves relative paths against these roots, canonicalizes the result, and rejects directories, missing files, traversal outside both roots, and symlinks whose final target is outside both roots. Model-facing attachment references use stable relative forms such as `attachments/<id>.pdf` or `workspace/<path>` rather than absolute server paths.

Successful tool execution returns a typed file artifact in addition to its model-facing observation. The agent converts the artifact into `AgentOutputEvent::FileReady { path, media_type, caption }` and awaits the active output sink. Tools and the agent remain independent of Telegram.

The Telegram sink sends supported image MIME types with `sendPhoto` and all other files with `sendDocument`. Only successful gateway delivery produces a successful function output. Delivery failure becomes a tool failure observation, allowing the model to explain the problem or choose another file before producing its final answer.

The CLI does not add file delivery in this release. Its output sink returns a clear unsupported-interface error for `FileReady`.

## Output boundary

Generalize the current text-only streaming boundary into agent output events. Text deltas preserve their current behavior. File events are emitted during tool-call processing and are not encoded into final-answer text.

Presentation-only activity events remain separate. A file delivery is an agent action whose result affects the `send_file` function output, so unlike an activity-status update, its failure is observable by the model.

## Component boundaries

- `agent::message` owns `UserInput`, ordered message content, and attachment metadata.
- `memory` owns session-directory paths, atomic `messages.json` persistence, legacy history migration, and the session-scoped provider-file cache abstraction.
- `llm::openai` owns Files and Responses HTTP operations, supported provider MIME classification, upload-purpose selection, stale-ID recovery, and wire-format translation. It accesses cached IDs through the memory abstraction rather than writing session JSON itself.
- `tools` owns `send_file` path validation and typed file artifacts without depending on a gateway.
- the agent runtime turns file artifacts into output events and converts sink delivery results into function outputs.
- `interfaces::telegram` owns downloads, Telegram size enforcement, `sendPhoto`/`sendDocument`, and command parsing.
- `app` supplies the session directory, attachment/cache abstractions, allowed file roots, provider file client, and output sink when composing a run.
- a session-deletion application service coordinates the session store and provider file client. Telegram invokes this service and never calls the Files API directly.

## Configuration

Add optional attachment configuration with backward-compatible defaults:

```yaml
attachments:
  max_file_size_mb: 20
  image_detail: auto
```

`max_file_size_mb` limits incoming Telegram storage. Outbound delivery remains subject to the selected gateway's own API limit and reports a delivery failure through the `send_file` function result. `image_detail` accepts `auto`, `low`, or `high` and is attached to Responses `input_image` parts. A provider or model may reject a detail mode it does not support; Codrik reports that provider error rather than silently changing the requested value.

Provider document limits remain fixed by the API contract and are not overridden by configuration.

## Session deletion and remote cleanup

Add `/sessions delete <id>` to Telegram. It may delete only a session owned by the current chat, and it refuses to delete the active session. No implicit session switch occurs.

Deletion obtains exclusive ownership of the target session so it cannot race with history writes or file uploads. It then attempts to delete every cached provider file ID before removing the local directory and its record from `index.json`.

Remote deletion is best effort. Provider deletion failures do not retain the local session: Codrik removes the local directory and index record and reports that the session was deleted locally together with the number of remote files that could not be cleaned up. A fully successful cleanup returns an ordinary success message.

## Error handling and security

- Never trust Telegram's declared size, file name, extension, or MIME type without local verification.
- Use generated local names and retain the sanitized original name only as metadata.
- Write downloads and JSON updates through temporary files followed by atomic replacement.
- Do not persist API keys, absolute server paths, or file bytes in JSON metadata.
- Keep file paths scoped to an authorized actor workspace or the active session directory.
- Treat unsupported provider formats as metadata-only attachments rather than failed Telegram inputs.
- Do not record an assistant response or function call until a valid Responses result has been assembled.
- Preserve the stored user input when provider upload or generation fails so a later turn can retry.
- Log provider and Telegram errors with context while avoiding file bytes and credentials.

## Testing

Implement focused tests for:

### Domain and memory

- ordered text and attachments in `UserInput` and messages;
- string-to-text-only `UserInput` compatibility;
- text-only legacy message deserialization;
- new session-directory creation and relative attachment persistence;
- atomic legacy `<session-id>.json` migration;
- safe generated names, content MIME detection, byte-count limits, and digest calculation;
- serialized history and provider-cache updates under concurrent access.

### Provider adapter

- image upload with purpose `vision` and `input_image` mapping;
- document upload with purpose `user_data` and `input_file` mapping;
- remote file-ID cache reuse;
- one-time stale-ID re-upload and request retry;
- newest-first document selection within the combined 50 MB limit;
- metadata fallback for old, individually oversized, and unsupported files;
- Responses text streaming and function-call accumulation;
- correct function output association by `call_id`;
- preserved multimodal history order;
- upload and response failures leaving local history and cache consistent.

Provider tests use a mock HTTP server and assert request bodies and multipart upload metadata without external API calls.

### Tool and gateway

- accepted workspace and session paths;
- rejected traversal, directories, missing files, and escaping symlinks;
- successful `FileReady` delivery producing successful function output;
- sink failure producing a tool failure observation;
- Telegram photo, image-document, arbitrary-document, and captionless input handling;
- image delivery through `sendPhoto` and other delivery through `sendDocument`;
- configured size-limit enforcement while streaming;
- refusal to delete active or foreign sessions;
- successful inactive-session deletion and partial provider-cleanup reporting.

Before handoff, run:

```text
rtk cargo test
rtk cargo check
rtk cargo fmt --check
rtk cargo clippy --all-targets --all-features
```

## Out of scope

- Telegram media-group aggregation;
- CLI file input or file delivery;
- audio-specific model input semantics;
- archive extraction or executable-file interpretation;
- OCR or local document parsers;
- File Search, vector stores, or hosted code execution;
- Chat Completions fallback;
- automatic compaction or deletion of old attachments;
- adding a separate `send_image` tool.
