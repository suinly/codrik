# Telegram Inbound Attachments Design

## Goal

Allow Telegram users to send images and other files to a Codrik actor. Each
Telegram update becomes one durable user turn. The model receives supported
content directly; unsupported content remains available as a managed local file
and appears to the model as metadata.

This change covers Telegram ingress only. CLI attachments, media-group
aggregation, media conversion, frame extraction, and transcription are out of
scope.

## Supported Telegram content

Accept these private-message fields:

- `photo`;
- `document`;
- `video`;
- `animation`;
- `audio`;
- `voice`;
- `video_note`;
- `sticker`.

For `photo`, use the largest available `PhotoSize`. Every other supported field
identifies one Telegram file. If an update contains a caption, construct the
user input with the caption first and the attachment second. A missing or empty
caption is valid and still starts the actor.

Telegram sends media-group members as separate updates. Codrik does not buffer
or aggregate them: each update creates a separate user turn.

Text messages and `/link` retain their existing behavior. Non-private messages,
bot messages, malformed media, and unsupported update shapes remain
unsupported.

## Architecture

Extend the existing gateway-independent attachment model rather than creating a
Telegram-specific agent input. The durable runtime becomes the owner of inbound
attachment paths:

```text
<CODRIK_HOME>/attachments/<actor-id>/<sha256>.<extension>
```

Responsibilities remain separated:

- `interfaces::telegram::types` parses Telegram media metadata and classifies an
  update as a command, text input, attachment input, or unsupported input.
- `interfaces::telegram::api` resolves `file_id` with `getFile` and streams the
  file from Telegram's standard Bot API download endpoint.
- a runtime attachment store writes actor-scoped content-addressed files and
  returns verified `Attachment` metadata.
- Telegram ingress authorizes the identity, stores the file, persists one
  inbound event, and signals the actor.
- runtime dispatch decodes the event into `Message::user(UserInput)`.
- `llm::openai` maps supported attachments to provider content and falls back to
  metadata for unsupported formats.
- forced actor deletion removes the actor's managed attachment directory after
  durable actor state has been deleted.

The old session-directory attachment store must not be treated as the runtime
storage model. Runtime conversation history is SQLite-backed and actor-scoped,
so its attachment root is actor-scoped as well.

## Telegram API and size limit

Use only Telegram's standard hosted Bot API at `https://api.telegram.org`.
Local Bot API server configuration is out of scope.

Add `getFile` and streaming download operations to the Telegram ingress API
abstraction. The download URL contains the bot token and must never be logged or
included in user-facing errors.

Enforce Telegram's hosted download limit as exactly `20_000_000` bytes against
the bytes actually read. Declared `file_size` may reject an obviously oversized
file early, but it is never sufficient validation. Stop and fail if the stream
crosses the limit.

The existing `attachments.max_file_size_mb` setting no longer controls Telegram
ingress. It may remain for legacy attachment paths until those paths are removed
separately; this feature does not introduce another configurable Telegram
limit.

## Managed storage

Create an actor-scoped attachment store rooted at
`<CODRIK_HOME>/attachments`. For each download it:

1. validates the actor identifier using the existing workspace-safe rules;
2. creates the actor directory without following an unsafe parent path;
3. streams into a generated temporary file under that directory;
4. counts bytes, computes SHA-256, and retains a small prefix for MIME
   inference;
5. flushes the completed temporary file;
6. chooses an extension from verified content, falling back to a safe extension
   from the display name, then `bin`;
7. atomically renames it to `<sha256>.<extension>`, or removes the temporary file
   when that content-addressed object already exists;
8. returns `Attachment` metadata with a path relative to the actor attachment
   root.

The display name is sanitized to a basename and used only as metadata. Telegram
MIME type, filename, extension, and size are untrusted hints. The stored MIME
type is inferred from content; unknown content uses
`application/octet-stream`.

Content addressing deduplicates identical bytes for one actor. Actor boundaries
remain explicit even when two actors upload identical bytes.

## Durable event contract

Add a typed inbound payload for a user attachment. Its serialized JSON contains:

- payload type;
- optional caption;
- attachment ID;
- actor-relative path;
- sanitized display name;
- verified MIME type;
- actual byte size;
- SHA-256 digest.

Raw bytes, absolute paths, Telegram download URLs, and bot credentials are never
stored in SQLite.

The file is finalized before ingress attempts to insert the event. The event is
the durable marker that the upload should become actor work. Dispatch converts
it to `UserInput` in this order:

1. non-empty caption text, when present;
2. attachment.

An attachment-only input is valid. Event replay reconstructs the same message
after process restart. Existing text and webhook event formats remain unchanged.

Telegram `update_id` remains the idempotency key. A duplicate event does not
signal or execute the actor again. Since storage is content-addressed, a file
downloaded before discovering a duplicate is harmless and remains part of the
actor's retained attachment set.

## Provider behavior

Reuse the existing OpenAI Responses attachment machinery:

- supported images become `input_image`;
- supported documents become `input_file`;
- another format is sent directly only if the provider adapter explicitly
  supports it;
- unsupported formats become metadata text containing the safe name, verified
  MIME type, byte size, and safe local path.

Do not add video conversion, audio transcription, archive extraction, or sticker
conversion. Provider capability is explicit in the adapter; Codrik does not
claim support based only on a Telegram media category or filename extension.

The OpenAI attachment context resolves actor-relative paths beneath the current
actor's managed attachment root. Canonical path validation must reject absolute
paths, traversal, and symlink escape. Existing provider-file caching by digest
and upload purpose remains applicable.

Provider upload or generation failure does not remove the event or local file.
Normal runtime retry behavior may retry the provider operation later.

## Failure behavior

Authorization occurs before download. An unlinked identity or disabled actor
receives the existing response and causes no file write.

`getFile` failure, download failure, malformed metadata, unsafe storage path,
disk write failure, or actual size above `20_000_000` bytes causes a concise
Telegram error response. No inbound event is inserted and the actor is not
signaled. Partial temporary files are removed best-effort.

If final file storage succeeds but the event insert fails, retaining the
content-addressed file is safe. It may be reused by a later upload and is removed
with the actor. No background orphan collector or TTL is introduced.

## Retention and deletion

Files remain available for the lifetime of the actor's durable history. They
are not removed after one model request and have no TTL.

`actors delete --force` removes `<CODRIK_HOME>/attachments/<actor-id>` after the
SQLite deletion transaction succeeds. A failed filesystem cleanup must be
reported with context without restoring the deleted actor or exposing private
paths to remote users. Normal actor disablement and non-forced administration do
not remove attachments.

## Security

- Authorize identity and actor before network download.
- Enforce the actual streamed-byte limit, regardless of Telegram metadata.
- Generate temporary and final storage names locally.
- Keep every resolved path beneath the actor attachment root.
- Never log bot tokens, credential-bearing URLs, file bytes, or user-controlled
  absolute paths.
- Persist only actor-relative paths and verified metadata.
- Treat attachment contents and captions as untrusted user input, not system
  instructions.

## Testing

Add focused tests for:

- classification of `photo`, `document`, `video`, `animation`, `audio`, `voice`,
  `video_note`, and `sticker`;
- selection of the largest `photo` size;
- caption-before-attachment ordering and valid attachment-only input;
- one media-group member producing one independent turn;
- `getFile` request and credential-safe streaming download behavior;
- exact acceptance at `20_000_000` bytes and rejection above it;
- cleanup of partial downloads after stream failure or overflow;
- content MIME inference, safe display names, digest, extension fallback, and
  actor-local deduplication;
- linked, unlinked, disabled, duplicate, and failed-download ingress outcomes;
- webhook and polling parity because both consume the same `TelegramUpdate` and
  ingress service;
- durable attachment event decoding after reopening SQLite;
- provider image/document mapping and metadata fallback;
- canonical path escape rejection;
- retention during normal operation and cleanup during `actors delete --force`.

Run `rtk cargo test`, `rtk cargo check`, `rtk cargo fmt --check`, and
`rtk cargo clippy --all-targets --all-features` before completion.
