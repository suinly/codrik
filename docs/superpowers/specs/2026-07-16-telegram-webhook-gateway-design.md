# Telegram Webhook Gateway Design

## Goal

Add one Telegram bot to `codrik serve` as the first external webhook gateway.
An unlinked Telegram private identity can redeem a one-time link code. A linked
identity can submit text to the existing durable actor loop and receive
streaming activity plus durable final text and managed files.

Telegram remains an adapter around gateway-neutral runtime boundaries. Telegram
types, Bot API calls, chat IDs, and webhook semantics do not enter the agent,
memory, runner, dispatcher, or identity-linking core.

## Scope

The first release supports:

- one Telegram bot per runtime;
- an embedded HTTP listener bound to a local address behind a reverse proxy;
- automatic `setWebhook` registration during `codrik serve`;
- Telegram private chats only;
- incoming text messages and `/link` commands;
- outgoing text, photos, and documents;
- best-effort streaming activity and text edits;
- durable, retryable final delivery;
- shared actor memory across linked private identities.

The first release does not support:

- groups, supergroups, channels, business chats, or topics;
- incoming photos, documents, voice, or other attachments;
- multiple Telegram bot instances;
- built-in TLS termination;
- automatic identity transfer between actors;
- callback queries, inline queries, edited messages, reactions, or polls;
- durable replay of every intermediate streaming delta;
- issuing a new plaintext link code through Telegram;
- autonomous notifications without an explicit delivery route.

## Architecture

`codrik serve` composes three Telegram-facing components:

- `telegram-webhook` authenticates and normalizes incoming updates;
- `telegram-streaming` renders transient runtime activity;
- `telegram-delivery` claims and sends durable gateway deliveries.

The inbound path is:

```text
Telegram HTTPS webhook
    -> reverse proxy
    -> local Codrik HTTP listener
    -> authenticate secret and validate update
    -> handle /link or persist normalized event
    -> commit SQLite transaction
    -> notify actor dispatcher when work was accepted
    -> return 2xx
```

The execution and delivery path is:

```text
actor dispatcher
    -> fenced actor runner
    -> transient GatewayActivityHub events
    -> immutable semantic outbox intents
    -> durable gateway deliveries
    -> Telegram Bot API worker
```

The webhook never waits for a model call, tool call, or final response.

## Configuration

`AppConfig` gains one optional strict block:

```yaml
telegram:
  token: "123456:bot-token"
  public_url: "https://agent.example.com/webhooks/telegram"
  listen: "127.0.0.1:8080"
  webhook_secret: "stable-random-secret"
```

Rules:

- omitting `telegram` disables the Telegram gateway;
- `listen` defaults to `127.0.0.1:8080`;
- `public_url` must be an HTTPS URL with no query or fragment;
- `webhook_secret` must be 1-256 characters from
  `A-Z`, `a-z`, `0-9`, `_`, and `-`;
- token and secret values are never logged, serialized into runtime events, or
  included in user-facing errors;
- unknown fields remain configuration errors.

The reverse proxy owns TLS certificates and public ingress. Codrik owns only
the configured local HTTP listener.

Runtime limits are fixed for this release:

- webhook request body: 1 MiB;
- simultaneous webhook connections: 64;
- Telegram API response body: 1 MiB;
- API connect timeout: 5 seconds;
- text/edit request timeout: 30 seconds;
- file upload request timeout: 120 seconds.

## Startup and Webhook Reconciliation

Telegram startup occurs before the runtime announces readiness:

1. Bind the local HTTP listener.
2. Call `getMe` and retain the stable bot ID and username.
3. Call `setWebhook` with:
   - the configured `public_url`;
   - `secret_token`;
   - `allowed_updates: ["message"]`;
   - `drop_pending_updates: false`.
4. Call `getWebhookInfo`.
5. Verify the installed URL and visible webhook configuration.
6. Mark the Telegram components ready.

An invalid token, failed registration, or mismatched installed URL is a startup
error. Codrik does not call `deleteWebhook` during graceful shutdown. The next
startup reconciles the same webhook again without dropping pending updates.

## Telegram Update Model

The adapter deserializes only fields needed by this release:

- `update_id`;
- `message.message_id`;
- `message.from.id`, `username`, and `is_bot`;
- `message.chat.id` and `type`;
- `message.text`.

A supported user message must:

- contain `message`;
- have `chat.type == "private"`;
- have a non-bot `from`;
- contain nonblank text.

Valid but unsupported updates return `200 OK` without ingress. Malformed JSON
returns `400 Bad Request`.

The gateway namespace is `telegram:<bot_id>`. `update_id` rendered as a decimal
string is the external idempotency ID. The verified identity is:

```text
provider = telegram:<bot_id>
subject  = from.id
username = from.username
```

The delivery address uses `message.chat.id`; identity ownership always uses
`message.from.id`.

## Audience and Delivery Route

Memory disclosure scope and reply destination are separate concepts.

Telegram private messages use:

```text
Audience::ActorPrivate
```

This permits shared actor memory across linked private channels.

Each accepted external user event also carries a gateway-neutral
`DeliveryRoute`:

```rust
pub struct DeliveryRoute {
    pub gateway: String,
    pub address: String,
    pub reply_to_external_id: Option<String>,
}
```

For Telegram:

- `gateway` is `telegram:<bot_id>`;
- `address` is the decimal `chat.id`;
- `reply_to_external_id` is the decimal incoming `message_id`.

The event attachment transaction selects the route from the newest
incorporated compatible user event. The attached run carries that route.
Finalization projects each final semantic intent to that route. Local IPC
continues to use local request bundles and does not fabricate a delivery route.

## Linking Commands

Linking commands are recognized before ordinary ingress.

Supported forms in a private chat are:

```text
/link CODE
/link@bot_username CODE
/link
/link@bot_username
```

Whitespace between command and argument is normalized. The code itself is
passed to the existing identity-linking service, which owns code
normalization, hashing, expiry, rate limiting, and conflict rules.

Behavior:

- unlinked identity plus `/link CODE` redeems the code;
- `/link` without a code returns instructions to run `codrik link`;
- linked identity plus `/link CODE` follows ordinary redemption semantics,
  including same-actor confirmation and cross-actor conflict;
- linking commands never create events, work items, runs, model input, or
  actor memory.

User-facing outcomes are:

```text
This channel is now linked.
This channel was already linked.
Invalid or expired link code.
Too many failed attempts. Try again later.
This channel is already linked to another actor.
This channel is not linked. Run `codrik link` in an existing channel, then send `/link CODE` here.
```

Exact retry timestamps may be included for rate limiting, but responses never
reveal whether a submitted code exists.

## Idempotent Gateway Commands

Ordinary ingress already deduplicates `(gateway, external_id)` through the
events table. Linking commands do not create events, so they require a durable
gateway command ledger.

The ledger stores:

- gateway and external update ID as a primary key;
- command kind;
- an immutable normalized outcome;
- creation timestamp.

Identity-link redemption and command outcome persistence occur in one SQLite
transaction. Repeating the same update returns the stored outcome without
reapplying the command.

The corresponding user response is inserted into the gateway delivery queue
with a deterministic unique intent key derived from the update ID and response
kind. If the process commits the command but fails before committing the
delivery, Telegram receives non-2xx. Its retry reads the stored command outcome
and idempotently completes the missing delivery.

## Ordinary Unlinked Messages

An ordinary message from an unlinked identity does not create actor work.
Instead, the adapter inserts a durable linking-instruction delivery using a
deterministic intent key derived from the Telegram update ID.

Repeating the update confirms the same delivery. No actor is created
automatically.

## Durable Gateway Deliveries

`gateway_deliveries` is a channel-delivery queue independent from semantic
agent outbox intents. It supports both agent results and gateway system
responses.

Each row records:

- delivery ID and unique immutable `intent_key`;
- optional source outbox ID;
- gateway and address;
- optional reply-to external message ID;
- typed payload JSON;
- state;
- attempt count;
- `next_attempt_at`;
- claim owner and claim expiry;
- optional remote Telegram message ID;
- error class and bounded error summary;
- created and updated timestamps.

States are:

```text
pending -> delivering -> delivered
pending -> delivering -> failed_retryable -> pending
pending -> delivering -> failed_terminal
pending -> delivering -> outcome_unknown
```

An expired claim is retryable only when the previous operation is known
retry-safe. A delivery in `outcome_unknown` is not automatically repeated when
doing so could duplicate an external side effect.

Agent finalization inserts immutable semantic outbox intents as before. When an
attached external run has a delivery route, the same finalization transaction
also projects the intents into gateway deliveries. Gateway command and
authorization responses insert gateway deliveries directly without synthetic
work items or runs.

## Telegram API Adapter

The Telegram API client exposes focused typed operations:

- `get_me`;
- `set_webhook`;
- `get_webhook_info`;
- `send_message`;
- `edit_message_text`;
- `send_photo`;
- `send_document`.

It depends on an injectable HTTP transport for tests. It enforces request
timeouts, bounded response bodies, and strict decoding of Telegram's
`ok/result/error_code/description/parameters` envelope.

Failure classification:

- network errors before a request is sent are retryable;
- Telegram `429` uses `parameters.retry_after`;
- Telegram `5xx` is retryable;
- known permanent `4xx` is terminal;
- `editMessageText` reporting that the message is not modified is success;
- a disconnected or timed-out response after a non-idempotent `sendMessage`,
  `sendPhoto`, or `sendDocument` crosses the transport boundary is
  `outcome_unknown`;
- edits to a known message ID are retry-safe because they converge on
  deterministic text.

Bot token and webhook secret never appear in URLs written to logs. Logged API
operations use method names and typed error classes only.

## Streaming

`GatewayActivityHub` publishes transient gateway-neutral events keyed by
work item and delivery route:

- model step started;
- description;
- tool started and finished;
- text delta;
- completed, cancelled, or failed.

The Telegram streaming worker:

1. Sends `Thinking…` after the first activity.
2. Persists the returned Telegram message ID.
3. Buffers the complete accumulated text.
4. Calls `editMessageText` no more than once per second.
5. Skips unchanged text.
6. Uses plain UTF-8 without Markdown or HTML parse mode.

Intermediate activity and deltas are best effort. A restart may lose them.
Persisting the placeholder message ID allows a durable final delivery to edit
that message after restart. If no message ID is known, final delivery sends a
new message.

## Text Chunking

Final text is split before delivery into Unicode-safe chunks of at most 4096
characters. Each chunk is a separate durable delivery with a deterministic
ordinal intent key.

If a streaming message ID exists:

- the first chunk edits the streaming message;
- later chunks use `sendMessage`.

Otherwise every chunk uses `sendMessage`.

Chunking happens before any Telegram side effect, so partially delivered long
answers resume from the first incomplete durable chunk.

## Managed File Delivery

Managed file payloads retain the immutable artifact metadata already produced
by the runtime.

Before upload, the worker revalidates:

- the path remains inside the managed artifact root;
- the file exists and is a regular file;
- the size matches;
- the SHA-256 matches.

`image/jpeg`, `image/png`, and `image/webp` artifacts no larger than 10 MB are
sent with `sendPhoto`. Other artifacts are sent with `sendDocument`. The cloud
Bot API 50 MB outbound document limit is checked before transport. Oversized
files and failed integrity checks become terminal delivery failures.

Captions are limited to 1024 characters. A longer caption is split into a
separate durable text delivery followed by the file without that caption.

## HTTP Boundary

The embedded server accepts only the configured path and:

- `POST`;
- `Content-Type: application/json`;
- a bounded request body;
- an exact `X-Telegram-Bot-Api-Secret-Token` value.

Secret comparison does not short-circuit on the first differing byte.

Responses:

- wrong path: `404`;
- wrong method: `405`;
- missing or invalid secret: `401`;
- unsupported content type: `415`;
- oversized body: `413`;
- malformed JSON: `400`;
- valid unsupported update: `200`;
- durable authority or storage failure: `503`;
- committed new or duplicate outcome: `200`.

When an update requires durable ingress, a command outcome, or a system
delivery, the endpoint returns success only after that state has committed.
Valid unsupported updates may return success without persistence.

## Concurrency and Ordering

One Telegram chat has at most one active delivery operation. Different chats
may deliver concurrently under a global semaphore.

Delivery claims use expiring owner leases and bounded batches. `429` responses
set `next_attempt_at` from Telegram's retry delay. Other retryable failures use
bounded exponential backoff with jitter.

The initial delivery settings are:

- claim batch: 32;
- global delivery concurrency: 4;
- claim duration: 30 seconds;
- claim renewal interval: 10 seconds;
- exponential retry base: 1 second;
- exponential retry cap: 5 minutes.

Streaming edits for one work item are serialized. Final delivery fences and
closes its streaming state before editing or sending final chunks.

## Supervision and Shutdown

The supervisor owns:

- the HTTP accept loop;
- Telegram streaming;
- durable Telegram delivery.

Unexpected exit of any enabled Telegram component is terminal for
`codrik serve`. During shutdown:

1. Stop accepting new webhook connections.
2. Allow in-flight committed webhook transactions to finish.
3. Stop transient streaming updates.
4. Release or finish delivery claims according to known outcome state.
5. Leave the Telegram webhook registered.

The existing runtime shutdown recovery remains authoritative for actor leases,
runs, artifacts, and semantic outbox state.

## Observability

Structured events may include:

- component;
- transition;
- bot ID;
- update ID;
- actor, work item, run, outbox, and delivery IDs;
- delivery attempt and state;
- HTTP status class;
- Telegram method name;
- typed error class.

They must not include:

- bot token;
- webhook secret;
- full Telegram request or response bodies;
- link codes or hashes;
- Telegram message text;
- file content;
- full identity subject or chat address.

Identity and address values may be represented only by a stable
domain-separated diagnostic fingerprint when correlation is necessary.

## Testing

### Configuration and startup

- strict parsing, defaults, URL and secret validation;
- token and secret redaction;
- `getMe -> setWebhook -> getWebhookInfo` ordering;
- startup failure on invalid token or reconciliation mismatch;
- `drop_pending_updates: false` and `allowed_updates: ["message"]`.

### HTTP and update parsing

- secret, method, path, content type, and body-size matrix;
- private-chat acceptance;
- group, channel, bot, attachment-only, and unsupported update filtering;
- malformed JSON and missing required message fields;
- `200` only after durable commit.

### Linking

- successful, already-linked, invalid, expired, rate-limited, and conflict
  outcomes;
- `/link` without a code returns CLI instructions without issuing a code;
- duplicate update before and after simulated process failure;
- command ledger and delivery completion;
- no events, work items, runs, model calls, or memory rows from commands.

### Ingress and shared memory

- linked text becomes one durable event;
- duplicate `update_id` remains one event;
- unlinked text creates only one instruction delivery;
- two linked Telegram identities resolve to the same actor and may use
  actor-private memory;
- newest incorporated event selects the reply route.

### Streaming and delivery

- one-second edit throttling and unchanged-text suppression;
- restart fallback with and without a persisted streaming message ID;
- Unicode-safe 4096-character chunking and deterministic ordinals;
- text, photo, document, and caption behavior;
- path, size, hash, and 50 MB checks;
- retryable, terminal, rate-limited, and unknown-outcome transitions;
- per-chat serialization and cross-chat concurrency.

### End to end

A mocked Telegram server verifies:

1. startup webhook reconciliation;
2. `/link CODE` from an unlinked private identity;
3. an ordinary linked text update;
4. durable ingress and actor execution;
5. transient streaming edits;
6. durable final Telegram delivery;
7. no duplicate event or final response after replaying the same update.

## Documentation

The README will document:

- the `telegram` config block;
- reverse-proxy and HTTPS expectations;
- automatic webhook registration;
- linking flow using `codrik link`;
- private-chat and text-only inbound scope;
- streaming versus durable delivery guarantees;
- startup and delivery troubleshooting without exposing secrets.

## References

- [Telegram Bot API](https://core.telegram.org/bots/api)
- [Telegram Bots FAQ](https://core.telegram.org/bots/faq)
- [Telegram webhook guide](https://core.telegram.org/bots/webhooks)
