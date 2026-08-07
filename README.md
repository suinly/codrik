# codrik

Codrik is a foreground Rust agent runtime with durable local execution over a
private Unix socket.

## Installation

Install the latest release on Linux or macOS:

```sh
curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh | sh
```

The installer verifies the release checksum, installs `codrik` into
`~/.local/bin`, and can create a user-level systemd or launchd service. The
service runs `codrik serve`; Codrik never daemonizes itself.

On a clean interactive install, the installer writes
`runtime.actor_id: actor:local:owner`. The first `codrik serve` run
automatically creates the first actor in SQLite as enabled with standard-tool
authorization `tools: ["*"]`.

## Configuration

Codrik looks for configuration in this order:

1. the path in `CODRIK_CONFIG`;
2. `./config.yml`;
3. `~/.codrik/config.yml`.

A minimal configuration for `codrik serve` is:

```yaml
api_key: "..."
base_url: "https://api.openai.com/v1"
model: "gpt-5"
runtime:
  actor_id: actor:local:owner
```

A complete configuration with every supported field is:

```yaml
api_key: "..."
base_url: "https://api.openai.com/v1"
model: "gpt-5"

attachments:
  max_file_size_mb: 20
  image_detail: auto

runtime:
  actor_id: actor:local:owner
  database_path: /absolute/path/to/runtime.sqlite
  socket_path: /absolute/path/to/codrik.sock
  lock_path: /absolute/path/to/runtime.lock
  artifact_path: /absolute/path/to/artifacts

telegram:
  token: "..."
  mode: webhook
  public_url: "https://agent.example.com/webhooks/telegram"
  listen: "127.0.0.1:8080"
  webhook_secret: "..."

reticulum:
  rns_address: "127.0.0.1:4242"
  python: "/absolute/path/to/venv/bin/python3"

webhooks:
  listen: "127.0.0.1:8081"
  endpoints:
    grafana:
      path: "/webhooks/grafana"
      token: "..."
      actor_id: "owner"
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `api_key` | Yes | None | Provider API key. Keep the configuration file private. |
| `base_url` | Yes | None | OpenAI-compatible API base URL. |
| `model` | Yes | None | Model name sent to the configured provider. |
| `attachments.max_file_size_mb` | No | `20` | Maximum legacy/session attachment size in MiB. Hosted Telegram downloads always use Telegram's fixed 20,000,000-byte limit. |
| `attachments.image_detail` | No | `auto` | Image detail: `auto`, `low`, or `high`. |
| `runtime.actor_id` | For `serve` | None | Actor selected by the runtime; automatically created only when the actors table is empty. |
| `runtime.database_path` | No | `<CODRIK_HOME>/runtime.sqlite` | Durable SQLite database. |
| `runtime.socket_path` | No | `<CODRIK_HOME>/codrik.sock` | Private Unix socket. |
| `runtime.lock_path` | No | `<CODRIK_HOME>/runtime.lock` | Exclusive server instance lock. |
| `runtime.artifact_path` | No | `<CODRIK_HOME>/artifacts` | Managed tool-result files. |
| `telegram.token` | When Telegram is enabled | None | Bot token obtained from BotFather. Keep it private. |
| `telegram.mode` | No | `webhook` | Ingress transport: `webhook` or `polling`. |
| `telegram.public_url` | In webhook mode | None | Public HTTPS webhook URL without a query or fragment. |
| `telegram.listen` | In webhook mode | `127.0.0.1:8080` | Local HTTP listener behind the HTTPS reverse proxy. |
| `telegram.webhook_secret` | In webhook mode | None | Secret-token value used to authenticate Telegram webhook requests. |
| `reticulum.rns_address` | When Reticulum is enabled | None | Existing Reticulum `TCPServerInterface` endpoint as `host:port`. |
| `reticulum.python` | No | `python3` | Python executable containing the `RNS` and `LXMF` packages. |
| `webhooks.listen` | When generic webhooks are enabled | None | Local HTTP listener, normally loopback behind a TLS reverse proxy. |
| `webhooks.endpoints.<name>.path` | Yes | None | Exact POST path for this endpoint. Paths must be unique. |
| `webhooks.endpoints.<name>.token` | Yes | None | Private bearer token used to authenticate requests. |
| `webhooks.endpoints.<name>.actor_id` | Yes | None | Existing enabled actor receiving this endpoint's events. |

### Runtime paths

`CODRIK_HOME` controls the runtime data directory and defaults to
`~/.codrik`. Client request recovery metadata is always stored under
`<CODRIK_HOME>/client/requests`; this path is not configurable.

When a configured runtime path starts with `~/`, Codrik resolves it relative
to `CODRIK_HOME`, not directly relative to the operating-system home
directory. For example, with `CODRIK_HOME=/srv/codrik`,
`~/data/runtime.sqlite` resolves to `/srv/codrik/data/runtime.sqlite`.
Codrik does not expand `$HOME` or a `~` embedded elsewhere in a path.

Other relative paths remain relative to the working directory of
`codrik serve`. Prefer the defaults or absolute paths when Codrik is managed
by systemd, launchd, or another service manager.

### Actor bootstrap

The `runtime` section is required by `codrik serve`, and `runtime.actor_id`
must not be blank. On an empty SQLite database, Codrik creates the configured
actor as enabled with `tools: ["*"]` before starting the runtime.

Once any actor exists, bootstrap never creates another one. If
`runtime.actor_id` names an absent actor in a nonempty database, startup fails
instead of silently granting a new actor access. Disabled configured actors
also prevent startup.

### Common configuration errors

- `runtime configuration is required`: add `runtime.actor_id`.
- `runtime.actor_id must not be blank`: configure a nonempty actor ID.
- `configured runtime actor ... does not exist`: correct `runtime.actor_id` or
  add the actor through an authorized runtime management path.
- `configured runtime actor ... is disabled`: enable the selected actor or
  choose another one.
- Unsafe, writable, or symlinked runtime directories are rejected before the
  Unix socket is opened.
- Malformed YAML, invalid value types, duplicate fields, and obsolete
  unsupported top-level fields cause configuration loading to fail.

## Commands

Start the foreground runtime:

```sh
codrik serve
```

Submit a request through the running daemon:

```sh
codrik "question"
```

Create a one-time code for linking another supported channel to the configured
actor:

```sh
codrik link
```

The daemon prints an eight-character code and the exact `/link CODE` message to
send in the new channel. Codes expire after 10 minutes, can be used once, and a
new code invalidates the actor's previous unused code.

### Actor administration

Manage actors through the running daemon's private Unix socket:

```sh
codrik actors list
codrik actors create alice
codrik actors show alice
codrik actors tools grant alice '*'
codrik actors tools grant alice bash
codrik actors tools list alice
codrik link alice
codrik actors disable alice
codrik actors delete alice --force
```

New actors are enabled with no tool grants. Grant `'*'` for standard tools;
privileged `bash` still requires its own explicit grant. The actor configured
as `runtime.actor_id` is the local default and cannot be disabled or deleted.
Codrik serves all enabled actors concurrently. Permission changes apply on the
next run; a run already in progress keeps its original tool grants. Disabling
an actor lets its active work finish but prevents new work from starting. A
normal delete only removes an empty actor; `--force` permanently removes all
durable state for an already disabled and idle actor, and cannot be undone.

`codrik link` issues a code for `runtime.actor_id`; `codrik link alice` targets
Alice instead. Every linked channel resolves to the selected actor's shared
memory and durable knowledge.

## Telegram gateway

Telegram support is optional. Set `telegram.mode` explicitly to `polling` when
the Codrik host cannot accept public inbound connections:

```yaml
telegram:
  token: "..."
  mode: polling
```

Polling and webhook are mutually exclusive ingress modes. Change
`telegram.mode` and restart `codrik serve` to switch between them; Codrik does
not fall back from one mode to the other automatically.

At startup, polling mode calls `getMe`, removes any existing webhook without
dropping pending updates, verifies that the webhook URL is empty, and starts
Telegram long polling. Only one running polling instance should use a bot
token. `public_url`, `listen`, and `webhook_secret` are ignored in this mode.
Polling retries transient failures with delays of 1, 2, 4, 8, 16, then 30
seconds; Telegram's `retry_after` value takes precedence. Update replay after a
restart is safe because ingress is durable and deduplicated by update ID.

### Incoming files

Private Telegram chats accept photos, documents, videos, animations, audio,
voice messages, video notes, and stickers. Every Telegram update is a separate
agent turn; album members are not grouped. A caption is passed before its file,
while a file without a caption is also valid input.

Codrik uses Telegram's hosted Bot API and accepts at most 20,000,000 downloaded
bytes per file. It verifies the actual downloaded size and derives MIME type
from the content rather than trusting Telegram metadata. Files are retained
under `<CODRIK_HOME>/attachments/<actor-id>/` until that actor is force-deleted.
Provider-supported images and documents are sent to the model. Other formats
remain available to the model as verified metadata and a safe local path.

### Webhook mode

Webhook is the default when `telegram.mode` is omitted. `codrik serve` binds
the configured local listener, calls `getMe`, registers the webhook with
`setWebhook`, and verifies the resulting webhook information before the
runtime becomes ready. Startup fails if registration or verification does not
match the configured public URL.

TLS termination belongs to a reverse proxy. Proxy only the exact webhook path
to Codrik's local listener. For example, with Caddy:

```caddyfile
agent.example.com {
    @telegram path /webhooks/telegram
    reverse_proxy @telegram 127.0.0.1:8080
}
```

Or with Nginx:

```nginx
server {
    listen 443 ssl;
    server_name agent.example.com;

    location = /webhooks/telegram {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

Keep `telegram.listen` on loopback unless the surrounding network provides an
equivalent access boundary. The public URL must use HTTPS and must have the
same path as the reverse-proxy rule. The bot token and webhook secret are
redacted from debug output and are never included in runtime logs.

### Linking Telegram

Generate a one-time code in an already authorized local channel:

```sh
codrik link
```

Then send the printed command to the bot in a private chat:

```text
/link CODE
```

The link command is handled by the gateway itself. It does not create an agent
event, work item, model call, or memory entry. Once linked, Telegram and local
CLI requests resolve to the same actor and therefore share actor-private
memory and durable knowledge.

Link codes expire after 10 minutes and are single-use. Replaying the same
Telegram update is idempotent. A Telegram identity already linked to another
actor is not silently reassigned.

### Supported Telegram scope

Inbound support is intentionally narrow:

- private chats only;
- non-bot senders only;
- text messages and `/link` commands only;
- one Telegram bot per Codrik runtime.

Groups, channels, callback queries, incoming photos, documents, and other
attachments are ignored. Outbound replies support text and managed files.
JPEG, PNG, and WebP files up to 10 MiB use Telegram photo delivery; other
managed files up to 50 MiB use document delivery.

While the model is generating, Codrik sends Telegram's `typing` chat action
every four seconds. Text deltas are not posted or edited into the chat. When
the agent starts a tool, Codrik posts a transient elapsed-time status such as
`Работаю над задачей — 10 сек`; an LLM-provided activity description replaces
the default text when available.

Durable Telegram text is delivered through Rich Messages, so supported Rich
Markdown constructs such as headings, lists, tables, fenced code, links,
quotations, spoilers, formulas, and details blocks render natively. Codrik
passes text to Telegram unchanged. If Telegram definitively rejects a rich
message, Codrik sends the same chunk as readable plain text. Retryable or
outcome-unknown rich sends never trigger fallback, avoiding duplicate messages.

The durable Telegram text chunk limit remains 4096 characters. A chunk boundary
may split Markdown syntax; if Telegram rejects that chunk, the plain-text
fallback preserves its content. In private chats durable messages do not use
Telegram's reply-to UI because the conversation target is already unambiguous.
Files remain durable; captions use a 1024-character limit.

Telegram API retryable failures use bounded exponential backoff. A Telegram
`429 retry_after` value takes precedence. Terminal API responses are recorded
as `failed_terminal`. If Codrik cannot determine whether Telegram accepted a
send, the delivery becomes `outcome_unknown` and is not automatically repeated
because doing so could duplicate a message.

### Telegram troubleshooting

- `401 Unauthorized`: the
  `X-Telegram-Bot-Api-Secret-Token` header is missing or does not exactly match
  `telegram.webhook_secret`. Let Telegram set this header; do not replace it in
  the proxy.
- `413 Payload Too Large`: the webhook body exceeded 1 MiB. Standard private
  text updates should remain well below this limit.
- `503 Service Unavailable`: Codrik could not durably process the update,
  usually because SQLite authority or storage was unavailable, or the
  64-request webhook concurrency limit was saturated. Telegram may retry the
  update using the same update ID.
- Webhook reconciliation mismatch during startup: verify that
  `telegram.public_url` exactly matches the externally reachable HTTPS URL and
  that the configured bot token belongs to the intended bot.
- Telegram `429`: Codrik schedules the delivery at Telegram's requested retry
  time. Persistent rate limiting usually indicates excessive outbound traffic.
- `failed_terminal`: Telegram definitively rejected the delivery, for example
  because the chat is unavailable or a managed file violates a delivery
  constraint. Correct the underlying configuration or channel state; Codrik
  does not automatically retry terminal failures.
- `outcome_unknown`: a transport interruption occurred after a send may have
  reached Telegram. Inspect the chat before taking manual action to avoid a
  duplicate message.

## Local skills

Codrik discovers project skills from `.codrik/skills`, user skills from
`~/.codrik/skills`, and compiled built-in skills in that precedence order. It
exposes `skills_list` and `skills_read` for discovery and reading, plus three
strict user-skill mutations.

`skills_create` creates only new user skills, `skills_update` replaces only an
existing writable user's `SKILL.md`, and `skills_delete` permanently removes
the complete writable user-skill directory only when `confirm` is `true`.
Project and built-in skills remain read-only. Mutation tools never fall back to
a different operation.

## Generic webhook gateway

Generic webhooks are optional authenticated JSON ingress. Each endpoint maps an
exact path to one existing enabled actor. Codrik validates all endpoint actors,
binds `webhooks.listen`, then reports runtime readiness. Invalid actors or a
listener bind conflict fail startup. Keep the listener on loopback and terminate
public HTTPS at a reverse proxy; expose only configured exact paths.

Requests must use `POST`, `Content-Type: application/json`, and
`Authorization: Bearer <token>`. Codrik accepts arbitrary valid JSON up to 1
MiB. A successful request returns a bodyless `202 Accepted` only after the event
and work item commit to SQLite; model processing and delivery continue
asynchronously.

Supply an `Idempotency-Key` header when the sender has a stable event ID. The
key may contain 1-256 visible ASCII characters. Explicit keys deduplicate
per endpoint permanently, even when a replay's body differs. Without that
header, Codrik hashes the body and suppresses identical bodies for 24 hours,
including a replay exactly 24 hours after acceptance.

Webhook runs receive a fixed frame identifying the endpoint and reception time,
with the submitted JSON marked as untrusted data. They can use only the
read-only `skills_list` and `skills_read` tools. The endpoint actor's ordinary
runs retain their configured tools.

If the actor has previously accepted Telegram text, Codrik snapshots that
actor's latest Telegram chat route into the webhook event. The final result is
delivered to that immutable chat route without Telegram reply-to. When no route
exists, processing still completes and delivery is deferred. A later accepted
Telegram message releases only the newest deferred result; older deferred
results remain suppressed.

### Grafana contact point

Create a Grafana webhook contact point targeting, for example,
`https://agent.example.com/webhooks/grafana`. Configure these headers:

```text
Authorization: Bearer <the configured endpoint token>
Content-Type: application/json
Idempotency-Key: {{ .GroupKey }}
```

Use Grafana's default webhook body or any custom JSON body. Grafana-specific
schema parsing is unnecessary; Codrik treats the complete body as untrusted
event data. Choose a stable notification identifier for `Idempotency-Key` if
`.GroupKey` does not match the desired replay scope.

### Webhook troubleshooting

- `400 Bad Request`: malformed JSON or an empty, oversized, or non-visible
  `Idempotency-Key`.
- `401 Unauthorized`: missing or mismatched bearer token.
- `404 Not Found`: path does not exactly match a configured endpoint.
- `405 Method Not Allowed`: endpoint called with a method other than `POST`.
- `413 Payload Too Large`: body exceeds 1 MiB.
- `415 Unsupported Media Type`: `Content-Type` is not `application/json`.
- `503 Service Unavailable`: SQLite could not commit, the endpoint actor became
  unavailable, or the 64-request concurrency limit was saturated. Retry with
  the same explicit idempotency key.

## Reticulum LXMF gateway

Reticulum support is optional. Codrik starts a bundled Python bridge and
connects it to an existing RNS `TCPServerInterface`. Create a dedicated Python
environment and install LXMF, which includes its `RNS` dependency:

```sh
python3 -m venv ~/.codrik/reticulum-venv
~/.codrik/reticulum-venv/bin/python -m pip install lxmf
```

Configure the TCP endpoint and that environment's Python executable:

```yaml
reticulum:
  rns_address: "127.0.0.1:4242"
  python: "/absolute/path/to/venv/bin/python3"
```

`rns_address` targets the configured RNS `TCPServerInterface`; it is not the
Reticulum local shared-instance port. Codrik does not install Python packages,
start `rnsd`, or configure a propagation node.

At startup, the structured runtime log includes `reticulum_destination`, the
32-character public LXMF destination. The private identity remains at
`<CODRIK_HOME>/reticulum/identity` and is reused across restarts. Do not delete
that file unless changing the public destination is intentional.

Generate a one-time code with `codrik link`, then send this direct LXMF text
message from Sideband, MeshChat, or another compatible client:

```text
/link CODE
```

The first version accepts direct, signed UTF-8 text only. Attachments, titles,
fields, groups, rich rendering, and propagation-node retrieval are unsupported.
Normal messages use the LXMF message hash for durable deduplication. Replies
are plain text. A definitive failure is recorded as `failed_terminal`; a bridge
loss after submission is `outcome_unknown` and is not automatically resent.

Reticulum replies are intentionally concise and limited to 500 Unicode
characters to reduce airtime. CLI and Telegram replies are unaffected.

The Reticulum bridge is part of the fail-fast runtime. If it exits, `codrik
serve` stops with an error; systemd, launchd, or another service manager owns
restart policy.

### Reticulum troubleshooting

- `ModuleNotFoundError: RNS` or `ModuleNotFoundError: LXMF`: set
  `reticulum.python` to the virtual environment where `lxmf` was installed.
- TCP connection refused or startup readiness timeout: verify `rnsd` is running,
  port `4242` is reachable, and `rns_address` names its `TCPServerInterface`.
- No path to a peer: have the peer announce its LXMF destination and verify both
  nodes share reachable Reticulum interfaces.
- State permission failure: `<CODRIK_HOME>/reticulum` must be a real,
  owner-controlled mode-`0700` directory; identity and generated config files
  must be regular mode-`0600` files.
- `failed_terminal`: correct the destination or message; Codrik will not retry.
- `outcome_unknown`: inspect the remote client before manual retry to avoid a
  duplicate.
- Bridge exit: inspect the preceding Reticulum bridge error, then let the
  external service manager restart `codrik serve` after correcting it.

### Manual Reticulum check

1. Start `rnsd` with `TCPServerInterface` on port `4242`.
2. Start `codrik serve`; record `reticulum_destination`.
3. Run `codrik link`; send `/link CODE` from an LXMF client.
4. Send `hello over LXMF`; verify exactly one agent reply.
5. Restart Codrik; verify `reticulum_destination` is unchanged.
6. Replay the same LXMF message; verify no duplicate agent work occurs.

Resume a disconnected request:

```sh
codrik resume <request-id>
```

Cancel the durable work associated with a request:

```sh
codrik cancel <request-id>
```

Install the latest release:

```sh
codrik update
```

`codrik serve` owns the runtime database, socket, dispatcher, and durable
delivery worker for its entire foreground lifetime. A service manager may own
background execution and restart policy. A second server fails without
removing the live server's socket.

Ctrl-C while `codrik "question"` is running disconnects only the client; it
does not cancel durable work. Codrik prints the exact `codrik resume
<request-id>` recovery command. Use `codrik cancel <request-id>` when
cancellation is intended.

Final output is verified from an immutable durable result bundle before local
display. If the connection is lost after display but before the bundle ACK,
the same final result may be displayed again on resume. Delivery is therefore
at least once locally.

SQLite state changes are exactly once, but a model provider call cannot share
the SQLite transaction. If Codrik crashes after the provider accepts a call
but before its output is checkpointed, recovery may repeat that LLM call.
