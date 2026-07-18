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
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `api_key` | Yes | None | Provider API key. Keep the configuration file private. |
| `base_url` | Yes | None | OpenAI-compatible API base URL. |
| `model` | Yes | None | Model name sent to the configured provider. |
| `attachments.max_file_size_mb` | No | `20` | Maximum accepted attachment size in MiB. |
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
as `runtime.actor_id` cannot be disabled or deleted. Disabling an actor lets
its active work finish but prevents new work from starting. A normal delete
only removes an empty actor; `--force` permanently removes all durable state
for an already disabled and idle actor, and cannot be undone.

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
