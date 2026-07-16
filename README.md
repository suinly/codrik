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
