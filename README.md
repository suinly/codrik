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

On a clean interactive install, `~/.codrik` is mode `0700`, `users.json` is
mode `0600`, and the installer creates the enabled actor
`actor:local:owner` with standard-tool authorization `tools: ["*"]`. Existing
authorization is user-owned and is never rewritten; the installer asks which
existing actor ID the runtime should use.

## Configuration

Codrik loads configuration from `CODRIK_CONFIG`, then `./config.yml`, then
`~/.codrik/config.yml`. A minimal runtime configuration is:

```yaml
api_key: "..."
base_url: "https://api.openai.com/v1"
model: "gpt-5"
runtime:
  actor_id: actor:local:owner
```

Runtime paths honor `CODRIK_HOME` and default to:

- database: `~/.codrik/runtime.sqlite`
- socket: `~/.codrik/codrik.sock`
- lock: `~/.codrik/runtime.lock`
- managed artifacts: `~/.codrik/artifacts`

The configured actor must exist in `users.json` on first startup and must be
enabled. Authorization is imported into SQLite once without changing
`users.json`.

## Commands

Start the foreground runtime:

```sh
codrik serve
```

Submit a request through the running daemon:

```sh
codrik "question"
```

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
