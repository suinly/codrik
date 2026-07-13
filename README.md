# codrik

codrik is a small Rust agent runtime that runs user turns through an LLM,
optional tools, and session memory.

## Installation

Install the latest release on Linux or macOS:

```sh
curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh | sh
```

The installer downloads the release binary for the current platform, verifies
the `.sha256` checksum, and installs it as `codrik` into `~/.local/bin`. It can
also create `~/.codrik/config.yml` interactively and install a user
service for a configured gateway.
Release assets are produced by `scripts/release.sh`.

Supported release targets:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `raspberry-pi-5-aarch64-unknown-linux-gnu`
- `x86_64-unknown-linux-gnu`

Install a specific release:

```sh
curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh | env CODRIK_VERSION=v0.2.0 sh
```

Install to another directory:

```sh
curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh | env CODRIK_INSTALL_DIR=/usr/local/bin sh
```

Skip interactive configuration:

```sh
curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh | env CODRIK_SKIP_CONFIG=1 sh
```

Skip gateway service setup:

```sh
curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh | env CODRIK_SKIP_SERVICE=1 sh
```

Override the release repository or target:

```sh
curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh \
  | env CODRIK_REPO_URL=https://github.com/suinly/codrik \
    CODRIK_TARGET=x86_64-unknown-linux-gnu \
    sh
```

## Usage

Run a one-shot prompt:

```sh
codrik "hello"
```

Run a named session:

```sh
codrik --session work "hello"
```

Run the Telegram gateway:

```sh
codrik gateway telegram
```

Telegram access is controlled by `~/.codrik/users.json`. The first Telegram
user who sends `/start` is bootstrapped as enabled with access to standard
tools. Later users who send `/start` are added as disabled entries;
enable them manually and set their `tools` list:

```json
{
  "version": 1,
  "actors": {
    "actor:telegram:12312931": {
      "enabled": true,
      "display_name": "SomeUserName",
      "identities": [
        {
          "provider": "telegram",
          "subject": "12312931",
          "username": "SomeUserName"
        }
      ],
      "tools": ["*"]
    }
  }
}
```

Use `"tools": ["*"]` to grant access to standard tools such as `datetime`,
the embedded Obscura `web_browser`, and the sandboxed `bashkit` tool. The real
server `bash` tool is privileged and must be granted explicitly, for example
`"tools": ["*", "bash"]`.

For actor-scoped runs, real `bash` starts in that actor's workspace. Use a
relative output path such as `report.pdf`, then deliver it with
`send_file("workspace/report.pdf")`. The absolute `/workspace` mount exists
only inside `bashkit`.

## Skills

codrik discovers skills from these sources, in precedence order:

1. `.codrik/skills/<name>/SKILL.md` in the current working directory
2. `~/.codrik/skills/<name>/SKILL.md`
3. skills compiled into the codrik binary

The first skill with a given name wins, so project skills can override user and
built-in skills, and user skills can override built-ins. Built-in and project
skills are read-only through the skill tools; user skills are writable. Skills
are available through standard tools, so actors with `"tools": ["*"]` can list,
read, create, and update user skills.

codrik ships with `skill-creator`, a built-in workflow for creating and
reviewing reusable user skills. It is available without installing additional
files and can be overridden by a project or user skill with the same name.

codrik includes a compact skill index in the agent instructions so the model can
match tasks against skill names and descriptions before it answers. The full
`SKILL.md` content is still loaded on demand through `skills_read`.

The runtime exposes:

- `skills_list`: returns available skill names, descriptions, and sources
- `skills_read`: reads `SKILL.md` or a relative reference file inside a skill
- `skills_create`: writes `~/.codrik/skills/<name>/SKILL.md`
- `skills_update`: rewrites an existing user skill in `~/.codrik/skills`

Project and built-in skills are read-only through the skill tools. If a higher
precedence skill hides a user skill with the same name, `skills_update` refuses
to edit the hidden user skill.

Minimal skill:

```md
---
name: telegram-debug
description: Use when debugging Telegram gateway behavior, auth, sessions, or delivery failures.
---

# Telegram Debug

1. Check gateway logs.
2. Inspect `~/.codrik/users.json`.
3. Verify the active Telegram session.
```

`web_browser` uses Obscura as an embedded Rust browser API, pinned as a git
dependency. The first build can take longer than usual because Obscura builds
its browser runtime dependencies from source.

Telegram sessions are stored under `~/.codrik/sessions/<telegram-chat-id>/`,
with chat-local metadata in `index.json`. Each session has its own directory
containing `messages.json`, attachments, and the provider file cache. Send
`/new` to create and switch to a fresh session. Send `/sessions` to list recent
sessions, `/sessions <id>` to switch, or `/sessions delete <id>` to delete an
inactive session and its local/provider files.

Telegram accepts text, photos, and documents. Captions and files are preserved
in their original order as one user turn. Supported images and documents are
uploaded lazily for model input; unsupported binary formats are still stored
and can be returned with `send_file`, but the model receives metadata only.

Configure attachment limits and image detail in `config.yml`:

```yaml
api_key: "..."
base_url: "https://api.openai.com/v1"
model: "gpt-5"
attachments:
  max_file_size_mb: 20
  image_detail: auto # auto, low, or high
telegram:
  token: "..."
```

The configured `base_url` must implement the OpenAI Responses API. When the
provider also implements the Files API, uploaded files are cached per session
and cleaned up when the session is deleted. Providers without the Files API,
such as Ollama, receive images as inline `data:` URLs; other file types fall
back to metadata-only context.

Update to the latest release:

```sh
codrik update
```

If the Telegram gateway user service is running, `codrik update` restarts it
after replacing the binary.

By default, `codrik` loads config from `CODRIK_CONFIG`, then
`./config.yml`, then `~/.codrik/config.yml`.

## Gateway Service

On Linux, the installer creates a user-level systemd service and tries to enable
systemd lingering so it keeps running after logout. Manage it without `sudo` and
with `--user`:

```sh
systemctl --user status codrik-telegram.service
systemctl --user restart codrik-telegram.service
journalctl --user -u codrik-telegram.service -f
```

If the gateway stops after logout, enable lingering manually and restart it:

```sh
loginctl enable-linger "$USER"
systemctl --user restart codrik-telegram.service
```
