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
