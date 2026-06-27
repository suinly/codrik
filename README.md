# codrik

codrik is a small Rust agent runtime that runs user turns through an LLM,
optional tools, and session memory.

## Installation

Install the latest release on Linux or macOS:

```sh
curl -fsSL https://gitflow.suinly.com/codrik/codrik/raw/branch/main/scripts/install.sh | sh
```

The installer downloads the release binary for the current platform, verifies
the `.sha256` checksum, and installs it as `codrik` into `~/.local/bin`.
Release assets are produced by `scripts/release.sh`.

Supported release targets:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `raspberry-pi-5-aarch64-unknown-linux-gnu`
- `x86_64-unknown-linux-gnu`

Install a specific release:

```sh
curl -fsSL https://gitflow.suinly.com/codrik/codrik/raw/branch/main/scripts/install.sh | env CODRIK_VERSION=v0.2.0 sh
```

Install to another directory:

```sh
curl -fsSL https://gitflow.suinly.com/codrik/codrik/raw/branch/main/scripts/install.sh | env CODRIK_INSTALL_DIR=/usr/local/bin sh
```

Override the release repository or target:

```sh
curl -fsSL https://gitflow.suinly.com/codrik/codrik/raw/branch/main/scripts/install.sh \
  | env CODRIK_REPO_URL=https://gitflow.suinly.com/codrik/codrik \
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
