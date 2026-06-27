#!/usr/bin/env sh
set -eu

BIN_NAME="codrik"
DEFAULT_REPO_URL="https://github.com/suinly/codrik"
DEFAULT_BASE_URL="https://api.openai.com/v1"
DEFAULT_MODEL="gpt-4.1-mini"
CONFIGURED_GATEWAY="none"
CONFIGURED_CONFIG_FILE=""
if [ -n "${HOME:-}" ]; then
  DEFAULT_INSTALL_DIR="$HOME/.local/bin"
  DEFAULT_CONFIG_DIR="$HOME/.codrik"
else
  DEFAULT_INSTALL_DIR=""
  DEFAULT_CONFIG_DIR=""
fi

die() {
  echo "error: $*" >&2
  exit 1
}

need_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

usage() {
  cat <<USAGE
Install codrik from release assets.

Usage:
  curl -fsSL https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh | sh

Environment:
  CODRIK_REPO_URL      Release repository URL. Default: ${DEFAULT_REPO_URL}
  CODRIK_VERSION       Release tag or version. Default: latest
  CODRIK_INSTALL_DIR   Install directory. Default: ~/.local/bin
  CODRIK_TARGET        Override release asset target suffix.
  CODRIK_CONFIG_DIR    Config directory. Default: ~/.codrik
  CODRIK_HOME          Runtime data directory. Default: CODRIK_CONFIG_DIR
  CODRIK_SKIP_CONFIG   Set to 1 to skip interactive config setup.
  CODRIK_SKIP_SERVICE  Set to 1 to skip gateway service setup.

Examples:
  CODRIK_VERSION=v0.2.0 sh scripts/install.sh
  CODRIK_INSTALL_DIR=/usr/local/bin sh scripts/install.sh
  CODRIK_SKIP_CONFIG=1 sh scripts/install.sh
  CODRIK_SKIP_SERVICE=1 sh scripts/install.sh
USAGE
}

normalize_tag() {
  version="$1"
  case "$version" in
    v*) printf '%s\n' "$version" ;;
    *) printf 'v%s\n' "$version" ;;
  esac
}

resolve_latest_tag() {
  repo_url="$1"
  effective_url="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$repo_url/releases/latest")" \
    || die "could not resolve latest release from $repo_url"
  tag="${effective_url##*/}"

  case "$tag" in
    v[0-9]*)
      printf '%s\n' "$tag"
      ;;
    *)
      die "could not detect latest release tag from $effective_url; set CODRIK_VERSION explicitly"
      ;;
  esac
}

detect_target() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os:$arch" in
    Darwin:arm64 | Darwin:aarch64)
      printf '%s\n' "aarch64-apple-darwin"
      ;;
    Darwin:x86_64)
      printf '%s\n' "x86_64-apple-darwin"
      ;;
    Linux:aarch64 | Linux:arm64)
      printf '%s\n' "raspberry-pi-5-aarch64-unknown-linux-gnu"
      ;;
    Linux:x86_64 | Linux:amd64)
      printf '%s\n' "x86_64-unknown-linux-gnu"
      ;;
    *)
      die "unsupported platform: $os $arch"
      ;;
  esac
}

verify_checksum() {
  checksum_file="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "$checksum_file"
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -c "$checksum_file"
    return
  fi

  die "missing required command: sha256sum or shasum"
}

is_interactive() {
  [ -r /dev/tty ] && [ -w /dev/tty ]
}

ask() {
  prompt="$1"
  default_value="${2:-}"
  value=""

  if [ -n "$default_value" ]; then
    printf '%s [%s]: ' "$prompt" "$default_value" >/dev/tty
  else
    printf '%s: ' "$prompt" >/dev/tty
  fi

  IFS= read -r value </dev/tty || value=""
  if [ -z "$value" ]; then
    value="$default_value"
  fi

  printf '%s\n' "$value"
}

ask_secret() {
  prompt="$1"
  value=""

  printf '%s: ' "$prompt" >/dev/tty
  if command -v stty >/dev/null 2>&1; then
    stty -echo </dev/tty
    IFS= read -r value </dev/tty || value=""
    stty echo </dev/tty
    printf '\n' >/dev/tty
  else
    IFS= read -r value </dev/tty || value=""
  fi

  printf '%s\n' "$value"
}

ask_yes_no() {
  prompt="$1"
  default_value="${2:-y}"
  answer=""

  while :; do
    answer="$(ask "$prompt" "$default_value")"
    case "$answer" in
      y | Y | yes | YES) return 0 ;;
      n | N | no | NO) return 1 ;;
      *) echo "Please answer y or n." >&2 ;;
    esac
  done
}

yaml_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

write_config() {
  config_file="$1"
  api_key="$2"
  base_url="$3"
  model="$4"
  gateway="$5"
  telegram_token="$6"

  umask 077
  {
    printf 'api_key: "%s"\n' "$(yaml_escape "$api_key")"
    printf 'base_url: "%s"\n' "$(yaml_escape "$base_url")"
    printf 'model: "%s"\n' "$(yaml_escape "$model")"
    if [ "$gateway" = "telegram" ]; then
      printf 'telegram:\n'
      printf '  token: "%s"\n' "$(yaml_escape "$telegram_token")"
    else
      printf 'telegram: null\n'
    fi
  } >"$config_file"
}

xml_escape() {
  printf '%s' "$1" \
    | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g'
}

write_systemd_user_service() {
  service_file="$1"
  bin_path="$2"
  config_file="$3"
  gateway="$4"
  config_dir="$5"

  mkdir -p "$(dirname "$service_file")"
  cat >"$service_file" <<SERVICE
[Unit]
Description=Codrik $gateway gateway
After=network-online.target

[Service]
Type=simple
Environment=CODRIK_CONFIG=$config_file
Environment=CODRIK_HOME=$config_dir
ExecStart=$bin_path gateway $gateway
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
SERVICE
}

write_launchd_service() {
  plist_file="$1"
  bin_path="$2"
  config_file="$3"
  gateway="$4"
  config_dir="$5"
  label="com.suinly.codrik.$gateway"

  mkdir -p "$(dirname "$plist_file")"
  cat >"$plist_file" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$(xml_escape "$label")</string>
  <key>ProgramArguments</key>
  <array>
    <string>$(xml_escape "$bin_path")</string>
    <string>gateway</string>
    <string>$(xml_escape "$gateway")</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>CODRIK_CONFIG</key>
    <string>$(xml_escape "$config_file")</string>
    <key>CODRIK_HOME</key>
    <string>$(xml_escape "$config_dir")</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$(xml_escape "$config_dir/$gateway.log")</string>
  <key>StandardErrorPath</key>
  <string>$(xml_escape "$config_dir/$gateway.err.log")</string>
</dict>
</plist>
PLIST
}

print_service_management_hint() {
  os="$1"
  gateway="$2"

  case "$os" in
    Linux)
      cat <<HINT
Manage the gateway service with:
  systemctl --user status codrik-$gateway.service
  systemctl --user restart codrik-$gateway.service
  journalctl --user -u codrik-$gateway.service -f

This is a user service, so do not use sudo, service, or systemctl without --user.
To start it at boot without logging in, run:
  loginctl enable-linger $(id -un 2>/dev/null || printf '%s' "\$USER")
HINT
      ;;
    Darwin)
      cat <<HINT
Manage the gateway service with:
  launchctl print gui/$(id -u)/com.suinly.codrik.$gateway
  launchctl kickstart -k gui/$(id -u)/com.suinly.codrik.$gateway

Logs:
  ~/.codrik/$gateway.log
  ~/.codrik/$gateway.err.log
HINT
      ;;
  esac
}

install_gateway_service() {
  gateway="$1"
  bin_path="$2"
  config_file="$3"
  runtime_dir="$4"
  os="$(uname -s)"

  [ "$gateway" != "none" ] || return
  mkdir -p "$runtime_dir"

  case "$os" in
    Linux)
      need_command systemctl
      service_file="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/codrik-$gateway.service"
      write_systemd_user_service "$service_file" "$bin_path" "$config_file" "$gateway" "$runtime_dir"
      systemctl --user daemon-reload
      systemctl --user enable --now "codrik-$gateway.service"
      echo "Started user service codrik-$gateway.service"
      print_service_management_hint "$os" "$gateway"
      ;;
    Darwin)
      need_command launchctl
      need_command id
      plist_file="$HOME/Library/LaunchAgents/com.suinly.codrik.$gateway.plist"
      label="com.suinly.codrik.$gateway"
      write_launchd_service "$plist_file" "$bin_path" "$config_file" "$gateway" "$runtime_dir"
      launchctl bootout "gui/$(id -u)" "$plist_file" >/dev/null 2>&1 || true
      launchctl bootstrap "gui/$(id -u)" "$plist_file"
      launchctl enable "gui/$(id -u)/$label"
      launchctl kickstart -k "gui/$(id -u)/$label"
      echo "Started LaunchAgent $label"
      print_service_management_hint "$os" "$gateway"
      ;;
    *)
      echo "Gateway service setup is not supported on $os." >&2
      ;;
  esac
}

configure_codrik() {
  config_dir="$1"
  config_file="$config_dir/codrik.config.yml"
  CONFIGURED_GATEWAY="none"
  CONFIGURED_CONFIG_FILE="$config_file"

  [ -n "$config_dir" ] || die "could not determine config directory; set CODRIK_CONFIG_DIR"

  if ! is_interactive; then
    echo "Skipping config setup because stdin/stdout is not interactive." >&2
    echo "Run $BIN_NAME after creating $config_file or set CODRIK_CONFIG." >&2
    return
  fi

  if ! ask_yes_no "Configure codrik now?" "y"; then
    return
  fi

  if [ -f "$config_file" ] && ! ask_yes_no "$config_file already exists. Overwrite it?" "n"; then
    echo "Keeping existing config: $config_file"
    if ask_yes_no "Install or restart a gateway service for the existing config?" "n"; then
      while :; do
        CONFIGURED_GATEWAY="$(ask "Gateway service to run (telegram)" "telegram")"
        case "$CONFIGURED_GATEWAY" in
          telegram) break ;;
          *) echo "Supported gateway service: telegram." >&2 ;;
        esac
      done
    fi
    return
  fi

  api_key=""
  while [ -z "$api_key" ]; do
    api_key="$(ask_secret "OpenAI-compatible API key")"
    if [ -z "$api_key" ]; then
      echo "API key is required." >&2
    fi
  done

  base_url="$(ask "OpenAI-compatible base URL" "$DEFAULT_BASE_URL")"
  model="$(ask "Model" "$DEFAULT_MODEL")"

  gateway="none"
  if ask_yes_no "Configure a gateway?" "n"; then
    while :; do
      gateway="$(ask "Gateway (telegram)" "telegram")"
      case "$gateway" in
        telegram) break ;;
        *) echo "Supported gateway: telegram." >&2 ;;
      esac
    done
  fi

  telegram_token=""
  if [ "$gateway" = "telegram" ]; then
    while [ -z "$telegram_token" ]; do
      telegram_token="$(ask_secret "Telegram bot token")"
      if [ -z "$telegram_token" ]; then
        echo "Telegram bot token is required for telegram gateway." >&2
      fi
    done
  fi

  mkdir -p "$config_dir"
  write_config "$config_file" "$api_key" "$base_url" "$model" "$gateway" "$telegram_token"
  CONFIGURED_GATEWAY="$gateway"
  echo "Wrote config to $config_file"
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
esac

need_command curl
need_command uname
need_command mktemp
need_command chmod
need_command mkdir
need_command mv
need_command sed
need_command dirname

repo_url="${CODRIK_REPO_URL:-$DEFAULT_REPO_URL}"
version="${CODRIK_VERSION:-latest}"
target="${CODRIK_TARGET:-$(detect_target)}"
install_dir="${CODRIK_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
config_dir="${CODRIK_CONFIG_DIR:-$DEFAULT_CONFIG_DIR}"
runtime_dir="${CODRIK_HOME:-$config_dir}"

[ -n "$install_dir" ] || die "could not determine install directory; set CODRIK_INSTALL_DIR"

if [ "$version" = "latest" ]; then
  tag="$(resolve_latest_tag "$repo_url")"
else
  tag="$(normalize_tag "$version")"
fi

asset="${BIN_NAME}-${tag}-${target}"
download_url="${repo_url}/releases/download/${tag}/${asset}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

echo "Downloading $asset"
curl -fsSLo "$tmp_dir/$asset" "$download_url" \
  || die "could not download $download_url"
curl -fsSLo "$tmp_dir/$asset.sha256" "$download_url.sha256" \
  || die "could not download $download_url.sha256"

(
  cd "$tmp_dir"
  verify_checksum "$asset.sha256"
)

chmod 755 "$tmp_dir/$asset"
mkdir -p "$install_dir"
mv "$tmp_dir/$asset" "$install_dir/$BIN_NAME"

echo "Installed $BIN_NAME $tag to $install_dir/$BIN_NAME"

if [ "${CODRIK_SKIP_CONFIG:-0}" != "1" ]; then
  configure_codrik "$config_dir"
fi

if [ "${CODRIK_SKIP_SERVICE:-0}" != "1" ] && [ "$CONFIGURED_GATEWAY" != "none" ]; then
  if is_interactive && ask_yes_no "Install and start $CONFIGURED_GATEWAY gateway service?" "y"; then
    install_gateway_service "$CONFIGURED_GATEWAY" "$install_dir/$BIN_NAME" "$CONFIGURED_CONFIG_FILE" "$runtime_dir"
  fi
fi

case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) echo "Add $install_dir to PATH to run $BIN_NAME from any directory." >&2 ;;
esac
