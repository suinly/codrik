#!/usr/bin/env sh
set -eu

BIN_NAME="codrik"
DEFAULT_REPO_URL="https://github.com/suinly/codrik"
DEFAULT_BASE_URL="https://api.openai.com/v1"
DEFAULT_MODEL="gpt-4.1-mini"
CONFIGURED_CONFIG_FILE=""
CONFIGURED_RUNTIME_READY=0
CLEAN_INTERACTIVE_INSTALL=0
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
  CODRIK_SKIP_SERVICE  Set to 1 to skip foreground service setup.

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
  actor_id="$5"

  umask 077
  {
    printf 'api_key: "%s"\n' "$(yaml_escape "$api_key")"
    printf 'base_url: "%s"\n' "$(yaml_escape "$base_url")"
    printf 'model: "%s"\n' "$(yaml_escape "$model")"
    printf 'runtime:\n'
    if [ "$actor_id" = "actor:local:owner" ]; then
      printf '  actor_id: actor:local:owner\n'
    else
      printf '  actor_id: "%s"\n' "$(yaml_escape "$actor_id")"
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
  runtime_dir="$4"

  mkdir -p "$(dirname "$service_file")"
  cat >"$service_file" <<SERVICE
[Unit]
Description=Codrik foreground runtime
After=network-online.target

[Service]
Type=simple
Environment=CODRIK_CONFIG=$config_file
Environment=CODRIK_HOME=$runtime_dir
ExecStart=$bin_path serve
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
  runtime_dir="$4"
  label="com.suinly.codrik"

  mkdir -p "$(dirname "$plist_file")"
  cat >"$plist_file" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.suinly.codrik</string>
  <key>ProgramArguments</key>
  <array>
    <string>$(xml_escape "$bin_path")</string>
    <string>serve</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>CODRIK_CONFIG</key>
    <string>$(xml_escape "$config_file")</string>
    <key>CODRIK_HOME</key>
    <string>$(xml_escape "$runtime_dir")</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$(xml_escape "$runtime_dir/codrik.log")</string>
  <key>StandardErrorPath</key>
  <string>$(xml_escape "$runtime_dir/codrik.err.log")</string>
</dict>
</plist>
PLIST
}

print_service_management_hint() {
  os="$1"

  case "$os" in
    Linux)
      cat <<HINT
Manage the Codrik service with:
  systemctl --user status codrik.service
  systemctl --user restart codrik.service
  journalctl --user -u codrik.service -f

This is a user service, so do not use sudo, service, or systemctl without --user.
To start it at boot without logging in, run:
  loginctl enable-linger $(id -un 2>/dev/null || printf '%s' "\$USER")
HINT
      ;;
    Darwin)
      cat <<HINT
Manage the Codrik service with:
  launchctl print gui/$(id -u)/com.suinly.codrik
  launchctl kickstart -k gui/$(id -u)/com.suinly.codrik

Logs:
  ~/.codrik/codrik.log
  ~/.codrik/codrik.err.log
HINT
      ;;
  esac
}

enable_user_linger() {
  user_name="$(id -un 2>/dev/null || printf '%s' "${USER:-}")"

  if [ -z "$user_name" ]; then
    echo "Could not determine current user; run loginctl enable-linger manually." >&2
    return
  fi

  if ! command -v loginctl >/dev/null 2>&1; then
    echo "loginctl is not available; user service may stop after logout." >&2
    return
  fi

  if loginctl enable-linger "$user_name"; then
    echo "Enabled lingering for $user_name so Codrik can run after logout."
  else
    cat >&2 <<HINT
Could not enable lingering automatically.
The Codrik service may stop after logout. Run:
  loginctl enable-linger $user_name
or, if your system requires elevated privileges:
  sudo loginctl enable-linger $user_name
HINT
  fi
}

install_serve_service() {
  bin_path="$1"
  config_file="$2"
  runtime_dir="$3"
  os="$(uname -s)"

  mkdir -p "$runtime_dir"
  chmod 700 "$runtime_dir"

  case "$os" in
    Linux)
      need_command systemctl
      need_command id
      service_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
      service_file="$service_dir/codrik.service"
      systemctl --user disable --now codrik-telegram.service >/dev/null 2>&1 || true
      rm -f "$service_dir/codrik-telegram.service"
      write_systemd_user_service "$service_file" "$bin_path" "$config_file" "$runtime_dir"
      systemctl --user daemon-reload
      systemctl --user enable --now codrik.service
      enable_user_linger
      echo "Started user service codrik.service"
      print_service_management_hint "$os"
      ;;
    Darwin)
      need_command launchctl
      need_command id
      launch_dir="$HOME/Library/LaunchAgents"
      old_plist="$launch_dir/com.suinly.codrik.telegram.plist"
      plist_file="$launch_dir/com.suinly.codrik.plist"
      label="com.suinly.codrik"
      launchctl bootout "gui/$(id -u)" "$old_plist" >/dev/null 2>&1 || true
      rm -f "$old_plist"
      write_launchd_service "$plist_file" "$bin_path" "$config_file" "$runtime_dir"
      launchctl bootout "gui/$(id -u)" "$plist_file" >/dev/null 2>&1 || true
      launchctl bootstrap "gui/$(id -u)" "$plist_file"
      launchctl enable "gui/$(id -u)/$label"
      launchctl kickstart -k "gui/$(id -u)/$label"
      echo "Started LaunchAgent $label"
      print_service_management_hint "$os"
      ;;
    *)
      echo "Service setup is not supported on $os." >&2
      ;;
  esac
}

installer_validator_binary() {
  if [ -n "${CODRIK_VALIDATOR_BIN:-}" ]; then
    printf '%s\n' "$CODRIK_VALIDATOR_BIN"
  else
    printf '%s\n' "${INSTALLER_BINARY:-}"
  fi
}

installer_validate_config() {
  config_file="$1"
  validator="$(installer_validator_binary)"
  [ -n "$validator" ] && [ -x "$validator" ] || return 1
  "$validator" __installer_validate_config "$config_file"
}

capture_install_state() {
  config_dir="$1"
  runtime_dir="$2"
  installed_binary="${3:-}"
  config_file="$config_dir/config.yml"
  legacy_config_file="$config_dir/codrik.config.yml"
  service_present=0
  case "$(uname -s)" in
    Linux)
      [ -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/codrik.service" ] && service_present=1
      [ -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/codrik-telegram.service" ] && service_present=1
      ;;
    Darwin)
      [ -f "$HOME/Library/LaunchAgents/com.suinly.codrik.plist" ] && service_present=1
      [ -f "$HOME/Library/LaunchAgents/com.suinly.codrik.telegram.plist" ] && service_present=1
      ;;
  esac
  if [ ! -e "$config_dir" ] && [ ! -e "$runtime_dir" ] \
    && { [ -z "$installed_binary" ] || [ ! -e "$installed_binary" ]; } \
    && [ ! -f "$config_file" ] && [ ! -f "$legacy_config_file" ] \
    && [ "$service_present" = "0" ]; then
    CLEAN_INTERACTIVE_INSTALL=1
  else
    CLEAN_INTERACTIVE_INSTALL=0
  fi
}

print_missing_runtime_actor() {
  cat >&2 <<'YAML'
Existing config is missing runtime.actor_id. Add exactly:
runtime:
  actor_id: <existing-actor-id>
Codrik service was not started.
YAML
}

configure_codrik() {
  config_dir="$1"
  config_file="$config_dir/config.yml"
  legacy_config_file="$config_dir/codrik.config.yml"
  CONFIGURED_CONFIG_FILE="$config_file"
  CONFIGURED_RUNTIME_READY=0

  [ -n "$config_dir" ] || die "could not determine config directory; set CODRIK_CONFIG_DIR"

  if ! is_interactive; then
    echo "Skipping config setup because stdin/stdout is not interactive." >&2
    echo "Run $BIN_NAME after creating $config_file or set CODRIK_CONFIG." >&2
    return
  fi

  if ! ask_yes_no "Configure codrik now?" "y"; then
    return
  fi

  if [ ! -f "$config_file" ] && [ -f "$legacy_config_file" ]; then
    mv "$legacy_config_file" "$config_file"
    echo "Renamed existing config to $config_file"
  fi

  if [ -f "$config_file" ] && ! ask_yes_no "$config_file already exists. Overwrite it?" "n"; then
    echo "Keeping existing config: $config_file"
    if actor_id="$(installer_validate_config "$config_file" 2>/dev/null)"; then
      CONFIGURED_RUNTIME_READY=1
    else
      print_missing_runtime_actor
      echo "Config failed production validation; fix it before starting Codrik." >&2
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
  actor_id="actor:local:owner"

  mkdir -p "$config_dir"
  write_config "$config_file" "$api_key" "$base_url" "$model" "$actor_id"
  CONFIGURED_RUNTIME_READY=1
  echo "Wrote config to $config_file"
}

maybe_install_serve_service() {
  bin_path="$1"
  if [ "${CODRIK_SKIP_SERVICE:-0}" != "1" ] && [ "$CONFIGURED_RUNTIME_READY" = "1" ]; then
    if is_interactive && ask_yes_no "Install and start the Codrik service?" "y"; then
      install_serve_service "$bin_path" "$CONFIGURED_CONFIG_FILE" "$runtime_dir"
    fi
  fi
}

if [ "${CODRIK_INSTALL_LIBRARY_ONLY:-0}" = "1" ]; then
  return 0 2>/dev/null || exit 0
fi

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
need_command rm

repo_url="${CODRIK_REPO_URL:-$DEFAULT_REPO_URL}"
version="${CODRIK_VERSION:-latest}"
target="${CODRIK_TARGET:-$(detect_target)}"
install_dir="${CODRIK_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
config_dir="${CODRIK_CONFIG_DIR:-$DEFAULT_CONFIG_DIR}"
runtime_dir="${CODRIK_HOME:-$config_dir}"

capture_install_state "$config_dir" "$runtime_dir" "$install_dir/$BIN_NAME"

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
INSTALLER_BINARY="$install_dir/$BIN_NAME"

echo "Installed $BIN_NAME $tag to $install_dir/$BIN_NAME"

if [ "${CODRIK_SKIP_CONFIG:-0}" != "1" ]; then
  configure_codrik "$config_dir"
fi

maybe_install_serve_service "$install_dir/$BIN_NAME"

case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) echo "Add $install_dir to PATH to run $BIN_NAME from any directory." >&2 ;;
esac
