#!/usr/bin/env sh
set -eu

BIN_NAME="codrik"
DEFAULT_REPO_URL="https://gitflow.suinly.com/codrik/codrik"
if [ -n "${HOME:-}" ]; then
  DEFAULT_INSTALL_DIR="$HOME/.local/bin"
else
  DEFAULT_INSTALL_DIR=""
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
  curl -fsSL ${DEFAULT_REPO_URL}/raw/branch/main/scripts/install.sh | sh

Environment:
  CODRIK_REPO_URL      Release repository URL. Default: ${DEFAULT_REPO_URL}
  CODRIK_VERSION       Release tag or version. Default: latest
  CODRIK_INSTALL_DIR   Install directory. Default: ~/.local/bin
  CODRIK_TARGET        Override release asset target suffix.

Examples:
  CODRIK_VERSION=v0.2.0 sh scripts/install.sh
  CODRIK_INSTALL_DIR=/usr/local/bin sh scripts/install.sh
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

repo_url="${CODRIK_REPO_URL:-$DEFAULT_REPO_URL}"
version="${CODRIK_VERSION:-latest}"
target="${CODRIK_TARGET:-$(detect_target)}"
install_dir="${CODRIK_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"

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

case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) echo "Add $install_dir to PATH to run $BIN_NAME from any directory." >&2 ;;
esac
