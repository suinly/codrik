#!/usr/bin/env bash
set -euo pipefail

BIN_NAME="codrik-rs"
MAC_TARGET="aarch64-apple-darwin"
PI_TARGET="aarch64-unknown-linux-gnu"
DIST_DIR="dist"
VERSION_FILES_UPDATED=0
RELEASE_COMMIT_CREATED=0

cleanup_on_error() {
  local status=$?
  if [[ "$status" -ne 0 && "$VERSION_FILES_UPDATED" == "1" && "$RELEASE_COMMIT_CREATED" == "0" ]]; then
    echo "Release failed before commit; restoring Cargo.toml and Cargo.lock" >&2
    git restore -- Cargo.toml Cargo.lock
    rm -rf "$DIST_DIR/$TAG"
  fi
  exit "$status"
}

trap cleanup_on_error EXIT

usage() {
  cat <<USAGE
Usage: $0 <version>

Examples:
  $0 0.2.0
  $0 v0.2.0

Environment:
  TEA_LOGIN  Optional tea login name, passed as: --login <value>
  TEA_REPO   Optional repository slug, passed as: --repo <value>

Required tools:
  git, rustup, cargo, cargo-zigbuild, zig, tea, perl, shasum
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

VERSION="${1:-}"
[[ -n "$VERSION" ]] || {
  usage >&2
  exit 1
}

VERSION="${VERSION#v}"
[[ "$VERSION" =~ ^[0-9]+[.][0-9]+[.][0-9]+([-+][0-9A-Za-z.-]+)?$ ]] \
  || die "version must look like 1.2.3 or v1.2.3"

TAG="v$VERSION"

need_command git
need_command rustup
need_command cargo
command -v zig >/dev/null 2>&1 \
  || die "missing required command: zig; install it with: brew install zig"
need_command tea
need_command perl
need_command shasum

cargo zigbuild --help >/dev/null 2>&1 \
  || die "missing cargo-zigbuild; install it with: cargo install cargo-zigbuild"

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not inside a git repository"

if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is not clean; commit or stash changes before tagging a release"
fi

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  die "local tag already exists: $TAG"
fi

if git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1; then
  die "remote tag already exists on origin: $TAG"
fi

CURRENT_VERSION="$(perl -0ne 'if (/\[package\][\s\S]*?\nversion = "([^"]+)"/) { print $1; exit }' Cargo.toml)"
[[ -n "$CURRENT_VERSION" ]] || die "could not read package version from Cargo.toml"
[[ "$CURRENT_VERSION" != "$VERSION" ]] || die "Cargo.toml already has version $VERSION"

rustup target add "$MAC_TARGET" "$PI_TARGET"

echo "Updating Cargo.toml package version from $CURRENT_VERSION to $VERSION"
VERSION="$VERSION" perl -0pi -e \
  's/(\[package\][\s\S]*?\nversion = ")[^"]+(")/$1$ENV{VERSION}$2/' \
  Cargo.toml
VERSION_FILES_UPDATED=1

echo "Updating Cargo.lock package version"
cargo check

rm -rf "$DIST_DIR/$TAG"
mkdir -p "$DIST_DIR/$TAG"

echo "Building $BIN_NAME for macOS Apple Silicon ($MAC_TARGET)"
cargo build --release --target "$MAC_TARGET"

echo "Building $BIN_NAME for Raspberry Pi 5 ($PI_TARGET)"
cargo zigbuild --release --target "$PI_TARGET"

MAC_ASSET="$DIST_DIR/$TAG/$BIN_NAME-$TAG-$MAC_TARGET"
PI_ASSET="$DIST_DIR/$TAG/$BIN_NAME-$TAG-raspberry-pi-5-$PI_TARGET"

cp "target/$MAC_TARGET/release/$BIN_NAME" "$MAC_ASSET"
cp "target/$PI_TARGET/release/$BIN_NAME" "$PI_ASSET"
chmod 755 "$MAC_ASSET" "$PI_ASSET"

(
  cd "$DIST_DIR/$TAG"
  shasum -a 256 "$(basename "$MAC_ASSET")" >"$(basename "$MAC_ASSET").sha256"
  shasum -a 256 "$(basename "$PI_ASSET")" >"$(basename "$PI_ASSET").sha256"
)

NOTE_FILE="$DIST_DIR/$TAG/release-notes.md"
cat >"$NOTE_FILE" <<NOTES
Release $TAG

Assets:
- $BIN_NAME-$TAG-$MAC_TARGET
- $BIN_NAME-$TAG-raspberry-pi-5-$PI_TARGET
NOTES

echo "Creating git tag $TAG"
git add Cargo.toml Cargo.lock
git commit -m "chore(release): $TAG"
RELEASE_COMMIT_CREATED=1
git tag -a "$TAG" -m "Release $TAG"

echo "Pushing release commit and git tag $TAG to origin"
git push origin HEAD
git push origin "$TAG"

TEA_ARGS=()
if [[ -n "${TEA_LOGIN:-}" ]]; then
  TEA_ARGS+=(--login "$TEA_LOGIN")
fi
if [[ -n "${TEA_REPO:-}" ]]; then
  TEA_ARGS+=(--repo "$TEA_REPO")
fi

echo "Creating Forgejo release $TAG"
tea releases create ${TEA_ARGS+"${TEA_ARGS[@]}"} \
  --tag "$TAG" \
  --title "$TAG" \
  --note-file "$NOTE_FILE" \
  --asset "$MAC_ASSET" \
  --asset "$MAC_ASSET.sha256" \
  --asset "$PI_ASSET" \
  --asset "$PI_ASSET.sha256"

echo "Release $TAG created"
