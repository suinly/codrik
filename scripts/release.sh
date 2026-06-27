#!/usr/bin/env bash
set -euo pipefail

BIN_NAME="codrik"
MAC_TARGET="aarch64-apple-darwin"
MAC_X64_TARGET="x86_64-apple-darwin"
PI_TARGET="aarch64-unknown-linux-gnu"
LINUX_X64_TARGET="x86_64-unknown-linux-gnu"
DIST_DIR="dist"
INSTALL_URL="https://raw.githubusercontent.com/suinly/codrik/main/scripts/install.sh"
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
  GH_REPO  Optional GitHub repository slug, passed as: --repo <value>

Required tools:
  git, rustup, cargo, cargo-zigbuild, zig, gh, perl, shasum
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

commit_lines_matching() {
  local range="$1"
  local pattern="$2"

  git log --no-merges --format='%s' "$range" \
    | grep -E "$pattern" \
    | sed -E 's/^/- /' \
    || true
}

write_commit_section() {
  local note_file="$1"
  local title="$2"
  local range="$3"
  local pattern="$4"
  local lines

  lines="$(commit_lines_matching "$range" "$pattern")"
  if [[ -n "$lines" ]]; then
    {
      printf '\n## %s\n\n' "$title"
      printf '%s\n' "$lines"
    } >>"$note_file"
  fi
}

write_other_commit_section() {
  local note_file="$1"
  local range="$2"
  local lines

  lines="$(
    git log --no-merges --format='%s' "$range" \
      | grep -Ev '^(feat|fix|docs|build|ci|chore|refactor|perf|test)(\([^)]+\))?!?:' \
      | sed -E 's/^/- /' \
      || true
  )"

  if [[ -n "$lines" ]]; then
    {
      printf '\n## Other Changes\n\n'
      printf '%s\n' "$lines"
    } >>"$note_file"
  fi
}

write_closed_issues_section() {
  local note_file="$1"
  local range="$2"
  local issues

  issues="$(
    git log --format='%B' "$range" \
      | grep -Eio '(^|[[:space:]])(close[sd]?|fix(e[sd])?|resolve[sd]?)[[:space:]]+#?[0-9]+' \
      | grep -Eo '#?[0-9]+' \
      | sed -E 's/^#?/#/' \
      | sort -u \
      | sed -E 's/^/- /' \
      || true
  )"

  if [[ -n "$issues" ]]; then
    {
      printf '\n## Closed Issues\n\n'
      printf '%s\n' "$issues"
    } >>"$note_file"
  fi
}

write_release_notes() {
  local note_file="$1"
  local tag="$2"
  local range="$3"

  cat >"$note_file" <<NOTES
Release $tag
NOTES

  write_commit_section "$note_file" "Features" "$range" '^feat(\([^)]+\))?!?:'
  write_commit_section "$note_file" "Fixes" "$range" '^fix(\([^)]+\))?!?:'
  write_commit_section "$note_file" "Documentation" "$range" '^docs(\([^)]+\))?!?:'
  write_commit_section "$note_file" "Build and Release" "$range" '^(build|ci|chore)(\([^)]+\))?!?:'
  write_commit_section "$note_file" "Internal" "$range" '^(refactor|perf|test)(\([^)]+\))?!?:'
  write_other_commit_section "$note_file" "$range"
  write_closed_issues_section "$note_file" "$range"

  cat >>"$note_file" <<NOTES

## Install

\`\`\`sh
curl -fsSL $INSTALL_URL | sh
\`\`\`

## Compatibility

- macOS Apple Silicon
- macOS Intel
- Linux x86_64
- Raspberry Pi 5

## Checksums

Checksums are published as \`.sha256\` files next to each binary.
NOTES
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
need_command gh
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

PREVIOUS_TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"
if [[ -n "$PREVIOUS_TAG" ]]; then
  CHANGELOG_RANGE="$PREVIOUS_TAG..HEAD"
else
  CHANGELOG_RANGE="HEAD"
fi

CURRENT_VERSION="$(perl -0ne 'if (/\[package\][\s\S]*?\nversion = "([^"]+)"/) { print $1; exit }' Cargo.toml)"
[[ -n "$CURRENT_VERSION" ]] || die "could not read package version from Cargo.toml"
[[ "$CURRENT_VERSION" != "$VERSION" ]] || die "Cargo.toml already has version $VERSION"

rustup target add "$MAC_TARGET" "$MAC_X64_TARGET" "$PI_TARGET" "$LINUX_X64_TARGET"

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

echo "Building $BIN_NAME for macOS Intel ($MAC_X64_TARGET)"
cargo build --release --target "$MAC_X64_TARGET"

echo "Building $BIN_NAME for Raspberry Pi 5 ($PI_TARGET)"
cargo zigbuild --release --target "$PI_TARGET"

echo "Building $BIN_NAME for Linux x86_64 ($LINUX_X64_TARGET)"
cargo zigbuild --release --target "$LINUX_X64_TARGET"

MAC_ASSET="$DIST_DIR/$TAG/$BIN_NAME-$TAG-$MAC_TARGET"
MAC_X64_ASSET="$DIST_DIR/$TAG/$BIN_NAME-$TAG-$MAC_X64_TARGET"
PI_ASSET="$DIST_DIR/$TAG/$BIN_NAME-$TAG-raspberry-pi-5-$PI_TARGET"
LINUX_X64_ASSET="$DIST_DIR/$TAG/$BIN_NAME-$TAG-$LINUX_X64_TARGET"

cp "target/$MAC_TARGET/release/$BIN_NAME" "$MAC_ASSET"
cp "target/$MAC_X64_TARGET/release/$BIN_NAME" "$MAC_X64_ASSET"
cp "target/$PI_TARGET/release/$BIN_NAME" "$PI_ASSET"
cp "target/$LINUX_X64_TARGET/release/$BIN_NAME" "$LINUX_X64_ASSET"
chmod 755 "$MAC_ASSET" "$MAC_X64_ASSET" "$PI_ASSET" "$LINUX_X64_ASSET"

(
  cd "$DIST_DIR/$TAG"
  shasum -a 256 "$(basename "$MAC_ASSET")" >"$(basename "$MAC_ASSET").sha256"
  shasum -a 256 "$(basename "$MAC_X64_ASSET")" >"$(basename "$MAC_X64_ASSET").sha256"
  shasum -a 256 "$(basename "$PI_ASSET")" >"$(basename "$PI_ASSET").sha256"
  shasum -a 256 "$(basename "$LINUX_X64_ASSET")" >"$(basename "$LINUX_X64_ASSET").sha256"
)

NOTE_FILE="$DIST_DIR/$TAG/release-notes.md"
write_release_notes "$NOTE_FILE" "$TAG" "$CHANGELOG_RANGE"

echo "Creating git tag $TAG"
git add Cargo.toml Cargo.lock
git commit -m "chore(release): $TAG"
RELEASE_COMMIT_CREATED=1
git tag -a "$TAG" -m "Release $TAG"

echo "Pushing release commit and git tag $TAG to origin"
git push origin HEAD
git push origin "$TAG"

GH_ARGS=()
if [[ -n "${GH_REPO:-}" ]]; then
  GH_ARGS+=(--repo "$GH_REPO")
fi

echo "Creating GitHub release $TAG"
gh release create "$TAG" ${GH_ARGS+"${GH_ARGS[@]}"} \
  --title "$TAG" \
  --notes-file "$NOTE_FILE" \
  "$MAC_ASSET" \
  "$MAC_ASSET.sha256" \
  "$MAC_X64_ASSET" \
  "$MAC_X64_ASSET.sha256" \
  "$PI_ASSET" \
  "$PI_ASSET.sha256" \
  "$LINUX_X64_ASSET" \
  "$LINUX_X64_ASSET.sha256"

echo "Release $TAG created"
