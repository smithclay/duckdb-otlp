#!/bin/sh
# Install the duckdb-otlp CLI from a GitHub release.
#
#   curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sh
#   curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sh -s -- --version v0.7.2
#
# This script lives in site/public/ because that is what makes it a published
# URL: Astro copies site/public/ into the Pages artifact verbatim, so the file
# in this repository and the one the one-liner fetches are the same file.
#
# Exit codes follow the CLI's own contract: 0 installed, 1 the install failed,
# 2 the command line was wrong. Data-free, so everything is printed on stderr.

set -eu

REPO=smithclay/duckdb-otlp
BINARY=duckdb-otlp
PLATFORMS="linux-amd64 linux-arm64 darwin-amd64 darwin-arm64"

# Both are overridable so the test suite can point the script at a local
# release fixture instead of github.com.
BASE_URL=${DUCKDB_OTLP_BASE_URL:-https://github.com/$REPO/releases/download}
API_URL=${DUCKDB_OTLP_API_URL:-https://api.github.com/repos/$REPO/releases/latest}

tmp=
staged=
cleanup() {
  [ -n "$tmp" ] && rm -rf "$tmp"
  [ -n "$staged" ] && rm -f "$staged"
  return 0
}
trap cleanup EXIT INT TERM

say() {
  printf '%s\n' "$*" >&2
}

die() {
  printf 'install: %s\n' "$*" >&2
  exit 1
}

usage_error() {
  printf 'install: %s\n' "$*" >&2
  printf 'Run with --help for usage.\n' >&2
  exit 2
}

usage() {
  cat >&2 <<EOF
Install the duckdb-otlp CLI.

Usage:
  curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sh
  curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sh -s -- [options]

Options:
  --version <tag>     Release to install, e.g. v0.7.2 (default: the latest release)
  --bin-dir <dir>     Where to put the binary (default: \$HOME/.local/bin)
  --platform <id>     Override platform detection: $PLATFORMS
  -h, --help          Show this help

Environment (a flag always wins over its variable):
  DUCKDB_OTLP_VERSION, DUCKDB_OTLP_BIN_DIR, DUCKDB_OTLP_PLATFORM

The binary is verified against the release's SHA256SUMS before it is installed.
EOF
}

version=${DUCKDB_OTLP_VERSION:-}
bin_dir=${DUCKDB_OTLP_BIN_DIR:-}
platform=${DUCKDB_OTLP_PLATFORM:-}

need_value() {
  [ "$2" -gt 1 ] || usage_error "$1 needs a value"
}

while [ $# -gt 0 ]; do
  case $1 in
  --version)
    need_value --version $#
    version=$2
    shift
    ;;
  --version=*) version=${1#*=} ;;
  --bin-dir)
    need_value --bin-dir $#
    bin_dir=$2
    shift
    ;;
  --bin-dir=*) bin_dir=${1#*=} ;;
  --platform)
    need_value --platform $#
    platform=$2
    shift
    ;;
  --platform=*) platform=${1#*=} ;;
  -h | --help)
    usage
    exit 0
    ;;
  *) usage_error "unknown option: $1" ;;
  esac
  shift
done

[ -n "$bin_dir" ] || bin_dir=${HOME:-}/.local/bin
[ "$bin_dir" != "/.local/bin" ] || die "HOME is not set; pass --bin-dir <dir>"

if [ -n "$platform" ]; then
  case " $PLATFORMS " in
  *" $platform "*) ;;
  *) usage_error "unsupported platform: $platform (expected one of: $PLATFORMS)" ;;
  esac
else
  os=$(uname -s 2>/dev/null || echo unknown)
  arch=$(uname -m 2>/dev/null || echo unknown)
  case $os in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) die "no release binary for $os; on Windows use the Docker image, or build from source" ;;
  esac
  case $arch in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) die "no release binary for $arch (supported: $PLATFORMS)" ;;
  esac
  platform=$os-$arch
fi

if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL --retry 3 -o "$2" "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -q -O "$2" "$1"; }
else
  die "neither curl nor wget is installed"
fi

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
  die "neither sha256sum nor shasum is installed; cannot verify the download"
fi

tmp=$(mktemp -d 2>/dev/null) || die "cannot create a temporary directory"

if [ -z "$version" ]; then
  fetch "$API_URL" "$tmp/latest.json" ||
    die "cannot reach $API_URL; pass --version <tag> to skip the lookup"
  version=$(sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$tmp/latest.json" | head -n 1)
  [ -n "$version" ] || die "no tag_name in the release response from $API_URL"
fi

archive=$BINARY-$version-$platform.tar.gz
say "Installing $BINARY $version ($platform) into $bin_dir"

fetch "$BASE_URL/$version/$archive" "$tmp/$archive" ||
  die "cannot download $BASE_URL/$version/$archive (is $version a released tag?)"
fetch "$BASE_URL/$version/SHA256SUMS" "$tmp/SHA256SUMS" ||
  die "cannot download $BASE_URL/$version/SHA256SUMS"

# SHA256SUMS lines are "<digest>  <file>"; a binary-mode digest marks the name with a "*".
expected=$(awk -v want="$archive" '$2 == want || $2 == "*" want { print $1; exit }' "$tmp/SHA256SUMS")
[ -n "$expected" ] || die "SHA256SUMS has no entry for $archive"
actual=$(sha256 "$tmp/$archive")
[ "$expected" = "$actual" ] ||
  die "checksum mismatch for $archive (expected $expected, got $actual); nothing was installed"

mkdir -p "$tmp/unpacked"
tar -xzf "$tmp/$archive" -C "$tmp/unpacked" || die "cannot unpack $archive"
[ -f "$tmp/unpacked/$BINARY" ] || die "$archive does not contain $BINARY"

mkdir -p "$bin_dir" 2>/dev/null ||
  die "cannot create $bin_dir; re-run with --bin-dir <dir>, or with sudo"
[ -w "$bin_dir" ] ||
  die "cannot write to $bin_dir; re-run with --bin-dir <dir>, or with sudo"

# Stage inside the target directory and rename over the old binary: mv is atomic
# there, so a running duckdb-otlp is never left half-written (and replacing the
# file rather than writing through it avoids ETXTBSY).
staged=$bin_dir/.$BINARY.$$
cp "$tmp/unpacked/$BINARY" "$staged" || die "cannot write to $bin_dir"
chmod 0755 "$staged"
mv -f "$staged" "$bin_dir/$BINARY" || {
  rm -f "$staged"
  die "cannot replace $bin_dir/$BINARY"
}
staged=

# Run it once: a binary that cannot start here (a glibc too old for the release
# build, say) is better reported now than at the user's first command. It is not
# fatal, because --platform can name a machine other than this one.
if installed=$("$bin_dir/$BINARY" --version 2>/dev/null); then
  say "Installed $installed at $bin_dir/$BINARY"
else
  say "Installed $bin_dir/$BINARY, but it did not run here"
  say "(expected when --platform names another machine; otherwise check $platform is right)"
fi
case ":${PATH:-}:" in
*":$bin_dir:"*) ;;
*)
  say ""
  say "$bin_dir is not on your PATH. Add it with:"
  say "    export PATH=\"$bin_dir:\$PATH\""
  ;;
esac
say ""
say "Next: $BINARY serve   (OTLP/HTTP on 127.0.0.1:4318, OTLP/gRPC on 127.0.0.1:4317)"
