#!/usr/bin/env bash
# Build a PGO-tuned rga from the current checkout.
#
# Usage: build-pgo.sh [target] [output-dir] [version]
#   target     Rust target triple (default: x86_64-unknown-linux-gnu)
#   output-dir Where to put the final archive (default: ./dist-pgo)
#   version    Version string for the archive name (default: Cargo.toml
#              version; pass the release tag name in CI)
#
# Steps: instrumented build -> train on the bench corpus (preprocess every
# adapter file cold + run a real search) -> llvm-profdata merge -> final
# release build with -Cprofile-use. Requires: stable rust with the
# llvm-tools-preview component (added automatically), and the adapter tools
# (poppler/pandoc) on PATH for training to exercise those code paths.
set -euo pipefail

TARGET="${1:-x86_64-unknown-linux-gnu}"
OUT="${2:-$PWD/dist-pkg-pgo}"
VERSION="${3:-$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)}"

HOST="$(rustc -vV | sed -n 's/^host: //p')"
rustup component add llvm-tools-preview >/dev/null 2>&1 || true
LLVM_PROFDATA="$(rustc --print sysroot)/lib/rustlib/$HOST/bin/llvm-profdata"
# MSYS appends .exe on exec, but the -x check needs the exact name
[ -x "$LLVM_PROFDATA" ] || [ -x "$LLVM_PROFDATA.exe" ] \
  || { echo "llvm-profdata not found (install llvm-tools-preview)" >&2; exit 1; }

if [ ! -d benches/corpus ]; then
  echo "generating training corpus..."
  python3 benches/gen_corpus.py
fi

PROF_DIR="$(mktemp -d)"
TRAIN_CACHE="$(mktemp -d)"
trap 'rm -rf "$PROF_DIR" "$TRAIN_CACHE"' EXIT

echo "==> [1/4] instrumented build ($TARGET)"
RUSTFLAGS="-Cprofile-generate=$PROF_DIR" \
  cargo build --release --locked --target "$TARGET"

BIN="./target/$TARGET/release"
[ "$TARGET" = "$HOST" ] || true

echo "==> [2/4] training run over the corpus"
# preprocess every adapter-backed file with a cold cache: exercises adapter
# detection, decompression, sqlite cache writes, prefix postprocessing
find benches/corpus -maxdepth 1 -type f \
  \( -name '*.pdf' -o -name '*.docx' -o -name '*.zip' -o -name '*.tar.gz' -o -name '*.sqlite' \) |
  while read -r f; do
    "$BIN/rga-preproc" "--rga-cache-path=$TRAIN_CACHE" "$f" >/dev/null
  done
# exercise the search path itself (plain text + inside archives)
"$BIN/rga" --rga-no-cache "the quick brown" benches/corpus >/dev/null || true

echo "==> [3/4] merging profile data"
"$LLVM_PROFDATA" merge -o "$PROF_DIR/merged.profdata" "$PROF_DIR"

echo "==> [4/4] final PGO build"
RUSTFLAGS="-Cprofile-use=$PROF_DIR/merged.profdata -Cllvm-args=-pgo-warn-missing-function" \
  cargo build --release --locked --target "$TARGET"

echo "==> packaging"
mkdir -p "$OUT"
PKG="ripgrep_all-v$VERSION-$TARGET-pgo"
ARCHIVE="$OUT/$PKG.tar.gz"
rm -rf "/tmp/$PKG" && mkdir -p "/tmp/$PKG"
for b in rga rga-preproc rga-fzf rga-fzf-open; do
  [ -f "$BIN/$b" ] && cp "$BIN/$b" "/tmp/$PKG/"
done
cp LICENSE.md "/tmp/$PKG/" 2>/dev/null || true
tar -C /tmp -czf "$ARCHIVE" "$PKG"
rm -rf "/tmp/$PKG"
echo "==> done: $ARCHIVE"
