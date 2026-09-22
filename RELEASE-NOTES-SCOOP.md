ripgrep-all community build 0.10.10.3

A Windows/Linux/macOS build of [ripgrep-all](https://github.com/phiresky/ripgrep-all) based on upstream master (after v0.10.9) with additional search capability and performance work. This is not an official rga release. The Windows binaries are built from source with a statically linked C runtime, so no Visual C++ redistributable is needed.

## New in 0.10.10.3

- **Whitespace-only adapter bail-out.** Adapters whose entire output is whitespace (e.g. OCR-style form feeds from scanned PDFs with no text layer) are now discarded instead of flooding results with blank lines. This is the groundwork discussed in upstream issue #3.
- **Clustered `-a` detection for `rg -a/--text/--binary`.** Passing `-ai` (or any cluster of boolean short flags containing `a`) now correctly enables binary passthrough, while value-taking clusters like `-ta` (type filter) are not misread. The cache key now also includes the text flag, so toggling it invalidates cached results.
- **Linux x86-64 Zen 4 (znver4) binary.** New `ripgrep_all-<version>-x86_64-unknown-linux-gnu-znver4.tar.gz` release asset, built with the same recipe as the Arch/CachyOS `ripgrep-all-znver4` pacman package (`-C target-cpu=znver4`, fat LTO, single codegen unit). Requires a Zen 4+ (or Intel equivalent AVX-512) CPU.
- **Windows x86-64 Zen 4 (znver4) binary.** New `ripgrep_all-<version>-x86_64-pc-windows-msvc-znver4.zip` release asset, same tuning; requires a Zen 4+ CPU. Available in scoop as `ripgrep-all-znver4` from the scoop-alts bucket.
- **Arch / CachyOS pacman packages** for the generic x86-64 and znver4 variants, built from `packaging/arch` (`PKGBUILD`, `PKGBUILD-znver4`, `build-packages.sh`).
- Linux arm64 (`aarch64-unknown-linux-gnu`) binary continues to be shipped.

## Previously in 0.10.10.2

Ten upstream corrections plus community features:

- fix(adapters): force LF line endings from pdftotext and pandoc on Windows (#352)
- fix(sqlite): open databases with immutable=1 to avoid -wal/-shm side effects (#287)
- fix(pandoc): map file extensions through pandoc format aliases for --from= (#205)
- fix(ffmpeg): strip LRC timestamps from lyrics metadata tags (#293)
- fix(rga-fzf): report correct error when fzf prints no output (#264)
- fix(matching): fall back to ZIP magic bytes when mime detection fails (#214)
- docs(readme): require Rust 1.85+ for compilation from source (#342)
- feat: honor rg's -a/--text/--binary flag for binary detection (#70)
- feat: custom adapters with the same name as a built-in now override it (#232)
- fix(rga-fzf): file names starting with '-' broke the preview command (#261)

## Requirements

Same as upstream: ripgrep in PATH, plus poppler (pdf), pandoc (docx, odt, epub, rtf, ...), ffmpeg (subtitles, metadata), and sqlite handled internally. antiword is needed for legacy .doc. Run `rga --rga-doctor` to check your setup.

Source: https://github.com/danalec/ripgrep-all (branch `main`; `master` mirrors upstream), based on phiresky/ripgrep-all master (v0.10.9+) plus the changes listed above.
