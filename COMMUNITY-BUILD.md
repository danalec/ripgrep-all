ripgrep-all community build 0.10.10.1

A Windows build of [ripgrep-all](https://github.com/phiresky/ripgrep-all) based on upstream master (after v0.10.9) with additional search capability and performance work. This is not an official rga release. The binaries are built from source with a statically linked C runtime, so no Visual C++ redistributable is needed.

## New

- **Search legacy Word 97-2003 .doc files.** A new antiword adapter extracts text from old OLE2 .doc files, both standalone and inside archives (members are spooled to a temporary file since antiword only reads seekable paths). Matched by the .doc extension or by the application/msword mime type with `--rga-accurate`. Requires antiword.
- **Search .rtf files.** The pandoc adapter now also converts RTF to plain text. Requires pandoc.

## Faster

- **Line prefixing is about 35% faster** on newline-dense text (about 1.25 to 1.7 GiB/s in the bench-postproc workload). The prefixer now does a single memchr-guided pass per chunk with an exactly-sized output buffer, instead of two regex replacement passes, and chunks without any line break are forwarded without copying. Behavior is unchanged, including chunk-local CRLF handling.

## Also included from the community branch

- Persistent cache daemon (rga --daemon) with password support for encrypted archives
- Tesseract OCR adapter for images (opt-in)
- rga-doctor and cache clear/prune tools
- Unified ffmpeg streaming and configurable zip/ffmpeg extensions
- Cache keys include config hash and file mtime

## Requirements

Same as upstream: ripgrep in PATH, plus poppler (pdf), pandoc (docx, odt, epub, rtf, ...), ffmpeg (subtitles, metadata), and sqlite handled internally. antiword is needed for legacy .doc. Run `rga --rga-doctor` to check your setup.

Source: https://github.com/danalec/ripgrep-all (branch `scoop-alts`), based on phiresky/ripgrep-all master (v0.10.9+) plus the changes listed above.
