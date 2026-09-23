# ripgrep-all — this fork

`rga` (ripgrep-all) searches through everything — PDFs, Office documents, archives, media metadata, 3D models, ML model files — by plugging text-extraction adapters into ripgrep.

This fork exists to move faster than upstream ([phiresky/ripgrep-all](https://github.com/phiresky/ripgrep-all)): new adapters, new compression formats, structured output, and robustness fixes land here first. **Source of truth is our Forgejo** (`forgejo/main`); the GitHub repo is a mirror.

Status markers: ✅ shipped on `main` (latest release: **v0.10.10.5**) · 🚧 in a branch PR, merge pending.

## Quick start

```sh
cargo build --release
# rga + rga-preproc land in target/release
rga "needle" ~/Documents
```

Optional adapters need their tools on PATH (`rga --rga-doctor` checks): `pandoc`, `pdftotext`, `ffmpeg`, `tesseract`, and the opt-in ones below.

## What's new vs upstream

### Archives & compression ✅ / 🚧

| Feature | Status | Notes |
| --- | --- | --- |
| 7z / cb7 | ✅ | native (`sevenz-rust2`), recurses into nested archives like zip/tar |
| zip magic-byte fallback | ✅ | extension-less zips still detected (#214) |
| cpio (newc) | 🚧 `feat/wave3c-cpio` | native parser, regular files only |
| pax / zipx / cbz | 🚧 `feat/wave3a-quickwins` | pax is POSIX tar; zipx/cbz are zips |
| brotli `.br`, lz4 `.lzma`, compress `.Z` | 🚧 `feat/wave3b-decompress` | `lz4_flex`, `liblzma`, `newtua-lzw-z` — all pure/light, no FFI |
| suffix aliases `taz/tpz/tb2/tz2/txz/tlz/tzst/tgz…` | 🚧 `feat/wave3b-decompress` | `foo.tar.xz` shorts resolve to `.tar` inside |
| multi-member gzip | 🚧 `feat/wave3b-decompress` | concatenated `gz` streams search as one file |
| zip compression methods | ✅ | stored/deflate/deflate64/bzip2/lzma/xz/zstd round-trip tested |

### Native adapters (no external tools) ✅

- **3D & point clouds**: `glb` (JSON scene), `stl`, `ply`, `fbx` (binary node tree), `pcd`, `las`/`laz` (lidar headers + bounding boxes), `vtk`
- **Geodata**: `shp` (shapefile index/bbox), `dbf` (dBase/FoxPro records as TSV — the attribute side of shapefiles), GeoTIFF tags + geokeys
- **Science & data**: `fits` (astronomy header cards), `parquet` (footer schema), `sqlite`/`db` (WAL-safe), `npy`
- **ML/LLM model files**: `gguf` (Llama.cpp etc.), `safetensors`, `onnx`/`pb` (protobuf string walk), `tiktoken`
- **Mail**: `mbox`/`eml`

### Media & documents 🚧

| Adapter | Branch | Formats |
| --- | --- | --- |
| JPEG EXIF + GPS, audio tags, DICOM | `feat/media-adapters` | jpg/jpeg, mp3/flac/ogg/opus/wav, dcm — pure Rust, header-only, fast |
| exiftool (opt-in) | `feat/wave3d-spawning` | `--rga-adapters=+exiftool`: image metadata, reads stdin (works inside archives) |
| soffice (opt-in) | `feat/wave3d-spawning` | `--rga-adapters=+soffice`: xlsx/pptx/ppt/ods/odp → text (LibreOffice 7.4+) |
| openssl (opt-in) | `feat/wave3a-quickwins` | pem/crt/cer/der certificates as text |
| xls2csv, tesseract (opt-in) | ✅ | legacy `.xls`, OCR — `--rga-adapters=+xls2csv,+tesseract` |

Existing spawning adapters are improved: pandoc 5 (format heuristics, LF output), poppler 2 (PDF passwords via `--rga-password`, CRLF-safe), antiword for legacy `.doc`.

### Search output & filtering 🚧 `feat/wave4-output`

Wrapper modes (force `rg --json`, re-render; colors are lost, documented):

- `--rga-format=csv|xml|'TEMPLATE'` — one record per match; `%f` path, `%n` line, `%c` column, `%m` match, `%d` line; `\n`/`\t` escapes
- `--rga-and=PAT` / `--rga-not=PAT` — boolean line filters (ugrep `--and`/`--not`)
- `--rga-replace=TEMPLATE` — `rg --replace` with `%fields` instead of `$1`
- `--rga-max-files=N` — stop printing after N files with matches

Exit codes follow rg's contract (1 = nothing found, even when filters drop everything).

### Robustness

- **MS Office lock files** (`~$doc.docx`) no longer explode searches — excluded from preprocessing, skipped in archives 🚧 `fix/151-office-lock-files`
- **Adapter failures degrade to a searchable marker** `[rga: preprocessing failed: …]` instead of aborting the search with exit code 2 — `rga "preprocessing failed"` finds every broken file 🚧 same branch
- Bail-on-empty: an adapter that produces nothing (e.g. pdftotext on an image-only PDF) steps aside so the next one (e.g. OCR) can try ✅
- Corrupt archives are still rejected loudly (adapter contract, CRC-verified)

### UX & infrastructure

- `--rga-complete=<shell>` and `--rga-manpage` (clap_complete/clap_mangen) 🚧 `feat/wave3a-quickwins`
- `--rga-save-config` — dump the merged config to jsonc 🚧 same
- `config.jsonc` **with JSON schema** (`--rga-print-config-schema`) ✅
- Persistent cache daemon (TCP) — shared preprocessing cache across runs ✅
- `rga-fzf` / `rga-fzf-open` — interactive search ✅
- UTF-16/32 BOM sniffing ✅
- `--rga-accurate` — magic-byte mime detection in the first 8 KiB ✅

## Ugrep parity

`docs/PARITY-UGREP.md` tracks this fork against ugrep 4.x feature-by-feature (compression formats, archive handling, `ugrep+` document filters, output modes). Verdict so far: the fork covers everything `ugrep+` does by default and most of what it does with flags, and is far ahead in adapters (3D, ML models, geodata — areas no other grep-family tool touches).

## Branch & merge status

| Branch | Content | Base |
| --- | --- | --- |
| `main` | v0.10.10.5: wave-2 adapters + 7z | — |
| `feat/media-adapters` | EXIF / audio tags / DICOM | main |
| `feat/wave3a-quickwins` | extensions, openssl, completions, manpage, save-config | main |
| `feat/wave3b-decompress` | brotli/lz4/lzma/.Z, aliases, multi-gzip | main |
| `feat/wave3c-cpio` | cpio adapter, zip method parity tests | main |
| `feat/wave3d-spawning` | exiftool/soffice opt-in builtins | main |
| `feat/wave4-output` | csv/xml/%fields, and/not/replace, max-files + parity doc | main |
| `fix/151-office-lock-files` | Office lock files, failure markers | main |

Branches are independent (all fork from `main`); trivial overlap: `wave-3a`/`wave-3d` both append to the builtin-adapter list, `wave-3a`/`wave-4` both add CLI flags. Merge order `media → 3a → 3b → 3c → 3d → 4 → fix/151` resolves cleanly.

## Development

```sh
cargo test --lib          # unit tests (91 on the fix branch)
cargo build --bins        # rga, rga-preproc, rga-fzf, rga-fzf-open
```

More docs: `docs/PARITY-UGREP.md` (ugrep comparison), `docs/ROADMAP-PERF-PACKAGING.md` (performance/packaging roadmap), `.rga-triage/TRIAGE.md` (upstream issue triage).

## License

AGPL-3.0-or-later, same as upstream.
