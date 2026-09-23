# Benchmarks

Preprocess throughput benchmarks over a deterministic, generated corpus.
Numbers reflect the real `rga-preproc` binary end to end: adapter detection,
preprocessing, prefix postprocessing, and (warm group) cache hits.

## Quick start

```sh
# once per machine / after cloning
python benches/gen_corpus.py        # needs reportlab + python-docx

# run everything
cargo bench --bench preproc

# quick smoke run
cargo bench --bench preproc -- --warm-up-time 1 --measurement-time 2 pdf_text
```

## What is measured

- `preproc_cold/<file>` — isolated cache dir per iteration: adapter + cache
  write. Reflects first contact with a file.
- `preproc_warm/<file>` — shared cache dir populated once: pure cache hit.
  Reflects repeated searches.

The corpus (`benches/corpus/`, gitignored) covers the hot adapters: poppler
(pdf), pandoc (docx), zip/tar.gz (flat + nested), sqlite. Plain text is
intentionally absent — rg searches it directly and `rga-preproc` never sees it.

## Adapter dependencies

The spawned `rga-preproc` finds adapter tools via `PATH`:

| adapter | tool      | install (Arch)        | install (Windows)  |
|---------|-----------|-----------------------|--------------------|
| poppler | pdftotext | `pacman -S poppler`   | `scoop install poppler` |
| pandoc  | pandoc    | `pacman -S pandoc`    | `scoop install pandoc`  |

On Windows, Git Bash's `/mingw64/bin` shadows scoop's poppler with an old
xpdf build that rga's adapter rejects. Prepend scoop's shims (or poppler's
`bin`) to `PATH` when running the benches from Git Bash:

```sh
export PATH="/c/Users/$USER/scoop/shims:$PATH"
```

Alternatively set `RGA_BENCH_PATH_PREPEND` to prepend a directory only to the
benchmark child processes:

```sh
export RGA_BENCH_PATH_PREPEND='C:\Users\you\scoop\apps\poppler\current\bin'
```

CI (`.github/workflows/bench.yml`) installs the tools with apt and needs
neither workaround.

## Rules

Every performance claim in a PR must cite numbers from this harness (median
throughput of the relevant group), per `docs/ROADMAP-PERF-PACKAGING.md`.
