# Draft — upstream PR for issue #3 ("Idea: allow adapters to bail out")

> Status: **DRAFT — do not open on GitHub yet.**
> Open only after the first wave of fixes has been merged upstream
> (in particular #352, which is a test prerequisite on Windows), to avoid
> competing for the maintainer's attention.
> PR branch: `feat/adapter-bailout` — **already prepared** on both mirrors
> (GitHub `danalec/ripgrep-all` and forgejo), with 2 atomic commits on top of
> `phiresky/ripgrep-all:master` (`8dabb3d` core + `d7e3d13` poppler,
> with the whitespace-only fix squashed in), no community build content.
> Reference implementation: `poc/adapter-bailout` (forgejo PR #1),
> 3 commits (`5b3e05d`, `72715a0`, `5ce2a7f`), **40/40 tests green** and
> e2e-validated with a real scanned PDF.

---

## Proposed title

```
feat: allow adapters to bail out and fall back to the next adapter
```

(add `closes #3` to the body, not the title, so the issue is not closed
prematurely if the PR is rejected)

## Proposed body

```markdown
Implements the idea from #3: adapters get a way to decline a file *without*
failing the whole pipeline, and the next matching adapter gets a chance.

The motivating example from #3 works end to end: when poppler (pdftotext)
extracts no text from a PDF (e.g. a scan without a text layer), the poppler
adapter bails, matching re-runs with poppler excluded, and an OCR adapter
(e.g. tesseract / OCRmyPDF) takes over instead of the user getting an empty
result. Verified with a real image-only PDF.

## How it works

- New `AdapterBail` marker error + `adapter_bail(reason)` constructor in
  `src/adapters.rs`. An adapter returns it from `adapt()` to say
  "I decline this file".
- `rga_preproc` catches the bail, re-runs adapter matching with the bailing
  adapter excluded, and retries with the next matching adapter. Since
  `AdaptInfo.inp` is consumed by then, the file is re-opened from
  `filepath_hint`.
- When no adapter is left after bails, the error message names the adapters
  that bailed instead of failing silently.

## New adapter capability: bail_if_empty_output

Custom adapters get a `bail_if_empty_output: bool` flag in their config. When
set and the spawned program produces no text output for a real file, the
adapter bails instead of returning empty output. Enabled for the built-in poppler
adapter. The README documents an OCRmyPDF-based `pdf` override
recipe.

"No text output" means the first chunk of stdout contains no bytes other
than ASCII whitespace/control (<= 0x20 or 0x7F). This matters in practice:
`pdftotext` on an image-only PDF emits a lone form-feed page separator per
page rather than zero bytes, so a zero-byte check would never fire for real
scans.

Also: tolerate a closed stdin pipe when feeding the child's stdin — a tool
that never reads stdin (e.g. `echo`) is not a conversion failure, and a
warning is logged when the empty-output bail cannot be honored inside
archives.

## Limitations (documented on the type)

- Only real files on disk can be retried. A bail on a file inside an archive
  is a hard error (archive member streams cannot be rewound).
- The bailing adapter must not need the original input stream back — adapters
  that run external tools on the file path (poppler, pandoc, …) are fine.

## Tests

- fallback to a second adapter after a bail
- no bail when output has text (peeked chunk is not dropped)
- bail on whitespace-only output (the real scanned-PDF case)
- error (not a hang) when every matching adapter bailed

## Relation to other open PRs

Builds on #352 (Windows LF normalization): without it, five pre-existing
PDF fixture tests fail on Windows regardless of this PR. With #352 merged,
this branch is fully green on Windows (29/29).
```

---

## Internal notes (not for the PR body)

- **Order**: open only after the first wave (#352–#359), **#352 first** (that
  PR already carries the 23→29 before/after evidence in its description).
  Once #352 is merged:
  `git fetch upstream && git rebase upstream/master` on `feat/adapter-bailout`
  — rehearsal already done locally (branch `rehearse/pr3-after-352`), rebase
  is clean and 29/29.
- **Branch**: does NOT need to be created anymore — `feat/adapter-bailout`
  already exists on both mirrors with the whitespace-only fix squashed into
  `d7e3d13`. History was rewritten once (force-push); if anyone else has
  clones, let them know.
- **Upstream CI**: will hit the same fork-approval gate as the first wave.
- **E2E validated**: a real image-only PDF → poppler bails ("produced no
  text output") → with no adapter left, the error names poppler; with a
  custom OCR adapter configured, the fallback delivers the text (`rga`
  propagates config via the `RGA_CONFIG` env var; standalone `rga-preproc`
  does not read the config file — that is upstream behavior, unchanged by
  this PR).
- Issue #3 has 0 comments; the text above aims to be self-contained to make
  the maintainer's life easy.
