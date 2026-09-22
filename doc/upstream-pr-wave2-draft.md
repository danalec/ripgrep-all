# Drafts — upstream PRs, wave 2

> Status: **DRAFT — do not open on GitHub yet.**
> Open only after the first wave (#352–#359) has been merged or reviewed.
> Both branches are already prepared on `danalec/ripgrep-all` (GitHub and
> forgejo mirrors in sync), each a single atomic commit on clean
> `upstream/master`:
>
> - `feat/70-honor-rg-text-flag` (`38ea739`)
> - `feat/232-adapter-override` (`d59643e`)
>
> No PRs opened against phiresky for these.

---

## PR 1 — honor rg's -a/--text flag

Branch: `feat/70-honor-rg-text-flag`
Closes: https://github.com/phiresky/ripgrep-all/issues/70

### Proposed title

```
feat: honor rg's -a/--text flag for rga's own binary detection
```

### Proposed body

```markdown
Fixes #70.

rga replaces content it detects as binary with `[rga: binary data]`, even
when the user passes `-a`/`--text`/`--binary` to rg — so binary content
inside archives could never actually be searched.

Detect those flags in the passthrough args and skip the binary
replacement in that case. Also expose the same switch as a `text` config
file option so it can be set persistently.

Touches `src/adapters/postproc.rs` (skip binary replacement when text
mode is on) and `src/config.rs` (flag detection + `text` option).

## Relation to other open PRs

Independent of all currently open fix PRs; touches different regions
than the upcoming adapter work (#3).
```

---

## PR 2 — custom adapters override built-ins with the same name

Branch: `feat/232-adapter-override`
Closes: https://github.com/phiresky/ripgrep-all/issues/232

### Proposed title

```
feat: custom adapters with the same name as a built-in now override it
```

### Proposed body

```markdown
Fixes #232.

Previously, a custom adapter named e.g. `poppler` conflicted with the
built-in one ("Warning: found multiple adapters"), so there was no clean
way to change the arguments passed to pdftotext (e.g. adding `-layout`).

A custom adapter with a built-in's name now replaces it in place, keeping
the original priority — the built-in can be reconfigured without losing
its position in the matching order.

This also composes with the README's OCR recipe for the adapter
bail-out work (issue #3): users can either add a *new* adapter for the
same extension (fallback chain) or *replace* the built-in (same-name
override) depending on which behavior they want.

Tests cover: same-name replacement (exactly one adapter survives — the
custom one, with the built-in's metadata gone) and a new-name custom
adapter keeping its priority.
```

---

## Internal notes (not for the PR bodies)

- **Order**: wave 2 opens only after wave 1 lands, one at a time, so the
  maintainer never sees more than one new PR from us at once. Suggested
  order within wave 2: #232 first (small, self-contained, only touches
  `src/adapters.rs`), then #70 (`postproc.rs` + `config.rs`).
- Both branches were built directly on `upstream/master`, so each PR's
  diff is exactly one commit; no community build content leaks in.
- The commit message of `38ea739` ends with a duplicated `Fixes #70`
  trailer (editor artifact). Harmless, but consider cleaning it with a
  rebase --autosquash-style amend before opening the PR.
- Neither branch touches the regions modified by wave 1 fixes, so no
  rebase is expected even if some wave 1 PRs merge first. The exception
  would be a wave 1 change to `get_all_adapters`/`get_adapters_filtered`
  — check before opening.
- `feat/70-honor-rg-text-flag` ships **no new tests** (postproc binary
  replacement is only covered indirectly). Before opening, consider a
  small unit test for the flag detection (`-a`/`--text`/`--binary` in
  passthrough args → `text: true`), or state the manual test plan in the
  PR body. `feat/232-adapter-override` does include tests (2 new).
