# Draft — upstream PR for issue #3 ("Idea: allow adapters to bail out")

> Status: **DRAFT — não abrir ainda no GitHub.**
> Abrir somente depois que a onda 1 de fixes tiver sido mergeada no upstream
> (ou pelo menos avaliada), para não competir por atenção do mantenedor.
> Branch alvo da PR: uma branch atômica `feat/adapter-bailout` rebased sobre
> `phiresky/ripgrep-all:master` contendo apenas os commits
> `5b3e05d` + `72715a0` (sem o community build).
> Implementação de referência já validada: `poc/adapter-bailout` (forgejo PR #1),
> 33 testes passando.

---

## Título proposto

```
feat: allow adapters to bail out and fall back to the next adapter
```

(adicione `closes #3` no corpo, não no título, para não fechar a issue prematuramente se o PR for rejeitado)

## Corpo proposto

```markdown
Implements the idea from #3: adapters get a way to decline a file *without*
failing the whole pipeline, and the next matching adapter gets a chance.

The motivating example from #3 works end to end: when poppler (pdftotext)
extracts no text from a PDF (e.g. a scan without a text layer), the poppler
adapter bails, matching re-runs with poppler excluded, and an OCR adapter
(e.g. tesseract / OCRmyPDF) takes over instead of the user getting an empty
result.

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
set and the spawned program produces no output for a real file, the adapter
bails instead of returning empty output. Enabled for the built-in poppler
adapter. The README documents an OCRmyPDF-based `pdf` override recipe.

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
- no bail when output is non-empty
- error (not a hang) when every matching adapter bailed
- archive-member bail is a hard error

## Relation to other open PRs

Independent of all currently open fix PRs; touches `src/adapters.rs`,
`src/preproc.rs`, `src/adapters/custom.rs` and README. Will be rebased as
needed.
```

---

## Notas internas (não ir no corpo da PR)

- **Ordem**: abrir só depois da onda 1 (#352–#359). Se alguma delas tocar
  `src/adapters.rs`/`custom.rs` no mesmo trecho, rebasar antes.
- **Branch**: criar `feat/adapter-bailout` a partir de `upstream/master` com
  cherry-pick de `5b3e05d` + `72715a0`. Cuidado: esses commits foram escritos
  sobre o community build — se o cherry-pick conflitar em imports
  (`preproc.rs` diverge do upstream por causa de daemon/antiword etc.), a
  versão upstream precisa ser reconstruída manualmente sobre o master limpo.
- **CI upstream**: vai cair no mesmo gate de aprovação de fork da onda 1.
- A issue #3 tem 0 comentários; o texto acima tenta ser autocontido para
  facilitar a vida do mantenedor.
