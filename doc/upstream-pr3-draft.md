# Draft — upstream PR for issue #3 ("Idea: allow adapters to bail out")

> Status: **DRAFT — não abrir ainda no GitHub.**
> Abrir somente depois que a onda 1 de fixes tiver sido mergeada no upstream
> (em particular a #352, que é pré-requisito de testes no Windows), para não
> competir por atenção do mantenedor.
> Branch da PR: `feat/adapter-bailout` — **já preparada** nos dois mirrors
> (GitHub `danalec/ripgrep-all` e forgejo), com 2 commits atômicos sobre
> `phiresky/ripgrep-all:master` (`8dabb3d` core + `d7e3d13` poppler,
> squash do fix de whitespace-only output incluído), sem o community build.
> Implementação de referência: `poc/adapter-bailout` (forgejo PR #1),
> 3 commits (`5b3e05d`, `72715a0`, `5ce2a7f`), **40/40 testes verdes** e
> e2e validado com PDF scanneado real.

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
adapter bails instead of returning empty output. Enabled for the built-in
poppler adapter. The README documents an OCRmyPDF-based `pdf` override
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

## Notas internas (não ir no corpo da PR)

- **Ordem**: abrir só depois da onda 1 (#352–#359), **#352 primeiro** (a PR
  já tem evidência do antes/depois 23→29 na descrição). Com a #352 mergeada:
  `git fetch upstream && git rebase upstream/master` na `feat/adapter-bailout`
  — ensaio já feito localmente (branch `rehearse/pr3-after-352`), rebase
  limpo e 29/29.
- **Branch**: NÃO precisa mais ser criada — `feat/adapter-bailout` já existe
  nos dois mirrors com o fix de whitespace-only squashado em `d7e3d13`.
  Histórico foi reescrito uma vez (force-push); se alguém mais tiver clones,
  avisar.
- **CI upstream**: vai cair no mesmo gate de aprovação de fork da onda 1.
- **E2E validado**: PDF só-imagem real → poppler baila ("produced no text
  output") → sem adapter restante, erro nomeia o poppler; com adapter OCR
  custom configurado, o fallback entrega o texto (`rga` propaga o config
  via env `RGA_CONFIG`; `rga-preproc` isolado não lê config file — isso é do
  upstream, não muda nesta PR).
- A issue #3 tem 0 comentários; o texto acima tenta ser autocontido para
  facilitar a vida do mantenedor.
