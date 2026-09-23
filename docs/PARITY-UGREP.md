# Paridade de recursos: rga × ugrep

Baseline da comparação:

| Item | Versão / referência |
| --- | --- |
| rga (source of truth: Forgejo) | `forgejo/main` fd42136 (v0.10.10.5) + branches wave-3a/3b/3c/3d e wave-4 pós-release, `Cargo.toml` v0.10.11, clap 4.6 já migrado |
| ugrep | 4.x — README oficial e man page (genivia/ugrep, ugrep.com) |
| engine de busca do rga | ripgrep 15.0.0 (o rga é wrapper; tudo de *matching* é do rg) |

Legenda: ✅ paridade · 🟡 parcial · ❌ faltando · ➕ rga à frente

A comparação separa três camadas, porque vivem em lugares diferentes do código:

1. **Compressão e arquivos** — adapter `decompress`/`zip`/`tar`/`sevenz` (domínio nativo do rga)
2. **Documentos e mídia** — adapters builtin de spawning (equivalente ao `ugrep+`)
3. **Engine de busca e UX** — quase tudo é delegado ao rg; o que o rg não tem, o rga só alcança com pós-processamento próprio ou está bloqueado

---

## 1. Compressão (ugrep `-z` × adapter `decompress`)

Referência: tabela de formatos do `ugrep -z` (README, seção "Searching compressed files and archives with -z").

| Formato | Sufixos ugrep | Short p/ tar | rga hoje (`EXTENSIONS` em `decompress.rs`) | Status | Ação |
| --- | --- | --- | --- | --- | --- |
| gzip | `.gz` | `.taz`, `.tgz`, `.tpz` | `gz`, `tgz`, `taz`, `tpz` | ✅ | — |
| compress | `.Z` | `.taZ`, `.tZ` | `Z`, `taZ`, `tZ` (`newtua-lzw-z`) | ✅ | — |
| bzip2 | `.bz`, `.bz2`, `.bzip2` | `.tb2`, `.tbz`, `.tbz2`, `.tz2` | `bz2`, `tbz`, `tbz2`, `bz`, `bzip2`, `tb2`, `tz2` | ✅ | — |
| xz | `.xz` | `.txz` | `xz`, `txz` | ✅ | — |
| zstd | `.zst`, `.zstd` | `.tzst` | `zst`, `zstd`, `tzst` | ✅ | — |
| lzma | `.lzma` | `.tlz` | `lzma`, `tlz` (`liblzma`) | ✅ | — |
| lz4 | `.lz4` | — | `lz4` (`lz4_flex`) | ✅ | — |
| brotli | `.br` | — | `br` (async-compression) | ✅ | — |
| bzip3 | `.bz3` | — | — | ❌ | sem crate Rust consolidada; avaliar bindings ou CLI externa |
| 7z | `.7z` | — | adapter próprio (`7z`, `cb7`) | ✅➕ | ver seção 2 |
| concatenados (gz/bz2/xz/lz4/zstd) | pesquisados como um arquivo | — | MultiGzip (streams concatenados, gzip multi-membro) | ✅ | — |

Notas:

- `get_inner_filename` (decompress.rs:80) só mapeia `tgz|tbz|tbz2 → .tar`; os demais sufixos compostos dependem de strip de extensão dupla (`foo.tar.xz → foo.tar`), que já funciona — os novos short suffixes precisam de entrada no mapa (`txz`, `tlz`, `tzst`, `taz`, `tpz`, `tb2`, `tz2` → `.tar`) e em `EXTENSIONS`.
- ugrep detecta gzip/compress/zip/bzip2/xz/zstd por magic bytes inclusive em stdin. O rga usa extensão por padrão e magic bytes com `--rga-accurate` (slow matchers por mime: hoje gzip/bzip/xz/zstd). Ao adicionar brotli/lz4/lzma, incluir também os mime types nos slow matchers.
- ugrep exige sufixo para 7z/brotli/bzip3/lzma/lz4 (sem magic em stdin) — mesmo comportamento do rga é aceitável.

## 2. Arquivos (archives)

Recurso | ugrep | rga hoje | Status | Ação
--- | --- | --- | --- | ---
zip | `.zip`, `.zipx`, `.ZIP` | `zip`, `zipx`, `cbz`, `jar`, `xpi`, `kra`, `snagx`, `3mf`, `usdz` + fallback magic (#214) | ✅ | matching de extensão case-insensitive; `.ZIP` ok
rar/cbr | — | — | — | fora de escopo dos dois
tar | `.tar` | `tar`, `pax` | ✅ | pax é tar POSIX; crate `tar` lê transparentemente
cpio | `.cpio` | adapter `cpio` (newc) nativo | ✅ | —
7z | builtin, não aninhável (SDK LZMA exige seekable), RAM alta | adapter `sevenz` (`7z`, `cb7`), recursa como os outros archives | ➕ | rga à frente (nesting suportado)
jar | via zip | via zip | ✅ | —
recursão aninhada | `--zmax` 1–99, default 1 | `--rga-max-archive-recursion` default 5 | ➕ | default mais útil; ugrep permite até 99 (irrelevante na prática)
seleção de membros (`-g`/`-O`/`-t` dentro do archive) | ✅ | rg aplica globs nos caminhos virtuais (`archive.zip/inner/file.txt`) | ✅ | —
métodos de compressão dentro do zip | stored, deflate, bzip2, lzma, xz, zstd | async-zip 0.0.19 feature `full` — paridade método-a-método testada (round-trip nos 6 métodos) | ✅ | —
zip64 / data descriptors | parcial (limitações próprias) | limitação async-zip (#292/#295) | 🟡 | já mapeado no TRIAGE.md como não corrigível no rga hoje
SingleFile HTML (zip após HTML) | — | — | ❌ | upstream #269 (scan de zip pulando prefixo) — não trivial

## 3. Documentos e mídia (`ugrep+` × adapters builtin)

O `ugrep+` usa `pdftotext`, `antiword`, `pandoc` e `exiftool` (quando instalados). Os exemplos do README mostram ainda `soffice`, `in2csv`, `openssl`, `iconv` via `--filter`.

Formato | ugrep+ | rga hoje | Status | Ação
--- | --- | --- | --- | ---
PDF | pdftotext | poppler v2 (senha `-opw`, `-eol unix`, bail p/ OCR #3) | ✅➕ | —
doc (Word 97-2003) | antiword | antiword | ✅ | —
docx, epub, odt, fb2, ipynb, html, rtf | pandoc | pandoc v5 (FormatHeuristics `htm→html`, `--eol=lf`) | ✅➕ | rtf além do básico do ugrep
OCR em imagens | — | tesseract (opt-in) | ➕ | —
xls | filtro de exemplo (xls2csv/in2csv) | xls2csv (opt-in) | ✅ | —
xlsx, pptx, ppt, ods, odp | via `--filter` com soffice/in2csv (exemplos, não default) | soffice builtin opt-in (`--headless --cat`, LibreOffice 7.4+; precisa de path seekable, falha com GUI aberta) | ✅ | —
metadata de imagens (jpg, png, webp, gif, bmp) | exiftool (default do ug+) | exiftool builtin opt-in (stdin; funciona dentro de archives) + adapters nativos wave-2 (EXIF jpeg, tags de áudio, DICOM) | ✅ | —
certificados pem/crt/der | exemplo openssl | openssl-x509/openssl-der builtins opt-in (wave-3a) | ✅ | —
sqlite | — | adapter nativo (WAL-safe #287) | ➕ | —
vídeo/áudio (legendas, capítulos, metadata) | exiftool (metadata) | ffmpeg (legendas + lyrics + metadata, LRC-strip #293) | ➕ | —
e-mail mbox/eml | — | mail (opt-in) | ➕ | —
parquet, fits, shp, dbf, npy, gguf, safetensors, onnx/pb, tiktoken, 3D (glb/stl/ply/fbx/pcd/las/vtk) | — | adapters nativos wave-2 | ➕➕ | muito à frente; sem contraparte no ugrep

Conclusão da seção: em documentos o rga cobre o default do `ugrep+` inteiro (pdftotext, antiword, pandoc) **e** os filtros de exemplo (soffice, openssl, exiftool) como builtins opt-in — sem perder a filosofia de parsers nativos leves para os casos quentes (EXIF, áudio, DICOM na wave-2).

## 4. Engine de busca (delegado ao rg)

O rga não faz matching — repassa ao rg. Recursos do ugrep que o rg 15 já cobre ficam ✅ de graça; os que o rg não tem exigem pós-processamento no rga ou estão bloqueados.

Recurso | ugrep | rg 15 | Status | Caminho no rga
--- | --- | --- | --- | ---
Unicode + UTF-8/16/32 automático | ✅ | BOM sniffing UTF-16/32 (`bench-utf16` no fork) | ✅ | —
Multilinha `\n`/`\R` sem flag | default | `-U/--multiline` | 🟡 | diferença de ergonomia apenas
Padrões booleanos `-%`, `--and`, `--andnot`, `--not`, `-N` | ✅ | só múltiplos `-e` (OR) | ✅ | `--rga-and`/`--rga-not` (wave-4): pós-filtro de linhas sobre o stream `rg --json`; perde cores. Pipeline `rga A | rg B` continua válido para streaming puro
Fuzzy `-Z` (Levenshtein) | ✅ | — | ❌ | bloqueado: regex não suporta matching aproximado; exigiria matcher próprio
`--sort=best` | ✅ | — | ❌ | depende de fuzzy
`--sort` name/size/changed/created | ✅ | `--sort` path/modified/accessed/created | ✅ | —
TUI `-Q` | ✅ | — | 🟡 | `rga-fzf` cobre o caso interativo (fork mantém rga-fzf + rga-fzf-open); TUI nativa = projeto grande, não recomendado duplicar fzf
Saída JSON | `--json` | `--json` (esquema diferente) | ✅ | —
Saída CSV / XML / formato `%` custom (`--format`) | ✅ | — | ✅ | `--rga-format=csv|xml|TEMPLATE` (wave-4): consome `rg --json`, emite um registro por submatch; %fields `%f` `%n` `%c` `%m` `%d`
`--replace` (com %fields) | ✅ | `--replace` (só grupos `$1`) + `--passthrough` | ✅➕ | `--rga-replace=TEMPLATE` (wave-4): substitui o match dentro da linha, com os mesmos %fields
Colunas `-k` | ✅ | `--column` | ✅ | —
Hexdump de binário `-X`/`-W`/`--hexdump` | ✅ | — | ❌ | bloqueado no lado rg (matching binário não passa pelo rga)
Magic bytes `-M` | ✅ | — | ✅➕ | `--rga-accurate` (mime por magic bytes nos primeiros 8 KiB)
Tipos `-t`, extensões `-O`, globs `-g` (com `^` negado), hidden `-.`, ignore-files | ✅ | `-t`, `-g`, `--hidden`, `--ignore-file` (sem `^` negado) | ✅ | —
Encoding específico (`--encoding=LATIN1` etc.) | ✅ | `--encoding` (exige flag explícita) | 🟡 | ugrep converte ISO-8859-1..16, CP437/850, MACROMAN, KOI8; rg suporta os mesmos via explicit flag — igual na prática
`--filter` por utilitário | ✅ | — | ➕ | equivalente rga é superior: custom adapters em config.jsonc com match por mime, override de builtin (#232), bail-on-empty (#3)
`--max-files`, `--min-files`, `--range N-M` | ✅ | — (rg tem `--max-filesize` em bytes) | 🟡 | `--rga-max-files=N` (wave-4) via pós-contagem no wrapper de saída; `--min-files`/`--range` sem uso claro no rga
Profundidade `-1`..`-9` | ✅ | `--max-depth=N` | ✅ | —
PCRE `-P` | ✅ | `-P` (PCRE2, com `--engine`) | ✅ | —
BRE `-G` / drop-in grep-egrep-fgrep | ✅ | — | ❌ | fora de escopo pragmático (rg não é drop-in do grep por design)
Config file | `.ugrep` + `--save-config` | — | ✅➕ | config.jsonc **com schema** (`--rga-print-config-schema`) + `--rga-save-config` (wave-3a) — à frente do ugrep

## 5. UX / entrega

Recurso | ugrep | rga | Status | Ação
--- | --- | --- | --- | ---
completions de shell | ✅ | ❌ | ✅ | `--rga-complete=<shell>` (wave-3a, clap_complete)
man page | ✅ | ❌ | ✅ | `--rga-manpage` (wave-3a, clap_mangen)
`--help WHAT` contextual | ✅ | — | ❌ | aproximação: longo `--help` organizado; opcional
índice de filesystem (ugrep-indexer) | ✅ (ferramenta à parte) | — | 🟡 | mecanismo diferente: cache rga (sqlite+zstd) + daemon TCP do fork atacam o mesmo problema para arquivos adaptados; índice de paths frios não existe
benchmarks públicos | ugrep-benchmarks | `benches/` em construção (ROADMAP fase 0) | 🟡 | em andamento no ROADMAP-PERF-PACKAGING.md
variante "pesquisa tudo" | `ug+`/`ugrep+` | `--rga-adapters=+tesseract,xls2csv` | ✅ | equivalente funcional
fzf | — | rga-fzf, rga-fzf-open | ➕ | —
cache compartilhado | — | daemon.rs (TCP) | ➕ | —

## 6. Ondas de implementação (estado: concluídas)

Cada item com esforço estimado e dependência. Ondas 1–2 (adapters wave-2, quick wins do TRIAGE) já estão no histórico. **Wave-3 completa (3a–3d) e wave-4 implementadas; verificação: suite de testes da lib 97 passed / 0 failed, e2e manual com `rg` real.**

### wave-3a — triviais (✅ `feat/wave3a-quickwins`, commit 1979e55)

| Item | Onde | Esforço |
| --- | --- | --- |
| ✅ `pax` no tar adapter (extensão) | `tar.rs` EXTENSIONS | XS |
| ✅ `zipx`, `cbz` no zip adapter | `zip.rs` EXTENSIONS | XS |
| ✅ completions + manpage (clap_complete, clap_mangen) | `src/bin/rga.rs` + handlers | S |
| ✅ `--rga-save-config` (gravar config mesclada em jsonc) | CLI/config.rs | S |
| ✅ adapter openssl (pem/crt/der) builtin opt-in | `custom.rs` BUILTIN_SPAWNING_ADAPTERS | XS |

### wave-3b — decompressores (✅ `feat/wave3b-decompress`, commit a1c387d)

| Item | Onde | Esforço |
| --- | --- | --- |
| ✅ brotli `.br` (async-compression já linkada com all-algorithms) + mime | `decompress.rs` | S |
| ✅ lz4 `.lz4` (`lz4_flex`) + mime | `decompress.rs` + Cargo.toml | S |
| ✅ aliases e shorts: `bz`, `bzip2`, `zstd`, `taz`, `tpz`, `tb2`, `tz2`, `txz`, `tzst`, `tgz` (+ mapa → `.tar`) | `decompress.rs` | XS |
| ✅ lzma `.lzma`/`.tlz` (crate `liblzma` — xz2 conflita links=lzma) | `decompress.rs` | M |
| ✅ compress `.Z`/`.taZ`/`.tZ` (LZW via `newtua-lzw-z`, zero deps) | `decompress.rs` | M |
| ⏸ bzip3 `.bz3` — sem crate consolidada; fica pendente | `decompress.rs` | M |
| ✅ streams concatenados: MultiGzip (gzip multi-membro; EOF por delta de ReadBuf) | `decompress.rs` + tests | XS |

### wave-3c — archives (✅ `feat/wave3c-cpio`, commit 291b9c7)

| Item | Onde | Esforço |
| --- | --- | --- |
| ✅ adapter cpio (newc) | `src/adapters/cpio.rs` novo | M |
| ✅ métodos de compressão do async-zip vs ugrep (stored/deflate/bzip2/lzma/xz/zstd round-trip) | teste `all_compression_methods_roundtrip` | S |
| ❌ SingleFile HTML (#269) — zip com prefixo | `zip.rs` | M — permanece aberto (upstream) |

### wave-3d — documentos (✅ `feat/wave3d-spawning`, commit 76727ea)

| Item | Onde | Esforço |
| --- | --- | --- |
| ✅ adapter exiftool opt-in (jpg/png/webp/gif/bmp/tiff, stdin → funciona em archives) | `custom.rs` builtin | S |
| ✅ exif/áudio/DICOM nativos (substitui a necessidade do kamadak-exif p/ jpeg) | adapters nativos wave-2 (`feat/media-adapters`) | M — feito na wave-2 |
| ✅ adapter soffice opt-in (xlsx/pptx/ppt/ods/odp; `--headless --cat` LO 7.4+) | `custom.rs` builtin | S (caveat GUI aberta) |

### wave-4 — engine / saída (✅ `feat/wave4-output`, este branch)

| Item | Onde | Esforço |
| --- | --- | --- |
| ✅ saída csv/xml/`--rga-format` a partir de `rg --json` (wrapper mode força `--json --color=never`) | `src/output.rs` novo + `rga.rs` | M |
| ✅ %fields no `--rga-replace` (`%f` `%n` `%c` `%m` `%d`, substitui o match na linha) | `src/output.rs` | S |
| ✅ `--rga-and`/`--rga-not` pós-filtro de linhas (smart-case; exit 1 se tudo filtrado) | `src/output.rs` | M (perde cores, documentado) |
| ✅ `--rga-max-files` (pós-contagem; a busca continua até o fim) | `src/output.rs` | XS |

### Bloqueados / fora de escopo (documentados, não implementados)

- **fuzzy `-Z`** e **`--sort=best`** — regex sem matching aproximado; exigiria matcher próprio no rga
- **TUI `-Q` nativa** — duplicaria fzf; `rga-fzf` é a resposta do projeto
- **hexdump** — matching binário acontece no rg, o rga não intercepta
- **BRE / drop-in grep** — decisão de design do rg
- **índice de filesystem** (ugrep-indexer) — o cache rga cobre o caso adaptado; índice de paths é outro produto
- **bzip3** — sem crate Rust consolidada
- **SingleFile HTML** (#269) — exige scan de zip com prefixo, não trivial
- mitigação documentável: pipelines (`rga A | rg B` para AND; `fzf` para fuzzy/interativo)

## Fontes

- ugrep README (seções "What does ugrep add", "Searching compressed files and archives with -z", "Filters"): https://github.com/Genivia/ugrep/blob/master/README.md
- ugrep man page: https://manpages.debian.org/testing/ugrep/ugrep.1
- ugrep-indexer: https://github.com/Genivia/ugrep-indexer
- rga upstream: https://github.com/phiresky/ripgrep-all
- estado local: `.rga-triage/TRIAGE.md`, `docs/ROADMAP-PERF-PACKAGING.md`, `src/adapters/` (forgejo = source of truth)
