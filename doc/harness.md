# Using rga as the primary search tool in an LLM harness

`rga` is a drop-in `rg` replacement: it accepts every ripgrep flag and adds
adapters that turn binary artifacts into searchable text before matching.
In an agent harness this makes it the single entry point for "find me X in
the workspace", instead of a zoo of per-format scripts.

## Why rga first

| the agent wants…                    | plain rg            | rga |
|-------------------------------------|---------------------|-----|
| a string in source code             | yes                 | yes (identical output) |
| text inside PDFs / Office / epub    | no                  | yes (pandoc, poppler) |
| a file inside zip / tar.gz / 7z     | no                  | yes, with `archive.zip:inner/path` prefixes |
| scene names inside .glb / .fbx      | no                  | yes (`glb`, `fbx` adapters) |
| model config inside .gguf / .safetensors | no             | yes (`gguf`, `safetensors` adapters) |
| dtype/shape of a .npy dataset       | no                  | yes (`npy` adapter) |
| node names inside .onnx graphs      | no                  | yes (`protobuf` adapter) |
| rows inside .sqlite                 | no                  | yes (`sqlite` adapter) |

Because adapters are cacheable and streamed, repeated searches over big
binary trees are cheap after the first pass.

## Recommended invocation for agents

```sh
rga --no-heading --with-filename --line-number --context 2 \
    --max-count 20 --max-filesize 64M \
    --rga-max-archive-recursion 2 \
    -i PATTERN [PATH]
```

Flag rationale:

- `--no-heading --with-filename --line-number`: stable, machine-readable
  `path:line:text` records — easy to split, no ANSI layout to parse.
- `--context 2`: enough context for the LLM to judge relevance without
  blowing the context window.
- `--max-count 20`: bounded output per file; archives can match thousands
  of lines otherwise.
- `--max-filesize 64M`: skips multi-GB model weights; the `gguf` adapter
  only needs the header, but a corrupt/giant file should not stall a run.
- `--rga-max-archive-recursion 2`: model/artifact packages nest
  (e.g. `.usdz` → textures); depth 2 covers real layouts, deeper risks
  combinatorial blowups on pathological archives.
- `-i`: agents rarely know the exact case the author used.

For one-shot identification of a model file, also useful:

```sh
rga --no-filename --max-count 5 'architecture|context_length|model_type' model.gguf adapter.safetensors
```

## Output contract you can rely on

- Matches inside archives look like `archive.zip:inner/file.txt:line: text`,
  so the agent can both display and open the hit (the path prefix up to the
  first `:` that exists on disk is the archive).
- Adapter-extracted text (e.g. a GLB's JSON scene) uses
  `file.glb:json-key: value` lines, so grepping `material` in a directory of
  assets returns the asset *and* the key that matched.
- If an adapter declines a file (e.g. ASCII STL, which is already plain
  text), rga falls back to the next adapter or raw passthrough — a file is
  never silently dropped from results.

## Failure modes to expect

- Missing external tools: `pandoc`, `pdftotext` and `ffmpeg` are spawned
  adapters; if they are not installed, those formats error per-file but the
  overall search continues. Run `rga --rga-list-adapters` to see which are
  available and `rga-preproc --help` for the cache.
- Corrupt archives abort recursion for that archive only.
- Very wide archives (10k+ files) are handled lazily, but consider
  `--max-count` to keep output bounded.

## Wiring it into a harness

1. Point the harness's "search workspace" tool at `rga` instead of `rg`.
2. Treat non-zero exit codes like `rg`: `1` = no match, `2` = error.
3. Cache the rga cache directory (`~/.cache/rga` on Linux,
   `%LOCALAPPDATA%\rga` on Windows) between runs — it is keyed by file
   mtime and adapter version, so it is safe to persist.
