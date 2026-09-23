# Roadmap: performance & packaging (community build)

Scope: ripgrep-all community build (danalec/ripgrep-all). Each phase produces
verifiable deliverables; every performance claim must be backed by the Phase 0
harness before it ships. Upstreamable pieces are extracted as atomic PRs to
phiresky/ripgrep-all once proven here.

Current baseline (v0.10.10.3):
- 14 release targets, Windows znver4 + ARM64, pacman generic + znver4 (CI)
- cache: sqlite + zstd (already compressed)
- release profile: `debug = true`, `lto = "thin"`, default CGU — the PKGBUILD
  overrides this locally, `Cargo.toml` does not
- no custom global allocator, no benchmarks in-tree

---

## Phase 0 — Benchmark harness (foundation, everything depends on this)

**Goal:** a régua. Without it every optimization below is faith — the znver4
"26 zmm placebo" incident is the cautionary tale.

Deliverables:
- `benches/corpus/` generator script producing a deterministic, versioned
  corpus: representative samples per hot adapter — PDF (text-dense, generated
  via a script), Office (docx via pandoc), zip/tar.gz with nesting, sqlite db,
  plain text of varying sizes (1 KiB–100 MiB)
- `benches/` criterion suites:
  - `preproc`: per-adapter preprocess throughput (bytes/s) — pdf, pandoc, zip,
    tar.gz, sqlite, plain
  - `cache`: cache write (cold) vs read (warm) latency, hit ratio
  - `e2e`: rga invocation over the corpus vs plain rg overhead ratio
- `.github/workflows/bench.yml`:
  - PR runs: bench job posts a comment with deltas vs `main` baseline
  - `main` runs: store baseline artifact (criterion report) for comparison
- Acceptance: corpus regenerates bit-identically; CI bench job green on a
  no-op PR; noise floor measured (same commit rerun, expect < ±2%)

Effort: ~2–3 days. Blocks: nothing.

## Phase 1 — Quick wins (allocator + release profile)

**Goal:** cheap, measurable wins shipped as atomic changes.

1. mimalloc global allocator (`src/main.rs`, `#[global_allocator]`), feature-
   gated if needed for platforms where mimalloc struggles (macOS is fine,
   Windows fine). Decision rule: adopt iff harness shows ≥ 3% median win on
   `preproc` or `e2e` with no regression > 2% elsewhere.
2. `Cargo.toml [profile.release]`: `lto = "fat"`, `codegen-units = 1`,
   `strip = "debuginfo"`, `debug = "line-tables-only"` (keeps useful stack
   traces, drops the bloat). Already proven in the pacman pipeline.

Upstream extraction (after community proof):
- PR-A: `Cargo.toml` release profile (one file, trivially atomic)
- PR-B: mimalloc — only if the benchmark delta is convincing

Effort: ~2 days incl. measurement. Blocks: Phase 0.

## Phase 2 — PGO (Profile-Guided Optimization)

**Goal:** the big prize nobody in this niche ships ready-made.

Pipeline (new workflow or extension of release.yml):
1. build instrumented (`-Cprofile-generate`)
2. run training workload: preprocess the full Phase 0 corpus + a scripted
   search session (covers both rga and rga-preproc code paths)
3. `llvm-profdata merge`
4. rebuild with `-Cprofile-use`, same fat LTO settings
5. measure vs non-PGO build on the harness; ship iff ≥ 5% median win

Rollout: first as a separate asset variant (`-pgo`) in the community release;
promote to default only after a full release cycle without incident.
Upstream: propose only after we have public benchmark numbers to cite.

Risks: training corpus bias (mitigate: corpus reviewed to cover all adapters),
CI time (+~10 min per target — restrict PGO to x86_64 linux/windows first),
rustc version pinning of profdata (build and train with same toolchain).

Effort: ~1 week. Blocks: Phase 0 (+Phase 1 profile recommended).

## Phase 3 — Supply chain hardening

**Goal:** every release asset carries cryptographic provenance — the argument
made in upstream PR #360, realized.

1. GitHub artifact attestations (`actions/attest-build-provenance`) for all
   release assets — native, no key management
2. Verification docs: how users check an asset against the attestation
3. Optional stretch: cosign keyless signing for the pacman packages
   (`pacman-key`-compatible? document limits)

Upstream extraction: attestation workflow is a ~20-line PR; offer after #360
is reviewed (avoids competing with ourselves).

Effort: ~2 days. Blocks: nothing (can run parallel to Phase 2).

## Phase 4 — Distribution: AUR + winget

**Goal:** be *the* way to install rga on Arch and on Windows outside scoop.

1. AUR: publish `ripgrep-all-community-bin` (from release assets) using the
   Phase 3 attestations as the trust story; automated push on release tag
   (SSH deploy key, aurpublish or plain git push)
2. winget: `komac`-based workflow opening PRs to `microsoft/winget-pkgs` on
   release (version, URL, sha256 from the release metadata)
3. scoop stays as-is (scoop-alts already handles the bucket)

Effort: ~3 days + one-time AUR account setup. Blocks: Phase 3 (attestation
strengthens the AUR trust argument).

## Phase 5 — Upstream campaign

Standing rule from earlier work: one atomic PR per feature, never bundled.

Order (by merge-probability × value):
1. pacman CI — PR #360 (already open, ready for review)
2. release profile PR-A (after Phase 1 numbers exist)
3. mimalloc PR-B (only with benchmark evidence)
4. attestation workflow (after Phase 3)
5. PGO (only with public numbers; possibly rejected for CI cost — fine)
6. winget/AUR docs — upstream README links

---

## Cross-cutting rules

- Official releases are immutable: new features land via pre-release tags
  (`-rcN`) and only promote after full CI + manual smoke on at least two
  platforms (learned the hard way with the pacman sha instability)
- Every perf claim in any PR body must cite harness numbers
- znver4 remains the only tuned variant unless a ≥ 3% win is measured for a
  znver5 build (see PR #360 rationale)
- No retagging, ever: `v0.10.10.3` and earlier are frozen

## Timeline (indicative)

| Week | Focus |
|------|-------|
| ~~1~~ done | Phase 0 harness + corpus (plus mimalloc discovery: +59-62% on archives) |
| ~~2~~ done | Phase 1 quick wins — mimalloc default-on, fat LTO release profile |
| ~~2~~ done (code) | Phase 2 PGO pipeline (linux+windows workflows); e2e numbers pending a test tag |
| ~~2~~ done | Phase 3 attestations on all asset-producing workflows + verify docs |
| ~~3~~ code done | Phase 4 AUR + winget workflows (self-skip without secrets); account setup pending |
| ongoing | Phase 5 upstream campaign as reviews land (#360 pacman open) |
