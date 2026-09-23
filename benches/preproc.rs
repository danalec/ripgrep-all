//! Preprocess throughput benchmarks over the generated corpus.
//!
//! Runs the real `rga-preproc` binary, so numbers reflect what users get:
//! adapter detection, preprocessing, postproc prefixing and (for the warm
//! group) cache hits. Process-spawn overhead is included; it is a constant
//! offset that does not affect relative comparisons.
//!
//! Generate the corpus first: `python benches/gen_corpus.py`

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benches/corpus")
}

fn corpus_files() -> Vec<PathBuf> {
    // plain text is searched by rg directly (rga-preproc never sees it), so
    // only adapter-backed formats are benchmarked here
    const ADAPTER_EXTS: [&str; 5] = ["pdf", "docx", "zip", "tar.gz", "sqlite"];
    let mut v: Vec<_> = std::fs::read_dir(corpus_dir())
        .expect("corpus missing - run: python benches/gen_corpus.py")
        .map(|e| e.unwrap().path())
        .filter(|p| {
            let name = p.file_name().unwrap().to_str().unwrap();
            ADAPTER_EXTS.iter().any(|ext| name.ends_with(ext))
        })
        .collect();
    v.sort();
    v
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn fresh_cache_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rga-bench-{}-{}-{}",
        tag,
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn run_preproc(file: &Path, cache: &Path) -> u64 {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rga-preproc"));
    // clap requires equals for value assignment on this option
    cmd.arg(format!("--rga-cache-path={}", cache.display()))
        .arg(file);
    // Optional PATH prepend so adapter binaries (e.g. a modern poppler
    // pdftotext) win over stale ones shadowing it (Git Bash ships xpdf 4.00,
    // which rga's poppler adapter rejects). Not needed on CI, where apt's
    // poppler is the only one in PATH.
    if let Ok(prepend) = std::env::var("RGA_BENCH_PATH_PREPEND") {
        let old = std::env::var("PATH").unwrap_or_default();
        cmd.env("PATH", format!("{prepend};{old}"));
    }
    let out = cmd.output().expect("failed to spawn rga-preproc");
    if !out.status.success() {
        panic!(
            "rga-preproc failed on {}: {}",
            file.display(),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    out.stdout.len() as u64
}

fn bench_cold(c: &mut Criterion) {
    let mut g = c.benchmark_group("preproc_cold");
    for file in corpus_files() {
        let size = std::fs::metadata(&file).unwrap().len();
        g.throughput(Throughput::Bytes(size));
        g.sample_size(10);
        g.bench_function(file.file_name().unwrap().to_str().unwrap(), |b| {
            b.iter_with_large_drop(|| {
                let cache = fresh_cache_dir("cold");
                let n = run_preproc(&file, &cache);
                std::fs::remove_dir_all(&cache).ok();
                n
            })
        });
    }
    g.finish();
}

fn bench_warm(c: &mut Criterion) {
    let mut g = c.benchmark_group("preproc_warm");
    for file in corpus_files() {
        let size = std::fs::metadata(&file).unwrap().len();
        g.throughput(Throughput::Bytes(size));
        g.sample_size(10);
        g.bench_with_input(
            criterion::BenchmarkId::from_parameter(
                file.file_name().unwrap().to_str().unwrap(),
            ),
            &file,
            |b, file| {
                // one shared cache dir per file: the first call populates it,
                // every measured iteration after that is a pure cache hit
                let cache = fresh_cache_dir("warm");
                run_preproc(file, &cache);
                b.iter_with_large_drop(|| run_preproc(file, &cache));
                std::fs::remove_dir_all(&cache).ok();
            },
        );
    }
    g.finish();
}

criterion_group!(benches, bench_cold, bench_warm);
criterion_main!(benches);
