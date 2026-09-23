use crate::adapted_iter::AdaptedFilesIterBox;
use crate::adapters::*;
use crate::caching_writer::async_read_and_write_to_cache;
use crate::config::RgaConfig;
use crate::matching::*;
use crate::preproc_cache::CacheKey;
use crate::recurse::concat_read_streams;
use crate::{
    preproc_cache::{PreprocCache, open_cache_db},
    print_bytes,
};
use anyhow::*;
use async_compression::tokio::bufread::ZstdDecoder;
use async_stream::stream;
// use futures::future::{BoxFuture, FutureExt};
use log::*;
use postproc::PostprocPrefix;
use std::future::Future;
use std::io::Cursor;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::io::{AsyncBufRead, AsyncReadExt};

pub type ActiveAdapters = Vec<Arc<dyn FileAdapter>>;

async fn choose_adapter(
    config: &RgaConfig,
    filepath_hint: &Path,
    archive_recursion_depth: i32,
    exclude_adapters: &[String],
    inp: &mut (impl AsyncBufRead + Unpin),
    active_adapters: Option<&ActiveAdapters>,
) -> Result<Option<(Arc<dyn FileAdapter>, FileMatcher, ActiveAdapters)>> {
    let computed_adapters;
    let active_adapters = match active_adapters {
        Some(a) => a,
        None => {
            computed_adapters = get_adapters_filtered(config.custom_adapters.clone(), &config.adapters, config)?
                .into_iter()
                // adapters that already bailed out on this file must not be chosen again (issue #3)
                .filter(|a| !exclude_adapters.iter().any(|x| x == &a.metadata().name))
                .collect();
            &computed_adapters
        }
    };
    let adapters = adapter_matcher(active_adapters, config.accurate)?;
    let filename = filepath_hint
        .file_name()
        .ok_or_else(|| format_err!("Empty filename"))?;
    debug!("Archive recursion depth: {}", archive_recursion_depth);

    let mimetype = if config.accurate {
        let buf = inp.fill_buf().await?; // fill but do not consume!
        if buf.starts_with(b"From \x0d") || buf.starts_with(b"From -") {
            Some("application/mbox")
        } else {
            let detected = infer::get(buf).map(|t| t.mime_type());
            // infer sometimes fails to detect archives (reporting
            // application/octet-stream), which breaks --rga-accurate matching.
            // Fall back to checking the ZIP magic bytes directly.
            // https://github.com/phiresky/ripgrep-all/issues/214
            let mimetype = match detected {
                Some("application/octet-stream")
                    if buf.starts_with(b"PK\x03\x04")
                        || buf.starts_with(b"PK\x05\x06")
                        || buf.starts_with(b"PK\x07\x08") =>
                {
                    Some("application/zip")
                }
                other => other,
            };
            debug!("mimetype: {:?}", mimetype);
            mimetype
        }
    } else {
        None
    };
    let adapter = adapters(FileMeta {
        mimetype,
        lossy_filename: filename.to_string_lossy().to_string(),
    });
    Ok(adapter.map(|e| (e.0, e.1, active_adapters.clone())))
}

enum Ret {
    Recurse(AdaptInfo, Arc<dyn FileAdapter>, FileMatcher, ActiveAdapters),
    Passthrough(AdaptInfo),
}
async fn buf_choose_adapter(ai: AdaptInfo, exclude_adapters: &[String], active_adapters: Option<&ActiveAdapters>) -> Result<Ret> {
    // Only use a buffer if we need to detect mime types (accurate mode)
    // or if it's a real file (where buffering helps performance).
    // For files already in memory or from other streams, a large buffer might be redundant.
    let capacity = if ai.config.accurate { 8192 } else { 1024 };
    let mut inp = BufReader::with_capacity(capacity, ai.inp);
    let adapter = choose_adapter(
        &ai.config,
        &ai.filepath_hint,
        ai.archive_recursion_depth,
        exclude_adapters,
        &mut inp,
        active_adapters,
    )
    .await?;
    let ai = AdaptInfo {
        inp: Box::pin(inp),
        ..ai
    };
    let (a, b, c) = match adapter {
        Some(x) => x,
        None => {
            // allow passthrough if the file is in an archive or accurate matching is enabled
            // otherwise it should have been filtered out by rg pre-glob since rg can handle those better than us
            let allow_cat = !ai.is_real_file || ai.config.accurate;
            if allow_cat {
                if !exclude_adapters.is_empty() {
                    eprintln!(
                        "rga: no adapter left for '{}' after adapter(s) {} bailed — passing raw content through",
                        ai.filepath_hint.to_string_lossy(),
                        exclude_adapters.join(", ")
                    );
                }
                if ai.postprocess {
                    (
                        Arc::new(PostprocPrefix {}) as Arc<dyn FileAdapter>,
                        FileMatcher::Fast(FastFileMatcher::FileExtension("default".to_string())),
                        Vec::new(),
                    )
                } else {
                    return Ok(Ret::Passthrough(ai));
                }
            } else {
                let after_bail = if exclude_adapters.is_empty() {
                    String::new()
                } else {
                    format!(
                        " after adapter(s) {} bailed out",
                        exclude_adapters.join(", ")
                    )
                };
                return Err(format_err!(
                    "No adapter found for file {:?}{}, passthrough disabled.",
                    ai.filepath_hint
                        .file_name()
                        .ok_or_else(|| format_err!("Empty filename"))?,
                    after_bail
                ));
            }
        }
    };
    Ok(Ret::Recurse(ai, a, b, c))
}

/**
 * preprocess a file as defined in `ai`.
 *
 * If a cache is passed, read/write to it.
 *
 */
pub async fn rga_preproc(ai: AdaptInfo) -> Result<ReadBox> {
    debug!("path (hint) to preprocess: {:?}", ai.filepath_hint);

    // Destructure so we can rebuild AdaptInfo with a fresh input stream when an
    // adapter bails (ai.inp is consumed by the adapter, but real files can be re-opened).
    let AdaptInfo {
        filepath_hint,
        is_real_file,
        archive_recursion_depth,
        inp,
        line_prefix,
        postprocess,
        config,
        file_mtime_unix_ms,
    } = ai;
    let mut inp: ReadBox = inp;

    // Adapters that bailed out and must be excluded when re-matching (issue #3).
    let mut excluded: Vec<String> = Vec::new();
    loop {
        // todo: figure out when using a bufreader is a good idea and when it is not
        // seems to be good for File::open() reads, but not sure about within archives (tar, zip)
        let ai = AdaptInfo {
            filepath_hint: filepath_hint.clone(),
            is_real_file,
            archive_recursion_depth,
            inp,
            line_prefix: line_prefix.clone(),
            postprocess,
            config: config.clone(),
            file_mtime_unix_ms,
        };
        match buf_choose_adapter(ai, &excluded, None).await? {
            Ret::Passthrough(ai) => return Ok(ai.inp),
            Ret::Recurse(ai, adapter, detection_reason, active_adapters) => {
                let adapter_name = adapter.metadata().name.clone();
                let path_hint_copy = ai.filepath_hint.clone();
                match adapt_caching(ai, adapter, detection_reason, active_adapters).await {
                    std::result::Result::Ok(read_box) => return Ok(read_box),
                    Err(e) => {
                        let bail_reason = e
                            .chain()
                            .find_map(|cause| cause.downcast_ref::<AdapterBail>())
                            .map(|b| b.reason.clone());
                        match bail_reason {
                            Some(reason) => {
                                excluded.push(adapter_name.clone());
                                if !is_real_file {
                                    return Err(e).with_context(|| {
                                        format!(
                                            "adapter '{}' bailed on '{}' inside an archive (input stream cannot be rewound)",
                                            adapter_name,
                                            path_hint_copy.to_string_lossy()
                                        )
                                    });
                                }
                                eprintln!(
                                    "rga: adapter '{}' bailed on '{}' ({}) — trying next adapter",
                                    adapter_name,
                                    path_hint_copy.to_string_lossy(),
                                    reason
                                );
                                inp = Box::pin(
                                    tokio::fs::File::open(&path_hint_copy)
                                        .await
                                        .with_context(|| {
                                            format!(
                                                "re-opening '{}' after adapter bail",
                                                path_hint_copy.to_string_lossy()
                                            )
                                        })?,
                                );
                            }
                            None => {
                                // hard adapter failure (e.g. pandoc choking on
                                // a corrupt file): emit a searchable marker
                                // line instead of aborting with exit code 2
                                // (issue #151)
                                eprintln!(
                                    "rga: preprocessing '{}' failed — emitting '[rga: preprocessing failed]' marker",
                                    path_hint_copy.to_string_lossy()
                                );
                                return Ok(
                                    marker_entry(
                                        &line_prefix,
                                        &config,
                                        archive_recursion_depth,
                                        &path_hint_copy,
                                        &adapter_name,
                                        e,
                                    )
                                    .inp,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn adapt_caching(
    ai: AdaptInfo,
    adapter: Arc<dyn FileAdapter>,
    detection_reason: FileMatcher,
    active_adapters: ActiveAdapters,
) -> Result<ReadBox> {
    let meta = adapter.metadata();
    debug!(
        "Chose adapter '{}' because of matcher {:?}",
        meta.name, detection_reason
    );
    eprintln!(
        "{} adapter: {}",
        ai.filepath_hint.to_string_lossy(),
        meta.name
    );
    // Note: adapt_caching is only called from rga_preproc for the top-level file.
    // Recursive files inside archives go through loop_adapt directly and never hit this function,
    // so in practice only --rga-no-cache triggers the None path here (is_real_file is always true).
    let cache_compression_level = ai.config.cache.compression_level;
    let cache_max_blob_len = ai.config.cache.max_blob_len;

    // Use the persistent cache daemon when it is reachable on the configured
    // port, otherwise fall back to the local sqlite cache.
    let cache: Option<Box<dyn PreprocCache + Send>> =
        if ai.is_real_file && !ai.config.cache.disabled
        {
            let daemon_port = ai.config.cache.daemon_port;
            // Check if daemon is alive with a quick timeout
            let daemon_available = tokio::time::timeout(
                std::time::Duration::from_millis(10),
                tokio::net::TcpStream::connect(format!("127.0.0.1:{}", daemon_port)),
            )
            .await
            .is_ok_and(|res| res.is_ok());

            if daemon_available {
                debug!("Using daemon for caching on port {}", daemon_port);
                Some(Box::new(crate::daemon::DaemonCacheClient::new(daemon_port)))
            } else {
                debug!(
                    "Daemon not found on port {}, using local sqlite cache",
                    daemon_port
                );
                Some(open_cache_db(&ai.config).await?)
            }
        } else {
            debug!("cache disabled, running adapter without caching...");
            None
        };

    if let Some(mut cache) = cache {
        let file_mtime_unix_ms = ai.file_mtime_unix_ms.unwrap_or_else(|| {
            std::fs::metadata(&ai.filepath_hint)
                .and_then(|m| m.modified())
                .and_then(|t| {
                    t.duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                })
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });
        let cache_key = CacheKey::new(
            &ai.filepath_hint,
            file_mtime_unix_ms,
            adapter.as_ref(),
            &active_adapters,
            &ai.config,
        )?;
        let cached = cache.get(&cache_key).await.context("cache.get")?;
        match cached {
            Some(cached) => Ok(Box::pin(ZstdDecoder::new(Cursor::new(cached)))),
            None => {
                debug!("cache MISS, running adapter with caching...");
                let inp = loop_adapt(adapter.as_ref(), detection_reason, ai, active_adapters).await?;
                let inp = concat_read_streams(inp);
                let inp = async_read_and_write_to_cache(
                    inp,
                    cache_max_blob_len.0,
                    cache_compression_level.0,
                    Box::new(move |(uncompressed_size, compressed)| {
                        Box::pin(async move {
                            debug!(
                                "uncompressed output: {}",
                                print_bytes(uncompressed_size as f64)
                            );
                            if let Some(cached) = compressed {
                                debug!("compressed output: {}", print_bytes(cached.len() as f64));
                                cache
                                    .set(&cache_key, cached)
                                    .await
                                    .context("writing to cache")?
                            }
                            Ok(())
                        })
                    }),
                )?;

                Ok(Box::pin(inp))
            }
        }
    } else {
        debug!("cache DISABLED, running adapter directly...");
        let inp = loop_adapt(adapter.as_ref(), detection_reason, ai, active_adapters).await?;
        Ok(concat_read_streams(inp))
    }
}

async fn read_discard(mut x: ReadBox) -> Result<()> {
    let mut buf = [0u8; 1 << 16];
    loop {
        let n = x.read(&mut buf).await?;
        if n == 0 {
            break;
        }
    }
    Ok(())
}

/// Flatten an error into a single line for the `[rga: ...]` markers
/// (search results are line-based; raw multi-line errors would garble them).
pub fn one_line_error(err: &anyhow::Error) -> String {
    let msg = format!("{err:#}")
        .replace('\n', " | ")
        .trim_end_matches(" | ")
        .trim()
        .to_string();
    let mut chars = msg.chars();
    let truncated: String = chars.by_ref().take(300).collect();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

/// Build a one-line AdaptInfo flagging a preprocessing failure, following the
/// same convention as the other `[rga: ...]` markers: the degradation stays
/// visible in the output and is searchable (`rga "preprocessing failed"`
/// finds all broken files) instead of aborting the whole search with
/// ripgrep's exit code 2. https://github.com/phiresky/ripgrep-all/issues/151
fn marker_entry(
    line_prefix: &str,
    config: &RgaConfig,
    archive_recursion_depth: i32,
    path: &Path,
    context: &str,
    err: anyhow::Error,
) -> AdaptInfo {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string_lossy().into_owned());
    let context = if context.is_empty() {
        String::new()
    } else {
        format!("{context}: ")
    };
    let line = format!(
        "{line_prefix}[rga: preprocessing failed: {context}{}: {}]\n",
        name,
        one_line_error(&err)
    );
    AdaptInfo {
        filepath_hint: path.to_path_buf(),
        is_real_file: false,
        file_mtime_unix_ms: None,
        archive_recursion_depth,
        inp: Box::pin(Cursor::new(line.into_bytes())),
        line_prefix: line_prefix.to_string(),
        // the marker is final text; don't run it through postprocessing
        postprocess: false,
        config: config.clone(),
    }
}

/// Error while copying adapter output to the final destination, split by
/// side so callers can tell adapter failures (→ searchable marker line,
/// issue #151) apart from output-write failures (e.g. closed pipe).
pub enum AdapterCopyError {
    Input(std::io::Error),
    Output(std::io::Error),
}

/// Copy adapter output chunk by chunk; unlike `tokio::io::copy` this
/// distinguishes read errors from write errors.
pub async fn copy_adapter_output(
    inp: &mut ReadBox,
    out: &mut (impl tokio::io::AsyncWrite + Unpin),
) -> std::result::Result<(), AdapterCopyError> {
    use tokio::io::AsyncWriteExt;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = inp
            .read(&mut buf)
            .await
            .map_err(AdapterCopyError::Input)?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n])
            .await
            .map_err(AdapterCopyError::Output)?;
    }
    out.flush().await.map_err(AdapterCopyError::Output)?;
    std::result::Result::Ok(())
}

pub fn loop_adapt(
    adapter: &dyn FileAdapter,
    detection_reason: FileMatcher,
    ai: AdaptInfo,
    active_adapters: ActiveAdapters,
) -> Pin<Box<dyn Future<Output = anyhow::Result<AdaptedFilesIterBox>> + Send + '_>> {
    Box::pin(async move { loop_adapt_inner(adapter, detection_reason, ai, active_adapters).await })
}
pub async fn loop_adapt_inner(
    adapter: &dyn FileAdapter,
    detection_reason: FileMatcher,
    ai: AdaptInfo,
    active_adapters: ActiveAdapters,
) -> anyhow::Result<AdaptedFilesIterBox> {
    let fph = ai.filepath_hint.clone();
    let inp = adapter.adapt(ai, &detection_reason).await;
    let inp = if adapter.metadata().name == "postprocprefix" {
        // don't add confusing error context
        inp?
    } else {
        inp.with_context(|| {
            format!(
                "adapting {} via {} failed",
                fph.to_string_lossy(),
                adapter.metadata().name
            )
        })?
    };
    let s = stream! {
        for await file in inp {
            trace!("next file");
            let file = file?;
            // MS Office owner/lock files inside archives are never real
            // documents — drain and skip instead of failing (issue #151)
            if file
                .filepath_hint
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("~$"))
            {
                debug!("skipping MS Office lock file {}", file.filepath_hint.to_string_lossy());
                read_discard(file.inp).await?;
                continue;
            }
            match buf_choose_adapter(file, &[], Some(&active_adapters)).await? {
                Ret::Recurse(ai, adapter, detection_reason, _active_adapters) => {
                    if ai.archive_recursion_depth >= ai.config.max_archive_recursion.0 {
                        // some adapters (esp. zip) assume that the entry is read fully and might hang otherwise
                        read_discard(ai.inp).await?;
                        let s = format!("{}[rga: max archive recursion reached ({})]\n", ai.line_prefix, ai.archive_recursion_depth).into_bytes();
                        yield Ok(AdaptInfo {
                            inp: Box::pin(Cursor::new(s)),
                            ..ai
                        });
                        continue;
                    }
                    debug!(
                        "Chose adapter '{}' because of matcher {:?}",
                        adapter.metadata().name, detection_reason
                    );
                    eprintln!(
                        "{} adapter: {}",
                        ai.filepath_hint.to_string_lossy(),
                        adapter.metadata().name
                    );
                    for await ifile in loop_adapt(adapter.as_ref(), detection_reason, ai, active_adapters.clone()).await? {
                        yield ifile;
                    }
                }
                Ret::Passthrough(ai) => {
                    debug!("no adapter for {}, ending recursion", ai.filepath_hint.to_string_lossy());
                    yield Ok(ai);
                }
            }
            trace!("done with files");
        }
        trace!("stream ended");
    };
    Ok(Box::pin(s))
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::adapters::custom::CustomAdapterConfig;
    use crate::test_utils::simple_adapt_info;
    use std::io::Cursor;
    use std::path::PathBuf;

    /// A custom adapter that matches `.failtest` and always fails hard
    /// (exits 1 with no output) — deterministic stand-in for e.g. pandoc
    /// choking on a corrupt file, without needing external binaries.
    /// A custom adapter that matches `.failtest` and always fails hard at
    /// adapt time (its binary does not exist, so spawning fails) —
    /// deterministic stand-in for e.g. pandoc choking on a corrupt file,
    /// without needing external binaries.
    fn failing_adapter_config() -> CustomAdapterConfig {
        CustomAdapterConfig {
            name: "failing".to_string(),
            description: "always fails".to_string(),
            disabled_by_default: None,
            version: 1,
            extensions: vec!["failtest".to_string()],
            mimetypes: None,
            binary: "rga-test-nonexistent-binary".to_string(),
            args: vec![],
            match_only_by_mime: None,
            output_path_hint: None,
            bail_if_empty_output: None,
        }
    }


    fn config_with_failing_adapter() -> RgaConfig {
        let mut config = RgaConfig::default();
        config.cache.disabled = true;
        config.custom_adapters = Some(vec![failing_adapter_config()]);
        config
    }

    async fn preproc_to_string(filepath: &str, content: Vec<u8>, config: RgaConfig) -> Result<String> {
        let (ai, _) = simple_adapt_info(&PathBuf::from(filepath), Box::pin(Cursor::new(content)));
        let ai = AdaptInfo { config, ..ai };
        let mut out = rga_preproc(ai).await?;
        let mut buf = Vec::new();
        out.read_to_end(&mut buf).await?;
        Ok(String::from_utf8(buf)?)
    }

    #[tokio::test]
    async fn hard_adapter_failure_emits_searchable_marker() -> Result<()> {
        // a hard adapter error (not an AdapterBail) used to abort rga-preproc
        // with a non-zero exit, making rg report a preprocessor failure and
        // exit 2. Now it degrades to a `[rga: ...]` marker line (issue #151).
        let text = preproc_to_string("broken.failtest", b"content".to_vec(), config_with_failing_adapter()).await?;
        assert!(
            text.contains("[rga: preprocessing failed"),
            "expected marker in output: {text:?}"
        );
        assert!(text.contains("failing"), "expected adapter name: {text:?}");
        Ok(())
    }

    #[tokio::test]
    async fn zip_skips_office_lock_files() -> Result<()> {
        use async_zip::{Compression, ZipEntryBuilder, base::write::ZipFileWriter};
        let mut cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipFileWriter::with_tokio(&mut cursor);
        // an ordinary member that must come through unchanged
        writer
            .write_entry_whole(
                ZipEntryBuilder::new("normal.txt".into(), Compression::Stored),
                b"normal text file",
            )
            .await?;
        // an MS Office owner/lock file: never a real document, must be skipped
        writer
            .write_entry_whole(
                ZipEntryBuilder::new("~$lock.docx".into(), Compression::Stored),
                b"garbage lock content",
            )
            .await?;
        writer.close().await?;
        let archive = cursor.into_inner();

        let text = preproc_to_string("test.zip", archive, config_with_failing_adapter()).await?;
        assert!(text.contains("normal text file"), "missing normal member: {text:?}");
        assert!(
            !text.contains("garbage lock content"),
            "lock file content must be skipped: {text:?}"
        );
        Ok(())
    }

    #[test]
    fn one_line_error_flattens_and_trims() {
        let err = format_err!("line1\nline2\n");
        assert_eq!(one_line_error(&err), "line1 | line2");

        let flattened = one_line_error(&anyhow::Error::msg("x".repeat(500)));
        assert!(flattened.ends_with('…'), "{flattened:?}");
        assert!(flattened.chars().count() <= 301, "{flattened:?}");
    }
}
