use super::*;
use crate::print_bytes;
use anyhow::*;
use async_stream::stream;
use lazy_static::lazy_static;
use log::*;
use sevenz_rust2::{ArchiveReader, Password};
use std::io::{Cursor, Read, Seek};

// TODO: allow users to configure file extensions instead of hard coding the list
// https://github.com/phiresky/ripgrep-all/pull/208#issuecomment-2173241243
static EXTENSIONS: &[&str] = &["7z", "cb7"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "7z".to_owned(),
        version: 1,
        description: "Reads a 7z file and recurses down into its contents".to_owned(),
        recurses: true,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: Some(vec![FileMatcher::MimeType(
            "application/x-7z-compressed".to_owned()
        )]),
        keep_fast_matchers_if_accurate: false,
        disabled_by_default: false
    };
}
#[derive(Default, Clone)]
pub struct SevenZAdapter;

impl SevenZAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for SevenZAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

/// The 7z container needs `Read + Seek` (the header lives at the end of the
/// file), so unlike the zip adapter we cannot decode a plain stream. Real
/// files are read from disk; a streamed input (a 7z nested inside another
/// archive) is buffered whole in memory first.
trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

#[async_trait]
impl FileAdapter for SevenZAdapter {
    async fn adapt(
        &self,
        ai: AdaptInfo,
        _detection_reason: &FileMatcher,
    ) -> Result<AdaptedFilesIterBox> {
        let AdaptInfo {
            inp,
            filepath_hint,
            archive_recursion_depth,
            postprocess,
            line_prefix,
            config,
            is_real_file,
            ..
        } = ai;

        let source: Box<dyn ReadSeek + Send> = if is_real_file {
            Box::new(std::fs::File::open(&filepath_hint)?)
        } else {
            use tokio::io::AsyncReadExt;
            let mut buf = Vec::new();
            let mut inp = inp;
            inp.read_to_end(&mut buf).await?;
            debug!(
                "{}buffered {} of nested 7z input",
                line_prefix,
                print_bytes(buf.len() as f64)
            );
            Box::new(Cursor::new(buf))
        };

        // sevenz-rust2 decodes synchronously; run it on the blocking pool and
        // hand entries over one at a time through a bounded channel so the
        // async consumer applies backpressure and at most one entry is
        // buffered in memory.
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Result<(String, u64, u64, Vec<u8>)>>(1);
        tokio::task::spawn_blocking(move || {
            let result: Result<()> = (|| {
                let mut archive = ArchiveReader::new(source, Password::empty())?;
                archive.for_each_entries(
                    // NB: `use anyhow::*` exports an `anyhow::Ok` variant that
                    // defaults the error type to anyhow::Error and shadows the
                    // std `Ok`, so the closure returns must be fully qualified.
                    |entry, entry_reader| -> std::result::Result<bool, sevenz_rust2::Error> {
                        if entry.is_directory || entry.is_anti_item || !entry.has_stream {
                            return std::result::Result::Ok(true);
                        }
                        // Cap the initial allocation: entry.size comes from the
                        // archive header and must not be trusted blindly.
                        let mut buf = Vec::with_capacity((entry.size as usize).min(1 << 20));
                        entry_reader
                            .read_to_end(&mut buf)
                            .map_err(|e| sevenz_rust2::Error::Io(e, "entry".into()))?;
                        tx.blocking_send(Ok((
                            entry.name.clone(),
                            entry.size,
                            entry.compressed_size,
                            buf,
                        )))
                        .map_err(|_| {
                            sevenz_rust2::Error::Other("7z entry receiver dropped".into())
                        })?;
                        std::result::Result::Ok(true)
                    },
                )?;
                Ok(())
            })();
            if let Err(e) = result {
                // The consumer may already be gone; ignore send failure.
                let _ = tx.blocking_send(Err(e));
            }
        });

        let s = stream! {
            while let Some(item) = rx.recv().await {
                let (filename, size, compressed_size, buf) = item?;
                if filename.ends_with('/') {
                    continue;
                }
                debug!(
                    "{}{}|{}: {} ({} packed)",
                    line_prefix,
                    filepath_hint.display(),
                    filename,
                    print_bytes(size as f64),
                    print_bytes(compressed_size as f64)
                );
                let new_line_prefix = format!("{}{}: ", line_prefix, filename);
                yield Ok(AdaptInfo {
                    filepath_hint: PathBuf::from(filename),
                    is_real_file: false,
                    file_mtime_unix_ms: None,
                    inp: Box::pin(Cursor::new(buf)),
                    line_prefix: new_line_prefix,
                    archive_recursion_depth: archive_recursion_depth + 1,
                    postprocess,
                    config: config.clone(),
                });
            }
        };

        Ok(Box::pin(s))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{preproc::loop_adapt, test_utils::*};
    use pretty_assertions::assert_eq;
    use sevenz_rust2::{ArchiveEntry, ArchiveWriter};

    #[async_recursion::async_recursion]
    async fn create_7z(fname: &str, content: &str, add_inner: bool) -> Result<Vec<u8>> {
        let mut cursor = std::io::Cursor::new(Vec::new());
        {
            let mut sz = ArchiveWriter::new(&mut cursor)?;
            sz.push_archive_entry(
                ArchiveEntry::new_file(fname),
                Some(content.as_bytes()),
            )?;
            if add_inner {
                let inner = create_7z("inner.txt", "inner text file", false).await?;
                sz.push_archive_entry(
                    ArchiveEntry::new_file("inner.7z"),
                    Some(inner.as_slice()),
                )?;
            }
            sz.finish()?;
        }
        Ok(cursor.into_inner())
    }

    #[tokio::test]
    async fn directories_and_files() -> Result<()> {
        let mut cursor = std::io::Cursor::new(Vec::new());
        {
            let mut sz = ArchiveWriter::new(&mut cursor)?;
            sz.push_archive_entry(ArchiveEntry::new_directory("dir"), None::<&[u8]>)?;
            sz.push_archive_entry(
                ArchiveEntry::new_file("dir/first.txt"),
                Some("first".as_bytes()),
            )?;
            sz.push_archive_entry(
                ArchiveEntry::new_file("second.txt"),
                Some("second".as_bytes()),
            )?;
            sz.finish()?;
        }
        let bytes = cursor.into_inner();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.7z");
        tokio::fs::write(&path, &bytes).await?;
        // both the real-file (seek) and the nested-stream (buffered) path
        for is_real_file in [false, true] {
            let (mut ai, reason) =
                simple_adapt_info(&path, Box::pin(std::io::Cursor::new(bytes.clone())));
            ai.is_real_file = is_real_file;
            let output = adapted_to_vec(
                loop_adapt(
                    &SevenZAdapter::new(),
                    reason,
                    ai,
                    crate::adapters::get_all_adapters(None).0,
                )
                .await?,
            )
            .await?;
            assert_eq!(
                String::from_utf8(output)?,
                "PREFIX:dir/first.txt: first\nPREFIX:second.txt: second\n"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn recurse() -> Result<()> {
        let archive = create_7z("outer.txt", "outer text file", true).await?;
        let (ai, reason) = simple_adapt_info(
            &PathBuf::from("outer.7z"),
            Box::pin(std::io::Cursor::new(archive)),
        );
        let buf = adapted_to_vec(
            loop_adapt(
                &SevenZAdapter::new(),
                reason,
                ai,
                crate::adapters::get_all_adapters(None).0,
            )
            .await?,
        )
        .await?;
        assert_eq!(
            String::from_utf8(buf)?,
            "PREFIX:outer.txt: outer text file\nPREFIX:inner.7z: inner.txt: inner text file\n",
        );
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_file_is_rejected() -> Result<()> {
        let mut bytes = create_7z("file.txt", "original content", false).await?;
        // flip bytes in the middle of the archive (packed data area)
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        bytes[mid + 1] ^= 0xff;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("corrupt.7z");
        tokio::fs::write(&path, bytes).await?;
        let (ai, reason) = simple_fs_adapt_info(&path).await?;
        let result = adapted_to_vec(
            loop_adapt(
                &SevenZAdapter::new(),
                reason,
                ai,
                crate::adapters::get_all_adapters(None).0,
            )
            .await?,
        )
        .await;
        assert!(result.is_err(), "corrupted 7z content must fail");
        Ok(())
    }
}