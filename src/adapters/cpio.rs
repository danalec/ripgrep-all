//! Adapter for cpio archives in the SVR4 "newc" format (the layout used by
//! initramfs images, rpm2cpio output and `cpio -o -H newc`).
//!
//! The format is a flat sequence of fixed-size records:
//! ```text
//! "070701" | 13 × 8-char hex fields | name (NUL-terminated, padded to 4)
//!          | file data (padded to 4) | ... | "TRAILER!!!" entry
//! ```
//! Regular files are yielded as nested AdaptInfos like the tar adapter;
//! directories and device nodes are skipped.

use crate::adapted_iter::AdaptedFilesIterBox;
use crate::adapters::AdapterMeta;
use crate::matching::{FastFileMatcher, FileMatcher};

use anyhow::{Context, Result, bail, format_err};
use async_stream::stream;
use async_trait::async_trait;
use lazy_static::lazy_static;
use std::path::PathBuf;
use tokio_stream::StreamExt;

use super::{AdaptInfo, FileAdapter, GetMetadata};

static EXTENSIONS: &[&str] = &["cpio"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "cpio".to_owned(),
        version: 1,
        description: "Reads a cpio (newc) archive and recurses down into its contents".to_owned(),
        recurses: true,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false
    };
}

#[derive(Default, Clone)]
pub struct CpioAdapter;

impl CpioAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for CpioAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

const HEADER_LEN: usize = 6 + 13 * 8; // magic + hex fields
const MAX_ENTRY: usize = 1 << 30;

struct NewcEntry {
    path: PathBuf,
    mode: u32,
    /// offset and length of the file data within the archive buffer
    data_off: usize,
    data_len: usize,
}

/// parse the whole archive buffer into a list of regular-file entries.
/// newc records are small and the padding rules need random access, so
/// buffering beats trying to stream the walk.
fn parse_newc(data: &[u8]) -> Result<Vec<NewcEntry>> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    loop {
        if pos + HEADER_LEN > data.len() {
            bail!("cpio: truncated header at offset {pos}");
        }
        if &data[pos..pos + 6] != b"070701" && &data[pos..pos + 6] != b"070702" {
            bail!("cpio: bad magic at offset {pos}");
        }
        let field = |i: usize| -> Result<usize> {
            let s = std::str::from_utf8(&data[pos + 6 + i * 8..pos + 6 + i * 8 + 8])
                .context("cpio: header field is not ascii")?;
            usize::from_str_radix(s, 16).context("cpio: header field is not hex")
        };
        let mode = field(1)? as u32;
        let filesize = field(6)?;
        let namesize = field(11)?;
        if filesize > MAX_ENTRY || namesize > MAX_ENTRY {
            bail!("cpio: implausible entry size");
        }
        let name_start = pos + HEADER_LEN;
        let name_end = name_start
            .checked_add(namesize)
            .context("cpio: name offset overflow")?;
        if name_end > data.len() {
            bail!("cpio: name overruns archive");
        }
        let name_bytes = &data[name_start..name_end];
        let name = match name_bytes.iter().position(|b| *b == 0) {
            Some(nul) => &name_bytes[..nul],
            None => bail!("cpio: name is not NUL-terminated"),
        };
        if name == b"TRAILER!!!" {
            break;
        }
        // data starts after the name, padded to a 4-byte boundary
        let data_off = align4(name_end);
        let data_end = data_off
            .checked_add(filesize)
            .context("cpio: data offset overflow")?;
        if data_end > data.len() {
            bail!("cpio: file data overruns archive");
        }
        let is_regular = mode & 0o170000 == 0o100000;
        if is_regular && !name.is_empty() {
            entries.push(NewcEntry {
                path: PathBuf::from(String::from_utf8_lossy(name).into_owned()),
                mode,
                data_off,
                data_len: filesize,
            });
        }
        pos = align4(data_end);
    }
    Ok(entries)
}

const fn align4(n: usize) -> usize {
    (n + 3) & !3
}

#[async_trait]
impl FileAdapter for CpioAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let AdaptInfo {
            filepath_hint: _,
            inp,
            line_prefix,
            archive_recursion_depth,
            config,
            postprocess,
            ..
        } = ai;
        let mut data = Vec::new();
        let mut inp = inp;
        tokio::io::AsyncReadExt::read_to_end(&mut inp, &mut data).await?;
        let entries = parse_newc(&data)?;
        if entries.is_empty() {
            bail!("cpio: archive has no regular files");
        }
        let s = stream! {
            for e in entries {
                let line_prefix = format!("{}{}: ", line_prefix, e.path.display());
                let ai2 = AdaptInfo {
                    filepath_hint: e.path.clone(),
                    is_real_file: false,
                    file_mtime_unix_ms: None,
                    archive_recursion_depth: archive_recursion_depth + 1,
                    inp: Box::pin(std::io::Cursor::new(
                        data[e.data_off..e.data_off + e.data_len].to_vec(),
                    )),
                    line_prefix,
                    config: config.clone(),
                    postprocess,
                };
                yield Ok(ai2);
            }
        };
        Ok(Box::pin(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::assert_eq;

    fn newc_entry(name: &str, mode: u32, content: &[u8]) -> Vec<u8> {
        let mut v = b"070701".to_vec();
        let fields = [
            1u32, // ino
            mode, // mode
            0,
            0,
            1,
            0,                    // uid, gid, nlink, mtime
            content.len() as u32, // filesize
            0,
            0,
            0,
            0,                     // devmajor, devminor, rdevmajor, rdevminor
            name.len() as u32 + 1, // namesize (includes NUL)
            0,                     // check
        ];
        for f in fields {
            v.extend_from_slice(format!("{f:08x}").as_bytes());
        }
        v.extend_from_slice(name.as_bytes());
        v.push(0);
        while v.len() % 4 != 0 {
            v.push(0);
        }
        v.extend_from_slice(content);
        while v.len() % 4 != 0 {
            v.push(0);
        }
        v
    }

    fn newc_trailer() -> Vec<u8> {
        newc_entry("TRAILER!!!", 0, b"")
    }

    #[tokio::test]
    async fn cpio_two_files() -> Result<()> {
        let mut archive = newc_entry("hello.txt", 0o100644, b"hello from cpio\n");
        archive.extend_from_slice(&newc_entry("dir/world.txt", 0o100644, b"world\n"));
        archive.extend_from_slice(&newc_trailer());

        let (a, d) = simple_adapt_info(
            std::path::Path::new("initrd.cpio"),
            Box::pin(std::io::Cursor::new(archive)),
        );
        let mut it = CpioAdapter.adapt(a, &d).await?;
        use tokio::io::AsyncReadExt;
        let mut collected = Vec::new();
        while let Some(ai) = tokio_stream::StreamExt::next(&mut it).await {
            let mut ai = ai?;
            let mut text = String::new();
            ai.inp.read_to_string(&mut text).await?;
            collected.push(format!("{}{}", ai.line_prefix, text));
        }
        assert_eq!(
            collected.join(""),
            "PREFIX:hello.txt: hello from cpio\nPREFIX:dir/world.txt: world\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cpio_rejects_bad_magic() {
        let (a, d) = simple_adapt_info(
            std::path::Path::new("x.cpio"),
            Box::pin(std::io::Cursor::new(vec![0u8; 256])),
        );
        let res = CpioAdapter.adapt(a, &d).await;
        assert!(
            res.err()
                .expect("should fail")
                .to_string()
                .contains("bad magic")
        );
    }

    #[tokio::test]
    async fn cpio_skips_directories() -> Result<()> {
        let mut archive = newc_entry("adir", 0o40755, b"");
        archive.extend_from_slice(&newc_entry("adir/file.txt", 0o100644, b"nested\n"));
        archive.extend_from_slice(&newc_trailer());

        let (a, d) = simple_adapt_info(
            std::path::Path::new("initrd.cpio"),
            Box::pin(std::io::Cursor::new(archive)),
        );
        let mut it = CpioAdapter.adapt(a, &d).await?;
        use tokio::io::AsyncReadExt;
        let mut names = Vec::new();
        while let Some(ai) = tokio_stream::StreamExt::next(&mut it).await {
            let mut ai = ai?;
            let mut text = String::new();
            ai.inp.read_to_string(&mut text).await?;
            names.push(format!("{}{}", ai.line_prefix, text));
        }
        assert_eq!(names, vec!["PREFIX:adir/file.txt: nested\n"]);
        Ok(())
    }
}
