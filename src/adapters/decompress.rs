use crate::adapted_iter::one_file;

use super::*;

use anyhow::Result;
use lazy_static::lazy_static;
use tokio::io::BufReader;

use std::io::{Cursor, Read as _};
use std::path::{Path, PathBuf};

static EXTENSIONS: &[&str] = &[
    "als", "br", "bz", "bz2", "bzip2", "gz", "lz4", "lzma", "taz", "tb2", "tbz", "tbz2", "tgz",
    "tlz", "tpz", "txz", "tz2", "tzst", "xz", "z", "zst", "zstd",
];
static MIME_TYPES: &[&str] = &[
    "application/gzip",
    "application/x-bzip",
    "application/x-xz",
    "application/zstd",
    "application/x-brotli",
    "application/x-lz4",
    "application/x-lzma",
    "application/x-compress",
];
lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "decompress".to_owned(),
        version: 1,
        description:
            "Reads compressed file as a stream and runs a different extractor on the contents."
                .to_owned(),
        recurses: true,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: Some(
            MIME_TYPES
                .iter()
                .map(|s| FileMatcher::MimeType(s.to_string()))
                .collect()
        ),
        disabled_by_default: false,
        keep_fast_matchers_if_accurate: true
    };
}
#[derive(Default)]
pub struct DecompressAdapter;

impl DecompressAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for DecompressAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

/// gzip multi-member decoder: async-compression's GzipDecoder stops at the
/// end of the first member, but concatenated .gz files (e.g. `cat a.gz b.gz`)
/// are common enough (log rotations, pigz --conc) to support. When the first
/// decoder reaches EOF, its buffered reader is reused for the next member.
struct MultiGzip<R: tokio::io::AsyncBufRead + Unpin> {
    reader: Option<async_compression::tokio::bufread::GzipDecoder<R>>,
    /// reader between members: waiting for poll_fill_buf to decide whether
    /// another member follows
    pending: Option<R>,
    done: bool,
}

impl<R: tokio::io::AsyncBufRead + Unpin> MultiGzip<R> {
    fn new(inner: R) -> Self {
        Self {
            reader: Some(async_compression::tokio::bufread::GzipDecoder::new(inner)),
            pending: None,
            done: false,
        }
    }
}

impl<R: tokio::io::AsyncBufRead + Unpin> tokio::io::AsyncRead for MultiGzip<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        use tokio::io::AsyncBufRead;
        loop {
            if self.done {
                return Poll::Ready(Ok(()));
            }
            // between members: is there another member in the buffer?
            if let Some(inner) = self.pending.as_mut() {
                match std::pin::Pin::new(inner).poll_fill_buf(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(rest)) => {
                        if rest.is_empty() {
                            self.done = true;
                            self.pending = None;
                            return Poll::Ready(Ok(()));
                        }
                        let inner = self.pending.take().unwrap();
                        self.reader =
                            Some(async_compression::tokio::bufread::GzipDecoder::new(inner));
                    }
                }
            }
            let Some(dec) = self.reader.as_mut() else {
                self.done = true;
                return Poll::Ready(Ok(()));
            };
            // note: callers may reuse `buf` across polls, so EOF is
            // "no NEW bytes this call", not "buf empty"
            let before = buf.filled().len();
            match std::pin::Pin::new(dec).poll_read(cx, buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    if buf.filled().len() == before {
                        // member finished: move the reader to `pending` and
                        // loop back to the fill_buf check above
                        let dec = self.reader.take().unwrap();
                        self.pending = Some(dec.into_inner());
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}

/// streaming decoders wrap the input directly; sync-crate decoders
/// (lz4/lzma/compress) buffer the whole compressed input in memory
async fn decompress_any(reason: &FileMatcher, inp: ReadBox) -> Result<ReadBox> {
    use FastFileMatcher::*;
    use FileMatcher::*;
    use async_compression::tokio::bufread;
    let gz = |inp: ReadBox| -> ReadBox { Box::pin(MultiGzip::new(BufReader::new(inp))) };
    let bz2 = |inp: ReadBox| Box::pin(bufread::BzDecoder::new(BufReader::new(inp)));
    let xz = |inp: ReadBox| Box::pin(bufread::XzDecoder::new(BufReader::new(inp)));
    let zst = |inp: ReadBox| Box::pin(bufread::ZstdDecoder::new(BufReader::new(inp)));
    let br = |inp: ReadBox| Box::pin(bufread::BrotliDecoder::new(BufReader::new(inp)));

    // whole-input sync decoders
    let read_all = |mut inp: ReadBox| async move {
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut inp, &mut buf).await?;
        Ok::<_, anyhow::Error>(buf)
    };
    let lz4 = |buf: &[u8]| -> Result<Vec<u8>> {
        let mut out = Vec::new();
        lz4_flex::frame::FrameDecoder::new(buf)
            .read_to_end(&mut out)
            .map_err(|e| format_err!("lz4 decode failed: {e}"))?;
        Ok(out)
    };
    let lzma = |buf: &[u8]| -> Result<Vec<u8>> {
        let stream = liblzma::stream::Stream::new_lzma_decoder(1 << 30)
            .map_err(|e| format_err!("lzma init failed: {e}"))?;
        let mut out = Vec::new();
        liblzma::read::XzDecoder::new_stream(buf, stream)
            .read_to_end(&mut out)
            .map_err(|e| format_err!("lzma decode failed: {e}"))?;
        Ok(out)
    };
    let compress_z = |buf: &[u8]| -> Result<Vec<u8>> {
        lzw_z::decompress_slice(buf).map_err(|e| format_err!("compress (.Z) decode failed: {e}"))
    };

    Ok(match reason {
        Fast(FileExtension(ext)) => match ext.to_ascii_lowercase().as_str() {
            "als" | "gz" | "taz" | "tgz" | "tpz" => gz(inp),
            "bz" | "bz2" | "bzip2" | "tb2" | "tbz" | "tbz2" | "tz2" => bz2(inp),
            "zst" | "zstd" | "tzst" => zst(inp),
            "xz" | "txz" => xz(inp),
            "br" => br(inp),
            "lz4" => Box::pin(Cursor::new(lz4(&read_all(inp).await?)?)),
            "lzma" | "tlz" => Box::pin(Cursor::new(lzma(&read_all(inp).await?)?)),
            // extension matching is case-insensitive, so this arm covers .Z
            "z" => Box::pin(Cursor::new(compress_z(&read_all(inp).await?)?)),
            ext => Err(format_err!("don't know how to decompress {}", ext))?,
        },
        MimeType(mime) => match mime.as_ref() {
            "application/gzip" => gz(inp),
            "application/x-bzip" => bz2(inp),
            "application/x-xz" => xz(inp),
            "application/zstd" => zst(inp),
            "application/x-brotli" => br(inp),
            "application/x-lz4" => Box::pin(Cursor::new(lz4(&read_all(inp).await?)?)),
            "application/x-lzma" => Box::pin(Cursor::new(lzma(&read_all(inp).await?)?)),
            "application/x-compress" => Box::pin(Cursor::new(compress_z(&read_all(inp).await?)?)),
            mime => Err(format_err!("don't know how to decompress mime {}", mime))?,
        },
        other => Err(format_err!("unsupported detection reason {:?}", other))?,
    })
}
fn get_inner_filename(filename: &Path) -> PathBuf {
    let extension = filename
        .extension()
        .map(|e| Cow::Owned(e.to_string_lossy().to_ascii_lowercase()))
        .unwrap_or(Cow::Borrowed(""));
    let stem = filename
        .file_stem()
        .expect("no filename given?")
        .to_string_lossy();
    let new_extension = match extension.as_ref() {
        // tar short suffixes: the decompressed content is a tar stream
        "tgz" | "taz" | "tpz" | "tb2" | "tbz" | "tbz2" | "tz2" | "txz" | "tzst" | "tlz" => ".tar",
        _other => "",
    };
    filename.with_file_name(format!("{}{}", stem, new_extension))
}

#[async_trait]
impl FileAdapter for DecompressAdapter {
    async fn adapt(
        &self,
        ai: AdaptInfo,
        detection_reason: &FileMatcher,
    ) -> Result<AdaptedFilesIterBox> {
        Ok(one_file(AdaptInfo {
            filepath_hint: get_inner_filename(&ai.filepath_hint),
            is_real_file: false,
            file_mtime_unix_ms: None,
            archive_recursion_depth: ai.archive_recursion_depth + 1,
            inp: decompress_any(detection_reason, ai.inp).await?,
            line_prefix: ai.line_prefix,
            config: ai.config.clone(),
            postprocess: ai.postprocess,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preproc::loop_adapt;
    use crate::test_utils::*;
    use pretty_assertions::assert_eq;
    use tokio::fs::File;

    #[test]
    fn test_inner_filename() {
        for (a, b) in &[
            ("hi/test.tgz", "hi/test.tar"),
            ("hi/hello.gz", "hi/hello"),
            ("a/b/initramfs", "a/b/initramfs"),
            ("hi/test.tbz2", "hi/test.tar"),
            ("hi/test.tbz", "hi/test.tar"),
            ("hi/test.hi.bz2", "hi/test.hi"),
            ("hello.tar.gz", "hello.tar"),
            // ugrep short tar suffixes (matching is case-insensitive)
            ("hi/test.taz", "hi/test.tar"),
            ("hi/test.TAZ", "hi/test.tar"),
            ("hi/test.tpz", "hi/test.tar"),
            ("hi/test.tb2", "hi/test.tar"),
            ("hi/test.tz2", "hi/test.tar"),
            ("hi/test.txz", "hi/test.tar"),
            ("hi/test.tzst", "hi/test.tar"),
            ("hi/test.tlz", "hi/test.tar"),
            ("hi/data.txz", "hi/data.tar"),
            ("hi/data.xz", "hi/data"),
            ("hi/data.br", "hi/data"),
            ("hi/data.lz4", "hi/data"),
            ("hi/data.lzma", "hi/data"),
            ("hi/data.zstd", "hi/data"),
            ("hi/data.Z", "hi/data"),
        ] {
            assert_eq!(get_inner_filename(&PathBuf::from(a)), PathBuf::from(*b));
        }
    }

    #[tokio::test]
    async fn multigzip_file_single_member() -> Result<()> {
        let f = File::open(test_data_dir().join("hello.gz")).await?;
        let mut mg = MultiGzip::new(BufReader::new(Box::pin(f) as ReadBox));
        let mut out = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut mg, &mut out).await?;
        assert_eq!(out, b"hello\n");
        Ok(())
    }

    #[tokio::test]
    async fn gz() -> Result<()> {
        let adapter = DecompressAdapter;

        let filepath = test_data_dir().join("hello.gz");

        let (a, d) = simple_adapt_info(&filepath, Box::pin(File::open(&filepath).await?));
        let r = adapter.adapt(a, &d).await?;
        let o = adapted_to_vec(r).await?;
        assert_eq!(String::from_utf8(o)?, "hello\n");
        Ok(())
    }

    #[tokio::test]
    async fn pdf_gz() -> Result<()> {
        let adapter = DecompressAdapter;

        let filepath = test_data_dir().join("short.pdf.gz");

        let (a, d) = simple_adapt_info(&filepath, Box::pin(File::open(&filepath).await?));
        let r = loop_adapt(&adapter, d, a, crate::adapters::get_all_adapters(None).0).await?;
        let o = adapted_to_vec(r).await?;
        assert_eq!(
            String::from_utf8(o)?,
            "PREFIX:Page 1: hello world
PREFIX:Page 1: this is just a test.
PREFIX:Page 1:
PREFIX:Page 1: 1
PREFIX:Page 1:
PREFIX:Page 1:
"
        );
        Ok(())
    }

    async fn adapt_ext(name: &str, data: Vec<u8>) -> Result<String> {
        let adapter = DecompressAdapter;
        let (a, d) = simple_adapt_info(std::path::Path::new(name), Box::pin(Cursor::new(data)));
        let r = adapter.adapt(a, &d).await?;
        Ok(String::from_utf8(adapted_to_vec(r).await?)?)
    }

    #[tokio::test]
    async fn brotli_roundtrip() -> Result<()> {
        use async_compression::tokio::write::BrotliEncoder;
        use tokio::io::AsyncWriteExt;
        let mut enc = BrotliEncoder::new(Vec::new());
        enc.write_all(b"brotli payload").await?;
        enc.shutdown().await?;
        let compressed = enc.into_inner();
        assert_eq!(adapt_ext("x.br", compressed).await?, "brotli payload");
        Ok(())
    }

    #[tokio::test]
    async fn lz4_roundtrip() -> Result<()> {
        use std::io::Write as _;
        let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
        enc.write_all(b"lz4 payload")?;
        let compressed = enc.finish()?;
        assert_eq!(adapt_ext("x.lz4", compressed).await?, "lz4 payload");
        Ok(())
    }

    #[tokio::test]
    async fn lzma_roundtrip() -> Result<()> {
        use std::io::Read as _;
        let stream = liblzma::stream::Stream::new_lzma_encoder(
            &liblzma::stream::LzmaOptions::new_preset(6)?,
        )?;
        let mut enc = liblzma::read::XzEncoder::new_stream(b"lzma payload".as_slice(), stream);
        let mut compressed = Vec::new();
        enc.read_to_end(&mut compressed)?;
        assert_eq!(
            adapt_ext("x.lzma", compressed.clone()).await?,
            "lzma payload"
        );
        // .tlz is the tar short suffix for lzma-alone streams
        assert_eq!(adapt_ext("x.tlz", compressed).await?, "lzma payload");
        Ok(())
    }

    #[tokio::test]
    async fn concatenated_gzip_streams() -> Result<()> {
        use async_compression::tokio::write::GzipEncoder;
        use tokio::io::AsyncWriteExt;
        let mut enc1 = GzipEncoder::new(Vec::new());
        enc1.write_all(b"first\n").await?;
        enc1.shutdown().await?;
        let mut compressed = enc1.into_inner();
        let mut enc2 = GzipEncoder::new(Vec::new());
        enc2.write_all(b"second\n").await?;
        enc2.shutdown().await?;
        compressed.extend_from_slice(&enc2.into_inner());
        assert_eq!(adapt_ext("x.gz", compressed).await?, "first\nsecond\n");
        Ok(())
    }

    #[tokio::test]
    async fn compress_z_garbage_errors() {
        // a .Z file must start with the 1f 9d magic; anything else errors
        let res = adapt_ext("x.z", b"not compress data".to_vec()).await;
        assert!(res.is_err(), "expected error, got {res:?}");
    }
}
