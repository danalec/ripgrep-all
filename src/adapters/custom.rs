use super::*;
use super::{AdaptInfo, AdapterMeta, FileAdapter, GetMetadata};
use crate::adapted_iter::one_file;

use crate::{
    adapted_iter::AdaptedFilesIterBox,
    expand::expand_str_ez,
    matching::{FastFileMatcher, FileMatcher},
};
use crate::{join_handle_to_stream, to_io_err};
use anyhow::Result;
use async_stream::stream;
use bytes::Bytes;
use lazy_static::lazy_static;
use log::debug;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::process::Command;

use tokio_util::io::StreamReader;
// mostly the same as AdapterMeta + SpawningFileAdapter
#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, PartialEq, Clone)]
pub struct CustomAdapterConfig {
    /// The unique identifier and name of this adapter.
    ///
    /// Must only include a-z, 0-9, _.
    pub name: String,

    /// The description of this adapter shown in help.
    pub description: String,

    /// If true, the adapter will be disabled by default.
    pub disabled_by_default: Option<bool>,

    /// Version identifier used to key cache entries.
    ///
    /// Change this if the configuration or program changes.
    pub version: i32,

    /// The file extensions this adapter supports, for example `["epub", "mobi"]`.
    pub extensions: Vec<String>,

    /// If not null and `--rga-accurate` is enabled, mimetype matching is used instead of file name matching.
    pub mimetypes: Option<Vec<String>>,

    /// If `--rga-accurate`, only match by mime types and ignore extensions completely.
    pub match_only_by_mime: Option<bool>,

    /// The name or path of the binary to run.
    pub binary: String,

    /// The arguments to run the program with.
    /// Placeholders:
    /// - `$input_file_extension`: the file extension (without dot). e.g. foo.tar.gz -> gz
    /// - `$input_file_pandoc_format`: like `$input_file_extension`, but with pandoc-style
    ///   extension aliases applied (e.g. `htm` -> `html`), for use with pandoc's `--from=`
    /// - `$input_file_stem`: the file name without the last extension. e.g. foo.tar.gz -> foo.tar
    /// - `$input_virtual_path`: the full input file path.
    ///   Note that this path may not actually exist on disk because it is the result of another adapter.
    ///
    /// stdin of the program will be connected to the input file, and stdout is assumed to be the converted file
    pub args: Vec<String>,

    /// The output path hint.
    /// The placeholders are the same as for `.args`
    ///
    /// If not set, defaults to `"${input_virtual_path}.txt"`.
    ///
    /// Setting this is useful if the output format is not plain text (.txt) but instead some other format that should be passed to another adapter
    pub output_path_hint: Option<String>,

    /// If true, the adapter declines the file (bails out, see issue #3) when the
    /// spawned program produces no text output — e.g. pdftotext on a PDF that
    /// consists only of scanned images, which emits nothing but form-feed page
    /// separators. Another adapter (such as a user-configured OCR adapter
    /// matching the same extension) then gets a chance to handle it.
    ///
    /// "No text output" means the first chunk of output contains no bytes other
    /// than ASCII whitespace and control characters (<= 0x20 or 0x7F).
    ///
    /// Only effective for real files on disk; inside archives a bail is a hard error.
    pub bail_if_empty_output: Option<bool>,
}

fn strs(arr: &[&str]) -> Vec<String> {
    arr.iter().map(ToString::to_string).collect()
}

lazy_static! {
    pub static ref BUILTIN_SPAWNING_ADAPTERS: Vec<CustomAdapterConfig> = vec![
        // from https://github.com/jgm/pandoc/blob/master/src/Text/Pandoc/App/FormatHeuristics.hs
        // excluding formats that could cause problems (.db ?= sqlite) or that are already text formats (e.g. xml-based)
        //"db"       -> Just "docbook"
        //"adoc"     -> Just "asciidoc"
        //"asciidoc" -> Just "asciidoc"
        //"context"  -> Just "context"
        //"ctx"      -> Just "context"
        //"dokuwiki" -> Just "dokuwiki"
        //"htm"      -> Just "html"
        //"html"     -> Just "html"
        //"json"     -> Just "json"
        //"latex"    -> Just "latex"
        //"lhs"      -> Just "markdown+lhs"
        //"ltx"      -> Just "latex"
        //"markdown" -> Just "markdown"
        //"md"       -> Just "markdown"
        //"ms"       -> Just "ms"
        //"muse"     -> Just "muse"
        //"native"   -> Just "native"
        //"opml"     -> Just "opml"
        //"org"      -> Just "org"
        //"roff"     -> Just "ms"
        //"rst"      -> Just "rst"
        //"s5"       -> Just "s5"
        //"t2t"      -> Just "t2t"
        //"tei"      -> Just "tei"
        //"tei.xml"  -> Just "tei"
        //"tex"      -> Just "latex"
        //"texi"     -> Just "texinfo"
        //"texinfo"  -> Just "texinfo"
        //"textile"  -> Just "textile"
        //"text"     -> Just "markdown"
        //"txt"      -> Just "markdown"
        //"xhtml"    -> Just "html"
        //"wiki"     -> Just "mediawiki"
        CustomAdapterConfig {
            name: "pandoc".to_string(),
            description: "Uses pandoc to convert binary/unreadable text documents to plain markdown-like text".to_string(),
            version: 5,
            extensions: strs(&["epub", "odt", "docx", "fb2", "ipynb", "html", "htm", "rtf"]),
            binary: "pandoc".to_string(),
            mimetypes: None,
            // simpler markdown (with more information loss but plainer text)
            //.arg("--to=commonmark-header_attributes-link_attributes-fenced_divs-markdown_in_html_blocks-raw_html-native_divs-native_spans-bracketed_spans")
            args: strs(&[
                "--from=$input_file_pandoc_format",
                "--to=plain",
                "--wrap=none",
                "--markdown-headings=atx",
                // pandoc 3.x uses the platform's native line ending (CRLF on Windows);
                // force LF so adapter output is consistent across platforms
                "--eol=lf"
            ]),
            disabled_by_default: None,
            match_only_by_mime: None,
            output_path_hint: None,
            bail_if_empty_output: None,
        },
        CustomAdapterConfig {
            name: "poppler".to_owned(),
            version: 2,
            description: "Uses pdftotext (from poppler-utils) to extract plain text from PDF files"
                .to_owned(),

            extensions: strs(&["pdf"]),
            mimetypes: Some(strs(&["application/pdf"])),

            binary: "pdftotext".to_string(),
            // -eol unix: poppler builds on Windows default to \r\n line endings,
            // which would leak \r into the pagebreak postprocessing and its tests.
            // -opw $password: pass the configured password to encrypted PDFs.
            args: strs(&["-eol", "unix", "-opw", "$password", "-", "-"]),
            disabled_by_default: None,
            match_only_by_mime: None,
            output_path_hint: Some("${input_virtual_path}.txt.asciipagebreaks".into()),
            // PDF without a text layer -> pdftotext outputs nothing -> bail so
            // a user-configured OCR adapter for .pdf can take over (issue #3)
            bail_if_empty_output: Some(true),
        },
        CustomAdapterConfig {
            name: "tesseract".to_owned(),
            version: 1,
            description: "Uses tesseract to extract text from images".to_owned(),
            extensions: strs(&["jpg", "jpeg", "png", "webp", "tiff", "bmp", "gif"]),
            mimetypes: Some(strs(&["image/jpeg", "image/png", "image/webp", "image/tiff", "image/bmp", "image/gif"])),
            binary: "tesseract".to_string(),
            args: strs(&["stdin", "stdout"]),
            disabled_by_default: Some(true),
            match_only_by_mime: None,
            output_path_hint: None,
            bail_if_empty_output: None
        },
        CustomAdapterConfig {
            name: "xls2csv".to_owned(),
            version: 1,
            description: "Uses xls2csv (from catdoc) to convert legacy binary Excel (.xls) spreadsheets to CSV".to_owned(),
            extensions: strs(&["xls"]),
            mimetypes: None,
            binary: "xls2csv".to_string(),
            // xls2csv needs a seekable file path, so archives-in-stream are
            // not supported; it is opt-in for that reason
            args: strs(&["$input_virtual_path"]),
            disabled_by_default: Some(true),
            match_only_by_mime: None,
            output_path_hint: None,
            bail_if_empty_output: None
        },
        // ugrep+ runs exiftool by default on images; rga keeps it opt-in
        // because it is an external binary, but registering it as a builtin
        // means `--rga-adapters=+exiftool` is enough to reach parity.
        CustomAdapterConfig {
            name: "exiftool".to_owned(),
            version: 1,
            description: "Uses exiftool to dump EXIF/metadata from images as searchable text (reads from stdin)".to_owned(),
            extensions: strs(&["jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff", "tif"]),
            mimetypes: Some(strs(&["image/jpeg", "image/png", "image/webp", "image/gif", "image/bmp", "image/tiff"])),
            binary: "exiftool".to_string(),
            // "-" makes exiftool read the file from stdin, so this also works
            // for images inside archives (unlike the path-based adapters below)
            args: strs(&["-"]),
            disabled_by_default: Some(true),
            match_only_by_mime: None,
            output_path_hint: None,
            bail_if_empty_output: None
        },
        // soffice cannot read from stdin and needs a seekable path, so files
        // inside archives are not supported (same limitation as xls2csv above).
        // NOTE: soffice refuses to run headless while a GUI instance of
        // LibreOffice is open; conversion fails in that case (same caveat the
        // ugrep --filter examples carry).
        CustomAdapterConfig {
            name: "soffice".to_owned(),
            version: 1,
            description: "Uses LibreOffice headless to convert office documents (xlsx, pptx, ppt, ods, odp) to plain text".to_owned(),
            extensions: strs(&["xlsx", "pptx", "ppt", "ods", "odp"]),
            mimetypes: Some(strs(&[
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                "application/vnd.ms-powerpoint",
                "application/vnd.oasis.opendocument.spreadsheet",
                "application/vnd.oasis.opendocument.presentation",
            ])),
            binary: "soffice".to_string(),
            // --cat requires LibreOffice 7.4+; older versions only convert
            // to files (--convert-to), which does not fit the streaming model
            args: strs(&["--headless", "--cat", "$input_virtual_path"]),
            disabled_by_default: Some(true),
            match_only_by_mime: None,
            output_path_hint: None,
            bail_if_empty_output: None
        }
    ];
}

/// replace a Command.spawn() error "File not found" with a more readable error
/// to indicate some program is not installed
pub fn map_exe_error(err: std::io::Error, exe_name: &str, help: &str) -> anyhow::Error {
    use std::io::ErrorKind::*;
    match err.kind() {
        NotFound => format_err!("Could not find executable \"{}\". {}", exe_name, help),
        _ => anyhow::Error::from(err),
    }
}

fn proc_wait(mut child: Child, context: impl FnOnce() -> String) -> impl AsyncRead {
    let s = stream! {
        let res = child.wait().await?;
        if res.success() {
            yield std::io::Result::Ok(Bytes::new());
        } else {
            let mut stderr_text = String::new();
            if let Some(mut stderr) = child.stderr.take() {
                use tokio::io::AsyncReadExt as _;
                let _ = stderr.read_to_string(&mut stderr_text).await;
            }
            let err = if stderr_text.is_empty() { format!("{:?}", res) } else { format!("{:?}\n{}", res, stderr_text) };
            Err(format_err!("{}", err)).with_context(context).map_err(to_io_err)?;
        }
    };
    StreamReader::new(s)
}

pub fn pipe_output(
    _line_prefix: &str,
    mut cmd: Command,
    inp: ReadBox,
    exe_name: &str,
    help: &str,
) -> Result<ReadBox> {
    let cmd_log = format!("{:?}", cmd); // todo: perf
    let mut cmd = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| map_exe_error(e, exe_name, help))?;
    let mut stdi = cmd.stdin.take().context("stdin not piped")?;
    let stdo = cmd.stdout.take().context("stdout not piped")?;
    let crlf = regex::bytes::Regex::new("\r\n").unwrap();
    let stdo_stream = tokio_util::io::ReaderStream::new(stdo);
    let normalized_stream = async_stream::stream! {
        for await chunk in stdo_stream {
            match chunk {
                Err(e) => yield Err(e),
                Ok(chunk) => {
                    let replaced = crlf.replace_all(&chunk, &b"\n"[..]);
                    yield Ok(bytes::Bytes::copy_from_slice(&replaced));
                }
            }
        }
    };
    let stdo_norm = StreamReader::new(normalized_stream);

    let join = tokio::spawn(async move {
        let mut z = inp;
        match tokio::io::copy(&mut z, &mut stdi).await {
            Ok(_) => {}
            // the child may legitimately not read stdin at all (e.g. echo);
            // a closed stdin pipe is not a conversion failure
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(e) => return Err(e),
        }
        std::io::Result::Ok(())
    });
    Ok(Box::pin(stdo_norm.chain(
        proc_wait(cmd, move || format!("subprocess: {cmd_log}")).chain(join_handle_to_stream(join)),
    )))
}

pub struct CustomSpawningFileAdapter {
    binary: String,
    args: Vec<String>,
    meta: AdapterMeta,
    output_path_hint: Option<String>,
    bail_if_empty_output: bool,
}
impl GetMetadata for CustomSpawningFileAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &self.meta
    }
}
fn arg_replacer(arg: &str, filepath_hint: &Path, config: &RgaConfig) -> Result<String> {
    // pandoc's FormatHeuristics extension aliases, for the extensions rga hands to pandoc.
    // Without this, `--from=htm` fails with "Unknown input format htm".
    // https://github.com/jgm/pandoc/blob/master/src/Text/Pandoc/App/FormatHeuristics.hs
    fn pandoc_format_alias(ext: &str) -> String {
        match ext.to_ascii_lowercase().as_str() {
            "htm" | "xhtml" => "html",
            "adoc" => "asciidoc",
            "text" | "txt" => "markdown",
            "lhs" => "markdown+lhs",
            "texi" => "texinfo",
            "tei.xml" => "tei",
            "wiki" => "mediawiki",
            _ => ext,
        }
        .to_owned()
    }
    expand_str_ez(arg, |s| match s {
        "input_virtual_path" => Ok(filepath_hint.to_string_lossy()),
        "input_file_stem" => Ok(filepath_hint
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()),
        "input_file_extension" => Ok(filepath_hint
            .extension()
            .unwrap_or_default()
            .to_string_lossy()),
        "password" => Ok(config.password.clone().unwrap_or_default().into()),
        "input_file_pandoc_format" => Ok(std::borrow::Cow::Owned(
            filepath_hint
                .extension()
                .map(|e| pandoc_format_alias(&e.to_string_lossy()))
                .unwrap_or_default(),
        )),
        e => Err(anyhow::format_err!("unknown replacer ${{{e}}}")),
    })
}
impl CustomSpawningFileAdapter {
    fn command(
        &self,
        filepath_hint: &std::path::Path,
        config: &RgaConfig,
        mut command: tokio::process::Command,
    ) -> Result<tokio::process::Command> {
        // When no password is configured, drop `-opw $password` pairs entirely:
        // some pdftotext builds (e.g. xpdf) reject an empty -opw argument.
        let password_empty = config.password.as_deref().unwrap_or("").is_empty();
        let mut args = Vec::with_capacity(self.args.len());
        let mut iter = self.args.iter().peekable();
        while let Some(arg) = iter.next() {
            if password_empty
                && arg == "-opw"
                && iter.peek().map(|s| s.as_str()) == Some("$password")
            {
                iter.next();
                continue;
            }
            args.push(arg_replacer(arg, filepath_hint, config)?);
        }
        command.args(args);
        log::debug!("running command {:?}", command);
        Ok(command)
    }
}
#[async_trait]
impl FileAdapter for CustomSpawningFileAdapter {
    async fn adapt(
        &self,
        ai: AdaptInfo,
        _detection_reason: &FileMatcher,
    ) -> Result<AdaptedFilesIterBox> {
        let AdaptInfo {
            filepath_hint,
            inp,
            line_prefix,
            archive_recursion_depth,
            postprocess,
            config,
            is_real_file,
            ..
        } = ai;

        let cmd = Command::new(&self.binary);
        let cmd = self
            .command(&filepath_hint, &config, cmd)
            .with_context(|| format!("Could not set cmd arguments for {}", self.binary))?;
        debug!("executing {:?}", cmd);
        if self.bail_if_empty_output && !is_real_file {
            // the empty-output bail needs to rewind the input stream on retry,
            // which is only possible for real files on disk
            log::warn!(
                "bail_if_empty_output is ignored for '{}' inside an archive (stream cannot be rewound)",
                filepath_hint.to_string_lossy()
            );
        }
        let output = if self.bail_if_empty_output && is_real_file {
            match self.spawn_peek_empty(cmd, inp).await? {
                PeekOutcome::Bailed { reason } => return Err(adapter_bail(reason)),
                PeekOutcome::Output(r) => r,
            }
        } else {
            pipe_output(&line_prefix, cmd, inp, &self.binary, "")?
        };
        Ok(one_file(AdaptInfo {
            filepath_hint: PathBuf::from(arg_replacer(
                self.output_path_hint
                    .as_deref()
                    .unwrap_or("${input_virtual_path}.txt"),
                &filepath_hint,
                &config,
            )?),
            inp: output,
            line_prefix,
            is_real_file: false,
            file_mtime_unix_ms: None,
            archive_recursion_depth: archive_recursion_depth + 1,
            postprocess,
            config,
        }))
    }
}

enum PeekOutcome {
    /// the program produced no text output; decline the file (issue #3)
    Bailed { reason: String },
    Output(ReadBox),
}

impl CustomSpawningFileAdapter {
    /// like `pipe_output`, but reads the first chunk of the child's stdout
    /// before deciding: if the program produced no text output (e.g.
    /// pdftotext on a PDF without a text layer, which emits only form-feed
    /// page separators), bail so the next adapter (e.g. OCR) can handle the
    /// file. The consumed first chunk is prepended to the returned stream.
    async fn spawn_peek_empty(&self, mut cmd: Command, inp: ReadBox) -> Result<PeekOutcome> {
        let cmd_log = format!("{:?}", cmd); // todo: perf
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| map_exe_error(e, &self.binary, ""))?;
        let mut stdi = child.stdin.take().expect("is piped");
        let mut stdo = child.stdout.take().expect("is piped");

        let join = tokio::spawn(async move {
            let mut z = inp;
            match tokio::io::copy(&mut z, &mut stdi).await {
                Ok(_) => {}
                // the child may legitimately not read stdin at all (e.g. echo);
                // a closed stdin pipe is not a conversion failure
                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
                Err(e) => return Err(e),
            }
            std::io::Result::Ok(())
        });

        // collect the first chunk of output to decide whether the program
        // produced any text at all. pdftotext on an image-only PDF emits a
        // lone form-feed per page rather than zero bytes, so "empty" here
        // means "no bytes other than ASCII whitespace/control". reads may be
        // partial, so keep filling the chunk until text appears or it is full.
        let mut first = vec![0u8; 4096];
        let mut filled = 0;
        let mut has_text = false;
        while filled < first.len() && !has_text {
            let n = stdo.read(&mut first[filled..]).await?;
            if n == 0 {
                break;
            }
            has_text = first[filled..filled + n]
                .iter()
                .any(|&b| b > 0x20 && b != 0x7f);
            filled += n;
        }
        if filled == 0 || !has_text {
            // reap the child so it does not linger, then decline the file
            let _ = child.wait().await;
            return Ok(PeekOutcome::Bailed {
                reason: format!("{} produced no text output", self.binary),
            });
        }
        Ok(PeekOutcome::Output(Box::pin(
            std::io::Cursor::new(first[..filled].to_vec())
                .chain(stdo)
                .chain(proc_wait(child, move || format!("subprocess: {cmd_log}")))
                .chain(join_handle_to_stream(join)),
        )))
    }
}
impl CustomAdapterConfig {
    pub fn to_adapter(&self) -> CustomSpawningFileAdapter {
        CustomSpawningFileAdapter {
            binary: self.binary.clone(),
            args: self.args.clone(),
            output_path_hint: self.output_path_hint.clone(),
            bail_if_empty_output: self.bail_if_empty_output.unwrap_or(false),
            meta: AdapterMeta {
                name: self.name.clone(),
                version: self.version,
                description: format!(
                    "{}\nRuns: {} {}",
                    self.description,
                    self.binary,
                    self.args.join(" ")
                ),
                recurses: true,
                fast_matchers: self
                    .extensions
                    .iter()
                    .map(|s| FastFileMatcher::FileExtension(s.to_string()))
                    .collect(),
                slow_matchers: self.mimetypes.as_ref().map(|mimetypes| {
                    mimetypes
                        .iter()
                        .map(|s| FileMatcher::MimeType(s.to_string()))
                        .collect()
                }),
                keep_fast_matchers_if_accurate: !self.match_only_by_mime.unwrap_or(false),
                disabled_by_default: self.disabled_by_default.unwrap_or(false),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::FileAdapter;
    use super::*;
    use crate::preproc::loop_adapt;
    use crate::test_utils::*;
    use anyhow::Result;
    use pretty_assertions::assert_eq;
    use tokio::fs::File;

    #[test]
    fn pandoc_from_format_alias() -> Result<()> {
        // https://github.com/phiresky/ripgrep-all/issues/205
        // pandoc rejects "--from=htm" ("Unknown input format htm"), so the extension
        // must be mapped through pandoc's own FormatHeuristics aliases.
        let adapter = CustomAdapterConfig {
            name: "pandoc".to_string(),
            description: "test".to_string(),
            disabled_by_default: None,
            version: 1,
            extensions: vec!["html".to_string(), "htm".to_string()],
            mimetypes: None,
            match_only_by_mime: None,
            binary: "pandoc".to_string(),
            args: vec!["--from=$input_file_pandoc_format".to_string()],
            output_path_hint: None,
            bail_if_empty_output: None,
        }
        .to_adapter();
        for (file, expected) in [
            ("page.htm", "--from=html"),
            ("page.html", "--from=html"),
            ("page.docx", "--from=docx"),
            ("page.HTM", "--from=html"),
        ] {
            let cmd = adapter.command(Path::new(file), &RgaConfig::default(), Command::new("pandoc"))?;
            let debug = format!("{:?}", cmd);
            assert!(
                debug.contains(expected),
                "command for {file} should contain {expected:?}, got: {debug}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn poppler() -> Result<()> {
        let adapter = poppler_adapter();

        let filepath = test_data_dir().join("short.pdf");

        let (a, d) = simple_adapt_info(&filepath, Box::pin(File::open(&filepath).await?));
        // let r = adapter.adapt(a, &d)?;
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

    #[tokio::test]
    async fn pandoc_rtf() -> Result<()> {
        let adapter = BUILTIN_SPAWNING_ADAPTERS
            .iter()
            .find(|e| e.name == "pandoc")
            .expect("no pandoc adapter")
            .to_adapter();
        let rtf = br"{\rtf1\ansi{\fonttbl{\f0 Arial;}}\f0\pard Hello RTF fixture\par}";
        let filepath = std::path::Path::new("test.rtf");
        let (a, d) = simple_adapt_info(filepath, Box::pin(std::io::Cursor::new(rtf.as_slice())));
        let r = loop_adapt(&adapter, d, a, crate::adapters::get_all_adapters(None).0).await?;
        let o = adapted_to_vec(r).await?;
        let text = String::from_utf8(o)?;
        assert!(
            text.contains("PREFIX:Hello RTF fixture"),
            "unexpected output: {text:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn spawning_builtins_exiftool_and_soffice_registered() -> Result<()> {
        use crate::config::RgaConfig;

        let exiftool = BUILTIN_SPAWNING_ADAPTERS
            .iter()
            .find(|e| e.name == "exiftool")
            .expect("no exiftool adapter");
        assert_eq!(exiftool.disabled_by_default, Some(true));
        assert!(exiftool.extensions.iter().any(|e| e == "jpg"));
        assert!(exiftool.extensions.iter().any(|e| e == "png"));
        // exiftool reads the image from stdin, so no path placeholder is used
        assert_eq!(exiftool.args, strs(&["-"]));

        let soffice = BUILTIN_SPAWNING_ADAPTERS
            .iter()
            .find(|e| e.name == "soffice")
            .expect("no soffice adapter");
        assert_eq!(soffice.disabled_by_default, Some(true));
        for ext in ["xlsx", "pptx", "ppt", "ods", "odp"] {
            assert!(
                soffice.extensions.iter().any(|e| e == ext),
                "soffice adapter missing extension {ext}"
            );
        }
        // soffice needs a seekable path: the command must carry the expanded
        // virtual path (which is empty for archive members, declining them)
        let adapter = soffice.to_adapter();
        let cmd = adapter.command(
            std::path::Path::new("sheet.xlsx"),
            &RgaConfig::default(),
            tokio::process::Command::new("soffice"),
        )?;
        let debug = format!("{cmd:?}");
        assert!(
            debug.contains("--headless") && debug.contains("--cat") && debug.contains("sheet.xlsx"),
            "unexpected soffice command: {debug}"
        );
        Ok(())
    }

    use crate::{
        adapters::custom::CustomAdapterConfig,
        test_utils::{adapted_to_vec, simple_adapt_info},
    };
    use std::io::Cursor;

    #[tokio::test]
    async fn streaming() -> anyhow::Result<()> {
        // an adapter that converts input line by line (deadlocks if the parent process tries to write everything and only then read it)
        let adapter = CustomAdapterConfig {
            name: "simple text replacer".to_string(),
            description: "oo".to_string(),
            disabled_by_default: None,
            version: 1,
            extensions: vec!["txt".to_string()],
            mimetypes: None,
            match_only_by_mime: None,
            binary: "sed".to_string(),
            args: vec!["s/e/u/g".to_string()],
            output_path_hint: None,
            bail_if_empty_output: None,
        };

        let adapter = adapter.to_adapter();
        let input = r#"
        This is the story of a
        very strange lorry
        with a long dead crew
        and a witch with the flu
        "#;
        let input = format!("{input}{input}{input}{input}");
        let input = format!("{input}{input}{input}{input}");
        let input = format!("{input}{input}{input}{input}");
        let input = format!("{input}{input}{input}{input}");
        let input = format!("{input}{input}{input}{input}");
        let input = format!("{input}{input}{input}{input}");
        let (a, d) = simple_adapt_info(
            Path::new("foo.txt"),
            Box::pin(Cursor::new(Vec::from(input))),
        );
        let output = adapter.adapt(a, &d).await.unwrap();

        let oup = adapted_to_vec(output).await?;
        println!("output: {}", String::from_utf8_lossy(&oup));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_bail_falls_back_to_next_adapter() -> Result<()> {
        use crate::config::RgaConfig;
        use crate::preproc::rga_preproc;
        use tokio::io::AsyncReadExt;

        // first adapter: matches .bailtest and always produces empty output -> bails
        #[cfg(windows)]
        let (silent_bin, silent_args) = ("cmd", vec!["/c".to_string(), "exit 0".to_string()]);
        #[cfg(unix)]
        let (silent_bin, silent_args) = ("true", vec![]);

        // second adapter: matches .bailtest too and prints "rescued"
        #[cfg(windows)]
        let (echo_bin, echo_args) = ("cmd", vec!["/c".to_string(), "echo rescued".to_string()]);
        #[cfg(unix)]
        let (echo_bin, echo_args) = ("echo", vec!["rescued".to_string()]);

        let mk = |name: &str, binary: &str, args: Vec<String>, bail: Option<bool>| {
            CustomAdapterConfig {
                name: name.to_string(),
                description: "test adapter".to_string(),
                disabled_by_default: None,
                version: 1,
                extensions: vec!["bailtest".to_string()],
                mimetypes: None,
                match_only_by_mime: None,
                binary: binary.to_string(),
                args,
                output_path_hint: None,
                bail_if_empty_output: bail,
            }
        };

        let mut config = RgaConfig::default();
        config.cache.disabled = true;
        config.custom_adapters = Some(vec![
            mk("silent", silent_bin, silent_args, Some(true)),
            mk("rescuer", echo_bin, echo_args, None),
        ]);

        let filepath = std::env::temp_dir().join("rga-bail-poc.bailtest");
        tokio::fs::write(&filepath, b"dummy content").await?;

        let ai = AdaptInfo {
            filepath_hint: filepath.clone(),
            is_real_file: true,
            archive_recursion_depth: 0,
            inp: Box::pin(File::open(&filepath).await?),
            line_prefix: "PREFIX: ".to_string(),
            postprocess: true,
            config,
            file_mtime_unix_ms: None,
        };
        let mut out = rga_preproc(ai).await?;
        let mut bytes = Vec::new();
        out.read_to_end(&mut bytes).await?;
        tokio::fs::remove_file(&filepath).await.ok();
        let text = String::from_utf8(bytes)?;
        assert!(
            text.contains("rescued"),
            "expected the second adapter to rescue the file, got: {text:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn bail_flag_with_output_does_not_bail() -> Result<()> {
        use crate::config::RgaConfig;
        // an adapter with bail_if_empty_output whose program DOES produce output
        // must behave exactly like a normal adapter (the peeked first chunk is
        // prepended to the returned stream, not dropped)
        #[cfg(windows)]
        let (bin, args) = ("cmd", vec!["/c".to_string(), "echo peeked-output".to_string()]);
        #[cfg(unix)]
        let (bin, args) = ("echo", vec!["peeked-output".to_string()]);

        let adapter = CustomAdapterConfig {
            name: "echoer".to_string(),
            description: "test adapter".to_string(),
            disabled_by_default: None,
            version: 1,
            extensions: vec!["echoext".to_string()],
            mimetypes: None,
            match_only_by_mime: None,
            binary: bin.to_string(),
            args,
            output_path_hint: None,
            bail_if_empty_output: Some(true),
        }
        .to_adapter();

        let filepath = std::env::temp_dir().join("rga-bail-echo.echoext");
        tokio::fs::write(&filepath, b"dummy content").await?;
        let ai = AdaptInfo {
            filepath_hint: filepath.clone(),
            is_real_file: true,
            archive_recursion_depth: 0,
            inp: Box::pin(File::open(&filepath).await?),
            line_prefix: String::new(),
            postprocess: false,
            config: RgaConfig::default(),
            file_mtime_unix_ms: None,
        };
        let detection = FileMatcher::Fast(FastFileMatcher::FileExtension("echoext".to_string()));
        let output = adapter.adapt(ai, &detection).await?;
        let mut out = adapted_to_vec(output).await?;
        tokio::fs::remove_file(&filepath).await.ok();
        let text = String::from_utf8(std::mem::take(&mut out))?;
        assert!(
            text.contains("peeked-output"),
            "expected the adapter output to pass through, got: {text:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn bail_on_whitespace_only_output() -> Result<()> {
        use crate::config::RgaConfig;
        // pdftotext on an image-only PDF emits only form-feed page separators
        // (no bytes other than ASCII whitespace/control), which must count as
        // "no output" for the bail — otherwise the fallback never triggers
        // for real scanned PDFs.
        #[cfg(windows)]
        let (bin, args) = ("cmd", vec!["/c".to_string(), "echo.".to_string()]);
        #[cfg(unix)]
        let (bin, args) = ("echo", vec![String::new()]);

        let adapter = CustomAdapterConfig {
            name: "ffonly".to_string(),
            description: "test adapter".to_string(),
            disabled_by_default: None,
            version: 1,
            extensions: vec!["ffonlyext".to_string()],
            mimetypes: None,
            match_only_by_mime: None,
            binary: bin.to_string(),
            args,
            output_path_hint: None,
            bail_if_empty_output: Some(true),
        }
        .to_adapter();

        let filepath = std::env::temp_dir().join("rga-bail-ffonly.ffonlyext");
        tokio::fs::write(&filepath, b"dummy content").await?;
        let ai = AdaptInfo {
            filepath_hint: filepath.clone(),
            is_real_file: true,
            archive_recursion_depth: 0,
            inp: Box::pin(File::open(&filepath).await?),
            line_prefix: String::new(),
            postprocess: false,
            config: RgaConfig::default(),
            file_mtime_unix_ms: None,
        };
        let detection = FileMatcher::Fast(FastFileMatcher::FileExtension("ffonlyext".to_string()));
        let err = match adapter.adapt(ai, &detection).await {
            std::result::Result::Ok(_) => panic!("expected a bail on whitespace-only output"),
            Err(e) => e,
        };
        tokio::fs::remove_file(&filepath).await.ok();
        assert!(
            err.chain()
                .any(|cause| cause.downcast_ref::<crate::adapters::AdapterBail>().is_some()),
            "expected an AdapterBail, got: {err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn bail_exhaustion_errors_instead_of_looping() -> Result<()> {
        use crate::config::RgaConfig;
        use crate::preproc::rga_preproc;
        // when the only matching adapter bails and no other adapter can handle
        // the file, rga_preproc must fail with a clear message (not hang or loop)
        #[cfg(windows)]
        let (bin, args) = ("cmd", vec!["/c".to_string(), "exit 0".to_string()]);
        #[cfg(unix)]
        let (bin, args) = ("true", vec![]);

        let mut config = RgaConfig::default();
        config.cache.disabled = true;
        config.custom_adapters = Some(vec![CustomAdapterConfig {
            name: "silent".to_string(),
            description: "test adapter".to_string(),
            disabled_by_default: None,
            version: 1,
            extensions: vec!["bailonly".to_string()],
            mimetypes: None,
            match_only_by_mime: None,
            binary: bin.to_string(),
            args,
            output_path_hint: None,
            bail_if_empty_output: Some(true),
        }]);

        let filepath = std::env::temp_dir().join("rga-bail-only.bailonly");
        tokio::fs::write(&filepath, b"dummy content").await?;
        let ai = AdaptInfo {
            filepath_hint: filepath.clone(),
            is_real_file: true,
            archive_recursion_depth: 0,
            inp: Box::pin(File::open(&filepath).await?),
            line_prefix: String::new(),
            postprocess: true,
            config,
            file_mtime_unix_ms: None,
        };
        let result = rga_preproc(ai).await;
        let err = match result {
            std::result::Result::Ok(_) => panic!("expected an error when every adapter bailed"),
            Err(e) => e,
        };
        tokio::fs::remove_file(&filepath).await.ok();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("bailed"),
            "error should mention the bail, got: {msg}"
        );
        Ok(())
    }
}
