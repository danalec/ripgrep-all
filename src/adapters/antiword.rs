use super::custom::map_exe_error;
use super::*;
use anyhow::*;
use async_trait::async_trait;
use lazy_static::lazy_static;
use std::process::Stdio;
use tokio::io::AsyncWrite;
use tokio::process::Command;
use writing::WritingFileAdapter;

static EXTENSIONS: &[&str] = &["doc"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "antiword".to_owned(),
        version: 1,
        description: "Uses antiword to extract text from legacy (Word 97-2003, OLE2) .doc files"
            .to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: Some(vec![FileMatcher::MimeType("application/msword".to_owned())]),
        disabled_by_default: false,
        keep_fast_matchers_if_accurate: true
    };
}

#[derive(Default, Clone)]
pub struct AntiwordAdapter;

impl AntiwordAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for AntiwordAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl WritingFileAdapter for AntiwordAdapter {
    async fn adapt_write(
        ai: AdaptInfo,
        _detection_reason: &FileMatcher,
        mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()> {
        let AdaptInfo {
            is_real_file,
            filepath_hint,
            mut inp,
            ..
        } = ai;

        // antiword only accepts a seekable file path, so archive members and
        // other streams are spooled to a temporary file first
        let temp_dir;
        let inp_fname = if is_real_file {
            filepath_hint.clone()
        } else {
            temp_dir = tempfile::tempdir()?;
            let t_path = temp_dir.path().join(
                filepath_hint
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("document.doc")),
            );
            let mut f = tokio::fs::File::create(&t_path).await?;
            tokio::io::copy(&mut inp, &mut f).await?;
            t_path
        };

        let mut cmd = Command::new("antiword")
            .arg(&inp_fname)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                map_exe_error(
                    e,
                    "antiword",
                    "Install antiword to search legacy .doc files.",
                )
            })?;
        let mut stdo = cmd.stdout.take().context("antiword stdout not piped")?;
        tokio::io::copy(&mut stdo, &mut oup).await?;
        let exit = cmd.wait().await?;
        if !exit.success() {
            let mut stderr_str = String::new();
            if let Some(mut stderr) = cmd.stderr.take() {
                use tokio::io::AsyncReadExt as _;
                let _ = stderr.read_to_string(&mut stderr_str).await;
            }
            return Err(format_err!("antiword failed: {:?}\n{}", exit, stderr_str));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RgaConfig;

    #[test]
    fn antiword_adapter_registered_and_matches_doc_extension() {
        let adapters = crate::adapters::get_adapters_filtered(
            None,
            &Vec::<String>::new(),
            &RgaConfig::default(),
        )
        .unwrap();
        let antiword = adapters
            .iter()
            .find(|a| a.metadata().name == "antiword")
            .expect("antiword adapter is registered by default");
        assert!(!antiword.metadata().recurses);
        let matchers = &antiword.metadata().fast_matchers;
        assert_eq!(matchers.len(), 1);
        match &matchers[0] {
            FastFileMatcher::FileExtension(ext) => assert_eq!(ext, "doc"),
        }
    }

    #[test]
    fn antiword_matches_msword_mimetype_in_accurate_mode() {
        let matcher = crate::matching::adapter_matcher(
            &[Arc::new(AntiwordAdapter::new()) as Arc<dyn FileAdapter>],
            true,
        )
        .unwrap();
        let chosen = matcher(crate::matching::FileMeta {
            lossy_filename: "no_extension_here".to_string(),
            mimetype: Some("application/msword"),
        });
        assert!(chosen.is_some(), "mime type should select antiword");
        assert_eq!(chosen.unwrap().0.metadata().name, "antiword");
    }
}
