//! Adapter for FITS (Flexible Image Transport System), the astronomy /
//! scientific-imaging container used by telescopes and observatories.
//!
//! A FITS file is a sequence of 2880-byte blocks. Each header unit is a set
//! of 80-character "cards" of the form `KEYWORD  = value / comment` followed
//! by an END card. This adapter turns the primary header cards into
//! `KEYWORD: value` lines so e.g. `rga OBJECT survey.fits` finds the target
//! name, instrument, exposure time and WCS coordinates of an observation.
//!
//! Data payloads (images/tables) are not decoded.

use super::*;
use crate::adapted_iter::one_file;
use crate::config::RgaConfig;

use anyhow::{Result, bail};
use lazy_static::lazy_static;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

lazy_static! {
    static ref FITS_META: AdapterMeta = AdapterMeta {
        name: "fits".to_owned(),
        version: 1,
        description: "Extracts the header cards (target, instrument, exposure, WCS) from FITS astronomy/scientific image files".to_owned(),
        recurses: true,
        fast_matchers: vec![
            FastFileMatcher::FileExtension("fits".to_string()),
            FastFileMatcher::FileExtension("fit".to_string()),
            FastFileMatcher::FileExtension("fts".to_string()),
        ],
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    };
}

const CARD_LEN: usize = 80;
const BLOCK_LEN: usize = 2880;
/// safety bound: stop after this many header blocks (megabyte-scale headers)
const MAX_HEADER_CARDS: usize = 20_000;

#[derive(Default)]
pub struct FitsAdapter;
impl FitsAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for FitsAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &FITS_META
    }
}

fn parse_fits(data: &[u8]) -> Result<String> {
    if data.len() < BLOCK_LEN {
        bail!("fits file too small");
    }
    let first8 = &data[0..8];
    if first8 != b"SIMPLE  " && first8 != b"XTENSION" {
        bail!("not a FITS file (no SIMPLE/XTENSION keyword)");
    }
    let mut out = String::new();
    let mut pos = 0;
    let mut cards = 0;
    loop {
        if pos + CARD_LEN > data.len() || cards >= MAX_HEADER_CARDS {
            bail!("fits header not terminated by END card");
        }
        let card = &data[pos..pos + CARD_LEN];
        pos += CARD_LEN;
        cards += 1;
        let keyword = String::from_utf8_lossy(&card[0..8]).trim().to_string();
        if keyword == "END" {
            break;
        }
        // value cards have '= ' in columns 9-10; comment/history cards don't
        if card.len() >= 10 && &card[8..10] == b"= " {
            let value_field = String::from_utf8_lossy(&card[10..]).into_owned();
            let (raw_value, comment) = match value_field.find('/') {
                Some(i) => (value_field[..i].trim(), Some(value_field[i + 1..].trim())),
                None => (value_field.trim(), None),
            };
            // string values are quoted ('NGC 1234'); strip the quotes
            let value = raw_value.trim_matches('\'').trim();
            match (comment, value.is_empty()) {
                (Some(c), false) if !c.is_empty() => {
                    out.push_str(&format!("{keyword}: {value} ({c})\n"));
                }
                _ => out.push_str(&format!("{keyword}: {value}\n")),
            }
        } else if keyword == "COMMENT" || keyword == "HISTORY" {
            let text = String::from_utf8_lossy(&card[8..]).trim().to_string();
            if !text.is_empty() {
                out.push_str(&format!("{keyword}: {text}\n"));
            }
        }
    }
    if out.is_empty() {
        bail!("fits header has no cards");
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for FitsAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let AdaptInfo {
            filepath_hint,
            line_prefix,
            archive_recursion_depth,
            postprocess,
            config,
            mut inp,
            ..
        } = ai;
        let mut data = Vec::new();
        inp.read_to_end(&mut data).await?;
        let text = parse_fits(&data)
            .map_err(|e| adapter_bail(format!("fits: {e}")))?;
        let mut out_path: PathBuf = filepath_hint;
        out_path.set_extension("txt");
        Ok(one_file(AdaptInfo {
            filepath_hint: out_path,
            is_real_file: false,
            file_mtime_unix_ms: None,
            archive_recursion_depth: archive_recursion_depth + 1,
            inp: Box::pin(Cursor::new(text.into_bytes())),
            line_prefix,
            postprocess,
            config,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::assert_eq;
    use std::io::Cursor;

    fn card(keyword: &str, value: &str) -> Vec<u8> {
        // KEYWORD  = value    with FITS column alignment, padded to 80
        let mut c = format!("{keyword:<8}= {value}");
        c.truncate(80);
        format!("{c:<80}").into_bytes()
    }

    fn make_fits(cards: &[Vec<u8>]) -> Vec<u8> {
        let mut v: Vec<u8> = cards.concat();
        v.extend_from_slice(b"END");
        v.resize(v.len().div_ceil(BLOCK_LEN) * BLOCK_LEN, b' ');
        v
    }

    async fn adapt_to_string(data: Vec<u8>) -> Result<String> {
        let (a, d) = simple_adapt_info(
            std::path::Path::new("obs.fits"),
            Box::pin(Cursor::new(data)),
        );
        let out = FitsAdapter.adapt(a, &d).await?;
        Ok(String::from_utf8(adapted_to_vec(out).await?)?.trim().to_string())
    }

    #[tokio::test]
    async fn fits_header_cards() -> Result<()> {
        let data = make_fits(&[
            card("SIMPLE", "T / conforms to FITS standard"),
            card("BITPIX", "-32 / floating point"),
            card("OBJECT", "'NGC 6946' / target name"),
            card("EXPTIME", "1200.0 / seconds"),
            card("TELESCOP", "'HST'"),
        ]);
        let out = adapt_to_string(data).await?;
        assert_eq!(
            out,
            "SIMPLE: T (conforms to FITS standard)\nBITPIX: -32 (floating point)\nOBJECT: NGC 6946 (target name)\nEXPTIME: 1200.0 (seconds)\nTELESCOP: HST"
        );
        Ok(())
    }

    #[tokio::test]
    async fn fits_comment_and_history() -> Result<()> {
        let mut comment = b"COMMENT reduced with flat field".to_vec();
        comment.resize(80, b' ');
        let data = make_fits(&[card("SIMPLE", "T"), comment]);
        let out = adapt_to_string(data).await?;
        assert!(out.contains("COMMENT: reduced with flat field"), "got {out}");
        Ok(())
    }

    #[tokio::test]
    async fn fits_rejects_non_fits() {
        let data = vec![0u8; 3000];
        let (a, d) = simple_adapt_info(
            std::path::Path::new("x.fits"),
            Box::pin(Cursor::new(data)),
        );
        let res = FitsAdapter.adapt(a, &d).await;
        assert!(res.err().expect("should bail").downcast_ref::<AdapterBail>().is_some());
    }
}
