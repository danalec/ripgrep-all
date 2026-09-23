//! Adapter for dBase/FOX DBF files (xBase): the attribute-table side of
//! shapefiles, legacy business data, and Clipper/FoxPro systems.
//!
//! Parses the file header (version, record count, field descriptors) and
//! emits the fields plus the records themselves as tab-separated rows, so
//! `rga "springfield" cadastral.dbf` finds actual attribute values, not
//! just column names. Deleted records (deletion flag 0x2A) are skipped.

use super::*;
use crate::adapted_iter::one_file;
use crate::config::RgaConfig;

use anyhow::{Result, bail};
use lazy_static::lazy_static;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

lazy_static! {
    static ref DBF_META: AdapterMeta = AdapterMeta {
        name: "dbf".to_owned(),
        version: 1,
        description:
            "Extracts the field descriptors and record rows (as TSV) from dBase/FoxPro DBF files"
                .to_owned(),
        recurses: true,
        fast_matchers: vec![FastFileMatcher::FileExtension("dbf".to_string())],
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    };
}

/// cap the number of rows emitted; very wide tables still stay bounded
const MAX_ROWS: u32 = 10_000;

#[derive(Default)]
pub struct DbfAdapter;
impl DbfAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for DbfAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &DBF_META
    }
}

struct Field {
    name: String,
    type_char: char,
    len: usize,
    decimals: u8,
}

fn parse_dbf(data: &[u8]) -> Result<String> {
    if data.len() < 33 {
        bail!("dbf file too small");
    }
    let version = data[0];
    // dBASE/FoxPro/Visual FoxPro versions span 0x02..0x8b plus 0xf5..0xfb
    // (FoxPro 2.x, VFP with DBC, etc.); instead of an allowlist, accept any
    // non-zero version and rely on the structural validation below
    // (record size must equal the sum of field lengths) to reject junk.
    if version == 0x00 {
        bail!("not a DBF file (version byte is zero)");
    }
    let num_records = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let header_size = u16::from_le_bytes(data[8..10].try_into().unwrap()) as usize;
    let record_size = u16::from_le_bytes(data[10..12].try_into().unwrap()) as usize;
    if header_size < 33 || record_size < 1 || header_size > data.len() {
        bail!("dbf header sizes are inconsistent");
    }
    let field_count = (header_size - 33) / 32;
    let mut fields = Vec::with_capacity(field_count);
    for i in 0..field_count {
        let off = 32 + i * 32;
        if off + 32 > data.len() {
            bail!("dbf field descriptor overruns file");
        }
        let raw_name = &data[off..off + 11];
        let name_end = raw_name
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(raw_name.len());
        let name = String::from_utf8_lossy(&raw_name[..name_end])
            .trim()
            .to_string();
        if name.is_empty() {
            continue;
        }
        fields.push(Field {
            name,
            type_char: data[off + 11] as char,
            len: data[off + 16] as usize,
            decimals: data[off + 17],
        });
    }
    if fields.is_empty() {
        bail!("dbf has no fields");
    }
    if fields.iter().map(|f| f.len).sum::<usize>() + 1 != record_size {
        bail!("dbf record size does not match field lengths");
    }

    let mut out = format!(
        "dbf_version: {version:#x}\nrecords: {num_records}\nfields: {}\n",
        fields
            .iter()
            .map(|f| format!(
                "{} {}{}",
                f.name,
                f.type_char,
                if f.decimals > 0 && matches!(f.type_char, 'N' | 'F' | 'B' | 'Y') {
                    format!("({},{})", f.len, f.decimals)
                } else {
                    format!("({})", f.len)
                }
            ))
            .collect::<Vec<_>>()
            .join(", ")
    );
    out.push_str(&format!(
        "rows:\n{}\n",
        fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join("\t")
    ));

    let mut emitted = 0u32;
    let mut pos = header_size;
    while pos + record_size <= data.len() && emitted < MAX_ROWS {
        let record = &data[pos..pos + record_size];
        pos += record_size;
        if record[0] == 0x2a {
            continue; // deleted record
        }
        let mut values = Vec::with_capacity(fields.len());
        let mut col = 1; // skip the deletion flag byte
        for f in &fields {
            let cell = &record[col..col + f.len];
            col += f.len;
            let text = match f.type_char {
                // keep dates and numerals as-is; trim padding everywhere
                _ => String::from_utf8_lossy(cell).trim().to_string(),
            };
            values.push(text);
        }
        out.push_str(&values.join("\t"));
        out.push('\n');
        emitted += 1;
        if out.len() > 10_000_000 {
            out.push_str("... (rows truncated)\n");
            break;
        }
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for DbfAdapter {
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
        let text = parse_dbf(&data).map_err(|e| adapter_bail(format!("dbf: {e}")))?;
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

    fn make_dbf(rows: &[(&str, &str)], deleted: bool) -> Vec<u8> {
        // two C(10) columns
        let fields = [(b"NAME".to_vec(), 10usize), (b"CITY".to_vec(), 10usize)];
        let header_size = 32 + fields.len() * 32 + 1;
        let record_size = 1 + fields.iter().map(|(_, l)| l).sum::<usize>();
        let mut v = vec![0u8; 32];
        v[0] = 0x03;
        v[4..8].copy_from_slice(&(rows.len() as u32).to_le_bytes());
        v[8..10].copy_from_slice(&(header_size as u16).to_le_bytes());
        v[10..12].copy_from_slice(&(record_size as u16).to_le_bytes());
        for (name, len) in &fields {
            let mut d = vec![0u8; 32];
            d[..name.len()].copy_from_slice(name);
            d[11] = b'C';
            d[16] = *len as u8;
            v.extend_from_slice(&d);
        }
        v.push(0x0d); // header terminator
        assert_eq!(v.len(), header_size);
        for (i, (name, city)) in rows.iter().enumerate() {
            v.push(if deleted && i == 0 { 0x2a } else { 0x20 });
            let mut cell = vec![b' '; 10];
            cell[..name.len()].copy_from_slice(name.as_bytes());
            v.extend_from_slice(&cell);
            let mut cell = vec![b' '; 10];
            cell[..city.len()].copy_from_slice(city.as_bytes());
            v.extend_from_slice(&cell);
        }
        v.push(0x1a); // EOF marker
        v
    }

    #[tokio::test]
    async fn dbf_fields_and_rows() -> Result<()> {
        let data = make_dbf(&[("alice", "lyon"), ("bob", "nantes")], false);
        let (a, d) = simple_adapt_info(
            std::path::Path::new("cities.dbf"),
            Box::pin(Cursor::new(data)),
        );
        let out = DbfAdapter.adapt(a, &d).await?;
        let text = String::from_utf8(adapted_to_vec(out).await?)?
            .trim()
            .to_string();
        assert_eq!(
            text,
            "dbf_version: 0x3\nrecords: 2\nfields: NAME C(10), CITY C(10)\nrows:\nNAME\tCITY\nalice\tlyon\nbob\tnantes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn dbf_accepts_foxpro_version() -> Result<()> {
        // FoxPro 2.x uses version byte 0xf5, outside the old 0x02..=0x8b
        // allowlist; structural validation must accept it
        let mut data = make_dbf(&[("alice", "lyon")], false);
        data[0] = 0xf5;
        let (a, d) = simple_adapt_info(
            std::path::Path::new("legacy.dbf"),
            Box::pin(Cursor::new(data)),
        );
        let out = DbfAdapter.adapt(a, &d).await?;
        let text = String::from_utf8(adapted_to_vec(out).await?)?
            .trim()
            .to_string();
        assert!(text.starts_with("dbf_version: 0xf5"), "got {text}");
        assert!(text.contains("alice\tlyon"), "got {text}");
        Ok(())
    }

    #[tokio::test]
    async fn dbf_skips_deleted_records() -> Result<()> {
        let data = make_dbf(&[("alice", "lyon"), ("bob", "nantes")], true);
        let (a, d) = simple_adapt_info(
            std::path::Path::new("cities.dbf"),
            Box::pin(Cursor::new(data)),
        );
        let out = DbfAdapter.adapt(a, &d).await?;
        let text = String::from_utf8(adapted_to_vec(out).await?)?
            .trim()
            .to_string();
        assert!(!text.contains("alice"), "deleted row leaked: {text}");
        assert!(text.contains("bob\tnantes"), "got {text}");
        Ok(())
    }

    #[tokio::test]
    async fn dbf_rejects_non_dbf() {
        let (a, d) = simple_adapt_info(
            std::path::Path::new("x.dbf"),
            Box::pin(Cursor::new(vec![0u8; 100])),
        );
        let res = DbfAdapter.adapt(a, &d).await;
        assert!(
            res.err()
                .expect("should bail")
                .downcast_ref::<AdapterBail>()
                .is_some()
        );
    }
}
