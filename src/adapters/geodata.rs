//! Adapters for geospatial and image-with-metadata formats:
//! - Shapefile (.shp): ESRI geometry format; reads the 100-byte header and
//!   walks the record index to report the shape type, bounding box and
//!   per-record shape type histogram
//! - TIFF (.tif/.tiff, including GeoTIFF and BigTIFF): reads the first IFD
//!   and decodes ASCII tags (ImageDescription, Make, Model, Software,
//!   DateTime, ...) plus the numeric GeoTIFF keys (pixel scale, tiepoints,
//!   geokey directory)
//!
//! Text-based geospatial formats (GeoJSON, KML, GPX, GML, WKT) and
//! attribute tables in GeoPackage/MBTiles (SQLite) need no adapter.

use super::*;
use crate::adapted_iter::one_file;
use crate::config::RgaConfig;

use anyhow::{Result, bail};
use lazy_static::lazy_static;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

macro_rules! read_input {
    ($ai:ident) => {{
        let AdaptInfo {
            filepath_hint,
            line_prefix,
            archive_recursion_depth,
            postprocess,
            config,
            mut inp,
            ..
        } = $ai;
        let mut data = Vec::new();
        inp.read_to_end(&mut data).await?;
        (
            data,
            filepath_hint,
            line_prefix,
            archive_recursion_depth,
            postprocess,
            config,
        )
    }};
}

fn text_result(
    filepath_hint: PathBuf,
    line_prefix: String,
    archive_recursion_depth: i32,
    postprocess: bool,
    config: RgaConfig,
    text: String,
) -> Result<AdaptedFilesIterBox> {
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

// ---------------------------------------------------------------------------
// Shapefile
// ---------------------------------------------------------------------------

lazy_static! {
    static ref SHP_META: AdapterMeta = AdapterMeta {
        name: "shp".to_owned(),
        version: 1,
        description: "Extracts the geometry type, bounding box and record/shape-type index from ESRI shapefiles".to_owned(),
        recurses: true,
        fast_matchers: vec![FastFileMatcher::FileExtension("shp".to_string())],
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    };
}

#[derive(Default)]
pub struct ShpAdapter;
impl ShpAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for ShpAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &SHP_META
    }
}

fn shape_type_name(t: u32) -> &'static str {
    match t {
        0 => "Null",
        1 => "Point",
        3 => "PolyLine",
        5 => "Polygon",
        8 => "MultiPoint",
        11 => "PointZ",
        13 => "PolyLineZ",
        15 => "PolygonZ",
        18 => "MultiPointZ",
        21 => "PointM",
        23 => "PolyLineM",
        25 => "PolygonM",
        28 => "MultiPointM",
        31 => "MultiPatch",
        _ => "Unknown",
    }
}

fn parse_shp(data: &[u8]) -> Result<String> {
    if data.len() < 100 {
        bail!("shapefile header truncated");
    }
    if u32::from_be_bytes(data[0..4].try_into().unwrap()) != 9994 {
        bail!("not a shapefile (bad file code)");
    }
    let version = u32::from_le_bytes(data[28..32].try_into().unwrap());
    let shape_type = u32::from_le_bytes(data[32..36].try_into().unwrap());
    let get_f64 = |off: usize| f64::from_le_bytes(data[off..off + 8].try_into().unwrap());
    let (xmin, ymin, xmax, ymax) = (get_f64(36), get_f64(44), get_f64(52), get_f64(60));
    let mut out = format!(
        "version: {version}\nshape_type: {} ({})\nbbox: {xmin} {ymin} {xmax} {ymax}\n",
        shape_type_name(shape_type),
        shape_type
    );
    // walk the record index: 8-byte BE header + content length in 16-bit words
    let mut pos = 100usize;
    let mut records: u64 = 0;
    let mut types: std::collections::BTreeMap<u32, u64> = Default::default();
    while pos + 8 <= data.len() {
        let content_words = u32::from_be_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let rec_start = pos + 8;
        let rec_end = rec_start + content_words * 2;
        if rec_end > data.len() || content_words == 0 {
            break;
        }
        let t = u32::from_le_bytes(data[rec_start..rec_start + 4].try_into().unwrap());
        *types.entry(t).or_default() += 1;
        records += 1;
        pos = rec_end;
    }
    out.push_str(&format!("records: {records}\n"));
    for (t, n) in types {
        out.push_str(&format!("  {}: {n}\n", shape_type_name(t)));
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for ShpAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, path, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_shp(&data).map_err(|e| adapter_bail(format!("shp: {e}")))?;
        text_result(path, prefix, depth, postprocess, config, text)
    }
}

// ---------------------------------------------------------------------------
// TIFF / GeoTIFF
// ---------------------------------------------------------------------------

lazy_static! {
    static ref TIFF_META: AdapterMeta = AdapterMeta {
        name: "tiff".to_owned(),
        version: 1,
        description: "Extracts ASCII tags (description, make, model, software, datetime) and GeoTIFF geokeys from TIFF image files".to_owned(),
        recurses: true,
        fast_matchers: vec![
            FastFileMatcher::FileExtension("tif".to_string()),
            FastFileMatcher::FileExtension("tiff".to_string()),
        ],
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    };
    /// geotiff tags worth emitting even though they are numeric
    static ref NUMERIC_TAGS: std::collections::HashMap<u16, &'static str> = {
        let mut m = std::collections::HashMap::new();
        m.insert(33550, "ModelPixelScale");
        m.insert(33922, "ModelTiepoint");
        m.insert(34264, "ModelTransformation");
        m.insert(34735, "GeoKeyDirectory");
        m.insert(34736, "GeoDoubleParams");
        m.insert(34737, "GeoAsciiParams");
        m
    };
}

#[derive(Default)]
pub struct TiffAdapter;
impl TiffAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for TiffAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &TIFF_META
    }
}

struct TiffReader<'a> {
    data: &'a [u8],
    big_endian: bool,
    big: bool, // BigTIFF: 64-bit counts and offsets
}

impl<'a> TiffReader<'a> {
    fn u16(&self, off: usize) -> Result<u16> {
        if off + 2 > self.data.len() {
            bail!("tiff: truncated");
        }
        let b = &self.data[off..off + 2];
        Ok(if self.big_endian {
            u16::from_be_bytes(b.try_into().unwrap())
        } else {
            u16::from_le_bytes(b.try_into().unwrap())
        })
    }
    fn u32(&self, off: usize) -> Result<u32> {
        if off + 4 > self.data.len() {
            bail!("tiff: truncated");
        }
        let b = &self.data[off..off + 4];
        Ok(if self.big_endian {
            u32::from_be_bytes(b.try_into().unwrap())
        } else {
            u32::from_le_bytes(b.try_into().unwrap())
        })
    }
    fn u64(&self, off: usize) -> Result<u64> {
        if off + 8 > self.data.len() {
            bail!("tiff: truncated");
        }
        let b = &self.data[off..off + 8];
        Ok(if self.big_endian {
            u64::from_be_bytes(b.try_into().unwrap())
        } else {
            u64::from_le_bytes(b.try_into().unwrap())
        })
    }
    fn count(&self, off: usize) -> Result<u64> {
        if self.big {
            self.u64(off)
        } else {
            Ok(self.u32(off)? as u64)
        }
    }
}

fn parse_tiff(data: &[u8]) -> Result<String> {
    if data.len() < 8 {
        bail!("tiff file too small");
    }
    let big_endian = match &data[0..2] {
        b"II" => false,
        b"MM" => true,
        _ => bail!("not a TIFF file (bad byte order marker)"),
    };
    let magic = if big_endian {
        u16::from_be_bytes(data[2..4].try_into().unwrap())
    } else {
        u16::from_le_bytes(data[2..4].try_into().unwrap())
    };
    let (big, _value_area) = match magic {
        42 => (false, 4usize),
        43 => (true, 8),
        _ => bail!("not a TIFF file (bad magic {magic})"),
    };
    let r = TiffReader {
        data,
        big_endian,
        big,
    };
    let mut ifd_off = if big {
        r.u64(8)? as usize
    } else {
        r.u32(4)? as usize
    };
    let mut out = String::new();
    let mut visited = 0;
    // follow the IFD chain (main + subIFDs), bounded
    while ifd_off != 0 && visited < 8 {
        visited += 1;
        let n = r.u16(ifd_off)? as usize;
        let entry_size = if big { 20 } else { 12 };
        let entries_start = ifd_off + 2;
        if entries_start + n * entry_size > data.len() {
            bail!("tiff: IFD overruns file");
        }
        for i in 0..n {
            let e = entries_start + i * entry_size;
            let tag = r.u16(e)?;
            let type_id = r.u16(e + 2)?;
            let count = r.count(e + 4)?;
            let value_off = e + if big { 12 } else { 8 };
            // TIFF field types: 1=BYTE 2=ASCII 3=SHORT 4=LONG 5=RATIONAL
            // 6=SBYTE 7=UNDEFINED 8=SSHORT 9=SLONG 10=SRATIONAL 11=FLOAT 12=DOUBLE
            match type_id {
                2 if count >= 1 => {
                    // ASCII: inline if it fits the value area, else offset
                    let (off, inline) = if big {
                        (r.u64(value_off)? as usize, count <= 8)
                    } else {
                        (r.u32(value_off)? as usize, count <= 4)
                    };
                    let str_off = if inline { value_off } else { off };
                    if str_off + count as usize > data.len() {
                        continue;
                    }
                    let bytes = &data[str_off..str_off + count as usize];
                    let s = String::from_utf8_lossy(bytes)
                        .trim_end_matches('\0')
                        .trim()
                        .to_string();
                    if !s.is_empty()
                        && s.chars().filter(|c| !c.is_control()).count() == s.chars().count()
                    {
                        out.push_str(&format!("tag{tag}: {s}\n"));
                    }
                }
                3 | 4 | 12 | 11 if NUMERIC_TAGS.contains_key(&tag) => {
                    let name = NUMERIC_TAGS[&tag];
                    let elem_size = match type_id {
                        3 => 2,
                        4 => 4,
                        11 => 4,
                        _ => 8,
                    };
                    let (off, inline_limit) = if big {
                        (r.u64(value_off)? as usize, 8)
                    } else {
                        (r.u32(value_off)? as usize, 4)
                    };
                    let arr_off = if count as usize * elem_size <= inline_limit {
                        value_off
                    } else {
                        off
                    };
                    let show = count.min(8) as usize;
                    if arr_off + show * elem_size > data.len() {
                        continue;
                    }
                    let mut vals = Vec::new();
                    for j in 0..show {
                        let v = match type_id {
                            3 => r.u16(arr_off + j * 2)?.to_string(),
                            4 => r.u32(arr_off + j * 4)?.to_string(),
                            11 => {
                                let o = arr_off + j * 4;
                                let b: [u8; 4] = data[o..o + 4].try_into().unwrap();
                                let f = if r.big_endian {
                                    f32::from_be_bytes(b)
                                } else {
                                    f32::from_le_bytes(b)
                                };
                                format!("{f}")
                            }
                            _ => {
                                let o = arr_off + j * 8;
                                let b: [u8; 8] = data[o..o + 8].try_into().unwrap();
                                let f = if r.big_endian {
                                    f64::from_be_bytes(b)
                                } else {
                                    f64::from_le_bytes(b)
                                };
                                format!("{f}")
                            }
                        };
                        vals.push(v);
                    }
                    if count > 8 {
                        vals.push(format!("... ({} values)", count));
                    }
                    out.push_str(&format!("{name}: {}\n", vals.join(" ")));
                }
                _ => {}
            }
        }
        // next IFD offset follows the entries
        let next_off = entries_start + n * entry_size;
        ifd_off = if big {
            r.u64(next_off)? as usize
        } else {
            r.u32(next_off)? as usize
        };
    }
    if out.is_empty() {
        bail!("tiff: no readable tags found");
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for TiffAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, path, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_tiff(&data).map_err(|e| adapter_bail(format!("tiff: {e}")))?;
        text_result(path, prefix, depth, postprocess, config, text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::assert_eq;
    use std::io::Cursor;

    async fn adapt_to_string(
        adapter: impl FileAdapter,
        name: &str,
        data: Vec<u8>,
    ) -> Result<String> {
        let (a, d) = simple_adapt_info(std::path::Path::new(name), Box::pin(Cursor::new(data)));
        let out = adapter.adapt(a, &d).await?;
        Ok(String::from_utf8(adapted_to_vec(out).await?)?
            .trim()
            .to_string())
    }

    fn make_shp(shape_type: u32, records: &[u32]) -> Vec<u8> {
        let mut v = vec![0u8; 100];
        v[0..4].copy_from_slice(&9994u32.to_be_bytes());
        v[28..32].copy_from_slice(&1000u32.to_le_bytes());
        v[32..36].copy_from_slice(&shape_type.to_le_bytes());
        v[36..44].copy_from_slice(&1.0f64.to_le_bytes());
        v[52..60].copy_from_slice(&9.0f64.to_le_bytes());
        for (i, t) in records.iter().enumerate() {
            // record header: number + content length (16-bit words), BE
            let content: Vec<u8> = t.to_le_bytes().to_vec(); // just the shape type word
            let words = (content.len() / 2) as u32;
            v.extend_from_slice(&((i as u32) + 1).to_be_bytes());
            v.extend_from_slice(&words.to_be_bytes());
            v.extend_from_slice(&content);
        }
        v
    }

    #[tokio::test]
    async fn shp_header_and_records() -> Result<()> {
        let shp = make_shp(5, &[5, 5, 1]);
        let out = adapt_to_string(ShpAdapter, "roads.shp", shp).await?;
        assert!(out.contains("shape_type: Polygon (5)"), "got {out}");
        assert!(out.contains("bbox: 1 0 9 0"), "got {out}");
        assert!(out.contains("records: 3"), "got {out}");
        assert!(out.contains("Polygon: 2"), "got {out}");
        assert!(out.contains("Point: 1"), "got {out}");
        Ok(())
    }

    #[tokio::test]
    async fn shp_rejects_non_shp() {
        let (a, d) = simple_adapt_info(
            std::path::Path::new("x.shp"),
            Box::pin(Cursor::new(vec![0u8; 200])),
        );
        let res = ShpAdapter.adapt(a, &d).await;
        assert!(
            res.err()
                .expect("should bail")
                .downcast_ref::<AdapterBail>()
                .is_some()
        );
    }

    /// minimal little-endian classic TIFF with one ASCII tag (ImageDescription
    /// = "satellite scene") stored out-of-line
    fn make_tiff() -> Vec<u8> {
        let desc = b"satellite scene\0";
        let mut v = Vec::new();
        v.extend_from_slice(b"II");
        v.extend_from_slice(&42u16.to_le_bytes());
        v.extend_from_slice(&8u32.to_le_bytes()); // IFD at offset 8
        // IFD with 1 entry
        v.extend_from_slice(&1u16.to_le_bytes());
        v.extend_from_slice(&270u16.to_le_bytes()); // ImageDescription
        v.extend_from_slice(&2u16.to_le_bytes()); // ASCII
        v.extend_from_slice(&(desc.len() as u32).to_le_bytes());
        v.extend_from_slice(&26u32.to_le_bytes()); // value offset after IFD (8+2+12+4)
        v.extend_from_slice(&0u32.to_le_bytes()); // next IFD
        v.extend_from_slice(desc);
        v
    }

    #[tokio::test]
    async fn tiff_ascii_tag() -> Result<()> {
        let out = adapt_to_string(TiffAdapter, "scene.tif", make_tiff()).await?;
        assert_eq!(out, "tag270: satellite scene");
        Ok(())
    }

    #[tokio::test]
    async fn tiff_rejects_non_tiff() {
        let (a, d) = simple_adapt_info(
            std::path::Path::new("x.tif"),
            Box::pin(Cursor::new(vec![0u8; 100])),
        );
        let res = TiffAdapter.adapt(a, &d).await;
        assert!(
            res.err()
                .expect("should bail")
                .downcast_ref::<AdapterBail>()
                .is_some()
        );
    }
}
