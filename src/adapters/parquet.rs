//! Adapter for Apache Parquet files (datasets, ML pipelines, lakehouses).
//!
//! Parquet data pages are heavily compressed and out of scope for a text
//! search tool, but the file *footer* is a Thrift-compact-encoded
//! `FileMetaData` that contains exactly what an agent needs to identify a
//! dataset: the row count and the full column schema (names, primitive
//! types, repetition), including nested groups.
//!
//! This module implements the minimal Thrift compact protocol needed to
//! read that footer (no external dependencies, no Thrift codegen).

use super::*;
use crate::adapted_iter::one_file;
use crate::config::RgaConfig;

use anyhow::{Result, bail, format_err};
use lazy_static::lazy_static;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

lazy_static! {
    static ref PARQUET_META: AdapterMeta = AdapterMeta {
        name: "parquet".to_owned(),
        version: 1,
        description: "Extracts the row count and column schema (names, types, repetition, nesting) from the Parquet file footer".to_owned(),
        recurses: true,
        fast_matchers: vec![FastFileMatcher::FileExtension("parquet".to_string())],
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    };
}

const MAX_SCHEMA_ELEMENTS: usize = 100_000;

#[derive(Default)]
pub struct ParquetAdapter;
impl ParquetAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for ParquetAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &PARQUET_META
    }
}

// ---------------------------------------------------------------------------
// Thrift compact protocol reader (subset)
// ---------------------------------------------------------------------------

const T_STOP: u8 = 0;
const T_TRUE: u8 = 1;
const T_FALSE: u8 = 2;
const T_BYTE: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;

struct CReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> CReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            bail!("thrift: truncated");
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(*self.take(1)?.first().unwrap())
    }
    fn varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        for shift in (0..70).step_by(7) {
            let byte = self.u8()?;
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
        }
        bail!("thrift: varint too long")
    }
    fn zigzag(&mut self) -> Result<i64> {
        let v = self.varint()?;
        Ok(((v >> 1) as i64) ^ -((v & 1) as i64))
    }
    fn binary(&mut self) -> Result<&'a [u8]> {
        let len = self.varint()? as usize;
        if len > 1 << 24 {
            bail!("thrift: binary too long ({len})");
        }
        self.take(len)
    }
    fn list_header(&mut self) -> Result<(u8, usize)> {
        let b = self.u8()?;
        let elem_type = b & 0x0f;
        let size_nibble = (b >> 4) & 0x0f;
        let size = if size_nibble == 15 {
            self.varint()? as usize
        } else {
            size_nibble as usize
        };
        Ok((elem_type, size))
    }
    /// skip a value of the given type (unknown fields)
    fn skip(&mut self, type_id: u8) -> Result<()> {
        match type_id {
            T_TRUE | T_FALSE => Ok(()),
            T_BYTE => {
                self.u8()?;
                Ok(())
            }
            T_I16 | T_I32 | T_I64 => {
                self.zigzag()?;
                Ok(())
            }
            T_DOUBLE => {
                self.take(8)?;
                Ok(())
            }
            T_BINARY => {
                self.binary()?;
                Ok(())
            }
            T_LIST | T_SET => {
                let (elem_type, n) = self.list_header()?;
                if n > 1 << 20 {
                    bail!("thrift: list too long ({n})");
                }
                for _ in 0..n {
                    self.skip(elem_type)?;
                }
                Ok(())
            }
            T_MAP => {
                // compact protocol: single byte packs key (high nibble) and
                // value (low nibble) types; the size is ALWAYS a varint
                let b = self.u8()?;
                let key_type = (b >> 4) & 0x0f;
                let val_type = b & 0x0f;
                let n = self.varint()? as usize;
                if n > 1 << 20 {
                    bail!("thrift: map too long ({n})");
                }
                for _ in 0..n {
                    self.skip(key_type)?;
                    self.skip(val_type)?;
                }
                Ok(())
            }
            T_STRUCT => self.skip_struct(),
            other => bail!("thrift: unsupported type {other}"),
        }
    }
    /// read a struct skipping all fields (nested unknown structs)
    fn skip_struct(&mut self) -> Result<()> {
        loop {
            let b = self.u8()?;
            if b == T_STOP {
                return Ok(());
            }
            let type_id = b & 0x0f;
            if b >> 4 == 0 {
                // explicit field id follows
                self.zigzag()?;
            }
            self.skip(type_id)?;
        }
    }
}

/// read one field header; returns None on STOP; field ids accumulate deltas
fn field_begin(r: &mut CReader, last_id: &mut i16) -> Result<Option<u8>> {
    let b = r.u8()?;
    if b == T_STOP {
        return Ok(None);
    }
    let type_id = b & 0x0f;
    let delta = (b >> 4) & 0x0f;
    if delta == 0 {
        *last_id = r.zigzag()? as i16;
    } else {
        *last_id += delta as i16;
    }
    Ok(Some(type_id))
}

// ---------------------------------------------------------------------------
// Parquet footer (FileMetaData + SchemaElement subset)
// ---------------------------------------------------------------------------

struct SchemaElement {
    name: String,
    primitive_type: Option<i32>,
    repetition: Option<i32>,
    num_children: i32,
}

fn parquet_type_name(t: i32) -> &'static str {
    match t {
        0 => "BOOLEAN",
        1 => "INT32",
        2 => "INT64",
        3 => "INT96",
        4 => "FLOAT",
        5 => "DOUBLE",
        6 => "BYTE_ARRAY",
        7 => "FIXED_LEN_BYTE_ARRAY",
        _ => "UNKNOWN",
    }
}

fn parquet_repetition_name(r: i32) -> &'static str {
    match r {
        0 => "REQUIRED",
        1 => "OPTIONAL",
        2 => "REPEATED",
        _ => "UNKNOWN",
    }
}

fn read_schema_element(r: &mut CReader) -> Result<SchemaElement> {
    let mut el = SchemaElement {
        name: String::new(),
        primitive_type: None,
        repetition: None,
        num_children: 0,
    };
    let mut last_id: i16 = 0;
    while let Some(type_id) = field_begin(r, &mut last_id)? {
        match (last_id, type_id) {
            (1, T_I32) => el.primitive_type = Some(r.zigzag()? as i32),
            (3, T_I32) => el.repetition = Some(r.zigzag()? as i32),
            (4, T_BINARY) => el.name = String::from_utf8_lossy(r.binary()?).into_owned(),
            (5, T_I32) => el.num_children = r.zigzag()? as i32,
            _ => r.skip(type_id)?,
        }
    }
    Ok(el)
}

fn parse_footer(metadata: &[u8]) -> Result<(i64, Vec<SchemaElement>)> {
    let mut r = CReader::new(metadata);
    // parquet writers usually serialize FileMetaData directly as a compact
    // struct; some include the full thrift message envelope (0x82 + version)
    if r.u8()? == 0x82 {
        r.u8()?; // protocol version
    } else {
        r.pos = 0;
    }
    let mut num_rows: Option<i64> = None;
    let mut schema: Option<Vec<SchemaElement>> = None;
    let mut last_id: i16 = 0;
    while let Some(type_id) = field_begin(&mut r, &mut last_id)? {
        match (last_id, type_id) {
            (2, T_LIST) => {
                let (elem_type, n) = r.list_header()?;
                if elem_type != T_STRUCT || n > MAX_SCHEMA_ELEMENTS {
                    bail!("parquet schema list malformed");
                }
                let mut elems = Vec::with_capacity(n);
                for _ in 0..n {
                    elems.push(read_schema_element(&mut r)?);
                }
                schema = Some(elems);
            }
            (3, T_I64) => num_rows = Some(r.zigzag()?),
            _ => r.skip(type_id)?,
        }
    }
    let schema = schema.ok_or_else(|| format_err!("parquet footer has no schema"))?;
    Ok((num_rows.unwrap_or(-1), schema))
}

/// render the flat schema element list as an indented tree using the
/// `num_children` nesting of the root group. recursion depth is capped so a
/// crafted footer cannot overflow the stack; deeper levels flatten out.
fn render_schema(elems: &[SchemaElement]) -> String {
    const MAX_DEPTH: usize = 64;
    let mut out = String::new();
    let mut idx = 0;
    fn render(elems: &[SchemaElement], idx: &mut usize, depth: usize, out: &mut String) {
        if *idx >= elems.len() {
            return;
        }
        let el = &elems[*idx];
        *idx += 1;
        let indent = "  ".repeat(depth.min(MAX_DEPTH));
        if el.num_children > 0 {
            out.push_str(&format!("{indent}{} (group)\n", el.name));
            if depth < MAX_DEPTH {
                for _ in 0..el.num_children {
                    render(elems, idx, depth + 1, out);
                }
            }
        } else {
            let t = el
                .primitive_type
                .map(parquet_type_name)
                .unwrap_or("UNKNOWN");
            let rep = el.repetition.map(parquet_repetition_name).unwrap_or("");
            out.push_str(&format!("{indent}{}: {t} {rep}\n", el.name));
        }
    }
    render(elems, &mut idx, 0, &mut out);
    out
}

fn parse_parquet(data: &[u8]) -> Result<String> {
    if data.len() < 12 {
        bail!("parquet file too small");
    }
    if &data[0..4] != b"PAR1" {
        bail!("not a parquet file (bad magic)");
    }
    let footer_len_pos = data.len() - 8;
    if &data[footer_len_pos + 4..data.len()] != b"PAR1" {
        bail!("not a parquet file (bad footer magic)");
    }
    let metadata_len =
        u32::from_le_bytes(data[footer_len_pos..footer_len_pos + 4].try_into().unwrap()) as usize;
    if metadata_len + 8 > data.len() {
        bail!("parquet footer length overruns file");
    }
    let metadata = &data[data.len() - 8 - metadata_len..footer_len_pos];
    let (num_rows, schema) = parse_footer(metadata)?;
    Ok(format!("num_rows: {num_rows}\n{}", render_schema(&schema)))
}

#[async_trait]
impl FileAdapter for ParquetAdapter {
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
        let text = parse_parquet(&data).map_err(|e| adapter_bail(format!("parquet: {e}")))?;
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

    // -- minimal thrift-compact encoder for fixtures --

    fn zz(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let mut b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            out.push(b);
            if v == 0 {
                break;
            }
        }
    }
    fn field(delta: i16, type_id: u8, out: &mut Vec<u8>) {
        out.push(((delta as u8) << 4) | type_id);
    }
    fn i32_field(delta: i16, v: i32, out: &mut Vec<u8>) {
        field(delta, T_I32, out);
        varint(zz(v as i64), out);
    }
    fn i64_field(delta: i16, v: i64, out: &mut Vec<u8>) {
        field(delta, T_I64, out);
        varint(zz(v), out);
    }
    fn bin_field(delta: i16, s: &str, out: &mut Vec<u8>) {
        field(delta, T_BINARY, out);
        varint(s.len() as u64, out);
        out.extend_from_slice(s.as_bytes());
    }
    fn stop(out: &mut Vec<u8>) {
        out.push(T_STOP);
    }

    fn make_group(name: &str, num_children: i32) -> Vec<u8> {
        let mut v = Vec::new();
        bin_field(4, name, &mut v); // field id 4 (delta 4): name
        i32_field(1, num_children, &mut v); // field id 5: num_children
        stop(&mut v);
        v
    }

    fn make_leaf(name: &str, ptype: i32, rep: i32) -> Vec<u8> {
        let mut v = Vec::new();
        i32_field(1, ptype, &mut v); // field id 1: primitive type
        i32_field(2, rep, &mut v); // field id 3 (delta 2): repetition
        bin_field(1, name, &mut v); // field id 4 (delta 1): name
        stop(&mut v);
        v
    }

    fn make_parquet(metadata_body: &[u8]) -> Vec<u8> {
        // real parquet files serialize FileMetaData without the thrift
        // message envelope, so the fixture does the same
        let footer = metadata_body.to_vec();
        let mut file = vec![];
        file.extend_from_slice(b"PAR1");
        file.extend_from_slice(&footer);
        file.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        file.extend_from_slice(b"PAR1");
        file
    }

    #[tokio::test]
    async fn parquet_flat_schema() -> Result<()> {
        // FileMetaData: version(1), schema(2), num_rows(3)
        let mut md = Vec::new();
        i32_field(1, 1, &mut md);
        field(1, T_LIST, &mut md);
        // list<struct> in the extended-length form, 3 elements:
        // root group + 2 leaf columns
        md.push(0xf0 | T_STRUCT);
        varint(3, &mut md);
        md.extend_from_slice(&make_group("message", 2));
        md.extend_from_slice(&make_leaf("name", 6, 1)); // BYTE_ARRAY OPTIONAL
        md.extend_from_slice(&make_leaf("score", 5, 1)); // DOUBLE OPTIONAL
        i64_field(1, 1000000, &mut md); // num_rows (delta 1 from field 2)
        stop(&mut md);
        let file = make_parquet(&md);

        let (a, d) = simple_adapt_info(
            std::path::Path::new("data.parquet"),
            Box::pin(Cursor::new(file)),
        );
        let out = ParquetAdapter.adapt(a, &d).await?;
        let text = String::from_utf8(adapted_to_vec(out).await?)?
            .trim()
            .to_string();
        assert_eq!(
            text,
            "num_rows: 1000000\nmessage (group)\n  name: BYTE_ARRAY OPTIONAL\n  score: DOUBLE OPTIONAL"
        );
        Ok(())
    }

    #[tokio::test]
    async fn parquet_deeply_nested_schema_does_not_stack_overflow() {
        // a crafted footer with a linear chain of 10k nested groups must
        // not overflow the stack; output flattens at the depth cap
        const N: usize = 10_000;
        let mut md = Vec::new();
        i32_field(1, 1, &mut md);
        field(1, T_LIST, &mut md);
        md.push(0xf0 | T_STRUCT);
        varint(N as u64, &mut md);
        for i in 0..N {
            md.extend_from_slice(&make_group(&format!("g{i}"), 1));
        }
        // final element: a leaf so the chain ends
        md.extend_from_slice(&make_leaf("leaf", 1, 0));
        i64_field(1, 1, &mut md);
        stop(&mut md);
        let file = make_parquet(&md);

        let (a, d) = simple_adapt_info(
            std::path::Path::new("deep.parquet"),
            Box::pin(Cursor::new(file)),
        );
        let out = ParquetAdapter.adapt(a, &d).await;
        let text =
            String::from_utf8(adapted_to_vec(out.expect("should parse")).await.unwrap()).unwrap();
        // cap at MAX_DEPTH=64: 65 groups render (g0..g64), then output stops
        assert!(
            text.contains("g0 (group)"),
            "got {}",
            text.lines().next().unwrap_or("")
        );
        assert!(text.contains("g64 (group)"), "missing capped tail");
        assert!(!text.contains("g65"), "should flatten at the depth cap");
    }

    #[tokio::test]
    async fn parquet_map_field_is_skipped_via_varint_size() {
        // FileMetaData with an unknown field holding a compact map with a
        // varint size > 14 (old code read the size nibble and bailed).
        // field ids ascend: version(1), schema(2), num_rows(3), map(4)
        let mut md = Vec::new();
        i32_field(1, 1, &mut md); // version
        field(1, T_LIST, &mut md); // field 2: schema
        md.push(0xf0 | T_STRUCT);
        varint(2, &mut md);
        md.extend_from_slice(&make_group("message", 1));
        md.extend_from_slice(&make_leaf("leaf", 1, 0)); // INT32 REQUIRED
        i64_field(1, 0, &mut md); // field 3: num_rows
        // unknown field 4: map<binary, binary> with 20 entries (varint size)
        field(1, T_MAP, &mut md);
        md.push((T_BINARY << 4) | T_BINARY);
        varint(20, &mut md);
        for i in 0..20 {
            let k = format!("k{i}");
            varint(k.len() as u64, &mut md);
            md.extend_from_slice(k.as_bytes());
            let v = format!("v{i}");
            varint(v.len() as u64, &mut md);
            md.extend_from_slice(v.as_bytes());
        }
        stop(&mut md);
        let file = make_parquet(&md);

        let (a, d) = simple_adapt_info(
            std::path::Path::new("map.parquet"),
            Box::pin(Cursor::new(file)),
        );
        let out = ParquetAdapter.adapt(a, &d).await.expect("should parse");
        let text = String::from_utf8(adapted_to_vec(out).await.unwrap()).unwrap();
        assert!(text.contains("message (group)"), "got {text}");
        assert!(text.contains("leaf: INT32 REQUIRED"), "got {text}");
        assert!(text.contains("num_rows: 0"), "got {text}");
    }
}
