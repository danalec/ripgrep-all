//! Adapters for machine-learning model files, aimed at LLM-era workflows:
//! - GGUF (llama.cpp et al.): parses the metadata key-value section, so
//!   `rga "context_length" model.gguf` finds architectures, tokenizer info,
//!   RoPE parameters etc. without loading the (often huge) tensor data.
//! - Safetensors (Hugging Face): lists tensor names, dtypes, shapes and the
//!   `__metadata__` dict (often holds training config / model card hints).
//!
//! These formats are binary and unsearchable with plain `rg`, but their
//! metadata is exactly what an agent needs to identify a model variant.

use super::*;
use crate::adapted_iter::one_file;
use crate::config::RgaConfig;

use anyhow::{Context, Result, bail, format_err};
use lazy_static::lazy_static;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

fn meta(name: &str, description: &str, extensions: &[&str]) -> AdapterMeta {
    AdapterMeta {
        name: name.to_owned(),
        version: 1,
        description: description.to_owned(),
        recurses: true,
        fast_matchers: extensions
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    }
}

fn text_result(
    filepath_hint: PathBuf,
    line_prefix: String,
    archive_recursion_depth: i32,
    postprocess: bool,
    config: RgaConfig,
    text: String,
) -> Result<AdaptedFilesIterBox> {
    let mut out_path = filepath_hint;
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

// ---------------------------------------------------------------------------
// GGUF
// ---------------------------------------------------------------------------

lazy_static! {
    static ref GGUF_META: AdapterMeta = meta(
        "gguf",
        "Extracts the metadata key-value section (architecture, context length, tokenizer, RoPE) from GGUF model files",
        &["gguf"]
    );
}

#[derive(Default)]
pub struct GgufAdapter;
impl GgufAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for GgufAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &GGUF_META
    }
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            bail!("unexpected end of file");
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(*self.take(1)?.first().unwrap())
    }
    fn i8(&mut self) -> Result<i8> {
        Ok(self.u8()? as i8)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

/// format a gguf metadata value; arrays are truncated with a count hint
fn gguf_value(r: &mut Reader, type_id: u32, depth: u32) -> Result<String> {
    const ARRAY_MAX: usize = 8;
    match type_id {
        0 => Ok(format!("{}", r.u8()?)),
        1 => Ok(format!("{}", r.i8()?)),
        2 => Ok(format!("{}", r.u16()?)),
        3 => Ok(format!("{}", r.i16()?)),
        4 => Ok(format!("{}", r.u32()?)),
        5 => Ok(format!("{}", r.i32()?)),
        6 => Ok(format!("{}", r.f32()?)),
        7 => Ok(if r.u8()? != 0 { "true" } else { "false" }.to_string()),
        8 => {
            let len = r.u64()? as usize;
            if len > 1 << 24 {
                bail!("gguf string too long ({len})");
            }
            Ok(format!("\"{}\"", String::from_utf8_lossy(r.take(len)?)))
        }
        9 => {
            if depth > 0 {
                bail!("nested gguf arrays are not supported by the spec");
            }
            let elem_type = r.u32()?;
            if elem_type == 9 {
                bail!("array of arrays is not a valid gguf type");
            }
            let len = r.u64()? as usize;
            if len <= ARRAY_MAX {
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    items.push(gguf_value(r, elem_type, depth + 1)?);
                }
                Ok(format!("[{}]", items.join(", ")))
            } else {
                let mut items = Vec::with_capacity(ARRAY_MAX);
                for _ in 0..ARRAY_MAX {
                    items.push(gguf_value(r, elem_type, depth + 1)?);
                }
                // consume the rest without formatting
                for _ in ARRAY_MAX..len {
                    gguf_value(r, elem_type, depth + 1)?;
                }
                Ok(format!("[{}, ... ({} items)]", items.join(", "), len))
            }
        }
        10 => Ok(format!("{}", r.u64()?)),
        11 => Ok(format!("{}", r.i64()?)),
        12 => Ok(format!("{}", r.f64()?)),
        other => bail!("unknown gguf value type {other}"),
    }
}

fn parse_gguf(data: &[u8]) -> Result<String> {
    if data.len() < 24 || &data[0..4] != b"GGUF" {
        bail!("not a GGUF file (bad magic)");
    }
    let mut r = Reader::new(data);
    r.take(4)?;
    let version = r.u32()?;
    // GGUF v1 used 32-bit counts and string lengths
    let read_count = |r: &mut Reader, v: u32| -> Result<u64> {
        if v == 1 {
            Ok(r.u32()? as u64)
        } else {
            r.u64()
        }
    };
    let read_str = |r: &mut Reader, v: u32| -> Result<String> {
        let len = read_count(r, v)? as usize;
        if len > 1 << 20 {
            bail!("gguf key too long ({len})");
        }
        Ok(String::from_utf8_lossy(r.take(len)?).into_owned())
    };
    let tensor_count = read_count(&mut r, version)?;
    let kv_count = read_count(&mut r, version)?;

    let mut out = format!("gguf_version: {version}\ntensors: {tensor_count}\n");
    for _ in 0..kv_count {
        let key = read_str(&mut r, version)?;
        let type_id = r.u32()?;
        let value = gguf_value(&mut r, type_id, 0)
            .with_context(|| format!("reading gguf metadata key {key:?}"))?;
        out.push_str(&format!("{key}: {value}\n"));
        if out.len() > 10_000_000 {
            out.push_str("... (metadata output truncated)\n");
            break;
        }
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for GgufAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_gguf(&data)
            .map_err(|e| format_err!("gguf: {e} in {}", filepath_hint.display()))?;
        text_result(filepath_hint, prefix, depth, postprocess, config, text)
    }
}

// ---------------------------------------------------------------------------
// Safetensors
// ---------------------------------------------------------------------------

lazy_static! {
    static ref SAFETENSORS_META: AdapterMeta = meta(
        "safetensors",
        "Extracts tensor names, dtypes, shapes and __metadata__ from Hugging Face Safetensors files",
        &["safetensors"]
    );
}

#[derive(Default)]
pub struct SafetensorsAdapter;
impl SafetensorsAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for SafetensorsAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &SAFETENSORS_META
    }
}

fn parse_safetensors(data: &[u8]) -> Result<String> {
    if data.len() < 8 {
        bail!("safetensors file too small");
    }
    let header_len = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    if 8 + header_len > data.len() {
        bail!("safetensors header overruns file");
    }
    let header: serde_json::Value =
        serde_json::from_slice(&data[8..8 + header_len]).context("safetensors header is not valid JSON")?;
    let obj = header
        .as_object()
        .ok_or_else(|| format_err!("safetensors header is not an object"))?;

    let mut out = String::new();
    // __metadata__ first: it is the human-authored part
    if let Some(md) = obj.get("__metadata__").and_then(|v| v.as_object()) {
        for (k, v) in md {
            out.push_str(&format!("{k}: {}\n", v.as_str().unwrap_or("<non-string>")));
        }
    }
    let mut tensors: Vec<(&String, &serde_json::Value)> = obj
        .iter()
        .filter(|(k, _)| k.as_str() != "__metadata__")
        .collect();
    // serde_json preserves insertion order when built with preserve_order
    tensors.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (name, info) in tensors {
        let dtype = info.get("dtype").and_then(|v| v.as_str()).unwrap_or("?");
        let shape = info
            .get("shape")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .map(|x| x.as_i64().map(|n| n.to_string()).unwrap_or("?".into()))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        out.push_str(&format!("tensor {name}: dtype={dtype} shape=[{shape}]\n"));
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for SafetensorsAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_safetensors(&data)
            .map_err(|e| format_err!("safetensors: {e} in {}", filepath_hint.display()))?;
        text_result(filepath_hint, prefix, depth, postprocess, config, text)
    }
}

// ---------------------------------------------------------------------------
// NPY (NumPy array header)
// ---------------------------------------------------------------------------

lazy_static! {
    static ref NPY_META: AdapterMeta = meta(
        "npy",
        "Extracts the dtype, shape and memory-layout header from NumPy .npy array files (data payload is not decoded)",
        &["npy"]
    );
}

#[derive(Default)]
pub struct NpyAdapter;
impl NpyAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for NpyAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &NPY_META
    }
}

fn parse_npy(data: &[u8]) -> Result<String> {
    if data.len() < 10 || &data[0..6] != b"\x93NUMPY" {
        bail!("not an NPY file (bad magic)");
    }
    let major = data[6];
    let (header_len, header_start) = match major {
        1 => (
            u16::from_le_bytes(data[8..10].try_into().unwrap()) as usize,
            10,
        ),
        2 | 3 => {
            if data.len() < 12 {
                bail!("npy header truncated");
            }
            (
                u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize,
                12,
            )
        }
        other => bail!("unsupported npy format version {other}"),
    };
    if header_start + header_len > data.len() {
        bail!("npy header overruns file");
    }
    let header = String::from_utf8_lossy(&data[header_start..header_start + header_len]);
    // the header is a python dict literal like
    // {'descr': '<f4', 'fortran_order': False, 'shape': (1000, 768), }
    let extract = |key: &str| -> Option<String> {
        let pos = header.find(&format!("'{key}':"))?;
        let rest = header[pos + key.len() + 4..].trim_start();
        let end = match rest.chars().next() {
            Some('(') => {
                // tuple: scan to the matching close paren
                let mut depth = 0;
                rest.find(|c| {
                    if c == '(' {
                        depth += 1;
                    }
                    if c == ')' {
                        depth -= 1;
                    }
                    depth == 0
                })?
                    + 1
            }
            Some('\'') => rest[1..].find('\'')? + 2,
            _ => rest
                .find([',', '\n', '}'])
                .unwrap_or(rest.len()),
        };
        Some(rest[..end].trim().trim_matches('\'').to_string())
    };
    let descr = extract("descr").ok_or_else(|| format_err!("npy header has no descr"))?;
    let fortran = extract("fortran_order").unwrap_or_else(|| "?".into());
    let shape = extract("shape").ok_or_else(|| format_err!("npy header has no shape"))?;
    Ok(format!(
        "descr: {descr}\nfortran_order: {fortran}\nshape: {shape}\n"
    ))
}

#[async_trait]
impl FileAdapter for NpyAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_npy(&data)
            .map_err(|e| adapter_bail(format!("npy: {e}")))?;
        text_result(filepath_hint, prefix, depth, postprocess, config, text)
    }
}

// ---------------------------------------------------------------------------
// Generic protobuf string extractor (ONNX, TensorFlow .pb, SentencePiece ...)
// ---------------------------------------------------------------------------

lazy_static! {
    static ref PROTOBUF_META: AdapterMeta = AdapterMeta {
        name: "protobuf".to_owned(),
        version: 1,
        description: "Walks the protobuf wire format and extracts field strings (node/op names, metadata) from ONNX and other protobuf model files".to_owned(),
        recurses: true,
        fast_matchers: vec![
            FastFileMatcher::FileExtension("onnx".to_string()),
            FastFileMatcher::FileExtension("pb".to_string()),
        ],
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    };
}

#[derive(Default)]
pub struct ProtobufAdapter;
impl ProtobufAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for ProtobufAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &PROTOBUF_META
    }
}

const PROTO_MAX_LINES: usize = 100_000;

fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    for shift in (0..70).step_by(7) {
        if *pos >= data.len() {
            bail!("protobuf: truncated varint");
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    bail!("protobuf: varint too long")
}

/// is this byte slice a plausible human-authored string field?
fn looks_like_string(bytes: &[u8]) -> bool {
    if bytes.len() < 2 || bytes.len() > 512 || bytes.contains(&0) {
        return false;
    }
    let s = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let printable = s
        .chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
        .count();
    printable == s.chars().count()
}

/// try to interpret `bytes` as a nested message; returns how many strings it
/// contained if it parsed strictly to the end
fn protobuf_walk(
    data: &[u8],
    depth: usize,
    out: &mut String,
    lines: &mut usize,
) -> Result<usize> {
    let mut pos = 0;
    let mut strings = 0;
    while pos < data.len() {
        if *lines >= PROTO_MAX_LINES {
            return Ok(strings);
        }
        let tag = read_varint(data, &mut pos)?;
        if tag == 0 || tag > u32::MAX as u64 {
            bail!("protobuf: invalid tag {tag}");
        }
        let field = tag >> 3;
        match tag & 7 {
            0 => {
                read_varint(data, &mut pos)?;
            }
            1 => {
                if pos + 8 > data.len() {
                    bail!("protobuf: truncated fixed64");
                }
                pos += 8;
            }
            5 => {
                if pos + 4 > data.len() {
                    bail!("protobuf: truncated fixed32");
                }
                pos += 4;
            }
            2 => {
                let len = read_varint(data, &mut pos)? as usize;
                if pos + len > data.len() {
                    bail!("protobuf: length-delimited field overruns buffer");
                }
                let bytes = &data[pos..pos + len];
                pos += len;
                if looks_like_string(bytes) {
                    strings += 1;
                    *lines += 1;
                    out.push_str(&format!(
                        "field{field}: {}\n",
                        String::from_utf8_lossy(bytes)
                    ));
                } else if depth < 6 {
                    let mut nested = String::new();
                    let mut nested_lines = 0;
                    if protobuf_walk(bytes, depth + 1, &mut nested, &mut nested_lines).is_ok()
                        && !nested.is_empty()
                    {
                        for line in nested.lines() {
                            out.push_str(&format!("field{field}.{line}\n"));
                        }
                        *lines += nested_lines;
                    }
                }
            }
            other => bail!("protobuf: unsupported wire type {other}"),
        }
    }
    Ok(strings)
}

fn parse_protobuf(data: &[u8]) -> Result<String> {
    let mut out = String::new();
    let mut lines = 0;
    protobuf_walk(data, 0, &mut out, &mut lines)?;
    if out.is_empty() {
        bail!("protobuf: no string fields found");
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for ProtobufAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_protobuf(&data)
            .map_err(|e| adapter_bail(format!("protobuf: {e}")))?;
        text_result(filepath_hint, prefix, depth, postprocess, config, text)
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
        Ok(String::from_utf8(adapted_to_vec(out).await?)?.trim().to_string())
    }

    fn make_gguf_v2(kvs: &[(String, u32, Vec<u8>)]) -> Vec<u8> {
        // kv tuple: (key, type_id, encoded value bytes)
        let mut v = Vec::new();
        v.extend_from_slice(b"GGUF");
        v.extend_from_slice(&2u32.to_le_bytes());
        v.extend_from_slice(&1u64.to_le_bytes()); // tensor_count
        v.extend_from_slice(&(kvs.len() as u64).to_le_bytes());
        for (k, type_id, val) in kvs {
            v.extend_from_slice(&(k.len() as u64).to_le_bytes());
            v.extend_from_slice(k.as_bytes());
            v.extend_from_slice(&type_id.to_le_bytes());
            v.extend_from_slice(val);
        }
        v
    }

    fn str_val(s: &str) -> Vec<u8> {
        let mut v = (s.len() as u64).to_le_bytes().to_vec();
        v.extend_from_slice(s.as_bytes());
        v
    }

    #[tokio::test]
    async fn gguf_metadata() -> Result<()> {
        let mut rope = Vec::new();
        rope.extend_from_slice(&4u32.to_le_bytes()); // elem type u32
        rope.extend_from_slice(&3u64.to_le_bytes()); // len
        for x in [1u32, 2, 3] {
            rope.extend_from_slice(&x.to_le_bytes());
        }
        let data = make_gguf_v2(&[
            ("general.architecture".into(), 8, str_val("llama")),
            ("llama.context_length".into(), 4, 4096u32.to_le_bytes().to_vec()),
            ("llama.rope.dimension_count".into(), 9, rope),
        ]);
        let out = adapt_to_string(GgufAdapter, "model.gguf", data).await?;
        assert_eq!(
            out,
            "gguf_version: 2\ntensors: 1\ngeneral.architecture: \"llama\"\nllama.context_length: 4096\nllama.rope.dimension_count: [1, 2, 3]"
        );
        Ok(())
    }

    #[tokio::test]
    async fn gguf_rejects_wrong_magic() {
        let data = b"NOTGGUFdata........................".to_vec();
        let (a, d) = simple_adapt_info(std::path::Path::new("m.gguf"), Box::pin(Cursor::new(data)));
        let res = GgufAdapter.adapt(a, &d).await;
        let err = res.err().expect("should fail");
        assert!(err.to_string().contains("bad magic"), "got {err:?}");
    }

    #[tokio::test]
    async fn safetensors_header() -> Result<()> {
        let header = serde_json::json!({
            "__metadata__": {"format": "pt"},
            "weight": {"dtype": "F16", "shape": [2, 3], "data_offsets": [0, 12]},
            "bias": {"dtype": "F32", "shape": [3], "data_offsets": [12, 24]},
        });
        let header_s = serde_json::to_string(&header)?;
        let mut data = (header_s.len() as u64).to_le_bytes().to_vec();
        data.extend_from_slice(header_s.as_bytes());
        data.resize(data.len() + 24, 0);
        let out = adapt_to_string(SafetensorsAdapter, "model.safetensors", data).await?;
        assert_eq!(
            out,
            "format: pt\ntensor bias: dtype=F32 shape=[3]\ntensor weight: dtype=F16 shape=[2, 3]"
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // NPY
    // -----------------------------------------------------------------------

    fn make_npy(header_dict: &str, payload: usize) -> Vec<u8> {
        let mut v = b"\x93NUMPY\x01\x00".to_vec();
        v.extend_from_slice(&(header_dict.len() as u16).to_le_bytes());
        v.extend_from_slice(header_dict.as_bytes());
        v.resize(v.len() + payload, 0);
        v
    }

    #[tokio::test]
    async fn npy_header_v1() -> Result<()> {
        let data = make_npy("{'descr': '<f4', 'fortran_order': False, 'shape': (1000, 768), }", 1000 * 768 * 4);
        let out = adapt_to_string(NpyAdapter, "embeddings.npy", data).await?;
        assert_eq!(out, "descr: <f4\nfortran_order: False\nshape: (1000, 768)");
        Ok(())
    }

    #[tokio::test]
    async fn npy_rejects_bad_magic() {
        let data = b"\x94NUMPY garbage".to_vec();
        let (a, d) = simple_adapt_info(std::path::Path::new("x.npy"), Box::pin(Cursor::new(data)));
        let res = NpyAdapter.adapt(a, &d).await;
        assert!(res.err().expect("should bail").downcast_ref::<AdapterBail>().is_some());
    }

    // -----------------------------------------------------------------------
    // Protobuf
    // -----------------------------------------------------------------------

    fn pb_varint(mut v: u64) -> Vec<u8> {
        let mut out = Vec::new();
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
        out
    }

    fn pb_string_field(field: u64, s: &str) -> Vec<u8> {
        let mut v = pb_varint(field << 3 | 2);
        v.extend_from_slice(&pb_varint(s.len() as u64));
        v.extend_from_slice(s.as_bytes());
        v
    }

    #[tokio::test]
    async fn protobuf_strings_nested() -> Result<()> {
        // field 1: "llama"; field 2: nested message {field 1: "quantized"};
        // field 3: varint 7
        let mut data = pb_string_field(1, "llama");
        let mut nested = pb_string_field(1, "quantized");
        nested.extend_from_slice(&pb_varint(3 << 3));
        nested.extend_from_slice(&pb_varint(7));
        data.extend_from_slice(&pb_varint(2 << 3 | 2));
        data.extend_from_slice(&pb_varint(nested.len() as u64));
        data.extend_from_slice(&nested);
        let out = adapt_to_string(ProtobufAdapter, "model.onnx", data).await?;
        assert!(out.contains("field1: llama"), "got {out}");
        assert!(out.contains("quantized"), "got {out}");
        Ok(())
    }

    #[tokio::test]
    async fn protobuf_rejects_non_protobuf() {
        let data = vec![0xffu8; 100];
        let (a, d) = simple_adapt_info(std::path::Path::new("x.pb"), Box::pin(Cursor::new(data)));
        let res = ProtobufAdapter.adapt(a, &d).await;
        assert!(res.err().expect("should bail").downcast_ref::<AdapterBail>().is_some());
    }
}
