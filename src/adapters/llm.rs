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
}
