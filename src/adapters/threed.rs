//! Adapters for 3D modeling / geometry exchange formats.
//!
//! Covers the binary formats that `rg` cannot search directly:
//! - GLB (binary glTF): extracts the embedded JSON scene description
//! - STL: binary stereolithography files (ASCII STL is plain text and passes through)
//! - PLY: binary polygon file format (ASCII PLY passes through)
//! - FBX: binary Autodesk FBX node/property outline (ASCII FBX passes through)
//!
//! Text-based siblings (obj, mtl, off, usda, ascii stl/ply/fbx) need no
//! adapter — `rg` searches them natively.

use super::*;
use crate::adapted_iter::one_file;
use crate::config::RgaConfig;

use anyhow::{Result, bail, format_err};
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

fn finish(
    filepath_hint: PathBuf,
    line_prefix: String,
    archive_recursion_depth: i32,
    postprocess: bool,
    config: RgaConfig,
    text: Result<String>,
) -> Result<AdaptedFilesIterBox> {
    let text = text.map_err(|e| adapter_bail(format!("{e}")))?;
    text_result(
        filepath_hint,
        line_prefix,
        archive_recursion_depth,
        postprocess,
        config,
        text,
    )
}

// ---------------------------------------------------------------------------
// GLB (binary glTF)
// ---------------------------------------------------------------------------

lazy_static! {
    static ref GLB_META: AdapterMeta = meta(
        "glb",
        "Extracts the JSON scene description (nodes, meshes, materials, animations) from binary glTF (.glb) files",
        &["glb"]
    );
}

#[derive(Default)]
pub struct GlbAdapter;
impl GlbAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for GlbAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &GLB_META
    }
}

fn parse_glb(data: &[u8]) -> Result<String> {
    if data.len() < 12 || &data[0..4] != b"glTF" {
        bail!("not a GLB file (bad magic)");
    }
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let total_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    if version != 2 {
        bail!("unsupported GLB version {version}");
    }
    let mut out = format!("glb_version: {version}\n");
    let mut pos = 12;
    let mut json_seen = false;
    while pos + 8 <= data.len() && pos + 8 <= total_len.max(pos + 8) {
        let chunk_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let chunk_type = &data[pos + 4..pos + 8];
        let start = pos + 8;
        let end = start + chunk_len;
        if end > data.len() {
            bail!("GLB chunk overruns file");
        }
        match chunk_type {
            b"JSON" => {
                json_seen = true;
                // pretty-print when possible so grep shows one match per line
                match serde_json::from_slice::<serde_json::Value>(&data[start..end]) {
                    Ok(v) => {
                        out.push_str(&serde_json::to_string_pretty(&v)?);
                        out.push('\n');
                    }
                    Err(_) => {
                        out.push_str(&String::from_utf8_lossy(&data[start..end]));
                        out.push('\n');
                    }
                }
            }
            b"BIN\0" => {
                out.push_str(&format!("bin_chunk_bytes: {chunk_len}\n"));
            }
            other => {
                out.push_str(&format!(
                    "chunk: {} ({} bytes)\n",
                    String::from_utf8_lossy(other),
                    chunk_len
                ));
            }
        }
        pos = end;
    }
    if !json_seen {
        bail!("GLB has no JSON chunk");
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for GlbAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, path, prefix, depth, postprocess, config) = read_input!(ai);
        finish(path, prefix, depth, postprocess, config, parse_glb(&data))
    }
}

// ---------------------------------------------------------------------------
// STL
// ---------------------------------------------------------------------------

lazy_static! {
    static ref STL_META: AdapterMeta = meta(
        "stl",
        "Extracts header and triangle count from binary STL files (ASCII STL is searched as plain text)",
        &["stl"]
    );
}

#[derive(Default)]
pub struct StlAdapter;
impl StlAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for StlAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &STL_META
    }
}

fn stl_binary_triangle_count(data: &[u8]) -> Option<u32> {
    if data.len() < 84 {
        return None;
    }
    let count = u32::from_le_bytes(data[80..84].try_into().unwrap());
    // exact size match distinguishes binary from ascii even when the header
    // happens to start with "solid"
    if data.len() as u64 == 84 + 50 * count as u64 {
        Some(count)
    } else {
        None
    }
}

#[async_trait]
impl FileAdapter for StlAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, path, prefix, depth, postprocess, config) = read_input!(ai);
        let text = (|| {
            if let Some(count) = stl_binary_triangle_count(&data) {
                let header = String::from_utf8_lossy(&data[..80]);
                let header = header.trim_end_matches('\0').trim();
                let mut out = String::from("format: binary stl\n");
                if !header.is_empty() {
                    out.push_str(&format!("header: {header}\n"));
                }
                out.push_str(&format!("triangles: {count}\n"));
                return Ok(out);
            }
            if data.starts_with(b"solid") {
                // ASCII STL is plain text; searching the raw file is more useful
                bail!("ascii stl is plain text");
            }
            bail!("unrecognized stl file")
        })();
        finish(path, prefix, depth, postprocess, config, text)
    }
}

// ---------------------------------------------------------------------------
// PLY
// ---------------------------------------------------------------------------

lazy_static! {
    static ref PLY_META: AdapterMeta = meta(
        "ply",
        "Extracts element/property structure from binary PLY files (ASCII PLY is searched as plain text)",
        &["ply"]
    );
}

#[derive(Default)]
pub struct PlyAdapter;
impl PlyAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for PlyAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &PLY_META
    }
}

#[async_trait]
impl FileAdapter for PlyAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, path, prefix, depth, postprocess, config) = read_input!(ai);
        let text = (|| {
            if !data.starts_with(b"ply") {
                bail!("not a ply file");
            }
            let header_end = match find_subslice(&data, b"end_header") {
                Some(i) => i,
                None => bail!("ply header not terminated"),
            };
            let header = String::from_utf8_lossy(&data[..header_end + b"end_header".len()]);
            let mut format: Option<String> = None;
            let mut out = String::new();
            for line in header.lines() {
                let tokens: Vec<&str> = line.split_whitespace().collect();
                match tokens.first().copied() {
                    Some("format") if tokens.len() >= 2 => {
                        format = Some(tokens[1].to_string());
                        out.push_str(&format!("format: {}\n", tokens[1..].join(" ")));
                    }
                    Some("comment") => {
                        out.push_str(&format!("comment: {}\n", tokens[1..].join(" ")));
                    }
                    Some("element") if tokens.len() >= 3 => {
                        out.push_str(&format!("element {}: {}\n", tokens[1], tokens[2]));
                    }
                    Some("property") if tokens.len() >= 3 => {
                        out.push_str(&format!("  property {}\n", tokens[1..].join(" ")));
                    }
                    _ => {}
                }
            }
            match format.as_deref() {
                Some("ascii") => bail!("ascii ply is plain text"),
                Some(_) => Ok(out),
                None => bail!("ply header has no format line"),
            }
        })();
        finish(path, prefix, depth, postprocess, config, text)
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

// ---------------------------------------------------------------------------
// FBX (binary)
// ---------------------------------------------------------------------------

lazy_static! {
    static ref FBX_META: AdapterMeta = AdapterMeta {
        name: "fbx".to_owned(),
        version: 1,
        description: "Extracts the node tree and string properties from binary Autodesk FBX files (ASCII FBX is searched as plain text)".to_owned(),
        recurses: true,
        fast_matchers: vec![FastFileMatcher::FileExtension("fbx".to_string())],
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false,
    };
    static ref FBX_MAGIC: &'static [u8] = b"Kaydara FBX Binary  \x00\x1a\x00";
}

const FBX_MAX_LINES: usize = 100_000;

#[derive(Default)]
pub struct FbxAdapter;
impl FbxAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for FbxAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &FBX_META
    }
}

struct FbxReader<'a> {
    data: &'a [u8],
    pos: usize,
    big: bool, // version >= 7500: 64-bit node record fields
    lines: usize,
}

impl<'a> FbxReader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            bail!("fbx: unexpected end of file");
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn offset(&mut self) -> Result<u64> {
        if self.big {
            self.u64()
        } else {
            Ok(self.u32()? as u64)
        }
    }
    /// size of one null record at the current format width
    fn null_size(&self) -> usize {
        if self.big {
            25
        } else {
            13
        }
    }
}

fn fbx_prop_string(r: &mut FbxReader) -> Result<Option<String>> {
    let code = *r.take(1)?.first().unwrap();
    match code {
        b'Y' => {
            r.take(2)?;
        }
        b'C' => {
            r.take(1)?;
        }
        b'I' => {
            r.take(4)?;
        }
        b'F' => {
            r.take(4)?;
        }
        b'D' => {
            r.take(8)?;
        }
        b'L' => {
            r.take(8)?;
        }
        b'S' | b'R' => {
            let len = r.u32()? as usize;
            let bytes = r.take(len)?;
            if code == b'S' {
                return Ok(Some(String::from_utf8_lossy(bytes).into_owned()));
            }
        }
        b'f' | b'd' | b'l' | b'i' | b'b' => {
            let array_len = r.u32()? as usize;
            let encoding = r.u32()?;
            let comp_len = r.u32()? as usize;
            r.take(comp_len)?;
            let _ = (array_len, encoding);
        }
        other => bail!("fbx: unknown property type code {other:#x}"),
    }
    Ok(None)
}

/// read one node record; returns None on a null record (list terminator)
fn fbx_read_node(
    r: &mut FbxReader,
    out: &mut String,
    depth: usize,
) -> Result<Option<()>> {
    let record_start = r.pos;
    let end_offset = r.offset()?;
    let num_props = r.offset()?;
    let _prop_list_len = r.offset()?;
    let name_len = *r.take(1)?.first().unwrap() as usize;
    let name = String::from_utf8_lossy(r.take(name_len)?).into_owned();

    if end_offset == 0 {
        // null record: rewind so the caller can also detect it, then stop
        r.pos = record_start;
        return Ok(None);
    }

    if r.lines >= FBX_MAX_LINES {
        return Ok(Some(()));
    }
    r.lines += 1;

    let mut strings = Vec::new();
    for _ in 0..num_props {
        if let Some(s) = fbx_prop_string(r)? {
            // keep path-relevant strings (connections, names, types)
            if strings.len() < 8 {
                strings.push(s);
            }
        }
    }

    let indent = "  ".repeat(depth);
    if strings.is_empty() {
        out.push_str(&format!("{indent}{name}\n"));
    } else {
        out.push_str(&format!("{indent}{name}: {}\n", strings.join(" | ")));
    }

    // nested nodes until the null record marking the end of this node
    let body_end = end_offset as usize;
    loop {
        if r.pos + r.null_size() > r.data.len() {
            break;
        }
        // peek: a null record is all zeroes
        if r.data[r.pos..r.pos + r.null_size()].iter().all(|b| *b == 0) {
            r.pos += r.null_size();
            break;
        }
        if r.pos >= body_end {
            break;
        }
        fbx_read_node(r, out, depth + 1)?;
    }
    // be tolerant about trailing padding
    if r.pos < body_end && body_end <= r.data.len() {
        r.pos = body_end;
    }
    Ok(Some(()))
}

fn parse_fbx(data: &[u8]) -> Result<String> {
    if data.len() < 27 || &data[..FBX_MAGIC.len()] != *FBX_MAGIC {
        bail!("not a binary fbx file");
    }
    let version = u32::from_le_bytes(data[23..27].try_into().unwrap());
    let mut out = format!("fbx_version: {version}\n");
    let mut r = FbxReader {
        data,
        pos: 27,
        big: version >= 7500,
        lines: 0,
    };
    while r.pos + r.null_size() <= r.data.len() {
        if r.data[r.pos..r.pos + r.null_size()].iter().all(|b| *b == 0) {
            break;
        }
        if fbx_read_node(&mut r, &mut out, 0)?.is_none() {
            break;
        }
        if r.lines >= FBX_MAX_LINES {
            out.push_str("... (output truncated)\n");
            break;
        }
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for FbxAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        match parse_fbx(&data) {
            Ok(text) => text_result(filepath_hint, prefix, depth, postprocess, config, text),
            Err(e) => {
                // ASCII FBX is plain text and better searched raw
                if data.starts_with(b"; FBX") || data.starts_with(b"FBXHeaderExtension") {
                    return Err(adapter_bail("ascii fbx is plain text"));
                }
                Err(format_err!("fbx: {e} in {}", filepath_hint.display()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::assert_eq;
    use std::io::Cursor;

    async fn adapt_to_string(adapter: impl FileAdapter, name: &str, data: Vec<u8>) -> Result<String> {
        let (a, d) = simple_adapt_info(std::path::Path::new(name), Box::pin(Cursor::new(data)));
        let out = adapter.adapt(a, &d).await?;
        Ok(String::from_utf8(adapted_to_vec(out).await?)?.trim().to_string())
    }

    fn make_binary_stl(header: &[u8; 80], triangles: u32) -> Vec<u8> {
        let mut v = header.to_vec();
        v.extend_from_slice(&triangles.to_le_bytes());
        v.resize(84 + 50 * triangles as usize, 0);
        v
    }

    #[tokio::test]
    async fn stl_binary() -> Result<()> {
        let mut header = [0u8; 80];
        header[..9].copy_from_slice(b"test part");
        let data = make_binary_stl(&header, 42);
        let out = adapt_to_string(StlAdapter, "part.stl", data).await?;
        assert_eq!(out, "format: binary stl\nheader: test part\ntriangles: 42");
        Ok(())
    }

    #[tokio::test]
    async fn stl_ascii_bails() {
        let data = b"solid ascii\nendsolid".to_vec();
        let (a, d) = simple_adapt_info(std::path::Path::new("a.stl"), Box::pin(Cursor::new(data)));
        let res = StlAdapter.adapt(a, &d).await;
        let err = res.err().expect("should bail");
        assert!(err.downcast_ref::<AdapterBail>().is_some(), "got {err:?}");
    }

    fn make_glb(json: &str, bin_len: usize) -> Vec<u8> {
        let mut glb = Vec::new();
        let total = 12 + 8 + json.len() + if bin_len > 0 { 8 + bin_len } else { 0 };
        glb.extend_from_slice(b"glTF");
        glb.extend_from_slice(&2u32.to_le_bytes());
        glb.extend_from_slice(&(total as u32).to_le_bytes());
        glb.extend_from_slice(&(json.len() as u32).to_le_bytes());
        glb.extend_from_slice(b"JSON");
        glb.extend_from_slice(json.as_bytes());
        if bin_len > 0 {
            glb.extend_from_slice(&(bin_len as u32).to_le_bytes());
            glb.extend_from_slice(b"BIN\0");
            glb.resize(glb.len() + bin_len, 0);
        }
        glb
    }

    #[tokio::test]
    async fn glb_json_chunk() -> Result<()> {
        let glb = make_glb(r#"{"asset":{"version":"2.0"},"meshes":[{"name":"cube"}]}"#, 120);
        let out = adapt_to_string(GlbAdapter, "scene.glb", glb).await?;
        assert!(out.contains("glb_version: 2"));
        assert!(out.contains("\"version\": \"2.0\""), "got {out}");
        assert!(out.contains("cube"));
        assert!(out.contains("bin_chunk_bytes: 120"));
        Ok(())
    }

    #[tokio::test]
    async fn ply_binary_header() -> Result<()> {
        let header = b"ply\nformat binary_little_endian 1.0\ncomment made by test\nelement vertex 3\nproperty float x\nproperty float y\nelement face 1\nproperty list uchar int vertex_indices\nend_header\n";
        let mut data = header.to_vec();
        data.resize(header.len() + 100, 0);
        let out = adapt_to_string(PlyAdapter, "scan.ply", data).await?;
        assert!(out.contains("format: binary_little_endian 1.0"));
        assert!(out.contains("element vertex: 3"));
        assert!(out.contains("property float x"));
        assert!(out.contains("comment: made by test"));
        Ok(())
    }

    #[tokio::test]
    async fn ply_ascii_bails() {
        let data = b"ply\nformat ascii 1.0\nelement vertex 1\nend_header\n0 0 0\n".to_vec();
        let (a, d) = simple_adapt_info(std::path::Path::new("a.ply"), Box::pin(Cursor::new(data)));
        let res = PlyAdapter.adapt(a, &d).await;
        assert!(res.err().expect("should bail").downcast_ref::<AdapterBail>().is_some());
    }

    fn make_fbx(version: u32, node_name: &str, prop: &str) -> Vec<u8> {
        // small-format node: end, nprops, proplen, namelen, name, prop('S'), null(13)
        let name = node_name.as_bytes();
        let pb = prop.as_bytes();
        let node_len = 13 + name.len() + 5 + pb.len() + 13;
        let mut v = Vec::new();
        v.extend_from_slice(*FBX_MAGIC);
        v.extend_from_slice(&version.to_le_bytes());
        // one top-level node
        v.extend_from_slice(&(27 + node_len as u32).to_le_bytes()); // end offset
        v.extend_from_slice(&1u32.to_le_bytes()); // num props
        v.extend_from_slice(&((5 + pb.len()) as u32).to_le_bytes()); // prop list len
        v.push(name.len() as u8);
        v.extend_from_slice(name);
        v.push(b'S');
        v.extend_from_slice(&(pb.len() as u32).to_le_bytes());
        v.extend_from_slice(pb);
        v.extend_from_slice(&[0u8; 13]); // null record
        v
    }

    #[tokio::test]
    async fn fbx_binary_strings() -> Result<()> {
        let fbx = make_fbx(7400, "Objects", "Geometry::cube");
        let out = adapt_to_string(FbxAdapter, "cube.fbx", fbx).await?;
        assert!(out.contains("fbx_version: 7400"));
        assert!(out.contains("Objects: Geometry::cube"), "got {out}");
        Ok(())
    }

    #[tokio::test]
    async fn fbx_ascii_bails() {
        let data = b"; FBX 7.4.0\nFBXHeaderExtension:  {\n}".to_vec();
        let (a, d) = simple_adapt_info(std::path::Path::new("a.fbx"), Box::pin(Cursor::new(data)));
        let res = FbxAdapter.adapt(a, &d).await;
        assert!(res.err().expect("should bail").downcast_ref::<AdapterBail>().is_some());
    }
}
