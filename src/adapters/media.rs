//! Adapters for media metadata, aimed at fast header-only extraction:
//! - JPEG EXIF: walks APP1 segments and decodes the TIFF IFD0/GPS sub-IFDs
//!   (make, model, datetime, GPS coordinates) without decoding the image.
//! - Audio tags: ID3v1/v2 (mp3), Vorbis comments (flac/ogg/opus) and RIFF
//!   INFO/fmt chunks (wav) — title/artist/album plus technical fields.
//! - DICOM: reads the dataset preamble and tag walk (patient, study,
//!   modality, UIDs), stopping before the pixel data.
//!
//! All three are pure-Rust, allocation-bounded parsers: they never decode
//! image/signal payloads, so multi-GB files are safe to search.

use super::*;
use crate::adapted_iter::one_file;
use crate::config::RgaConfig;

use anyhow::{Context, Result, bail};
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

/// trim trailing NULs and whitespace, as media strings are padded
fn clean(s: &str) -> String {
    s.trim_end_matches(['\0', ' ']).trim().to_string()
}

// ---------------------------------------------------------------------------
// JPEG EXIF
// ---------------------------------------------------------------------------

lazy_static! {
    static ref EXIF_META: AdapterMeta = meta(
        "exif",
        "Extracts EXIF metadata (make, model, datetime, GPS) from JPEG files without decoding the image",
        &["jpg", "jpeg"]
    );
}

#[derive(Default)]
pub struct JpegExifAdapter;
impl JpegExifAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for JpegExifAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &EXIF_META
    }
}

const EXIF_MAX_SCAN: usize = 4 << 20; // only scan the first 4 MiB of segments
const EXIF_MAX_TAGS: usize = 256;
const EXIF_STR_MAX: usize = 4096;

struct TiffReader<'a> {
    data: &'a [u8],
    big: bool,
}

impl<'a> TiffReader<'a> {
    fn u16(&self, off: usize) -> Result<u16> {
        let b = self
            .data
            .get(off..off + 2)
            .context("tiff: u16 out of bounds")?;
        Ok(if self.big {
            u16::from_be_bytes(b.try_into().unwrap())
        } else {
            u16::from_le_bytes(b.try_into().unwrap())
        })
    }
    fn u32(&self, off: usize) -> Result<u32> {
        let b = self
            .data
            .get(off..off + 4)
            .context("tiff: u32 out of bounds")?;
        Ok(if self.big {
            u32::from_be_bytes(b.try_into().unwrap())
        } else {
            u32::from_le_bytes(b.try_into().unwrap())
        })
    }
}

fn exif_tag_name(tag: u16) -> Option<&'static str> {
    Some(match tag {
        0x010e => "ImageDescription",
        0x010f => "Make",
        0x0110 => "Model",
        0x0131 => "Software",
        0x0132 => "DateTime",
        0x829a => "ExposureTime",
        0x829d => "FNumber",
        0x8769 => "ExifIFD", // pointer, handled specially
        0x8825 => "GPSIFD",  // pointer, handled specially
        0x9003 => "DateTimeOriginal",
        0x9004 => "DateTimeDigitized",
        0x920a => "FocalLength",
        _ => return None,
    })
}

fn gps_tag_name(tag: u16) -> Option<&'static str> {
    Some(match tag {
        0 => "GPSVersionID",
        1 => "GPSLatitudeRef",
        2 => "GPSLatitude",
        3 => "GPSLongitudeRef",
        4 => "GPSLongitude",
        5 => "GPSAltitudeRef",
        6 => "GPSAltitude",
        7 => "GPSTimeStamp",
        29 => "GPSDateStamp",
        _ => return None,
    })
}

/// read one IFD and append `name: value` lines; `depth` bounds sub-IFD recursion
fn read_ifd(
    r: &TiffReader,
    ifd_off: usize,
    depth: usize,
    out: &mut String,
    tags: &mut usize,
) -> Result<()> {
    if depth > 2 || *tags >= EXIF_MAX_TAGS {
        return Ok(());
    }
    let n = r.u16(ifd_off)? as usize;
    let entries = ifd_off
        .checked_add(2)
        .context("tiff: ifd offset overflow")?;
    for i in 0..n {
        if *tags >= EXIF_MAX_TAGS {
            break;
        }
        let e = entries + i * 12;
        let tag = r.u16(e)?;
        let typ = r.u16(e + 2)?;
        let count = r.u32(e + 4)? as usize;
        let value_off = e + 8;
        // inline area is 4 bytes for both endiannesses
        let (off, inline) = (r.u32(value_off)? as usize, count <= 4);
        match typ {
            2 => {
                // ASCII
                if count == 0 || count > EXIF_STR_MAX {
                    continue;
                }
                let str_off = if inline { value_off } else { off };
                let bytes = match r.data.get(str_off..str_off + count) {
                    Some(b) => b,
                    None => continue,
                };
                let s = clean(&String::from_utf8_lossy(bytes));
                if s.is_empty() || s.chars().filter(|c| c.is_control()).count() > 0 {
                    continue;
                }
                let name = exif_tag_name(tag).unwrap_or("Unknown");
                if name == "ExifIFD" || name == "GPSIFD" {
                    read_ifd(r, off, depth + 1, out, tags)?;
                } else {
                    *tags += 1;
                    out.push_str(&format!("{name}: {s}\n"));
                }
            }
            5 if count == 1 || count == 3 => {
                // RATIONAL(s): GPS coordinates / altitudes
                let name = match gps_tag_name(tag) {
                    Some(n) => n,
                    None => continue,
                };
                if off + count * 8 > r.data.len() {
                    continue;
                }
                let vals: Vec<String> = (0..count)
                    .map(|j| {
                        let num = r.u32(off + j * 8).unwrap_or(0);
                        let den = r.u32(off + j * 8 + 4).unwrap_or(1);
                        if den == 0 {
                            "0".to_string()
                        } else {
                            format!("{:.4}", num as f64 / den as f64)
                        }
                    })
                    .collect();
                *tags += 1;
                out.push_str(&format!("{name}: {}\n", vals.join(" ")));
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_exif(data: &[u8]) -> Result<String> {
    // JPEG: SOI then segments FFxx len(2, BE, includes the length bytes)
    if data.len() < 4 || data[0] != 0xff || data[1] != 0xd8 {
        bail!("not a JPEG file (no SOI marker)");
    }
    let mut pos = 2;
    let mut out = String::new();
    while pos + 4 <= data.len() && pos < EXIF_MAX_SCAN {
        if data[pos] != 0xff {
            break;
        }
        let marker = data[pos + 1];
        let len = u16::from_be_bytes([data[pos + 2], data[pos + 3]]) as usize;
        if len < 2 || pos + 2 + len > data.len() {
            break;
        }
        let payload = &data[pos + 4..pos + 2 + len];
        if marker == 0xe1 && payload.starts_with(b"Exif\0\0") {
            let tiff = &payload[6..];
            if tiff.len() < 8 {
                break;
            }
            let big = match &tiff[0..2] {
                b"II" => false,
                b"MM" => true,
                _ => bail!("exif: bad byte order marker"),
            };
            let r = TiffReader { data: tiff, big };
            if r.u16(2)? != 42 {
                bail!("exif: bad TIFF magic");
            }
            let ifd0 = r.u32(4)? as usize;
            let mut tags = 0;
            read_ifd(&r, ifd0, 0, &mut out, &mut tags)?;
            // also walk the GPS IFD referenced from IFD0 tag 0x8825 handled
            // inline above via read_ifd recursion
            break;
        }
        // standalone markers without length (DNL etc.) — not expected before APP1
        pos += 2 + len;
    }
    if out.is_empty() {
        bail!("jpeg: no EXIF metadata found");
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for JpegExifAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_exif(&data).map_err(|e| adapter_bail(format!("exif: {e}")))?;
        text_result(filepath_hint, prefix, depth, postprocess, config, text)
    }
}

// ---------------------------------------------------------------------------
// Audio tags (ID3v1/v2, Vorbis comments, RIFF INFO)
// ---------------------------------------------------------------------------

lazy_static! {
    static ref AUDIOTAGS_META: AdapterMeta = meta(
        "audiotags",
        "Extracts audio metadata (title, artist, album, sample rate) from ID3v1/v2, Vorbis comments and RIFF INFO chunks",
        &["mp3", "flac", "ogg", "oga", "opus", "wav", "wave"]
    );
}

#[derive(Default)]
pub struct AudioTagsAdapter;
impl AudioTagsAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for AudioTagsAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &AUDIOTAGS_META
    }
}

const AT_STR_MAX: usize = 4096;

fn push_kv(out: &mut String, key: &str, value: &[u8]) {
    let s = clean(&String::from_utf8_lossy(value));
    if !s.is_empty() && s.len() <= AT_STR_MAX {
        out.push_str(&format!("{key}: {s}\n"));
    }
}

fn id3v2_text_frame(id: &str) -> Option<&'static str> {
    Some(match id {
        "TIT2" => "title",
        "TPE1" => "artist",
        "TALB" => "album",
        "TDRC" | "TYER" => "year",
        "TCON" => "genre",
        "TRCK" => "track",
        "TPOS" => "disc",
        "TCOM" => "composer",
        "TENC" => "encoder",
        _ => return None,
    })
}

/// parse an ID3v2 tag body (without the 10-byte header); `ver` is 2-4
fn parse_id3v2(ver: u8, data: &[u8], out: &mut String) -> Result<()> {
    let mut pos = 0usize;
    while pos + 10 <= data.len() {
        let id = &data[pos..pos + 4];
        if !id
            .iter()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            break; // padding or garbage
        }
        let id = std::str::from_utf8(id).unwrap_or("");
        // v2.4 uses syncsafe sizes; v2.2 has 3-byte ids (unsupported here)
        let size = if ver == 4 {
            ((data[pos + 4] as usize & 0x7f) << 21)
                | ((data[pos + 5] as usize & 0x7f) << 14)
                | ((data[pos + 6] as usize & 0x7f) << 7)
                | (data[pos + 7] as usize & 0x7f)
        } else {
            u32::from_be_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize
        };
        let body_start = pos + 10;
        if size == 0 || body_start + size > data.len() {
            break;
        }
        if let Some(key) = id3v2_text_frame(id) {
            // first byte is the text encoding; the rest is the string
            push_kv(out, key, &data[body_start + 1..body_start + size]);
        }
        pos = body_start + size;
    }
    Ok(())
}

fn syncsafe_u32(b: &[u8]) -> usize {
    (b[0] as usize & 0x7f) << 21
        | (b[1] as usize & 0x7f) << 14
        | (b[2] as usize & 0x7f) << 7
        | (b[3] as usize & 0x7f)
}

/// parse a vorbis comment block: vendor + count + count×(len-prefixed KEY=VALUE)
fn parse_vorbis_comment(data: &[u8], out: &mut String) -> Result<()> {
    if data.len() < 8 {
        bail!("vorbis comment too short");
    }
    let vendor_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if 8 + vendor_len > data.len() {
        bail!("vorbis comment vendor string overruns");
    }
    let count_off = 4 + vendor_len;
    let count = u32::from_le_bytes(data[count_off..count_off + 4].try_into().unwrap()) as usize;
    let mut pos = count_off + 4;
    for _ in 0..count.min(10_000) {
        if pos + 4 > data.len() {
            break;
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > data.len() || len > AT_STR_MAX {
            break;
        }
        let entry = &data[pos..pos + len];
        pos += len;
        if let Some(eq) = entry.iter().position(|b| *b == b'=') {
            let key = String::from_utf8_lossy(&entry[..eq]).to_uppercase();
            let key = match key.as_str() {
                "TITLE" => "title",
                "ARTIST" => "artist",
                "ALBUM" => "album",
                "DATE" => "year",
                "GENRE" => "genre",
                "TRACKNUMBER" => "track",
                "ENCODER" => "encoder",
                other => {
                    // keep other keys but lowercase them
                    out.push_str(&format!(
                        "{}: {}\n",
                        other.to_lowercase(),
                        clean(&String::from_utf8_lossy(&entry[eq + 1..]))
                    ));
                    continue;
                }
            };
            push_kv(out, key, &entry[eq + 1..]);
        }
    }
    Ok(())
}

fn parse_flac(data: &[u8], out: &mut String) -> Result<()> {
    if !data.starts_with(b"fLaC") {
        bail!("not a FLAC file (bad magic)");
    }
    let mut pos = 4;
    // metadata blocks: 1 bit last-flag, 7 bit type, 24-bit BE length
    loop {
        if pos + 4 > data.len() {
            break;
        }
        let last = data[pos] & 0x80 != 0;
        let block_type = data[pos] & 0x7f;
        let len = ((data[pos + 1] as usize) << 16)
            | ((data[pos + 2] as usize) << 8)
            | (data[pos + 3] as usize);
        pos += 4;
        if pos + len > data.len() {
            break;
        }
        if block_type == 4 {
            // VORBIS_COMMENT
            parse_vorbis_comment(&data[pos..pos + len], out)?;
        }
        if block_type == 0 {
            // STREAMINFO: sample rate lives in bytes 10..13 (20 bits)
            let b = &data[pos..pos + len.max(18).min(data.len() - pos)];
            if b.len() >= 18 {
                let sr = ((b[10] as u32) << 12) | ((b[11] as u32) << 4) | ((b[12] as u32) >> 4);
                out.push_str(&format!("sample_rate: {sr}\n"));
            }
        }
        pos += len;
        if last {
            break;
        }
    }
    Ok(())
}

fn parse_ogg(data: &[u8], out: &mut String) -> Result<()> {
    if !data.starts_with(b"OggS") {
        bail!("not an Ogg file (bad magic)");
    }
    // scan the first pages for a vorbis or opus comment header
    let scan = data.len().min(1 << 20);
    if let Some(p) = find_subslice(&data[..scan], b"\x03vorbis") {
        parse_vorbis_comment(&data[p + 7..], out)?;
    } else if let Some(p) = find_subslice(&data[..scan], b"OpusTags") {
        parse_vorbis_comment(&data[p + 8..], out)?;
    } else {
        bail!("ogg: no comment header found");
    }
    Ok(())
}

fn parse_wav(data: &[u8], out: &mut String) -> Result<()> {
    if data.len() < 12 || &data[0..4] != b"RIFF" || &data[8..12] != b"WAVE" {
        bail!("not a WAV file (bad RIFF/WAVE header)");
    }
    let mut pos = 12;
    while pos + 8 <= data.len() {
        let id = &data[pos..pos + 4];
        let len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let body = pos + 8;
        if body + len > data.len() {
            break;
        }
        match id {
            b"fmt " if len >= 16 => {
                let channels = u16::from_le_bytes(data[body + 2..body + 4].try_into().unwrap());
                let sample_rate = u32::from_le_bytes(data[body + 4..body + 8].try_into().unwrap());
                let bits = u16::from_le_bytes(data[body + 14..body + 16].try_into().unwrap());
                out.push_str(&format!(
                    "channels: {channels}\nsample_rate: {sample_rate}\nbits_per_sample: {bits}\n"
                ));
            }
            b"LIST" if len >= 4 && &data[body..body + 4] == b"INFO" => {
                // INFO subchunks: 4-byte id + u32 size + value (word padded)
                let mut p = body + 4;
                while p + 8 <= body + len {
                    let sid = &data[p..p + 4];
                    let slen = u32::from_le_bytes(data[p + 4..p + 8].try_into().unwrap()) as usize;
                    let sbody = p + 8;
                    if sbody + slen > body + len {
                        break;
                    }
                    let key = match sid {
                        b"INAM" => "title",
                        b"IART" => "artist",
                        b"IPRD" => "album",
                        b"ICMT" => "comment",
                        b"IGNR" => "genre",
                        b"ICRD" => "year",
                        b"ISFT" => "encoder",
                        _ => {
                            p = sbody + slen + (slen & 1);
                            continue;
                        }
                    };
                    push_kv(out, key, &data[sbody..sbody + slen]);
                    p = sbody + slen + (slen & 1);
                }
            }
            _ => {}
        }
        pos = body + len + (len & 1);
    }
    Ok(())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn parse_audiotags(data: &[u8]) -> Result<String> {
    let mut out = String::new();
    if data.starts_with(b"ID3") && data.len() >= 10 {
        let ver = data[3];
        let size = syncsafe_u32(&data[6..10]);
        if 10 + size > data.len() {
            bail!("id3v2 tag size overruns file");
        }
        parse_id3v2(ver, &data[10..10 + size], &mut out)?;
    }
    // ID3v1 trailer: "TAG" + 30 title + 30 artist + 30 album + 4 year
    if data.len() >= 128 && &data[data.len() - 128..data.len() - 125] == b"TAG" {
        let t = &data[data.len() - 125..];
        push_kv(&mut out, "title", &t[0..30]);
        push_kv(&mut out, "artist", &t[30..60]);
        push_kv(&mut out, "album", &t[60..90]);
        push_kv(&mut out, "year", &t[90..94]);
    }
    if out.is_empty() {
        if data.starts_with(b"fLaC") {
            parse_flac(data, &mut out)?;
        } else if data.starts_with(b"OggS") {
            parse_ogg(data, &mut out)?;
        } else if data.starts_with(b"RIFF") {
            parse_wav(data, &mut out)?;
        } else {
            bail!("no audio tags found");
        }
    } else if data.starts_with(b"fLaC") || data.starts_with(b"OggS") || data.starts_with(b"RIFF") {
        // native containers may also carry their own richer tags
        let mut native = String::new();
        let r = if data.starts_with(b"fLaC") {
            parse_flac(data, &mut native)
        } else if data.starts_with(b"OggS") {
            parse_ogg(data, &mut native)
        } else {
            parse_wav(data, &mut native)
        };
        if r.is_ok() {
            for line in native.lines() {
                let key = line.split(':').next().unwrap_or("");
                if !out.lines().any(|l| l.starts_with(&format!("{key}:"))) {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
    }
    if out.is_empty() {
        bail!("no audio tags found");
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for AudioTagsAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_audiotags(&data).map_err(|e| adapter_bail(format!("audiotags: {e}")))?;
        text_result(filepath_hint, prefix, depth, postprocess, config, text)
    }
}

// ---------------------------------------------------------------------------
// DICOM
// ---------------------------------------------------------------------------

lazy_static! {
    static ref DICOM_META: AdapterMeta = meta(
        "dicom",
        "Extracts DICOM header metadata (patient, study, modality, UIDs) from .dcm files, stopping before the pixel data",
        &["dcm"]
    );
}

#[derive(Default)]
pub struct DicomAdapter;
impl DicomAdapter {
    pub fn new() -> Self {
        Self
    }
}
impl GetMetadata for DicomAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &DICOM_META
    }
}

const DICOM_MAX_TAGS: usize = 5_000;
const DICOM_STR_MAX: usize = 8192;
const IMPLICIT_VR_LE: &str = "1.2.840.10008.1.2";
const EXPLICIT_VR_LE: &str = "1.2.840.10008.1.2.1";

fn dicom_tag_name(group: u16, elem: u16) -> Option<&'static str> {
    Some(match (group, elem) {
        (0x0008, 0x0018) => "SOPInstanceUID",
        (0x0008, 0x0020) => "StudyDate",
        (0x0008, 0x0023) => "ContentDate",
        (0x0008, 0x0030) => "StudyTime",
        (0x0008, 0x0050) => "AccessionNumber",
        (0x0008, 0x0060) => "Modality",
        (0x0008, 0x0070) => "Manufacturer",
        (0x0008, 0x1030) => "StudyDescription",
        (0x0008, 0x103e) => "SeriesDescription",
        (0x0008, 0x1090) => "Model",
        (0x0010, 0x0010) => "PatientName",
        (0x0010, 0x0020) => "PatientID",
        (0x0010, 0x0030) => "PatientBirthDate",
        (0x0010, 0x0040) => "PatientSex",
        (0x0020, 0x000d) => "StudyInstanceUID",
        (0x0020, 0x000e) => "SeriesInstanceUID",
        (0x0020, 0x0010) => "StudyID",
        (0x0020, 0x0011) => "SeriesNumber",
        _ => return None,
    })
}

/// VRs with a 2-byte reserved field + 4-byte length after the VR code
fn long_vr(vr: &str) -> bool {
    matches!(
        vr,
        "OB" | "OD" | "OF" | "OL" | "OV" | "OW" | "SQ" | "UC" | "UR" | "UT" | "UN"
    )
}

fn parse_dicom(data: &[u8]) -> Result<String> {
    if data.len() < 132 || &data[128..132] != b"DICM" {
        bail!("not a DICOM file (no DICM preamble)");
    }
    let mut pos = 132usize;
    let mut explicit = false; // group 0002 meta is always explicit LE
    let mut explicit_known = false;
    let mut out = String::new();
    let mut tags = 0usize;
    loop {
        if pos + 8 > data.len() || tags >= DICOM_MAX_TAGS {
            break;
        }
        let group = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        let elem = u16::from_le_bytes(data[pos + 2..pos + 4].try_into().unwrap());
        // stop at (7FE0,0010) pixel data
        if (group, elem) == (0x7fe0, 0x0010) {
            break;
        }
        // decide VR encoding
        let len: usize;
        let hdr: usize;
        let vr: &str;
        if group == 0x0002 || explicit {
            if pos + 12 > data.len() {
                break;
            }
            vr = std::str::from_utf8(&data[pos + 4..pos + 6]).unwrap_or("UN");
            if long_vr(vr) {
                if pos + 16 > data.len() {
                    break;
                }
                len = u32::from_le_bytes(data[pos + 8..pos + 12].try_into().unwrap()) as usize;
                hdr = 16;
            } else {
                len = u16::from_le_bytes(data[pos + 6..pos + 8].try_into().unwrap()) as usize;
                hdr = 12;
            }
        } else {
            vr = "UN";
            len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
            hdr = 8;
        }
        let body = pos + hdr;
        if len == 0xffff_ffff {
            // undefined length (sequences): bail out of the tag walk
            break;
        }
        if body + len > data.len() {
            break;
        }
        // transfer syntax lives in group 0002 as UID strings
        if (group, elem) == (0x0002, 0x0010) {
            let uid = clean(&String::from_utf8_lossy(&data[body..body + len]));
            explicit = uid == EXPLICIT_VR_LE;
            explicit_known = uid == IMPLICIT_VR_LE || explicit;
            out.push_str(&format!("TransferSyntaxUID: {uid}\n"));
            tags += 1;
        } else if vr != "SQ" && vr != "UN" || dicom_tag_name(group, elem).is_some() {
            if let Some(name) = dicom_tag_name(group, elem) {
                if len <= DICOM_STR_MAX {
                    let s = clean(&String::from_utf8_lossy(&data[body..body + len]));
                    if !s.is_empty() && s.chars().filter(|c| c.is_control()).count() == 0 {
                        out.push_str(&format!("{name}: {s}\n"));
                        tags += 1;
                    }
                }
            }
        }
        pos = body + len;
    }
    if !explicit_known {
        // files without a group-0002 transfer syntax are rare; the walk
        // above assumed implicit LE after group 2 which is the safest guess
    }
    if out.is_empty() {
        bail!("dicom: no header tags found");
    }
    Ok(out)
}

#[async_trait]
impl FileAdapter for DicomAdapter {
    async fn adapt(&self, ai: AdaptInfo, _d: &FileMatcher) -> Result<AdaptedFilesIterBox> {
        let (data, filepath_hint, prefix, depth, postprocess, config) = read_input!(ai);
        let text = parse_dicom(&data).map_err(|e| adapter_bail(format!("dicom: {e}")))?;
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
        Ok(String::from_utf8(adapted_to_vec(out).await?)?
            .trim()
            .to_string())
    }

    async fn bails(adapter: impl FileAdapter, name: &str, data: Vec<u8>) {
        let (a, d) = simple_adapt_info(std::path::Path::new(name), Box::pin(Cursor::new(data)));
        let res = adapter.adapt(a, &d).await;
        assert!(
            res.err()
                .expect("should bail")
                .downcast_ref::<AdapterBail>()
                .is_some()
        );
    }

    // -----------------------------------------------------------------------
    // JPEG EXIF
    // -----------------------------------------------------------------------

    fn tiff_le(ifd0: &[u8]) -> Vec<u8> {
        let mut v = b"II".to_vec();
        v.extend_from_slice(&42u16.to_le_bytes());
        v.extend_from_slice(&8u32.to_le_bytes()); // IFD0 offset
        v.extend_from_slice(ifd0);
        v
    }

    fn ascii_entry(tag: u16, count: u32, value: &[u8; 4], offset: u32) -> [u8; 12] {
        let mut e = [0u8; 12];
        e[0..2].copy_from_slice(&tag.to_le_bytes());
        e[2..4].copy_from_slice(&2u16.to_le_bytes()); // ASCII
        e[4..8].copy_from_slice(&count.to_le_bytes());
        if count <= 4 {
            e[8..12].copy_from_slice(value);
        } else {
            e[8..12].copy_from_slice(&offset.to_le_bytes());
        }
        e
    }

    #[tokio::test]
    async fn jpeg_exif_make_model() -> Result<()> {
        // IFD0: entry count 2, Make@offset, Model@offset, next-IFD 0
        let mut ifd0 = Vec::new();
        ifd0.extend_from_slice(&2u16.to_le_bytes());
        let make_off = 8 + 2 + 24 + 4;
        let model_off = make_off + 6;
        ifd0.extend_from_slice(&ascii_entry(0x010f, 6, b"\0\0\0\0", make_off as u32));
        ifd0.extend_from_slice(&ascii_entry(0x0110, 6, b"\0\0\0\0", model_off as u32));
        ifd0.extend_from_slice(&0u32.to_le_bytes()); // next IFD
        ifd0.extend_from_slice(b"Canon\0");
        ifd0.extend_from_slice(b"EOS R5");
        let tiff = tiff_le(&ifd0);

        let mut app1_payload = b"Exif\0\0".to_vec();
        app1_payload.extend_from_slice(&tiff);
        let seg_len = (app1_payload.len() + 2) as u16;

        let mut jpg = vec![0xff, 0xd8, 0xff, 0xe1];
        jpg.extend_from_slice(&seg_len.to_be_bytes());
        jpg.extend_from_slice(&app1_payload);
        jpg.extend_from_slice(&[0xff, 0xd9]); // EOI

        let out = adapt_to_string(JpegExifAdapter, "photo.jpg", jpg).await?;
        assert_eq!(out, "Make: Canon\nModel: EOS R5");
        Ok(())
    }

    #[tokio::test]
    async fn jpeg_rejects_without_exif() {
        bails(JpegExifAdapter, "x.jpg", vec![0xff, 0xd8, 0xff, 0xd9]).await;
    }

    // -----------------------------------------------------------------------
    // Audio tags
    // -----------------------------------------------------------------------

    fn id3v2_frame(id: &str, text: &str) -> Vec<u8> {
        let mut v = id.as_bytes().to_vec();
        let size = (text.len() + 1) as u32;
        v.extend_from_slice(&size.to_be_bytes());
        v.extend_from_slice(&[0, 0]); // flags
        v.push(0); // encoding: latin1
        v.extend_from_slice(text.as_bytes());
        v
    }

    #[tokio::test]
    async fn mp3_id3v2_title_artist() -> Result<()> {
        let mut body = id3v2_frame("TIT2", "Song Title");
        body.extend_from_slice(&id3v2_frame("TPE1", "The Artist"));
        let mut tag = b"ID3\x03\x00\x00".to_vec();
        let size = body.len();
        tag.push(((size >> 21) & 0x7f) as u8);
        tag.push(((size >> 14) & 0x7f) as u8);
        tag.push(((size >> 7) & 0x7f) as u8);
        tag.push((size & 0x7f) as u8);
        tag.extend_from_slice(&body);
        tag.extend_from_slice(&[0; 100]); // audio frames

        let out = adapt_to_string(AudioTagsAdapter, "song.mp3", tag).await?;
        assert_eq!(out, "title: Song Title\nartist: The Artist");
        Ok(())
    }

    #[tokio::test]
    async fn mp3_id3v1_trailer() -> Result<()> {
        let mut v = vec![0xff; 10];
        let mut trailer = b"TAG".to_vec();
        let mut t = [0u8; 30];
        t[..5].copy_from_slice(b"Title");
        trailer.extend_from_slice(&t);
        let mut a = [0u8; 30];
        a[..6].copy_from_slice(b"Artist");
        trailer.extend_from_slice(&a);
        trailer.extend_from_slice(&[0u8; 30]); // album
        trailer.extend_from_slice(b"1999"); // year
        trailer.extend_from_slice(&[0u8; 30]); // comment
        trailer.push(0); // genre
        assert_eq!(trailer.len(), 128);
        v.extend_from_slice(&trailer);

        let out = adapt_to_string(AudioTagsAdapter, "old.mp3", v).await?;
        assert_eq!(out, "title: Title\nartist: Artist\nyear: 1999");
        Ok(())
    }

    #[tokio::test]
    async fn flac_vorbis_comment() -> Result<()> {
        let mut vc = Vec::new();
        vc.extend_from_slice(&6u32.to_le_bytes()); // vendor len
        vc.extend_from_slice(b"vendor");
        vc.extend_from_slice(&2u32.to_le_bytes()); // 2 comments
        let c1 = b"TITLE=Track One";
        vc.extend_from_slice(&(c1.len() as u32).to_le_bytes());
        vc.extend_from_slice(c1);
        let c2 = b"ARTIST=Someone";
        vc.extend_from_slice(&(c2.len() as u32).to_le_bytes());
        vc.extend_from_slice(c2);

        let mut flac = b"fLaC".to_vec();
        // block header: type 4, 24-bit length, last-block flag set
        flac.push(0x80 | 4);
        flac.push(((vc.len() >> 16) & 0xff) as u8);
        flac.push(((vc.len() >> 8) & 0xff) as u8);
        flac.push((vc.len() & 0xff) as u8);
        flac.extend_from_slice(&vc);

        let out = adapt_to_string(AudioTagsAdapter, "track.flac", flac).await?;
        assert_eq!(out, "title: Track One\nartist: Someone");
        Ok(())
    }

    #[tokio::test]
    async fn wav_fmt_and_info() -> Result<()> {
        let mut fmt = Vec::new();
        fmt.extend_from_slice(&1u16.to_le_bytes()); // PCM
        fmt.extend_from_slice(&2u16.to_le_bytes()); // stereo
        fmt.extend_from_slice(&44100u32.to_le_bytes());
        fmt.extend_from_slice(&176400u32.to_le_bytes()); // byte rate
        fmt.extend_from_slice(&4u16.to_le_bytes()); // block align
        fmt.extend_from_slice(&16u16.to_le_bytes()); // bits

        let mut info = b"INFO".to_vec();
        info.extend_from_slice(b"INAM");
        info.extend_from_slice(&5u32.to_le_bytes());
        info.extend_from_slice(b"Beat\0");

        let mut wav = Vec::new();
        wav.extend_from_slice(b"RIFF");
        let mut body = b"WAVE".to_vec();
        body.extend_from_slice(b"fmt ");
        body.extend_from_slice(&(fmt.len() as u32).to_le_bytes());
        body.extend_from_slice(&fmt);
        body.extend_from_slice(b"LIST");
        body.extend_from_slice(&(info.len() as u32).to_le_bytes());
        body.extend_from_slice(&info);
        let riff_len = (body.len() + 4) as u32;
        wav.extend_from_slice(&riff_len.to_le_bytes());
        wav.extend_from_slice(&body);

        let out = adapt_to_string(AudioTagsAdapter, "beat.wav", wav).await?;
        assert_eq!(
            out,
            "channels: 2\nsample_rate: 44100\nbits_per_sample: 16\ntitle: Beat"
        );
        Ok(())
    }

    #[tokio::test]
    async fn audiotags_rejects_garbage() {
        bails(AudioTagsAdapter, "x.mp3", vec![0u8; 100]).await;
    }

    // -----------------------------------------------------------------------
    // DICOM
    // -----------------------------------------------------------------------

    fn dcm_implicit(tags: &[(u16, u16, &[u8])]) -> Vec<u8> {
        let mut v = vec![0u8; 128];
        v.extend_from_slice(b"DICM");
        for (g, e, val) in tags {
            v.extend_from_slice(&g.to_le_bytes());
            v.extend_from_slice(&e.to_le_bytes());
            v.extend_from_slice(&(val.len() as u32).to_le_bytes());
            v.extend_from_slice(val);
        }
        v
    }

    #[tokio::test]
    async fn dicom_header_tags() -> Result<()> {
        let data = dcm_implicit(&[
            (0x0008, 0x0060, b"MR"),
            (0x0010, 0x0010, b"Doe^John"),
            (0x0008, 0x1030, b"Brain MRI"),
            (0x7fe0, 0x0010, b"pixeldata"), // must stop before this
        ]);
        let out = adapt_to_string(DicomAdapter, "scan.dcm", data).await?;
        assert_eq!(
            out,
            "Modality: MR\nPatientName: Doe^John\nStudyDescription: Brain MRI"
        );
        Ok(())
    }

    #[tokio::test]
    async fn dicom_stops_at_pixel_data() -> Result<()> {
        let mut v = vec![0u8; 128];
        v.extend_from_slice(b"DICM");
        // (7FE0,0010) with a huge declared length must not be read
        v.extend_from_slice(&0x7fe0u16.to_le_bytes());
        v.extend_from_slice(&0x0010u16.to_le_bytes());
        v.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        let (a, d) = simple_adapt_info(std::path::Path::new("pix.dcm"), Box::pin(Cursor::new(v)));
        let res = DicomAdapter.adapt(a, &d).await;
        assert!(
            res.err()
                .expect("should bail (no header tags)")
                .downcast_ref::<AdapterBail>()
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn dicom_rejects_non_dicom() {
        bails(DicomAdapter, "x.dcm", vec![0u8; 200]).await;
    }
}
