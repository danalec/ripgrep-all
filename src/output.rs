//! Output post-processing for rga ("wrapper modes").
//!
//! When any of `--rga-format`, `--rga-and`, `--rga-not`, `--rga-replace` or
//! `--rga-max-files` is given, rga can no longer let ripgrep stream its
//! rendered output straight to the terminal. Instead it forces `rg --json
//! --color=never`, parses the JSON message stream and re-renders it here.
//!
//! Consequences (documented in `--help`):
//! - colors are lost in wrapper modes (there is no public rg API to re-emit
//!   them faithfully),
//! - output is still streamed line by line, but the rendering differs from
//!   rg's default (`path:line:text`, one record per match sub-line),
//! - boolean line filters (`--rga-and`/`--rga-not`) apply to the *adapted
//!   text lines*, i.e. they see exactly what the adapters extracted.
//!
//! This is how rga reaches the ugrep `--format`/`--and`/`--not`/`--max-files`
//! feature set without reimplementing the search engine. See
//! docs/PARITY-UGREP.md, wave-4.

use anyhow::{Context, Result, bail};
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use std::io::{BufRead, Write};

/// User-facing output options (already parsed/validated).
pub struct OutputOptions {
    /// None | "csv" | "xml" | a custom %-field format string
    pub format: Option<String>,
    pub and: Vec<Regex>,
    pub not: Vec<Regex>,
    /// %-field template applied to each match (like rg --replace, but with fields)
    pub replace: Option<String>,
    pub max_files: Option<usize>,
}

impl OutputOptions {
    pub fn is_active(&self) -> bool {
        self.format.is_some()
            || !self.and.is_empty()
            || !self.not.is_empty()
            || self.replace.is_some()
            || self.max_files.is_some()
    }
}

/// Compile the raw CLI strings into [OutputOptions], validating %-fields early
/// so a typo fails before rg starts preprocessing files.
pub fn build_output_options(
    format: Option<String>,
    and: &[String],
    not: &[String],
    replace: Option<String>,
    max_files: Option<usize>,
) -> Result<OutputOptions> {
    let compile = |pats: &[String]| -> Result<Vec<Regex>> {
        pats.iter()
            .map(|p| {
                // rg's --smart-case rule: a literal uppercase character makes
                // the pattern case-sensitive, otherwise case-insensitive
                let case_sensitive = p.chars().any(|c| c.is_uppercase());
                RegexBuilder::new(p)
                    .case_insensitive(!case_sensitive)
                    .build()
                    .with_context(|| format!("invalid --rga-and/--rga-not pattern: {p:?}"))
            })
            .collect()
    };
    if let Some(ref f) = format {
        validate_format(f)?;
    }
    if let Some(ref r) = replace {
        validate_format(r)?;
    }
    Ok(OutputOptions {
        format: format,
        and: compile(and)?,
        not: compile(not)?,
        replace,
        max_files,
    })
}

/// %fields available in --rga-format and --rga-replace templates.
const KNOWN_FIELDS: &[char] = &['f', 'n', 'c', 'm', 'd'];

fn validate_format(fmt: &str) -> Result<()> {
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            // any escaped char is fine; skip it
            chars.next();
        } else if c == '%' {
            match chars.next() {
                Some(f) if KNOWN_FIELDS.contains(&f) => {}
                other => {
                    bail!("unknown %field in format {fmt:?}: {other:?} (known: {KNOWN_FIELDS:?})")
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// rg --json message model (only the parts we need)
// https://docs.rs/grep-printer/.../struct.JSON.html is private; this mirrors
// the documented stream format.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct JsonMessage {
    r#type: String,
    data: MessageData,
}

#[derive(Deserialize)]
struct MessageData {
    /// path of the file this message belongs to
    path: Option<JsonText>,
    /// full text block the submatches refer to (may span lines)
    lines: Option<JsonText>,
    /// 1-based line number of the start of `lines`
    line_number: Option<u64>,
    submatches: Option<Vec<SubMatch>>,
}

#[derive(Deserialize)]
struct JsonText {
    /// None when the text is not valid UTF-8 (then `bytes` holds base64)
    text: Option<String>,
}

#[derive(Deserialize)]
struct SubMatch {
    /// the matched text
    #[serde(rename = "match")]
    matched: JsonText,
    /// byte offset into the `lines` text block where the match starts
    start: usize,
}

/// One flattened match: a single submatch located on a single line.
struct FlatMatch {
    path: String,
    line_number: u64,
    /// 1-based byte column within the line
    column: usize,
    /// the whole line without the trailing newline
    line: String,
    /// the matched text
    matched: String,
}

/// Locate `start` within `block`: which 1-based line is it on, at which
/// 1-based byte column, and what is that line's text?
fn flatten_submatch(
    path: &str,
    block: &str,
    block_line: u64,
    start: usize,
    matched: &str,
) -> FlatMatch {
    // line containing byte offset `start`: count newlines before it
    let before = &block[..start.min(block.len())];
    let extra_lines = before.matches('\n').count() as u64;
    let line_start = before.rfind('\n').map(|i| i + 1).unwrap_or(0);
    let after = &block[start.min(block.len())..];
    let line_end_rel = after.find('\n').unwrap_or(after.len());
    let line = block[line_start..(start.min(block.len()) + line_end_rel)].to_string();
    FlatMatch {
        path: path.to_string(),
        line_number: block_line + extra_lines,
        column: start - line_start + 1,
        line,
        matched: matched.to_string(),
    }
}

/// Expand a %-field template for one flattened match.
/// Precondition: template validated by [validate_format].
fn expand_format(fmt: &str, m: &FlatMatch) -> String {
    let mut out = String::with_capacity(fmt.len() + m.line.len());
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else if c == '%' {
            match chars.next() {
                Some('f') => out.push_str(&m.path),
                Some('n') => out.push_str(&m.line_number.to_string()),
                Some('c') => out.push_str(&m.column.to_string()),
                Some('m') => out.push_str(&m.matched),
                Some('d') => out.push_str(&m.line),
                // validated above; keep defensively
                Some(other) => {
                    out.push('%');
                    out.push(other);
                }
                None => out.push('%'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn csv_field(out: &mut String, s: &str) {
    // RFC 4180: quote always — simplest and unambiguous
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
}

fn xml_escape(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
}

fn render_record(opts: &OutputOptions, m: &FlatMatch) -> String {
    match opts.format.as_deref() {
        Some("csv") => {
            let mut out = String::new();
            csv_field(&mut out, &m.path);
            out.push(',');
            csv_field(&mut out, &m.line_number.to_string());
            out.push(',');
            csv_field(&mut out, &m.column.to_string());
            out.push(',');
            csv_field(&mut out, &m.line);
            out.push('\n');
            out
        }
        Some("xml") => {
            let mut out = String::from("<match file=\"");
            xml_escape(&mut out, &m.path);
            out.push_str("\" line=\"");
            out.push_str(&m.line_number.to_string());
            out.push_str("\" column=\"");
            out.push_str(&m.column.to_string());
            out.push_str("\">");
            xml_escape(&mut out, &m.line);
            out.push_str("</match>\n");
            out
        }
        Some(fmt) => {
            let mut s = expand_format(fmt, m);
            s.push('\n');
            s
        }
        // no --rga-format: --rga-and/--rga-not/--rga-replace/--rga-max-files
        // render like rg's standard `path:line:text` (colors are not available);
        // --rga-replace substitutes each match inside the line, like rg
        // --replace but with %-fields instead of $-groups
        None => {
            let line = match opts.replace {
                Some(ref tmpl) => {
                    let expansion = expand_format(tmpl, m);
                    let start = m.column - 1;
                    let end = (start + m.matched.len()).min(m.line.len());
                    format!("{}{}{}", &m.line[..start], expansion, &m.line[end..])
                }
                None => m.line.clone(),
            };
            format!("{}:{}:{}\n", m.path, m.line_number, line)
        }
    }
}

/// Stream-transform rg `--json` output into the requested representation.
/// Returns the number of records written; rg's own exit status is handled by
/// the caller.
pub fn transform<R: BufRead, W: Write>(
    input: R,
    mut output: W,
    opts: &OutputOptions,
) -> Result<usize> {
    if opts.format.as_deref() == Some("xml") {
        output.write_all(b"<rga>\n")?;
    }
    if opts.format.as_deref() == Some("csv") {
        output.write_all(b"\"path\",\"line\",\"column\",\"text\"\n")?;
    }

    let mut printed_files: usize = 0;
    let mut first_match_of_file = true;

    let mut line = String::new();
    let mut input = input;
    let mut records: usize = 0;
    loop {
        line.clear();
        let n = input.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        let msg: JsonMessage = match serde_json::from_str(trimmed) {
            Ok(m) => m,
            // rg may print non-JSON diagnostics to stdout in weird cases;
            // forward them rather than failing the whole search
            Err(_) => {
                output.write_all(trimmed.as_bytes())?;
                output.write_all(b"\n")?;
                continue;
            }
        };
        match msg.r#type.as_str() {
            "begin" => {
                first_match_of_file = true;
            }
            "match" => {
                let (Some(path), Some(lines), Some(line_number)) = (
                    msg.data.path.as_ref().and_then(|p| p.text.as_ref()),
                    msg.data.lines.as_ref().and_then(|l| l.text.as_ref()),
                    msg.data.line_number,
                ) else {
                    continue;
                };
                if let Some(max) = opts.max_files {
                    if printed_files >= max {
                        continue; // keep draining rg's output so it can exit
                    }
                }
                let submatches = msg.data.submatches.unwrap_or_default();
                let mut emitted = false;
                for sm in &submatches {
                    let Some(matched) = sm.matched.text.as_ref() else {
                        continue;
                    };
                    let fm = flatten_submatch(path, lines, line_number, sm.start, matched);
                    // --rga-and: line must match ALL patterns; --rga-not: NONE
                    if !opts.and.iter().all(|re| re.is_match(&fm.line)) {
                        continue;
                    }
                    if opts.not.iter().any(|re| re.is_match(&fm.line)) {
                        continue;
                    }
                    let record = render_record(opts, &fm);
                    output.write_all(record.as_bytes())?;
                    emitted = true;
                    records += 1;
                }
                // count a file only if it actually produced output
                // (all its matches may have been filtered out)
                if emitted && first_match_of_file {
                    first_match_of_file = false;
                    printed_files += 1;
                }
            }
            // "context", "end", "summary": nothing to render
            _ => {}
        }
    }
    if opts.format.as_deref() == Some("xml") {
        output.write_all(b"</rga>\n")?;
    }
    output.flush()?;
    Ok(records)
}

#[cfg(test)]
mod test {
    use super::*;

    fn opts(
        format: Option<&str>,
        and: &[&str],
        not: &[&str],
        replace: Option<&str>,
        max: Option<usize>,
    ) -> OutputOptions {
        build_output_options(
            format.map(|s| s.to_string()),
            &and.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &not.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            replace.map(|s| s.to_string()),
            max,
        )
        .unwrap()
    }

    const BEGIN: &str = r#"{"type":"begin","data":{"path":{"text":"doc.zip/inner.txt"}}}"#;
    const MATCH: &str = r#"{"type":"match","data":{"path":{"text":"doc.zip/inner.txt"},"lines":{"text":"hello world\nfoo needle bar\n"},"line_number":7,"absolute_offset":42,"submatches":[{"match":{"text":"needle"},"start":16,"end":22}]}}"#;
    const END: &str = r#"{"type":"end","data":{"path":{"text":"doc.zip/inner.txt"},"binary_offset":null,"stats":{"elapsed":{"secs":0,"nanos":1,"human":"0.000001s"},"searches":1,"searches_with_match":1,"bytes_searched":1337,"bytes_printed":100,"matched_lines":2,"matches":2}}}"#;

    fn run(jsonl: &str, o: &OutputOptions) -> String {
        let mut out = Vec::new();
        transform(jsonl.as_bytes(), &mut out, o).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn csv_output() {
        let out = run(
            &format!("{BEGIN}\n{MATCH}\n{END}\n"),
            &opts(Some("csv"), &[], &[], None, None),
        );
        assert_eq!(
            out,
            "\"path\",\"line\",\"column\",\"text\"\n\
             \"doc.zip/inner.txt\",\"8\",\"5\",\"foo needle bar\"\n"
        );
    }

    #[test]
    fn xml_output_escapes_and_wraps() {
        let out = run(
            &format!("{BEGIN}\n{MATCH}\n{END}\n"),
            &opts(Some("xml"), &[], &[], None, None),
        );
        assert_eq!(
            out,
            "<rga>\n<match file=\"doc.zip/inner.txt\" line=\"8\" column=\"5\">foo needle bar</match>\n</rga>\n"
        );
    }

    #[test]
    fn custom_format_fields() {
        let out = run(
            &format!("{BEGIN}\n{MATCH}\n{END}\n"),
            &opts(Some("[%n:%c] %f: %m <%d>\\n"), &[], &[], None, None),
        );
        assert_eq!(out, "[8:5] doc.zip/inner.txt: needle <foo needle bar>\n\n");
    }

    #[test]
    fn and_not_filter_lines() {
        // line contains "needle" but filter requires "zzz" -> no output
        let out = run(
            &format!("{BEGIN}\n{MATCH}\n{END}\n"),
            &opts(None, &["zzz"], &[], None, None),
        );
        assert_eq!(out, "");
        // --rga-not excludes the line
        let out = run(
            &format!("{BEGIN}\n{MATCH}\n{END}\n"),
            &opts(None, &[], &["needle"], None, None),
        );
        assert_eq!(out, "");
        // matching --rga-and passes
        let out = run(
            &format!("{BEGIN}\n{MATCH}\n{END}\n"),
            &opts(None, &["foo"], &[], None, None),
        );
        assert_eq!(out, "doc.zip/inner.txt:8:foo needle bar\n");
    }

    #[test]
    fn replace_template_applies_to_rendered_line() {
        let out = run(
            &format!("{BEGIN}\n{MATCH}\n{END}\n"),
            &opts(None, &[], &[], Some("%f:%n -> %m"), None),
        );
        assert_eq!(
            out,
            "doc.zip/inner.txt:8:foo doc.zip/inner.txt:8 -> needle bar\n"
        );
    }

    #[test]
    fn max_files_stops_printing_new_files() {
        let two_files = format!("{BEGIN}\n{MATCH}\n{END}\n{BEGIN}\n{MATCH}\n{END}\n");
        let out = run(&two_files, &opts(None, &[], &[], None, Some(1)));
        assert_eq!(out.matches("needle").count(), 1);
    }

    #[test]
    fn invalid_field_rejected_early() {
        assert!(build_output_options(Some("%q".to_string()), &[], &[], None, None).is_err());
        assert!(build_output_options(Some("%f %n".to_string()), &[], &[], None, None).is_ok());
        assert!(build_output_options(None, &["(unclosed".to_string()], &[], None, None).is_err());
    }

    #[test]
    fn multiline_block_flattens_line_numbers() {
        // submatch starting on the second line of a multiline block
        let m = r#"{"type":"match","data":{"path":{"text":"a.txt"},"lines":{"text":"l1\nl2 needle\n"},"line_number":10,"absolute_offset":0,"submatches":[{"match":{"text":"needle"},"start":6,"end":12}]}}"#;
        let out = run(
            &format!("{BEGIN}\n{m}\n{END}\n"),
            &opts(None, &[], &[], None, None),
        );
        assert_eq!(out, "a.txt:11:l2 needle\n");
    }

    #[test]
    fn non_json_stdout_is_forwarded() {
        let out = run("some rg warning\n", &opts(None, &[], &[], None, None));
        assert_eq!(out, "some rg warning\n");
    }
}
