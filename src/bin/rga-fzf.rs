use anyhow::Context;
use clap::Parser;
use rga::adapters::custom::map_exe_error;
use ripgrep_all as rga;

use std::process::{Command, Stdio};

#[derive(Parser, Debug, Clone)]
#[clap(name = "rga-fzf", about = "FZF frontend for rga", disable_help_flag = false)]
struct Args {
    /// Initial query for fzf
    #[clap(value_parser)]
    initial_query: Option<String>,
    /// Extra parameters to pass to ripgrep (list view)
    #[clap(long = "rg-params", require_equals = true)]
    rg_params: Option<String>,
    /// Extra parameters to pass to ripgrep preview (content view)
    #[clap(long = "rg-preview-params", require_equals = true)]
    rg_preview_params: Option<String>,
    /// Extra parameters to pass to fzf
    #[clap(long = "fzf-params", require_equals = true)]
    fzf_params: Option<String>,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();
    let initial_query = args.initial_query.clone().unwrap_or_default();

    let exe = std::env::current_exe().context("Could not get executable location")?;
    let preproc_exe = exe.with_file_name("rga");
    let preproc_exe = preproc_exe
        .to_str()
        .context("rga executable is in non-unicode path")?;
    let open_exe = exe.with_file_name("rga-fzf-open");
    let open_exe = open_exe
        .to_str()
        .context("rga-fzf-open executable is in non-unicode path")?;

    let rg_prefix = if let Some(p) = &args.rg_params {
        format!("{preproc_exe} --files-with-matches --rga-cache-max-blob-len=10M {p}")
    } else {
        format!("{preproc_exe} --files-with-matches --rga-cache-max-blob-len=10M")
    };
    let rg_preview = if let Some(p) = &args.rg_preview_params {
        format!("{preproc_exe} --pretty --context 5 {p} {{q}} --rga-fzf-path=_{{}}")
    } else {
        format!("{preproc_exe} --pretty --context 5 {{q}} --rga-fzf-path=_{{}}")
    };

    let mut cmd = Command::new("fzf");
    cmd.arg(format!("--preview={rg_preview}"))
        .arg("--preview-window=70%:wrap")
        .arg("--phony")
        .arg("--query")
        .arg(&initial_query)
        .arg("--print-query")
        .arg(format!("--bind=change:reload: {rg_prefix} {{q}}"))
        .arg(format!("--bind=ctrl-m:execute:{open_exe} {{q}} {{}}"))
        .env(
            "FZF_DEFAULT_COMMAND",
            format!("{} '{}'", rg_prefix, initial_query),
        )
        .env("RGA_FZF_INSTANCE", format!("{}", std::process::id()))
        .stdout(Stdio::piped());
    if let Some(p) = &args.fzf_params {
        for token in p.split_whitespace() {
            cmd.arg(token);
        }
    }
    let child = cmd
        .spawn()
        .map_err(|e| map_exe_error(e, "fzf", "Please make sure you have fzf installed."))?;

    let output = child.wait_with_output().with_context(|| "waiting for fzf output")?;
    // fzf --print-query always prints the final query on the first line, and the selected
    // file on the second line. Distinguish the failure modes instead of always failing
    // with the misleading "fzf output not two line"
    // (https://github.com/phiresky/ripgrep-all/issues/264).
    let stdout =
        String::from_utf8(output.stdout).context("fzf output not valid utf8")?;
    if stdout.is_empty() {
        return Err(anyhow::anyhow!(
            "fzf printed no output (fzf was closed without a selection, e.g. via ctrl-c)"
        ));
    }
    let mut lines = stdout.lines();
    let final_query = lines.next().context("fzf output empty")?;
    let selected_file = lines
        .next()
        .context("fzf printed only the query but no selected file")?;
    println!("query='{final_query}', file='{selected_file}'");

    Ok(())
}

