use rga::adapters::*;
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use rga::preproc::*;
use rga::print_dur;
use ripgrep_all as rga;

use anyhow::Context;
use log::debug;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::BufReader;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut arg_arr: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let last = arg_arr.pop().expect("No filename specified");
    let config = rga::config::parse_args(arg_arr, true)?;
    //clap::App::new("rga-preproc").arg(Arg::from_usage())
    let path = {
        let filepath = last;
        std::env::current_dir()?.join(filepath)
    };

    let i = File::open(&path)
        .await
        .context("Specified input file not found")?;

    // MS Office owner/lock files (~$name.docx) are never real documents; skip
    // them silently even when reached directly (issue #151). The main rga
    // binary normally filters them out via --pre-glob, but rga-preproc can be
    // invoked directly or through other frontends (rga-fzf).
    if path
        .file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with("~$"))
    {
        debug!("skipping MS Office lock file {}", path.display());
        return Ok(());
    }
    let meta = i.metadata().await?;
    let file_mtime_unix_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64);

    let i = BufReader::new(i);
    let mut o = tokio::io::stdout();
    let ai = AdaptInfo {
        inp: Box::pin(i),
        filepath_hint: path,
        is_real_file: true,
        file_mtime_unix_ms,
        line_prefix: "".to_string(),
        archive_recursion_depth: 0,
        postprocess: !config.no_prefix_filenames,
        config,
    };

    let start = Instant::now();
    let mut oup = rga_preproc(ai).await.context("during preprocessing")?;
    debug!("finding and starting adapter took {}", print_dur(start));
    match rga::preproc::copy_adapter_output(&mut oup, &mut o).await {
        Ok(()) => {}
        Err(rga::preproc::AdapterCopyError::Input(e)) => {
            // the adapter died mid-stream (e.g. pandoc on a corrupt file):
            // append a searchable marker line and exit successfully, instead
            // of making rg report a preprocessor failure (exit code 2).
            // issue #151
            let err = anyhow::Error::from(e);
            eprintln!("rga: preprocessing failed mid-stream: {err:#}");
            use tokio::io::AsyncWriteExt;
            let _ = o
                .write_all(
                    format!(
                        "\n[rga: preprocessing failed: {}]\n",
                        rga::preproc::one_line_error(&err)
                    )
                    .as_bytes(),
                )
                .await;
        }
        Err(rga::preproc::AdapterCopyError::Output(e)) => {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                // happens if e.g. ripgrep detects binary data in the pipe so it cancels reading
                debug!("output cancelled (broken pipe)");
            } else {
                Err(e).context("writing adapter output to stdout")?;
            }
        }
    }
    debug!("running adapter took {} total", print_dur(start));
    Ok(())
}
