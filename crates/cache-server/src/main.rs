use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use cache_server::{Config, Server};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "kached", version, about = "A tag-aware cache server")]
struct Cli {
    /// Path to a TOML config file. Defaults are used when omitted.
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Address to listen on. Overrides the config file.
    #[arg(short, long)]
    listen: Option<SocketAddr>,

    /// Directory holding the database. Overrides the config file.
    #[arg(short, long)]
    data: Option<PathBuf>,

    /// Start from an empty database, and do not sync to disk.
    #[arg(long)]
    ephemeral: bool,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mut config = match &cli.config {
        Some(path) => Config::load(path)?,
        None => Config::default(),
    };

    if let Some(listen) = cli.listen {
        config.server.listen = listen;
    }
    if let Some(data) = cli.data {
        config.store.path = data;
    }
    if cli.ephemeral {
        config.store.durability = cache_server::config::Durability::Ephemeral;
        config.store.wipe_on_start = true;
    }
    config.validate()?;

    init_tracing(&config)?;

    // The runtime is built by hand rather than via `#[tokio::main]` so tracing
    // is initialised first and startup failures are reported in the configured
    // format rather than on a bare stderr.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;

    runtime.block_on(async {
        let server = Server::bind(config).await?;
        server.serve(shutdown_signal()).await
    })
}

fn init_tracing(config: &Config) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.observability.log_level))
        .context("building the log filter")?;

    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match config.observability.log_format.as_str() {
        "json" => builder.json().init(),
        "pretty" => builder.init(),
        other => {
            anyhow::bail!("unknown observability.log_format {other:?}, expected json or pretty")
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %e, "failed to listen for ctrl-c; shutdown signal disabled");
        // Never returning is right here: failing to install the handler must not
        // look like a shutdown request.
        std::future::pending::<()>().await;
    }
}
