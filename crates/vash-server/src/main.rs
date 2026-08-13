use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::EnvFilter;
use vash_server::{Config, Server};

/// The request path allocates per request — a response buffer, and a copy of
/// the value out of the memory map — from as many threads as there are cores.
/// The system allocator is a contended global under that shape; mimalloc is
/// per-thread with a free list per size class, which is the shape that matches.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "vash-server", version, about = "A tag-aware cache server")]
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

    /// Allow `FLUSH` / `flush_all`, which empties the cache for any client that
    /// can reach the port. Off unless asked for.
    #[arg(long)]
    enable_flush: bool,

    /// Allow `LIST_KEYS` / `LIST_TAGS`, which page through the keyspace and the
    /// tag registry for any client that can reach the port. Off unless asked
    /// for.
    #[arg(long)]
    enable_listing: bool,

    /// Serve `/metrics`, `/health` and `/stats` on this address. Off unless
    /// given one — the endpoints have no authentication of their own, so the
    /// address should be a private interface.
    #[arg(long, value_name = "HOST:PORT")]
    admin_listen: Option<SocketAddr>,

    /// Stop serving the memcached text and meta protocols. A connection opening
    /// with one is closed unanswered, and the handshake stops advertising it.
    #[arg(long)]
    disable_memcached: bool,

    /// Stop serving the Redis protocol, on the same terms as
    /// `--disable-memcached`.
    #[arg(long)]
    disable_resp: bool,

    /// Address of another node's cache port, to forward tag invalidations to.
    /// Repeatable. Replaces the config file's peer list rather than adding to
    /// it, so the flag always describes the whole cluster.
    #[arg(long = "peer", value_name = "HOST:PORT")]
    peers: Vec<String>,

    /// Require authentication on the cache port. Overrides the config file.
    #[arg(long)]
    require_auth: bool,

    /// Credential file to authenticate against. Overrides the config file.
    #[arg(long, value_name = "PATH")]
    auth_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Subcommand>,
}

#[derive(clap::Subcommand, Debug)]
enum Subcommand {
    /// Generate a credential and print the line to add to the credential file.
    ///
    /// The secret is generated rather than accepted, which is what keeps the
    /// server's fast unsalted hash the right storage for it: that argument
    /// holds for a high-entropy token and fails for a guessable string.
    AuthGen {
        /// Identity the credential is for.
        #[arg(default_value = vash_server::auth::DEFAULT_NAME)]
        name: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(Subcommand::AuthGen { name }) = &cli.command {
        return auth_gen(name);
    }

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
        config.store.durability = vash_server::config::Durability::Ephemeral;
        config.store.wipe_on_start = true;
    }
    if cli.enable_flush {
        config.protocol.flush_enabled = true;
    }
    if cli.enable_listing {
        config.protocol.listing_enabled = true;
    }
    // One-directional, like the `--enable-*` pair above it: a flag that is
    // absent means "whatever the config file said", never "put it back". The
    // asymmetry is the default's — these two are on, those two are off.
    if cli.disable_memcached {
        config.protocol.memcached_enabled = false;
    }
    if cli.disable_resp {
        config.protocol.resp_enabled = false;
    }
    // A `SocketAddr` rather than the config key's string, so a typo is a clap
    // error instead of a failed bind — and because the flag can only turn the
    // endpoint on, there is nothing to express with the empty string that it
    // accepts.
    if let Some(admin) = cli.admin_listen {
        config.observability.admin_listen = admin.to_string();
    }
    if let Some(path) = cli.auth_file {
        config.auth.file = path;
    }
    if cli.require_auth {
        config.auth.required = true;
    }
    if !cli.peers.is_empty() {
        config.cluster.peers = cli.peers;
    }
    config.validate()?;

    init_tracing(&config)?;

    // The runtime is built by hand rather than via `#[tokio::main]` so tracing
    // is initialised first and startup failures are reported in the configured
    // format rather than on a bare stderr.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Bounded so the ceiling on concurrent reads is known, and can be
        // checked against the LMDB reader-slot table. The default of 512 would
        // silently exceed it.
        .max_blocking_threads(config.server.max_blocking_threads)
        .build()
        .context("building the tokio runtime")?;

    runtime.block_on(async {
        let server = Server::bind(config).await?;
        server.serve(shutdown_signal()).await
    })
}

/// Prints a generated credential: the secret on stderr, the file line on
/// stdout.
///
/// Split across the two streams so `vash-server auth-gen api >> credentials`
/// appends the row and still shows a human the secret, which is the only time
/// it is ever displayed. Nothing is written to disk here — where the secret
/// goes is the operator's business, and a tool that helpfully saved it would be
/// putting a plaintext credential somewhere nobody asked for.
fn auth_gen(name: &str) -> anyhow::Result<()> {
    let (secret, line) = vash_server::auth::generate(name)?;
    eprintln!("secret for {name} (shown once, store it in your secret manager):\n{secret}\n");
    println!("{line}");
    Ok(())
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
