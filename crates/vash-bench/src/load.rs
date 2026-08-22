//! End-to-end load generator: the numbers the §13 performance goals are stated
//! in.
//!
//! ```text
//! cargo run --release -p vash-bench --bin load -- --workload get --connections 64
//! cargo run --release -p vash-bench --bin load -- --addr 10.0.0.5:11311 --workload set
//! ```
//!
//! Drives real VCP frames over a real socket, because that is what a client
//! does. The micro-benchmarks in `benches/hot_path.rs` say what the framing and
//! liveness checks cost; this says what the server actually delivers with the
//! network tier, the shard routing, the blocking pool and the storage engine all
//! in the path.
//!
//! # Two modes, because the goals are two different questions
//!
//! **Throughput** (`--pipeline N`) keeps N requests in flight per connection.
//! That is what the VCP frame format exists for — `request_id` is echoed so a
//! client need not wait — and it is the only way to reach a throughput ceiling
//! without opening a socket per concurrent request. Latency under deep
//! pipelining is not a user-facing number and is not reported.
//!
//! **Latency** (`--pipeline 1`, the default) is closed-loop: one request in
//! flight per connection, so every measurement is a complete client-visible
//! round trip. Sweeping the connection count gives a rate/latency curve, and the
//! p99 at a given rate is read off that curve.
//!
//! # What this cannot tell you
//!
//! The load generator shares a machine with the server, so both are competing
//! for the same cores, and loopback is not a network. Numbers from here are a
//! floor for throughput and an optimistic p99. Run it against a separate host
//! before believing either.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use vash_proto::vcp::{FrameLen, Opcode, encode_request, encode_set_body};

struct Options {
    addr: Option<String>,
    connections: usize,
    pipeline: usize,
    duration: Duration,
    value_bytes: usize,
    keys: u64,
    workload: Workload,
    shards: usize,
    /// Drive the server's TLS port instead of its plaintext one.
    tls: bool,
    /// PEM of the CA that signed the server's certificate.
    tls_ca: Option<String>,
    /// The name the certificate is expected to carry.
    tls_name: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    Get,
    Set,
    /// Nine reads to one write, the shape a cache actually sees.
    Mixed,
    /// Touches no storage at all. Subtract this from the others and what is
    /// left is the network tier: the socket, the framing and the hop to the
    /// storage threads. Without it there is no way to tell a slow store from a
    /// slow path to the store.
    Ping,
}

impl Workload {
    fn parse(raw: &str) -> Self {
        match raw {
            "get" => Self::Get,
            "set" => Self::Set,
            "mixed" => Self::Mixed,
            "ping" => Self::Ping,
            other => panic!("unknown workload {other:?}, expected get, set, mixed or ping"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Set => "set",
            Self::Mixed => "mixed (9:1 read:write)",
            Self::Ping => "ping (no storage)",
        }
    }

    /// Whether the run needs keys written before it starts.
    fn reads(self) -> bool {
        matches!(self, Self::Get | Self::Mixed)
    }
}

impl Options {
    fn parse() -> Self {
        let mut options = Self {
            addr: None,
            connections: 64,
            pipeline: 1,
            duration: Duration::from_secs(10),
            value_bytes: 1024,
            keys: 100_000,
            workload: Workload::Get,
            shards: 0,
            tls: false,
            tls_ca: None,
            tls_name: "localhost".into(),
        };

        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            let value = || {
                args.get(i + 1)
                    .unwrap_or_else(|| panic!("{} needs a value", args[i]))
                    .clone()
            };
            match args[i].as_str() {
                "--addr" => options.addr = Some(value()),
                "--connections" => options.connections = value().parse().expect("a number"),
                "--pipeline" => options.pipeline = value().parse().expect("a number"),
                "--duration" => {
                    options.duration = Duration::from_secs(value().parse().expect("seconds"))
                }
                "--value-bytes" => options.value_bytes = value().parse().expect("a number"),
                "--keys" => options.keys = value().parse().expect("a number"),
                "--workload" => options.workload = Workload::parse(&value()),
                "--shards" => options.shards = value().parse().expect("a number"),
                "--tls" => {
                    options.tls = true;
                    i -= 1; // takes no value
                }
                "--tls-ca" => options.tls_ca = Some(value()),
                "--tls-name" => options.tls_name = value(),
                "--help" | "-h" => {
                    println!(
                        "load [--addr HOST:PORT] [--workload get|set|mixed] [--connections N]\n     \
                         [--pipeline N] [--duration SECS] [--value-bytes N] [--keys N] [--shards N]\n\n\
                         With no --addr, a server is started in this process on an ephemeral store."
                    );
                    std::process::exit(0);
                }
                other => panic!("unknown argument {other:?}; try --help"),
            }
            i += 2;
        }
        assert!(options.connections > 0 && options.pipeline > 0);
        options
    }
}

/// Fixed-bucket latency histogram.
///
/// A `Vec` of every sample would be tens of millions of entries at these rates,
/// and the allocation would show up in the thing being measured. Resolution is
/// 1µs up to a millisecond and 100µs beyond it, which is where the interesting
/// percentiles live for a cache.
struct Histogram {
    buckets: Vec<u32>,
    count: u64,
    sum_us: u64,
    max_us: u64,
}

const FINE_BUCKETS: usize = 1_000;
const COARSE_BUCKETS: usize = 990;

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: vec![0; FINE_BUCKETS + COARSE_BUCKETS + 1],
            count: 0,
            sum_us: 0,
            max_us: 0,
        }
    }

    fn index(micros: u64) -> usize {
        if micros < FINE_BUCKETS as u64 {
            micros as usize
        } else if micros < 100_000 {
            FINE_BUCKETS + (micros as usize - FINE_BUCKETS) / 100
        } else {
            FINE_BUCKETS + COARSE_BUCKETS
        }
    }

    fn value_of(index: usize) -> u64 {
        if index < FINE_BUCKETS {
            index as u64
        } else if index < FINE_BUCKETS + COARSE_BUCKETS {
            FINE_BUCKETS as u64 + (index - FINE_BUCKETS) as u64 * 100
        } else {
            100_000
        }
    }

    fn record(&mut self, elapsed: Duration) {
        let micros = elapsed.as_micros() as u64;
        self.buckets[Self::index(micros)] += 1;
        self.count += 1;
        self.sum_us += micros;
        self.max_us = self.max_us.max(micros);
    }

    fn merge(&mut self, other: &Self) {
        for (mine, theirs) in self.buckets.iter_mut().zip(&other.buckets) {
            *mine += theirs;
        }
        self.count += other.count;
        self.sum_us += other.sum_us;
        self.max_us = self.max_us.max(other.max_us);
    }

    fn percentile(&self, fraction: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = (self.count as f64 * fraction) as u64;
        let mut seen = 0u64;
        for (index, hits) in self.buckets.iter().enumerate() {
            seen += *hits as u64;
            if seen >= target {
                return Self::value_of(index) as f64 / 1000.0;
            }
        }
        self.max_us as f64 / 1000.0
    }

    fn mean_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_us as f64 / self.count as f64 / 1000.0
        }
    }
}

fn key_for(index: u64) -> Vec<u8> {
    format!("bench:key:{index:012}").into_bytes()
}

/// The client's inbound buffer.
///
/// Reads greedily into spare capacity and hands out frames by slicing, rather
/// than reading exactly the bytes the next frame needs. That sounds like an
/// implementation detail and is not: a reader that asks for a 12-byte header
/// and then a body performs two syscalls per response and can never coalesce a
/// pipelined batch, so it plateaus at a few tens of thousands of operations a
/// second — and the plateau looks exactly like a server bottleneck. This
/// harness measured one for a while before the mistake was its own.
struct Inbound {
    buf: Vec<u8>,
    start: usize,
}

impl Inbound {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256 * 1024),
            start: 0,
        }
    }

    fn pending(&self) -> &[u8] {
        &self.buf[self.start..]
    }

    /// Waits for one whole frame and returns it, consuming it from the buffer.
    async fn next_frame<S: AsyncRead + Unpin>(&mut self, stream: &mut S) -> std::io::Result<&[u8]> {
        loop {
            match vash_proto::vcp::peek_frame_len(self.pending()) {
                FrameLen::Complete(len) => {
                    let from = self.start;
                    self.start += len;
                    return Ok(&self.buf[from..from + len]);
                }
                FrameLen::TooLarge => {
                    return Err(std::io::Error::other("server frame exceeded the maximum"));
                }
                FrameLen::Incomplete { .. } => {
                    // Reclaim what has been consumed before growing, so a long
                    // run does not walk the buffer forward forever.
                    if self.start > 0 {
                        self.buf.drain(..self.start);
                        self.start = 0;
                    }
                    let filled = self.buf.len();
                    if self.buf.capacity() - filled < 16 * 1024 {
                        self.buf.reserve(256 * 1024);
                    }
                    // Read into all the spare capacity: whatever the server has
                    // already sent arrives in one syscall.
                    self.buf.resize(self.buf.capacity(), 0);
                    let read = stream.read(&mut self.buf[filled..]).await?;
                    self.buf.truncate(filled + read);
                    if read == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "server closed the connection",
                        ));
                    }
                }
            }
        }
    }
}

async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&vash_core::PROTOCOL_VERSION.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());

    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Hello, 0, &body);
    stream.write_all(&frame).await?;

    let mut inbound = Inbound::new();
    let reply = inbound.next_frame(stream).await?;
    let status = u16::from_le_bytes(reply[2..4].try_into().expect("header"));
    if status != 0 {
        return Err(std::io::Error::other(format!("handshake failed: {status}")));
    }
    Ok(())
}

/// Everything a connection needs, so that a worker does not have to know
/// whether it is speaking TLS.
///
/// The stream is boxed in *both* modes, deliberately: a `dyn` call per read is
/// a cost the plaintext run should pay too, or the comparison would be
/// measuring the box rather than the cryptography.
trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

#[derive(Clone)]
struct Connector {
    addr: String,
    tls: Option<(
        tokio_rustls::TlsConnector,
        rustls::pki_types::ServerName<'static>,
    )>,
}

impl Connector {
    fn new(options: &Options, addr: String) -> anyhow::Result<Self> {
        if !options.tls {
            return Ok(Self { addr, tls: None });
        }

        let mut roots = rustls::RootCertStore::empty();
        let ca = options
            .tls_ca
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--tls needs --tls-ca pointing at the issuing CA"))?;
        let file = std::fs::File::open(ca)?;
        for cert in rustls_pemfile::certs(&mut std::io::BufReader::new(file)) {
            roots.add(cert?)?;
        }

        let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();

        let name = rustls::pki_types::ServerName::try_from(options.tls_name.clone())?;
        Ok(Self {
            addr,
            tls: Some((
                tokio_rustls::TlsConnector::from(std::sync::Arc::new(config)),
                name,
            )),
        })
    }

    async fn connect(&self) -> std::io::Result<Box<dyn Duplex>> {
        let stream = TcpStream::connect(&self.addr).await?;
        stream.set_nodelay(true)?;
        match &self.tls {
            None => Ok(Box::new(stream)),
            Some((connector, name)) => Ok(Box::new(connector.connect(name.clone(), stream).await?)),
        }
    }
}

/// One connection's share of the load.
///
/// Returns the operations it completed, how many of those were hits, and its
/// latency histogram.
async fn worker(
    connector: Connector,
    options: Arc<OptionsShared>,
    id: u64,
    stop: Arc<AtomicBool>,
    started: Arc<tokio::sync::Barrier>,
) -> std::io::Result<(u64, u64, Histogram)> {
    let mut stream = connector.connect().await?;
    handshake(&mut stream).await?;

    let mut histogram = Histogram::new();
    let mut inbound = Inbound::new();
    let mut out: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut completed = 0u64;
    let mut hits = 0u64;
    // Each connection walks its own stride through the keyspace, so the shards
    // are loaded evenly and no two connections contend on the same key.
    let mut cursor = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);

    started.wait().await;

    while !stop.load(Ordering::Relaxed) {
        out.clear();
        let batch = options.pipeline;
        let sent_at = Instant::now();

        for slot in 0..batch {
            cursor = cursor.wrapping_add(0x9E37_79B9_7F4A_7C15);
            if options.workload == Workload::Ping {
                encode_request(&mut out, Opcode::Ping, slot as u32, &[]);
                continue;
            }

            let key = key_for(cursor % options.keys);
            let write = match options.workload {
                Workload::Set => true,
                // Deterministic 9:1 so runs are comparable.
                Workload::Mixed => cursor.is_multiple_of(10),
                Workload::Get | Workload::Ping => false,
            };

            if write {
                let mut body = Vec::with_capacity(16 + key.len() + options.value.len());
                encode_set_body(&mut body, &key, &options.value, 0, &[]);
                encode_request(&mut out, Opcode::Set, slot as u32, &body);
            } else {
                encode_request(&mut out, Opcode::Get, slot as u32, &key);
            }
        }
        stream.write_all(&out).await?;

        for _ in 0..batch {
            let reply = inbound.next_frame(&mut stream).await?;
            let status = u16::from_le_bytes(reply[2..4].try_into().expect("header"));
            if status == 0 && reply[0] == Opcode::Get as u8 {
                hits += 1;
            }
            completed += 1;
        }

        // With `pipeline > 1` this is the batch's latency, not a request's, and
        // is reported only as an aid to spotting stalls — never as a p99.
        let elapsed = sent_at.elapsed();
        histogram.record(if batch == 1 {
            elapsed
        } else {
            elapsed / batch as u32
        });
    }

    let _ = stream.shutdown().await;
    Ok((completed, hits, histogram))
}

struct OptionsShared {
    pipeline: usize,
    keys: u64,
    value: Vec<u8>,
    workload: Workload,
}

/// Writes every key the read workload will ask for.
///
/// A GET benchmark against an empty store measures the miss path, which is
/// faster and completely uninteresting — so this runs first and the hit rate is
/// reported afterwards to prove it worked.
async fn populate(connector: &Connector, keys: u64, value: &[u8]) -> std::io::Result<()> {
    const BATCH: u64 = 256;
    let mut stream = connector.connect().await?;
    handshake(&mut stream).await?;

    let mut inbound = Inbound::new();
    let mut written = 0u64;
    while written < keys {
        let batch = BATCH.min(keys - written);
        let mut out = Vec::new();
        for i in 0..batch {
            let mut body = Vec::new();
            encode_set_body(&mut body, &key_for(written + i), value, 0, &[]);
            encode_request(&mut out, Opcode::Set, i as u32, &body);
        }
        stream.write_all(&out).await?;
        for _ in 0..batch {
            inbound.next_frame(&mut stream).await?;
        }
        written += batch;
    }
    let _ = stream.shutdown().await;
    Ok(())
}

/// Issues a CA and a `localhost` leaf into `dir`, for the embedded server's
/// TLS listener.
///
/// A benchmark wants one command, not a certificate-management exercise, so
/// the run mints its own throwaway PKI and trusts exactly it. Nothing here
/// resembles how a deployment should get a certificate — see
/// `docs/tls-proposal.md` §10.
fn issue_pki(dir: &std::path::Path) -> anyhow::Result<(String, String, String)> {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, PKCS_ECDSA_P256_SHA256};

    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let mut ca_params = CertificateParams::new(Vec::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.clone().self_signed(&ca_key)?;

    let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let leaf = CertificateParams::new(vec!["localhost".to_string()])?
        .signed_by(&leaf_key, &rcgen::Issuer::new(ca_params, ca_key))?;

    let ca_path = dir.join("ca.pem");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&ca_path, ca.pem())?;
    // Leaf first, then the issuer: the order a chain is read in.
    std::fs::write(&cert_path, format!("{}{}", leaf.pem(), ca.pem()))?;
    std::fs::write(&key_path, leaf_key.serialize_pem())?;

    Ok((
        ca_path.display().to_string(),
        cert_path.display().to_string(),
        key_path.display().to_string(),
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut options = Options::parse();

    // An embedded server unless one was named. Convenient, and honest as long
    // as it is reported: the generator and the server share these cores, so the
    // throughput figure is a floor rather than the server's ceiling.
    let (addr, _server) = match &options.addr {
        Some(addr) => (addr.clone(), None),
        None => {
            let dir = tempfile::tempdir()?;
            let mut config = vash_server::Config::default();
            config.server.listen = "127.0.0.1:0".parse()?;
            config.store.path = dir.path().join("db");
            config.store.durability = vash_server::config::Durability::Lazy;
            config.store.wipe_on_start = true;
            config.store.map_size_mb = 4096;
            config.observability.admin_listen = String::new();
            if options.shards > 0 {
                config.store.shards = options.shards;
            }
            if options.tls {
                let (ca, cert, key) = issue_pki(dir.path())?;
                config.tls.listen = "127.0.0.1:0".into();
                config.tls.cert = cert.into();
                config.tls.key = key.into();
                options.tls_ca = Some(ca);
            }

            let server = vash_server::Server::bind(config).await?;
            // The TLS port when the run asked for one, so that `--tls` against
            // the embedded server needs no second argument.
            let addr = match options.tls {
                true => server
                    .tls_addr()
                    .ok_or_else(|| anyhow::anyhow!("the TLS listener did not bind"))?
                    .to_string(),
                false => server.local_addr()?.to_string(),
            };
            let handle = tokio::spawn(server.serve(std::future::pending::<()>()));
            (addr, Some((handle, dir)))
        }
    };

    let value = vec![b'x'; options.value_bytes];
    println!(
        "workload {} | {} connections | pipeline {} | {} keys x {} B | {}s\ntarget {}\n",
        options.workload.as_str(),
        options.connections,
        options.pipeline,
        options.keys,
        options.value_bytes,
        options.duration.as_secs(),
        addr,
    );
    if options.tls {
        println!("over TLS\n");
    }

    let connector = Connector::new(&options, addr.clone())?;

    if options.workload.reads() {
        let started = Instant::now();
        populate(&connector, options.keys, &value).await?;
        println!(
            "populated {} keys in {:.2?} ({:.0} writes/s)",
            options.keys,
            started.elapsed(),
            options.keys as f64 / started.elapsed().as_secs_f64()
        );
    }

    let shared = Arc::new(OptionsShared {
        pipeline: options.pipeline,
        keys: options.keys,
        value,
        workload: options.workload,
    });
    let stop = Arc::new(AtomicBool::new(false));
    // Every connection is up and handshaken before the clock starts, so setup
    // does not land in the measurement.
    let barrier = Arc::new(tokio::sync::Barrier::new(options.connections + 1));

    let mut tasks = Vec::with_capacity(options.connections);
    for id in 0..options.connections as u64 {
        tasks.push(tokio::spawn(worker(
            connector.clone(),
            Arc::clone(&shared),
            id,
            Arc::clone(&stop),
            Arc::clone(&barrier),
        )));
    }

    barrier.wait().await;
    let started = Instant::now();
    tokio::time::sleep(options.duration).await;
    stop.store(true, Ordering::Relaxed);

    let mut total = Histogram::new();
    let completed = AtomicU64::new(0);
    let hits = AtomicU64::new(0);
    let mut failures = 0;
    for task in tasks {
        match task.await? {
            Ok((ops, hit, histogram)) => {
                completed.fetch_add(ops, Ordering::Relaxed);
                hits.fetch_add(hit, Ordering::Relaxed);
                total.merge(&histogram);
            }
            Err(e) => {
                failures += 1;
                if failures == 1 {
                    eprintln!("a connection failed: {e}");
                }
            }
        }
    }
    let elapsed = started.elapsed();

    let ops = completed.load(Ordering::Relaxed);
    let rate = ops as f64 / elapsed.as_secs_f64();
    println!(
        "\n{:>14} {:.0} ops/s over {:.2?} ({ops} operations)",
        "throughput:", rate, elapsed
    );
    if options.workload.reads() {
        let hit = hits.load(Ordering::Relaxed);
        println!(
            "{:>14} {:.1}% ({hit} hits) — a low number here means the run measured misses",
            "hit rate:",
            100.0 * hit as f64 / ops.max(1) as f64
        );
    }

    if options.pipeline == 1 {
        println!(
            "{:>14} mean {:.3} ms | p50 {:.3} | p99 {:.3} | p99.9 {:.3} | max {:.3}",
            "latency:",
            total.mean_ms(),
            total.percentile(0.50),
            total.percentile(0.99),
            total.percentile(0.999),
            total.max_us as f64 / 1000.0,
        );
    } else {
        println!(
            "{:>14} not reported: with pipeline {} a measurement covers a whole batch, \n{:>15}so it is not a client-visible round trip",
            "latency:", options.pipeline, ""
        );
    }
    if failures > 0 {
        println!("{failures} connection(s) failed");
    }

    Ok(())
}
