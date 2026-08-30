//! The admin HTTP endpoints.
//!
//! Three read-only routes, served on a separate port from cache traffic so they
//! can be bound to a private interface and so a flood of scrapes cannot crowd
//! out real requests.
//!
//! | Route | Purpose |
//! |---|---|
//! | `/metrics` | Prometheus exposition |
//! | `/health` | 200 while serving, 503 when a shard has stopped accepting writes |
//! | `/stats` | the same numbers as JSON, for humans |
//!
//! Written directly against the socket rather than pulling in an HTTP stack:
//! the surface is three GETs with fixed responses, and the request only has to
//! be understood well enough to read its path.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::metrics::render_prometheus;
use crate::state::ServerState;

/// Longest request head accepted. Anything larger is not a request this server
/// serves, and reading unboundedly from an unauthenticated socket is how you
/// get an easy denial of service.
const MAX_REQUEST_HEAD: usize = 8 * 1024;

/// Admin requests served at once.
///
/// This port has no authentication — it is meant to be bound to a private
/// interface — so the only thing bounding it is this. Generous against any real
/// scraper (Prometheus opens one connection per scrape) and small enough that
/// filling it costs nothing worth having.
const MAX_CONCURRENT: usize = 32;

/// How long one admin request has to send its head and read its response.
///
/// Without it a client that connects and sends a single byte parks a task
/// forever: unlike the cache port, there is no credential to be waiting for and
/// so no pre-auth deadline to fold this into.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long in-flight requests get to finish once shutdown is signalled, before
/// they are aborted outright.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn serve(
    listener: TcpListener,
    state: Arc<ServerState>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    info!(addr = ?listener.local_addr().ok(), "admin endpoints listening");
    tokio::pin!(shutdown);

    // **Tracked rather than detached, and that is the point.** Every one of
    // these holds an `Arc<ServerState>`, which shares a reference count with
    // the store handle `Server::serve` releases after awaiting this function.
    // A detached request still running at that moment would keep the storage
    // environment open past shutdown — the same hazard the reload task is
    // joined for, one layer down.
    let mut live: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };

                // Reap whatever has finished before judging the limit, so a
                // busy-but-healthy endpoint is not refused for connections that
                // are already gone.
                while live.try_join_next().is_some() {}
                if live.len() >= MAX_CONCURRENT {
                    debug!(
                        in_flight = live.len(),
                        "refusing an admin connection: too many in flight"
                    );
                    drop(stream);
                    continue;
                }

                let state = Arc::clone(&state);
                live.spawn(async move {
                    match tokio::time::timeout(REQUEST_TIMEOUT, handle(stream, state)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => debug!(error = %e, "admin request failed"),
                        Err(_) => debug!(
                            timeout = ?REQUEST_TIMEOUT,
                            "closing an admin request that did not finish"
                        ),
                    }
                });
            }
            // Keeps the set from accumulating finished tasks while the endpoint
            // is idle. Disabled when there is nothing outstanding, so an empty
            // set cannot make this arm fire in a loop.
            Some(_) = live.join_next(), if !live.is_empty() => {}
            _ = &mut shutdown => break,
        }
    }

    // Stop accepting first, then let what is in flight finish. Anything still
    // running past the grace period is aborted: `shutdown` awaits every task,
    // so no admin request survives this function and none can hold the store
    // open behind it.
    drop(listener);
    let drained = tokio::time::timeout(DRAIN_TIMEOUT, async {
        while live.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        debug!(
            in_flight = live.len(),
            "admin drain timed out; aborting requests still in flight"
        );
        live.shutdown().await;
    }
}

async fn handle(mut stream: TcpStream, state: Arc<ServerState>) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];

    // Read until the end of the request head; the body, if any, is ignored.
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        // Where the terminator could start given what just arrived: three bytes
        // back from the old end, since the four-byte sequence may straddle the
        // boundary. Rescanning the whole buffer each time would be quadratic in
        // the head's length for no gain.
        let from = buf.len().saturating_sub(3);
        buf.extend_from_slice(&chunk[..read]);

        // Checked before the terminator search, so the cap is the cap rather
        // than the cap plus whatever the last read happened to bring.
        if buf.len() > MAX_REQUEST_HEAD {
            return respond(&mut stream, 431, "text/plain", "request head too large").await;
        }
        if buf[from..].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    let Some((method, path, query)) = request_line(&buf) else {
        return respond(&mut stream, 400, "text/plain", "malformed request").await;
    };
    if method != "GET" {
        return respond(&mut stream, 405, "text/plain", "only GET is supported").await;
    }

    match path {
        "/metrics" => {
            let store = state.store.stats().unwrap_or_default();
            let body = render_prometheus(
                &state.metrics,
                &store,
                state.cluster.metrics(),
                state.cluster.peers_reachable(),
                state.introspect.slowlog_counters(),
            );
            respond(&mut stream, 200, "text/plain; version=0.0.4", &body).await
        }
        "/health" => {
            let store = state.store.stats().unwrap_or_default();
            // Unhealthy when writes are being refused: the server is up, but it
            // is not doing its job, and a load balancer should know.
            let healthy = store.pressure != "critical";
            let (code, body) = if healthy {
                (200, "ok\n".to_string())
            } else {
                (
                    503,
                    format!("unhealthy: capacity pressure {}\n", store.pressure),
                )
            };
            respond(&mut stream, code, "text/plain", &body).await
        }
        "/stats" => {
            let store = state.store.stats().unwrap_or_default();
            let body = stats_json(
                &store,
                &state.cluster.view(),
                state.cluster.peers_reachable(),
                state.cluster.metrics().since_last_exchange_ms(),
            );
            respond(&mut stream, 200, "application/json", &body).await
        }
        "/slowlog" => slowlog(&mut stream, &state, query).await,
        "/hotkeys" => hotkeys(&mut stream, &state, query).await,
        _ => respond(&mut stream, 404, "text/plain", "not found").await,
    }
}

/// `GET /slowlog[?count=N]` — the requests that crossed the threshold, newest
/// first.
///
/// Answers **501** rather than 404 in a build without the `introspect` feature:
/// the route exists and the binary cannot serve it, which is a different thing
/// from a path that means nothing, and an operator reading a 404 would go
/// looking for a typo. The same reasoning refuses a `tls.listen` at startup in
/// a build without TLS rather than quietly serving in the clear.
#[cfg(feature = "introspect")]
async fn slowlog(stream: &mut TcpStream, state: &ServerState, query: &str) -> std::io::Result<()> {
    const DEFAULT_COUNT: usize = 32;

    let count = query_param(query, "count")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_COUNT)
        .clamp(1, MAX_SLOWLOG_COUNT);

    let body = slowlog_json(
        &state.introspect.slowlog,
        count,
        state.protocol.listing_enabled,
    );
    respond(stream, 200, "application/json", &body).await
}

/// `GET /hotkeys[?count=N]` — the keys taking the most traffic, hottest first.
///
/// **The whole route is behind `protocol.listing_enabled`**, where `/slowlog`
/// gates only the key bytes. The difference is what each one is: a slowlog
/// entry is still useful with its key reduced to a digest — "the same key every
/// time" is most of the diagnosis — where a hot-key ranking reduced to digests
/// is a list of numbers with nothing to act on. There is no useful degraded
/// form, so it is the gate or nothing, and this port has no authentication of
/// its own. §2 and §5.
#[cfg(feature = "introspect")]
async fn hotkeys(stream: &mut TcpStream, state: &ServerState, query: &str) -> std::io::Result<()> {
    const DEFAULT_COUNT: usize = 20;
    const MAX_COUNT: usize = 1024;

    if !state.protocol.listing_enabled {
        return respond(
            stream,
            403,
            "text/plain",
            "hot keys name keys, so they need protocol.listing_enabled",
        )
        .await;
    }

    let count = query_param(query, "count")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_COUNT)
        .clamp(1, MAX_COUNT);

    let body = hotkeys_json(&state.introspect.hotkeys, count);
    respond(stream, 200, "application/json", &body).await
}

#[cfg(not(feature = "introspect"))]
async fn hotkeys(
    stream: &mut TcpStream,
    _state: &ServerState,
    _query: &str,
) -> std::io::Result<()> {
    respond(
        stream,
        501,
        "text/plain",
        "hot-key sampling needs a build with --features introspect",
    )
    .await
}

#[cfg(not(feature = "introspect"))]
async fn slowlog(
    stream: &mut TcpStream,
    _state: &ServerState,
    _query: &str,
) -> std::io::Result<()> {
    respond(
        stream,
        501,
        "text/plain",
        "the slowlog needs a build with --features introspect",
    )
    .await
}

fn request_line(buf: &[u8]) -> Option<(&str, &str, &str)> {
    let line = buf.split(|b| *b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?.trim_end();
    let mut parts = line.split(' ');
    let method = parts.next()?;
    let target = parts.next()?;
    // The path and the query separately. `/slowlog` takes a `count`; the other
    // three take nothing and ignore whatever arrives beside them, which is what
    // they did when this function dropped the query outright.
    match target.split_once('?') {
        Some((path, query)) => Some((method, path, query)),
        None => Some((method, target, "")),
    }
}

/// One `name=value` from a query string, if present.
///
/// Deliberately not a general parser: there is one parameter on one route, and
/// percent-decoding a number would be machinery in service of nothing.
#[cfg(feature = "introspect")]
fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value)
}

/// The most entries `/slowlog` will render in one response.
///
/// A ceiling on the *request*, not on the log: a caller asking for a million
/// gets this many. The log itself is bounded per lane by
/// `observability.introspect.slowlog_max_len`.
#[cfg(feature = "introspect")]
const MAX_SLOWLOG_COUNT: usize = 1024;

/// One rendered slowlog entry.
///
/// Field names follow `slowlog::Entry`, with the one substitution §3.3 of
/// docs/slowlog-proposal.md requires: `key` carries the key's bytes only where
/// enumeration is already permitted, and its digest otherwise.
#[cfg(feature = "introspect")]
#[derive(serde::Serialize)]
struct SlowlogEntryBody {
    id: u64,
    timestamp: u64,
    /// Time in the shard writer queue, and time in the store with that taken
    /// out. Disjoint, and summing to what the request cost inside the server.
    queue_us: u64,
    exec_us: u64,
    command: &'static str,
    dialect: &'static str,
    /// The connection's row in `stats conns`, and where it came from — the same
    /// two facts that listing shows, so an entry here can be matched against a
    /// live connection while it is still open.
    conn_id: u64,
    peer: String,
    keys: u32,
    /// The first key, or `xxh3:<hex>` of it — see [`slowlog_key`].
    key: String,
    /// Whether `key` is the name or the digest, so a reader never has to guess
    /// which of the two they are looking at.
    key_is_digest: bool,
    value_bytes: u64,
    batch: u32,
}

/// The `/slowlog` body.
#[cfg(feature = "introspect")]
#[derive(serde::Serialize)]
struct SlowlogBody {
    /// What `crosses` is comparing against, so a reader can tell an empty log
    /// from a disabled one without a second request.
    threshold_us: u64,
    /// Entries admitted and entries pushed out to make room, for the whole
    /// process. Monotonic; a future `SLOWLOG RESET` clears the entries and
    /// leaves these alone.
    admitted: u64,
    evicted: u64,
    /// Whether keys are rendered as names. `false` means every `key` below is a
    /// digest.
    keys_disclosed: bool,
    entries: Vec<SlowlogEntryBody>,
}

/// How an entry names its key.
///
/// §3.3: the bytes only under `protocol.listing_enabled`, which is the same gate
/// that decides whether any client may enumerate keys — a slowlog is
/// enumeration by another name, and this port has no authentication of its own.
/// Otherwise the whole key's `xxh3`, which still answers "is it the same key
/// every time" and discloses nothing.
#[cfg(feature = "introspect")]
fn slowlog_key(entry: &crate::introspect::slowlog::Entry, disclose: bool) -> (String, bool) {
    if disclose {
        (escape_key(&entry.key), false)
    } else {
        (format!("xxh3:{:016x}", entry.digest), true)
    }
}

/// Renders arbitrary key bytes as a JSON-safe string.
///
/// Keys are bytes, not text: they may be invalid UTF-8 and may contain control
/// characters. Anything outside printable ASCII becomes `%XX`, and a literal
/// `%` is escaped too so the mapping can be reversed. Lossy UTF-8 conversion
/// was the alternative and is worse: it maps distinct keys onto the same
/// rendering, which is exactly the confusion an operator is here to resolve.
#[cfg(feature = "introspect")]
fn escape_key(key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len());
    for byte in key {
        match byte {
            b'%' => out.push_str("%25"),
            0x20..=0x7e => out.push(*byte as char),
            other => out.push_str(&format!("%{other:02x}")),
        }
    }
    out
}

#[cfg(feature = "introspect")]
fn slowlog_json(log: &crate::introspect::slowlog::Slowlog, count: usize, disclose: bool) -> String {
    use std::sync::atomic::Ordering;

    let entries = log
        .entries(count)
        .into_iter()
        .map(|entry| {
            let (key, key_is_digest) = slowlog_key(&entry, disclose);
            SlowlogEntryBody {
                id: entry.id,
                timestamp: entry.timestamp,
                queue_us: entry.queue_us,
                exec_us: entry.exec_us,
                command: entry.command.name(),
                dialect: entry.dialect.name(),
                conn_id: entry.conn_id,
                peer: entry.peer.to_string(),
                keys: entry.keys,
                key,
                key_is_digest,
                value_bytes: entry.value_bytes,
                batch: entry.batch,
            }
        })
        .collect();

    let body = SlowlogBody {
        threshold_us: log.threshold_us(),
        admitted: log.admitted.load(Ordering::Relaxed),
        evicted: log.evicted.load(Ordering::Relaxed),
        keys_disclosed: disclose,
        entries,
    };

    // Pretty-printed, as `/stats` is and for the same reason: this is the route
    // a person reads during an incident.
    serde_json::to_string_pretty(&body).unwrap_or_else(|_| String::from("{}"))
}

/// One key in the hot-key ranking.
///
/// Every count here is a **sample** count or a number derived from one, and the
/// field names say so. §2: an estimate that looks exact is the same failure as a
/// counter reading zero because the feature does not exist.
#[cfg(feature = "introspect")]
#[derive(serde::Serialize)]
struct HotKeyBody {
    key: String,
    /// `xxh3` of the whole key, so a row here can be matched against a
    /// `/slowlog` entry whose key was withheld.
    digest: String,
    /// Whether `key` is the whole of it or the first 128 bytes.
    truncated: bool,
    /// Samples scaled by the sample rate. **An estimate.**
    estimated_ops: u64,
    /// Its relative standard error, `1/sqrt(samples)`. Published beside the
    /// estimate so a reader can see which rows are solid.
    relative_error: f64,
    /// What the estimate was scaled from, split the way the two problems are:
    /// a hot read key wants a client-side cache, a hot written key is a
    /// serialisation point on one shard's single writer.
    samples: u64,
    read_samples: u64,
    write_samples: u64,
    /// Space-Saving's inherited count. The key's true sample count lies in
    /// `(samples - samples_error, samples]`; a large value means this row
    /// arrived recently and inherited an evicted key's total rather than
    /// earning it.
    samples_error: u64,
}

/// The `/hotkeys` body.
#[cfg(feature = "introspect")]
#[derive(serde::Serialize)]
struct HotKeysBody {
    /// Whether sampling is running, so an empty ranking on an idle server is
    /// distinguishable from one on a server that is not looking.
    enabled: bool,
    /// One request in this many is sampled; every `estimated_ops` is scaled by
    /// it.
    sample_rate: u64,
    /// Samples behind the whole ranking.
    sampled: u64,
    /// Counters per thread-lane — the sensitivity. A summary of `m` is
    /// guaranteed to hold anything above `1/m` of its lane's stream, so a key
    /// absent from this list is only known to be below that.
    counters_per_lane: usize,
    keys: Vec<HotKeyBody>,
}

#[cfg(feature = "introspect")]
fn hotkeys_json(sampler: &crate::introspect::hotkeys::HotKeys, count: usize) -> String {
    let merged = sampler.merge();
    let keys = merged
        .counters
        .iter()
        .take(count)
        .map(|counter| HotKeyBody {
            key: escape_key(&counter.key),
            digest: format!("xxh3:{:016x}", counter.digest),
            truncated: counter.truncated,
            estimated_ops: merged.estimated_ops(counter),
            relative_error: rounded(merged.relative_error(counter), 4),
            samples: counter.total(),
            read_samples: counter.read_samples,
            write_samples: counter.write_samples,
            samples_error: counter.error,
        })
        .collect();

    let body = HotKeysBody {
        enabled: sampler.is_enabled(),
        sample_rate: merged.sample_rate,
        sampled: merged.sampled,
        counters_per_lane: sampler.counters_per_lane(),
        keys,
    };

    serde_json::to_string_pretty(&body).unwrap_or_else(|_| String::from("{}"))
}

/// The `/stats` body.
///
/// **A struct rather than a format string, and the field names are the keys.**
/// This was a `format!` over a `concat!` with twenty-four positional
/// arguments, which is a shape where inserting a field in the middle silently
/// reassigns every value after it — `evicted` reporting the tag count and so
/// on — and produces output that still parses. Nothing would have caught it:
/// the test below checks the shape, not which number landed under which name.
///
/// Here a field cannot drift from its key, because it *is* its key. Several of
/// these names deliberately differ from the `StoreStats` field they come from
/// — `items` for `entries`, `expired_reclaimed` for `reclaimed` — and the
/// `rename` makes that mapping something you can read rather than count.
#[derive(serde::Serialize)]
struct StatsBody<'a> {
    shards: u32,
    pressure: &'a str,
    #[serde(rename = "items")]
    entries: u64,
    used_bytes: u64,
    map_size: u64,
    utilisation: f64,
    evicted: u64,
    #[serde(rename = "expired_reclaimed")]
    reclaimed: u64,
    tag_reclaimed: u64,
    pending_reclaims: u64,
    sweep_lag_ms: u64,
    tags: u64,
    expiry_entries: u64,
    tag_index_entries: u64,
    commits: u64,
    committed_ops: u64,
    mean_batch_size: f64,
    readers_in_use: u32,
    max_readers: u32,
    epoch: u32,
    cluster_mode: &'a str,
    cluster_peers: usize,
    cluster_peers_reachable: u64,
    cluster_last_exchange_age_ms: u64,
}

/// Rounds a float to `places` decimals.
///
/// The two float fields were rendered `{:.6}` and `{:.2}`, and serde would
/// otherwise emit every digit an `f64` has. Rounding the value rather than the
/// rendering keeps the body byte-identical to what it has always been, which
/// matters for something a dashboard may already be scraping.
fn rounded(value: f64, places: i32) -> f64 {
    if !value.is_finite() {
        return 0.0;
    }
    let scale = 10f64.powi(places);
    (value * scale).round() / scale
}

fn stats_json(
    store: &vash_store::StoreStats,
    view: &vash_core::ClusterInfo,
    peers_reachable: u64,
    last_exchange_age_ms: u64,
) -> String {
    let body = StatsBody {
        shards: store.shards,
        pressure: store.pressure,
        entries: store.entries,
        used_bytes: store.used_bytes,
        map_size: store.map_size,
        utilisation: rounded(store.utilisation, 6),
        evicted: store.evicted,
        reclaimed: store.reclaimed,
        tag_reclaimed: store.tag_reclaimed,
        pending_reclaims: store.pending_reclaims,
        sweep_lag_ms: store.sweep_lag_ms,
        tags: store.tags,
        expiry_entries: store.expiry_entries,
        tag_index_entries: store.tag_index_entries,
        commits: store.commits,
        committed_ops: store.committed_ops,
        mean_batch_size: rounded(store.mean_batch_size(), 2),
        readers_in_use: store.readers_in_use,
        max_readers: store.max_readers,
        epoch: store.epoch,
        cluster_mode: view.mode.as_str(),
        cluster_peers: view.peers.len(),
        cluster_peers_reachable: peers_reachable,
        cluster_last_exchange_age_ms: last_exchange_age_ms,
    };

    // Pretty-printed because `/stats` is the route for humans; `/metrics` is
    // the one a machine reads. Serialisation cannot fail for a struct of
    // scalars, and answering with an empty object would be a lie, so the
    // fallback names the failure instead.
    match serde_json::to_string_pretty(&body) {
        Ok(mut json) => {
            json.push('\n');
            json
        }
        Err(e) => {
            tracing::error!(error = %e, "could not render the stats body");
            format!("{{\"error\": \"could not render stats: {e}\"}}\n")
        }
    }
}

async fn respond(
    stream: &mut TcpStream,
    code: u16,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        403 => "Forbidden",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Error",
    };

    let head = format!(
        "HTTP/1.1 {code} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_line_is_parsed_and_the_query_kept_apart() {
        assert_eq!(
            request_line(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n"),
            Some(("GET", "/metrics", ""))
        );
        // The path must not carry the query, or `match path` stops matching.
        assert_eq!(
            request_line(b"GET /stats?pretty=1 HTTP/1.1\r\n\r\n"),
            Some(("GET", "/stats", "pretty=1"))
        );
        assert_eq!(
            request_line(b"GET /slowlog?count=5&x=1 HTTP/1.1\r\n\r\n"),
            Some(("GET", "/slowlog", "count=5&x=1"))
        );
        assert_eq!(
            request_line(b"POST /metrics HTTP/1.1\r\n\r\n"),
            Some(("POST", "/metrics", ""))
        );
        assert_eq!(request_line(b"garbage\r\n\r\n"), None);
    }

    #[cfg(feature = "introspect")]
    mod slowlog {
        use super::*;
        use crate::introspect::slowlog::{Record, Slowlog};
        use crate::metrics::{CommandKind, Dialect};

        fn logged(key: &[u8]) -> Slowlog {
            let log = Slowlog::new(1, 8);
            log.record(Record {
                queue_us: 40_000,
                exec_us: 84,
                command: CommandKind::Set,
                origin: crate::dispatch::Origin {
                    dialect: Dialect::Resp,
                    conn_id: 7,
                    peer: "10.0.0.7:54321".parse().expect("a peer"),
                },
                keys: 1,
                key,
                value_bytes: 1024,
                batch: 1,
            });
            log
        }

        #[test]
        fn count_is_read_from_the_query_and_defaults_without_one() {
            assert_eq!(query_param("count=5", "count"), Some("5"));
            assert_eq!(query_param("x=1&count=7&y=2", "count"), Some("7"));
            assert_eq!(query_param("x=1", "count"), None);
            assert_eq!(query_param("", "count"), None);
            // A bare flag is not a parameter, and must not panic.
            assert_eq!(query_param("count", "count"), None);
        }

        /// The two durations are the whole reason this is not Redis's slowlog,
        /// so the body has to keep them apart and keep them disjoint.
        #[test]
        fn the_body_reports_queue_and_execution_separately() {
            let json = slowlog_json(&logged(b"session:a9f"), 32, true);
            let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
            let entry = &value["entries"][0];

            assert_eq!(entry["queue_us"], 40_000);
            assert_eq!(entry["exec_us"], 84);
            assert_eq!(entry["command"], "set");
            assert_eq!(entry["dialect"], "resp");
            assert_eq!(entry["conn_id"], 7);
            assert_eq!(entry["peer"], "10.0.0.7:54321");
            assert_eq!(value["threshold_us"], 1);
            assert_eq!(value["admitted"], 1);
        }

        /// §3.3: the key's bytes only where enumeration is already permitted,
        /// its digest otherwise — and the body says which of the two it gave.
        #[test]
        fn the_key_is_disclosed_only_under_the_listing_gate() {
            let log = logged(b"session:a9f");

            let open: serde_json::Value =
                serde_json::from_str(&slowlog_json(&log, 32, true)).expect("valid JSON");
            assert_eq!(open["keys_disclosed"], true);
            assert_eq!(open["entries"][0]["key"], "session:a9f");
            assert_eq!(open["entries"][0]["key_is_digest"], false);

            let closed: serde_json::Value =
                serde_json::from_str(&slowlog_json(&log, 32, false)).expect("valid JSON");
            assert_eq!(closed["keys_disclosed"], false);
            assert_eq!(closed["entries"][0]["key_is_digest"], true);
            let digest = closed["entries"][0]["key"].as_str().expect("a string");
            assert!(digest.starts_with("xxh3:"), "got {digest}");
            assert!(
                !digest.contains("session"),
                "the gate leaked the key it was closed over: {digest}"
            );
        }

        /// A stable digest is the whole value of the closed form: it answers
        /// "is it the same key every time" without naming it.
        #[test]
        fn the_digest_is_stable_for_a_key_and_differs_between_keys() {
            let render = |key: &[u8]| {
                let json = slowlog_json(&logged(key), 32, false);
                let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
                value["entries"][0]["key"]
                    .as_str()
                    .expect("a string")
                    .to_string()
            };
            assert_eq!(render(b"a"), render(b"a"));
            assert_ne!(render(b"a"), render(b"b"));
        }

        /// Keys are bytes. A key that is not UTF-8 must still render, and two
        /// different keys must not collapse onto one rendering.
        #[test]
        fn a_key_that_is_not_text_still_renders_reversibly() {
            assert_eq!(escape_key(b"plain:key"), "plain:key");
            assert_eq!(escape_key(&[0xff, 0x00]), "%ff%00");
            assert_eq!(escape_key(b"a b"), "a b");
            // A literal percent is escaped, so the mapping can be reversed.
            assert_eq!(escape_key(b"100%"), "100%25");
            assert_ne!(escape_key(&[0xff]), escape_key(&[0xfe]));
        }

        #[test]
        fn an_empty_log_still_reports_its_threshold() {
            let json = slowlog_json(&Slowlog::new(10_000, 8), 32, false);
            let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
            assert_eq!(value["threshold_us"], 10_000);
            assert_eq!(value["admitted"], 0);
            assert_eq!(
                value["entries"].as_array().expect("an array").len(),
                0,
                "an empty log is distinguishable from a disabled one by its threshold"
            );
        }
    }

    #[cfg(feature = "introspect")]
    mod hotkeys {
        use super::*;
        use crate::introspect::hotkeys::HotKeys;

        #[test]
        fn the_body_names_every_count_as_a_sample_or_an_estimate() {
            let hot = HotKeys::new(1, 8);
            for _ in 0..30 {
                hot.observe(b"hot:key", false);
            }
            for _ in 0..10 {
                hot.observe(b"hot:key", true);
            }
            hot.observe(b"cold:key", false);

            let json = hotkeys_json(&hot, 10);
            let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

            assert_eq!(value["enabled"], true);
            assert_eq!(value["sample_rate"], 1);
            assert_eq!(value["sampled"], 41);
            assert_eq!(value["counters_per_lane"], 8);

            let hottest = &value["keys"][0];
            assert_eq!(hottest["key"], "hot:key");
            assert_eq!(hottest["samples"], 40);
            assert_eq!(hottest["read_samples"], 30);
            assert_eq!(hottest["write_samples"], 10);
            // At a rate of one, an estimate is the count — which is the case
            // where a wrong scaling would be invisible, so it is pinned.
            assert_eq!(hottest["estimated_ops"], 40);
            assert!(
                hottest["digest"]
                    .as_str()
                    .expect("a digest")
                    .starts_with("xxh3:")
            );
        }

        /// §2: an estimate has to be named as one, and its error published
        /// beside it rather than left in a document.
        #[test]
        fn an_estimate_carries_its_error_and_its_scale() {
            let hot = HotKeys::new(64, 8);
            for _ in 0..10_000 {
                hot.observe(b"hot:key", false);
            }

            let json = hotkeys_json(&hot, 10);
            let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
            assert_eq!(value["sample_rate"], 64);

            let hottest = &value["keys"][0];
            let samples = hottest["samples"].as_u64().expect("a sample count");
            let estimated = hottest["estimated_ops"].as_u64().expect("an estimate");
            assert_eq!(estimated, samples * 64, "the estimate is samples x rate");

            let error = hottest["relative_error"].as_f64().expect("an error");
            assert!(error > 0.0 && error < 1.0, "got {error}");
        }

        #[test]
        fn a_disabled_sampler_says_so_rather_than_looking_idle() {
            let hot = HotKeys::new(0, 8);
            hot.observe(b"hot:key", false);

            let json = hotkeys_json(&hot, 10);
            let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
            assert_eq!(value["enabled"], false);
            assert_eq!(value["sampled"], 0);
            assert_eq!(value["keys"].as_array().expect("an array").len(), 0);
        }

        /// Keys are bytes here as everywhere, and two that differ must not
        /// collapse onto one rendering.
        #[test]
        fn a_key_that_is_not_text_renders_without_colliding() {
            let hot = HotKeys::new(1, 8);
            hot.observe(&[0xff, 0x01], false);
            hot.observe(&[0xfe, 0x01], false);

            let json = hotkeys_json(&hot, 10);
            let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
            let keys: Vec<&str> = value["keys"]
                .as_array()
                .expect("an array")
                .iter()
                .map(|k| k["key"].as_str().expect("a key"))
                .collect();
            assert_eq!(keys.len(), 2);
            assert_ne!(keys[0], keys[1]);
        }
    }

    #[test]
    fn stats_render_as_parseable_json() {
        let json = stats_json(
            &vash_store::StoreStats::default(),
            &vash_core::ClusterInfo::standalone(),
            0,
            0,
        );
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(parsed["pressure"], "normal");
        assert_eq!(parsed["cluster_mode"], "local");
        assert!(json.ends_with('\n'), "the body should end in a newline");
    }

    /// **Every value lands under its own name.**
    ///
    /// The shape this replaces was twenty-four positional arguments against a
    /// `concat!` template, where inserting a field in the middle shifted every
    /// value after it onto the wrong key and still produced valid JSON. The
    /// old test counted colons and would have passed through all of it. This
    /// gives every field a distinct value and checks where each one arrives,
    /// so a mapping mistake fails here rather than on a dashboard.
    #[test]
    fn every_stat_lands_under_its_own_key() {
        let store = vash_store::StoreStats {
            shards: 1,
            pressure: "high",
            evicted: 2,
            entries: 3,
            expiry_entries: 4,
            tag_index_entries: 5,
            tags: 6,
            pending_reclaims: 7,
            map_size: 8,
            used_bytes: 9,
            utilisation: 0.101_112_9,
            readers_in_use: 11,
            max_readers: 12,
            oldest_reader_age_ms: 13,
            epoch: 14,
            commits: 4,
            committed_ops: 15,
            sweeps: 16,
            reclaimed: 17,
            sweep_lag_ms: 18,
            tag_reclaimed: 19,
            queue_wait_us: 20,
            commit_us: 21,
        };
        let json = stats_json(&store, &vash_core::ClusterInfo::standalone(), 22, 23);
        let got: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        for (key, want) in [
            ("shards", 1.0),
            ("items", 3.0),
            ("used_bytes", 9.0),
            ("map_size", 8.0),
            // Rounded to six places, as the format string it replaced was.
            ("utilisation", 0.101_113),
            ("evicted", 2.0),
            ("expired_reclaimed", 17.0),
            ("tag_reclaimed", 19.0),
            ("pending_reclaims", 7.0),
            ("sweep_lag_ms", 18.0),
            ("tags", 6.0),
            ("expiry_entries", 4.0),
            ("tag_index_entries", 5.0),
            ("commits", 4.0),
            ("committed_ops", 15.0),
            // 15 operations over 4 commits, to two places.
            ("mean_batch_size", 3.75),
            ("readers_in_use", 11.0),
            ("max_readers", 12.0),
            ("epoch", 14.0),
            ("cluster_peers", 0.0),
            ("cluster_peers_reachable", 22.0),
            ("cluster_last_exchange_age_ms", 23.0),
        ] {
            let found = got[key]
                .as_f64()
                .unwrap_or_else(|| panic!("{key} is missing or not a number: {json}"));
            assert_eq!(found, want, "{key} carries the wrong value");
        }
        assert_eq!(got["pressure"], "high");
    }
}
