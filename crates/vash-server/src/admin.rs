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

    let Some((method, path)) = request_line(&buf) else {
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
        _ => respond(&mut stream, 404, "text/plain", "not found").await,
    }
}

fn request_line(buf: &[u8]) -> Option<(&str, &str)> {
    let line = buf.split(|b| *b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?.trim_end();
    let mut parts = line.split(' ');
    let method = parts.next()?;
    let target = parts.next()?;
    // Strip any query string; none of these routes take parameters.
    Some((method, target.split('?').next().unwrap_or(target)))
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
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
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
    fn request_line_is_parsed_and_the_query_dropped() {
        assert_eq!(
            request_line(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n"),
            Some(("GET", "/metrics"))
        );
        assert_eq!(
            request_line(b"GET /stats?pretty=1 HTTP/1.1\r\n\r\n"),
            Some(("GET", "/stats"))
        );
        assert_eq!(
            request_line(b"POST /metrics HTTP/1.1\r\n\r\n"),
            Some(("POST", "/metrics"))
        );
        assert_eq!(request_line(b"garbage\r\n\r\n"), None);
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
