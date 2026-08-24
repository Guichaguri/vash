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

fn stats_json(
    store: &vash_store::StoreStats,
    view: &vash_core::ClusterInfo,
    peers_reachable: u64,
    last_exchange_age_ms: u64,
) -> String {
    format!(
        concat!(
            "{{\n",
            "  \"shards\": {},\n",
            "  \"pressure\": \"{}\",\n",
            "  \"items\": {},\n",
            "  \"used_bytes\": {},\n",
            "  \"map_size\": {},\n",
            "  \"utilisation\": {:.6},\n",
            "  \"evicted\": {},\n",
            "  \"expired_reclaimed\": {},\n",
            "  \"tag_reclaimed\": {},\n",
            "  \"pending_reclaims\": {},\n",
            "  \"sweep_lag_ms\": {},\n",
            "  \"tags\": {},\n",
            "  \"expiry_entries\": {},\n",
            "  \"tag_index_entries\": {},\n",
            "  \"commits\": {},\n",
            "  \"committed_ops\": {},\n",
            "  \"mean_batch_size\": {:.2},\n",
            "  \"readers_in_use\": {},\n",
            "  \"max_readers\": {},\n",
            "  \"epoch\": {},\n",
            "  \"cluster_mode\": \"{}\",\n",
            "  \"cluster_peers\": {},\n",
            "  \"cluster_peers_reachable\": {},\n",
            "  \"cluster_last_exchange_age_ms\": {}\n",
            "}}\n"
        ),
        store.shards,
        store.pressure,
        store.entries,
        store.used_bytes,
        store.map_size,
        store.utilisation,
        store.evicted,
        store.reclaimed,
        store.tag_reclaimed,
        store.pending_reclaims,
        store.sweep_lag_ms,
        store.tags,
        store.expiry_entries,
        store.tag_index_entries,
        store.commits,
        store.committed_ops,
        store.mean_batch_size(),
        store.readers_in_use,
        store.max_readers,
        store.epoch,
        view.mode.as_str(),
        view.peers.len(),
        peers_reachable,
        last_exchange_age_ms,
    )
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
        // No serde here, so the shape is checked rather than assumed.
        assert!(json.starts_with('{') && json.trim_end().ends_with('}'));
        assert_eq!(
            json.matches(':').count(),
            json.lines()
                .filter(|l| l.contains('"') && l.contains(':'))
                .count(),
            "every line with a key should have exactly one colon: {json}"
        );
        assert!(json.contains("\"pressure\": \"normal\""));
    }
}
