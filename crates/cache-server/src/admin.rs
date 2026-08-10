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

use cache_store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

use crate::metrics::render_prometheus;
use crate::state::ServerState;

/// Longest request head accepted. Anything larger is not a request this server
/// serves, and reading unboundedly from an unauthenticated socket is how you
/// get an easy denial of service.
const MAX_REQUEST_HEAD: usize = 8 * 1024;

pub async fn serve(
    listener: TcpListener,
    state: Arc<ServerState>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    info!(addr = ?listener.local_addr().ok(), "admin endpoints listening");
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = handle(stream, state).await {
                        debug!(error = %e, "admin request failed");
                    }
                });
            }
            _ = &mut shutdown => break,
        }
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
        buf.extend_from_slice(&chunk[..read]);

        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > MAX_REQUEST_HEAD {
            return respond(&mut stream, 431, "text/plain", "request head too large").await;
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
            let body = render_prometheus(&state.metrics, &store);
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
            respond(&mut stream, 200, "application/json", &stats_json(&store)).await
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

fn stats_json(store: &cache_store::StoreStats) -> String {
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
            "  \"epoch\": {}\n",
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
        let json = stats_json(&cache_store::StoreStats::default());
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
