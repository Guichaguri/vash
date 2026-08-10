//! Request-level counters, and Prometheus rendering.
//!
//! Rendered straight from atomics rather than through a metrics facade. With
//! this few instrumentation points a global recorder would be more machinery
//! than it saves, and the exposition format stays explicit â€” which matters,
//! because a dashboard is only as trustworthy as the meaning of its series.
//!
//! Nothing here reports a value it does not measure. A counter that is always
//! zero because the feature does not exist is worse than an absent one: it
//! looks like healthy silence.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use cache_store::StoreStats;

#[derive(Debug, Default)]
pub struct ServerMetrics {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub connections_rejected: AtomicU64,

    pub commands_total: AtomicU64,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,

    /// Requests answered with a non-OK status, split by the classes an operator
    /// acts on differently.
    pub errors_client: AtomicU64,
    pub errors_capacity: AtomicU64,
    pub errors_overloaded: AtomicU64,
    pub errors_internal: AtomicU64,
}

impl ServerMetrics {
    pub fn connection_opened(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn connection_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn read(&self, hits: u64, misses: u64) {
        self.commands_total.fetch_add(1, Ordering::Relaxed);
        self.reads.fetch_add(1, Ordering::Relaxed);
        if hits > 0 {
            self.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses > 0 {
            self.misses.fetch_add(misses, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn write(&self) {
        self.commands_total.fetch_add(1, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn other(&self) {
        self.commands_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn error(&self, class: ErrorClass) {
        match class {
            ErrorClass::Client => &self.errors_client,
            ErrorClass::Capacity => &self.errors_capacity,
            ErrorClass::Overloaded => &self.errors_overloaded,
            ErrorClass::Internal => &self.errors_internal,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}

/// Error groupings an operator responds to differently: a client error is the
/// caller's problem, capacity means the cache is too small, overload means it
/// is too slow, and internal means something is wrong here.
#[derive(Debug, Clone, Copy)]
pub enum ErrorClass {
    Client,
    Capacity,
    Overloaded,
    Internal,
}

/// Renders the Prometheus text exposition format.
pub fn render_prometheus(server: &ServerMetrics, store: &StoreStats) -> String {
    let mut out = String::with_capacity(4096);
    let load = |value: &AtomicU64| value.load(Ordering::Relaxed);

    fn metric(out: &mut String, name: &str, kind: &str, help: &str, value: String) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        let _ = writeln!(out, "{name} {value}");
    }
    macro_rules! metric {
        ($name:expr, $kind:expr, $help:expr, $value:expr $(,)?) => {
            metric(&mut out, $name, $kind, $help, $value)
        };
    }

    metric!(
        "kached_connections_total",
        "counter",
        "Connections accepted since start.",
        load(&server.connections_total).to_string(),
    );
    metric!(
        "kached_connections_active",
        "gauge",
        "Connections currently open.",
        load(&server.connections_active).to_string(),
    );
    metric!(
        "kached_connections_rejected_total",
        "counter",
        "Connections refused because the limit was reached.",
        load(&server.connections_rejected).to_string(),
    );
    metric!(
        "kached_commands_total",
        "counter",
        "Commands executed.",
        load(&server.commands_total).to_string(),
    );
    metric!(
        "kached_reads_total",
        "counter",
        "Read commands executed.",
        load(&server.reads).to_string(),
    );
    metric!(
        "kached_writes_total",
        "counter",
        "Write commands executed.",
        load(&server.writes).to_string(),
    );
    metric!(
        "kached_hits_total",
        "counter",
        "Keys served from cache. Counted per key, so a multi-get adds several.",
        load(&server.hits).to_string(),
    );
    metric!(
        "kached_misses_total",
        "counter",
        "Keys looked up and not found live.",
        load(&server.misses).to_string(),
    );

    for (class, value) in [
        ("client", load(&server.errors_client)),
        ("capacity", load(&server.errors_capacity)),
        ("overloaded", load(&server.errors_overloaded)),
        ("internal", load(&server.errors_internal)),
    ] {
        if class == "client" {
            let _ = writeln!(
                out,
                "# HELP kached_errors_total Requests answered with an error."
            );
            let _ = writeln!(out, "# TYPE kached_errors_total counter");
        }
        let _ = writeln!(out, "kached_errors_total{{class=\"{class}\"}} {value}");
    }

    metric!(
        "kached_shards",
        "gauge",
        "Independent storage environments, and therefore concurrent writers.",
        store.shards.to_string(),
    );
    metric!(
        "kached_items",
        "gauge",
        "Records on disk, including any not yet reclaimed.",
        store.entries.to_string(),
    );
    metric!(
        "kached_bytes_used",
        "gauge",
        "Bytes occupied, excluding pages on the free list.",
        store.used_bytes.to_string(),
    );
    metric!(
        "kached_bytes_limit",
        "gauge",
        "Total map size across shards.",
        store.map_size.to_string(),
    );
    metric!(
        "kached_utilisation",
        "gauge",
        "Fullest shard, as a fraction. The eviction watermarks act on this.",
        format!("{:.6}", store.utilisation),
    );
    metric!(
        "kached_evicted_total",
        "counter",
        "Live records dropped to reclaim space. Rising means the cache is too small.",
        store.evicted.to_string(),
    );
    metric!(
        "kached_expired_total",
        "counter",
        "Records reclaimed after their TTL passed.",
        store.reclaimed.to_string(),
    );
    metric!(
        "kached_tag_reclaimed_total",
        "counter",
        "Records reclaimed after a tag invalidation.",
        store.tag_reclaimed.to_string(),
    );
    metric!(
        "kached_pending_reclaims",
        "gauge",
        "Tag invalidations whose space has not been freed yet.",
        store.pending_reclaims.to_string(),
    );
    metric!(
        "kached_sweep_lag_ms",
        "gauge",
        "How far behind the oldest due expiry is, on the worst shard. Sustained growth means reclamation is losing.",
        store.sweep_lag_ms.to_string(),
    );
    metric!(
        "kached_tags",
        "gauge",
        "Registered tag names.",
        store.tags.to_string(),
    );
    metric!(
        "kached_commits_total",
        "counter",
        "Write transactions committed.",
        store.commits.to_string(),
    );
    metric!(
        "kached_committed_ops_total",
        "counter",
        "Operations those commits carried. Divided by commits, the mean batch size.",
        store.committed_ops.to_string(),
    );
    metric!(
        "kached_readers_in_use",
        "gauge",
        "LMDB reader slots held. Approaching the limit means reads will start failing.",
        store.readers_in_use.to_string(),
    );
    metric!(
        "kached_readers_max",
        "gauge",
        "Reader slots available.",
        store.max_readers.to_string(),
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_series_is_typed_and_documented() {
        let metrics = ServerMetrics::default();
        metrics.connection_opened();
        metrics.read(3, 1);
        metrics.error(ErrorClass::Capacity);

        let rendered = render_prometheus(&metrics, &StoreStats::default());

        // A Prometheus scrape rejects a sample without a preceding TYPE line
        // for its family, so every emitted series needs one.
        for line in rendered.lines().filter(|l| !l.starts_with('#')) {
            let family = line
                .split(['{', ' '])
                .next()
                .expect("a sample line has a name");
            assert!(
                rendered.contains(&format!("# TYPE {family} ")),
                "{family} is emitted without a TYPE line"
            );
            assert!(
                rendered.contains(&format!("# HELP {family} ")),
                "{family} is emitted without a HELP line"
            );
        }
    }

    #[test]
    fn counters_record_what_happened() {
        let metrics = ServerMetrics::default();
        metrics.read(2, 1);
        metrics.write();
        metrics.error(ErrorClass::Overloaded);

        let rendered = render_prometheus(&metrics, &StoreStats::default());
        assert!(rendered.contains("kached_hits_total 2"), "{rendered}");
        assert!(rendered.contains("kached_misses_total 1"));
        assert!(rendered.contains("kached_writes_total 1"));
        assert!(rendered.contains("kached_errors_total{class=\"overloaded\"} 1"));
        assert!(rendered.contains("kached_errors_total{class=\"internal\"} 0"));
    }
}
