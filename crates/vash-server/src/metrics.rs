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

use vash_store::StoreStats;

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

/// Counters for cluster invalidation.
///
/// The three questions an operator has about it: is fan-out getting through, is
/// anti-entropy running, and how long since this node last heard from a peer.
/// The last is the honest form of "convergence lag" — a node cannot measure how
/// stale it is, only how long it has been since it last had the chance to find
/// out.
#[derive(Debug, Default)]
pub struct ClusterMetrics {
    peers: AtomicU64,
    fanout_sent: AtomicU64,
    fanout_failed: AtomicU64,
    gossip_rounds: AtomicU64,
    gossip_failed: AtomicU64,
    merged: AtomicU64,
    /// Milliseconds since the process started, at the last successful exchange.
    /// Zero means there has not been one.
    last_gossip_ms: AtomicU64,
    started: std::sync::OnceLock<std::time::Instant>,
}

impl ClusterMetrics {
    fn uptime_ms(&self) -> u64 {
        self.started
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_millis() as u64
    }

    pub fn peers_configured(&self, count: u64) {
        // Starts the clock, so "time since the last round" is measured from
        // startup rather than from the first success.
        let _ = self.uptime_ms();
        self.peers.store(count, Ordering::Relaxed);
    }

    pub fn fanout_sent(&self) {
        self.fanout_sent.fetch_add(1, Ordering::Relaxed);
        self.last_gossip_ms
            .store(self.uptime_ms(), Ordering::Relaxed);
    }

    pub fn fanout_failed(&self) {
        self.fanout_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn gossip_round(&self) {
        self.gossip_rounds.fetch_add(1, Ordering::Relaxed);
        self.last_gossip_ms
            .store(self.uptime_ms(), Ordering::Relaxed);
    }

    pub fn gossip_rounds(&self) -> u64 {
        self.gossip_rounds.load(Ordering::Relaxed)
    }

    pub fn gossip_failed(&self) {
        self.gossip_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn merged(&self, count: u64) {
        self.merged.fetch_add(count, Ordering::Relaxed);
    }

    /// How long since a peer exchange last succeeded, in milliseconds.
    ///
    /// Measured from startup until the first success, so a node that has never
    /// reached anybody reports a growing number rather than a reassuring zero.
    pub fn since_last_exchange_ms(&self) -> u64 {
        self.uptime_ms()
            .saturating_sub(self.last_gossip_ms.load(Ordering::Relaxed))
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
pub fn render_prometheus(
    server: &ServerMetrics,
    store: &StoreStats,
    cluster: &ClusterMetrics,
    peers_reachable: u64,
) -> String {
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
        "vash_connections_total",
        "counter",
        "Connections accepted since start.",
        load(&server.connections_total).to_string(),
    );
    metric!(
        "vash_connections_active",
        "gauge",
        "Connections currently open.",
        load(&server.connections_active).to_string(),
    );
    metric!(
        "vash_connections_rejected_total",
        "counter",
        "Connections refused because the limit was reached.",
        load(&server.connections_rejected).to_string(),
    );
    metric!(
        "vash_commands_total",
        "counter",
        "Commands executed.",
        load(&server.commands_total).to_string(),
    );
    metric!(
        "vash_reads_total",
        "counter",
        "Read commands executed.",
        load(&server.reads).to_string(),
    );
    metric!(
        "vash_writes_total",
        "counter",
        "Write commands executed.",
        load(&server.writes).to_string(),
    );
    metric!(
        "vash_hits_total",
        "counter",
        "Keys served from cache. Counted per key, so a multi-get adds several.",
        load(&server.hits).to_string(),
    );
    metric!(
        "vash_misses_total",
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
                "# HELP vash_errors_total Requests answered with an error."
            );
            let _ = writeln!(out, "# TYPE vash_errors_total counter");
        }
        let _ = writeln!(out, "vash_errors_total{{class=\"{class}\"}} {value}");
    }

    metric!(
        "vash_shards",
        "gauge",
        "Independent storage environments, and therefore concurrent writers.",
        store.shards.to_string(),
    );
    metric!(
        "vash_items",
        "gauge",
        "Records on disk, including any not yet reclaimed.",
        store.entries.to_string(),
    );
    metric!(
        "vash_bytes_used",
        "gauge",
        "Bytes occupied, excluding pages on the free list.",
        store.used_bytes.to_string(),
    );
    metric!(
        "vash_bytes_limit",
        "gauge",
        "Total map size across shards.",
        store.map_size.to_string(),
    );
    metric!(
        "vash_utilisation",
        "gauge",
        "Fullest shard, as a fraction. The eviction watermarks act on this.",
        format!("{:.6}", store.utilisation),
    );
    metric!(
        "vash_evicted_total",
        "counter",
        "Live records dropped to reclaim space. Rising means the cache is too small.",
        store.evicted.to_string(),
    );
    metric!(
        "vash_expired_total",
        "counter",
        "Records reclaimed after their TTL passed.",
        store.reclaimed.to_string(),
    );
    metric!(
        "vash_tag_reclaimed_total",
        "counter",
        "Records reclaimed after a tag invalidation.",
        store.tag_reclaimed.to_string(),
    );
    metric!(
        "vash_pending_reclaims",
        "gauge",
        "Tag invalidations whose space has not been freed yet.",
        store.pending_reclaims.to_string(),
    );
    metric!(
        "vash_sweep_lag_ms",
        "gauge",
        "How far behind the oldest due expiry is, on the worst shard. Sustained growth means reclamation is losing.",
        store.sweep_lag_ms.to_string(),
    );
    metric!(
        "vash_tags",
        "gauge",
        "Registered tag names.",
        store.tags.to_string(),
    );
    metric!(
        "vash_commits_total",
        "counter",
        "Write transactions committed.",
        store.commits.to_string(),
    );
    metric!(
        "vash_committed_ops_total",
        "counter",
        "Operations those commits carried. Divided by commits, the mean batch size.",
        store.committed_ops.to_string(),
    );
    metric!(
        "vash_readers_in_use",
        "gauge",
        "LMDB reader slots held. Approaching the limit means reads will start failing.",
        store.readers_in_use.to_string(),
    );
    metric!(
        "vash_readers_max",
        "gauge",
        "Reader slots available.",
        store.max_readers.to_string(),
    );

    metric!(
        "vash_cluster_peers",
        "gauge",
        "Peers configured.",
        load(&cluster.peers).to_string(),
    );
    metric!(
        "vash_cluster_peers_reachable",
        "gauge",
        "Peers whose last exchange succeeded.",
        peers_reachable.to_string(),
    );
    metric!(
        "vash_cluster_fanout_total",
        "counter",
        "Invalidations forwarded to a peer.",
        load(&cluster.fanout_sent).to_string(),
    );
    metric!(
        "vash_cluster_fanout_failures_total",
        "counter",
        "Invalidations that did not reach a peer. Anti-entropy repairs these, so a low rate is \
         normal and a sustained one means a peer is down.",
        load(&cluster.fanout_failed).to_string(),
    );
    metric!(
        "vash_cluster_gossip_rounds_total",
        "counter",
        "Anti-entropy exchanges completed.",
        load(&cluster.gossip_rounds).to_string(),
    );
    metric!(
        "vash_cluster_gossip_failures_total",
        "counter",
        "Anti-entropy exchanges that failed.",
        load(&cluster.gossip_failed).to_string(),
    );
    metric!(
        "vash_cluster_merged_total",
        "counter",
        "Tag generations received from peers and max-merged.",
        load(&cluster.merged).to_string(),
    );
    metric!(
        "vash_cluster_last_exchange_age_ms",
        "gauge",
        "Milliseconds since an exchange with any peer last succeeded. Growing past a few gossip \
         intervals means this node is diverging.",
        cluster.since_last_exchange_ms().to_string(),
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

        let rendered = render_prometheus(
            &metrics,
            &StoreStats::default(),
            &ClusterMetrics::default(),
            0,
        );

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

        let cluster = ClusterMetrics::default();
        cluster.peers_configured(3);
        cluster.fanout_sent();
        cluster.fanout_failed();
        cluster.merged(4);

        let rendered = render_prometheus(&metrics, &StoreStats::default(), &cluster, 2);
        assert!(rendered.contains("vash_hits_total 2"), "{rendered}");
        assert!(rendered.contains("vash_misses_total 1"));
        assert!(rendered.contains("vash_writes_total 1"));
        assert!(rendered.contains("vash_errors_total{class=\"overloaded\"} 1"));
        assert!(rendered.contains("vash_errors_total{class=\"internal\"} 0"));
        assert!(rendered.contains("vash_cluster_peers 3"));
        assert!(rendered.contains("vash_cluster_peers_reachable 2"));
        assert!(rendered.contains("vash_cluster_fanout_total 1"));
        assert!(rendered.contains("vash_cluster_fanout_failures_total 1"));
        assert!(rendered.contains("vash_cluster_merged_total 4"));
    }

    #[test]
    fn a_node_that_has_never_reached_a_peer_does_not_report_zero_age() {
        // A reassuring zero would read as "just synchronised" on a node that
        // has in fact never talked to anybody.
        let cluster = ClusterMetrics::default();
        cluster.peers_configured(1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(cluster.since_last_exchange_ms() > 0);

        cluster.gossip_round();
        assert!(cluster.since_last_exchange_ms() < 5);
    }
}
