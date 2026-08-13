//! The counter snapshot behind memcached `stats` and Redis `INFO`.
//!
//! **One list, two renderings.** Both commands ask the same question — what has
//! this server been doing — and answering it twice would be two sets of numbers
//! that had to agree forever. So there is one canonical list here, and each
//! dialect picks from it by name: memcached prints every pair as a `STAT` line,
//! and `vash_proto::resp::encode::info` walks a section table that maps these
//! names onto Redis's.
//!
//! The names are memcached's wherever memcached has one, because that dialect
//! prints them verbatim and a client reading `curr_items` should find
//! `curr_items`. Everything else carries a `vash_` prefix, which is also what
//! keeps a vash-only counter from ever being mistaken for an upstream one.
//!
//! **Nothing here is reported that is not measured.** A counter that reads zero
//! because the feature does not exist is worse than an absent field: it looks
//! like healthy silence. That rule is why `stats` has no `bytes_read`, why the
//! metadump line has no `la=`, and why a store that cannot be queried
//! contributes no keys at all rather than a row of zeroes.

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::error;

use crate::metrics::CommandKind;
use crate::state::ServerState;

/// Builds the counter list. See the module docs for what belongs in it.
pub fn collect(state: &ServerState) -> Vec<(String, String)> {
    let metrics = &state.metrics;
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    let command = |kind| metrics.commands.total(kind);

    let mut stats: Vec<(String, String)> = vec![
        ("pid".into(), std::process::id().to_string()),
        (
            "version".into(),
            vash_proto::memcached::encode::VERSION.into(),
        ),
        ("pointer_size".into(), usize::BITS.to_string()),
        (
            "uptime".into(),
            state.started.elapsed().as_secs().to_string(),
        ),
        (
            "time".into(),
            (vash_core::Clock::new().now_ms() / 1_000).to_string(),
        ),
        ("max_connections".into(), state.max_connections.to_string()),
        (
            "curr_connections".into(),
            load(&metrics.connections_active).to_string(),
        ),
        (
            "total_connections".into(),
            load(&metrics.connections_total).to_string(),
        ),
        (
            "rejected_connections".into(),
            load(&metrics.connections_rejected).to_string(),
        ),
        // memcached counts a retrieval per command, not per key, and so does
        // this: `cmd_get` is how many `get`s were issued, `get_hits` is how many
        // keys came back. A multi-get adds one to the first and several to the
        // second, which is upstream's own arithmetic.
        (
            "cmd_get".into(),
            (command(CommandKind::Get)
                + command(CommandKind::GetMany)
                + command(CommandKind::GetAndTouch))
            .to_string(),
        ),
        (
            "cmd_set".into(),
            (command(CommandKind::Set) + command(CommandKind::SetMany)).to_string(),
        ),
        ("cmd_touch".into(), command(CommandKind::Touch).to_string()),
        ("cmd_flush".into(), command(CommandKind::Flush).to_string()),
        ("get_hits".into(), load(&metrics.hits).to_string()),
        ("get_misses".into(), load(&metrics.misses).to_string()),
        (
            "vash_commands".into(),
            load(&metrics.commands_total).to_string(),
        ),
        ("vash_reads".into(), load(&metrics.reads).to_string()),
        ("vash_writes".into(), load(&metrics.writes).to_string()),
    ];

    match state.store.stats() {
        Ok(s) => stats.extend([
            ("curr_items".into(), s.entries.to_string()),
            ("bytes".into(), s.used_bytes.to_string()),
            ("limit_maxbytes".into(), s.map_size.to_string()),
            ("evictions".into(), s.evicted.to_string()),
            // Beyond memcached's set, but they are what this server is actually
            // about.
            ("vash_shards".into(), s.shards.to_string()),
            ("vash_utilisation".into(), format!("{:.4}", s.utilisation)),
            ("vash_expiry_entries".into(), s.expiry_entries.to_string()),
            ("vash_tags".into(), s.tags.to_string()),
            (
                "vash_tag_index_entries".into(),
                s.tag_index_entries.to_string(),
            ),
            (
                "vash_pending_reclaims".into(),
                s.pending_reclaims.to_string(),
            ),
            ("vash_commits".into(), s.commits.to_string()),
            ("vash_committed_ops".into(), s.committed_ops.to_string()),
            (
                "vash_mean_batch".into(),
                format!("{:.2}", s.mean_batch_size()),
            ),
            ("vash_sweeps".into(), s.sweeps.to_string()),
            ("vash_reclaimed".into(), s.reclaimed.to_string()),
            ("vash_tag_reclaimed".into(), s.tag_reclaimed.to_string()),
            ("vash_sweep_lag_ms".into(), s.sweep_lag_ms.to_string()),
            ("vash_epoch".into(), s.epoch.to_string()),
            ("vash_readers_in_use".into(), s.readers_in_use.to_string()),
            (
                "vash_oldest_reader_age_ms".into(),
                s.oldest_reader_age_ms.to_string(),
            ),
        ]),
        // Omitted rather than zeroed, and the whole group goes together: a
        // `curr_items` of 0 beside a real `cmd_get` reads as an empty cache
        // rather than as an unanswered question.
        Err(e) => error!(error = %e, "could not read store stats"),
    }

    let view = state.cluster.view();
    stats.extend([
        ("vash_cluster_mode".into(), view.mode.as_str().to_string()),
        ("vash_cluster_peers".into(), view.peers.len().to_string()),
        (
            "vash_cluster_peers_reachable".into(),
            state.cluster.peers_reachable().to_string(),
        ),
    ]);

    stats
}

#[cfg(test)]
mod tests {
    /// Two counters under one name would have each dialect's renderer pick
    /// whichever came first, and the choice would change with the list's order.
    #[test]
    fn every_counter_is_named_once() {
        let names: Vec<&str> = SAMPLE.iter().map(|(name, _)| *name).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        let count = sorted.len();
        sorted.dedup();
        assert_eq!(sorted.len(), count, "two counters share a name");
    }

    /// The names `vash_proto::resp::encode::info` looks up have to exist here,
    /// and the check has to be against something — a live `ServerState` needs a
    /// store, which this crate's unit tests do not have. The integration suite
    /// asserts the real thing end to end; this pins the spelling.
    const SAMPLE: &[(&str, &str)] = &[
        ("pid", "1"),
        ("version", "1.6.38-vash"),
        ("pointer_size", "64"),
        ("uptime", "0"),
        ("time", "0"),
        ("max_connections", "1024"),
        ("curr_connections", "0"),
        ("total_connections", "0"),
        ("rejected_connections", "0"),
        ("cmd_get", "0"),
        ("cmd_set", "0"),
        ("cmd_touch", "0"),
        ("cmd_flush", "0"),
        ("get_hits", "0"),
        ("get_misses", "0"),
        ("vash_commands", "0"),
        ("vash_reads", "0"),
        ("vash_writes", "0"),
        ("curr_items", "0"),
        ("bytes", "0"),
        ("limit_maxbytes", "0"),
        ("evictions", "0"),
        ("vash_shards", "1"),
        ("vash_utilisation", "0.0000"),
        ("vash_expiry_entries", "0"),
        ("vash_tags", "0"),
        ("vash_tag_index_entries", "0"),
        ("vash_pending_reclaims", "0"),
        ("vash_commits", "0"),
        ("vash_committed_ops", "0"),
        ("vash_mean_batch", "0.00"),
        ("vash_sweeps", "0"),
        ("vash_reclaimed", "0"),
        ("vash_tag_reclaimed", "0"),
        ("vash_sweep_lag_ms", "0"),
        ("vash_epoch", "0"),
        ("vash_readers_in_use", "0"),
        ("vash_oldest_reader_age_ms", "0"),
        ("vash_cluster_mode", "standalone"),
        ("vash_cluster_peers", "0"),
        ("vash_cluster_peers_reachable", "0"),
    ];

    /// Every name `INFO` sources from a counter must be one this module emits.
    ///
    /// The two live in different crates — the table is in `vash-proto`, the
    /// counters here — so nothing but this check stops a rename in one from
    /// silently dropping a field from the other.
    #[test]
    fn the_info_table_sources_only_names_that_exist() {
        for name in vash_proto::resp::encode::info_sourced_names() {
            assert!(
                SAMPLE.iter().any(|(known, _)| known == &name),
                "INFO sources `{name}`, which `stats::collect` does not emit"
            );
        }
    }
}
