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
//! like healthy silence. That rule is why the metadump line has no `la=`, why
//! `INFO` has no `mem_fragmentation_ratio`, and why a store that cannot be
//! queried contributes no keys at all rather than a row of zeroes.
//!
//! Two corollaries, both of which bit while this was written:
//!
//! - **A name matching is not a meaning matching.** memcached's `reclaimed`
//!   counts entries stored into the memory of an expired one — a slab-reuse
//!   number. This server's sweeper reclaim count is a different quantity that
//!   happens to share a word, so it is `vash_reclaimed` and memcached's
//!   `reclaimed` is absent. The same reasoning keeps `expires` out of `INFO`'s
//!   `db0` line.
//! - **A field that is permanently zero is honest.** `udpport 0` and
//!   `ssl_enabled no` measure a decision; they are not placeholders.
//!
//! The `stats` **subcommands** are specified nowhere — upstream's protocol
//! document says their data "is not documented in this version of the protocol,
//! and [is] subject to change" — so the framing is matched against what
//! memcached 1.6.45 sends and the field list is deliberately a subset. See
//! `docs/stats-subcommands.md`.

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::error;
use vash_proto::memcached::encode::StatsSection;

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
        (
            "max_connections".into(),
            state.binding.max_connections.to_string(),
        ),
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
        (
            "cmd_meta".into(),
            load(&metrics.outcomes.meta_commands).to_string(),
        ),
        ("get_hits".into(), load(&metrics.hits).to_string()),
        ("get_misses".into(), load(&metrics.misses).to_string()),
        // The per-command splits. `get_hits`/`get_misses` above answer "is the
        // cache working"; these answer "which command is missing", which is
        // often the more actionable question.
        (
            "delete_hits".into(),
            load(&metrics.outcomes.delete_hits).to_string(),
        ),
        (
            "delete_misses".into(),
            load(&metrics.outcomes.delete_misses).to_string(),
        ),
        (
            "incr_hits".into(),
            load(&metrics.outcomes.incr_hits).to_string(),
        ),
        (
            "incr_misses".into(),
            load(&metrics.outcomes.incr_misses).to_string(),
        ),
        (
            "decr_hits".into(),
            load(&metrics.outcomes.decr_hits).to_string(),
        ),
        (
            "decr_misses".into(),
            load(&metrics.outcomes.decr_misses).to_string(),
        ),
        (
            "cas_hits".into(),
            load(&metrics.outcomes.cas_hits).to_string(),
        ),
        (
            "cas_misses".into(),
            load(&metrics.outcomes.cas_misses).to_string(),
        ),
        (
            "cas_badval".into(),
            load(&metrics.outcomes.cas_badval).to_string(),
        ),
        (
            "touch_hits".into(),
            load(&metrics.outcomes.touch_hits).to_string(),
        ),
        (
            "touch_misses".into(),
            load(&metrics.outcomes.touch_misses).to_string(),
        ),
        (
            "total_items".into(),
            load(&metrics.outcomes.total_items).to_string(),
        ),
        (
            "store_too_large".into(),
            load(&metrics.outcomes.store_too_large).to_string(),
        ),
        (
            "store_no_memory".into(),
            load(&metrics.errors_capacity).to_string(),
        ),
        // Both halves of upstream's pair: every attempt, and the ones that
        // failed. A failure rate that is not zero is the alert worth having.
        (
            "auth_cmds".into(),
            (load(&metrics.auth_ok) + load(&metrics.auth_failed)).to_string(),
        ),
        ("auth_errors".into(), load(&metrics.auth_failed).to_string()),
        (
            "bytes_read".into(),
            load(&metrics.outcomes.bytes_read).to_string(),
        ),
        (
            "bytes_written".into(),
            load(&metrics.outcomes.bytes_written).to_string(),
        ),
        // The listener is never disabled: past the connection limit a
        // connection is refused outright rather than queued, which is the
        // `maxconns_fast` behaviour `stats settings` reports.
        ("accepting_conns".into(), "1".into()),
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

/// Builds one `stats` subcommand's counters.
///
/// [`StatsSection::General`] never reaches here: it is the only section that is
/// a question about the *cache*, so it travels as a `Command::Stats` through
/// the shared boundary and comes back as [`collect`]. Everything else describes
/// the server and is answered from configuration and metrics alone.
pub fn section(state: &ServerState, section: StatsSection) -> Vec<(String, String)> {
    match section {
        // Answered through the boundary; see the doc comment. Rendering the
        // general set here as well keeps a routing slip from producing an empty
        // reply that reads like an empty server.
        StatsSection::General => collect(state),
        StatsSection::Settings => settings(state),
        StatsSection::Items => items(state),
        StatsSection::Slabs => slabs(state),
        StatsSection::Conns => connections(state),
        // Upstream keeps the item-size histogram only under `-o track_sizes`
        // and answers exactly this line otherwise, so this is byte-identical to
        // a stock memcached rather than an approximation of one.
        StatsSection::Sizes => vec![("sizes_status".into(), "disabled".into())],
        // `extstore` and `proxy`: a bare `END`, which is what a memcached built
        // without them answers. There is neither here.
        StatsSection::Empty => Vec::new(),
    }
}

/// `stats settings` — the configuration this node is running.
///
/// The section with the best fit, because nearly every field upstream reports
/// is a configuration value and this server has configuration. Names are
/// memcached's where the *meaning* matches; two matched exactly and are worth
/// noting, since they are the reason this is more than a translation table:
/// `flush_enabled` and `dump_enabled` gate precisely what `protocol.flush_enabled`
/// and `protocol.listing_enabled` gate.
fn settings(state: &ServerState) -> Vec<(String, String)> {
    let protocol = &state.protocol;
    let yes_no = |flag: bool| if flag { "yes" } else { "no" }.to_string();

    let mut settings: Vec<(String, String)> = vec![
        ("maxconns".into(), state.binding.max_connections.to_string()),
        ("tcpport".into(), state.binding.addr.port().to_string()),
        // A standing non-goal: UDP is an amplification vector (plan §16). A
        // permanent zero measures that decision rather than standing in for a
        // number nobody took.
        ("udpport".into(), "0".into()),
        ("inter".into(), state.binding.addr.ip().to_string()),
        // `verbosity` is accepted and ignored, so it is never anything else.
        ("verbosity".into(), "0".into()),
        // Capacity pressure always evicts; there is no no-eviction mode.
        ("evictions".into(), "on".into()),
        ("domain_socket".into(), "NULL".into()),
        ("shutdown_command".into(), "no".into()),
        ("cas_enabled".into(), "yes".into()),
        // SASL lives only in the binary protocol, which is a standing non-goal.
        ("auth_enabled_sasl".into(), "no".into()),
        (
            "auth_enabled_ascii".into(),
            yes_no(state.auth.current().required()),
        ),
        // Over the limit a connection is refused rather than accepted and
        // starved, which is what `maxconns_fast` names.
        ("maxconns_fast".into(), "yes".into()),
        ("flush_enabled".into(), yes_no(protocol.flush_enabled)),
        ("dump_enabled".into(), yes_no(protocol.listing_enabled)),
        // The `lru_crawler` dumps are the enumeration this gate covers.
        ("lru_crawler".into(), yes_no(protocol.listing_enabled)),
        (
            "lru_crawler_tocrawl".into(),
            protocol.listing_max_scan.to_string(),
        ),
        // There is no LRU to maintain, no temporary segment, and no size
        // tracking — each of these is a measured "no", not an unfilled field.
        ("lru_maintainer_thread".into(), "no".into()),
        ("temp_lru".into(), "no".into()),
        ("track_sizes".into(), "no".into()),
        // `stats detail` is not implemented, so it is never enabled.
        ("detail_enabled".into(), "no".into()),
        // No TLS in v1 (plan §16). A client that checks this before sending a
        // credential must not be told otherwise.
        ("ssl_enabled".into(), "no".into()),
        ("proxy_enabled".into(), "no".into()),
        // `mc_flags` is a `u32`.
        ("client_flags_size".into(), "4".into()),
    ];

    // The configuration that has no memcached name, which is most of what an
    // operator actually needs to see.
    settings.extend([
        ("vash_shards".into(), state.info.shards.to_string()),
        (
            "vash_max_value_bytes".into(),
            state.info.max_value_len.to_string(),
        ),
        (
            "vash_max_tags_per_record".into(),
            state.info.max_tags_per_record.to_string(),
        ),
        (
            "vash_listing_max_scan".into(),
            protocol.listing_max_scan.to_string(),
        ),
        // Under `vash_` names rather than as memcached's `threads` and
        // `num_threads`, which count the workers serving *connections*. This
        // pool does storage work; connections are served by the async runtime.
        // Two pools, neither of them memcached's.
        (
            "vash_max_blocking_threads".into(),
            state.binding.max_blocking_threads.to_string(),
        ),
        (
            "vash_read_buffer".into(),
            state.binding.read_buffer.to_string(),
        ),
        (
            "vash_scan_cursors".into(),
            protocol.scan_cursors.to_string(),
        ),
        (
            "vash_scan_cursor_ttl_ms".into(),
            protocol.scan_cursor_ttl_ms.to_string(),
        ),
        (
            "vash_memcached_enabled".into(),
            yes_no(protocol.memcached_enabled),
        ),
        ("vash_resp_enabled".into(), yes_no(protocol.resp_enabled)),
        ("vash_inline_reads".into(), yes_no(state.inline_reads)),
        (
            "vash_cluster_mode".into(),
            state.cluster.view().mode.as_str().to_string(),
        ),
        (
            "vash_cluster_peers".into(),
            state.cluster.view().peers.len().to_string(),
        ),
    ]);

    // `item_size_max` and `maxbytes` come from the store rather than from
    // configuration, because what matters is what was actually opened.
    match state.store.stats() {
        Ok(s) => settings.extend([
            ("maxbytes".into(), s.map_size.to_string()),
            ("item_size_max".into(), state.info.max_value_len.to_string()),
            ("vash_max_readers".into(), s.max_readers.to_string()),
        ]),
        Err(e) => error!(error = %e, "could not read store stats for settings"),
    }

    settings
}

/// The one slab class this server reports.
///
/// There are no slab classes, so everything is in one — and this is the same
/// constant `lru_crawler metadump` prints as `cls=`, so the class a tool
/// discovers through `stats items` is the class the dumps accept.
fn class_id() -> &'static str {
    // Borrowed from the encoder rather than restated, so the two cannot drift.
    std::str::from_utf8(vash_proto::memcached::encode::DUMP_CLASS).expect("ascii")
}

/// `stats items` — per slab class, of which there is one.
fn items(state: &ServerState) -> Vec<(String, String)> {
    let class = class_id();
    let mut items = Vec::new();

    match state.store.stats() {
        Ok(s) => items.extend([
            (format!("items:{class}:number"), s.entries.to_string()),
            (format!("items:{class}:evicted"), s.evicted.to_string()),
            // Upstream's `outofmemory` counts stores refused for want of
            // space, which is exactly what this counts.
            (
                format!("items:{class}:outofmemory"),
                state
                    .metrics
                    .errors_capacity
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            // Beyond upstream's set. `reclaimed` in particular is *not*
            // upstream's `reclaimed`, which counts slab reuse on store — see
            // the module docs.
            (
                format!("items:{class}:vash_expiry_entries"),
                s.expiry_entries.to_string(),
            ),
            (
                format!("items:{class}:vash_tag_index_entries"),
                s.tag_index_entries.to_string(),
            ),
            (
                format!("items:{class}:vash_pending_reclaims"),
                s.pending_reclaims.to_string(),
            ),
            (
                format!("items:{class}:vash_reclaimed"),
                s.reclaimed.to_string(),
            ),
            (
                format!("items:{class}:vash_tag_reclaimed"),
                s.tag_reclaimed.to_string(),
            ),
        ]),
        Err(e) => error!(error = %e, "could not read store stats for items"),
    }

    items
}

/// `stats slabs` — what a slab allocator would report.
///
/// LMDB is not one, so most of the geometry has no honest value: a page here
/// holds records of many sizes, and reporting it as a chunk geometry would let
/// a tool compute a slab efficiency that means nothing. What survives is the
/// per-class command counters, which are real — and with one class, the class
/// totals are the server totals.
///
/// Thin, and implemented anyway because tooling calls it unconditionally and an
/// `ERROR` reads as a broken server.
fn slabs(state: &ServerState) -> Vec<(String, String)> {
    let class = class_id();
    let metrics = &state.metrics;
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);

    let mut slabs = vec![
        (format!("{class}:get_hits"), load(&metrics.hits).to_string()),
        (
            format!("{class}:cmd_set"),
            (metrics.commands.total(CommandKind::Set)
                + metrics.commands.total(CommandKind::SetMany))
            .to_string(),
        ),
    ];

    // Read once, and used on both sides of the per-class/totals split below.
    let store = state.store.stats();
    if let Err(e) = &store {
        error!(error = %e, "could not read store stats for slabs");
    }

    if let Ok(s) = &store {
        // **The one geometry field with an exact meaning here**, and the reason
        // it is reported where its siblings are not: upstream's `used_chunks`
        // counts the chunks allocated to live items, and upstream allocates one
        // per item — measured against 1.6.45, where it tracked the item count
        // exactly and fell by one on a delete. There is no chunking at all in
        // this store, so one record is one unit of storage in use and the
        // mapping is exact rather than an approximation.
        //
        // `total_chunks`, `free_chunks` and `chunk_size` stay absent, and that
        // is what keeps this one honest: the meaningless number a slab geometry
        // invites is `used_chunks / total_chunks`, and without a denominator
        // nobody can compute it.
        slabs.push((format!("{class}:used_chunks"), s.entries.to_string()));
    }

    // The totals come after every per-class field, which is upstream's layout.
    // The order is not part of any contract — upstream's own `stats settings`
    // says its fields arrive in no particular order — but a reply read by a
    // human should not interleave the two.
    slabs.push(("active_slabs".into(), class.into()));
    if let Ok(s) = &store {
        slabs.push(("total_malloced".into(), s.map_size.to_string()));
    }

    slabs
}

/// `stats conns` — one block per open connection.
fn connections(state: &ServerState) -> Vec<(String, String)> {
    state.connections.render(state.binding.addr)
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
