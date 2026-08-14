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

use vash_core::{Command, Reply};
use vash_store::StoreStats;

/// Upper bounds of the latency buckets, in microseconds.
///
/// Chosen for a cache rather than taken from a library default: the interesting
/// range is tens of microseconds to a few milliseconds, so that is where the
/// resolution goes. Everything past 100 ms falls into the overflow bucket,
/// because by then the answer is "something is wrong" rather than a number worth
/// resolving further.
const LATENCY_BOUNDS_US: [u64; 11] = [
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 25_000, 100_000,
];

/// A fixed-bucket latency histogram.
///
/// Hand-rolled, like everything else here: a global recorder would be more
/// machinery than it saves at this instrumentation count, and keeping the
/// exposition explicit is what makes the buckets mean something. Observations
/// are relaxed atomic adds — a scrape can read a bucket and the sum a few
/// nanoseconds apart, which is fine for a histogram and not worth a lock on the
/// request path.
#[derive(Debug, Default)]
pub struct Histogram {
    /// One counter per bound, plus one for everything above the last.
    buckets: [AtomicU64; LATENCY_BOUNDS_US.len() + 1],
    sum_us: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    #[inline]
    pub fn observe(&self, elapsed: std::time::Duration) {
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = LATENCY_BOUNDS_US
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(LATENCY_BOUNDS_US.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders one Prometheus histogram family, with `labels` on every series.
    ///
    /// Prometheus buckets are **cumulative**, so each `le` line carries the
    /// running total rather than the bucket's own count — getting that wrong
    /// produces a histogram that renders but quantiles wrongly, which is the
    /// kind of error a dashboard hides rather than shows.
    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let mut running = 0u64;
        for (index, bound) in LATENCY_BOUNDS_US.iter().enumerate() {
            running += self.buckets[index].load(Ordering::Relaxed);
            let seconds = *bound as f64 / 1e6;
            let _ = writeln!(out, "{name}_bucket{{{labels}le=\"{seconds}\"}} {running}");
        }
        running += self.buckets[LATENCY_BOUNDS_US.len()].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{{labels}le=\"+Inf\"}} {running}");
        let _ = writeln!(
            out,
            "{name}_sum{{{}}} {:.6}",
            labels.trim_end_matches(','),
            self.sum_us.load(Ordering::Relaxed) as f64 / 1e6
        );
        let _ = writeln!(
            out,
            "{name}_count{{{}}} {}",
            labels.trim_end_matches(','),
            self.count.load(Ordering::Relaxed)
        );
    }
}

/// Which wire dialect a request arrived on.
///
/// Carried alongside the command so the counters can answer "who is sending
/// this" — the question that decides whether a compatibility surface is still
/// earning its maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Vcp,
    Memcached,
    Resp,
}

impl Dialect {
    pub const ALL: [Dialect; 3] = [Dialect::Vcp, Dialect::Memcached, Dialect::Resp];

    fn name(self) -> &'static str {
        match self {
            Self::Vcp => "vcp",
            Self::Memcached => "memcached",
            Self::Resp => "resp",
        }
    }
}

/// One counted command.
///
/// Coarser than the wire: `EXISTS`, `TYPE` and `TTL` all arrive as a deadline
/// query and are counted as one, because that is the operation the store
/// actually performs and the thing whose latency an operator can act on. The
/// dialect label is what distinguishes them when that matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    Hello,
    Ping,
    Get,
    GetMany,
    Deadline,
    Set,
    SetMany,
    Delete,
    DeleteMany,
    Touch,
    Expire,
    Append,
    Arithmetic,
    GetAndTouch,
    DeleteByTag,
    Flush,
    TagSync,
    Cluster,
    Listing,
    Stats,
    Other,
}

impl CommandKind {
    pub const ALL: [CommandKind; 21] = [
        Self::Hello,
        Self::Ping,
        Self::Get,
        Self::GetMany,
        Self::Deadline,
        Self::Set,
        Self::SetMany,
        Self::Delete,
        Self::DeleteMany,
        Self::Touch,
        Self::Expire,
        Self::Append,
        Self::Arithmetic,
        Self::GetAndTouch,
        Self::DeleteByTag,
        Self::Flush,
        Self::TagSync,
        Self::Cluster,
        Self::Listing,
        Self::Stats,
        Self::Other,
    ];

    pub fn of(command: &Command<'_>) -> Self {
        match command {
            Command::Hello { .. } => Self::Hello,
            Command::Ping => Self::Ping,
            Command::Get { .. } => Self::Get,
            Command::GetMany(_) => Self::GetMany,
            Command::Deadline { .. } | Command::Deadlines(_) => Self::Deadline,
            Command::Set(_) => Self::Set,
            Command::SetMany { .. } => Self::SetMany,
            Command::Delete { .. } => Self::Delete,
            Command::DeleteMany(_) => Self::DeleteMany,
            Command::Touch { .. } => Self::Touch,
            Command::Expire { .. } => Self::Expire,
            Command::Append { .. } => Self::Append,
            Command::Arithmetic(_) => Self::Arithmetic,
            Command::GetAndTouch { .. } => Self::GetAndTouch,
            Command::DeleteByTag { .. } => Self::DeleteByTag,
            Command::Flush => Self::Flush,
            Command::TagSync { .. } => Self::TagSync,
            Command::Cluster => Self::Cluster,
            Command::ListKeys(_) | Command::ListTags(_) => Self::Listing,
            Command::Stats => Self::Stats,
            Command::Version | Command::Quit => Self::Other,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::Ping => "ping",
            Self::Get => "get",
            Self::GetMany => "get_many",
            Self::Deadline => "deadline",
            Self::Set => "set",
            Self::SetMany => "set_many",
            Self::Delete => "delete",
            Self::DeleteMany => "delete_many",
            Self::Touch => "touch",
            Self::Expire => "expire",
            Self::Append => "append",
            Self::Arithmetic => "arithmetic",
            Self::GetAndTouch => "get_and_touch",
            Self::DeleteByTag => "delete_by_tag",
            Self::Flush => "flush",
            Self::TagSync => "tag_sync",
            Self::Cluster => "cluster",
            Self::Listing => "listing",
            Self::Stats => "stats",
            Self::Other => "other",
        }
    }

    fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|kind| *kind == self)
            .expect("every kind is in ALL")
    }
}

/// Per-command rates and latencies.
///
/// Two axes, deliberately not three: the **counter** carries the dialect, so an
/// operator can see who is sending what, and the **histogram** does not, because
/// a command's latency is a property of the storage work rather than of the
/// wire format that asked for it. Splitting the histograms by dialect as well
/// would triple the series to separate numbers that measure the same thing.
#[derive(Debug, Default)]
pub struct CommandMetrics {
    counts: Vec<AtomicU64>,
    latency: Vec<Histogram>,
}

impl CommandMetrics {
    fn new() -> Self {
        Self {
            counts: (0..CommandKind::ALL.len() * Dialect::ALL.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            latency: (0..CommandKind::ALL.len())
                .map(|_| Histogram::default())
                .collect(),
        }
    }

    #[inline]
    pub fn observe(&self, kind: CommandKind, dialect: Dialect, elapsed: std::time::Duration) {
        let index = kind.index();
        self.counts[index * Dialect::ALL.len() + dialect as usize].fetch_add(1, Ordering::Relaxed);
        self.latency[index].observe(elapsed);
    }

    /// How many times this command ran, across every dialect.
    ///
    /// What `stats` and `INFO` report: both describe the server, not the port a
    /// request happened to arrive on. The per-dialect split stays available to
    /// Prometheus, which is where the question "who is sending this" belongs.
    pub fn total(&self, kind: CommandKind) -> u64 {
        let base = kind.index() * Dialect::ALL.len();
        self.counts[base..base + Dialect::ALL.len()]
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .sum()
    }

    fn render(&self, out: &mut String) {
        let _ = writeln!(
            out,
            "# HELP vash_command_total Commands executed, by command and dialect."
        );
        let _ = writeln!(out, "# TYPE vash_command_total counter");
        for kind in CommandKind::ALL {
            for dialect in Dialect::ALL {
                let value = self.counts[kind.index() * Dialect::ALL.len() + dialect as usize]
                    .load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "vash_command_total{{command=\"{}\",dialect=\"{}\"}} {value}",
                    kind.name(),
                    dialect.name()
                );
            }
        }

        let _ = writeln!(
            out,
            "# HELP vash_command_seconds Time to execute a command, once it reached a thread that could."
        );
        let _ = writeln!(out, "# TYPE vash_command_seconds histogram");
        for kind in CommandKind::ALL {
            self.latency[kind.index()].render(
                out,
                "vash_command_seconds",
                &format!("command=\"{}\",", kind.name()),
            );
        }
    }
}

/// Per-command outcomes: how often each command found what it was aimed at.
///
/// The aggregate `hits`/`misses` pair answers "is the cache working"; this
/// answers "which command is missing", which is a different and often more
/// useful question — a delete miss rate climbing means a client is deleting keys
/// that expired underneath it, and nothing else in the server would say so.
///
/// **Not memcached-shaped, despite being what `stats` needed.** Every one of
/// these is derivable from a reply the executor already inspects, in any
/// dialect, so they are exported to Prometheus too. A counter visible over one
/// wire format and not the others would be an odd thing to have built.
#[derive(Debug, Default)]
pub struct OutcomeMetrics {
    pub delete_hits: AtomicU64,
    pub delete_misses: AtomicU64,
    pub incr_hits: AtomicU64,
    pub incr_misses: AtomicU64,
    pub decr_hits: AtomicU64,
    pub decr_misses: AtomicU64,
    /// A `cas` against a key that exists but has moved on. Distinct from a miss,
    /// and the distinction is the whole point of CAS: it separates "somebody
    /// else got there first" from "it is not there at all".
    pub cas_hits: AtomicU64,
    pub cas_misses: AtomicU64,
    pub cas_badval: AtomicU64,
    pub touch_hits: AtomicU64,
    pub touch_misses: AtomicU64,
    /// Writes refused for exceeding `store.max_value_bytes`.
    pub store_too_large: AtomicU64,
    /// Stores that actually applied — a guarded write that was rejected does
    /// not count, which is what makes this "items stored" rather than "stores
    /// attempted".
    pub total_items: AtomicU64,
    /// Commands that arrived on the memcached *meta* dialect rather than the
    /// classic one.
    pub meta_commands: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl OutcomeMetrics {
    #[inline]
    fn add(counter: &AtomicU64, n: u64) {
        if n > 0 {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Records what a command found, from the reply the executor already has.
    ///
    /// One place, at the single point every command passes through, so the
    /// numbers cannot drift from what was actually served — the same reasoning
    /// that put the hit/miss counting there.
    pub fn observe(&self, command: &Command<'_>, reply: &Reply) {
        use vash_core::{Delta, SetMode, Stored};

        match (command, reply) {
            (Command::Delete { .. }, Reply::Deleted) => Self::add(&self.delete_hits, 1),
            (Command::Delete { .. }, Reply::NotFound) => Self::add(&self.delete_misses, 1),
            (Command::DeleteMany(_), Reply::DeletedMany(hits)) => {
                let found = hits.iter().filter(|hit| **hit).count() as u64;
                Self::add(&self.delete_hits, found);
                Self::add(&self.delete_misses, hits.len() as u64 - found);
            }

            (Command::Touch { .. }, Reply::Touched) => Self::add(&self.touch_hits, 1),
            (Command::Touch { .. }, Reply::NotFound) => Self::add(&self.touch_misses, 1),

            // Direction is a property of the operation, not of the dialect:
            // memcached's `decr` and Redis's `DECRBY` both move a counter down,
            // and an operator asking "how many decrements" means both.
            (Command::Arithmetic(op), reply) => {
                let down = match op.delta {
                    Delta::Counter { decrement, .. } => decrement,
                    Delta::Int { delta, .. } => delta < 0,
                    Delta::Float { delta, .. } => delta < 0.0,
                };
                let (hits, misses) = if down {
                    (&self.decr_hits, &self.decr_misses)
                } else {
                    (&self.incr_hits, &self.incr_misses)
                };
                match reply {
                    Reply::Arithmetic(_) => Self::add(hits, 1),
                    Reply::NotFound => Self::add(misses, 1),
                    _ => {}
                }
            }

            // CAS is the one write whose three outcomes are all distinct, and
            // the only place `Exists` means something other than an error.
            (Command::Set(set), Reply::Stored(outcome)) if matches!(set.mode, SetMode::Cas(_)) => {
                match outcome {
                    Stored::Stored(_) => {
                        Self::add(&self.cas_hits, 1);
                        Self::add(&self.total_items, 1);
                    }
                    Stored::Exists => Self::add(&self.cas_badval, 1),
                    Stored::NotFound | Stored::NotStored => Self::add(&self.cas_misses, 1),
                }
            }

            (_, Reply::Stored(Stored::Stored(_))) => Self::add(&self.total_items, 1),
            (_, Reply::Swapped { outcome, .. }) => {
                if matches!(outcome, Stored::Stored(_)) {
                    Self::add(&self.total_items, 1);
                }
            }
            (_, Reply::StoredMany(stored)) => Self::add(&self.total_items, stored.len() as u64),

            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct ServerMetrics {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub connections_rejected: AtomicU64,

    /// Successful authentications, failed ones, and commands refused for want
    /// of one.
    ///
    /// A failure rate that is not zero is the alert worth having. So, on a
    /// server with authentication required, is a sudden *drop* to zero: it
    /// means nothing is reaching the gate any more.
    pub auth_ok: AtomicU64,
    pub auth_failed: AtomicU64,
    pub auth_refused: AtomicU64,
    /// Connections dropped for not authenticating inside `auth.timeout_ms`, and
    /// refused because the unauthenticated-connection budget was full.
    pub auth_timeouts: AtomicU64,
    pub auth_capacity_rejected: AtomicU64,

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

    /// Per-command rates and latencies. See [`CommandMetrics`].
    pub commands: CommandMetrics,
    /// What each command found. See [`OutcomeMetrics`].
    pub outcomes: OutcomeMetrics,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            connections_total: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            auth_ok: AtomicU64::new(0),
            auth_failed: AtomicU64::new(0),
            auth_refused: AtomicU64::new(0),
            auth_timeouts: AtomicU64::new(0),
            auth_capacity_rejected: AtomicU64::new(0),
            commands_total: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            errors_client: AtomicU64::new(0),
            errors_capacity: AtomicU64::new(0),
            errors_overloaded: AtomicU64::new(0),
            errors_internal: AtomicU64::new(0),
            commands: CommandMetrics::new(),
            outcomes: OutcomeMetrics::default(),
        }
    }
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

    pub fn auth_ok(&self) {
        self.auth_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_failed(&self) {
        self.auth_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_refused(&self) {
        self.auth_refused.fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_timeout(&self) {
        self.auth_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_capacity_rejected(&self) {
        self.auth_capacity_rejected.fetch_add(1, Ordering::Relaxed);
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

    server.commands.render(&mut out);

    // Exported as well as reported over memcached's `stats`, because a delete
    // miss rate is an operational number in any dialect. One family with two
    // labels rather than eighteen series, so a dashboard can sum over either
    // axis.
    let _ = writeln!(
        out,
        "# HELP vash_command_outcome_total What a command found, by command and outcome."
    );
    let _ = writeln!(out, "# TYPE vash_command_outcome_total counter");
    for (command, outcome, value) in [
        ("delete", "hit", load(&server.outcomes.delete_hits)),
        ("delete", "miss", load(&server.outcomes.delete_misses)),
        ("incr", "hit", load(&server.outcomes.incr_hits)),
        ("incr", "miss", load(&server.outcomes.incr_misses)),
        ("decr", "hit", load(&server.outcomes.decr_hits)),
        ("decr", "miss", load(&server.outcomes.decr_misses)),
        ("cas", "hit", load(&server.outcomes.cas_hits)),
        ("cas", "miss", load(&server.outcomes.cas_misses)),
        // A `cas` against a key that exists but has moved on — somebody else
        // got there first, which is a different event from a miss.
        ("cas", "badval", load(&server.outcomes.cas_badval)),
        ("touch", "hit", load(&server.outcomes.touch_hits)),
        ("touch", "miss", load(&server.outcomes.touch_misses)),
    ] {
        let _ = writeln!(
            out,
            "vash_command_outcome_total{{command=\"{command}\",outcome=\"{outcome}\"}} {value}"
        );
    }

    metric!(
        "vash_items_stored_total",
        "counter",
        "Stores that applied. A guarded write that was rejected is not one, \
         which is what separates this from the store command rate.",
        load(&server.outcomes.total_items).to_string(),
    );
    metric!(
        "vash_store_too_large_total",
        "counter",
        "Writes refused for exceeding store.max_value_bytes. Distinct from a \
         full map: this is the client's problem, not the cache's size.",
        load(&server.outcomes.store_too_large).to_string(),
    );
    metric!(
        "vash_meta_commands_total",
        "counter",
        "Commands that arrived on the memcached meta dialect rather than the classic one.",
        load(&server.outcomes.meta_commands).to_string(),
    );
    metric!(
        "vash_network_read_bytes_total",
        "counter",
        "Bytes read from client sockets.",
        load(&server.outcomes.bytes_read).to_string(),
    );
    metric!(
        "vash_network_written_bytes_total",
        "counter",
        "Bytes written to client sockets.",
        load(&server.outcomes.bytes_written).to_string(),
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
        "vash_oldest_reader_age_ms",
        "gauge",
        "Age of the oldest open read transaction. Sustained growth means a reader \
         is pinning a snapshot and blocking page reuse, which grows the file without bound.",
        store.oldest_reader_age_ms.to_string(),
    );

    // The two halves of plan §12's split. Totals rather than a histogram: against
    // `vash_commits_total` and `vash_committed_ops_total` they give the mean of
    // each stage, which is what answers whether the shard writer is the
    // bottleneck — and per-shard histograms would be a great many series to
    // answer a question a ratio already answers.
    metric!(
        "vash_writer_queue_wait_seconds_total",
        "counter",
        "Time write jobs spent waiting in a shard queue. Divided by \
         vash_committed_ops_total, the mean wait; when it dominates the commit time \
         below, the shard writer is the bottleneck.",
        format!("{:.6}", store.queue_wait_us as f64 / 1e6),
    );
    metric!(
        "vash_writer_commit_seconds_total",
        "counter",
        "Time spent applying and committing write batches. Divided by \
         vash_commits_total, the mean commit; when it dominates, the device is the \
         bottleneck and more shards would make it worse.",
        format!("{:.6}", store.commit_us as f64 / 1e6),
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

    /// **The invariant the fused `GET` path stands on.**
    ///
    /// [`crate::dispatch::execute_get_into`] renders a `GET` hit straight out
    /// of the store, so it never builds a [`Reply`] and never reaches
    /// [`OutcomeMetrics::observe`]. That is only safe because `observe` has
    /// nothing to say about a `Command::Get`: the hit and miss counts come from
    /// [`ServerMetrics::read`], which the fused path does call.
    ///
    /// Adding a `Get` arm to `observe` would therefore silently stop counting
    /// whatever it counted, on every `GET` the server serves. This fails first
    /// instead.
    #[test]
    fn observe_has_nothing_to_say_about_a_get() {
        let rendered = |metrics: &ServerMetrics| {
            render_prometheus(
                metrics,
                &StoreStats::default(),
                &ClusterMetrics::default(),
                0,
            )
        };

        let metrics = ServerMetrics::default();
        let before = rendered(&metrics);

        let key = vash_core::Key::from_stored(b"k");
        let value = vash_core::Value {
            data: bytes::Bytes::from_static(b"v"),
            mc_flags: 0,
            cas: 1,
            expires_at_ms: Some(0),
        };
        metrics
            .outcomes
            .observe(&Command::Get { key }, &Reply::Value(value));
        metrics
            .outcomes
            .observe(&Command::Get { key }, &Reply::NotFound);

        assert_eq!(
            before,
            rendered(&metrics),
            "observe now counts something for a GET, which the fused read path \
             in dispatch::execute_get_into bypasses -- that counting would be \
             silently lost. Either count it in execute_get_into too, or take \
             GET off the fused path."
        );
    }

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
        //
        // A histogram declares its type once, on the family: `_bucket`, `_sum`
        // and `_count` are that family's series and carry no TYPE of their own.
        // Stripping the suffix is what makes this check apply to them, rather
        // than exempting them and losing the check entirely.
        for line in rendered.lines().filter(|l| !l.starts_with('#')) {
            let series = line
                .split(['{', ' '])
                .next()
                .expect("a sample line has a name");
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|suffix| series.strip_suffix(suffix))
                .filter(|family| rendered.contains(&format!("# TYPE {family} histogram")))
                .unwrap_or(series);
            assert!(
                rendered.contains(&format!("# TYPE {family} ")),
                "{series} is emitted without a TYPE line"
            );
            assert!(
                rendered.contains(&format!("# HELP {family} ")),
                "{series} is emitted without a HELP line"
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

#[cfg(test)]
mod phase6_tests {
    use super::*;
    use std::time::Duration;

    /// Prometheus buckets are cumulative, and getting that wrong produces a
    /// histogram that renders cleanly and quantiles wrongly — the kind of error
    /// a dashboard hides rather than shows.
    #[test]
    fn histogram_buckets_are_cumulative() {
        let histogram = Histogram::default();
        histogram.observe(Duration::from_micros(5)); // first bucket
        histogram.observe(Duration::from_micros(300)); // 500us bucket
        histogram.observe(Duration::from_secs(1)); // overflow

        let mut out = String::new();
        histogram.render(&mut out, "test_seconds", "");

        assert!(
            out.contains("test_seconds_bucket{le=\"0.00001\"} 1"),
            "{out}"
        );
        // Everything at or under 500us: the 5us and the 300us observations.
        assert!(
            out.contains("test_seconds_bucket{le=\"0.0005\"} 2"),
            "{out}"
        );
        // The overflow observation only appears in +Inf.
        assert!(out.contains("test_seconds_bucket{le=\"0.1\"} 2"), "{out}");
        assert!(out.contains("test_seconds_bucket{le=\"+Inf\"} 3"), "{out}");
        assert!(out.contains("test_seconds_count{} 3"), "{out}");
    }

    #[test]
    fn a_command_is_counted_against_its_dialect_and_timed_once() {
        let metrics = ServerMetrics::default();
        metrics
            .commands
            .observe(CommandKind::Get, Dialect::Resp, Duration::from_micros(20));
        metrics
            .commands
            .observe(CommandKind::Get, Dialect::Vcp, Duration::from_micros(20));

        let rendered = render_prometheus(
            &metrics,
            &StoreStats::default(),
            &ClusterMetrics::default(),
            0,
        );
        assert!(rendered.contains("vash_command_total{command=\"get\",dialect=\"resp\"} 1"));
        assert!(rendered.contains("vash_command_total{command=\"get\",dialect=\"vcp\"} 1"));
        assert!(rendered.contains("vash_command_total{command=\"set\",dialect=\"vcp\"} 0"));
        // The histogram carries no dialect: latency is a property of the storage
        // work, not of the wire format that asked for it.
        assert!(rendered.contains("vash_command_seconds_count{command=\"get\"} 2"));
    }

    /// Every command kind must map to exactly one name, or two commands would
    /// share a series and their rates would silently add together.
    #[test]
    fn command_kind_names_are_distinct() {
        let mut names: Vec<&str> = CommandKind::ALL.iter().map(|kind| kind.name()).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count, "two command kinds share a name");
    }

    /// The index a kind stores itself at has to be stable and in range, since it
    /// is what the counter and histogram arrays are addressed by.
    #[test]
    fn every_command_kind_has_its_own_slot() {
        for (expected, kind) in CommandKind::ALL.iter().enumerate() {
            assert_eq!(kind.index(), expected, "{kind:?} is at the wrong index");
        }
    }

    #[test]
    fn the_reader_age_and_writer_split_are_rendered() {
        // Plan §15's High-impact risk needs an alarm to be writable against it,
        // which needs the series to exist even at zero.
        let store = StoreStats {
            oldest_reader_age_ms: 4_200,
            queue_wait_us: 1_500_000,
            commit_us: 500_000,
            ..StoreStats::default()
        };
        let rendered = render_prometheus(
            &ServerMetrics::default(),
            &store,
            &ClusterMetrics::default(),
            0,
        );
        assert!(
            rendered.contains("vash_oldest_reader_age_ms 4200"),
            "{rendered}"
        );
        assert!(rendered.contains("vash_writer_queue_wait_seconds_total 1.500000"));
        assert!(rendered.contains("vash_writer_commit_seconds_total 0.500000"));
    }
}
