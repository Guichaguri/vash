//! Request-level counters, and Prometheus rendering.
//!
//! Rendered straight from atomics rather than through a metrics facade. With
//! this few instrumentation points a global recorder would be more machinery
//! than it saves, and the exposition format stays explicit — which matters,
//! because a dashboard is only as trustworthy as the meaning of its series.
//!
//! Nothing here reports a value it does not measure. A counter that is always
//! zero because the feature does not exist is worse than an absent one: it
//! looks like healthy silence.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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

    fn snapshot(&self) -> Buckets {
        Buckets {
            buckets: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            sum_us: self.sum_us.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        render_histogram(out, name, labels, &self.snapshot());
    }
}

/// One histogram's cells, already read.
///
/// The exposition takes this rather than the live cells because the striped
/// command histograms have to be summed across lanes before they can be
/// rendered, and a second copy of the cumulative-bucket arithmetic is the last
/// thing this file needs. See [`render_histogram`].
#[derive(Debug, Default, Clone, Copy)]
struct Buckets {
    buckets: [u64; LATENCY_BOUNDS_US.len() + 1],
    sum_us: u64,
    count: u64,
}

impl Buckets {
    fn merge(&mut self, other: &Buckets) {
        for (mine, theirs) in self.buckets.iter_mut().zip(other.buckets) {
            *mine += theirs;
        }
        self.sum_us += other.sum_us;
        self.count += other.count;
    }
}

/// Renders one Prometheus histogram family, with `labels` on every series.
///
/// Prometheus buckets are **cumulative**, so each `le` line carries the
/// running total rather than the bucket's own count — getting that wrong
/// produces a histogram that renders but quantiles wrongly, which is the
/// kind of error a dashboard hides rather than shows.
fn render_histogram(out: &mut String, name: &str, labels: &str, read: &Buckets) {
    let mut running = 0u64;
    for (index, bound) in LATENCY_BOUNDS_US.iter().enumerate() {
        running += read.buckets[index];
        let seconds = *bound as f64 / 1e6;
        let _ = writeln!(out, "{name}_bucket{{{labels}le=\"{seconds}\"}} {running}");
    }
    running += read.buckets[LATENCY_BOUNDS_US.len()];
    let _ = writeln!(out, "{name}_bucket{{{labels}le=\"+Inf\"}} {running}");
    let _ = writeln!(
        out,
        "{name}_sum{{{}}} {:.6}",
        labels.trim_end_matches(','),
        read.sum_us as f64 / 1e6
    );
    let _ = writeln!(
        out,
        "{name}_count{{{}}} {}",
        labels.trim_end_matches(','),
        read.count
    );
}

// ---- striping --------------------------------------------------------------
//
// Every counter below this line is written once per command by every thread
// that serves one. Kept as plain shared `AtomicU64`s, that made a `GET` pay
// seven locked read-modify-writes across three cache lines — and measured, the
// cost *rose* with core count in aggregate: 41.8 ns on one thread, 89.5 ns on
// eight, so adding cores made the instrumentation slower system-wide than it
// had been. `readers.rs` in `vash-store` already answers this shape, and this
// is the same answer. See `docs/performance-proposals.md` §19.

/// Threads that get a private copy of the per-command counters.
///
/// Sized above the default blocking pool (`server.max_blocking_threads`, 128)
/// plus a runtime worker per core, because those are the threads that reach a
/// counter. Past it, threads share [`SHARED_LANE`] and pay a locked add for it —
/// slower, never wrong.
pub(crate) const LANES: usize = 256;

/// The lane threads fall back to once every private one is taken.
///
/// Reserved rather than handed out, so no thread ever *owns* a lane that a
/// latecomer might also be writing. That is the whole correctness condition for
/// [`bump`]: an owned lane has exactly one writer.
const SHARED_LANE: usize = LANES - 1;

/// Which stripe a thread writes, and whether it has that stripe to itself.
///
/// Shared with [`crate::introspect`], which stripes its per-thread rings and
/// summaries the same way and for the same reason. One lane allocator rather
/// than two: the reclaim behaviour below — a lane handed back when its thread
/// exits — is the part that is easy to leave out and expensive to discover
/// missing, and it should exist once.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Lane {
    pub(crate) index: usize,
    owned: bool,
}

/// The lane a thread with nowhere private to write uses.
const FALLBACK: Lane = Lane {
    index: SHARED_LANE,
    owned: false,
};

/// Private lanes handed out so far. Touched once in a thread's life.
static NEXT_LANE: AtomicUsize = AtomicUsize::new(0);

/// Lanes given back by threads that have exited, for the next one to claim.
///
/// **Without this the win decays.** Tokio retires an idle blocking thread after
/// ten seconds, so a server under bursty load churns through them; a scheme that
/// never reclaimed a lane would hand out all 255 over a few hours and then
/// quietly put every thread after that on the shared one. A mutex is ample for
/// something touched twice in a thread's life.
static FREED_LANES: std::sync::Mutex<Vec<usize>> = std::sync::Mutex::new(Vec::new());

/// Holds a lane for the life of a thread and gives it back on the way out.
struct LaneGuard(Lane);

impl LaneGuard {
    fn claim() -> Self {
        if let Some(index) = FREED_LANES.lock().ok().and_then(|mut free| free.pop()) {
            return Self(Lane { index, owned: true });
        }
        let claimed = NEXT_LANE.fetch_add(1, Ordering::Relaxed);
        Self(if claimed < SHARED_LANE {
            Lane {
                index: claimed,
                owned: true,
            }
        } else {
            // Pinned rather than left to climb, so a very long uptime cannot
            // wrap the counter. Every thread past the limit shares one lane.
            NEXT_LANE.store(SHARED_LANE, Ordering::Relaxed);
            FALLBACK
        })
    }
}

/// How many lanes have been handed back, ever.
///
/// Test-only, and it exists because the free list's *length* stopped being a
/// sound thing to assert on: `crate::introspect` stripes its rings and
/// summaries over this same allocator, so any test thread may pop a freed lane
/// while another test is looking at the list. A monotonic count cannot be
/// disturbed that way — a concurrent claim removes an entry from the list but
/// never un-returns one.
#[cfg(test)]
static LANES_RETURNED: AtomicU64 = AtomicU64::new(0);

impl Drop for LaneGuard {
    fn drop(&mut self) {
        if self.0.owned
            && let Ok(mut free) = FREED_LANES.lock()
        {
            free.push(self.0.index);
            #[cfg(test)]
            LANES_RETURNED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

thread_local! {
    static LANE: LaneGuard = LaneGuard::claim();
}

/// This thread's lane.
///
/// Falls back to the shared one while thread-locals are being destroyed, which
/// is the one moment `with` cannot hand anything back. A count taken there is
/// still counted, just not privately.
#[inline]
pub(crate) fn lane() -> Lane {
    LANE.try_with(|guard| guard.0).unwrap_or(FALLBACK)
}

/// Adds `n` to one thread's cell.
///
/// **On an owned lane this is deliberately not a `fetch_add`.** Nothing else
/// writes the cell, so nothing can interleave between the load and the store,
/// and dropping the lock prefix is most of what this arrangement buys: measured,
/// seven of these cost 3.0 ns against 41.8 ns for seven shared `fetch_add`s on
/// one thread, and 0.9 ns against 89.5 ns on eight.
///
/// A scraper summing the lanes sees exactly the staleness a relaxed `fetch_add`
/// already gave it — these counters were never a consistent snapshot across
/// series, and nothing reads them expecting one.
#[inline]
fn bump(cell: &AtomicU64, n: u64, owned: bool) {
    if owned {
        cell.store(
            cell.load(Ordering::Relaxed).wrapping_add(n),
            Ordering::Relaxed,
        );
    } else {
        cell.fetch_add(n, Ordering::Relaxed);
    }
}

/// One thread's copy of the four counters every command touches.
///
/// Cache-line aligned, which is the other half of §19: these were five adjacent
/// `AtomicU64`s in one struct — one cache line, written by every core on every
/// command.
#[repr(align(64))]
#[derive(Debug, Default)]
struct RateLane {
    commands_total: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// The command-rate counters, one lane per thread.
#[derive(Debug)]
pub struct Rates {
    lanes: Box<[RateLane]>,
}

impl Default for Rates {
    fn default() -> Self {
        Self {
            lanes: (0..LANES).map(|_| RateLane::default()).collect(),
        }
    }
}

impl Rates {
    fn sum(&self, pick: impl Fn(&RateLane) -> &AtomicU64) -> u64 {
        self.lanes
            .iter()
            .map(|lane| pick(lane).load(Ordering::Relaxed))
            .sum()
    }

    pub fn commands_total(&self) -> u64 {
        self.sum(|lane| &lane.commands_total)
    }

    pub fn reads(&self) -> u64 {
        self.sum(|lane| &lane.reads)
    }

    pub fn writes(&self) -> u64 {
        self.sum(|lane| &lane.writes)
    }

    pub fn hits(&self) -> u64 {
        self.sum(|lane| &lane.hits)
    }

    pub fn misses(&self) -> u64 {
        self.sum(|lane| &lane.misses)
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

    pub fn name(self) -> &'static str {
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

    pub fn name(self) -> &'static str {
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
/// One thread's copy of the per-command counters and latency histograms.
///
/// Cache-line aligned for the reason in [`RateLane`], and one allocation rather
/// than one per counter so a thread's cells sit together.
#[repr(align(64))]
#[derive(Debug)]
struct CommandLane {
    /// Indexed `kind * Dialect::ALL.len() + dialect`, as before.
    counts: [AtomicU64; CommandKind::ALL.len() * Dialect::ALL.len()],
    latency: [LatencyCells; CommandKind::ALL.len()],
}

impl Default for CommandLane {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            latency: std::array::from_fn(|_| LatencyCells::default()),
        }
    }
}

/// One histogram's live cells, for one thread.
#[derive(Debug, Default)]
struct LatencyCells {
    buckets: [AtomicU64; LATENCY_BOUNDS_US.len() + 1],
    sum_us: AtomicU64,
    count: AtomicU64,
}

#[derive(Debug)]
pub struct CommandMetrics {
    lanes: Box<[CommandLane]>,
}

impl Default for CommandMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandMetrics {
    fn new() -> Self {
        Self {
            lanes: (0..LANES).map(|_| CommandLane::default()).collect(),
        }
    }

    #[inline]
    pub fn observe(&self, kind: CommandKind, dialect: Dialect, elapsed: std::time::Duration) {
        let lane = lane();
        let mine = &self.lanes[lane.index];
        let index = kind.index();
        bump(
            &mine.counts[index * Dialect::ALL.len() + dialect as usize],
            1,
            lane.owned,
        );

        // The bucket search that `Histogram::observe` does, against this
        // thread's cells rather than the shared ones.
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = LATENCY_BOUNDS_US
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(LATENCY_BOUNDS_US.len());
        let cells = &mine.latency[index];
        bump(&cells.buckets[bucket], 1, lane.owned);
        bump(&cells.sum_us, micros, lane.owned);
        bump(&cells.count, 1, lane.owned);
    }

    /// This command's count on one dialect, across every lane.
    fn count_of(&self, kind: CommandKind, dialect: Dialect) -> u64 {
        let slot = kind.index() * Dialect::ALL.len() + dialect as usize;
        self.lanes
            .iter()
            .map(|lane| lane.counts[slot].load(Ordering::Relaxed))
            .sum()
    }

    /// This command's latency histogram, across every lane.
    fn latency_of(&self, kind: CommandKind) -> Buckets {
        let index = kind.index();
        let mut merged = Buckets::default();
        for lane in &self.lanes {
            let cells = &lane.latency[index];
            merged.merge(&Buckets {
                buckets: std::array::from_fn(|b| cells.buckets[b].load(Ordering::Relaxed)),
                sum_us: cells.sum_us.load(Ordering::Relaxed),
                count: cells.count.load(Ordering::Relaxed),
            });
        }
        merged
    }

    /// How many times this command ran, across every dialect.
    ///
    /// What `stats` and `INFO` report: both describe the server, not the port a
    /// request happened to arrive on. The per-dialect split stays available to
    /// Prometheus, which is where the question "who is sending this" belongs.
    pub fn total(&self, kind: CommandKind) -> u64 {
        Dialect::ALL
            .into_iter()
            .map(|dialect| self.count_of(kind, dialect))
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
                let value = self.count_of(kind, dialect);
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
            render_histogram(
                out,
                "vash_command_seconds",
                &format!("command=\"{}\",", kind.name()),
                &self.latency_of(kind),
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
    /// Handshakes that completed, and connections currently holding one.
    ///
    /// `tls_connections_active` against `connections_active` is the rollout's
    /// progress bar: an operator may close the plaintext port when the two
    /// agree. Kept beside the connection counters because that comparison is
    /// the only reason either exists.
    pub tls_handshakes: AtomicU64,
    pub tls_connections_active: AtomicU64,
    /// Handshakes that did not complete, split by which way they failed.
    ///
    /// Two counters rather than one, because they are two different operator
    /// actions: `rejected` is almost always a certificate or CA mismatch and
    /// is fixed on the client, while `timeout` is a peer that opened a socket
    /// and did not finish — a network problem, or somebody probing the port.
    pub tls_handshake_rejected: AtomicU64,
    pub tls_handshake_timeouts: AtomicU64,
    /// Certificates that verified against the CA and matched no credential
    /// row.
    ///
    /// A third reason, and a different one from the two above: the
    /// cryptography succeeded and the *authorisation* did not. It is what an
    /// operator sees when a new service has been issued a certificate but not
    /// yet added to the table, which is the ordinary rollout mistake.
    pub tls_identity_rejected: AtomicU64,
    /// When the certificate chain in use stops verifying, as a Unix timestamp.
    ///
    /// `0` means it does not verify *now* — expired, not yet valid, or a chain
    /// that does not lead to its own anchor. That is deliberately the same
    /// value as "the epoch": any alert worth writing fires on both.
    pub tls_cert_expiry: AtomicU64,
    /// How long completed handshakes took.
    pub tls_handshake_latency: Histogram,

    /// Successful authentications, failed ones, and commands refused for want
    /// of one.
    ///
    /// A failure rate that is not zero is the alert worth having. So, on a
    /// server with authentication required, is a sudden *drop* to zero: it
    /// means nothing is reaching the gate any more.
    pub auth_ok: AtomicU64,
    pub auth_failed: AtomicU64,
    pub auth_refused: AtomicU64,
    /// Commands refused because the identity lacked a role, one counter per
    /// role.
    ///
    /// **Not folded into `auth_refused`.** That one counts commands refused for
    /// want of *authentication*, and an alert that cannot tell "nobody rolled
    /// their credential" from "someone's role set is wrong" is an alert that
    /// gets muted. Split by role rather than by identity because the
    /// cardinality is fixed at six, where an identity label is unbounded.
    ///
    /// A command missing two roles counts once under each, which is what makes
    /// the sum answer "how much authority is being asked for and refused"
    /// rather than "how many commands failed".
    pub auth_denied: [AtomicU64; crate::auth::Roles::COUNT],
    /// Connections dropped for not authenticating inside `auth.timeout_ms`, and
    /// refused because the unauthenticated-connection budget was full.
    pub auth_timeouts: AtomicU64,
    pub auth_capacity_rejected: AtomicU64,

    /// Command rates and the hit/miss split, **striped per thread**.
    ///
    /// These five were adjacent `AtomicU64`s written by every core on every
    /// command, which is one cache line and three to four locked adds per
    /// request. See the striping block above and §19.
    pub rates: Rates,

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
            tls_handshakes: AtomicU64::new(0),
            tls_connections_active: AtomicU64::new(0),
            tls_handshake_rejected: AtomicU64::new(0),
            tls_handshake_timeouts: AtomicU64::new(0),
            tls_identity_rejected: AtomicU64::new(0),
            tls_cert_expiry: AtomicU64::new(0),
            tls_handshake_latency: Histogram::default(),
            auth_ok: AtomicU64::new(0),
            auth_failed: AtomicU64::new(0),
            auth_refused: AtomicU64::new(0),
            auth_denied: std::array::from_fn(|_| AtomicU64::new(0)),
            auth_timeouts: AtomicU64::new(0),
            auth_capacity_rejected: AtomicU64::new(0),
            rates: Rates::default(),
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

    /// A handshake completed. The connection is encrypted from here.
    pub fn tls_established(&self, elapsed: std::time::Duration) {
        self.tls_handshakes.fetch_add(1, Ordering::Relaxed);
        self.tls_connections_active.fetch_add(1, Ordering::Relaxed);
        self.tls_handshake_latency.observe(elapsed);
    }

    /// An encrypted connection closed. Only called for one that got as far as
    /// completing its handshake, so this can never take the gauge negative.
    pub fn tls_closed(&self) {
        self.tls_connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn tls_handshake_rejected(&self) {
        self.tls_handshake_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn tls_handshake_timeout(&self) {
        self.tls_handshake_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn tls_identity_rejected(&self) {
        self.tls_identity_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Records when the certificate now in use expires. Called at startup and
    /// on every reload, never on a request path.
    pub fn tls_cert_expires_at(&self, unix_seconds: u64) {
        self.tls_cert_expiry.store(unix_seconds, Ordering::Relaxed);
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

    /// Counts a denial under every role that was missing.
    pub fn auth_denied(&self, missing: crate::auth::Roles) {
        for (index, _, role) in crate::auth::Roles::INDEXED {
            if missing.allows(role) {
                self.auth_denied[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn auth_timeout(&self) {
        self.auth_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_capacity_rejected(&self) {
        self.auth_capacity_rejected.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn read(&self, hits: u64, misses: u64) {
        let lane = lane();
        let mine = &self.rates.lanes[lane.index];
        bump(&mine.commands_total, 1, lane.owned);
        bump(&mine.reads, 1, lane.owned);
        // Still guarded: a `GET_MANY` that hit nothing should not write the hit
        // counter's cache line at all, and on the shared lane that guard is the
        // difference between a locked add and none.
        if hits > 0 {
            bump(&mine.hits, hits, lane.owned);
        }
        if misses > 0 {
            bump(&mine.misses, misses, lane.owned);
        }
    }

    #[inline]
    pub fn write(&self) {
        let lane = lane();
        let mine = &self.rates.lanes[lane.index];
        bump(&mine.commands_total, 1, lane.owned);
        bump(&mine.writes, 1, lane.owned);
    }

    #[inline]
    pub fn other(&self) {
        let lane = lane();
        bump(&self.rates.lanes[lane.index].commands_total, 1, lane.owned);
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
        "vash_tls_handshakes_total",
        "counter",
        "TLS handshakes completed.",
        load(&server.tls_handshakes).to_string(),
    );
    metric!(
        "vash_tls_connections_active",
        "gauge",
        "Encrypted connections currently open. Equal to vash_connections_active \
         when nothing is arriving in the clear, which is what makes closing the \
         plaintext port safe.",
        load(&server.tls_connections_active).to_string(),
    );

    // Split by reason, because the two are different operator actions: a
    // rejection is a certificate the client would not accept, a timeout is a
    // peer that never finished.
    let _ = writeln!(
        out,
        "# HELP vash_tls_handshake_failures_total Handshakes that did not complete, by reason."
    );
    let _ = writeln!(out, "# TYPE vash_tls_handshake_failures_total counter");
    for (reason, value) in [
        ("rejected", load(&server.tls_handshake_rejected)),
        ("timeout", load(&server.tls_handshake_timeouts)),
        ("no_identity", load(&server.tls_identity_rejected)),
    ] {
        let _ = writeln!(
            out,
            "vash_tls_handshake_failures_total{{reason=\"{reason}\"}} {value}"
        );
    }

    // The family declares its own type and help once; `render` emits only the
    // series beneath it, the same arrangement `vash_command_seconds` uses.
    let _ = writeln!(
        out,
        "# HELP vash_tls_handshake_seconds Time to complete a TLS handshake, \
         which is the most expensive thing an unauthenticated peer can ask for."
    );
    let _ = writeln!(out, "# TYPE vash_tls_handshake_seconds histogram");
    server
        .tls_handshake_latency
        .render(&mut out, "vash_tls_handshake_seconds", "");

    // A timestamp rather than a remaining duration, which is the convention
    // every other exporter of this follows — the alert is written as
    // `vash_tls_cert_expiry_timestamp_seconds - time() < 7 * 86400`, and a
    // gauge that counted down would go stale between scrapes instead.
    metric!(
        "vash_tls_cert_expiry_timestamp_seconds",
        "gauge",
        "When the certificate chain in use stops verifying. 0 means it does \
         not verify now — expired, not yet valid, or an incomplete chain.",
        load(&server.tls_cert_expiry).to_string(),
    );

    metric!(
        "vash_auth_ok_total",
        "counter",
        "Successful authentications.",
        load(&server.auth_ok).to_string(),
    );
    metric!(
        "vash_auth_failed_total",
        "counter",
        "Rejected credentials.",
        load(&server.auth_failed).to_string(),
    );
    metric!(
        "vash_auth_refused_total",
        "counter",
        "Commands refused because the connection had not authenticated.",
        load(&server.auth_refused).to_string(),
    );

    // Split by role, and kept apart from `vash_auth_refused_total`: that one is
    // "nobody rolled their credential", this one is "someone's role set is
    // wrong", and an alert that cannot tell them apart gets muted. A command
    // missing two roles counts once under each.
    let _ = writeln!(
        out,
        "# HELP vash_auth_denied_total Commands refused because the identity lacked a role."
    );
    let _ = writeln!(out, "# TYPE vash_auth_denied_total counter");
    for (index, role, _) in crate::auth::Roles::INDEXED {
        let _ = writeln!(
            out,
            "vash_auth_denied_total{{role=\"{role}\"}} {}",
            load(&server.auth_denied[index])
        );
    }

    metric!(
        "vash_commands_total",
        "counter",
        "Commands executed.",
        server.rates.commands_total().to_string(),
    );
    metric!(
        "vash_reads_total",
        "counter",
        "Read commands executed.",
        server.rates.reads().to_string(),
    );
    metric!(
        "vash_writes_total",
        "counter",
        "Write commands executed.",
        server.rates.writes().to_string(),
    );
    metric!(
        "vash_hits_total",
        "counter",
        "Keys served from cache. Counted per key, so a multi-get adds several.",
        server.rates.hits().to_string(),
    );
    metric!(
        "vash_misses_total",
        "counter",
        "Keys looked up and not found live.",
        server.rates.misses().to_string(),
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
mod striping_tests {
    use super::*;

    /// **Striping must not lose a count.** An owned lane is updated with a plain
    /// load-add-store rather than a `fetch_add`, which is sound only while
    /// exactly one thread writes it — so this is the test that the lane
    /// bookkeeping actually delivers that, rather than the comment claiming it.
    #[test]
    fn every_count_survives_being_spread_across_lanes() {
        const THREADS: usize = 16;
        const EACH: u64 = 20_000;

        let metrics = std::sync::Arc::new(ServerMetrics::default());
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let metrics = std::sync::Arc::clone(&metrics);
                scope.spawn(move || {
                    for _ in 0..EACH {
                        metrics.read(1, 1);
                        metrics.write();
                        metrics.commands.observe(
                            CommandKind::Get,
                            Dialect::Resp,
                            std::time::Duration::from_micros(7),
                        );
                    }
                });
            }
        });

        let total = THREADS as u64 * EACH;
        assert_eq!(metrics.rates.reads(), total);
        assert_eq!(metrics.rates.writes(), total);
        assert_eq!(metrics.rates.hits(), total);
        assert_eq!(metrics.rates.misses(), total);
        // `read` and `write` each count one command.
        assert_eq!(metrics.rates.commands_total(), total * 2);
        assert_eq!(metrics.commands.total(CommandKind::Get), total);

        let latency = metrics.commands.latency_of(CommandKind::Get);
        assert_eq!(latency.count, total);
        assert_eq!(latency.sum_us, total * 7);
        assert_eq!(
            latency.buckets.iter().sum::<u64>(),
            total,
            "every observation lands in exactly one bucket"
        );
    }

    /// The shared lane is the one place two threads write the same cells, so it
    /// is the one place the plain store would lose counts. It must use
    /// `fetch_add` — driven here directly, because exhausting 255 real lanes in
    /// a test would be slower than it is worth.
    #[test]
    fn the_shared_lane_does_not_lose_counts() {
        const THREADS: usize = 8;
        const EACH: u64 = 50_000;

        let cell = std::sync::Arc::new(AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let cell = std::sync::Arc::clone(&cell);
                scope.spawn(move || {
                    for _ in 0..EACH {
                        bump(&cell, 1, FALLBACK.owned);
                    }
                });
            }
        });
        assert_eq!(cell.load(Ordering::Relaxed), THREADS as u64 * EACH);
    }

    /// A lane comes back when its thread exits, so a server that churns through
    /// blocking threads does not quietly exhaust them and fall back to the
    /// shared lane for the rest of its uptime.
    #[test]
    fn a_lane_is_returned_when_its_thread_exits() {
        const THREADS: u64 = 4;

        // Counted rather than measured off the free list's length. `introspect`
        // stripes over this allocator too, so any other test's thread may pop a
        // freed lane between two reads of that list — which it did, and this
        // test failed the day the second consumer arrived.
        let before = LANES_RETURNED.load(Ordering::Relaxed);
        for _ in 0..THREADS {
            std::thread::spawn(|| {
                let claimed = lane();
                assert!(claimed.owned, "a lone thread should get a private lane");
            })
            .join()
            .expect("thread");
        }
        let after = LANES_RETURNED.load(Ordering::Relaxed);
        assert!(
            after >= before + THREADS,
            "lanes are not being returned: {before} before, {after} after"
        );
    }

    /// Rendering reads the lanes the same way the `stats` surfaces do, so a
    /// counter that summed correctly for one and not the other would be a
    /// dashboard that disagrees with `INFO`.
    #[test]
    fn the_exposition_sums_the_same_lanes() {
        let metrics = ServerMetrics::default();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        metrics.commands.observe(
                            CommandKind::Get,
                            Dialect::Resp,
                            std::time::Duration::from_micros(3),
                        );
                    }
                });
            }
        });

        let mut rendered = String::new();
        metrics.commands.render(&mut rendered);
        assert!(
            rendered.contains("vash_command_total{command=\"get\",dialect=\"resp\"} 4000"),
            "{rendered}"
        );
        assert!(
            rendered.contains("vash_command_seconds_count{command=\"get\"} 4000"),
            "{rendered}"
        );
    }
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
    ///
    /// `Command::GetMany` is held to the same rule, because a plain memcached
    /// `get` naming one key is fused as well and arrives as one — see the
    /// retrieval branch of `dispatch::execute_memcached`. A `GetMany` arm here
    /// would be lost for exactly the single-key reads that matter most, and
    /// kept for the multi-key ones, which is the worst of both.
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
            .observe(&Command::Get { key }, &Reply::Value(value.clone()));
        metrics
            .outcomes
            .observe(&Command::Get { key }, &Reply::NotFound);

        // The single-key `get` the memcached path fuses, in both outcomes.
        let one = Command::GetMany(vash_core::KeyList::from(key));
        metrics
            .outcomes
            .observe(&one, &Reply::Values(vec![Some(value)]));
        metrics.outcomes.observe(&one, &Reply::Values(vec![None]));

        assert_eq!(
            before,
            rendered(&metrics),
            "observe now counts something for a GET or a one-key GET_MANY, \
             which the fused read path in dispatch::execute_get_into bypasses \
             -- that counting would be silently lost. Either count it in \
             execute_get_into too, or take that command off the fused path."
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
