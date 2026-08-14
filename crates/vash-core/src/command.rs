//! The boundary type between protocol adapters and the storage engine.
//!
//! Both VCP and the memcached protocols decode into [`Command`], and both
//! encode from [`Reply`]. The storage engine therefore has no knowledge of wire
//! formats, and the protocol crates have no knowledge of LMDB. Adding a third
//! protocol means adding a decoder, and nothing else.

use bytes::Bytes;

use crate::key::Key;

/// Capability bits advertised in the [`ServerInfo`] handshake.
pub mod capability {
    /// Server understands tags and `DELETE_BY_TAG`.
    pub const TAGS: u32 = 1 << 0;
    /// The memcached text and meta protocols are served on this port.
    ///
    /// Enablement, not support — the same contract as [`CLUSTER`]. A dialect
    /// turned off in configuration is not merely refused: a connection opening
    /// with it is closed at first-byte detection, before any parser sees it, so
    /// a client that ignores this bit sees a bare disconnect and no error.
    pub const MEMCACHED: u32 = 1 << 1;
    /// Server participates in cluster-wide tag invalidation.
    pub const CLUSTER: u32 = 1 << 2;
    /// `LIST_KEYS`/`LIST_TAGS` are enabled here.
    ///
    /// Enablement, not support — the same contract as [`CLUSTER`]. They are off
    /// by default, so a client reads a clear bit as "not here" rather than
    /// having to tell an `UNAUTHORIZED` apart from an older build.
    pub const LISTING: u32 = 1 << 3;
    /// This connection must send `AUTH` before any other command.
    ///
    /// Set only when authentication is being enforced, not merely because the
    /// build supports it — the same contract as [`CLUSTER`]. `HELLO` stays
    /// legal before `AUTH` because first-byte detection requires a VCP
    /// connection to open with it, so this bit is how a client discovers it
    /// must authenticate rather than guessing from a refusal.
    pub const AUTH_REQUIRED: u32 = 1 << 4;
    /// `FLUSH` is enabled here.
    ///
    /// Enablement, not support, and for the same reason [`LISTING`] has a bit:
    /// `FLUSH` is off by default and answers `UNAUTHORIZED` when disabled, so
    /// without this a client cannot tell a server that refuses to flush from
    /// one too old to know the opcode — and the only way to find out would be
    /// to try wiping the cache.
    pub const FLUSH: u32 = 1 << 5;
    /// The Redis protocol (RESP2 and RESP3) is served on this port.
    ///
    /// Enablement, not support, and closed at detection when clear — the same
    /// contract as [`MEMCACHED`].
    pub const RESP: u32 = 1 << 6;
}

/// The VCP protocol version this build implements.
pub const PROTOCOL_VERSION: u16 = 1;

/// Upper bound on items in a single batch request.
///
/// Bounds both the work one frame can demand and the size of the write
/// transaction a batch turns into, so a client cannot stall the shard writer
/// with one enormous `SET_MANY`.
pub const MAX_BATCH_ITEMS: usize = 4096;

/// A decoded request, borrowing from the connection's read buffer.
#[derive(Debug, Clone)]
pub enum Command<'a> {
    Hello {
        protocol_version: u16,
    },
    Ping,
    Get {
        key: Key<'a>,
    },
    GetMany(Vec<Key<'a>>),
    Set(Set<'a>),
    SetMany {
        sets: Vec<Set<'a>>,
        /// Applies to the batch as a whole (Redis `MSETEX NX`/`XX`). The other
        /// dialects send [`BatchGuard::Always`], which is why this is one
        /// command rather than two.
        guard: BatchGuard,
    },
    Delete {
        key: Key<'a>,
    },
    DeleteMany(Vec<Key<'a>>),
    /// A live key's deadline, without copying its value.
    ///
    /// What every command that asks *about* a key rather than for it is made of
    /// — Redis's `TTL`, `TYPE` and `EXISTS` all reduce to this, differing only
    /// in how they render the answer.
    Deadline {
        key: Key<'a>,
    },
    Deadlines(Vec<Key<'a>>),
    /// Change a key's deadline under a guard (Redis `EXPIRE`/`PERSIST`).
    Expire {
        key: Key<'a>,
        ttl: crate::arith::TtlChange,
        guard: ExpireGuard,
    },
    /// Concatenate onto a value, creating it if absent (Redis `APPEND`).
    ///
    /// memcached's `append` is a *conditional* write instead — it refuses an
    /// absent key — and travels as [`Command::Set`] with [`SetMode::Append`].
    Append {
        key: Key<'a>,
        suffix: &'a [u8],
    },
    /// Extends (or clears, with `ttl_secs` of 0) a key's lifetime without
    /// resending its value.
    Touch {
        key: Key<'a>,
        ttl_secs: u32,
    },
    /// Invalidates every record carrying the tag, in constant time regardless
    /// of how many keys that is.
    DeleteByTag {
        tag: &'a [u8],
    },
    /// Empties the cache.
    Flush,

    /// Fetch several keys and re-stamp their TTL in one pass (memcached `gat`).
    GetAndTouch {
        keys: Vec<Key<'a>>,
        ttl_secs: u32,
    },
    /// Atomic read-modify-write on a counter.
    ///
    /// One variant for every dialect's arithmetic: memcached's `incr`/`decr` and
    /// Redis's `INCR` family alike, which differ in their numeric domain and in
    /// nothing else. See [`crate::arith`].
    Arithmetic(crate::arith::Arithmetic<'a>),

    /// Cluster: merge tag generations a peer reported.
    ///
    /// Not a client command. Generations merge by maximum, so this is
    /// idempotent and order-independent — see [`crate::cluster`].
    TagSync {
        /// The sender listed its **whole** table, so the receiver may answer
        /// with entries the sender never mentioned. A partial push gets a reply
        /// covering only the names it named.
        full: bool,
        entries: Vec<(&'a [u8], u64)>,
    },
    /// Cluster: this node's view of its peer list.
    Cluster,

    /// Administrative: one page of the keys that are currently live.
    ListKeys(crate::listing::ListRequest<'a>),
    /// Administrative: one page of the tag registry.
    ListTags(crate::listing::ListRequest<'a>),

    /// Protocol-level commands with no storage effect.
    Stats,
    Version,
    Quit,
}

impl Command<'_> {
    /// Whether an async runtime worker may execute this itself.
    ///
    /// **A safety predicate, not an optimisation hint.** A command that answers
    /// `true` here may be run on a runtime worker instead of the blocking pool,
    /// so answering `true` wrongly stalls that worker and every other connection
    /// it serves — the failure the whole network/storage split exists to
    /// prevent. When in doubt, answer `false`: the cost of being wrong that way
    /// is one thread hop.
    ///
    /// Two different things disqualify a command, which is why this is not
    /// simply "does it write":
    ///
    /// 1. **It can reach the shard writer.** `GetAndTouch` re-stamps a TTL and
    ///    `Arithmetic` rewrites its value, so both are writes despite reading
    ///    like retrievals; `TagSync` durably merges generations a peer reported.
    /// 2. **It can hold the thread for a long time.** The listings never write a
    ///    byte, and are still refused: a scan is bounded by `listing_max_scan`
    ///    records, not by anything a worker should be blocked for. This is the
    ///    case that makes the name of this method "inline safe" rather than
    ///    "read only".
    pub fn inline_safe(&self) -> bool {
        match self {
            // No storage effect at all.
            Self::Hello { .. } | Self::Ping | Self::Stats | Self::Version | Self::Quit => true,
            Self::Cluster => true,
            // Short reads.
            Self::Get { .. } | Self::GetMany(_) | Self::Deadline { .. } | Self::Deadlines(_) => {
                true
            }
            // Reads, but long ones. See (2) above.
            Self::ListKeys(_) | Self::ListTags(_) => false,
            // Writes. Listed rather than caught by a wildcard so that a new
            // command has to be classified deliberately: a variant that falls
            // through to `false` by accident is merely slow, but one that falls
            // through to `true` stalls a runtime worker.
            Self::Set(_)
            | Self::SetMany { .. }
            | Self::Delete { .. }
            | Self::DeleteMany(_)
            | Self::Touch { .. }
            | Self::Expire { .. }
            | Self::Append { .. }
            | Self::Arithmetic(_)
            | Self::GetAndTouch { .. }
            | Self::DeleteByTag { .. }
            | Self::Flush
            | Self::TagSync { .. } => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Set<'a> {
    pub key: Key<'a>,
    pub value: &'a [u8],
    /// What happens to the key's lifetime.
    ///
    /// A [`TtlChange`] rather than a bare `ttl_secs` so that Redis's `KEEPTTL`
    /// can be expressed as *not touching the deadline* rather than as reading it
    /// and writing the same number back. The read-then-write form has a race in
    /// it — a deadline changed in between is overwritten with the older one —
    /// and it costs a lookup the writer was going to do anyway.
    ///
    /// [`TtlChange`]: crate::arith::TtlChange
    pub ttl: crate::arith::TtlChange,
    /// Memcached client flags, stored verbatim so a value written over VCP and
    /// read over the memcached protocol round-trips.
    pub mc_flags: u32,
    /// Tag names. Empty for untagged writes, which costs no allocation.
    pub tags: Vec<&'a [u8]>,
    /// The condition under which the write applies.
    pub mode: SetMode,
    /// Report the value the key held beforehand (Redis `SET … GET`).
    ///
    /// Part of the request rather than a separate command because only the store
    /// can capture it: reading it here, before the write, is the race this field
    /// exists to avoid. Costs a value copy, so it stays off unless asked for.
    pub return_previous: bool,
}

impl<'a> Set<'a> {
    /// An unconditional write with no tags — the common case.
    pub fn plain(key: Key<'a>, value: &'a [u8], ttl_secs: u32) -> Self {
        Self::with_ttl(key, value, crate::arith::TtlChange::Set(ttl_secs))
    }

    /// As [`Set::plain`], for a caller that already has a [`TtlChange`].
    pub fn with_ttl(key: Key<'a>, value: &'a [u8], ttl: crate::arith::TtlChange) -> Self {
        Self {
            key,
            value,
            ttl,
            mc_flags: 0,
            tags: Vec::new(),
            mode: SetMode::Set,
            return_previous: false,
        }
    }
}

/// The guard on a conditional expiry change (`EXPIRE`'s `NX`/`XX`/`GT`/`LT`).
///
/// Evaluated inside the writer's transaction against the deadline the record
/// actually holds, so the guard cannot be decided against a value that has since
/// moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExpireGuard {
    #[default]
    Always,
    /// Only when the key currently has no deadline.
    IfPersistent,
    /// Only when it already has one.
    IfVolatile,
    /// Only when the new deadline is later than the current one. A key with no
    /// deadline is infinitely far off, so this never applies to one.
    IfLater,
    /// The mirror image, where a key with no deadline always loses.
    IfEarlier,
}

/// The guard on a conditional batch write (`MSETEX`'s `NX`/`XX`).
///
/// **Atomic within a shard, and only within a shard.** The keys of one batch are
/// spread across shards by key hash, and a batch spanning shards is several
/// transactions — plan §16's standing non-goal. A guard that has to see every
/// key at once therefore holds exactly when the batch lands in one shard, which
/// includes every single-shard deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BatchGuard {
    #[default]
    Always,
    /// Only when *none* of the keys exist.
    IfAllAbsent,
    /// Only when *all* of them do.
    IfAllPresent,
}

/// When a write is allowed to take effect.
///
/// Modelled as one field rather than one command per variant because they all
/// resolve to the same storage operation under a different guard — which is
/// also why they can share a transaction and a code path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetMode {
    /// Always store.
    #[default]
    Set,
    /// Store only if the key is absent (memcached `add`).
    Add,
    /// Store only if the key is present (memcached `replace`).
    Replace,
    /// Concatenate onto an existing value; no-op if absent. The existing
    /// TTL and client flags are kept, as memcached does.
    Append,
    Prepend,
    /// Store only if the key is present with exactly this CAS token.
    Cas(u64),
}

/// The outcome of a conditional write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stored {
    Stored(u64),
    /// The guard rejected it: `add` on a present key, `replace`/`append`/
    /// `prepend` on an absent one.
    NotStored,
    /// `cas` on a key that exists but has moved on.
    Exists,
    /// `cas` on a key that is not there at all.
    NotFound,
}

/// The outcome of a command, ready to be encoded by whichever adapter received it.
///
/// Batch replies carry one entry per request item, in request order. They have
/// no per-item error variant on purpose: everything that can be rejected per
/// item (key length, value size, tag limits) is rejected while decoding, and a
/// failure at execution time — the map filling up, an LMDB error — fails the
/// whole transaction. So by the time a batch runs, either all of it applies or
/// none of it does.
#[derive(Debug, Clone)]
pub enum Reply {
    Hello(ServerInfo),
    Pong,
    Value(Value),
    /// One slot per requested key; `None` is a miss.
    Values(Vec<Option<Value>>),
    Stored(Stored),
    /// A write that reported what it displaced (Redis `SET … GET`).
    ///
    /// Separate from [`Reply::Stored`] because the client is told what was
    /// there and never whether the write applied — that is Redis's contract,
    /// and folding the two would make every other dialect carry an
    /// `Option<Value>` it never populates.
    Swapped {
        outcome: Stored,
        previous: Option<Value>,
    },
    StoredMany(Vec<u64>),
    /// A guarded change either applied or it did not (Redis `EXPIRE`,
    /// `PERSIST`).
    Applied(bool),
    /// The length a value reached after being concatenated onto.
    Length(u64),
    /// A key's deadline: `None` if it is not live, `Some(NEVER)` if it never
    /// expires.
    Deadline(Option<u64>),
    Deadlines(Vec<Option<u64>>),
    Deleted,
    /// `true` where the key was live before the delete.
    DeletedMany(Vec<bool>),
    Touched,
    /// A tag was invalidated. `false` means the tag was never registered, so
    /// nothing could have referenced it.
    Invalidated(bool),
    /// The cache was emptied, carrying the new flush epoch.
    Flushed(u32),
    /// Where a counter ended up, and how far it moved.
    Arithmetic(crate::arith::Applied),
    /// Answer to a peer's `TAG_SYNC`: the generations this node holds that the
    /// sender is behind on. Empty when the sender was already up to date.
    TagSync(Vec<crate::cluster::TagGeneration>),
    Cluster(crate::cluster::ClusterInfo),
    /// One page of a listing. Shared by both listing opcodes, which is the
    /// point of them having one shape.
    Listing(crate::listing::Listing),
    Stats(Vec<(String, String)>),
    Version(&'static str),
    /// The client asked to hang up.
    Closing,
    NotFound,
}

#[derive(Debug, Clone)]
pub struct Value {
    /// The stored bytes.
    ///
    /// M0 copies these out of the mmap because the read happens on a blocking
    /// pool and the result crosses back to the network task after the read
    /// transaction closes. From M1 the storage thread encodes the response
    /// frame directly while the transaction is still open, reducing this to a
    /// single mmap-to-wire-buffer copy — the same copy any server must make.
    pub data: Bytes,
    pub mc_flags: u32,
    pub cas: u64,
    /// Absolute expiry in unix milliseconds, or [`NEVER`] for no expiry.
    ///
    /// `None` means the transport did not report it. The store always fills it
    /// in — it is what lets the memcached `t` flag give a real number — but the
    /// VCP wire format does not carry expiry on a `GET`, so a value decoded by
    /// a client has `None` rather than a plausible-looking lie.
    ///
    /// [`NEVER`]: crate::record::NEVER
    pub expires_at_ms: Option<u64>,
}

/// A value borrowed where it lies, rather than copied out.
///
/// The exact fields of [`Value`], except that `data` points at storage owned by
/// someone else — for a read that means the memory map, valid only while the
/// transaction that produced it is open. Encoders take this rather than
/// `&Value` so that one rendering serves both a value that was copied out and
/// one that never was; see [`Value::borrowed`] for the conversion that keeps
/// the owned paths working unchanged.
#[derive(Debug, Clone, Copy)]
pub struct ValueRef<'a> {
    pub data: &'a [u8],
    pub mc_flags: u32,
    pub cas: u64,
    /// As [`Value::expires_at_ms`], including its `None`: a borrowed value must
    /// not invent an expiry that the owned one would have declined to report.
    pub expires_at_ms: Option<u64>,
}

impl ValueRef<'_> {
    /// As [`Value::remaining_ttl_secs`].
    pub fn remaining_ttl_secs(&self, now_ms: u64) -> Option<i64> {
        Value::remaining_ttl_from(self.expires_at_ms, now_ms)
    }
}

impl Value {
    /// This value, borrowed, so an owned value can be rendered by the same code
    /// that renders one still sitting in the map.
    pub fn borrowed(&self) -> ValueRef<'_> {
        ValueRef {
            data: &self.data,
            mc_flags: self.mc_flags,
            cas: self.cas,
            expires_at_ms: self.expires_at_ms,
        }
    }

    /// Remaining lifetime in seconds, in the form memcached's `t` flag uses:
    /// `-1` for an item that never expires, never negative otherwise (an
    /// expired item would not have been returned), and `None` when the
    /// transport did not report an expiry at all.
    pub fn remaining_ttl_secs(&self, now_ms: u64) -> Option<i64> {
        Self::remaining_ttl_from(self.expires_at_ms, now_ms)
    }

    /// The rule itself, shared with [`ValueRef`] so the borrowed and owned forms
    /// cannot answer differently.
    fn remaining_ttl_from(expires_at_ms: Option<u64>, now_ms: u64) -> Option<i64> {
        match expires_at_ms {
            None => None,
            Some(crate::record::NEVER) => Some(-1),
            Some(at) => Some(at.saturating_sub(now_ms).div_ceil(1000) as i64),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerInfo {
    pub protocol_version: u16,
    pub shards: u16,
    pub max_key_len: u32,
    pub max_value_len: u32,
    pub capabilities: u32,
    /// Tags one record may carry, from `store.tags.max_per_record`.
    ///
    /// Advertised for the same reason as `max_value_len`: it is configurable,
    /// a breach is refused with a bare `BAD_REQUEST` that carries no detail,
    /// and under `NO_REPLY` there is no refusal to read at all. A `u16` for a
    /// limit the record header caps at [`ABSOLUTE_MAX_TAGS`], so the wire does
    /// not have to change if that header ever grows.
    ///
    /// [`ABSOLUTE_MAX_TAGS`]: crate::ABSOLUTE_MAX_TAGS
    pub max_tags_per_record: u16,
}
