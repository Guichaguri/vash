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
    /// Server also speaks the memcached protocol.
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
    SetMany(Vec<Set<'a>>),
    Delete {
        key: Key<'a>,
    },
    DeleteMany(Vec<Key<'a>>),
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
    /// Atomic numeric add or subtract (memcached `incr`/`decr`).
    ///
    /// Operates on the decimal text of the value, because that is what the
    /// memcached protocol defines and what clients round-trip.
    Incr {
        key: Key<'a>,
        delta: u64,
        decrement: bool,
    },

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
    StoredMany(Vec<u64>),
    Deleted,
    /// `true` where the key was live before the delete.
    DeletedMany(Vec<bool>),
    Touched,
    /// A tag was invalidated. `false` means the tag was never registered, so
    /// nothing could have referenced it.
    Invalidated(bool),
    /// The cache was emptied, carrying the new flush epoch.
    Flushed(u32),
    /// New value of a counter after `incr`/`decr`.
    Counter(u64),
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

impl Value {
    /// Remaining lifetime in seconds, in the form memcached's `t` flag uses:
    /// `-1` for an item that never expires, never negative otherwise (an
    /// expired item would not have been returned), and `None` when the
    /// transport did not report an expiry at all.
    pub fn remaining_ttl_secs(&self, now_ms: u64) -> Option<i64> {
        match self.expires_at_ms {
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
}
