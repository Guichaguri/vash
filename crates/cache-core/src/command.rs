//! The boundary type between protocol adapters and the storage engine.
//!
//! Both KCP and the memcached protocols decode into [`Command`], and both
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
}

/// The KCP protocol version this build implements.
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
}

#[derive(Debug, Clone)]
pub struct Set<'a> {
    pub key: Key<'a>,
    pub value: &'a [u8],
    /// Relative TTL in seconds; 0 means no expiry.
    pub ttl_secs: u32,
    /// Memcached client flags, stored verbatim so a value written over KCP and
    /// read over the memcached protocol round-trips.
    pub mc_flags: u32,
    /// Tag names. Empty for untagged writes, which costs no allocation.
    pub tags: Vec<&'a [u8]>,
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
    Stored {
        cas: u64,
    },
    StoredMany(Vec<u64>),
    Deleted,
    /// `true` where the key was live before the delete.
    DeletedMany(Vec<bool>),
    Touched,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerInfo {
    pub protocol_version: u16,
    pub shards: u16,
    pub max_key_len: u32,
    pub max_value_len: u32,
    pub capabilities: u32,
}
