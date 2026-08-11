//! Cluster-facing domain types.
//!
//! Instances are otherwise shared-nothing (plan §10): clients shard the
//! keyspace, no data moves between nodes, and there is no consensus anywhere.
//! The one thing that has to cross a node boundary is **tag invalidation**,
//! because a tag's keys are spread by key hash across every node, so an
//! invalidation that reached only the node the client happened to call would
//! leave the rest being served.
//!
//! What crosses the wire is therefore a [`TagGeneration`] — a name and a
//! counter — and nothing else.

/// A tag name paired with the generation a node holds for it.
///
/// **Names are the global identity; ids are node-local.** A tag id is a dense
/// per-shard counter that exists only to keep the per-record tag table small,
/// and two nodes will happily assign different ids to the same name. Cluster
/// messages therefore carry names, and a node that has never seen a name
/// creates it on receipt.
///
/// Generations merge by **maximum**, which makes them a CRDT: applying the same
/// message twice changes nothing, applying two messages in either order reaches
/// the same value, and a message may be retried freely. That is what lets
/// fan-out be fire-and-forget and anti-entropy be a plain digest exchange, with
/// no acknowledgement protocol and no agreement on membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagGeneration {
    pub name: Box<[u8]>,
    pub generation: u64,
}

impl TagGeneration {
    pub fn new(name: impl Into<Box<[u8]>>, generation: u64) -> Self {
        Self {
            name: name.into(),
            generation,
        }
    }
}

/// Most entries one `TAG_SYNC` message may carry.
///
/// Bounds both the work one frame can demand and the memory a peer can make
/// this node allocate. A node whose registry is larger than this gossips a
/// rotating window instead of its whole table, which still converges — every
/// entry is eventually in some window, and every pair of nodes eventually
/// gossips — it just takes more rounds.
pub const MAX_TAG_SYNC_ENTRIES: usize = 8192;

/// How `DELETE_BY_TAG` reaches the rest of the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ClusterMode {
    /// No fan-out at all. The client is responsible for calling every node.
    /// Zero server-side coupling.
    Local = 0,
    /// Bump locally, reply immediately, forward to peers in the background.
    /// Staleness elsewhere is bounded by the gossip interval.
    #[default]
    Fanout = 1,
    /// As `Fanout`, but the reply waits for reachable peers to acknowledge.
    /// Higher latency, tighter staleness bound.
    FanoutSync = 2,
}

impl ClusterMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Fanout => "fanout",
            Self::FanoutSync => "fanout_sync",
        }
    }

    pub fn from_u8(raw: u8) -> Option<Self> {
        Some(match raw {
            0 => Self::Local,
            1 => Self::Fanout,
            2 => Self::FanoutSync,
            _ => return None,
        })
    }

    /// Whether an invalidation is propagated to peers at all.
    #[inline]
    pub fn fans_out(self) -> bool {
        !matches!(self, Self::Local)
    }
}

/// One peer, as this node currently sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub addr: String,
    /// Whether the last exchange with this peer succeeded. `false` before the
    /// first one has been attempted — it reports what is known, not a guess.
    pub reachable: bool,
}

/// This node's view of the cluster, as returned by the `CLUSTER` opcode.
///
/// Membership is static configuration, not a negotiated set: there is no
/// consensus on who is in the cluster, and each node simply reports what it was
/// told. A client can compare views across nodes to detect drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    pub mode: ClusterMode,
    pub peers: Vec<PeerInfo>,
}

impl ClusterInfo {
    /// The view of a node with no peers configured.
    pub fn standalone() -> Self {
        Self {
            mode: ClusterMode::Local,
            peers: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_roundtrip_through_the_wire_encoding() {
        for mode in [
            ClusterMode::Local,
            ClusterMode::Fanout,
            ClusterMode::FanoutSync,
        ] {
            assert_eq!(ClusterMode::from_u8(mode as u8), Some(mode));
        }
        assert_eq!(ClusterMode::from_u8(9), None);
    }

    #[test]
    fn only_local_declines_to_fan_out() {
        assert!(!ClusterMode::Local.fans_out());
        assert!(ClusterMode::Fanout.fans_out());
        assert!(ClusterMode::FanoutSync.fans_out());
    }
}
