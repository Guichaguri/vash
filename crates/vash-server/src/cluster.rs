//! Cluster tag invalidation: fan-out, anti-entropy, and the peer connections
//! both run over.
//!
//! Nodes are shared-nothing (plan §10). Clients shard the keyspace, no data
//! moves between nodes, and nothing is agreed on. The single exception is tag
//! invalidation, because a tag's keys are spread by key hash across *every*
//! node — so a `DELETE_BY_TAG` that reached only the node the client happened
//! to call would leave most of the affected keys being served.
//!
//! # Why this needs no protocol
//!
//! Tag generations merge by maximum, which makes them a CRDT. Delivery is
//! therefore allowed to be sloppy in every way that usually costs a protocol:
//!
//! - **idempotent** — a replayed message changes nothing, so no deduplication;
//! - **order-independent** — messages may arrive in any order, so no sequencing;
//! - **retry-safe** — at-least-once is enough, so no acknowledgement protocol;
//! - **loss-tolerant** — a dropped message is corrected by the next gossip
//!   round, so a full queue can simply discard rather than block a write.
//!
//! That is why the code below is a queue, a timer and a digest exchange rather
//! than a consensus implementation.
//!
//! # The two mechanisms
//!
//! **Fan-out** carries an invalidation to peers immediately: one message, one
//! tag. It is what makes the common case fast.
//!
//! **Anti-entropy** exchanges whole tag→generation digests with each peer every
//! `gossip_interval`. It is what makes the system correct: it closes the gap
//! for a node that was down, partitioned, restarted, or simply missed a
//! message. Fan-out is an optimisation on top of it, not the other way round.
//!
//! # Trust
//!
//! Peer traffic arrives on the cache port and is not authenticated, so any
//! client that can reach the port can raise a generation. That is the same
//! authority a client already has through `DELETE_BY_TAG`, with one addition:
//! it can jump a generation far ahead rather than by one. Bind the port to a
//! private network, as the deployment notes say.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use vash_core::{ClusterInfo, ClusterMode, MAX_TAG_SYNC_ENTRIES, PeerInfo, TagGeneration};
use vash_store::{LmdbStore, Store};

use crate::config::ClusterConfig;
use crate::metrics::ClusterMetrics;

/// The cluster tier: peer connections, the fan-out path and the gossip timer.
///
/// Always present, even with no peers configured, so `CLUSTER` and `/stats`
/// have something truthful to report and the dispatch path has no special case.
pub struct Cluster {
    mode: ClusterMode,
    peers: Vec<Arc<Peer>>,
    fanout_timeout: Duration,
    metrics: Arc<ClusterMetrics>,
    shutdown: watch::Sender<bool>,
    /// Held so shutdown can wait for the tasks to let go of the store: the
    /// environment cannot close while another handle is outstanding.
    tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

struct Peer {
    addr: String,
    /// Whether the last exchange succeeded. `false` before the first attempt —
    /// it reports what is known, never an optimistic guess.
    reachable: AtomicBool,
    /// What this node presents to the peer. A peer is an ordinary VCP client on
    /// the ordinary port, so with authentication enforced it has to
    /// authenticate like any other. Startup refuses a clustered node that has
    /// auth required and no credential here.
    credential: Option<vash_client::Credential>,
    tx: mpsc::Sender<PeerMessage>,
}

/// Work handed to a peer's connection task.
enum PeerMessage {
    /// One tag, pushed as it happens.
    Invalidate {
        name: Arc<[u8]>,
        generation: u64,
        /// Present only in `fanout_sync`, where the client's reply waits on it.
        ack: Option<crossbeam_channel::Sender<()>>,
    },
    /// A digest exchange. The reply carries whatever the peer knows better.
    Gossip {
        full: bool,
        entries: Vec<TagGeneration>,
        reply: oneshot::Sender<Result<Vec<TagGeneration>, PeerError>>,
    },
}

#[derive(Debug, thiserror::Error)]
enum PeerError {
    #[error("connecting to the peer timed out")]
    ConnectTimeout,
    #[error("{0}")]
    Client(#[from] vash_client::ClientError),
    #[error("the exchange timed out")]
    Timeout,
}

impl Cluster {
    /// Starts the peer connections and the gossip timer.
    ///
    /// Must be called from within the tokio runtime that will serve traffic.
    pub fn start(
        config: &ClusterConfig,
        store: Arc<LmdbStore>,
        metrics: Arc<ClusterMetrics>,
    ) -> Arc<Self> {
        let mode = ClusterMode::from(config.delete_by_tag);
        let fanout_timeout = Duration::from_millis(config.fanout_timeout_ms);
        let (shutdown, _) = watch::channel(false);

        let mut peers = Vec::with_capacity(config.peers.len());
        let mut tasks = Vec::new();

        let credential = config
            .credential()
            .map(|(name, secret)| vash_client::Credential::new(name, secret));

        for addr in &config.peers {
            let (tx, rx) = mpsc::channel(config.queue_depth);
            let peer = Arc::new(Peer {
                addr: addr.clone(),
                reachable: AtomicBool::new(false),
                credential: credential.clone(),
                tx,
            });
            tasks.push(tokio::spawn(peer_loop(
                Arc::clone(&peer),
                rx,
                Arc::clone(&store),
                Arc::clone(&metrics),
                fanout_timeout,
                shutdown.subscribe(),
            )));
            tasks.push(tokio::spawn(gossip_loop(
                Arc::clone(&peer),
                Arc::clone(&store),
                Arc::clone(&metrics),
                Duration::from_millis(config.gossip_interval_ms),
                fanout_timeout,
                shutdown.subscribe(),
            )));
            peers.push(peer);
        }

        if !peers.is_empty() {
            info!(
                peers = peers.len(),
                mode = mode.as_str(),
                gossip_interval_ms = config.gossip_interval_ms,
                "cluster invalidation enabled"
            );
        }

        metrics.peers_configured(peers.len() as u64);

        Arc::new(Self {
            mode,
            peers,
            fanout_timeout,
            metrics,
            shutdown,
            tasks: std::sync::Mutex::new(tasks),
        })
    }

    /// Whether this node propagates invalidations, which is what the `CLUSTER`
    /// capability bit claims.
    pub fn active(&self) -> bool {
        self.mode.fans_out() && !self.peers.is_empty()
    }

    /// This node's view of the cluster, for the `CLUSTER` opcode and `/stats`.
    pub fn view(&self) -> ClusterInfo {
        ClusterInfo {
            mode: self.mode,
            peers: self
                .peers
                .iter()
                .map(|peer| PeerInfo {
                    addr: peer.addr.clone(),
                    reachable: peer.reachable.load(Ordering::Relaxed),
                })
                .collect(),
        }
    }

    pub fn metrics(&self) -> &ClusterMetrics {
        &self.metrics
    }

    pub fn peers_reachable(&self) -> u64 {
        self.peers
            .iter()
            .filter(|peer| peer.reachable.load(Ordering::Relaxed))
            .count() as u64
    }

    /// Propagates a local invalidation.
    ///
    /// Called from the blocking thread that ran the write, so `fanout_sync` can
    /// simply wait here — blocking is what these threads are for.
    ///
    /// Never reports failure to the caller, and deliberately: the local
    /// invalidation has already committed and cannot be undone, so an error
    /// would describe the peers rather than the request. Failures are counted
    /// and logged, and anti-entropy repairs them.
    pub fn invalidate(&self, name: &[u8], generation: u64) {
        if !self.mode.fans_out() || self.peers.is_empty() {
            return;
        }
        let name: Arc<[u8]> = Arc::from(name);
        let synchronous = self.mode == ClusterMode::FanoutSync;

        let mut acks = Vec::new();
        for peer in &self.peers {
            let (ack, receipt) = if synchronous {
                let (tx, rx) = crossbeam_channel::bounded(1);
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };

            let message = PeerMessage::Invalidate {
                name: Arc::clone(&name),
                generation,
                ack,
            };
            match peer.tx.try_send(message) {
                Ok(()) => acks.extend(receipt.map(|rx| (Arc::clone(peer), rx))),
                // The peer is down or hopelessly behind. Dropping is safe:
                // generations max-merge, so the next gossip round delivers this
                // anyway. Queueing without bound against a dead peer would not
                // be.
                Err(_) => {
                    debug!(peer = %peer.addr, "dropped an invalidation: peer queue is full or closed");
                    self.metrics.fanout_failed();
                }
            }
        }

        // Only reachable peers can acknowledge, so this waits for a bounded
        // time and then gives up rather than holding the client behind a node
        // that is down.
        let deadline = Instant::now() + self.fanout_timeout;
        for (peer, rx) in acks {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if rx.recv_timeout(remaining).is_err() {
                debug!(peer = %peer.addr, "no acknowledgement within the fan-out timeout");
            }
        }
    }

    /// Stops the peer and gossip tasks and waits for them to release the store.
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let tasks =
            std::mem::take(&mut *self.tasks.lock().expect("cluster task list lock poisoned"));
        for task in tasks {
            let _ = task.await;
        }
    }
}

/// Owns one peer's connection, serialising every exchange with it.
///
/// One connection rather than one per message: the VCP client is
/// request-at-a-time, and a peer that is down should cost one failed connect
/// per attempt rather than one per queued invalidation.
async fn peer_loop(
    peer: Arc<Peer>,
    mut rx: mpsc::Receiver<PeerMessage>,
    store: Arc<LmdbStore>,
    metrics: Arc<ClusterMetrics>,
    timeout: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut client: Option<vash_client::Client> = None;

    loop {
        let message = tokio::select! {
            message = rx.recv() => match message {
                Some(message) => message,
                None => break,
            },
            _ = shutdown.changed() => break,
        };

        match message {
            PeerMessage::Invalidate {
                name,
                generation,
                ack,
            } => {
                let entries = [(&*name, generation)];
                match exchange(&mut client, &peer, timeout, false, &entries).await {
                    Ok(learned) => {
                        metrics.fanout_sent();
                        // The peer may know a higher generation for this tag
                        // than we do — two nodes invalidating at once. Taking
                        // its answer converges immediately instead of waiting
                        // for a gossip round.
                        merge_into(&store, &metrics, learned).await;
                    }
                    Err(e) => {
                        debug!(peer = %peer.addr, error = %e, "fan-out failed");
                        metrics.fanout_failed();
                    }
                }
                // Sent whether or not it worked: the caller is waiting for this
                // peer to have been *tried*, and a peer that cannot be reached
                // must not hold up the client's reply.
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
            }

            PeerMessage::Gossip {
                full,
                entries,
                reply,
            } => {
                let offered: Vec<(&[u8], u64)> = entries
                    .iter()
                    .map(|entry| (&*entry.name, entry.generation))
                    .collect();
                let result = exchange(&mut client, &peer, timeout, full, &offered).await;
                let _ = reply.send(result);
            }
        }
    }

    debug!(peer = %peer.addr, "peer connection closed");
}

/// One `TAG_SYNC` round trip, reconnecting once if the existing connection has
/// gone stale.
///
/// Reachability and the cached connection are settled here, once, from the
/// single outcome. Setting them on each failure path instead was a bug waiting
/// to happen, and was one: a connect that *timed out* rather than being refused
/// returned early and left the peer marked reachable indefinitely, so `/metrics`
/// reported a full cluster while an invalidation was going nowhere.
async fn exchange(
    client: &mut Option<vash_client::Client>,
    peer: &Peer,
    timeout: Duration,
    full: bool,
    entries: &[(&[u8], u64)],
) -> Result<Vec<TagGeneration>, PeerError> {
    let result = try_exchange(client, peer, timeout, full, entries).await;
    let was_reachable = peer.reachable.swap(result.is_ok(), Ordering::Relaxed);
    if let Err(error) = &result {
        // A rejected credential is a configuration error, not unreachability,
        // and it will not fix itself — so it is logged at `warn` and only on
        // the transition, rather than once per gossip interval forever.
        if was_reachable || peer.credential.is_some() && is_unauthorized(error) {
            log_refusal(peer, error, was_reachable);
        }
        // Whatever went wrong, this connection is not worth keeping; the next
        // attempt starts from a fresh one.
        *client = None;
    }
    result
}

fn is_unauthorized(error: &PeerError) -> bool {
    matches!(
        error,
        PeerError::Client(vash_client::ClientError::Status(
            vash_proto::vcp::Status::Unauthorized
        ))
    )
}

fn log_refusal(peer: &Peer, error: &PeerError, was_reachable: bool) {
    if is_unauthorized(error) {
        warn!(
            peer = %peer.addr,
            "peer refused this node's credential; tag invalidation will not converge with it. \
             cluster.auth_name and cluster.auth_secret must name a credential the peer also has"
        );
    } else if was_reachable {
        debug!(peer = %peer.addr, error = %error, "peer became unreachable");
    }
}

async fn try_exchange(
    client: &mut Option<vash_client::Client>,
    peer: &Peer,
    timeout: Duration,
    full: bool,
    entries: &[(&[u8], u64)],
) -> Result<Vec<TagGeneration>, PeerError> {
    for attempt in 0..2 {
        if client.is_none() {
            let connecting = async {
                match &peer.credential {
                    Some(credential) => {
                        vash_client::Client::connect_with(&peer.addr, credential).await
                    }
                    None => vash_client::Client::connect(&peer.addr).await,
                }
            };
            *client = Some(
                tokio::time::timeout(timeout, connecting)
                    .await
                    .map_err(|_| PeerError::ConnectTimeout)??,
            );
        }

        let connection = client.as_mut().expect("connected above");
        match tokio::time::timeout(timeout, connection.tag_sync(full, entries)).await {
            Ok(Ok(learned)) => return Ok(learned),
            // A connection idle since the last round may have been closed at
            // the far end. One reconnect, then give up: past that it is the
            // peer that is broken, not the socket.
            Ok(Err(e)) => {
                *client = None;
                if attempt == 1 {
                    return Err(e.into());
                }
                debug!(peer = %peer.addr, error = %e, "peer connection failed; reconnecting");
            }
            Err(_) => return Err(PeerError::Timeout),
        }
    }
    unreachable!("the loop returns on its second attempt")
}

/// Applies generations learned from a peer.
///
/// On a blocking thread: merging is a write per shard, and a write may wait on
/// the writer queue.
async fn merge_into(store: &Arc<LmdbStore>, metrics: &ClusterMetrics, learned: Vec<TagGeneration>) {
    if learned.is_empty() {
        return;
    }
    let store = Arc::clone(store);
    let applied = tokio::task::spawn_blocking(move || apply_merges(&store, &learned))
        .await
        .unwrap_or(0);
    metrics.merged(applied);
}

/// Max-merges a batch of tag generations, returning how many were applied.
///
/// Shared with the receiving side of `TAG_SYNC`, so an invalidation learned by
/// gossip and one received by fan-out go through exactly the same path.
pub fn apply_merges(store: &LmdbStore, entries: &[TagGeneration]) -> u64 {
    let mut applied = 0;
    for entry in entries {
        // Generation 0 means "never invalidated anywhere", which carries no
        // information — and merging it would register the name here, letting a
        // peer's registry grow ours for nothing.
        if entry.generation == 0 {
            continue;
        }
        match store.merge_tag_generation(&entry.name, entry.generation) {
            Ok(_) => applied += 1,
            Err(e) => warn!(
                tag = ?String::from_utf8_lossy(&entry.name),
                error = %e,
                "could not merge a tag generation from a peer"
            ),
        }
    }
    applied
}

/// Anti-entropy: exchanges digests with one peer, forever.
///
/// **One task per peer**, rather than a single loop visiting a peer per
/// interval. The plan proposed the latter, sampling a random peer, which is the
/// right shape when membership is large and discovered. Here it is a static
/// list of a handful of addresses, and the single loop had a flaw worth more
/// than the saving: an unresponsive peer blocked the loop for its whole
/// timeout, so *one* node being down slowed convergence between all the healthy
/// ones. Measured against a killed node, gossip dropped from one round a second
/// to roughly one every three.
///
/// A task each removes the coupling entirely and tightens the bound: every peer
/// is reached every interval, rather than every `peers × interval`.
async fn gossip_loop(
    peer: Arc<Peer>,
    store: Arc<LmdbStore>,
    metrics: Arc<ClusterMetrics>,
    interval: Duration,
    timeout: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    // The first round is immediate. A node that has just restarted converges
    // now rather than one interval from now, which is the whole difference for
    // invalidations it missed while it was down.
    let mut round = 0usize;
    loop {
        if *shutdown.borrow() {
            break;
        }
        gossip_round(&peer, &store, &metrics, timeout, round).await;
        round = round.wrapping_add(1);

        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => break,
        }
    }
}

async fn gossip_round(
    peer: &Arc<Peer>,
    store: &Arc<LmdbStore>,
    metrics: &Arc<ClusterMetrics>,
    timeout: Duration,
    round: usize,
) {
    let digest_store = Arc::clone(store);
    let Ok(Ok(digest)) =
        tokio::task::spawn_blocking(move || build_digest(&digest_store, round)).await
    else {
        warn!("could not build a gossip digest");
        return;
    };
    let (full, entries) = digest;

    let (reply_tx, reply_rx) = oneshot::channel();
    let message = PeerMessage::Gossip {
        full,
        entries,
        reply: reply_tx,
    };
    if peer.tx.send(message).await.is_err() {
        return; // the peer task has stopped, which means so are we
    }

    // The peer task applies its own timeout; this one only guards against the
    // task itself having gone away mid-exchange.
    match tokio::time::timeout(timeout * 2, reply_rx).await {
        Ok(Ok(Ok(learned))) => {
            metrics.gossip_round();
            merge_into(store, metrics, learned).await;
        }
        Ok(Ok(Err(e))) => {
            debug!(peer = %peer.addr, error = %e, "gossip round failed");
            metrics.gossip_failed();
        }
        _ => metrics.gossip_failed(),
    }
}

/// Builds the digest offered to a peer.
///
/// Only tags with a non-zero generation: one that has never been invalidated
/// anywhere says nothing, and leaving it out keeps digests proportional to
/// invalidation activity rather than to registry size.
///
/// Returns `full = true` when the whole table fits in one message, which is
/// what licenses the peer to answer with entries this node never mentioned.
/// Past that the digest becomes a rotating window: convergence then takes more
/// rounds, but every entry is eventually offered.
fn build_digest(
    store: &LmdbStore,
    offset: usize,
) -> vash_store::Result<(bool, Vec<TagGeneration>)> {
    let mut all: Vec<TagGeneration> = store
        .tag_generations()?
        .into_iter()
        .filter(|entry| entry.generation > 0)
        .collect();

    if all.len() <= MAX_TAG_SYNC_ENTRIES {
        return Ok((true, all));
    }

    // Sorted so the window advances over a stable order rather than over
    // whatever the registry happened to iterate.
    all.sort_by(|a, b| a.name.cmp(&b.name));
    let start = (offset * MAX_TAG_SYNC_ENTRIES) % all.len();
    let window = all
        .iter()
        .cycle()
        .skip(start)
        .take(MAX_TAG_SYNC_ENTRIES)
        .cloned()
        .collect();
    Ok((false, window))
}
