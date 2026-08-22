//! Who is connected, for `stats conns`.
//!
//! The one piece of shared mutable state this server keeps about connections,
//! and it is deliberately small: a peer address and a dialect written once, and
//! a last-command timestamp written on every request. Everything else a client
//! might want to know — what it is doing right now, how much it has sent — is
//! either not measured or belongs to the metrics.
//!
//! **The hot path takes no lock.** A connection holds an `Arc` to its own entry
//! from the moment it is accepted, so recording a command is one relaxed store
//! into a word nothing else writes. The map is locked only when a connection
//! opens or closes.
//!
//! **Ids are monotonic, not file descriptors.** Upstream keys this table by fd,
//! and fds are reused the instant one closes — so two `stats conns` calls a
//! second apart can show the same number meaning two different clients. A
//! counter that never repeats is what makes correlating them possible.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

use vash_proto::Protocol;

/// What is known about one open connection.
pub struct ConnInfo {
    pub id: u64,
    peer: SocketAddr,
    /// The dialect first-byte detection settled on, or `None` until it has.
    ///
    /// A `u8` rather than a lock because it is written once, from the task that
    /// owns the connection, and read only by `stats conns`.
    dialect: AtomicU8,
    authenticated: AtomicBool,
    /// Whether this connection arrived on the TLS port and completed a
    /// handshake.
    ///
    /// Written once by the accept path before the connection is served, read
    /// only by `stats conns` — the same discipline as `dialect`. It is the one
    /// thing an operator closing the plaintext port has to be able to check,
    /// and nothing else in the server can answer it per connection.
    tls: AtomicBool,
    /// Milliseconds since the registry's epoch, at the last command.
    last_command_ms: AtomicU64,
}

/// `dialect` before detection has run.
const UNDETECTED: u8 = 0;

impl ConnInfo {
    /// Records that this connection just ran a command.
    ///
    /// One relaxed store, on the request path of every dialect. Relaxed is
    /// right: the value is only ever read to be subtracted from *now* and
    /// rendered to whole seconds, so a reader seeing a slightly stale one is
    /// seeing a number that was true a moment ago.
    #[inline]
    pub fn touched(&self, since_epoch: &Instant) {
        self.last_command_ms
            .store(since_epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    pub fn dialect_chosen(&self, protocol: Protocol) {
        self.dialect.store(protocol as u8 + 1, Ordering::Relaxed);
    }

    pub fn authenticated(&self) {
        self.authenticated.store(true, Ordering::Relaxed);
    }

    /// Records that this connection is encrypted. Called once, after the
    /// handshake completes and before any request is served.
    pub fn tls_established(&self) {
        self.tls.store(true, Ordering::Relaxed);
    }

    pub fn is_tls(&self) -> bool {
        self.tls.load(Ordering::Relaxed)
    }

    fn dialect_name(&self) -> &'static str {
        match self.dialect.load(Ordering::Relaxed) {
            n if n == Protocol::Vcp as u8 + 1 => "vcp",
            n if n == Protocol::Memcached as u8 + 1 => "memcached",
            n if n == Protocol::Resp as u8 + 1 => "resp",
            // Detection needs a byte, and a connection that has sent none has
            // no dialect yet. "unknown" is the true answer, not a placeholder.
            _ => "unknown",
        }
    }
}

/// Every connection currently open.
pub struct Registry {
    live: Mutex<HashMap<u64, Arc<ConnInfo>>>,
    next: AtomicU64,
    /// What `last_command_ms` is measured from.
    ///
    /// A monotonic base rather than unix time, so a clock stepped backwards by
    /// NTP cannot make a connection report a command in the future.
    epoch: Instant,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            live: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            epoch: Instant::now(),
        }
    }
}

impl Registry {
    /// Registers a newly accepted connection.
    ///
    /// The returned handle is the connection's own; dropping it is not enough
    /// to deregister, because the map holds a clone — [`Registry::close`] does
    /// that, from the one place that knows the connection is finished.
    pub fn open(&self, peer: SocketAddr) -> Arc<ConnInfo> {
        let info = Arc::new(ConnInfo {
            id: self.next.fetch_add(1, Ordering::Relaxed),
            peer,
            dialect: AtomicU8::new(UNDETECTED),
            authenticated: AtomicBool::new(false),
            tls: AtomicBool::new(false),
            last_command_ms: AtomicU64::new(self.epoch.elapsed().as_millis() as u64),
        });
        self.lock().insert(info.id, Arc::clone(&info));
        info
    }

    pub fn close(&self, id: u64) {
        self.lock().remove(&id);
    }

    /// The clock connections stamp themselves against.
    pub fn epoch(&self) -> &Instant {
        &self.epoch
    }

    /// Renders `stats conns`.
    ///
    /// Sorted by id so two calls are comparable, and so the listener — which is
    /// synthetic and has no id of its own — always comes first, as upstream's
    /// does.
    pub fn render(&self, listen: SocketAddr) -> Vec<(String, String)> {
        let now = self.epoch.elapsed().as_millis() as u64;

        let mut connections: Vec<Arc<ConnInfo>> = self.lock().values().map(Arc::clone).collect();
        connections.sort_unstable_by_key(|info| info.id);

        // Upstream reports the listening socket alongside the accepted ones,
        // and `conn_listening` is the one state here that is unambiguous — the
        // listener really is doing exactly that.
        let mut out = vec![
            ("0:addr".into(), format!("tcp:{listen}")),
            ("0:state".into(), "conn_listening".into()),
        ];

        for info in connections {
            let id = info.id;
            out.extend([
                (format!("{id}:addr"), format!("tcp:{}", info.peer)),
                (format!("{id}:listen_addr"), format!("tcp:{listen}")),
                (
                    format!("{id}:secs_since_last_cmd"),
                    (now.saturating_sub(info.last_command_ms.load(Ordering::Relaxed)) / 1_000)
                        .to_string(),
                ),
                // Offered where upstream reports `state`. Its ten values name
                // positions in an event-loop state machine that does not exist
                // here — a connection is an async task, and the honest answer
                // would be "somewhere in a select". This answers the question
                // an operator was actually asking.
                (format!("{id}:vash_dialect"), info.dialect_name().into()),
                (
                    format!("{id}:vash_authenticated"),
                    if info.authenticated.load(Ordering::Relaxed) {
                        "yes".into()
                    } else {
                        "no".into()
                    },
                ),
                // The rollout's only honest progress bar: an operator about to
                // close `server.listen` needs to know whether anything is
                // still arriving in the clear, and no aggregate can say which
                // client it is.
                (
                    format!("{id}:vash_tls"),
                    if info.is_tls() {
                        "yes".into()
                    } else {
                        "no".into()
                    },
                ),
            ]);
        }

        out
    }

    /// Connections currently registered. Test-only.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().len()
    }

    /// A poisoned lock is recovered from rather than propagated: the entries are
    /// plain data, every operation on them is total, so a poisoning means a
    /// panic elsewhere — and refusing every `stats conns` for the rest of the
    /// process's life would be a strange way to react to it.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Arc<ConnInfo>>> {
        self.live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn listen() -> SocketAddr {
        "0.0.0.0:11211".parse().unwrap()
    }

    fn field<'a>(rendered: &'a [(String, String)], name: &str) -> Option<&'a str> {
        rendered
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn a_connection_appears_while_it_is_open_and_not_after() {
        let registry = Registry::default();
        let info = registry.open(peer(5000));

        let rendered = registry.render(listen());
        assert_eq!(
            field(&rendered, &format!("{}:addr", info.id)),
            Some("tcp:127.0.0.1:5000")
        );
        assert_eq!(
            field(&rendered, &format!("{}:listen_addr", info.id)),
            Some("tcp:0.0.0.0:11211")
        );

        registry.close(info.id);
        assert_eq!(registry.len(), 0);
        assert!(field(&registry.render(listen()), &format!("{}:addr", info.id)).is_none());
    }

    /// Upstream keys this table by file descriptor, and an fd is reused the
    /// moment one closes — so the same number can mean two clients a second
    /// apart. Correlating two calls needs an id that never repeats.
    #[test]
    fn ids_are_never_reused() {
        let registry = Registry::default();
        let first = registry.open(peer(5000));
        registry.close(first.id);
        let second = registry.open(peer(5001));

        assert_ne!(first.id, second.id);
        assert!(second.id > first.id);
    }

    #[test]
    fn the_listener_is_always_reported() {
        let registry = Registry::default();
        let rendered = registry.render(listen());
        assert_eq!(field(&rendered, "0:addr"), Some("tcp:0.0.0.0:11211"));
        assert_eq!(field(&rendered, "0:state"), Some("conn_listening"));
    }

    #[test]
    fn a_dialect_is_unknown_until_a_byte_has_arrived() {
        let registry = Registry::default();
        let info = registry.open(peer(5000));
        assert_eq!(
            field(
                &registry.render(listen()),
                &format!("{}:vash_dialect", info.id)
            ),
            Some("unknown"),
            "detection needs a byte, and none has been sent"
        );

        for (protocol, name) in [
            (Protocol::Vcp, "vcp"),
            (Protocol::Memcached, "memcached"),
            (Protocol::Resp, "resp"),
        ] {
            info.dialect_chosen(protocol);
            assert_eq!(
                field(
                    &registry.render(listen()),
                    &format!("{}:vash_dialect", info.id)
                ),
                Some(name)
            );
        }
    }

    #[test]
    fn authentication_is_reported_once_it_has_happened() {
        let registry = Registry::default();
        let info = registry.open(peer(5000));
        let authenticated = |registry: &Registry| {
            field(
                &registry.render(listen()),
                &format!("{}:vash_authenticated", info.id),
            )
            .map(str::to_owned)
        };

        assert_eq!(authenticated(&registry).as_deref(), Some("no"));
        info.authenticated();
        assert_eq!(authenticated(&registry).as_deref(), Some("yes"));
    }

    #[test]
    fn a_command_resets_the_idle_clock() {
        let registry = Registry::default();
        let info = registry.open(peer(5000));
        std::thread::sleep(std::time::Duration::from_millis(5));
        info.touched(registry.epoch());

        assert_eq!(
            field(
                &registry.render(listen()),
                &format!("{}:secs_since_last_cmd", info.id)
            ),
            Some("0")
        );
    }
}
