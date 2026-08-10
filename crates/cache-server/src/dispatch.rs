//! Command execution: the only place that maps domain outcomes onto wire status
//! codes.

use cache_core::{Command, Reply, ServerInfo};
use cache_proto::kcp::{DecodeError, Decoded, Status, decode, encode_error, encode_reply};
use cache_store::{Store, StoreError};
use tracing::{error, warn};

use crate::metrics::ErrorClass;
use crate::state::ServerState;

/// Executes a memcached command, rendering the reply in that dialect.
///
/// Shares [`execute`] with the KCP path: the storage layer never learns which
/// wire format a request arrived on.
pub fn execute_memcached(
    state: &ServerState,
    parsed: &cache_proto::memcached::Parsed<'_>,
    out: &mut Vec<u8>,
) -> Closing {
    use cache_proto::memcached::encode as mc;

    let result = execute(state, &parsed.command);
    let closing = matches!(result, Ok(Reply::Closing));

    match result {
        Ok(reply) => {
            // `noreply` suppresses the response but never the work.
            if !parsed.noreply {
                mc::encode(out, &parsed.style, &parsed.command, &reply);
            }
        }
        Err(status) => {
            if !parsed.noreply {
                mc::encode_error(out, memcached_error(status));
            }
        }
    }

    if closing { Closing::Yes } else { Closing::No }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Closing {
    Yes,
    No,
}

/// Maps an internal status onto the memcached error vocabulary.
fn memcached_error(status: Status) -> cache_proto::memcached::ErrorKind {
    use cache_proto::memcached::ErrorKind;

    // The wording is memcached's, verbatim: the differential suite compares
    // response bytes against a real server.
    match status {
        Status::TooLarge => ErrorKind::Server("object too large for cache"),
        Status::BadRequest => ErrorKind::Client("bad command line format"),
        Status::NotNumeric => ErrorKind::Client("cannot increment or decrement non-numeric value"),
        Status::Unsupported => ErrorKind::Error,
        Status::Unauthorized => ErrorKind::Client("command disabled by configuration"),
        Status::CapacityFull => ErrorKind::Server("out of memory storing object"),
        Status::Overloaded => ErrorKind::Server("server is overloaded"),
        Status::NotStored => ErrorKind::Server("not stored"),
        _ => ErrorKind::Server("internal error"),
    }
}

/// Decodes and executes one complete frame, returning the encoded response.
///
/// Runs on a blocking thread, and takes ownership of the frame bytes, so the
/// borrowed key and value slices produced by the decoder never cross a task
/// boundary and never need copying. An empty return means "send nothing" — a
/// `NO_REPLY` request.
pub fn execute_frame(state: &ServerState, frame: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();

    match decode(frame) {
        Ok(Decoded::Request { request, .. }) => {
            let result = execute(state, &request.command);

            if request.no_reply {
                if let Err(status) = result {
                    warn!(?status, opcode = ?request.opcode, "no-reply request failed");
                }
                return out;
            }

            match result {
                Ok(reply) => encode_reply(&mut out, request.opcode, request.request_id, &reply),
                Err(status) => {
                    encode_error(&mut out, request.opcode as u8, request.request_id, status)
                }
            }
        }

        Err(DecodeError::Body {
            request_id,
            opcode,
            status,
            detail,
            ..
        }) => {
            warn!(request_id, opcode, detail, "rejected malformed request");
            encode_error(&mut out, opcode, request_id, status);
        }

        // The caller only passes frames whose length it already validated, so
        // neither of these is reachable. Answering rather than panicking keeps a
        // logic slip from taking the process down.
        Ok(Decoded::Incomplete { .. }) | Err(DecodeError::Fatal { .. }) => {
            error!("frame passed to execute_frame was not a complete, valid frame");
            encode_error(&mut out, 0, 0, Status::Internal);
        }
    }

    out
}

fn execute(state: &ServerState, command: &Command<'_>) -> Result<Reply, Status> {
    let outcome = execute_inner(state, command);

    // Counted here, at the single point every command passes through, so the
    // numbers cannot drift apart from what was actually served.
    match &outcome {
        Ok(reply) => match reply {
            Reply::Value(_) => state.metrics.read(1, 0),
            Reply::Values(values) => {
                let hits = values.iter().filter(|v| v.is_some()).count() as u64;
                state.metrics.read(hits, values.len() as u64 - hits);
            }
            Reply::NotFound if is_read(command) => state.metrics.read(0, 1),
            Reply::Stored(_)
            | Reply::StoredMany(_)
            | Reply::Deleted
            | Reply::DeletedMany(_)
            | Reply::Touched
            | Reply::Counter(_)
            | Reply::Invalidated(_)
            | Reply::Flushed(_) => state.metrics.write(),
            _ => state.metrics.other(),
        },
        Err(status) => {
            state.metrics.other();
            state.metrics.error(match status {
                Status::CapacityFull => ErrorClass::Capacity,
                Status::Overloaded => ErrorClass::Overloaded,
                Status::Internal => ErrorClass::Internal,
                _ => ErrorClass::Client,
            });
        }
    }

    outcome
}

fn is_read(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::Get { .. } | Command::GetMany(_) | Command::GetAndTouch { .. }
    )
}

fn execute_inner(state: &ServerState, command: &Command<'_>) -> Result<Reply, Status> {
    match command {
        Command::Ping => Ok(Reply::Pong),

        Command::Hello { protocol_version } => {
            if *protocol_version != cache_core::PROTOCOL_VERSION {
                warn!(
                    client = protocol_version,
                    server = cache_core::PROTOCOL_VERSION,
                    "client requested an unsupported protocol version"
                );
                return Err(Status::Unsupported);
            }
            Ok(Reply::Hello(state.info))
        }

        Command::Get { key } => match state.store.get(*key).map_err(to_status)? {
            Some(value) => Ok(Reply::Value(value)),
            None => Ok(Reply::NotFound),
        },

        Command::GetMany(keys) => Ok(Reply::Values(
            state.store.get_many(keys).map_err(to_status)?,
        )),

        Command::Set(set) => Ok(Reply::Stored(state.store.store(set).map_err(to_status)?)),

        Command::GetAndTouch { keys, ttl_secs } => Ok(Reply::Values(
            state
                .store
                .get_and_touch(keys, *ttl_secs)
                .map_err(to_status)?,
        )),

        Command::Incr {
            key,
            delta,
            decrement,
        } => match state
            .store
            .incr(*key, *delta, *decrement)
            .map_err(to_status)?
        {
            Some(value) => Ok(Reply::Counter(value)),
            None => Ok(Reply::NotFound),
        },

        Command::Stats => Ok(Reply::Stats(collect_stats(state))),
        Command::Version => Ok(Reply::Version(cache_proto::memcached::encode::VERSION)),
        Command::Quit => Ok(Reply::Closing),

        Command::SetMany(sets) => Ok(Reply::StoredMany(
            state.store.set_many(sets).map_err(to_status)?,
        )),

        Command::Delete { key } => {
            if state.store.delete(*key).map_err(to_status)? {
                Ok(Reply::Deleted)
            } else {
                Ok(Reply::NotFound)
            }
        }

        Command::DeleteMany(keys) => Ok(Reply::DeletedMany(
            state.store.delete_many(keys).map_err(to_status)?,
        )),

        Command::Touch { key, ttl_secs } => {
            if state.store.touch(*key, *ttl_secs).map_err(to_status)? {
                Ok(Reply::Touched)
            } else {
                Ok(Reply::NotFound)
            }
        }

        Command::DeleteByTag { tag } => Ok(Reply::Invalidated(
            state.store.delete_by_tag(tag).map_err(to_status)?,
        )),

        Command::Flush => {
            if !state.flush_enabled {
                // A remote cache-wipe primitive stays off unless deliberately
                // enabled.
                warn!("rejected flush: disabled by configuration");
                return Err(Status::Unauthorized);
            }
            Ok(Reply::Flushed(state.store.flush().map_err(to_status)?))
        }
    }
}

/// Maps a storage failure onto the status the client sees.
///
/// Internal failures are logged here and reported as a bare `INTERNAL`: the
/// client gets enough to retry or fall back, and nothing about the server's
/// internals leaks onto the wire.
fn to_status(err: StoreError) -> Status {
    use cache_core::CoreError;

    match err {
        StoreError::CapacityFull => Status::CapacityFull,
        StoreError::Overloaded | StoreError::ShuttingDown => Status::Overloaded,
        StoreError::Unsupported(what) => {
            warn!(what, "client used a feature this build does not implement");
            Status::Unsupported
        }
        StoreError::Core(CoreError::ValueTooLarge { .. } | CoreError::KeyTooLong { .. }) => {
            Status::TooLarge
        }
        StoreError::TagLimit(limit) => {
            warn!(limit, "tag registry is full");
            Status::CapacityFull
        }
        // memcached reports this as a client error, not a miss, and with its
        // own wording — see `memcached_error`.
        StoreError::NotNumeric => Status::NotNumeric,
        StoreError::Core(_) => Status::BadRequest,
        other => {
            error!(error = %other, "storage failure");
            Status::Internal
        }
    }
}

/// The `stats` payload.
///
/// A subset of memcached's counters, restricted to what this server actually
/// measures — plus its own. Reporting a plausible-looking zero for something we
/// do not track would mislead any dashboard reading it.
fn collect_stats(state: &ServerState) -> Vec<(String, String)> {
    let mut stats = vec![
        ("pid".into(), std::process::id().to_string()),
        (
            "version".into(),
            cache_proto::memcached::encode::VERSION.into(),
        ),
        ("pointer_size".into(), usize::BITS.to_string()),
    ];

    match state.store.stats() {
        Ok(s) => stats.extend([
            ("curr_items".into(), s.entries.to_string()),
            ("bytes".into(), s.used_bytes.to_string()),
            ("limit_maxbytes".into(), s.map_size.to_string()),
            // Beyond memcached's set, but they are what this server is actually
            // about.
            ("kached_utilisation".into(), format!("{:.4}", s.utilisation)),
            ("kached_expiry_entries".into(), s.expiry_entries.to_string()),
            ("kached_tags".into(), s.tags.to_string()),
            (
                "kached_tag_index_entries".into(),
                s.tag_index_entries.to_string(),
            ),
            (
                "kached_pending_reclaims".into(),
                s.pending_reclaims.to_string(),
            ),
            ("kached_commits".into(), s.commits.to_string()),
            ("kached_committed_ops".into(), s.committed_ops.to_string()),
            (
                "kached_mean_batch".into(),
                format!("{:.2}", s.mean_batch_size()),
            ),
            ("kached_sweeps".into(), s.sweeps.to_string()),
            ("kached_reclaimed".into(), s.reclaimed.to_string()),
            ("kached_tag_reclaimed".into(), s.tag_reclaimed.to_string()),
            ("kached_sweep_lag_ms".into(), s.sweep_lag_ms.to_string()),
            ("kached_epoch".into(), s.epoch.to_string()),
            ("kached_readers_in_use".into(), s.readers_in_use.to_string()),
        ]),
        Err(e) => error!(error = %e, "could not read store stats"),
    }

    stats
}

/// Builds the handshake response advertised to clients.
pub fn server_info(shards: u16, max_value_len: usize) -> ServerInfo {
    ServerInfo {
        protocol_version: cache_core::PROTOCOL_VERSION,
        shards,
        max_key_len: cache_core::MAX_KEY_LEN as u32,
        max_value_len: max_value_len as u32,
        // Advertised only as each milestone lands: claiming a capability the
        // server does not have would make a client trust invalidation that is
        // not happening.
        capabilities: cache_core::capability::TAGS | cache_core::capability::MEMCACHED,
    }
}
