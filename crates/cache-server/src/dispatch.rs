//! Command execution: the only place that maps domain outcomes onto wire status
//! codes.

use cache_core::{Command, Reply, ServerInfo};
use cache_proto::kcp::{DecodeError, Decoded, Status, decode, encode_error, encode_reply};
use cache_store::{Store, StoreError};
use tracing::{error, warn};

use crate::state::ServerState;

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

        Command::Set(set) => {
            let cas = state.store.set(set).map_err(to_status)?;
            Ok(Reply::Stored { cas })
        }

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
        StoreError::Core(_) => Status::BadRequest,
        other => {
            error!(error = %other, "storage failure");
            Status::Internal
        }
    }
}

/// Builds the handshake response advertised to clients.
pub fn server_info(shards: u16, max_value_len: usize) -> ServerInfo {
    ServerInfo {
        protocol_version: cache_core::PROTOCOL_VERSION,
        shards,
        max_key_len: cache_core::MAX_KEY_LEN as u32,
        max_value_len: max_value_len as u32,
        // Tags, memcached and cluster are advertised as each milestone lands.
        // Advertising them early would make a client trust a feature that is
        // not there.
        capabilities: 0,
    }
}
