//! Domain types for the cache server.
//!
//! This crate performs no I/O, spawns no threads and knows nothing about LMDB
//! or about any wire protocol. It defines the record format, the validation
//! rules and the [`Command`]/[`Reply`] boundary that the storage and protocol
//! adapters meet at.
//!
//! [`Command`]: command::Command
//! [`Reply`]: command::Reply

pub mod clock;
pub mod command;
pub mod error;
pub mod key;
pub mod record;
pub mod value;

pub use clock::{Clock, MAX_TTL_SECS};
pub use command::{Command, PROTOCOL_VERSION, Reply, ServerInfo, Set, Value, capability};
pub use error::{CoreError, Result};
pub use key::{Key, MAX_KEY_LEN};
pub use record::{
    MAX_TAG_LEN, MAX_TAGS, NEVER, RECORD_HEADER_LEN, RECORD_VERSION, RecordMeta, RecordRef, TagRef,
    encode_record, record_len,
};
pub use value::{ABSOLUTE_MAX_VALUE_LEN, DEFAULT_MAX_VALUE_LEN, validate_value};
