//! Memcached response rendering.
//!
//! A reply's shape depends on which command asked for it â€” `get` answers with
//! `VALUE`/`END`, `mg` with `HD`/`VA`/`EN`, `incr` with a bare number â€” so the
//! parser records a [`ResponseStyle`] alongside the command and the encoder
//! renders against that. Keeping the choice in the parser means the storage
//! layer never learns that two dialects exist.

use vash_core::{Command, Reply, Stored};

use super::ErrorKind;
use super::meta::MetaFlags;

/// How to render the reply to a particular command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseStyle {
    /// `VALUE <key> <flags> <bytes> [<cas>]` lines, then `END`.
    Retrieval {
        with_cas: bool,
    },
    /// `STORED` / `NOT_STORED` / `EXISTS` / `NOT_FOUND`.
    Storage,
    /// `DELETED` / `NOT_FOUND`.
    Deleted,
    /// `TOUCHED` / `NOT_FOUND`.
    Touched,
    /// The bare new value, or `NOT_FOUND`.
    Counter,
    /// `OK`.
    Ok,
    Stats,
    Version,
    Quit,
    Meta(MetaStyle),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaStyle {
    Get(MetaFlags),
    Set(MetaFlags),
    Delete(MetaFlags),
    Arithmetic(MetaFlags),
    /// `mn`
    NoOp,
    /// `me`
    Debug,
}

pub const VERSION: &str = "1.6.38-vash";

/// Appends the rendered reply. `command` supplies the keys that `VALUE` lines
/// need, which the reply itself does not carry.
pub fn encode(out: &mut Vec<u8>, style: &ResponseStyle, command: &Command<'_>, reply: &Reply) {
    match style {
        ResponseStyle::Retrieval { with_cas } => encode_retrieval(out, *with_cas, command, reply),

        ResponseStyle::Storage => out.extend_from_slice(match reply {
            Reply::Stored(Stored::Stored(_)) => b"STORED\r\n",
            Reply::Stored(Stored::NotStored) => b"NOT_STORED\r\n",
            Reply::Stored(Stored::Exists) => b"EXISTS\r\n",
            Reply::Stored(Stored::NotFound) | Reply::NotFound => b"NOT_FOUND\r\n",
            _ => b"SERVER_ERROR unexpected reply\r\n",
        }),

        ResponseStyle::Deleted => out.extend_from_slice(match reply {
            Reply::Deleted | Reply::Invalidated(true) => b"DELETED\r\n",
            _ => b"NOT_FOUND\r\n",
        }),

        ResponseStyle::Touched => out.extend_from_slice(match reply {
            Reply::Touched => b"TOUCHED\r\n",
            _ => b"NOT_FOUND\r\n",
        }),

        ResponseStyle::Counter => match reply {
            Reply::Counter(value) => {
                out.extend_from_slice(value.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            _ => out.extend_from_slice(b"NOT_FOUND\r\n"),
        },

        ResponseStyle::Ok => out.extend_from_slice(b"OK\r\n"),

        ResponseStyle::Version => {
            out.extend_from_slice(b"VERSION ");
            out.extend_from_slice(VERSION.as_bytes());
            out.extend_from_slice(b"\r\n");
        }

        ResponseStyle::Stats => {
            if let Reply::Stats(entries) = reply {
                for (name, value) in entries {
                    out.extend_from_slice(b"STAT ");
                    out.extend_from_slice(name.as_bytes());
                    out.push(b' ');
                    out.extend_from_slice(value.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
            }
            out.extend_from_slice(b"END\r\n");
        }

        // The connection closes; nothing is sent.
        ResponseStyle::Quit => {}

        ResponseStyle::Meta(meta) => encode_meta(out, meta, command, reply),
    }
}

fn encode_retrieval(out: &mut Vec<u8>, with_cas: bool, command: &Command<'_>, reply: &Reply) {
    let keys = match command {
        Command::GetMany(keys) | Command::GetAndTouch { keys, .. } => keys.as_slice(),
        _ => &[],
    };

    // A batch reply holds one slot per requested key, in order, so misses are
    // simply skipped â€” memcached omits them rather than reporting them.
    if let Reply::Values(values) = reply {
        for (key, value) in keys.iter().zip(values) {
            let Some(value) = value else { continue };
            out.extend_from_slice(b"VALUE ");
            out.extend_from_slice(key.as_bytes());
            out.push(b' ');
            out.extend_from_slice(value.mc_flags.to_string().as_bytes());
            out.push(b' ');
            out.extend_from_slice(value.data.len().to_string().as_bytes());
            if with_cas {
                out.push(b' ');
                out.extend_from_slice(value.cas.to_string().as_bytes());
            }
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(&value.data);
            out.extend_from_slice(b"\r\n");
        }
    }
    out.extend_from_slice(b"END\r\n");
}

fn encode_meta(out: &mut Vec<u8>, style: &MetaStyle, command: &Command<'_>, reply: &Reply) {
    match style {
        MetaStyle::NoOp => out.extend_from_slice(b"MN\r\n"),

        MetaStyle::Get(flags) => {
            // `mg` addresses one key, but a `T` flag turns it into a
            // get-and-touch, whose reply is a batch of one.
            let value = match reply {
                Reply::Value(value) => Some(value),
                Reply::Values(values) => values.first().and_then(|v| v.as_ref()),
                _ => None,
            };

            let Some(value) = value else {
                out.extend_from_slice(b"EN\r\n");
                return;
            };

            if flags.want_value {
                out.extend_from_slice(b"VA ");
                out.extend_from_slice(value.data.len().to_string().as_bytes());
            } else {
                out.extend_from_slice(b"HD");
            }
            write_return_flags(out, flags, command, Some(value));
            out.extend_from_slice(b"\r\n");

            if flags.want_value {
                out.extend_from_slice(&value.data);
                out.extend_from_slice(b"\r\n");
            }
        }

        MetaStyle::Set(flags) => {
            match reply {
                Reply::Stored(Stored::Stored(cas)) => {
                    out.extend_from_slice(b"HD");
                    if flags.want_cas {
                        out.extend_from_slice(b" c");
                        out.extend_from_slice(cas.to_string().as_bytes());
                    }
                    write_key_and_opaque(out, flags, command);
                }
                Reply::Stored(Stored::NotStored) => out.extend_from_slice(b"NS"),
                Reply::Stored(Stored::Exists) => out.extend_from_slice(b"EX"),
                Reply::Stored(Stored::NotFound) | Reply::NotFound => out.extend_from_slice(b"NF"),
                _ => out.extend_from_slice(b"SERVER_ERROR unexpected reply"),
            }
            out.extend_from_slice(b"\r\n");
        }

        MetaStyle::Delete(flags) => {
            match reply {
                Reply::Deleted | Reply::Invalidated(true) => {
                    out.extend_from_slice(b"HD");
                    write_key_and_opaque(out, flags, command);
                }
                _ => out.extend_from_slice(b"NF"),
            }
            out.extend_from_slice(b"\r\n");
        }

        MetaStyle::Arithmetic(flags) => match reply {
            Reply::Counter(value) => {
                let text = value.to_string();
                if flags.want_value {
                    out.extend_from_slice(b"VA ");
                    out.extend_from_slice(text.len().to_string().as_bytes());
                    write_key_and_opaque(out, flags, command);
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(text.as_bytes());
                } else {
                    out.extend_from_slice(b"HD");
                    write_key_and_opaque(out, flags, command);
                }
                out.extend_from_slice(b"\r\n");
            }
            _ => out.extend_from_slice(b"NF\r\n"),
        },

        MetaStyle::Debug => match reply {
            Reply::Value(value) => {
                out.extend_from_slice(b"ME ");
                out.extend_from_slice(command_key(command).unwrap_or(b""));
                out.extend_from_slice(b" cas=");
                out.extend_from_slice(value.cas.to_string().as_bytes());
                out.extend_from_slice(b" size=");
                out.extend_from_slice(value.data.len().to_string().as_bytes());
                out.extend_from_slice(b" fetch=yes\r\n");
            }
            _ => out.extend_from_slice(b"EN\r\n"),
        },
    }
}

/// Appends the return flags a meta request asked for, in a stable order.
fn write_return_flags(
    out: &mut Vec<u8>,
    flags: &MetaFlags,
    command: &Command<'_>,
    value: Option<&vash_core::Value>,
) {
    if let Some(value) = value {
        if flags.want_client_flags {
            out.extend_from_slice(b" f");
            out.extend_from_slice(value.mc_flags.to_string().as_bytes());
        }
        if flags.want_size {
            out.extend_from_slice(b" s");
            out.extend_from_slice(value.data.len().to_string().as_bytes());
        }
        if flags.want_cas {
            out.extend_from_slice(b" c");
            out.extend_from_slice(value.cas.to_string().as_bytes());
        }
        if flags.want_ttl {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            // `-1` is memcached's "never expires", and also the only honest
            // answer if the value arrived without an expiry.
            let remaining = value.remaining_ttl_secs(now_ms).unwrap_or(-1);
            out.extend_from_slice(b" t");
            out.extend_from_slice(remaining.to_string().as_bytes());
        }
    }
    write_key_and_opaque(out, flags, command);
}

fn write_key_and_opaque(out: &mut Vec<u8>, flags: &MetaFlags, command: &Command<'_>) {
    if flags.want_key
        && let Some(key) = command_key(command)
    {
        out.extend_from_slice(b" k");
        out.extend_from_slice(key);
    }
    if let Some(opaque) = &flags.opaque {
        out.extend_from_slice(b" O");
        out.extend_from_slice(opaque);
    }
}

fn command_key<'a>(command: &'a Command<'a>) -> Option<&'a [u8]> {
    match command {
        Command::Get { key } | Command::Delete { key } | Command::Touch { key, .. } => {
            Some(key.as_bytes())
        }
        Command::Incr { key, .. } => Some(key.as_bytes()),
        Command::Set(set) => Some(set.key.as_bytes()),
        Command::GetAndTouch { keys, .. } | Command::GetMany(keys) => {
            keys.first().map(|k| k.as_bytes())
        }
        _ => None,
    }
}

/// Renders an error line.
pub fn encode_error(out: &mut Vec<u8>, kind: ErrorKind) {
    match kind {
        ErrorKind::Error => out.extend_from_slice(b"ERROR\r\n"),
        ErrorKind::Client(detail) => {
            out.extend_from_slice(b"CLIENT_ERROR ");
            out.extend_from_slice(detail.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        ErrorKind::Server(detail) => {
            out.extend_from_slice(b"SERVER_ERROR ");
            out.extend_from_slice(detail.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use vash_core::{Key, Value};

    fn value(data: &'static [u8], mc_flags: u32, cas: u64) -> Value {
        Value {
            data: Bytes::from_static(data),
            mc_flags,
            cas,
            expires_at_ms: None,
        }
    }

    fn rendered(style: ResponseStyle, command: Command<'_>, reply: Reply) -> String {
        let mut out = Vec::new();
        encode(&mut out, &style, &command, &reply);
        String::from_utf8(out).expect("responses are ascii")
    }

    #[test]
    fn get_renders_value_lines_and_skips_misses() {
        let keys = vec![
            Key::from_stored(b"a"),
            Key::from_stored(b"gone"),
            Key::from_stored(b"c"),
        ];
        let reply = Reply::Values(vec![
            Some(value(b"1", 7, 100)),
            None,
            Some(value(b"333", 0, 101)),
        ]);

        assert_eq!(
            rendered(
                ResponseStyle::Retrieval { with_cas: false },
                Command::GetMany(keys),
                reply
            ),
            "VALUE a 7 1\r\n1\r\nVALUE c 0 3\r\n333\r\nEND\r\n"
        );
    }

    #[test]
    fn gets_includes_the_cas_token() {
        let reply = Reply::Values(vec![Some(value(b"x", 0, 42))]);
        assert_eq!(
            rendered(
                ResponseStyle::Retrieval { with_cas: true },
                Command::GetMany(vec![Key::from_stored(b"k")]),
                reply
            ),
            "VALUE k 0 1 42\r\nx\r\nEND\r\n"
        );
    }

    #[test]
    fn a_get_with_no_hits_is_just_end() {
        assert_eq!(
            rendered(
                ResponseStyle::Retrieval { with_cas: false },
                Command::GetMany(vec![Key::from_stored(b"k")]),
                Reply::Values(vec![None])
            ),
            "END\r\n"
        );
    }

    #[test]
    fn storage_outcomes_map_to_their_lines() {
        let command = || Command::Ping;
        for (outcome, expected) in [
            (Stored::Stored(1), "STORED\r\n"),
            (Stored::NotStored, "NOT_STORED\r\n"),
            (Stored::Exists, "EXISTS\r\n"),
            (Stored::NotFound, "NOT_FOUND\r\n"),
        ] {
            assert_eq!(
                rendered(ResponseStyle::Storage, command(), Reply::Stored(outcome)),
                expected
            );
        }
    }

    #[test]
    fn counters_render_bare() {
        assert_eq!(
            rendered(ResponseStyle::Counter, Command::Ping, Reply::Counter(41)),
            "41\r\n"
        );
        assert_eq!(
            rendered(ResponseStyle::Counter, Command::Ping, Reply::NotFound),
            "NOT_FOUND\r\n"
        );
    }

    #[test]
    fn meta_get_renders_hd_without_the_value_flag() {
        let flags = MetaFlags {
            want_cas: true,
            ..MetaFlags::default()
        };
        assert_eq!(
            rendered(
                ResponseStyle::Meta(MetaStyle::Get(flags)),
                Command::Get {
                    key: Key::from_stored(b"k")
                },
                Reply::Value(value(b"hello", 0, 9))
            ),
            "HD c9\r\n"
        );
    }

    #[test]
    fn meta_get_renders_va_with_the_value_flag() {
        let flags = MetaFlags {
            want_value: true,
            want_client_flags: true,
            want_size: true,
            want_key: true,
            opaque: Some(b"op1".to_vec()),
            ..MetaFlags::default()
        };
        assert_eq!(
            rendered(
                ResponseStyle::Meta(MetaStyle::Get(flags)),
                Command::Get {
                    key: Key::from_stored(b"k")
                },
                Reply::Value(value(b"hello", 3, 9))
            ),
            "VA 5 f3 s5 kk Oop1\r\nhello\r\n"
        );
    }

    #[test]
    fn a_meta_miss_is_en() {
        assert_eq!(
            rendered(
                ResponseStyle::Meta(MetaStyle::Get(MetaFlags::default())),
                Command::Get {
                    key: Key::from_stored(b"k")
                },
                Reply::NotFound
            ),
            "EN\r\n"
        );
    }

    #[test]
    fn meta_set_outcomes_use_two_letter_codes() {
        for (outcome, expected) in [
            (Stored::NotStored, "NS\r\n"),
            (Stored::Exists, "EX\r\n"),
            (Stored::NotFound, "NF\r\n"),
        ] {
            assert_eq!(
                rendered(
                    ResponseStyle::Meta(MetaStyle::Set(MetaFlags::default())),
                    Command::Ping,
                    Reply::Stored(outcome)
                ),
                expected
            );
        }
    }

    #[test]
    fn errors_render_in_memcached_form() {
        let mut out = Vec::new();
        encode_error(&mut out, ErrorKind::Error);
        encode_error(&mut out, ErrorKind::Client("bad data chunk"));
        encode_error(&mut out, ErrorKind::Server("out of memory"));
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "ERROR\r\nCLIENT_ERROR bad data chunk\r\nSERVER_ERROR out of memory\r\n"
        );
    }
}
