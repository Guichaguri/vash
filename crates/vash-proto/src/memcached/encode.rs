//! Memcached response rendering.
//!
//! A reply's shape depends on which command asked for it â€” `get` answers with
//! `VALUE`/`END`, `mg` with `HD`/`VA`/`EN`, `incr` with a bare number â€” so the
//! parser records a [`ResponseStyle`] alongside the command and the encoder
//! renders against that. Keeping the choice in the parser means the storage
//! layer never learns that two dialects exist.

use vash_core::{Command, Reply, Stored, Value, ValueRef};

use super::ErrorKind;
use super::meta::MetaFlags;
use crate::digits::{push_i64, push_u64};

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
    /// `STAT <name> <value>` lines, then `END`. The section decides which
    /// counters those are.
    Stats(StatsSection),
    Version,
    Quit,
    Meta(MetaStyle),
    /// An `lru_crawler` key dump. Rendered by the executor rather than here,
    /// because it pages the listing internally — see `dump_line`.
    Dump(Dump),
    /// `stats cachedump`. Rendered by the executor for the same reason.
    CacheDump(CacheDump),
}

/// `stats cachedump <class> <limit>` — upstream's older key dump.
///
/// Superseded upstream by `lru_crawler metadump`, which is also implemented
/// here and is the better command in every respect: it pages the whole keyspace
/// where this returns one capped page, and its `key=value` line has room for an
/// encoding where this positional one does not.
///
/// Implemented anyway because older tooling calls it, and because upstream's own
/// version is *less* complete than this one: `cachedump` walks only the COLD
/// segment of a class's LRU, so a freshly written key does not appear until the
/// maintainer thread has moved it — measured against 1.6.45, where two keys were
/// invisible for several seconds after being stored. There is no LRU here, so
/// every live key in the class is dumped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheDump {
    /// The class argument named this server's one class.
    pub selected: bool,
    /// Entries to return at most, already resolved to `1..=MAX_LIST_LIMIT`.
    ///
    /// Upstream reads a limit of `0` as **no limit**, not as "nothing" —
    /// verified against 1.6.45, where `stats cachedump 1 0` dumped the class.
    /// That is resolved by the parser, so nothing downstream carries a zero
    /// that means the opposite of zero.
    pub limit: u32,
}

/// Which `stats` was asked for.
///
/// The upstream specification declines to document these — "the kinds of
/// arguments and the data sent are not documented in this version of the
/// protocol, and are subject to change for the convenience of memcache
/// developers" — so the *framing* is matched against what memcached 1.6.45
/// actually sends, and the field list is deliberately a subset. See
/// `docs/stats-subcommands.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsSection {
    /// `stats` — the general counters, and the only section that travels as a
    /// [`vash_core::Command::Stats`]. The rest describe the server rather than
    /// the cache, so there is no storage command for them to be.
    General,
    /// `stats settings` — the configuration this node is running.
    Settings,
    /// `stats items` — per slab class, of which there is one.
    Items,
    /// `stats slabs` — what a slab allocator would report. LMDB is not one, so
    /// this is the per-class command counters and the two totals.
    Slabs,
    /// `stats conns` — one block per open connection.
    Conns,
    /// `stats sizes` — the item-size histogram, which upstream only keeps under
    /// `-o track_sizes`. Answering `sizes_status disabled` is **byte-identical
    /// to a stock memcached**, and is a constant rather than an approximation.
    Sizes,
    /// `stats extstore` and `stats proxy` — a bare `END`, which is what a
    /// memcached without external storage or the proxy compiled in answers.
    /// There is neither here, so the empty reply is exact.
    Empty,
}

/// Which dump was asked for, and whether its class argument named anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dump {
    pub style: DumpStyle,
    /// The class argument selected this server's one class. When clear, the
    /// answer is a bare terminator: the class named is genuinely empty, so
    /// there is nothing to scan for.
    pub selected: bool,
}

/// The two `lru_crawler` dumps.
///
/// One walk, two line formats. `metadump` describes each key for a reader;
/// `mgdump` emits a ready-to-send `mg` command, so a dump is its own replay
/// script — which is also why its terminator is the meta protocol's `EN`
/// rather than `END`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpStyle {
    Meta,
    Mg,
}

impl DumpStyle {
    /// The acknowledgement upstream sends before a single key.
    ///
    /// Verified against `memcached:1.6-alpine`: a dump is `OK`, then the lines,
    /// then the terminator — and the `OK` comes even when the class named holds
    /// nothing. A client that reads it as the first data line would lose a key,
    /// so it is not optional.
    pub const ACK: &'static [u8] = b"OK\r\n";

    /// What ends the dump when it completes.
    ///
    /// Never written when the dump was cut short — see the executor. A tool
    /// reads lines until it sees this, so a truncated dump ending in one would
    /// claim the keyspace is smaller than it is.
    ///
    /// `mgdump` ends with the meta protocol's `EN` rather than `END`, which is
    /// what lets its output be piped back in as commands.
    pub fn terminator(self) -> &'static [u8] {
        match self {
            Self::Meta => b"END\r\n",
            Self::Mg => b"EN\r\n",
        }
    }
}

/// The slab class this server reports.
///
/// There are no slab classes here, so everything is in one and this is both the
/// `cls=` a dump prints and the class id its argument accepts. One constant, so
/// the two cannot drift apart.
pub const DUMP_CLASS: &[u8] = b"1";

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
            // `to_text` rather than a counter-specific render: it is the same
            // decimal text in every numeric domain, and this dialect only ever
            // produces the unsigned one.
            Reply::Arithmetic(applied) => {
                out.extend_from_slice(applied.value.to_text().as_bytes());
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

        ResponseStyle::Stats(_) => {
            if let Reply::Stats(entries) = reply {
                stat_lines(out, entries);
            }
            out.extend_from_slice(b"END\r\n");
        }

        // The connection closes; nothing is sent.
        ResponseStyle::Quit => {}

        ResponseStyle::Meta(meta) => encode_meta(out, meta, command, reply),

        // Written line by line as the executor pages the listing, so there is
        // no single `Reply` to render here. Reaching either arm means a dump was
        // routed through the ordinary path; answering with the terminator keeps
        // the stream framed rather than leaving the client waiting.
        ResponseStyle::Dump(dump) => out.extend_from_slice(dump.style.terminator()),
        ResponseStyle::CacheDump(_) => out.extend_from_slice(b"END\r\n"),
    }
}

/// Appends one `stats cachedump` line.
///
/// ```text
/// ITEM session%3A01 [0 b; 1786683924 s]
/// ```
///
/// **`size` is always `0` and must not be read.** The field cannot be omitted —
/// this is a positional bracket format, unlike `metadump`'s `key=value` pairs,
/// so dropping it would break every parser that reads the line. Carrying a real
/// length would mean a `value_len` on every `ListEntry`, which every VCP listing
/// would pay for and never read; `mg <key> s` answers the size of one key
/// without that. So the field is present, constant, and documented as
/// meaningless — the one place in this server where a zero does not mean a
/// measurement, and it is spelled out in `docs/protocol.md` because of it.
///
/// `exp` is absolute unix seconds, and **`0` means "never expires"** — note that
/// `metadump` spells the same thing `-1`. That asymmetry is upstream's, verified
/// against 1.6.45, and is reproduced rather than tidied.
pub fn cachedump_line(out: &mut Vec<u8>, entry: &vash_core::ListEntry) {
    out.extend_from_slice(b"ITEM ");
    // `Percent::Literal`, as `mgdump` uses: every printable byte passes through
    // including `%`, so the line is byte-identical to upstream's for every key
    // a memcached client could have written. Upstream does not encode here at
    // all — it never has to, because its own parser refuses to store a key with
    // a space or a CRLF in it. This keyspace is shared with Redis and VCP
    // clients that can, and such a key would otherwise end the line early and
    // let the rest of it be read as further items.
    uriencode(out, &entry.name, Percent::Literal);

    out.extend_from_slice(b" [0 b; ");
    match entry.expires_at_ms {
        Some(vash_core::NEVER) | None => out.push(b'0'),
        Some(at) => push_u64(out, at / 1_000),
    }
    out.extend_from_slice(b" s]\r\n");
}

/// Appends `STAT <name> <value>` lines, without a terminator.
///
/// Shared by every `stats` section: they differ in which counters they carry,
/// never in how a counter is written.
pub fn stat_lines(out: &mut Vec<u8>, entries: &[(String, String)]) {
    for (name, value) in entries {
        out.extend_from_slice(b"STAT ");
        out.extend_from_slice(name.as_bytes());
        out.push(b' ');
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

/// Appends one dumped key.
///
/// `metadump` is measured against upstream's specification, which guarantees
/// only that the fields "will include at least" `key`, `exp`, `la`, `cas` and
/// `fetch`. Of those, `la` and `fetch` are LRU bookkeeping and there is no LRU
/// here (plan §6), so they are **omitted rather than zeroed** — a `la=0` would
/// claim every key was last touched at the epoch. `size` is not in the
/// guaranteed set and is absent for a different reason: it would mean carrying a
/// value length on every `ListEntry` that every VCP listing pays for and never
/// reads, and `mg <key> s` already answers it per key.
pub fn dump_line(out: &mut Vec<u8>, style: DumpStyle, entry: &vash_core::ListEntry) {
    match style {
        // `mg <key>\r\n`, and **`%` is left alone** — upstream does not encode
        // it here, and this line is meant to be sent back as a command, so a
        // key holding a `%` has to replay as itself. Verified against 1.6.
        DumpStyle::Mg => {
            out.extend_from_slice(b"mg ");
            uriencode(out, &entry.name, Percent::Literal);
            out.extend_from_slice(b"\r\n");
        }

        // Space-separated `field=value` pairs, a **trailing space**, and a bare
        // `\n` — not a CRLF. Both are upstream's, verified byte for byte, and
        // both are the kind of detail a parser keyed on the line ending notices.
        DumpStyle::Meta => {
            out.extend_from_slice(b"key=");
            uriencode(out, &entry.name, Percent::Encoded);

            out.extend_from_slice(b" exp=");
            match entry.expires_at_ms {
                // `-1` is upstream's "never expires" in this line, and the same
                // convention `Value::remaining_ttl_secs` already uses.
                Some(vash_core::NEVER) | None => out.extend_from_slice(b"-1"),
                Some(at) => push_u64(out, at / 1_000),
            }

            out.extend_from_slice(b" cas=");
            push_u64(out, entry.version);

            out.extend_from_slice(b" cls=");
            out.extend_from_slice(DUMP_CLASS);
            out.extend_from_slice(b" \n");
        }
    }
}

/// Whether `%` is itself encoded.
///
/// Upstream's own asymmetry, reproduced rather than tidied: `metadump` encodes
/// it, `mgdump` does not. The two lines have different jobs — one is read, one
/// is replayed — and a client parsing either against a real memcached must see
/// what a real memcached sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Percent {
    Encoded,
    Literal,
}

/// Percent-encodes a key, as upstream does.
///
/// **Load-bearing rather than cosmetic.** The keyspace is shared across
/// dialects, so a Redis or VCP client can store a key holding a space, a control
/// byte or a CRLF — none of which the memcached parser could have produced, and
/// any of which would corrupt a dump line and desynchronise the reader. Encoding
/// turns that into a non-issue with no keys skipped and no escape scheme nobody
/// parses.
///
/// Upstream never meets such a key, because its own parser refuses to store one;
/// this server can, which is why the encoding matters more here than there.
fn uriencode(out: &mut Vec<u8>, key: &[u8], percent: Percent) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in key {
        let literal = matches!(byte, 0x21..=0x7e) && (*byte != b'%' || percent == Percent::Literal);
        if literal {
            out.push(*byte);
        } else {
            out.push(b'%');
            out.push(HEX[(byte >> 4) as usize]);
            out.push(HEX[(byte & 0x0f) as usize]);
        }
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
            push_u64(out, u64::from(value.mc_flags));
            out.push(b' ');
            push_u64(out, value.data.len() as u64);
            if with_cas {
                out.push(b' ');
                push_u64(out, value.cas);
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
                Reply::Value(value) => Some(value.borrowed()),
                Reply::Values(values) => {
                    values.first().and_then(|v| v.as_ref()).map(Value::borrowed)
                }
                _ => None,
            };

            let Some(value) = value else {
                out.extend_from_slice(b"EN\r\n");
                return;
            };
            encode_meta_get_hit(out, flags, command, value);
        }

        MetaStyle::Set(flags) => {
            match reply {
                Reply::Stored(Stored::Stored(cas)) => {
                    out.extend_from_slice(b"HD");
                    if flags.want_cas {
                        out.extend_from_slice(b" c");
                        push_u64(out, *cas);
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
            Reply::Arithmetic(applied) => {
                let text = applied.value.to_text();
                if flags.want_value {
                    out.extend_from_slice(b"VA ");
                    push_u64(out, text.len() as u64);
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
                push_u64(out, value.cas);
                out.extend_from_slice(b" size=");
                push_u64(out, value.data.len() as u64);
                out.extend_from_slice(b" fetch=yes\r\n");
            }
            _ => out.extend_from_slice(b"EN\r\n"),
        },
    }
}

/// Appends the return flags a meta request asked for, in a stable order.
/// Renders an `mg` hit: the `VA <size>`/`HD` line, its return flags, and the
/// payload when one was asked for.
///
/// Split out and taking a borrowed value so the fused read path can call it
/// with a value that is still in the memory map, while the owned path above
/// reaches it through [`vash_core::Value::borrowed`]. One rendering either way.
pub fn encode_meta_get_hit(
    out: &mut Vec<u8>,
    flags: &MetaFlags,
    command: &Command<'_>,
    value: ValueRef<'_>,
) {
    if flags.want_value {
        out.extend_from_slice(b"VA ");
        push_u64(out, value.data.len() as u64);
    } else {
        out.extend_from_slice(b"HD");
    }
    write_return_flags(out, flags, command, Some(value));
    out.extend_from_slice(b"\r\n");

    if flags.want_value {
        out.extend_from_slice(value.data);
        out.extend_from_slice(b"\r\n");
    }
}

fn write_return_flags(
    out: &mut Vec<u8>,
    flags: &MetaFlags,
    command: &Command<'_>,
    value: Option<ValueRef<'_>>,
) {
    if let Some(value) = value {
        if flags.want_client_flags {
            out.extend_from_slice(b" f");
            push_u64(out, u64::from(value.mc_flags));
        }
        if flags.want_size {
            out.extend_from_slice(b" s");
            push_u64(out, value.data.len() as u64);
        }
        if flags.want_cas {
            out.extend_from_slice(b" c");
            push_u64(out, value.cas);
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
            push_i64(out, remaining);
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
        Command::Arithmetic(op) => Some(op.key.as_bytes()),
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
            rendered(
                ResponseStyle::Counter,
                Command::Ping,
                Reply::Arithmetic(vash_core::Applied {
                    value: vash_core::Number::Counter(41),
                    applied: vash_core::Number::Counter(1),
                    wrote: true
                })
            ),
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

    fn dumped(style: DumpStyle, name: &[u8], version: u64, expires_at_ms: Option<u64>) -> String {
        let mut entry = vash_core::ListEntry::new(name.to_vec(), version);
        entry.expires_at_ms = expires_at_ms;
        let mut out = Vec::new();
        dump_line(&mut out, style, &entry);
        String::from_utf8(out).expect("dump lines are ascii once encoded")
    }

    /// Checked against `memcached:1.6-alpine` byte for byte: a **trailing
    /// space** and a **bare `\n`**, not a CRLF. Both are the kind of detail a
    /// parser keyed on line endings would notice.
    #[test]
    fn a_metadump_line_matches_upstreams_framing() {
        assert_eq!(
            dumped(DumpStyle::Meta, b"alpha", 4, Some(vash_core::NEVER)),
            "key=alpha exp=-1 cas=4 cls=1 \n"
        );
        assert_eq!(
            dumped(DumpStyle::Meta, b"beta", 7, Some(1_786_605_851_000)),
            "key=beta exp=1786605851 cas=7 cls=1 \n"
        );
        // A listing that carries no deadline — a tag, or an entry off the wire
        // — reports "never" rather than inventing one.
        assert_eq!(
            dumped(DumpStyle::Meta, b"gamma", 1, None),
            "key=gamma exp=-1 cas=1 cls=1 \n"
        );
    }

    /// `mgdump` emits a command, so its line ends in CRLF like every other
    /// command does.
    #[test]
    fn an_mgdump_line_is_a_command() {
        assert_eq!(dumped(DumpStyle::Mg, b"alpha", 4, None), "mg alpha\r\n");
    }

    /// The keyspace is shared across dialects, so a Redis or VCP client can
    /// store a key that would otherwise end the dump early and inject a line of
    /// its own choosing.
    #[test]
    fn a_key_that_would_break_the_framing_is_encoded_rather_than_skipped() {
        let hostile = b"a b\r\nEND\r\n";
        let line = dumped(DumpStyle::Meta, hostile, 1, None);
        assert_eq!(line, "key=a%20b%0D%0AEND%0D%0A exp=-1 cas=1 cls=1 \n");
        assert_eq!(line.matches('\n').count(), 1, "still exactly one line");

        // High bytes and control characters go the same way.
        assert_eq!(
            dumped(DumpStyle::Meta, b"\x00\xff", 1, None),
            "key=%00%FF exp=-1 cas=1 cls=1 \n"
        );
    }

    /// Upstream's own asymmetry, reproduced rather than tidied: `metadump`
    /// encodes `%` and `mgdump` does not, because the second is meant to be sent
    /// back and would otherwise name a different key.
    #[test]
    fn percent_is_encoded_in_a_metadump_and_left_alone_in_an_mgdump() {
        assert!(dumped(DumpStyle::Meta, b"be%ta", 1, None).starts_with("key=be%25ta "));
        assert_eq!(dumped(DumpStyle::Mg, b"be%ta", 1, None), "mg be%ta\r\n");
    }

    #[test]
    fn the_dumps_end_with_the_terminator_their_readers_expect() {
        assert_eq!(DumpStyle::Meta.terminator(), b"END\r\n");
        assert_eq!(DumpStyle::Mg.terminator(), b"EN\r\n");
        assert_eq!(DumpStyle::ACK, b"OK\r\n");
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
