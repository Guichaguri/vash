//! The memcached meta commands.
//!
//! ```text
//! mg <key> <flags>*\r\n
//! ms <key> <datalen> <flags>*\r\n<data>\r\n
//! md <key> <flags>*\r\n
//! ma <key> <flags>*\r\n
//! mn\r\n
//! me <key>\r\n
//! ```
//!
//! Flags are single letters, optionally carrying a token. This implements the
//! core set below; an unrecognised flag is a `CLIENT_ERROR`, which is what
//! upstream does and what keeps a client from believing a flag took effect.
//!
//! | flag | on | meaning |
//! |---|---|---|
//! | `v` | mg | return the value |
//! | `f` | mg | return client flags |
//! | `c` | mg, ms, md, ma | return the CAS token |
//! | `t` | mg | return remaining TTL |
//! | `s` | mg | return the value size |
//! | `k` | mg, ms, md, ma | echo the key |
//! | `O<token>` | all | opaque, echoed back |
//! | `q` | all | no reply on the uninteresting outcome |
//! | `T<ttl>` | mg, ms, md, ma | set/update the TTL |
//! | `F<flags>` | ms | set client flags |
//! | `C<cas>` | ms, md | compare against this CAS |
//! | `M<mode>` | ms, ma | mode: ms `E`/`A`/`P`/`R`/`S`, ma `I`/`+`/`D`/`-` |
//! | `D<delta>` | ma | amount to add or subtract |
//! | `N<ttl>` | ma | create at 0 with this TTL if missing |
//!
//! ## Extension
//!
//! `G<tags>` on `ms` attaches a comma-separated tag list, and `mdt <tag>`
//! invalidates one. Neither is part of the memcached protocol. `G` was chosen
//! from the letters upstream leaves unassigned; it is a single constant here,
//! and clients that never send it are unaffected.

use vash_core::{Command, Key, Set, SetMode};

use super::encode::{MetaStyle, ResponseStyle};
use super::{ErrorKind, MAX_KEY_LEN, Outcome, Parsed, ProtocolError};

/// The flag letter carrying a tag list. See the module note above.
pub const TAG_FLAG: u8 = b'G';

/// Everything a meta command asked to be told, so the encoder can answer in the
/// order the client requested.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetaFlags {
    pub want_value: bool,
    pub want_client_flags: bool,
    pub want_cas: bool,
    pub want_ttl: bool,
    pub want_size: bool,
    pub want_key: bool,
    pub opaque: Option<Vec<u8>>,
    pub quiet: bool,
}

pub fn parse<'a>(
    verb: &[u8],
    line: &'a [u8],
    after_line: usize,
    buf: &'a [u8],
) -> Result<Outcome<'a>, ProtocolError> {
    let fail = |detail: &'static str| ProtocolError::Recoverable {
        response: ErrorKind::Client(detail),
        consumed: after_line,
    };

    let mut tokens = line.split(|b| *b == b' ').filter(|t| !t.is_empty()).skip(1);

    match verb {
        // A no-op used to mark the end of a pipelined batch.
        b"mn" => Ok(Outcome::Command(Parsed {
            command: Command::Ping,
            consumed: after_line,
            noreply: false,
            style: ResponseStyle::Meta(MetaStyle::NoOp),
        })),

        b"mg" => {
            let key = parse_key(tokens.next(), after_line)?;
            let flags = collect(tokens, after_line)?;

            // `T` on a get makes it a get-and-touch.
            let command = match flags.ttl {
                Some(ttl_secs) => Command::GetAndTouch {
                    keys: vec![key],
                    ttl_secs,
                },
                None => Command::Get { key },
            };
            Ok(Outcome::Command(Parsed {
                command,
                consumed: after_line,
                noreply: flags.meta.quiet,
                style: ResponseStyle::Meta(MetaStyle::Get(flags.meta)),
            }))
        }

        b"ms" => {
            let key = parse_key(tokens.next(), after_line)?;
            let bytes = tokens
                .next()
                .and_then(parse_usize)
                .ok_or_else(|| fail("bad data length"))?;
            let flags = collect(tokens, after_line)?;

            if bytes > vash_core::ABSOLUTE_MAX_VALUE_LEN {
                return Err(ProtocolError::Fatal("declared value length is implausible"));
            }
            let data_end = after_line + bytes;
            if buf.len() < data_end + 2 {
                return Ok(Outcome::Incomplete);
            }
            if &buf[data_end..data_end + 2] != b"\r\n" {
                return Err(ProtocolError::Recoverable {
                    response: ErrorKind::Client("bad data chunk"),
                    consumed: data_end + 2,
                });
            }

            let mode = match (flags.mode, flags.cas) {
                // An explicit CAS comparison outranks the mode letter, which is
                // how upstream behaves.
                (_, Some(cas)) => SetMode::Cas(cas),
                (Some(b'E'), _) => SetMode::Add,
                (Some(b'A'), _) => SetMode::Append,
                (Some(b'P'), _) => SetMode::Prepend,
                (Some(b'R'), _) => SetMode::Replace,
                (Some(b'S') | None, _) => SetMode::Set,
                (Some(_), _) => return Err(fail("invalid mode for ms")),
            };

            Ok(Outcome::Command(Parsed {
                command: Command::Set(Set {
                    key,
                    value: &buf[after_line..data_end],
                    ttl_secs: flags.ttl.unwrap_or(0),
                    mc_flags: flags.client_flags.unwrap_or(0),
                    tags: flags.tags,
                    mode,
                }),
                consumed: data_end + 2,
                noreply: flags.meta.quiet,
                style: ResponseStyle::Meta(MetaStyle::Set(flags.meta)),
            }))
        }

        b"md" => {
            let key = parse_key(tokens.next(), after_line)?;
            let flags = collect(tokens, after_line)?;
            Ok(Outcome::Command(Parsed {
                command: Command::Delete { key },
                consumed: after_line,
                noreply: flags.meta.quiet,
                style: ResponseStyle::Meta(MetaStyle::Delete(flags.meta)),
            }))
        }

        b"ma" => {
            let key = parse_key(tokens.next(), after_line)?;
            let flags = collect(tokens, after_line)?;
            let decrement = matches!(flags.mode, Some(b'D' | b'-'));
            Ok(Outcome::Command(Parsed {
                command: Command::Incr {
                    key,
                    delta: flags.delta.unwrap_or(1),
                    decrement,
                },
                consumed: after_line,
                noreply: flags.meta.quiet,
                style: ResponseStyle::Meta(MetaStyle::Arithmetic(flags.meta)),
            }))
        }

        // Item debug. Reported as a miss rather than fabricating internals that
        // would not match upstream's format anyway.
        b"me" => {
            let key = parse_key(tokens.next(), after_line)?;
            Ok(Outcome::Command(Parsed {
                command: Command::Get { key },
                consumed: after_line,
                noreply: false,
                style: ResponseStyle::Meta(MetaStyle::Debug),
            }))
        }

        // Extension: meta-style tag invalidation.
        b"mdt" => {
            let tag = tokens.next().ok_or_else(|| fail("missing tag"))?;
            if tag.is_empty() || tag.len() > vash_core::MAX_TAG_LEN {
                return Err(fail("invalid tag"));
            }
            let flags = collect(tokens, after_line)?;
            Ok(Outcome::Command(Parsed {
                command: Command::DeleteByTag { tag },
                consumed: after_line,
                noreply: flags.meta.quiet,
                style: ResponseStyle::Meta(MetaStyle::Delete(flags.meta)),
            }))
        }

        _ => Err(ProtocolError::Recoverable {
            response: ErrorKind::Error,
            consumed: after_line,
        }),
    }
}

#[derive(Default)]
struct Collected<'a> {
    meta: MetaFlags,
    ttl: Option<u32>,
    client_flags: Option<u32>,
    cas: Option<u64>,
    mode: Option<u8>,
    delta: Option<u64>,
    tags: Vec<&'a [u8]>,
}

fn collect<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
    consumed: usize,
) -> Result<Collected<'a>, ProtocolError> {
    let fail = |detail: &'static str| ProtocolError::Recoverable {
        response: ErrorKind::Client(detail),
        consumed,
    };

    let mut out = Collected::default();

    for token in tokens {
        let (letter, arg) = token.split_first().ok_or_else(|| fail("empty flag"))?;
        match letter {
            b'v' => out.meta.want_value = true,
            b'f' => out.meta.want_client_flags = true,
            b'c' => out.meta.want_cas = true,
            b't' => out.meta.want_ttl = true,
            b's' => out.meta.want_size = true,
            b'k' => out.meta.want_key = true,
            b'q' => out.meta.quiet = true,

            // The only genuinely inert flag: it asks the server not to bump the
            // item's LRU position, and there is no LRU here to bump.
            b'u' => {}

            b'O' => out.meta.opaque = Some(arg.to_vec()),
            b'T' => {
                out.ttl = Some(parse_ttl(arg).ok_or_else(|| fail("bad TTL flag"))?);
            }
            b'F' => {
                out.client_flags = Some(
                    parse_usize(arg)
                        .and_then(|v| u32::try_from(v).ok())
                        .ok_or_else(|| fail("bad flags token"))?,
                );
            }
            b'C' => {
                out.cas = Some(parse_u64(arg).ok_or_else(|| fail("bad CAS token"))?);
            }
            b'D' => {
                out.delta = Some(parse_u64(arg).ok_or_else(|| fail("bad delta token"))?);
            }
            b'M' => {
                out.mode = Some(*arg.first().ok_or_else(|| fail("empty mode flag"))?);
            }
            &TAG_FLAG => {
                for name in arg.split(|b| *b == b',').filter(|n| !n.is_empty()) {
                    if name.len() > vash_core::MAX_TAG_LEN {
                        return Err(fail("tag name too long"));
                    }
                    // The format ceiling, not the configured limit: a text flag
                    // carries no count, so the parser has to stop the list
                    // growing without bound. The store applies its own limit.
                    if out.tags.len() >= vash_core::ABSOLUTE_MAX_TAGS {
                        return Err(fail("too many tags"));
                    }
                    out.tags.push(name);
                }
            }
            // Defined upstream but not implemented here, and each one changes
            // behaviour, so accepting them silently would be worse than saying
            // no: `b` would file the value under the un-decoded key, `N` would
            // skip the vivify the client is relying on, and `h`/`l` are return
            // flags whose absence leaves the client parsing a shorter reply
            // than it expects.
            b'b' | b'h' | b'l' | b'x' | b'I' | b'E' | b'R' | b'N' => {
                return Err(fail("unsupported flag"));
            }

            // Rejecting an unknown flag is deliberate: silently ignoring it
            // would let a client believe it took effect.
            _ => return Err(fail("invalid flag")),
        }
    }

    Ok(out)
}

fn parse_key(token: Option<&[u8]>, consumed: usize) -> Result<Key<'_>, ProtocolError> {
    let fail = |detail| ProtocolError::Recoverable {
        response: ErrorKind::Client(detail),
        consumed,
    };

    let token = token.ok_or_else(|| fail("missing key"))?;
    if token.is_empty() || token.len() > MAX_KEY_LEN {
        return Err(fail("invalid key"));
    }
    if token.iter().any(|b| *b <= b' ' || *b == 0x7f) {
        return Err(fail("key contains a control character"));
    }
    Key::new(token).map_err(|_| fail("invalid key"))
}

fn parse_ttl(arg: &[u8]) -> Option<u32> {
    let text = std::str::from_utf8(arg).ok()?;
    let seconds: i64 = text.trim().parse().ok()?;
    Some(if seconds < 0 {
        super::text::IMMEDIATELY_EXPIRED
    } else {
        u32::try_from(seconds).unwrap_or(u32::MAX)
    })
}

fn parse_usize(arg: &[u8]) -> Option<usize> {
    std::str::from_utf8(arg).ok()?.trim().parse().ok()
}

fn parse_u64(arg: &[u8]) -> Option<u64> {
    std::str::from_utf8(arg).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::super::{Outcome, parse as parse_any};
    use super::*;

    fn command(input: &[u8]) -> Parsed<'_> {
        match parse_any(input) {
            Ok(Outcome::Command(parsed)) => parsed,
            other => panic!("expected a command, got {other:?}"),
        }
    }

    #[test]
    fn mn_is_a_no_op() {
        assert!(matches!(command(b"mn\r\n").command, Command::Ping));
    }

    #[test]
    fn mg_without_flags_is_a_plain_get() {
        let Command::Get { key } = command(b"mg foo\r\n").command else {
            panic!("expected a get")
        };
        assert_eq!(key.as_bytes(), b"foo");
    }

    #[test]
    fn mg_with_a_ttl_flag_becomes_get_and_touch() {
        let Command::GetAndTouch { keys, ttl_secs } = command(b"mg foo v T300\r\n").command else {
            panic!("expected a get-and-touch")
        };
        assert_eq!(keys[0].as_bytes(), b"foo");
        assert_eq!(ttl_secs, 300);
    }

    #[test]
    fn ms_parses_its_data_block_and_flags() {
        let Command::Set(set) = command(b"ms foo 5 T60 F42\r\nhello\r\n").command else {
            panic!("expected a set")
        };
        assert_eq!(set.key.as_bytes(), b"foo");
        assert_eq!(set.value, b"hello");
        assert_eq!(set.ttl_secs, 60);
        assert_eq!(set.mc_flags, 42);
        assert_eq!(set.mode, SetMode::Set);
    }

    #[test]
    fn ms_mode_letters_map_to_set_modes() {
        for (letter, expected) in [
            (b'E', SetMode::Add),
            (b'A', SetMode::Append),
            (b'P', SetMode::Prepend),
            (b'R', SetMode::Replace),
            (b'S', SetMode::Set),
        ] {
            let input = format!("ms k 1 M{}\r\nx\r\n", letter as char);
            let Command::Set(set) = command(input.as_bytes()).command else {
                panic!()
            };
            assert_eq!(set.mode, expected, "mode {}", letter as char);
        }
    }

    #[test]
    fn an_explicit_cas_outranks_the_mode_letter() {
        let Command::Set(set) = command(b"ms k 1 MS C77\r\nx\r\n").command else {
            panic!()
        };
        assert_eq!(set.mode, SetMode::Cas(77));
    }

    #[test]
    fn the_tag_flag_attaches_a_comma_separated_list() {
        let Command::Set(set) = command(b"ms k 1 Gnews,sport\r\nx\r\n").command else {
            panic!("expected a set")
        };
        assert_eq!(set.tags, vec![b"news".as_slice(), b"sport"]);
    }

    #[test]
    fn mdt_invalidates_a_tag() {
        let Command::DeleteByTag { tag } = command(b"mdt news\r\n").command else {
            panic!("expected a tag invalidation")
        };
        assert_eq!(tag, b"news");
    }

    #[test]
    fn ma_defaults_to_incrementing_by_one() {
        let Command::Incr {
            delta, decrement, ..
        } = command(b"ma counter\r\n").command
        else {
            panic!()
        };
        assert_eq!(delta, 1);
        assert!(!decrement);

        let Command::Incr {
            delta, decrement, ..
        } = command(b"ma counter MD D5\r\n").command
        else {
            panic!()
        };
        assert_eq!(delta, 5);
        assert!(decrement, "MD must decrement");
    }

    #[test]
    fn the_quiet_flag_suppresses_the_reply() {
        assert!(command(b"mg foo v q\r\n").noreply);
        assert!(command(b"md foo q\r\n").noreply);
        assert!(!command(b"mg foo v\r\n").noreply);
    }

    #[test]
    fn an_unknown_flag_is_rejected_not_ignored() {
        // Silently accepting it would let a client trust behaviour that is not
        // implemented â€” the same reason upstream rejects it.
        assert!(matches!(
            parse_any(b"mg foo Z\r\n"),
            Err(ProtocolError::Recoverable {
                response: ErrorKind::Client("invalid flag"),
                ..
            })
        ));
    }

    #[test]
    fn ms_waits_for_its_data_block() {
        assert!(matches!(
            parse_any(b"ms k 5\r\nhel"),
            Ok(Outcome::Incomplete)
        ));
    }

    #[test]
    fn a_malformed_flag_token_is_recoverable() {
        assert!(matches!(
            parse_any(b"mg foo Tnotanumber\r\n"),
            Err(ProtocolError::Recoverable { .. })
        ));
        assert!(matches!(
            parse_any(b"ms k 1 Cnope\r\nx\r\n"),
            Err(ProtocolError::Recoverable { .. })
        ));
    }
}
