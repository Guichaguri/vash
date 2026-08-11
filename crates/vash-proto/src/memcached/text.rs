//! The classic memcached text commands.
//!
//! ```text
//! <storage> <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
//! get|gets <key>+\r\n
//! gat|gats <exptime> <key>+\r\n
//! delete <key> [noreply]\r\n
//! touch <key> <exptime> [noreply]\r\n
//! incr|decr <key> <delta> [noreply]\r\n
//! flush_all [delay] [noreply]\r\n
//! ```

use vash_core::{Command, Key, Set, SetMode};

use super::encode::ResponseStyle;
use super::{ErrorKind, MAX_KEY_LEN, Outcome, Parsed, ProtocolError};

/// memcached's catch-all rejection for a malformed command line. Reproduced
/// verbatim: the differential suite compares response bytes against a real
/// server, so a friendlier message would read as a divergence.
pub(crate) const BAD_LINE: &str = "bad command line format";

pub fn parse<'a>(
    verb: &[u8],
    line: &'a [u8],
    after_line: usize,
    buf: &'a [u8],
) -> Result<Outcome<'a>, ProtocolError> {
    let consumed = after_line;

    let mut tokens = line.split(|b| *b == b' ').filter(|t| !t.is_empty()).skip(1);

    let fail = |kind: ErrorKind| ProtocolError::Recoverable {
        response: kind,
        consumed,
    };

    match verb {
        b"set" | b"add" | b"replace" | b"append" | b"prepend" | b"cas" => {
            parse_storage(verb, line, buf, after_line)
        }

        b"get" | b"gets" => {
            let keys = parse_keys(tokens, consumed)?;
            Ok(Outcome::Command(Parsed {
                command: Command::GetMany(keys),
                consumed,
                noreply: false,
                style: ResponseStyle::Retrieval {
                    with_cas: verb == b"gets",
                },
            }))
        }

        b"gat" | b"gats" => {
            let ttl_secs = parse_exptime(tokens.next(), consumed)?;
            let keys = parse_keys(tokens, consumed)?;
            Ok(Outcome::Command(Parsed {
                command: Command::GetAndTouch { keys, ttl_secs },
                consumed,
                noreply: false,
                style: ResponseStyle::Retrieval {
                    with_cas: verb == b"gats",
                },
            }))
        }

        b"delete" => {
            let key = parse_key(tokens.next(), consumed)?;
            let noreply = has_noreply(tokens);
            Ok(Outcome::Command(Parsed {
                command: Command::Delete { key },
                consumed,
                noreply,
                style: ResponseStyle::Deleted,
            }))
        }

        b"touch" => {
            let key = parse_key(tokens.next(), consumed)?;
            let ttl_secs = parse_exptime(tokens.next(), consumed)?;
            let noreply = has_noreply(tokens);
            Ok(Outcome::Command(Parsed {
                command: Command::Touch { key, ttl_secs },
                consumed,
                noreply,
                style: ResponseStyle::Touched,
            }))
        }

        b"incr" | b"decr" => {
            let key = parse_key(tokens.next(), consumed)?;
            let delta = parse_u64(tokens.next())
                .ok_or_else(|| fail(ErrorKind::Client("invalid numeric delta argument")))?;
            let noreply = has_noreply(tokens);
            Ok(Outcome::Command(Parsed {
                command: Command::Incr {
                    key,
                    delta,
                    decrement: verb == b"decr",
                },
                consumed,
                noreply,
                style: ResponseStyle::Counter,
            }))
        }

        // The delay argument is accepted for compatibility but not honoured:
        // a deferred wipe would need a scheduler, and every client that sends
        // one sends 0.
        b"flush_all" => {
            let noreply = has_noreply(tokens);
            Ok(Outcome::Command(Parsed {
                command: Command::Flush,
                consumed,
                noreply,
                style: ResponseStyle::Ok,
            }))
        }

        // Extension: invalidate every key carrying a tag. Not part of the
        // memcached protocol, and named distinctly so it cannot collide with a
        // future upstream command.
        b"delete_by_tag" => {
            let tag = tokens
                .next()
                .ok_or_else(|| fail(ErrorKind::Client("missing tag")))?;
            if tag.is_empty() || tag.len() > vash_core::MAX_TAG_LEN {
                return Err(fail(ErrorKind::Client("invalid tag")));
            }
            let noreply = has_noreply(tokens);
            Ok(Outcome::Command(Parsed {
                command: Command::DeleteByTag { tag },
                consumed,
                noreply,
                style: ResponseStyle::Deleted,
            }))
        }

        b"stats" => Ok(Outcome::Command(Parsed {
            command: Command::Stats,
            consumed,
            noreply: false,
            style: ResponseStyle::Stats,
        })),
        b"version" => Ok(Outcome::Command(Parsed {
            command: Command::Version,
            consumed,
            noreply: false,
            style: ResponseStyle::Version,
        })),
        b"quit" => Ok(Outcome::Command(Parsed {
            command: Command::Quit,
            consumed,
            noreply: false,
            style: ResponseStyle::Quit,
        })),
        // Accepted and ignored: it only controls server-side log verbosity.
        b"verbosity" => Ok(Outcome::Command(Parsed {
            command: Command::Version,
            consumed,
            noreply: has_noreply(tokens),
            style: ResponseStyle::Version,
        })),

        // An empty line is a malformed command, not an unknown one — which is
        // the distinction memcached draws, and it shows up in practice after a
        // rejected data block leaves a stray terminator behind.
        b"" => Err(fail(ErrorKind::Client(BAD_LINE))),

        _ => Err(fail(ErrorKind::Error)),
    }
}

/// `<verb> <key> <flags> <exptime> <bytes> [<cas>] [noreply]\r\n<data>\r\n`
fn parse_storage<'a>(
    verb: &[u8],
    line: &'a [u8],
    buf: &'a [u8],
    line_consumed: usize,
) -> Result<Outcome<'a>, ProtocolError> {
    let tokens: Vec<&[u8]> = line
        .split(|b| *b == b' ')
        .filter(|t| !t.is_empty())
        .collect();

    // The declared length is read before anything is validated, so that a
    // malformed line still consumes its data block. Skipping the block would
    // leave the value in the stream to be parsed as commands, and every request
    // after it on that connection would be misread.
    let bytes = parse_u64(tokens.get(4).copied())
        .and_then(|v| usize::try_from(v).ok())
        .ok_or(ProtocolError::Recoverable {
            response: ErrorKind::Client(BAD_LINE),
            consumed: line_consumed,
        })?;

    // Reject an absurd length before waiting for that many bytes to arrive,
    // or a hostile client could pin a connection's buffer at will.
    if bytes > vash_core::ABSOLUTE_MAX_VALUE_LEN {
        return Err(ProtocolError::Fatal("declared value length is implausible"));
    }

    // The block plus its own CRLF must have arrived before any verdict is
    // reported, because every one of them consumes it.
    let data_end = line_consumed + bytes;
    if buf.len() < data_end + 2 {
        return Ok(Outcome::Incomplete);
    }
    let consumed = data_end + 2;

    let fail = |kind: ErrorKind| ProtocolError::Recoverable {
        response: kind,
        consumed,
    };

    if &buf[data_end..data_end + 2] != b"\r\n" {
        return Err(fail(ErrorKind::Client("bad data chunk")));
    }

    let key = parse_key(tokens.get(1).copied(), consumed)?;
    let mc_flags = parse_u64(tokens.get(2).copied())
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| fail(ErrorKind::Client(BAD_LINE)))?;
    let ttl_secs = parse_exptime(tokens.get(3).copied(), consumed)?;

    let mode = match verb {
        b"set" => SetMode::Set,
        b"add" => SetMode::Add,
        b"replace" => SetMode::Replace,
        b"append" => SetMode::Append,
        b"prepend" => SetMode::Prepend,
        b"cas" => {
            let cas = parse_u64(tokens.get(5).copied())
                .ok_or_else(|| fail(ErrorKind::Client(BAD_LINE)))?;
            SetMode::Cas(cas)
        }
        _ => unreachable!("caller matched the verb"),
    };
    let noreply = has_noreply(tokens.iter().copied());
    let data = &buf[line_consumed..data_end];

    Ok(Outcome::Command(Parsed {
        command: Command::Set(Set {
            key,
            value: data,
            ttl_secs,
            mc_flags,
            tags: Vec::new(),
            mode,
        }),
        consumed,
        noreply,
        style: ResponseStyle::Storage,
    }))
}

fn parse_key(token: Option<&[u8]>, consumed: usize) -> Result<Key<'_>, ProtocolError> {
    let fail = |detail| ProtocolError::Recoverable {
        response: ErrorKind::Client(detail),
        consumed,
    };

    // Every one of these is `bad command line format` because that is verbatim
    // what memcached answers, and the differential suite compares the bytes. A
    // more descriptive message would be a divergence a client could trip over.
    let token = token.ok_or_else(|| fail(BAD_LINE))?;
    if token.is_empty() || token.len() > MAX_KEY_LEN {
        return Err(fail(BAD_LINE));
    }
    // Control characters would break the line framing.
    if token.iter().any(|b| *b <= b' ' || *b == 0x7f) {
        return Err(fail(BAD_LINE));
    }

    Key::new(token).map_err(|_| fail(BAD_LINE))
}

fn parse_keys<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
    consumed: usize,
) -> Result<Vec<Key<'a>>, ProtocolError> {
    let mut keys = Vec::new();
    for token in tokens {
        if keys.len() >= vash_core::MAX_BATCH_ITEMS {
            return Err(ProtocolError::Recoverable {
                response: ErrorKind::Client("too many keys"),
                consumed,
            });
        }
        keys.push(parse_key(Some(token), consumed)?);
    }
    if keys.is_empty() {
        return Err(ProtocolError::Recoverable {
            response: ErrorKind::Client(BAD_LINE),
            consumed,
        });
    }
    Ok(keys)
}

/// Converts memcached's `exptime` to a relative TTL in seconds.
///
/// The protocol overloads one field three ways: 0 is "never", a value above 30
/// days is an absolute unix timestamp, and a negative value means "already
/// expired". The absolute form is converted by the caller's clock; here it is
/// passed through, and the server treats anything past the threshold as
/// absolute.
fn parse_exptime(token: Option<&[u8]>, consumed: usize) -> Result<u32, ProtocolError> {
    let fail = || ProtocolError::Recoverable {
        response: ErrorKind::Client("bad command line format"),
        consumed,
    };

    let text = std::str::from_utf8(token.ok_or_else(fail)?).map_err(|_| fail())?;
    let seconds: i64 = text.trim().parse().map_err(|_| fail())?;

    Ok(if seconds < 0 {
        // Negative means immediately expired. One second in the past is the
        // smallest TTL we can express, and the read path rejects it at once.
        IMMEDIATELY_EXPIRED
    } else {
        u32::try_from(seconds).unwrap_or(u32::MAX)
    })
}

/// Sentinel TTL meaning "already expired".
///
/// memcached's negative `exptime` has no positive equivalent, so the server
/// recognises this value and stores an expiry in the past.
pub const IMMEDIATELY_EXPIRED: u32 = u32::MAX;

fn has_noreply<'a>(mut tokens: impl Iterator<Item = &'a [u8]>) -> bool {
    tokens.any(|t| t == b"noreply")
}

fn parse_u64(token: Option<&[u8]>) -> Option<u64> {
    std::str::from_utf8(token?).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::super::{Outcome, parse};
    use super::*;

    fn command(input: &[u8]) -> Parsed<'_> {
        match parse(input) {
            Ok(Outcome::Command(parsed)) => parsed,
            other => panic!("expected a command, got {other:?}"),
        }
    }

    #[test]
    fn parses_set_with_its_data_block() {
        let parsed = command(b"set foo 42 300 5\r\nhello\r\n");
        assert_eq!(parsed.consumed, 25);
        assert!(!parsed.noreply);

        let Command::Set(set) = parsed.command else {
            panic!("expected a set")
        };
        assert_eq!(set.key.as_bytes(), b"foo");
        assert_eq!(set.value, b"hello");
        assert_eq!(set.mc_flags, 42);
        assert_eq!(set.ttl_secs, 300);
        assert_eq!(set.mode, SetMode::Set);
    }

    #[test]
    fn waits_for_the_whole_data_block() {
        // The line has arrived but the value has not.
        assert!(matches!(
            parse(b"set foo 0 0 5\r\nhel"),
            Ok(Outcome::Incomplete)
        ));
        // Value present but its terminator is not.
        assert!(matches!(
            parse(b"set foo 0 0 5\r\nhello"),
            Ok(Outcome::Incomplete)
        ));
    }

    #[test]
    fn a_value_may_contain_crlf() {
        // Length-delimited, not terminator-delimited: this is the case a naive
        // line-based parser gets wrong.
        let parsed = command(b"set foo 0 0 7\r\na\r\nb\r\nc\r\n");
        let Command::Set(set) = parsed.command else {
            panic!()
        };
        assert_eq!(set.value, b"a\r\nb\r\nc");
    }

    #[test]
    fn a_malformed_command_line_still_consumes_its_data_block() {
        // `notanumber` is not valid flags, but the length token is readable, so
        // the block must be swallowed anyway — otherwise the value would be
        // parsed as the next command and every request after it misread.
        let input = b"set foo notanumber 0 1\r\nx\r\n";
        let Err(ProtocolError::Recoverable { consumed, .. }) = parse(input) else {
            panic!("expected a recoverable client error")
        };
        assert_eq!(consumed, input.len(), "the data block must be consumed too");
    }

    #[test]
    fn a_malformed_line_with_no_readable_length_consumes_only_the_line() {
        // Nothing can be swallowed when the length itself is unreadable.
        let input = b"set foo 0 0 notanumber\r\n";
        let Err(ProtocolError::Recoverable { consumed, .. }) = parse(input) else {
            panic!("expected a recoverable client error")
        };
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn an_empty_line_is_malformed_rather_than_unknown() {
        // memcached draws this distinction, and it shows up right after a
        // rejected data block leaves a stray terminator behind.
        assert!(matches!(
            parse(b"\r\n"),
            Err(ProtocolError::Recoverable {
                response: ErrorKind::Client(BAD_LINE),
                ..
            })
        ));
    }

    #[test]
    fn a_mismatched_data_block_is_recoverable_and_consumes_it() {
        // The declared length must still be skipped, or the stream desyncs.
        let input = b"set foo 0 0 3\r\nhello\r\n";
        let Err(ProtocolError::Recoverable {
            response: ErrorKind::Client(_),
            consumed,
        }) = parse(input)
        else {
            panic!("expected a recoverable client error")
        };
        assert_eq!(consumed, 15 + 3 + 2);
    }

    #[test]
    fn each_storage_verb_maps_to_its_mode() {
        for (verb, expected) in [
            (&b"set"[..], SetMode::Set),
            (b"add", SetMode::Add),
            (b"replace", SetMode::Replace),
            (b"append", SetMode::Append),
            (b"prepend", SetMode::Prepend),
        ] {
            let mut input = Vec::new();
            input.extend_from_slice(verb);
            input.extend_from_slice(b" k 0 0 1\r\nx\r\n");
            let Command::Set(set) = command(&input).command else {
                panic!("expected a set for {verb:?}")
            };
            assert_eq!(set.mode, expected, "{verb:?}");
        }

        let Command::Set(set) = command(b"cas k 0 0 1 99\r\nx\r\n").command else {
            panic!()
        };
        assert_eq!(set.mode, SetMode::Cas(99));
    }

    #[test]
    fn parses_multi_key_get() {
        let Command::GetMany(keys) = command(b"get a bb ccc\r\n").command else {
            panic!("expected a multi-get")
        };
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[2].as_bytes(), b"ccc");
    }

    #[test]
    fn extra_spaces_between_tokens_are_tolerated() {
        let Command::GetMany(keys) = command(b"get  a   b \r\n").command else {
            panic!()
        };
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn parses_gat() {
        let Command::GetAndTouch { keys, ttl_secs } = command(b"gat 120 a b\r\n").command else {
            panic!("expected a gat")
        };
        assert_eq!(ttl_secs, 120);
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn parses_incr_and_decr() {
        let Command::Incr {
            delta, decrement, ..
        } = command(b"incr counter 5\r\n").command
        else {
            panic!()
        };
        assert_eq!(delta, 5);
        assert!(!decrement);

        let Command::Incr { decrement, .. } = command(b"decr counter 1\r\n").command else {
            panic!()
        };
        assert!(decrement);
    }

    #[test]
    fn noreply_is_recognised_on_every_command_that_takes_it() {
        assert!(command(b"set k 0 0 1 noreply\r\nx\r\n").noreply);
        assert!(command(b"delete k noreply\r\n").noreply);
        assert!(command(b"touch k 5 noreply\r\n").noreply);
        assert!(command(b"incr k 1 noreply\r\n").noreply);
        assert!(command(b"flush_all noreply\r\n").noreply);
        assert!(!command(b"get k\r\n").noreply);
    }

    #[test]
    fn negative_exptime_means_already_expired() {
        let Command::Set(set) = command(b"set k 0 -1 1\r\nx\r\n").command else {
            panic!()
        };
        assert_eq!(set.ttl_secs, IMMEDIATELY_EXPIRED);
    }

    #[test]
    fn keys_are_validated_against_memcached_rules() {
        // Over memcached's 250-byte limit, even though LMDB would take it.
        // The wording is memcached's own — a differential run against a real
        // server compares these bytes.
        let mut long = b"get ".to_vec();
        long.extend_from_slice(&vec![b'k'; MAX_KEY_LEN + 1]);
        long.extend_from_slice(b"\r\n");
        assert!(matches!(
            parse(&long),
            Err(ProtocolError::Recoverable {
                response: ErrorKind::Client(BAD_LINE),
                ..
            })
        ));

        // Control characters would break the line framing.
        assert!(matches!(
            parse(b"get a\x01b\r\n"),
            Err(ProtocolError::Recoverable {
                response: ErrorKind::Client(_),
                ..
            })
        ));
    }

    #[test]
    fn an_implausible_value_length_is_fatal_before_it_is_buffered() {
        let input = format!("set k 0 0 {}\r\n", u64::MAX);
        assert!(matches!(
            parse(input.as_bytes()),
            Err(ProtocolError::Fatal(_))
        ));
    }

    #[test]
    fn parses_the_tag_extension() {
        let Command::DeleteByTag { tag } = command(b"delete_by_tag news\r\n").command else {
            panic!("expected a tag invalidation")
        };
        assert_eq!(tag, b"news");
    }

    #[test]
    fn parses_bare_commands() {
        assert!(matches!(command(b"version\r\n").command, Command::Version));
        assert!(matches!(command(b"stats\r\n").command, Command::Stats));
        assert!(matches!(command(b"quit\r\n").command, Command::Quit));
        assert!(matches!(command(b"flush_all\r\n").command, Command::Flush));
    }

    #[test]
    fn commands_parse_back_to_back() {
        let input = b"version\r\nget a\r\n";
        let first = command(input);
        assert_eq!(first.consumed, 9);

        let Command::GetMany(keys) = command(&input[first.consumed..]).command else {
            panic!()
        };
        assert_eq!(keys[0].as_bytes(), b"a");
    }
}
