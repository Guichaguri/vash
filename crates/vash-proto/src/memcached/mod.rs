//! The memcached protocols.
//!
//! Hand-written rather than built on a parser combinator: the grammar is
//! line-oriented with no nesting, and owning the parser is what lets keys and
//! values be borrowed slices of the read buffer with no intermediate
//! allocation. See plan §7.
//!
//! Two dialects share this module — the classic text commands nearly every
//! client speaks, and the newer meta commands (`mg`/`ms`/`md`/…) — because they
//! share the framing, the error vocabulary and the key rules.
//!
//! The legacy binary protocol (magic `0x80`) is deliberately absent: upstream
//! deprecated it in favour of the meta commands, and it would be a third parser
//! to fuzz and maintain.

pub mod encode;
pub mod meta;
pub mod text;

use vash_core::Command;

/// Memcached's own key limit. Stricter than ours, and enforced here so a key
/// that a real memcached would reject does not quietly succeed.
pub const MAX_KEY_LEN: usize = 250;

/// Longest command line accepted before the connection is dropped.
///
/// A client that never sends `\r\n` would otherwise grow the read buffer without
/// bound. Generous enough for a `get` of many keys at 250 bytes each.
pub const MAX_LINE_LEN: usize = 16 * 1024;

/// A parsed command plus how many bytes of the buffer it consumed.
#[derive(Debug)]
pub struct Parsed<'a> {
    pub command: Command<'a>,
    pub consumed: usize,
    /// The client asked for no response (`noreply`, or a meta `q` flag).
    pub noreply: bool,
    /// How the reply should be rendered. Decided here because it depends on
    /// which command was issued, which the storage layer never sees.
    pub style: encode::ResponseStyle,
}

#[derive(Debug)]
pub enum Outcome<'a> {
    /// More bytes are needed. Unlike the binary protocol there is often no way
    /// to know how many, so the caller simply reads more.
    Incomplete,
    Command(Parsed<'a>),
}

/// A protocol error.
///
/// The distinction is the same one the binary protocol makes: if the framing is
/// still intelligible the server answers and carries on, otherwise it hangs up.
#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// Answer with an error line and keep the connection.
    Recoverable {
        response: ErrorKind,
        consumed: usize,
    },
    /// The stream cannot be resynchronised.
    Fatal(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Unknown command: bare `ERROR\r\n`.
    Error,
    /// `CLIENT_ERROR <reason>` — the request was malformed.
    Client(&'static str),
    /// `SERVER_ERROR <reason>` — the server could not comply.
    Server(&'static str),
}

/// Attempts to parse one command from the front of `buf`.
///
/// Dispatches on the first token: meta commands are exactly two characters
/// beginning with `m`, everything else is classic.
pub fn parse(buf: &[u8]) -> Result<Outcome<'_>, ProtocolError> {
    let Some(line_end) = find_line(buf)? else {
        return Ok(Outcome::Incomplete);
    };

    let line = &buf[..line_end];
    // Bytes through the end of the line, including its terminator.
    let after_line = line_end + line_terminator_len(buf, line_end);

    let verb = line.split(|b| *b == b' ').next().unwrap_or(line);

    if is_meta_verb(verb) {
        meta::parse(verb, line, after_line, buf)
    } else {
        text::parse(verb, line, after_line, buf)
    }
}

fn is_meta_verb(verb: &[u8]) -> bool {
    matches!(verb, b"mg" | b"ms" | b"md" | b"mn" | b"ma" | b"me" | b"mdt")
}

/// Locates the CRLF ending the first line.
///
/// Tolerates a bare `\n`, which some hand-written clients and `telnet` sessions
/// send; real memcached does the same.
fn find_line(buf: &[u8]) -> Result<Option<usize>, ProtocolError> {
    match memchr::memchr(b'\n', buf) {
        Some(newline) => {
            let end = if newline > 0 && buf[newline - 1] == b'\r' {
                newline - 1
            } else {
                newline
            };
            Ok(Some(end))
        }
        None => {
            if buf.len() > MAX_LINE_LEN {
                return Err(ProtocolError::Fatal(
                    "command line exceeded the maximum length",
                ));
            }
            Ok(None)
        }
    }
}

/// Length of the terminator that followed a line, so callers can compute the
/// consumed byte count. Always 2 for `\r\n`; 1 when the client sent a bare
/// `\n`.
pub(crate) fn line_terminator_len(buf: &[u8], line_end: usize) -> usize {
    if buf.get(line_end) == Some(&b'\r') {
        2
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_until_a_newline_arrives() {
        assert!(matches!(parse(b"get foo"), Ok(Outcome::Incomplete)));
        assert!(matches!(parse(b""), Ok(Outcome::Incomplete)));
    }

    #[test]
    fn an_endless_line_is_fatal_rather_than_unbounded() {
        let flood = vec![b'x'; MAX_LINE_LEN + 1];
        assert!(matches!(parse(&flood), Err(ProtocolError::Fatal(_))));
    }

    #[test]
    fn a_bare_newline_is_accepted() {
        // telnet and some hand-rolled clients omit the carriage return.
        let Ok(Outcome::Command(parsed)) = parse(b"version\n") else {
            panic!("bare newline should parse")
        };
        assert_eq!(parsed.consumed, 8);
    }

    #[test]
    fn an_unknown_command_is_recoverable() {
        assert!(matches!(
            parse(b"frobnicate x\r\n"),
            Err(ProtocolError::Recoverable {
                response: ErrorKind::Error,
                ..
            })
        ));
    }
}
