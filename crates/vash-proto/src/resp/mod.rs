//! The Redis serialisation protocol.
//!
//! **One parser serves RESP2 and RESP3**, because a *request* is identical in
//! the two: an array of bulk strings. They diverge only in replies — nulls,
//! doubles and maps — so the version is state the connection carries and only
//! the encoder reads. See [`Version`] and [`encode`].
//!
//! **Inline commands are deliberately unsupported.** `GET foo\r\n` typed into a
//! telnet session is legal RESP, but it is also a legal memcached command, and
//! this server picks a dialect from the first byte of a connection (see
//! [`crate::detect`]). Accepting both would make that byte ambiguous, and the
//! ambiguity would be resolved differently depending on which command a client
//! happened to send first. Every real client library sends the array form.
//!
//! Like the memcached adapter this is hand-written and borrows key and value
//! bytes straight out of the read buffer; the only allocation is the argument
//! vector, which a variadic command needs anyway.

pub mod command;
pub mod encode;

pub use command::{Command, parse_command};

/// Which RESP dialect a connection has negotiated.
///
/// Connections start at RESP2 and move to RESP3 only when the client asks with
/// `HELLO 3`. There is no way back, and none is needed: no client downgrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Version {
    #[default]
    Resp2,
    Resp3,
}

/// Longest argument accepted, matching the ceiling the store itself enforces.
pub const MAX_BULK_LEN: usize = vash_core::ABSOLUTE_MAX_VALUE_LEN;

/// Ceiling on arguments in one command.
///
/// Two per batch item covers the widest shape, `MSET`'s key/value pairs, plus
/// room for the verb and its option tokens. Checked before anything is
/// allocated, so a client cannot make the server reserve a huge vector by
/// announcing an array it never sends.
pub const MAX_ARGS: usize = 2 * vash_core::MAX_BATCH_ITEMS + 8;

/// Longest `*`/`$` header line tolerated. A decimal `i64` and its sign fit in
/// 20 bytes; anything longer is a client writing nonsense.
const MAX_HEADER_LEN: usize = 32;

/// A parsed command plus how many bytes of the buffer it consumed.
#[derive(Debug)]
pub struct Parsed<'a> {
    pub command: Command<'a>,
    pub consumed: usize,
}

#[derive(Debug)]
pub enum Outcome<'a> {
    /// More bytes are needed. As with the memcached text protocol there is
    /// often no way to say how many, so the caller simply reads more.
    Incomplete,
    Command(Parsed<'a>),
}

/// A protocol error.
///
/// The same distinction the other adapters draw: if the framing is still
/// intelligible the server answers and carries on, otherwise it answers once
/// and hangs up. Redis behaves the same way — a protocol error gets a reply
/// *and* a disconnect, because there is no way to find the next command.
#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError<'a> {
    /// Answer with an error and keep the connection.
    Recoverable {
        reply: ErrorReply<'a>,
        consumed: usize,
    },
    /// The stream cannot be resynchronised. Rendered as
    /// `-ERR Protocol error: <detail>`, which is Redis's own wording.
    Fatal(&'static str),
}

/// An error line, kept as borrowed pieces so the parser still allocates
/// nothing on the path a hostile client is most likely to exercise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorReply<'a> {
    /// `-ERR unknown command '…'`
    UnknownCommand(&'a [u8]),
    /// `-ERR wrong number of arguments for '…' command`
    WrongArity(&'static str),
    /// `-ERR invalid expire time in '…' command`
    InvalidExpire(&'static str),
    /// `-ERR <message>`
    Err(&'static str),
    /// `-<code> <message>`, for the prefixes clients special-case — `NOPROTO`,
    /// `OOM`, `WRONGTYPE`.
    Coded(&'static str, &'static str),
}

impl ErrorReply<'_> {
    /// Redis's wording for a value that should have been an integer. Used for
    /// both stored values and command arguments, exactly as Redis does.
    pub const NOT_AN_INTEGER: Self = Self::Err("value is not an integer or out of range");
    /// Likewise for the float commands.
    pub const NOT_A_FLOAT: Self = Self::Err("value is not a valid float");
    pub const SYNTAX: Self = Self::Err("syntax error");
    pub const OVERFLOW: Self = Self::Err("increment or decrement would overflow");
}

/// Attempts to parse one command from the front of `buf`.
///
/// Empty arrays are skipped rather than reported: Redis accepts `*0\r\n` as a
/// no-op, and a client that sends one is waiting for the *next* command's
/// reply, not for an error.
pub fn parse(buf: &[u8]) -> Result<Outcome<'_>, ProtocolError<'_>> {
    let mut pos = 0;

    loop {
        let Some((count, after_header)) = read_header(buf, pos, b'*', "invalid multibulk length")?
        else {
            return Ok(Outcome::Incomplete);
        };

        // A request is never null, and a negative count is how a desynchronised
        // stream usually presents itself.
        if count < 0 || count as u64 > MAX_ARGS as u64 {
            return Err(ProtocolError::Fatal("invalid multibulk length"));
        }

        if count == 0 {
            pos = after_header;
            continue;
        }

        let mut args = Args::with_capacity(count as usize);
        let mut cursor = after_header;
        for _ in 0..count {
            let Some((arg, next)) = read_bulk(buf, cursor)? else {
                return Ok(Outcome::Incomplete);
            };
            args.push(arg);
            cursor = next;
        }

        return parse_command(&args, cursor).map(Outcome::Command);
    }
}

/// How many arguments one command holds without touching the heap.
///
/// Eight covers every fixed-arity command in the dialect — `GET` sends two and
/// `SET key value EX 300 XX GET` sends eight, which is the widest — so the heap
/// is reached only by the genuinely variadic ones (`MGET`, `MSET`, `DEL`),
/// where the allocation is proportional to a batch the client chose to send.
const INLINE_ARGS: usize = 8;

/// The argument slices of one RESP command.
///
/// **Why this is not a `Vec`.** Every RESP command is a multibulk array, so
/// every RESP command allocated a vector of borrowed slices to hold the two or
/// three it actually has — and paid for it twice, because the connection parses
/// each block once in `conn::measure_resp` to find where its commands end and
/// again in `resp::execute_block` to run them. Unlike the memcached dialect
/// there is no single-key command that escapes this: `GET` is a multibulk array
/// exactly as `MSET` is.
///
/// The header states the count before any argument is read, so this is inline
/// or heap from the first push and never grows.
enum Args<'a> {
    Inline {
        buf: [&'a [u8]; INLINE_ARGS],
        len: usize,
    },
    Heap(Vec<&'a [u8]>),
}

impl<'a> Args<'a> {
    fn with_capacity(count: usize) -> Self {
        if count <= INLINE_ARGS {
            Self::Inline {
                buf: [b""; INLINE_ARGS],
                len: 0,
            }
        } else {
            Self::Heap(Vec::with_capacity(count))
        }
    }

    fn push(&mut self, arg: &'a [u8]) {
        match self {
            Self::Inline { buf, len } if *len < INLINE_ARGS => {
                buf[*len] = arg;
                *len += 1;
            }
            // Unreachable by construction: the caller sizes this from the
            // header and then pushes exactly that many. Promoting rather than
            // indexing past the end means a later change to that loop is a
            // slower parse instead of a panic, and this parser reads bytes from
            // unauthenticated clients.
            Self::Inline { buf, len } => {
                let mut heap = Vec::with_capacity(*len + 1);
                heap.extend_from_slice(&buf[..*len]);
                heap.push(arg);
                *self = Self::Heap(heap);
            }
            Self::Heap(heap) => heap.push(arg),
        }
    }
}

impl<'a> std::ops::Deref for Args<'a> {
    type Target = [&'a [u8]];

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Inline { buf, len } => &buf[..*len],
            Self::Heap(heap) => heap,
        }
    }
}

/// Reads a `<prefix><decimal>\r\n` header at `pos`.
///
/// `Ok(None)` means the terminator has not arrived yet. A bare `\n` is
/// tolerated, as it is in the memcached adapter, because it costs nothing and
/// hand-written clients send it.
fn read_header(
    buf: &[u8],
    pos: usize,
    prefix: u8,
    detail: &'static str,
) -> Result<Option<(i64, usize)>, ProtocolError<'static>> {
    let Some(first) = buf.get(pos) else {
        return Ok(None);
    };
    if *first != prefix {
        return Err(ProtocolError::Fatal(detail));
    }

    let Some(newline) = memchr::memchr(b'\n', &buf[pos..]).map(|offset| pos + offset) else {
        // Bounded before the buffer can grow: a client that never terminates a
        // header would otherwise pin memory for free.
        if buf.len() - pos > MAX_HEADER_LEN {
            return Err(ProtocolError::Fatal(detail));
        }
        return Ok(None);
    };

    let end = if newline > pos && buf[newline - 1] == b'\r' {
        newline - 1
    } else {
        newline
    };

    let text = std::str::from_utf8(&buf[pos + 1..end]).map_err(|_| ProtocolError::Fatal(detail))?;
    let value: i64 = text.parse().map_err(|_| ProtocolError::Fatal(detail))?;

    Ok(Some((value, newline + 1)))
}

/// Reads one `$<len>\r\n<bytes>\r\n` argument at `pos`.
fn read_bulk(buf: &[u8], pos: usize) -> Result<Option<(&[u8], usize)>, ProtocolError<'static>> {
    let Some((len, after_header)) = read_header(buf, pos, b'$', "invalid bulk length")? else {
        return Ok(None);
    };

    // Rejected before waiting for that many bytes to arrive, or a client could
    // pin a connection's buffer at will by announcing a huge argument.
    if len < 0 || len as u64 > MAX_BULK_LEN as u64 {
        return Err(ProtocolError::Fatal("invalid bulk length"));
    }
    let len = len as usize;

    let end = after_header + len;
    if buf.len() < end + 2 {
        return Ok(None);
    }
    // The payload is length-delimited, so this terminator carries no
    // information — but checking it is what turns a desynchronised stream into
    // one clear error instead of a run of nonsense commands.
    if &buf[end..end + 2] != b"\r\n" {
        return Err(ProtocolError::Fatal("expected '\\r\\n'"));
    }

    Ok(Some((&buf[after_header..end], end + 2)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(input: &[u8]) -> Parsed<'_> {
        match parse(input) {
            Ok(Outcome::Command(parsed)) => parsed,
            other => panic!("expected a command, got {other:?}"),
        }
    }

    /// [`Args`] holds [`INLINE_ARGS`] slices inline and spills to the heap past
    /// that, so the boundary is a seam where an off-by-one would either drop an
    /// argument or panic. Walked across it here, one argument at a time, using
    /// `MGET` because it is the command that accepts any number of them.
    ///
    /// The count is what the *parser* sees, verb included, so an `MGET` of
    /// seven keys is the eight-argument case.
    #[test]
    fn arguments_survive_the_boundary_between_inline_and_heap() {
        for key_count in 1..=(INLINE_ARGS + 4) {
            let mut input = format!("*{}\r\n$4\r\nMGET\r\n", key_count + 1).into_bytes();
            for i in 0..key_count {
                let key = format!("k{i}");
                input.extend_from_slice(format!("${}\r\n{key}\r\n", key.len()).as_bytes());
            }

            let parsed = command(&input);
            let Command::MGet { keys } = parsed.command else {
                panic!("expected an MGET for {key_count} keys")
            };
            assert_eq!(keys.len(), key_count, "wrong key count at {key_count}");
            // The last key is the one a lost argument takes with it.
            assert_eq!(
                keys[key_count - 1],
                format!("k{}", key_count - 1).as_bytes(),
                "wrong final key at {key_count}"
            );
            assert_eq!(parsed.consumed, input.len());
        }
    }

    #[test]
    fn incomplete_until_every_argument_has_arrived() {
        for prefix in [
            b"".as_slice(),
            b"*2\r\n",
            b"*2\r\n$3\r\n",
            b"*2\r\n$3\r\nGET\r\n",
            b"*2\r\n$3\r\nGET\r\n$3\r\nfo",
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo",
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r",
        ] {
            assert!(
                matches!(parse(prefix), Ok(Outcome::Incomplete)),
                "{prefix:?} should be incomplete"
            );
        }

        let parsed = command(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(parsed.consumed, 22);
    }

    #[test]
    fn commands_parse_back_to_back() {
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
        let first = command(input);
        assert!(matches!(first.command, Command::Ping { message: None }));

        let second = command(&input[first.consumed..]);
        assert!(matches!(second.command, Command::Get { key: b"a" }));
        assert_eq!(first.consumed + second.consumed, input.len());
    }

    #[test]
    fn an_empty_array_is_skipped_rather_than_answered() {
        // Redis accepts `*0` as a no-op; a client that sends one is waiting for
        // the next command's reply, not for an error.
        let parsed = command(b"*0\r\n*1\r\n$4\r\nPING\r\n");
        assert!(matches!(parsed.command, Command::Ping { .. }));
        assert_eq!(parsed.consumed, 18);
    }

    #[test]
    fn a_value_may_contain_crlf() {
        // Length-delimited, not terminator-delimited: the case a line-oriented
        // parser gets wrong.
        let Command::Set(set) = command(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n").command
        else {
            panic!("expected a set")
        };
        assert_eq!(set.value, b"a\r\nb");
    }

    #[test]
    fn binary_arguments_survive_intact() {
        let input = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\n\x00\xff\n\r\n";
        let Command::Set(set) = command(input).command else {
            panic!()
        };
        assert_eq!(set.value, b"\x00\xff\n");
    }

    #[test]
    fn junk_framing_is_fatal_rather_than_guessed() {
        for input in [
            b"+OK\r\n".as_slice(),   // not an array
            b"*2\r\n+OK\r\n",        // argument is not a bulk string
            b"*-1\r\n",              // a request is never null
            b"*notanumber\r\n",      //
            b"*1\r\n$-1\r\n",        // nor is an argument
            b"*1\r\n$1\r\nabcd\r\n", // length disagrees with the payload
        ] {
            assert!(
                matches!(parse(input), Err(ProtocolError::Fatal(_))),
                "{input:?} should be fatal"
            );
        }
    }

    #[test]
    fn an_endless_header_is_fatal_before_it_is_buffered() {
        let flood = [b"*".as_slice(), &[b'9'; MAX_HEADER_LEN + 1]].concat();
        assert!(matches!(parse(&flood), Err(ProtocolError::Fatal(_))));
    }

    #[test]
    fn an_implausible_length_is_rejected_before_anything_is_reserved() {
        let too_many = format!("*{}\r\n", MAX_ARGS + 1);
        assert!(matches!(
            parse(too_many.as_bytes()),
            Err(ProtocolError::Fatal(_))
        ));

        let too_long = format!("*1\r\n${}\r\n", MAX_BULK_LEN + 1);
        assert!(matches!(
            parse(too_long.as_bytes()),
            Err(ProtocolError::Fatal(_))
        ));
    }

    #[test]
    fn an_unknown_command_is_recoverable() {
        let input = b"*2\r\n$5\r\nLPUSH\r\n$1\r\nk\r\n";
        let Err(ProtocolError::Recoverable { reply, consumed }) = parse(input) else {
            panic!("expected a recoverable error")
        };
        assert_eq!(reply, ErrorReply::UnknownCommand(b"LPUSH"));
        assert_eq!(consumed, input.len(), "the whole command must be consumed");
    }
}
