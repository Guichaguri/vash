//! RESP response rendering.
//!
//! Everything a client can be told goes through these writers, and the ones
//! that differ between the two dialects — [`null`], [`double`], [`hello`] —
//! take the connection's negotiated [`Version`]. That is the whole of the
//! RESP2/RESP3 split for this command set: requests are identical, and the
//! remaining reply types are byte-for-byte the same in both.

use super::{ErrorReply, Version};

/// The version this server reports to `HELLO`.
///
/// It answers `server: redis` for the same reason the memcached adapter answers
/// with a real memcached version number: client libraries branch on it, and a
/// name they have never seen sends some of them down an error path. The suffix
/// is what tells a human what they are actually talking to.
pub const VERSION: &str = "7.4.0-vash";

pub fn simple(out: &mut Vec<u8>, text: &str) {
    out.push(b'+');
    out.extend_from_slice(text.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn ok(out: &mut Vec<u8>) {
    out.extend_from_slice(b"+OK\r\n");
}

/// `-<code> <message>`. `code` is the part clients pattern-match on, so it is
/// separated from the prose rather than baked into it.
pub fn error(out: &mut Vec<u8>, code: &str, message: &str) {
    out.push(b'-');
    out.extend_from_slice(code.as_bytes());
    out.push(b' ');
    out.extend_from_slice(message.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Renders a parser rejection.
///
/// The wording is Redis's own, verbatim, because clients match on some of it —
/// `unknown command` in particular is how a library decides a server does not
/// support a feature and falls back.
pub fn error_reply(out: &mut Vec<u8>, reply: &ErrorReply<'_>) {
    match reply {
        ErrorReply::UnknownCommand(name) => {
            out.extend_from_slice(b"-ERR unknown command '");
            escape(out, name);
            out.extend_from_slice(b"'\r\n");
        }
        ErrorReply::WrongArity(name) => {
            error(
                out,
                "ERR",
                &format!("wrong number of arguments for '{name}' command"),
            );
        }
        ErrorReply::InvalidExpire(name) => {
            error(
                out,
                "ERR",
                &format!("invalid expire time in '{name}' command"),
            );
        }
        ErrorReply::Err(message) => error(out, "ERR", message),
        ErrorReply::Coded(code, message) => error(out, code, message),
    }
}

/// A protocol error, which is also the last thing written on that connection.
pub fn protocol_error(out: &mut Vec<u8>, detail: &str) {
    error(out, "ERR", &format!("Protocol error: {detail}"));
}

pub fn integer(out: &mut Vec<u8>, value: i64) {
    out.push(b':');
    out.extend_from_slice(itoa(value).as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn bulk(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(itoa(bytes.len() as i64).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
}

/// The absent value.
///
/// RESP2 spells it as a bulk string of length −1 and RESP3 has a type of its
/// own. This is the divergence that actually matters for this command set —
/// every miss goes through it.
pub fn null(out: &mut Vec<u8>, version: Version) {
    out.extend_from_slice(match version {
        Version::Resp2 => b"$-1\r\n".as_slice(),
        Version::Resp3 => b"_\r\n",
    });
}

pub fn array(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(itoa(len as i64).as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// A floating-point value.
///
/// RESP3 has a double type; RESP2 has to send the decimal text as a bulk
/// string. `INCRBYFLOAT` uses the bulk form in *both*, which is why it calls
/// [`bulk`] directly rather than coming through here — only `INCREX` in
/// `BYFLOAT` mode takes the version-dependent path.
pub fn double(out: &mut Vec<u8>, value: f64, version: Version) {
    let text = format_float(value);
    match version {
        Version::Resp2 => bulk(out, text.as_bytes()),
        Version::Resp3 => {
            out.push(b',');
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// The `HELLO` reply: a map in RESP3, the same pairs flattened into an array in
/// RESP2.
pub fn hello(out: &mut Vec<u8>, version: Version) {
    const FIELDS: usize = 7;
    match version {
        Version::Resp2 => array(out, FIELDS * 2),
        Version::Resp3 => {
            out.push(b'%');
            out.extend_from_slice(itoa(FIELDS as i64).as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }

    bulk(out, b"server");
    bulk(out, b"redis");
    bulk(out, b"version");
    bulk(out, VERSION.as_bytes());
    bulk(out, b"proto");
    integer(
        out,
        match version {
            Version::Resp2 => 2,
            Version::Resp3 => 3,
        },
    );
    bulk(out, b"id");
    // Connections are not registered anywhere, so there is no id to report.
    // Answering zero is honest; inventing a counter would imply a `CLIENT`
    // command that does not exist here.
    integer(out, 0);
    bulk(out, b"mode");
    bulk(out, b"standalone");
    bulk(out, b"role");
    bulk(out, b"master");
    bulk(out, b"modules");
    array(out, 0);
}

/// Renders a float the way Redis does, re-exported from the domain crate.
///
/// It moved for the same reason the numeric parsers did: this renders the reply,
/// and `vash_core::arith` renders the *stored* value that a later `GET` returns.
/// A client that increments a float and then reads it must see one number, not
/// two spellings of it.
pub use vash_core::format_float;

/// Escapes a command name for the `unknown command` line, which is echoed back
/// to the client and must not be able to inject a CRLF into the reply stream.
fn escape(out: &mut Vec<u8>, name: &[u8]) {
    // Bounded as well as escaped: the name came off the wire and may be long.
    for byte in name.iter().take(128) {
        match byte {
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\'' | b'\\' => {
                out.push(b'\\');
                out.push(*byte);
            }
            0x20..=0x7e => out.push(*byte),
            other => out.extend_from_slice(format!("\\x{other:02x}").as_bytes()),
        }
    }
}

fn itoa(value: i64) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(write: impl FnOnce(&mut Vec<u8>)) -> String {
        let mut out = Vec::new();
        write(&mut out);
        String::from_utf8(out).expect("replies are valid UTF-8 in these tests")
    }

    #[test]
    fn nulls_differ_between_the_dialects() {
        assert_eq!(rendered(|out| null(out, Version::Resp2)), "$-1\r\n");
        assert_eq!(rendered(|out| null(out, Version::Resp3)), "_\r\n");
    }

    #[test]
    fn doubles_differ_between_the_dialects() {
        assert_eq!(
            rendered(|out| double(out, 1.75, Version::Resp2)),
            "$4\r\n1.75\r\n"
        );
        assert_eq!(
            rendered(|out| double(out, 1.75, Version::Resp3)),
            ",1.75\r\n"
        );
    }

    #[test]
    fn bulk_strings_are_binary_safe() {
        assert_eq!(
            rendered(|out| bulk(out, b"a\r\nb")),
            "$4\r\na\r\nb\r\n",
            "the payload is length-delimited, so a CRLF inside it is data"
        );
        assert_eq!(rendered(|out| bulk(out, b"")), "$0\r\n\r\n");
    }

    #[test]
    fn floats_drop_a_trailing_zero_the_way_redis_does() {
        assert_eq!(format_float(3.0), "3");
        assert_eq!(format_float(-0.5), "-0.5");
        assert_eq!(format_float(1.75), "1.75");
        assert_eq!(format_float(5000.0), "5000");
        // Never exponent notation, whatever the magnitude.
        assert_eq!(format_float(1e20), "100000000000000000000");
    }

    #[test]
    fn an_unknown_command_name_cannot_inject_a_reply() {
        // The name is echoed straight back, so a CRLF in it would end the error
        // line early and let the client read the rest as another reply.
        let out = rendered(|out| error_reply(out, &ErrorReply::UnknownCommand(b"a\r\n+OK")));
        assert_eq!(out, "-ERR unknown command 'a\\r\\n+OK'\r\n");
        assert_eq!(out.matches("\r\n").count(), 1);
    }

    #[test]
    fn hello_is_a_map_in_resp3_and_a_flat_array_in_resp2() {
        assert!(rendered(|out| hello(out, Version::Resp3)).starts_with("%7\r\n"));
        assert!(rendered(|out| hello(out, Version::Resp2)).starts_with("*14\r\n"));
    }

    #[test]
    fn errors_keep_their_code_separate_from_their_prose() {
        assert_eq!(
            rendered(|out| error_reply(out, &ErrorReply::Coded("NOPROTO", "unsupported"))),
            "-NOPROTO unsupported\r\n"
        );
        assert_eq!(
            rendered(|out| error_reply(out, &ErrorReply::WrongArity("get"))),
            "-ERR wrong number of arguments for 'get' command\r\n"
        );
    }
}
