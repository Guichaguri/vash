//! Writes the seed corpus the fuzz targets start from.
//!
//! ```text
//! cargo run -p vash-proto --example seed_corpus -- fuzz/seeds
//! ```
//!
//! A coverage-guided fuzzer finds valid input on its own eventually, but
//! "eventually" is the problem: a VCP frame needs a known opcode in byte 0 and
//! a `body_len` that matches the bytes actually present, and until it stumbles
//! on both, every input dies at the same early return. Seeding with valid
//! examples means the budget goes on the arithmetic *inside* the body decoders,
//! which is where the bugs would be.
//!
//! Generated rather than committed by hand so the corpus cannot drift from the
//! encoders: regenerate it whenever the wire format changes.

use std::path::Path;

use vash_proto::vcp::{Opcode, encode_request, encode_set_body, encode_tag_sync_body};

fn write(dir: &Path, name: &str, bytes: &[u8]) {
    std::fs::create_dir_all(dir).expect("creating the corpus directory");
    std::fs::write(dir.join(name), bytes).expect("writing a seed");
}

fn frame(opcode: Opcode, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_request(&mut out, opcode, 1, body);
    out
}

fn vcp_seeds(root: &Path) {
    let dir = root.join("vcp_decode");

    write(&dir, "hello", &frame(Opcode::Hello, &1u32.to_le_bytes()));
    write(&dir, "ping", &frame(Opcode::Ping, &[]));
    write(&dir, "get", &frame(Opcode::Get, b"some:key"));
    write(&dir, "delete", &frame(Opcode::Delete, b"some:key"));
    write(&dir, "flush", &frame(Opcode::Flush, &[]));
    write(&dir, "cluster", &frame(Opcode::Cluster, &[]));
    write(&dir, "delete_by_tag", &frame(Opcode::DeleteByTag, b"news"));

    let mut touch = Vec::new();
    vash_proto::vcp::encode_touch_body(&mut touch, b"some:key", 300);
    write(&dir, "touch", &frame(Opcode::Touch, &touch));

    let mut set = Vec::new();
    encode_set_body(&mut set, b"some:key", b"a value", 300, &[]);
    write(&dir, "set", &frame(Opcode::Set, &set));

    // The shape with the most length fields to disagree with each other.
    let mut tagged = Vec::new();
    encode_set_body(
        &mut tagged,
        b"some:key",
        b"a value",
        0,
        &[b"news".as_slice(), b"sport"],
    );
    write(&dir, "set_tagged", &frame(Opcode::Set, &tagged));

    let mut keys = Vec::new();
    vash_proto::vcp::encode_key_list_body(&mut keys, &[b"a".as_slice(), b"bb", b"ccc"]);
    write(&dir, "get_many", &frame(Opcode::GetMany, &keys));
    write(&dir, "delete_many", &frame(Opcode::DeleteMany, &keys));

    let mut batch = Vec::new();
    vash_proto::vcp::encode_batch_count(&mut batch, 2);
    encode_set_body(&mut batch, b"k1", b"v1", 0, &[]);
    encode_set_body(&mut batch, b"k2", b"v2", 60, &[b"tag".as_slice()]);
    write(&dir, "set_many", &frame(Opcode::SetMany, &batch));

    let mut digest = Vec::new();
    encode_tag_sync_body(
        &mut digest,
        true,
        [(b"news".as_slice(), 3u64), (b"sport".as_slice(), 1)].into_iter(),
    );
    write(&dir, "tag_sync", &frame(Opcode::TagSync, &digest));

    // The listing bodies carry two attacker-controlled lengths and a pattern
    // the matcher then runs against every record it walks, so the fuzzer wants
    // a first page, a resumed page and an escaped pattern to mutate from.
    let mut first_page = Vec::new();
    vash_proto::vcp::encode_list_body(&mut first_page, 64, b"", b"session:*");
    write(&dir, "list_keys", &frame(Opcode::ListKeys, &first_page));

    let mut resumed = Vec::new();
    vash_proto::vcp::encode_list_body(&mut resumed, 1024, b"\x00\x00session:41", b"*");
    write(
        &dir,
        "list_keys_resumed",
        &frame(Opcode::ListKeys, &resumed),
    );

    let mut escaped = Vec::new();
    vash_proto::vcp::encode_list_body(&mut escaped, 1, b"news", br"a\*b?c");
    write(&dir, "list_tags", &frame(Opcode::ListTags, &escaped));

    // Two frames back to back: the pipelining path, where a boundary mistake
    // shows up as one request executed against another's bytes.
    let mut pipelined = frame(Opcode::Get, b"first");
    pipelined.extend_from_slice(&frame(Opcode::Get, b"second"));
    write(&dir, "pipelined", &pipelined);
}

/// Seeds for the `AUTH` body parser.
///
/// Its own corpus rather than more frames in `vcp_decode`, because the target
/// takes a bare body: it is the one parser reachable before any credential has
/// been presented, so it is fuzzed on its own rather than behind a frame header
/// the fuzzer has to keep valid.
fn auth_seeds(root: &Path) {
    let dir = root.join("vcp_auth");

    let body = |mechanism: u8, name: &[u8], secret: &[u8]| {
        let mut out = Vec::new();
        vash_proto::vcp::encode_auth_body(&mut out, mechanism, name, secret);
        out
    };

    let secret = b"0123456789abcdef0123456789abcdef";
    write(&dir, "plain", &body(0, b"billing-api", secret));
    // An empty name is the `default` identity, not a malformed body.
    write(&dir, "default_identity", &body(0, b"", secret));
    // Mechanism 1 is specified and unbuilt; an empty secret is how a challenge
    // would be asked for. Both must *parse*, and be refused above the parser.
    write(&dir, "hmac_challenge", &body(1, b"peer", b""));
    write(&dir, "hmac_response", &body(1, b"peer", &[7u8; 32]));
    // An unknown mechanism still has to frame correctly, so that the executor
    // is what answers `UNSUPPORTED`.
    write(&dir, "unknown_mechanism", &body(0xff, b"x", b"y"));
    // Exactly at both ceilings, which is where the length arithmetic is most
    // likely to be off by one.
    write(
        &dir,
        "max_lengths",
        &body(
            0,
            &[b'n'; vash_proto::vcp::MAX_AUTH_NAME_LEN],
            &[b's'; vash_proto::vcp::MAX_AUTH_SECRET_LEN],
        ),
    );
    write(&dir, "header_only", &body(0, b"", b""));
    // Neither field is text to this parser.
    write(
        &dir,
        "binary",
        &body(0, &[0xff, 0x00, 0xfe], &[0x00, 0x80, 0xff]),
    );
}

fn memcached_seeds(root: &Path) {
    let dir = root.join("memcached_text");
    for (name, line) in [
        ("get", "get foo\r\n"),
        ("get_multi", "get a b c\r\n"),
        ("gets", "gets foo\r\n"),
        ("set", "set foo 0 300 5\r\nhello\r\n"),
        ("set_noreply", "set foo 0 0 2 noreply\r\nhi\r\n"),
        ("add", "add foo 7 60 3\r\nabc\r\n"),
        ("cas", "cas foo 0 0 3 42\r\nabc\r\n"),
        ("append", "append foo 0 0 3\r\nabc\r\n"),
        ("delete", "delete foo\r\n"),
        ("touch", "touch foo 120\r\n"),
        ("gat", "gat 120 a b\r\n"),
        ("incr", "incr counter 5\r\n"),
        ("decr", "decr counter 1\r\n"),
        ("flush_all", "flush_all\r\n"),
        ("stats", "stats\r\n"),
        ("version", "version\r\n"),
        ("quit", "quit\r\n"),
        ("delete_by_tag", "delete_by_tag news\r\n"),
        // The framing case: a value containing CRLF must not be parsed as
        // commands, and the command after it must still be found.
        ("embedded_crlf", "set foo 0 0 6\r\na\r\nb\r\nversion\r\n"),
        ("pipelined", "get a\r\nget b\r\nversion\r\n"),
    ] {
        write(&dir, name, line.as_bytes());
    }

    // The meta target prefixes its own verb from the first byte, so these seeds
    // start at the arguments.
    let dir = root.join("memcached_meta");
    for (index, (name, rest)) in [
        ("get", "foo v f s c t k Oopaque\r\n"),
        ("get_quiet", "foo v q\r\n"),
        ("get_and_touch", "foo v T120\r\n"),
        ("set", "foo 5 T60 F9 c\r\nhello\r\n"),
        ("set_tagged", "foo 5 Gnews,sport\r\nhello\r\n"),
        ("set_mode", "foo 3 ME\r\nabc\r\n"),
        ("set_cas", "foo 3 C42\r\nabc\r\n"),
        ("delete", "foo k Oz\r\n"),
        ("arithmetic", "counter v D5 MI\r\n"),
        ("noop", "\r\n"),
        ("debug", "foo\r\n"),
        ("tag_invalidate", "news\r\n"),
        ("refused_flag", "foo b\r\n"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut bytes = vec![index as u8];
        bytes.extend_from_slice(rest.as_bytes());
        write(&dir, name, &bytes);
    }
}

fn resp_seeds(root: &Path) {
    let dir = root.join("resp_decode");

    /// A RESP request array, which is the only request shape the server takes.
    fn request(args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    for (name, args) in [
        ("hello", &["HELLO", "3"][..]),
        ("ping", &["PING"]),
        ("get", &["GET", "foo"]),
        ("set", &["SET", "foo", "hello"]),
        (
            "set_options",
            &["SET", "foo", "hello", "NX", "GET", "EX", "60"],
        ),
        ("set_keepttl", &["SET", "foo", "hello", "XX", "KEEPTTL"]),
        ("del", &["DEL", "a", "b", "c"]),
        ("unlink", &["UNLINK", "a"]),
        ("mset", &["MSET", "a", "1", "b", "2"]),
        ("mget", &["MGET", "a", "b", "c"]),
        (
            "msetex",
            &["MSETEX", "2", "a", "1", "b", "2", "NX", "EX", "30"],
        ),
        ("exists", &["EXISTS", "a", "b"]),
        ("expire", &["EXPIRE", "foo", "60", "GT"]),
        ("expireat", &["EXPIREAT", "foo", "1700000000"]),
        ("persist", &["PERSIST", "foo"]),
        ("ttl", &["TTL", "foo"]),
        ("append", &["APPEND", "foo", "bar"]),
        ("incr", &["INCR", "counter"]),
        ("incrby", &["INCRBY", "counter", "-5"]),
        ("incrbyfloat", &["INCRBYFLOAT", "counter", "0.25"]),
        // The widest option list in the command set, which is where the
        // ordering and mutual-exclusion rules have the most room to be wrong.
        (
            "increx",
            &[
                "INCREX", "hits", "BYINT", "1", "LBOUND", "0", "UBOUND", "100", "SATURATE", "EX",
                "60", "ENX",
            ],
        ),
        (
            "increx_float",
            &["INCREX", "f", "BYFLOAT", "0.5", "PERSIST"],
        ),
        ("unknown", &["LPUSH", "k", "v"]),
    ] {
        write(&dir, name, &request(args));
    }

    // The framing cases. A value containing CRLF must not be read as another
    // command, and the command after it must still be found.
    let mut embedded = request(&["SET", "k", "a\r\nb"]);
    embedded.extend_from_slice(&request(&["PING"]));
    write(&dir, "embedded_crlf", &embedded);

    let mut pipelined = request(&["GET", "a"]);
    pipelined.extend_from_slice(&request(&["GET", "b"]));
    pipelined.extend_from_slice(&request(&["PING"]));
    write(&dir, "pipelined", &pipelined);

    // Redis accepts an empty array as a no-op, so the parser has to skip it
    // and still find what follows.
    let mut empty = b"*0\r\n".to_vec();
    empty.extend_from_slice(&request(&["PING"]));
    write(&dir, "empty_array", &empty);
}

fn record_seeds(root: &Path) {
    use vash_core::{RecordMeta, TagRef, encode_record};

    let dir = root.join("record_header");
    let meta = RecordMeta {
        epoch: 3,
        mc_flags: 9,
        expires_at_ms: 1_700_000_000_000,
        cas: 42,
    };

    let mut buf = Vec::new();
    encode_record(&mut buf, RecordMeta::default(), &[], b"").expect("no tags");
    write(&dir, "empty", &buf);

    let mut buf = Vec::new();
    encode_record(&mut buf, meta, &[], b"a value").expect("no tags");
    write(&dir, "plain", &buf);

    let mut buf = Vec::new();
    let tags = [TagRef::new(1, 7), TagRef::new(2, 0)];
    encode_record(&mut buf, meta, &tags, b"a value").expect("two tags");
    write(&dir, "tagged", &buf);

    // The header of a record claiming a tag table that is not there: the shape
    // a truncated page produces, and the one the parser must refuse.
    let mut truncated = buf.clone();
    truncated.truncate(vash_core::RECORD_HEADER_LEN + 4);
    write(&dir, "truncated_tag_table", &truncated);
}

fn main() {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "fuzz/seeds".to_string());
    let root = Path::new(&root);

    vcp_seeds(root);
    auth_seeds(root);
    memcached_seeds(root);
    resp_seeds(root);
    record_seeds(root);

    println!("wrote the seed corpus to {}", root.display());
}
