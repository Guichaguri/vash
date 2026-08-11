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

    // Two frames back to back: the pipelining path, where a boundary mistake
    // shows up as one request executed against another's bytes.
    let mut pipelined = frame(Opcode::Get, b"first");
    pipelined.extend_from_slice(&frame(Opcode::Get, b"second"));
    write(&dir, "pipelined", &pipelined);
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
    memcached_seeds(root);
    record_seeds(root);

    println!("wrote the seed corpus to {}", root.display());
}
