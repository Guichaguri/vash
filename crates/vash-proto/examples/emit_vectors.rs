//! Emits the VCP conformance corpus consumed by the client libraries.
//!
//! ```text
//! cargo run -p vash-proto --example emit_vectors -- ../vash-client-libraries/conformance/vectors
//! ```
//!
//! The nine client libraries are checked against these bytes in both
//! directions. Generating them from the real encoders rather than writing them
//! by hand is the whole point: a hand-written corpus encodes its author's
//! reading of the spec, which is the same reading the corpus exists to check.
//! Anything emitted here is what this server actually produces and accepts.
//!
//! Output is committed to the client repository so the nine language jobs need
//! nothing from this repo. A CI job re-runs this and diffs; that job is the
//! drift check.
//!
//! JSON is written by hand because vash-proto has no serialiser dependency and
//! this is not worth adding one for.

use std::fmt::Write as _;

use bytes::Bytes;
use vash_core::{
    Applied, Arithmetic, ClusterInfo, ClusterMode, Delta, Key, ListEntry, Listing, Missing, Number,
    OnBound, PeerInfo, Reply, ServerInfo, TtlChange, Value,
};
use vash_proto::vcp::{
    Opcode, Status, encode_arithmetic_body, encode_auth_body, encode_batch_count, encode_error,
    encode_key_list_body, encode_list_body, encode_reply, encode_request, encode_set_body,
    encode_touch_body, flags,
};

// ---------------------------------------------------------------------------
// Minimal JSON writer
// ---------------------------------------------------------------------------

enum J {
    U(u64),
    S(String),
    B(bool),
    A(Vec<J>),
    O(Vec<(&'static str, J)>),
}

impl J {
    fn s(v: impl Into<String>) -> J {
        J::S(v.into())
    }

    fn write(&self, out: &mut String, indent: usize) {
        let pad = "  ".repeat(indent);
        let pad_inner = "  ".repeat(indent + 1);
        match self {
            J::U(n) => {
                let _ = write!(out, "{n}");
            }
            J::B(b) => {
                let _ = write!(out, "{b}");
            }
            J::S(s) => {
                out.push('"');
                for c in s.chars() {
                    match c {
                        '"' => out.push_str("\\\""),
                        '\\' => out.push_str("\\\\"),
                        '\n' => out.push_str("\\n"),
                        '\r' => out.push_str("\\r"),
                        '\t' => out.push_str("\\t"),
                        c if (c as u32) < 0x20 => {
                            let _ = write!(out, "\\u{:04x}", c as u32);
                        }
                        c => out.push(c),
                    }
                }
                out.push('"');
            }
            J::A(items) if items.is_empty() => out.push_str("[]"),
            J::A(items) => {
                out.push_str("[\n");
                for (i, item) in items.iter().enumerate() {
                    out.push_str(&pad_inner);
                    item.write(out, indent + 1);
                    if i + 1 < items.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                out.push_str(&pad);
                out.push(']');
            }
            J::O(pairs) if pairs.is_empty() => out.push_str("{}"),
            J::O(pairs) => {
                out.push_str("{\n");
                for (i, (k, v)) in pairs.iter().enumerate() {
                    let _ = write!(out, "{pad_inner}\"{k}\": ");
                    v.write(out, indent + 1);
                    if i + 1 < pairs.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                out.push_str(&pad);
                out.push('}');
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ---------------------------------------------------------------------------
// Vector construction
// ---------------------------------------------------------------------------

struct Vectors {
    items: Vec<J>,
}

impl Vectors {
    fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// A frame the client must be able to **produce**.
    fn request(
        &mut self,
        name: &'static str,
        note: &'static str,
        frame: Vec<u8>,
        fields: Vec<(&'static str, J)>,
    ) {
        self.push("request", name, note, frame, fields);
    }

    /// A frame the client must be able to **parse**.
    fn response(
        &mut self,
        name: &'static str,
        note: &'static str,
        frame: Vec<u8>,
        fields: Vec<(&'static str, J)>,
    ) {
        self.push("response", name, note, frame, fields);
    }

    fn push(
        &mut self,
        direction: &'static str,
        name: &'static str,
        note: &'static str,
        frame: Vec<u8>,
        fields: Vec<(&'static str, J)>,
    ) {
        assert!(frame.len() >= 12, "{name}: frame shorter than a header");
        let body_len = u32::from_le_bytes(frame[8..12].try_into().unwrap()) as usize;
        assert_eq!(
            frame.len(),
            12 + body_len,
            "{name}: body_len disagrees with the frame length"
        );

        self.items.push(J::O(vec![
            ("name", J::s(name)),
            ("direction", J::s(direction)),
            ("note", J::s(note)),
            ("opcode", J::U(frame[0] as u64)),
            ("flags", J::U(frame[1] as u64)),
            (
                "status",
                J::U(u16::from_le_bytes(frame[2..4].try_into().unwrap()) as u64),
            ),
            (
                "request_id",
                J::U(u32::from_le_bytes(frame[4..8].try_into().unwrap()) as u64),
            ),
            ("body_len", J::U(body_len as u64)),
            ("frame", J::s(hex(&frame))),
            ("body", J::s(hex(&frame[12..]))),
            ("fields", J::O(fields)),
        ]));
    }

    fn finish(self, title: &str, description: &str) -> String {
        let doc = J::O(vec![
            ("title", J::s(title)),
            ("description", J::s(description)),
            ("protocol_version", J::U(vash_core::PROTOCOL_VERSION as u64)),
            ("generator", J::s("vash-proto/examples/emit_vectors.rs")),
            ("vectors", J::A(self.items)),
        ]);
        let mut out = String::new();
        doc.write(&mut out, 0);
        out.push('\n');
        out
    }
}

fn req(opcode: Opcode, id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = Vec::new();
    encode_request(&mut f, opcode, id, body);
    f
}

fn reply(opcode: Opcode, id: u32, r: &Reply) -> Vec<u8> {
    let mut f = Vec::new();
    encode_reply(&mut f, opcode, id, r);
    f
}

fn err(opcode: Opcode, id: u32, status: Status) -> Vec<u8> {
    let mut f = Vec::new();
    encode_error(&mut f, opcode as u8, id, status);
    f
}

fn arithmetic_body(op: &Arithmetic<'_>) -> Vec<u8> {
    let mut body = Vec::new();
    encode_arithmetic_body(&mut body, op);
    body
}

/// An `OK` with an empty body, which `encode_error` cannot express.
fn encode_ok_empty(opcode: Opcode, id: u32) -> Vec<u8> {
    let mut f = Vec::new();
    vash_proto::vcp::encode_response(&mut f, opcode, id, Status::Ok, &[]);
    f
}

fn value(data: &'static [u8], cas: u64) -> Value {
    Value {
        data: Bytes::from_static(data),
        mc_flags: 0,
        cas,
        expires_at_ms: None,
    }
}

fn keys(list: &[&str]) -> Vec<J> {
    list.iter().map(|k| J::s(*k)).collect()
}

// ---------------------------------------------------------------------------

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: emit_vectors <output-dir>");
        std::process::exit(2);
    });
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).expect("create output directory");

    write(&dir, "frames.json", frames());
    write(&dir, "strings.json", strings());

    println!("wrote frames.json and strings.json to {}", dir.display());
}

fn write(dir: &std::path::Path, name: &str, contents: String) {
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

// ---------------------------------------------------------------------------
// frames.json — every opcode, both directions, every status
// ---------------------------------------------------------------------------

fn frames() -> String {
    let mut v = Vectors::new();

    // --- The four exchanges printed in SPEC.md §2.5. Byte-for-byte. ---------

    let mut hello_body = Vec::new();
    hello_body.extend_from_slice(&vash_core::PROTOCOL_VERSION.to_le_bytes());
    hello_body.extend_from_slice(&0u16.to_le_bytes());
    v.request(
        "hello_request",
        "must be the first frame on every connection",
        req(Opcode::Hello, 1, &hello_body),
        vec![("protocol_version", J::U(1)), ("reserved", J::U(0))],
    );

    let info = ServerInfo {
        protocol_version: 1,
        shards: 1,
        max_key_len: 511,
        max_value_len: 1024 * 1024,
        capabilities: vash_core::capability::TAGS
            | vash_core::capability::MEMCACHED
            | vash_core::capability::RESP,
        max_tags_per_record: vash_core::DEFAULT_MAX_TAGS as u16,
    };
    v.response(
        "hello_response",
        "capabilities 0x43 = TAGS|MEMCACHED|RESP, what a DEFAULT server answers; CLUSTER absent \
         means invalidate every node yourself, FLUSH and LISTING absent means those opcodes are \
         disabled here",
        reply(Opcode::Hello, 1, &Reply::Hello(info)),
        vec![
            ("protocol_version", J::U(1)),
            ("shards", J::U(1)),
            ("max_key_len", J::U(511)),
            ("max_value_len", J::U(1048576)),
            ("capabilities", J::U(0x43)),
            ("max_tags_per_record", J::U(32)),
        ],
    );

    let mut set_foo = Vec::new();
    encode_set_body(&mut set_foo, b"foo", b"bar", 300, &[]);
    v.request(
        "set_request",
        "SPEC.md §2.5 golden bytes",
        req(Opcode::Set, 2, &set_foo),
        vec![
            ("key", J::s("foo")),
            ("value", J::s("bar")),
            ("ttl_secs", J::U(300)),
            ("tags", J::A(vec![])),
        ],
    );
    v.response(
        "set_response",
        "the new CAS token; 1 because it is the first write to an empty store",
        reply(Opcode::Set, 2, &Reply::Stored(vash_core::Stored::Stored(1))),
        vec![("cas", J::U(1))],
    );

    v.request(
        "get_request",
        "the whole body is the key — no length prefix",
        req(Opcode::Get, 3, b"foo"),
        vec![("key", J::s("foo"))],
    );
    v.response(
        "get_response_hit",
        "mc_flags is always 0 for a value written over VCP",
        reply(Opcode::Get, 3, &Reply::Value(value(b"bar", 1))),
        vec![
            ("mc_flags", J::U(0)),
            ("cas", J::U(1)),
            ("value", J::s("bar")),
        ],
    );
    v.response(
        "get_response_miss",
        "a miss is a status, not an empty value; the body is empty",
        reply(Opcode::Get, 4, &Reply::NotFound),
        vec![],
    );

    // --- PING --------------------------------------------------------------

    v.request(
        "ping_request",
        "empty body",
        req(Opcode::Ping, 10, &[]),
        vec![],
    );
    v.response(
        "ping_response",
        "liveness only — does not touch storage",
        reply(Opcode::Ping, 10, &Reply::Pong),
        vec![],
    );

    // --- SET variants ------------------------------------------------------

    let mut tagged = Vec::new();
    encode_set_body(
        &mut tagged,
        b"article:1",
        b"hello",
        300,
        &[b"news".as_slice(), b"sport"],
    );
    v.request(
        "set_request_tagged",
        "tag list follows the value: (tag_len u16, tag bytes) per tag",
        req(Opcode::Set, 11, &tagged),
        vec![
            ("key", J::s("article:1")),
            ("value", J::s("hello")),
            ("ttl_secs", J::U(300)),
            ("tags", J::A(keys(&["news", "sport"]))),
        ],
    );

    let max_tags: Vec<Vec<u8>> = (0..vash_core::DEFAULT_MAX_TAGS)
        .map(|i| format!("t{i:02}").into_bytes())
        .collect();
    let max_tag_refs: Vec<&[u8]> = max_tags.iter().map(|t| t.as_slice()).collect();
    let mut set_32 = Vec::new();
    encode_set_body(&mut set_32, b"k", b"v", 0, &max_tag_refs);
    v.request(
        "set_request_32_tags",
        "the server POLICY default. The format ceiling is 255 — validate there, not here",
        req(Opcode::Set, 12, &set_32),
        vec![
            ("key", J::s("k")),
            ("value", J::s("v")),
            ("ttl_secs", J::U(0)),
            ("tag_count", J::U(vash_core::DEFAULT_MAX_TAGS as u64)),
        ],
    );

    let mut empty_value = Vec::new();
    encode_set_body(&mut empty_value, b"k", b"", 0, &[]);
    v.request(
        "set_request_empty_value",
        "an empty value is valid and round-trips",
        req(Opcode::Set, 13, &empty_value),
        vec![
            ("key", J::s("k")),
            ("value", J::s("")),
            ("value_len", J::U(0)),
        ],
    );

    let mut ttl_absolute = Vec::new();
    encode_set_body(&mut ttl_absolute, b"k", b"v", 2_000_000_000, &[]);
    v.request(
        "set_request_ttl_long_offset",
        "a plain offset, 63 years out. VCP does NOT flip to a timestamp past 30 days — memcached does",
        req(Opcode::Set, 14, &ttl_absolute),
        vec![
            ("ttl_secs", J::U(2_000_000_000)),
            ("interpretation", J::s("relative offset in seconds")),
        ],
    );

    let mut ttl_boundary = Vec::new();
    encode_set_body(&mut ttl_boundary, b"k", b"v", vash_core::MAX_TTL_SECS, &[]);
    v.request(
        "set_request_ttl_boundary",
        "2592000 is memcached's threshold and means nothing on VCP — both sides of it are offsets",
        req(Opcode::Set, 15, &ttl_boundary),
        vec![
            ("ttl_secs", J::U(vash_core::MAX_TTL_SECS as u64)),
            ("interpretation", J::s("relative offset in seconds")),
        ],
    );

    let mut ttl_expired = Vec::new();
    encode_set_body(&mut ttl_expired, b"k", b"v", u32::MAX, &[]);
    v.request(
        "set_request_ttl_max",
        "u32::MAX is an offset that saturates at the 2106 ceiling — NOT a pre-expired record on VCP",
        req(Opcode::Set, 16, &ttl_expired),
        vec![
            ("ttl_secs", J::U(u32::MAX as u64)),
            ("interpretation", J::s("offset, saturates at the 2106 ceiling")),
        ],
    );

    let max_key = vec![b'k'; vash_core::MAX_KEY_LEN];
    let mut set_max_key = Vec::new();
    encode_set_body(&mut set_max_key, &max_key, b"v", 0, &[]);
    v.request(
        "set_request_max_key",
        "511 bytes is accepted; 512 is TOO_LARGE",
        req(Opcode::Set, 17, &set_max_key),
        vec![("key_len", J::U(vash_core::MAX_KEY_LEN as u64))],
    );

    let mut no_reply = req(Opcode::Set, 18, &set_foo);
    no_reply[1] = flags::NO_REPLY;
    v.request(
        "set_request_no_reply",
        "flags bit 1: the server does the work and sends nothing, including on failure",
        no_reply,
        vec![("flags", J::U(flags::NO_REPLY as u64))],
    );

    // --- DELETE / TOUCH ----------------------------------------------------

    v.request(
        "delete_request",
        "the whole body is the key",
        req(Opcode::Delete, 20, b"foo"),
        vec![("key", J::s("foo"))],
    );
    v.response(
        "delete_response_deleted",
        "OK means the key was live before the delete",
        reply(Opcode::Delete, 20, &Reply::Deleted),
        vec![("was_live", J::B(true))],
    );
    v.response(
        "delete_response_missing",
        "NOT_FOUND. An expired-but-unreclaimed record reports this — it was already invisible",
        reply(Opcode::Delete, 21, &Reply::NotFound),
        vec![("was_live", J::B(false))],
    );

    let mut touch = Vec::new();
    encode_touch_body(&mut touch, b"foo", 600);
    v.request(
        "touch_request",
        "ttl_secs u32 THEN the key — the opposite order from SET",
        req(Opcode::Touch, 22, &touch),
        vec![("ttl_secs", J::U(600)), ("key", J::s("foo"))],
    );
    v.response(
        "touch_response_ok",
        "value unchanged, CAS advances",
        reply(Opcode::Touch, 22, &Reply::Touched),
        vec![("was_live", J::B(true))],
    );

    // --- Batches -----------------------------------------------------------

    let mut get_many = Vec::new();
    encode_key_list_body(
        &mut get_many,
        &[b"a".as_slice(), b"missing", b"a", b"\xc3\xa9"],
    );
    v.request(
        "get_many_request",
        "duplicates are legal and produce duplicate slots; the map API collapses them",
        req(Opcode::GetMany, 30, &get_many),
        vec![
            ("count", J::U(4)),
            ("keys", J::A(keys(&["a", "missing", "a", "é"]))),
        ],
    );
    v.response(
        "get_many_response",
        "one slot per requested key, in request order; found u8 then payload only if 1",
        reply(
            Opcode::GetMany,
            30,
            &Reply::Values(vec![
                Some(value(b"1", 7)),
                None,
                Some(value(b"1", 7)),
                Some(value(b"", 9)),
            ]),
        ),
        vec![
            ("count", J::U(4)),
            (
                "found",
                J::A(vec![J::B(true), J::B(false), J::B(true), J::B(true)]),
            ),
            ("note", J::s("slot 3 is a hit with a zero-length value")),
        ],
    );

    let mut empty_batch = Vec::new();
    encode_key_list_body(&mut empty_batch, &[]);
    v.request(
        "get_many_request_empty",
        "an empty key list is VALID — do not short-circuit it into an error",
        req(Opcode::GetMany, 31, &empty_batch),
        vec![("count", J::U(0))],
    );
    v.response(
        "get_many_response_empty",
        "count 0, no items",
        reply(Opcode::GetMany, 31, &Reply::Values(vec![])),
        vec![("count", J::U(0))],
    );

    let mut set_many = Vec::new();
    encode_batch_count(&mut set_many, 2);
    encode_set_body(&mut set_many, b"a", b"1", 60, &[]);
    encode_set_body(&mut set_many, b"b", b"2", 60, &[b"news".as_slice()]);
    v.request(
        "set_many_request",
        "count, then whole SET bodies back to back — each carries its own TTL and tags",
        req(Opcode::SetMany, 32, &set_many),
        vec![
            ("count", J::U(2)),
            ("keys", J::A(keys(&["a", "b"]))),
            ("note", J::s("second entry is tagged 'news'")),
        ],
    );
    v.response(
        "set_many_response",
        "CAS per entry in request order. NOT ascending across shards — each shard numbers alone",
        reply(Opcode::SetMany, 32, &Reply::StoredMany(vec![41, 12])),
        vec![("count", J::U(2)), ("cas", J::A(vec![J::U(41), J::U(12)]))],
    );

    let mut delete_many = Vec::new();
    encode_key_list_body(&mut delete_many, &[b"a".as_slice(), b"missing"]);
    v.request(
        "delete_many_request",
        "same body layout as GET_MANY",
        req(Opcode::DeleteMany, 33, &delete_many),
        vec![("count", J::U(2))],
    );
    v.response(
        "delete_many_response",
        "one u8 per key: 1 if it was live",
        reply(
            Opcode::DeleteMany,
            33,
            &Reply::DeletedMany(vec![true, false]),
        ),
        vec![
            ("count", J::U(2)),
            ("was_live", J::A(vec![J::B(true), J::B(false)])),
        ],
    );

    // --- Tags, flush, cluster ---------------------------------------------

    v.request(
        "delete_by_tag_request",
        "the whole body is the tag name",
        req(Opcode::DeleteByTag, 40, b"news"),
        vec![("tag", J::s("news"))],
    );
    v.response(
        "delete_by_tag_response_ok",
        "constant time however many keys carried the tag",
        reply(Opcode::DeleteByTag, 40, &Reply::Invalidated(true)),
        vec![("existed", J::B(true))],
    );
    v.response(
        "delete_by_tag_response_unknown",
        "NOT_FOUND: the tag was never registered, so nothing could have carried it",
        reply(Opcode::DeleteByTag, 41, &Reply::Invalidated(false)),
        vec![("existed", J::B(false))],
    );

    v.request(
        "flush_request",
        "empty body",
        req(Opcode::Flush, 50, &[]),
        vec![],
    );
    v.response(
        "flush_response",
        "the new flush epoch",
        reply(Opcode::Flush, 50, &Reply::Flushed(2)),
        vec![("epoch", J::U(2))],
    );
    v.response(
        "flush_response_unauthorized",
        "the DEFAULT server answers this — flush is off unless protocol.flush_enabled",
        err(Opcode::Flush, 51, Status::Unauthorized),
        vec![],
    );

    v.request(
        "cluster_request",
        "empty body",
        req(Opcode::Cluster, 60, &[]),
        vec![],
    );
    v.response(
        "cluster_response",
        "membership is static config — what this node was told, not a negotiated set",
        reply(
            Opcode::Cluster,
            60,
            &Reply::Cluster(ClusterInfo {
                mode: ClusterMode::Fanout,
                peers: vec![
                    PeerInfo {
                        addr: "10.0.0.2:11311".into(),
                        reachable: true,
                    },
                    PeerInfo {
                        addr: "10.0.0.3:11311".into(),
                        reachable: false,
                    },
                ],
            }),
        ),
        vec![
            ("mode", J::U(1)),
            ("mode_name", J::s("fanout")),
            ("peer_count", J::U(2)),
            ("reachable", J::A(vec![J::B(true), J::B(false)])),
        ],
    );
    v.response(
        "cluster_response_standalone",
        "no peers: the client must invalidate on every node itself",
        reply(
            Opcode::Cluster,
            61,
            &Reply::Cluster(ClusterInfo {
                mode: ClusterMode::Local,
                peers: vec![],
            }),
        ),
        vec![
            ("mode", J::U(0)),
            ("mode_name", J::s("local")),
            ("peer_count", J::U(0)),
        ],
    );

    // --- AUTH (0x03) -------------------------------------------------------
    //
    // The one opcode whose reply is sent even under NO_REPLY, and one of three
    // bodies that reject trailing bytes.

    let mut auth = Vec::new();
    encode_auth_body(&mut auth, 0, b"web", b"s3cret");
    v.request(
        "auth_request",
        "mechanism 0 is PLAIN, the only one implemented; trailing bytes are BAD_REQUEST",
        req(Opcode::Auth, 80, &auth),
        vec![
            ("mechanism", J::U(0)),
            ("name", J::s("web")),
            ("secret", J::s("s3cret")),
        ],
    );

    let mut auth_default = Vec::new();
    encode_auth_body(&mut auth_default, 0, b"", b"s3cret");
    v.request(
        "auth_request_default_identity",
        "name_len 0 means the server's `default` identity, not an empty name",
        req(Opcode::Auth, 81, &auth_default),
        vec![("mechanism", J::U(0)), ("name_len", J::U(0))],
    );

    v.response(
        "auth_response_ok",
        "empty body; the reply carries nothing but the status",
        encode_ok_empty(Opcode::Auth, 80),
        vec![],
    );
    v.response(
        "auth_response_refused",
        "a bad name and a bad secret are the same answer, so it does not confirm which names exist",
        err(Opcode::Auth, 81, Status::Unauthorized),
        vec![],
    );

    // --- ARITHMETIC (0x14) -------------------------------------------------
    //
    // A fixed 32-byte prefix carries all three numeric domains, with delta,
    // lower and upper reinterpreted by the mode byte. Every enum byte gets a
    // vector because none of them is defaulted on the server.

    let counter = Arithmetic {
        key: Key::new(b"hits").expect("valid key"),
        delta: Delta::Counter {
            delta: 1,
            decrement: false,
        },
        on_bound: OnBound::Fail,
        missing: Missing::CreateAtZero,
        ttl: TtlChange::Keep,
    };
    v.request(
        "arithmetic_request_counter",
        "mode 0 COUNTER: delta is a u64, lower and upper are ignored",
        req(Opcode::Arithmetic, 90, &arithmetic_body(&counter)),
        vec![
            ("mode", J::s("counter")),
            ("delta", J::U(1)),
            ("create_at_zero", J::B(true)),
        ],
    );

    let decrement = Arithmetic {
        delta: Delta::Counter {
            delta: 5,
            decrement: true,
        },
        missing: Missing::Fail,
        ..counter.clone()
    };
    v.request(
        "arithmetic_request_decrement",
        "flags bit 1 DECREMENT is counter-mode only; the other domains sign the delta instead",
        req(Opcode::Arithmetic, 91, &arithmetic_body(&decrement)),
        vec![
            ("mode", J::s("counter")),
            ("delta", J::U(5)),
            ("decrement", J::B(true)),
            ("create_at_zero", J::B(false)),
        ],
    );

    let signed = Arithmetic {
        delta: Delta::Int {
            delta: -3,
            lower: -100,
            upper: 100,
        },
        on_bound: OnBound::Clamp,
        ttl: TtlChange::Set(60),
        ..counter.clone()
    };
    v.request(
        "arithmetic_request_int_clamp",
        "mode 1 INT: all three numbers are i64 two's complement. on_bound 2 clamps",
        req(Opcode::Arithmetic, 92, &arithmetic_body(&signed)),
        vec![
            ("mode", J::s("int")),
            ("delta", J::s("-3")),
            ("lower", J::s("-100")),
            ("upper", J::s("100")),
            ("on_bound", J::s("clamp")),
            ("ttl_secs", J::U(60)),
        ],
    );

    let float = Arithmetic {
        delta: Delta::Float {
            delta: 1.5,
            lower: f64::NEG_INFINITY,
            upper: f64::INFINITY,
        },
        on_bound: OnBound::Skip,
        ttl: TtlChange::SetIfPersistent(300),
        ..counter.clone()
    };
    v.request(
        "arithmetic_request_float_skip",
        "mode 2 FLOAT: IEEE-754 bit patterns. Unbounded passes the type's own limits",
        req(Opcode::Arithmetic, 93, &arithmetic_body(&float)),
        vec![
            ("mode", J::s("float")),
            ("delta", J::s("1.5")),
            ("lower", J::s("-inf")),
            ("upper", J::s("inf")),
            ("on_bound", J::s("skip")),
            ("ttl_kind", J::s("set_if_persistent")),
            ("ttl_secs", J::U(300)),
        ],
    );

    v.response(
        "arithmetic_response_counter",
        "the mode is ECHOED so a client decodes the reply without remembering what it asked",
        reply(
            Opcode::Arithmetic,
            90,
            &Reply::Arithmetic(Applied {
                value: Number::Counter(11),
                applied: Number::Counter(10),
                wrote: true,
            }),
        ),
        vec![
            ("mode", J::U(0)),
            ("wrote", J::B(true)),
            ("value", J::U(11)),
            ("applied", J::U(10)),
        ],
    );

    v.response(
        "arithmetic_response_int_negative",
        "value and applied are read in the reply's own domain — here as i64",
        reply(
            Opcode::Arithmetic,
            92,
            &Reply::Arithmetic(Applied {
                value: Number::Int(-25),
                applied: Number::Int(-3),
                wrote: true,
            }),
        ),
        vec![
            ("mode", J::U(1)),
            ("value", J::s("-25")),
            ("applied", J::s("-3")),
        ],
    );

    v.response(
        "arithmetic_response_float",
        "IEEE-754 bits again on the way back",
        reply(
            Opcode::Arithmetic,
            93,
            &Reply::Arithmetic(Applied {
                value: Number::Float(1.75),
                applied: Number::Float(0.25),
                wrote: true,
            }),
        ),
        vec![
            ("mode", J::U(2)),
            ("value", J::s("1.75")),
            ("applied", J::s("0.25")),
        ],
    );

    v.response(
        "arithmetic_response_skipped",
        "wrote 0: a bound held the value, so NOTHING was stored - not even the deadline. \
         The only way to tell this from an increment that moved the counter by zero",
        reply(
            Opcode::Arithmetic,
            93,
            &Reply::Arithmetic(Applied {
                value: Number::Counter(10),
                applied: Number::Counter(0),
                wrote: false,
            }),
        ),
        vec![
            ("wrote", J::B(false)),
            ("value", J::U(10)),
            ("applied", J::U(0)),
        ],
    );

    v.response(
        "arithmetic_response_not_numeric",
        "a stored value that does not parse in the requested domain, or a bound breached under \
         on_bound 0",
        err(Opcode::Arithmetic, 94, Status::NotNumeric),
        vec![],
    );

    // --- LIST_KEYS (0x50) and LIST_TAGS (0x51) -----------------------------
    //
    // The two share a request and response body field for field, so one
    // decoder and one pagination loop serve both.

    let mut first_page = Vec::new();
    encode_list_body(&mut first_page, 100, b"", b"session:*");
    v.request(
        "list_keys_request",
        "an empty cursor starts from the beginning; the pattern is a byte-wise glob",
        req(Opcode::ListKeys, 100, &first_page),
        vec![
            ("limit", J::U(100)),
            ("cursor_len", J::U(0)),
            ("pattern", J::s("session:*")),
        ],
    );

    let mut resumed = Vec::new();
    encode_list_body(&mut resumed, 100, b"session:042", b"session:*");
    v.request(
        "list_keys_request_resumed",
        "a cursor is OPAQUE: echo back exactly the bytes the previous page returned",
        req(Opcode::ListKeys, 101, &resumed),
        vec![
            ("limit", J::U(100)),
            ("cursor_len", J::U(11)),
            ("pattern", J::s("session:*")),
        ],
    );

    let mut tags_request = Vec::new();
    encode_list_body(&mut tags_request, 1024, b"", b"");
    v.request(
        "list_tags_request",
        "identical body to LIST_KEYS; an empty pattern matches everything",
        req(Opcode::ListTags, 102, &tags_request),
        vec![("limit", J::U(1024)), ("pattern", J::s(""))],
    );

    v.response(
        "list_response_page",
        "a non-empty cursor means there is more. Entries are TAG_SYNC's layout: version, len, name",
        reply(
            Opcode::ListKeys,
            100,
            &Reply::Listing(Listing {
                entries: vec![
                    ListEntry::new(b"session:001".to_vec(), 41),
                    ListEntry::new(b"session:002".to_vec(), 57),
                ],
                scanned: 90_000,
                cursor: Some(b"session:002".to_vec().into_boxed_slice()),
                truncated: true,
            }),
        ),
        vec![
            ("count", J::U(2)),
            ("truncated", J::B(true)),
            ("scanned", J::U(90_000)),
            ("cursor", J::s("session:002")),
            ("names", J::A(keys(&["session:001", "session:002"]))),
        ],
    );

    v.response(
        "list_response_complete",
        "an EMPTY cursor means the listing is complete, and that is the whole termination rule. \
         Expect this empty last page even after a page that filled `limit` exactly",
        reply(
            Opcode::ListKeys,
            101,
            &Reply::Listing(Listing {
                entries: Vec::new(),
                scanned: 12,
                cursor: None,
                truncated: false,
            }),
        ),
        vec![
            ("count", J::U(0)),
            ("cursor_len", J::U(0)),
            ("scanned", J::U(12)),
        ],
    );

    v.response(
        "list_response_no_matches",
        "a pattern matching nothing is count 0, NEVER NOT_FOUND - no matches is not a miss",
        reply(
            Opcode::ListTags,
            102,
            &Reply::Listing(Listing {
                entries: Vec::new(),
                scanned: 4096,
                cursor: None,
                truncated: false,
            }),
        ),
        vec![("count", J::U(0)), ("scanned", J::U(4096))],
    );

    v.response(
        "list_response_disabled",
        "the DEFAULT server answers this - the listings are off unless protocol.listing_enabled",
        err(Opcode::ListKeys, 103, Status::Unauthorized),
        vec![],
    );

    // --- Every error status ------------------------------------------------

    for (name, note, status) in [
        (
            "error_bad_request",
            "malformed body, empty key, oversized batch",
            Status::BadRequest,
        ),
        (
            "error_too_large",
            "key, value or tag over its limit",
            Status::TooLarge,
        ),
        (
            "error_overloaded",
            "write queue full or shutting down — RETRYABLE with backoff",
            Status::Overloaded,
        ),
        (
            "error_capacity_full",
            "the store is out of space, or the tag registry is full",
            Status::CapacityFull,
        ),
        (
            "error_unsupported",
            "unknown opcode or unsupported protocol version",
            Status::Unsupported,
        ),
        (
            "error_internal",
            "server-side failure; details are logged, not sent",
            Status::Internal,
        ),
    ] {
        v.response(name, note, err(Opcode::Get, 70, status), vec![]);
    }

    // Reserved statuses. Not emitted over VCP today, but a client must map them
    // rather than special-case them, so the corpus carries them.
    for (name, status) in [
        ("error_reserved_exists", Status::Exists),
        ("error_reserved_not_stored", Status::NotStored),
        ("error_reserved_not_numeric", Status::NotNumeric),
    ] {
        v.response(
            name,
            "reserved for conditional writes and arithmetic; map it, do not special-case it",
            err(Opcode::Set, 71, status),
            vec![],
        );
    }

    // Hand-built: the encoder cannot produce a status it has no variant for,
    // and that is exactly the case a client must survive.
    let mut unknown_status = err(Opcode::Get, 72, Status::Ok);
    unknown_status[2..4].copy_from_slice(&200u16.to_le_bytes());
    v.response(
        "error_unknown_status",
        "status 200 is not in this build's enum. FAIL THE OPERATION, not the frame",
        unknown_status,
        vec![("status", J::U(200))],
    );

    let mut unknown_opcode = err(Opcode::Get, 73, Status::Unsupported);
    unknown_opcode[0] = 0xfe;
    v.response(
        "error_unknown_opcode_echoed",
        "the ORIGINAL opcode byte is echoed even when unknown, so the client can still correlate",
        unknown_opcode,
        vec![("opcode", J::U(0xfe))],
    );

    v.finish(
        "VCP frame vectors",
        "Generated from the server's own encoders. Each vector is one complete frame: \
         `frame` is the full hex including the 12-byte header, `body` is the hex after it, \
         and `fields` is what a correct decoder must recover. Run both directions.",
    )
}

// ---------------------------------------------------------------------------
// strings.json — the UTF-8 boundary
// ---------------------------------------------------------------------------

fn strings() -> String {
    let mut v = Vectors::new();

    // Character count and byte count differ by up to 4x. Every limit the server
    // enforces is a BYTE limit, so a client validating on string length is
    // wrong, and wrong in the direction that loses whole batches.
    for (name, key, val, chars, bytes) in [
        ("utf8_1_byte_ascii", "plain", "value", 5usize, 5usize),
        ("utf8_2_byte_latin", "café", "café", 4, 5),
        ("utf8_3_byte_cjk", "日本語", "日本語", 3, 9),
        ("utf8_4_byte_emoji", "🔑", "🔑", 1, 4),
        ("utf8_mixed", "a日🔑", "a日🔑", 3, 8),
    ] {
        let mut body = Vec::new();
        encode_set_body(&mut body, key.as_bytes(), val.as_bytes(), 0, &[]);
        assert_eq!(key.chars().count(), chars, "{name}: char count");
        assert_eq!(key.len(), bytes, "{name}: byte count");

        v.request(
            name,
            "key_len and value_len are BYTE counts, never character counts",
            req(Opcode::Set, 100, &body),
            vec![
                ("key", J::s(key)),
                ("value", J::s(val)),
                ("key_chars", J::U(chars as u64)),
                ("key_bytes", J::U(bytes as u64)),
            ],
        );
    }

    // 170 CJK characters: comfortably under 511 by character count, 510 bytes —
    // still legal. One more character is 513 bytes and must be rejected
    // LOCALLY. A client counting characters sends it and loses the request.
    let cjk_key: String = "日".repeat(170);
    assert_eq!(cjk_key.chars().count(), 170);
    assert_eq!(cjk_key.len(), 510);
    let mut body = Vec::new();
    encode_set_body(&mut body, cjk_key.as_bytes(), b"v", 0, &[]);
    v.request(
        "utf8_key_near_byte_limit",
        "170 characters, 510 bytes — legal. 171 characters is 513 bytes and must be rejected locally",
        req(Opcode::Set, 101, &body),
        vec![
            ("key_chars", J::U(170)),
            ("key_bytes", J::U(510)),
            ("max_key_len", J::U(vash_core::MAX_KEY_LEN as u64)),
        ],
    );

    // Valid UTF-8, and the reason the C API takes an explicit length.
    let mut nul_body = Vec::new();
    encode_set_body(&mut nul_body, b"k", "a\0b".as_bytes(), 0, &[]);
    v.request(
        "utf8_embedded_nul",
        "U+0000 is valid UTF-8 and must survive — a NUL-terminated C API truncates here",
        req(Opcode::Set, 102, &nul_body),
        vec![("value_bytes", J::U(3)), ("value_chars", J::U(3))],
    );

    let mut tag_body = Vec::new();
    encode_set_body(
        &mut tag_body,
        b"k",
        b"v",
        0,
        &["ニュース".as_bytes(), "スポーツ".as_bytes()],
    );
    v.request(
        "utf8_tags",
        "tags are 1-255 BYTES; these are 4 characters and 12 bytes each",
        req(Opcode::Set, 103, &tag_body),
        vec![
            ("tags", J::A(keys(&["ニュース", "スポーツ"]))),
            ("tag_bytes_each", J::U(12)),
        ],
    );

    // The decode side. The store is binary-safe and shared with memcached and
    // Redis clients, so a value this client did not write can be any bytes at
    // all. U+FFFD substitution here is a conformance failure.
    v.response(
        "invalid_utf8_value",
        "0xff 0xfe is not UTF-8. MUST raise VashEncodingError — never substitute U+FFFD",
        reply(Opcode::Get, 104, &Reply::Value(value(b"\xff\xfe", 3))),
        vec![
            ("value_hex", J::s("fffe")),
            ("expect", J::s("VashEncodingError")),
            ("must_not", J::s("U+FFFD replacement")),
        ],
    );

    v.response(
        "truncated_utf8_value",
        "a 3-byte sequence cut to 2 bytes — the shape a naive byte-slicing cache produces",
        reply(Opcode::Get, 105, &Reply::Value(value(b"\xe6\x97", 4))),
        vec![
            ("value_hex", J::s("e697")),
            ("expect", J::s("VashEncodingError")),
        ],
    );

    v.response(
        "valid_utf8_value_multibyte",
        "the positive control for the two above",
        reply(
            Opcode::Get,
            106,
            &Reply::Value(value("日本語".as_bytes(), 5)),
        ),
        vec![("value", J::s("日本語")), ("value_bytes", J::U(9))],
    );

    v.finish(
        "UTF-8 boundary vectors",
        "The encoding boundary is where nine implementations disagree. Requests here must be \
         producible; responses must decode strictly — an invalid sequence raises rather than \
         substituting U+FFFD. See SPEC.md §6.",
    )
}
