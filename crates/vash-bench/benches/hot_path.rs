//! Micro-benchmarks for the per-request hot path.
//!
//! ```text
//! cargo bench -p vash-bench
//! ```
//!
//! Everything here runs **per request**, so a regression is multiplied by the
//! ops/s the server is trying to reach. That is also the reason to measure it
//! separately from the end-to-end load test: at a million operations a second a
//! request has roughly a microsecond of CPU to spend in total, and these numbers
//! say how much of it the framing and liveness checks are taking before any
//! storage work happens at all.
//!
//! The end-to-end throughput and latency figures — the ones the §13 goals are
//! stated in — come from the `load` binary, not from here.

use vash_core::{Command, RecordMeta, RecordRef, TagRef, encode_record};
use vash_proto::memcached;
use vash_proto::vcp::{self, Opcode, encode_request, encode_set_body};

fn main() {
    divan::main();
}

/// A 1 KiB value, the size the performance goals are stated for.
const VALUE: &[u8] = &[b'x'; 1024];

// ---- framing ---------------------------------------------------------------

/// Decoding a `GET`: the single most executed piece of code in the server.
#[divan::bench]
fn vcp_decode_get(bencher: divan::Bencher) {
    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Get, 1, b"user:1234:profile");

    bencher.bench(|| {
        let decoded = vcp::decode(divan::black_box(&frame));
        divan::black_box(matches!(decoded, Ok(vcp::Decoded::Request { .. })))
    });
}

/// The cheap pre-pass the connection uses to split a frame off the read buffer
/// before it decodes anything. It runs once per frame on top of the decode, so
/// it has to be nearly free.
#[divan::bench]
fn vcp_peek_frame_len(bencher: divan::Bencher) {
    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Get, 1, b"user:1234:profile");

    bencher.bench(|| divan::black_box(vcp::peek_frame_len(divan::black_box(&frame))));
}

/// Decoding a `SET` with a 1 KiB value. Should cost about the same as a `GET`:
/// the value is a borrowed subslice, never copied, so size must not show up
/// here. If it starts scaling with the value, something has begun copying.
#[divan::bench(args = [0, 1024, 65536])]
fn vcp_decode_set(bencher: divan::Bencher, value_len: usize) {
    let value = vec![b'x'; value_len];
    let mut body = Vec::new();
    encode_set_body(&mut body, b"user:1234:profile", &value, 300, &[]);
    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Set, 1, &body);

    bencher.bench(|| {
        let decoded = vcp::decode(divan::black_box(&frame));
        divan::black_box(matches!(decoded, Ok(vcp::Decoded::Request { .. })))
    });
}

/// A 64-key multi-get, which is one frame's worth of work for 64 requests.
#[divan::bench]
fn vcp_decode_get_many(bencher: divan::Bencher) {
    let keys: Vec<String> = (0..64).map(|i| format!("user:{i}:profile")).collect();
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    let mut body = Vec::new();
    vcp::encode_key_list_body(&mut body, &refs);
    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::GetMany, 1, &body);

    bencher.bench(|| {
        let decoded = vcp::decode(divan::black_box(&frame));
        divan::black_box(matches!(decoded, Ok(vcp::Decoded::Request { .. })))
    });
}

/// The same request over the memcached text protocol. The gap between this and
/// `vcp_decode_get` is what the native protocol buys per request, and is the
/// concrete form of plan §3's "text framing costs".
#[divan::bench]
fn memcached_parse_get(bencher: divan::Bencher) {
    let line = b"get user:1234:profile\r\n";
    bencher.bench(|| divan::black_box(memcached::parse(divan::black_box(line)).is_ok()));
}

#[divan::bench]
fn memcached_parse_set(bencher: divan::Bencher) {
    let mut input = b"set user:1234:profile 0 300 1024\r\n".to_vec();
    input.extend_from_slice(VALUE);
    input.extend_from_slice(b"\r\n");

    bencher.bench(|| divan::black_box(memcached::parse(divan::black_box(&input)).is_ok()));
}

/// The meta dialect, whose flags are parsed one character at a time.
#[divan::bench]
fn memcached_parse_meta_get(bencher: divan::Bencher) {
    let line = b"mg user:1234:profile v f s c t k Oopaque\r\n";
    bencher.bench(|| divan::black_box(memcached::parse(divan::black_box(line)).is_ok()));
}

// ---- record and liveness ---------------------------------------------------

/// Parsing a stored record. This is a cast plus bounds checks over bytes that
/// are already in the page cache, so it should be single-digit nanoseconds; if
/// it is not, the zero-copy claim in plan §8 has quietly stopped being true.
#[divan::bench(args = [0, 1, 4, 32])]
fn record_parse(bencher: divan::Bencher, tag_count: usize) {
    let tags: Vec<TagRef> = (0..tag_count as u32).map(|i| TagRef::new(i, 1)).collect();
    let mut buf = Vec::new();
    encode_record(&mut buf, RecordMeta::default(), &tags, VALUE).expect("within the tag limit");

    bencher.bench(|| divan::black_box(RecordRef::parse(divan::black_box(&buf)).is_ok()));
}

/// The liveness check: expiry, flush epoch and every tag generation, all from
/// RAM. It decides whether a record is served, so it runs on every read of
/// every key — and it is what makes O(1) tag invalidation possible, so its cost
/// is the price of that design.
#[divan::bench(args = [0, 1, 4, 32])]
fn record_liveness(bencher: divan::Bencher, tag_count: usize) {
    let tags: Vec<TagRef> = (0..tag_count as u32).map(|i| TagRef::new(i, 1)).collect();
    let mut buf = Vec::new();
    encode_record(&mut buf, RecordMeta::default(), &tags, VALUE).expect("within the tag limit");
    let record = RecordRef::parse(&buf).expect("valid");

    bencher.bench(|| {
        divan::black_box(record.is_alive(divan::black_box(1_700_000_000_000), 0, |_| Some(1)))
    });
}

/// Building the record a write stores. Unlike everything else here this is
/// O(value), because the value has to be copied into the record exactly once —
/// the one copy plan §8 accepts.
#[divan::bench(args = [64, 1024, 16384])]
fn record_encode(bencher: divan::Bencher, value_len: usize) {
    let value = vec![b'x'; value_len];
    let tags = [TagRef::new(1, 1)];

    bencher.bench(|| {
        let mut buf = Vec::new();
        encode_record(
            &mut buf,
            RecordMeta::default(),
            &tags,
            divan::black_box(&value),
        )
        .ok();
        divan::black_box(buf.len())
    });
}

// ---- routing ---------------------------------------------------------------

/// Shard selection. Runs once per key on every request, including once per key
/// in a multi-get, and has to be a stable hash rather than the fast default one
/// — so this is the cost of that requirement (plan §9).
#[divan::bench(args = [8, 32, 250])]
fn shard_routing_hash(bencher: divan::Bencher, key_len: usize) {
    let key = vec![b'k'; key_len];
    bencher.bench(|| divan::black_box(xxhash_rust::xxh3::xxh3_64(divan::black_box(&key))));
}

// ---- reply encoding --------------------------------------------------------

/// Rendering a `GET` hit. The value is appended straight from the store into
/// the write buffer, which is the second and last copy on a read path.
#[divan::bench]
fn vcp_encode_value_reply(bencher: divan::Bencher) {
    let reply = vash_core::Reply::Value(vash_core::Value {
        data: bytes::Bytes::from_static(VALUE),
        mc_flags: 0,
        cas: 42,
        expires_at_ms: Some(0),
    });

    bencher.bench(|| {
        let mut out = Vec::new();
        vcp::encode_reply(&mut out, Opcode::Get, 1, divan::black_box(&reply));
        divan::black_box(out.len())
    });
}

/// The same reply in the memcached dialect, where the lengths are rendered as
/// decimal text.
#[divan::bench]
fn memcached_encode_value_reply(bencher: divan::Bencher) {
    let reply = vash_core::Reply::Values(vec![Some(vash_core::Value {
        data: bytes::Bytes::from_static(VALUE),
        mc_flags: 0,
        cas: 42,
        expires_at_ms: Some(0),
    })]);
    let command = Command::GetMany(vec![vash_core::Key::from_stored(b"user:1234:profile")]);
    let style = memcached::encode::ResponseStyle::Retrieval { with_cas: false };

    bencher.bench(|| {
        let mut out = Vec::new();
        memcached::encode::encode(&mut out, &style, &command, divan::black_box(&reply));
        divan::black_box(out.len())
    });
}

/// The same memcached hit rendered into a buffer that already exists, which is
/// what actually happens — `conn::handle` keeps one write buffer per connection
/// and clears it between reads.
///
/// The two arms above allocate their output afresh every iteration, so the
/// allocator dominates them and small changes inside the encoder hide in the
/// noise. This one holds the buffer still and measures the rendering, which is
/// the only way to see what the decimal writers cost: a `VALUE` line spells
/// three integers, and each was a `String` allocation until `digits` replaced
/// them.
#[divan::bench]
fn memcached_encode_value_reply_reusing_the_buffer(bencher: divan::Bencher) {
    let reply = vash_core::Reply::Values(vec![Some(vash_core::Value {
        data: bytes::Bytes::from_static(VALUE),
        mc_flags: 0,
        cas: 42,
        expires_at_ms: Some(0),
    })]);
    let command = Command::GetMany(vec![vash_core::Key::from_stored(b"user:1234:profile")]);
    let style = memcached::encode::ResponseStyle::Retrieval { with_cas: true };

    let mut out = Vec::with_capacity(VALUE.len() + 64);
    bencher.bench_local(|| {
        out.clear();
        memcached::encode::encode(&mut out, &style, &command, divan::black_box(&reply));
        divan::black_box(out.len())
    });
}

/// The RESP equivalent: a bulk string, whose length prefix is the one integer
/// every Redis `GET` hit has to spell.
#[divan::bench]
fn resp_encode_bulk_reusing_the_buffer(bencher: divan::Bencher) {
    let mut out = Vec::with_capacity(VALUE.len() + 64);
    bencher.bench_local(|| {
        out.clear();
        vash_proto::resp::encode::bulk(&mut out, divan::black_box(VALUE));
        divan::black_box(out.len())
    });
}

// ---- the copy on the read path ---------------------------------------------
//
// M6 recorded that a value is copied twice on the way out — once out of the map
// into a `Value`, once out of that into the write buffer — where plan §8 wanted
// one, and that removing the first means encoding while the read transaction is
// still open. That trade costs the `Store`/`Reply` boundary, so it is worth
// knowing what it buys before paying for it.
//
// **Both arms end in the same place: bytes appended to a write buffer.** That
// is the only fair comparison, and it is why the answer is smaller than "one
// copy in three". The first copy is also what pulls the value into cache; take
// it away and the remaining copy pays the misses the other one was absorbing.

/// Today's path: `get` copies the value out of the map, then the encoder copies
/// it into the write buffer.
#[divan::bench(args = [64, 1024, 4096, 16384], sample_size = 500)]
fn read_copy_then_encode(bencher: divan::Bencher, value_len: usize) {
    let store = ReadFixture::new(value_len);
    let key = vash_core::Key::from_stored(ReadFixture::KEY);

    // Reused across iterations, exactly as the connection's write buffer is —
    // measuring the read, not the allocator.
    let mut out = Vec::with_capacity(value_len + 64);
    bencher.bench_local(|| {
        out.clear();
        let value = vash_store::Store::get(&store.store, divan::black_box(key))
            .unwrap()
            .unwrap();
        out.extend_from_slice(&value.data);
        divan::black_box(out.len())
    });
}

/// What a plain memcached `get` naming one key used to do: a batch read, whose
/// reply is a `Vec<Option<Value>>` — so the copy above plus a heap allocation
/// for the vector and the `Option` slot holding it.
///
/// This is the single most executed command the server serves, because it is
/// what every memcached client library sends for a one-key read. It was on this
/// path only because the grammar admits more than one key.
#[divan::bench(args = [64, 1024, 4096, 16384], sample_size = 500)]
fn read_batch_of_one_then_encode(bencher: divan::Bencher, value_len: usize) {
    let store = ReadFixture::new(value_len);
    let keys = [vash_core::Key::from_stored(ReadFixture::KEY)];

    let mut out = Vec::with_capacity(value_len + 64);
    bencher.bench_local(|| {
        out.clear();
        let values = vash_store::Store::get_many(&store.store, divan::black_box(&keys)).unwrap();
        for value in values.into_iter().flatten() {
            out.extend_from_slice(&value.data);
        }
        divan::black_box(out.len())
    });
}

/// The proposed path: the value is appended straight out of the map, inside the
/// read transaction, and never lands in a `Value` at all.
#[divan::bench(args = [64, 1024, 4096, 16384], sample_size = 500)]
fn read_encode_in_txn(bencher: divan::Bencher, value_len: usize) {
    let store = ReadFixture::new(value_len);
    let key = vash_core::Key::from_stored(ReadFixture::KEY);

    let mut out = Vec::with_capacity(value_len + 64);
    bencher.bench_local(|| {
        out.clear();
        vash_store::Store::get_with(&store.store, divan::black_box(key), &mut |value| {
            out.extend_from_slice(value.data)
        })
        .unwrap();
        divan::black_box(out.len())
    });
}

/// One populated single-shard environment, kept alive with its directory.
struct ReadFixture {
    store: vash_store::LmdbStore,
    _dir: tempfile::TempDir,
}

impl ReadFixture {
    const KEY: &'static [u8] = b"user:1234:profile";

    fn new(value_len: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = vash_store::StoreConfig {
            path: dir.path().join("db"),
            map_size: 256 * 1024 * 1024,
            shards: 1,
            ..vash_store::StoreConfig::default()
        };
        let store = vash_store::LmdbStore::open(&config).unwrap();
        vash_store::Store::set(
            &store,
            &vash_core::Set {
                key: vash_core::Key::new(Self::KEY).unwrap(),
                value: &vec![b'x'; value_len],
                ttl: vash_core::TtlChange::Set(0),
                return_previous: false,
                mc_flags: 0,
                tags: Vec::new(),
                mode: vash_core::SetMode::Set,
            },
        )
        .unwrap();
        Self { store, _dir: dir }
    }
}
