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
#[divan::bench(sample_size = 500)]
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
#[divan::bench(sample_size = 500)]
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
#[divan::bench(sample_size = 500)]
fn memcached_parse_get(bencher: divan::Bencher) {
    let line = b"get user:1234:profile\r\n";
    bencher.bench(|| divan::black_box(memcached::parse(divan::black_box(line)).is_ok()));
}

/// `delete` is the same shape as `get` — one verb, one key, one line — and it
/// has never allocated, because `Command::Delete { key }` holds the key
/// directly.
///
/// It is here as the control for [`memcached_parse_get`]. `get` decodes into
/// `Command::GetMany`, which used to be a `Vec<Key>` and so allocated for one
/// borrowed key; the gap between the two arms was ~50ns, paid **twice** per
/// command because the connection parses each block once to find where its
/// commands end and again to run them. `KeyList` closed it. The two arms
/// should now sit close together, and this one is what says so.
#[divan::bench(sample_size = 500)]
fn memcached_parse_delete(bencher: divan::Bencher) {
    let line = b"delete user:1234:profile\r\n";
    bencher.bench(|| divan::black_box(memcached::parse(divan::black_box(line)).is_ok()));
}

#[divan::bench(sample_size = 500)]
fn memcached_parse_set(bencher: divan::Bencher) {
    let mut input = b"set user:1234:profile 0 300 1024\r\n".to_vec();
    input.extend_from_slice(VALUE);
    input.extend_from_slice(b"\r\n");

    bencher.bench(|| divan::black_box(memcached::parse(divan::black_box(&input)).is_ok()));
}

/// The meta dialect, whose flags are parsed one character at a time.
#[divan::bench(sample_size = 500)]
fn memcached_parse_meta_get(bencher: divan::Bencher) {
    let line = b"mg user:1234:profile v f s c t k Oopaque\r\n";
    bencher.bench(|| divan::black_box(memcached::parse(divan::black_box(line)).is_ok()));
}

/// The same request over RESP, which is what a Redis client sends.
///
/// Unlike the memcached arms there is no allocation-free control command to
/// compare against — every RESP command travels as the same multibulk array —
/// so a change here is judged against the memcached and VCP arms in the same
/// run, which it does not touch. A round where those moved is a round to
/// discard. See plan §13.
#[divan::bench(sample_size = 500)]
fn resp_parse_get(bencher: divan::Bencher) {
    let line = b"*2\r\n$3\r\nGET\r\n$17\r\nuser:1234:profile\r\n";
    bencher.bench(|| divan::black_box(vash_proto::resp::parse(divan::black_box(line)).is_ok()));
}

/// `SET key value EX 300` — five arguments, which is the shape a client library
/// sends for a write with a TTL and the widest common command.
#[divan::bench(sample_size = 500)]
fn resp_parse_set(bencher: divan::Bencher) {
    let mut input = b"*5\r\n$3\r\nSET\r\n$17\r\nuser:1234:profile\r\n$1024\r\n".to_vec();
    input.extend_from_slice(VALUE);
    input.extend_from_slice(b"\r\n$2\r\nEX\r\n$3\r\n300\r\n");

    bencher.bench(|| divan::black_box(vash_proto::resp::parse(divan::black_box(&input)).is_ok()));
}

/// **What `conn::measure_resp` actually pays now.** The measuring pass walks a
/// block only to find where each command ends and which tier it belongs to, and
/// everything it measures is parsed again by `resp::execute_block`. It used to
/// run `resp_parse_get` above to answer that; it now runs this, which stops at
/// the framing. The pair is the whole of §17: the gap between them is per
/// command, on every RESP connection, and is spent on a result that is dropped.
///
/// The bar to clear is `vcp_peek_frame_len`, which is the same job in the
/// dialect that has always done it this way.
#[divan::bench(sample_size = 500)]
fn resp_peek_get(bencher: divan::Bencher) {
    let line = b"*2\r\n$3\r\nGET\r\n$17\r\nuser:1234:profile\r\n";
    bencher.bench(|| {
        divan::black_box(
            vash_proto::resp::peek(divan::black_box(line), |args: &[&[u8]]| args.len()).is_ok(),
        )
    });
}

/// [`resp_peek_get`] for the write shape. Closer to its `parse` twin than the
/// read is, and deliberately so: classifying a `SET` still has to scan its
/// option tokens, because that is what decides whether the run can be awaited.
#[divan::bench(sample_size = 500)]
fn resp_peek_set(bencher: divan::Bencher) {
    let mut input = b"*5\r\n$3\r\nSET\r\n$17\r\nuser:1234:profile\r\n$1024\r\n".to_vec();
    input.extend_from_slice(VALUE);
    input.extend_from_slice(b"\r\n$2\r\nEX\r\n$3\r\n300\r\n");

    bencher.bench(|| {
        divan::black_box(
            vash_proto::resp::peek(divan::black_box(&input), |args: &[&[u8]]| args.len()).is_ok(),
        )
    });
}

/// The variadic read, where the parse the measuring pass used to run also
/// **allocated** a `Vec` of the key slices — once to measure and once to
/// execute. `peek` allocates nothing at any batch size.
#[divan::bench(args = [2, 16, 100], sample_size = 500)]
fn resp_measure_mget(bencher: divan::Bencher, keys: usize) {
    let mut input = format!("*{}\r\n$4\r\nMGET\r\n", keys + 1).into_bytes();
    for i in 0..keys {
        let key = format!("user:{i}:profile");
        input.extend_from_slice(format!("${}\r\n{key}\r\n", key.len()).as_bytes());
    }
    bencher.bench(|| {
        divan::black_box(
            vash_proto::resp::peek(divan::black_box(&input), |args: &[&[u8]]| args.len()).is_ok(),
        )
    });
}

/// [`resp_measure_mget`]'s predecessor, kept beside it so the allocation the
/// measuring pass used to make is visible rather than asserted.
#[divan::bench(args = [2, 16, 100], sample_size = 500)]
fn resp_parse_mget(bencher: divan::Bencher, keys: usize) {
    let mut input = format!("*{}\r\n$4\r\nMGET\r\n", keys + 1).into_bytes();
    for i in 0..keys {
        let key = format!("user:{i}:profile");
        input.extend_from_slice(format!("${}\r\n{key}\r\n", key.len()).as_bytes());
    }
    bencher.bench(|| divan::black_box(vash_proto::resp::parse(divan::black_box(&input)).is_ok()));
}

// ---- instrumentation -------------------------------------------------------

/// **Everything a `GET` pays before it touches storage.** One
/// `CommandMetrics::observe` and one `ServerMetrics::read`, which between them
/// used to be seven locked read-modify-writes across three cache lines shared by
/// every core. They are striped per thread now — see §19 — so this measures a
/// handful of plain stores into lines nobody else touches.
///
/// Single-threaded, which is the *worst* case for the change: the old counters
/// were uncontended here and still cost 41.8 ns in the standalone probe. What
/// striping is really for does not show up until several threads count at once,
/// and a divan bench cannot show that; `metrics::striping_tests` and the probe
/// in §19 carry that half.
#[divan::bench(sample_size = 500)]
fn metrics_for_a_read(bencher: divan::Bencher) {
    use vash_server::metrics::{CommandKind, Dialect, ServerMetrics};

    let metrics = ServerMetrics::default();
    let elapsed = std::time::Duration::from_micros(7);
    bencher.bench_local(|| {
        metrics
            .commands
            .observe(CommandKind::Get, Dialect::Resp, divan::black_box(elapsed));
        metrics.read(1, 0);
        divan::black_box(&metrics);
    });
}

/// The two clock reads that bracket every command, plus the one every read
/// transaction takes for its snapshot. Not a change — measured so §19's claim
/// that they are the larger half of a command's instrumentation is checked
/// against this host rather than asserted.
#[divan::bench(sample_size = 500)]
fn metrics_clock_reads(bencher: divan::Bencher) {
    bencher.bench(|| {
        let started = std::time::Instant::now();
        divan::black_box(started.elapsed());
        divan::black_box(std::time::SystemTime::now())
    });
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
    let command = Command::GetMany(vash_core::Key::from_stored(b"user:1234:profile").into());
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
    let command = Command::GetMany(vash_core::Key::from_stored(b"user:1234:profile").into());
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

/// **What a multi-get costs today.** `get_many` copies every hit out of the map
/// into a `Value`, allocates a vector to hold them, and the caller then copies
/// each one again into the reply buffer.
#[divan::bench(args = [2, 16, 100], sample_size = 200)]
fn read_batch_of_many_then_encode(bencher: divan::Bencher, keys: usize) {
    let store = BatchFixture::new(keys);
    let lookups = store.keys();

    let mut out = Vec::with_capacity(keys * (BatchFixture::VALUE_LEN + 64));
    bencher.bench_local(|| {
        out.clear();
        let values = vash_store::Store::get_many(&store.store, divan::black_box(&lookups)).unwrap();
        for value in values.into_iter().flatten() {
            out.extend_from_slice(&value.data);
        }
        divan::black_box(out.len())
    });
}

/// The same batch through `get_many_with`: one reader per shard, keys walked in
/// request order, every value appended straight out of the map. No `Value`, no
/// result vector, no second copy. This is §18.
#[divan::bench(args = [2, 16, 100], sample_size = 200)]
fn read_batch_of_many_in_txn(bencher: divan::Bencher, keys: usize) {
    let store = BatchFixture::new(keys);
    let lookups = store.keys();

    let mut out = Vec::with_capacity(keys * (BatchFixture::VALUE_LEN + 64));
    bencher.bench_local(|| {
        out.clear();
        vash_store::Store::get_many_with(&store.store, divan::black_box(&lookups), &mut |_, v| {
            if let Some(value) = v {
                out.extend_from_slice(value.data);
            }
        })
        .unwrap();
        divan::black_box(out.len())
    });
}

/// **A VCP `GET_MANY` as it was framed.** Three copies of every value — out of
/// the map into a `Value`, out of that into the body, out of the body into the
/// write buffer — and two allocations on top of the one per hit that building a
/// `Value` costs. The frame header carries the body length, which is why the
/// body had to exist before the header could be written.
#[divan::bench(args = [2, 16, 100], sample_size = 200)]
fn vcp_batch_reply_buffered(bencher: divan::Bencher, keys: usize) {
    use vash_proto::vcp::Opcode;

    let store = BatchFixture::new(keys);
    let lookups = store.keys();

    let mut out = Vec::with_capacity(keys * (BatchFixture::VALUE_LEN + 64));
    bencher.bench_local(|| {
        out.clear();
        let values = vash_store::Store::get_many(&store.store, divan::black_box(&lookups)).unwrap();
        vash_proto::vcp::encode_reply(
            &mut out,
            Opcode::GetMany,
            1,
            &vash_core::Reply::Values(values),
        );
        divan::black_box(out.len())
    });
}

/// The same reply streamed: header with a placeholder length, entries appended
/// straight out of the memory map in request order, length patched in at the
/// end. One copy, no allocations. See `ValuesFrame` and §18.
#[divan::bench(args = [2, 16, 100], sample_size = 200)]
fn vcp_batch_reply_streamed(bencher: divan::Bencher, keys: usize) {
    use vash_proto::vcp::{Opcode, ValuesFrame};

    let store = BatchFixture::new(keys);
    let lookups = store.keys();

    let mut out = Vec::with_capacity(keys * (BatchFixture::VALUE_LEN + 64));
    bencher.bench_local(|| {
        out.clear();
        let frame = ValuesFrame::open(&mut out, Opcode::GetMany, 1, lookups.len() as u32);
        vash_store::Store::get_many_with(&store.store, divan::black_box(&lookups), &mut |_, v| {
            match v {
                Some(value) => frame.hit(&mut out, value),
                None => frame.miss(&mut out),
            }
        })
        .unwrap();
        frame.finish(&mut out).unwrap();
        divan::black_box(out.len())
    });
}

/// A populated multi-shard environment, which is the shape the fused path has to
/// earn its keep in: four shards means four readers held for one batch, and the
/// grouping allocations the old path made are proportional to that.
struct BatchFixture {
    store: vash_store::LmdbStore,
    names: Vec<String>,
    _dir: tempfile::TempDir,
}

impl BatchFixture {
    const VALUE_LEN: usize = 1024;

    fn new(keys: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = vash_store::StoreConfig {
            path: dir.path().join("db"),
            map_size: 256 * 1024 * 1024,
            shards: 4,
            ..vash_store::StoreConfig::default()
        };
        let store = vash_store::LmdbStore::open(&config).unwrap();
        let names: Vec<String> = (0..keys).map(|i| format!("user:{i}:profile")).collect();
        let value = vec![b'x'; Self::VALUE_LEN];
        let sets: Vec<vash_core::Set<'_>> = names
            .iter()
            .map(|name| vash_core::Set {
                key: vash_core::Key::new(name.as_bytes()).unwrap(),
                value: &value,
                ttl: vash_core::TtlChange::Set(0),
                return_previous: false,
                mc_flags: 0,
                tags: Vec::new(),
                mode: vash_core::SetMode::Set,
            })
            .collect();
        vash_store::Store::set_many(&store, &sets).unwrap();
        Self {
            store,
            names,
            _dir: dir,
        }
    }

    fn keys(&self) -> Vec<vash_core::Key<'_>> {
        self.names
            .iter()
            .map(|name| vash_core::Key::new(name.as_bytes()).unwrap())
            .collect()
    }
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

// ---- introspection ---------------------------------------------------------
//
// Phase 0 of docs/slowlog-proposal.md. The gates in §7 are stated end to end,
// and phase 0 found that end to end cannot see them: pipelined `GET` on this
// box drifts by more than 15% across ten consecutive runs, which is an order of
// magnitude larger than the 1% being looked for. These price the same code
// directly, where the noise floor is nanoseconds instead of hundreds of
// thousands of ops/s.

#[cfg(feature = "introspect")]
mod introspect {
    use vash_server::config::IntrospectConfig;
    use vash_server::dispatch::Origin;
    use vash_server::introspect::Introspect;
    use vash_server::metrics::{CommandKind, Dialect};

    /// What a `GET` pays for the whole instrument: the sampler's PRNG step and
    /// the slowlog's threshold compare, at the settings that ship.
    ///
    /// **This is the number §7's gate 1 is really about.** Divided into the
    /// per-request CPU budget at a million operations a second — roughly a
    /// microsecond — it is the throughput percentage the feature costs.
    #[divan::bench(sample_size = 1000)]
    fn observed_get(bencher: divan::Bencher) {
        let introspect = Introspect::new(&IntrospectConfig::default());
        let key = vash_core::Key::new(b"user:1234:profile").unwrap();
        let elapsed = std::time::Duration::from_micros(40);

        bencher.bench(|| {
            introspect.observed_get(
                divan::black_box(key),
                CommandKind::Get,
                Origin::internal(Dialect::Vcp),
                divan::black_box(elapsed),
            )
        });
    }

    /// The same call with sampling off, which isolates the slowlog's compare
    /// from the sampler. The difference between this and `observed_get` is what
    /// the hot-key sampler costs, and nothing else.
    #[divan::bench(sample_size = 1000)]
    fn observed_get_sampling_off(bencher: divan::Bencher) {
        let introspect = Introspect::new(&IntrospectConfig {
            hotkeys_enabled: false,
            ..IntrospectConfig::default()
        });
        let key = vash_core::Key::new(b"user:1234:profile").unwrap();
        let elapsed = std::time::Duration::from_micros(40);

        bencher.bench(|| {
            introspect.observed_get(
                divan::black_box(key),
                CommandKind::Get,
                Origin::internal(Dialect::Vcp),
                divan::black_box(elapsed),
            )
        });
    }

    /// The sampler alone, at the rate that ships. Amortised over the 127
    /// requests it declines and the one it admits, which is the number a
    /// request actually pays rather than either half of it.
    #[divan::bench(args = [0, 16, 128, 1024], sample_size = 1000)]
    fn sampler(bencher: divan::Bencher, rate: u64) {
        let introspect = Introspect::new(&IntrospectConfig {
            hotkeys_sample_rate: rate.max(1),
            hotkeys_enabled: rate != 0,
            // Never crosses, so this measures the sampler and not the ring.
            slowlog_threshold_us: u64::MAX,
            ..IntrospectConfig::default()
        });
        let key = vash_core::Key::new(b"user:1234:profile").unwrap();
        let elapsed = std::time::Duration::from_micros(40);

        bencher.bench(|| {
            introspect.observed_get(
                divan::black_box(key),
                CommandKind::Get,
                Origin::internal(Dialect::Vcp),
                divan::black_box(elapsed),
            )
        });
    }
}
