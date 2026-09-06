//! The bounds that hold for **every** connection, whoever it turns out to be.
//!
//! These exist because the ones before them did not. `conn` had two limits on
//! what a stranger could consume — a pre-auth buffer ceiling and a deadline to
//! authenticate — and both were written as `enforcing && !authenticated`, where
//! `enforcing` is `auth.required`. That is off by default, so in the shipped
//! configuration neither armed and there was no ceiling on a connection's
//! memory and no deadline on one that never spoke.
//!
//! Every test here therefore runs with **authentication off**, which is the
//! case that used to be unprotected. `tests/auth.rs` covers the tighter
//! pre-auth ceilings, which still apply and are still stricter.

mod common;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use vash_proto::vcp::{HEADER_LEN, MAX_BODY_LEN, Opcode, encode_request};

/// Opens a connection and completes the VCP handshake.
async fn vcp(addr: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connecting");
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Hello, 1, &body);
    stream.write_all(&frame).await.expect("sending hello");

    // Header first, then exactly the body it declares, so the stream is left on
    // a frame boundary rather than wherever one read happened to stop.
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).await.expect("reading hello");
    let body_len = u32::from_le_bytes(header[8..12].try_into().expect("four bytes")) as usize;
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .await
        .expect("reading the hello body");
    stream
}

/// Reads until the peer hangs up, or gives up waiting.
///
/// `Ok(true)` means the server closed the connection. Written as a timeout
/// rather than a bare read because the failure being tested for is the server
/// *not* closing, which without this would hang the suite instead of failing
/// it.
async fn closed_within(stream: &mut TcpStream, within: Duration) -> bool {
    let mut sink = [0u8; 256];
    matches!(
        tokio::time::timeout(within, stream.read(&mut sink)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

/// V-01. A frame header is a claim, not a delivery.
///
/// The header says 64 MiB and the body never comes. What is being asserted is
/// the allocation, which a test cannot see directly — so it asserts the
/// observable consequence instead: the connection survives, because a clamped
/// reserve is not an error, and the buffer it was given did not grow to hold a
/// frame nobody sent. `capacity` is checked in the unit test in `conn`; here it
/// is enough that twelve bytes do not buy a disconnect *or* a gigabyte.
#[tokio::test]
async fn a_declared_frame_length_is_not_an_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let mut running = common::start(common::config(dir.path())).await;

    let mut streams = Vec::new();
    for _ in 0..16 {
        let mut stream = vcp(running.addr).await;
        // The largest body the protocol allows, announced and never sent.
        let mut header = Vec::new();
        header.extend_from_slice(&[Opcode::Set as u8, 0, 0, 0]);
        header.extend_from_slice(&2u32.to_le_bytes());
        header.extend_from_slice(&MAX_BODY_LEN.to_le_bytes());
        assert_eq!(header.len(), HEADER_LEN);
        stream.write_all(&header).await.expect("sending the header");
        streams.push(stream);
    }

    // The connections are still live: a clamped reserve changes what is
    // allocated, not what is accepted. A frame this large is legal and the
    // server is still willing to receive it — one byte at a time if need be.
    for stream in &mut streams {
        assert!(
            !closed_within(stream, Duration::from_millis(200)).await,
            "an incomplete frame is not an error; the server must keep waiting for its body"
        );
    }

    // And the server is still serving everyone else.
    let mut other = vcp(running.addr).await;
    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Ping, 7, &[]);
    other.write_all(&frame).await.expect("pinging");
    let mut reply = [0u8; HEADER_LEN];
    other
        .read_exact(&mut reply)
        .await
        .expect("reading the pong");
    assert_eq!(reply[0], Opcode::Ping as u8);

    drop(streams);
    drop(other);
    running.stop().await;
}

/// V-02, and the case that motivated it.
///
/// One RESP command may declare 8200 arguments of 64 MiB each, and every byte
/// is buffered before the parser is allowed an opinion — so before the ceiling
/// this connection could grow without bound. Now it is closed at
/// `server.max_buffered_bytes`, with no credential anywhere in the story.
#[tokio::test]
async fn an_unfinished_resp_command_is_closed_at_the_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = common::config(dir.path());
    // Small enough to reach in a test, and still above `max_value_bytes` plus
    // its framing, which validation requires.
    config.store.max_value_bytes = 64 * 1024;
    config.server.max_buffered_bytes = 1024 * 1024;
    let mut running = common::start(config).await;

    let mut stream = TcpStream::connect(running.addr).await.expect("connecting");
    // A multibulk header promising far more than will ever arrive, then bulk
    // arguments that are individually legal and collectively past the ceiling.
    stream.write_all(b"*8200\r\n").await.expect("sending");

    let chunk = {
        let mut chunk = Vec::new();
        chunk.extend_from_slice(b"$65536\r\n");
        chunk.extend_from_slice(&vec![b'A'; 65536]);
        chunk.extend_from_slice(b"\r\n");
        chunk
    };

    // 32 of these is 2 MiB against a 1 MiB ceiling. The write itself may fail
    // once the server closes, which is the outcome being tested for.
    let mut closed = false;
    for _ in 0..32 {
        if stream.write_all(&chunk).await.is_err() {
            closed = true;
            break;
        }
    }

    assert!(
        closed || closed_within(&mut stream, Duration::from_secs(2)).await,
        "a connection past server.max_buffered_bytes must be closed, with or without \
         authentication configured"
    );

    running.stop().await;
}

/// The other half of V-02: the ceiling must not fire on legitimate traffic.
///
/// A value at `store.max_value_bytes` is exactly what the configuration says
/// this server accepts, so a client writing one must not be disconnected for
/// it. This is the case `Config::validate` refuses to let an operator get
/// wrong, checked end to end.
#[tokio::test]
async fn a_value_at_the_configured_maximum_still_stores() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = common::config(dir.path());
    config.store.max_value_bytes = 512 * 1024;
    config.server.max_buffered_bytes = 2 * 1024 * 1024;
    let mut running = common::start(config).await;

    let value = vec![b'v'; 512 * 1024];
    let mut stream = vcp(running.addr).await;
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes()); // ttl
    body.extend_from_slice(&(b"big".len() as u16).to_le_bytes());
    body.push(0); // tag_count
    body.push(0); // reserved
    body.extend_from_slice(&(value.len() as u32).to_le_bytes());
    body.extend_from_slice(b"big");
    body.extend_from_slice(&value);
    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Set, 2, &body);
    stream.write_all(&frame).await.expect("sending the set");

    let mut reply = [0u8; HEADER_LEN];
    stream
        .read_exact(&mut reply)
        .await
        .expect("a write at the configured maximum must be answered, not disconnected");
    let status = u16::from_le_bytes([reply[2], reply[3]]);
    assert_eq!(status, 0, "the write should have stored");

    running.stop().await;
}

/// V-05. A socket that never speaks does not hold a connection slot for ever.
#[tokio::test]
async fn a_connection_that_never_sends_a_byte_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = common::config(dir.path());
    config.server.first_byte_timeout_ms = 300;
    let mut running = common::start(config).await;

    let mut stream = TcpStream::connect(running.addr).await.expect("connecting");
    assert!(
        closed_within(&mut stream, Duration::from_secs(5)).await,
        "a connection that has sent nothing must be closed even with auth.required off"
    );

    running.stop().await;
}

/// And the promise it must not break.
///
/// `docs/protocol.md` says an established connection is never idle-reaped, and
/// pooled clients depend on it. The deadline covers connections that have said
/// *nothing*; one byte is enough to leave that population for good.
#[tokio::test]
async fn a_connection_that_has_spoken_is_never_reaped() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = common::config(dir.path());
    config.server.first_byte_timeout_ms = 200;
    let mut running = common::start(config).await;

    let mut stream = vcp(running.addr).await;
    // Well past the deadline, doing nothing.
    tokio::time::sleep(Duration::from_millis(700)).await;

    let mut frame = Vec::new();
    encode_request(&mut frame, Opcode::Ping, 9, &[]);
    stream
        .write_all(&frame)
        .await
        .expect("the connection must still be open");
    let mut reply = [0u8; HEADER_LEN];
    stream
        .read_exact(&mut reply)
        .await
        .expect("and must still be served");
    assert_eq!(reply[0], Opcode::Ping as u8);

    running.stop().await;
}

/// Zero disables the deadline, for a pool that opens connections eagerly.
#[tokio::test]
async fn a_zero_first_byte_timeout_disables_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = common::config(dir.path());
    config.server.first_byte_timeout_ms = 0;
    let mut running = common::start(config).await;

    let mut stream = TcpStream::connect(running.addr).await.expect("connecting");
    assert!(
        !closed_within(&mut stream, Duration::from_millis(500)).await,
        "first_byte_timeout_ms = 0 must leave a silent connection alone"
    );

    running.stop().await;
}
