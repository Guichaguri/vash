//! A KCP client.
//!
//! Deliberately simple: one request in flight at a time. It exists to drive
//! integration tests, to be the reference for what the protocol looks like from
//! the outside, and to carry the server's own peer traffic — a cluster peer is
//! just another KCP client, so cluster invalidation goes over the published
//! protocol rather than a private side channel. Pipelining and out-of-order
//! completion â€” which the frame format already supports via `request_id` â€” are a
//! later concern.

use bytes::{Bytes, BytesMut};
use cache_core::{ServerInfo, Value};
use cache_proto::kcp::{
    FrameLen, HEADER_LEN, Opcode, Status, encode_request, encode_set_body, peek_frame_len,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use zerocopy_shim::parse_header;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The server answered with a non-OK status.
    #[error("server returned {0:?}")]
    Status(Status),

    #[error("server sent a malformed response: {0}")]
    Protocol(&'static str),

    #[error("server speaks protocol version {server}, client speaks {client}")]
    VersionMismatch { server: u16, client: u16 },
}

pub type Result<T, E = ClientError> = std::result::Result<T, E>;

pub struct Client {
    stream: TcpStream,
    buf: BytesMut,
    next_id: u32,
    info: ServerInfo,
}

impl Client {
    /// Connects and completes the handshake.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let mut client = Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            next_id: 0,
            info: ServerInfo {
                protocol_version: 0,
                shards: 0,
                max_key_len: 0,
                max_value_len: 0,
                capabilities: 0,
            },
        };

        let mut body = Vec::with_capacity(4);
        body.extend_from_slice(&cache_core::PROTOCOL_VERSION.to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());

        let (status, payload) = client.round_trip(Opcode::Hello, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }

        let info = cache_proto::kcp::encode::decode_hello_response(&payload)
            .ok_or(ClientError::Protocol("truncated hello response"))?;
        if info.protocol_version != cache_core::PROTOCOL_VERSION {
            return Err(ClientError::VersionMismatch {
                server: info.protocol_version,
                client: cache_core::PROTOCOL_VERSION,
            });
        }
        client.info = info;
        Ok(client)
    }

    pub fn server_info(&self) -> &ServerInfo {
        &self.info
    }

    pub async fn ping(&mut self) -> Result<()> {
        match self.round_trip(Opcode::Ping, &[]).await? {
            (Status::Ok, _) => Ok(()),
            (status, _) => Err(ClientError::Status(status)),
        }
    }

    /// Returns `None` when the key is absent or no longer live.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        let (status, body) = self.round_trip(Opcode::Get, key).await?;
        match status {
            Status::NotFound => Ok(None),
            Status::Ok => {
                if body.len() < cache_proto::kcp::encode::VALUE_PREFIX_LEN {
                    return Err(ClientError::Protocol("truncated value response"));
                }
                let mc_flags = u32::from_le_bytes(body[0..4].try_into().unwrap());
                let cas = u64::from_le_bytes(body[4..12].try_into().unwrap());
                Ok(Some(Value {
                    data: body.slice(12..),
                    mc_flags,
                    cas,
                    expires_at_ms: None,
                }))
            }
            other => Err(ClientError::Status(other)),
        }
    }

    /// Stores a value, returning its CAS token. `ttl_secs` of 0 means no expiry.
    pub async fn set(&mut self, key: &[u8], value: &[u8], ttl_secs: u32) -> Result<u64> {
        self.set_tagged(key, value, ttl_secs, &[]).await
    }

    pub async fn set_tagged(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl_secs: u32,
        tags: &[&[u8]],
    ) -> Result<u64> {
        let mut body = Vec::with_capacity(16 + key.len() + value.len());
        encode_set_body(&mut body, key, value, ttl_secs, tags);

        match self.round_trip(Opcode::Set, &body).await? {
            (Status::Ok, payload) if payload.len() >= 8 => {
                Ok(u64::from_le_bytes(payload[0..8].try_into().unwrap()))
            }
            (Status::Ok, _) => Err(ClientError::Protocol("truncated set response")),
            (status, _) => Err(ClientError::Status(status)),
        }
    }

    /// Returns whether the key was live before the delete.
    pub async fn delete(&mut self, key: &[u8]) -> Result<bool> {
        match self.round_trip(Opcode::Delete, key).await? {
            (Status::Ok, _) => Ok(true),
            (Status::NotFound, _) => Ok(false),
            (status, _) => Err(ClientError::Status(status)),
        }
    }

    /// Replaces a key's TTL without resending its value. Returns whether the
    /// key was live.
    pub async fn touch(&mut self, key: &[u8], ttl_secs: u32) -> Result<bool> {
        let mut body = Vec::with_capacity(4 + key.len());
        cache_proto::kcp::encode_touch_body(&mut body, key, ttl_secs);

        match self.round_trip(Opcode::Touch, &body).await? {
            (Status::Ok, _) => Ok(true),
            (Status::NotFound, _) => Ok(false),
            (status, _) => Err(ClientError::Status(status)),
        }
    }

    /// Invalidates every record carrying `tag`.
    ///
    /// Constant time on the server regardless of how many keys are affected.
    /// Returns `false` if the tag was never registered, so nothing referenced
    /// it.
    pub async fn delete_by_tag(&mut self, tag: &[u8]) -> Result<bool> {
        match self.round_trip(Opcode::DeleteByTag, tag).await? {
            (Status::Ok, _) => Ok(true),
            (Status::NotFound, _) => Ok(false),
            (status, _) => Err(ClientError::Status(status)),
        }
    }

    /// Exchanges tag generations with the server.
    ///
    /// The peer-to-peer half of the protocol, exposed here because a peer is
    /// just another KCP client. `full` says these entries are the sender's
    /// whole table, which licenses the server to answer with tags the sender
    /// never mentioned; a partial push gets an answer covering only the names
    /// it named. The reply is whatever the server holds a **higher**
    /// generation for, so the caller can max-merge it and converge.
    pub async fn tag_sync(
        &mut self,
        full: bool,
        entries: &[(&[u8], u64)],
    ) -> Result<Vec<cache_core::TagGeneration>> {
        let mut body = Vec::new();
        cache_proto::kcp::encode_tag_sync_body(&mut body, full, entries.iter().copied());

        let (status, payload) = self.round_trip(Opcode::TagSync, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }

        let (_, learned) = cache_proto::kcp::decode_tag_sync(&payload)
            .map_err(|_| ClientError::Protocol("malformed tag sync response"))?;
        Ok(learned
            .into_iter()
            .map(|(name, generation)| cache_core::TagGeneration::new(name, generation))
            .collect())
    }

    /// The server's view of its peer list.
    ///
    /// Membership is static configuration rather than a negotiated set, so this
    /// is what one node was told — comparing it across nodes is how a client
    /// detects drift.
    pub async fn cluster(&mut self) -> Result<cache_core::ClusterInfo> {
        let (status, payload) = self.round_trip(Opcode::Cluster, &[]).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }
        cache_proto::kcp::decode_cluster_response(&payload)
            .ok_or(ClientError::Protocol("malformed cluster response"))
    }

    /// Empties the cache, returning the new flush epoch. Refused unless the
    /// server has `protocol.flush_enabled` set.
    pub async fn flush(&mut self) -> Result<u32> {
        match self.round_trip(Opcode::Flush, &[]).await? {
            (Status::Ok, payload) if payload.len() >= 4 => {
                Ok(u32::from_le_bytes(payload[0..4].try_into().unwrap()))
            }
            (Status::Ok, _) => Err(ClientError::Protocol("truncated flush response")),
            (status, _) => Err(ClientError::Status(status)),
        }
    }

    /// Stores a value with tags attached.
    pub async fn set_with_tags(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl_secs: u32,
        tags: &[&[u8]],
    ) -> Result<u64> {
        self.set_tagged(key, value, ttl_secs, tags).await
    }

    /// Fetches many keys in one round trip, against one consistent snapshot.
    /// The result has one slot per requested key, in order; `None` is a miss.
    pub async fn get_many(&mut self, keys: &[&[u8]]) -> Result<Vec<Option<Value>>> {
        let mut body = Vec::with_capacity(4 + keys.len() * 16);
        cache_proto::kcp::encode_key_list_body(&mut body, keys);

        let (status, payload) = self.round_trip(Opcode::GetMany, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }

        let mut c = Reader::new(payload);
        let count = c.u32()? as usize;
        let mut values = Vec::with_capacity(count.min(keys.len()));
        for _ in 0..count {
            if c.u8()? == 0 {
                values.push(None);
                continue;
            }
            let mc_flags = c.u32()?;
            let cas = c.u64()?;
            let len = c.u32()? as usize;
            values.push(Some(Value {
                data: c.take(len)?,
                mc_flags,
                cas,
                expires_at_ms: None,
            }));
        }
        Ok(values)
    }

    /// Stores many values in one round trip. All of them apply or none do.
    pub async fn set_many(&mut self, items: &[(&[u8], &[u8], u32)]) -> Result<Vec<u64>> {
        let mut body = Vec::new();
        cache_proto::kcp::encode_batch_count(&mut body, items.len());
        for (key, value, ttl_secs) in items {
            encode_set_body(&mut body, key, value, *ttl_secs, &[]);
        }

        let (status, payload) = self.round_trip(Opcode::SetMany, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }

        let mut c = Reader::new(payload);
        let count = c.u32()? as usize;
        (0..count).map(|_| c.u64()).collect()
    }

    /// Deletes many keys in one round trip. Each result is whether that key was
    /// live beforehand.
    pub async fn delete_many(&mut self, keys: &[&[u8]]) -> Result<Vec<bool>> {
        let mut body = Vec::with_capacity(4 + keys.len() * 16);
        cache_proto::kcp::encode_key_list_body(&mut body, keys);

        let (status, payload) = self.round_trip(Opcode::DeleteMany, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }

        let mut c = Reader::new(payload);
        let count = c.u32()? as usize;
        (0..count).map(|_| Ok(c.u8()? != 0)).collect()
    }

    async fn round_trip(&mut self, opcode: Opcode, body: &[u8]) -> Result<(Status, Bytes)> {
        let request_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);

        let mut frame = Vec::with_capacity(HEADER_LEN + body.len());
        encode_request(&mut frame, opcode, request_id, body);
        self.stream.write_all(&frame).await?;

        let response = self.read_frame().await?;
        let (header, payload) = parse_header(&response)?;

        if header.request_id != request_id {
            return Err(ClientError::Protocol(
                "response id did not match the request",
            ));
        }
        let status =
            Status::from_u16(header.status).ok_or(ClientError::Protocol("unknown status code"))?;

        Ok((status, payload))
    }

    async fn read_frame(&mut self) -> Result<Bytes> {
        loop {
            match peek_frame_len(&self.buf) {
                FrameLen::Complete(len) => return Ok(self.buf.split_to(len).freeze()),
                FrameLen::TooLarge => {
                    return Err(ClientError::Protocol(
                        "server frame exceeded the maximum size",
                    ));
                }
                FrameLen::Incomplete { needed } => {
                    self.buf.reserve(needed.saturating_sub(self.buf.len()));
                    if self.stream.read_buf(&mut self.buf).await? == 0 {
                        return Err(ClientError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "server closed the connection mid-frame",
                        )));
                    }
                }
            }
        }
    }
}

/// Bounds-checked forward reader over a response body.
///
/// Every accessor returns a protocol error rather than panicking, so a
/// malformed or truncated response is an ordinary error the caller can handle.
struct Reader {
    buf: Bytes,
    pos: usize,
}

impl Reader {
    fn new(buf: Bytes) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<Bytes> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(ClientError::Protocol("response length overflowed"))?;
        if end > self.buf.len() {
            return Err(ClientError::Protocol("response body was truncated"));
        }
        let slice = self.buf.slice(self.pos..end);
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let raw = self.take(4)?;
        Ok(u32::from_le_bytes(raw[..].try_into().expect("4 bytes")))
    }

    fn u64(&mut self) -> Result<u64> {
        let raw = self.take(8)?;
        Ok(u64::from_le_bytes(raw[..].try_into().expect("8 bytes")))
    }
}

/// Header access kept in one place so the field decoding is not duplicated
/// across every response path.
mod zerocopy_shim {
    use super::{Bytes, ClientError};
    use cache_proto::kcp::HEADER_LEN;

    pub struct Header {
        pub request_id: u32,
        pub status: u16,
    }

    pub fn parse_header(frame: &Bytes) -> Result<(Header, Bytes), ClientError> {
        if frame.len() < HEADER_LEN {
            return Err(ClientError::Protocol(
                "response shorter than a frame header",
            ));
        }
        let header = Header {
            status: u16::from_le_bytes(frame[2..4].try_into().unwrap()),
            request_id: u32::from_le_bytes(frame[4..8].try_into().unwrap()),
        };
        Ok((header, frame.slice(HEADER_LEN..)))
    }
}
