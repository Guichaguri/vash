//! A KCP client.
//!
//! Deliberately simple: one request in flight at a time. It exists to drive
//! integration tests and to be the reference for what the protocol looks like
//! from the outside. Pipelining and out-of-order completion — which the frame
//! format already supports via `request_id` — are a later concern, once there
//! is a sharded server able to benefit from them.

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
