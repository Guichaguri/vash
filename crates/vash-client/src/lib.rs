//! A VCP client.
//!
//! Deliberately simple: one request in flight at a time. It exists to drive
//! integration tests, to be the reference for what the protocol looks like from
//! the outside, and to carry the server's own peer traffic — a cluster peer is
//! just another VCP client, so cluster invalidation goes over the published
//! protocol rather than a private side channel. Pipelining and out-of-order
//! completion â€” which the frame format already supports via `request_id` â€” are a
//! later concern.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
// Re-exported rather than merely imported: every one of these appears in this
// crate's public signatures, so a caller that cannot name them cannot use it
// without depending on `vash-core` directly.
pub use vash_core::{ListEntry, Listing, ServerInfo, Value};
use vash_proto::vcp::{
    FrameLen, HEADER_LEN, Opcode, Status, encode_request, encode_set_body, peek_frame_len,
};
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

    /// The TLS handshake did not complete.
    ///
    /// Separate from [`ClientError::Io`] on purpose, and it is the whole
    /// reason this variant exists: an unknown CA, a certificate that does not
    /// carry the name being dialled, one that has expired — these are
    /// configuration errors that a caller must not report as a peer being
    /// down. The cluster relies on the distinction to log a bad CA as the
    /// mistake it is rather than marking a healthy node unreachable forever.
    #[cfg(feature = "tls")]
    #[error("TLS handshake failed: {0}")]
    Tls(String),
}

#[cfg(feature = "tls")]
impl ClientError {
    fn tls(error: std::io::Error) -> Self {
        Self::Tls(error.to_string())
    }
}

pub type Result<T, E = ClientError> = std::result::Result<T, E>;

/// What the client is talking over.
///
/// An enum rather than a type parameter on [`Client`], which is the opposite
/// of the choice the server made for `conn::handle`. The reason is the API:
/// `Client` is public and the cluster holds an `Option<Client>` per peer, so a
/// type parameter would spread from here into the peer tasks, the cluster's
/// error type and every caller — to save one branch per syscall on a client
/// that does one request at a time. The branch is free beside the syscall; the
/// churn is not.
enum Stream {
    Plain(TcpStream),
    // Boxed because the TLS session is far larger than a socket, and this enum
    // is as big as its widest variant in every `Client` that exists.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Stream {
    async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => {
                s.write_all(bytes).await?;
                // A no-op on a socket, and mandatory on a TLS session: see the
                // note below and `vash_server::conn`.
                s.flush().await
            }
            #[cfg(feature = "tls")]
            Self::Tls(s) => {
                s.write_all(bytes).await?;
                // Not hygiene. `write_all` on a TLS stream means the session
                // accepted the bytes, not that they reached the socket; an
                // unflushed tail sits as ciphertext inside `rustls` while both
                // ends wait on reads. That is the hang recorded in
                // `docs/tls-proposal.md` §8.4.
                s.flush().await
            }
        }
    }

    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read_buf(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(s) => s.read_buf(buf).await,
        }
    }
}

/// How a client verifies the server it dials.
///
/// Built once and shared: parsing a CA bundle and constructing a `rustls`
/// configuration is not per-connection work, and the cluster reconnects to a
/// peer every time one goes away.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct TlsConfig {
    config: std::sync::Arc<rustls::ClientConfig>,
    /// The name the certificate has to carry.
    ///
    /// Separate from the address because they are different things whenever a
    /// peer is named by IP: `10.0.0.5:11312` has no name in it, and a
    /// certificate cannot carry one for it unless it was issued with an IP
    /// SAN. This is the override for that case.
    server_name: rustls::pki_types::ServerName<'static>,
}

#[cfg(feature = "tls")]
impl TlsConfig {
    /// Trusts `ca` — a PEM bundle — and expects the server to present
    /// `server_name`.
    ///
    /// The CA is required rather than optional. The deployments this is for
    /// issue their own: a cache reached over a private network by an internal
    /// name is not getting a WebPKI certificate, and an `Option` here would
    /// mean either bundling a root store this crate has no business carrying,
    /// or a `None` that trusts nothing and fails every handshake.
    pub fn new(ca: &std::path::Path, server_name: &str) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        let file = std::fs::File::open(ca)?;
        let mut reader = std::io::BufReader::new(file);
        let mut added = 0;
        for cert in rustls_pemfile::certs(&mut reader) {
            roots
                .add(cert?)
                .map_err(|_| ClientError::Protocol("the CA bundle holds a bad certificate"))?;
            added += 1;
        }
        if added == 0 {
            return Err(ClientError::Protocol(
                "the CA bundle contains no certificate",
            ));
        }

        let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| ClientError::Protocol("TLS 1.3 is not available in this build"))?
        .with_root_certificates(roots)
        .with_no_client_auth();

        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|_| {
                ClientError::Protocol("the TLS server name is not a valid DNS name or IP")
            })?;

        Ok(Self {
            config: std::sync::Arc::new(config),
            server_name,
        })
    }
}

pub struct Client {
    stream: Stream,
    buf: BytesMut,
    next_id: u32,
    info: ServerInfo,
}

/// What a client presents to a server that requires authentication.
///
/// Only `PLAIN` exists: the secret crosses the wire and the server holds a
/// digest. The challenge–response mechanism is specified in `docs/auth.md`
/// §6.3 and not built, which is why this is a pair of strings rather than a
/// trait.
#[derive(Debug, Clone)]
pub struct Credential {
    pub name: String,
    pub secret: String,
}

impl Credential {
    pub fn new(name: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            secret: secret.into(),
        }
    }
}

impl Client {
    /// Connects and completes the handshake.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        Self::connect_inner(addr, None, None).await
    }

    /// Connects, completes the handshake, and authenticates before returning.
    ///
    /// A refusal comes back as `Status(Unauthorized)`, distinguishable from a
    /// dead server — which is what lets the cluster log a bad peer credential
    /// as the configuration error it is rather than as unreachability.
    pub async fn connect_with(addr: impl ToSocketAddrs, credential: &Credential) -> Result<Self> {
        Self::connect_inner(addr, Some(credential), None).await
    }

    /// Connects over TLS, verifying the server against `tls`.
    ///
    /// The credential is optional and orthogonal: a deployment can use a
    /// certificate to protect the wire and a credential to say who is asking,
    /// which is the pairing docs/auth.md §1 always said mattered. Client
    /// certificates — where the certificate *is* the identity — are §7 of the
    /// proposal and not built.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        addr: impl ToSocketAddrs,
        credential: Option<&Credential>,
        tls: &TlsConfig,
    ) -> Result<Self> {
        Self::connect_inner(addr, credential, Some(tls)).await
    }

    async fn connect_inner(
        addr: impl ToSocketAddrs,
        credential: Option<&Credential>,
        #[cfg(feature = "tls")] tls: Option<&TlsConfig>,
        #[cfg(not(feature = "tls"))] _tls: Option<&()>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        #[cfg(feature = "tls")]
        let stream = match tls {
            None => Stream::Plain(stream),
            Some(tls) => {
                let connector =
                    tokio_rustls::TlsConnector::from(std::sync::Arc::clone(&tls.config));
                // A handshake failure is a configuration error far more often
                // than a dead peer — an unknown CA, a name the certificate
                // does not carry — so it must not come back as `Io` and be
                // read as unreachability. See `ClientError::Tls`.
                let stream = connector
                    .connect(tls.server_name.clone(), stream)
                    .await
                    .map_err(ClientError::tls)?;
                Stream::Tls(Box::new(stream))
            }
        };
        #[cfg(not(feature = "tls"))]
        let stream = Stream::Plain(stream);

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
                max_tags_per_record: 0,
            },
        };

        let mut body = Vec::with_capacity(4);
        body.extend_from_slice(&vash_core::PROTOCOL_VERSION.to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());

        let (status, payload) = client.round_trip(Opcode::Hello, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }

        let info = vash_proto::vcp::encode::decode_hello_response(&payload)
            .ok_or(ClientError::Protocol("truncated hello response"))?;
        if info.protocol_version != vash_core::PROTOCOL_VERSION {
            return Err(ClientError::VersionMismatch {
                server: info.protocol_version,
                client: vash_core::PROTOCOL_VERSION,
            });
        }
        client.info = info;

        // After `HELLO`, because that is the order the protocol requires: the
        // dialect has to be announced before anything else, and the reply's
        // `AUTH_REQUIRED` bit is how a client without a credential learns it
        // needs one.
        if let Some(credential) = credential {
            client.authenticate(credential).await?;
        } else if info.capabilities & vash_core::capability::AUTH_REQUIRED != 0 {
            // Failing here rather than on the first command turns "every
            // request is Unauthorized" into one clear error at the point the
            // configuration is wrong.
            return Err(ClientError::Protocol(
                "server requires authentication; use Client::connect_with",
            ));
        }

        Ok(client)
    }

    /// Sends `AUTH`. Replaces any identity the connection already had.
    pub async fn authenticate(&mut self, credential: &Credential) -> Result<()> {
        let mut body = Vec::with_capacity(4 + credential.name.len() + credential.secret.len());
        vash_proto::vcp::encode_auth_body(
            &mut body,
            0, // PLAIN
            credential.name.as_bytes(),
            credential.secret.as_bytes(),
        );

        match self.round_trip(Opcode::Auth, &body).await? {
            (Status::Ok, _) => Ok(()),
            (status, _) => Err(ClientError::Status(status)),
        }
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
                if body.len() < vash_proto::vcp::encode::VALUE_PREFIX_LEN {
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

    /// Stores a value, returning its CAS token.
    ///
    /// `ttl_secs` of 0 means no expiry. Anything else is an offset in seconds
    /// at any magnitude: unlike memcached's `exptime`, a TTL past 30 days is
    /// still a TTL and not a unix timestamp.
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
        vash_proto::vcp::encode_touch_body(&mut body, key, ttl_secs);

        match self.round_trip(Opcode::Touch, &body).await? {
            (Status::Ok, _) => Ok(true),
            (Status::NotFound, _) => Ok(false),
            (status, _) => Err(ClientError::Status(status)),
        }
    }

    /// Applies an atomic read-modify-write to a counter.
    ///
    /// One round trip and one storage operation: the read and the write happen
    /// inside the shard writer's transaction, so concurrent callers cannot lose
    /// an update. Returns where the counter ended up and how far it moved, or
    /// `None` when the key was absent and `op.missing` does not create one.
    ///
    /// The reply echoes its own numeric mode, so it decodes without the caller
    /// having to remember what it asked for — which matters because VCP
    /// responses may arrive out of order.
    pub async fn arithmetic(
        &mut self,
        op: &vash_core::Arithmetic<'_>,
    ) -> Result<Option<vash_core::Applied>> {
        let mut body = Vec::with_capacity(32 + op.key.len());
        vash_proto::vcp::encode_arithmetic_body(&mut body, op);

        let (status, payload) = self.round_trip(Opcode::Arithmetic, &body).await?;
        match status {
            Status::NotFound => return Ok(None),
            Status::Ok => {}
            other => return Err(ClientError::Status(other)),
        }
        if payload.len() < vash_proto::vcp::ARITHMETIC_RESPONSE_LEN {
            return Err(ClientError::Protocol("arithmetic response is too short"));
        }

        let raw_value = u64::from_le_bytes(payload[4..12].try_into().expect("eight bytes"));
        let raw_applied = u64::from_le_bytes(payload[12..20].try_into().expect("eight bytes"));
        let number = |bits: u64| match payload[0] {
            vash_proto::vcp::arithmetic_mode::COUNTER => Some(vash_core::Number::Counter(bits)),
            vash_proto::vcp::arithmetic_mode::INT => Some(vash_core::Number::Int(bits as i64)),
            vash_proto::vcp::arithmetic_mode::FLOAT => {
                Some(vash_core::Number::Float(f64::from_bits(bits)))
            }
            _ => None,
        };

        Ok(Some(vash_core::Applied {
            value: number(raw_value).ok_or(ClientError::Protocol("unknown arithmetic mode"))?,
            applied: number(raw_applied).ok_or(ClientError::Protocol("unknown arithmetic mode"))?,
            wrote: payload[1] != 0,
        }))
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
    /// just another VCP client. `full` says these entries are the sender's
    /// whole table, which licenses the server to answer with tags the sender
    /// never mentioned; a partial push gets an answer covering only the names
    /// it named. The reply is whatever the server holds a **higher**
    /// generation for, so the caller can max-merge it and converge.
    pub async fn tag_sync(
        &mut self,
        full: bool,
        entries: &[(&[u8], u64)],
    ) -> Result<Vec<vash_core::TagGeneration>> {
        let mut body = Vec::new();
        vash_proto::vcp::encode_tag_sync_body(&mut body, full, entries.iter().copied());

        let (status, payload) = self.round_trip(Opcode::TagSync, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }

        let (_, learned) = vash_proto::vcp::decode_tag_sync(&payload)
            .map_err(|_| ClientError::Protocol("malformed tag sync response"))?;
        Ok(learned
            .into_iter()
            .map(|(name, generation)| vash_core::TagGeneration::new(name, generation))
            .collect())
    }

    /// The server's view of its peer list.
    ///
    /// Membership is static configuration rather than a negotiated set, so this
    /// is what one node was told — comparing it across nodes is how a client
    /// detects drift.
    pub async fn cluster(&mut self) -> Result<vash_core::ClusterInfo> {
        let (status, payload) = self.round_trip(Opcode::Cluster, &[]).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }
        vash_proto::vcp::decode_cluster_response(&payload)
            .ok_or(ClientError::Protocol("malformed cluster response"))
    }

    /// One page of the live keys. Refused unless the server has
    /// `protocol.listing_enabled` set.
    ///
    /// Pass an empty `cursor` for the first page, then the cursor from the
    /// previous page, and stop when the reply carries none. An empty `pattern`
    /// matches everything; `*` and `?` are the only metacharacters.
    ///
    /// Administrative — a linear scan over the keyspace. Not for building an
    /// index on.
    pub async fn list_keys(
        &mut self,
        limit: u32,
        cursor: &[u8],
        pattern: &[u8],
    ) -> Result<vash_core::Listing> {
        self.list(Opcode::ListKeys, limit, cursor, pattern).await
    }

    /// One page of the tag registry, in name order, with the generation this
    /// node holds for each. Gated and paged exactly as [`Client::list_keys`].
    pub async fn list_tags(
        &mut self,
        limit: u32,
        cursor: &[u8],
        pattern: &[u8],
    ) -> Result<vash_core::Listing> {
        self.list(Opcode::ListTags, limit, cursor, pattern).await
    }

    /// Every live key matching `pattern`, paged to exhaustion.
    ///
    /// The paging loop is the one thing a caller can get wrong — echo the
    /// cursor, stop when a page carries none — so it is written once here
    /// rather than in each caller.
    ///
    /// **Collects the whole listing into memory.** That is right for the
    /// administrative uses these commands exist for, and wrong for a keyspace
    /// larger than the process can hold; page with [`Client::list_keys`]
    /// directly if that is a risk.
    pub async fn list_all_keys(&mut self, pattern: &[u8]) -> Result<Vec<ListEntry>> {
        self.list_all(Opcode::ListKeys, pattern).await
    }

    /// Every tag matching `pattern`, paged to exhaustion. See
    /// [`Client::list_all_keys`].
    pub async fn list_all_tags(&mut self, pattern: &[u8]) -> Result<Vec<ListEntry>> {
        self.list_all(Opcode::ListTags, pattern).await
    }

    async fn list_all(&mut self, opcode: Opcode, pattern: &[u8]) -> Result<Vec<ListEntry>> {
        let mut all = Vec::new();
        let mut cursor: Vec<u8> = Vec::new();
        loop {
            // The largest page the server will serve: fewer round trips, and
            // the limit is a transport detail that cannot change the result.
            let page = self
                .list(opcode, vash_core::MAX_LIST_LIMIT, &cursor, pattern)
                .await?;
            all.extend(page.entries);
            match page.cursor {
                Some(next) => cursor = next.into_vec(),
                None => return Ok(all),
            }
        }
    }

    /// Both listings share a body, a reply and therefore this.
    async fn list(
        &mut self,
        opcode: Opcode,
        limit: u32,
        cursor: &[u8],
        pattern: &[u8],
    ) -> Result<vash_core::Listing> {
        let mut body = Vec::new();
        vash_proto::vcp::encode_list_body(&mut body, limit, cursor, pattern);

        let (status, payload) = self.round_trip(opcode, &body).await?;
        if status != Status::Ok {
            return Err(ClientError::Status(status));
        }
        vash_proto::vcp::decode_listing(&payload)
            .ok_or(ClientError::Protocol("malformed listing response"))
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
        vash_proto::vcp::encode_key_list_body(&mut body, keys);

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
        vash_proto::vcp::encode_batch_count(&mut body, items.len());
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
        vash_proto::vcp::encode_key_list_body(&mut body, keys);

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
    use vash_proto::vcp::HEADER_LEN;

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
