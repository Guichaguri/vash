//! The memcached dialects: the classic text grammar and the meta one.
//!
//! Split out of `dispatch`, which is the boundary all three dialects meet at
//! and should hold nothing that is true of only one of them. This is the
//! memcached half of what used to live there; [`crate::vcp`] is the other, and
//! [`crate::resp`] was already its own module.
//!
//! Everything that touches storage goes through [`crate::dispatch::execute`],
//! so the storage layer never learns which wire format a request arrived on.
//! What is here is the parts that cannot be shared: where a command ends, what
//! its reply looks like, and the three things this dialect does that no other
//! does — tunnelling credentials through a `set`, paging a keyspace dump with
//! no cursor in the grammar, and rendering `stats` sections.

use tracing::{error, warn};
use vash_core::{Command, Reply};
use vash_proto::vcp::Status;

use crate::auth::ConnAuth;
use crate::dispatch::{
    Closing, Failed, Origin, SetBatch, awaitable, batchable, deny, execute, execute_get_into,
    listing_gate,
};
use crate::metrics::CommandKind;
use crate::state::ServerState;

/// Decodes a memcached block that is nothing but plain `set`s.
///
/// `None` the moment anything else appears, so the caller falls back to the
/// ordinary executor rather than this path having to grow a second opinion on
/// what a command means. The block has already been measured as all-writes, so
/// `None` here is a disagreement between the two and the safe answer is the
/// slow one.
pub(crate) fn parse_memcached_writes(block: &[u8]) -> Option<crate::conn::WriteRun<'_>> {
    use vash_proto::memcached::{Outcome, parse};

    let mut run = crate::conn::WriteRun {
        sets: Vec::new(),
        suppress: Vec::new(),
    };
    let mut rest = block;
    while !rest.is_empty() {
        let Ok(Outcome::Command(parsed)) = parse(rest) else {
            return None;
        };
        if !matches!(
            parsed.style,
            vash_proto::memcached::encode::ResponseStyle::Storage
        ) {
            return None;
        }
        if !awaitable(&parsed.command) {
            return None;
        }
        run.sets.push(batchable(&parsed.command)?.clone());
        run.suppress.push(parsed.noreply);
        rest = &rest[parsed.consumed..];
    }
    Some(run)
}

/// Executes every complete memcached command in `block`, appending the replies.
///
/// The block was already measured by the caller to end on a command boundary,
/// so parsing here cannot run short; re-parsing rather than passing the parsed
/// form across is what keeps the borrowed key and value slices from crossing a
/// task boundary.
pub fn execute_memcached_block(
    state: &ServerState,
    conn: &mut ConnAuth,
    // Who is asking, built once per connection: the dialect the counters need,
    // and the connection a slowlog entry names.
    origin: Origin,
    block: &[u8],
    tls: bool,
    out: &mut Vec<u8>,
) -> Closing {
    use vash_proto::memcached::{Outcome, ProtocolError, parse};

    // Consecutive plain `set`s are held here and submitted together — see
    // [`SetBatch`]. Everything else in this loop flushes it first, so a command
    // can never be answered from a state the writes before it have not reached.
    let mut batch: SetBatch<'_, bool> = SetBatch::default();

    let mut rest = block;
    while !rest.is_empty() {
        match parse(rest) {
            Ok(Outcome::Command(parsed)) => {
                let consumed = parsed.consumed;

                // A plain `set` from the classic grammar, which is the command
                // every memcached client sends to write. `add`/`replace`/`cas`
                // answer with a verdict and `ms` renders through the meta
                // encoder, so both fall through to the ordinary path below.
                if matches!(
                    parsed.style,
                    vash_proto::memcached::encode::ResponseStyle::Storage
                ) && let Some(set) = batchable(&parsed.command)
                    && !(conn_gated(state, conn, &parsed.command))
                {
                    if batch.holds(set.key) {
                        flush_memcached(state, origin, &mut batch, out);
                    }
                    batch.push(set.clone(), parsed.noreply);
                    rest = &rest[consumed..];
                    continue;
                }

                flush_memcached(state, origin, &mut batch, out);
                if execute_memcached(state, conn, origin, &parsed, tls, out) == Closing::Yes {
                    // `quit`: nothing after it on this connection matters.
                    return Closing::Yes;
                }
                rest = &rest[consumed..];
            }
            Err(ProtocolError::Recoverable { response, consumed }) => {
                flush_memcached(state, origin, &mut batch, out);
                // An unparseable command from an unauthenticated connection is
                // still an unauthenticated command, and upstream answers it as
                // one — an unknown verb, a bad command line and outright
                // garbage all get `CLIENT_ERROR unauthenticated` rather than
                // the parse error, so a stranger cannot probe which commands
                // the server understands. Verified against memcached 1.6.45.
                //
                // The same reasoning put the VCP gate ahead of opcode
                // recognition; this is that rule for the text dialect. The byte
                // count is the parser's either way, so a refused storage
                // command still steps over its data block.
                if !conn.is_authenticated() && state.auth.required() {
                    state.metrics.auth_refused();
                    vash_proto::memcached::encode::encode_error(
                        out,
                        vash_proto::memcached::ErrorKind::Client("unauthenticated"),
                    );
                } else {
                    vash_proto::memcached::encode::encode_error(out, response);
                }
                rest = &rest[consumed..];
            }
            // Unreachable: the caller only includes whole commands. Stopping
            // rather than looping keeps a logic slip from spinning a core.
            Ok(Outcome::Incomplete) | Err(ProtocolError::Fatal(_)) => {
                error!("a memcached block did not end on a command boundary");
                break;
            }
        }
    }
    // The flush every other caller in this loop is protected from forgetting,
    // because forgetting it here would drop the writes silently.
    flush_memcached(state, origin, &mut batch, out);
    Closing::No
}

/// Whether this connection may not simply execute a `set` — because it has yet
/// to authenticate, or because its identity has no `write` role.
///
/// A gated connection's `set` is refused rather than executed, and the refusal
/// belongs to [`execute_memcached_unauthenticated`] or to the role gate in
/// [`execute_memcached`], so such a command must never reach the batch. This is
/// the memcached half of the rule that the gate has to sit *ahead* of the
/// batching, not inside `execute`: a batched write never passes through
/// `execute` at all.
///
/// The decision itself is [`crate::auth::AuthState::allows`]; what is local
/// here is only which roles a memcached command needs.
fn conn_gated(state: &ServerState, conn: &ConnAuth, command: &Command<'_>) -> bool {
    !state
        .auth
        .allows(conn, crate::auth::roles_for_command(command))
}

/// Submits a held run of `set`s and writes `STORED` — or the failure — for each.
fn flush_memcached(
    state: &ServerState,
    origin: Origin,
    batch: &mut SetBatch<'_, bool>,
    out: &mut Vec<u8>,
) {
    batch.flush(state, origin, out, |out, noreply, result| {
        // `noreply` suppresses the response but never the work, exactly as it
        // does on the unbatched path.
        if *noreply {
            return;
        }
        match result {
            Ok(_) => out.extend_from_slice(b"STORED\r\n"),
            Err(failed) => {
                vash_proto::memcached::encode::encode_error(out, memcached_error(failed.status));
            }
        }
    });
}

/// Executes one memcached command, rendering the reply in that dialect.
///
/// Shares [`execute`] with the VCP path: the storage layer never learns which
/// wire format a request arrived on.
pub fn execute_memcached(
    state: &ServerState,
    conn: &mut ConnAuth,
    // Who is asking, built once per connection: the dialect the counters need,
    // and the connection a slowlog entry names.
    origin: Origin,
    parsed: &vash_proto::memcached::Parsed<'_>,
    // `tls`: whether *this* connection is encrypted. `stats settings` reports
    // it as `ssl_enabled`, per connection rather than per server, because the
    // two ports serve one store: a client on the plaintext one must not be
    // told the cache is encrypted because some other port is.
    tls: bool,
    out: &mut Vec<u8>,
) -> Closing {
    use vash_proto::memcached::encode as mc;

    if !conn.is_authenticated() && state.auth.required() {
        return execute_memcached_unauthenticated(state, conn, parsed, out);
    }

    // The role gate, in the same place and for the same reason: ahead of the
    // batching above and of every execution path below, because two of those
    // paths do not go through `execute`.
    //
    // `stats cachedump` and `lru_crawler metadump` are the exception the
    // `Command` mapping cannot cover — they are response *styles* that reach
    // the listing without a domain command — so they name `LIST` here, beside
    // the `listing_gate` that already refuses them when enumeration is off.
    let needed = match &parsed.style {
        vash_proto::memcached::encode::ResponseStyle::Dump(_)
        | vash_proto::memcached::encode::ResponseStyle::CacheDump(_) => crate::auth::Roles::LIST,
        _ => crate::auth::roles_for_command(&parsed.command),
    };
    if let Some(crate::auth::Refused::Missing(missing)) = state.auth.refusal(conn, needed) {
        deny(
            state,
            conn,
            missing,
            CommandKind::of(&parsed.command).name(),
        );
        return refuse_memcached(state, parsed, out, "unauthorized");
    }

    // A dump answers with as many lines as the keyspace has, which is more than
    // one `Reply` can carry — so it pages the listing itself and writes as it
    // goes. See [`execute_dump`].
    if let vash_proto::memcached::encode::ResponseStyle::Dump(dump) = &parsed.style {
        if let Err(failed) = execute_dump(state, origin, *dump, out) {
            mc::encode_error(out, memcached_error(failed.status));
        }
        return Closing::No;
    }

    // `stats cachedump` is one page and writes `ITEM` lines, so it does not
    // share the paging loop above — but it is the same listing underneath.
    if let vash_proto::memcached::encode::ResponseStyle::CacheDump(dump) = &parsed.style {
        if let Err(failed) = execute_cachedump(state, origin, *dump, out) {
            mc::encode_error(out, memcached_error(failed.status));
        }
        return Closing::No;
    }

    // Every `stats` section except the general one describes the *server* — its
    // configuration, its connections, the shape of its storage — rather than
    // the cache. There is no storage command for those to be, so they are
    // rendered from `crate::stats` directly instead of travelling as
    // `Command::Stats`. They are still counted, at the same point everything
    // else is.
    if let vash_proto::memcached::encode::ResponseStyle::Stats(section) = &parsed.style
        && *section != vash_proto::memcached::encode::StatsSection::General
    {
        state.metrics.other();
        mc::stat_lines(out, &crate::stats::section(state, *section, tls));
        out.extend_from_slice(b"END\r\n");
        return Closing::No;
    }

    // The one thing that distinguishes the two memcached dialects at execution
    // time, and the only place that knows: the parser records which grammar a
    // command came from, and the storage layer never learns there are two.
    if matches!(
        parsed.style,
        vash_proto::memcached::encode::ResponseStyle::Meta(_)
    ) {
        state
            .metrics
            .outcomes
            .meta_commands
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // A plain `get` or `gets`, naming any number of keys. A single-key read is
    // what every memcached client library sends — `pymemcache`'s `get`,
    // `libmemcached`'s `memcached_get` — so it is the most executed command this
    // server has, and a multi-get is what every library sends the moment an
    // application asks for two things at once.
    //
    // **Both fuse now.** This used to be one key only, on the grounds that a
    // multi-key `get` groups its keys by shard before reading them, so the
    // values come back out of request order — and putting them back in order was
    // what the `Vec` this avoids was for. That was true of the loop, not of the
    // problem: `Store::get_many_with` walks the keys in request order and routes
    // each to its shard's already-open reader, so the order holds by
    // construction and nothing is collected. See §18.
    //
    // `GetAndTouch` shares this response style and must not come through here —
    // it writes. Matching the command rather than the style is what excludes
    // it, exactly as it is for `mg` below.
    if let vash_proto::memcached::encode::ResponseStyle::Retrieval { with_cas } = parsed.style
        && let Command::GetMany(keys) = &parsed.command
        && !parsed.noreply
    {
        let keys = keys.as_slice();
        let rendered = crate::dispatch::execute_get_many_into(
            state,
            keys,
            CommandKind::GetMany,
            origin,
            // A miss writes no `VALUE` line at all — memcached omits misses
            // rather than reporting them — which is why this dialect needs the
            // index only to name the key it is answering.
            &mut |index, value| {
                if let Some(value) = value {
                    mc::encode_value_line(out, with_cas, keys[index].as_bytes(), value);
                }
            },
        );
        match rendered {
            Ok(()) => out.extend_from_slice(mc::RETRIEVAL_END),
            Err(failed) => mc::encode_error(out, memcached_error(failed.status)),
        }
        return Closing::No;
    }

    // The fused read path for the meta dialect, which is `mg` alone. A `T` flag
    // turns `mg` into a get-and-touch, which writes — the parser renders that
    // as `GetAndTouch`, so matching the command rather than the style is what
    // keeps this off the write path.
    if let vash_proto::memcached::encode::ResponseStyle::Meta(
        vash_proto::memcached::encode::MetaStyle::Get(flags),
    ) = &parsed.style
        && let Command::Get { key } = parsed.command
        && !parsed.noreply
    {
        let rendered = execute_get_into(state, key, CommandKind::Get, origin, &mut |value| {
            mc::encode_meta_get_hit(out, flags, &parsed.command, value)
        });
        match rendered {
            Ok(true) => {}
            Ok(false) => out.extend_from_slice(b"EN\r\n"),
            Err(failed) => mc::encode_error(out, memcached_error(failed.status)),
        }
        return Closing::No;
    }

    let result = execute(state, &parsed.command, origin);
    let closing = matches!(result, Ok(Reply::Closing));

    match result {
        Ok(reply) => {
            // `noreply` suppresses the response but never the work.
            if !parsed.noreply {
                mc::encode(out, &parsed.style, &parsed.command, &reply);
            }
        }
        Err(failed) => {
            if !parsed.noreply {
                mc::encode_error(out, memcached_error(failed.status));
            }
        }
    }

    if closing { Closing::Yes } else { Closing::No }
}

/// Handles a memcached command on a connection that has not authenticated.
///
/// The only thing accepted is upstream's ASCII authentication, which tunnels
/// credentials through a `set` whose key is the username and whose data block
/// is `<user> <pass>`:
///
/// ```text
/// set billing-api 0 0 34\r\n
/// billing-api s3cr3t-token-goes-here-here\r\n
/// → STORED
/// ```
///
/// It is an ugly mechanism — the text protocol had no room for a verb old
/// clients would tolerate — but it is the only one memcached clients implement,
/// and compatibility is the entire reason this dialect exists here. Inventing
/// our own verb would be a command nobody sends.
///
/// **The parser is not involved**, and that is the point: it sees an ordinary
/// storage command and consumes the length-delimited block exactly as it always
/// does. Deciding here rather than there is what keeps a refused command from
/// leaving its data block in the stream to be read as commands, and it means
/// the fuzz targets do not grow a mode.
fn execute_memcached_unauthenticated(
    state: &ServerState,
    conn: &mut ConnAuth,
    parsed: &vash_proto::memcached::Parsed<'_>,
    out: &mut Vec<u8>,
) -> Closing {
    use vash_core::{Command, SetMode, Stored};
    use vash_proto::memcached::encode as mc;

    // `quit` stays legal: hanging up is not a use of the cache, and refusing it
    // would leave a client with no way to close politely.
    if matches!(parsed.command, Command::Quit) {
        return Closing::Yes;
    }

    // Anything that is not a plain `set` is simply not the auth command. All
    // three of these strings are upstream's, verified against memcached 1.6.45
    // by `tests/compat/differential.py --dialect memcached --auth`.
    let Command::Set(set) = &parsed.command else {
        return refuse_memcached(state, parsed, out, "unauthenticated");
    };
    if set.mode != SetMode::Set {
        return refuse_memcached(state, parsed, out, "unauthenticated");
    }

    // A `set` that reaches here is an authentication attempt, so a block that
    // is not `<user> <pass>` is a malformed attempt rather than an
    // unauthenticated command — upstream distinguishes the two and so does
    // this.
    let Some((name, secret)) = split_credential(set.value) else {
        conn.fail();
        state.metrics.auth_failed();
        return refuse_memcached(state, parsed, out, "bad authentication token format");
    };

    match state.auth.current().verify(name, secret) {
        Some(identity) => {
            conn.succeed(identity);
            state.metrics.auth_ok();
            // `STORED`, which is what upstream answers a successful
            // authentication. The CAS token is meaningless — nothing was
            // stored — and no client reads it from this reply.
            if !parsed.noreply {
                mc::encode(
                    out,
                    &parsed.style,
                    &parsed.command,
                    &Reply::Stored(Stored::Stored(0)),
                );
            }
            Closing::No
        }
        None => {
            conn.fail();
            warn!(
                name = ?crate::auth::log_name(name),
                failures = conn.failures(),
                "authentication failed"
            );
            state.metrics.auth_failed();
            refuse_memcached(state, parsed, out, "authentication failure")
        }
    }
}

/// Answers an `lru_crawler` dump, paging the listing until it runs out.
///
/// **The grammar has no cursor**, so resumption happens here: each iteration is
/// an ordinary `LIST_KEYS` that opens and closes its own read transaction, so
/// nothing pins a snapshot for the length of the dump — the footgun plan §9
/// exists to avoid, and the reason `LIST_KEYS` pages at all. Going through
/// [`execute`] each time is what keeps the gate, the counters and the error
/// classification on the shared boundary; only the line writing is this
/// dialect's.
///
/// A dump therefore counts as one listing per page rather than one in total.
/// That is honest — it is that many scans — and worth knowing when reading
/// `vash_command_total{command="listing"}`.
fn execute_dump(
    state: &ServerState,
    // Who is asking, built once per connection: the dialect the counters need,
    // and the connection a slowlog entry names.
    origin: Origin,
    dump: vash_proto::memcached::encode::Dump,
    out: &mut Vec<u8>,
) -> Result<(), Failed> {
    use vash_proto::memcached::encode as mc;

    // Checked even when the class selects nothing: "this command is disabled"
    // is true regardless of which class was named, and answering `END` would
    // say the class was merely empty.
    listing_gate(state)?;

    // Upstream acknowledges before it streams anything, and does so even for a
    // class holding nothing. A client that mistook it for the first line would
    // lose a key.
    out.extend_from_slice(mc::DumpStyle::ACK);

    if !dump.selected {
        out.extend_from_slice(dump.style.terminator());
        return Ok(());
    }

    // One dump gets one `LIST_KEYS` call's budget, spent across as many pages as
    // it needs. Each page is allowed its own full budget by `execute`, so the
    // true ceiling is one page's overshoot past this — bounded, which is the
    // point, rather than exact.
    let budget = state.protocol.listing_max_scan as u64;
    let mut scanned = 0u64;
    let mut cursor: Option<Box<[u8]>> = None;

    loop {
        let request = vash_core::ListRequest {
            limit: vash_core::MAX_LIST_LIMIT,
            cursor: cursor.as_deref().unwrap_or(&[]),
            pattern: &[],
        };
        let reply = execute(state, &Command::ListKeys(request), origin)?;
        let Reply::Listing(page) = reply else {
            error!("a listing produced a reply it cannot render");
            return Err(Failed::status(Status::Internal));
        };

        for entry in &page.entries {
            mc::dump_line(out, dump.style, entry);
        }
        scanned += page.scanned;

        match page.cursor {
            // More to walk, and budget left to walk it with.
            Some(next) if scanned < budget => cursor = Some(next),

            // Cut short. **The error replaces the terminator rather than
            // following it**, and that substitution is the whole point: a tool
            // reads lines until the terminator, so ending a truncated dump with
            // one would report a keyspace smaller than the real one. An error
            // where the terminator belongs cannot be mistaken for a complete
            // answer.
            Some(_) => {
                warn!(scanned, budget, "dump exceeded its scan budget");
                mc::encode_error(
                    out,
                    vash_proto::memcached::ErrorKind::Server(
                        "dump exceeded the scan budget; use SCAN or LIST_KEYS to page the keyspace",
                    ),
                );
                return Ok(());
            }

            // Walked to the end of the keyspace.
            None => {
                out.extend_from_slice(dump.style.terminator());
                return Ok(());
            }
        }
    }
}

/// Answers `stats cachedump <class> <limit>`.
///
/// One page, no acknowledgement and no cursor — the whole command is a single
/// `LIST_KEYS`, which is why this does not share [`execute_dump`]'s loop. Gated
/// with everything else that enumerates the keyspace; upstream gates its own
/// behind `dump_enabled`, which is the same setting under another name.
fn execute_cachedump(
    state: &ServerState,
    // Who is asking, built once per connection: the dialect the counters need,
    // and the connection a slowlog entry names.
    origin: Origin,
    dump: vash_proto::memcached::encode::CacheDump,
    out: &mut Vec<u8>,
) -> Result<(), Failed> {
    use vash_proto::memcached::encode as mc;

    // Checked before the class and the limit, because "this command is
    // disabled" is true regardless of either — answering `END` would say the
    // class was merely empty.
    listing_gate(state)?;

    // A class this server does not have holds nothing, and needs no scan to say
    // so. The limit is already resolved to a page size by the parser, so there
    // is no zero to special-case here.
    if !dump.selected {
        out.extend_from_slice(b"END\r\n");
        return Ok(());
    }

    let request = vash_core::ListRequest {
        limit: dump.limit,
        cursor: &[],
        pattern: &[],
    };
    let reply = execute(state, &Command::ListKeys(request), origin)?;
    let Reply::Listing(page) = reply else {
        error!("a listing produced a reply it cannot render");
        return Err(Failed::status(Status::Internal));
    };

    for entry in &page.entries {
        mc::cachedump_line(out, entry);
    }
    out.extend_from_slice(b"END\r\n");
    Ok(())
}

fn refuse_memcached(
    state: &ServerState,
    parsed: &vash_proto::memcached::Parsed<'_>,
    out: &mut Vec<u8>,
    reason: &'static str,
) -> Closing {
    if reason == "unauthenticated" {
        state.metrics.auth_refused();
    }
    if !parsed.noreply {
        vash_proto::memcached::encode::encode_error(
            out,
            vash_proto::memcached::ErrorKind::Client(reason),
        );
    }
    Closing::No
}

/// Splits an ASCII-auth data block into `(user, password)`.
///
/// Exactly one space separates them; a password may not contain one, which is
/// upstream's limitation and not worth diverging over.
///
/// The command's key also names the identity, and upstream repeats it in the
/// block — but only the block is read here. Both orderings are observably the
/// same: a block naming a different user either fails to verify, or verifies as
/// the user it actually named, and upstream answers `authentication failure`
/// for a mismatch either way.
fn split_credential(block: &[u8]) -> Option<(&[u8], &[u8])> {
    let space = block.iter().position(|b| *b == b' ')?;
    let (name, rest) = block.split_at(space);
    let secret = &rest[1..];
    if name.is_empty() || secret.is_empty() || secret.contains(&b' ') {
        return None;
    }
    Some((name, secret))
}

/// Maps an internal status onto the memcached error vocabulary.
fn memcached_error(status: Status) -> vash_proto::memcached::ErrorKind {
    use vash_proto::memcached::ErrorKind;

    // The wording is memcached's, verbatim: the differential suite compares
    // response bytes against a real server.
    match status {
        Status::TooLarge => ErrorKind::Server("object too large for cache"),
        Status::BadRequest => ErrorKind::Client("bad command line format"),
        Status::NotNumeric => ErrorKind::Client("cannot increment or decrement non-numeric value"),
        Status::Unsupported => ErrorKind::Error,
        Status::Unauthorized => ErrorKind::Client("command disabled by configuration"),
        Status::CapacityFull => ErrorKind::Server("out of memory storing object"),
        Status::Overloaded => ErrorKind::Server("server is overloaded"),
        Status::NotStored => ErrorKind::Server("not stored"),
        _ => ErrorKind::Server("internal error"),
    }
}
