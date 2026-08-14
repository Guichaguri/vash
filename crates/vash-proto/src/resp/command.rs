//! The command subset this server answers, and the parser that produces it.
//!
//! Only the string and expiry commands a cache actually needs are here.
//! Everything else — lists, hashes, sets, transactions, scripting, pub/sub —
//! is answered with `unknown command`, which is what a client library probes
//! for anyway. The two connection commands that are not data commands at all,
//! `HELLO` and `PING`, are accepted because `HELLO` is the only way a client
//! can negotiate RESP3 and `PING` is what every connection pool sends on
//! checkout.

use super::{ErrorReply, Parsed, ProtocolError};

/// Longest verb this server recognises, rounded up. Used to upper-case a
/// command name into a fixed buffer so the dispatch stays one `match` and
/// allocates nothing.
const MAX_VERB_LEN: usize = 16;

#[derive(Debug)]
pub enum Command<'a> {
    // ---- connection ------------------------------------------------------
    /// `HELLO [protover [AUTH user pass]]`. `version: None` asks for the server
    /// description without changing the negotiated version.
    Hello {
        version: Option<i64>,
        /// Authenticate and negotiate in one round trip, which is what a client
        /// library does on checkout when it has a credential.
        auth: Option<Credential<'a>>,
    },
    Ping {
        message: Option<&'a [u8]>,
    },
    Quit,
    /// `AUTH password` or `AUTH username password`.
    ///
    /// The one-argument form is Redis's pre-6 shape and still the most common;
    /// it means the `default` identity, which is why the name is optional here
    /// rather than being two commands.
    Auth(Credential<'a>),

    // ---- strings ---------------------------------------------------------
    Get {
        key: &'a [u8],
    },
    Set(Set<'a>),
    /// `DEL` and `UNLINK`. They differ only in whether Redis frees the memory
    /// on the calling thread, which is not a distinction this store has:
    /// reclamation is always the background reclaimer's job.
    Delete {
        keys: Vec<&'a [u8]>,
    },
    MGet {
        keys: Vec<&'a [u8]>,
    },
    MSet {
        pairs: Vec<(&'a [u8], &'a [u8])>,
    },
    MSetEx {
        pairs: Vec<(&'a [u8], &'a [u8])>,
        condition: Condition,
        expiry: Expiry,
        /// `MSETTAGS`: one tag list, carried by every pair in the batch. Empty
        /// for `MSETEX`.
        tags: Vec<&'a [u8]>,
        /// Whether the client sent `MSETTAGS`. See [`Set::tagged`].
        tagged: bool,
    },
    Exists {
        keys: Vec<&'a [u8]>,
    },
    /// `TYPE key`. Every value this server stores is a string, so the answer is
    /// only ever `string` or `none` — but a client library that probes the type
    /// before deciding how to read a key needs to be told that rather than
    /// meeting `unknown command`.
    Type {
        key: &'a [u8],
    },
    Append {
        key: &'a [u8],
        value: &'a [u8],
    },

    // ---- tags ------------------------------------------------------------
    /// `DELBYTAG tag [tag …]`: invalidate every record carrying each named tag.
    ///
    /// The other half of the tag surface — attaching them — is not a command of
    /// its own: `SETTAGS` and `MSETTAGS` are [`Set`] and [`Command::MSetEx`]
    /// carrying a tag list, because they are the same write with the same
    /// options and the same reply.
    ///
    /// Named for what it does to *records*, selected by tag. Nothing here
    /// deletes the tag itself: the registry keeps a name once it has seen it.
    DelByTag {
        tags: Vec<&'a [u8]>,
    },

    // ---- expiry ----------------------------------------------------------
    /// `EXPIRE` and `EXPIREAT`, which differ only in how the deadline is
    /// expressed — and [`Expiry`] already carries that distinction.
    Expire {
        key: &'a [u8],
        expiry: Expiry,
        condition: ExpireCondition,
    },
    Persist {
        key: &'a [u8],
    },
    Ttl {
        key: &'a [u8],
    },

    // ---- arithmetic ------------------------------------------------------
    /// `INCR`, `INCRBY`, `DECR`, `DECRBY` and `INCRBYFLOAT`. The reply shape
    /// follows the [`Number`] variant: integer for the first four, bulk string
    /// for the last.
    Incr {
        key: &'a [u8],
        delta: Number,
    },
    IncrEx(IncrEx<'a>),

    // ---- introspection ---------------------------------------------------
    /// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`.
    Scan(Scan<'a>),
    /// `INFO [section [section …]]`.
    Info {
        sections: super::encode::Sections,
    },
}

/// `SCAN`, decoded.
///
/// The cursor is still the **token** the client sent, not a position: turning
/// one into the other needs the server's table and this parser has no state.
/// See `vash_server::scan`.
#[derive(Debug)]
pub struct Scan<'a> {
    pub token: u64,
    /// `MATCH`. Empty matches everything, as an empty listing pattern does.
    pub pattern: &'a [u8],
    /// `COUNT`, already defaulted and clamped to what a listing page may hold.
    pub count: u32,
    /// `TYPE`. Every value here is a string, so anything else selects nothing —
    /// carried rather than rejected because that is a true answer and Redis
    /// gives it for a type that exists but has no keys.
    pub only_strings: bool,
}

/// A name and a secret, as presented on the wire.
///
/// `name` is `None` for Redis's one-argument `AUTH`, which addresses the
/// `default` identity. Resolving that default is the server's business, not the
/// parser's.
#[derive(Debug, PartialEq, Eq)]
pub struct Credential<'a> {
    pub name: Option<&'a [u8]>,
    pub secret: &'a [u8],
}

#[derive(Debug)]
pub struct Set<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub condition: Condition,
    pub expiry: Expiry,
    /// `GET`: reply with the value the key held beforehand rather than `OK`.
    pub return_previous: bool,
    /// Tag names to attach, in the order the client listed them. Empty for
    /// `SET`, which costs no allocation.
    pub tags: Vec<&'a [u8]>,
    /// Whether the client sent `SETTAGS` rather than `SET`.
    ///
    /// Not derivable from `tags` — `SETTAGS key value 0` is a legal tagless
    /// write — and needed because Redis names the command in its expiry error,
    /// which the executor raises after the parser is done.
    pub tagged: bool,
}

/// `INCREX`, which is every other arithmetic command plus bounds and an
/// expiry, applied in one step.
#[derive(Debug)]
pub struct IncrEx<'a> {
    pub key: &'a [u8],
    pub delta: Number,
    /// Result floor and ceiling. `None` means the limit of the mode's own type.
    pub lower: Option<Number>,
    pub upper: Option<Number>,
    /// Clamp an out-of-bounds result to the bound instead of skipping the
    /// write and reporting a zero increment.
    pub saturate: bool,
    /// `None` leaves the key's lifetime alone, which is what `INCREX` does
    /// when no expiry option is given.
    pub expiry: Option<Expiry>,
    /// `ENX`: apply the expiry only to a key that currently has none.
    pub only_if_persistent: bool,
}

/// A numeric argument, in whichever mode the command is operating.
///
/// The variant is also what decides how the reply is rendered, which is why it
/// survives parsing rather than being widened to `f64` immediately.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    Int(i64),
    Float(f64),
}

/// When a write is allowed to take effect (`NX`/`XX`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Condition {
    #[default]
    Always,
    /// `NX`: only when the key is absent — for `MSETEX`, only when *none* of
    /// the keys exist.
    IfAbsent,
    /// `XX`: only when it is present — for `MSETEX`, only when *all* of them
    /// are.
    IfPresent,
}

/// The `EXPIRE`/`EXPIREAT` guards added in Redis 7.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExpireCondition {
    #[default]
    Always,
    /// `NX`: only when the key has no expiry.
    IfPersistent,
    /// `XX`: only when it already has one.
    IfVolatile,
    /// `GT`: only when the new deadline is later than the current one. A key
    /// with no expiry counts as infinitely far off, so `GT` never applies to
    /// one.
    IfLater,
    /// `LT`: the mirror image, where a key with no expiry always loses.
    IfEarlier,
}

/// What a command asks to happen to a key's lifetime.
///
/// Every unit is normalised to milliseconds here so the executor has one form
/// to reason about: `EX` and `PX` become [`Expiry::After`], `EXAT` and `PXAT`
/// become [`Expiry::At`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiry {
    /// No expiry option was given. What that means is the command's business:
    /// `SET` clears any existing TTL, `INCREX` leaves it untouched.
    Unset,
    /// Milliseconds from now. May be zero or negative, which `EXPIRE` uses to
    /// mean "delete the key".
    After(i64),
    /// An absolute unix millisecond stamp.
    At(i64),
    /// `KEEPTTL`: whatever the key already carries.
    Keep,
    /// `PERSIST`: clear the expiry.
    Persist,
}

/// Parses one command from its already-framed arguments.
///
/// `consumed` is how many bytes the whole command occupied, and is carried into
/// the error so a rejected command still advances the stream by exactly its own
/// length.
pub fn parse_command<'a>(
    args: &[&'a [u8]],
    consumed: usize,
) -> Result<Parsed<'a>, ProtocolError<'a>> {
    let raw_verb = args[0]; // the framing layer rejects an empty array
    let fail = |reply: ErrorReply<'a>| ProtocolError::Recoverable { reply, consumed };

    // ASCII, and short: upper-casing into a fixed buffer keeps the dispatch a
    // single `match` without allocating. A name too long to be any verb we know
    // falls through to the unknown-command arm.
    let mut buffer = [0u8; MAX_VERB_LEN];
    let verb: &[u8] = if raw_verb.len() <= MAX_VERB_LEN {
        let upper = &mut buffer[..raw_verb.len()];
        upper.copy_from_slice(raw_verb);
        upper.make_ascii_uppercase();
        upper
    } else {
        &[]
    };

    let command = match verb {
        b"HELLO" => {
            let version = match args.get(1) {
                None => None,
                Some(token) => Some(parse_int(token).ok_or_else(|| {
                    fail(ErrorReply::Coded("NOPROTO", "unsupported protocol version"))
                })?),
            };

            // `HELLO 3 AUTH user pass` authenticates and negotiates in one
            // round trip. `SETNAME` stays refused: there is still no client
            // registry, and quietly accepting it would report a name back that
            // nothing had stored.
            let auth = match args.len() {
                ..=2 => None,
                5 if args[2].eq_ignore_ascii_case(b"AUTH") => Some(Credential {
                    name: Some(args[3]),
                    secret: args[4],
                }),
                _ => {
                    return Err(fail(ErrorReply::Err(
                        "HELLO option SETNAME is not supported, and AUTH takes a username \
                         and a password",
                    )));
                }
            };
            Command::Hello { version, auth }
        }

        // Redis 6 added the two-argument form for ACL users and kept the
        // one-argument one, which every pre-6 client still sends.
        //
        // The two failure modes are different errors, which is Redis's own
        // asymmetry and not ours: no arguments is an arity error, too many is a
        // syntax error. Verified against Redis 7.4.10.
        b"AUTH" => match args.len() {
            2 => Command::Auth(Credential {
                name: None,
                secret: args[1],
            }),
            3 => Command::Auth(Credential {
                name: Some(args[1]),
                secret: args[2],
            }),
            1 => return Err(fail(ErrorReply::WrongArity("auth"))),
            _ => return Err(fail(ErrorReply::SYNTAX)),
        },

        b"PING" => match args.len() {
            1 => Command::Ping { message: None },
            2 => Command::Ping {
                message: Some(args[1]),
            },
            _ => return Err(fail(ErrorReply::WrongArity("ping"))),
        },

        b"QUIT" => Command::Quit,

        b"GET" => Command::Get {
            key: exactly(args, 2, "get", consumed)?[1],
        },

        b"SET" => parse_set(args, consumed, Tagged::No)?,
        b"SETTAGS" => parse_set(args, consumed, Tagged::Yes)?,

        b"DEL" => Command::Delete {
            keys: at_least(args, 2, "del", consumed)?[1..].to_vec(),
        },
        b"UNLINK" => Command::Delete {
            keys: at_least(args, 2, "unlink", consumed)?[1..].to_vec(),
        },

        b"MGET" => Command::MGet {
            keys: at_least(args, 2, "mget", consumed)?[1..].to_vec(),
        },

        b"MSET" => {
            let args = at_least(args, 3, "mset", consumed)?;
            if args.len() % 2 != 1 {
                return Err(fail(ErrorReply::WrongArity("mset")));
            }
            Command::MSet {
                pairs: pairs(&args[1..]),
            }
        }

        b"MSETEX" => parse_msetex(args, consumed, Tagged::No)?,
        b"MSETTAGS" => parse_msetex(args, consumed, Tagged::Yes)?,

        // One command per tag would do the same work; taking a list saves the
        // round trips a framework spends invalidating several at once.
        b"DELBYTAG" => {
            let args = at_least(args, 2, "delbytag", consumed)?;
            let mut tags = Vec::with_capacity(args.len() - 1);
            for tag in &args[1..] {
                tags.push(check_tag(tag).map_err(fail)?);
            }
            Command::DelByTag { tags }
        }

        b"EXISTS" => Command::Exists {
            keys: at_least(args, 2, "exists", consumed)?[1..].to_vec(),
        },

        b"TYPE" => Command::Type {
            key: exactly(args, 2, "type", consumed)?[1],
        },

        b"APPEND" => {
            let args = exactly(args, 3, "append", consumed)?;
            Command::Append {
                key: args[1],
                value: args[2],
            }
        }

        b"EXPIRE" => parse_expire(args, consumed, "expire", false)?,
        b"EXPIREAT" => parse_expire(args, consumed, "expireat", true)?,

        b"PERSIST" => Command::Persist {
            key: exactly(args, 2, "persist", consumed)?[1],
        },

        b"TTL" => Command::Ttl {
            key: exactly(args, 2, "ttl", consumed)?[1],
        },

        b"INCR" => Command::Incr {
            key: exactly(args, 2, "incr", consumed)?[1],
            delta: Number::Int(1),
        },
        b"DECR" => Command::Incr {
            key: exactly(args, 2, "decr", consumed)?[1],
            delta: Number::Int(-1),
        },

        b"INCRBY" | b"DECRBY" => {
            let name = if verb == b"INCRBY" {
                "incrby"
            } else {
                "decrby"
            };
            let args = exactly(args, 3, name, consumed)?;
            let magnitude = parse_int(args[2]).ok_or_else(|| fail(ErrorReply::NOT_AN_INTEGER))?;
            // `DECRBY key -9223372036854775808` has no representable negation,
            // and Redis reports that as an overflow rather than as bad input.
            let delta = if verb == b"DECRBY" {
                magnitude
                    .checked_neg()
                    .ok_or_else(|| fail(ErrorReply::OVERFLOW))?
            } else {
                magnitude
            };
            Command::Incr {
                key: args[1],
                delta: Number::Int(delta),
            }
        }

        b"INCRBYFLOAT" => {
            let args = exactly(args, 3, "incrbyfloat", consumed)?;
            let delta = parse_float(args[2]).ok_or_else(|| fail(ErrorReply::NOT_A_FLOAT))?;
            Command::Incr {
                key: args[1],
                delta: Number::Float(delta),
            }
        }

        b"INCREX" => parse_increx(args, consumed)?,

        b"SCAN" => parse_scan(args, consumed)?,

        // `INFO` never fails on its arguments: Redis answers an unrecognised
        // section with an empty string rather than an error, so a client
        // probing for a section it may not have gets a usable answer.
        b"INFO" => {
            let mut sections = match args.len() {
                1 => super::encode::Sections::DEFAULT,
                _ => super::encode::Sections::NONE,
            };
            for name in &args[1..] {
                if name.eq_ignore_ascii_case(b"all") || name.eq_ignore_ascii_case(b"everything") {
                    sections = super::encode::Sections::ALL;
                } else if let Some(section) = super::encode::Sections::named(name) {
                    sections = sections.with(section);
                }
            }
            Command::Info { sections }
        }

        _ => return Err(fail(ErrorReply::UnknownCommand(raw_verb))),
    };

    Ok(Parsed { command, consumed })
}

/// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`
fn parse_scan<'a>(args: &[&'a [u8]], consumed: usize) -> Result<Command<'a>, ProtocolError<'a>> {
    let args = at_least(args, 2, "scan", consumed)?;
    let fail = |reply: ErrorReply<'a>| ProtocolError::Recoverable { reply, consumed };

    // Redis's own wording, and its own rule: the cursor is decimal and
    // unsigned. A token this server never issued is refused later, by the table
    // that would have issued it.
    let token = std::str::from_utf8(args[1])
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| fail(ErrorReply::Err("invalid cursor")))?;

    let mut scan = Scan {
        token,
        pattern: b"",
        // Redis's default, and a hint rather than a limit — see the clamp below.
        count: 10,
        only_strings: true,
    };

    let mut cursor = Options::new(&args[2..], "scan", consumed);
    while let Some(token) = cursor.next() {
        if eq(token, b"MATCH") {
            scan.pattern = cursor.argument()?;
            // This server's glob is `*`, `?` and `\` and deliberately nothing
            // else; Redis's `stringmatchlen` also has `[a-z]` and `[^x]`.
            // Treating `[` as a literal byte would make `MATCH k[0-9]*` match
            // nothing and say nothing, so it is named — the same call
            // `SET … IFEQ` makes.
            if has_unescaped_class(scan.pattern) {
                return Err(fail(ErrorReply::Err(
                    "character classes are not supported in MATCH",
                )));
            }
            vash_core::glob::validate(scan.pattern)
                .map_err(|_| fail(ErrorReply::Err("invalid MATCH pattern")))?;
        } else if eq(token, b"COUNT") {
            let count =
                parse_int(cursor.argument()?).ok_or_else(|| fail(ErrorReply::NOT_AN_INTEGER))?;
            if count <= 0 {
                return Err(fail(ErrorReply::SYNTAX));
            }
            // **Clamped, where VCP's `limit` is rejected.** `docs/opcodes.md`
            // refuses an over-limit because a VCP client that asked for 10000
            // and silently got 1024 would page incorrectly. Redis specifies
            // `COUNT` as a hint the server may ignore, clients pass large ones
            // freely, and it is the returned cursor that drives the loop — so
            // clamping here is both legal and invisible.
            scan.count = count.min(vash_core::MAX_LIST_LIMIT as i64) as u32;
        } else if eq(token, b"TYPE") {
            scan.only_strings = cursor.argument()?.eq_ignore_ascii_case(b"string");
        } else {
            return Err(fail(ErrorReply::SYNTAX));
        }
    }

    Ok(Command::Scan(scan))
}

/// Whether a pattern opens a character class this server cannot match.
///
/// An escaped `\[` is a literal bracket and is fine; a bare one is a class.
fn has_unescaped_class(pattern: &[u8]) -> bool {
    let mut i = 0;
    while i < pattern.len() {
        match pattern[i] {
            vash_core::glob::ESCAPE => i += 2,
            b'[' => return true,
            _ => i += 1,
        }
    }
    false
}

/// `SET key value [NX | XX] [GET] [EX s | PX ms | EXAT ts | PXAT ms | KEEPTTL]`,
/// and `SETTAGS key value numtags tag [tag …] [the same options]`.
///
/// One parser for both: the tag list is the only difference, and routing
/// `SETTAGS` through a parser of its own would mean maintaining two copies of
/// an option grammar that has to stay identical.
fn parse_set<'a>(
    args: &[&'a [u8]],
    consumed: usize,
    tagged: Tagged,
) -> Result<Command<'a>, ProtocolError<'a>> {
    let name = tagged.pick("set", "settags");
    let args = at_least(args, if tagged.yes() { 4 } else { 3 }, name, consumed)?;
    let fail = |reply: ErrorReply<'a>| ProtocolError::Recoverable { reply, consumed };

    let (tags, options_at) = tag_list(args, 3, tagged, name, consumed)?;

    let mut set = Set {
        key: args[1],
        value: args[2],
        condition: Condition::Always,
        expiry: Expiry::Unset,
        return_previous: false,
        tags,
        tagged: tagged.yes(),
    };

    let mut cursor = Options::new(&args[options_at..], name, consumed);
    while let Some(token) = cursor.next() {
        if eq(token, b"NX") {
            cursor.set_condition(&mut set.condition, Condition::IfAbsent)?;
        } else if eq(token, b"XX") {
            cursor.set_condition(&mut set.condition, Condition::IfPresent)?;
        } else if eq(token, b"GET") {
            set.return_previous = true;
        } else if let Some(expiry) = cursor.expiry(token, ExpiryOptions::WITH_KEEPTTL)? {
            cursor.set_expiry(&mut set.expiry, expiry)?;
        } else if eq(token, b"IFEQ")
            || eq(token, b"IFNE")
            || eq(token, b"IFDEQ")
            || eq(token, b"IFDNE")
        {
            // Named rather than lumped into `syntax error`: they are real SET
            // options, and a client using one deserves to know why it failed.
            return Err(fail(ErrorReply::Err(
                "SET value conditions (IFEQ, IFNE, IFDEQ, IFDNE) are not supported",
            )));
        } else {
            return Err(fail(ErrorReply::SYNTAX));
        }
    }

    Ok(Command::Set(set))
}

/// `MSETEX numkeys key value [key value ...] [NX | XX] [EX … | KEEPTTL]`, and
/// `MSETTAGS numkeys key value [key value …] numtags tag [tag …] [the same
/// options]`.
///
/// The batch shares one tag list rather than carrying one per pair. The
/// boundary would take per-pair lists — `SetMany` holds a `Vec<Set>`, each with
/// its own tags — but the wire form for that is a second counted list per pair,
/// and a batch written together is a batch invalidated together in every use
/// this is for.
fn parse_msetex<'a>(
    args: &[&'a [u8]],
    consumed: usize,
    tagged: Tagged,
) -> Result<Command<'a>, ProtocolError<'a>> {
    let name = tagged.pick("msetex", "msettags");
    let args = at_least(args, if tagged.yes() { 5 } else { 4 }, name, consumed)?;
    let fail = |reply: ErrorReply<'a>| ProtocolError::Recoverable { reply, consumed };

    let count = parse_int(args[1]).ok_or_else(|| fail(ErrorReply::NOT_AN_INTEGER))?;
    if count <= 0 {
        return Err(fail(ErrorReply::Err("numkeys should be greater than 0")));
    }
    // Checked against the batch ceiling before the pairs are collected, so an
    // enormous `numkeys` costs nothing.
    if count as u64 > vash_core::MAX_BATCH_ITEMS as u64 {
        return Err(fail(ErrorReply::Err("too many keys")));
    }

    let body = 2 + (count as usize) * 2;
    if args.len() < body {
        return Err(fail(ErrorReply::WrongArity(name)));
    }

    let (tags, options_at) = tag_list(args, body, tagged, name, consumed)?;

    let mut condition = Condition::Always;
    let mut expiry = Expiry::Unset;

    let mut cursor = Options::new(&args[options_at..], name, consumed);
    while let Some(token) = cursor.next() {
        if eq(token, b"NX") {
            cursor.set_condition(&mut condition, Condition::IfAbsent)?;
        } else if eq(token, b"XX") {
            cursor.set_condition(&mut condition, Condition::IfPresent)?;
        } else if let Some(parsed) = cursor.expiry(token, ExpiryOptions::WITH_KEEPTTL)? {
            cursor.set_expiry(&mut expiry, parsed)?;
        } else {
            return Err(fail(ErrorReply::SYNTAX));
        }
    }

    Ok(Command::MSetEx {
        pairs: pairs(&args[2..body]),
        condition,
        expiry,
        tags,
        tagged: tagged.yes(),
    })
}

/// Whether the verb being parsed carries a tag list.
///
/// A named type rather than a bare `bool` because it also picks the command
/// name every error in these two parsers is worded with, and a call site
/// reading `Tagged::Yes` cannot get the sense backwards.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Tagged {
    No,
    Yes,
}

impl Tagged {
    fn yes(self) -> bool {
        self == Self::Yes
    }

    fn pick(self, plain: &'static str, tagged: &'static str) -> &'static str {
        if self.yes() { tagged } else { plain }
    }
}

/// Reads `numtags tag [tag …]` at `at`, and reports where the options begin.
///
/// Counted rather than delimited, because tag names are binary-safe: a
/// comma-separated list — which is what the memcached extension has to use,
/// having nowhere else to put them — cannot express a name containing a comma.
///
/// `numtags 0` is accepted, unlike `MSETEX`'s `numkeys`. A batch of no keys is a
/// meaningless write; a write with no tags is an ordinary one, and a client
/// building a command from a possibly-empty list should not have to switch
/// verbs to send it.
fn tag_list<'t, 'a>(
    args: &'t [&'a [u8]],
    at: usize,
    tagged: Tagged,
    name: &'static str,
    consumed: usize,
) -> Result<(Vec<&'a [u8]>, usize), ProtocolError<'a>> {
    let fail = |reply: ErrorReply<'a>| ProtocolError::Recoverable { reply, consumed };
    if !tagged.yes() {
        return Ok((Vec::new(), at));
    }

    let count = parse_int(args[at]).ok_or_else(|| fail(ErrorReply::NOT_AN_INTEGER))?;
    if count < 0 {
        return Err(fail(ErrorReply::Err(
            "numtags should be greater than or equal to 0",
        )));
    }
    // The record format's ceiling, not the configured limit: `tag_count` is a
    // `u8`, so a longer list cannot be written down at all. Checking it before
    // the names are collected means an enormous `numtags` costs nothing. The
    // store still applies `store.tags.max_per_record`, which is lower.
    if count as u64 > vash_core::ABSOLUTE_MAX_TAGS as u64 {
        return Err(fail(ErrorReply::Err("too many tags")));
    }

    let first = at + 1;
    let end = first + count as usize;
    if args.len() < end {
        return Err(fail(ErrorReply::WrongArity(name)));
    }

    let mut tags = Vec::with_capacity(count as usize);
    for tag in &args[first..end] {
        tags.push(check_tag(tag).map_err(fail)?);
    }
    Ok((tags, end))
}

/// Checks one tag name against the format's limits.
///
/// The same two rules the memcached extension applies, in this dialect's
/// wording: a name is 1–255 bytes and otherwise binary-safe. Rejected here so a
/// batch that cannot be written never reaches the writer queue.
fn check_tag<'a>(tag: &'a [u8]) -> Result<&'a [u8], ErrorReply<'a>> {
    if tag.is_empty() || tag.len() > vash_core::MAX_TAG_LEN {
        return Err(ErrorReply::Err("invalid tag"));
    }
    Ok(tag)
}

/// `EXPIRE key seconds [NX | XX | GT | LT]` and its `EXPIREAT` twin.
///
/// Both take seconds; the only difference is whether the number is an offset
/// from now or a stamp, which is what `absolute` says.
fn parse_expire<'a>(
    args: &[&'a [u8]],
    consumed: usize,
    name: &'static str,
    absolute: bool,
) -> Result<Command<'a>, ProtocolError<'a>> {
    let args = at_least(args, 3, name, consumed)?;
    let fail = |reply: ErrorReply<'a>| ProtocolError::Recoverable { reply, consumed };

    let value = parse_int(args[2]).ok_or_else(|| fail(ErrorReply::NOT_AN_INTEGER))?;
    // Non-positive is legal here, unlike `SET`: it means "delete the key". Only
    // a value too large to express in milliseconds is refused.
    let millis = value
        .checked_mul(1_000)
        .ok_or_else(|| fail(ErrorReply::InvalidExpire(name)))?;
    let expiry = if absolute {
        Expiry::At(millis)
    } else {
        Expiry::After(millis)
    };

    let mut condition = ExpireCondition::Always;
    if let Some(token) = args.get(3) {
        if args.len() > 4 {
            return Err(fail(ErrorReply::SYNTAX));
        }
        condition = if eq(token, b"NX") {
            ExpireCondition::IfPersistent
        } else if eq(token, b"XX") {
            ExpireCondition::IfVolatile
        } else if eq(token, b"GT") {
            ExpireCondition::IfLater
        } else if eq(token, b"LT") {
            ExpireCondition::IfEarlier
        } else {
            return Err(fail(ErrorReply::SYNTAX));
        };
    }

    Ok(Command::Expire {
        key: args[1],
        expiry,
        condition,
    })
}

/// `INCREX key [BYFLOAT inc | BYINT inc] [LBOUND lb] [UBOUND ub] [SATURATE]`
/// `[EX s | PX ms | EXAT ts | PXAT ms | PERSIST] [ENX]`
///
/// Options may appear in any order, which is why the bounds are collected as
/// raw tokens and only converted once the mode is known: `LBOUND 5` means an
/// integer or a float depending on a `BYFLOAT` that may come after it.
fn parse_increx<'a>(args: &[&'a [u8]], consumed: usize) -> Result<Command<'a>, ProtocolError<'a>> {
    let args = at_least(args, 2, "increx", consumed)?;
    let fail = |reply: ErrorReply<'a>| ProtocolError::Recoverable { reply, consumed };

    let mut delta: Option<Number> = None;
    let mut lower_token: Option<&[u8]> = None;
    let mut upper_token: Option<&[u8]> = None;
    let mut saturate = false;
    let mut expiry: Option<Expiry> = None;
    let mut only_if_persistent = false;

    let mut cursor = Options::new(&args[2..], "increx", consumed);
    while let Some(token) = cursor.next() {
        if eq(token, b"BYINT") || eq(token, b"BYFLOAT") {
            if delta.is_some() {
                return Err(fail(ErrorReply::SYNTAX));
            }
            let value = cursor.argument()?;
            delta = Some(if eq(token, b"BYINT") {
                Number::Int(parse_int(value).ok_or_else(|| fail(ErrorReply::NOT_AN_INTEGER))?)
            } else {
                Number::Float(parse_float(value).ok_or_else(|| fail(ErrorReply::NOT_A_FLOAT))?)
            });
        } else if eq(token, b"LBOUND") {
            lower_token = Some(cursor.argument()?);
        } else if eq(token, b"UBOUND") {
            upper_token = Some(cursor.argument()?);
        } else if eq(token, b"SATURATE") {
            saturate = true;
        } else if eq(token, b"ENX") {
            only_if_persistent = true;
        } else if eq(token, b"PERSIST") {
            if expiry.is_some() {
                return Err(fail(ErrorReply::SYNTAX));
            }
            expiry = Some(Expiry::Persist);
        } else if let Some(parsed) = cursor.expiry(token, ExpiryOptions::PLAIN)? {
            if expiry.is_some() {
                return Err(fail(ErrorReply::SYNTAX));
            }
            expiry = Some(parsed);
        } else {
            return Err(fail(ErrorReply::SYNTAX));
        }
    }

    let delta = delta.unwrap_or(Number::Int(1));
    let bound = |token: Option<&[u8]>| -> Result<Option<Number>, ProtocolError<'a>> {
        let Some(token) = token else { return Ok(None) };
        Ok(Some(match delta {
            Number::Int(_) => {
                Number::Int(parse_int(token).ok_or_else(|| fail(ErrorReply::NOT_AN_INTEGER))?)
            }
            Number::Float(_) => {
                Number::Float(parse_float(token).ok_or_else(|| fail(ErrorReply::NOT_A_FLOAT))?)
            }
        }))
    };
    let lower = bound(lower_token)?;
    let upper = bound(upper_token)?;

    if let (Some(low), Some(high)) = (lower, upper)
        && as_float(low) > as_float(high)
    {
        return Err(fail(ErrorReply::Err(
            "LBOUND must be less than or equal to UBOUND",
        )));
    }

    // `ENX` only means anything alongside a deadline, and "only set the expiry
    // if there is no expiry" cannot be reconciled with "remove the expiry".
    if only_if_persistent && !matches!(expiry, Some(Expiry::After(_) | Expiry::At(_))) {
        return Err(fail(ErrorReply::SYNTAX));
    }

    Ok(Command::IncrEx(IncrEx {
        key: args[1],
        delta,
        lower,
        upper,
        saturate,
        expiry,
        only_if_persistent,
    }))
}

/// A cursor over a command's trailing option tokens.
///
/// Exists so the four commands that take options share one set of rules about
/// what a missing argument, a repeated option or a conflicting pair means.
struct Options<'t, 'a> {
    tokens: &'t [&'a [u8]],
    position: usize,
    name: &'static str,
    consumed: usize,
}

impl<'t, 'a> Options<'t, 'a> {
    fn new(tokens: &'t [&'a [u8]], name: &'static str, consumed: usize) -> Self {
        Self {
            tokens,
            position: 0,
            name,
            consumed,
        }
    }

    fn fail(&self, reply: ErrorReply<'a>) -> ProtocolError<'a> {
        ProtocolError::Recoverable {
            reply,
            consumed: self.consumed,
        }
    }

    #[allow(clippy::should_implement_trait)]
    fn next(&mut self) -> Option<&'a [u8]> {
        let token = self.tokens.get(self.position)?;
        self.position += 1;
        Some(token)
    }

    /// The value belonging to the option just returned by [`Options::next`].
    fn argument(&mut self) -> Result<&'a [u8], ProtocolError<'a>> {
        let token = self
            .tokens
            .get(self.position)
            .ok_or_else(|| self.fail(ErrorReply::SYNTAX))?;
        self.position += 1;
        Ok(token)
    }

    /// Records a condition, rejecting a second one — `SET k v NX XX` is a
    /// syntax error in Redis, not a last-one-wins.
    fn set_condition(
        &self,
        slot: &mut Condition,
        value: Condition,
    ) -> Result<(), ProtocolError<'a>> {
        if *slot != Condition::Always {
            return Err(self.fail(ErrorReply::SYNTAX));
        }
        *slot = value;
        Ok(())
    }

    fn set_expiry(&self, slot: &mut Expiry, value: Expiry) -> Result<(), ProtocolError<'a>> {
        if *slot != Expiry::Unset {
            return Err(self.fail(ErrorReply::SYNTAX));
        }
        *slot = value;
        Ok(())
    }

    /// Recognises `EX`/`PX`/`EXAT`/`PXAT`, and `KEEPTTL` where the command
    /// takes it. `Ok(None)` means the token was not an expiry option at all,
    /// which leaves the caller to decide whether that is a syntax error.
    fn expiry(
        &mut self,
        token: &[u8],
        options: ExpiryOptions,
    ) -> Result<Option<Expiry>, ProtocolError<'a>> {
        if options.keep_ttl && eq(token, b"KEEPTTL") {
            return Ok(Some(Expiry::Keep));
        }

        let (unit_ms, absolute) = if eq(token, b"EX") {
            (1_000, false)
        } else if eq(token, b"PX") {
            (1, false)
        } else if eq(token, b"EXAT") {
            (1_000, true)
        } else if eq(token, b"PXAT") {
            (1, true)
        } else {
            return Ok(None);
        };

        let raw = self.argument()?;
        let value = parse_int(raw).ok_or_else(|| self.fail(ErrorReply::NOT_AN_INTEGER))?;
        // Unlike `EXPIRE`, these commands take a *positive* time only: Redis
        // refuses to express "already gone" as a `SET` option.
        let millis = value
            .checked_mul(unit_ms)
            .filter(|millis| *millis > 0)
            .ok_or_else(|| self.fail(ErrorReply::InvalidExpire(self.name)))?;

        Ok(Some(if absolute {
            Expiry::At(millis)
        } else {
            Expiry::After(millis)
        }))
    }
}

/// Which expiry tokens a particular command accepts.
#[derive(Clone, Copy)]
struct ExpiryOptions {
    keep_ttl: bool,
}

impl ExpiryOptions {
    const PLAIN: Self = Self { keep_ttl: false };
    const WITH_KEEPTTL: Self = Self { keep_ttl: true };
}

fn exactly<'t, 'a>(
    args: &'t [&'a [u8]],
    count: usize,
    name: &'static str,
    consumed: usize,
) -> Result<&'t [&'a [u8]], ProtocolError<'a>> {
    if args.len() != count {
        return Err(ProtocolError::Recoverable {
            reply: ErrorReply::WrongArity(name),
            consumed,
        });
    }
    Ok(args)
}

fn at_least<'t, 'a>(
    args: &'t [&'a [u8]],
    count: usize,
    name: &'static str,
    consumed: usize,
) -> Result<&'t [&'a [u8]], ProtocolError<'a>> {
    if args.len() < count {
        return Err(ProtocolError::Recoverable {
            reply: ErrorReply::WrongArity(name),
            consumed,
        });
    }
    Ok(args)
}

fn pairs<'a>(flat: &[&'a [u8]]) -> Vec<(&'a [u8], &'a [u8])> {
    flat.chunks_exact(2)
        .map(|pair| (pair[0], pair[1]))
        .collect()
}

/// Option and command names are ASCII and case-insensitive in Redis.
fn eq(token: &[u8], name: &[u8]) -> bool {
    token.eq_ignore_ascii_case(name)
}

fn as_float(number: Number) -> f64 {
    match number {
        Number::Int(value) => value as f64,
        Number::Float(value) => value,
    }
}

/// Redis's `string2ll` and its float counterpart, re-exported from the domain
/// crate where they now live.
///
/// They moved because the same rules judge a command's *argument*, here, and the
/// *stored counter* that the increment reads back, in `vash_core::arith` — and
/// two copies of a rule this exacting is how a value gets written that the
/// server then refuses to read. The re-export keeps every call site in this
/// module spelling them the way it always did.
pub use vash_core::{parse_float, parse_int};

#[cfg(test)]
mod tests {
    use super::super::{Outcome, parse};
    use super::*;

    /// Builds a RESP array from its arguments.
    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    /// Parses one command from its arguments.
    ///
    /// The parser borrows from the buffer, so the buffer has to outlive the
    /// call — leaking it is what lets every test below stay a single
    /// expression instead of a `let` for the bytes and another for the parse.
    /// A few hundred bytes per test, freed when the process exits.
    fn command(args: &[&[u8]]) -> Command<'static> {
        let buffer: &'static [u8] = Box::leak(encode(args).into_boxed_slice());
        match parse(buffer) {
            Ok(Outcome::Command(parsed)) => parsed.command,
            other => panic!("expected a command, got {other:?}"),
        }
    }

    fn rejected(args: &[&[u8]]) -> ErrorReply<'static> {
        let buffer: &'static [u8] = Box::leak(encode(args).into_boxed_slice());
        match parse(buffer) {
            Err(ProtocolError::Recoverable { reply, .. }) => reply,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn verbs_are_case_insensitive() {
        for verb in [&b"get"[..], b"GET", b"Get", b"gEt"] {
            assert!(matches!(command(&[verb, b"k"]), Command::Get { key: b"k" }));
        }
    }

    #[test]
    fn parses_set_with_every_supported_option() {
        let Command::Set(set) = command(&[b"SET", b"k", b"v", b"NX", b"GET", b"EX", b"60"]) else {
            panic!("expected a set")
        };
        assert_eq!(set.key, b"k");
        assert_eq!(set.value, b"v");
        assert_eq!(set.condition, Condition::IfAbsent);
        assert!(set.return_previous);
        assert_eq!(set.expiry, Expiry::After(60_000));

        // Options are order-independent and case-insensitive.
        let Command::Set(set) = command(&[b"set", b"k", b"v", b"keepttl", b"xx"]) else {
            panic!()
        };
        assert_eq!(set.condition, Condition::IfPresent);
        assert_eq!(set.expiry, Expiry::Keep);
    }

    #[test]
    fn every_expiry_unit_normalises_to_milliseconds() {
        for (option, value, expected) in [
            (&b"EX"[..], &b"1"[..], Expiry::After(1_000)),
            (b"PX", b"1500", Expiry::After(1_500)),
            (b"EXAT", b"2000000000", Expiry::At(2_000_000_000_000)),
            (b"PXAT", b"1700000000123", Expiry::At(1_700_000_000_123)),
        ] {
            let Command::Set(set) = command(&[b"SET", b"k", b"v", option, value]) else {
                panic!()
            };
            assert_eq!(set.expiry, expected, "{option:?}");
        }
    }

    #[test]
    fn conflicting_options_are_a_syntax_error() {
        for args in [
            &[b"SET".as_slice(), b"k", b"v", b"NX", b"XX"][..],
            &[b"SET", b"k", b"v", b"EX", b"1", b"PX", b"1"],
            &[b"SET", b"k", b"v", b"EX", b"1", b"KEEPTTL"],
            &[b"SET", b"k", b"v", b"NONSENSE"],
        ] {
            assert_eq!(rejected(args), ErrorReply::SYNTAX, "{args:?}");
        }
    }

    #[test]
    fn set_refuses_a_non_positive_expiry() {
        // Redis will not let `SET` express "already gone"; `EXPIRE` is where
        // that lives.
        for value in [&b"0"[..], b"-1"] {
            assert_eq!(
                rejected(&[b"SET", b"k", b"v", b"EX", value]),
                ErrorReply::InvalidExpire("set")
            );
        }
    }

    #[test]
    fn del_and_unlink_are_the_same_command() {
        for verb in [&b"DEL"[..], b"UNLINK"] {
            let Command::Delete { keys } = command(&[verb, b"a", b"b"]) else {
                panic!("expected a delete for {verb:?}")
            };
            assert_eq!(keys, vec![&b"a"[..], b"b"]);
        }
    }

    #[test]
    fn mset_needs_whole_pairs() {
        let Command::MSet { pairs } = command(&[b"MSET", b"a", b"1", b"b", b"2"]) else {
            panic!()
        };
        assert_eq!(pairs, vec![(&b"a"[..], &b"1"[..]), (&b"b"[..], &b"2"[..])]);

        assert_eq!(
            rejected(&[b"MSET", b"a", b"1", b"b"]),
            ErrorReply::WrongArity("mset")
        );
    }

    #[test]
    fn parses_msetex() {
        let Command::MSetEx {
            pairs,
            condition,
            expiry,
            tags,
            tagged,
        } = command(&[b"MSETEX", b"2", b"a", b"1", b"b", b"2", b"NX", b"EX", b"30"])
        else {
            panic!("expected an msetex")
        };
        assert_eq!(pairs.len(), 2);
        assert_eq!(condition, Condition::IfAbsent);
        assert_eq!(expiry, Expiry::After(30_000));
        assert!(tags.is_empty());
        assert!(!tagged);
    }

    #[test]
    fn msetex_checks_numkeys_against_the_arguments() {
        // The count is what says where the pairs stop and the options start,
        // so a wrong one is not something to guess around.
        assert_eq!(
            rejected(&[b"MSETEX", b"3", b"a", b"1", b"b", b"2"]),
            ErrorReply::WrongArity("msetex")
        );
        assert_eq!(
            rejected(&[b"MSETEX", b"0", b"a", b"1"]),
            ErrorReply::Err("numkeys should be greater than 0")
        );
    }

    #[test]
    fn parses_settags() {
        let Command::Set(set) = command(&[
            b"SETTAGS",
            b"k",
            b"v",
            b"2",
            b"news",
            b"author:7",
            b"EX",
            b"60",
            b"NX",
        ]) else {
            panic!("expected a set")
        };
        assert_eq!(set.key, b"k");
        assert_eq!(set.value, b"v");
        assert_eq!(set.tags, vec![&b"news"[..], &b"author:7"[..]]);
        assert!(set.tagged);
        // The tag list changes nothing about the options that follow it.
        assert_eq!(set.expiry, Expiry::After(60_000));
        assert_eq!(set.condition, Condition::IfAbsent);
    }

    /// A tagless write is an ordinary thing to ask for, and a client building a
    /// command from a list that turned out empty should not have to switch
    /// verbs. `tagged` is what remembers which verb was sent.
    #[test]
    fn settags_accepts_no_tags() {
        let Command::Set(set) = command(&[b"SETTAGS", b"k", b"v", b"0", b"KEEPTTL"]) else {
            panic!("expected a set")
        };
        assert!(set.tags.is_empty());
        assert!(set.tagged);
        assert_eq!(set.expiry, Expiry::Keep);
    }

    #[test]
    fn set_carries_no_tags_and_settags_needs_a_count() {
        let Command::Set(set) = command(&[b"SET", b"k", b"v"]) else {
            panic!("expected a set")
        };
        assert!(set.tags.is_empty());
        assert!(!set.tagged);

        // Without the count there is nothing to say where the tags stop.
        assert_eq!(
            rejected(&[b"SETTAGS", b"k", b"v"]),
            ErrorReply::WrongArity("settags")
        );
    }

    #[test]
    fn settags_checks_numtags_against_the_arguments() {
        assert_eq!(
            rejected(&[b"SETTAGS", b"k", b"v", b"3", b"news"]),
            ErrorReply::WrongArity("settags")
        );
        assert_eq!(
            rejected(&[b"SETTAGS", b"k", b"v", b"-1", b"news"]),
            ErrorReply::Err("numtags should be greater than or equal to 0")
        );
        assert_eq!(
            rejected(&[b"SETTAGS", b"k", b"v", b"two", b"news"]),
            ErrorReply::NOT_AN_INTEGER
        );
        // Past the record format's own ceiling, and refused before a list that
        // long is collected.
        assert_eq!(
            rejected(&[b"SETTAGS", b"k", b"v", b"256", b"news"]),
            ErrorReply::Err("too many tags")
        );
    }

    #[test]
    fn a_tag_name_is_one_to_255_bytes() {
        let long = vec![b'x'; vash_core::MAX_TAG_LEN + 1];
        assert_eq!(
            rejected(&[b"SETTAGS", b"k", b"v", b"1", &long]),
            ErrorReply::Err("invalid tag")
        );
        assert_eq!(
            rejected(&[b"SETTAGS", b"k", b"v", b"1", b""]),
            ErrorReply::Err("invalid tag")
        );
        assert_eq!(
            rejected(&[b"DELBYTAG", b"news", b""]),
            ErrorReply::Err("invalid tag")
        );

        // The limit itself is allowed, on both verbs.
        let edge = vec![b'x'; vash_core::MAX_TAG_LEN];
        assert!(matches!(
            command(&[b"SETTAGS", b"k", b"v", b"1", &edge]),
            Command::Set(_)
        ));
        assert!(matches!(
            command(&[b"DELBYTAG", &edge]),
            Command::DelByTag { .. }
        ));
    }

    /// A tag name is binary-safe, which is why the list is counted rather than
    /// comma-separated: the memcached extension cannot express this at all.
    #[test]
    fn a_tag_name_may_contain_a_comma() {
        let Command::Set(set) = command(&[b"SETTAGS", b"k", b"v", b"1", b"a,b"]) else {
            panic!("expected a set")
        };
        assert_eq!(set.tags, vec![&b"a,b"[..]]);
    }

    #[test]
    fn parses_msettags() {
        let Command::MSetEx {
            pairs,
            condition,
            expiry,
            tags,
            tagged,
        } = command(&[
            b"MSETTAGS",
            b"2",
            b"a",
            b"1",
            b"b",
            b"2",
            b"1",
            b"news",
            b"XX",
            b"EX",
            b"30",
        ])
        else {
            panic!("expected an msetex")
        };
        assert_eq!(pairs.len(), 2);
        assert_eq!(tags, vec![&b"news"[..]]);
        assert!(tagged);
        assert_eq!(condition, Condition::IfPresent);
        assert_eq!(expiry, Expiry::After(30_000));
    }

    #[test]
    fn msettags_counts_the_tags_after_the_pairs() {
        // `numkeys` says where the pairs stop; `numtags` says where the tags do.
        // Getting either wrong is an arity error, not a guess.
        assert_eq!(
            rejected(&[b"MSETTAGS", b"2", b"a", b"1", b"b", b"2", b"2", b"news"]),
            ErrorReply::WrongArity("msettags")
        );
        assert_eq!(
            rejected(&[b"MSETTAGS", b"1", b"a", b"1"]),
            ErrorReply::WrongArity("msettags")
        );
        assert_eq!(
            rejected(&[b"MSETTAGS", b"0", b"a", b"1", b"0"]),
            ErrorReply::Err("numkeys should be greater than 0")
        );
    }

    #[test]
    fn parses_delbytag() {
        let Command::DelByTag { tags } = command(&[b"DELBYTAG", b"news", b"author:7"]) else {
            panic!("expected a tag invalidation")
        };
        assert_eq!(tags, vec![&b"news"[..], &b"author:7"[..]]);

        assert_eq!(rejected(&[b"DELBYTAG"]), ErrorReply::WrongArity("delbytag"));
    }

    #[test]
    fn expire_accepts_a_non_positive_deadline() {
        // Which is how Redis spells "delete it".
        let Command::Expire { expiry, .. } = command(&[b"EXPIRE", b"k", b"-1"]) else {
            panic!()
        };
        assert_eq!(expiry, Expiry::After(-1_000));
    }

    #[test]
    fn parses_every_expire_condition() {
        for (token, expected) in [
            (&b"NX"[..], ExpireCondition::IfPersistent),
            (b"XX", ExpireCondition::IfVolatile),
            (b"GT", ExpireCondition::IfLater),
            (b"LT", ExpireCondition::IfEarlier),
        ] {
            let Command::Expire { condition, .. } = command(&[b"EXPIRE", b"k", b"10", token])
            else {
                panic!()
            };
            assert_eq!(condition, expected, "{token:?}");
        }
    }

    #[test]
    fn expireat_is_absolute() {
        let Command::Expire { expiry, .. } = command(&[b"EXPIREAT", b"k", b"1700000000"]) else {
            panic!()
        };
        assert_eq!(expiry, Expiry::At(1_700_000_000_000));
    }

    #[test]
    fn the_plain_counters_are_incr_with_a_fixed_delta() {
        for (args, expected) in [
            (&[b"INCR".as_slice(), b"k"][..], 1),
            (&[b"DECR", b"k"], -1),
            (&[b"INCRBY", b"k", b"5"], 5),
            (&[b"DECRBY", b"k", b"5"], -5),
        ] {
            let Command::Incr { delta, .. } = command(args) else {
                panic!("expected an incr for {args:?}")
            };
            assert_eq!(delta, Number::Int(expected), "{args:?}");
        }

        let Command::Incr { delta, .. } = command(&[b"INCRBYFLOAT", b"k", b"0.25"]) else {
            panic!()
        };
        assert_eq!(delta, Number::Float(0.25));
    }

    #[test]
    fn decrby_reports_an_unnegatable_delta_as_overflow() {
        assert_eq!(
            rejected(&[b"DECRBY", b"k", b"-9223372036854775808"]),
            ErrorReply::OVERFLOW
        );
    }

    #[test]
    fn parses_the_rate_limiter_shape_of_increx() {
        let Command::IncrEx(op) = command(&[
            b"INCREX", b"hits", b"BYINT", b"1", b"UBOUND", b"100", b"EX", b"60", b"ENX",
        ]) else {
            panic!("expected an increx")
        };
        assert_eq!(op.key, b"hits");
        assert_eq!(op.delta, Number::Int(1));
        assert_eq!(op.upper, Some(Number::Int(100)));
        assert!(!op.saturate);
        assert_eq!(op.expiry, Some(Expiry::After(60_000)));
        assert!(op.only_if_persistent);
    }

    #[test]
    fn increx_defaults_to_incrementing_by_one_and_keeping_the_ttl() {
        let Command::IncrEx(op) = command(&[b"INCREX", b"k"]) else {
            panic!()
        };
        assert_eq!(op.delta, Number::Int(1));
        assert_eq!(op.expiry, None);
        assert_eq!((op.lower, op.upper), (None, None));
    }

    #[test]
    fn increx_reads_its_bounds_in_the_mode_the_increment_chose() {
        // `LBOUND` arrives before the `BYFLOAT` that decides how to read it,
        // which is why the bounds are resolved after the whole option list.
        let Command::IncrEx(op) =
            command(&[b"INCREX", b"k", b"LBOUND", b"0.5", b"BYFLOAT", b"0.25"])
        else {
            panic!()
        };
        assert_eq!(op.lower, Some(Number::Float(0.5)));

        // In integer mode the same bound would not parse.
        assert_eq!(
            rejected(&[b"INCREX", b"k", b"LBOUND", b"0.5"]),
            ErrorReply::NOT_AN_INTEGER
        );
    }

    #[test]
    fn increx_rejects_contradictory_options() {
        // ENX has nothing to guard without a deadline, and cannot be squared
        // with removing one.
        assert_eq!(rejected(&[b"INCREX", b"k", b"ENX"]), ErrorReply::SYNTAX);
        assert_eq!(
            rejected(&[b"INCREX", b"k", b"PERSIST", b"ENX"]),
            ErrorReply::SYNTAX
        );
        assert_eq!(
            rejected(&[b"INCREX", b"k", b"EX", b"1", b"PERSIST"]),
            ErrorReply::SYNTAX
        );
        assert_eq!(
            rejected(&[b"INCREX", b"k", b"LBOUND", b"10", b"UBOUND", b"5"]),
            ErrorReply::Err("LBOUND must be less than or equal to UBOUND")
        );
    }

    #[test]
    fn hello_carries_the_requested_version() {
        assert!(matches!(
            command(&[b"HELLO"]),
            Command::Hello {
                version: None,
                auth: None
            }
        ));
        assert!(matches!(
            command(&[b"HELLO", b"3"]),
            Command::Hello {
                version: Some(3),
                auth: None
            }
        ));
    }

    /// The combined form is what a client library sends on checkout when it has
    /// a credential, and it is the reason `HELLO` is in the pre-auth set.
    #[test]
    fn hello_can_authenticate_and_negotiate_at_once() {
        let Command::Hello { version, auth } = command(&[b"HELLO", b"3", b"AUTH", b"u", b"p"])
        else {
            panic!("expected HELLO");
        };
        assert_eq!(version, Some(3));
        assert_eq!(
            auth,
            Some(Credential {
                name: Some(b"u"),
                secret: b"p"
            })
        );

        // Lower case, because option tokens are case-insensitive.
        assert!(matches!(
            command(&[b"HELLO", b"2", b"auth", b"u", b"p"]),
            Command::Hello { auth: Some(_), .. }
        ));
    }

    #[test]
    fn setname_is_still_refused() {
        assert_eq!(
            rejected(&[b"HELLO", b"3", b"SETNAME", b"app"]),
            ErrorReply::Err(
                "HELLO option SETNAME is not supported, and AUTH takes a username and a password"
            )
        );
    }

    /// Redis's pre-6 one-argument form addresses the `default` identity; the
    /// two-argument form names one.
    #[test]
    fn auth_takes_one_or_two_arguments() {
        assert!(matches!(
            command(&[b"AUTH", b"secret"]),
            Command::Auth(Credential {
                name: None,
                secret: b"secret"
            })
        ));
        assert!(matches!(
            command(&[b"AUTH", b"billing", b"secret"]),
            Command::Auth(Credential {
                name: Some(b"billing"),
                secret: b"secret"
            })
        ));
        // Redis's own asymmetry, verified against 7.4.10 by the differential
        // suite: no arguments is an arity error, too many is a syntax error.
        assert_eq!(rejected(&[b"AUTH"]), ErrorReply::WrongArity("auth"));
        assert_eq!(rejected(&[b"AUTH", b"a", b"b", b"c"]), ErrorReply::SYNTAX);
    }

    #[test]
    fn arity_is_checked_per_command() {
        for (args, name) in [
            (&[b"GET".as_slice()][..], "get"),
            (&[b"GET", b"a", b"b"], "get"),
            (&[b"SET", b"k"], "set"),
            (&[b"APPEND", b"k"], "append"),
            (&[b"TTL"], "ttl"),
            (&[b"DEL"], "del"),
            (&[b"EXPIRE", b"k"], "expire"),
        ] {
            assert_eq!(rejected(args), ErrorReply::WrongArity(name), "{args:?}");
        }
    }

    #[test]
    fn integers_follow_redis_rather_than_rust() {
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"-1"), Some(-1));
        assert_eq!(parse_int(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_int(b"-9223372036854775808"), Some(i64::MIN));

        // Every one of these is accepted by `str::parse` and rejected by Redis.
        for token in [
            &b""[..],
            b"007",
            b"+1",
            b" 1",
            b"1 ",
            b"-0",
            b"1.0",
            b"9223372036854775808",
            b"-9223372036854775809",
        ] {
            assert_eq!(parse_int(token), None, "{token:?}");
        }
    }

    #[test]
    fn floats_reject_nan_and_padding() {
        assert_eq!(parse_float(b"1.5"), Some(1.5));
        assert_eq!(parse_float(b"-3"), Some(-3.0));
        assert_eq!(parse_float(b"5.0e3"), Some(5000.0));
        assert_eq!(parse_float(b"inf"), Some(f64::INFINITY));

        for token in [&b""[..], b"nan", b" 1.5", b"1.5 ", b"abc"] {
            assert_eq!(parse_float(token), None, "{token:?}");
        }
    }
}
