//! RESP response rendering.
//!
//! Everything a client can be told goes through these writers, and the ones
//! that differ between the two dialects — [`null`], [`double`], [`hello`] —
//! take the connection's negotiated [`Version`]. That is the whole of the
//! RESP2/RESP3 split for this command set: requests are identical, and the
//! remaining reply types are byte-for-byte the same in both.

use std::fmt::Write as _;

use super::{ErrorReply, Version};

/// The version this server reports to `HELLO`.
///
/// It answers `server: redis` for the same reason the memcached adapter answers
/// with a real memcached version number: client libraries branch on it, and a
/// name they have never seen sends some of them down an error path. The suffix
/// is what tells a human what they are actually talking to.
pub const VERSION: &str = "7.4.0-vash";

pub fn simple(out: &mut Vec<u8>, text: &str) {
    out.push(b'+');
    out.extend_from_slice(text.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn ok(out: &mut Vec<u8>) {
    out.extend_from_slice(b"+OK\r\n");
}

/// `-<code> <message>`. `code` is the part clients pattern-match on, so it is
/// separated from the prose rather than baked into it.
pub fn error(out: &mut Vec<u8>, code: &str, message: &str) {
    out.push(b'-');
    out.extend_from_slice(code.as_bytes());
    out.push(b' ');
    out.extend_from_slice(message.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Renders a parser rejection.
///
/// The wording is Redis's own, verbatim, because clients match on some of it —
/// `unknown command` in particular is how a library decides a server does not
/// support a feature and falls back.
pub fn error_reply(out: &mut Vec<u8>, reply: &ErrorReply<'_>) {
    match reply {
        ErrorReply::UnknownCommand(name) => {
            out.extend_from_slice(b"-ERR unknown command '");
            escape(out, name);
            out.extend_from_slice(b"'\r\n");
        }
        ErrorReply::WrongArity(name) => {
            error(
                out,
                "ERR",
                &format!("wrong number of arguments for '{name}' command"),
            );
        }
        ErrorReply::InvalidExpire(name) => {
            error(
                out,
                "ERR",
                &format!("invalid expire time in '{name}' command"),
            );
        }
        ErrorReply::Err(message) => error(out, "ERR", message),
        ErrorReply::Coded(code, message) => error(out, code, message),
    }
}

/// A protocol error, which is also the last thing written on that connection.
pub fn protocol_error(out: &mut Vec<u8>, detail: &str) {
    error(out, "ERR", &format!("Protocol error: {detail}"));
}

pub fn integer(out: &mut Vec<u8>, value: i64) {
    out.push(b':');
    itoa(out, value);
    out.extend_from_slice(b"\r\n");
}

pub fn bulk(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(b'$');
    itoa(out, bytes.len() as i64);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
}

/// The absent value.
///
/// RESP2 spells it as a bulk string of length −1 and RESP3 has a type of its
/// own. This is the divergence that actually matters for this command set —
/// every miss goes through it.
pub fn null(out: &mut Vec<u8>, version: Version) {
    out.extend_from_slice(match version {
        Version::Resp2 => b"$-1\r\n".as_slice(),
        Version::Resp3 => b"_\r\n",
    });
}

pub fn array(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    itoa(out, len as i64);
    out.extend_from_slice(b"\r\n");
}

/// A floating-point value.
///
/// RESP3 has a double type; RESP2 has to send the decimal text as a bulk
/// string. `INCRBYFLOAT` uses the bulk form in *both*, which is why it calls
/// [`bulk`] directly rather than coming through here — only `INCREX` in
/// `BYFLOAT` mode takes the version-dependent path.
pub fn double(out: &mut Vec<u8>, value: f64, version: Version) {
    let text = format_float(value);
    match version {
        Version::Resp2 => bulk(out, text.as_bytes()),
        Version::Resp3 => {
            out.push(b',');
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// The `HELLO` reply: a map in RESP3, the same pairs flattened into an array in
/// RESP2.
pub fn hello(out: &mut Vec<u8>, version: Version) {
    const FIELDS: usize = 7;
    match version {
        Version::Resp2 => array(out, FIELDS * 2),
        Version::Resp3 => {
            out.push(b'%');
            itoa(out, FIELDS as i64);
            out.extend_from_slice(b"\r\n");
        }
    }

    bulk(out, b"server");
    bulk(out, b"redis");
    bulk(out, b"version");
    bulk(out, VERSION.as_bytes());
    bulk(out, b"proto");
    integer(
        out,
        match version {
            Version::Resp2 => 2,
            Version::Resp3 => 3,
        },
    );
    bulk(out, b"id");
    // Connections are not registered anywhere, so there is no id to report.
    // Answering zero is honest; inventing a counter would imply a `CLIENT`
    // command that does not exist here.
    integer(out, 0);
    bulk(out, b"mode");
    bulk(out, b"standalone");
    bulk(out, b"role");
    bulk(out, b"master");
    bulk(out, b"modules");
    array(out, 0);
}

/// A RESP3 verbatim string: text that carries its own format hint.
///
/// What real Redis answers `INFO` and `LATENCY DOCTOR` with, so that a client
/// knows the payload is prose to be displayed rather than a value to be parsed.
/// RESP2 has no such type and takes the bulk string, exactly as [`double`]
/// falls back — the length includes the three-character hint and its colon.
pub fn verbatim(out: &mut Vec<u8>, text: &str, version: Version) {
    match version {
        Version::Resp2 => bulk(out, text.as_bytes()),
        Version::Resp3 => {
            out.push(b'=');
            itoa(out, (text.len() + 4) as i64);
            out.extend_from_slice(b"\r\ntxt:");
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

// ---- INFO ---------------------------------------------------------------

/// One `INFO` section.
///
/// The set is Redis's, minus the sections whose entire content would be
/// unmeasured here — `commandstats`, `latencystats`, `cpu`, `errorstats` — plus
/// [`Section::Vash`] for the counters that have no Redis name at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    Server,
    Clients,
    Memory,
    Persistence,
    Stats,
    Replication,
    Cluster,
    Keyspace,
    /// This server's own counters, under their own names. Not in the default
    /// set, exactly as Redis keeps its optional sections out of it.
    Vash,
}

impl Section {
    /// Every section, in the order `INFO` prints them.
    pub const ALL: [Section; 9] = [
        Self::Server,
        Self::Clients,
        Self::Memory,
        Self::Persistence,
        Self::Stats,
        Self::Replication,
        Self::Cluster,
        Self::Keyspace,
        Self::Vash,
    ];

    /// The `# Header` this section prints under, and the name a client selects
    /// it by — lower-cased.
    pub const fn title(self) -> &'static str {
        match self {
            Self::Server => "Server",
            Self::Clients => "Clients",
            Self::Memory => "Memory",
            Self::Persistence => "Persistence",
            Self::Stats => "Stats",
            Self::Replication => "Replication",
            Self::Cluster => "Cluster",
            Self::Keyspace => "Keyspace",
            Self::Vash => "Vash",
        }
    }

    fn matches(self, name: &[u8]) -> bool {
        name.eq_ignore_ascii_case(self.title().as_bytes())
    }
}

/// Which sections one `INFO` asked for.
///
/// A bitmask rather than a list: the set is fixed and small, a client may name
/// the same section twice, and `Copy` keeps it out of the borrowed-argument
/// lifetime that every other part of a parsed command lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sections(u16);

impl Sections {
    /// Nothing selected. What an unrecognised section name leaves behind, and
    /// Redis answers that with an empty string rather than an error.
    pub const NONE: Self = Self(0);

    /// What a bare `INFO` prints: everything except [`Section::Vash`], which
    /// only `all` and `everything` reach — the same shape as Redis keeping
    /// `commandstats` out of the default.
    pub const DEFAULT: Self = Self(0b1111_1111);

    /// `INFO all` / `INFO everything`.
    pub const ALL: Self = Self(0b1_1111_1111);

    /// The section a name selects, or `None` if it is not one.
    pub fn named(name: &[u8]) -> Option<Section> {
        Section::ALL.into_iter().find(|s| s.matches(name))
    }

    pub fn with(self, section: Section) -> Self {
        Self(self.0 | 1 << section as u16)
    }

    pub fn contains(self, section: Section) -> bool {
        self.0 & 1 << section as u16 != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Where an `INFO` field's value comes from.
enum Source {
    /// A counter of this name in the `stats` list. A field whose counter is
    /// absent — the store could not be queried — is **omitted**, never zeroed.
    Stat(&'static str),
    /// A constant this build knows and no counter carries.
    Literal(&'static str),
    /// Derived from one or more counters. See [`computed`].
    Computed(Computed),
}

enum Computed {
    Os,
    UptimeInDays,
    UsedMemoryHuman,
    Keyspace,
}

/// The `INFO` field table: Redis's name on the left, where it comes from on the
/// right.
///
/// Everything a client can read out of `INFO` is here, so what this server
/// claims about itself is one screen of text rather than a rendering function to
/// be read. Fields Redis has and this server cannot measure are absent, not
/// zeroed — `used_memory_rss`, `mem_fragmentation_ratio`,
/// `instantaneous_ops_per_sec`, `latest_fork_usec` and the rest.
const FIELDS: &[(Section, &str, Source)] = &[
    (Section::Server, "redis_version", Source::Literal(VERSION)),
    (Section::Server, "redis_mode", Source::Literal("standalone")),
    (Section::Server, "os", Source::Computed(Computed::Os)),
    (Section::Server, "arch_bits", Source::Stat("pointer_size")),
    (Section::Server, "process_id", Source::Stat("pid")),
    (Section::Server, "uptime_in_seconds", Source::Stat("uptime")),
    (
        Section::Server,
        "uptime_in_days",
        Source::Computed(Computed::UptimeInDays),
    ),
    // What this actually is. `redis_version` above is a compatibility claim —
    // client libraries gate features on it — and this is the honest neighbour.
    (
        Section::Server,
        "vash_version",
        Source::Literal(env!("CARGO_PKG_VERSION")),
    ),
    (
        Section::Clients,
        "connected_clients",
        Source::Stat("curr_connections"),
    ),
    (
        Section::Clients,
        "maxclients",
        Source::Stat("max_connections"),
    ),
    // Measured, and always zero: no command here can block a client. An honest
    // constant rather than an unmeasured one.
    (Section::Clients, "blocked_clients", Source::Literal("0")),
    (Section::Memory, "used_memory", Source::Stat("bytes")),
    (
        Section::Memory,
        "used_memory_human",
        Source::Computed(Computed::UsedMemoryHuman),
    ),
    (Section::Memory, "maxmemory", Source::Stat("limit_maxbytes")),
    // The closest true statement in Redis's vocabulary for plan §6's "expired
    // first, then soonest-to-expire". An approximation, and in the divergences
    // table as one.
    (
        Section::Memory,
        "maxmemory_policy",
        Source::Literal("volatile-ttl"),
    ),
    // Nothing here loads, forks or rewrites, so all three are honestly zero —
    // and a client that checks `loading` before issuing traffic needs to see it.
    (Section::Persistence, "loading", Source::Literal("0")),
    (
        Section::Persistence,
        "rdb_bgsave_in_progress",
        Source::Literal("0"),
    ),
    (Section::Persistence, "aof_enabled", Source::Literal("0")),
    (
        Section::Stats,
        "total_connections_received",
        Source::Stat("total_connections"),
    ),
    (
        Section::Stats,
        "total_commands_processed",
        Source::Stat("vash_commands"),
    ),
    (
        Section::Stats,
        "rejected_connections",
        Source::Stat("rejected_connections"),
    ),
    (Section::Stats, "keyspace_hits", Source::Stat("get_hits")),
    (
        Section::Stats,
        "keyspace_misses",
        Source::Stat("get_misses"),
    ),
    (
        Section::Stats,
        "expired_keys",
        Source::Stat("vash_reclaimed"),
    ),
    (Section::Stats, "evicted_keys", Source::Stat("evictions")),
    (
        Section::Stats,
        "total_reads_processed",
        Source::Stat("vash_reads"),
    ),
    (
        Section::Stats,
        "total_writes_processed",
        Source::Stat("vash_writes"),
    ),
    // There is no replication, so this is true rather than aspirational.
    // Sentinel-aware clients and health checks parse `role`.
    (Section::Replication, "role", Source::Literal("master")),
    (
        Section::Replication,
        "connected_slaves",
        Source::Literal("0"),
    ),
    // **Load-bearing.** Client libraries read this to decide whether to speak
    // Redis Cluster — `CLUSTER SLOTS`, `MOVED`/`ASK` redirection, hash-slot
    // routing — none of which exists here. vash's clustering is tag
    // invalidation between shared-nothing nodes and is not the same thing under
    // the same name. A `1` here breaks every cluster-aware client on connect.
    (Section::Cluster, "cluster_enabled", Source::Literal("0")),
    (
        Section::Keyspace,
        "db0",
        Source::Computed(Computed::Keyspace),
    ),
];

/// The counter names [`FIELDS`] reads from.
///
/// Exported so `vash-server` can assert that every one of them is a counter
/// `stats::collect` actually emits. The table and the counters live in
/// different crates, so without that check a rename on either side would drop a
/// field from `INFO` silently — which is the failure mode this whole module is
/// written to avoid.
pub fn info_sourced_names() -> impl Iterator<Item = &'static str> {
    FIELDS.iter().filter_map(|(_, _, source)| match source {
        Source::Stat(name) => Some(*name),
        _ => None,
    })
}

/// Renders `INFO`.
///
/// A pure function of the counter list: it opens nothing and reads no state,
/// which is what lets it live beside the other encoders rather than in the
/// server.
pub fn info(out: &mut Vec<u8>, sections: Sections, stats: &[(String, String)], version: Version) {
    let mut text = String::with_capacity(1024);
    for section in Section::ALL {
        if !sections.contains(section) {
            continue;
        }

        // Built into a scratch buffer so a section whose every field is
        // unavailable prints no header either, rather than a title with nothing
        // under it.
        let mut body = String::new();
        if section == Section::Vash {
            // Stated, not counted: what a client wants to know here is whether
            // this dialect answers `SETTAGS`/`MSETTAGS`/`DELBYTAG` at all, and
            // sending one to find out is a write. It lives here rather than in
            // `stats::collect` because it is a fact about *this* dialect, and
            // the memcached `stats` payload should not carry it.
            let _ = writeln!(body, "vash_resp_tags:1\r");
            // The rest are rendered by prefix rather than from a table: they are
            // already named for this server, and a second list of them would be
            // a second thing to keep in step with `stats::collect`.
            for (name, value) in stats.iter().filter(|(name, _)| name.starts_with("vash_")) {
                let _ = writeln!(body, "{name}:{value}\r");
            }
        } else {
            for (_, name, source) in FIELDS.iter().filter(|(s, _, _)| *s == section) {
                let value = match source {
                    Source::Stat(stat) => find(stats, stat).map(str::to_owned),
                    Source::Literal(literal) => Some((*literal).to_owned()),
                    Source::Computed(computed) => compute(computed, stats),
                };
                if let Some(value) = value {
                    let _ = writeln!(body, "{name}:{value}\r");
                }
            }
        }

        if !body.is_empty() {
            // A blank line **before** each section after the first, which is
            // Redis's own framing — not one after each, which would leave a
            // trailing blank the real thing does not send.
            if !text.is_empty() {
                text.push_str("\r\n");
            }
            let _ = writeln!(text, "# {}\r", section.title());
            text.push_str(&body);
        }
    }

    verbatim(out, &text, version);
}

/// A counter's value, or `None` when it is not in the list.
fn find<'a>(stats: &'a [(String, String)], name: &str) -> Option<&'a str> {
    stats
        .iter()
        .find(|(known, _)| known == name)
        .map(|(_, value)| value.as_str())
}

/// The handful of fields that are not a counter copied across.
fn compute(what: &Computed, stats: &[(String, String)]) -> Option<String> {
    let find = |name: &str| find(stats, name);
    Some(match what {
        // Redis reports `Linux 5.15.0 x86_64`. There is no portable way to read
        // a kernel version here, so the two parts that are known are reported
        // and the one that is not is left out.
        Computed::Os => format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
        Computed::UptimeInDays => (find("uptime")?.parse::<u64>().ok()? / 86_400).to_string(),
        Computed::UsedMemoryHuman => human_bytes(find("bytes")?.parse::<u64>().ok()?),
        Computed::Keyspace => {
            let keys: u64 = find("curr_items")?.parse().ok()?;
            // Redis omits the line for an empty database rather than printing a
            // zero, and `redis-py`'s `parse_info` expects that.
            if keys == 0 {
                return None;
            }
            // **`expires` is absent, and `vash_expiry_entries` is not it.**
            // Redis's `expires` counts keys carrying a TTL; that counter is
            // rows in the expiry index, which has one per record whether or not
            // it expires — a store with two keys and one deadline reports two.
            // Nothing here measures Redis's quantity, so nothing here claims to.
            //
            // `avg_ttl` stays because zero is Redis's own value for "not
            // computed", which is the one place a zero is honest: averaging it
            // would mean walking the index on every `INFO`.
            format!("keys={keys},avg_ttl=0")
        }
    })
}

/// Redis's `bytesToHuman`, to the digit.
fn human_bytes(bytes: u64) -> String {
    const K: f64 = 1024.0;
    let n = bytes as f64;
    if bytes < 1024 {
        format!("{bytes}B")
    } else if n < K * K {
        format!("{:.2}K", n / K)
    } else if n < K * K * K {
        format!("{:.2}M", n / (K * K))
    } else {
        format!("{:.2}G", n / (K * K * K))
    }
}

/// Renders a float the way Redis does, re-exported from the domain crate.
///
/// It moved for the same reason the numeric parsers did: this renders the reply,
/// and `vash_core::arith` renders the *stored* value that a later `GET` returns.
/// A client that increments a float and then reads it must see one number, not
/// two spellings of it.
pub use vash_core::format_float;

/// Escapes a command name for the `unknown command` line, which is echoed back
/// to the client and must not be able to inject a CRLF into the reply stream.
fn escape(out: &mut Vec<u8>, name: &[u8]) {
    // Bounded as well as escaped: the name came off the wire and may be long.
    for byte in name.iter().take(128) {
        match byte {
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\'' | b'\\' => {
                out.push(b'\\');
                out.push(*byte);
            }
            0x20..=0x7e => out.push(*byte),
            other => out.extend_from_slice(format!("\\x{other:02x}").as_bytes()),
        }
    }
}

/// Appends a decimal integer. Named for what it does; it used to be
/// `value.to_string()`, which allocated on every reply that carried a length.
fn itoa(out: &mut Vec<u8>, value: i64) {
    crate::digits::push_i64(out, value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(write: impl FnOnce(&mut Vec<u8>)) -> String {
        let mut out = Vec::new();
        write(&mut out);
        String::from_utf8(out).expect("replies are valid UTF-8 in these tests")
    }

    #[test]
    fn nulls_differ_between_the_dialects() {
        assert_eq!(rendered(|out| null(out, Version::Resp2)), "$-1\r\n");
        assert_eq!(rendered(|out| null(out, Version::Resp3)), "_\r\n");
    }

    #[test]
    fn doubles_differ_between_the_dialects() {
        assert_eq!(
            rendered(|out| double(out, 1.75, Version::Resp2)),
            "$4\r\n1.75\r\n"
        );
        assert_eq!(
            rendered(|out| double(out, 1.75, Version::Resp3)),
            ",1.75\r\n"
        );
    }

    #[test]
    fn bulk_strings_are_binary_safe() {
        assert_eq!(
            rendered(|out| bulk(out, b"a\r\nb")),
            "$4\r\na\r\nb\r\n",
            "the payload is length-delimited, so a CRLF inside it is data"
        );
        assert_eq!(rendered(|out| bulk(out, b"")), "$0\r\n\r\n");
    }

    #[test]
    fn floats_drop_a_trailing_zero_the_way_redis_does() {
        assert_eq!(format_float(3.0), "3");
        assert_eq!(format_float(-0.5), "-0.5");
        assert_eq!(format_float(1.75), "1.75");
        assert_eq!(format_float(5000.0), "5000");
        // Never exponent notation, whatever the magnitude.
        assert_eq!(format_float(1e20), "100000000000000000000");
    }

    #[test]
    fn an_unknown_command_name_cannot_inject_a_reply() {
        // The name is echoed straight back, so a CRLF in it would end the error
        // line early and let the client read the rest as another reply.
        let out = rendered(|out| error_reply(out, &ErrorReply::UnknownCommand(b"a\r\n+OK")));
        assert_eq!(out, "-ERR unknown command 'a\\r\\n+OK'\r\n");
        assert_eq!(out.matches("\r\n").count(), 1);
    }

    #[test]
    fn hello_is_a_map_in_resp3_and_a_flat_array_in_resp2() {
        assert!(rendered(|out| hello(out, Version::Resp3)).starts_with("%7\r\n"));
        assert!(rendered(|out| hello(out, Version::Resp2)).starts_with("*14\r\n"));
    }

    fn counters() -> Vec<(String, String)> {
        [
            ("pid", "42"),
            ("pointer_size", "64"),
            ("uptime", "172800"),
            ("max_connections", "10000"),
            ("curr_connections", "3"),
            ("total_connections", "9"),
            ("rejected_connections", "0"),
            ("get_hits", "7"),
            ("get_misses", "2"),
            ("vash_commands", "18"),
            ("vash_reads", "9"),
            ("vash_writes", "9"),
            ("curr_items", "5"),
            ("bytes", "1048576"),
            ("limit_maxbytes", "17179869184"),
            ("evictions", "0"),
            ("vash_reclaimed", "1"),
            ("vash_shards", "4"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
    }

    fn rendered_info(sections: Sections, stats: &[(String, String)]) -> String {
        let mut out = Vec::new();
        info(&mut out, sections, stats, Version::Resp2);
        let text = String::from_utf8(out).expect("info is ascii");
        // Strip the bulk header and its trailing CRLF.
        let body = text.split_once("\r\n").expect("a bulk header").1;
        body[..body.len() - 2].to_string()
    }

    /// Redis puts a blank line **before** each section after the first, not
    /// after each one — so a reply has no trailing blank. Verified against
    /// redis 7.4.
    #[test]
    fn sections_are_separated_the_way_redis_separates_them() {
        let body = rendered_info(Sections::DEFAULT, &counters());
        assert!(body.starts_with("# Server\r\n"));
        assert!(body.contains("\r\n\r\n# Clients\r\n"));
        assert!(
            !body.ends_with("\r\n\r\n"),
            "no trailing blank line: {body:?}"
        );
    }

    #[test]
    fn a_field_whose_counter_is_missing_is_omitted_rather_than_zeroed() {
        // What a store that could not be queried leaves behind.
        let sparse: Vec<(String, String)> = counters()
            .into_iter()
            .filter(|(name, _)| name != "bytes" && name != "curr_items")
            .collect();
        let body = rendered_info(Sections::ALL, &sparse);

        assert!(!body.contains("used_memory:"), "{body:?}");
        assert!(!body.contains("used_memory_human:"), "{body:?}");
        assert!(
            !body.contains("db0:"),
            "an unknown keyspace is not an empty one"
        );
        // The section header goes too when nothing under it survived.
        assert!(!body.contains("# Keyspace"), "{body:?}");
        // Neighbouring fields are unaffected.
        assert!(body.contains("maxmemory:17179869184\r\n"));
    }

    /// The three fields a client library actually branches on.
    #[test]
    fn the_fields_clients_branch_on_are_present_and_honest() {
        let body = rendered_info(Sections::DEFAULT, &counters());
        // A `1` here sends a client into Redis Cluster's protocol, which this
        // server does not speak.
        assert!(body.contains("cluster_enabled:0\r\n"));
        assert!(body.contains("role:master\r\n"));
        assert!(body.contains("loading:0\r\n"));
        assert!(body.contains(&format!("redis_version:{VERSION}\r\n")));
    }

    #[test]
    fn computed_fields_follow_redis_arithmetic() {
        let body = rendered_info(Sections::ALL, &counters());
        assert!(body.contains("uptime_in_seconds:172800\r\n"));
        assert!(body.contains("uptime_in_days:2\r\n"));
        assert!(body.contains("used_memory_human:1.00M\r\n"));
        // `expires` is deliberately absent: the nearest counter here is rows in
        // the expiry index, which is a different quantity from keys with a TTL.
        assert!(body.contains("db0:keys=5,avg_ttl=0\r\n"));
    }

    #[test]
    fn human_bytes_matches_redis_bytes_to_human() {
        assert_eq!(human_bytes(0), "0B");
        assert_eq!(human_bytes(1023), "1023B");
        assert_eq!(human_bytes(1024), "1.00K");
        assert_eq!(human_bytes(1024 * 1024), "1.00M");
        assert_eq!(human_bytes(1536 * 1024), "1.50M");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.00G");
    }

    #[test]
    fn the_vash_section_is_out_of_the_default_and_in_all() {
        assert!(!rendered_info(Sections::DEFAULT, &counters()).contains("vash_shards"));

        let all = rendered_info(Sections::ALL, &counters());
        assert!(all.contains("# Vash\r\n"));
        assert!(all.contains("vash_shards:4\r\n"));
        // How a client learns this dialect has tags without writing to find out.
        assert!(all.contains("vash_resp_tags:1\r\n"));
        assert!(!rendered_info(Sections::DEFAULT, &counters()).contains("vash_resp_tags"));
    }

    #[test]
    fn sections_are_selected_by_name_case_insensitively() {
        assert_eq!(Sections::named(b"server"), Some(Section::Server));
        assert_eq!(Sections::named(b"KEYSPACE"), Some(Section::Keyspace));
        assert_eq!(Sections::named(b"Vash"), Some(Section::Vash));
        assert_eq!(Sections::named(b"commandstats"), None);

        let one = Sections::NONE.with(Section::Clients);
        assert!(one.contains(Section::Clients) && !one.contains(Section::Server));
        // Naming the same section twice is not an error, which is why this is a
        // bitmask rather than a list.
        assert_eq!(one.with(Section::Clients), one);
    }

    /// An unrecognised section renders as an empty reply, not an error — which
    /// is Redis's behaviour, and lets a client probe for a section it may not
    /// have.
    #[test]
    fn an_empty_selection_renders_as_an_empty_reply() {
        assert_eq!(
            rendered(|out| info(out, Sections::NONE, &counters(), Version::Resp2)),
            "$0\r\n\r\n"
        );
        assert_eq!(
            rendered(|out| info(out, Sections::NONE, &counters(), Version::Resp3)),
            "=4\r\ntxt:\r\n"
        );
    }

    #[test]
    fn verbatim_strings_carry_their_hint_only_in_resp3() {
        assert_eq!(
            rendered(|out| verbatim(out, "hello", Version::Resp3)),
            "=9\r\ntxt:hello\r\n",
            "the declared length covers the hint"
        );
        assert_eq!(
            rendered(|out| verbatim(out, "hello", Version::Resp2)),
            "$5\r\nhello\r\n"
        );
    }

    #[test]
    fn errors_keep_their_code_separate_from_their_prose() {
        assert_eq!(
            rendered(|out| error_reply(out, &ErrorReply::Coded("NOPROTO", "unsupported"))),
            "-NOPROTO unsupported\r\n"
        );
        assert_eq!(
            rendered(|out| error_reply(out, &ErrorReply::WrongArity("get"))),
            "-ERR wrong number of arguments for 'get' command\r\n"
        );
    }
}
