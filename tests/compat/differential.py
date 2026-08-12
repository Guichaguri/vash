#!/usr/bin/env python3
"""Byte-for-byte differential against a real memcached or a real Redis.

Sends identical command sequences to two servers and compares the raw
responses. A client library smooths over a lot â€” exact error strings, edge-case
verdicts, response ordering â€” and this is where those divergences show up.

    python differential.py --reference 127.0.0.1:11211 --subject 127.0.0.1:11311
    python differential.py --dialect redis --reference ... --subject ...
    python differential.py --dialect memcached --suite auth --reference ...

`docker_differential.py` is the usual entry point: it starts pinned reference
images and runs every dialect and suite against them. This script takes
addresses so it can also be pointed at servers someone else started.

The comparison machinery is dialect-agnostic because it works on raw bytes: a
probe is a list of things to send and the reply is whatever comes back before
the server goes quiet. Only the probe lists and the normalisers differ.

Values that legitimately differ between two servers (CAS tokens, version
strings, connection ids) are normalised before comparison; everything else must
match exactly.
"""

import argparse
import re
import socket
import sys
import time
import uuid

# Kept in step with servers.py, which configures the reference servers with it.
AUTH_USER = "default"
AUTH_SECRET = "s3cr3t-token-goes-here"


def resp(*args):
    """A RESP command, as an argument list.

    Deliberately *not* encoded here. RESP is length-prefixed, so the key-prefix
    substitution has to happen before the `$<len>` headers are computed — encode
    first and every probe would send a frame whose declared lengths disagree
    with its payload, which both servers answer with a protocol error. The
    encoding happens in [`render`], after substitution.
    """
    return [arg.encode() if isinstance(arg, str) else arg for arg in args]


def encode_resp(args):
    out = b"*%d\r\n" % len(args)
    for arg in args:
        out += b"$%d\r\n%s\r\n" % (len(arg), arg)
    return out


def render(entry, prefix):
    """Turns one probe entry into bytes on the wire, substituting the prefix.

    A `bytes` entry is sent verbatim — that is the memcached dialect, where the
    text is the wire format. A list is a RESP argument list, substituted
    per argument and only then encoded.
    """
    if isinstance(entry, bytes):
        return entry.replace(b"~", prefix)
    return encode_resp([arg.replace(b"~", prefix) for arg in entry])


def memcached_auth(user=AUTH_USER, secret=AUTH_SECRET):
    """Upstream's ASCII authentication: a `set` carrying `<user> <pass>`."""
    block = f"{user} {secret}".encode()
    return b"set %s 0 0 %d\r\n%s\r\n" % (user.encode(), len(block), block)


# Probe: (name, commands, terminator that ends the whole exchange)
MEMCACHED_PROBES = [
    ("basic set/get", [b"set ~1 5 0 3\r\nabc\r\n", b"get ~1\r\n"], b"END\r\n"),
    ("get miss", [b"get ~nothing-here\r\n"], b"END\r\n"),
    ("empty value", [b"set ~2 0 0 0\r\n\r\n", b"get ~2\r\n"], b"END\r\n"),
    (
        "multi-get with a miss in the middle",
        [b"set ~3 0 0 1\r\nx\r\n", b"get ~3 absent ~3\r\n"],
        b"END\r\n",
    ),
    ("add on absent then present", [b"add ~4 0 0 1\r\na\r\n", b"add ~4 0 0 1\r\nb\r\n"], b"\r\n"),
    ("replace on absent", [b"replace ~nope 0 0 1\r\nx\r\n"], b"\r\n"),
    ("append on absent", [b"append ~nope 0 0 1\r\nx\r\n"], b"\r\n"),
    ("prepend on absent", [b"prepend ~nope 0 0 1\r\nx\r\n"], b"\r\n"),
    (
        "append keeps flags",
        [b"set ~5 7 0 1\r\na\r\n", b"append ~5 0 0 1\r\nb\r\n", b"get ~5\r\n"],
        b"END\r\n",
    ),
    ("cas on absent key", [b"cas ~nope 0 0 1 1\r\nx\r\n"], b"\r\n"),
    (
        "cas with a stale token",
        [b"set ~6 0 0 1\r\na\r\n", b"cas ~6 0 0 1 1\r\nb\r\n"],
        b"\r\n",
    ),
    ("delete miss", [b"delete ~not-there\r\n"], b"\r\n"),
    ("touch miss", [b"touch ~not-there 10\r\n"], b"\r\n"),
    ("gat miss", [b"gat 10 ~not-there\r\n"], b"END\r\n"),
    ("incr miss", [b"incr ~not-there 1\r\n"], b"\r\n"),
    (
        "incr on a non-numeric value",
        [b"set ~7 0 0 3\r\nabc\r\n", b"incr ~7 1\r\n"],
        b"\r\n",
    ),
    (
        "decr below zero clamps",
        [b"set ~8 0 0 1\r\n5\r\n", b"decr ~8 100\r\n"],
        b"\r\n",
    ),
    (
        "incr wraps at 64 bits",
        [b"set ~9 0 0 20\r\n18446744073709551615\r\n", b"incr ~9 1\r\n"],
        b"\r\n",
    ),
    ("incr with a non-numeric delta", [b"incr ~9 abc\r\n"], b"\r\n"),
    ("unknown command", [b"frobnicate\r\n"], b"\r\n"),
    ("bad data chunk", [b"set ~10 0 0 3\r\nhello\r\n"], b"\r\n"),
    ("missing key on get", [b"get\r\n"], b"\r\n"),
    ("key too long", [b"get " + b"k" * 251 + b"\r\n"], b"\r\n"),
    ("negative exptime is already expired", [b"set ~11 0 -1 1\r\nx\r\n", b"get ~11\r\n"], b"END\r\n"),
    (
        "value containing crlf",
        [b"set ~12 0 0 7\r\na\r\nb\r\nc\r\n", b"get ~12\r\n"],
        b"END\r\n",
    ),
    ("bad command line format", [b"set ~13 notanumber 0 1\r\nx\r\n"], b"\r\n"),
    # ---- meta ----
    ("meta no-op", [b"mn\r\n"], b"MN\r\n"),
    ("meta get miss", [b"mg ~absent-key v\r\n"], b"\r\n"),
    ("meta set then get", [b"ms ~14 3 F9\r\nabc\r\n", b"mg ~14 v f\r\n"], b"\r\n"),
    ("meta get without v", [b"mg ~14\r\n"], b"\r\n"),
    ("meta add mode on present", [b"ms ~14 1 ME\r\nz\r\n"], b"\r\n"),
    ("meta replace on absent", [b"ms ~absent-key 1 MR\r\nz\r\n"], b"\r\n"),
    ("meta append", [b"ms ~14 1 MA\r\nZ\r\n", b"mg ~14 v\r\n"], b"\r\n"),
    ("meta delete then miss", [b"md ~14\r\n", b"md ~14\r\n"], b"\r\n"),
    ("meta unknown flag", [b"mg ~14 Z\r\n"], b"\r\n"),
    ("meta opaque echo", [b"ms ~15 1 Oabc\r\nx\r\n"], b"\r\n"),
    ("meta key echo", [b"mg ~15 v k\r\n"], b"\r\n"),
    (
        "meta arithmetic",
        [b"ms ~16 2 \r\n10\r\n", b"ma ~16\r\n", b"ma ~16 MD D3 v\r\n"],
        b"\r\n",
    ),
]

# Run against a memcached started with `-Y authfile` and a vash started with
# `--require-auth`. Every probe here is on a *fresh* connection, so "before
# authenticating" means what it says.
MEMCACHED_AUTH_PROBES = [
    ("get before auth", [b"get ~1\r\n"], b"\r\n"),
    ("set before auth", [b"set ~1 0 0 1\r\nx\r\n"], b"\r\n"),
    ("version before auth", [b"version\r\n"], b"\r\n"),
    ("stats before auth", [b"stats\r\n"], b"\r\n"),
    ("delete before auth", [b"delete ~1\r\n"], b"\r\n"),
    ("incr before auth", [b"incr ~1 1\r\n"], b"\r\n"),
    ("flush_all before auth", [b"flush_all\r\n"], b"\r\n"),
    ("unknown command before auth", [b"frobnicate\r\n"], b"\r\n"),
    ("meta get before auth", [b"mg ~1 v\r\n"], b"\r\n"),
    ("meta no-op before auth", [b"mn\r\n"], b"\r\n"),
    ("meta set before auth", [b"ms ~1 1\r\nx\r\n"], b"\r\n"),
    ("meta delete before auth", [b"md ~1\r\n"], b"\r\n"),
    ("meta arithmetic before auth", [b"ma ~1\r\n"], b"\r\n"),
    ("meta debug before auth", [b"me ~1\r\n"], b"\r\n"),
    # Starts with a lowercase letter so first-byte detection still routes it to
    # the memcached dialect — `\x01` is VCP's `HELLO` opcode, and a probe
    # opening with one would be testing protocol detection, not authentication.
    ("garbage before auth", [b"zz\x01\x02 oops\r\n"], b"\r\n"),
    ("quit before auth", [b"quit\r\n"], b"\r\n"),
    ("authenticate", [memcached_auth()], b"\r\n"),
    ("authenticate then get", [memcached_auth(), b"get ~1\r\n"], b"END\r\n"),
    ("authenticate then set and get", [
        memcached_auth(),
        b"set ~2 0 0 3\r\nabc\r\n",
        b"get ~2\r\n",
    ], b"END\r\n"),
    ("authenticate twice", [memcached_auth(), memcached_auth()], b"\r\n"),
    ("wrong password", [memcached_auth(secret="wrong-secret-here")], b"\r\n"),
    ("unknown user", [memcached_auth(user="nobody", secret="whatever-here")], b"\r\n"),
    # The block names a different user than the key does. Upstream reads the
    # block, so this is an ordinary failure rather than a framing error.
    (
        "block names a different user",
        [b"set %s 0 0 30\r\nsomeone %s\r\n" % (AUTH_USER.encode(), AUTH_SECRET.encode())],
        b"\r\n",
    ),
    # No space in the block: not a credential at all.
    (
        "block is not user and password",
        [b"set %s 0 0 22\r\n%s\r\n" % (AUTH_USER.encode(), AUTH_SECRET.encode())],
        b"\r\n",
    ),
    ("empty block", [b"set %s 0 0 0\r\n\r\n" % AUTH_USER.encode()], b"\r\n"),
    # A "wrong password, then a command" probe used to live here and was
    # removed: upstream answers it differently depending on whether the two
    # commands land in one read, so it matched on two runs in five and diverged
    # on the other three. A differential can only compare behaviour that is the
    # same every time; the property it was after — a failed attempt leaves the
    # connection unauthenticated — is covered deterministically by
    # `a_failed_memcached_attempt_does_not_authenticate` in tests/auth.rs.
    #
    # A refused storage command still has to consume its data block, or every
    # command after it is read out of the middle of a value. The block holds no
    # `~` on purpose: the prefix substitution would change its length and the
    # probe would be testing its own arithmetic instead.
    (
        "refused set does not desynchronise the stream",
        [b"add zz 0 0 7\r\nget z\r\n\r\n", memcached_auth()],
        b"\r\n",
    ),
]

REDIS_PROBES = [
    ("set then get", [resp("SET", "~1", "abc"), resp("GET", "~1")], b"\r\n"),
    ("get miss", [resp("GET", "~absent")], b"\r\n"),
    ("empty value", [resp("SET", "~2", ""), resp("GET", "~2")], b"\r\n"),
    ("del", [resp("SET", "~3", "x"), resp("DEL", "~3"), resp("DEL", "~3")], b"\r\n"),
    ("exists counts duplicates", [resp("SET", "~4", "x"), resp("EXISTS", "~4", "~4", "~none")], b"\r\n"),
    ("type", [resp("SET", "~5", "x"), resp("TYPE", "~5"), resp("TYPE", "~none")], b"\r\n"),
    ("mset and mget", [resp("MSET", "~6", "a", "~7", "b"), resp("MGET", "~6", "~7", "~none")], b"\r\n"),
    ("append creates", [resp("APPEND", "~8", "ab"), resp("APPEND", "~8", "cd"), resp("GET", "~8")], b"\r\n"),
    ("incr family", [
        resp("SET", "~9", "10"),
        resp("INCR", "~9"),
        resp("INCRBY", "~9", "5"),
        resp("DECR", "~9"),
        resp("DECRBY", "~9", "3"),
    ], b"\r\n"),
    ("incr on a non-numeric value", [resp("SET", "~10", "abc"), resp("INCR", "~10")], b"\r\n"),
    ("incr overflow", [resp("SET", "~11", "9223372036854775807"), resp("INCR", "~11")], b"\r\n"),
    ("incrbyfloat", [resp("SET", "~12", "10.5"), resp("INCRBYFLOAT", "~12", "0.1")], b"\r\n"),
    ("ttl on a key with no expiry", [resp("SET", "~13", "x"), resp("TTL", "~13")], b"\r\n"),
    ("ttl on an absent key", [resp("TTL", "~absent")], b"\r\n"),
    ("expire then ttl", [resp("SET", "~14", "x"), resp("EXPIRE", "~14", "100"), resp("TTL", "~14")], b"\r\n"),
    ("persist", [resp("SET", "~15", "x"), resp("EXPIRE", "~15", "100"), resp("PERSIST", "~15"), resp("TTL", "~15")], b"\r\n"),
    ("expire with a condition", [
        resp("SET", "~16", "x"),
        resp("EXPIRE", "~16", "100"),
        resp("EXPIRE", "~16", "50", "GT"),
        resp("EXPIRE", "~16", "200", "GT"),
        resp("TTL", "~16"),
    ], b"\r\n"),
    ("set nx and xx", [
        resp("SET", "~17", "a", "NX"),
        resp("SET", "~17", "b", "NX"),
        resp("SET", "~17", "c", "XX"),
        resp("GET", "~17"),
    ], b"\r\n"),
    ("set with get", [resp("SET", "~18", "a"), resp("SET", "~18", "b", "GET")], b"\r\n"),
    ("set keepttl", [
        resp("SET", "~19", "a", "EX", "100"),
        resp("SET", "~19", "b", "KEEPTTL"),
        resp("TTL", "~19"),
    ], b"\r\n"),
    ("negative expiry is rejected", [resp("SET", "~20", "x", "EX", "0")], b"\r\n"),
    ("unknown command", [resp("FROBNICATE", "a")], b"\r\n"),
    ("wrong arity", [resp("GET")], b"\r\n"),
    ("syntax error", [resp("SET", "~21", "x", "NX", "XX")], b"\r\n"),
    ("ping", [resp("PING"), resp("PING", "hello")], b"\r\n"),
    ("hello 3 then a miss", [resp("HELLO", "3"), resp("GET", "~absent")], b"\r\n"),
    ("hello 2 then a miss", [resp("HELLO", "2"), resp("GET", "~absent")], b"\r\n"),
    ("hello with a bad version", [resp("HELLO", "9")], b"\r\n"),
]

REDIS_AUTH_PROBES = [
    ("get before auth", [resp("GET", "~1")], b"\r\n"),
    ("set before auth", [resp("SET", "~1", "x")], b"\r\n"),
    ("ping before auth", [resp("PING")], b"\r\n"),
    ("bare hello before auth", [resp("HELLO")], b"\r\n"),
    ("bare hello 3 before auth", [resp("HELLO", "3")], b"\r\n"),
    ("unknown command before auth", [resp("FROBNICATE")], b"\r\n"),
    ("quit before auth", [resp("QUIT")], b"\r\n"),
    ("auth one argument", [resp("AUTH", AUTH_SECRET)], b"\r\n"),
    ("auth two arguments", [resp("AUTH", AUTH_USER, AUTH_SECRET)], b"\r\n"),
    ("auth wrong password", [resp("AUTH", "wrong")], b"\r\n"),
    ("auth unknown user", [resp("AUTH", "nobody", AUTH_SECRET)], b"\r\n"),
    ("auth no arguments", [resp("AUTH")], b"\r\n"),
    ("auth too many arguments", [resp("AUTH", "a", "b", "c")], b"\r\n"),
    ("auth then get", [resp("AUTH", AUTH_SECRET), resp("GET", "~1")], b"\r\n"),
    ("auth then set and get", [
        resp("AUTH", AUTH_SECRET),
        resp("SET", "~2", "abc"),
        resp("GET", "~2"),
    ], b"\r\n"),
    ("auth twice", [resp("AUTH", AUTH_SECRET), resp("AUTH", AUTH_SECRET)], b"\r\n"),
    ("failed auth leaves the connection unauthenticated", [
        resp("AUTH", "wrong"),
        resp("GET", "~1"),
    ], b"\r\n"),
    ("hello 3 with auth", [resp("HELLO", "3", "AUTH", AUTH_USER, AUTH_SECRET)], b"\r\n"),
    ("hello 2 with auth", [resp("HELLO", "2", "AUTH", AUTH_USER, AUTH_SECRET)], b"\r\n"),
    ("hello with a bad credential", [resp("HELLO", "3", "AUTH", AUTH_USER, "wrong")], b"\r\n"),
    # The version is validated before the credential is looked at, so this must
    # answer NOPROTO *and* leave the connection unauthenticated.
    ("hello with a bad version and a good credential", [
        resp("HELLO", "9", "AUTH", AUTH_USER, AUTH_SECRET),
        resp("GET", "~1"),
    ], b"\r\n"),
    ("hello auth then a resp3 miss", [
        resp("HELLO", "3", "AUTH", AUTH_USER, AUTH_SECRET),
        resp("GET", "~absent"),
    ], b"\r\n"),
]

SUITES = {
    ("memcached", "core"): MEMCACHED_PROBES,
    ("memcached", "auth"): MEMCACHED_AUTH_PROBES,
    ("redis", "core"): REDIS_PROBES,
    ("redis", "auth"): REDIS_AUTH_PROBES,
}

# Divergences that are known, deliberate and justified. Listing them here keeps
# the suite green while still failing on anything *new* — the alternative,
# quietly relaxing a comparison, loses the difference forever.
KNOWN_DIVERGENCES = {
    ("memcached", "core", "key too long"): (
        "memcached emits a stray empty line after the error (verified at 251, "
        "400 bytes, and with several bad keys), then carries on normally. "
        "vash sends the error alone. Reproducing an extra protocol line "
        "would mean a pipelining client counts one more response than it sent "
        "commands, so this one is not copied."
    ),
    ("memcached", "auth", "meta no-op before auth"): (
        "memcached closes the connection without a word on `mn` and `ms` "
        "before authenticating, while answering CLIENT_ERROR unauthenticated "
        "for `mg`, `md`, `ma` and `me` — measured repeatedly against 1.6.45. "
        "vash answers the error for all six. A silent disconnect gives a client "
        "nothing to report, and an asymmetry across four commands that behave "
        "one way and two that behave another reads as an upstream oversight "
        "rather than a decision."
    ),
    ("memcached", "auth", "meta set before auth"): "See `meta no-op before auth`.",
    ("memcached", "auth", "refused set does not desynchronise the stream"): (
        "memcached stops reading a connection's pipeline after the first thing "
        "it refuses while unauthenticated — here by closing outright, without "
        "answering the valid credential that followed. Measured alongside it: "
        "two unauthenticated `get`s draw one reply rather than two, and a "
        "refused command followed by a good credential answers `bad command "
        "line termination` and never authenticates. vash answers every command "
        "in the pipeline, in the position it occupied. Copying upstream would "
        "silently discard commands a client had sent, and `-Y` is marked "
        "EXPERIMENTAL upstream, which is what this reads like. The probe's own "
        "subject — that vash consumes the refused command's declared data block "
        "rather than reading `get z` as a command — its reply proves."
    ),
    ("redis", "core", "unknown command"): (
        "Redis names the arguments as well as the command in its "
        "`unknown command` error; vash names the command only. Reproducing the "
        "argument list means echoing attacker-controlled bytes into a log line "
        "and an error reply, to no benefit a client can act on."
    ),
    ("redis", "auth", "unknown command before auth"): (
        "Same message difference as `unknown command` in the core suite. Note "
        "what does *not* differ: Redis answers `unknown command` rather than "
        "`NOAUTH` to an unauthenticated client, so it tells a stranger which "
        "commands it knows, and vash matches that. memcached goes the other "
        "way — everything unparseable is `unauthenticated` there — and vash "
        "matches that too. Each dialect keeps its own answer."
    ),
}

# Per-server values that cannot be equal and say nothing about compatibility.
NORMALISERS = {
    "memcached": [
        (re.compile(rb"^(VALUE \S+ \d+ \d+) \d+$", re.M), rb"\1 <CAS>"),
        (re.compile(rb"^VERSION .*$", re.M), rb"VERSION <V>"),
        (re.compile(rb"\bc\d+\b"), rb"c<CAS>"),
    ],
    "redis": [
        # The HELLO map carries a connection id and a version string.
        (re.compile(rb"\$2\r\nid\r\n:\d+\r\n"), rb"$2\r\nid\r\n:<ID>\r\n"),
        (re.compile(rb"\$7\r\nversion\r\n\$\d+\r\n[^\r]*\r\n"), rb"$7\r\nversion\r\n<V>\r\n"),
        # INCRBYFLOAT is 80-bit long double upstream and f64 here, a documented
        # divergence; the last digits of a long chain can differ.
        (re.compile(rb"\$\d+\r\n(\d+\.\d{6})\d+\r\n"), rb"$<L>\r\n\1<PRECISION>\r\n"),
        # A TTL probe sets a lifetime and reads it back a moment later. When the
        # second boundary falls between those two commands the reply is one
        # lower — and the two servers do not cross it at the same instant, so
        # roughly one run in ten reported a difference that was the clock, not
        # the code. Only the exact values these probes request are collapsed, so
        # a real off-by-one in TTL handling still shows up as one.
        (re.compile(rb":99\r\n"), rb":100\r\n"),
        (re.compile(rb":199\r\n"), rb":200\r\n"),
    ],
}


def normalise(data, dialect):
    for pattern, replacement in NORMALISERS[dialect]:
        data = pattern.sub(replacement, data)
    return data


class Server:
    def __init__(self, address):
        host, _, port = address.partition(":")
        self.address = address
        self.host = host
        self.port = int(port)

    def run(self, commands, _terminator=None):
        """Runs a probe on a fresh connection, returning the raw response.

        Reads until the server goes quiet rather than until some expected
        terminator: a probe may draw several responses, and stopping at the
        first one would report a difference that is really just a short read.
        """
        sock = socket.create_connection((self.host, self.port), timeout=5)
        received = b""
        try:
            for command in commands:
                sock.sendall(command)

            sock.settimeout(0.25)
            deadline = time.time() + 3
            while time.time() < deadline:
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    break  # quiet: everything that is coming has come
                except ConnectionResetError:
                    # A server hanging up rudely rather than closing cleanly is
                    # itself a result — memcached does it on some pre-auth
                    # commands. Whatever arrived before the reset is the reply.
                    break
                if not chunk:
                    break  # server closed
                received += chunk
        finally:
            sock.close()
        return received


def run_suite(dialect, suite, reference, subject, verbose=False):
    """Runs one (dialect, suite) pair. Returns the number of unexpected diffs."""
    probes = SUITES[(dialect, suite)]

    # Probe keys are written as `~1`, `~2`, â€¦ and given a fresh prefix on every
    # run. Without it a probe like "add on an absent key" would pass once and
    # then fail forever against a server that keeps its data between runs.
    prefix = b"dx" + uuid.uuid4().hex[:8].encode()

    print(f"\n=== {dialect}/{suite} ===")
    print(f"reference: {reference.address}   subject: {subject.address}")
    print(f"key prefix: {prefix.decode()}\n")

    same = 0
    known = []
    differences = []

    for name, raw_commands, terminator in probes:
        commands = [render(c, prefix) for c in raw_commands]
        expected = normalise(reference.run(commands, terminator), dialect)
        actual = normalise(subject.run(commands, terminator), dialect)
        key = (dialect, suite, name)

        if expected == actual:
            same += 1
            print(f"  ok    {name}")
            if verbose:
                print(f"        both: {expected!r}")
        elif key in KNOWN_DIVERGENCES:
            known.append(name)
            print(f"  known {name}")
            print(f"        {dialect}: {expected!r}")
            print(f"        vash{' ' * (len(dialect) - 4)}: {actual!r}")
            print(f"        why: {KNOWN_DIVERGENCES[key]}")
        else:
            differences.append(name)
            print(f"  DIFF  {name}")
            print(f"        {dialect}: {expected!r}")
            print(f"        vash{' ' * (len(dialect) - 4)}: {actual!r}")

    print(f"\n{same} identical, {len(known)} known divergences, {len(differences)} unexpected")

    # A known divergence that has gone away is worth knowing about too: it means
    # the note is stale and should be deleted. Only checked for probes that
    # actually ran, so a note about another dialect is not reported here.
    names = {p[0] for p in probes}
    for entry_dialect, entry_suite, entry_name in KNOWN_DIVERGENCES:
        if (entry_dialect, entry_suite) != (dialect, suite):
            continue
        if entry_name in names and entry_name not in known:
            print(f"note: '{entry_name}' no longer diverges; remove it from KNOWN_DIVERGENCES")

    return len(differences)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, help="the real server, host:port")
    parser.add_argument("--subject", required=True, help="vash, host:port")
    parser.add_argument(
        "--dialect", default="memcached", choices=sorted({d for d, _ in SUITES})
    )
    parser.add_argument(
        "--suite",
        default="core",
        choices=sorted({s for _, s in SUITES}),
        help="`core` needs plain servers; `auth` needs both configured with a credential",
    )
    parser.add_argument("--verbose", action="store_true", help="print matching replies too")
    args = parser.parse_args()

    failures = run_suite(
        args.dialect,
        args.suite,
        Server(args.reference),
        Server(args.subject),
        verbose=args.verbose,
    )
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()

