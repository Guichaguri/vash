#!/usr/bin/env python3
"""Byte-for-byte differential against a real memcached.

Sends identical command sequences to two servers and compares the raw
responses. A client library smooths over a lot â€” exact error strings, edge-case
verdicts, response ordering â€” and this is where those divergences show up.

    python differential.py --reference 127.0.0.1:11211 --subject 127.0.0.1:11311

Values that legitimately differ between two servers (CAS tokens, version
strings) are normalised before comparison; everything else must match exactly.
"""

import argparse
import re
import socket
import sys
import time
import uuid

# Probe: (name, commands, terminator that ends the whole exchange)
PROBES = [
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

# Divergences that are known, deliberate and justified. Listing them here keeps
# the suite green while still failing on anything *new* — the alternative,
# quietly relaxing a comparison, loses the difference forever.
KNOWN_DIVERGENCES = {
    "key too long": (
        "memcached emits a stray empty line after the error (verified at 251, "
        "400 bytes, and with several bad keys), then carries on normally. "
        "kached sends the error alone. Reproducing an extra protocol line "
        "would mean a pipelining client counts one more response than it sent "
        "commands, so this one is not copied."
    ),
}

# CAS tokens and version strings are per-server; everything else must match.
NORMALISERS = [
    (re.compile(rb"^(VALUE \S+ \d+ \d+) \d+$", re.M), rb"\1 <CAS>"),
    (re.compile(rb"^VERSION .*$", re.M), rb"VERSION <V>"),
    (re.compile(rb"\bc\d+\b"), rb"c<CAS>"),
]


def normalise(data):
    for pattern, replacement in NORMALISERS:
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
                if not chunk:
                    break  # server closed
                received += chunk
        finally:
            sock.close()
        return received


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, help="real memcached, host:port")
    parser.add_argument("--subject", required=True, help="kached, host:port")
    args = parser.parse_args()

    reference = Server(args.reference)
    subject = Server(args.subject)

    # Probe keys are written as `~1`, `~2`, â€¦ and given a fresh prefix on every
    # run. Without it a probe like "add on an absent key" would pass once and
    # then fail forever against a server that keeps its data between runs.
    prefix = b"dx" + uuid.uuid4().hex[:8].encode()

    print(f"reference: {reference.address}   subject: {subject.address}")
    print(f"key prefix: {prefix.decode()}\n")

    same = 0
    known = []
    differences = []

    for name, raw_commands, terminator in PROBES:
        commands = [c.replace(b"~", prefix) for c in raw_commands]
        expected = normalise(reference.run(commands, terminator))
        actual = normalise(subject.run(commands, terminator))

        if expected == actual:
            same += 1
            print(f"  ok    {name}")
        elif name in KNOWN_DIVERGENCES:
            known.append(name)
            print(f"  known {name}")
            print(f"        memcached: {expected!r}")
            print(f"        kached   : {actual!r}")
            print(f"        why: {KNOWN_DIVERGENCES[name]}")
        else:
            differences.append((name, commands, expected, actual))
            print(f"  DIFF  {name}")
            print(f"        memcached: {expected!r}")
            print(f"        kached   : {actual!r}")

    print(f"\n{same} identical, {len(known)} known divergences, {len(differences)} unexpected")

    # A known divergence that has gone away is worth knowing about too: it means
    # the note is stale and should be deleted.
    for name in KNOWN_DIVERGENCES:
        if name not in known and any(p[0] == name for p in PROBES):
            print(f"note: '{name}' no longer diverges; remove it from KNOWN_DIVERGENCES")

    if differences:
        sys.exit(1)


if __name__ == "__main__":
    main()

