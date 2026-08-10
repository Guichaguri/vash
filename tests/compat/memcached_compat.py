#!/usr/bin/env python3
"""Memcached client-library compatibility suite.

Runs a real client library (pymemcache) against a server and checks the
behaviour a client actually depends on. It is written to pass against **both**
kached and a real memcached, so any divergence shows up as a failure here
rather than in someone's production cache.

    pip install pymemcache
    python memcached_compat.py 127.0.0.1:11311        # kached
    python memcached_compat.py 127.0.0.1:11211        # real memcached

Options:
    --flush     also exercise flush_all (kached needs protocol.flush_enabled)
    --tags      also exercise the tag extension (kached only)
"""

import argparse
import sys
import time

from pymemcache.client.base import Client
from pymemcache.serde import pickle_serde

FAILURES = []
PASSED = 0


def check(name, fn):
    global PASSED
    try:
        fn()
    except AssertionError as e:
        FAILURES.append((name, str(e) or "assertion failed"))
        print(f"  FAIL  {name}\n        {e}")
    except Exception as e:  # noqa: BLE001 - report, do not mask
        FAILURES.append((name, f"{type(e).__name__}: {e}"))
        print(f"  ERROR {name}\n        {type(e).__name__}: {e}")
    else:
        PASSED += 1
        print(f"  ok    {name}")


def make_client(addr, serde=None):
    host, _, port = addr.partition(":")
    # default_noreply=False so writes actually wait for the server's verdict;
    # otherwise add/replace failures would be invisible.
    return Client((host, int(port)), default_noreply=False, timeout=5, serde=serde)


def run(addr, do_flush, do_tags):
    c = make_client(addr)
    c.delete_many([b"k", b"n", b"a", b"b", b"cnt", b"big", b"bin", b"t1", b"t2"])

    def basic_round_trip():
        assert c.set(b"k", b"value") is True
        assert c.get(b"k") == b"value"

    def missing_key_is_none():
        c.delete(b"absent")
        assert c.get(b"absent") is None

    def client_flags_round_trip():
        # A serde is what puts anything in the flags field: pickle_serde tags an
        # int with flags=2 and relies on getting that number back to decode it.
        # Without one, pymemcache sends flags=0 and this would prove nothing.
        typed = make_client(addr, serde=pickle_serde)
        typed.set(b"typed", 12345)
        assert typed.get(b"typed") == 12345, "flags must round-trip for the value to decode"
        typed.set(b"typed", {"a": [1, 2]})
        assert typed.get(b"typed") == {"a": [1, 2]}
        typed.close()

    def add_is_conditional():
        c.delete(b"a")
        assert c.add(b"a", b"first") is True
        assert c.add(b"a", b"second") is False
        assert c.get(b"a") == b"first"

    def replace_is_conditional():
        c.delete(b"b")
        assert c.replace(b"b", b"nope") is False
        c.set(b"b", b"here")
        assert c.replace(b"b", b"replaced") is True
        assert c.get(b"b") == b"replaced"

    def append_and_prepend():
        c.set(b"k", b"mid")
        assert c.append(b"k", b"-end") is True
        assert c.prepend(b"k", b"start-") is True
        assert c.get(b"k") == b"start-mid-end"

    def append_on_missing_fails():
        c.delete(b"gone")
        assert c.append(b"gone", b"x") is False

    def gets_and_cas():
        c.set(b"k", b"one")
        value, cas = c.gets(b"k")
        assert value == b"one"
        assert cas is not None, "gets must report a cas token"

        assert c.cas(b"k", b"two", cas) is True
        # The token has moved on, so a replay must be rejected.
        assert c.cas(b"k", b"three", cas) is False
        assert c.get(b"k") == b"two"

    def cas_on_missing_is_none():
        c.delete(b"absent")
        _, cas = c.gets(b"k")
        assert c.cas(b"absent", b"x", cas) is None, "cas on a missing key is NOT_FOUND"

    def incr_and_decr():
        c.set(b"cnt", b"10")
        assert c.incr(b"cnt", 5) == 15
        assert c.decr(b"cnt", 3) == 12
        # The value stays plain decimal text.
        assert c.get(b"cnt") == b"12"

    def incr_on_missing_is_none():
        c.delete(b"nocount")
        assert c.incr(b"nocount", 1) is None

    def delete_reports_outcome():
        c.set(b"k", b"x")
        assert c.delete(b"k", noreply=False) is True
        assert c.delete(b"k", noreply=False) is False

    def get_many():
        c.set(b"a", b"1")
        c.set(b"b", b"2")
        c.delete(b"missing")
        got = c.get_many([b"a", b"b", b"missing"])
        assert got == {b"a": b"1", b"b": b"2"}, f"misses must be omitted, got {got}"

    def touch_extends_expiry():
        c.set(b"k", b"x", expire=1)
        assert c.touch(b"k", 100, noreply=False) is True
        time.sleep(1.3)
        assert c.get(b"k") == b"x", "touch should have replaced the 1s expiry"

    def expiry_is_honoured():
        c.set(b"k", b"x", expire=1)
        assert c.get(b"k") == b"x"
        time.sleep(1.3)
        assert c.get(b"k") is None, "the key should have expired"

    def large_value():
        payload = bytes(range(256)) * 400  # 100 KiB of binary
        c.set(b"big", payload)
        assert c.get(b"big") == payload

    def binary_safe_values():
        payload = b"\x00\r\n\xff binary \x1b"
        c.set(b"bin", payload)
        assert c.get(b"bin") == payload, "values must be length-delimited, not line-delimited"

    def stats_and_version():
        stats = c.stats()
        assert isinstance(stats, dict) and stats, "stats must return entries"
        assert b"curr_items" in stats or "curr_items" in stats, f"missing curr_items: {list(stats)[:5]}"
        assert c.version(), "version must return something"

    def noreply_writes_still_apply():
        c.set(b"k", b"quiet", noreply=True)
        assert c.get(b"k") == b"quiet"

    def oversized_key_is_rejected():
        try:
            c.set(b"k" * 251, b"x")
        except Exception:
            return  # a client-side or server-side rejection both count
        raise AssertionError("a 251-byte key should have been rejected")

    for name, fn in [
        ("set/get round trip", basic_round_trip),
        ("missing key returns None", missing_key_is_none),
        ("client flags round-trip", client_flags_round_trip),
        ("add is conditional", add_is_conditional),
        ("replace is conditional", replace_is_conditional),
        ("append and prepend", append_and_prepend),
        ("append on a missing key fails", append_on_missing_fails),
        ("gets returns a usable cas token", gets_and_cas),
        ("cas on a missing key", cas_on_missing_is_none),
        ("incr and decr", incr_and_decr),
        ("incr on a missing key", incr_on_missing_is_none),
        ("delete reports hit and miss", delete_reports_outcome),
        ("get_many omits misses", get_many),
        ("touch extends expiry", touch_extends_expiry),
        ("expiry is honoured", expiry_is_honoured),
        ("100 KiB value", large_value),
        ("binary-safe values", binary_safe_values),
        ("stats and version", stats_and_version),
        ("noreply writes still apply", noreply_writes_still_apply),
        ("oversized key is rejected", oversized_key_is_rejected),
    ]:
        check(name, fn)

    if do_flush:
        def flush_empties():
            c.set(b"k", b"x")
            c.flush_all(noreply=False)
            assert c.get(b"k") is None

        check("flush_all empties the cache", flush_empties)

    if do_tags:
        # Extension: not part of memcached, so only run against kached.
        raw = make_client(addr)

        def tag_invalidation():
            sock_write(raw, b"ms t1 1 Gnews\r\n1\r\n", b"HD\r\n")
            sock_write(raw, b"ms t2 1 Gsport\r\n2\r\n", b"HD\r\n")
            sock_write(raw, b"mdt news\r\n", b"HD\r\n")
            sock_write(raw, b"mg t1 v\r\n", b"EN\r\n")
            sock_write(raw, b"mg t2 v\r\n", b"VA 1\r\n2\r\n")

        check("tag invalidation via meta", tag_invalidation)

    c.close()


def sock_write(client, request, expected):
    """Sends a raw command over pymemcache's socket and checks the reply."""
    client._connect()
    client.sock.sendall(request)

    received = b""
    deadline = time.time() + 5
    while not received.endswith(expected) and time.time() < deadline:
        chunk = client.sock.recv(4096)
        if not chunk:
            break
        received += chunk
    assert received == expected, f"for {request!r}: expected {expected!r}, got {received!r}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("address", help="host:port")
    parser.add_argument("--flush", action="store_true")
    parser.add_argument("--tags", action="store_true")
    args = parser.parse_args()

    print(f"memcached client compatibility suite against {args.address}")
    run(args.address, args.flush, args.tags)

    print(f"\n{PASSED} passed, {len(FAILURES)} failed")
    if FAILURES:
        for name, detail in FAILURES:
            print(f"  - {name}: {detail}")
        sys.exit(1)


if __name__ == "__main__":
    main()
