#!/usr/bin/env python3
"""Runs the whole differential against pinned reference servers in Docker.

    cargo build --release --bin vash-server
    python tests/compat/docker_differential.py

Five combinations. Four of them are split by authentication, which is a startup
flag on all three servers and cannot be turned on mid-connection:

    memcached/core   plain memcached          vs  plain vash
    memcached/auth   memcached -Y authfile    vs  vash --require-auth
    memcached/stats  plain memcached          vs  vash --enable-listing
    redis/core       plain redis              vs  plain vash
    redis/auth       redis --requirepass      vs  vash --require-auth

`memcached/stats` needs its own combination for a different reason: `stats
settings` reports the gates, so the subject has to be started with the ones
whose reporting is being checked actually turned on.

Pinning the reference images is the point. `apt-get install memcached` gives a
different version on every distro and every year, which is not a baseline for a
suite whose entire job is byte-for-byte equality — and there was no Redis
baseline at all before this.

Exits non-zero if any suite reports an unexpected difference.
"""

import argparse
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import differential
from servers import AUTH_SECRET, Memcached, Redis, Vash, require_docker, write_credentials


def default_binary():
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    for profile in ("release", "debug"):
        for name in ("vash-server", "vash-server.exe"):
            candidate = os.path.join(root, "target", profile, name)
            if os.path.exists(candidate):
                return candidate
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=default_binary(), help="path to vash-server")
    parser.add_argument(
        "--only",
        action="append",
        choices=[
            "memcached/core",
            "memcached/auth",
            "memcached/stats",
            "redis/core",
            "redis/auth",
        ],
        help="run only these combinations (repeatable)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not args.binary or not os.path.exists(args.binary):
        print("vash-server not found; run `cargo build --release --bin vash-server` first")
        return 2
    require_docker()

    selected = set(
        args.only
        or [
            "memcached/core",
            "memcached/auth",
            "memcached/stats",
            "redis/core",
            "redis/auth",
        ]
    )
    failures = 0

    with tempfile.TemporaryDirectory(prefix="vash-diff-creds-") as credential_dir:
        vash_credentials, memcached_authfile = write_credentials(credential_dir, args.binary)

        if "memcached/core" in selected:
            with Memcached() as reference, Vash(args.binary) as subject:
                failures += differential.run_suite(
                    "memcached", "core", wrap(reference), wrap(subject), args.verbose
                )

        if "memcached/auth" in selected:
            with (
                Memcached(auth_file=memcached_authfile) as reference,
                Vash(args.binary, credentials=vash_credentials) as subject,
            ):
                failures += differential.run_suite(
                    "memcached", "auth", wrap(reference), wrap(subject), args.verbose
                )

        if "memcached/stats" in selected:
            # `--enable-listing` because `stats settings` reports the gates, and
            # a suite checking that report should see them on.
            with (
                Memcached() as reference,
                Vash(args.binary, listing=True) as subject,
            ):
                failures += differential.run_suite(
                    "memcached", "stats", wrap(reference), wrap(subject), args.verbose
                )

        if "redis/core" in selected:
            with Redis() as reference, Vash(args.binary) as subject:
                failures += differential.run_suite(
                    "redis", "core", wrap(reference), wrap(subject), args.verbose
                )

        if "redis/auth" in selected:
            with (
                Redis(requirepass=AUTH_SECRET) as reference,
                Vash(args.binary, credentials=vash_credentials) as subject,
            ):
                failures += differential.run_suite(
                    "redis", "auth", wrap(reference), wrap(subject), args.verbose
                )

    print(f"\n{'FAILED' if failures else 'OK'}: {failures} unexpected difference(s)")
    return 1 if failures else 0


def wrap(server):
    return differential.Server(server.address)


if __name__ == "__main__":
    sys.exit(main())
