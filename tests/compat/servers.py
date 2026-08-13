#!/usr/bin/env python3
"""Brings up the servers a differential run compares.

Reference servers run in Docker, pinned to a version, so the suite compares
against the *same* memcached and Redis everywhere it runs — a developer's
machine, CI, and whatever distro packages happen to be installed on either.
Before this, the reference was whatever `apt-get install memcached` produced,
which is a moving target for a suite whose whole job is byte-for-byte equality.

The subject (vash) runs as a local process, because that is the binary under
test and building it in an image on every run would cost more than it explains.

Nothing here is imported by the Rust tests: this is the outer harness, driven by
`docker_differential.py`.
"""

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

MEMCACHED_IMAGE = "memcached:1.6-alpine"
REDIS_IMAGE = "redis:7.4-alpine"

# The credential every server in a differential run is configured with. Long
# enough to clear vash's 16-byte floor, and free of spaces because memcached's
# ASCII mechanism splits the token on one.
AUTH_USER = "default"
AUTH_SECRET = "s3cr3t-token-goes-here"


def wait_for_port(port, what, timeout=30.0):
    """Blocks until something accepts on `port`."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"{what} never started listening on port {port}")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Container:
    """A reference server in Docker, stopped when the block exits."""

    def __init__(self, name, image, port, command=None, mounts=()):
        self.name = name
        self.image = image
        self.port = port
        self.command = command or []
        self.mounts = mounts

    def __enter__(self):
        # Removed first in case a previous run died before its cleanup. `-f` so
        # a container that is merely stopped goes too.
        subprocess.run(
            ["docker", "rm", "-f", self.name],
            capture_output=True,
            check=False,
        )

        argv = ["docker", "run", "-d", "--rm", "--name", self.name]
        for host_path, container_path in self.mounts:
            argv += ["-v", f"{host_path}:{container_path}:ro"]
        argv += ["-p", f"127.0.0.1:{self.port}:{self.internal_port}", self.image]
        argv += self.command

        result = subprocess.run(argv, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"starting {self.name}: {result.stderr.strip()}")

        try:
            wait_for_port(self.port, self.name)
        except RuntimeError:
            # The logs are the only useful thing when a server refuses its own
            # configuration — memcached rejects an unreadable auth file this
            # way, and the exit is otherwise silent.
            logs = subprocess.run(
                ["docker", "logs", self.name], capture_output=True, text=True
            )
            self.__exit__(None, None, None)
            raise RuntimeError(
                f"{self.name} did not come up.\n"
                f"  stdout: {logs.stdout.strip()}\n  stderr: {logs.stderr.strip()}"
            ) from None
        return self

    def __exit__(self, *_):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True, check=False)
        return False

    @property
    def address(self):
        return f"127.0.0.1:{self.port}"


class Memcached(Container):
    internal_port = 11211

    def __init__(self, auth_file=None):
        command = ["memcached", "-m", "64"]
        mounts = ()
        if auth_file:
            command += ["-Y", "/authfile"]
            mounts = ((auth_file, "/authfile"),)
        super().__init__(
            name=f"vash-diff-memcached-{'auth' if auth_file else 'plain'}",
            image=MEMCACHED_IMAGE,
            port=_free_port(),
            command=command,
            mounts=mounts,
        )


class Redis(Container):
    internal_port = 6379

    def __init__(self, requirepass=None):
        command = ["redis-server"]
        if requirepass:
            command += ["--requirepass", requirepass]
        super().__init__(
            name=f"vash-diff-redis-{'auth' if requirepass else 'plain'}",
            image=REDIS_IMAGE,
            port=_free_port(),
            command=command,
        )


class Vash:
    """The subject, as a local process."""

    def __init__(self, binary, credentials=None, listing=False):
        self.binary = binary
        self.credentials = credentials
        # Keyspace enumeration is off by default here and always on upstream, so
        # a suite that compares what the gates *report* has to be able to turn
        # them on. See the memcached/stats combination.
        self.listing = listing
        self.port = _free_port()
        self.process = None
        self.data = None

    def __enter__(self):
        self.data = tempfile.mkdtemp(prefix="vash-diff-")
        argv = [
            self.binary,
            "--listen",
            f"127.0.0.1:{self.port}",
            "--data",
            os.path.join(self.data, "db"),
            "--ephemeral",
            # The differential compares `flush_all` against a real memcached,
            # which always has it.
            "--enable-flush",
        ]
        if self.listing:
            argv.append("--enable-listing")
        if self.credentials:
            argv += ["--require-auth", "--auth-file", self.credentials]

        self.process = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        try:
            wait_for_port(self.port, "vash")
        except RuntimeError:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *_):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
        if self.data:
            shutil.rmtree(self.data, ignore_errors=True)
        return False

    @property
    def address(self):
        return f"127.0.0.1:{self.port}"


def write_credentials(directory, binary):
    """Writes the credential files the two dialects need.

    Returns `(vash_file, memcached_file)`. Both name the same identity with the
    same secret, in each server's own format — vash stores a digest, memcached
    stores the secret in the clear, which is one of the differences §3.1 of
    docs/auth.md is about.

    The secret is a fixed constant rather than `auth-gen`'s output, because all
    three servers have to be given the *same* one and only vash can consume a
    digest. `auth-gen` is still exercised: its line format is asserted here, so
    a change to it fails this suite rather than going unnoticed.
    """
    import hashlib

    generated = subprocess.run(
        [binary, "auth-gen", AUTH_USER],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    prefix = f"{AUTH_USER}  sha256:"
    assert generated.startswith(prefix) and len(generated) == len(prefix) + 64, (
        f"auth-gen no longer emits `{prefix}<64 hex>`: {generated!r}"
    )

    vash_file = os.path.join(directory, "vash-credentials")
    with open(vash_file, "w", encoding="utf-8") as handle:
        handle.write(f"{AUTH_USER}  sha256:{hashlib.sha256(AUTH_SECRET.encode()).hexdigest()}\n")
    # vash refuses a group- or world-readable credential file on Unix.
    os.chmod(vash_file, 0o600)

    memcached_file = os.path.join(directory, "memcached-authfile")
    with open(memcached_file, "w", encoding="utf-8") as handle:
        handle.write(f"{AUTH_USER}:{AUTH_SECRET}\n")
    # Readable inside the container, where memcached runs as a different user.
    os.chmod(memcached_file, 0o644)

    return vash_file, memcached_file


def require_docker():
    if shutil.which("docker") is None:
        print("docker is not on PATH; the differential needs it for the reference servers")
        sys.exit(2)
    result = subprocess.run(["docker", "info"], capture_output=True)
    if result.returncode != 0:
        print("docker is installed but not running")
        sys.exit(2)
