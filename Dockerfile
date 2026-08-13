# A statically linked vash on a scratch image.
#
#   docker build -t vash .
#   docker run --rm -p 11311:11311 -v vash-data:/var/lib/vash vash
#
# Static because the alternative is shipping a glibc and pretending the base
# image is not part of the deployment. `heed` compiles LMDB from source, so the
# build needs a C toolchain; musl gives one that links statically without the
# well-known glibc `getaddrinfo` caveat, and nothing here resolves hostnames
# except the optional cluster peer list, which takes addresses.

FROM rust:1.92-alpine AS build

# musl-dev for the C toolchain LMDB needs; the rest is what cargo wants.
RUN apk add --no-cache musl-dev

WORKDIR /src

# Manifests first, so a change to the source does not re-download and rebuild
# every dependency. The dummy sources exist only to give cargo something to
# compile in that layer.
COPY Cargo.toml Cargo.lock ./
COPY crates/vash-core/Cargo.toml crates/vash-core/
COPY crates/vash-store/Cargo.toml crates/vash-store/
COPY crates/vash-proto/Cargo.toml crates/vash-proto/
COPY crates/vash-server/Cargo.toml crates/vash-server/
COPY crates/vash-client/Cargo.toml crates/vash-client/
COPY crates/vash-bench/Cargo.toml crates/vash-bench/
RUN mkdir -p crates/vash-core/src crates/vash-store/src crates/vash-proto/src \
             crates/vash-server/src crates/vash-client/src crates/vash-bench/src \
    && echo "fn main() {}" > crates/vash-server/src/main.rs \
    && echo "fn main() {}" > crates/vash-bench/src/load.rs \
    && touch crates/vash-core/src/lib.rs crates/vash-store/src/lib.rs \
             crates/vash-proto/src/lib.rs crates/vash-server/src/lib.rs \
             crates/vash-client/src/lib.rs \
    && cargo build --release --bin vash-server 2>/dev/null || true

COPY . .
# Touch the real sources so cargo does not trust the dummy build's timestamps.
RUN find crates -name "*.rs" -exec touch {} + \
    && cargo build --release --bin vash-server \
    && strip target/release/vash-server

# scratch, not alpine: nothing in the image but the binary means nothing in the
# image to have a CVE. There is no shell to exec into, which is the point.
FROM scratch

COPY --from=build --chown=65534:65534 /src/target/release/vash-server /vash-server

# 11311 is the cache port for all three protocols. 9090 is the conventional
# admin port, declared here for an operator who wants it — nothing listens on it
# unless the command adds `--admin-listen 0.0.0.0:9090`, and it should be
# published only to a private network even then.
EXPOSE 11311 9090
VOLUME ["/var/lib/vash"]

# Unprivileged: nothing here needs root, and the data directory is a volume the
# operator owns. Numeric because there is no /etc/passwd to resolve a name.
USER 65534:65534

ENTRYPOINT ["/vash-server"]
CMD ["--listen", "0.0.0.0:11311", "--data", "/var/lib/vash"]
