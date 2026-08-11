# A statically linked kached on a scratch image.
#
#   docker build -t kached .
#   docker run --rm -p 11311:11311 -v kached-data:/var/lib/kached kached
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
COPY crates/cache-core/Cargo.toml crates/cache-core/
COPY crates/cache-store/Cargo.toml crates/cache-store/
COPY crates/cache-proto/Cargo.toml crates/cache-proto/
COPY crates/cache-server/Cargo.toml crates/cache-server/
COPY crates/cache-client/Cargo.toml crates/cache-client/
COPY crates/cache-bench/Cargo.toml crates/cache-bench/
RUN mkdir -p crates/cache-core/src crates/cache-store/src crates/cache-proto/src \
             crates/cache-server/src crates/cache-client/src crates/cache-bench/src \
    && echo "fn main() {}" > crates/cache-server/src/main.rs \
    && echo "fn main() {}" > crates/cache-bench/src/load.rs \
    && touch crates/cache-core/src/lib.rs crates/cache-store/src/lib.rs \
             crates/cache-proto/src/lib.rs crates/cache-server/src/lib.rs \
             crates/cache-client/src/lib.rs \
    && cargo build --release --bin kached 2>/dev/null || true

COPY . .
# Touch the real sources so cargo does not trust the dummy build's timestamps.
RUN find crates -name "*.rs" -exec touch {} + \
    && cargo build --release --bin kached \
    && strip target/release/kached

# scratch, not alpine: nothing in the image but the binary means nothing in the
# image to have a CVE. There is no shell to exec into, which is the point.
FROM scratch

COPY --from=build /src/target/release/kached /kached

# 11311 is the cache port for both protocols; 9090 is the admin port, which
# should be published only to a private network if at all.
EXPOSE 11311 9090
VOLUME ["/var/lib/kached"]

# Unprivileged: nothing here needs root, and the data directory is a volume the
# operator owns. Numeric because there is no /etc/passwd to resolve a name.
USER 65534:65534

ENTRYPOINT ["/kached"]
CMD ["--listen", "0.0.0.0:11311", "--data", "/var/lib/kached"]
