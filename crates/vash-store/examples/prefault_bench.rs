//! Does `store.prefault` earn its startup cost? Linux only.
//!
//! The claim under test is the one plan §9 is built around: a read against a
//! map that is not resident waits on the device, and warming the file at
//! startup turns that wait into nothing.
//!
//! It can only be measured against a **cold page cache**, which is the whole
//! difficulty. `drop_caches` needs root and would evict the entire machine, so
//! each variant instead evicts exactly the files under test with
//! `posix_fadvise(DONTNEED)` — unprivileged — and `mincore` verifies the
//! eviction happened rather than trusting it. Without that check the benchmark
//! would quietly compare warm against warm and report that the flag does
//! nothing.
//!
//! ```sh
//! cargo run --release -p vash-store --example prefault_bench
//! ```
//!
//! Set `VASH_PREFAULT_DB` to place the database; it is filled once and reused,
//! so delete it to change the shape. **Put it on a real filesystem** — a
//! `/mnt/c` style mount does not have the page-cache semantics this measures.

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!(
        "prefault_bench is Linux-only: it needs posix_fadvise to evict the page \
         cache and mincore to prove the eviction happened."
    );
}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::io::AsRawFd;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use vash_core::{Key, Set};
    use vash_store::{Durability, LmdbStore, Store, StoreConfig};

    const KEYS: u32 = 750_000;
    const VALUE_LEN: usize = 4096;
    const SHARDS: usize = 4;
    const GETS: usize = 20_000;
    const PAGE: usize = 4096;

    fn db_path() -> PathBuf {
        std::env::var_os("VASH_PREFAULT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("vash-prefault-db"))
    }

    fn config(prefault: bool) -> StoreConfig {
        StoreConfig {
            path: db_path(),
            map_size: 4 * 1024 * 1024 * 1024,
            shards: SHARDS,
            durability: Durability::Lazy,
            prefault,
            ..StoreConfig::default()
        }
    }

    fn shard_files() -> Vec<PathBuf> {
        (0..SHARDS)
            .map(|i| db_path().join(format!("shard-{i}")).join("data.mdb"))
            .collect()
    }

    /// Drops a file's clean pages from the page cache. Unprivileged, unlike
    /// `drop_caches`, and it targets exactly the file under test rather than
    /// everything the machine happens to have cached.
    fn evict(path: &Path) {
        let file = std::fs::File::open(path).unwrap();
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }

    /// Resident bytes of a file, and the bytes it actually occupies on disk.
    ///
    /// The `mincore` half proves the eviction. The `blocks` half matters because
    /// **LMDB creates `data.mdb` at the full map size and leaves it sparse**, so
    /// the file's length is `map_size` and says nothing about the data — the
    /// same trap that made the first version of `prefault` read 16 GiB of holes.
    fn resident(path: &Path) -> (u64, u64) {
        let file = std::fs::File::open(path).unwrap();
        let meta = file.metadata().unwrap();
        let (allocated, len) = (meta.blocks() * 512, meta.len() as usize);
        if len == 0 {
            return (0, 0);
        }
        unsafe {
            let map = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            );
            assert_ne!(map, libc::MAP_FAILED, "mmap");
            let mut vec = vec![0u8; len.div_ceil(PAGE)];
            assert_eq!(libc::mincore(map, len, vec.as_mut_ptr()), 0, "mincore");
            let hot = vec.iter().filter(|b| *b & 1 == 1).count();
            libc::munmap(map, len);
            ((hot * PAGE) as u64, allocated)
        }
    }

    fn resident_total() -> (u64, u64) {
        shard_files()
            .iter()
            .map(|p| resident(p))
            .fold((0, 0), |(a, b), (c, d)| (a + c, b + d))
    }

    fn mib(bytes: u64) -> f64 {
        bytes as f64 / 1024.0 / 1024.0
    }

    /// SplitMix64, so the benchmark needs no dependency and every variant walks
    /// the identical key sequence.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    fn fill() {
        if db_path().exists() {
            println!("reusing {}", db_path().display());
            return;
        }
        println!("filling {KEYS} keys x {VALUE_LEN} B across {SHARDS} shards...");
        let started = Instant::now();
        let store = LmdbStore::open(&config(false)).unwrap();
        let value = vec![7u8; VALUE_LEN];
        // Batched, because one `set` per key is a writer-queue round trip and
        // filling this much would take the best part of an hour.
        for chunk in (0..KEYS).collect::<Vec<_>>().chunks(1000) {
            let keys: Vec<String> = chunk.iter().map(|i| format!("key-{i:08}")).collect();
            let sets: Vec<Set<'_>> = keys
                .iter()
                .map(|k| Set {
                    key: Key::new(k.as_bytes()).unwrap(),
                    value: &value,
                    ttl: vash_core::TtlChange::Set(0),
                    return_previous: false,
                    mc_flags: 0,
                    tags: Vec::new(),
                    mode: vash_core::SetMode::Set,
                })
                .collect();
            store.set_many(&sets).unwrap();
        }
        store.close();
        println!("  filled in {:?}", started.elapsed());
    }

    fn percentile(sorted: &[Duration], p: f64) -> Duration {
        sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
    }

    fn run(prefault: bool) {
        println!("\n--- prefault = {prefault} ---");
        for f in shard_files() {
            evict(&f);
        }
        let (hot, total) = resident_total();
        println!(
            "  resident before open: {:.0} / {:.0} MiB allocated",
            mib(hot),
            mib(total)
        );

        let t = Instant::now();
        let store = LmdbStore::open(&config(prefault)).unwrap();
        let open = t.elapsed();
        println!("  open: {open:?}");
        println!("  resident after open:  {:.0} MiB", mib(resident_total().0));

        // The same key sequence for every variant, so residency is the only
        // thing that differs.
        let mut rng = Rng(0x5EED);
        let mut samples = Vec::with_capacity(GETS);
        let mut hits = 0usize;
        let started = Instant::now();
        for _ in 0..GETS {
            let key = format!("key-{:08}", rng.next() % KEYS as u64);
            let t = Instant::now();
            let got = store.get(Key::new(key.as_bytes()).unwrap()).unwrap();
            samples.push(t.elapsed());
            hits += got.is_some() as usize;
        }
        let wall = started.elapsed();
        store.close();

        samples.sort_unstable();
        println!(
            "  {GETS} random GETs ({hits} hits): {wall:?} total, {:.0} ops/s",
            GETS as f64 / wall.as_secs_f64()
        );
        println!(
            "    p50 {:?}  p90 {:?}  p99 {:?}  p999 {:?}  max {:?}",
            percentile(&samples, 0.50),
            percentile(&samples, 0.90),
            percentile(&samples, 0.99),
            percentile(&samples, 0.999),
            samples[samples.len() - 1],
        );
    }

    pub fn main() {
        fill();
        println!(
            "database: {:.0} MiB allocated across {SHARDS} shards",
            mib(resident_total().1)
        );
        // Interleaved and repeated: one pass of each cannot tell a real
        // difference from drift on a machine doing other things.
        for round in 0..2 {
            println!("\n===== round {round} =====");
            run(false);
            run(true);
        }
    }
}
