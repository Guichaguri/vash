//! What a fresh database actually costs on disk, per engine.
//!
//! ```text
//! cargo run -p vash-store --features mdbx --example mdbx_geometry
//! ```
//!
//! `store.map_size_mb` is documented as costing nothing until the data arrives,
//! and under LMDB that is true — the map is an address-space reservation and the
//! file is sparse. Under a growable geometry it is a different claim, and this
//! is what checks it.

fn dir_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().expect("metadata");
            total += if meta.is_dir() {
                dir_bytes(&entry.path())
            } else {
                meta.len()
            };
        }
    }
    total
}

fn main() {
    let dir = tempfile::tempdir().expect("temp dir");
    println!(
        "{:>8}  {:>10}  {:>14}",
        "backend", "map_size", "on disk, fresh"
    );

    for backend in [vash_store::BackendKind::Lmdb, vash_store::BackendKind::Mdbx] {
        for map_size_mb in [64usize, 1024, 4096] {
            let path = dir
                .path()
                .join(format!("{}-{map_size_mb}", backend.as_str()));
            let config = vash_store::StoreConfig {
                path: path.clone(),
                backend,
                map_size: map_size_mb * 1024 * 1024,
                shards: 1,
                ..vash_store::StoreConfig::default()
            };
            // Without the `mdbx` feature the second engine is refused rather
            // than opened, which is the point of that refusal — so say so and
            // carry on rather than panicking in an example.
            let handle = match vash_store::open(&config) {
                Ok(handle) => handle,
                Err(err) => {
                    println!(
                        "{:>8}  {:>8} MiB  skipped: {err}",
                        backend.as_str(),
                        map_size_mb
                    );
                    continue;
                }
            };
            let bytes = dir_bytes(&path);
            handle.close();
            println!(
                "{:>8}  {:>8} MiB  {:>11.1} MiB",
                backend.as_str(),
                map_size_mb,
                bytes as f64 / (1024.0 * 1024.0)
            );
            std::fs::remove_dir_all(&path).ok();
        }
    }
}
