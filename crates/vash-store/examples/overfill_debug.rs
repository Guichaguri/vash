//! Diagnostic for capacity behaviour: writes far more than the map holds and
//! reports what the watermarks and the evictor actually did.
//!
//! ```text
//! cargo run -p vash-store --example overfill_debug
//! ```

use vash_core::{Key, Set};
use vash_store::{LmdbStore, Store, StoreConfig, StoreError, WriteConfig};

fn main() {
    let map_mb: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(2);
    println!("map size: {map_mb} MiB");

    let dir = tempfile::tempdir().unwrap();
    let store = LmdbStore::open(&StoreConfig {
        path: dir.path().join("db"),
        map_size: map_mb * 1024 * 1024,
        write: WriteConfig {
            sweep_interval_ms: 1,
            ..WriteConfig::default()
        },
        ..StoreConfig::default()
    })
    .unwrap();

    let value = vec![b'x'; 4096];
    let mut accepted = 0usize;
    let mut refused = 0usize;

    println!(
        "{:>6}  {:>8} {:>8}  {:>9} {:>8} {:>10}  pressure",
        "i", "accepted", "refused", "util", "entries", "evicted"
    );

    for i in 0..2_000u32 {
        let key = format!("k{i}");
        match store.set(&Set::plain(Key::new(key.as_bytes()).unwrap(), &value, 0)) {
            Ok(_) => accepted += 1,
            Err(StoreError::CapacityFull) => refused += 1,
            Err(e) => {
                println!("write {i} failed: {e}");
                break;
            }
        }

        if i % 100 == 0 {
            let s = store.stats().unwrap();
            println!(
                "{i:>6}  {accepted:>8} {refused:>8}  {:>9.4} {:>8} {:>10}  {}",
                s.utilisation, s.entries, s.evicted, s.pressure
            );
        }
    }

    let s = store.stats().unwrap();
    println!();
    println!(
        "final: accepted {accepted}, refused {refused}, entries {}, evicted {}, util {:.4}, pressure {}",
        s.entries, s.evicted, s.utilisation, s.pressure
    );
    println!("used {} of {} bytes", s.used_bytes, s.map_size);
    store.close();
}
