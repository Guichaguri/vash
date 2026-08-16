//! What has to hold about *choosing* an engine, rather than about storage.
//!
//! Everything here needs both backends compiled, so the whole file is behind
//! the `mdbx` feature. The storage invariants themselves live in `store.rs`,
//! which runs against whichever engine is built — see its header.

#![cfg(feature = "mdbx")]

use vash_core::{Key, Set, SetMode, TtlChange};
use vash_store::{BackendKind, Store, StoreConfig};

fn config(path: &std::path::Path, backend: BackendKind) -> StoreConfig {
    StoreConfig {
        path: path.to_path_buf(),
        backend,
        map_size: 64 * 1024 * 1024,
        ..StoreConfig::default()
    }
}

fn set(store: &dyn Store, key: &[u8], value: &[u8]) {
    store
        .set(&Set {
            key: Key::new(key).unwrap(),
            value,
            ttl: TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: SetMode::Set,
        })
        .unwrap();
}

/// The one thing a wrong `store.backend` must never do is look like it worked.
///
/// mdbx is a fork of LMDB's *design*, not of its file format, so an LMDB
/// directory opened as mdbx would create a second, empty database beside the
/// first: the data would still be on disk, occupying space, while every read
/// missed. That is the same silent, total cache loss the shard-count check
/// exists to prevent, and it gets the same answer.
#[test]
fn a_database_written_by_one_engine_is_refused_by_the_other() {
    for (wrote, then_opened_as) in [
        (BackendKind::Lmdb, BackendKind::Mdbx),
        (BackendKind::Mdbx, BackendKind::Lmdb),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");

        let handle = vash_store::open(&config(&path, wrote)).unwrap();
        set(handle.store().as_ref(), b"k", b"v");
        handle.close();

        let Err(err) = vash_store::open(&config(&path, then_opened_as)) else {
            panic!(
                "a {} database opened as {} and did not complain",
                wrote.as_str(),
                then_opened_as.as_str()
            );
        };

        // The operator has to be told what to do about it, not merely that
        // something is wrong: the fix is a wipe, and nothing else will work.
        let message = err.to_string();
        assert!(
            message.contains(wrote.as_str()),
            "the error should name the engine that wrote the database: {message}"
        );
        assert!(
            message.contains("wipe"),
            "the error should say a wipe is the fix: {message}"
        );
    }
}

/// Reopening the same directory with the same engine must keep working — the
/// refusal above has to be about the *other* engine's files, not about any
/// file being present.
#[test]
fn reopening_with_the_same_engine_keeps_the_data() {
    for backend in [BackendKind::Lmdb, BackendKind::Mdbx] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");

        let handle = vash_store::open(&config(&path, backend)).unwrap();
        set(handle.store().as_ref(), b"k", b"v");
        handle.close();

        let handle = vash_store::open(&config(&path, backend)).unwrap();
        let found = handle.store().get(Key::new(b"k").unwrap()).unwrap();
        assert_eq!(
            found.map(|v| v.data.to_vec()),
            Some(b"v".to_vec()),
            "{} lost its data across a reopen",
            backend.as_str()
        );
        handle.close();
    }
}

/// Pinning the map has to work where the platform allows it, and must never
/// claim to have worked where it did not.
///
/// `store.resident_mode` reads exactly this bit before it takes reads off the
/// blocking pool, so a false negative silently costs the largest read win this
/// project has measured — GET went from 17,443 to 173,659 closed loop when it
/// engaged — and a false positive would put page faults on a runtime worker.
/// Neither shows up as a failure anywhere else.
///
/// **On Windows this is a regression test with teeth.** `VirtualLock` refuses
/// past the process working-set maximum, so `MdbxBackend::warm` raises it
/// first; without that call, Phase 0 measured `winerror 1453` and this bit
/// coming back false. See `docs/mdbx-proposal.md` §Q4.
#[test]
fn pinning_the_map_does_not_lie() {
    let dir = tempfile::tempdir().unwrap();

    let mut off = config(&dir.path().join("off"), BackendKind::Mdbx);
    off.prefault = true;
    off.lock_map = false;
    let handle = vash_store::open(&off).unwrap();
    assert!(
        !handle.store().map_locked(),
        "reported a pinned map without being asked to pin one"
    );
    handle.close();

    let mut on = config(&dir.path().join("on"), BackendKind::Mdbx);
    on.prefault = true;
    on.lock_map = true;
    let handle = vash_store::open(&on).unwrap();
    let locked = handle.store().map_locked();

    if cfg!(windows) {
        assert!(
            locked,
            "mdbx could not pin the map on Windows; the working-set limit is \
             raised before the warm-up precisely so that it can"
        );
    } else if !locked {
        // Elsewhere the cap is RLIMIT_MEMLOCK, which an unprivileged process
        // cannot raise past its hard limit — a container defaulting to 64 MiB
        // is the normal case, not a bug. Reporting `false` is then the correct
        // answer, which is the half this assertion is really about.
        eprintln!("note: the map was not pinned; RLIMIT_MEMLOCK is probably the reason");
    }
    handle.close();
}

/// `store.preallocate` has to actually reserve the file, and has to stay a
/// no-op for the engine that has nothing to reserve.
///
/// It exists because growth is not free on the write path — batched `lazy`
/// writes measured 0.70x LMDB with a growing file and 0.99x with 64 MiB
/// preallocated — and a knob that quietly did nothing would leave that
/// unfixed while looking fixed. See `docs/benchmarks.md`.
#[test]
fn preallocation_reserves_the_file_up_front() {
    fn bytes_on_disk(path: &std::path::Path) -> u64 {
        std::fs::read_dir(path)
            .expect("read_dir")
            .flatten()
            .map(|e| e.metadata().expect("metadata").len())
            .sum()
    }

    let dir = tempfile::tempdir().unwrap();
    let reserve = 32 * 1024 * 1024;

    let grown = dir.path().join("grown");
    let mut cfg = config(&grown, BackendKind::Mdbx);
    let handle = vash_store::open(&cfg).unwrap();
    let on_demand = bytes_on_disk(&grown);
    handle.close();

    let reserved = dir.path().join("reserved");
    cfg = config(&reserved, BackendKind::Mdbx);
    cfg.preallocate = reserve;
    let handle = vash_store::open(&cfg).unwrap();
    let up_front = bytes_on_disk(&reserved);
    handle.close();

    assert!(
        on_demand < reserve as u64,
        "a store that grows on demand should start far below {reserve} bytes, got {on_demand}"
    );
    assert!(
        up_front >= reserve as u64,
        "preallocating {reserve} bytes left only {up_front} on disk"
    );

    // LMDB has nothing to reserve — it sizes its file to `map_size` at creation
    // either way — so the setting must not change what it does, and above all
    // must not be refused.
    let lmdb = dir.path().join("lmdb");
    let mut cfg = config(&lmdb, BackendKind::Lmdb);
    cfg.preallocate = reserve;
    vash_store::open(&cfg).unwrap().close();
}

/// Both engines have to agree on what they promise the rest of the server.
///
/// Not an exhaustive comparison — `store.rs` is that, run twice — but the
/// handful of numbers other components *branch* on, which would each be a
/// silent behaviour change rather than a failure.
#[test]
fn the_engines_agree_on_what_they_advertise() {
    let mut seen = Vec::new();
    for backend in [BackendKind::Lmdb, BackendKind::Mdbx] {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(&dir.path().join("db"), backend);
        cfg.shards = 2;
        let handle = vash_store::open(&cfg).unwrap();
        let store = handle.store();

        set(store.as_ref(), b"k", b"v");
        let stats = store.stats().unwrap();

        seen.push((
            store.shard_count(),
            stats.shards,
            stats.entries,
            // The input to every capacity watermark. It has to be a fraction of
            // the size the operator configured, on both engines, or `soft`,
            // `hard` and `critical` mean different things depending on which
            // one is running.
            stats.map_size,
            stats.utilisation < 0.5,
        ));
        handle.close();
    }
    assert_eq!(seen[0], seen[1], "the engines advertise different things");
}
