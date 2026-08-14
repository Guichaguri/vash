//! Making a shard's data file resident before it serves a request.
//!
//! Plan §9 puts every read on the blocking pool for one reason: a cold read
//! touches an unmapped page and stalls the calling thread for a disk I/O —
//! ~100 µs on NVMe — and doing that on a runtime worker collapses tail latency
//! for every connection that worker serves. `store.inline_reads` removes the
//! hand-off and is therefore only safe when the working set is already
//! resident, which today is an assertion the operator makes and nothing checks.
//!
//! This module is the server doing something about it instead of asking.
//!
//! # Why not `MAP_POPULATE`, and why not `madvise` on the map
//!
//! Both were the obvious answer and neither is reachable through LMDB:
//!
//! - `MAP_POPULATE` is a flag on `mmap`, and LMDB owns that call. It passes
//!   `MAP_SHARED` and nothing else; there is no environment flag, no
//!   `mdb_env_set_*`, and no build knob that adds one.
//! - `madvise` needs the mapping's address. LMDB's only `madvise` call is the
//!   `MADV_RANDOM` behind `MDB_NORDAHEAD` (`mdb.c:4680`), which is the
//!   *opposite* advice — it exists for databases larger than RAM. And the
//!   address is not exposed: `mdb_env_info` fills `me_mapaddr` from the meta
//!   page (`mdb.c:10916`), which is zero unless the environment was created
//!   with the experimental `MDB_FIXEDMAP`.
//!
//! So the map is warmed through the *file* rather than through the mapping.
//! Reading `data.mdb` sequentially pulls exactly the same physical pages into
//! the OS page cache, and it is the page cache that decides whether a later
//! fault waits on the disk. That is the whole of the 100 µs; what survives is a
//! minor fault, which allocates a page-table entry against a page already in
//! memory and costs well under a microsecond without ever blocking on I/O.
//!
//! On Linux the residual minor faults go too, by finding LMDB's mapping in
//! `/proc/self/maps` and populating it directly — see [`populate_page_tables`].
//! That is a smaller win than it sounds, which is why it is the second step and
//! not the mechanism: the sequential read is what removes the stall, and it
//! works everywhere.
//!
//! # Why not huge pages
//!
//! A 4 GiB map walked at random is a TLB-miss machine, so `MADV_HUGEPAGE` was
//! the obvious next thing to reach for. It was measured rather than assumed,
//! and there is nothing to take: on LMDB's mapping the call **returns success
//! and does nothing**. That is worse than failing — a knob that reports having
//! worked invites an operator to believe TLB pressure has been dealt with.
//!
//! The kernel says so directly. Probed with a 256 MiB mapping of each shape,
//! every page touched, reading `THPeligible` and the huge-page counters back
//! out of `/proc/self/smaps`:
//!
//! | mapping | `madvise` | `THPeligible` | huge pages |
//! |---|---|---|---|
//! | anonymous — control | `0` | 1 | 168 MiB |
//! | file-backed `MAP_SHARED`, ext4 — **what LMDB does** | `0` | 0 | none |
//! | file-backed `MAP_SHARED`, tmpfs | `0` | 0 | none |
//!
//! Transparent huge pages cover anonymous memory and shmem. A shared mapping of
//! a file on an ordinary filesystem is not eligible however it is advised, and
//! the control line is what makes that a fact about the mapping rather than
//! about the probe.
//!
//! Two paths remain, and neither is this crate's to take. tmpfs becomes
//! eligible once `/sys/kernel/mm/transparent_hugepage/shmem_enabled` is moved
//! off its `never` default — root, system-wide, the operator's call, and only
//! relevant to an `ephemeral` store placed on tmpfs. `hugetlbfs` would need the
//! mapping to come from a hugetlbfs file, and LMDB opens and maps its own.
//!
//! # What it buys, measured
//!
//! `examples/prefault_bench` is the measurement, and it is the only honest way
//! to take one: the effect exists only against a cold page cache, so it evicts
//! the data files with `posix_fadvise(DONTNEED)` and checks with `mincore` that
//! they went. On a 5.9 GiB four-shard store, 20,000 random `GET`s:
//!
//! | | cold | prefaulted |
//! |---|---|---|
//! | p50 | 800 µs | **6.6 µs** |
//! | p99 | 2.9 ms – 58 ms | **80 µs** |
//! | throughput | 445 – 1,094 ops/s | **~75,000 ops/s** |
//!
//! Two things to read carefully. The **p99 spread across rounds** — 2.9 ms in
//! one, 58 ms in the next — is not noise to be averaged away; it is the
//! tail-latency collapse plan §9 exists to prevent, appearing and disappearing
//! with the page cache's mood. And the run was on a kernel too old for
//! `MADV_POPULATE_READ`, so **every bit of this came from the sequential read
//! alone**, which is the strongest evidence that the read is the mechanism and
//! the `madvise` below is only a refinement.
//!
//! The absolute figures are inflated: this was WSL2 against a virtual disk,
//! where a cold read costs ~800 µs rather than the ~100 µs plan §9 assumes of
//! NVMe. Read the ratio as a ceiling and the direction as certain.
//!
//! # What it costs
//!
//! Startup time proportional to the data, at sequential-read bandwidth:
//! **~2.2–3.7 s per GiB cold** on the rig above, and ~260 ms per GiB when the
//! file is already cached, which is the copy floor. A 10 GB database is
//! therefore tens of seconds against the sub-second cold start plan §13 asks
//! for, and that is why this is off by default. It buys predictable tail
//! latency for the first requests after a restart, and it is the precondition
//! that makes `inline_reads` an informed choice rather than a gamble.

use std::io::Read;
use std::path::Path;

/// LMDB's data file within an environment directory.
///
/// Fixed rather than discovered: the environment is opened without
/// `MDB_NOSUBDIR`, so LMDB chooses this name and the directory holds only this
/// and `lock.mdb`.
const DATA_FILE: &str = "data.mdb";

/// Read size for the warming pass. Large enough that the per-call overhead
/// disappears against the copy, small enough to stay out of the way.
const CHUNK: usize = 1024 * 1024;

/// Warms one environment's map. Returns the bytes made resident.
///
/// `high_water` is how far into the file LMDB has ever written —
/// `(last_page_number + 1) × page_size` — and it is not optional. **On Linux
/// LMDB creates `data.mdb` at the full map size and leaves it sparse**, so the
/// file's own length is `store.map_size` from the moment the environment is
/// created and says nothing about how much data exists. Reading to that length
/// warms gigabytes of holes.
///
/// This was measured rather than reasoned about, and it was not subtle: on a
/// 4-shard store holding 3 GiB behind a 4 GiB-per-shard map, the file length
/// said 16 GiB. Warming it took 21–31 s, and on a 9 GB machine it filled the
/// page cache with zeroes and evicted the real data on the way past — so the
/// flag cost half a minute of startup to make reads no faster. Bounded here
/// instead, the same store warms ~1.5 GiB per shard of pages that exist.
///
/// Note that the bound is deliberately the high-water mark and **not**
/// [`crate::engine::LmdbEngine::used_bytes_in`], which excludes the free list.
/// Free pages sit interleaved with live ones below the high-water mark; they
/// are real blocks on disk, and reading straight through them costs less than
/// seeking around them.
///
/// Errors are the caller's to log and ignore: everything here is a performance
/// measure, and a database that could not be warmed is still a database that
/// serves correctly.
pub(crate) fn prefault(dir: &Path, high_water: u64) -> std::io::Result<u64> {
    let path = dir.join(DATA_FILE);
    let bytes = warm_page_cache(&path, high_water)?;

    #[cfg(target_os = "linux")]
    populate_page_tables(&path, bytes);

    Ok(bytes)
}

/// Reads the data file up to `limit`, so its pages are in the OS page cache.
///
/// The bytes are read into a buffer and thrown away — the copy is the point of
/// contact, not the data. Sequential, so the device delivers it at streaming
/// bandwidth rather than at the random-access rate a fault-driven warm-up would
/// get.
fn warm_page_cache(path: &Path, limit: u64) -> std::io::Result<u64> {
    let mut file = std::fs::File::open(path)?;
    let mut buf = vec![0u8; CHUNK];
    let mut total = 0u64;
    while total < limit {
        let want = CHUNK.min(usize::try_from(limit - total).unwrap_or(CHUNK));
        match file.read(&mut buf[..want]) {
            // Short file: the environment was created with a larger map than it
            // has ever filled, which is the ordinary case everywhere but Linux.
            Ok(0) => return Ok(total),
            Ok(n) => total += n as u64,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(total)
}

/// Populates this process's page tables over LMDB's mapping, so that even the
/// first touch of a warm page takes no fault at all.
///
/// The mapping is found by inode in `/proc/self/maps` rather than by path:
/// the path in that table is the kernel's own, which a bind mount, a symlink or
/// a directory name containing a space would each render differently, and an
/// inode compares exactly. A mapping the kernel has split into several
/// ranges is handled by advising each one against its own file offset.
///
/// Every failure here is silent and harmless. This is a hint on top of a warm
/// page cache: if the file is not found, the table cannot be read, the kernel is
/// too old, or the range is refused, the caller has already got the part that
/// mattered.
#[cfg(target_os = "linux")]
fn populate_page_tables(path: &Path, warmed: u64) {
    use std::os::unix::fs::MetadataExt;

    /// Prefault a range for reading — `MAP_POPULATE` after the fact, and the
    /// reason this function exists. Linux 5.14+; older kernels reject it and
    /// fall back below. Spelled out rather than taken from `libc`, which has
    /// carried it only since 0.2.113.
    const MADV_POPULATE_READ: libc::c_int = 22;

    if warmed == 0 {
        return;
    }
    let Ok(inode) = std::fs::metadata(path).map(|meta| meta.ino()) else {
        return;
    };
    let Ok(maps) = std::fs::read_to_string("/proc/self/maps") else {
        return;
    };

    for line in maps.lines() {
        // `7f3c1c000000-7f3c9c000000 r--s 00000000 08:03 1234   /data/data.mdb`
        let Some((range, rest)) = line.split_once(' ') else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let (Some(perms), Some(offset), Some(_dev), Some(ino)) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        // LMDB maps the file shared. Checking it keeps this off any private
        // mapping of the same inode that some other part of the process made.
        if !perms.ends_with('s') || ino.parse::<u64>() != Ok(inode) {
            continue;
        }
        let (Some((start, end)), Ok(offset)) =
            (range.split_once('-'), u64::from_str_radix(offset, 16))
        else {
            continue;
        };
        let (Ok(start), Ok(end)) = (
            usize::from_str_radix(start, 16),
            usize::from_str_radix(end, 16),
        ) else {
            continue;
        };
        // The map is a reservation — `store.map_size`, gigabytes of it — and on
        // Linux the file is that long too, sparse above the high-water mark.
        // Neither the reservation nor the holes can be populated: `madvise`
        // gives up when it reaches them rather than populating the part that
        // was real, so an unclamped call is not merely wasteful, it is the
        // difference between this working and doing nothing. Measured —
        // advising a whole 512 MiB reservation over an 8 MiB file is refused
        // outright. Clamped to what was warmed, at this range's own offset,
        // which is also what keeps a mapping the kernel has split into several
        // ranges correct.
        let Some(backed) = warmed.checked_sub(offset).filter(|n| *n > 0) else {
            continue;
        };
        let len = end
            .saturating_sub(start)
            .min(usize::try_from(backed).unwrap_or(usize::MAX));
        if len == 0 {
            continue;
        }

        // SAFETY: the range is one this process's own mapping table just
        // reported, clamped to the file behind it. Both advices are read-only
        // hints — neither writes, frees, or changes what is mapped, and the
        // kernel validates the range and returns an error rather than acting on
        // memory the process does not own.
        let addr = start as *mut libc::c_void;
        let populated = unsafe { libc::madvise(addr, len, MADV_POPULATE_READ) };
        if populated != 0 {
            // Pre-5.14, where `POPULATE_READ` is `EINVAL`. `WILLNEED` is
            // advisory readahead rather than a guarantee and should not be
            // mistaken for one: on a cold 8 MiB file it left 128 of 2048 pages
            // resident, because it queues readahead within the kernel's window
            // and returns without waiting. Nothing is lost by that here — the
            // warming pass above already did the work this is asking for — and
            // it is exactly why the pass is the mechanism and this is the hint.
            unsafe { libc::madvise(addr, len, libc::MADV_WILLNEED) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_data_file(len: usize) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join(DATA_FILE), vec![7u8; len]).expect("write");
        dir
    }

    /// The warm pass covers everything below the high-water mark, over more
    /// than one chunk so the read loop goes round.
    #[test]
    fn warms_up_to_the_high_water_mark() {
        let len = CHUNK + 4096;
        let dir = with_data_file(len);
        assert_eq!(
            prefault(dir.path(), len as u64).expect("prefault"),
            len as u64
        );
    }

    /// **The regression that made the flag worse than useless.** On Linux LMDB
    /// creates `data.mdb` at the full map size and leaves it sparse, so the
    /// file is `store.map_size` long from the moment it exists. Warming to the
    /// file's length read gigabytes of holes, which on a machine smaller than
    /// the map evicted the real data on the way past. Nothing above the mark
    /// may be touched, however long the file is.
    #[test]
    fn stops_at_the_high_water_mark_of_a_sparse_file() {
        let dir = with_data_file(8 * CHUNK);
        let high_water = CHUNK as u64;
        assert_eq!(
            prefault(dir.path(), high_water).expect("prefault"),
            high_water
        );
    }

    /// A mark beyond the file — an environment whose map has never been filled,
    /// which is every platform but Linux — stops at what is actually there.
    #[test]
    fn stops_at_the_end_of_a_short_file() {
        let dir = with_data_file(4096);
        assert_eq!(
            prefault(dir.path(), 8 * CHUNK as u64).expect("prefault"),
            4096
        );
    }

    /// An environment directory without a data file is a startup failure to
    /// report, not one to panic on.
    #[test]
    fn reports_a_missing_data_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(prefault(dir.path(), CHUNK as u64).is_err());
    }
}
