//! The libmdbx entry points this crate uses, and nothing else.
//!
//! Hand-written rather than generated. The [`Backend`] trait needs about
//! fifteen functions, three structs and a dozen constants; running `bindgen`
//! over a 340 KB header to get them would add a build-time toolchain
//! requirement — and the crates that ship *pre-generated* bindings instead do
//! not all cover Windows. See `build.rs` for the rest of that argument.
//!
//! **The one structure layout that has to be right is checked at runtime by
//! libmdbx itself.** [`Envinfo`] is a declared prefix of mdbx's own
//! `MDBX_envinfo`, and `mdbx_env_info_ex` takes the caller's `size_of` and
//! refuses anything that is not one of four known prefix lengths. So a mistake
//! here is `MDBX_EINVAL` on the first call rather than silently misread memory;
//! `backend::mdbx::tests::env_info_layout_matches_the_c_library` is the test
//! that makes that failure immediate.
//!
//! [`Backend`]: crate::backend::Backend

#![allow(non_camel_case_types)]

use std::ffi::{c_char, c_int, c_uint, c_void};

pub type MDBX_env = c_void;
pub type MDBX_txn = c_void;
pub type MDBX_cursor = c_void;
pub type MDBX_dbi = u32;

/// `mdbx_mode_t` is the POSIX file mode, and Windows narrows it.
#[cfg(windows)]
pub type mdbx_mode_t = u16;
#[cfg(not(windows))]
pub type mdbx_mode_t = u32;

/// mdbx's `MDBX_val`: a pointer and a length, in that order.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Val {
    pub base: *mut c_void,
    pub len: usize,
}

impl Val {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            base: bytes.as_ptr() as *mut c_void,
            len: bytes.len(),
        }
    }

    pub fn empty() -> Self {
        Self {
            base: std::ptr::null_mut(),
            len: 0,
        }
    }

    /// The bytes this value points at.
    ///
    /// # Safety
    ///
    /// The caller must know the value came from a successful mdbx read and that
    /// the transaction it was read in is still open. mdbx points straight into
    /// the memory map, so the bytes live exactly as long as that transaction —
    /// which is the borrow the [`crate::backend::ReadTxn`] signatures encode.
    pub unsafe fn as_slice<'t>(&self) -> &'t [u8] {
        if self.base.is_null() {
            return &[];
        }
        // SAFETY: the caller's contract, above.
        unsafe { std::slice::from_raw_parts(self.base as *const u8, self.len) }
    }
}

/// `MDBX_stat`, in full — it is small and stable.
#[repr(C)]
#[derive(Default)]
pub struct Stat {
    pub psize: u32,
    pub depth: u32,
    pub branch_pages: u64,
    pub leaf_pages: u64,
    pub overflow_pages: u64,
    pub entries: u64,
    pub mod_txnid: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct Geo {
    pub lower: u64,
    pub upper: u64,
    pub current: u64,
    pub shrink: u64,
    pub grow: u64,
}

/// The prefix of `MDBX_envinfo` up to (not including) `mi_bootid`.
///
/// mdbx accepts exactly four lengths for this structure — the whole thing, or
/// the prefixes ending at `mi_bootid`, `mi_pgop_stat` and `mi_dxbid` — so
/// declaring the shortest one that carries what this crate needs is both
/// supported and self-checking. Everything after this point is page-operation
/// statistics and boot identifiers that nothing here reads.
#[repr(C)]
#[derive(Default)]
pub struct Envinfo {
    pub geo: Geo,
    pub mapsize: u64,
    pub last_pgno: u64,
    pub recent_txnid: u64,
    pub latter_reader_txnid: u64,
    pub self_latter_reader_txnid: u64,
    pub meta_txnid: [u64; 3],
    pub meta_sign: [u64; 3],
    pub maxreaders: u32,
    pub numreaders: u32,
    pub dxb_pagesize: u32,
    pub sys_pagesize: u32,
}

// ---- environment flags --------------------------------------------------

pub const MDBX_SYNC_DURABLE: u32 = 0;
pub const MDBX_NOMETASYNC: u32 = 0x40000;
pub const MDBX_SAFE_NOSYNC: u32 = 0x10000;
pub const MDBX_WRITEMAP: u32 = 0x80000;
/// LIFO recycling of the garbage list, which reuses pages still hot in the
/// write-back cache. See `docs/mdbx-proposal.md` §7.
pub const MDBX_LIFORECLAIM: u32 = 0x4000000;
/// **Never set.** Here to be named in the test that asserts its absence:
/// unsetting it is what keeps reader slots thread-local, and every maintained
/// Rust wrapper sets it. Measured at 56× on reads — `docs/mdbx-proposal.md` §Q3.
pub const MDBX_NOSTICKYTHREADS: u32 = 0x200000;

// ---- transaction, database and cursor flags -----------------------------

pub const MDBX_TXN_READWRITE: u32 = 0;
pub const MDBX_TXN_RDONLY: u32 = 0x20000;
pub const MDBX_CREATE: u32 = 0x40000;

pub const MDBX_FIRST: c_int = 0;
pub const MDBX_NEXT: c_int = 8;
pub const MDBX_SET_RANGE: c_int = 17;

// ---- options ------------------------------------------------------------

pub const MDBX_OPT_MAX_DB: c_int = 0;
pub const MDBX_OPT_MAX_READERS: c_int = 1;

// ---- warm-up ------------------------------------------------------------

pub const MDBX_WARMUP_FORCE: u32 = 1;
pub const MDBX_WARMUP_LOCK: u32 = 4;

// ---- results ------------------------------------------------------------

pub const MDBX_SUCCESS: c_int = 0;
pub const MDBX_RESULT_TRUE: c_int = -1;
pub const MDBX_NOTFOUND: c_int = -30798;
pub const MDBX_MAP_FULL: c_int = -30792;
pub const MDBX_TXN_FULL: c_int = -30788;

unsafe extern "C" {
    pub fn mdbx_env_create(penv: *mut *mut MDBX_env) -> c_int;
    pub fn mdbx_env_set_option(env: *mut MDBX_env, option: c_int, value: u64) -> c_int;
    pub fn mdbx_env_set_geometry(
        env: *mut MDBX_env,
        size_lower: isize,
        size_now: isize,
        size_upper: isize,
        growth_step: isize,
        shrink_threshold: isize,
        pagesize: isize,
    ) -> c_int;
    pub fn mdbx_env_open(
        env: *mut MDBX_env,
        pathname: *const c_char,
        flags: u32,
        mode: mdbx_mode_t,
    ) -> c_int;
    pub fn mdbx_env_close_ex(env: *mut MDBX_env, dont_sync: bool) -> c_int;
    pub fn mdbx_env_sync_ex(env: *mut MDBX_env, force: bool, nonblock: bool) -> c_int;
    pub fn mdbx_env_info_ex(
        env: *const MDBX_env,
        txn: *const MDBX_txn,
        info: *mut Envinfo,
        bytes: usize,
    ) -> c_int;
    pub fn mdbx_env_warmup(
        env: *const MDBX_env,
        txn: *const MDBX_txn,
        flags: u32,
        timeout_seconds_16dot16: c_uint,
    ) -> c_int;

    pub fn mdbx_txn_begin_ex(
        env: *mut MDBX_env,
        parent: *mut MDBX_txn,
        flags: u32,
        txn: *mut *mut MDBX_txn,
        context: *mut c_void,
    ) -> c_int;
    pub fn mdbx_txn_commit_ex(txn: *mut MDBX_txn, latency: *mut c_void) -> c_int;
    pub fn mdbx_txn_abort(txn: *mut MDBX_txn) -> c_int;

    pub fn mdbx_dbi_open(
        txn: *mut MDBX_txn,
        name: *const c_char,
        flags: u32,
        dbi: *mut MDBX_dbi,
    ) -> c_int;
    pub fn mdbx_dbi_stat(
        txn: *const MDBX_txn,
        dbi: MDBX_dbi,
        stat: *mut Stat,
        bytes: usize,
    ) -> c_int;
    /// `del = false` empties the sub-database and keeps the handle; `true`
    /// would delete the handle too, which nothing here wants.
    pub fn mdbx_drop(txn: *mut MDBX_txn, dbi: MDBX_dbi, del: bool) -> c_int;

    pub fn mdbx_get(txn: *const MDBX_txn, dbi: MDBX_dbi, key: *const Val, data: *mut Val) -> c_int;
    pub fn mdbx_put(
        txn: *mut MDBX_txn,
        dbi: MDBX_dbi,
        key: *const Val,
        data: *mut Val,
        flags: u32,
    ) -> c_int;
    pub fn mdbx_del(txn: *mut MDBX_txn, dbi: MDBX_dbi, key: *const Val, data: *const Val) -> c_int;

    pub fn mdbx_cursor_open(
        txn: *const MDBX_txn,
        dbi: MDBX_dbi,
        cursor: *mut *mut MDBX_cursor,
    ) -> c_int;
    pub fn mdbx_cursor_close(cursor: *mut MDBX_cursor);
    pub fn mdbx_cursor_get(
        cursor: *mut MDBX_cursor,
        key: *mut Val,
        data: *mut Val,
        op: c_int,
    ) -> c_int;

    pub fn mdbx_strerror(errnum: c_int) -> *const c_char;
}
