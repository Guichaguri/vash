//! Compiles the vendored libmdbx, and only when the `mdbx` feature asks for it.
//!
//! The default build does nothing here: LMDB comes from `lmdb-master-sys` via
//! `heed`, which has a build script of its own.
//!
//! **Why vendored C and a hand-written FFI rather than a wrapper crate.** Every
//! maintained Rust wrapper for libmdbx sets `MDBX_NOSTICKYTHREADS`
//! unconditionally, which measured 56× worse on reads in this project's own
//! benchmark — see `docs/mdbx-proposal.md` §Q3. The one whose licence matched
//! also ships an empty `bindings_windows.rs`. Owning the build is also what
//! makes the musl fix below possible at all. The `Backend` trait needs about
//! fifteen entry points; that is a smaller surface to maintain than a fork of a
//! wrapper would be.

fn main() {
    println!("cargo:rerun-if-changed=vendor/libmdbx/mdbx.c");
    println!("cargo:rerun-if-changed=build.rs");

    if std::env::var_os("CARGO_FEATURE_MDBX").is_none() {
        return;
    }

    let mut cc = cc::Build::new();
    cc.flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-function")
        // Matches what `signet-mdbx-sys` compiles with, so the library behaves
        // the way every other Rust consumer's does.
        .define("MDBX_BUILD_FLAGS", "\"vash\"")
        .define("MDBX_DEBUG", "0")
        .define("NDEBUG", None);

    // **musl cannot link mdbx's runtime SIMD dispatch.** mdbx picks between
    // SSE2, AVX2 and AVX-512 implementations of its free-list scan using GCC's
    // `__builtin_cpu_supports`, and musl's libgcc does not carry the CPU-model
    // symbols that emits:
    //
    //     undefined reference to `__cpu_indicator_init_local'
    //     undefined reference to `__cpu_model'
    //
    // glibc and MSVC are unaffected. musl is not optional here — the static
    // binary is a CI gate and the Docker image is `scratch` — so the dispatch
    // is turned off for that target, leaving the compile-time choice, which on
    // x86-64 is still SSE2.
    //
    // It has to be set by whoever compiles `mdbx.c`; adding `-lgcc` downstream
    // does not work. That is one of the reasons this build script exists rather
    // than a `-sys` dependency. See `docs/mdbx-proposal.md` §Q1.
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("musl") {
        cc.define("MDBX_HAVE_BUILTIN_CPU_SUPPORTS", "0");
    }

    cc.file("vendor/libmdbx/mdbx.c").compile("mdbx");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        // mdbx reaches into the native API for file locking and for the
        // volatile-state checks its Windows durability rests on.
        for lib in ["ntdll", "user32", "kernel32", "advapi32", "ole32"] {
            println!("cargo:rustc-link-lib=dylib={lib}");
        }
    }
}
