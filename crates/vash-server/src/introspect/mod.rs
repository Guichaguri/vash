//! What was slow, and which key is hot.
//!
//! Proposed in `docs/slowlog-proposal.md`. This is **phase 0**: the
//! instrumentation and nothing that reads it, so §7's gates have something to
//! measure before any surface is built on top of it.
//!
//! # Why the whole thing is behind a feature
//!
//! §7 states the gates as a percentage against *the same build with these
//! compiled out*, rather than against a historical number, because run-to-run
//! variance on this project's benchmark box is around ±25% and a 1% regression
//! cannot be seen through that. Two binaries measured back to back can be
//! compared; a binary measured against last week's README cannot.
//!
//! # The shape of the seam
//!
//! One type with one set of methods, whose bodies vanish without the feature.
//! Not two divergent APIs behind a `cfg`, because the call sites in
//! [`crate::dispatch`] are on the hottest path in the program and the version
//! that ships by default must be the one that is read every day.

#[cfg(feature = "introspect")]
pub mod hotkeys;
#[cfg(feature = "introspect")]
pub mod slowlog;

use vash_core::{Command, Key};

use crate::metrics::{CommandKind, Dialect};

/// The slowlog and the hot-key sampler, as one thing dispatch holds.
pub struct Introspect {
    #[cfg(feature = "introspect")]
    pub slowlog: slowlog::Slowlog,
    #[cfg(feature = "introspect")]
    pub hotkeys: hotkeys::HotKeys,
}

impl Introspect {
    #[cfg(feature = "introspect")]
    pub fn new(config: &crate::config::IntrospectConfig) -> Self {
        Self {
            slowlog: slowlog::Slowlog::new(config.slowlog_threshold_us, config.slowlog_max_len),
            hotkeys: hotkeys::HotKeys::new(
                if config.hotkeys_enabled {
                    config.hotkeys_sample_rate
                } else {
                    0
                },
                config.hotkeys_counters,
            ),
        }
    }

    #[cfg(not(feature = "introspect"))]
    pub fn new(_config: &crate::config::IntrospectConfig) -> Self {
        Self {}
    }

    /// Records one executed command.
    ///
    /// Called from [`crate::dispatch::execute`] with the duration it already
    /// measured for the histogram, so nothing here reads a clock on the common
    /// path.
    ///
    /// `total` is the whole of `execute_inner`, which for a write *includes* the
    /// shard queue wait. The entry stores the two disjoint — `queue_us` as the
    /// writer reported it and `exec_us` as the remainder — and the threshold is
    /// compared against their sum, which is `total`. §3.1.
    #[inline]
    pub fn observed(
        &self,
        _command: &Command<'_>,
        _kind: CommandKind,
        _dialect: Dialect,
        _total: std::time::Duration,
    ) {
        // Taken on every command, not only on a slow one: reading **clears**,
        // and a wait left in the slot would be charged to whatever this thread
        // ran next. See `vash_store::queue_wait`.
        let _queue_us = vash_store::queue_wait::take();

        #[cfg(feature = "introspect")]
        {
            let write = !is_read(_command);
            for_each_key(_command, |key| self.hotkeys.observe(key, write));

            let total_us = _total.as_micros().min(u64::MAX as u128) as u64;
            if self.slowlog.crosses(total_us) {
                let key = first_key(_command);
                self.slowlog.record(slowlog::Record {
                    queue_us: _queue_us.min(total_us),
                    exec_us: total_us.saturating_sub(_queue_us),
                    command: _kind,
                    dialect: _dialect,
                    keys: key_count(_command),
                    key,
                    value_bytes: value_bytes(_command),
                    batch: 1,
                });
            }
        }
    }

    /// Records one fused single-key read.
    ///
    /// [`crate::dispatch::execute_get_into`] renders straight out of the store
    /// and never builds a `Command`, so it cannot go through
    /// [`Self::observed`]. A read has no queue wait to report.
    #[inline]
    pub fn observed_get(
        &self,
        _key: Key<'_>,
        _kind: CommandKind,
        _dialect: Dialect,
        _total: std::time::Duration,
    ) {
        #[cfg(feature = "introspect")]
        {
            self.hotkeys.observe(_key.as_bytes(), false);

            let total_us = _total.as_micros().min(u64::MAX as u128) as u64;
            if self.slowlog.crosses(total_us) {
                self.slowlog.record(slowlog::Record {
                    queue_us: 0,
                    exec_us: total_us,
                    command: _kind,
                    dialect: _dialect,
                    keys: 1,
                    key: _key.as_bytes(),
                    value_bytes: 0,
                    batch: 1,
                });
            }
        }
    }

    /// Clears the queue wait a completed batch published without consuming it.
    ///
    /// The awaited `set_many` path does not pass through [`Self::observed`], so
    /// without this its wait would sit in the slot and be charged to the next
    /// command this thread ran. Phase 1 gives the batch an entry of its own and
    /// this becomes a use rather than a discard — §3.4.
    #[inline]
    pub fn discard_queue_wait(&self) {
        let _ = vash_store::queue_wait::take();
    }
}

/// Whether a command only reads. Mirrors `dispatch::is_read`, which is private
/// to that module and describes the same set for a different purpose.
#[cfg(feature = "introspect")]
fn is_read(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::Get { .. }
            | Command::GetMany(_)
            | Command::GetAndTouch { .. }
            | Command::Deadline { .. }
            | Command::Deadlines(_)
    )
}

/// Offers every key a command names to `f`.
///
/// **Every** key, not just the first: a `GET_MANY` of a hundred keys is a
/// hundred key lookups, and counting it as one would under-report exactly the
/// multi-get workloads that make a key hot. This runs only on the one request in
/// `sample_rate` the sampler admits, so the cost is amortised away — §4.2.
#[cfg(feature = "introspect")]
fn for_each_key<'a>(command: &'a Command<'a>, mut f: impl FnMut(&'a [u8])) {
    match command {
        Command::Get { key } | Command::Deadline { key } | Command::Delete { key } => {
            f(key.as_bytes())
        }
        Command::Touch { key, .. } | Command::Expire { key, .. } | Command::Append { key, .. } => {
            f(key.as_bytes())
        }
        Command::GetMany(keys) | Command::GetAndTouch { keys, .. } => {
            keys.iter().for_each(|key| f(key.as_bytes()))
        }
        Command::Deadlines(keys) | Command::DeleteMany(keys) => {
            keys.iter().for_each(|key| f(key.as_bytes()))
        }
        Command::Set(set) => f(set.key.as_bytes()),
        Command::SetMany { sets, .. } => sets.iter().for_each(|set| f(set.key.as_bytes())),
        Command::Arithmetic(op) => f(op.key.as_bytes()),
        // Named no key, or named a tag rather than a key. `DELETE_BY_TAG`
        // reaches keys the caller cannot name, which is precisely why it has
        // none to offer here.
        Command::Hello { .. }
        | Command::Ping
        | Command::DeleteByTag { .. }
        | Command::Flush
        | Command::TagSync { .. }
        | Command::Cluster
        | Command::ListKeys(_)
        | Command::ListTags(_)
        | Command::Stats
        | Command::Version
        | Command::Quit => {}
    }
}

/// The first key a command names, or empty for one that names none.
#[cfg(feature = "introspect")]
fn first_key<'a>(command: &'a Command<'a>) -> &'a [u8] {
    let mut first: &[u8] = b"";
    for_each_key(command, |key| {
        if first.is_empty() {
            first = key;
        }
    });
    first
}

#[cfg(feature = "introspect")]
fn key_count(command: &Command<'_>) -> u32 {
    let mut count = 0u32;
    for_each_key(command, |_| count = count.saturating_add(1));
    count
}

/// Value bytes this command wrote, which is what makes a slow write slow when
/// it is not the queue.
#[cfg(feature = "introspect")]
fn value_bytes(command: &Command<'_>) -> u64 {
    match command {
        Command::Set(set) => set.value.len() as u64,
        Command::SetMany { sets, .. } => sets.iter().map(|set| set.value.len() as u64).sum(),
        Command::Append { suffix, .. } => suffix.len() as u64,
        _ => 0,
    }
}

#[cfg(all(test, feature = "introspect"))]
mod tests {
    use super::*;
    use vash_core::KeyList;

    fn key(bytes: &[u8]) -> Key<'_> {
        Key::new(bytes).expect("a valid key")
    }

    #[test]
    fn every_key_of_a_multi_get_is_offered() {
        let keys = vec![key(b"a"), key(b"b"), key(b"c")];
        let command = Command::GetMany(KeyList::Many(keys));

        let mut seen: Vec<Vec<u8>> = Vec::new();
        for_each_key(&command, |k| seen.push(k.to_vec()));

        assert_eq!(seen, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(key_count(&command), 3);
        assert_eq!(first_key(&command), b"a");
    }

    /// `DELETE_BY_TAG` reaches keys the caller never named, so there is nothing
    /// honest to attribute the request to.
    #[test]
    fn a_command_naming_no_key_offers_none() {
        let command = Command::DeleteByTag { tag: b"users" };
        let mut count = 0;
        for_each_key(&command, |_| count += 1);
        assert_eq!(count, 0);
        assert_eq!(first_key(&command), b"");
        assert_eq!(key_count(&command), 0);
    }

    #[test]
    fn value_bytes_sums_a_batch_and_ignores_a_read() {
        let set = |k: &'static [u8], v: &'static [u8]| vash_core::Set {
            key: key(k),
            value: v,
            ttl: vash_core::TtlChange::Keep,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
            return_previous: false,
        };
        let many = Command::SetMany {
            sets: vec![set(b"a", b"1234"), set(b"b", b"567")],
            guard: vash_core::BatchGuard::Always,
        };
        assert_eq!(value_bytes(&many), 7);
        assert_eq!(value_bytes(&Command::Get { key: key(b"a") }), 0);
    }
}
