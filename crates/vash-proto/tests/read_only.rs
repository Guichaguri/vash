//! The three inline-safety predicates must agree.
//!
//! Each dialect decides for itself whether a request may be run on a runtime
//! worker, because each has to decide it at a different moment: VCP from an
//! opcode byte before the body is decoded, memcached and Redis from a parsed
//! command. Three judgements about one fact is exactly the shape that drifts.
//!
//! **What drift would cost.** A request wrongly called inline-safe is executed
//! on an async runtime worker rather than the blocking pool. If it then reaches
//! the writer queue, or simply takes a long time, it blocks that worker and
//! every other connection the worker serves — the failure the whole
//! network/storage split exists to prevent. It only bites with
//! `store.inline_reads` enabled, which is why it has never been noticed; these
//! tests are what stop it being noticed the hard way.
//!
//! The canonical answer is [`vash_core::Command::inline_safe`]. Everything here
//! checks a shortcut against it. Writing them found a real divergence on the
//! first run: the listing opcodes never write, so a "does it write" reading of
//! the question called them safe, while the opcode table had always — correctly
//! — refused them. That is what renamed the predicate.

use vash_core::Command;
use vash_proto::vcp::Opcode;

/// A representative command for each opcode, for comparing the byte-level
/// shortcut against the domain's answer.
///
/// Every opcode is listed. Adding one without listing it fails to compile,
/// because the `match` is exhaustive — which is the point of writing it as a
/// match rather than a table.
fn command_for(opcode: Opcode) -> Option<Command<'static>> {
    let key = || vash_core::Key::new(b"k").expect("valid");
    let request = || vash_core::ListRequest {
        limit: 1,
        cursor: &[],
        pattern: b"",
    };
    Some(match opcode {
        Opcode::Hello => Command::Hello {
            protocol_version: 1,
        },
        Opcode::Ping => Command::Ping,
        Opcode::Stats => Command::Stats,
        Opcode::Cluster => Command::Cluster,
        Opcode::Get => Command::Get { key: key() },
        Opcode::GetMany => Command::GetMany(vec![key()]),
        Opcode::Set => Command::Set(vash_core::Set::plain(key(), b"v", 0)),
        Opcode::SetMany => Command::SetMany {
            sets: vec![vash_core::Set::plain(key(), b"v", 0)],
            guard: vash_core::BatchGuard::Always,
        },
        Opcode::Delete => Command::Delete { key: key() },
        Opcode::DeleteMany => Command::DeleteMany(vec![key()]),
        Opcode::Touch => Command::Touch {
            key: key(),
            ttl_secs: 1,
        },
        Opcode::DeleteByTag => Command::DeleteByTag { tag: b"t" },
        Opcode::Flush => Command::Flush,
        Opcode::TagSync => Command::TagSync {
            full: true,
            entries: Vec::new(),
        },
        Opcode::ListKeys => Command::ListKeys(request()),
        Opcode::ListTags => Command::ListTags(request()),
        // Authentication is a property of a connection, not an operation on the
        // cache, so the domain has no command for it — and `Opcode::inline_safe`
        // answers `false`, which is the safe direction regardless.
        Opcode::Auth => return None,
    })
}

const EVERY_OPCODE: &[Opcode] = &[
    Opcode::Hello,
    Opcode::Ping,
    Opcode::Auth,
    Opcode::Stats,
    Opcode::Cluster,
    Opcode::Get,
    Opcode::Set,
    Opcode::Delete,
    Opcode::Touch,
    Opcode::GetMany,
    Opcode::SetMany,
    Opcode::DeleteMany,
    Opcode::DeleteByTag,
    Opcode::Flush,
    Opcode::TagSync,
    Opcode::ListKeys,
    Opcode::ListTags,
];

#[test]
fn the_opcode_table_lists_every_opcode() {
    // Guards the two tests below from silently skipping a new opcode: the
    // constant is hand-written, so it has to be checked against the decoder,
    // which is not.
    for byte in 0u8..=255 {
        if let Some(opcode) = Opcode::from_u8(byte) {
            assert!(
                EVERY_OPCODE.contains(&opcode),
                "{opcode:?} (0x{byte:02x}) is missing from EVERY_OPCODE"
            );
        }
    }
}

#[test]
fn the_vcp_opcode_shortcut_agrees_with_the_domain() {
    for &opcode in EVERY_OPCODE {
        let Some(command) = command_for(opcode) else {
            continue;
        };
        assert_eq!(
            opcode.inline_safe(),
            command.inline_safe(),
            "{opcode:?} is classified differently by the opcode table and by the command"
        );
    }
}

#[test]
fn no_opcode_is_inline_safe_by_accident() {
    // The direction that matters. A write called inline-safe stalls a runtime
    // worker; a read called unsafe is merely slower, so this asserts the
    // dangerous direction explicitly rather than relying on the equality above
    // to have covered it.
    for &opcode in EVERY_OPCODE {
        if !opcode.inline_safe() {
            continue;
        }
        let command = command_for(opcode).expect("an inline-safe opcode has a command");
        assert!(
            command.inline_safe(),
            "{opcode:?} claims to be inline-safe but its command is not"
        );
    }
}
