//! Framing fuzzer for `MessageExtractor::extract_messages` (WP-9 Task 3, P2 §4.4/§5.2).
//!
//! `extract_messages` parses the client->backend frontend stream
//! OBSERVATIONALLY (for event logging and pooling/pinning state tracking) — it
//! never alters the forwarded bytes. Historically it discarded the incomplete
//! trailing bytes of a `read()` call, so any message split across two TCP
//! reads was silently lost from state tracking (though the forwarded stream
//! itself was always unaffected). This property test proves that reassembly
//! now makes the fragmented-call-sequence result IDENTICAL to feeding the
//! whole buffer in a single call, for arbitrary cut points — including cuts
//! inside the 5-byte header and inside payloads — and that arbitrary/malformed
//! byte input never panics.

use proptest::prelude::*;
use scry::protocol::{
    Message, MessageExtractor, MSG_BIND, MSG_CLOSE, MSG_EXECUTE, MSG_PARSE, MSG_QUERY, MSG_SYNC,
};

/// Serialize a `Message` back to correctly-framed wire bytes. This is test-only
/// scaffolding (the library has no need to re-serialize frontend messages it
/// only observes), so it lives here rather than in `protocol/`.
fn serialize_message(msg: &Message) -> Vec<u8> {
    match msg {
        Message::Query { query } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(query.as_bytes());
            payload.push(0);
            frame(MSG_QUERY, &payload)
        }
        Message::Parse { name, query, param_oids } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(name.as_bytes());
            payload.push(0);
            payload.extend_from_slice(query.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
            for oid in param_oids {
                payload.extend_from_slice(&oid.to_be_bytes());
            }
            frame(MSG_PARSE, &payload)
        }
        Message::Bind { portal, statement, format_codes, params_raw } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(portal.as_bytes());
            payload.push(0);
            payload.extend_from_slice(statement.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&(format_codes.len() as i16).to_be_bytes());
            for code in format_codes {
                payload.extend_from_slice(&code.to_be_bytes());
            }
            payload.extend_from_slice(&(params_raw.len() as i16).to_be_bytes());
            for param in params_raw {
                match param {
                    Some(bytes) => {
                        payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        payload.extend_from_slice(bytes);
                    }
                    None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            payload.extend_from_slice(&0i16.to_be_bytes()); // 0 result format codes
            frame(MSG_BIND, &payload)
        }
        Message::Execute { portal } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(portal.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&0i32.to_be_bytes()); // max rows: unlimited
            frame(MSG_EXECUTE, &payload)
        }
        Message::Close { kind, name } => {
            let mut payload = Vec::new();
            payload.push(*kind as u8);
            payload.extend_from_slice(name.as_bytes());
            payload.push(0);
            frame(MSG_CLOSE, &payload)
        }
        Message::Sync => frame(MSG_SYNC, &[]),
        Message::Terminate => frame(b'X', &[]),
    }
}

/// Wrap a payload in the standard 1-byte-type + 4-byte-big-endian-length frame.
/// The length field includes itself (4 bytes) but excludes the type byte.
fn frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let length = (payload.len() + 4) as i32;
    let mut out = Vec::with_capacity(1 + payload.len() + 4);
    out.push(msg_type);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Strategy generating a single valid frontend `Message`, mixing the message
/// types real clients send: `Query`, `Parse` (with varying-length SQL),
/// `Bind`, `Execute`, `Sync`, `Close`.
fn message_strategy() -> impl Strategy<Value = Message> {
    prop_oneof![
        "[ -~]{0,80}".prop_map(|query| Message::Query { query }),
        ("[a-z0-9_]{0,10}", "[ -~]{0,300}", prop::collection::vec(any::<u32>(), 0..4))
            .prop_map(|(name, query, param_oids)| Message::Parse { name, query, param_oids }),
        (
            "[a-z0-9_]{0,10}",
            "[a-z0-9_]{0,10}",
            prop::collection::vec(any::<i16>(), 0..3),
            prop::collection::vec(
                prop::option::of(prop::collection::vec(any::<u8>(), 0..20)),
                0..3
            ),
        )
            .prop_map(|(portal, statement, format_codes, params_raw)| Message::Bind {
                portal,
                statement,
                format_codes,
                params_raw,
            }),
        "[a-z0-9_]{0,10}".prop_map(|portal| Message::Execute { portal }),
        Just(Message::Sync),
        (prop_oneof![Just('S'), Just('P')], "[a-z0-9_]{0,10}")
            .prop_map(|(kind, name)| Message::Close { kind, name }),
    ]
}

/// A Vec of 0..30 valid frontend messages, serialized to one correctly-framed
/// byte buffer (as they'd appear across arbitrary TCP read boundaries).
fn message_stream_strategy() -> impl Strategy<Value = (Vec<Message>, Vec<u8>)> {
    prop::collection::vec(message_strategy(), 0..30).prop_map(|msgs| {
        let mut buf = Vec::new();
        for m in &msgs {
            buf.extend_from_slice(&serialize_message(m));
        }
        (msgs, buf)
    })
}

/// Feed `data` to a fresh extractor in one call — the reference framing.
fn reference_extract(data: &[u8]) -> Vec<Message> {
    let extractor = MessageExtractor::new();
    extractor.extract_messages(data)
}

/// Feed `data` to a fresh extractor split at `cuts` (sorted, deduped, clamped
/// into range), concatenating each call's output.
fn fragmented_extract(data: &[u8], mut cuts: Vec<usize>) -> Vec<Message> {
    let extractor = MessageExtractor::new();
    cuts.retain(|&c| c > 0 && c < data.len());
    cuts.sort_unstable();
    cuts.dedup();

    let mut out = Vec::new();
    let mut prev = 0;
    for &cut in &cuts {
        out.extend(extractor.extract_messages(&data[prev..cut]));
        prev = cut;
    }
    out.extend(extractor.extract_messages(&data[prev..]));
    out
}

proptest! {
    /// The central property: fragmenting a correctly-framed frontend byte
    /// stream at ARBITRARY cut points (including inside the 5-byte header and
    /// inside payloads) and feeding the pieces to `extract_messages` across
    /// multiple calls must produce EXACTLY the same message sequence as
    /// feeding the whole buffer in one call. No panics, no dropped messages,
    /// no reordering, no corruption.
    #[test]
    fn fragmented_equals_reference(
        (expected, buf) in message_stream_strategy(),
        cut_points in prop::collection::vec(any::<usize>(), 0..40),
    ) {
        let reference = reference_extract(&buf);
        prop_assert_eq!(&reference, &expected, "reference (single-call) extraction should match the input messages");

        // Map arbitrary usize cut "seeds" into byte offsets within the buffer
        // so proptest can explore any fragmentation, including pathological
        // ones (e.g. splitting every single byte).
        let cuts: Vec<usize> = if buf.is_empty() {
            vec![]
        } else {
            cut_points.iter().map(|c| c % buf.len().max(1)).collect()
        };

        let fragmented = fragmented_extract(&buf, cuts);
        prop_assert_eq!(
            fragmented, reference,
            "fragmented extraction must equal reference extraction regardless of cut points"
        );
    }

    /// Byte-by-byte fragmentation (the most pathological case: every
    /// `extract_messages` call gets exactly one byte) must still reassemble
    /// to the exact reference sequence.
    #[test]
    fn fragmented_byte_by_byte_equals_reference((expected, buf) in message_stream_strategy()) {
        let reference = reference_extract(&buf);
        prop_assert_eq!(&reference, &expected);

        let extractor = MessageExtractor::new();
        let mut out = Vec::new();
        for byte in &buf {
            out.extend(extractor.extract_messages(std::slice::from_ref(byte)));
        }
        prop_assert_eq!(out, reference);
    }

    /// Arbitrary/malformed byte sequences (random bytes, truncated headers)
    /// must never panic. Parsed output is allowed to be empty or partial —
    /// only "no panic" is asserted, since garbage input has no well-defined
    /// reference sequence.
    #[test]
    fn malformed_input_never_panics(chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..50), 0..20)) {
        let extractor = MessageExtractor::new();
        for chunk in &chunks {
            let _ = extractor.extract_messages(chunk);
        }
    }

    /// Truncated / cut-short valid-looking headers (a real message type byte
    /// followed by a random number of length-field bytes) must never panic.
    #[test]
    fn truncated_header_never_panics(
        msg_type in prop_oneof![Just(MSG_QUERY), Just(MSG_PARSE), Just(MSG_BIND), Just(MSG_EXECUTE), Just(MSG_SYNC)],
        header_tail in prop::collection::vec(any::<u8>(), 0..4),
    ) {
        let extractor = MessageExtractor::new();
        let mut data = vec![msg_type];
        data.extend_from_slice(&header_tail);
        let _ = extractor.extract_messages(&data);
    }
}

/// A single message larger than a typical 8KB read buffer must be reassembled
/// correctly when delivered across many reads of that size.
#[test]
fn large_message_across_many_8kb_reads() {
    let big_query = "a".repeat(50_000); // well over an 8KB read buffer
    let expected = Message::Query { query: big_query.clone() };
    let buf = serialize_message(&expected);

    let extractor = MessageExtractor::new();
    let mut out = Vec::new();
    for chunk in buf.chunks(8192) {
        out.extend(extractor.extract_messages(chunk));
    }

    assert_eq!(out, vec![expected]);
}

/// Sanity check that the reference/fragmented harness itself is exercising
/// real reassembly, not accidentally passing because both sides are empty.
#[test]
fn reference_extraction_is_nontrivial_for_known_stream() {
    let msgs = vec![
        Message::Parse { name: "s1".into(), query: "SELECT 1".into(), param_oids: vec![] },
        Message::Bind {
            portal: "".into(),
            statement: "s1".into(),
            format_codes: vec![],
            params_raw: vec![],
        },
        Message::Execute { portal: "".into() },
        Message::Sync,
    ];
    let mut buf = Vec::new();
    for m in &msgs {
        buf.extend_from_slice(&serialize_message(m));
    }

    let reference = reference_extract(&buf);
    assert_eq!(reference, msgs);

    // Fragment at every header/payload boundary explicitly.
    let cuts: Vec<usize> = (1..buf.len()).collect();
    let fragmented = fragmented_extract(&buf, cuts);
    assert_eq!(fragmented, reference);
}
