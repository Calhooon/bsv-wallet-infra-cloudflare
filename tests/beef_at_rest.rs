//! NL-7, the witness of the reference form: `internalizeAction` takes a
//! reference `{ r2Key, size, etag, bucket }` to bytes at rest, reads them as a
//! stream (bsv-rs 0.4.1 `verify_stream_async`, one element in hand), asks the
//! header service for every root, and hands the storage the subject alone. A
//! refusal names the offset and the kind. One function per measured reading
//! would race the counting allocator, so the measured readings share one test.

mod support;

use std::io::Read;

use bsv_sdk::transaction::{verify_stream, Verdict};
use rust_wallet_infra::beef_at_rest::{
    parse_internalize_params, read_at_rest, AtRestError, BeefAtRest, InternalizeParams,
};
use rust_wallet_infra::services::chaintracker::HeaderService;
use serde_json::json;
use support::beef_chain::{
    display, funding_root, heap, links_over, mib, subject, ChainSource, Chunked,
    DRAIN_INLINE_BYTES, HEIGHT, ISOLATE_BYTES,
};

#[global_allocator]
static ALLOC: heap::Counting = heap::Counting;

const CHUNK: usize = 64 * 1024;

/// Headers that carry the chain's one root at its height, and count the asks.
struct ChainHeaders {
    asked: std::cell::Cell<u32>,
    carry: bool,
    fail: bool,
}

impl ChainHeaders {
    fn honest() -> Self {
        Self {
            asked: 0.into(),
            carry: true,
            fail: false,
        }
    }
}

impl HeaderService for ChainHeaders {
    async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool, String> {
        self.asked.set(self.asked.get() + 1);
        if self.fail {
            return Err("the header service did not answer".to_string());
        }
        Ok(self.carry && height as u64 == HEIGHT && root == display(&funding_root()))
    }
}

fn reference(size: u64) -> serde_json::Value {
    json!({ "r2Key": "02aa/0b5e.beef", "size": size, "etag": "e-1", "bucket": "bsv-messagebox-beefs" })
}

fn args_with(beef: serde_json::Value) -> serde_json::Value {
    json!({
        "beefAtRest": beef,
        "outputs": [{ "outputIndex": 0, "protocol": "basket insertion",
                      "insertionRemittance": { "basket": "nl7" } }],
        "description": "MessageBox delivery payment",
        "labels": [],
        "seekPermission": false
    })
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

// ---------------------------------------------------------------------------
// The argument
// ---------------------------------------------------------------------------

#[test]
fn the_reference_is_an_argument_of_internalize_action_named_or_positional() {
    let named = args_with(reference(6_200_098));
    let positional = json!([{ "identityKey": "02aa" }, named.clone()]);
    for params in [named, positional] {
        match parse_internalize_params(&params).expect("the reference parses") {
            InternalizeParams::AtRest { reference, args } => {
                assert_eq!(
                    reference,
                    BeefAtRest {
                        r2_key: "02aa/0b5e.beef".to_string(),
                        size: 6_200_098,
                        etag: "e-1".to_string(),
                        bucket: "bsv-messagebox-beefs".to_string(),
                    }
                );
                assert!(args.tx.is_empty(), "no byte of the BEEF is in the argument");
                assert_eq!(args.outputs.len(), 1);
                assert_eq!(args.description, "MessageBox delivery payment");
            }
            InternalizeParams::Inline(_) => panic!("a reference read as inline bytes"),
        }
    }
}

#[test]
fn the_inline_bytes_are_still_an_argument_in_both_shapes() {
    for tx in [json!("0100beef"), json!([1, 0, 190, 239])] {
        let mut args = args_with(json!(null));
        args.as_object_mut().unwrap().remove("beefAtRest");
        args["tx"] = tx;
        match parse_internalize_params(&args).expect("inline parses") {
            InternalizeParams::Inline(args) => assert_eq!(args.tx, vec![1, 0, 190, 239]),
            InternalizeParams::AtRest { .. } => panic!("inline bytes read as a reference"),
        }
    }
}

#[test]
fn a_reference_with_bytes_beside_it_or_neither_or_half_a_reference_is_refused() {
    let mut both = args_with(reference(10));
    both["tx"] = json!("0100beef");
    let mut neither = args_with(json!(null));
    neither.as_object_mut().unwrap().remove("beefAtRest");
    let half = args_with(json!({ "r2Key": "k", "size": 10, "bucket": "b" }));
    let empty_key = args_with(json!({ "r2Key": "", "size": 10, "etag": "e", "bucket": "b" }));
    for (name, params) in [
        ("both", both),
        ("neither", neither),
        ("no etag", half),
        ("empty key", empty_key),
    ] {
        let err = parse_internalize_params(&params).err();
        assert!(err.is_some(), "{name}: accepted");
        println!("{name}: {}", err.unwrap());
    }
}

// ---------------------------------------------------------------------------
// The reading
// ---------------------------------------------------------------------------

/// The relay's two shapes at rest: the 100,000-link payment and the first
/// chain over the relay's 8 MiB line. Neither is ever whole in this process.
#[test]
fn the_payments_at_rest_are_read_one_element_in_hand_and_their_subject_handed_over() {
    for n in [100_000usize, links_over(DRAIN_INLINE_BYTES)] {
        let size = support::beef_chain::atomic_size(n);
        let (raw, txid) = subject(n);
        let headers = ChainHeaders::honest();
        let start = heap::start();
        let mut source = Chunked {
            inner: ChainSource::atomic(n),
            size: CHUNK,
        };
        let read = block_on(read_at_rest(&mut source, &headers, size)).expect("accepted");
        let peak = heap::peak_since(start);
        println!(
            "at rest: {n} links, {size} bytes ({:.2} MiB): peak {:.1} MiB ({:.0} B per element), {} root asked",
            mib(size as usize),
            mib(peak),
            peak as f64 / n as f64,
            headers.asked.get()
        );
        assert_eq!(read.txid, display(&txid));
        assert_eq!(read.raw_tx, raw);
        assert_eq!(read.outputs, vec![(1_000, vec![0x51])]);
        assert!(!read.has_proof, "the subject is unproven");
        assert_eq!(read.bytes_read, size);
        assert_eq!(headers.asked.get(), 1, "the one root, asked once");
        // Amended with the move onto bsv-rs 0.6.0: the bound was a quarter of
        // the isolate. 0.6.0's stream index keeps a proven coinbase's verified
        // height on every entry (`coinbase_height: Option<u64>`,
        // `transaction/beef_stream.rs:1765`), 16 bytes more per transaction,
        // and the index's table rounds to 262,144 slots here: 135,299 links
        // read 27.4 MiB on 0.4.5 and 33.4 MiB on 0.6.0 (213 to 259 B per
        // element). A third of the isolate still holds the claim: never whole.
        assert!(peak < ISOLATE_BYTES / 3, "{n} links: {:.1} MiB", mib(peak));
        if n > 100_000 {
            assert!(size > DRAIN_INLINE_BYTES);
        }
    }
}

#[test]
fn a_proven_subject_is_handed_over_with_its_proof() {
    let size = support::beef_chain::atomic_size(1);
    let mut source = Chunked {
        inner: ChainSource::atomic(1),
        size: 7,
    };
    let read = block_on(read_at_rest(&mut source, &ChainHeaders::honest(), size)).unwrap();
    assert!(read.has_proof);
    assert_eq!(read.txid, display(&funding_root()));
}

/// The library's own verdict on the same bytes, with every root carried.
fn library_verdict(bytes: &[u8]) -> Verdict {
    verify_stream(
        bytes,
        std::collections::HashMap::from([(HEIGHT, funding_root())]),
        None,
    )
    .unwrap()
}

#[test]
fn a_refusal_names_the_offset_and_the_kind_the_library_names() {
    let mut whole = Vec::new();
    ChainSource::atomic(5).read_to_end(&mut whole).unwrap();
    // The last transaction's input names a transaction the BEEF does not hold.
    let mut broken = whole.clone();
    let last_input = whole.len() - (4 + 1 + 32 + 4 + 1 + 4 + 1 + 8 + 1 + 1 + 4) + 5;
    broken[last_input] ^= 0xFF;
    // Cut inside the third transaction.
    let cut = whole[..whole.len() - 70].to_vec();
    for (name, bytes) in [("broken", broken), ("cut", cut)] {
        let Verdict::Invalid { offset, kind, .. } = library_verdict(&bytes) else {
            panic!("{name}: the library accepts it");
        };
        let mut source = Chunked {
            inner: &bytes[..],
            size: 13,
        };
        let err = block_on(read_at_rest(
            &mut source,
            &ChainHeaders::honest(),
            bytes.len() as u64,
        ))
        .unwrap_err();
        println!("{name}: {err}");
        assert_eq!(
            err,
            AtRestError::Refused {
                offset,
                kind: format!("{kind:?}"),
                reason: err.reason()
            }
        );
        assert!(err.to_string().contains(&format!("offset {offset}")));
        assert!(err.to_string().contains(&format!("{kind:?}")));
    }
}

/// Added with the move onto bsv-rs 0.6.0: an unproven subject with a nonzero
/// lock time and a non-final sequence needs the mining tip (the node's
/// `IsFinalTx`), and none is supplied here. 0.6.0 declines with
/// `Verdict::Unverifiable`; the Worker names it a decline, never a refusal of
/// the bytes, and writes nothing (on 0.4.5 this BEEF was accepted).
#[test]
fn a_subject_that_needs_the_mining_tip_is_declined_not_refused() {
    let mut raw = support::beef_chain::spend(&funding_root(), 1);
    let sequence_at = 4 + 1 + 32 + 4 + 1;
    raw[sequence_at..sequence_at + 4].copy_from_slice(&0u32.to_le_bytes());
    let lock_at = raw.len() - 4;
    raw[lock_at..].copy_from_slice(&500u32.to_le_bytes());
    let txid = bsv_sdk::primitives::sha256d(&raw);
    let mut bytes = 0x0101_0101u32.to_le_bytes().to_vec();
    bytes.extend_from_slice(&txid);
    bytes.extend_from_slice(&0xEFBE_0002u32.to_le_bytes());
    bytes.push(1);
    bytes.extend(support::beef_chain::varint(HEIGHT));
    bytes.extend_from_slice(&[1, 1, 0, 2]);
    bytes.extend_from_slice(&funding_root());
    bytes.push(2);
    bytes.extend_from_slice(&[1, 0]);
    bytes.extend(support::beef_chain::funding());
    bytes.push(0);
    bytes.extend_from_slice(&raw);
    assert!(
        matches!(library_verdict(&bytes), Verdict::Unverifiable { .. }),
        "the library declines"
    );
    let mut source = Chunked {
        inner: &bytes[..],
        size: CHUNK,
    };
    let err = block_on(read_at_rest(
        &mut source,
        &ChainHeaders::honest(),
        bytes.len() as u64,
    ))
    .unwrap_err();
    println!("{err}");
    assert!(
        matches!(&err, AtRestError::Unverifiable { txid: t, check, .. }
            if *t == display(&txid) && check == "Finality"),
        "{err:?}"
    );
    assert!(err.to_string().contains("not judged invalid"));
    assert!(err.reason().is_empty(), "no refusal of the bytes");
}

#[test]
fn a_root_the_headers_do_not_carry_is_refused_at_its_bump() {
    let size = support::beef_chain::atomic_size(3);
    let headers = ChainHeaders {
        carry: false,
        ..ChainHeaders::honest()
    };
    let mut source = Chunked {
        inner: ChainSource::atomic(3),
        size: CHUNK,
    };
    let err = block_on(read_at_rest(&mut source, &headers, size)).unwrap_err();
    println!("{err}");
    // The prefix (36 bytes), the version word (4), the BUMP count (1).
    assert!(
        matches!(&err, AtRestError::Refused { offset: 41, kind, .. } if kind == "RootNotCarried")
    );
}

#[test]
fn a_header_service_that_cannot_answer_is_no_acceptance_and_no_refusal() {
    let size = support::beef_chain::atomic_size(3);
    let headers = ChainHeaders {
        fail: true,
        ..ChainHeaders::honest()
    };
    let mut source = Chunked {
        inner: ChainSource::atomic(3),
        size: CHUNK,
    };
    let err = block_on(read_at_rest(&mut source, &headers, size)).unwrap_err();
    assert!(matches!(err, AtRestError::Headers(_)), "{err}");
}

#[test]
fn bytes_other_than_the_reference_names_are_not_read_as_it() {
    let size = support::beef_chain::atomic_size(3);
    for claimed in [size - 1, size + 1] {
        let mut source = Chunked {
            inner: ChainSource::atomic(3),
            size: CHUNK,
        };
        let err =
            block_on(read_at_rest(&mut source, &ChainHeaders::honest(), claimed)).unwrap_err();
        println!("{err}");
        assert!(matches!(err, AtRestError::Size { .. }), "{err}");
    }
}

#[test]
fn a_beef_that_is_not_atomic_is_refused() {
    let size = {
        let mut s = ChainSource::new(3);
        std::io::copy(&mut s, &mut std::io::sink()).unwrap()
    };
    let mut source = Chunked {
        inner: ChainSource::new(3),
        size: CHUNK,
    };
    let err = block_on(read_at_rest(&mut source, &ChainHeaders::honest(), size)).unwrap_err();
    assert_eq!(err, AtRestError::NotAtomic);
}
