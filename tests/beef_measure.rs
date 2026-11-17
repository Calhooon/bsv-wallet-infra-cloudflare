//! A BEEF is measured before it is parsed (`door::beef_measure`).
//!
//! A counting global allocator charges every block its size plus 16 bytes (at
//! least what a wasm32 block costs its allocator; a native type is at least as
//! large as its wasm32 form), and these tests hold:
//!
//! 1. the estimate at or over the measured heap of `Beef::from_binary` and
//!    `Beef::sort_txs` on every family of `support/beef_shapes.rs` at sizes
//!    from 10 KB to 1.5 MB, on random valid BEEFs (V1, V2, AtomicBEEF), and on
//!    a pair merged as processAction merges it;
//! 2. each door's request under the ceiling, through the steps the Worker takes
//!    for it: the body, the JSON-RPC parse, the argument's clone and decode,
//!    then, for `internalizeAction`, the measure, the parse, its clone and
//!    `verify_valid`, and the Extended Format conversion of the broadcast; for
//!    `processAction`, the measure, the signed transaction's parse, the merge
//!    into the stored BEEF and the conversion. Each family at the body bound,
//!    and the largest one the measure admits;
//! 3. the refusal's wire shape, the same as a BEEF the parser refuses;
//! 4. with `BEEF_MEASURE_ROWS` naming a JSONL file of stored requests
//!    (`txid`, `raw_tx`, `input_beef` in hex), every row admitted as each
//!    caller builds it, and the estimate over the measured parse.
//!
//! The tests share the allocator's counters, so each holds one lock.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bsv_sdk::transaction::{Beef, Transaction};
use rust_wallet_infra::beef_at_rest::{parse_internalize_params, InternalizeParams};
use rust_wallet_infra::door::beef_measure::{
    admit_beef, estimate, estimate_tx, measure, measure_tx, BEEF_TOO_LARGE, PARSE_BUDGET,
    TX_TOO_LARGE,
};
use rust_wallet_infra::door::bounds::MAX_JSON_BODY_BYTES;
use rust_wallet_infra::door::errors::{public_parts, rpc_error};
use rust_wallet_infra::json_rpc::JsonRpcRequest;
use rust_wallet_infra::services::arcade::beef_to_ef_batch;
use rust_wallet_infra::storage::internalize_action::parse_inline_beef;
use rust_wallet_infra::storage::process_action::admit_signed_tx;
use rust_wallet_infra::types::StorageProcessActionArgs;
use serde_json::json;

#[path = "support/beef_shapes.rs"]
mod shapes;
use shapes::*;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// What each block is charged over its size.
const BLOCK_OVERHEAD: usize = 16;

struct Counting;

// SAFETY: every call forwards to `System` with the caller's own layout and pointer; the
// counters are only arithmetic beside it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded as is.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let size = layout.size() + BLOCK_OVERHEAD;
            let live = LIVE.fetch_add(size, Ordering::SeqCst) + size;
            PEAK.fetch_max(live, Ordering::SeqCst);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: forwarded as is.
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size() + BLOCK_OVERHEAD, Ordering::SeqCst);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The peak growth of the live heap while `f` runs, over the level at its entry.
fn peak_of<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let base = LIVE.load(Ordering::SeqCst);
    PEAK.store(base, Ordering::SeqCst);
    let out = f();
    (out, PEAK.load(Ordering::SeqCst).saturating_sub(base))
}

fn lock() -> std::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
}

fn mib(n: impl Into<u64>) -> f64 {
    n.into() as f64 / (1024.0 * 1024.0)
}

/// The heap of the parse the estimate bounds.
fn parse_peak(bytes: &[u8]) -> (bool, usize) {
    peak_of(|| match Beef::from_binary(bytes) {
        Ok(mut beef) => {
            beef.sort_txs();
            true
        }
        Err(_) => false,
    })
}

/// The heap of processAction's merge: the stored BEEF parsed, the signed
/// transaction merged in, the whole sorted.
fn merge_peak(stored: &[u8], raw: &[u8]) -> usize {
    peak_of(|| {
        let mut beef = Beef::from_binary(stored).expect("the stored BEEF parses");
        beef.merge_raw_tx(raw.to_vec(), None);
        beef.sort_txs();
    })
    .1
}

/// The estimate is at or over the parse's heap on every family and size.
#[test]
fn the_estimate_bounds_the_parse() {
    let _one = lock();
    let mut worst = f64::MAX;
    let mut target = 10_000f64;
    while target < 1_600_000.0 {
        for (name, bytes) in families(target as usize) {
            let m = measure(&bytes);
            assert!(m.readable, "{name}: the measure reads the whole BEEF");
            let (parsed, peak) = parse_peak(&bytes);
            assert!(parsed, "{name}: the BEEF parses");
            let est = estimate(&m.counts);
            println!(
                "{name} ({} bytes): parse {:.2} MiB, estimate {:.2} MiB ({:.2}x)",
                bytes.len(),
                mib(peak as u64),
                mib(est),
                est as f64 / peak as f64
            );
            assert!(
                est >= peak as u64,
                "{name} at {} bytes: the estimate {est} is under the parse's {peak}",
                bytes.len()
            );
            worst = worst.min(est as f64 / peak as f64);
        }
        target *= 1.7;
    }
    let mut rng = Rng(0x00be_ef00);
    for k in 0..300 {
        let bytes = random_beef(&mut rng, if k % 5 == 0 { 2_000 } else { 150 });
        let m = measure(&bytes);
        assert!(m.readable, "random BEEF {k}: the measure reads it");
        let (parsed, peak) = parse_peak(&bytes);
        assert!(parsed, "random BEEF {k} parses");
        let est = estimate(&m.counts);
        assert!(
            est >= peak as u64,
            "random BEEF {k} ({} bytes): the estimate {est} is under the parse's {peak}",
            bytes.len()
        );
        worst = worst.min(est as f64 / peak as f64);
    }
    // processAction's pair: a stored BEEF and a signed transaction of each shape.
    let parent = root(&[25; 600], 0);
    let pid = id(&parent);
    let stored = beef(V2, &[bump_of(1, pid)], &[Entry::Proven(0, parent)]);
    let signed: Vec<Vec<u8>> = vec![
        tx(
            &(0..600)
                .map(|k| In {
                    txid: pid,
                    vout: k,
                    script: 107,
                })
                .collect::<Vec<_>>(),
            &[25],
            0,
        ),
        tx(
            &[In {
                txid: pid,
                vout: 0,
                script: 107,
            }],
            &vec![0; 50_000],
            0,
        ),
        tx(
            &[In {
                txid: pid,
                vout: 0,
                script: 300_000,
            }],
            &[25],
            0,
        ),
    ];
    for raw in &signed {
        let est = estimate(&measure(&stored).counts.with_tx(measure_tx(raw).0));
        let peak = merge_peak(&stored, raw);
        assert!(
            est >= peak as u64,
            "a merged pair: the estimate {est} is under the merge's {peak}"
        );
        worst = worst.min(est as f64 / peak as f64);
    }
    println!("the estimate over the parse: at least {worst:.2}x");
}

/// The JSON-RPC body of `internalizeAction` with `tx` as hex, or as an array of bytes.
fn internalize_body(beef: &[u8], as_array: bool) -> Vec<u8> {
    let tx = if as_array {
        json!(beef)
    } else {
        json!(hex::encode(beef))
    };
    serde_json::to_vec(&json!({
        "jsonrpc": "2.0", "method": "internalizeAction", "id": 1,
        "params": [{"identityKey": "02aa"}, {
            "tx": tx,
            "outputs": [{"outputIndex": 0, "protocol": "basket insertion", "insertionRemittance": {"basket": "b"}}],
            "description": "a payment"
        }]
    }))
    .unwrap()
}

/// The JSON-RPC body of `processAction` with the signed transaction as an array of bytes.
fn process_body(raw: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "jsonrpc": "2.0", "method": "processAction", "id": 1,
        "params": [{"identityKey": "02aa"}, {
            "isNewTx": true, "isSendWith": false, "isNoSend": false, "isDelayed": false,
            "reference": "r", "txid": hex::encode(id(raw).iter().rev().copied().collect::<Vec<u8>>()),
            "rawTx": raw, "sendWith": []
        }]
    }))
    .unwrap()
}

/// What a door did with the request.
#[derive(Debug, PartialEq)]
enum Outcome {
    Refused(String),
    Parsed,
}

/// The peak of `internalizeAction`'s inline door on `body`.
fn internalize_peak(body: &[u8]) -> (Outcome, usize) {
    peak_of(|| {
        let request_body = body.to_vec(); // the body as the BRC-31 layer hands it over
        let rpc: JsonRpcRequest = serde_json::from_slice(&request_body).expect("the body parses");
        let params = rpc.params;
        let InternalizeParams::Inline(args) =
            parse_internalize_params(&params).expect("the args parse")
        else {
            panic!("an inline BEEF")
        };
        let outcome = match parse_inline_beef(&args.tx) {
            Err(e) => Outcome::Refused(public_parts(&e).1),
            Ok(beef) => {
                let mut clone = beef.clone();
                let _ = clone.verify_valid(false);
                drop((clone, beef));
                // The broadcast: Arcade's Extended Format conversion of the same bytes.
                let _ = beef_to_ef_batch(&hex::encode(&args.tx));
                Outcome::Parsed
            }
        };
        drop((request_body, params, args));
        outcome
    })
}

/// The peak of `processAction` on `body`, `stored` the BEEF createAction stored.
fn process_peak(body: &[u8], stored: &[u8]) -> (Outcome, usize) {
    peak_of(|| {
        let request_body = body.to_vec();
        let stored_hex = hex::encode(stored); // the row's `hex(input_beef)`
        let rpc: JsonRpcRequest = serde_json::from_slice(&request_body).expect("the body parses");
        let params = rpc.params;
        let args: StorageProcessActionArgs =
            serde_json::from_value(params[1].clone()).expect("the args parse");
        let raw = args.raw_tx.as_deref().expect("a signed transaction");
        let input_beef = hex::decode(&stored_hex).unwrap();
        let outcome = match admit_signed_tx(raw, Some(&input_beef)) {
            Err(e) => Outcome::Refused(public_parts(&e).1),
            Ok(()) => {
                // The local script check's parse, then the merge and the conversion.
                drop(Transaction::from_binary(raw));
                let mut beef = Beef::from_binary(&input_beef).expect("the stored BEEF parses");
                beef.merge_raw_tx(raw.to_vec(), None);
                let merged = hex::encode(beef.to_binary());
                drop(beef);
                let _ = beef_to_ef_batch(&merged);
                Outcome::Parsed
            }
        };
        drop((request_body, stored_hex, params, args, input_beef));
        outcome
    })
}

/// The ceiling a request's peak is held under (native, every block charged its
/// overhead): the isolate's 128 MB less what the runtime, the module and the
/// body's own JavaScript copy hold.
const REQUEST_CEILING: usize = 96 * 1024 * 1024;

/// The largest BEEF of a family the measure admits: a search on the family's size.
fn largest_admitted(family_k: usize, wrap: impl Fn(&[u8]) -> Vec<u8>) -> Option<Vec<u8>> {
    let admitted = |target: usize| -> Option<Vec<u8>> {
        let bytes = wrap(&family(family_k, target).1);
        admit_beef(&bytes).ok().map(|_| bytes)
    };
    let (mut lo, mut hi) = (2_000usize, 2 * 1024 * 1024);
    admitted(lo)?;
    while hi - lo > lo / 50 {
        let mid = (lo + hi) / 2;
        if admitted(mid).is_some() {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    admitted(lo)
}

/// An AtomicBEEF of `beef` (its subject a placeholder: the measure and the parse do not look).
fn atomic_of(beef: &[u8]) -> Vec<u8> {
    if beef.starts_with(&ATOMIC.to_le_bytes()) {
        return beef.to_vec();
    }
    atomic([0x11; 32], beef)
}

/// internalizeAction: each family at the body bound is refused before its
/// parse (or, made of few large elements, parsed), and the largest each family
/// admits is parsed, all under the ceiling.
#[test]
fn the_inline_door_holds_its_request_under_the_ceiling() {
    let _one = lock();
    let at_bound = (MAX_JSON_BODY_BYTES as usize - 1024) / 2;
    let mut worst_admitted = 0usize;
    for k in 0..FAMILIES {
        // The family filled as near the body bound as it goes.
        let mut target = at_bound;
        let (name, bytes, body) = loop {
            let (name, bytes) = family(k, target);
            let bytes = atomic_of(&bytes);
            let body = internalize_body(&bytes, false);
            if body.len() as u64 <= MAX_JSON_BODY_BYTES {
                break (name, bytes, body);
            }
            target -= target / 100;
        };
        let (outcome, peak) = internalize_peak(&body);
        println!(
            "internalize, {name} at the body bound ({} bytes): {outcome:?}, peak {:.1} MiB",
            bytes.len(),
            mib(peak as u64)
        );
        // Refused before its parse, or (a family of few large elements) parsed:
        // either way within the ceiling.
        assert!(
            outcome == Outcome::Refused(BEEF_TOO_LARGE.to_string()) || outcome == Outcome::Parsed,
            "{name}: {outcome:?}"
        );
        assert!(
            peak <= REQUEST_CEILING,
            "{name}: the request held {:.1} MiB",
            mib(peak as u64)
        );

        let largest = largest_admitted(k, atomic_of).expect("a small one is admitted");
        for as_array in [false, true] {
            if as_array && largest.len() > 1_000_000 {
                continue; // the array form carries at most a million bytes
            }
            let (outcome, peak) = internalize_peak(&internalize_body(&largest, as_array));
            println!(
                "internalize, {name}, the largest admitted ({} bytes, estimate {:.1} MiB{}): {outcome:?}, peak {:.1} MiB",
                largest.len(),
                mib(estimate(&measure(&largest).counts)),
                if as_array { ", tx as an array" } else { "" },
                mib(peak as u64)
            );
            assert_eq!(outcome, Outcome::Parsed);
            assert!(
                peak <= REQUEST_CEILING,
                "{name}: an admitted request held {:.1} MiB",
                mib(peak as u64)
            );
            worst_admitted = worst_admitted.max(peak);
        }
    }
    println!(
        "internalize: the largest admitted request held {:.1} MiB",
        mib(worst_admitted as u64)
    );
}

/// processAction: a signed transaction of each shape at the body bound is
/// refused before its parse; the largest admitted is parsed under the ceiling.
#[test]
fn the_process_door_holds_its_request_under_the_ceiling() {
    let _one = lock();
    // The array form: a million items at most (`door::bounds::MAX_ARRAY_ITEMS`).
    let at_bound = 1_000_000 - 512;
    // The BEEF createAction stores for 1,000 inputs, each of its own parent
    // mined in its own block (a BUMP of 20 levels each).
    let mut bumps = Vec::new();
    let mut parents = Vec::new();
    let mut entries = Vec::new();
    for k in 0..1_000u64 {
        let p = root(&[25, 25], k as u32);
        parents.push(id(&p));
        bumps.push(bump(800_000 + k, &path_of(id(&p), 20, &mut Rng(k + 1))));
        entries.push(Entry::Proven(k, p));
    }
    let stored = beef(V2, &bumps, &entries);
    let parents: &'static [[u8; 32]] = Box::leak(parents.into_boxed_slice());
    let spend = move |vout: u32, script: usize| In {
        txid: parents[vout as usize % 1_000],
        vout: 0,
        script,
    };
    type Shape = (&'static str, Box<dyn Fn(usize) -> Vec<u8>>);
    let shapes: Vec<Shape> = vec![
        (
            "many outputs",
            Box::new(move |n| tx(&[spend(0, 0)], &vec![0; n / 9], 0)),
        ),
        (
            "many inputs",
            Box::new(move |n| {
                tx(
                    &(0..n / 41).map(|k| spend(k as u32, 0)).collect::<Vec<_>>(),
                    &[0],
                    0,
                )
            }),
        ),
        (
            "one large unlock",
            Box::new(move |n| tx(&[spend(0, n.saturating_sub(64))], &[0], 0)),
        ),
        (
            "one large lock",
            Box::new(move |n| tx(&[spend(0, 0)], &[n.saturating_sub(64)], 0)),
        ),
    ];
    // The stored BEEF createAction builds: the 1,000 parents above, or the
    // stored BEEF of an earlier payment being spent (a chain of tiny
    // transactions the inline door admitted, estimated near 20 MiB).
    let earlier = family(1, 290_000).1;
    let stored_beefs = [
        ("1,000 parents", stored),
        ("an earlier payment's chain", earlier),
    ];
    let mut worst_admitted = 0usize;
    for (stored_name, stored) in &stored_beefs {
        println!(
            "stored: {stored_name}, {} bytes, estimate {:.1} MiB",
            stored.len(),
            mib(estimate(&measure(stored).counts))
        );
        for (name, make) in &shapes {
            let raw = make(at_bound);
            let (outcome, peak) = process_peak(&process_body(&raw), stored);
            println!(
                "processAction, {name} at the bound ({} bytes): {outcome:?}, peak {:.1} MiB",
                raw.len(),
                mib(peak as u64)
            );
            let refused = outcome == Outcome::Refused(TX_TOO_LARGE.to_string());
            assert!(
                refused || peak <= REQUEST_CEILING,
                "{name}: {:.1} MiB",
                mib(peak as u64)
            );
            // The largest the measure admits.
            let (mut lo, mut hi) = (64usize, at_bound);
            if admit_signed_tx(&make(lo), Some(stored)).is_err() {
                continue;
            }
            if admit_signed_tx(&make(hi), Some(stored)).is_ok() {
                lo = hi;
            }
            while hi - lo > lo / 50 {
                let mid = (lo + hi) / 2;
                if admit_signed_tx(&make(mid), Some(stored)).is_ok() {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let raw = make(lo);
            let (outcome, peak) = process_peak(&process_body(&raw), stored);
            println!("processAction, {name}, the largest admitted ({} bytes): {outcome:?}, peak {:.1} MiB", raw.len(), mib(peak as u64));
            assert_eq!(outcome, Outcome::Parsed);
            assert!(
                peak <= REQUEST_CEILING,
                "{name}: an admitted request held {:.1} MiB",
                mib(peak as u64)
            );
            worst_admitted = worst_admitted.max(peak);
        }
    }
    println!(
        "processAction: the largest admitted request held {:.1} MiB",
        mib(worst_admitted as u64)
    );
}

/// The refusal is the storage's refusal of a BEEF it cannot take: the same
/// JSON-RPC shape and code as a BEEF the parser refuses, its own message.
#[test]
fn the_refusal_is_the_storages_refusal_of_a_beef() {
    let _one = lock();
    let over = atomic_of(&family(0, 400_000).1);
    let refused = rpc_error(json!(7), &parse_inline_beef(&over).unwrap_err());
    let malformed = rpc_error(
        json!(7),
        &parse_inline_beef(&[0xEF, 0xBE, 0x00]).unwrap_err(),
    );
    assert_eq!(
        refused,
        json!({"jsonrpc": "2.0", "error": {"code": -32602, "message": BEEF_TOO_LARGE}, "id": 7})
    );
    assert_eq!(malformed["error"]["code"], refused["error"]["code"]);
    let keys = |v: &serde_json::Value| v.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
    assert_eq!(keys(&malformed), keys(&refused));
    assert_eq!(keys(&malformed["error"]), keys(&refused["error"]));
    assert!(malformed["error"]["message"]
        .as_str()
        .unwrap()
        .starts_with("Failed to parse AtomicBEEF"));

    // A BEEF the measure cannot read meets the parser's refusal, as before.
    let small = atomic_of(&family(1, 20_000).1);
    let cut = &small[..small.len() - 3];
    assert!(!measure(cut).readable);
    let e = parse_inline_beef(cut).unwrap_err().to_string();
    assert!(e.contains("Failed to parse AtomicBEEF"), "{e}");
    // ... unless what it read is already over the budget.
    let cut = &over[..over.len() - 3];
    assert!(!measure(cut).readable);
    assert!(parse_inline_beef(cut)
        .unwrap_err()
        .to_string()
        .contains(BEEF_TOO_LARGE));

    // processAction's refusal, the same shape.
    let raw = tx(
        &[In {
            txid: [1; 32],
            vout: 0,
            script: 0,
        }],
        &vec![0; 110_000],
        0,
    );
    let e = admit_signed_tx(&raw, None).unwrap_err();
    assert_eq!(
        rpc_error(json!(7), &e),
        json!({"jsonrpc": "2.0", "error": {"code": -32602, "message": TX_TOO_LARGE}, "id": 7})
    );
    println!("the budget: {:.0} MiB", mib(PARSE_BUDGET));
}

/// Stored requests, as each caller builds its BEEF: internalizeAction's
/// AtomicBEEF as it came, processAction's and the monitor's stored BEEF with
/// the signed transaction merged in, a transaction stored without a BEEF.
#[test]
fn stored_requests_pass_the_measure() {
    let Ok(file) = std::env::var("BEEF_MEASURE_ROWS") else {
        println!("BEEF_MEASURE_ROWS is not set: skipped");
        return;
    };
    let _one = lock();
    let text = std::fs::read_to_string(file).expect("the rows file reads");
    let (mut rows, mut with_beef, mut worst, mut worst_txid, mut least) =
        (0, 0, 0u64, String::new(), f64::MAX);
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let row: serde_json::Value = serde_json::from_str(line).expect("a JSON row");
        let txid = row["txid"].as_str().unwrap_or_default().to_string();
        let raw = hex::decode(row["raw_tx"].as_str().unwrap_or_default()).expect("raw_tx is hex");
        let stored =
            hex::decode(row["input_beef"].as_str().unwrap_or_default()).expect("input_beef is hex");
        rows += 1;
        let mut note = |est: u64, peak: usize| {
            assert!(
                est >= peak as u64,
                "{txid}: the estimate {est} is under the parse's {peak}"
            );
            least = least.min(est as f64 / peak as f64);
            if est > worst {
                worst = est;
                worst_txid = txid.clone();
            }
        };
        if stored.is_empty() {
            admit_signed_tx(&raw, None).unwrap_or_else(|e| panic!("{txid}: {e}"));
            let est = estimate_tx(&measure_tx(&raw).0);
            note(est, peak_of(|| Transaction::from_binary(&raw).is_ok()).1);
            continue;
        }
        with_beef += 1;
        let m = measure(&stored);
        assert!(m.readable, "{txid}: the measure reads the stored BEEF");
        admit_signed_tx(&raw, Some(&stored)).unwrap_or_else(|e| panic!("{txid}, merged: {e}"));
        note(
            estimate(&m.counts.with_tx(measure_tx(&raw).0)),
            merge_peak(&stored, &raw),
        );
        if stored.starts_with(&[1, 1, 1, 1]) {
            admit_beef(&stored).unwrap_or_else(|e| panic!("{txid}, as it came: {e}"));
            note(estimate(&m.counts), parse_peak(&stored).1);
        }
    }
    println!(
        "{rows} rows, {with_beef} with a BEEF: all admitted; the largest estimate {:.2} MiB ({worst_txid}), {:.1}% of the budget; the estimate at least {least:.2}x the parse",
        mib(worst),
        100.0 * worst as f64 / PARSE_BUDGET as f64
    );
}
