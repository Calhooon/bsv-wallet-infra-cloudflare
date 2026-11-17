//! The Extended Format batch's memory follows the BEEF.
//!
//! `services::arcade::beef_to_ef_batch` writes each input's EF entry from the one output it
//! spends, read in place from the source the BEEF holds. The peak growth of the live heap
//! (a counting global allocator) inside each call is held under a constant multiple of the
//! BEEF's bytes on the widest shapes: one parent of N outputs spent by one child's N inputs
//! (the parent unproven, then proven; one-byte locks and P2PKH-shaped ones), spenders of one
//! output with a large lock, and one transaction carried many times. Every figure is printed
//! before any is judged.
//!
//! Native only: a wasm32 build's heap is not measured here.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use bsv_sdk::script::{LockingScript, UnlockingScript};
use bsv_sdk::transaction::{Beef, MerklePath, MerklePathLeaf, Transaction};
use bsv_sdk::transaction::{TransactionInput, TransactionOutput};
use rust_wallet_infra::services::arcade::beef_to_ef_batch;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

// SAFETY: every call forwards to `System` with the caller's own layout and pointer; the
// counters are only arithmetic beside it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded as is.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: forwarded as is.
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The peak growth of the live heap while `f` runs, over the level at its entry.
fn peak_of<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let out = f();
    (out, PEAK.load(Ordering::Relaxed).saturating_sub(base))
}

/// The bound: at most this many times the BEEF's bytes, plus a floor for a small BEEF.
const PEAK_PER_BEEF_BYTE: usize = 24;
const PEAK_FLOOR: usize = 64 * 1024;

fn bound(beef: usize) -> usize {
    PEAK_PER_BEEF_BYTE * beef + PEAK_FLOOR
}

fn p2pkh(i: u32) -> LockingScript {
    let mut s = vec![0x76, 0xa9, 0x14];
    s.extend_from_slice(&[0u8; 16]);
    s.extend_from_slice(&i.to_le_bytes());
    s.extend_from_slice(&[0x88, 0xac]);
    LockingScript::from_binary(&s).unwrap()
}

fn one_byte(_: u32) -> LockingScript {
    LockingScript::from_binary(&[0x51]).unwrap()
}

/// A signature-and-key shaped push-only unlock (107 bytes), or an empty one.
fn unlock(k: u32, minimal: bool) -> UnlockingScript {
    if minimal {
        return UnlockingScript::from_binary(&[]).unwrap();
    }
    let mut s = vec![0x48];
    s.extend_from_slice(&[0x30; 68]);
    s.extend_from_slice(&k.to_le_bytes());
    s.push(0x21);
    s.extend_from_slice(&[0x02; 33]);
    UnlockingScript::from_binary(&s).unwrap()
}

fn input(source: &str, vout: u32, unlocking: UnlockingScript) -> TransactionInput {
    TransactionInput {
        source_txid: Some(source.to_string()),
        source_output_index: vout,
        unlocking_script: Some(unlocking),
        sequence: u32::MAX,
        ..Default::default()
    }
}

/// A transaction whose one input names a txid the BEEF does not carry.
fn rooted(outputs: impl IntoIterator<Item = TransactionOutput>) -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(input(&"ab".repeat(32), 0, unlock(0, true)));
    tx.outputs.extend(outputs);
    tx
}

/// Merge `tx`, proven by a one-leaf BUMP when `proven`.
fn merge(beef: &mut Beef, tx: &Transaction, proven: bool) {
    let bump = proven.then(|| {
        beef.merge_bump(
            MerklePath::new_unchecked(800_000, vec![vec![MerklePathLeaf::new_txid(0, tx.id())]])
                .unwrap(),
        )
    });
    beef.merge_raw_tx(tx.to_binary(), bump);
}

/// A parent paying `outputs`, merged into `beef`: proven by its own BUMP, or unproven
/// over a proven root it spends.
fn parent(
    beef: &mut Beef,
    outputs: impl IntoIterator<Item = TransactionOutput>,
    proven: bool,
) -> Transaction {
    if proven {
        let parent = rooted(outputs);
        merge(beef, &parent, true);
        return parent;
    }
    let root = rooted([TransactionOutput::new(1, one_byte(0))]);
    merge(beef, &root, true);
    let mut parent = Transaction::new();
    parent.inputs.push(input(&root.id(), 0, unlock(0, true)));
    parent.outputs.extend(outputs);
    merge(beef, &parent, false);
    parent
}

/// One parent of `n` outputs (`lock(i)` each), proven or not, and one unproven child
/// spending every one of them.
fn wide_parent(n: u32, lock: fn(u32) -> LockingScript, minimal: bool, proven: bool) -> Vec<u8> {
    let mut beef = Beef::new();
    let pid = parent(
        &mut beef,
        (0..n).map(|i| TransactionOutput::new(1, lock(i))),
        proven,
    )
    .id();
    let mut child = Transaction::new();
    for k in 0..n {
        child.inputs.push(input(&pid, k, unlock(k, minimal)));
    }
    child.outputs.push(TransactionOutput::new(1, p2pkh(0)));
    merge(&mut beef, &child, false);
    beef.to_binary()
}

/// A lock of `len` bytes (OP_TRUE then OP_NOPs).
fn big_lock(len: usize) -> LockingScript {
    let mut s = vec![0x51u8];
    s.extend(std::iter::repeat_n(0x61u8, len - 1));
    LockingScript::from_binary(&s).unwrap()
}

/// `k` unproven transactions each spending the one output (a `lock_len`-byte lock) of one
/// parent, proven or not.
fn spenders_of_one_output(k: u32, lock_len: usize, proven: bool) -> Vec<u8> {
    let mut beef = Beef::new();
    let pid = parent(
        &mut beef,
        [TransactionOutput::new(1, big_lock(lock_len))],
        proven,
    )
    .id();
    for i in 0..k {
        let mut spender = Transaction::new();
        spender.inputs.push(input(&pid, 0, unlock(i, true)));
        spender.outputs.push(TransactionOutput::new(1, one_byte(i)));
        spender.lock_time = i;
        merge(&mut beef, &spender, false);
    }
    beef.to_binary()
}

/// One unproven transaction spending a `lock_len`-byte lock, carried `k` times.
fn one_spender_carried(k: usize, lock_len: usize) -> Vec<u8> {
    let parent = rooted([TransactionOutput::new(1, big_lock(lock_len))]);
    let mut spender = Transaction::new();
    spender.inputs.push(input(&parent.id(), 0, unlock(0, true)));
    spender.outputs.push(TransactionOutput::new(1, one_byte(0)));
    let mut beef = Beef::new();
    merge(&mut beef, &parent, true);
    merge(&mut beef, &spender, false);
    let entry = beef.txs.last().unwrap().clone();
    for _ in 1..k {
        beef.txs.push(entry.clone());
    }
    beef.to_binary()
}

#[test]
fn ef_batch_memory_follows_the_beef() {
    let mut shapes: Vec<(String, Vec<u8>)> = Vec::new();
    for proven in [false, true] {
        for n in [250u32, 500, 1_000] {
            shapes.push((
                format!("wide parent proven={proven} one-byte N=M={n}"),
                wide_parent(n, one_byte, true, proven),
            ));
            shapes.push((
                format!("wide parent proven={proven} p2pkh N=M={n}"),
                wide_parent(n, p2pkh, false, proven),
            ));
        }
        shapes.push((
            format!("spenders of one output proven={proven} K=500 lock=100000"),
            spenders_of_one_output(500, 100_000, proven),
        ));
    }
    shapes.push((
        "one transaction carried K=500 lock=100000".into(),
        one_spender_carried(500, 100_000),
    ));

    let mut over = Vec::new();
    for (label, beef) in &shapes {
        let beef_hex = hex::encode(beef);
        let (converted, peak) = peak_of(|| beef_to_ef_batch(&beef_hex));
        let outcome = match &converted {
            Ok((efs, _)) => format!(
                "{} EF, {} bytes",
                efs.len(),
                efs.iter().map(|e| e.1.len()).sum::<usize>()
            ),
            Err(e) => format!("refused: {}", &e[..e.len().min(60)]),
        };
        println!(
            "EF {label}: beef={} peak={peak} ({:.1}x) {outcome}",
            beef.len(),
            peak as f64 / beef.len() as f64
        );
        if label.starts_with("wide") && converted.is_err() {
            over.push(format!("{label}: did not convert"));
        }
        if peak > bound(beef.len()) {
            over.push(format!("{label}: {peak} > {}", bound(beef.len())));
        }
    }
    assert!(over.is_empty(), "over the bound: {over:#?}");
}
