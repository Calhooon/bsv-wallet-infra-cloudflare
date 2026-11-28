//! A BEEF is measured before it is parsed.
//!
//! bsv-rs's `Beef::from_binary` holds every element it reads as owned Rust
//! values (each txid as a hex `String`, each script as its own `Vec`, an index
//! entry and a sort entry per transaction), so the heap a parse reaches
//! follows the number of elements as much as the number of bytes. Before a
//! caller's bytes are parsed, [`measure`] walks them once in the BEEF layout
//! (the version and the AtomicBEEF prefix, each BUMP with its levels and
//! leaves and which of them carry a hash, each transaction entry with its
//! format byte, its inputs and its outputs) and counts what the parse's cost
//! depends on, holding nothing proportional to the elements. [`estimate`]
//! turns the counts into an upper bound of the heap `Beef::from_binary`
//! followed by `Beef::sort_txs` reaches, and [`admit_beef`] refuses a BEEF
//! whose estimate is over [`PARSE_BUDGET`].
//!
//! The constants are measured, not derived: bsv-rs 0.8.15's parse and sort
//! under a counting allocator that charges every block its size plus 16 bytes
//! (`tests/beef_measure.rs`, which holds the estimate over the measured peak on
//! every shape it builds). A native build's types are at least as large as a
//! wasm32 build's, so the native measure bounds the Worker's.

use crate::error::Error;

/// The heap one parse of a caller's BEEF may reach, 24 MiB.
pub const PARSE_BUDGET: u64 = 24 * 1024 * 1024;

/// What a caller is told when its BEEF is refused by measure.
pub const BEEF_TOO_LARGE: &str = "the BEEF is too large to process";

/// What a caller is told when its transaction is refused by measure.
pub const TX_TOO_LARGE: &str = "the transaction is too large to process";

const ATOMIC_BEEF: u32 = 0x0101_0101;
const BEEF_V1: u32 = 0xEFBE_0001;
const BEEF_V2: u32 = 0xEFBE_0002;
const MAX_TREE_HEIGHT: u8 = 64;
const FORMAT_RAW_TX_AND_BUMP: u8 = 1;
const FORMAT_TXID_ONLY: u8 = 2;

/// The counts of one transaction's bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxCounts {
    /// The transaction's bytes.
    pub bytes: u64,
    /// Its inputs.
    pub inputs: u64,
    /// Its outputs.
    pub outputs: u64,
}

/// The counts a parse's heap depends on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BeefCounts {
    /// The bytes walked.
    pub bytes: u64,
    /// BUMPs.
    pub bumps: u64,
    /// BUMP levels, over every BUMP.
    pub bump_levels: u64,
    /// BUMP leaves, over every BUMP.
    pub bump_leaves: u64,
    /// The most leaves in any one BUMP.
    pub max_bump_leaves: u64,
    /// BUMP leaves that carry a hash, over every BUMP.
    pub bump_hashes: u64,
    /// The most leaves that carry a hash in any one BUMP.
    pub max_bump_hashes: u64,
    /// Transaction entries, txid-only ones among them.
    pub entries: u64,
    /// Txid-only entries.
    pub txid_only: u64,
    /// Inputs, over every transaction.
    pub inputs: u64,
    /// Outputs, over every transaction.
    pub outputs: u64,
    /// The bytes of every transaction.
    pub tx_bytes: u64,
    /// The transaction whose own parse costs the most.
    pub largest_tx: TxCounts,
}

impl BeefCounts {
    /// The counts after `tx` is added as one more entry (a raw transaction
    /// merged into a parsed BEEF).
    pub fn with_tx(mut self, tx: TxCounts) -> Self {
        self.add_tx(tx);
        self.bytes += tx.bytes + 1;
        self.entries += 1;
        self
    }

    fn add_tx(&mut self, tx: TxCounts) {
        self.inputs += tx.inputs;
        self.outputs += tx.outputs;
        self.tx_bytes += tx.bytes;
        if tx_cost(&tx) > tx_cost(&self.largest_tx) {
            self.largest_tx = tx;
        }
    }
}

/// A walk's answer: the counts, and whether the whole frame was read. When it
/// was not, the counts are of what was read before the walk stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Measure {
    /// The counts of what was read.
    pub counts: BeefCounts,
    /// Whether the bytes held a whole BEEF frame (bytes after it included).
    pub readable: bool,
}

// The cost per element, in heap bytes (see the module's note on how they are found).
const PER_PARSE: u64 = 4 * 1024;
const PER_BYTE: u64 = 2;
const PER_ENTRY: u64 = 2_700;
const PER_TXID_ONLY: u64 = 300;
const PER_INPUT: u64 = 950;
const PER_OUTPUT: u64 = 175;
const PER_BUMP: u64 = 64;
const PER_BUMP_LEVEL: u64 = 230;
// The four BUMP leaf constants (the two below, `BUMP_PER_LEAF` and
// `BUMP_PER_HASH`) are measured against bsv-rs 0.8.15's reader: the least that
// hold the estimate a fifth over every measured parse, rounded up. They are
// measured again at each bsv-rs pin: `tests/beef_measure.rs`,
// `the_estimate_bounds_the_parse`, prints each shape's estimate over its
// measured parse.
const PER_BUMP_LEAF: u64 = 85;
const PER_BUMP_HASH: u64 = 40;
// The transient of one transaction's own parse (bsv-rs reads a raw entry twice).
const TX_PER_PARSE: u64 = 256;
const TX_PER_BYTE: u64 = 7;
const TX_PER_INPUT: u64 = 32;
const TX_PER_OUTPUT: u64 = 110;
// The transient of one BUMP's own parse (its leaves sorted and indexed, its
// root computed from every txid it carries).
const BUMP_PER_LEAF: u64 = 510;
const BUMP_PER_HASH: u64 = 355;

fn tx_cost(tx: &TxCounts) -> u64 {
    TX_PER_PARSE
        .saturating_add(TX_PER_BYTE.saturating_mul(tx.bytes))
        .saturating_add(TX_PER_INPUT.saturating_mul(tx.inputs))
        .saturating_add(TX_PER_OUTPUT.saturating_mul(tx.outputs))
}

/// An upper bound of the heap `Beef::from_binary` and `Beef::sort_txs`
/// reach on bytes with these counts, the bytes themselves not included.
pub fn estimate(c: &BeefCounts) -> u64 {
    let terms = [
        PER_PARSE,
        PER_BYTE.saturating_mul(c.bytes),
        PER_ENTRY.saturating_mul(c.entries),
        PER_TXID_ONLY.saturating_mul(c.txid_only),
        PER_INPUT.saturating_mul(c.inputs),
        PER_OUTPUT.saturating_mul(c.outputs),
        PER_BUMP.saturating_mul(c.bumps),
        PER_BUMP_LEVEL.saturating_mul(c.bump_levels),
        PER_BUMP_LEAF.saturating_mul(c.bump_leaves),
        PER_BUMP_HASH.saturating_mul(c.bump_hashes),
        BUMP_PER_LEAF.saturating_mul(c.max_bump_leaves),
        BUMP_PER_HASH.saturating_mul(c.max_bump_hashes),
        tx_cost(&c.largest_tx),
    ];
    terms.iter().fold(0u64, |a, t| a.saturating_add(*t))
}

/// An upper bound of the heap a raw transaction's parse reaches.
pub fn estimate_tx(tx: &TxCounts) -> u64 {
    estimate(&BeefCounts::default().with_tx(*tx))
}

/// A cursor over the bytes; every read either advances or stops the walk.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: u64) -> Option<&[u8]> {
        let n = usize::try_from(n).ok()?;
        let end = self.at.checked_add(n)?;
        let out = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(out)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// A varint as bsv-rs reads one: canonical, and within a `usize`.
    fn varint(&mut self) -> Option<u64> {
        let (value, width) = match self.u8()? {
            n @ 0..=0xFC => (u64::from(n), 1),
            0xFD => {
                let b = self.take(2)?;
                (u64::from(u16::from_le_bytes([b[0], b[1]])), 3)
            }
            0xFE => (u64::from(self.u32()?), 5),
            _ => {
                let b = self.take(8)?;
                let mut le = [0u8; 8];
                le.copy_from_slice(b);
                (u64::from_le_bytes(le), 9)
            }
        };
        let canonical = match value {
            0..=0xFC => 1,
            0xFD..=0xFFFF => 3,
            0x1_0000..=0xFFFF_FFFF => 5,
            _ => 9,
        };
        if width != canonical || usize::try_from(value).is_err() {
            return None;
        }
        Some(value)
    }
}

/// Walk one transaction: its counts, or `None` where the bytes end inside it.
fn walk_tx(cur: &mut Cursor<'_>, partial: &mut TxCounts) -> Option<TxCounts> {
    let start = cur.at;
    cur.take(4)?;
    let inputs = cur.varint()?;
    for _ in 0..inputs {
        cur.take(36)?;
        let script = cur.varint()?;
        cur.take(script)?;
        cur.take(4)?;
        partial.inputs += 1;
    }
    let outputs = cur.varint()?;
    for _ in 0..outputs {
        cur.take(8)?;
        let script = cur.varint()?;
        cur.take(script)?;
        partial.outputs += 1;
    }
    cur.take(4)?;
    Some(TxCounts {
        bytes: (cur.at - start) as u64,
        inputs,
        outputs,
    })
}

/// Measure a raw transaction.
pub fn measure_tx(raw: &[u8]) -> (TxCounts, bool) {
    let mut cur = Cursor { bytes: raw, at: 0 };
    let mut partial = TxCounts::default();
    match walk_tx(&mut cur, &mut partial) {
        Some(tx) => (tx, true),
        None => (
            TxCounts {
                bytes: cur.at as u64,
                ..partial
            },
            false,
        ),
    }
}

/// Walk a BEEF (V1, V2, either behind the AtomicBEEF prefix) and count it.
pub fn measure(bytes: &[u8]) -> Measure {
    let mut counts = BeefCounts::default();
    let mut cur = Cursor { bytes, at: 0 };
    let readable = walk(&mut cur, &mut counts).is_some();
    counts.bytes = if readable {
        bytes.len() as u64
    } else {
        cur.at as u64
    };
    Measure { counts, readable }
}

fn walk(cur: &mut Cursor<'_>, c: &mut BeefCounts) -> Option<()> {
    let mut version = cur.u32()?;
    if version == ATOMIC_BEEF {
        cur.take(32)?;
        version = cur.u32()?;
    }
    if version != BEEF_V1 && version != BEEF_V2 {
        return None;
    }
    let bumps = cur.varint()?;
    for _ in 0..bumps {
        cur.varint()?; // the block height
        let height = cur.u8()?;
        if height > MAX_TREE_HEIGHT {
            return None;
        }
        c.bumps += 1;
        c.bump_levels += u64::from(height);
        let mut leaves_here = 0u64;
        let mut hashes_here = 0u64;
        for _ in 0..height {
            let leaves = cur.varint()?;
            for _ in 0..leaves {
                cur.varint()?; // the offset
                let flags = cur.u8()?;
                if flags & 1 == 0 {
                    cur.take(32)?;
                    c.bump_hashes += 1;
                    hashes_here += 1;
                    c.max_bump_hashes = c.max_bump_hashes.max(hashes_here);
                }
                c.bump_leaves += 1;
                leaves_here += 1;
                c.max_bump_leaves = c.max_bump_leaves.max(leaves_here);
            }
        }
    }
    let entries = cur.varint()?;
    for _ in 0..entries {
        c.entries += 1;
        if version == BEEF_V2 {
            match cur.u8()? {
                FORMAT_TXID_ONLY => {
                    cur.take(32)?;
                    c.txid_only += 1;
                    continue;
                }
                FORMAT_RAW_TX_AND_BUMP => {
                    cur.varint()?;
                }
                // bsv-rs reads every other format byte as a raw transaction.
                _ => {}
            }
            let mut partial = TxCounts::default();
            let tx = walk_tx(cur, &mut partial);
            c.add_tx(tx.unwrap_or(partial));
            tx?;
        } else {
            let mut partial = TxCounts::default();
            let tx = walk_tx(cur, &mut partial);
            c.add_tx(tx.unwrap_or(partial));
            tx?;
            if cur.u8()? != 0 {
                cur.varint()?;
            }
        }
    }
    Some(())
}

/// The refusal of a measure over the budget, as the storage refuses a BEEF it
/// cannot take (`-32602`, the message as the caller's).
fn refused(message: &str) -> Error {
    Error::ValidationError(message.to_string())
}

/// Admit a caller's BEEF to the parse, or refuse it as too large. A BEEF the
/// walk cannot read is left to the parser's own refusal unless what was read
/// of it is already over the budget.
pub fn admit_beef(bytes: &[u8]) -> Result<BeefCounts, Error> {
    let m = measure(bytes);
    if estimate(&m.counts) > PARSE_BUDGET {
        return Err(refused(BEEF_TOO_LARGE));
    }
    Ok(m.counts)
}

/// Admit a caller's raw transaction, merged into `stored` (the counts of the
/// BEEF it joins, when there is one), or refuse it as too large.
pub fn admit_tx(raw: &[u8], stored: Option<&BeefCounts>) -> Result<(), Error> {
    let (tx, _) = measure_tx(raw);
    let merged = stored.copied().unwrap_or_default().with_tx(tx);
    if estimate(&merged) > PARSE_BUDGET {
        return Err(refused(TX_TOO_LARGE));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_sdk::transaction::Beef;

    fn varint(n: u64, o: &mut Vec<u8>) {
        match n {
            0..=0xFC => o.push(n as u8),
            0xFD..=0xFFFF => {
                o.push(0xFD);
                o.extend_from_slice(&(n as u16).to_le_bytes());
            }
            _ => {
                o.push(0xFE);
                o.extend_from_slice(&(n as u32).to_le_bytes());
            }
        }
    }

    /// A transaction of `inputs` inputs (unlocks of `script` bytes) and `outputs` outputs.
    fn tx(inputs: usize, outputs: usize, script: usize, lock_time: u32) -> Vec<u8> {
        let mut o = 1u32.to_le_bytes().to_vec();
        varint(inputs as u64, &mut o);
        for k in 0..inputs {
            o.extend_from_slice(&[k as u8; 32]);
            o.extend_from_slice(&0u32.to_le_bytes());
            varint(script as u64, &mut o);
            o.resize(o.len() + script, 0x51);
            o.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        varint(outputs as u64, &mut o);
        for _ in 0..outputs {
            o.extend_from_slice(&1u64.to_le_bytes());
            varint(script as u64, &mut o);
            o.resize(o.len() + script, 0x51);
        }
        o.extend_from_slice(&lock_time.to_le_bytes());
        o
    }

    fn txid(raw: &[u8]) -> [u8; 32] {
        bsv_sdk::primitives::sha256d(raw)
    }

    /// A BUMP of `txid` at offset 0 and `more` duplicate leaves after it, as
    /// many levels as the offsets need (a tree of height h holds offsets below
    /// 2^h at level 0), each level above 0 carrying the duplicate sibling the
    /// txid's path pairs with.
    fn bump(txid: [u8; 32], more: u64) -> Vec<u8> {
        let height = (64 - more.leading_zeros()).max(1) as u8;
        let mut o = vec![0x01, height];
        varint(1 + more, &mut o);
        o.extend_from_slice(&[0, 2]);
        o.extend_from_slice(&txid);
        for k in 1..=more {
            varint(k, &mut o);
            o.push(1);
        }
        for _ in 1..height {
            o.extend_from_slice(&[1, 1, 1]);
        }
        o
    }

    /// A V2 BEEF: a proven parent with its BUMP, an unproven child, a txid-only entry.
    fn v2() -> Vec<u8> {
        let parent = tx(1, 3, 25, 0);
        let child = tx(2, 2, 107, 0);
        let mut o = BEEF_V2.to_le_bytes().to_vec();
        o.push(1);
        o.extend(bump(txid(&parent), 3));
        o.push(3);
        o.push(1);
        o.push(0);
        o.extend(&parent);
        o.push(0);
        o.extend(&child);
        o.push(2);
        o.extend_from_slice(&[9; 32]);
        o
    }

    #[test]
    fn the_counts_are_the_parses() {
        let bytes = v2();
        let m = measure(&bytes);
        assert!(m.readable);
        let beef = Beef::from_binary(&bytes).expect("the BEEF parses");
        let c = m.counts;
        assert_eq!(c.bytes, bytes.len() as u64);
        assert_eq!(c.entries, beef.txs.len() as u64);
        assert_eq!(
            c.txid_only,
            beef.txs.iter().filter(|t| t.is_txid_only()).count() as u64
        );
        let txs: Vec<_> = beef.txs.iter().filter_map(|t| t.tx()).collect();
        assert_eq!(
            c.inputs,
            txs.iter().map(|t| t.inputs.len() as u64).sum::<u64>()
        );
        assert_eq!(
            c.outputs,
            txs.iter().map(|t| t.outputs.len() as u64).sum::<u64>()
        );
        assert_eq!(c.bumps, beef.bumps.len() as u64);
        // Four leaves at level 0 (offsets 0 to 3, two levels) and the sibling at level 1.
        let leaves: u64 = beef
            .bumps
            .iter()
            .map(|b| b.path.iter().map(|l| l.len() as u64).sum::<u64>())
            .sum();
        assert_eq!(c.bump_leaves, leaves);
        assert_eq!(c.bump_leaves, 5);
        assert_eq!(c.max_bump_leaves, 5);
        // The leaves that carry a hash: the txid alone.
        let hashes: u64 = beef
            .bumps
            .iter()
            .map(|b| b.path.iter().flatten().filter(|l| l.hash.is_some()).count() as u64)
            .sum();
        assert_eq!(c.bump_hashes, hashes);
        assert_eq!((c.bump_hashes, c.max_bump_hashes), (1, 1));
        assert_eq!(
            c.bump_levels,
            beef.bumps.iter().map(|b| b.path.len() as u64).sum::<u64>()
        );
        assert_eq!(c.bump_levels, 2);
        assert_eq!(
            c.largest_tx,
            TxCounts {
                bytes: tx(2, 2, 107, 0).len() as u64,
                inputs: 2,
                outputs: 2
            }
        );
        // Behind the AtomicBEEF prefix: the same counts, 36 bytes more.
        let mut atomic = ATOMIC_BEEF.to_le_bytes().to_vec();
        atomic.extend_from_slice(&[7; 32]);
        atomic.extend(&bytes);
        let a = measure(&atomic);
        assert!(a.readable && Beef::from_binary(&atomic).is_ok());
        assert_eq!(
            a.counts,
            BeefCounts {
                bytes: c.bytes + 36,
                ..c
            }
        );
    }

    #[test]
    fn v1_and_its_bump_flag() {
        let parent = tx(1, 2, 25, 0);
        let child = tx(1, 1, 107, 0);
        let mut o = BEEF_V1.to_le_bytes().to_vec();
        o.push(1);
        o.extend(bump(txid(&parent), 0));
        o.push(2);
        o.extend(&parent);
        o.extend_from_slice(&[1, 0]);
        o.extend(&child);
        o.push(0);
        let m = measure(&o);
        assert!(m.readable && Beef::from_binary(&o).is_ok());
        assert_eq!(
            (m.counts.entries, m.counts.inputs, m.counts.outputs),
            (2, 2, 3)
        );
    }

    #[test]
    fn a_format_byte_the_parser_does_not_name_is_a_raw_transaction() {
        let mut bytes = BEEF_V2.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0, 1, 7]);
        bytes.extend(tx(1, 1, 0, 0));
        let m = measure(&bytes);
        assert!(m.readable && Beef::from_binary(&bytes).is_ok());
        assert_eq!((m.counts.entries, m.counts.inputs), (1, 1));
    }

    #[test]
    fn what_the_walk_cannot_read() {
        let bytes = v2();
        // Cut short: what was read is counted.
        let cut = measure(&bytes[..bytes.len() - 40]);
        assert!(!cut.readable && Beef::from_binary(&bytes[..bytes.len() - 40]).is_err());
        assert_eq!(cut.counts.bumps, 1);
        assert!(cut.counts.entries >= 2);
        // A version that is not a BEEF's.
        assert!(!measure(&[1, 0, 0, 0, 0, 0]).readable);
        // A varint written wider than it need be, as bsv-rs refuses it.
        let mut wide = BEEF_V2.to_le_bytes().to_vec();
        wide.extend_from_slice(&[0xFD, 0, 0, 0]);
        assert!(!measure(&wide).readable && Beef::from_binary(&wide).is_err());
        // A BUMP taller than 64 levels.
        let mut tall = BEEF_V2.to_le_bytes().to_vec();
        tall.extend_from_slice(&[1, 0, 65]);
        assert!(!measure(&tall).readable && Beef::from_binary(&tall).is_err());
        // Bytes after the frame: the frame is read (and counted); the parser refuses them.
        let mut trailing = bytes.clone();
        trailing.push(0);
        let t = measure(&trailing);
        assert!(t.readable && Beef::from_binary(&trailing).is_err());
        assert_eq!(t.counts.bytes, bytes.len() as u64 + 1);
        // Nothing at all.
        assert_eq!(
            measure(&[]),
            Measure {
                counts: BeefCounts::default(),
                readable: false
            }
        );
    }

    #[test]
    fn a_raw_transaction() {
        let raw = tx(3, 4, 10, 0);
        assert_eq!(
            measure_tx(&raw),
            (
                TxCounts {
                    bytes: raw.len() as u64,
                    inputs: 3,
                    outputs: 4
                },
                true
            )
        );
        let (cut, readable) = measure_tx(&raw[..raw.len() - 1]);
        assert!(!readable);
        assert_eq!((cut.inputs, cut.outputs), (3, 4));
    }

    #[test]
    fn the_budget_refuses_and_admits() {
        assert!(admit_beef(&v2()).is_ok());
        // 1,000,000 txid-only entries: 33 MB of bytes, far over the budget.
        let mut many = BEEF_V2.to_le_bytes().to_vec();
        many.push(0);
        varint(1_000_000, &mut many);
        many.resize(many.len() + 33 * 1_000_000, 2);
        match admit_beef(&many) {
            Err(Error::ValidationError(m)) => assert_eq!(m, BEEF_TOO_LARGE),
            other => panic!("{other:?}"),
        }
        // A measure that cannot read stays with the parser, unless what it read is over.
        assert!(admit_beef(&[0xEF, 0xBE]).is_ok());
        assert!(admit_beef(&many[..many.len() - 1]).is_err());
        // processAction's pair: a transaction of 200,000 outputs.
        let wide = tx(1, 200_000, 0, 0);
        match admit_tx(&wide, None) {
            Err(Error::ValidationError(m)) => assert_eq!(m, TX_TOO_LARGE),
            other => panic!("{other:?}"),
        }
        assert!(admit_tx(&tx(1, 2, 25, 0), Some(&measure(&v2()).counts)).is_ok());
        // The estimate saturates instead of wrapping.
        let huge = BeefCounts {
            entries: u64::MAX,
            ..Default::default()
        };
        assert_eq!(estimate(&huge), u64::MAX);
    }
}
