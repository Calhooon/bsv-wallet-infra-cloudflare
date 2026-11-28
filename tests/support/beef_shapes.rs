//! BEEFs built byte by byte for the measure's tests: each family is filled to a
//! target size with the smallest elements of its kind, and a seeded generator
//! makes random valid BEEFs of every kind mixed.
#![allow(dead_code)]

use bsv_sdk::primitives::sha256d;

pub const V1: u32 = 0xEFBE_0001;
pub const V2: u32 = 0xEFBE_0002;
pub const ATOMIC: u32 = 0x0101_0101;

pub fn varint(n: u64, o: &mut Vec<u8>) {
    if n < 0xFD {
        o.push(n as u8);
    } else if n <= 0xFFFF {
        o.push(0xFD);
        o.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n <= 0xFFFF_FFFF {
        o.push(0xFE);
        o.extend_from_slice(&(n as u32).to_le_bytes());
    } else {
        o.push(0xFF);
        o.extend_from_slice(&n.to_le_bytes());
    }
}

/// An input: the spent outpoint and the unlocking script's length.
#[derive(Clone, Copy)]
pub struct In {
    pub txid: [u8; 32],
    pub vout: u32,
    pub script: usize,
}

/// A raw transaction; scripts are filled with OP_TRUE bytes.
pub fn tx(inputs: &[In], outputs: &[usize], lock_time: u32) -> Vec<u8> {
    let mut o = 1u32.to_le_bytes().to_vec();
    varint(inputs.len() as u64, &mut o);
    for i in inputs {
        o.extend_from_slice(&i.txid);
        o.extend_from_slice(&i.vout.to_le_bytes());
        varint(i.script as u64, &mut o);
        o.resize(o.len() + i.script, 0x51);
        o.extend_from_slice(&u32::MAX.to_le_bytes());
    }
    varint(outputs.len() as u64, &mut o);
    for s in outputs {
        o.extend_from_slice(&1u64.to_le_bytes());
        varint(*s as u64, &mut o);
        o.resize(o.len() + s, 0x51);
    }
    o.extend_from_slice(&lock_time.to_le_bytes());
    o
}

/// The txid of a raw transaction, in the byte order an outpoint carries it.
pub fn id(raw: &[u8]) -> [u8; 32] {
    sha256d(raw)
}

/// A root transaction: one input from nowhere, `outputs` outputs.
pub fn root(outputs: &[usize], lock_time: u32) -> Vec<u8> {
    let from = In {
        txid: [0xAB; 32],
        vout: 0,
        script: 0,
    };
    tx(&[from], outputs, lock_time)
}

/// A BUMP: the block height and each level's leaves (offset, flags, hash).
pub fn bump(height: u64, levels: &[Vec<(u64, u8, [u8; 32])>]) -> Vec<u8> {
    let mut o = Vec::new();
    varint(height, &mut o);
    o.push(levels.len() as u8);
    for level in levels {
        varint(level.len() as u64, &mut o);
        for (offset, flags, hash) in level {
            varint(*offset, &mut o);
            o.push(*flags);
            if flags & 1 == 0 {
                o.extend_from_slice(hash);
            }
        }
    }
    o
}

/// A path of `levels` levels for `txid` at offset 0: its sibling at each
/// level, a hash or a duplicate at random.
pub fn path_of(txid: [u8; 32], levels: usize, rng: &mut Rng) -> Vec<Vec<(u64, u8, [u8; 32])>> {
    let mut path = vec![vec![
        (0u64, 2u8, txid),
        (1, (rng.below(2) as u8) & 1, [3u8; 32]),
    ]];
    for _ in 1..levels {
        path.push(vec![(1, (rng.below(2) as u8) & 1, [4u8; 32])]);
    }
    path
}

/// The levels of one BUMP of `n` leaves at level 0: `txid` at offset 0, a
/// duplicate at every offset after it, and as many levels as the offsets
/// need (a tree of height h holds offsets below 2^h at level 0), each level
/// above 0 carrying the duplicate sibling the txid's path pairs with.
pub fn many_leaves(txid: [u8; 32], n: u64) -> Vec<Vec<(u64, u8, [u8; 32])>> {
    let height = (64 - n.saturating_sub(1).leading_zeros()).max(1);
    let mut level0 = vec![(0u64, 2u8, txid)];
    level0.extend((1..n).map(|k| (k, 1u8, [0u8; 32])));
    let mut levels = vec![level0];
    levels.extend((1..height).map(|_| vec![(1u64, 1u8, [0u8; 32])]));
    levels
}

/// The one-leaf BUMP of `txid`.
pub fn bump_of(height: u64, txid: [u8; 32]) -> Vec<u8> {
    bump(height, &[vec![(0, 2, txid)]])
}

pub enum Entry {
    Raw(Vec<u8>),
    Proven(u64, Vec<u8>),
    TxidOnly([u8; 32]),
}

/// A BEEF of `version` (V1 or V2) with the BUMPs and entries given.
pub fn beef(version: u32, bumps: &[Vec<u8>], entries: &[Entry]) -> Vec<u8> {
    let mut o = version.to_le_bytes().to_vec();
    varint(bumps.len() as u64, &mut o);
    for b in bumps {
        o.extend_from_slice(b);
    }
    varint(entries.len() as u64, &mut o);
    for e in entries {
        match (version, e) {
            (V2, Entry::Raw(t)) => {
                o.push(0);
                o.extend_from_slice(t);
            }
            (V2, Entry::Proven(k, t)) => {
                o.push(1);
                varint(*k, &mut o);
                o.extend_from_slice(t);
            }
            (V2, Entry::TxidOnly(t)) => {
                o.push(2);
                o.extend_from_slice(t);
            }
            (_, Entry::Raw(t)) => {
                o.extend_from_slice(t);
                o.push(0);
            }
            (_, Entry::Proven(k, t)) => {
                o.extend_from_slice(t);
                o.push(1);
                varint(*k, &mut o);
            }
            (_, Entry::TxidOnly(_)) => panic!("V1 has no txid-only entry"),
        }
    }
    o
}

/// `beef` behind the AtomicBEEF prefix naming `subject`.
pub fn atomic(subject: [u8; 32], beef: &[u8]) -> Vec<u8> {
    let mut o = ATOMIC.to_le_bytes().to_vec();
    o.extend_from_slice(&subject);
    o.extend_from_slice(beef);
    o
}

/// How many families there are.
pub const FAMILIES: usize = 17;

/// The families, each filled to about `target` bytes.
pub fn families(target: usize) -> Vec<(&'static str, Vec<u8>)> {
    families_only(target, None)
}

/// Family `k` alone, filled to about `target` bytes.
pub fn family(k: usize, target: usize) -> (&'static str, Vec<u8>) {
    families_only(target, Some(k)).swap_remove(0)
}

fn families_only(target: usize, only: Option<usize>) -> Vec<(&'static str, Vec<u8>)> {
    let mut v = Vec::new();
    // Txid-only entries (33 bytes each).
    if only.is_none_or(|o| o == 0) {
        let n = target / 33;
        let e: Vec<Entry> = (0..n as u64)
            .map(|k| {
                let mut t = [0u8; 32];
                t[..8].copy_from_slice(&k.to_le_bytes());
                Entry::TxidOnly(t)
            })
            .collect();
        v.push(("txid-only entries", beef(V2, &[], &e)));
    }
    // A chain of tiny unproven transactions over a proven root.
    if only.is_none_or(|o| o == 1) {
        let n = target / 61;
        let r = root(&[0], 0);
        let mut prev = id(&r);
        let mut e = vec![Entry::Proven(0, r.clone())];
        for _ in 0..n {
            let t = tx(
                &[In {
                    txid: prev,
                    vout: 0,
                    script: 0,
                }],
                &[0],
                0,
            );
            prev = id(&t);
            e.push(Entry::Raw(t));
        }
        v.push((
            "chain of tiny transactions",
            beef(V2, &[bump_of(1, id(&r))], &e),
        ));
    }
    // A proven parent of N outputs, each spent by its own tiny child.
    if only.is_none_or(|o| o == 2) {
        let n = target / 70;
        let p = root(&vec![0; n], 0);
        let pid = id(&p);
        let mut e = vec![Entry::Proven(0, p)];
        for k in 0..n {
            e.push(Entry::Raw(tx(
                &[In {
                    txid: pid,
                    vout: k as u32,
                    script: 0,
                }],
                &[0],
                0,
            )));
        }
        v.push(("fan of tiny transactions", beef(V2, &[bump_of(1, pid)], &e)));
    }
    // One unproven transaction of N empty outputs.
    if only.is_none_or(|o| o == 3) {
        let n = target / 9;
        let r = root(&[0], 0);
        let t = tx(
            &[In {
                txid: id(&r),
                vout: 0,
                script: 0,
            }],
            &vec![0; n],
            0,
        );
        let e = vec![Entry::Proven(0, r.clone()), Entry::Raw(t)];
        v.push((
            "one transaction, many outputs",
            beef(V2, &[bump_of(1, id(&r))], &e),
        ));
    }
    // One unproven transaction of N empty inputs, all of one proven parent.
    if only.is_none_or(|o| o == 4) {
        let n = target / 50;
        let p = root(&vec![1; n], 0);
        let pid = id(&p);
        let ins: Vec<In> = (0..n)
            .map(|k| In {
                txid: pid,
                vout: k as u32,
                script: 0,
            })
            .collect();
        let e = vec![Entry::Proven(0, p), Entry::Raw(tx(&ins, &[1], 0))];
        v.push((
            "one transaction, many inputs",
            beef(V2, &[bump_of(1, pid)], &e),
        ));
    }
    // One unproven transaction of N empty inputs, each from a missing parent.
    if only.is_none_or(|o| o == 5) {
        let n = target / 41;
        let ins: Vec<In> = (0..n)
            .map(|k| {
                let mut t = [7u8; 32];
                t[..8].copy_from_slice(&(k as u64).to_le_bytes());
                In {
                    txid: t,
                    vout: 0,
                    script: 0,
                }
            })
            .collect();
        v.push((
            "one transaction, inputs of distinct parents",
            beef(V2, &[], &[Entry::Raw(tx(&ins, &[], 0))]),
        ));
    }
    // Proven tiny transactions, one BUMP each.
    if only.is_none_or(|o| o == 6) {
        let n = target / 120;
        let txs: Vec<Vec<u8>> = (0..n).map(|k| root(&[0], k as u32)).collect();
        let bumps: Vec<Vec<u8>> = txs
            .iter()
            .enumerate()
            .map(|(k, t)| bump_of(k as u64, id(t)))
            .collect();
        let e: Vec<Entry> = txs
            .into_iter()
            .enumerate()
            .map(|(k, t)| Entry::Proven(k as u64, t))
            .collect();
        v.push((
            "proven tiny transactions, one BUMP each",
            beef(V2, &bumps, &e),
        ));
    }
    // One BUMP of many duplicate leaves (2 bytes each) at level 0 over one transaction,
    // as tall as its offsets need.
    if only.is_none_or(|o| o == 7) {
        let r = root(&[0], 0);
        let n = target / 2;
        v.push((
            "one BUMP of many leaves",
            beef(
                V2,
                &[bump(1, &many_leaves(id(&r), n as u64))],
                &[Entry::Proven(0, r)],
            ),
        ));
    }
    // Many BUMPs, each 64 levels with one leaf on each level.
    if only.is_none_or(|o| o == 8) {
        let per = 1 + 1 + 64 * 3 + 2 + 32;
        let n = (target / per).max(1);
        let r = root(&[0], 0);
        let bumps: Vec<Vec<u8>> = (0..n as u64)
            .map(|h| bump(h, &path_of(id(&r), 64, &mut Rng(h + 1))))
            .collect();
        v.push(("many tall BUMPs", beef(V2, &bumps, &[Entry::Proven(0, r)])));
    }
    // One large lock spent once, with a large unlock.
    if only.is_none_or(|o| o == 9) {
        let half = target / 2 - 200;
        let p = root(&[half], 0);
        let t = tx(
            &[In {
                txid: id(&p),
                vout: 0,
                script: half,
            }],
            &[1],
            0,
        );
        v.push((
            "one large script each way",
            beef(
                V2,
                &[bump_of(1, id(&p))],
                &[Entry::Proven(0, p.clone()), Entry::Raw(t)],
            ),
        ));
    }
    // The chain in V1.
    if only.is_none_or(|o| o == 10) {
        let n = target / 61;
        let r = root(&[0], 0);
        let mut prev = id(&r);
        let mut e = vec![Entry::Proven(0, r.clone())];
        for _ in 0..n {
            let t = tx(
                &[In {
                    txid: prev,
                    vout: 0,
                    script: 0,
                }],
                &[0],
                0,
            );
            prev = id(&t);
            e.push(Entry::Raw(t));
        }
        v.push((
            "chain of tiny transactions, V1",
            beef(V1, &[bump_of(1, id(&r))], &e),
        ));
    }
    // Tiny transactions of 2 inputs and 2 outputs, half proven, half unproven spending them.
    if only.is_none_or(|o| o == 11) {
        let n = target / 300;
        let mut bumps = Vec::new();
        let mut e = Vec::new();
        for k in 0..n {
            let p = root(&[0, 0], k as u32);
            let pid = id(&p);
            bumps.push(bump_of(k as u64, pid));
            e.push(Entry::Proven(k as u64, p));
            let ins = [
                In {
                    txid: pid,
                    vout: 0,
                    script: 0,
                },
                In {
                    txid: pid,
                    vout: 1,
                    script: 0,
                },
            ];
            e.push(Entry::Raw(tx(&ins, &[0, 0], 0)));
        }
        v.push(("pairs: proven parent, unproven child", beef(V2, &bumps, &e)));
    }
    // Many transactions of 200 empty outputs each.
    if only.is_none_or(|o| o == 12) {
        let n = (target / (200 * 9 + 50)).max(1);
        let e: Vec<Entry> = (0..n)
            .map(|k| Entry::Raw(root(&[0; 200], k as u32)))
            .collect();
        v.push(("many transactions of many outputs", beef(V2, &[], &e)));
    }
    // Many transactions of 100 empty inputs each, every input from a missing parent.
    if only.is_none_or(|o| o == 13) {
        let n = (target / (100 * 41 + 20)).max(1);
        let e: Vec<Entry> = (0..n)
            .map(|k| {
                let ins: Vec<In> = (0..100u64)
                    .map(|i| {
                        let mut t = [5u8; 32];
                        t[..8].copy_from_slice(&(k as u64 * 1000 + i).to_le_bytes());
                        In {
                            txid: t,
                            vout: 0,
                            script: 0,
                        }
                    })
                    .collect();
                Entry::Raw(tx(&ins, &[0], 0))
            })
            .collect();
        v.push(("many transactions of many inputs", beef(V2, &[], &e)));
    }
    // Many BUMPs of 500 duplicate leaves at level 0 (nine levels), each over its own transaction.
    if only.is_none_or(|o| o == 14) {
        let n = (target / (500 * 3 + 120)).max(1);
        let txs: Vec<Vec<u8>> = (0..n).map(|k| root(&[0], k as u32)).collect();
        let bumps: Vec<Vec<u8>> = txs
            .iter()
            .enumerate()
            .map(|(k, t)| bump(k as u64, &many_leaves(id(t), 500)))
            .collect();
        let e: Vec<Entry> = txs
            .into_iter()
            .enumerate()
            .map(|(k, t)| Entry::Proven(k as u64, t))
            .collect();
        v.push(("many BUMPs of many leaves", beef(V2, &bumps, &e)));
    }
    // A chain whose links each carry 20 outputs, every one spent by the next link.
    if only.is_none_or(|o| o == 15) {
        let n = (target / (20 * 9 + 20 * 41 + 20)).max(1);
        let r = root(&[0; 20], 0);
        let mut prev = id(&r);
        let mut e = vec![Entry::Proven(0, r.clone())];
        for _ in 0..n {
            let ins: Vec<In> = (0..20)
                .map(|o| In {
                    txid: prev,
                    vout: o,
                    script: 0,
                })
                .collect();
            let t = tx(&ins, &[0; 20], 0);
            prev = id(&t);
            e.push(Entry::Raw(t));
        }
        v.push(("chain of wide links", beef(V2, &[bump_of(1, id(&r))], &e)));
    }
    // Txid-only entries behind an AtomicBEEF prefix, half of them repeated.
    if only.is_none_or(|o| o == 16) {
        let n = target / 33;
        let e: Vec<Entry> = (0..n as u64)
            .map(|k| {
                let mut t = [1u8; 32];
                t[..8].copy_from_slice(&(k / 2).to_le_bytes());
                Entry::TxidOnly(t)
            })
            .collect();
        v.push((
            "repeated txid-only entries, atomic",
            atomic([1u8; 32], &beef(V2, &[], &e)),
        ));
    }
    v
}

/// A small deterministic generator (xorshift).
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// A random valid BEEF: proven roots, unproven spenders of earlier outputs,
/// txid-only entries, BUMPs of random shape; V1, V2 or AtomicBEEF.
pub fn random_beef(rng: &mut Rng, max_entries: u64) -> Vec<u8> {
    let version = if rng.below(4) == 0 { V1 } else { V2 };
    let n = 1 + rng.below(max_entries);
    let mut bumps: Vec<Vec<u8>> = Vec::new();
    let mut e = Vec::new();
    let mut spendable: Vec<([u8; 32], u32)> = Vec::new();
    let mut last = [0u8; 32];
    for k in 0..n {
        let wide = if rng.below(8) == 0 { 300 } else { 12 };
        let outs: Vec<usize> = (0..1 + rng.below(wide))
            .map(|_| [0, 1, 25, 25, 34, 80, 300][rng.below(7) as usize])
            .collect();
        let kind = rng.below(10);
        if version == V2 && kind == 0 {
            let mut t = [0u8; 32];
            t[..8].copy_from_slice(&rng.next().to_le_bytes());
            e.push(Entry::TxidOnly(t));
            continue;
        }
        if spendable.is_empty() || kind < 4 {
            let t = root(&outs, k as u32);
            let tid = id(&t);
            let levels = 1 + rng.below(24) as usize;
            let path = path_of(tid, levels, rng);
            bumps.push(bump(800_000 + k, &path));
            e.push(Entry::Proven(bumps.len() as u64 - 1, t));
            spendable.extend((0..outs.len() as u32).map(|o| (tid, o)));
            last = tid;
        } else {
            let ins: Vec<In> = (0..1 + rng.below(wide).min(spendable.len() as u64 - 1))
                .map(|_| {
                    let (txid, vout) =
                        spendable.swap_remove(rng.below(spendable.len() as u64) as usize);
                    In {
                        txid,
                        vout,
                        script: [0, 1, 72, 107, 107, 300][rng.below(6) as usize],
                    }
                })
                .collect();
            let t = tx(&ins, &outs, 0);
            let tid = id(&t);
            spendable.extend((0..outs.len() as u32).map(|o| (tid, o)));
            last = tid;
            e.push(Entry::Raw(t));
        }
        if spendable.is_empty() {
            spendable.push(([9u8; 32], 0));
        }
    }
    let b = beef(version, &bumps, &e);
    if rng.below(3) == 0 {
        atomic(last, &b)
    } else {
        b
    }
}
