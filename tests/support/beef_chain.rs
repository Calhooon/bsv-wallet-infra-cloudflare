//! The deep chain of the no-limits program as a byte source that writes itself
//! link by link, so a reading of it holds no BEEF. Adapted from bsv-rs 0.4.1
//! `tests/support/beef_chain.rs` (the P0-5 chain: `tx[0]` carries a one-leaf
//! BUMP, every `tx[i]` spends `tx[i-1]:0` unproven, `OP_TRUE` locks spent by
//! empty unlocks), with the AtomicBEEF prefix (BRC-95) `internalizeAction`
//! asks for, and an asynchronous face for the Worker's reader.
#![allow(dead_code)]

use std::io::Read;

use bsv_sdk::primitives::sha256d;
use bsv_sdk::transaction::AsyncByteSource;

pub type Hash32 = [u8; 32];

pub const SATS: u64 = 1_000;
pub const HEIGHT: u64 = 800_000;
const OP_TRUE: u8 = 0x51;

/// The relay's line (rust-message-box `src/handoff.rs:356`, `DRAIN_INLINE_BYTES`):
/// a fee whose BEEF is over it is held in the ledger, never handed over.
pub const DRAIN_INLINE_BYTES: u64 = 8 * 1024 * 1024;

pub fn varint(n: u64) -> Vec<u8> {
    if n < 0xFD {
        vec![n as u8]
    } else if n < 0x1_0000 {
        let mut v = vec![0xFD];
        v.extend_from_slice(&(n as u16).to_le_bytes());
        v
    } else if n < 0x1_0000_0000 {
        let mut v = vec![0xFE];
        v.extend_from_slice(&(n as u32).to_le_bytes());
        v
    } else {
        let mut v = vec![0xFF];
        v.extend_from_slice(&n.to_le_bytes());
        v
    }
}

/// A transaction spending `prev:0` with an empty unlock, paying `outputs`
/// `OP_TRUE` outputs of `SATS` each.
pub fn spend(prev: &Hash32, outputs: usize) -> Vec<u8> {
    let mut v = 1u32.to_le_bytes().to_vec();
    v.push(1);
    v.extend_from_slice(prev);
    v.extend_from_slice(&0u32.to_le_bytes());
    v.push(0);
    v.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    v.extend(varint(outputs as u64));
    for _ in 0..outputs {
        v.extend_from_slice(&SATS.to_le_bytes());
        v.push(1);
        v.push(OP_TRUE);
    }
    v.extend_from_slice(&0u32.to_le_bytes());
    v
}

/// The proven funding transaction.
pub fn funding() -> Vec<u8> {
    spend(&[0xAA; 32], 2)
}

/// The funding transaction's txid: the root its one-leaf BUMP computes.
pub fn funding_root() -> Hash32 {
    sha256d(&funding())
}

/// The raw subject of the chain of `n` transactions, and its txid.
pub fn subject(n: usize) -> (Vec<u8>, Hash32) {
    let mut raw = funding();
    let mut prev = funding_root();
    for _ in 1..n {
        raw = spend(&prev, 1);
        prev = sha256d(&raw);
    }
    (raw, prev)
}

/// The txid in display order (reversed hex), as the wallet names it.
pub fn display(txid: &Hash32) -> String {
    let mut t = *txid;
    t.reverse();
    hex::encode(t)
}

/// The number of bytes of the AtomicBEEF of `n` links.
pub fn atomic_size(n: usize) -> u64 {
    let mut source = ChainSource::atomic(n);
    std::io::copy(&mut source, &mut std::io::sink()).unwrap()
}

/// The smallest chain whose AtomicBEEF is over `bytes`.
pub fn links_over(bytes: u64) -> usize {
    // 36 bytes of prefix, 47 of header, 64 of funding, 62 a link.
    let n = (bytes.saturating_sub(36 + 47 + 64) / 62) as usize + 1;
    let mut n = n.max(1);
    while atomic_size(n) <= bytes {
        n += 1;
    }
    n
}

/// The chain of `n` transactions as a BEEF V2, written as it is read; with
/// `atomic`, behind the BRC-95 prefix naming the last transaction.
pub struct ChainSource {
    n: usize,
    written: usize,
    prev: Hash32,
    piece: Vec<u8>,
    at: usize,
    /// The bytes handed out.
    pub total: u64,
}

impl ChainSource {
    pub fn new(n: usize) -> Self {
        Self::with_prefix(n, Vec::new())
    }

    pub fn atomic(n: usize) -> Self {
        let mut prefix = 0x0101_0101u32.to_le_bytes().to_vec();
        prefix.extend_from_slice(&subject(n).1);
        Self::with_prefix(n, prefix)
    }

    fn with_prefix(n: usize, mut piece: Vec<u8>) -> Self {
        assert!(n >= 1);
        piece.extend_from_slice(&0xEFBE_0002u32.to_le_bytes());
        piece.push(1);
        piece.extend(varint(HEIGHT));
        piece.extend_from_slice(&[1, 1, 0, 2]);
        piece.extend_from_slice(&funding_root());
        piece.extend(varint(n as u64));
        Self {
            n,
            written: 0,
            prev: [0u8; 32],
            piece,
            at: 0,
            total: 0,
        }
    }

    fn next_piece(&mut self) -> bool {
        if self.written == self.n {
            return false;
        }
        self.piece.clear();
        self.at = 0;
        let raw = if self.written == 0 {
            self.piece.extend_from_slice(&[1, 0]);
            funding()
        } else {
            self.piece.push(0);
            spend(&self.prev, 1)
        };
        self.prev = sha256d(&raw);
        self.piece.extend_from_slice(&raw);
        self.written += 1;
        true
    }
}

impl Read for ChainSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut n = 0;
        while n < buf.len() {
            if self.at == self.piece.len() && !self.next_piece() {
                break;
            }
            let take = (buf.len() - n).min(self.piece.len() - self.at);
            buf[n..n + take].copy_from_slice(&self.piece[self.at..self.at + take]);
            self.at += take;
            n += take;
        }
        self.total += n as u64;
        Ok(n)
    }
}

/// Any reader, a chunk of `size` bytes at a time, as an R2 body stream hands
/// them (`object.body().stream()`).
pub struct Chunked<R> {
    pub inner: R,
    pub size: usize,
}

impl<R: Read> AsyncByteSource for Chunked<R> {
    async fn next_chunk(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        let mut chunk = vec![0u8; self.size];
        let mut got = 0;
        while got < chunk.len() {
            let n = self.inner.read(&mut chunk[got..])?;
            if n == 0 {
                break;
            }
            got += n;
        }
        chunk.truncate(got);
        Ok((got > 0).then_some(chunk))
    }
}

/// A global allocator that counts the bytes live and their peak, so a test can
/// say what a reading held.
pub mod heap {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct Counting;

    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let p = System.alloc(layout);
            if !p.is_null() {
                let live = LIVE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
                PEAK.fetch_max(live, Ordering::SeqCst);
            }
            p
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout);
            LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
            let p = System.realloc(ptr, layout, new);
            if !p.is_null() {
                if new >= layout.size() {
                    let live =
                        LIVE.fetch_add(new - layout.size(), Ordering::SeqCst) + new - layout.size();
                    PEAK.fetch_max(live, Ordering::SeqCst);
                } else {
                    LIVE.fetch_sub(layout.size() - new, Ordering::SeqCst);
                }
            }
            p
        }
    }

    /// Start a window: the peak is reset to what is live now.
    pub fn start() -> usize {
        let live = LIVE.load(Ordering::SeqCst);
        PEAK.store(live, Ordering::SeqCst);
        live
    }

    /// The peak over the window, above what was live at its start.
    pub fn peak_since(start: usize) -> usize {
        PEAK.load(Ordering::SeqCst).saturating_sub(start)
    }
}

/// An isolate's memory on Cloudflare Workers: 128 MB.
pub const ISOLATE_BYTES: usize = 128 * 1024 * 1024;

pub fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}
