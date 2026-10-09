//! NL-7c, the witness of the monitor's broadcast of a subject at rest: a
//! subject internalized from bytes at rest (NL-7) with no proof of its own,
//! which the network does not know, is posted by the monitor (`send_waiting`,
//! after the escalation's Rebroadcast hands it back as `unsent`). Its stored
//! `input_beef` is the whole AtomicBEEF, in R2 (`proven_tx_reqs/<id>/input_beef`).
//!
//! At the base the monitor reads it whole (`r2::BlobStore::get`), parses it
//! (`Beef::from_binary`), merges the raw subject, writes it again
//! (`to_binary`), hex-encodes it and puts it in a JSON body
//! (`src/monitor.rs:836-871`, `src/services/arc.rs:438-444`): measured below
//! on the 100,000-link payment. With NL-7c the stored object's 36-byte BRC-95
//! prefix is read, and when it names the row's subject the BEEF behind it is
//! handed to the broadcaster as a body stream, one fresh body per endpoint,
//! never held: measured below through the same path with a broadcaster that
//! drains each body a chunk at a time as a fetch does. One test function, so
//! no other test of this binary allocates inside the windows.

mod support;

use std::cell::{Cell, RefCell};
use std::io::Read;

use bsv_sdk::transaction::{AsyncByteSource, Beef};
use rust_wallet_infra::broadcast_at_rest::{
    atomic_subject, post_stored, StoredBeef, StreamBroadcast, ATOMIC_PREFIX_LEN,
};
use rust_wallet_infra::services::{BroadcastError, BroadcastResult};
use support::beef_chain::{
    atomic_size, display, heap, mib, subject, ChainSource, Chunked, ISOLATE_BYTES,
};

#[global_allocator]
static ALLOC: heap::Counting = heap::Counting;

const CHUNK: usize = 64 * 1024;

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

/// The stored `input_beef` of the chain of `n` links as R2 holds it: a size,
/// a ranged read of its first bytes, and a body from an offset that writes
/// itself as it is read. `atomic` false stores the BEEF without the prefix
/// (the shape `createAction` stores: ancestors only, to be merged).
struct HostStored {
    n: usize,
    atomic: bool,
    size: u64,
    bodies: Cell<u32>,
}

impl HostStored {
    fn new(n: usize, atomic: bool) -> Self {
        let size = if atomic {
            atomic_size(n)
        } else {
            atomic_size(n) - ATOMIC_PREFIX_LEN
        };
        Self {
            n,
            atomic,
            size,
            bodies: Cell::new(0),
        }
    }

    fn source(&self) -> ChainSource {
        if self.atomic {
            ChainSource::atomic(self.n)
        } else {
            ChainSource::new(self.n)
        }
    }
}

/// The body from an offset as a chunked source: in the Worker the trait's
/// body is a JS stream handed to fetch, here a reader the broadcaster drains.
struct HostBody(Chunked<ChainSource>);

impl AsyncByteSource for HostBody {
    async fn next_chunk(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.0.next_chunk().await
    }
}

impl StoredBeef for HostStored {
    type Body = HostBody;

    async fn size(&self) -> Result<Option<u64>, String> {
        Ok(Some(self.size))
    }

    async fn prefix(&self, length: u64) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        self.source()
            .take(length)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        Ok(bytes)
    }

    async fn body_from(&self, offset: u64) -> Result<HostBody, String> {
        self.bodies.set(self.bodies.get() + 1);
        let mut source = self.source();
        std::io::copy(&mut (&mut source).take(offset), &mut std::io::sink())
            .map_err(|e| e.to_string())?;
        Ok(HostBody(Chunked {
            inner: source,
            size: CHUNK,
        }))
    }
}

/// A broadcaster of two endpoints that drains each body a chunk at a time,
/// as a fetch reads a request body, and compares it byte for byte with the
/// BEEF behind the prefix.
struct Drain {
    endpoints: u32,
    n: usize,
    received: RefCell<Vec<u64>>,
}

impl StreamBroadcast<HostBody> for Drain {
    async fn broadcast_beef_body<S: StoredBeef<Body = HostBody>>(
        &self,
        stored: &S,
        offset: u64,
        length: u64,
    ) -> Result<BroadcastResult, BroadcastError> {
        for _ in 0..self.endpoints {
            let mut body = stored
                .body_from(offset)
                .await
                .map_err(BroadcastError::ServiceError)?;
            let mut expected = ChainSource::new(self.n);
            let mut got = 0u64;
            while let Some(chunk) = body.next_chunk().await.unwrap() {
                let mut want = vec![0u8; chunk.len()];
                expected.read_exact(&mut want).unwrap();
                assert!(chunk == want, "the body differs from the BEEF at {got}");
                got += chunk.len() as u64;
            }
            assert_eq!(got, length, "the body is the length the post names");
            self.received.borrow_mut().push(got);
        }
        Ok(BroadcastResult {
            txid: String::new(),
            tx_status: "SEEN_ON_NETWORK".to_string(),
            seen_on_network: true,
        })
    }
}

/// The base's route for the same stored object, step by step as
/// `send_waiting` takes it at `50a282f`; the peak over it.
fn whole_load_peak(n: usize) -> (usize, usize) {
    let (raw, _) = subject(n);
    let raw_hex = hex::encode(&raw);
    let start = heap::start();
    let mut stored = Vec::new(); // BlobStore::get: the object's bytes
    ChainSource::atomic(n).read_to_end(&mut stored).unwrap();
    let mut beef = Beef::from_binary(&stored).expect("the BEEF parses");
    beef.merge_raw_tx(hex::decode(&raw_hex).unwrap(), None);
    let merged_hex = hex::encode(beef.to_binary());
    let body = serde_json::json!({ "rawTx": merged_hex }).to_string(); // arc.rs:442
    let posted = body.len();
    let peak = heap::peak_since(start);
    drop((stored, beef, body));
    (peak, posted)
}

#[test]
fn the_monitor_posts_a_subject_at_rest_as_a_stream() {
    let n = 100_000usize;
    let (_, txid) = subject(n);
    let txid = display(&txid);

    // The prefix: the subject an AtomicBEEF names, and nothing else.
    let stored = HostStored::new(n, true);
    let prefix = block_on(stored.prefix(ATOMIC_PREFIX_LEN)).unwrap();
    assert_eq!(atomic_subject(&prefix).as_deref(), Some(txid.as_str()));
    assert_eq!(atomic_subject(&prefix[4..]), None);
    assert_eq!(atomic_subject(&prefix[..35]), None);

    // The base: whole.
    let (whole_peak, posted) = whole_load_peak(n);
    println!(
        "the base, {n} links ({} bytes): the monitor's whole load peaks at {:.1} MiB, a {:.1} MiB JSON body",
        stored.size,
        mib(whole_peak),
        mib(posted)
    );

    // NL-7c: streamed.
    let drain = Drain {
        endpoints: 2,
        n,
        received: RefCell::new(Vec::new()),
    };
    let start = heap::start();
    let outcome = block_on(post_stored(&stored, &drain, &txid))
        .expect("the store answered")
        .expect("an AtomicBEEF naming the row's subject is posted as a stream");
    let peak = heap::peak_since(start);
    assert!(outcome.is_ok());
    let beef_len = stored.size - ATOMIC_PREFIX_LEN;
    assert_eq!(*drain.received.borrow(), vec![beef_len, beef_len]);
    assert_eq!(stored.bodies.get(), 2, "one fresh body per endpoint");
    println!(
        "NL-7c, {n} links: the monitor's streamed post peaks at {:.1} KiB over two endpoints, {beef_len} bytes each, byte for byte the BEEF behind the prefix",
        peak as f64 / 1024.0
    );
    assert!(
        whole_peak > ISOLATE_BYTES,
        "the base held {:.1} MiB, within an isolate",
        mib(whole_peak)
    );
    assert!(
        peak < 1024 * 1024,
        "the streamed post held {:.1} MiB",
        mib(peak)
    );
    assert!(peak < ISOLATE_BYTES / 100);

    // Not posted as a stream: the shapes the base's route keeps.
    let ancestors_only = HostStored::new(n, false);
    assert!(block_on(post_stored(&ancestors_only, &drain, &txid))
        .unwrap()
        .is_none());
    let another = HostStored::new(3, true);
    assert!(block_on(post_stored(&another, &drain, &txid))
        .unwrap()
        .is_none());
    assert_eq!(
        another.bodies.get(),
        0,
        "no body is opened for another subject"
    );
}
