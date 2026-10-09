//! NL-7, the witness of the inline door: what `internalizeAction` holds when a
//! BEEF arrives as its JSON-RPC argument, measured by a counting allocator
//! through the steps the Worker takes before its first D1 call:
//!
//! 1. the BRC-31 layer reads the body whole (`src/lib.rs`, `process_auth`,
//!    `AuthResult::Authenticated { body, .. }`), and it lives to the response;
//! 2. `serde_json::from_slice::<JsonRpcRequest>` (`src/lib.rs`);
//! 3. `extract_args` clones the argument (`src/dispatch.rs:42`);
//! 4. `InternalizeActionArgs` decodes `tx` (`src/dispatch.rs:246`);
//! 5. `Beef::from_binary`, a clone, `verify_valid`
//!    (`src/storage/internalize_action.rs:265-284`).
//!
//! The isolate a Worker runs in has 128 MB. One test function, so no other
//! test of this binary allocates inside the window.

mod support;

use bsv_sdk::transaction::Beef;
use bsv_sdk::wallet::InternalizeActionArgs;
use rust_wallet_infra::json_rpc::JsonRpcRequest;
use std::io::Read;
use support::beef_chain::{heap, links_over, mib, ChainSource, DRAIN_INLINE_BYTES, ISOLATE_BYTES};

#[global_allocator]
static ALLOC: heap::Counting = heap::Counting;

/// The JSON-RPC body a caller sends for the AtomicBEEF `beef`, `tx` in hex
/// (the shape `InternalizeActionArgs` decodes), positional as the toolbox's
/// StorageClient and the relay's WorkerStorageClient send it.
fn body_for(beef: &[u8]) -> Vec<u8> {
    let mut body = String::with_capacity(beef.len() * 2 + 512);
    body.push_str(r#"{"jsonrpc":"2.0","method":"internalizeAction","id":1,"params":[{"identityKey":"02aa"},{"tx":""#);
    body.push_str(&hex::encode(beef));
    body.push_str(r#"","outputs":[{"outputIndex":0,"protocol":"basket insertion","insertionRemittance":{"basket":"nl7"}}],"description":"the NL-7 witness"}]}"#);
    body.into_bytes()
}

/// The peak over the inline door's steps, and whether the BEEF was valid.
fn inline_peak(beef: &[u8]) -> (usize, bool) {
    let body_bytes = body_for(beef);
    let start = heap::start();
    let request_body = body_bytes.clone(); // the body as the BRC-31 layer hands it over
    let rpc: JsonRpcRequest = serde_json::from_slice(&request_body).expect("the body parses");
    let args_val = rpc.params[1].clone(); // extract_args
    let args: InternalizeActionArgs = serde_json::from_value(args_val).expect("the args parse");
    let beef_parsed = Beef::from_binary(&args.tx).expect("the BEEF parses");
    let mut beef_clone = beef_parsed.clone();
    let valid = beef_clone.verify_valid(false).valid;
    let peak = heap::peak_since(start);
    drop((request_body, rpc, args, beef_parsed, beef_clone));
    (peak, valid)
}

fn whole(n: usize) -> Vec<u8> {
    let mut beef = Vec::new();
    ChainSource::atomic(n).read_to_end(&mut beef).unwrap();
    beef
}

#[test]
fn the_inline_argument_holds_the_beef_many_times_over() {
    let over_the_line = links_over(DRAIN_INLINE_BYTES);
    for n in [100_000usize, over_the_line] {
        let beef = whole(n);
        let (peak, valid) = inline_peak(&beef);
        println!(
            "inline door: {n} links, {} bytes ({:.2} MiB): peak {:.1} MiB, {:.1} times the BEEF, valid {valid}",
            beef.len(),
            mib(beef.len()),
            mib(peak),
            peak as f64 / beef.len() as f64
        );
        assert!(valid, "the chain is a valid BEEF");
        assert!(
            peak > ISOLATE_BYTES,
            "{n} links: the inline door held {:.1} MiB, within an isolate",
            mib(peak)
        );
    }
}
