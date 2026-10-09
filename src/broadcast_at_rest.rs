//! The monitor's broadcast of a subject whose BEEF rests in R2 (NL-7c).
//!
//! A payment internalized from bytes at rest (NL-7) whose subject has no
//! proof of its own and which the network does not know is left to the
//! monitor: its `proven_tx_reqs` row is `unmined`, and the escalation's
//! Rebroadcast hands it back to `send_waiting` as `unsent`. Its stored
//! `input_beef` is the whole AtomicBEEF, in R2 (over the blob store's 4,096
//! bytes). The monitor used to read it whole, parse it, merge the raw subject
//! into it, write it again, hex-encode it and post it in a JSON body: on the
//! 100,000-link payment of 6.2 MB, a peak far over an isolate
//! (`tests/broadcast_at_rest.rs`).
//!
//! Here the object's 36-byte BRC-95 prefix is read (a ranged get), and when it
//! names the row's subject the BEEF behind it already holds the subject, so it
//! is posted as it rests: the broadcaster is handed a body stream from offset
//! 36 for each endpoint it asks, `application/octet-stream`, its length known,
//! and the bytes never enter the Worker's memory. ARC takes a BEEF as the
//! bytes of an octet-stream body and knows it by its version's `BEEF` marker
//! (`[SRC] bitcoin-sv/arc@e7efc5b6 internal/api/handler/parsers.go:50-51,
//! internal/beef/beef.go:24-34, internal/validator/helpers.go:36-38`), which
//! is why the AtomicBEEF prefix stays behind. Arcade's `/tx` takes a raw or
//! Extended Format transaction and no BEEF (`[SRC] bsv-blockchain/arcade@1ae1208
//! openapi/arcade.openapi.yaml:209-238`), so under either selection the stream
//! goes to ARC. Any other stored shape (the `createAction` ancestors, which
//! must be merged; an object of another subject) takes the route it took.

use std::cell::RefCell;
use std::future::Future;

use crate::services::{BroadcastError, BroadcastResult};

/// The BRC-95 prefix: four bytes `01010101` and the subject's txid.
pub const ATOMIC_PREFIX_LEN: u64 = 36;

/// The subject an AtomicBEEF's prefix names, in display order; `None` for
/// bytes that are not the prefix.
pub fn atomic_subject(prefix: &[u8]) -> Option<String> {
    if prefix.len() != ATOMIC_PREFIX_LEN as usize || prefix[..4] != [1, 1, 1, 1] {
        return None;
    }
    let mut txid = prefix[4..].to_vec();
    txid.reverse();
    Some(hex::encode(txid))
}

/// A stored BEEF the monitor posts without reading it: its size, its first
/// bytes, and a fresh body from an offset.
pub trait StoredBeef {
    /// What a broadcaster posts: a JS stream in the Worker.
    type Body;
    /// The object's size; `None` when there is no object.
    fn size(&self) -> impl Future<Output = Result<Option<u64>, String>>;
    /// The object's first `length` bytes.
    fn prefix(&self, length: u64) -> impl Future<Output = Result<Vec<u8>, String>>;
    /// A body of the object from `offset` to its end, unread.
    fn body_from(&self, offset: u64) -> impl Future<Output = Result<Self::Body, String>>;
}

/// A broadcaster that posts a BEEF as a body stream: `length` bytes of
/// `stored` from `offset`, one fresh body per endpoint it asks.
pub trait StreamBroadcast<Body> {
    fn broadcast_beef_body<S: StoredBeef<Body = Body>>(
        &self,
        stored: &S,
        offset: u64,
        length: u64,
    ) -> impl Future<Output = Result<BroadcastResult, BroadcastError>>;
}

/// Post the stored BEEF of the subject `txid` as a stream when the object is
/// an AtomicBEEF naming it. `Ok(None)`: not that shape (no object, another
/// subject, no prefix); the caller takes the route it took. `Err`: the store
/// did not answer.
pub async fn post_stored<S: StoredBeef, P: StreamBroadcast<S::Body>>(
    stored: &S,
    broadcaster: &P,
    txid: &str,
) -> Result<Option<Result<BroadcastResult, BroadcastError>>, String> {
    let Some(size) = stored.size().await? else {
        return Ok(None);
    };
    if size <= ATOMIC_PREFIX_LEN {
        return Ok(None);
    }
    let prefix = stored.prefix(ATOMIC_PREFIX_LEN).await?;
    if atomic_subject(&prefix).as_deref() != Some(txid) {
        return Ok(None);
    }
    Ok(Some(
        broadcaster
            .broadcast_beef_body(stored, ATOMIC_PREFIX_LEN, size - ATOMIC_PREFIX_LEN)
            .await,
    ))
}

// =============================================================================
// The object in R2
// =============================================================================

/// A stored BEEF in R2 at `key`. Every read after the head is conditional on
/// the etag the head saw, so the prefix and the bodies are one upload's.
pub struct R2Stored<'a> {
    bucket: &'a worker::Bucket,
    key: String,
    etag: RefCell<Option<String>>,
}

impl<'a> R2Stored<'a> {
    pub fn new(bucket: &'a worker::Bucket, key: String) -> Self {
        Self {
            bucket,
            key,
            etag: RefCell::new(None),
        }
    }

    async fn ranged(&self, range: worker::Range) -> Result<worker::Object, String> {
        let etag = self.etag.borrow().clone();
        let mut get = self.bucket.get(&self.key).range(range);
        if let Some(etag) = etag {
            get = get.only_if(worker::Conditional {
                etag_matches: Some(etag),
                etag_does_not_match: None,
                uploaded_before: None,
                uploaded_after: None,
            });
        }
        let object = get
            .execute()
            .await
            .map_err(|e| format!("R2 get {}: {e}", self.key))?
            .ok_or_else(|| format!("no object at {}", self.key))?;
        if object.body().is_none() {
            return Err(format!("another upload is at {}", self.key));
        }
        Ok(object)
    }
}

impl StoredBeef for R2Stored<'_> {
    type Body = wasm_bindgen::JsValue;

    async fn size(&self) -> Result<Option<u64>, String> {
        let head = self
            .bucket
            .head(&self.key)
            .await
            .map_err(|e| format!("R2 head {}: {e}", self.key))?;
        Ok(head.map(|object| {
            *self.etag.borrow_mut() = Some(object.etag());
            object.size()
        }))
    }

    async fn prefix(&self, length: u64) -> Result<Vec<u8>, String> {
        let object = self.ranged(worker::Range::Prefix { length }).await?;
        let body = object.body().ok_or("the prefix has no body")?;
        body.bytes()
            .await
            .map_err(|e| format!("R2 prefix {}: {e}", self.key))
    }

    /// The object's body from `offset`, piped through a `FixedLengthStream`
    /// so the post carries its length; the bytes stay in the runtime.
    async fn body_from(&self, offset: u64) -> Result<wasm_bindgen::JsValue, String> {
        use wasm_bindgen::JsCast;
        let object = self.ranged(worker::Range::OffsetToEnd { offset }).await?;
        let length = object.size() - offset;
        let body = object.body().ok_or("the body is gone")?;
        let worker::ResponseBody::Stream(stream) = body
            .response_body()
            .map_err(|e| format!("R2 body {}: {e}", self.key))?
        else {
            return Err(format!("R2 body {}: not a stream", self.key));
        };
        let fixed = if length < u32::MAX as u64 {
            worker::worker_sys::FixedLengthStream::new(length as u32)
        } else {
            worker::worker_sys::FixedLengthStream::new_big_int(js_sys::BigInt::from(length))
        }
        .map_err(|e| format!("FixedLengthStream: {e:?}"))?;
        // `stream.pipeThrough(fixed)`: a FixedLengthStream is a
        // TransformStream (web-sys's ReadableWritablePair is not a feature
        // this build turns on).
        let pipe_through: js_sys::Function =
            js_sys::Reflect::get(&stream, &wasm_bindgen::JsValue::from_str("pipeThrough"))
                .map_err(|e| format!("pipeThrough: {e:?}"))?
                .dyn_into()
                .map_err(|e| format!("pipeThrough: {e:?}"))?;
        pipe_through
            .call1(&stream, &fixed)
            .map_err(|e| format!("pipeThrough: {e:?}"))
    }
}
