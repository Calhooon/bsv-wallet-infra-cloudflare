//! `internalizeAction` from bytes at rest (NL-7, the no-limits program).
//!
//! Beside the inline `tx`, the method takes a reference to an AtomicBEEF that
//! rests in an R2 bucket this Worker is bound to:
//!
//! ```json
//! { "beefAtRest": { "r2Key": "<key>", "size": 6200098, "etag": "<etag>",
//!                   "bucket": "bsv-messagebox-beefs" },
//!   "outputs": [..], "description": "..", "labels": [..] }
//! ```
//!
//! The inline door holds the BEEF many times over before its first D1 call
//! (the body, the parsed argument, its clone, the decoded bytes, the parsed
//! `Beef` and its clone): `tests/inline_beef_witness.rs` measures 342 MiB for a
//! 100,000-link payment of 6.2 MB, so the relay holds a fee over 8 MiB in its
//! ledger and never hands it over (rust-message-box `src/handoff.rs:350-356`).
//! Here the object is held to the reference (bucket, etag, size), read as its
//! body stream (`object.body().stream()`, never `bytes()`), and verified by
//! bsv-rs 0.4.1's `AsyncStreamVerifier` with the scripts, one element in hand
//! (`src/transaction/beef_stream.rs:2804-2890`). Beside it a second decoder
//! sees the same chunks and keeps the one thing the storage needs, the
//! subject transaction, and the place of each BUMP. Each root the reading
//! returns is then asked of the header service, as the inline path asks it
//! (`storage/beef_verification.rs`); a root not carried is refused at its
//! BUMP's offset, a lookup that fails is neither an acceptance nor a refusal.
//! A refusal names the offset and the kind (one of bsv-rs's nineteen) or the
//! refused spend.
//!
//! The reference is honoured only for the caller the object names (NL-7c):
//! the relay writes the recipient's identity key on every object it spools as
//! the custom metadata `recipient-identity-key`, and the object's head is held
//! to the signed-in caller's identity key before the body is asked for. A
//! missing or different key is refused by reason, with no body read and no
//! copy. An abuse bound (a caller reads its own payments and nothing else),
//! never a size.

use std::collections::HashMap;
use std::io;

use bsv_sdk::transaction::beef_stream::{display_hex, AsyncVerifyError, Hash32, Step, TxBody};
use bsv_sdk::transaction::{
    AlwaysValidChainTracker, AsyncByteSource, AsyncStreamVerifier, BeefDecoder, Element, Verdict,
};
use bsv_sdk::wallet::InternalizeActionArgs;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Error;
use crate::services::chaintracker::HeaderService;

// =============================================================================
// The argument
// =============================================================================

/// A reference to an AtomicBEEF at rest in R2: the object at `r2_key` in
/// `bucket`, of `size` bytes, as uploaded with `etag`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeefAtRest {
    pub r2_key: String,
    pub size: u64,
    pub etag: String,
    pub bucket: String,
}

/// The arguments of `internalizeAction`: the BEEF inline, or a reference to it
/// at rest beside the rest of the arguments (whose `tx` is then empty).
#[derive(Debug, Clone)]
pub enum InternalizeParams {
    Inline(InternalizeActionArgs),
    AtRest {
        reference: BeefAtRest,
        args: InternalizeActionArgs,
    },
}

/// Read `internalizeAction`'s params, positional (`[auth, args]`) or named.
/// `tx` and `beefAtRest` are each the BEEF: exactly one of them is given.
pub fn parse_internalize_params(params: &Value) -> Result<InternalizeParams, Error> {
    let mut args_val = crate::dispatch::extract_args(params, true);
    let reference = args_val
        .as_object_mut()
        .and_then(|args| args.remove("beefAtRest"));
    let Some(reference) = reference else {
        // The bsv-sdk InternalizeActionArgs expects `tx` as a hex string (via
        // #[serde(with = "hex_bytes")]). The payment middleware sends `tx` as a
        // JSON array of byte values: convert array to hex so both decode.
        if let Some(tx_val) = args_val.get("tx") {
            if let Some(items) = tx_val.as_array() {
                let bytes: Vec<u8> = items
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect();
                args_val["tx"] = Value::String(hex::encode(&bytes));
            }
        }
        return Ok(InternalizeParams::Inline(serde_json::from_value(args_val)?));
    };
    if args_val.get("tx").is_some() {
        return Err(Error::ValidationError(
            "internalizeAction takes the BEEF as `tx` or as `beefAtRest`, not both".to_string(),
        ));
    }
    let reference: BeefAtRest = serde_json::from_value(reference)
        .map_err(|e| Error::ValidationError(format!("beefAtRest: {e}")))?;
    for (field, value) in [
        ("r2Key", &reference.r2_key),
        ("etag", &reference.etag),
        ("bucket", &reference.bucket),
    ] {
        if value.is_empty() {
            return Err(Error::ValidationError(format!(
                "beefAtRest: `{field}` is empty"
            )));
        }
    }
    args_val["tx"] = Value::String(String::new());
    let args = serde_json::from_value(args_val)?;
    Ok(InternalizeParams::AtRest { reference, args })
}

// =============================================================================
// The reading
// =============================================================================

/// What the storage is handed of a BEEF at rest that was accepted: the
/// subject and nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtRestReading {
    /// The subject's txid, display order.
    pub txid: String,
    pub raw_tx: Vec<u8>,
    pub version: u32,
    pub lock_time: u32,
    /// Each output's satoshis and locking script.
    pub outputs: Vec<(u64, Vec<u8>)>,
    /// The subject carries a BUMP of its own.
    pub has_proof: bool,
    pub bytes_read: u64,
}

/// Why a BEEF at rest was not internalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtRestError {
    /// The bytes are invalid: the offset of the byte and the kind.
    Refused {
        offset: u64,
        kind: String,
        reason: String,
    },
    /// The bytes are well formed and the interpreter refused a spend.
    SpendRefused {
        offset: u64,
        txid: String,
        input: Option<u32>,
        why: String,
    },
    /// No BRC-95 prefix names a subject.
    NotAtomic,
    /// The subject is a txid-only entry: no transaction to internalize.
    SubjectWithoutBody,
    /// The object is not as long as the reference says.
    Size { named: u64, read: u64 },
    /// The object store failed. Nothing about the bytes.
    Source(String),
    /// The header service failed. Nothing about the bytes.
    Headers(String),
}

impl AtRestError {
    /// The refusal's reason with its data, when it is a refusal of the bytes.
    pub fn reason(&self) -> String {
        match self {
            AtRestError::Refused { reason, .. } => reason.clone(),
            _ => String::new(),
        }
    }
}

impl std::fmt::Display for AtRestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtRestError::Refused {
                offset,
                kind,
                reason,
            } => write!(
                f,
                "the BEEF at rest is refused at offset {offset}: {kind} ({reason})"
            ),
            AtRestError::SpendRefused {
                offset,
                txid,
                input,
                why,
            } => match input {
                Some(i) => write!(
                    f,
                    "the BEEF at rest is refused at offset {offset}: input {i} of {txid} does not spend ({why})"
                ),
                None => write!(
                    f,
                    "the BEEF at rest is refused at offset {offset}: {txid} does not spend ({why})"
                ),
            },
            AtRestError::NotAtomic => {
                write!(f, "the BEEF at rest is not AtomicBEEF (no subject prefix)")
            }
            AtRestError::SubjectWithoutBody => {
                write!(f, "the BEEF at rest names a txid-only subject")
            }
            AtRestError::Size { named, read } => write!(
                f,
                "the object at rest is not the one referenced: {read} bytes read where the reference names {named}"
            ),
            AtRestError::Source(e) => write!(f, "the object at rest could not be read: {e}"),
            AtRestError::Headers(e) => write!(f, "the header service could not answer: {e}"),
        }
    }
}

impl From<AtRestError> for Error {
    fn from(e: AtRestError) -> Self {
        match e {
            AtRestError::Source(_) | AtRestError::Headers(_) => Error::InternalError(e.to_string()),
            _ => Error::ValidationError(e.to_string()),
        }
    }
}

/// The source as the verifier reads it, with a second decoder watching the
/// same chunks for the subject and the BUMPs' places. The decoder holds one
/// element at a time; the subject is kept when it passes.
struct Tee<'s, S> {
    inner: &'s mut S,
    decoder: BeefDecoder,
    watching: bool,
    subject: Option<(Hash32, TxBody, bool)>,
    /// Each BUMP's offset, height and root.
    bumps: Vec<(u64, u64, Hash32)>,
    read: u64,
    named: u64,
}

impl<S> Tee<'_, S> {
    fn watch(&mut self, mut input: &[u8]) {
        while self.watching {
            match self.decoder.next(&mut input) {
                Ok(Step::Element(Element::Tx {
                    txid,
                    body,
                    bump_index,
                    ..
                })) => {
                    if self.decoder.subject() == Some(txid) {
                        self.subject = Some((txid, body, bump_index.is_some()));
                    }
                }
                Ok(Step::Element(Element::Bump(bump))) => {
                    self.bumps.push((bump.offset, bump.block_height, bump.root));
                }
                Ok(Step::Element(Element::TxidOnly { .. })) => {}
                Ok(Step::NeedMore) | Ok(Step::Done) => break,
                // The verifier reads the same bytes and names the refusal.
                Err(_) => self.watching = false,
            }
        }
    }
}

impl<S: AsyncByteSource> AsyncByteSource for Tee<'_, S> {
    async fn next_chunk(&mut self) -> io::Result<Option<Vec<u8>>> {
        let chunk = self.inner.next_chunk().await?;
        if let Some(chunk) = &chunk {
            self.read += chunk.len() as u64;
            if self.read > self.named {
                return Err(io::Error::new(io::ErrorKind::InvalidData, OVER_NAMED));
            }
            self.watch(chunk);
        }
        Ok(chunk)
    }
}

const OVER_NAMED: &str = "more bytes than the reference names";

/// Read an AtomicBEEF from `source`, `named` bytes long, and verify it: the
/// structure and the scripts as it streams, then each root against `headers`.
/// What is handed back is the subject alone.
pub async fn read_at_rest<S: AsyncByteSource, H: HeaderService>(
    source: &mut S,
    headers: &H,
    named: u64,
) -> Result<AtRestReading, AtRestError> {
    let mut tee = Tee {
        inner: source,
        decoder: BeefDecoder::new(),
        watching: true,
        subject: None,
        bumps: Vec::new(),
        read: 0,
        named,
    };
    // Every root is carried while the bytes stream; each is asked below,
    // where the header service's own future runs (it is not `Send`).
    let tracker = AlwaysValidChainTracker::new(0);
    let verdict = match AsyncStreamVerifier::new(None).run(&mut tee, &tracker).await {
        Ok(verdict) => verdict,
        Err(AsyncVerifyError::Source(e)) if e.to_string() == OVER_NAMED => {
            return Err(AtRestError::Size {
                named,
                read: tee.read,
            })
        }
        Err(AsyncVerifyError::Source(e)) => return Err(AtRestError::Source(e.to_string())),
        Err(AsyncVerifyError::Headers(e)) => return Err(AtRestError::Headers(e.to_string())),
    };
    if tee.read != named {
        return Err(AtRestError::Size {
            named,
            read: tee.read,
        });
    }
    let roots = match verdict {
        Verdict::Valid {
            subject: Some(_),
            roots,
        } => roots,
        Verdict::Valid { subject: None, .. } => return Err(AtRestError::NotAtomic),
        Verdict::Invalid {
            offset,
            kind,
            reason,
        } => {
            return Err(AtRestError::Refused {
                offset,
                kind: format!("{kind:?}"),
                reason: format!("{reason:?}"),
            })
        }
        Verdict::SpendRefused {
            offset,
            txid,
            input,
            why,
        } => {
            return Err(AtRestError::SpendRefused {
                offset,
                txid: display_hex(&txid),
                input,
                why: format!("{why:?}"),
            })
        }
    };
    let mut asked: Vec<(u64, Hash32)> = Vec::new();
    for (height, root) in roots {
        if asked.contains(&(height, root)) {
            continue;
        }
        asked.push((height, root));
        let offset = tee
            .bumps
            .iter()
            .find(|(_, h, r)| *h == height && *r == root)
            .map(|(o, _, _)| *o)
            .unwrap_or(0);
        let not_carried = || AtRestError::Refused {
            offset,
            kind: "RootNotCarried".to_string(),
            reason: format!(
                "RootNotCarried {{ height: {height}, root: {} }}",
                display_hex(&root)
            ),
        };
        let Ok(h) = u32::try_from(height) else {
            return Err(not_carried());
        };
        match headers
            .is_valid_root_for_height(&display_hex(&root), h)
            .await
        {
            Ok(true) => {}
            Ok(false) => return Err(not_carried()),
            Err(e) => return Err(AtRestError::Headers(format!("height {height}: {e}"))),
        }
    }
    let Some((txid, body, has_proof)) = tee.subject else {
        return Err(AtRestError::SubjectWithoutBody);
    };
    let outputs = body
        .outputs
        .iter()
        .map(|o| (o.satoshis, body.raw[o.script.clone()].to_vec()))
        .collect();
    Ok(AtRestReading {
        txid: display_hex(&txid),
        version: body.version,
        lock_time: body.lock_time,
        outputs,
        raw_tx: body.raw,
        has_proof,
        bytes_read: tee.read,
    })
}

// =============================================================================
// The object in R2
// =============================================================================

/// The bucket references may name: the binding `BEEF_AT_REST` and the name of
/// the bucket bound there (`BEEF_AT_REST_BUCKET`; a binding does not say it).
pub struct AtRestBucket<'a> {
    pub bucket: &'a worker::Bucket,
    pub name: String,
}

/// An R2 body stream as the reader's source.
pub struct R2Body(pub worker::ByteStream);

impl AsyncByteSource for R2Body {
    async fn next_chunk(&mut self) -> io::Result<Option<Vec<u8>>> {
        use futures_util::StreamExt;
        match self.0.next().await {
            None => Ok(None),
            Some(Ok(chunk)) => Ok(Some(chunk)),
            Some(Err(e)) => Err(io::Error::other(e.to_string())),
        }
    }
}

/// The etag as R2 reports it, without the quotes of the HTTP form.
fn bare(etag: &str) -> &str {
    etag.trim_matches('"')
}

/// The custom metadata on a spooled object that names the identity key of the
/// caller it is for (the relay writes it, rust-message-box NL-7b).
pub const RECIPIENT_IDENTITY_KEY: &str = "recipient-identity-key";

/// Hold an object's head to the reference and the caller: the caller first,
/// so a refusal never says another caller's object's etag or size, and never
/// whose it is; then the etag and the size. Identity keys are compared as
/// the hex they are, in either case.
pub fn hold_to(
    r: &BeefAtRest,
    etag: &str,
    size: u64,
    metadata: &HashMap<String, String>,
    caller: &str,
) -> Result<(), Error> {
    match metadata.get(RECIPIENT_IDENTITY_KEY).map(String::as_str) {
        None | Some("") => {
            return Err(Error::ValidationError(format!(
                "beefAtRest: the object at {} names no recipient ({RECIPIENT_IDENTITY_KEY}); it is read only for the caller it names",
                r.r2_key
            )))
        }
        Some(named) if !named.eq_ignore_ascii_case(caller) => {
            return Err(Error::ValidationError(format!(
                "beefAtRest: the object at {} names another recipient than the caller; it is read only for the caller it names",
                r.r2_key
            )))
        }
        Some(_) => {}
    }
    if bare(etag) != bare(&r.etag) {
        return Err(Error::ValidationError(format!(
            "beefAtRest: another upload is at {} (etag {}, the reference names {})",
            r.r2_key, etag, r.etag
        )));
    }
    if size != r.size {
        return Err(Error::ValidationError(format!(
            "beefAtRest: the object at {} is {} bytes, the reference names {}",
            r.r2_key, size, r.size
        )));
    }
    Ok(())
}

fn custom_metadata(object: &worker::Object) -> Result<HashMap<String, String>, Error> {
    object
        .custom_metadata()
        .map_err(|e| Error::InternalError(format!("beefAtRest: the object's metadata: {e}")))
}

/// The object a reference names for `caller`, held to its bucket, the caller
/// it names, its etag and its size, with its body. The head is read first
/// and carries no body: an object for another caller is refused before its
/// body is asked for. The get is then conditional on the etag, and its head
/// is held again before the body is handed out. Another upload at the key is
/// never read as this one.
pub async fn open(
    store: &AtRestBucket<'_>,
    r: &BeefAtRest,
    caller: &str,
) -> Result<worker::Object, Error> {
    if r.bucket != store.name {
        return Err(Error::ValidationError(format!(
            "beefAtRest: bucket {} is not bound here (this Worker reads {})",
            r.bucket, store.name
        )));
    }
    let no_object = || Error::ValidationError(format!("beefAtRest: no object at {}", r.r2_key));
    let head = store
        .bucket
        .head(&r.r2_key)
        .await
        .map_err(|e| Error::InternalError(format!("beefAtRest: R2 head failed: {e}")))?
        .ok_or_else(no_object)?;
    hold_to(
        r,
        &head.etag(),
        head.size(),
        &custom_metadata(&head)?,
        caller,
    )?;
    let object = store
        .bucket
        .get(&r.r2_key)
        .only_if(worker::Conditional {
            etag_matches: Some(bare(&r.etag).to_string()),
            etag_does_not_match: None,
            uploaded_before: None,
            uploaded_after: None,
        })
        .execute()
        .await
        .map_err(|e| Error::InternalError(format!("beefAtRest: R2 get failed: {e}")))?
        .ok_or_else(no_object)?;
    hold_to(
        r,
        &object.etag(),
        object.size(),
        &custom_metadata(&object)?,
        caller,
    )?;
    if object.body().is_none() {
        return Err(Error::ValidationError(format!(
            "beefAtRest: another upload is at {} (etag {}, the reference names {})",
            r.r2_key,
            object.etag(),
            r.etag
        )));
    }
    Ok(object)
}

/// The object's body stream as the reader's source.
pub fn body_of(object: &worker::Object) -> Result<R2Body, Error> {
    let body = object
        .body()
        .ok_or_else(|| Error::InternalError("beefAtRest: the object has no body".to_string()))?;
    let stream = body
        .stream()
        .map_err(|e| Error::InternalError(format!("beefAtRest: the body stream: {e}")))?;
    Ok(R2Body(stream))
}

/// Copy the referenced object into `dest` at `key` as a stream, held to the
/// reference again: the bytes pass through one chunk at a time.
pub async fn copy_to(
    store: &AtRestBucket<'_>,
    r: &BeefAtRest,
    caller: &str,
    dest: &worker::Bucket,
    key: &str,
) -> Result<(), Error> {
    let object = open(store, r, caller).await?;
    let R2Body(stream) = body_of(&object)?;
    let fixed = worker::FixedLengthStream::wrap(stream, r.size);
    dest.put(key, worker::Data::Stream(fixed))
        .execute()
        .await
        .map_err(|e| Error::InternalError(format!("beefAtRest: R2 put of {key} failed: {e}")))?;
    Ok(())
}

/// The referenced object's bytes, read whole: for an object no larger than
/// the blob store keeps in D1 (`r2.rs`, 4,096 bytes), which goes the inline
/// way once it has been verified.
pub async fn read_small(
    store: &AtRestBucket<'_>,
    r: &BeefAtRest,
    caller: &str,
) -> Result<Vec<u8>, Error> {
    let object = open(store, r, caller).await?;
    let mut source = body_of(&object)?;
    let mut bytes = Vec::with_capacity(r.size as usize);
    while let Some(chunk) = source
        .next_chunk()
        .await
        .map_err(|e| Error::InternalError(format!("beefAtRest: {e}")))?
    {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
