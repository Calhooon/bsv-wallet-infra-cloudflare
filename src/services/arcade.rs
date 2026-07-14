//! Arcade V2 broadcast provider (the ARC successor, Teranode-native).
//!
//! The pure core (BEEF→EF batch conversion + the SSE verdict discipline) is ported from
//! the maintainer's mainnet-proven reqwest/tokio implementation — adapted here to this
//! crate's `BroadcastService` error taxonomy and to the `worker` Fetch transport
//! (streaming body read for SSE).
//!
//! The verified Arcade V2 facts this module encodes (empirically verified against the
//! live endpoint + the `github.com/bsv-blockchain/arcade` source, 2026-07-10):
//!   • **EF only** — Arcade rejects EVERY BEEF form (V1/V2/Atomic → 400). Submit Extended
//!     Format (BRC-30); every UNMINED ancestor in the BEEF must be individually converted
//!     (dependency order) and submitted — interior chain TXs no longer "ride along" inside
//!     a BEEF envelope. Arcade dedupes known ancestors.
//!   • **Async-only verdicts** — a valid-script TX gets `202 RECEIVED` at submit; the real
//!     verdict arrives later over SSE. Structural rejects (parse/EF/script/fee) ARE sync 400s.
//!   • **SSE push-wait** — gate on **SEEN_ON_NETWORK** (~3s measured); `SEEN_MULTIPLE_NODES`
//!     is erratic (>20s) — NEVER gate on it. 20s without the gate status = failure.
//!   • **Async `REJECTED` / `DOUBLE_SPEND_ATTEMPTED` FAIL HARD** — a caller that ignored
//!     them would "succeed" a dead TX (the money-rails verdict law).
//!   • Free `merklePath` arrives on the MINED SSE payload — OUT OF SCOPE this migration
//!     (the monitor's existing proof providers keep that job).

use std::collections::HashMap;

use bsv_sdk::transaction::{Beef, Transaction};

use super::{BroadcastError, BroadcastResult};

/// Default live mainnet endpoint. Override with the `ARCADE_URL` env var.
pub const ARCADE_URL_DEFAULT: &str = "https://arcade-v2-us-1.bsvblockchain.tech";

/// The propagation status the money rail gates on (~3s measured on mainnet).
/// NEVER gate on `SEEN_MULTIPLE_NODES` — it is erratic (sometimes >20s).
pub const ARCADE_GATE_STATUS: &str = "SEEN_ON_NETWORK";

/// Give up waiting for the verdict after this long — the TX was submitted but never became
/// demonstrably visible, so the caller must NOT treat it as sent (= keep inputs locked and
/// let the monitor retry, never a success).
const ARCADE_VERDICT_TIMEOUT_MS: u64 = 20_000;

/// Statuses that are hard rejects — never wait these out, never swallow them.
const ARCADE_FATAL_STATUSES: &[&str] = &["REJECTED", "DOUBLE_SPEND_ATTEMPTED"];

/// Sync submit HTTP codes worth one retry (transient). NOTE 5xx is handled by range, not
/// this list — Cloudflare fronts fetches with its own 52x/53x codes (e.g. 530 origin DNS
/// error for an unreachable host; caught by the 2026-07-13 staging dead-endpoint drill,
/// where a 530 mis-classified as structural marked a LIVE tx invalid and released its
/// inputs — the phantom-UTXO hazard).
const RETRYABLE_STATUS_CODES: &[u16] = &[408, 429];

/// True structural rejects are 4xx (minus the retryable ones). Anything ≥500 — including
/// Cloudflare's own 52x/53x fetch-failure codes — is an OUTAGE class, never a tx verdict.
fn is_structural_reject(status: u16) -> bool {
    (400..500).contains(&status) && !RETRYABLE_STATUS_CODES.contains(&status)
}

/// Rank Arcade lifecycle statuses so "target or better" comparisons work.
/// Unknown statuses rank 0 (lowest). Lifecycle (maintainer-confirmed):
/// `RECEIVED → SENT_TO_NETWORK → ACCEPTED_BY_NETWORK → SEEN_ON_NETWORK →
///  SEEN_MULTIPLE_NODES → MINED (→ IMMUTABLE)`, or `RECEIVED → REJECTED`.
pub fn arcade_status_rank(status: &str) -> u8 {
    match status {
        "RECEIVED" => 1,
        "STORED" => 2,
        "ANNOUNCED_TO_NETWORK" => 3,
        "REQUESTED_BY_NETWORK" => 4,
        "SENT_TO_NETWORK" => 5,
        "ACCEPTED_BY_NETWORK" => 6,
        "SEEN_ON_NETWORK" => 7,
        "SEEN_MULTIPLE_NODES" => 8,
        "MINED" => 9,
        "IMMUTABLE" => 10,
        _ => 0,
    }
}

// =============================================================================
// Failure taxonomy — how Arcade failures map onto the fallback decision
// =============================================================================

/// Why an Arcade broadcast did not return a clean verdict. The selector
/// (`services::selected`) folds this into the ARC/WoC fallback decision:
///
/// - `Outage` — Arcade never accepted the submission (unreachable, 5xx-exhausted,
///   or nothing submittable). The tx is NOT at Arcade: falling back to the ARC/WoC
///   path is safe and preserves today's behavior. THE ONLY fallback-eligible class.
/// - `Permanent` — a definitive reject (async REJECTED / DOUBLE_SPEND_ATTEMPTED, or a
///   sync structural 4xx on the BEEF path). A reject is a verdict, not an outage —
///   NEVER retried on another provider (re-broadcasting a rejected tx elsewhere
///   would fake a success for a dead tx).
/// - `Unverified` — submitted (202) but no gate-status verdict inside the timeout and
///   the status poll was inconclusive. The tx IS at Arcade; the caller must keep
///   inputs locked and let the monitor re-broadcast (Arcade dedupes + replays status).
///   No fallback: a second submission path would race the verdict.
pub enum ArcadeFailure {
    Outage(String),
    Permanent(BroadcastError),
    Unverified(String),
}

// =============================================================================
// 1. BEEF → EF batch (BRC-30) — the format Arcade actually accepts
// =============================================================================

/// One EF-encoded submission unit: `(txid, EF bytes)` — the txid rides along so the SSE
/// waiter can fatal-watch every submitted tx, not just the subject.
pub type EfTx = (String, Vec<u8>);

/// Convert a BEEF (hex) into Extended Format (BRC-30) binaries for Arcade V2 broadcast.
///
/// Every UNPROVEN (unmined) transaction in the BEEF must be submitted itself, since interior
/// chain TXs otherwise never reach the network. Source satoshis/scripts come from the BEEF's
/// own ancestry, so a valid BEEF always carries enough data for EF encoding.
///
/// THE KNOWN TRAP (paid for in the reference impl): bsv-rs `Transaction::to_ef()` requires
/// `input.source_transaction` to be linked, and the BEEF parser does NOT link it — we link
/// each input manually from the BEEF's own tx map, one level deep.
///
/// Returns `(efs, subject_txid)` — `(txid, EF binary)` pairs for all unproven TXs in
/// dependency order (`Beef::sort_txs`), and the txid of the BEEF's subject transaction.
/// The subject = the last-in-dependency-order tx; if the BEEF is ATOMIC-framed, its declared
/// `atomic_txid` must AGREE with that derivation (a disagreement = an inconsistent BEEF →
/// hard Err, fail closed). `efs` is empty when every transaction is already proven.
pub fn beef_to_ef_batch(beef_hex: &str) -> std::result::Result<(Vec<EfTx>, String), String> {
    let mut beef = Beef::from_hex(beef_hex).map_err(|e| format!("Arcade EF: BEEF parse: {e:?}"))?;
    beef.sort_txs();

    // txid → parsed transaction, for linking input sources one level deep.
    // BEEF-parsed transactions have no sources linked themselves, so clones stay flat.
    let mut tx_map: HashMap<String, Transaction> = HashMap::with_capacity(beef.txs.len());
    for btx in &beef.txs {
        if let Some(tx) = btx.tx() {
            tx_map.insert(btx.txid(), tx.clone());
        }
    }

    let mut efs = Vec::new();
    let mut subject_txid = String::new();

    for btx in &beef.txs {
        let txid = btx.txid();
        subject_txid = txid.clone();

        if btx.has_proof() {
            // Already mined — provides source data for children, nothing to broadcast.
            continue;
        }

        let tx = btx
            .tx()
            .ok_or_else(|| format!("Arcade EF: txid-only entry {txid} has no transaction data"))?;

        let mut tx = tx.clone();
        for input in &mut tx.inputs {
            if input.source_transaction.is_some() {
                continue;
            }
            let src_txid = input
                .source_txid
                .clone()
                .ok_or_else(|| format!("Arcade EF: input in {txid} has no source txid"))?;
            let src = tx_map.get(&src_txid).ok_or_else(|| {
                format!("Arcade EF: source tx {src_txid} for {txid} not present in BEEF")
            })?;
            input.source_transaction = Some(Box::new(src.clone()));
        }

        let ef = tx.to_ef().map_err(|e| format!("Arcade EF: {txid}: {e:?}"))?;
        efs.push((txid, ef));
    }

    // An ATOMIC-framed BEEF declares its subject — prefer it, but only if it AGREES with the
    // dependency-order derivation; a disagreement is an inconsistent BEEF (fail closed).
    if let Some(atomic) = &beef.atomic_txid {
        if *atomic != subject_txid {
            return Err(format!(
                "Arcade EF: AtomicBEEF declares subject {atomic} but the last tx in \
                 dependency order is {subject_txid} — inconsistent BEEF; STOP"
            ));
        }
        subject_txid = atomic.clone();
    }

    Ok((efs, subject_txid))
}

// =============================================================================
// 2. The SSE verdict scanner (pure — unit-tested against synthetic frames)
// =============================================================================

/// A definitive verdict pulled off the SSE stream.
#[derive(Debug, Clone, PartialEq)]
pub enum SseVerdict {
    /// The SUBJECT reached the target status or better — the value is the ACTUAL status
    /// pushed (e.g. `SEEN_ON_NETWORK`, or a later `MINED` if the replay skipped ahead).
    Reached(String),
    /// A tx of the submitted batch (the subject OR any ancestor riding it) hit a fatal
    /// status (`REJECTED` / `DOUBLE_SPEND_ATTEMPTED`) — the caller MUST fail hard: a dead
    /// ancestor kills the subject too, and ignoring either would "succeed" a dead TX (the
    /// money-rails law). Carries WHICH txid died, named in the error.
    Fatal { txid: String, status: String },
}

/// Incremental scanner over Arcade's `/events` SSE byte stream: feed it chunks as they
/// arrive; it buffers partial lines, parses `data: {"txid","txStatus",...}` frames, and
/// yields the first definitive [`SseVerdict`]: `Reached` gates on the SUBJECT txid only;
/// `Fatal` triggers on the subject OR any other txid of the submitted batch (a rejected
/// ancestor must surface as a named fast-fail, not a 20s timeout). Pure (no I/O) so the
/// verdict logic is unit-testable against recorded/synthetic frames.
pub struct SseVerdictScanner {
    buf: String,
    subject: String,
    /// Every OTHER txid submitted in the same batch (unmined ancestors) — fatal-watched.
    batch: Vec<String>,
    target_rank: u8,
    last_status: String,
}

impl SseVerdictScanner {
    /// `target` must be a known Arcade status (rank > 0) — waiting on an unrankable status
    /// could never resolve, which is an operator error, not a broadcast verdict.
    /// `batch_txids` = every txid submitted alongside the subject (the subject itself may
    /// be included or not — it is always watched).
    pub fn new(
        subject: &str,
        batch_txids: &[String],
        target: &str,
    ) -> std::result::Result<Self, String> {
        let target_rank = arcade_status_rank(target);
        if target_rank == 0 {
            return Err(format!("Arcade SSE: unknown target status {target:?}"));
        }
        Ok(Self {
            buf: String::new(),
            subject: subject.to_string(),
            batch: batch_txids.to_vec(),
            target_rank,
            last_status: String::new(),
        })
    }

    /// The last status observed for the subject txid (diagnostics for timeout/close errors).
    pub fn last_status(&self) -> &str {
        if self.last_status.is_empty() {
            "none"
        } else {
            &self.last_status
        }
    }

    /// Feed the next raw chunk; returns the first definitive verdict, if this chunk
    /// completed one. Non-`data:` lines (`id:`, `event:`, comments, blanks), frames for
    /// txids outside the batch, unparseable payloads, and below-target statuses are all
    /// skipped. A FATAL status for ANY batch txid is definitive.
    pub fn feed(&mut self, chunk: &[u8]) -> Option<SseVerdict> {
        self.buf.push_str(&String::from_utf8_lossy(chunk));
        while let Some(pos) = self.buf.find('\n') {
            let line: String = self.buf.drain(..=pos).collect();
            let line = line.trim();
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let Ok(json) = serde_json::from_str::<serde_json::Value>(data.trim()) else {
                continue;
            };
            let Some(frame_txid) = json.get("txid").and_then(|t| t.as_str()) else {
                continue;
            };
            let is_subject = frame_txid == self.subject;
            let in_batch = is_subject || self.batch.iter().any(|t| t == frame_txid);
            if !in_batch {
                continue;
            }
            let status = json.get("txStatus").and_then(|s| s.as_str()).unwrap_or("");

            // A dead BATCH member (ancestor or subject) kills the whole broadcast — surface
            // it named and fast, not as a 20s subject-timeout.
            if ARCADE_FATAL_STATUSES.contains(&status) {
                return Some(SseVerdict::Fatal {
                    txid: frame_txid.to_string(),
                    status: status.to_string(),
                });
            }
            if !is_subject {
                continue; // ancestors only fatal-watch; the GATE is the subject's alone
            }
            self.last_status = status.to_string();
            if arcade_status_rank(status) >= self.target_rank {
                return Some(SseVerdict::Reached(status.to_string()));
            }
        }
        None
    }
}

/// Map a fatal SSE status onto the crate's permanent broadcast errors.
fn fatal_to_broadcast_error(dead_txid: &str, status: &str, subject: &str) -> BroadcastError {
    let who = if dead_txid == subject {
        format!("the subject {dead_txid}")
    } else {
        format!("batch ancestor {dead_txid} (of subject {subject})")
    };
    let msg = format!(
        "Arcade FATAL verdict: {who} is {status} — the tx is DEAD on the network \
         (a swallowed async reject would fake a success)"
    );
    if status == "DOUBLE_SPEND_ATTEMPTED" {
        BroadcastError::DoubleSpend(msg)
    } else {
        BroadcastError::InvalidTx(msg)
    }
}

// =============================================================================
// 3. ArcadeProvider — the worker-Fetch transport (submit EF + SSE-wait the verdict)
// =============================================================================

pub struct ArcadeProvider {
    base_url: String,
}

impl ArcadeProvider {
    /// `base_url` from the `ARCADE_URL` env var; `None`/empty = the live mainnet default.
    pub fn new(base_url: Option<String>) -> Self {
        Self {
            base_url: base_url
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| ARCADE_URL_DEFAULT.to_string())
                .trim_end_matches('/')
                .to_string(),
        }
    }

    /// Broadcast a BEEF via Arcade: convert every unproven tx to EF, batch-submit, gate on
    /// the SSE SEEN_ON_NETWORK verdict.
    pub async fn broadcast_beef_arcade(
        &self,
        beef_hex: &str,
    ) -> std::result::Result<BroadcastResult, ArcadeFailure> {
        // Structural failure of the conversion itself (unparseable BEEF, inconsistent
        // atomic subject, missing source data) — the tx never left this process, and no
        // other provider gets a better BEEF: permanent, inputs released. Money-safe: a
        // never-submitted tx cannot spend anything.
        let (efs, subject_txid) = beef_to_ef_batch(beef_hex)
            .map_err(|e| ArcadeFailure::Permanent(BroadcastError::InvalidTx(e)))?;

        if efs.is_empty() {
            // Every tx in the BEEF is already proven/mined — nothing to submit, so there is
            // no fresh Arcade verdict to report. The ARC/WoC path handles a re-broadcast of
            // an already-mined tx gracefully ("already known" = success), so route there.
            return Err(ArcadeFailure::Outage(format!(
                "Arcade: BEEF for {subject_txid} contains no unproven tx — nothing to \
                 submit; deferring to the fallback provider"
            )));
        }

        let tx_count = efs.len();
        // Ancestors riding the batch (everything except the subject) — fatal-watched on SSE.
        let batch_txids: Vec<String> = efs
            .iter()
            .map(|(txid, _)| txid.clone())
            .filter(|t| t != &subject_txid)
            .collect();
        let (endpoint, body): (String, Vec<u8>) = if tx_count == 1 {
            (
                format!("{}/tx", self.base_url),
                efs.into_iter().next().expect("len 1").1,
            )
        } else {
            (
                format!("{}/txs", self.base_url),
                efs.iter().flat_map(|(_, ef)| ef.iter().copied()).collect(),
            )
        };

        let t0 = js_sys::Date::now();
        self.submit(&endpoint, body, &subject_txid, /*beef_path=*/ true)
            .await?;
        worker::console_log!(
            "BENCH broadcast.arcade[submit,txs={}]: {:.0} ms",
            tx_count,
            js_sys::Date::now() - t0
        );

        let t1 = js_sys::Date::now();
        let r = self.sse_wait(&subject_txid, &batch_txids).await;
        worker::console_log!(
            "BENCH broadcast.arcade[verdict,outcome={}]: {:.0} ms",
            match &r {
                Ok(_) => "ok",
                Err(ArcadeFailure::Permanent(_)) => "fatal",
                Err(ArcadeFailure::Unverified(_)) => "unverified",
                Err(ArcadeFailure::Outage(_)) => "outage",
            },
            js_sys::Date::now() - t1
        );
        r
    }

    /// Broadcast a bare raw tx via Arcade (`POST /tx`, hex text/plain). Arcade accepts a
    /// non-EF raw tx only when it already knows the input sources — on a sync 4xx we
    /// classify as `Outage` so the selector's ARC/WoC fallback (which handles bare raw
    /// txs natively) preserves today's behavior instead of failing the action on an
    /// Arcade capability limit.
    pub async fn broadcast_raw_tx_arcade(
        &self,
        raw_hex: &str,
    ) -> std::result::Result<BroadcastResult, ArcadeFailure> {
        let subject_txid = Transaction::from_hex(raw_hex)
            .map(|t| t.id())
            .map_err(|e| {
                ArcadeFailure::Permanent(BroadcastError::InvalidTx(format!(
                    "Arcade: raw tx unparseable: {e:?}"
                )))
            })?;

        let endpoint = format!("{}/tx", self.base_url);
        self.submit_raw_hex(&endpoint, raw_hex, &subject_txid)
            .await?;
        self.sse_wait(&subject_txid, &[]).await
    }

    /// POST an EF body (octet-stream) with the callback registration headers.
    /// `beef_path` decides the sync-4xx classification: on the BEEF path a structural 4xx
    /// is a definitive reject (Permanent); see `broadcast_raw_tx_arcade` for the raw path.
    async fn submit(
        &self,
        endpoint: &str,
        body: Vec<u8>,
        subject_txid: &str,
        beef_path: bool,
    ) -> std::result::Result<(), ArcadeFailure> {
        let mut last_error = String::new();
        for attempt in 0..2u8 {
            if attempt > 0 {
                worker::Delay::from(std::time::Duration::from_millis(500)).await;
            }
            let headers = worker::Headers::new();
            let _ = headers.set("Content-Type", "application/octet-stream");
            let _ = headers.set("X-CallbackToken", subject_txid);
            let _ = headers.set("X-FullStatusUpdates", "true");
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Post);
            init.with_headers(headers);
            init.with_body(Some(js_sys::Uint8Array::from(&body[..]).into()));

            let request = worker::Request::new_with_init(endpoint, &init)
                .map_err(|e| ArcadeFailure::Outage(format!("Arcade submit request: {e}")))?;
            let mut response = match worker::Fetch::Request(request).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = format!("Arcade submit fetch failed: {e}");
                    continue;
                }
            };
            let status = response.status_code();
            let text = response.text().await.unwrap_or_default();
            if status < 400 {
                // 202 Accepted — the verdict is ASYNC; do NOT read a success out of this.
                worker::console_log!(
                    "Arcade: submitted {} → HTTP {} {}",
                    subject_txid,
                    status,
                    text
                );
                return Ok(());
            }
            last_error = format!("Arcade submit HTTP {status}: {text}");
            if is_structural_reject(status) {
                // Sync 4xx = a STRUCTURAL reject (parse/EF/script/fee) — definitive on the
                // BEEF path (a reject is a verdict, never retried elsewhere).
                if beef_path {
                    return Err(ArcadeFailure::Permanent(BroadcastError::InvalidTx(
                        last_error,
                    )));
                }
                return Err(ArcadeFailure::Outage(last_error));
            }
        }
        Err(ArcadeFailure::Outage(last_error))
    }

    /// POST a raw tx hex (text/plain) with the callback registration headers.
    async fn submit_raw_hex(
        &self,
        endpoint: &str,
        raw_hex: &str,
        subject_txid: &str,
    ) -> std::result::Result<(), ArcadeFailure> {
        let mut last_error = String::new();
        for attempt in 0..2u8 {
            if attempt > 0 {
                worker::Delay::from(std::time::Duration::from_millis(500)).await;
            }
            let headers = worker::Headers::new();
            let _ = headers.set("Content-Type", "text/plain");
            let _ = headers.set("X-CallbackToken", subject_txid);
            let _ = headers.set("X-FullStatusUpdates", "true");
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Post);
            init.with_headers(headers);
            init.with_body(Some(wasm_bindgen::JsValue::from_str(raw_hex)));

            let request = worker::Request::new_with_init(endpoint, &init)
                .map_err(|e| ArcadeFailure::Outage(format!("Arcade submit request: {e}")))?;
            let mut response = match worker::Fetch::Request(request).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = format!("Arcade submit fetch failed: {e}");
                    continue;
                }
            };
            let status = response.status_code();
            let text = response.text().await.unwrap_or_default();
            if status < 400 {
                worker::console_log!(
                    "Arcade: submitted raw {} → HTTP {} {}",
                    subject_txid,
                    status,
                    text
                );
                return Ok(());
            }
            last_error = format!("Arcade raw submit HTTP {status}: {text}");
            if is_structural_reject(status) {
                // The raw (non-EF) path is an Arcade capability limit, not a tx verdict —
                // fall back so ARC/WoC classify it exactly as they do today.
                return Err(ArcadeFailure::Outage(last_error));
            }
        }
        Err(ArcadeFailure::Outage(last_error))
    }

    /// Wait for the subject to reach the gate status (or better) over the `/events` SSE
    /// stream registered at submit via `X-CallbackToken`. Connect-after-submit is race-free
    /// (a fresh connect replays pending statuses). On timeout or stream trouble, one
    /// `GET /tx/{txid}` status poll closes the already-terminal replay hole (a tx that went
    /// MINED before we connected never replays a non-terminal status).
    async fn sse_wait(
        &self,
        subject_txid: &str,
        batch_txids: &[String],
    ) -> std::result::Result<BroadcastResult, ArcadeFailure> {
        use futures_util::StreamExt;

        let mut scanner = SseVerdictScanner::new(subject_txid, batch_txids, ARCADE_GATE_STATUS)
            .map_err(ArcadeFailure::Unverified)?;
        let events_url = format!("{}/events?callbackToken={}", self.base_url, subject_txid);

        let sse = async {
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Get);
            let request = worker::Request::new_with_init(&events_url, &init)
                .map_err(|e| format!("Arcade SSE request: {e}"))?;
            let mut response = worker::Fetch::Request(request)
                .send()
                .await
                .map_err(|e| format!("Arcade SSE connect failed: {e}"))?;
            if response.status_code() >= 400 {
                return Err(format!("Arcade SSE connect HTTP {}", response.status_code()));
            }
            let mut stream = response
                .stream()
                .map_err(|e| format!("Arcade SSE stream: {e}"))?;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| {
                    format!(
                        "Arcade SSE read failed: {e} (last status: {})",
                        scanner.last_status()
                    )
                })?;
                if let Some(verdict) = scanner.feed(&chunk) {
                    return Ok(verdict);
                }
            }
            Err(format!(
                "Arcade SSE stream closed before {subject_txid} reached {ARCADE_GATE_STATUS} \
                 (last status: {})",
                scanner.last_status()
            ))
        };

        let timeout = async {
            worker::Delay::from(std::time::Duration::from_millis(ARCADE_VERDICT_TIMEOUT_MS))
                .await;
            Err(format!(
                "{subject_txid} submitted but no {ARCADE_GATE_STATUS} verdict within {}s",
                ARCADE_VERDICT_TIMEOUT_MS / 1000
            ))
        };

        futures_util::pin_mut!(sse, timeout);
        let outcome = match futures_util::future::select(sse, timeout).await {
            futures_util::future::Either::Left((r, _)) => r,
            futures_util::future::Either::Right((r, _)) => r,
        };

        match outcome {
            Ok(SseVerdict::Reached(status)) => Ok(BroadcastResult {
                txid: subject_txid.to_string(),
                tx_status: status.clone(),
                seen_on_network: arcade_status_rank(&status)
                    >= arcade_status_rank(ARCADE_GATE_STATUS),
            }),
            Ok(SseVerdict::Fatal { txid, status }) => Err(ArcadeFailure::Permanent(
                fatal_to_broadcast_error(&txid, &status, subject_txid),
            )),
            Err(sse_diag) => {
                // The SSE path was inconclusive — one status poll settles the
                // already-terminal case (dedup resubmits and MINED-before-connect).
                match self.poll_status(subject_txid).await {
                    Some(status) if ARCADE_FATAL_STATUSES.contains(&status.as_str()) => {
                        Err(ArcadeFailure::Permanent(fatal_to_broadcast_error(
                            subject_txid,
                            &status,
                            subject_txid,
                        )))
                    }
                    Some(status)
                        if arcade_status_rank(&status)
                            >= arcade_status_rank(ARCADE_GATE_STATUS) =>
                    {
                        worker::console_log!(
                            "Arcade: {} verdict via status poll: {} (SSE: {})",
                            subject_txid,
                            status,
                            sse_diag
                        );
                        Ok(BroadcastResult {
                            txid: subject_txid.to_string(),
                            tx_status: status,
                            seen_on_network: true,
                        })
                    }
                    other => Err(ArcadeFailure::Unverified(format!(
                        "{sse_diag}; status poll: {} — do not treat as sent",
                        other.unwrap_or_else(|| "unknown".to_string())
                    ))),
                }
            }
        }
    }

    /// `GET /tx/{txid}` → the current `txStatus`, if Arcade knows the tx.
    async fn poll_status(&self, txid: &str) -> Option<String> {
        let url = format!("{}/tx/{}", self.base_url, txid);
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        let request = worker::Request::new_with_init(&url, &init).ok()?;
        let mut response = worker::Fetch::Request(request).send().await.ok()?;
        if response.status_code() >= 400 {
            return None;
        }
        let json: serde_json::Value = response.json().await.ok()?;
        json.get("txStatus")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
    }
}

// =============================================================================
// Tests — the pure core (ported with the code; no network, no money)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_sdk::primitives::{sha256d, to_hex};
    use bsv_sdk::transaction::{MerklePath, MerklePathLeaf};

    // ── deterministic script-shaped vectors ──

    /// A canonical 25-byte P2PKH lock over a 20×`b` hash160.
    fn lock(b: u8) -> Vec<u8> {
        let mut s = vec![0x76, 0xa9, 0x14];
        s.extend(std::iter::repeat_n(b, 20));
        s.extend([0x88, 0xac]);
        s
    }

    /// Serialize a minimal 1-input v1 raw tx (counts < 0xfd). `prev` txid in DISPLAY hex.
    /// `unlock` = the input's unlocking script bytes (EF serialization requires one).
    fn raw_tx(prev_display: &str, vout: u32, unlock: &[u8], outputs: &[(u64, Vec<u8>)]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend(1u32.to_le_bytes());
        b.push(1u8);
        let mut id = hex::decode(prev_display).expect("prev txid hex");
        assert_eq!(id.len(), 32);
        id.reverse(); // wire = internal byte order
        b.extend(id);
        b.extend(vout.to_le_bytes());
        b.push(unlock.len() as u8);
        b.extend(unlock);
        b.extend(0xffff_ffffu32.to_le_bytes());
        b.push(outputs.len() as u8);
        for (sats, lck) in outputs {
            b.extend(sats.to_le_bytes());
            b.push(lck.len() as u8);
            b.extend(lck.clone());
        }
        b.extend(0u32.to_le_bytes());
        b
    }

    fn display_txid(raw: &[u8]) -> String {
        let h = sha256d(raw);
        let rev: Vec<u8> = h.iter().rev().copied().collect();
        to_hex(&rev)
    }

    /// Manually-encoded expected EF (BRC-30) for a 1-input tx: version ‖ 0000000000EF ‖
    /// inputs (each + source sats + source lock) ‖ outputs ‖ locktime.
    fn expected_ef(
        raw_version: u32,
        prev_display: &str,
        vout: u32,
        unlock: &[u8],
        source_sats: u64,
        source_lock: &[u8],
        outputs: &[(u64, Vec<u8>)],
    ) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend(raw_version.to_le_bytes());
        b.extend([0x00, 0x00, 0x00, 0x00, 0x00, 0xEF]);
        b.push(1u8); // input count
        let mut id = hex::decode(prev_display).unwrap();
        id.reverse();
        b.extend(id);
        b.extend(vout.to_le_bytes());
        b.push(unlock.len() as u8);
        b.extend(unlock);
        b.extend(0xffff_ffffu32.to_le_bytes());
        b.extend(source_sats.to_le_bytes());
        b.push(source_lock.len() as u8);
        b.extend(source_lock);
        b.push(outputs.len() as u8); // output count
        for (sats, lck) in outputs {
            b.extend(sats.to_le_bytes());
            b.push(lck.len() as u8);
            b.extend(lck.clone());
        }
        b.extend(0u32.to_le_bytes());
        b
    }

    // ── beef_to_ef_batch ──

    #[test]
    fn ef_batch_round_trips_and_orders_with_mined_root() {
        // ROOT (mined, carries a bump) → PARENT (unmined) → CHILD (unmined, the subject).
        // Only PARENT + CHILD get EFs, in that order; ROOT only supplies source data.
        let root = raw_tx(&"11".repeat(32), 0, &[0x51], &[(5000, lock(0x22))]);
        let root_txid = display_txid(&root);
        let parent = raw_tx(&root_txid, 0, &[0x52], &[(4000, lock(0x33))]);
        let parent_txid = display_txid(&parent);
        let child = raw_tx(
            &parent_txid,
            0,
            &[0x53],
            &[(999, lock(0x44)), (2900, lock(0x55))],
        );
        let child_txid = display_txid(&child);

        let mut beef = Beef::new();
        // Single-leaf block: the root IS the merkle root of its (synthetic) block.
        let bump = MerklePath::new_unchecked(
            842_000,
            vec![vec![MerklePathLeaf::new_txid(0, root_txid.clone())]],
        )
        .expect("single-leaf bump");
        let bump_index = beef.merge_bump(bump);
        // Merge OUT of dependency order — sort_txs must restore it.
        beef.merge_raw_tx(child, None);
        beef.merge_raw_tx(parent, None);
        beef.merge_raw_tx(root, Some(bump_index));
        let beef_hex = to_hex(&beef.to_binary());

        let (efs, subject) = beef_to_ef_batch(&beef_hex).expect("converts");
        assert_eq!(subject, child_txid);
        assert_eq!(
            efs.len(),
            2,
            "the MINED root is skipped (dedupe/source-only)"
        );

        // Byte-exact EF round-trip pins (source sats + lock pulled from the BEEF ancestry),
        // each pair carrying its txid (the batch-wide fatal-watch needs them).
        assert_eq!(efs[0].0, parent_txid, "dependency order: parent first");
        assert_eq!(
            efs[0].1,
            expected_ef(
                1,
                &root_txid,
                0,
                &[0x52],
                5000,
                &lock(0x22),
                &[(4000, lock(0x33))]
            ),
            "parent EF: BRC-30 bytes"
        );
        assert_eq!(efs[1].0, child_txid, "child (the subject) second");
        assert_eq!(
            efs[1].1,
            expected_ef(
                1,
                &parent_txid,
                0,
                &[0x53],
                4000,
                &lock(0x33),
                &[(999, lock(0x44)), (2900, lock(0x55))]
            ),
            "child EF second"
        );

        // And the library's own EF parser accepts both (full round-trip).
        for (txid, ef) in &efs {
            let parsed = Transaction::from_ef(ef).expect("EF reparses");
            assert_eq!(&parsed.id(), txid, "EF round-trip preserves the txid");
        }
    }

    #[test]
    fn ef_batch_pins_the_source_linking_trap() {
        // THE TRAP: a BEEF-parsed tx has NO source_transaction linked — to_ef() on it fails.
        // beef_to_ef_batch must link manually (that's its whole reason to exist).
        let (beef_hex, child_txid) = {
            let root = raw_tx(&"11".repeat(32), 0, &[0x51], &[(5000, lock(0x22))]);
            let root_txid = display_txid(&root);
            let child = raw_tx(&root_txid, 0, &[0x52], &[(4900, lock(0x33))]);
            let child_txid = display_txid(&child);
            let mut beef = Beef::new();
            let bump = MerklePath::new_unchecked(
                842_000,
                vec![vec![MerklePathLeaf::new_txid(0, root_txid.clone())]],
            )
            .unwrap();
            let idx = beef.merge_bump(bump);
            beef.merge_raw_tx(root, Some(idx));
            beef.merge_raw_tx(child, None);
            (to_hex(&beef.to_binary()), child_txid)
        };

        // Unlinked, the parser's tx cannot EF-serialize…
        let beef = Beef::from_hex(&beef_hex).unwrap();
        let unlinked = beef
            .txs
            .iter()
            .find(|t| t.txid() == child_txid)
            .and_then(|t| t.tx())
            .expect("child parsed");
        assert!(
            unlinked.to_ef().is_err(),
            "BEEF parser must NOT have linked source_transaction (else the trap moved)"
        );

        // …but the batch fn links from the BEEF's own tx map and succeeds.
        let (efs, subject) = beef_to_ef_batch(&beef_hex).expect("manual linking works");
        assert_eq!(subject, child_txid);
        assert_eq!(efs.len(), 1);
        assert_eq!(efs[0].0, child_txid);
        assert_eq!(Transaction::from_ef(&efs[0].1).unwrap().id(), child_txid);
    }

    /// (beef, parent_txid, child_txid) — two UNMINED txs, parent→child, for the atomic
    /// consistency tests. The parent's own source rides as a MINED root.
    fn beef_with_unmined_parent_child() -> (Beef, String, String) {
        let root = raw_tx(&"11".repeat(32), 0, &[0x51], &[(5000, lock(0x22))]);
        let root_txid = display_txid(&root);
        let parent = raw_tx(&root_txid, 0, &[0x52], &[(4000, lock(0x33))]);
        let parent_txid = display_txid(&parent);
        let child = raw_tx(&parent_txid, 0, &[0x53], &[(3900, lock(0x44))]);
        let child_txid = display_txid(&child);
        let mut beef = Beef::new();
        let bump = MerklePath::new_unchecked(
            842_000,
            vec![vec![MerklePathLeaf::new_txid(0, root_txid.clone())]],
        )
        .unwrap();
        let idx = beef.merge_bump(bump);
        beef.merge_raw_tx(root, Some(idx));
        beef.merge_raw_tx(parent, None);
        beef.merge_raw_tx(child, None);
        (beef, parent_txid, child_txid)
    }

    #[test]
    fn ef_batch_prefers_consistent_atomic_and_rejects_inconsistent() {
        // ATOMIC framed for the true subject (the child): agrees with the dependency-order
        // derivation → accepted, subject = the atomic txid.
        let (mut beef, parent_txid, child_txid) = beef_with_unmined_parent_child();
        let atomic_ok = to_hex(&beef.to_binary_atomic(&child_txid).unwrap());
        let (efs, subject) = beef_to_ef_batch(&atomic_ok).expect("consistent atomic converts");
        assert_eq!(subject, child_txid);
        assert_eq!(efs.len(), 2);

        // ATOMIC framed for the PARENT while a descendant rides the BEEF: the declared
        // subject disagrees with the dependency-order derivation → inconsistent, STOP.
        let (mut beef2, parent2, _child2) = beef_with_unmined_parent_child();
        let atomic_bad = to_hex(&beef2.to_binary_atomic(&parent2).unwrap());
        let err = beef_to_ef_batch(&atomic_bad).expect_err("inconsistent atomic must STOP");
        assert!(err.contains("inconsistent BEEF"), "{err}");
        assert!(err.contains(&parent_txid), "{err}");
    }

    #[test]
    fn ef_batch_rejects_garbage_and_missing_sources() {
        // Garbage / truncated BEEF → Err (STOP), never a silent empty batch.
        for garbage in ["", "zz", "deadbeef", &"00".repeat(64)] {
            assert!(
                beef_to_ef_batch(garbage).is_err(),
                "garbage BEEF {garbage:?} must be an Err"
            );
        }
        // An unmined tx whose source is NOT in the BEEF → Err naming the missing source
        // (EF needs its sats + lock; Arcade would reject the raw form anyway).
        let orphan = raw_tx(&"aa".repeat(32), 0, &[0x51], &[(1000, lock(0x22))]);
        let mut beef = Beef::new();
        beef.merge_raw_tx(orphan, None);
        let err =
            beef_to_ef_batch(&to_hex(&beef.to_binary())).expect_err("missing source must STOP");
        assert!(
            err.contains("not present in BEEF"),
            "the error names the gap: {err}"
        );
    }

    // ── the SSE verdict scanner (synthetic frames — the recorded Arcade wire shape) ──

    /// One SSE status frame exactly as Arcade emits it (id + event + data lines).
    fn frame(txid: &str, status: &str) -> String {
        format!(
            "id: 1752300000000000000\nevent: status\ndata: {{\"txid\":\"{txid}\",\"txStatus\":\"{status}\",\"timestamp\":\"2026-07-13T00:00:00Z\"}}\n\n"
        )
    }

    const SUBJECT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn scanner() -> SseVerdictScanner {
        SseVerdictScanner::new(SUBJECT, &[], ARCADE_GATE_STATUS).unwrap()
    }

    fn fatal(txid: &str, status: &str) -> SseVerdict {
        SseVerdict::Fatal {
            txid: txid.into(),
            status: status.into(),
        }
    }

    #[test]
    fn sse_resolves_on_seen_on_network() {
        let mut s = scanner();
        assert_eq!(s.feed(frame(SUBJECT, "RECEIVED").as_bytes()), None);
        assert_eq!(s.feed(frame(SUBJECT, "SENT_TO_NETWORK").as_bytes()), None);
        assert_eq!(
            s.feed(frame(SUBJECT, "SEEN_ON_NETWORK").as_bytes()),
            Some(SseVerdict::Reached("SEEN_ON_NETWORK".into()))
        );
    }

    #[test]
    fn sse_async_rejected_fails_hard() {
        // THE money-rails law test: an async REJECTED must surface as a HARD failure — a
        // caller that ignored it would "succeed" a dead TX.
        let mut s = scanner();
        assert_eq!(s.feed(frame(SUBJECT, "RECEIVED").as_bytes()), None);
        let verdict = s
            .feed(frame(SUBJECT, "REJECTED").as_bytes())
            .expect("REJECTED is definitive");
        assert_eq!(verdict, fatal(SUBJECT, "REJECTED"));
        // …and it maps to a PERMANENT broadcast error (inputs released, never a success).
        let err = fatal_to_broadcast_error(SUBJECT, "REJECTED", SUBJECT);
        assert!(err.is_permanent());
        assert!(matches!(err, BroadcastError::InvalidTx(_)));
    }

    #[test]
    fn sse_double_spend_attempted_fails_hard_as_double_spend() {
        let mut s = scanner();
        let verdict = s
            .feed(frame(SUBJECT, "DOUBLE_SPEND_ATTEMPTED").as_bytes())
            .expect("definitive");
        assert_eq!(verdict, fatal(SUBJECT, "DOUBLE_SPEND_ATTEMPTED"));
        let err = fatal_to_broadcast_error(SUBJECT, "DOUBLE_SPEND_ATTEMPTED", SUBJECT);
        assert!(err.is_permanent());
        assert!(matches!(err, BroadcastError::DoubleSpend(_)));
    }

    #[test]
    fn sse_later_statuses_also_resolve() {
        // The replay may skip straight past the gate (e.g. reconnect after MINED) — any
        // status RANKED at-or-above the target resolves, reported honestly as itself.
        for status in ["SEEN_MULTIPLE_NODES", "MINED", "IMMUTABLE"] {
            let mut s = scanner();
            assert_eq!(
                s.feed(frame(SUBJECT, status).as_bytes()),
                Some(SseVerdict::Reached(status.into())),
                "{status} outranks the gate"
            );
        }
    }

    #[test]
    fn sse_ignores_other_txids_including_their_verdicts() {
        let mut s = scanner();
        assert_eq!(s.feed(frame(OTHER, "SEEN_ON_NETWORK").as_bytes()), None);
        assert_eq!(s.feed(frame(OTHER, "REJECTED").as_bytes()), None);
        // …and the subject's own fatal still lands after the noise.
        assert_eq!(
            s.feed(frame(SUBJECT, "REJECTED").as_bytes()),
            Some(fatal(SUBJECT, "REJECTED"))
        );
    }

    #[test]
    fn sse_fatal_ancestor_in_batch_fails_fast_naming_it() {
        // A dead ANCESTOR kills the broadcast NOW, named — not a 20s subject-timeout.
        let mut s =
            SseVerdictScanner::new(SUBJECT, &[OTHER.to_string()], ARCADE_GATE_STATUS).unwrap();
        // ancestor lifecycle noise never gates…
        assert_eq!(s.feed(frame(OTHER, "SEEN_ON_NETWORK").as_bytes()), None);
        // …but its REJECTED is definitive and carries WHICH txid died.
        let verdict = s
            .feed(frame(OTHER, "REJECTED").as_bytes())
            .expect("ancestor REJECTED is definitive");
        assert_eq!(verdict, fatal(OTHER, "REJECTED"));
        let err = fatal_to_broadcast_error(OTHER, "REJECTED", SUBJECT);
        let msg = format!("{err}");
        assert!(msg.contains(OTHER), "names the dead ancestor: {msg}");
        assert!(msg.contains("ancestor"), "says it was an ancestor: {msg}");
        assert!(msg.contains(SUBJECT), "names the subject it kills: {msg}");
    }

    #[test]
    fn sse_reassembles_frames_split_across_chunks() {
        // The wire chunks arbitrarily — split one frame mid-JSON across three reads.
        let f = frame(SUBJECT, "SEEN_ON_NETWORK");
        let bytes = f.as_bytes();
        let mut s = scanner();
        assert_eq!(s.feed(&bytes[..10]), None);
        assert_eq!(s.feed(&bytes[10..40]), None);
        assert_eq!(
            s.feed(&bytes[40..]),
            Some(SseVerdict::Reached("SEEN_ON_NETWORK".into()))
        );
    }

    #[test]
    fn sse_catchup_replay_in_one_chunk_resolves() {
        // A fresh connect replays all pending statuses in one burst (the race-free property
        // connect-after-submit relies on) — the scanner resolves inside the burst.
        let burst = format!(
            "{}{}{}",
            frame(SUBJECT, "RECEIVED"),
            frame(SUBJECT, "ACCEPTED_BY_NETWORK"),
            frame(SUBJECT, "SEEN_ON_NETWORK")
        );
        let mut s = scanner();
        assert_eq!(
            s.feed(burst.as_bytes()),
            Some(SseVerdict::Reached("SEEN_ON_NETWORK".into()))
        );
    }

    #[test]
    fn sse_fatal_wins_even_when_burst_continues() {
        // If a replay burst carries REJECTED before anything else definitive, the FATAL is
        // the verdict — nothing later in the same chunk may override it.
        let burst = format!(
            "{}{}",
            frame(SUBJECT, "REJECTED"),
            frame(SUBJECT, "SEEN_ON_NETWORK") // pathological, but the scanner must not reach it
        );
        let mut s = scanner();
        assert_eq!(s.feed(burst.as_bytes()), Some(fatal(SUBJECT, "REJECTED")));
    }

    #[test]
    fn sse_skips_noise_lines_and_garbled_payloads() {
        let mut s = scanner();
        let noise =
            ": keepalive comment\nid: 42\nevent: status\ndata: not-json\n\ndata: {\"txid\":42}\n\n";
        assert_eq!(s.feed(noise.as_bytes()), None);
        assert_eq!(s.last_status(), "none", "noise never counts as a status");
        // a frame with a missing txStatus records as "" and does not resolve
        let no_status = format!("data: {{\"txid\":\"{SUBJECT}\"}}\n\n");
        assert_eq!(s.feed(no_status.as_bytes()), None);
        // …and the stream still works after all of it.
        assert_eq!(
            s.feed(frame(SUBJECT, "SEEN_ON_NETWORK").as_bytes()),
            Some(SseVerdict::Reached("SEEN_ON_NETWORK".into()))
        );
    }

    #[test]
    fn sse_below_target_statuses_never_resolve_and_track_last_status() {
        let mut s = scanner();
        for st in [
            "RECEIVED",
            "STORED",
            "ANNOUNCED_TO_NETWORK",
            "REQUESTED_BY_NETWORK",
            "SENT_TO_NETWORK",
            "ACCEPTED_BY_NETWORK",
        ] {
            assert_eq!(
                s.feed(frame(SUBJECT, st).as_bytes()),
                None,
                "{st} is pre-gate"
            );
            assert_eq!(
                s.last_status(),
                st,
                "diagnostics track the last real status"
            );
        }
    }

    #[test]
    fn sse_unknown_target_is_an_operator_error() {
        // NEVER gate on an unrankable status — the wait could not resolve.
        assert!(SseVerdictScanner::new(SUBJECT, &[], "SEEN_ON_NETWORK_LATER").is_err());
        assert!(SseVerdictScanner::new(SUBJECT, &[], "").is_err());
        assert_eq!(
            ARCADE_GATE_STATUS, "SEEN_ON_NETWORK",
            "the pinned default gate"
        );
    }

    #[test]
    fn arcade_status_ranks_are_strictly_ordered() {
        let lifecycle = [
            "RECEIVED",
            "STORED",
            "ANNOUNCED_TO_NETWORK",
            "REQUESTED_BY_NETWORK",
            "SENT_TO_NETWORK",
            "ACCEPTED_BY_NETWORK",
            "SEEN_ON_NETWORK",
            "SEEN_MULTIPLE_NODES",
            "MINED",
            "IMMUTABLE",
        ];
        for w in lifecycle.windows(2) {
            assert!(
                arcade_status_rank(w[0]) < arcade_status_rank(w[1]),
                "{} < {}",
                w[0],
                w[1]
            );
        }
        assert_eq!(arcade_status_rank("REJECTED"), 0, "fatals never rank");
        assert_eq!(arcade_status_rank("DOUBLE_SPEND_ATTEMPTED"), 0);
        assert_eq!(arcade_status_rank("nonsense"), 0);
    }

    #[test]
    fn submit_status_classification_pins_the_530_drill() {
        // CF fetches to a dead host surface as CF's own 52x/53x HTTP statuses (530 =
        // origin DNS error), NOT fetch errors — they are OUTAGES, never tx verdicts.
        // The 2026-07-13 staging dead-endpoint drill caught a 530 mis-classified as
        // structural: a LIVE tx got marked invalid and its inputs released.
        for outage in [500u16, 502, 503, 504, 520, 522, 525, 530] {
            assert!(
                !is_structural_reject(outage),
                "{outage} must be outage-class, not a verdict"
            );
        }
        for retryable in [408u16, 429] {
            assert!(!is_structural_reject(retryable), "{retryable} retries");
        }
        for structural in [400u16, 401, 404, 409, 413, 422] {
            assert!(is_structural_reject(structural), "{structural} is structural");
        }
    }

    #[test]
    fn provider_url_normalization() {
        assert_eq!(
            ArcadeProvider::new(None).base_url,
            ARCADE_URL_DEFAULT.trim_end_matches('/')
        );
        assert_eq!(
            ArcadeProvider::new(Some("  ".into())).base_url,
            ARCADE_URL_DEFAULT
        );
        assert_eq!(
            ArcadeProvider::new(Some("https://example.com/".into())).base_url,
            "https://example.com"
        );
    }
}
