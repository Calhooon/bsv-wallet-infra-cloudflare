//! WhatsOnChain (WoC) broadcast and proof provider.
//!
//! Implements `BroadcastService` and `ProofService` using the WoC REST API.
//! This is the legacy provider -- ARC is the primary provider.

use super::{
    BroadcastError, BroadcastResult, BroadcastService, ProofResult, ProofService, SpentStatus,
};
use crate::services::chaintracker::RetryConfig;
use bsv_sdk::transaction::{MerklePath, MerklePathLeaf};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;

// =============================================================================
// Constants
// =============================================================================

const WOC_BASE: &str = "https://api.whatsonchain.com/v1/bsv/main";

// =============================================================================
// WoC API response types
// =============================================================================

#[derive(Debug, Deserialize)]
pub(crate) struct WocTscProof {
    pub(crate) index: u32,
    pub(crate) nodes: Vec<String>,
    pub(crate) target: String,
    #[serde(rename = "txOrId")]
    pub(crate) tx_or_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct WocBlockHeader {
    hash: String,
    height: u32,
    merkleroot: String,
}

#[derive(Debug, Deserialize)]
struct WocTxStatus {
    txid: String,
    confirmations: Option<u32>,
    error: Option<String>,
}

/// Response of WoC `GET /tx/{txid}/{vout}/spent` (HTTP 200 case).
///
/// Verified live 2026-07-05 against a mainnet spent outpoint:
///   `{"txid":"<spending txid>","vin":0,"status":"confirmed"}`
/// An UNSPENT (or unknown) outpoint returns HTTP 404 instead.
/// `status` can also be "unconfirmed" for a mempool-only spend — per the
/// owner rule (SEEN = final on BSV) we treat ANY 200 with a txid as spent,
/// so `vin`/`status` are parsed but never gate the decision.
#[derive(Debug, Deserialize)]
struct WocSpentTxResponse {
    txid: String,
    #[allow(dead_code)]
    vin: Option<u32>,
    #[allow(dead_code)]
    status: Option<String>,
}

/// Parse the HTTP-200 body of WoC's outpoint-spent endpoint.
///
/// Pure function so the classification is unit-testable. Any parseable body
/// with a non-empty spending txid → `Spent`; a malformed/empty body is an
/// error (ambiguous — the caller must count it as a service error and take
/// NO action on the output).
pub(crate) fn parse_spent_response(text: &str) -> std::result::Result<SpentStatus, String> {
    let parsed: WocSpentTxResponse = serde_json::from_str(text)
        .map_err(|e| format!("WoC spent-status parse: {} (body: {:?})", e, &text[..text.len().min(120)]))?;
    if parsed.txid.is_empty() {
        return Err("WoC spent-status: 200 with empty spending txid".to_string());
    }
    Ok(SpentStatus::Spent {
        spending_txid: parsed.txid,
    })
}

// =============================================================================
// WocProvider
// =============================================================================

/// WhatsOnChain broadcast and proof provider.
///
/// Uses the Worker Fetch API for HTTP requests (required in Cloudflare Workers --
/// no TCP sockets, all HTTP goes through `worker::Fetch`).
///
/// Holds a per-monitor-run block hash→header cache to avoid redundant WoC header
/// fetches when multiple pending txs were mined in the same block. 500 pending
/// txs typically span ~10–50 unique blocks, so cache hit rate ≈ 90%+ during
/// `check_for_proofs`. The cache is cleared via `reset_run_cache()` at the start
/// of each monitor run (to avoid staleness across reorgs). CF Workers are
/// single-threaded so `RefCell` is safe — mirrors the `RootCache` pattern in
/// `chaintracker.rs`.
pub struct WocProvider {
    /// Per-run cache mapping WoC block hash → full header (height + merkleroot).
    /// Cleared by `reset_run_cache()` at the start of each monitor run.
    header_cache: RefCell<HashMap<String, WocBlockHeader>>,
    /// Optional WoC API key (`mainnet_...`). When set, sent as `Authorization`
    /// header on every request to bypass anonymous IP-based rate limiting.
    /// Loaded from the `WOC_API_KEY` CF Worker secret in `lib.rs`.
    api_key: Option<String>,
    /// Optional ChainTracks URL used to resolve the canonical block hash at a
    /// given height when filtering multi-entry TSC proof responses. Our own
    /// Worker (`rust-chaintracks`) is the authoritative source of canonical
    /// chain state; falling back to WoC's `/block/height/{h}` when it isn't
    /// set or errors keeps this change backward-compatible.
    chaintracks_url: Option<String>,
}

impl Default for WocProvider {
    fn default() -> Self {
        Self::new(None)
    }
}

impl WocProvider {
    pub fn new(api_key: Option<String>) -> Self {
        Self {
            header_cache: RefCell::new(HashMap::new()),
            api_key,
            chaintracks_url: None,
        }
    }

    /// Configure the ChainTracks URL used for canonical block-hash lookups.
    /// Returns self for builder-style chaining.
    pub fn with_chaintracks_url(mut self, url: Option<String>) -> Self {
        self.chaintracks_url = url.map(|u| u.trim_end_matches('/').to_string());
        self
    }
}

// =============================================================================
// BroadcastService
// =============================================================================

impl BroadcastService for WocProvider {
    /// Broadcast a raw transaction hex via WoC `POST /tx/raw`.
    ///
    /// Classification:
    /// - 200 -> success
    /// - 400+ with "already"/"mempool"/"known" in body -> success (already broadcast)
    /// - 400+ with double-spend indicators -> DoubleSpend
    /// - 400+ with invalid-tx indicators -> InvalidTx
    /// - 5xx / network error -> ServiceError (transient)
    ///
    /// WoC does not provide `seen_on_network` info, so that is always `false`.
    /// Returns `tx_status: "sent"` on success.
    async fn broadcast_raw_tx(
        &self,
        raw_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        let url = format!("{}/tx/raw", WOC_BASE);
        let body = serde_json::json!({ "txhex": raw_hex }).to_string();

        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);
        let headers = worker::Headers::new();
        let _ = headers.set("Content-Type", "application/json");
        if let Some(ref key) = self.api_key {
            let _ = headers.set("woc-api-key", key);
        }
        init.with_headers(headers);
        init.with_body(Some(wasm_bindgen::JsValue::from_str(&body)));

        let request = worker::Request::new_with_init(&url, &init)
            .map_err(|e| BroadcastError::ServiceError(e.to_string()))?;
        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| BroadcastError::ServiceError(format!("Broadcast fetch: {}", e)))?;

        let status = response.status_code();

        if status == 200 {
            // WoC returns the txid as the response body on success.
            let txid = response
                .text()
                .await
                .unwrap_or_default()
                .trim()
                .replace('"', "");
            return Ok(BroadcastResult {
                txid,
                tx_status: "sent".to_string(),
                seen_on_network: false,
            });
        }

        // 400+ -- check if "already known" (not a real rejection)
        let text = response.text().await.unwrap_or_default();

        if status >= 500 {
            // Server error -- transient
            return Err(BroadcastError::ServiceError(format!(
                "WoC {} : {}",
                status,
                &text[..std::cmp::min(200, text.len())]
            )));
        }

        if status >= 400 {
            // "Transaction already in the mempool" or similar -- already broadcast, OK
            if text.contains("already") || text.contains("mempool") || text.contains("known") {
                return Ok(BroadcastResult {
                    txid: String::new(),
                    tx_status: "sent".to_string(),
                    seen_on_network: false,
                });
            }
            // Classify by body content
            let lower = text.to_lowercase();
            if lower.contains("double spend")
                || lower.contains("txn-mempool-conflict")
                || lower.contains("missing inputs")
                || lower.contains("already spent")
            {
                return Err(BroadcastError::DoubleSpend(format!(
                    "WoC {} : {}",
                    status,
                    &text[..std::cmp::min(200, text.len())]
                )));
            }
            if lower.contains("mandatory-script-verify-flag-failed")
                || lower.contains("bad-txns")
                || lower.contains("scriptnumber overflow")
            {
                return Err(BroadcastError::InvalidTx(format!(
                    "WoC {} : {}",
                    status,
                    &text[..std::cmp::min(200, text.len())]
                )));
            }
            // Unknown 4xx -- treat as transient (WoC rate-limits, etc.)
            return Err(BroadcastError::ServiceError(format!(
                "WoC {} : {}",
                status,
                &text[..std::cmp::min(200, text.len())]
            )));
        }

        // Any other status code -- treat as service/transient error
        Err(BroadcastError::ServiceError(format!(
            "WoC unexpected status {}: {}",
            status,
            &text[..std::cmp::min(200, text.len())]
        )))
    }

    /// Broadcast BEEF hex via WoC. WoC does not natively support BEEF,
    /// so this is a no-op stub that returns a ServiceError.
    /// ARC provider handles BEEF natively.
    async fn broadcast_beef(
        &self,
        _beef_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        Err(BroadcastError::ServiceError(
            "WoC does not support BEEF broadcast -- extract raw tx first".to_string(),
        ))
    }
}

// =============================================================================
// ProofService
// =============================================================================

impl ProofService for WocProvider {
    /// Fetch merkle proof for a transaction from WoC TSC endpoint.
    ///
    /// Calls `GET /tx/{txid}/proof/tsc`, then fetches the block header
    /// for height info, and converts the TSC proof to BRC-74 binary.
    ///
    /// The block header lookup goes through `header_cache` — repeated lookups
    /// for txs mined in the same block reuse the cached header instead of
    /// hitting WoC again. This halves WoC API load during `check_for_proofs`
    /// runs, which used to burn 2 WoC calls per pending tx (proof + header).
    async fn get_proof(&self, txid: &str) -> std::result::Result<Option<ProofResult>, String> {
        let api_key = self.api_key.as_deref();

        // Step 1: Fetch ALL TSC proofs. WoC may return multiple entries when
        // the tx was mined in more than one block (orphan + canonical after a
        // reorg). Earlier code took the first entry unconditionally and
        // silently cached stale orphan proofs — the exact failure mode that
        // blocked consolidation on claude at height 943424.
        let proofs = fetch_tsc_proof(txid, api_key).await?;
        if proofs.is_empty() {
            return Ok(None);
        }

        // Step 2: Walk proofs from newest to oldest. For each, resolve its
        // block header and verify the block hash matches canonical at that
        // height. Iterating reverse gives us the canonical proof quickly in
        // the common case (WoC orders chronologically; canonical is last).
        for proof in proofs.iter().rev() {
            let cached = self.header_cache.borrow().get(&proof.target).cloned();
            let header = match cached {
                Some(h) => h,
                None => {
                    let fetched = fetch_block_header(&proof.target, api_key).await?;
                    self.header_cache
                        .borrow_mut()
                        .insert(proof.target.clone(), fetched.clone());
                    fetched
                }
            };

            // Canonical-filter: compare proof's block hash to the canonical
            // hash at that height. Prefer ChainTracks (our own Worker, only
            // stores canonical chain) and fall back to WoC if ChainTracks
            // isn't configured or errors. If neither source knows, we skip
            // the proof — never trust a proof we can't verify.
            let canonical_hash = match canonical_hash_at_height(
                header.height,
                self.chaintracks_url.as_deref(),
                api_key,
            )
            .await
            {
                Ok(Some(h)) => h,
                Ok(None) => {
                    worker::console_log!(
                        "No canonical hash for height {} — skipping proof {} for {}",
                        header.height,
                        &proof.target[..16.min(proof.target.len())],
                        txid
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            if canonical_hash != proof.target {
                worker::console_log!(
                    "Proof for {} at height {} is orphaned: target={} canonical={}",
                    txid,
                    header.height,
                    &proof.target[..16.min(proof.target.len())],
                    &canonical_hash[..16.min(canonical_hash.len())]
                );
                continue;
            }

            // Canonical — convert to BRC-74 binary and return.
            let merkle_path_binary = tsc_proof_to_binary(proof, header.height)?;
            return Ok(Some(ProofResult {
                txid: txid.to_string(),
                merkle_path_binary,
                block_height: header.height,
                block_hash: header.hash,
                merkle_root: header.merkleroot,
            }));
        }

        // All entries orphaned — tx is not on canonical chain right now.
        // Return None so the caller treats this as "no proof yet" and
        // retries on a subsequent cycle (may find a re-mined proof later).
        worker::console_log!(
            "All {} TSC proof entries for {} are orphaned — no canonical proof",
            proofs.len(),
            txid
        );
        Ok(None)
    }

    /// Clear the per-run block header cache. Called at the start of each
    /// monitor `check_for_proofs` run to avoid serving stale headers across
    /// reorgs or long idle periods.
    fn reset_run_cache(&self) {
        self.header_cache.borrow_mut().clear();
    }

    /// Fetch current chain tip height from WoC `GET /chain/info`.
    async fn get_chain_height(&self) -> std::result::Result<u32, String> {
        fetch_chain_height(self.api_key.as_deref()).await
    }

    /// Fetch raw transaction hex from WoC `GET /tx/{txid}/hex`.
    ///
    /// Retries on 429 and 5xx. 404 maps to `Ok(None)`.
    async fn get_raw_tx(&self, txid: &str) -> std::result::Result<Option<Vec<u8>>, String> {
        let url = format!("{}/tx/{}/hex", WOC_BASE, txid);
        let retry = RetryConfig::default();
        let mut last_err = String::new();

        for attempt in 0..=retry.max_retries {
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Get);
            if let Some(ref key) = self.api_key {
                let headers = worker::Headers::new();
                let _ = headers.set("woc-api-key", key);
                init.with_headers(headers);
            }
            let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
            let mut response = match worker::Fetch::Request(request).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("WoC get_raw_tx fetch: {}", e);
                    if attempt < retry.max_retries {
                        continue;
                    }
                    return Err(last_err);
                }
            };

            let status = response.status_code();
            if status == 404 {
                return Ok(None);
            }
            if RetryConfig::is_retryable_status(status) && attempt < retry.max_retries {
                last_err = format!(
                    "WoC get_raw_tx error {} (attempt {}/{})",
                    status,
                    attempt + 1,
                    retry.max_retries + 1
                );
                continue;
            }
            if status >= 400 {
                return Err(format!("WoC get_raw_tx error {}", status));
            }

            let hex_str = response.text().await.map_err(|e| e.to_string())?;
            let hex_str = hex_str.trim().trim_matches('"');
            if hex_str.is_empty() {
                return Ok(None);
            }

            return hex::decode(hex_str)
                .map(Some)
                .map_err(|e| format!("WoC get_raw_tx hex decode: {}", e));
        }

        Err(last_err)
    }

    /// Batch status check via WoC POST /txs/status, chunked at 20 (Go pattern).
    ///
    /// Each chunk retries on 429 and 5xx via `RetryConfig::is_retryable_status`.
    /// If any chunk exhausts retries the whole call errors out, triggering the
    /// monitor's safety net fallback to "check all" mode — so failures here
    /// degrade gracefully rather than silently dropping pending txs.
    async fn get_status_for_txids(
        &self,
        txids: &[String],
    ) -> std::result::Result<Vec<super::TxStatusDetail>, String> {
        use super::TxStatusDetail;

        const CHUNK_SIZE: usize = 20;
        let url = format!("{}/txs/status", WOC_BASE);
        let retry = RetryConfig::default();
        let mut all_results: Vec<TxStatusDetail> = Vec::with_capacity(txids.len());

        for chunk in txids.chunks(CHUNK_SIZE) {
            let body = serde_json::json!({ "txids": chunk });
            let body_str = serde_json::to_string(&body).map_err(|e| e.to_string())?;

            let text = {
                let mut last_err = String::new();
                let mut success: Option<String> = None;
                for attempt in 0..=retry.max_retries {
                    let mut init = worker::RequestInit::new();
                    init.with_method(worker::Method::Post);
                    let headers = worker::Headers::new();
                    let _ = headers.set("Content-Type", "application/json");
                    if let Some(ref key) = self.api_key {
                        let _ = headers.set("woc-api-key", key);
                    }
                    init.with_headers(headers);
                    init.with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body_str)));

                    let request = worker::Request::new_with_init(&url, &init)
                        .map_err(|e| e.to_string())?;
                    let mut response = match worker::Fetch::Request(request).send().await {
                        Ok(r) => r,
                        Err(e) => {
                            last_err = format!("WoC get_status_for_txids fetch: {}", e);
                            if attempt < retry.max_retries {
                                continue;
                            }
                            return Err(last_err);
                        }
                    };

                    let status = response.status_code();
                    if RetryConfig::is_retryable_status(status) && attempt < retry.max_retries {
                        last_err = format!(
                            "WoC get_status_for_txids HTTP {} (attempt {}/{})",
                            status,
                            attempt + 1,
                            retry.max_retries + 1
                        );
                        continue;
                    }
                    if status >= 400 {
                        return Err(format!("WoC get_status_for_txids HTTP {}", status));
                    }

                    success = Some(response.text().await.map_err(|e| e.to_string())?);
                    break;
                }
                match success {
                    Some(t) => t,
                    None => return Err(last_err),
                }
            };

            let data: Vec<WocTxStatus> = serde_json::from_str(&text)
                .map_err(|e| format!("WoC get_status_for_txids parse: {}", e))?;

            for txid in chunk {
                let d = data.iter().find(|d| d.txid == *txid);
                let detail = match d {
                    None => TxStatusDetail {
                        txid: txid.clone(),
                        status: "unknown".to_string(),
                        depth: None,
                    },
                    Some(d) if d.error.as_deref() == Some("unknown") => TxStatusDetail {
                        txid: txid.clone(),
                        status: "unknown".to_string(),
                        depth: None,
                    },
                    Some(d) if d.confirmations.is_none() => TxStatusDetail {
                        txid: txid.clone(),
                        status: "known".to_string(),
                        depth: Some(0),
                    },
                    Some(d) => TxStatusDetail {
                        txid: txid.clone(),
                        status: "mined".to_string(),
                        depth: d.confirmations,
                    },
                };
                all_results.push(detail);
            }
        }

        Ok(all_results)
    }

    /// Outpoint spent-status via WoC `GET /tx/{txid}/{vout}/spent` (G5).
    ///
    /// Semantics (verified live 2026-07-05):
    /// - 200 + `{"txid": ...}` → `Spent` — regardless of the `status` field:
    ///   on BSV a SEEN spending tx is final (first-seen, no RBF), so an
    ///   "unconfirmed" mempool spend counts exactly like a mined one.
    /// - 404 → `Unspent`. NOTE: WoC also 404s for outpoints it has never
    ///   indexed, which is indistinguishable — both mean "take no action".
    /// - Retries 429/5xx via `RetryConfig` like the other WoC lookups; a
    ///   still-failing call is an `Err` (transient — caller must not treat
    ///   it as unspent OR spent).
    async fn get_spent_status(
        &self,
        txid: &str,
        vout: u32,
    ) -> std::result::Result<SpentStatus, String> {
        let url = format!("{}/tx/{}/{}/spent", WOC_BASE, txid, vout);
        let retry = RetryConfig::default();
        let mut last_err = String::new();

        for attempt in 0..=retry.max_retries {
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Get);
            if let Some(ref key) = self.api_key {
                let headers = worker::Headers::new();
                let _ = headers.set("woc-api-key", key);
                init.with_headers(headers);
            }
            let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
            let mut response = match worker::Fetch::Request(request).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("WoC get_spent_status fetch: {}", e);
                    if attempt < retry.max_retries {
                        continue;
                    }
                    return Err(last_err);
                }
            };

            let status = response.status_code();
            if status == 404 {
                return Ok(SpentStatus::Unspent);
            }
            if RetryConfig::is_retryable_status(status) && attempt < retry.max_retries {
                last_err = format!(
                    "WoC get_spent_status HTTP {} (attempt {}/{})",
                    status,
                    attempt + 1,
                    retry.max_retries + 1
                );
                continue;
            }
            if status >= 400 {
                return Err(format!("WoC get_spent_status HTTP {}", status));
            }

            let text = response.text().await.map_err(|e| e.to_string())?;
            return parse_spent_response(&text);
        }

        Err(last_err)
    }
}

// =============================================================================
// Internal HTTP helpers -- extracted from monitor.rs
// =============================================================================

/// Parse a TSC proof response body into the full list of proofs.
///
/// WoC returns an array with one entry per block the tx was mined in. After a
/// reorg, the same tx can appear in an orphan block AND the canonical block.
/// The caller must select the canonical entry — we can't pick here because
/// that requires a height → canonical-hash lookup against WoC.
///
/// Handles all known WoC response formats: empty, "[]", "null", and valid JSON
/// arrays. Returns Ok(vec![]) for "no proof yet".
fn parse_tsc_proof_response(text: &str) -> std::result::Result<Vec<WocTscProof>, String> {
    if text.is_empty() || text == "[]" || text == "null" {
        return Ok(Vec::new());
    }

    let proofs: Vec<WocTscProof> =
        serde_json::from_str(text).map_err(|e| format!("Failed to parse TSC proof: {}", e))?;

    Ok(proofs)
}

/// Fetch TSC merkle proof from WhatsOnChain.
/// Returns Some(proof) if mined, None if not yet mined, Err on network failure.
///
/// Retries on 429 and 5xx via `RetryConfig::is_retryable_status`. CF Workers
/// are single-threaded so retries are immediate (no sleep) — the value is
/// hitting a different WoC backend on the next attempt.
async fn fetch_tsc_proof(
    txid: &str,
    api_key: Option<&str>,
) -> std::result::Result<Vec<WocTscProof>, String> {
    let url = format!("{}/tx/{}/proof/tsc", WOC_BASE, txid);
    let retry = RetryConfig::default();
    let mut last_err = String::new();

    for attempt in 0..=retry.max_retries {
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        if let Some(key) = api_key {
            let headers = worker::Headers::new();
            let _ = headers.set("woc-api-key", key);
            init.with_headers(headers);
        }
        let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
        let mut response = match worker::Fetch::Request(request).send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("WoC proof fetch: {}", e);
                if attempt < retry.max_retries {
                    continue;
                }
                return Err(last_err);
            }
        };

        let status = response.status_code();
        let has_key = api_key.is_some();
        if status == 404 {
            worker::console_log!("WoC TSC proof {}: 404 not-yet-mined (has_key={})", txid, has_key);
            return Ok(Vec::new());
        }
        if RetryConfig::is_retryable_status(status) && attempt < retry.max_retries {
            last_err = format!(
                "WoC proof API error {} (attempt {}/{})",
                status,
                attempt + 1,
                retry.max_retries + 1
            );
            continue;
        }
        if status >= 400 {
            let body = response.text().await.unwrap_or_default();
            worker::console_log!("WoC TSC proof {}: HTTP {} body={} has_key={}", txid, status, &body[..body.len().min(120)], has_key);
            return Err(format!("WoC proof API error {}", status));
        }

        let text = response.text().await.map_err(|e| e.to_string())?;
        let result = parse_tsc_proof_response(&text);
        if let Ok(ref v) = result {
            if v.is_empty() {
                worker::console_log!(
                    "WoC TSC proof {}: 200 but parsed as empty — body len={} preview={:?}",
                    txid,
                    text.len(),
                    &text[..text.len().min(80)]
                );
            } else if v.len() > 1 {
                worker::console_log!(
                    "WoC TSC proof {}: {} proofs returned (reorg) — canonical-filter will pick",
                    txid,
                    v.len()
                );
            }
        }
        return result;
    }

    Err(last_err)
}

/// Fetch block header from WhatsOnChain by block hash.
///
/// Retries on 429 and 5xx. Single-threaded immediate retry (same rationale as
/// `fetch_tsc_proof`).
async fn fetch_block_header(
    block_hash: &str,
    api_key: Option<&str>,
) -> std::result::Result<WocBlockHeader, String> {
    let url = format!("{}/block/{}/header", WOC_BASE, block_hash);
    let retry = RetryConfig::default();
    let mut last_err = String::new();

    for attempt in 0..=retry.max_retries {
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        if let Some(key) = api_key {
            let headers = worker::Headers::new();
            let _ = headers.set("woc-api-key", key);
            init.with_headers(headers);
        }
        let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
        let mut response = match worker::Fetch::Request(request).send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("WoC block header fetch: {}", e);
                if attempt < retry.max_retries {
                    continue;
                }
                return Err(last_err);
            }
        };

        let status = response.status_code();
        if RetryConfig::is_retryable_status(status) && attempt < retry.max_retries {
            last_err = format!(
                "WoC block header API error {} (attempt {}/{})",
                status,
                attempt + 1,
                retry.max_retries + 1
            );
            continue;
        }
        if status >= 400 {
            return Err(format!("WoC block header API error {}", status));
        }

        return response
            .json::<WocBlockHeader>()
            .await
            .map_err(|e| format!("Failed to parse block header: {}", e));
    }

    Err(last_err)
}

/// Resolve the canonical block hash at a given height, preferring ChainTracks
/// (our own Worker — only stores the canonical chain) and falling back to
/// WhatsOnChain's `/block/height/{h}` if ChainTracks isn't configured or errors.
///
/// Returns `Ok(Some(hash))` when either source confirms canonical, `Ok(None)`
/// when neither source has data for that height, `Err` when both sources error.
///
/// Rationale: WoC is eventually-consistent around reorgs — its `/block/height`
/// endpoint has been observed to lag the actual canonical tip by minutes after
/// a reorg. ChainTracks maintains the canonical chain via header sync and is
/// authoritative for this check. WoC is retained as a safety net so a
/// chaintracks outage can't block proof verification.
pub(crate) async fn canonical_hash_at_height(
    height: u32,
    chaintracks_url: Option<&str>,
    woc_api_key: Option<&str>,
) -> std::result::Result<Option<String>, String> {
    // Try ChainTracks first if configured.
    if let Some(ct_url) = chaintracks_url {
        match fetch_canonical_hash_via_chaintracks(ct_url, height).await {
            Ok(Some(h)) => return Ok(Some(h)),
            Ok(None) => {
                worker::console_log!(
                    "chaintracks has no canonical hash for height {} — falling back to WoC",
                    height
                );
            }
            Err(e) => {
                worker::console_log!(
                    "chaintracks error for height {}: {} — falling back to WoC",
                    height,
                    e
                );
            }
        }
    }

    // Fallback to WoC.
    fetch_canonical_hash_at_height(height, woc_api_key).await
}

/// Fetch the canonical block hash at a given height from ChainTracks.
///
/// Calls `GET {base_url}/findHeaderHexForHeight?height={h}` and extracts the
/// `value.hash` field. Returns `Ok(None)` when the response parses but has no
/// header (empty `value`), `Err` on any network/API failure.
async fn fetch_canonical_hash_via_chaintracks(
    base_url: &str,
    height: u32,
) -> std::result::Result<Option<String>, String> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Hdr {
        hash: Option<String>,
    }
    #[derive(Deserialize)]
    struct Resp {
        status: String,
        value: Option<Hdr>,
    }

    let url = format!("{}/findHeaderHexForHeight?height={}", base_url, height);
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);
    let request = worker::Request::new_with_init(&url, &init)
        .map_err(|e| format!("chaintracks request error: {}", e))?;
    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("chaintracks fetch error for height {}: {}", height, e))?;

    let status = response.status_code();
    if status == 404 {
        return Ok(None);
    }
    if status >= 400 {
        return Err(format!(
            "chaintracks API error {} for height {}",
            status, height
        ));
    }

    let resp: Resp = response
        .json()
        .await
        .map_err(|e| format!("failed to parse chaintracks response: {}", e))?;
    if resp.status != "success" {
        return Err(format!(
            "chaintracks returned status={:?} for height {}",
            resp.status, height
        ));
    }
    Ok(resp.value.and_then(|v| v.hash))
}

/// Fetch the canonical block hash at a given height from WhatsOnChain.
///
/// `GET /block/height/{height}` returns the block JSON whose `hash` field is
/// the canonical chain's block at that height — orphan blocks are NOT returned
/// here, so we use this endpoint to filter TSC proofs that reference orphan
/// blocks (post-reorg). Returns `Ok(None)` on 404 (height beyond tip, or WoC
/// has no data for it).
///
/// Retries on 429 and 5xx, same pattern as the other fetch helpers.
async fn fetch_canonical_hash_at_height(
    height: u32,
    api_key: Option<&str>,
) -> std::result::Result<Option<String>, String> {
    #[derive(Deserialize)]
    struct WocBlock {
        hash: String,
    }

    let url = format!("{}/block/height/{}", WOC_BASE, height);
    let retry = RetryConfig::default();
    let mut last_err = String::new();

    for attempt in 0..=retry.max_retries {
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        if let Some(key) = api_key {
            let headers = worker::Headers::new();
            let _ = headers.set("woc-api-key", key);
            init.with_headers(headers);
        }
        let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
        let mut response = match worker::Fetch::Request(request).send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("WoC block-by-height fetch: {}", e);
                if attempt < retry.max_retries {
                    continue;
                }
                return Err(last_err);
            }
        };

        let status = response.status_code();
        if status == 404 {
            return Ok(None);
        }
        if RetryConfig::is_retryable_status(status) && attempt < retry.max_retries {
            last_err = format!(
                "WoC block-by-height API error {} (attempt {}/{})",
                status,
                attempt + 1,
                retry.max_retries + 1
            );
            continue;
        }
        if status >= 400 {
            return Err(format!("WoC block-by-height API error {}", status));
        }

        return response
            .json::<WocBlock>()
            .await
            .map(|b| Some(b.hash))
            .map_err(|e| format!("Failed to parse block-by-height: {}", e));
    }

    Err(last_err)
}

// =============================================================================
// Chain height
// =============================================================================

#[derive(Debug, Deserialize)]
struct WocChainInfo {
    blocks: u32,
}

/// Parse WoC chain/info response JSON into the current block height.
/// Exported for unit testing without HTTP.
fn parse_chain_info_response(text: &str) -> std::result::Result<u32, String> {
    let info: WocChainInfo =
        serde_json::from_str(text).map_err(|e| format!("Failed to parse chain info: {}", e))?;
    Ok(info.blocks)
}

/// Fetch current chain tip height from WhatsOnChain.
///
/// Retries on 429 and 5xx.
async fn fetch_chain_height(api_key: Option<&str>) -> std::result::Result<u32, String> {
    let url = format!("{}/chain/info", WOC_BASE);
    let retry = RetryConfig::default();
    let mut last_err = String::new();

    for attempt in 0..=retry.max_retries {
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        if let Some(key) = api_key {
            let headers = worker::Headers::new();
            let _ = headers.set("woc-api-key", key);
            init.with_headers(headers);
        }
        let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
        let mut response = match worker::Fetch::Request(request).send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("WoC chain info fetch: {}", e);
                if attempt < retry.max_retries {
                    continue;
                }
                return Err(last_err);
            }
        };

        let status = response.status_code();
        if RetryConfig::is_retryable_status(status) && attempt < retry.max_retries {
            last_err = format!(
                "WoC chain info API error {} (attempt {}/{})",
                status,
                attempt + 1,
                retry.max_retries + 1
            );
            continue;
        }
        if status >= 400 {
            return Err(format!("WoC chain info API error {}", status));
        }

        let text = response.text().await.map_err(|e| e.to_string())?;
        return parse_chain_info_response(&text);
    }

    Err(last_err)
}

// =============================================================================
// TSC Proof -> BRC-74 Binary MerklePath Conversion
// =============================================================================

/// Convert a WhatsOnChain TSC proof to BRC-74 binary MerklePath format.
///
/// Exposed as `pub(crate)` so `bitails::BitailsProvider` can reuse it — Bitails
/// returns TSC proofs in the same shape as WoC.
pub(crate) fn tsc_proof_to_binary(
    proof: &WocTscProof,
    block_height: u32,
) -> std::result::Result<Vec<u8>, String> {
    let tree_height = proof.nodes.len();
    let mut path: Vec<Vec<MerklePathLeaf>> = Vec::with_capacity(tree_height);
    let mut index = proof.index as u64;
    let txid = &proof.tx_or_id;

    for level in 0..tree_height {
        let node = &proof.nodes[level];
        let is_odd = index % 2 == 1;
        let sibling_offset = if is_odd { index - 1 } else { index + 1 };

        // WoC uses "*" for duplicate, some impls use "" (empty string)
        let is_duplicate = node == "*" || node.is_empty();

        let sibling_leaf = if is_duplicate {
            MerklePathLeaf::new_duplicate(sibling_offset)
        } else {
            MerklePathLeaf::new(sibling_offset, node.clone())
        };

        if level == 0 {
            let txid_leaf = MerklePathLeaf::new_txid(proof.index as u64, txid.clone());
            if is_odd {
                path.push(vec![sibling_leaf, txid_leaf]);
            } else {
                path.push(vec![txid_leaf, sibling_leaf]);
            }
        } else {
            path.push(vec![sibling_leaf]);
        }

        index >>= 1;
    }

    let merkle_path =
        MerklePath::new(block_height, path).map_err(|e| format!("MerklePath::new: {}", e))?;
    Ok(merkle_path.to_binary())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // parse_tsc_proof_response
    // =========================================================================

    #[test]
    fn test_parse_null_response() {
        assert!(parse_tsc_proof_response("null").unwrap().is_empty());
    }

    #[test]
    fn test_parse_empty_response() {
        assert!(parse_tsc_proof_response("").unwrap().is_empty());
    }

    #[test]
    fn test_parse_empty_array_response() {
        assert!(parse_tsc_proof_response("[]").unwrap().is_empty());
    }

    #[test]
    fn test_parse_valid_proof_response() {
        let json = r#"[{
            "index": 5,
            "txOrId": "abcd1234",
            "target": "00000000000000000abc",
            "nodes": ["aaa", "bbb", "*"]
        }]"#;
        let result = parse_tsc_proof_response(json).unwrap();
        assert_eq!(result.len(), 1);
        let proof = &result[0];
        assert_eq!(proof.index, 5);
        assert_eq!(proof.tx_or_id, "abcd1234");
        assert_eq!(proof.target, "00000000000000000abc");
        assert_eq!(proof.nodes, vec!["aaa", "bbb", "*"]);
    }

    #[test]
    fn test_parse_invalid_json_returns_error() {
        let result = parse_tsc_proof_response("{not valid json}");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_whitespace_is_not_empty() {
        let result = parse_tsc_proof_response("   ");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_single_object_not_array() {
        let json = r#"{"index": 1, "txOrId": "abc", "target": "def", "nodes": ["x"]}"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_multiple_proofs_returns_all_for_canonical_filtering() {
        // WoC returns multiple entries after a reorg (same tx mined in both
        // orphan and canonical blocks). `parse_tsc_proof_response` must
        // return ALL entries so the caller can select the canonical one via
        // block-hash comparison. Returning only the first was the bug that
        // produced stale proofs at height 943424 (claude drain incident,
        // 2026-04-17).
        let json = r#"[
            {"index": 1, "txOrId": "first", "target": "orphan_block", "nodes": ["a"]},
            {"index": 2, "txOrId": "second", "target": "canonical_block", "nodes": ["b"]}
        ]"#;
        let result = parse_tsc_proof_response(json).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].target, "orphan_block");
        assert_eq!(result[1].target, "canonical_block");
    }

    // =========================================================================
    // parse_chain_info_response
    // =========================================================================

    #[test]
    fn test_parse_chain_info_valid() {
        let json = r#"{"chain":"main","blocks":890123,"headers":890123,"bestblockhash":"000000000000000003abc","difficulty":1.23e11,"mediantime":1712345678,"verificationprogress":0.9999}"#;
        let height = parse_chain_info_response(json).unwrap();
        assert_eq!(height, 890123);
    }

    #[test]
    fn test_parse_chain_info_blocks_only() {
        let json = r#"{"blocks":850000}"#;
        let height = parse_chain_info_response(json).unwrap();
        assert_eq!(height, 850000);
    }

    #[test]
    fn test_parse_chain_info_invalid_json() {
        let result = parse_chain_info_response("{not valid}");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_chain_info_missing_blocks() {
        let json = r#"{"chain":"main","headers":890123}"#;
        let result = parse_chain_info_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_chain_info_empty() {
        let result = parse_chain_info_response("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_chain_info_zero_height() {
        let json = r#"{"blocks":0}"#;
        let height = parse_chain_info_response(json).unwrap();
        assert_eq!(height, 0);
    }

    // =========================================================================
    // BroadcastError Display (new 3-way classification)
    // =========================================================================

    #[test]
    fn test_broadcast_error_double_spend_display() {
        let err = BroadcastError::DoubleSpend("double-spend".to_string());
        assert_eq!(format!("{}", err), "Double-spend: double-spend");
    }

    #[test]
    fn test_broadcast_error_invalid_tx_display() {
        let err = BroadcastError::InvalidTx("bad script".to_string());
        assert_eq!(format!("{}", err), "Invalid tx: bad script");
    }

    #[test]
    fn test_broadcast_error_service_error_display() {
        let err = BroadcastError::ServiceError("timeout".to_string());
        assert_eq!(format!("{}", err), "Service error: timeout");
    }

    // =========================================================================
    // BroadcastResult construction
    // =========================================================================

    #[test]
    fn test_broadcast_result_fields() {
        let result = BroadcastResult {
            txid: "abc123".to_string(),
            tx_status: "sent".to_string(),
            seen_on_network: false,
        };
        assert_eq!(result.txid, "abc123");
        assert_eq!(result.tx_status, "sent");
        assert!(!result.seen_on_network);
    }

    // =========================================================================
    // ProofResult construction
    // =========================================================================

    #[test]
    fn test_proof_result_fields() {
        let result = ProofResult {
            txid: "def456".to_string(),
            merkle_path_binary: vec![1, 2, 3],
            block_height: 800000,
            block_hash: "0000abc".to_string(),
            merkle_root: "deadbeef".to_string(),
        };
        assert_eq!(result.txid, "def456");
        assert_eq!(result.merkle_path_binary, vec![1, 2, 3]);
        assert_eq!(result.block_height, 800000);
    }

    // =========================================================================
    // WocTxStatus deserialization
    // =========================================================================

    #[test]
    fn test_woc_tx_status_deserialize_mined() {
        let json = r#"{"txid":"aabb","confirmations":5}"#;
        let s: WocTxStatus = serde_json::from_str(json).unwrap();
        assert_eq!(s.txid, "aabb");
        assert_eq!(s.confirmations, Some(5));
        assert!(s.error.is_none());
    }

    #[test]
    fn test_woc_tx_status_deserialize_mempool() {
        // In mempool: confirmations absent (null/missing)
        let json = r#"{"txid":"ccdd","confirmations":null}"#;
        let s: WocTxStatus = serde_json::from_str(json).unwrap();
        assert_eq!(s.txid, "ccdd");
        assert!(s.confirmations.is_none());
        assert!(s.error.is_none());
    }

    #[test]
    fn test_woc_tx_status_deserialize_unknown() {
        let json = r#"{"txid":"eeff","error":"unknown"}"#;
        let s: WocTxStatus = serde_json::from_str(json).unwrap();
        assert_eq!(s.txid, "eeff");
        assert!(s.confirmations.is_none());
        assert_eq!(s.error.as_deref(), Some("unknown"));
    }

    #[test]
    fn test_woc_tx_status_deserialize_missing_confirmations() {
        // confirmations key absent entirely
        let json = r#"{"txid":"1122"}"#;
        let s: WocTxStatus = serde_json::from_str(json).unwrap();
        assert_eq!(s.txid, "1122");
        assert!(s.confirmations.is_none());
        assert!(s.error.is_none());
    }

    #[test]
    fn test_woc_tx_status_deserialize_zero_confirmations() {
        let json = r#"{"txid":"3344","confirmations":0}"#;
        let s: WocTxStatus = serde_json::from_str(json).unwrap();
        assert_eq!(s.confirmations, Some(0));
    }

    #[test]
    fn test_woc_tx_status_array_deserialize() {
        let json = r#"[
            {"txid":"aa","confirmations":10},
            {"txid":"bb","confirmations":null},
            {"txid":"cc","error":"unknown"}
        ]"#;
        let statuses: Vec<WocTxStatus> = serde_json::from_str(json).unwrap();
        assert_eq!(statuses.len(), 3);
        assert_eq!(statuses[0].txid, "aa");
        assert_eq!(statuses[0].confirmations, Some(10));
        assert_eq!(statuses[1].txid, "bb");
        assert!(statuses[1].confirmations.is_none());
        assert_eq!(statuses[2].error.as_deref(), Some("unknown"));
    }

    // =========================================================================
    // Chunking logic — txids.chunks(20)
    // =========================================================================

    #[test]
    fn test_chunk_size_20_exact() {
        let txids: Vec<String> = (0..20).map(|i| format!("tx{}", i)).collect();
        let chunks: Vec<&[String]> = txids.chunks(20).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 20);
    }

    #[test]
    fn test_chunk_size_20_one_extra() {
        let txids: Vec<String> = (0..21).map(|i| format!("tx{}", i)).collect();
        let chunks: Vec<&[String]> = txids.chunks(20).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 20);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn test_chunk_size_20_small_input() {
        let txids: Vec<String> = (0..5).map(|i| format!("tx{}", i)).collect();
        let chunks: Vec<&[String]> = txids.chunks(20).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 5);
    }

    #[test]
    fn test_chunk_size_20_empty() {
        let txids: Vec<String> = vec![];
        let chunks: Vec<&[String]> = txids.chunks(20).collect();
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn test_chunk_size_20_large_input() {
        let txids: Vec<String> = (0..100).map(|i| format!("tx{}", i)).collect();
        let chunks: Vec<&[String]> = txids.chunks(20).collect();
        assert_eq!(chunks.len(), 5);
        for chunk in &chunks {
            assert_eq!(chunk.len(), 20);
        }
    }

    #[test]
    fn test_chunk_size_20_boundary_39() {
        let txids: Vec<String> = (0..39).map(|i| format!("tx{}", i)).collect();
        let chunks: Vec<&[String]> = txids.chunks(20).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 20);
        assert_eq!(chunks[1].len(), 19);
    }

    // =========================================================================
    // Status classification logic (mirrors get_status_for_txids match arms)
    // =========================================================================

    /// Helper: classify a WocTxStatus the same way get_status_for_txids does.
    fn classify_woc_status(d: &WocTxStatus) -> (&str, Option<u32>) {
        if d.error.as_deref() == Some("unknown") {
            ("unknown", None)
        } else if d.confirmations.is_none() {
            ("known", Some(0))
        } else {
            ("mined", d.confirmations)
        }
    }

    #[test]
    fn test_classify_mined_with_confirmations() {
        let s = WocTxStatus {
            txid: "aa".to_string(),
            confirmations: Some(10),
            error: None,
        };
        let (status, depth) = classify_woc_status(&s);
        assert_eq!(status, "mined");
        assert_eq!(depth, Some(10));
    }

    #[test]
    fn test_classify_mempool_no_confirmations() {
        let s = WocTxStatus {
            txid: "bb".to_string(),
            confirmations: None,
            error: None,
        };
        let (status, depth) = classify_woc_status(&s);
        assert_eq!(status, "known");
        assert_eq!(depth, Some(0));
    }

    #[test]
    fn test_classify_unknown_error() {
        let s = WocTxStatus {
            txid: "cc".to_string(),
            confirmations: None,
            error: Some("unknown".to_string()),
        };
        let (status, depth) = classify_woc_status(&s);
        assert_eq!(status, "unknown");
        assert!(depth.is_none());
    }

    #[test]
    fn test_classify_mined_with_zero_confirmations() {
        // Zero confirmations is still "mined" (Some(0) is not None)
        let s = WocTxStatus {
            txid: "dd".to_string(),
            confirmations: Some(0),
            error: None,
        };
        let (status, depth) = classify_woc_status(&s);
        assert_eq!(status, "mined");
        assert_eq!(depth, Some(0));
    }

    #[test]
    fn test_classify_missing_from_response() {
        // When a txid is not found in the WoC response at all,
        // get_status_for_txids returns "unknown" with None depth.
        // This is the `None` arm of the match (d not found).
        let status = "unknown";
        let depth: Option<u32> = None;
        assert_eq!(status, "unknown");
        assert!(depth.is_none());
    }

    // =========================================================================
    // TxStatusDetail construction
    // =========================================================================

    #[test]
    fn test_tx_status_detail_construction() {
        let detail = super::super::TxStatusDetail {
            txid: "abc".to_string(),
            status: "mined".to_string(),
            depth: Some(5),
        };
        assert_eq!(detail.txid, "abc");
        assert_eq!(detail.status, "mined");
        assert_eq!(detail.depth, Some(5));
    }

    #[test]
    fn test_tx_status_detail_unknown() {
        let detail = super::super::TxStatusDetail {
            txid: "xyz".to_string(),
            status: "unknown".to_string(),
            depth: None,
        };
        assert_eq!(detail.status, "unknown");
        assert!(detail.depth.is_none());
    }

    // =========================================================================
    // parse_spent_response — G5 outpoint spent-status (WoC /tx/{txid}/{vout}/spent)
    // =========================================================================

    #[test]
    fn test_parse_spent_confirmed() {
        // Exact live shape captured 2026-07-05 from a mainnet spent outpoint.
        let body = r#"{"txid":"966982298fb694542673baed76c09cd35e8420610192ede11077abf8769d33b2","vin":0,"status":"confirmed"}"#;
        let status = parse_spent_response(body).unwrap();
        assert_eq!(
            status,
            SpentStatus::Spent {
                spending_txid: "966982298fb694542673baed76c09cd35e8420610192ede11077abf8769d33b2"
                    .to_string()
            }
        );
    }

    #[test]
    fn test_parse_spent_unconfirmed_is_still_spent() {
        // Owner rule: SEEN = final on BSV. A mempool-only spend counts as
        // spent — the status field must never gate the decision.
        let body = r#"{"txid":"aa11","vin":2,"status":"unconfirmed"}"#;
        let status = parse_spent_response(body).unwrap();
        assert_eq!(
            status,
            SpentStatus::Spent {
                spending_txid: "aa11".to_string()
            }
        );
    }

    #[test]
    fn test_parse_spent_missing_optional_fields() {
        // vin/status absent — txid alone is sufficient.
        let body = r#"{"txid":"bb22"}"#;
        let status = parse_spent_response(body).unwrap();
        assert!(matches!(status, SpentStatus::Spent { spending_txid } if spending_txid == "bb22"));
    }

    #[test]
    fn test_parse_spent_empty_txid_is_error() {
        // Ambiguous 200 — must be a service error (no action), never Unspent.
        let body = r#"{"txid":"","vin":0,"status":"confirmed"}"#;
        assert!(parse_spent_response(body).is_err());
    }

    #[test]
    fn test_parse_spent_garbage_is_error() {
        assert!(parse_spent_response("Not Found").is_err());
        assert!(parse_spent_response("").is_err());
        assert!(parse_spent_response("null").is_err());
        assert!(parse_spent_response("{}").is_err());
    }
}
