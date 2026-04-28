//! Bitails proof provider — fallback for WhatsOnChain when WoC hasn't
//! indexed a canonical proof for a tx yet (typically post-reorg).
//!
//! Only implements `get_proof`; other `ProofService` methods delegate to a
//! default or return `Ok(None)` so `MultiProvider` can still use WoC for
//! raw-tx fetch, chain height, and batch status checks.
//!
//! Same TSC proof JSON shape as WoC, so we reuse `tsc_proof_to_binary` via
//! the `WocTscProof` struct from the `woc` module.

use super::woc::{canonical_hash_at_height, tsc_proof_to_binary, WocTscProof};
use super::{ProofResult, ProofService, TxStatusDetail};
use serde::Deserialize;

const BITAILS_BASE: &str = "https://api.bitails.io";

/// Bitails proof provider.
///
/// Holds an optional ChainTracks URL (wired in `lib.rs`) so the canonical-hash
/// filter can consult our authoritative chain state, falling back to WoC.
pub struct BitailsProvider {
    chaintracks_url: Option<String>,
    /// WoC API key is needed ONLY for the WoC fallback on canonical-hash
    /// lookups (Bitails itself has no API key in this path). Carrying the
    /// key here keeps the canonical filter identical to WocProvider's.
    woc_api_key: Option<String>,
}

impl BitailsProvider {
    pub fn new() -> Self {
        Self {
            chaintracks_url: None,
            woc_api_key: None,
        }
    }

    pub fn with_chaintracks_url(mut self, url: Option<String>) -> Self {
        self.chaintracks_url = url.map(|u| u.trim_end_matches('/').to_string());
        self
    }

    pub fn with_woc_api_key(mut self, key: Option<String>) -> Self {
        self.woc_api_key = key;
        self
    }
}

impl Default for BitailsProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Bitails TSC proof response has an extra `__meta` field that we ignore;
/// the top-level `index/txOrId/target/nodes` fields match WoC's format so
/// we deserialize into `WocTscProof` directly.
#[derive(Deserialize)]
struct BitailsTscResponse {
    #[serde(flatten)]
    proof: WocTscProof,
}

/// Lightweight block-header response from Bitails's `/block/{hash}`. We only
/// need the height for `tsc_proof_to_binary`.
#[derive(Deserialize)]
struct BitailsBlockHeader {
    height: u32,
    #[serde(rename = "merkleroot", alias = "merkleRoot", default)]
    merkle_root: Option<String>,
}

impl ProofService for BitailsProvider {
    async fn get_proof(&self, txid: &str) -> std::result::Result<Option<ProofResult>, String> {
        // Step 1: fetch the TSC proof from Bitails. Bitails returns a single
        // object (not an array) because it prunes old blocks — we don't see
        // the orphan/canonical multi-entry shape WoC produces. Still
        // canonical-filter the result: trust no external source blindly.
        let proof = match fetch_bitails_tsc_proof(txid).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        // Step 2: resolve the block height by fetching the header from Bitails.
        let header = match fetch_bitails_block_header(&proof.target).await? {
            Some(h) => h,
            None => {
                worker::console_log!(
                    "bitails header not found for block {} — skipping proof for {}",
                    &proof.target[..16.min(proof.target.len())],
                    txid
                );
                return Ok(None);
            }
        };

        // Step 3: canonical-filter via ChainTracks (→ WoC fallback).
        let canonical_hash = match canonical_hash_at_height(
            header.height,
            self.chaintracks_url.as_deref(),
            self.woc_api_key.as_deref(),
        )
        .await
        {
            Ok(Some(h)) => h,
            Ok(None) => {
                worker::console_log!(
                    "bitails: no canonical hash for height {} — skipping proof for {}",
                    header.height,
                    txid
                );
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        if canonical_hash != proof.target {
            worker::console_log!(
                "bitails proof for {} is orphaned: target={} canonical={}",
                txid,
                &proof.target[..16.min(proof.target.len())],
                &canonical_hash[..16.min(canonical_hash.len())]
            );
            return Ok(None);
        }

        // Step 4: convert TSC → BRC-74 and return.
        let merkle_path_binary = tsc_proof_to_binary(&proof, header.height)?;
        Ok(Some(ProofResult {
            txid: txid.to_string(),
            merkle_path_binary,
            block_height: header.height,
            block_hash: proof.target.clone(),
            merkle_root: header.merkle_root.unwrap_or_default(),
        }))
    }

    // Bitails isn't a primary raw-tx / chain-height / batch-status source in
    // this codebase — callers always have WoC available for those. Keep these
    // as minimal stubs so the trait is satisfied and the MultiProvider's
    // delegation to WoC continues to win for these ops.
    async fn get_raw_tx(&self, _txid: &str) -> std::result::Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    async fn get_chain_height(&self) -> std::result::Result<u32, String> {
        Err("bitails: get_chain_height not implemented".to_string())
    }

    async fn get_status_for_txids(
        &self,
        txids: &[String],
    ) -> std::result::Result<Vec<TxStatusDetail>, String> {
        // Default to "unknown" — MultiProvider routes to WoC for this op.
        Ok(txids
            .iter()
            .map(|t| TxStatusDetail {
                txid: t.clone(),
                status: "unknown".to_string(),
                depth: None,
            })
            .collect())
    }
}

async fn fetch_bitails_tsc_proof(
    txid: &str,
) -> std::result::Result<Option<WocTscProof>, String> {
    let url = format!("{}/tx/{}/proof/tsc", BITAILS_BASE, txid);
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);
    let request = worker::Request::new_with_init(&url, &init)
        .map_err(|e| format!("bitails tsc proof request error: {}", e))?;
    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("bitails tsc proof fetch error: {}", e))?;

    let status = response.status_code();
    if status == 404 {
        return Ok(None);
    }
    if status >= 400 {
        return Err(format!("bitails tsc proof API error {}", status));
    }

    // Bitails may return either a single object or an array depending on
    // version. Try object first, then array, then extract the flattened
    // `WocTscProof` payload.
    let text = response
        .text()
        .await
        .map_err(|e| format!("bitails tsc proof read error: {}", e))?;
    if text.is_empty() || text == "null" || text == "[]" {
        return Ok(None);
    }
    if let Ok(single) = serde_json::from_str::<BitailsTscResponse>(&text) {
        return Ok(Some(single.proof));
    }
    if let Ok(arr) = serde_json::from_str::<Vec<BitailsTscResponse>>(&text) {
        // Bitails is pruned — if it ever does return multiple, let the
        // caller canonical-filter by taking the LAST entry (newest block).
        return Ok(arr.into_iter().last().map(|r| r.proof));
    }
    Err(format!(
        "failed to parse bitails tsc proof: unexpected shape (first 120 bytes = {:?})",
        &text[..text.len().min(120)]
    ))
}

async fn fetch_bitails_block_header(
    block_hash: &str,
) -> std::result::Result<Option<BitailsBlockHeader>, String> {
    let url = format!("{}/block/{}", BITAILS_BASE, block_hash);
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);
    let request = worker::Request::new_with_init(&url, &init)
        .map_err(|e| format!("bitails block header request error: {}", e))?;
    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("bitails block header fetch error: {}", e))?;

    let status = response.status_code();
    if status == 404 {
        return Ok(None);
    }
    if status >= 400 {
        return Err(format!("bitails block header API error {}", status));
    }

    response
        .json::<BitailsBlockHeader>()
        .await
        .map(Some)
        .map_err(|e| format!("failed to parse bitails block header: {}", e))
}
