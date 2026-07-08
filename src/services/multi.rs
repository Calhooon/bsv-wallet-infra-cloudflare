//! Multi-provider that tries ARC first, falls back to WoC.
//!
//! ARC is the primary broadcast/proof provider (supports BEEF natively,
//! returns BRC-74 BUMP proofs). WoC is the fallback for when ARC is
//! unreachable. Permanent failures from ARC are NOT retried on WoC.

use super::arc::ArcProvider;
use super::bitails::BitailsProvider;
use super::woc::WocProvider;
use super::{
    BroadcastError, BroadcastResult, BroadcastService, ProofResult, ProofService, SpentStatus,
    TxStatusDetail,
};

// =============================================================================
// MultiProvider
// =============================================================================

/// Combined ARC + WoC + Bitails provider with automatic failover.
///
/// For broadcasts: tries ARC first. If ARC returns a `ServiceError`
/// (transient failure / all endpoints down), falls back to WoC.
/// If ARC returns a permanent failure (DoubleSpend, InvalidTx), the
/// error is returned immediately -- no WoC retry. (Bitails isn't used for
/// broadcast in this codebase — ARC+WoC cover it.)
///
/// For proofs: tries ARC → WoC → Bitails. Each provider canonical-filters
/// its own response (trust no external source blindly). First canonical
/// hit wins. If all return None, `MultiProvider` returns None and the
/// caller retries on the next monitor cycle.
///
/// Bitails was added on 2026-04-17 to unblock the drain of claude, whose
/// ancestors at block 943424 had orphan-only proofs from WoC after a
/// reorg. Bitails happened to have the canonical proofs.
pub struct MultiProvider {
    arc: ArcProvider,
    woc: WocProvider,
    bitails: BitailsProvider,
    /// Our own chaintracks Worker — preferred tip source when configured.
    chaintracks_url: Option<String>,
}

impl MultiProvider {
    pub fn new(arc_api_key: Option<String>, woc_api_key: Option<String>) -> Self {
        Self::with_chaintracks(arc_api_key, woc_api_key, None)
    }

    /// Construct a MultiProvider with ChainTracks enabled for canonical-hash
    /// verification. ChainTracks is our own Worker and is authoritative for
    /// canonical chain state; passing `None` preserves the prior
    /// WoC-only canonical check (backward compatible).
    pub fn with_chaintracks(
        arc_api_key: Option<String>,
        woc_api_key: Option<String>,
        chaintracks_url: Option<String>,
    ) -> Self {
        Self {
            arc: ArcProvider::new(arc_api_key),
            woc: WocProvider::new(woc_api_key.clone())
                .with_chaintracks_url(chaintracks_url.clone()),
            bitails: BitailsProvider::new()
                .with_chaintracks_url(chaintracks_url.clone())
                .with_woc_api_key(woc_api_key),
            chaintracks_url: chaintracks_url
                .map(|u| u.trim_end_matches('/').to_string()),
        }
    }
}

// =============================================================================
// BroadcastService
// =============================================================================

impl BroadcastService for MultiProvider {
    async fn broadcast_raw_tx(
        &self,
        raw_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        let arc_t0 = js_sys::Date::now();
        let arc_result = self.arc.broadcast_raw_tx(raw_hex).await;
        let arc_ms = js_sys::Date::now() - arc_t0;
        let arc_outcome = match &arc_result {
            Ok(_) => "ok",
            Err(BroadcastError::DoubleSpend(_)) => "double_spend",
            Err(BroadcastError::InvalidTx(_)) => "invalid_tx",
            Err(BroadcastError::ServiceError(_)) => "service_error",
        };
        worker::console_log!(
            "BENCH broadcast.arc[raw,outcome={}]: {:.0} ms",
            arc_outcome,
            arc_ms
        );
        match arc_result {
            Ok(result) => Ok(result),
            Err(BroadcastError::DoubleSpend(msg)) => Err(BroadcastError::DoubleSpend(msg)),
            Err(BroadcastError::InvalidTx(msg)) => Err(BroadcastError::InvalidTx(msg)),
            Err(BroadcastError::ServiceError(_)) => {
                let woc_t0 = js_sys::Date::now();
                let r = self.woc.broadcast_raw_tx(raw_hex).await;
                worker::console_log!(
                    "BENCH broadcast.woc[raw]: {:.0} ms",
                    js_sys::Date::now() - woc_t0
                );
                r
            }
        }
    }

    async fn broadcast_beef(
        &self,
        beef_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        let arc_t0 = js_sys::Date::now();
        let arc_result = self.arc.broadcast_beef(beef_hex).await;
        let arc_ms = js_sys::Date::now() - arc_t0;
        let arc_outcome = match &arc_result {
            Ok(_) => "ok",
            Err(BroadcastError::DoubleSpend(_)) => "double_spend",
            Err(BroadcastError::InvalidTx(_)) => "invalid_tx",
            Err(BroadcastError::ServiceError(_)) => "service_error",
        };
        worker::console_log!(
            "BENCH broadcast.arc[beef,outcome={},bytes={}]: {:.0} ms",
            arc_outcome,
            beef_hex.len() / 2,
            arc_ms
        );
        match arc_result {
            Ok(result) => Ok(result),
            Err(BroadcastError::DoubleSpend(msg)) => Err(BroadcastError::DoubleSpend(msg)),
            Err(BroadcastError::InvalidTx(msg)) => Err(BroadcastError::InvalidTx(msg)),
            Err(BroadcastError::ServiceError(_)) => {
                let woc_t0 = js_sys::Date::now();
                let r = self.woc.broadcast_beef(beef_hex).await;
                worker::console_log!(
                    "BENCH broadcast.woc[beef]: {:.0} ms",
                    js_sys::Date::now() - woc_t0
                );
                r
            }
        }
    }
}

// =============================================================================
// ProofService
// =============================================================================

impl ProofService for MultiProvider {
    async fn get_chain_height(&self) -> std::result::Result<u32, String> {
        // Chaintracks (our own Worker) first — it is the designated tip
        // truth source for the monitor's block-clocked attempt gate. A zero
        // height is a degraded-service signal (empty tip row — chaintracks
        // audit C4), never chain state: fall back to WoC, and let the
        // caller freeze the clock if both fail.
        if let Some(base) = &self.chaintracks_url {
            match fetch_chaintracks_height(base).await {
                Ok(h) if h > 0 => return Ok(h),
                Ok(h) => worker::console_error!(
                    "chaintracks currentHeight returned {} — degraded, falling back to WoC",
                    h
                ),
                Err(e) => worker::console_log!(
                    "chaintracks currentHeight failed ({}), falling back to WoC",
                    e
                ),
            }
        }
        self.woc.get_chain_height().await
    }

    async fn get_proof(&self, txid: &str) -> std::result::Result<Option<ProofResult>, String> {
        // ARC → WoC → Bitails. Each provider canonical-filters its own
        // response; first canonical hit wins. Previously only ARC→WoC was
        // tried, which left us stranded when WoC had only an orphan proof
        // for a tx (claude drain incident, 2026-04-17, block 943424).
        let tx_short = &txid[..16.min(txid.len())];

        match self.arc.get_proof(txid).await {
            Ok(Some(proof)) => {
                worker::console_log!(
                    "get_proof {}: ARC→Some (h={})",
                    tx_short,
                    proof.block_height
                );
                return Ok(Some(proof));
            }
            Ok(None) => {
                worker::console_log!("get_proof {}: ARC→None, try WoC", tx_short);
            }
            Err(e) => worker::console_log!("get_proof {}: ARC err={}, try WoC", tx_short, e),
        }

        match self.woc.get_proof(txid).await {
            Ok(Some(proof)) => {
                worker::console_log!(
                    "get_proof {}: WoC→Some (h={})",
                    tx_short,
                    proof.block_height
                );
                return Ok(Some(proof));
            }
            Ok(None) => {
                worker::console_log!("get_proof {}: WoC→None, try Bitails", tx_short);
            }
            Err(e) => {
                worker::console_log!("get_proof {}: WoC err={}, try Bitails", tx_short, e);
            }
        }

        let r = self.bitails.get_proof(txid).await;
        match &r {
            Ok(Some(p)) => worker::console_log!(
                "get_proof {}: Bitails→Some (h={})",
                tx_short,
                p.block_height
            ),
            Ok(None) => worker::console_log!("get_proof {}: Bitails→None (no canonical)", tx_short),
            Err(e) => worker::console_log!("get_proof {}: Bitails err={}", tx_short, e),
        }
        r
    }

    async fn get_raw_tx(&self, txid: &str) -> std::result::Result<Option<Vec<u8>>, String> {
        // ARC doesn't support raw tx fetch, go straight to WoC
        self.woc.get_raw_tx(txid).await
    }

    /// Delegate batch status check to WoC.
    ///
    /// Critical for the `check_for_proofs` triage optimization: without this
    /// delegation, `MultiProvider` would fall through to the default trait
    /// impl that returns all "unknown", causing the monitor's safety net to
    /// fire and "check all" — processing every pending tx even when most
    /// aren't mined yet. With this delegation, the triage filters to only
    /// actually-mined txs, roughly 9× reducing per-run WoC call volume.
    ///
    /// ARC has no equivalent batch status API, so we route directly to WoC.
    async fn get_status_for_txids(
        &self,
        txids: &[String],
    ) -> std::result::Result<Vec<TxStatusDetail>, String> {
        self.woc.get_status_for_txids(txids).await
    }

    /// Delegate outpoint spent-status to WoC (G5 external-spend scan).
    ///
    /// WoC is the only provider in this codebase with a working outpoint-spent
    /// index (`GET /tx/{txid}/{vout}/spent`). ARC has no outpoint query API,
    /// and Bitails' guessed equivalents all returned HTTP 500 when probed on
    /// 2026-07-05 — so no fallback chain here: a WoC failure surfaces as an
    /// error and the monitor's error budget bails the run (transient outages
    /// must not burn the cron or mis-mark outputs).
    async fn get_spent_status(
        &self,
        txid: &str,
        vout: u32,
    ) -> std::result::Result<SpentStatus, String> {
        self.woc.get_spent_status(txid, vout).await
    }

    fn reset_run_cache(&self) {
        // ARC is stateless; WoC holds the per-run header cache.
        self.woc.reset_run_cache();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_provider_construction_with_key() {
        let provider = MultiProvider::new(Some("test-key-123".to_string()), None);
        assert_eq!(provider.arc.api_key, Some("test-key-123".to_string()));
    }

    #[test]
    fn test_multi_provider_construction_without_key() {
        let provider = MultiProvider::new(None, None);
        assert_eq!(provider.arc.api_key, None);
    }
}

// =============================================================================
// Chaintracks tip fetch
// =============================================================================

/// Fetch the current tip height from our chaintracks Worker
/// (`GET {base}/currentHeight` → `{"status":"success","value":<u32>}`).
async fn fetch_chaintracks_height(base_url: &str) -> std::result::Result<u32, String> {
    #[derive(serde::Deserialize)]
    struct Resp {
        status: String,
        value: Option<u32>,
    }

    let url = format!("{}/currentHeight", base_url);
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);
    let request = worker::Request::new_with_init(&url, &init)
        .map_err(|e| format!("chaintracks request error: {}", e))?;
    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("chaintracks fetch error: {}", e))?;
    let status = response.status_code();
    if status >= 400 {
        return Err(format!("chaintracks currentHeight HTTP {}", status));
    }
    let resp: Resp = response
        .json()
        .await
        .map_err(|e| format!("chaintracks currentHeight parse: {}", e))?;
    if resp.status != "success" {
        return Err(format!("chaintracks currentHeight status={}", resp.status));
    }
    resp.value
        .ok_or_else(|| "chaintracks currentHeight: missing value".to_string())
}
