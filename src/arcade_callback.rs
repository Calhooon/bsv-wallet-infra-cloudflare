//! The Arcade V2 webhook proof path (`POST /arcade/callback`) — push-native merkle proofs.
//!
//! Arcade delivers status updates to the `X-CallbackUrl` registered at submit, authed with
//! `Authorization: Bearer <X-CallbackToken>` (verified against Arcade's own webhook sender:
//! `services/webhook/service.go::deliver` — JSON body = the `TransactionStatus` model). The
//! MINED payload carries `blockHash`/`blockHeight` and the free `merklePath` (BRC-74 BUMP,
//! hex) — so proofs arrive PUSHED minutes after mining instead of being polled out of
//! WoC/ARC by the cron monitor. The monitor's `check_for_proofs` stays untouched as the
//! FALLBACK (a missed/undelivered webhook costs one cron cycle, never the proof).
//!
//! Trust boundary (the trust-no-source law): the webhook is authenticated (shared secret)
//! but the PROOF is still verified before persisting — the BUMP must parse, contain OUR
//! txid, and its computed merkle root must be canonical for the claimed height per
//! ChainTracks (WoC header fallback). Only then does it ride `monitor::store_proof_result`
//! — the exact same persistence path the cron monitor uses (proven_txs upsert + req/tx →
//! completed + spendable re-enable).
//!
//! Bulk MINED fan-outs (`txids[]`, one event per block) share block fields but cannot carry
//! per-tx merklePaths inline — for any event missing a usable path, the handler re-reads
//! `GET /tx/{txid}` from Arcade (the stored record has it once MINED).
//!
//! The handler ALWAYS answers 2xx once authenticated: Arcade's reaper retries non-2xx
//! deliveries, and a per-tx processing error must not turn into a retry storm — the
//! monitor fallback owns anything skipped here.

use bsv_sdk::transaction::MerklePath;
use worker::*;

use crate::d1::Query;
use crate::services::chaintracker::HeaderService;

/// The proven_tx_reqs statuses that still await a proof — mirror of the monitor's
/// `check_for_proofs` candidate set. A webhook for any other row (already completed,
/// failed, or unknown to these books) is acknowledged and ignored.
const PENDING_STATUSES: &str = "('unmined', 'unknown', 'unconfirmed', 'callback', 'sending', 'reorg')";

#[derive(serde::Deserialize)]
struct ReqRow {
    proven_tx_req_id: Option<f64>,
    raw_tx: Option<String>, // hex from hex(raw_tx)
}

/// One per-tx event after unfanning a possible bulk payload.
struct TxEvent {
    txid: String,
    status: String,
    block_hash: String,
    block_height: Option<u64>,
    merkle_path_hex: Option<String>,
}

/// Unfan the webhook body into per-tx events (bulk = `txids[]` sharing status/block
/// fields; the shared `merklePath`, when present, is the block's compound BUMP — usable
/// per-tx because `compute_root(Some(txid))` walks the leaf for OUR txid).
fn unfan(body: &serde_json::Value) -> Vec<TxEvent> {
    let status = body
        .get("txStatus")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let block_hash = body
        .get("blockHash")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let block_height = body.get("blockHeight").and_then(|h| h.as_u64());
    let merkle_path_hex = body
        .get("merklePath")
        .and_then(|m| m.as_str())
        .filter(|m| !m.is_empty())
        .map(|m| m.to_string());

    let bulk: Vec<String> = body
        .get("txids")
        .and_then(|t| t.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    let txids: Vec<String> = if bulk.is_empty() {
        body.get("txid")
            .and_then(|t| t.as_str())
            .filter(|t| t.len() == 64)
            .map(|t| vec![t.to_string()])
            .unwrap_or_default()
    } else {
        bulk
    };

    txids
        .into_iter()
        .filter(|t| t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|txid| TxEvent {
            txid,
            status: status.clone(),
            block_hash: block_hash.clone(),
            block_height,
            merkle_path_hex: merkle_path_hex.clone(),
        })
        .collect()
}

/// Handle an authenticated webhook body. Returns a small JSON summary (also logged).
pub async fn handle(
    env: &Env,
    db: &D1Database,
    blobs: &Bucket,
    body: serde_json::Value,
) -> serde_json::Value {
    let events = unfan(&body);
    if events.is_empty() {
        console_log!("arcade-callback: no parseable tx event in payload");
        return serde_json::json!({ "ok": true, "processed": 0, "ignored": 0 });
    }

    let arcade_url = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| crate::services::arcade::ARCADE_URL_DEFAULT.to_string());
    let chaintracks_url = env
        .var("CHAINTRACKS_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty());
    let woc_api_key = env
        .secret("WOC_API_KEY")
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var("WOC_API_KEY").ok().map(|v| v.to_string()));
    let headers = crate::services::chaintracker::build_header_provider(chaintracks_url, woc_api_key);

    let mut processed = 0u32;
    let mut ignored = 0u32;
    for ev in events {
        match handle_one(db, blobs, &headers, &arcade_url, &ev).await {
            Ok(true) => processed += 1,
            Ok(false) => ignored += 1,
            Err(e) => {
                // Logged, acknowledged, NOT retried — the cron monitor owns the fallback.
                console_error!("arcade-callback: {} ({}): {}", ev.txid, ev.status, e);
                ignored += 1;
            }
        }
    }
    console_log!(
        "arcade-callback: processed={} ignored={} (status={})",
        processed,
        ignored,
        body.get("txStatus").and_then(|s| s.as_str()).unwrap_or("?")
    );
    serde_json::json!({ "ok": true, "processed": processed, "ignored": ignored })
}

/// Process one per-tx event. `Ok(true)` = a proof was verified + persisted; `Ok(false)` =
/// legitimately ignored (non-terminal status, not our tx, already proven).
async fn handle_one(
    db: &D1Database,
    blobs: &Bucket,
    headers: &crate::services::chaintracker::HeaderProvider,
    arcade_url: &str,
    ev: &TxEvent,
) -> std::result::Result<bool, String> {
    // Rung 1 scope: the PROOF push. MINED/IMMUTABLE carry (or imply) a merklePath;
    // everything else (lifecycle noise, rejects) stays with the existing broadcast
    // verdict + monitor status machinery.
    if ev.status != "MINED" && ev.status != "IMMUTABLE" {
        return Ok(false);
    }

    // Only txs these books are actually waiting on — mirror of check_for_proofs.
    let req: Option<ReqRow> = Query::new(format!(
        "SELECT proven_tx_req_id, hex(raw_tx) as raw_tx FROM proven_tx_reqs \
         WHERE txid = ? AND status IN {PENDING_STATUSES}"
    ))
    .bind(ev.txid.as_str())
    .fetch_optional(db)
    .await
    .map_err(|e| format!("req lookup: {e}"))?;
    let Some(req) = req else {
        return Ok(false); // unknown or already completed — ack + ignore
    };
    let req_id = req
        .proven_tx_req_id
        .map(|v| v as i64)
        .ok_or("req row missing id")?;

    // The merklePath: inline from the payload, else the stored record (bulk fan-outs
    // and terse payloads). A path that doesn't parse or doesn't contain OUR txid is
    // discarded the same way (fetch may still supply a good one).
    let mut candidates: Vec<String> = Vec::new();
    if let Some(h) = &ev.merkle_path_hex {
        candidates.push(h.clone());
    }
    let mut record_block_hash: Option<String> = None;
    if candidates.is_empty() {
        if let Some(record) = crate::services::arcade::fetch_tx_record(arcade_url, &ev.txid).await {
            if let Some(h) = record
                .get("merklePath")
                .and_then(|m| m.as_str())
                .filter(|m| !m.is_empty())
            {
                candidates.push(h.to_string());
            }
            record_block_hash = record
                .get("blockHash")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
        }
    }
    let Some(mp_hex) = candidates.into_iter().next() else {
        return Err("MINED but no merklePath available (payload or record)".to_string());
    };

    let mp_bin = hex::decode(&mp_hex).map_err(|e| format!("merklePath hex: {e}"))?;
    let mp = MerklePath::from_binary(&mp_bin).map_err(|e| format!("BUMP parse: {e:?}"))?;

    // Height: the BUMP encodes it; a payload height that disagrees is an inconsistent
    // event — refuse rather than guess (fail closed; the monitor will re-derive).
    let height = mp.block_height;
    if let Some(payload_h) = ev.block_height {
        if payload_h != height as u64 {
            return Err(format!(
                "payload blockHeight {payload_h} != BUMP height {height} — inconsistent event"
            ));
        }
    }

    // The BUMP must contain OUR txid (compute_root errs otherwise), and its root must be
    // canonical for that height — ChainTracks (WoC fallback) is the authority. NEVER
    // persist an unverified proof, however authenticated the webhook was.
    let root = mp
        .compute_root(Some(&ev.txid))
        .map_err(|e| format!("BUMP root for {}: {e:?}", ev.txid))?;
    let valid = headers
        .is_valid_root_for_height(&root, height)
        .await
        .map_err(|e| format!("root verification unavailable: {e}"))?;
    if !valid {
        return Err(format!(
            "BUMP root {root} is NOT canonical at height {height} — discarding pushed proof"
        ));
    }

    let block_hash = if !ev.block_hash.is_empty() {
        ev.block_hash.clone()
    } else {
        record_block_hash.unwrap_or_default()
    };

    let proof = crate::services::ProofResult {
        txid: ev.txid.clone(),
        merkle_path_binary: mp_bin,
        block_height: height,
        block_hash,
        merkle_root: root,
    };
    crate::monitor::store_proof_result(db, blobs, &ev.txid, req_id, &req.raw_tx, &proof)
        .await
        .map_err(|e| format!("store_proof_result: {e}"))?;
    console_log!(
        "arcade-callback: PROOF persisted for {} (h={}, pushed by Arcade)",
        ev.txid,
        height
    );
    Ok(true)
}

// =============================================================================
// Tests — the unfan + payload shapes (pure; the verify/persist legs ride the
// staging drill and the monitor's own coverage of store_proof_result)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const T1: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const T2: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn unfan_single_mined_event() {
        let body = serde_json::json!({
            "txid": T1, "txStatus": "MINED", "timestamp": "2026-07-13T00:00:00Z",
            "blockHash": "hash", "blockHeight": 900001, "merklePath": "fe012345"
        });
        let evs = unfan(&body);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].txid, T1);
        assert_eq!(evs[0].status, "MINED");
        assert_eq!(evs[0].block_height, Some(900001));
        assert_eq!(evs[0].merkle_path_hex.as_deref(), Some("fe012345"));
    }

    #[test]
    fn unfan_bulk_block_event_shares_fields() {
        // The bump-builder MINED fan-out: ONE event, txids[], shared block fields.
        let body = serde_json::json!({
            "txid": "", "txStatus": "MINED", "txids": [T1, T2],
            "blockHash": "hash", "blockHeight": 900001
        });
        let evs = unfan(&body);
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].txid, T1);
        assert_eq!(evs[1].txid, T2);
        for ev in evs {
            assert_eq!(ev.status, "MINED");
            assert_eq!(ev.block_height, Some(900001));
            assert!(ev.merkle_path_hex.is_none(), "bulk carries no per-tx path");
        }
    }

    #[test]
    fn unfan_rejects_garbage_txids() {
        for bad in ["short", "zz", ""] {
            let body = serde_json::json!({ "txid": bad, "txStatus": "MINED" });
            assert!(unfan(&body).is_empty(), "{bad:?} must not produce an event");
        }
        let body = serde_json::json!({ "txStatus": "MINED", "txids": ["short", T1] });
        let evs = unfan(&body);
        assert_eq!(evs.len(), 1, "garbage bulk entries are dropped, good ones kept");
        assert_eq!(evs[0].txid, T1);
    }

    #[test]
    fn unfan_keeps_non_terminal_statuses_for_the_handler_to_skip() {
        // unfan is shape-only; handle_one is where non-MINED gets ignored — so a
        // SEEN_ON_NETWORK webhook still parses (and is then counted as ignored).
        let body = serde_json::json!({ "txid": T1, "txStatus": "SEEN_ON_NETWORK" });
        let evs = unfan(&body);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].status, "SEEN_ON_NETWORK");
    }
}
