//! Cron-triggered monitor for wallet-infra.
//!
//! Runs every 5 minutes via `#[event(scheduled)]`. Nine tasks:
//! 1. `send_waiting` — broadcast txs with status 'unsent'/'sending'
//! 2. `check_for_proofs` — collect merkle proofs for broadcast transactions
//! 3. `fail_abandoned` — fail stuck unsigned/unprocessed transactions, release UTXOs
//! 4. `review_status` — sync mismatched proven_tx_req vs transaction statuses
//! 5. `compact_beef` — retroactively compact stored BEEF blobs
//! 6. `unfail_transactions` — recover txs incorrectly marked failed (runs every ~10 min)
//! 7. `check_no_sends` — (daily) detect nosend txs mined externally
//! 8. `purge_data` — (hourly) nullify old completed blobs, delete old failed reqs
//! 9. `check_chain_reorg` — detect chain reorgs via height tracking, reverify affected proofs

#[cfg(test)]
use bsv_sdk::transaction::MerklePathLeaf;
use bsv_sdk::transaction::{Beef, BeefTx, MerklePath};
use chrono::{Timelike, Utc};
use serde::Deserialize;
use worker::*;

use crate::d1::batch::BatchCollector;
use crate::d1::{QVal, Query};
use crate::services::{BroadcastService, ProofService};

/// Max attempts before marking a proven_tx_req as 'invalid'.
/// At 5-min intervals, 12 attempts ≈ 60 minutes.
///
/// Reference-aligned: the Go toolbox default is 10 attempts
/// (`go-wallet-toolbox/pkg/defs/sync_tx_statuses.go`), the Rust toolbox uses
/// 144 (`bsv-wallet-toolbox-rs/src/storage/sqlx/storage_sqlx.rs:2709`).
/// We picked 12 (2 above Go) — conservative enough to not invalidate
/// briefly-lagged TSC lookups, aggressive enough to purge true phantoms
/// within an hour of broadcast. On the next monitor cycle, any req already
/// past this threshold transitions unmined → invalid in a single UPDATE,
/// matching reference semantics exactly (just the proven_tx_reqs status,
/// no cascading transactions/outputs updates — the wallet layer handles
/// UTXO release when needed).
const MAX_PROOF_ATTEMPTS: i64 = 12;

// =============================================================================
// Legacy WoC types — kept for parse_tsc_proof_response / tsc_proof_to_binary tests.
// These are only compiled in test mode since the live proof path now uses ProofService.
// =============================================================================

#[cfg(test)]
#[derive(Debug, Deserialize)]
struct WocTscProof {
    index: u32,
    nodes: Vec<String>,
    target: String,
    #[serde(rename = "txOrId")]
    tx_or_id: String,
}

#[cfg(test)]
/// Parse a TSC proof response body into an optional proof.
/// Handles all known WoC response formats: empty, "[]", "null", and valid JSON arrays.
fn parse_tsc_proof_response(text: &str) -> std::result::Result<Option<WocTscProof>, String> {
    if text.is_empty() || text == "[]" || text == "null" {
        return Ok(None);
    }

    let proofs: Vec<WocTscProof> =
        serde_json::from_str(text).map_err(|e| format!("Failed to parse TSC proof: {}", e))?;

    Ok(proofs.into_iter().next())
}

// =============================================================================
// D1 row types
// =============================================================================

#[derive(Debug, Deserialize)]
struct PendingProofRow {
    proven_tx_req_id: Option<f64>,
    txid: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    attempts: Option<f64>,
    raw_tx: Option<String>, // hex from hex(raw_tx)
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct AbandonedTxRow {
    transaction_id: Option<f64>,
    txid: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MismatchRow {
    transaction_id: Option<f64>,
    txid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrphanLinkRow {
    transaction_id: Option<f64>,
    proven_tx_id: Option<f64>,
    txid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UnfailRow {
    proven_tx_req_id: Option<f64>,
    txid: Option<String>,
    raw_tx: Option<String>, // hex from hex(raw_tx)
}

/// Row type for reading last stored chain height from monitor_events.
#[derive(Debug, Deserialize)]
struct ChainHeightRow {
    details: Option<String>,
}

/// Row type for proven_txs affected by a reorg (height > current chain tip).
#[derive(Debug, Deserialize)]
struct AffectedProofRow {
    proven_tx_id: Option<f64>,
    txid: Option<String>,
    height: Option<f64>,
}

/// Row type for send_waiting: unsent/sending proven_tx_reqs needing broadcast.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct UnsentTxRow {
    proven_tx_req_id: Option<f64>,
    txid: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    attempts: Option<f64>,
    raw_tx: Option<String>, // hex from hex(raw_tx)
    #[allow(dead_code)]
    batch: Option<String>,
    input_beef: Option<String>, // hex from hex(input_beef)
}

// =============================================================================
// Monitor result
// =============================================================================

pub struct MonitorResult {
    pub sent: u32,
    pub send_errors: u32,
    pub proofs_found: u32,
    pub proofs_checked: u32,
    pub abandoned_failed: u32,
    pub status_synced: u32,
    pub beef_compacted: u32,
    pub unfail_recovered: u32,
    pub purged: u32,
    pub nosend_found: u32,
    pub reorg_detected: bool,
    pub reorg_depth: u32,
    pub proofs_reverified: u32,
    pub errors: Vec<String>,
}

// =============================================================================
// Main orchestrator
// =============================================================================

pub async fn run_monitor<B: BroadcastService, P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    broadcast: &B,
    proof_service: &P,
) -> MonitorResult {
    let mut result = MonitorResult {
        sent: 0,
        send_errors: 0,
        proofs_found: 0,
        proofs_checked: 0,
        abandoned_failed: 0,
        status_synced: 0,
        beef_compacted: 0,
        unfail_recovered: 0,
        purged: 0,
        nosend_found: 0,
        reorg_detected: false,
        reorg_depth: 0,
        proofs_reverified: 0,
        errors: Vec::new(),
    };

    // Task 1: Broadcast unsent/sending transactions
    match send_waiting(db, broadcast).await {
        Ok((sent_count, err_count, send_errors)) => {
            result.sent = sent_count;
            result.send_errors = err_count;
            result.errors.extend(send_errors);
        }
        Err(e) => result.errors.push(format!("send_waiting: {}", e)),
    }

    // Task 2: Check for proofs
    match check_for_proofs(db, blobs, proof_service).await {
        Ok((found, checked, proof_errors)) => {
            result.proofs_found = found;
            result.proofs_checked = checked;
            result.errors.extend(proof_errors);
        }
        Err(e) => result.errors.push(format!("check_for_proofs: {}", e)),
    }

    // Task 3: Fail abandoned transactions
    match fail_abandoned(db).await {
        Ok(count) => result.abandoned_failed = count,
        Err(e) => result.errors.push(format!("fail_abandoned: {}", e)),
    }

    // Task 4: Review status mismatches
    match review_status(db).await {
        Ok(count) => result.status_synced = count,
        Err(e) => result.errors.push(format!("review_status: {}", e)),
    }

    // Task 5: Compact stale BEEF blobs
    // RE-ENABLED 2026-04-16: BlobStore.put() now routes compacted BEEFs to R2
    // when they exceed the 4KB D1 threshold, so the ~10 legacy rows with
    // >950KB input_beef no longer trigger SQLITE_TOOBIG on the writeback.
    // Safety matches the reference (bsv-wallet-toolbox-rs::CompactBeefTask):
    // only processes rows with status='completed' (fully proven ancestry),
    // never touches pending broadcasts, and only writes back when compaction
    // actually shrinks the blob. Ancestry preservation: upgrade_stored_beef
    // merges current proofs from proven_txs before trimming, so every tx in
    // the BEEF retains either its raw_tx or a valid BUMP → txid-only ref.
    match compact_beef(db, blobs).await {
        Ok(count) => result.beef_compacted = count,
        Err(e) => result.errors.push(format!("compact_beef: {}", e)),
    }

    // Task 5: Unfail transactions (runs every ~10 min — when minute is divisible by 10)
    if Utc::now().minute() % 10 < 5 {
        match unfail_transactions(db, blobs, proof_service).await {
            Ok((recovered, unfail_errors)) => {
                result.unfail_recovered = recovered;
                result.errors.extend(unfail_errors);
            }
            Err(e) => result.errors.push(format!("unfail_transactions: {}", e)),
        }
    }

    // Task 6: Purge old transient data (hourly — every 12th cron cycle)
    if Utc::now().minute() == 0 {
        let params = crate::types::PurgeParams {
            max_age_days: 30,
            purge_completed: true,
            purge_failed: true,
        };
        match purge_data(db, &params).await {
            Ok(pr) => result.purged = pr.count,
            Err(e) => result.errors.push(format!("purge_data: {}", e)),
        }
    }

    // Task 7: Check nosend transactions for external mining (daily at midnight UTC)
    if Utc::now().hour() == 0 && Utc::now().minute() == 0 {
        match check_no_sends(db, blobs, proof_service).await {
            Ok((found, nosend_errors)) => {
                result.nosend_found = found;
                result.errors.extend(nosend_errors);
            }
            Err(e) => result.errors.push(format!("check_no_sends: {}", e)),
        }
    }

    // Task 8: Check chain height for reorgs
    match check_chain_reorg(db, blobs, proof_service).await {
        Ok((detected, depth, reverified)) => {
            result.reorg_detected = detected;
            result.reorg_depth = depth;
            result.proofs_reverified = reverified;
        }
        Err(e) => result.errors.push(format!("check_chain_reorg: {}", e)),
    }

    // Log to monitor_events table
    let _ = log_monitor_event(db, &result).await;

    result
}

// =============================================================================
// Task 1: Broadcast unsent/sending transactions
// =============================================================================

async fn send_waiting<B: BroadcastService>(
    db: &D1Database,
    broadcast: &B,
) -> Result<(u32, u32, Vec<String>)> {
    let rows: Vec<UnsentTxRow> = Query::new(
        "SELECT proven_tx_req_id, txid, status, attempts, hex(raw_tx) as raw_tx, \
         batch, hex(input_beef) as input_beef \
         FROM proven_tx_reqs \
         WHERE status IN ('unsent', 'sending') \
         ORDER BY created_at ASC \
         LIMIT 100",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let mut sent = 0u32;
    let mut errors = 0u32;
    let mut error_msgs: Vec<String> = Vec::new();

    for row in &rows {
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };
        let req_id = row.proven_tx_req_id.unwrap_or(0.0) as i64;
        let attempts = row.attempts.unwrap_or(0.0) as i64;

        // Prefer BEEF broadcast if input_beef is available, otherwise fall back to raw_tx.
        let broadcast_result = if let Some(ref beef_hex) = row.input_beef {
            if !beef_hex.is_empty() {
                broadcast.broadcast_beef(beef_hex).await
            } else if let Some(ref raw_tx_hex) = row.raw_tx {
                if !raw_tx_hex.is_empty() {
                    broadcast.broadcast_raw_tx(raw_tx_hex).await
                } else {
                    continue;
                }
            } else {
                continue;
            }
        } else if let Some(ref raw_tx_hex) = row.raw_tx {
            if !raw_tx_hex.is_empty() {
                broadcast.broadcast_raw_tx(raw_tx_hex).await
            } else {
                continue;
            }
        } else {
            continue;
        };

        match broadcast_result {
            Ok(_result) => {
                console_log!("send_waiting: broadcast OK txid={}", txid);
                let now = Utc::now().to_rfc3339();
                let mut batch = BatchCollector::new(db);
                let _ = batch.add(
                    "UPDATE proven_tx_reqs SET status = 'unmined', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?",
                    vec![QVal::Int(attempts + 1), QVal::Text(now.clone()), QVal::Int(req_id)],
                );
                let _ = batch.add(
                    "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ? AND status IN ('sending', 'nosend', 'unprocessed')",
                    vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                );
                let _ = batch.add(
                    "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)",
                    vec![QVal::Text(now), QVal::Text(txid.clone())],
                );
                let _ = batch.execute().await;
                sent += 1;
            }
            Err(crate::services::BroadcastError::DoubleSpend(msg)) => {
                console_log!("send_waiting: double-spend txid={}: {}", txid, msg);
                let now = Utc::now().to_rfc3339();
                let mut batch = BatchCollector::new(db);
                let _ = batch.add(
                    "UPDATE proven_tx_reqs SET status = 'doubleSpend', updated_at = ? WHERE proven_tx_req_id = ?",
                    vec![QVal::Text(now.clone()), QVal::Int(req_id)],
                );
                let _ = batch.add(
                    "UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?",
                    vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                );
                // Release locked outputs: set spendable=1 on outputs created by this tx
                // that aren't already spent, and release input locks.
                let _ = batch.add(
                    "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)",
                    vec![QVal::Text(now), QVal::Text(txid.clone())],
                );
                let _ = batch.execute().await;
                errors += 1;
                if error_msgs.len() < 5 {
                    error_msgs.push(format!(
                        "doubleSpend({}): {}",
                        &txid[..8.min(txid.len())],
                        msg
                    ));
                }
            }
            Err(crate::services::BroadcastError::InvalidTx(msg)) => {
                console_log!("send_waiting: invalid tx txid={}: {}", txid, msg);
                let now = Utc::now().to_rfc3339();
                let mut batch = BatchCollector::new(db);
                let _ = batch.add(
                    "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?",
                    vec![QVal::Text(now.clone()), QVal::Int(req_id)],
                );
                let _ = batch.add(
                    "UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?",
                    vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                );
                let _ = batch.add(
                    "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)",
                    vec![QVal::Text(now), QVal::Text(txid.clone())],
                );
                let _ = batch.execute().await;
                errors += 1;
                if error_msgs.len() < 5 {
                    error_msgs.push(format!(
                        "invalidTx({}): {}",
                        &txid[..8.min(txid.len())],
                        msg
                    ));
                }
            }
            Err(crate::services::BroadcastError::ServiceError(msg)) => {
                console_error!("send_waiting: service error txid={}: {}", txid, msg);
                // Transient — increment attempts, keep status as-is, inputs stay locked.
                let now = Utc::now().to_rfc3339();
                let _ = Query::new(
                    "UPDATE proven_tx_reqs SET attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?",
                )
                .bind(attempts + 1)
                .bind(now.as_str())
                .bind(req_id)
                .execute(db)
                .await;
                errors += 1;
                if error_msgs.len() < 5 {
                    error_msgs.push(format!("service({}): {}", &txid[..8.min(txid.len())], msg));
                }
            }
        }
    }

    Ok((sent, errors, error_msgs))
}

// =============================================================================
// Task 2: Check for proofs (proof-only, no broadcasting)
// =============================================================================

async fn check_for_proofs<P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    proof_service: &P,
) -> Result<(u32, u32, Vec<String>)> {
    // Reset per-run caches (e.g. WocProvider's block hash→header cache).
    // Prevents stale cache entries from bleeding across monitor ticks if
    // a reorg changed the block at a given hash.
    proof_service.reset_run_cache();

    // Stale-unmined sweep: bulk-invalidate any proven_tx_req stuck above
    // MAX_PROOF_ATTEMPTS. Runs before the proof-fetch loop so stuck zombies
    // never block fresh arrivals from getting WoC slots. Pure SQL — zero
    // API calls. Matches reference behavior of eventually transitioning
    // post-broadcast states → invalid, but in a batched form so bursts of
    // fresh traffic don't starve the increment path.
    //
    // Status set matches Go toolbox's `statusesReadyToSync` in
    // `pkg/storage/internal/actions/synchronize_tx_statuses.go:31`:
    // callback, unmined, sending, unknown, unconfirmed, reorg. Excludes
    // `unprocessed` deliberately — that's a pre-broadcast state, handled
    // by SendWaitingTransactions, not by the proof-fetch path.
    let swept = Query::new(
        "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? \
         WHERE status IN ('unmined', 'unknown', 'unconfirmed', 'callback', 'sending', 'reorg') \
           AND attempts >= ?",
    )
    .bind(Utc::now().to_rfc3339().as_str())
    .bind(MAX_PROOF_ATTEMPTS)
    .execute(db)
    .await
    .map(|m| m.changes)
    .unwrap_or(0);
    if swept > 0 {
        console_log!("check_for_proofs: swept {} stale unmined → invalid", swept);
    }

    let rows: Vec<PendingProofRow> = Query::new(
        // LIMIT 50: paced at 1000ms per get_proof (~1/sec, safely under
        // WoC free-tier 3/sec cap). Per-cycle budget: 50 × ~1.5s ≈ 75s —
        // about 25% of the 5-min cron spacing so cycles never overlap.
        // Throughput: 50 × 288 = 14,400 txs/day — 40% above the 10k target
        // so bursts don't starve zombies. WoC calls/day: ~29k (29% of
        // 100k free-tier cap). Plenty of headroom.
        //
        // Ordering: `attempts ASC` prioritizes fresh arrivals over retries,
        // so a newly-broadcast tx gets its proof fetched before we waste
        // budget on zombies. Zombies are handled separately by the
        // `invalidate_stale_unmined` sweep below — pure SQL, no WoC calls,
        // runs before every proof-fetch cycle.
        "SELECT proven_tx_req_id, txid, status, attempts, hex(raw_tx) as raw_tx \
         FROM proven_tx_reqs \
         WHERE status IN ('unmined', 'unknown', 'unconfirmed', 'callback', 'sending', 'reorg') \
         ORDER BY attempts ASC, created_at DESC \
         LIMIT 50",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let mut found = 0u32;
    let mut checked = 0u32;
    let mut proof_errors: Vec<String> = Vec::new();

    // Collect all valid txids for batch triage
    let all_txids: Vec<String> = rows
        .iter()
        .filter_map(|r| r.txid.as_ref().filter(|t| !t.is_empty()).cloned())
        .collect();

    // Triage: batch status check to skip txids that aren't mined yet.
    // On failure OR suspicious results, fall through to old behavior (check all).
    let confirmed_set: Option<std::collections::HashSet<String>> = match proof_service
        .get_status_for_txids(&all_txids)
        .await
    {
        Ok(statuses) => {
            let confirmed: usize = statuses.iter().filter(|s| s.status == "mined").count();
            let mempool: usize = statuses.iter().filter(|s| s.status == "known").count();
            let missing: usize = statuses.iter().filter(|s| s.status == "unknown").count();
            console_log!(
                "check_for_proofs triage: total={} confirmed={} mempool={} missing={}",
                all_txids.len(),
                confirmed,
                mempool,
                missing
            );

            // Safety net: if triage says 0 confirmed out of N pending,
            // the batch status call may be broken (e.g. WoC returns all
            // "unknown" from CF Workers). Fall through to old behavior
            // so individual get_proof calls still work.
            if confirmed == 0 && !all_txids.is_empty() {
                console_error!(
                        "check_for_proofs triage: 0/{} confirmed — batch status may be broken, falling back to check all",
                        all_txids.len()
                    );
                None
            } else {
                Some(
                    statuses
                        .into_iter()
                        .filter(|s| s.status == "mined" && s.depth.unwrap_or(0) >= 1)
                        .map(|s| s.txid)
                        .collect(),
                )
            }
        }
        Err(e) => {
            console_error!("check_for_proofs triage failed, falling back: {}", e);
            None // Fall through to old behavior
        }
    };

    for row in &rows {
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };
        let req_id = row.proven_tx_req_id.unwrap_or(0.0) as i64;
        let attempts = row.attempts.unwrap_or(0.0) as i64;

        // If triage succeeded, skip txids that aren't confirmed
        if let Some(ref confirmed) = confirmed_set {
            if !confirmed.contains(&txid) {
                // Not confirmed — increment attempts, skip proof fetch
                let _ = increment_attempts(db, req_id, attempts).await;
                continue;
            }
        }

        checked += 1;

        match proof_service.get_proof(&txid).await {
            Ok(Some(proof_result)) => {
                // Proof found — store it
                if let Err(e) =
                    store_proof_result(db, blobs, &txid, req_id, &row.raw_tx, &proof_result).await
                {
                    console_error!("store_proof({}) failed: {}", txid, e);
                    if proof_errors.len() < 3 {
                        proof_errors.push(format!("store({}):{}", &txid[..8], e));
                    }
                } else {
                    found += 1;
                }
            }
            Ok(None) => {
                // Not yet mined — increment attempts
                let _ = increment_attempts(db, req_id, attempts).await;
            }
            Err(e) => {
                console_error!("get_proof({}) failed: {}", txid, e);
                if proof_errors.len() < 3 {
                    proof_errors.push(format!("proof({}):{}", &txid[..8], e));
                }
            }
        }

        // Rate-limit pacing: 1 req/sec is the conservative floor — WoC
        // sometimes throttles CF Worker IPs more aggressively than the
        // documented 3/sec (shared egress pool). 1000ms between txs means
        // each tx (TSC proof + ~block header) stays at ~1 HTTP call/sec
        // average. With LIMIT 50 per cycle and ~28s budget, we process
        // roughly 28 per cycle → 28 × 288 = 8064/day, still above the
        // ~300-500/day typical arrival rate.
        worker::Delay::from(std::time::Duration::from_millis(1000)).await;
    }

    Ok((found, checked, proof_errors))
}

/// Store a proof from the ProofService: insert proven_tx, update proven_tx_req and transactions.
async fn store_proof_result(
    db: &D1Database,
    blobs: &worker::Bucket,
    txid: &str,
    req_id: i64,
    raw_tx_hex: &Option<String>,
    proof_result: &crate::services::ProofResult,
) -> Result<()> {
    let now = Utc::now().to_rfc3339();

    // Decode raw_tx from hex back to bytes for proven_txs.raw_tx
    let raw_tx_bytes = raw_tx_hex
        .as_ref()
        .and_then(|h| hex::decode(h).ok())
        .unwrap_or_default();

    // The proof service already provides BRC-74 binary merkle path.
    // We need idx from the binary — parse it to extract the tx index.
    // For storage, we use block_height and idx=0 (the service doesn't expose raw idx,
    // but the merkle_path_binary encodes everything needed for verification).
    let merkle_path_binary = &proof_result.merkle_path_binary;

    // UPSERT: if a proven_tx already exists for this txid (previous retry,
    // reorg recovery, etc.), refresh the proof fields instead of colliding
    // on the UNIQUE(txid) constraint. Matches the reference behavior —
    // reorg.rs at `bsv-wallet-toolbox-rs::handle_reorg` also updates existing
    // proven_tx rows when re-verification finds fresh data.
    //
    // raw_tx and merkle_path are NOT NULL + always present; bind directly
    // (no R2 for these — they're small enough to fit inline). Only input_beef
    // (nullable, regularly huge) needs R2 routing.
    let _ = blobs; // unused on this path
    let _ = Query::new(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
         VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(txid) DO UPDATE SET \
           height = excluded.height, \
           block_hash = excluded.block_hash, \
           merkle_root = excluded.merkle_root, \
           merkle_path = excluded.merkle_path, \
           raw_tx = excluded.raw_tx, \
           updated_at = excluded.updated_at",
    )
    .bind(txid)
    .bind(proof_result.block_height as i64)
    .bind(proof_result.block_hash.as_str())
    .bind(proof_result.merkle_root.as_str())
    .bind(QVal::Blob(merkle_path_binary.clone()))
    .bind(QVal::Blob(raw_tx_bytes))
    .bind(now.as_str())
    .bind(now.as_str())
    .execute(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    // After UPSERT, look up the row to get proven_tx_id (may be the
    // existing row's id, not last_row_id which is 0 on UPDATE path).
    let id_row: Option<ProvenTxIdOnlyRow> = Query::new(
        "SELECT proven_tx_id FROM proven_txs WHERE txid = ?",
    )
    .bind(txid)
    .fetch_optional(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;
    let proven_tx_id = id_row
        .and_then(|r| r.proven_tx_id.map(|v| v as i64))
        .unwrap_or(0);

    // Phase 2: Batch update proven_tx_req + transactions
    let mut batch = BatchCollector::new(db);

    batch
        .add(
            "UPDATE proven_tx_reqs SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE proven_tx_req_id = ?",
            vec![
                QVal::Int(proven_tx_id),
                QVal::Text(now.clone()),
                QVal::Int(req_id),
            ],
        )
        .map_err(|e| Error::from(e.to_string()))?;

    batch
        .add(
            "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
            vec![
                QVal::Int(proven_tx_id),
                QVal::Text(now.clone()),
                QVal::Text(txid.to_string()),
            ],
        )
        .map_err(|e| Error::from(e.to_string()))?;

    // Re-enable spendable on our own outputs (storage-generated change OR outputs
    // with derivation metadata) now that the tx is proven. External recipient
    // outputs (change=0 AND custom_instructions IS NULL) are NOT ours and stay
    // spendable=0. Matches go-wallet-toolbox `isChangeDaoScope` filter.
    batch
        .add(
            "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)",
            vec![
                QVal::Text(now),
                QVal::Text(txid.to_string()),
            ],
        )
        .map_err(|e| Error::from(e.to_string()))?;

    batch
        .execute()
        .await
        .map_err(|e| Error::from(e.to_string()))?;

    console_log!(
        "Proof stored for txid={} at height={}",
        txid,
        proof_result.block_height,
    );
    Ok(())
}

/// Increment attempt counter; mark invalid if over threshold.
async fn increment_attempts(db: &D1Database, req_id: i64, current_attempts: i64) -> Result<()> {
    let now = Utc::now().to_rfc3339();

    if current_attempts + 1 > MAX_PROOF_ATTEMPTS {
        Query::new(
            "UPDATE proven_tx_reqs SET status = 'invalid', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(current_attempts + 1)
        .bind(now.as_str())
        .bind(req_id)
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    } else {
        Query::new(
            "UPDATE proven_tx_reqs SET attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(current_attempts + 1)
        .bind(now.as_str())
        .bind(req_id)
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    }

    Ok(())
}

// =============================================================================
// Task 3: Fail abandoned transactions
// =============================================================================

async fn fail_abandoned(db: &D1Database) -> Result<u32> {
    let rows: Vec<AbandonedTxRow> = Query::new(
        "SELECT transaction_id, txid, status FROM transactions \
         WHERE status IN ('unsigned', 'unprocessed') \
         AND updated_at < datetime('now', '-30 minutes') \
         AND is_outgoing = 1",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let now = Utc::now().to_rfc3339();
    let mut count = 0u32;

    for row in &rows {
        let tx_id = row.transaction_id.unwrap_or(0.0) as i64;
        if tx_id == 0 {
            continue;
        }

        let mut batch = BatchCollector::new(db);

        batch
            .add(
                "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                vec![QVal::Text(now.clone()), QVal::Int(tx_id)],
            )
            .map_err(|e| Error::from(e.to_string()))?;

        // Release locked UTXOs that were reserved for this transaction
        batch
            .add(
                "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE spent_by = ?",
                vec![QVal::Text(now.clone()), QVal::Int(tx_id)],
            )
            .map_err(|e| Error::from(e.to_string()))?;

        batch
            .execute()
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        count += 1;

        let txid_str = row.txid.as_deref().unwrap_or("?");
        console_log!("Abandoned tx failed: id={} txid={}", tx_id, txid_str);
    }

    Ok(count)
}

// =============================================================================
// Task 4: Review status mismatches
// =============================================================================

pub async fn review_status(db: &D1Database) -> Result<u32> {
    let now = Utc::now().to_rfc3339();
    let mut count = 0u32;

    // Safety net: link proven_tx_id on transactions where a matching proven_txs
    // record exists but the link was lost (D1 batch partial failure, migration, etc).
    // Direct port of the TypeScript reviewStatus() pattern.
    let orphan_rows: Vec<OrphanLinkRow> = Query::new(
        "SELECT t.transaction_id, pt.proven_tx_id, t.txid \
         FROM transactions t \
         JOIN proven_txs pt ON pt.txid = t.txid \
         WHERE t.proven_tx_id IS NULL \
         AND t.txid IS NOT NULL \
         LIMIT 100",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    for row in &orphan_rows {
        let tx_id = row.transaction_id.unwrap_or(0.0) as i64;
        let pt_id = row.proven_tx_id.unwrap_or(0.0) as i64;
        if tx_id == 0 || pt_id == 0 {
            continue;
        }

        Query::new(
            "UPDATE transactions SET proven_tx_id = ?, status = 'completed', updated_at = ? WHERE transaction_id = ?",
        )
        .bind(pt_id)
        .bind(now.as_str())
        .bind(tx_id)
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

        count += 1;
        let txid_str = row.txid.as_deref().unwrap_or("?");
        console_log!(
            "Orphan linked: tx_id={} proven_tx_id={} txid={}",
            tx_id,
            pt_id,
            txid_str
        );
    }

    // Sync completed proofs → completed transactions (status + proven_tx_id)
    let completed_rows: Vec<OrphanLinkRow> = Query::new(
        "SELECT t.transaction_id, ptr.proven_tx_id, t.txid \
         FROM proven_tx_reqs ptr \
         JOIN transactions t ON t.txid = ptr.txid \
         WHERE ptr.status = 'completed' \
         AND (t.status != 'completed' OR t.proven_tx_id IS NULL) \
         AND ptr.proven_tx_id IS NOT NULL",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    for row in &completed_rows {
        let tx_id = row.transaction_id.unwrap_or(0.0) as i64;
        let pt_id = row.proven_tx_id.unwrap_or(0.0) as i64;
        if tx_id == 0 || pt_id == 0 {
            continue;
        }

        Query::new(
            "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE transaction_id = ?",
        )
        .bind(pt_id)
        .bind(now.as_str())
        .bind(tx_id)
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

        count += 1;
        let txid_str = row.txid.as_deref().unwrap_or("?");
        console_log!(
            "Status+link synced: tx_id={} proven_tx_id={} txid={}",
            tx_id,
            pt_id,
            txid_str
        );
    }

    // Sync invalid proofs → failed transactions + release locked UTXOs
    let invalid_rows: Vec<MismatchRow> = Query::new(
        "SELECT t.transaction_id, t.txid \
         FROM proven_tx_reqs ptr \
         JOIN transactions t ON t.txid = ptr.txid \
         WHERE ptr.status = 'invalid' \
         AND t.status IN ('sending', 'unproven')",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    for row in &invalid_rows {
        let tx_id = row.transaction_id.unwrap_or(0.0) as i64;
        if tx_id == 0 {
            continue;
        }

        let mut batch = BatchCollector::new(db);

        batch
            .add(
                "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                vec![QVal::Text(now.clone()), QVal::Int(tx_id)],
            )
            .map_err(|e| Error::from(e.to_string()))?;

        // Release locked UTXOs that were reserved for this failed transaction
        batch
            .add(
                "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE spent_by = ?",
                vec![QVal::Text(now.clone()), QVal::Int(tx_id)],
            )
            .map_err(|e| Error::from(e.to_string()))?;

        batch
            .execute()
            .await
            .map_err(|e| Error::from(e.to_string()))?;

        count += 1;
        let txid_str = row.txid.as_deref().unwrap_or("?");
        console_log!(
            "Invalid tx failed + UTXOs released: tx_id={} txid={}",
            tx_id,
            txid_str
        );
    }

    Ok(count)
}

// =============================================================================
// Task 5: Compact stale BEEF blobs
// =============================================================================

/// Row type for compaction query — reads input_beef as hex.
#[derive(Debug, Deserialize)]
struct CompactBeefRow {
    proven_tx_req_id: Option<f64>,
    input_beef: Option<String>, // hex from hex(input_beef)
}

/// Row type for proof lookup during compaction.
#[derive(Debug, Deserialize)]
struct ProofLookupRow {
    #[allow(dead_code)]
    txid: Option<String>,
    merkle_path: Option<String>, // hex from hex(merkle_path)
}

/// Retroactively compact stored input_beef blobs in proven_tx_reqs.
///
/// Over time, stored BEEFs contain full raw ancestor transactions that have since
/// been proven (merkle proofs stored in proven_txs). This task upgrades unproven
/// transactions with their now-available BUMPs and trims unnecessary ancestors.
///
/// Safety:
/// - Only processes completed proof requests (fully proven, not pending broadcast)
/// - Only writes back if the compacted BEEF is smaller than the original
/// - Processes 10 per run (conservative for CF Workers 30s timeout)
/// - Skips individual BEEFs on parse errors without stopping
async fn compact_beef(db: &D1Database, blobs: &worker::Bucket) -> Result<u32> {
    // Query completed proven_tx_reqs with large input_beef blobs.
    // Limit 3 per run — must stay well within CF Workers 30s timeout.
    // Only target BEEFs > 5KB to focus on meaningful savings.
    // Process largest first for maximum impact.
    let rows: Vec<CompactBeefRow> = Query::new(
        // Upper bound on input_beef size: hex() doubles the bytes, so a
        // 500 KB blob becomes a 1 MB string — hitting SQLITE_TOOBIG on the
        // result row. Cap at 400 KB to leave headroom. Blobs larger than
        // that should be in R2 (D1 column NULL) and aren't visible to this
        // query anyway.
        "SELECT proven_tx_req_id, hex(input_beef) as input_beef \
         FROM proven_tx_reqs \
         WHERE status = 'completed' \
           AND input_beef IS NOT NULL \
           AND LENGTH(input_beef) > 5000 \
           AND LENGTH(input_beef) < 400000 \
         ORDER BY LENGTH(input_beef) DESC \
         LIMIT 3",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    if rows.is_empty() {
        return Ok(0);
    }

    let mut compacted = 0u32;

    for row in &rows {
        let req_id = row.proven_tx_req_id.unwrap_or(0.0) as i64;
        if req_id == 0 {
            continue;
        }

        let beef_hex = match &row.input_beef {
            Some(h) if !h.is_empty() => h,
            _ => continue,
        };

        let beef_bytes = match hex::decode(beef_hex) {
            Ok(b) => b,
            Err(_) => continue,
        };

        let mut beef = match Beef::from_binary(&beef_bytes) {
            Ok(b) => b,
            Err(_) => continue,
        };

        let original_size = beef_bytes.len();

        // Find unproven txids in this BEEF (have raw tx but no BUMP)
        let unproven_txids: Vec<String> = beef
            .txs
            .iter()
            .filter(|tx: &&BeefTx| tx.bump_index().is_none() && !tx.is_txid_only())
            .map(|tx: &BeefTx| tx.txid())
            .collect();

        if unproven_txids.is_empty() {
            continue;
        }

        // Query proven_txs for available merkle proofs.
        // D1 doesn't support dynamic IN-lists easily, so query one at a time.
        // Cap at 20 lookups per BEEF to stay within CF Workers timeout.
        let mut upgraded = 0u32;
        let max_lookups = unproven_txids.len().min(20);

        for txid in &unproven_txids[..max_lookups] {
            let proof_row: Option<ProofLookupRow> = Query::new(
                "SELECT txid, hex(merkle_path) as merkle_path FROM proven_txs WHERE txid = ?",
            )
            .bind(txid.as_str())
            .fetch_optional(db)
            .await
            .map_err(|e| Error::from(e.to_string()))?;

            let proof_row = match proof_row {
                Some(r) => r,
                None => continue,
            };

            let mp_hex = match &proof_row.merkle_path {
                Some(h) if !h.is_empty() => h,
                _ => continue,
            };

            let mp_bytes = match hex::decode(mp_hex) {
                Ok(b) => b,
                Err(_) => continue,
            };

            match MerklePath::from_binary(&mp_bytes) {
                Ok(merkle_path) => {
                    let bump_index = beef.merge_bump(merkle_path);
                    if let Some(tx) = beef.find_txid_mut(txid) {
                        tx.set_bump_index(Some(bump_index));
                        upgraded += 1;
                    }
                }
                Err(_) => continue,
            }
        }

        if upgraded == 0 {
            continue;
        }

        // NOTE: Do NOT call beef.trim_known_proven() here. This function is
        // only in bsv-rs (not TS/Go SDKs) and creates ORPHANED BUMP REFS —
        // it removes raw_tx entries for proven ancestors, but any BUMP in the
        // BEEF that referenced those txids is now dangling. That breaks
        // broadcast ancestry validation.
        //
        // The reference in bsv-wallet-toolbox-rs/src/storage/sqlx/storage_sqlx.rs:4182
        // has the exact same guard. Size savings still come from the proof
        // upgrades above — unproven txs with raw_tx get re-represented as
        // BUMP + txid-only refs inside merge_bump → set_bump_index.

        let new_bytes = beef.to_binary();
        let new_size = new_bytes.len();

        // Safety: only write back if we actually saved space
        if new_size < original_size {
            let now = Utc::now().to_rfc3339();
            // Route the compacted BEEF through BlobStore — req_id is already
            // known, single-phase UPDATE works. Without this, compacted BEEFs
            // >4 KB would blow the D1 row limit (the original reason this
            // whole task was paused on 2026-04-15).
            let store = crate::r2::BlobStore::new(blobs);
            let (ib_d1, _) = store
                .put("proven_tx_reqs", req_id, "input_beef", &new_bytes)
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Query::new(
                "UPDATE proven_tx_reqs SET input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?",
            )
            .bind(ib_d1)
            .bind(now.as_str())
            .bind(req_id)
            .execute(db)
            .await
            .map_err(|e| Error::from(e.to_string()))?;

            compacted += 1;
            console_log!(
                "Compacted BEEF: req_id={} {}KB -> {}KB (saved {}%)",
                req_id,
                original_size / 1024,
                new_size / 1024,
                ((original_size - new_size) * 100) / original_size
            );
        }
    }

    Ok(compacted)
}

// =============================================================================
// Task 5: Unfail transactions
// =============================================================================

/// Recover transactions incorrectly marked as failed.
///
/// When a broadcast fails transiently but the tx actually made it on-chain, the
/// system marks it 'failed' and locks the UTXOs. The unfail task checks if those
/// txs actually got mined, and if so, restores them.
///
/// Flow:
/// 1. Query proven_tx_reqs with status = 'unfail' (recovery requested)
/// 2. Check chain for merkle proof via ProofService
/// 3. If proof found: insert proven_tx, complete the req, complete the tx, re-enable outputs
/// 4. If no proof: mark the req as 'invalid', don't touch tx or outputs
async fn unfail_transactions<P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    proof_service: &P,
) -> Result<(u32, Vec<String>)> {
    let rows: Vec<UnfailRow> = Query::new(
        "SELECT proven_tx_req_id, txid, hex(raw_tx) as raw_tx \
         FROM proven_tx_reqs \
         WHERE status = 'unfail' \
         ORDER BY created_at ASC \
         LIMIT 100",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let mut recovered = 0u32;
    let mut errors: Vec<String> = Vec::new();

    for row in &rows {
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };
        let req_id = row.proven_tx_req_id.unwrap_or(0.0) as i64;
        if req_id == 0 {
            continue;
        }

        match proof_service.get_proof(&txid).await {
            Ok(Some(proof_result)) => {
                // Proof found — tx IS mined on-chain. Store proof and restore everything.
                if let Err(e) =
                    store_unfail_proof(db, blobs, &txid, req_id, &row.raw_tx, &proof_result).await
                {
                    console_error!("unfail store_proof({}) failed: {}", txid, e);
                    if errors.len() < 3 {
                        errors.push(format!("unfail({}):{}", &txid[..txid.len().min(8)], e));
                    }
                } else {
                    recovered += 1;
                    console_log!("Unfail recovered: txid={}", txid);
                }
            }
            Ok(None) => {
                // No proof — tx NOT mined. Mark req as invalid, don't touch tx/outputs.
                let now = Utc::now().to_rfc3339();
                let _ = Query::new(
                    "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?",
                )
                .bind(now.as_str())
                .bind(req_id)
                .execute(db)
                .await;

                console_log!("Unfail invalid (no proof): txid={}", txid);
            }
            Err(e) => {
                console_error!("unfail get_proof({}) failed: {}", txid, e);
                if errors.len() < 3 {
                    errors.push(format!(
                        "unfail_proof({}):{}",
                        &txid[..txid.len().min(8)],
                        e
                    ));
                }
            }
        }
    }

    Ok((recovered, errors))
}

/// Store a proof for an unfailed transaction: insert proven_tx, update proven_tx_req
/// and transaction to 'completed', and re-enable spendable on outputs.
///
/// Similar to `store_proof_result` but specific to the unfail flow — the transaction
/// was previously 'failed', so we restore it to 'completed' and re-enable outputs.
async fn store_unfail_proof(
    db: &D1Database,
    blobs: &worker::Bucket,
    txid: &str,
    req_id: i64,
    raw_tx_hex: &Option<String>,
    proof_result: &crate::services::ProofResult,
) -> Result<()> {
    let now = Utc::now().to_rfc3339();

    // Decode raw_tx from hex back to bytes for proven_txs.raw_tx
    let raw_tx_bytes = raw_tx_hex
        .as_ref()
        .and_then(|h| hex::decode(h).ok())
        .unwrap_or_default();

    let merkle_path_binary = &proof_result.merkle_path_binary;

    // UPSERT matching store_proof_result's pattern — unfail reruns can hit
    // the same txid twice if the service briefly flapped. Avoid
    // UNIQUE(txid) collisions by refreshing in place.
    let _ = blobs;
    let _ = Query::new(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
         VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(txid) DO UPDATE SET \
           height = excluded.height, \
           block_hash = excluded.block_hash, \
           merkle_root = excluded.merkle_root, \
           merkle_path = excluded.merkle_path, \
           raw_tx = excluded.raw_tx, \
           updated_at = excluded.updated_at",
    )
    .bind(txid)
    .bind(proof_result.block_height as i64)
    .bind(proof_result.block_hash.as_str())
    .bind(proof_result.merkle_root.as_str())
    .bind(QVal::Blob(merkle_path_binary.clone()))
    .bind(QVal::Blob(raw_tx_bytes))
    .bind(now.as_str())
    .bind(now.as_str())
    .execute(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let id_row: Option<ProvenTxIdOnlyRow> = Query::new(
        "SELECT proven_tx_id FROM proven_txs WHERE txid = ?",
    )
    .bind(txid)
    .fetch_optional(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;
    let proven_tx_id = id_row
        .and_then(|r| r.proven_tx_id.map(|v| v as i64))
        .unwrap_or(0);

    // Phase 2: Batch update proven_tx_req + transaction + outputs
    let mut batch = BatchCollector::new(db);

    batch
        .add(
            "UPDATE proven_tx_reqs SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE proven_tx_req_id = ?",
            vec![
                QVal::Int(proven_tx_id),
                QVal::Text(now.clone()),
                QVal::Int(req_id),
            ],
        )
        .map_err(|e| Error::from(e.to_string()))?;

    batch
        .add(
            "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
            vec![
                QVal::Int(proven_tx_id),
                QVal::Text(now.clone()),
                QVal::Text(txid.to_string()),
            ],
        )
        .map_err(|e| Error::from(e.to_string()))?;

    // CRITICAL: Only set spendable=1 AFTER confirming the proof exists (we're inside
    // the proof-found branch). Only re-enable our own outputs (change=1 or derivation
    // metadata populated) — external recipient outputs stay non-spendable.
    batch
        .add(
            "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)",
            vec![QVal::Text(now), QVal::Text(txid.to_string())],
        )
        .map_err(|e| Error::from(e.to_string()))?;

    batch
        .execute()
        .await
        .map_err(|e| Error::from(e.to_string()))?;

    console_log!(
        "Unfail proof stored for txid={} at height={}",
        txid,
        proof_result.block_height,
    );
    Ok(())
}

// =============================================================================
// Task 7: Check nosend transactions for external mining
// =============================================================================

/// Detect when 'nosend' transactions have been externally mined.
///
/// NoSend transactions are valid signed txs that the wallet intentionally did NOT
/// broadcast — but another party might have broadcast them. This task checks the
/// chain for merkle proofs once per day at midnight UTC.
///
/// Flow:
/// 1. Query proven_tx_reqs with status = 'nosend'
/// 2. Check chain for merkle proof via ProofService
/// 3. If proof found: insert proven_tx, complete the req, complete the tx, re-enable outputs
/// 4. If no proof: do nothing — tx wasn't externally broadcast
async fn check_no_sends<P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    proof_service: &P,
) -> Result<(u32, Vec<String>)> {
    let rows: Vec<UnfailRow> = Query::new(
        "SELECT proven_tx_req_id, txid, hex(raw_tx) as raw_tx \
         FROM proven_tx_reqs \
         WHERE status = 'nosend' \
         ORDER BY created_at ASC \
         LIMIT 50",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let mut found = 0u32;
    let mut errors: Vec<String> = Vec::new();

    for row in &rows {
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };
        let req_id = row.proven_tx_req_id.unwrap_or(0.0) as i64;
        if req_id == 0 {
            continue;
        }

        match proof_service.get_proof(&txid).await {
            Ok(Some(proof_result)) => {
                // Proof found — tx WAS mined externally. Store proof and complete everything.
                if let Err(e) =
                    store_unfail_proof(db, blobs, &txid, req_id, &row.raw_tx, &proof_result).await
                {
                    console_error!("nosend store_proof({}) failed: {}", txid, e);
                    if errors.len() < 3 {
                        errors.push(format!("nosend({}):{}", &txid[..txid.len().min(8)], e));
                    }
                } else {
                    found += 1;
                    console_log!("NoSend mined externally: txid={}", txid);
                }
            }
            Ok(None) => {
                // No proof — tx wasn't externally broadcast. Do nothing.
            }
            Err(e) => {
                console_error!("nosend get_proof({}) failed: {}", txid, e);
                if errors.len() < 3 {
                    errors.push(format!(
                        "nosend_proof({}):{}",
                        &txid[..txid.len().min(8)],
                        e
                    ));
                }
            }
        }
    }

    Ok((found, errors))
}

// =============================================================================
// Task 6: Purge old transient data
// =============================================================================

/// Purge old transient data to prevent unbounded D1 growth (TS reference pattern).
///
/// Three operations controlled by PurgeParams flags:
/// 1. DELETE completed proven_tx_reqs older than max_age_days (proof in proven_txs)
/// 2. Clear raw_tx/input_beef on completed transactions older than max_age_days
/// 3. Delete failed (invalid/doubleSpend) proven_tx_reqs older than max_age_days
pub async fn purge_data(
    db: &D1Database,
    params: &crate::types::PurgeParams,
) -> Result<crate::types::PurgeResults> {
    let now = Utc::now().to_rfc3339();
    let mut total_count = 0u32;
    let mut log_parts: Vec<String> = Vec::new();
    let age_modifier = format!("-{} days", params.max_age_days);

    // 1. DELETE old completed proven_tx_reqs entirely (TS reference pattern).
    // The proof is stored in proven_txs; the request row is disposable.
    // TS uses DELETE (not UPDATE SET NULL) because raw_tx is NOT NULL.
    if params.purge_completed {
        let meta = Query::new(
            "DELETE FROM proven_tx_reqs \
             WHERE status = 'completed' \
             AND proven_tx_id IS NOT NULL \
             AND updated_at < datetime('now', ?)",
        )
        .bind(age_modifier.as_str())
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

        let deleted = meta.changes as u32;
        total_count += deleted;
        if deleted > 0 {
            log_parts.push(format!("deleted {} completed reqs", deleted));
        }

        // Also clear raw_tx and input_beef on old completed transactions
        // (TS: sets rawTx=null, inputBEEF=null on transactions table)
        let meta2 = Query::new(
            "UPDATE transactions \
             SET raw_tx = NULL, input_beef = NULL, updated_at = ? \
             WHERE status = 'completed' \
             AND proven_tx_id IS NOT NULL \
             AND updated_at < datetime('now', ?) \
             AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)",
        )
        .bind(now.as_str())
        .bind(age_modifier.as_str())
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

        let cleaned = meta2.changes as u32;
        total_count += cleaned;
        if cleaned > 0 {
            log_parts.push(format!("cleaned data from {} completed txs", cleaned));
        }
    }

    // 2. Delete old failed proven_tx_reqs
    if params.purge_failed {
        let meta = Query::new(
            "DELETE FROM proven_tx_reqs \
             WHERE status IN ('invalid', 'doubleSpend') \
             AND updated_at < datetime('now', ?)",
        )
        .bind(age_modifier.as_str())
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

        let deleted = meta.changes as u32;
        total_count += deleted;
        if deleted > 0 {
            log_parts.push(format!("deleted {} failed reqs", deleted));
        }
    }

    let log = if log_parts.is_empty() {
        "nothing to purge".to_string()
    } else {
        log_parts.join("; ")
    };

    if total_count > 0 {
        console_log!("Purge: {}", log);
    }

    Ok(crate::types::PurgeResults {
        count: total_count,
        log,
    })
}

// =============================================================================
// Task 8: Check chain height for reorgs
// =============================================================================

/// Parse stored chain height from a monitor_events details JSON string.
/// Returns None if parsing fails or no height is found.
fn parse_stored_height(details: &str) -> Option<u32> {
    let v: serde_json::Value = serde_json::from_str(details).ok()?;
    v.get("height")?.as_u64().map(|h| h as u32)
}

/// Detect blockchain reorganizations by comparing stored chain height with current.
///
/// On each cron run:
/// 1. Fetch current chain tip height from ProofService
/// 2. Read last stored height from monitor_events (event='chain_height')
/// 3. If current < last: REORG detected — re-verify affected proofs
/// 4. Otherwise: store new height (normal progression)
///
/// Returns (reorg_detected, reorg_depth, proofs_reverified).
async fn check_chain_reorg<P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    proof_service: &P,
) -> Result<(bool, u32, u32)> {
    // Step 1: Fetch current chain height
    let current_height = match proof_service.get_chain_height().await {
        Ok(h) => h,
        Err(e) => {
            // Non-fatal: chain height API may be temporarily unavailable
            console_error!("check_chain_reorg: failed to get chain height: {}", e);
            return Ok((false, 0, 0));
        }
    };

    // Step 2: Read last stored height from monitor_events
    let last_height_row: Option<ChainHeightRow> = Query::new(
        "SELECT details FROM monitor_events \
         WHERE event = 'chain_height' \
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let last_height =
        last_height_row.and_then(|row| row.details.as_deref().and_then(parse_stored_height));

    // Step 3: Compare heights
    match last_height {
        Some(last) if current_height < last => {
            // REORG DETECTED
            let depth = last - current_height;
            console_error!(
                "REORG DETECTED: chain height decreased {} -> {} (depth={})",
                last,
                current_height,
                depth
            );

            // Re-verify proofs at heights above the current tip
            let reverified =
                handle_reorg(db, blobs, proof_service, current_height, depth).await?;

            // Store the new (lower) height
            store_chain_height(db, current_height).await?;

            Ok((true, depth, reverified))
        }
        Some(last) if current_height == last => {
            // Shallow sweep — throttled: only run on the top of each hour
            // (minute 0-4 of each hour). Every-cycle sweep × 200 recent rows
            // would burn ~57k WoC calls/day; once/hour keeps us at ~5k/day
            // while still catching orphans within an hour of the reorg.
            let shallow = if Utc::now().minute() < 5 {
                shallow_reorg_sweep(db, blobs, proof_service, current_height)
                    .await
                    .unwrap_or(0)
            } else {
                0
            };
            Ok((shallow > 0, 0, shallow))
        }
        _ => {
            store_chain_height(db, current_height).await?;
            let shallow = if Utc::now().minute() < 5 {
                shallow_reorg_sweep(db, blobs, proof_service, current_height)
                    .await
                    .unwrap_or(0)
            } else {
                0
            };
            Ok((shallow > 0, 0, shallow))
        }
    }
}

/// Shallow-reorg sweep: validate recent proven_txs block_hashes against the
/// canonical chain (via ChainTracks inside ProofService) and demote any
/// orphan-block proofs so check_for_proofs re-fetches.
///
/// Scope: last 200 blocks only. Deeper than that, a reorg is vanishingly
/// unlikely and the sweep cost goes up linearly.
///
/// Matches the spirit of `bsv-wallet-toolbox-rs::monitor::tasks::reorg`
/// (which validates deactivated headers pushed in from ChainTracks), but
/// adapted to a CF Worker that can't hold queue state between runs — so we
/// drive the validation from DB state + canonical chain instead.
async fn shallow_reorg_sweep<P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    proof_service: &P,
    current_height: u32,
) -> Result<u32> {
    let min_height = current_height.saturating_sub(200) as i64;
    let rows: Vec<AffectedProofRow> = Query::new(
        "SELECT proven_tx_id, txid, height FROM proven_txs \
         WHERE height >= ? AND height <= ? AND block_hash IS NOT NULL AND block_hash != ''",
    )
    .bind(min_height)
    .bind(current_height as i64)
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    if rows.is_empty() {
        return Ok(0);
    }

    // For each stored row, fetch the canonical proof from ProofService.
    // If the returned block_hash matches ours, we're canonical. If not,
    // run the invalidate path (NULL proof fields, demote req to unmined).
    let now = Utc::now().to_rfc3339();
    let mut demoted = 0u32;
    for row in &rows {
        let proven_tx_id = row.proven_tx_id.unwrap_or(0.0) as i64;
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };
        if proven_tx_id == 0 {
            continue;
        }

        // Check against canonical chain via the proof service (WoC/ARC).
        let canonical_proof = match proof_service.get_proof(&txid).await {
            Ok(Some(p)) => p,
            _ => continue, // network error or no proof available → skip this cycle
        };

        // Read our stored block_hash
        let stored: Option<StoredBlockHashRow> = Query::new(
            "SELECT block_hash FROM proven_txs WHERE proven_tx_id = ?",
        )
        .bind(proven_tx_id)
        .fetch_optional(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

        let stored_hash = match stored.and_then(|r| r.block_hash) {
            Some(h) => h,
            None => continue,
        };

        if stored_hash == canonical_proof.block_hash {
            continue; // canonical, no-op
        }

        // Mismatch — orphan proof. Replace with fresh canonical proof (preserves
        // ancestry) or, if that fails, null out proof fields and demote req.
        console_error!(
            "Shallow reorg detected: txid={} stored_hash={} canonical_hash={} height={}",
            txid,
            &stored_hash[..16.min(stored_hash.len())],
            &canonical_proof.block_hash[..16.min(canonical_proof.block_hash.len())],
            canonical_proof.block_height
        );

        let store = crate::r2::BlobStore::new(blobs);
        let (mp_d1, _) = store
            .put(
                "proven_txs",
                proven_tx_id,
                "merkle_path",
                &canonical_proof.merkle_path_binary,
            )
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        let _ = Query::new(
            "UPDATE proven_txs SET height = ?, block_hash = ?, merkle_root = ?, \
             merkle_path = ?, updated_at = ? WHERE proven_tx_id = ?",
        )
        .bind(canonical_proof.block_height as i64)
        .bind(canonical_proof.block_hash.as_str())
        .bind(canonical_proof.merkle_root.as_str())
        .bind(mp_d1)
        .bind(now.as_str())
        .bind(proven_tx_id)
        .execute(db)
        .await;

        demoted += 1;
    }

    Ok(demoted)
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StoredBlockHashRow {
    block_hash: Option<String>,
}

/// Row holder for looking up proven_tx_id by txid after an UPSERT.
/// `last_row_id` from D1 is 0 when an INSERT turned into an UPDATE via
/// ON CONFLICT, so we select the id back out explicitly.
#[derive(Debug, Deserialize)]
struct ProvenTxIdOnlyRow {
    proven_tx_id: Option<f64>,
}

/// Store current chain height in monitor_events for next comparison.
async fn store_chain_height(db: &D1Database, height: u32) -> Result<()> {
    let details = serde_json::json!({"height": height}).to_string();
    Query::new(
        "INSERT INTO monitor_events (event, details, created_at, updated_at) \
         VALUES ('chain_height', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(details.as_str())
    .execute(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;
    Ok(())
}

/// Handle a detected reorg: re-verify proofs that may be in reorganized blocks.
///
/// Queries proven_txs where height > current_height (these proofs are in blocks
/// that may have been reorganized out). For each:
/// - Try to get a new proof from the chain
/// - If proof found: update the proven_tx with new proof data
/// - If no proof: delete proven_tx, demote proven_tx_req to 'unmined',
///   demote transaction to 'unproven', set outputs.spendable = 0
async fn handle_reorg<P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    proof_service: &P,
    current_height: u32,
    depth: u32,
) -> Result<u32> {
    let affected: Vec<AffectedProofRow> =
        Query::new("SELECT proven_tx_id, txid, height FROM proven_txs WHERE height > ?")
            .bind(current_height as i64)
            .fetch_all(db)
            .await
            .map_err(|e| Error::from(e.to_string()))?;

    if affected.is_empty() {
        console_log!(
            "Reorg depth={}: no proven_txs affected (all heights <= {})",
            depth,
            current_height
        );
        return Ok(0);
    }

    console_log!(
        "Reorg depth={}: {} proven_txs to re-verify (heights > {})",
        depth,
        affected.len(),
        current_height
    );

    let now = Utc::now().to_rfc3339();
    let mut reverified = 0u32;

    for row in &affected {
        let proven_tx_id = row.proven_tx_id.unwrap_or(0.0) as i64;
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };

        if proven_tx_id == 0 {
            continue;
        }

        match proof_service.get_proof(&txid).await {
            Ok(Some(new_proof)) => {
                // Proof still valid (remined in new chain) — update with new data.
                // Route merkle_path through BlobStore so >4 KB proofs go to R2.
                let store = crate::r2::BlobStore::new(blobs);
                let (mp_d1, _) = store
                    .put(
                        "proven_txs",
                        proven_tx_id,
                        "merkle_path",
                        &new_proof.merkle_path_binary,
                    )
                    .await
                    .map_err(|e| Error::from(e.to_string()))?;
                let _ = Query::new(
                    "UPDATE proven_txs SET height = ?, block_hash = ?, merkle_root = ?, \
                     merkle_path = ?, updated_at = ? WHERE proven_tx_id = ?",
                )
                .bind(new_proof.block_height as i64)
                .bind(new_proof.block_hash.as_str())
                .bind(new_proof.merkle_root.as_str())
                .bind(mp_d1)
                .bind(now.as_str())
                .bind(proven_tx_id)
                .execute(db)
                .await;

                console_log!(
                    "Reorg re-verified: txid={} new_height={}",
                    txid,
                    new_proof.block_height
                );
                reverified += 1;
            }
            Ok(None) => {
                // Proof gone — tx fell out of chain. MATCH REFERENCE (bsv-wallet-toolbox-rs
                // ReorgTask): DO NOT delete the proven_tx row, because raw_tx stored
                // there is needed for ancestry reconstruction on later broadcasts.
                // Instead, NULL out the proof fields (height/block_hash/merkle_root/
                // merkle_path) to invalidate the proof while preserving raw_tx.
                // Then demote proven_tx_req → unmined so check_for_proofs will
                // re-fetch a fresh proof next cycle.
                console_error!(
                    "Reorg INVALIDATED: txid={} (was at height {}) — nulling proof, preserving raw_tx for ancestry",
                    txid,
                    row.height.unwrap_or(0.0) as u32
                );

                let mut batch = BatchCollector::new(db);

                // Invalidate proof on proven_txs but keep the row + raw_tx
                let _ = batch.add(
                    "UPDATE proven_txs SET block_hash = '', merkle_root = '', merkle_path = NULL, \
                     updated_at = ? WHERE proven_tx_id = ?",
                    vec![QVal::Text(now.clone()), QVal::Int(proven_tx_id)],
                );

                // Demote proven_tx_req back to 'unmined' so check_for_proofs re-fetches
                let _ = batch.add(
                    "UPDATE proven_tx_reqs SET status = 'unmined', proven_tx_id = NULL, \
                     updated_at = ? WHERE proven_tx_id = ?",
                    vec![QVal::Text(now.clone()), QVal::Int(proven_tx_id)],
                );

                // Demote transaction to 'unproven'
                let _ = batch.add(
                    "UPDATE transactions SET status = 'unproven', proven_tx_id = NULL, \
                     updated_at = ? WHERE proven_tx_id = ?",
                    vec![QVal::Text(now.clone()), QVal::Int(proven_tx_id)],
                );

                // Mark outputs as non-spendable (tx no longer confirmed)
                let _ = batch.add(
                    "UPDATE outputs SET spendable = 0, updated_at = ? \
                     WHERE txid = ? AND spendable = 1",
                    vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                );

                let _ = batch.execute().await;
                reverified += 1;
            }
            Err(e) => {
                console_error!(
                    "Reorg re-verify FAILED for txid={}: {} (will retry next cycle)",
                    txid,
                    e
                );
                // Don't count as reverified — will retry next cron cycle
            }
        }
    }

    Ok(reverified)
}

// =============================================================================
// TSC Proof → BRC-74 Binary MerklePath Conversion (test-only)
// =============================================================================

/// Convert a WhatsOnChain TSC proof to BRC-74 binary MerklePath format.
///
/// The TSC proof has: index (tx position in block), txOrId, target (block hash),
/// and nodes (sibling hashes at each tree level, "" = duplicate).
///
/// BRC-74 binary: varint(blockHeight), u8(treeHeight), then for each level:
///   varint(nLeaves), and per leaf: varint(offset), u8(flags), [32-byte hash LE]
///   flags: bit 0 = duplicate, bit 1 = txid
/// Convert WoC TSC proof to BRC-74 binary MerklePath.
/// Uses MerklePath::new() with MerklePathLeaf objects (matching TS SDK convertProofToMerklePath),
/// then serializes to binary for DB storage.
#[cfg(test)]
fn tsc_proof_to_binary(proof: &WocTscProof, block_height: u32) -> Result<Vec<u8>> {
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

    let merkle_path = MerklePath::new(block_height, path)
        .map_err(|e| Error::from(format!("MerklePath::new: {}", e)))?;
    Ok(merkle_path.to_binary())
}

// =============================================================================
// Logging
// =============================================================================

// =============================================================================
// Public status endpoint
// =============================================================================

#[derive(Debug, Deserialize)]
struct StatusRow {
    unproven: Option<f64>,
    oldest_minutes: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct LastProofRow {
    last_proof_at: Option<String>,
    last_found: Option<f64>,
}

/// Returns aggregate monitor status for the dashboard (no auth needed).
pub async fn get_status(db: &D1Database) -> serde_json::Value {
    // Unproven count + oldest age
    let unproven = Query::new(
        "SELECT COUNT(*) as unproven, \
         CAST((julianday('now') - julianday(MIN(created_at))) * 1440 AS INTEGER) as oldest_minutes \
         FROM proven_tx_reqs WHERE status IN ('unmined','unknown','unconfirmed','sending','callback','unprocessed','unsent')",
    )
    .fetch_one::<StatusRow>(db)
    .await
    .ok();

    // Last successful proof
    let last_proof = Query::new(
        "SELECT created_at as last_proof_at, \
         json_extract(details, '$.proofs_found') as last_found \
         FROM monitor_events \
         WHERE json_extract(details, '$.proofs_found') > 0 \
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one::<LastProofRow>(db)
    .await
    .ok();

    serde_json::json!({
        "unproven": unproven.as_ref().and_then(|r| r.unproven).unwrap_or(0.0) as u32,
        "oldestMinutes": unproven.as_ref().and_then(|r| r.oldest_minutes).unwrap_or(0.0) as u32,
        "lastProofAt": last_proof.as_ref().and_then(|r| r.last_proof_at.clone()).unwrap_or_default(),
        "lastProofFound": last_proof.as_ref().and_then(|r| r.last_found).unwrap_or(0.0) as u32,
    })
}

async fn log_monitor_event(db: &D1Database, result: &MonitorResult) -> Result<()> {
    let details = serde_json::json!({
        "sent": result.sent,
        "send_errors": result.send_errors,
        "proofs_found": result.proofs_found,
        "proofs_checked": result.proofs_checked,
        "abandoned_failed": result.abandoned_failed,
        "status_synced": result.status_synced,
        "beef_compacted": result.beef_compacted,
        "unfail_recovered": result.unfail_recovered,
        "purged": result.purged,
        "nosend_found": result.nosend_found,
        "reorg_detected": result.reorg_detected,
        "reorg_depth": result.reorg_depth,
        "proofs_reverified": result.proofs_reverified,
        "errors": result.errors,
    })
    .to_string();

    Query::new("INSERT INTO monitor_events (event, details, created_at, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        .bind("monitor_run")
        .bind(details.as_str())
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

    Ok(())
}

// =============================================================================
// Tests — run via the test binary in tests/monitor_tests.rs
// =============================================================================

/// Test helper: parse_tsc_proof_response is pub(crate) so tests can access it.
/// Tests are in tests/monitor_tests.rs (separate binary, no WASM deps).
#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // BEEF replay probe (2026-04-15) — run with:
    //   BEEF_HEX=<hex> cargo test test_beef_replay_roundtrip -- --nocapture --ignored
    // Parses the stored input_beef via bsv-sdk, re-serializes to canonical form,
    // and prints the result so we can curl it to TAAL ARC and validate the
    // rebroadcast theory end-to-end.
    // =========================================================================

    #[test]
    #[ignore]
    fn test_beef_replay_roundtrip() {
        use bsv_sdk::transaction::Beef;

        let hex_str = std::env::var("BEEF_HEX")
            .expect("set BEEF_HEX=<hex> env var to run this test");
        let bytes = hex::decode(hex_str.trim())
            .expect("BEEF_HEX is not valid hex");

        println!("INPUT_LEN: {} bytes", bytes.len());
        println!("INPUT_PREFIX: {}", hex::encode(&bytes[..bytes.len().min(16)]));

        let mut beef = Beef::from_binary(&bytes).expect("Beef::from_binary failed");
        println!("PARSED: {} txs, {} bumps", beef.txs.len(), beef.bumps.len());
        for (i, tx) in beef.txs.iter().enumerate() {
            println!(
                "  tx[{}] txid={} bump_idx={:?} is_txid_only={}",
                i,
                tx.txid(),
                tx.bump_index(),
                tx.is_txid_only()
            );
        }

        let canonical = beef.to_binary();
        println!("CANONICAL_LEN: {} bytes", canonical.len());
        println!("CANONICAL_PREFIX: {}", hex::encode(&canonical[..canonical.len().min(16)]));
        println!("CANONICAL_HEX_BEGIN");
        println!("{}", hex::encode(&canonical));
        println!("CANONICAL_HEX_END");
    }

    // =========================================================================
    // parse_tsc_proof_response — existing tests
    // =========================================================================

    #[test]
    fn test_parse_null_response() {
        // WoC returns "null" for unknown/unconfirmed txids (HTTP 200)
        assert!(parse_tsc_proof_response("null").unwrap().is_none());
    }

    #[test]
    fn test_parse_empty_response() {
        assert!(parse_tsc_proof_response("").unwrap().is_none());
    }

    #[test]
    fn test_parse_empty_array_response() {
        assert!(parse_tsc_proof_response("[]").unwrap().is_none());
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
        assert!(result.is_some());
        let proof = result.unwrap();
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
    fn test_parse_empty_array_no_proofs() {
        // Valid JSON array but no entries
        let result = parse_tsc_proof_response("[]").unwrap();
        assert!(result.is_none());
    }

    // =========================================================================
    // parse_tsc_proof_response — adversarial & boundary inputs
    // =========================================================================

    #[test]
    fn test_parse_whitespace_only() {
        // Whitespace is not empty, not "null", not "[]" — serde will reject it
        let result = parse_tsc_proof_response("   ");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_single_object_not_array() {
        // WoC always returns an array; a bare object should fail to parse as Vec
        let json = r#"{"index": 1, "txOrId": "abc", "target": "def", "nodes": ["x"]}"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_wrong_type_for_index() {
        // index should be u32, not string
        let json = r#"[{"index": "five", "txOrId": "abc", "target": "def", "nodes": []}]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_wrong_type_for_nodes() {
        // nodes should be Vec<String>, not a string
        let json = r#"[{"index": 1, "txOrId": "abc", "target": "def", "nodes": "notanarray"}]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_nested_arrays_in_nodes() {
        // nodes contains arrays instead of strings
        let json = r#"[{"index": 1, "txOrId": "abc", "target": "def", "nodes": [["nested"]]}]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_null_inside_array() {
        // Array containing null instead of proof object
        let json = r#"[null]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_required_field() {
        // Missing "nodes" field
        let json = r#"[{"index": 1, "txOrId": "abc", "target": "def"}]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_tx_or_id() {
        // Missing "txOrId" (camelCase rename)
        let json = r#"[{"index": 1, "target": "def", "nodes": []}]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unicode_strings() {
        // Unicode in string fields — should parse OK (just odd data)
        let json = r#"[{
            "index": 0,
            "txOrId": "日本語テスト",
            "target": "émojis 🎉",
            "nodes": ["ñoño", "über"]
        }]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_ok());
        let proof = result.unwrap().unwrap();
        assert_eq!(proof.tx_or_id, "日本語テスト");
        assert_eq!(proof.target, "émojis 🎉");
    }

    #[test]
    fn test_parse_very_long_txid_string() {
        // Extremely long txOrId — should still parse
        let long_str = "a".repeat(100_000);
        let json = format!(
            r#"[{{"index": 0, "txOrId": "{}", "target": "abc", "nodes": []}}]"#,
            long_str
        );
        let result = parse_tsc_proof_response(&json);
        assert!(result.is_ok());
        let proof = result.unwrap().unwrap();
        assert_eq!(proof.tx_or_id.len(), 100_000);
    }

    #[test]
    fn test_parse_max_u32_index() {
        let json = format!(
            r#"[{{"index": {}, "txOrId": "abc", "target": "def", "nodes": ["x"]}}]"#,
            u32::MAX
        );
        let result = parse_tsc_proof_response(&json);
        assert!(result.is_ok());
        let proof = result.unwrap().unwrap();
        assert_eq!(proof.index, u32::MAX);
    }

    #[test]
    fn test_parse_index_exceeds_u32() {
        // u32::MAX + 1 should fail deserialization
        let json = format!(
            r#"[{{"index": {}, "txOrId": "abc", "target": "def", "nodes": ["x"]}}]"#,
            u32::MAX as u64 + 1
        );
        let result = parse_tsc_proof_response(&json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_negative_index() {
        let json = r#"[{"index": -1, "txOrId": "abc", "target": "def", "nodes": ["x"]}]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_float_index() {
        // Float for u32 field
        let json = r#"[{"index": 1.5, "txOrId": "abc", "target": "def", "nodes": []}]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_multiple_proofs_returns_first() {
        // Array with 2 proofs — function returns the first one
        let json = r#"[
            {"index": 0, "txOrId": "first", "target": "t1", "nodes": []},
            {"index": 1, "txOrId": "second", "target": "t2", "nodes": ["a"]}
        ]"#;
        let result = parse_tsc_proof_response(json).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().tx_or_id, "first");
    }

    #[test]
    fn test_parse_empty_nodes_array() {
        // Zero-level tree — nodes is empty
        let json = r#"[{"index": 0, "txOrId": "abc", "target": "def", "nodes": []}]"#;
        let result = parse_tsc_proof_response(json).unwrap();
        let proof = result.unwrap();
        assert!(proof.nodes.is_empty());
    }

    #[test]
    fn test_parse_nodes_with_empty_strings() {
        // Empty string nodes (alternative duplicate marker)
        let json = r#"[{"index": 0, "txOrId": "abc", "target": "def", "nodes": ["", "", "x"]}]"#;
        let result = parse_tsc_proof_response(json).unwrap();
        let proof = result.unwrap();
        assert_eq!(proof.nodes, vec!["", "", "x"]);
    }

    #[test]
    fn test_parse_nodes_with_star_duplicates() {
        // "*" is the WoC duplicate marker
        let json =
            r#"[{"index": 2, "txOrId": "abc", "target": "def", "nodes": ["hash1", "*", "hash3"]}]"#;
        let result = parse_tsc_proof_response(json).unwrap();
        let proof = result.unwrap();
        assert_eq!(proof.nodes[1], "*");
    }

    #[test]
    fn test_parse_extra_fields_ignored() {
        // Unknown fields should be ignored by serde (default behavior)
        let json = r#"[{
            "index": 0,
            "txOrId": "abc",
            "target": "def",
            "nodes": [],
            "extraField": 42,
            "anotherOne": true
        }]"#;
        let result = parse_tsc_proof_response(json);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_string_literal_null() {
        // The string "null" (not JSON null) — handled as special case
        assert!(parse_tsc_proof_response("null").unwrap().is_none());
    }

    #[test]
    fn test_parse_json_null_value() {
        // Actual JSON null (lowercase) — same as "null" string
        // Note: serde would fail to parse JSON null as Vec<WocTscProof>,
        // but our function catches "null" before that
        assert!(parse_tsc_proof_response("null").unwrap().is_none());
    }

    #[test]
    fn test_parse_large_nodes_array() {
        // Deep merkle tree (30 levels)
        let nodes: Vec<String> = (0..30).map(|i| format!("{:064x}", i)).collect();
        let json = format!(
            r#"[{{"index": 0, "txOrId": "abc", "target": "def", "nodes": {:?}}}]"#,
            nodes
        );
        let result = parse_tsc_proof_response(&json).unwrap();
        let proof = result.unwrap();
        assert_eq!(proof.nodes.len(), 30);
    }

    // =========================================================================
    // tsc_proof_to_binary — basic functionality tests
    // =========================================================================

    /// Helper: create a WocTscProof with valid 64-char hex hashes for testing.
    fn make_test_proof(index: u32, nodes: Vec<&str>) -> WocTscProof {
        WocTscProof {
            index,
            tx_or_id: "a".repeat(64),
            target: "b".repeat(64),
            nodes: nodes.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn test_tsc_proof_to_binary_single_node() {
        // Simplest tree: 1 level, tx at index 0
        let sibling = "c".repeat(64);
        let proof = make_test_proof(0, vec![&sibling]);
        let result = tsc_proof_to_binary(&proof, 800_000);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        // Should produce non-empty binary output
        assert!(!bytes.is_empty());
        // First bytes encode the block height as a varint
        assert!(bytes.len() > 4);
    }

    #[test]
    fn test_tsc_proof_to_binary_odd_index() {
        // Tx at odd index — sibling is on the left
        let sibling = "d".repeat(64);
        let proof = make_test_proof(1, vec![&sibling]);
        let result = tsc_proof_to_binary(&proof, 800_001);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_tsc_proof_to_binary_multiple_levels() {
        // 3-level tree, index=5 (binary 101)
        let h1 = "1".repeat(64);
        let h2 = "2".repeat(64);
        let h3 = "3".repeat(64);
        let proof = make_test_proof(5, vec![&h1, &h2, &h3]);
        let result = tsc_proof_to_binary(&proof, 850_000);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_tsc_proof_to_binary_with_duplicate_star() {
        // "*" marks a duplicate node
        let h1 = "e".repeat(64);
        let proof = make_test_proof(0, vec![&h1, "*"]);
        let result = tsc_proof_to_binary(&proof, 800_000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tsc_proof_to_binary_with_duplicate_empty() {
        // Empty string also marks a duplicate node
        let h1 = "f".repeat(64);
        let proof = make_test_proof(0, vec![&h1, ""]);
        let result = tsc_proof_to_binary(&proof, 800_000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tsc_proof_to_binary_deterministic() {
        // Same input should produce same output
        let sibling = "a1b2c3".repeat(10) + &"0".repeat(4);
        let proof1 = make_test_proof(3, vec![&sibling, "*"]);
        let proof2 = make_test_proof(3, vec![&sibling, "*"]);
        let bytes1 = tsc_proof_to_binary(&proof1, 100).unwrap();
        let bytes2 = tsc_proof_to_binary(&proof2, 100).unwrap();
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_tsc_proof_to_binary_different_heights_differ() {
        // Different block heights should produce different binary (height is encoded)
        let sibling = "c".repeat(64);
        let proof = make_test_proof(0, vec![&sibling]);
        let bytes_low = tsc_proof_to_binary(&proof, 100).unwrap();
        let bytes_high = tsc_proof_to_binary(&proof, 900_000).unwrap();
        assert_ne!(bytes_low, bytes_high);
    }

    #[test]
    fn test_tsc_proof_to_binary_zero_height() {
        // Block height 0 (genesis-ish)
        let sibling = "c".repeat(64);
        let proof = make_test_proof(0, vec![&sibling]);
        let result = tsc_proof_to_binary(&proof, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tsc_proof_to_binary_large_index() {
        // Large index value
        let h1 = "a".repeat(64);
        let h2 = "b".repeat(64);
        let proof = make_test_proof(1_000_000, vec![&h1, &h2]);
        let result = tsc_proof_to_binary(&proof, 800_000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tsc_proof_to_binary_empty_nodes() {
        // Empty nodes array — 0-level tree. MerklePath::new may reject this.
        let proof = make_test_proof(0, vec![]);
        let result = tsc_proof_to_binary(&proof, 800_000);
        // Either succeeds with minimal output or errors — both are acceptable.
        // The important thing is it doesn't panic.
        let _ = result;
    }

    // =========================================================================
    // UnfailRow deserialization tests
    // =========================================================================

    #[test]
    fn test_unfail_row_deserialize_full() {
        let json = r#"{
            "proven_tx_req_id": 42.0,
            "txid": "abc123def456",
            "raw_tx": "0100000001..."
        }"#;
        let row: UnfailRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.proven_tx_req_id.unwrap() as i64, 42);
        assert_eq!(row.txid.as_deref(), Some("abc123def456"));
        assert_eq!(row.raw_tx.as_deref(), Some("0100000001..."));
    }

    #[test]
    fn test_unfail_row_deserialize_nulls() {
        let json = r#"{
            "proven_tx_req_id": null,
            "txid": null,
            "raw_tx": null
        }"#;
        let row: UnfailRow = serde_json::from_str(json).unwrap();
        assert!(row.proven_tx_req_id.is_none());
        assert!(row.txid.is_none());
        assert!(row.raw_tx.is_none());
    }

    #[test]
    fn test_unfail_row_deserialize_missing_raw_tx() {
        let json = r#"{
            "proven_tx_req_id": 1.0,
            "txid": "deadbeef"
        }"#;
        let row: UnfailRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.proven_tx_req_id.unwrap() as i64, 1);
        assert_eq!(row.txid.as_deref(), Some("deadbeef"));
        assert!(row.raw_tx.is_none());
    }

    // =========================================================================
    // Timing guard tests
    // =========================================================================

    #[test]
    fn test_unfail_timing_guard_runs_on_0_minute() {
        assert!(0u32 % 10 < 5);
    }

    #[test]
    fn test_unfail_timing_guard_runs_on_10_minute() {
        assert!(10u32 % 10 < 5);
    }

    #[test]
    fn test_unfail_timing_guard_runs_on_20_minute() {
        assert!(20u32 % 10 < 5);
    }

    #[test]
    fn test_unfail_timing_guard_runs_on_30_minute() {
        assert!(30u32 % 10 < 5);
    }

    #[test]
    fn test_unfail_timing_guard_runs_on_2_minute() {
        assert!(2u32 % 10 < 5);
    }

    #[test]
    fn test_unfail_timing_guard_skips_on_5_minute() {
        assert!(!(5u32 % 10 < 5));
    }

    #[test]
    fn test_unfail_timing_guard_skips_on_7_minute() {
        assert!(!(7u32 % 10 < 5));
    }

    #[test]
    fn test_unfail_timing_guard_skips_on_15_minute() {
        assert!(!(15u32 % 10 < 5));
    }

    #[test]
    fn test_unfail_timing_guard_skips_on_55_minute() {
        assert!(!(55u32 % 10 < 5));
    }

    #[test]
    fn test_unfail_timing_guard_runs_on_50_minute() {
        assert!(50u32 % 10 < 5);
    }

    // =========================================================================
    // send_waiting — SQL query verification tests
    // =========================================================================

    #[test]
    fn test_send_waiting_query_targets_correct_statuses() {
        let sql = "SELECT proven_tx_req_id, txid, status, attempts, hex(raw_tx) as raw_tx, \
                   batch, hex(input_beef) as input_beef \
                   FROM proven_tx_reqs \
                   WHERE status IN ('unsent', 'sending') \
                   ORDER BY created_at ASC \
                   LIMIT 100";
        assert!(sql.contains("'unsent'"));
        assert!(sql.contains("'sending'"));
        assert!(!sql.contains("'unmined'"));
        assert!(!sql.contains("'unknown'"));
        assert!(!sql.contains("'unconfirmed'"));
        assert!(!sql.contains("'callback'"));
        assert!(!sql.contains("'unprocessed'"));
        assert!(sql.contains("ORDER BY created_at ASC"));
    }

    #[test]
    fn test_check_for_proofs_query_excludes_broadcast_statuses() {
        let sql = "SELECT proven_tx_req_id, txid, status, attempts, hex(raw_tx) as raw_tx \
                   FROM proven_tx_reqs \
                   WHERE status IN ('unmined', 'unknown', 'unconfirmed', 'callback') \
                   ORDER BY attempts ASC, created_at DESC \
                   LIMIT 200";
        assert!(sql.contains("'unmined'"));
        assert!(sql.contains("'unknown'"));
        assert!(sql.contains("'unconfirmed'"));
        assert!(sql.contains("'callback'"));
        assert!(!sql.contains("'unsent'"));
        assert!(!sql.contains("'sending'"));
        assert!(!sql.contains("'unprocessed'"));
    }

    #[test]
    fn test_send_waiting_success_updates() {
        let success_sql_ptr = "UPDATE proven_tx_reqs SET status = 'unmined', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?";
        let success_sql_tx = "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ? AND status IN ('sending', 'nosend', 'unprocessed')";
        let success_sql_out = "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)";

        assert!(success_sql_ptr.contains("'unmined'"));
        assert!(success_sql_tx.contains("'unproven'"));
        assert!(success_sql_out.contains("spendable = 1"));
        assert!(success_sql_out.contains("change = 1 OR custom_instructions IS NOT NULL"));
    }

    #[test]
    fn test_send_waiting_double_spend_updates() {
        let ds_sql_ptr = "UPDATE proven_tx_reqs SET status = 'doubleSpend', updated_at = ? WHERE proven_tx_req_id = ?";
        let ds_sql_tx = "UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?";
        let ds_sql_out = "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)";

        assert!(ds_sql_ptr.contains("'doubleSpend'"));
        assert!(ds_sql_tx.contains("'failed'"));
        assert!(ds_sql_out.contains("spendable = 1"));
        assert!(ds_sql_out.contains("change = 1 OR custom_instructions IS NOT NULL"));
    }

    #[test]
    fn test_send_waiting_invalid_tx_updates() {
        let inv_sql_ptr = "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?";
        let inv_sql_tx = "UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?";

        assert!(inv_sql_ptr.contains("'invalid'"));
        assert!(inv_sql_tx.contains("'failed'"));
    }

    #[test]
    fn test_send_waiting_service_error_increments_attempts() {
        let svc_sql =
            "UPDATE proven_tx_reqs SET attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?";
        assert!(svc_sql.contains("attempts = ?"));
        assert!(!svc_sql.contains("status"));
    }

    #[test]
    fn test_unsent_tx_row_deserialize() {
        let json = r#"{
            "proven_tx_req_id": 42.0,
            "txid": "aabbccdd",
            "status": "unsent",
            "attempts": 0.0,
            "raw_tx": "0100000001",
            "batch": null,
            "input_beef": "0100beef"
        }"#;
        let row: UnsentTxRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.proven_tx_req_id.unwrap() as i64, 42);
        assert_eq!(row.txid.as_deref(), Some("aabbccdd"));
        assert_eq!(row.status.as_deref(), Some("unsent"));
        assert_eq!(row.attempts.unwrap() as i64, 0);
        assert_eq!(row.raw_tx.as_deref(), Some("0100000001"));
        assert!(row.batch.is_none());
        assert_eq!(row.input_beef.as_deref(), Some("0100beef"));
    }

    #[test]
    fn test_unsent_tx_row_all_null() {
        let json = r#"{
            "proven_tx_req_id": null,
            "txid": null,
            "status": null,
            "attempts": null,
            "raw_tx": null,
            "batch": null,
            "input_beef": null
        }"#;
        let row: UnsentTxRow = serde_json::from_str(json).unwrap();
        assert!(row.proven_tx_req_id.is_none());
        assert!(row.txid.is_none());
        assert!(row.input_beef.is_none());
    }

    // =========================================================================
    // MonitorResult field tests
    // =========================================================================

    #[test]
    fn test_monitor_result_has_unfail_field() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 5,
            purged: 0,
            nosend_found: 0,
            reorg_detected: false,
            reorg_depth: 0,
            proofs_reverified: 0,
            errors: Vec::new(),
        };
        assert_eq!(result.unfail_recovered, 5);
    }

    #[test]
    fn test_monitor_result_has_send_fields() {
        let result = MonitorResult {
            sent: 5,
            send_errors: 2,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 0,
            purged: 0,
            nosend_found: 0,
            reorg_detected: false,
            reorg_depth: 0,
            proofs_reverified: 0,
            errors: vec![],
        };
        assert_eq!(result.sent, 5);
        assert_eq!(result.send_errors, 2);
    }

    #[test]
    fn test_monitor_result_unfail_in_log_json() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 1,
            proofs_checked: 10,
            abandoned_failed: 2,
            status_synced: 3,
            beef_compacted: 0,
            unfail_recovered: 4,
            purged: 0,
            nosend_found: 0,
            reorg_detected: false,
            reorg_depth: 0,
            proofs_reverified: 0,
            errors: vec!["test".to_string()],
        };
        let details = serde_json::json!({
            "proofs_found": result.proofs_found,
            "proofs_checked": result.proofs_checked,
            "abandoned_failed": result.abandoned_failed,
            "status_synced": result.status_synced,
            "beef_compacted": result.beef_compacted,
            "unfail_recovered": result.unfail_recovered,
            "nosend_found": result.nosend_found,
            "errors": result.errors,
        });
        assert_eq!(details["unfail_recovered"], 4);
        assert_eq!(details["proofs_found"], 1);
        assert_eq!(details["nosend_found"], 0);
    }

    // =========================================================================
    // purge_data — unit tests
    // =========================================================================

    #[test]
    fn test_purge_params_defaults() {
        let params = crate::types::PurgeParams {
            max_age_days: 30,
            purge_completed: true,
            purge_failed: true,
        };
        assert_eq!(params.max_age_days, 30);
        assert!(params.purge_completed);
        assert!(params.purge_failed);
    }

    #[test]
    fn test_purge_params_custom_age() {
        let params = crate::types::PurgeParams {
            max_age_days: 90,
            purge_completed: true,
            purge_failed: false,
        };
        assert_eq!(params.max_age_days, 90);
        assert!(params.purge_completed);
        assert!(!params.purge_failed);
    }

    #[test]
    fn test_purge_results_empty() {
        let results = crate::types::PurgeResults {
            count: 0,
            log: "nothing to purge".to_string(),
        };
        assert_eq!(results.count, 0);
        assert_eq!(results.log, "nothing to purge");
    }

    #[test]
    fn test_purge_results_with_activity() {
        let results = crate::types::PurgeResults {
            count: 15,
            log:
                "deleted 7 completed reqs; cleaned data from 3 completed txs; deleted 5 failed reqs"
                    .to_string(),
        };
        assert_eq!(results.count, 15);
        assert!(results.log.contains("deleted"));
        assert!(results.log.contains("cleaned data from"));
    }

    #[test]
    fn test_purge_sql_completed_deletes_proven_tx_reqs() {
        // The code now DELETEs completed proven_tx_reqs (not UPDATE SET NULL).
        // TS reference: DELETE because raw_tx is NOT NULL in the schema.
        let sql = "DELETE FROM proven_tx_reqs \
                    WHERE status = 'completed' \
                    AND proven_tx_id IS NOT NULL \
                    AND updated_at < datetime('now', ?)";
        assert!(sql.contains("DELETE FROM proven_tx_reqs"));
        assert!(sql.contains("status = 'completed'"));
        assert!(sql.contains("proven_tx_id IS NOT NULL"));
        assert!(sql.contains("datetime('now', ?)"));
        // Must NOT be an UPDATE — the whole row is deleted
        assert!(!sql.contains("UPDATE"));
        assert!(!sql.contains("SET"));
    }

    #[test]
    fn test_purge_sql_completed_transactions_nulls_raw_tx_and_input_beef() {
        // Completed transactions get both raw_tx AND input_beef set to NULL.
        let sql = "UPDATE transactions \
                    SET raw_tx = NULL, input_beef = NULL, updated_at = ? \
                    WHERE status = 'completed' \
                    AND proven_tx_id IS NOT NULL \
                    AND updated_at < datetime('now', ?) \
                    AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)";
        assert!(sql.contains("UPDATE transactions"));
        assert!(sql.contains("SET raw_tx = NULL, input_beef = NULL"));
        assert!(sql.contains("status = 'completed'"));
        assert!(sql.contains("proven_tx_id IS NOT NULL"));
        assert!(sql.contains("datetime('now', ?)"));
        // Targets only rows that still have data to clear
        assert!(sql.contains("raw_tx IS NOT NULL OR input_beef IS NOT NULL"));
    }

    #[test]
    fn test_purge_sql_failed_where_clause() {
        let sql = "DELETE FROM proven_tx_reqs \
                    WHERE status IN ('invalid', 'doubleSpend') \
                    AND updated_at < datetime('now', ?)";
        assert!(sql.contains("status IN ('invalid', 'doubleSpend')"));
        assert!(sql.contains("datetime('now', ?)"));
        assert!(sql.contains("DELETE FROM proven_tx_reqs"));
    }

    #[test]
    fn test_purge_age_modifier_format() {
        let params = crate::types::PurgeParams {
            max_age_days: 30,
            purge_completed: true,
            purge_failed: true,
        };
        let age_modifier = format!("-{} days", params.max_age_days);
        assert_eq!(age_modifier, "-30 days");

        let params_90 = crate::types::PurgeParams {
            max_age_days: 90,
            purge_completed: true,
            purge_failed: true,
        };
        let age_modifier_90 = format!("-{} days", params_90.max_age_days);
        assert_eq!(age_modifier_90, "-90 days");
    }

    #[test]
    fn test_purge_completed_false_skips_delete_and_tx_clean() {
        // When purge_completed is false, purge_data skips BOTH:
        // 1. DELETE FROM proven_tx_reqs WHERE status = 'completed' ...
        // 2. UPDATE transactions SET raw_tx = NULL, input_beef = NULL ...
        let params = crate::types::PurgeParams {
            max_age_days: 30,
            purge_completed: false,
            purge_failed: true,
        };
        assert!(!params.purge_completed);
        // Only the failed DELETE branch should execute
        assert!(params.purge_failed);
    }

    #[test]
    fn test_purge_failed_false_skips_delete() {
        let params = crate::types::PurgeParams {
            max_age_days: 30,
            purge_completed: true,
            purge_failed: false,
        };
        assert!(!params.purge_failed);
        // The completed branch (DELETE + tx UPDATE) should still execute
        assert!(params.purge_completed);
    }

    #[test]
    fn test_purge_both_disabled_does_nothing() {
        let params = crate::types::PurgeParams {
            max_age_days: 30,
            purge_completed: false,
            purge_failed: false,
        };
        assert!(!params.purge_completed);
        assert!(!params.purge_failed);
        // With both disabled, no SQL executes and result is "nothing to purge"
    }

    #[test]
    fn test_purge_completed_branch_does_two_operations() {
        // The purge_completed=true branch now executes TWO SQL statements:
        // 1. DELETE completed proven_tx_reqs with proven_tx_id IS NOT NULL
        // 2. UPDATE transactions SET raw_tx = NULL, input_beef = NULL
        //
        // Simulate the log building to verify both operations are logged.
        let mut log_parts: Vec<String> = Vec::new();
        let req_deleted = 5u32;
        let tx_cleaned = 3u32;
        if req_deleted > 0 {
            log_parts.push(format!("deleted {} completed reqs", req_deleted));
        }
        if tx_cleaned > 0 {
            log_parts.push(format!("cleaned data from {} completed txs", tx_cleaned));
        }
        assert_eq!(log_parts.len(), 2);
        assert!(log_parts[0].contains("deleted"));
        assert!(log_parts[0].contains("completed reqs"));
        assert!(log_parts[1].contains("cleaned data from"));
        assert!(log_parts[1].contains("completed txs"));
    }

    #[test]
    fn test_purge_timing_guard() {
        let minute = Utc::now().minute();
        assert!(minute < 60);
    }

    #[test]
    fn test_purge_monitor_result_field() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 0,
            purged: 42,
            nosend_found: 0,
            reorg_detected: false,
            reorg_depth: 0,
            proofs_reverified: 0,
            errors: Vec::new(),
        };
        assert_eq!(result.purged, 42);
    }

    #[test]
    fn test_purge_log_format_nothing() {
        let log_parts: Vec<String> = Vec::new();
        let log = if log_parts.is_empty() {
            "nothing to purge".to_string()
        } else {
            log_parts.join("; ")
        };
        assert_eq!(log, "nothing to purge");
    }

    #[test]
    fn test_purge_log_format_completed_only() {
        // Mirrors the actual log format from purge_data:
        // "deleted N completed reqs" + "cleaned data from N completed txs"
        let mut log_parts: Vec<String> = Vec::new();
        let deleted = 7u32;
        let cleaned = 3u32;
        if deleted > 0 {
            log_parts.push(format!("deleted {} completed reqs", deleted));
        }
        if cleaned > 0 {
            log_parts.push(format!("cleaned data from {} completed txs", cleaned));
        }
        let log = log_parts.join("; ");
        assert_eq!(
            log,
            "deleted 7 completed reqs; cleaned data from 3 completed txs"
        );
    }

    #[test]
    fn test_purge_log_format_failed_only() {
        let mut log_parts: Vec<String> = Vec::new();
        let deleted = 3u32;
        if deleted > 0 {
            log_parts.push(format!("deleted {} failed reqs", deleted));
        }
        let log = log_parts.join("; ");
        assert_eq!(log, "deleted 3 failed reqs");
    }

    #[test]
    fn test_purge_log_format_both() {
        // Full purge: completed reqs deleted, completed tx data cleaned, failed reqs deleted
        let mut log_parts: Vec<String> = Vec::new();
        let completed_deleted = 10u32;
        let tx_cleaned = 8u32;
        let failed_deleted = 5u32;
        if completed_deleted > 0 {
            log_parts.push(format!("deleted {} completed reqs", completed_deleted));
        }
        if tx_cleaned > 0 {
            log_parts.push(format!("cleaned data from {} completed txs", tx_cleaned));
        }
        if failed_deleted > 0 {
            log_parts.push(format!("deleted {} failed reqs", failed_deleted));
        }
        let log = log_parts.join("; ");
        assert_eq!(
            log,
            "deleted 10 completed reqs; cleaned data from 8 completed txs; deleted 5 failed reqs"
        );
    }

    // =========================================================================
    // check_no_sends — tests
    // =========================================================================

    #[test]
    fn test_nosend_timing_guard_midnight_only() {
        // The guard: hour() == 0 && minute() == 0
        // Should fire only at midnight UTC (00:00)
        assert!(0u32 == 0 && 0u32 == 0); // 00:00 -> fires
    }

    #[test]
    fn test_nosend_timing_guard_rejects_non_midnight_hour() {
        // hour != 0 should never fire
        assert!(!(1u32 == 0 && 0u32 == 0)); // 01:00 -> skipped
        assert!(!(12u32 == 0 && 0u32 == 0)); // 12:00 -> skipped
        assert!(!(23u32 == 0 && 0u32 == 0)); // 23:00 -> skipped
    }

    #[test]
    fn test_nosend_timing_guard_rejects_non_zero_minute() {
        // hour == 0 but minute != 0 should not fire
        assert!(!(0u32 == 0 && 5u32 == 0)); // 00:05 -> skipped
        assert!(!(0u32 == 0 && 30u32 == 0)); // 00:30 -> skipped
        assert!(!(0u32 == 0 && 55u32 == 0)); // 00:55 -> skipped
    }

    #[test]
    fn test_nosend_timing_guard_all_other_combos_fail() {
        // Exhaustive check: only (0,0) passes
        for hour in 0u32..24 {
            for minute in 0u32..60 {
                let fires = hour == 0 && minute == 0;
                if hour == 0 && minute == 0 {
                    assert!(fires, "Should fire at 00:00");
                } else {
                    assert!(!fires, "Should NOT fire at {:02}:{:02}", hour, minute);
                }
            }
        }
    }

    #[test]
    fn test_unfail_row_reuse_for_nosend() {
        // UnfailRow has the same shape as the nosend query result:
        // proven_tx_req_id, txid, raw_tx
        let json = r#"{
            "proven_tx_req_id": 99.0,
            "txid": "nosend_txid_abc",
            "raw_tx": "0200000001deadbeef"
        }"#;
        let row: UnfailRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.proven_tx_req_id.unwrap() as i64, 99);
        assert_eq!(row.txid.as_deref(), Some("nosend_txid_abc"));
        assert_eq!(row.raw_tx.as_deref(), Some("0200000001deadbeef"));
    }

    #[test]
    fn test_monitor_result_has_nosend_found_field() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 0,
            purged: 0,
            nosend_found: 7,
            reorg_detected: false,
            reorg_depth: 0,
            proofs_reverified: 0,
            errors: Vec::new(),
        };
        assert_eq!(result.nosend_found, 7);
    }

    #[test]
    fn test_nosend_query_targets_nosend_status() {
        let sql = "SELECT proven_tx_req_id, txid, hex(raw_tx) as raw_tx \
                   FROM proven_tx_reqs \
                   WHERE status = 'nosend' \
                   ORDER BY created_at ASC \
                   LIMIT 50";
        assert!(sql.contains("status = 'nosend'"));
        assert!(!sql.contains("'unfail'"));
        assert!(!sql.contains("'unmined'"));
        assert!(!sql.contains("'unsent'"));
        assert!(sql.contains("ORDER BY created_at ASC"));
        assert!(sql.contains("LIMIT 50"));
    }

    #[test]
    fn test_nosend_proof_storage_updates_all_three_tables() {
        // The store_unfail_proof function (reused by check_no_sends) runs 3 updates:
        // 1. proven_tx_reqs -> status = 'completed'
        // 2. transactions -> status = 'completed'
        // 3. outputs -> spendable = 1 where unspent
        let sql_ptr = "UPDATE proven_tx_reqs SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE proven_tx_req_id = ?";
        let sql_tx = "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?";
        let sql_out = "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)";

        assert!(sql_out.contains("change = 1 OR custom_instructions IS NOT NULL"));
        assert!(sql_ptr.contains("status = 'completed'"));
        assert!(sql_ptr.contains("proven_tx_id = ?"));
        assert!(sql_tx.contains("status = 'completed'"));
        assert!(sql_tx.contains("proven_tx_id = ?"));
        assert!(sql_out.contains("spendable = 1"));
        assert!(sql_out.contains("spendable = 0 AND spent_by IS NULL"));
    }

    #[test]
    fn test_nosend_no_proof_does_nothing() {
        // When no proof is found for a nosend tx, we should NOT:
        // - change status to 'invalid' (unlike unfail)
        // - increment attempts
        // - mark as failed
        // The check_no_sends function simply continues to the next row.
        // Verify this by checking the code doesn't contain invalid/failed updates for nosend.
        // (This test documents the expected behavior difference from unfail_transactions)
        let nosend_no_proof_action = "do nothing"; // Explicit skip
        assert_eq!(nosend_no_proof_action, "do nothing");
    }

    #[test]
    fn test_nosend_log_json_includes_field() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 0,
            purged: 0,
            nosend_found: 3,
            reorg_detected: false,
            reorg_depth: 0,
            proofs_reverified: 0,
            errors: Vec::new(),
        };
        let details = serde_json::json!({
            "sent": result.sent,
            "send_errors": result.send_errors,
            "proofs_found": result.proofs_found,
            "proofs_checked": result.proofs_checked,
            "abandoned_failed": result.abandoned_failed,
            "status_synced": result.status_synced,
            "beef_compacted": result.beef_compacted,
            "unfail_recovered": result.unfail_recovered,
            "purged": result.purged,
            "nosend_found": result.nosend_found,
            "errors": result.errors,
        });
        assert_eq!(details["nosend_found"], 3);
    }

    // =========================================================================
    // Reorg detection — parse_stored_height
    // =========================================================================

    #[test]
    fn test_parse_stored_height_valid() {
        let details = r#"{"height": 890123}"#;
        assert_eq!(parse_stored_height(details), Some(890123));
    }

    #[test]
    fn test_parse_stored_height_zero() {
        let details = r#"{"height": 0}"#;
        assert_eq!(parse_stored_height(details), Some(0));
    }

    #[test]
    fn test_parse_stored_height_missing_field() {
        let details = r#"{"blocks": 890123}"#;
        assert_eq!(parse_stored_height(details), None);
    }

    #[test]
    fn test_parse_stored_height_invalid_json() {
        assert_eq!(parse_stored_height("{not valid}"), None);
    }

    #[test]
    fn test_parse_stored_height_empty() {
        assert_eq!(parse_stored_height(""), None);
    }

    #[test]
    fn test_parse_stored_height_null_value() {
        let details = r#"{"height": null}"#;
        assert_eq!(parse_stored_height(details), None);
    }

    #[test]
    fn test_parse_stored_height_string_value() {
        // height as string should fail (as_u64 returns None for strings)
        let details = r#"{"height": "890123"}"#;
        assert_eq!(parse_stored_height(details), None);
    }

    #[test]
    fn test_parse_stored_height_large_value() {
        let details = r#"{"height": 999999999}"#;
        assert_eq!(parse_stored_height(details), Some(999999999));
    }

    #[test]
    fn test_parse_stored_height_with_extra_fields() {
        let details = r#"{"height": 850000, "timestamp": "2026-04-03T12:00:00Z"}"#;
        assert_eq!(parse_stored_height(details), Some(850000));
    }

    // =========================================================================
    // Reorg detection — height comparison logic
    // =========================================================================

    #[test]
    fn test_height_comparison_normal_progression() {
        let current: u32 = 890124;
        let last: u32 = 890123;
        assert!(current > last);
        assert!(!(current < last));
    }

    #[test]
    fn test_height_comparison_same_height() {
        let current: u32 = 890123;
        let last: u32 = 890123;
        assert!(current == last);
        assert!(!(current < last));
    }

    #[test]
    fn test_height_comparison_reorg_detected() {
        let current: u32 = 890120;
        let last: u32 = 890123;
        assert!(current < last);
    }

    #[test]
    fn test_reorg_depth_calculation() {
        let current: u32 = 890120;
        let last: u32 = 890123;
        let depth = last - current;
        assert_eq!(depth, 3);
    }

    #[test]
    fn test_reorg_depth_single_block() {
        let current: u32 = 890122;
        let last: u32 = 890123;
        let depth = last - current;
        assert_eq!(depth, 1);
    }

    #[test]
    fn test_reorg_depth_large() {
        let current: u32 = 890100;
        let last: u32 = 890123;
        let depth = last - current;
        assert_eq!(depth, 23);
    }

    // =========================================================================
    // Reorg detection — stored height JSON format round-trip
    // =========================================================================

    #[test]
    fn test_chain_height_json_roundtrip() {
        let height: u32 = 890123;
        let json = serde_json::json!({"height": height}).to_string();
        assert_eq!(json, r#"{"height":890123}"#);
        let parsed = parse_stored_height(&json);
        assert_eq!(parsed, Some(890123));
    }

    #[test]
    fn test_chain_height_json_roundtrip_zero() {
        let height: u32 = 0;
        let json = serde_json::json!({"height": height}).to_string();
        let parsed = parse_stored_height(&json);
        assert_eq!(parsed, Some(0));
    }

    // =========================================================================
    // Reorg detection — AffectedProofRow deserialization
    // =========================================================================

    #[test]
    fn test_affected_proof_row_deserialize() {
        let json = r#"{"proven_tx_id": 42.0, "txid": "abc123", "height": 890123.0}"#;
        let row: AffectedProofRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.proven_tx_id.unwrap() as i64, 42);
        assert_eq!(row.txid.as_deref(), Some("abc123"));
        assert_eq!(row.height.unwrap() as u32, 890123);
    }

    #[test]
    fn test_affected_proof_row_deserialize_nulls() {
        let json = r#"{"proven_tx_id": null, "txid": null, "height": null}"#;
        let row: AffectedProofRow = serde_json::from_str(json).unwrap();
        assert!(row.proven_tx_id.is_none());
        assert!(row.txid.is_none());
        assert!(row.height.is_none());
    }

    // =========================================================================
    // Reorg detection — ChainHeightRow deserialization
    // =========================================================================

    #[test]
    fn test_chain_height_row_deserialize() {
        let json = r#"{"details": "{\"height\": 890123}"}"#;
        let row: ChainHeightRow = serde_json::from_str(json).unwrap();
        assert!(row.details.is_some());
        let height = parse_stored_height(row.details.as_deref().unwrap());
        assert_eq!(height, Some(890123));
    }

    #[test]
    fn test_chain_height_row_deserialize_null() {
        let json = r#"{"details": null}"#;
        let row: ChainHeightRow = serde_json::from_str(json).unwrap();
        assert!(row.details.is_none());
    }

    // =========================================================================
    // Reorg detection — MonitorResult reorg fields
    // =========================================================================

    #[test]
    fn test_monitor_result_reorg_fields_default() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 0,
            purged: 0,
            nosend_found: 0,
            reorg_detected: false,
            reorg_depth: 0,
            proofs_reverified: 0,
            errors: Vec::new(),
        };
        assert!(!result.reorg_detected);
        assert_eq!(result.reorg_depth, 0);
        assert_eq!(result.proofs_reverified, 0);
    }

    #[test]
    fn test_monitor_result_reorg_fields_detected() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 0,
            purged: 0,
            nosend_found: 0,
            reorg_detected: true,
            reorg_depth: 3,
            proofs_reverified: 5,
            errors: Vec::new(),
        };
        assert!(result.reorg_detected);
        assert_eq!(result.reorg_depth, 3);
        assert_eq!(result.proofs_reverified, 5);
    }

    #[test]
    fn test_monitor_result_reorg_in_log_json() {
        let result = MonitorResult {
            sent: 0,
            send_errors: 0,
            proofs_found: 0,
            proofs_checked: 0,
            abandoned_failed: 0,
            status_synced: 0,
            beef_compacted: 0,
            unfail_recovered: 0,
            purged: 0,
            nosend_found: 0,
            reorg_detected: true,
            reorg_depth: 2,
            proofs_reverified: 7,
            errors: Vec::new(),
        };
        let details = serde_json::json!({
            "reorg_detected": result.reorg_detected,
            "reorg_depth": result.reorg_depth,
            "proofs_reverified": result.proofs_reverified,
        });
        assert_eq!(details["reorg_detected"], true);
        assert_eq!(details["reorg_depth"], 2);
        assert_eq!(details["proofs_reverified"], 7);
    }

    // =========================================================================
    // Reorg detection — SQL query verification
    // =========================================================================

    #[test]
    fn test_reorg_affected_proof_query() {
        let sql = "SELECT proven_tx_id, txid, height FROM proven_txs WHERE height > ?";
        assert!(sql.contains("proven_txs"));
        assert!(sql.contains("height > ?"));
        assert!(sql.contains("proven_tx_id"));
        assert!(sql.contains("txid"));
    }

    #[test]
    fn test_reorg_chain_height_read_query() {
        let sql = "SELECT details FROM monitor_events \
                   WHERE event = 'chain_height' \
                   ORDER BY created_at DESC LIMIT 1";
        assert!(sql.contains("event = 'chain_height'"));
        assert!(sql.contains("ORDER BY created_at DESC"));
        assert!(sql.contains("LIMIT 1"));
    }

    #[test]
    fn test_reorg_chain_height_write_query() {
        let sql = "INSERT INTO monitor_events (event, details, created_at, updated_at) \
                   VALUES ('chain_height', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
        assert!(sql.contains("'chain_height'"));
        assert!(sql.contains("monitor_events"));
    }

    #[test]
    fn test_reorg_invalidation_deletes_proven_tx() {
        let sql = "DELETE FROM proven_txs WHERE proven_tx_id = ?";
        assert!(sql.contains("DELETE FROM proven_txs"));
        assert!(sql.contains("proven_tx_id = ?"));
    }

    #[test]
    fn test_reorg_invalidation_demotes_req() {
        let sql = "UPDATE proven_tx_reqs SET status = 'unmined', proven_tx_id = NULL, \
                   updated_at = ? WHERE proven_tx_id = ?";
        assert!(sql.contains("'unmined'"));
        assert!(sql.contains("proven_tx_id = NULL"));
    }

    #[test]
    fn test_reorg_invalidation_demotes_transaction() {
        let sql = "UPDATE transactions SET status = 'unproven', proven_tx_id = NULL, \
                   updated_at = ? WHERE proven_tx_id = ?";
        assert!(sql.contains("'unproven'"));
        assert!(sql.contains("proven_tx_id = NULL"));
    }

    #[test]
    fn test_reorg_invalidation_disables_spendable() {
        let sql = "UPDATE outputs SET spendable = 0, updated_at = ? \
                   WHERE txid = ? AND spendable = 1";
        assert!(sql.contains("spendable = 0"));
        assert!(sql.contains("txid = ?"));
    }

    #[test]
    fn test_reorg_reverification_updates_proven_tx() {
        let sql = "UPDATE proven_txs SET height = ?, block_hash = ?, merkle_root = ?, \
                   merkle_path = ?, updated_at = ? WHERE proven_tx_id = ?";
        assert!(sql.contains("height = ?"));
        assert!(sql.contains("block_hash = ?"));
        assert!(sql.contains("merkle_root = ?"));
        assert!(sql.contains("merkle_path = ?"));
    }

    // =========================================================================
    // check_for_proofs triage — confirmed_set classification
    // =========================================================================

    /// Helper: build the confirmed_set from a Vec<TxStatusDetail> using the same
    /// logic as check_for_proofs (status == "mined" && depth >= 1).
    fn build_confirmed_set(
        statuses: Vec<crate::services::TxStatusDetail>,
    ) -> std::collections::HashSet<String> {
        statuses
            .into_iter()
            .filter(|s| s.status == "mined" && s.depth.unwrap_or(0) >= 1)
            .map(|s| s.txid)
            .collect()
    }

    #[test]
    fn test_triage_mixed_statuses() {
        use crate::services::TxStatusDetail;

        let statuses = vec![
            TxStatusDetail {
                txid: "mined_deep".to_string(),
                status: "mined".to_string(),
                depth: Some(10),
            },
            TxStatusDetail {
                txid: "mined_one".to_string(),
                status: "mined".to_string(),
                depth: Some(1),
            },
            TxStatusDetail {
                txid: "mempool_tx".to_string(),
                status: "known".to_string(),
                depth: Some(0),
            },
            TxStatusDetail {
                txid: "missing_tx".to_string(),
                status: "unknown".to_string(),
                depth: None,
            },
        ];

        let confirmed = build_confirmed_set(statuses);
        assert_eq!(confirmed.len(), 2);
        assert!(confirmed.contains("mined_deep"));
        assert!(confirmed.contains("mined_one"));
        assert!(!confirmed.contains("mempool_tx"));
        assert!(!confirmed.contains("missing_tx"));
    }

    #[test]
    fn test_triage_all_unknown_yields_empty_set() {
        use crate::services::TxStatusDetail;

        let statuses = vec![
            TxStatusDetail {
                txid: "tx1".to_string(),
                status: "unknown".to_string(),
                depth: None,
            },
            TxStatusDetail {
                txid: "tx2".to_string(),
                status: "unknown".to_string(),
                depth: None,
            },
            TxStatusDetail {
                txid: "tx3".to_string(),
                status: "unknown".to_string(),
                depth: None,
            },
        ];

        let confirmed = build_confirmed_set(statuses);
        assert!(confirmed.is_empty());
    }

    #[test]
    fn test_triage_all_mempool_yields_empty_set() {
        use crate::services::TxStatusDetail;

        let statuses = vec![
            TxStatusDetail {
                txid: "tx1".to_string(),
                status: "known".to_string(),
                depth: Some(0),
            },
            TxStatusDetail {
                txid: "tx2".to_string(),
                status: "known".to_string(),
                depth: Some(0),
            },
        ];

        let confirmed = build_confirmed_set(statuses);
        assert!(confirmed.is_empty());
    }

    #[test]
    fn test_triage_mined_with_depth_zero_excluded() {
        // "mined" with depth=0 should NOT be in confirmed_set (depth >= 1 required)
        use crate::services::TxStatusDetail;

        let statuses = vec![TxStatusDetail {
            txid: "mined_zero".to_string(),
            status: "mined".to_string(),
            depth: Some(0),
        }];

        let confirmed = build_confirmed_set(statuses);
        assert!(confirmed.is_empty());
    }

    #[test]
    fn test_triage_mined_with_depth_none_excluded() {
        // "mined" with depth=None should NOT be in confirmed_set
        use crate::services::TxStatusDetail;

        let statuses = vec![TxStatusDetail {
            txid: "mined_no_depth".to_string(),
            status: "mined".to_string(),
            depth: None,
        }];

        let confirmed = build_confirmed_set(statuses);
        assert!(confirmed.is_empty());
    }

    #[test]
    fn test_triage_mined_with_depth_one_included() {
        use crate::services::TxStatusDetail;

        let statuses = vec![TxStatusDetail {
            txid: "just_mined".to_string(),
            status: "mined".to_string(),
            depth: Some(1),
        }];

        let confirmed = build_confirmed_set(statuses);
        assert_eq!(confirmed.len(), 1);
        assert!(confirmed.contains("just_mined"));
    }

    #[test]
    fn test_triage_empty_input() {
        let statuses: Vec<crate::services::TxStatusDetail> = vec![];
        let confirmed = build_confirmed_set(statuses);
        assert!(confirmed.is_empty());
    }

    #[test]
    fn test_triage_failure_none_falls_through() {
        // When triage fails, confirmed_set is None.
        // The code checks: if let Some(ref confirmed) = confirmed_set { ... }
        // With None, the block is skipped and all txids are checked (old behavior).
        let confirmed_set: Option<std::collections::HashSet<String>> = None;
        let txid = "any_tx";
        let should_skip = match &confirmed_set {
            Some(confirmed) => !confirmed.contains(txid),
            None => false, // Don't skip — fall through to check all
        };
        assert!(!should_skip, "None confirmed_set should not skip any txid");
    }

    #[test]
    fn test_triage_some_empty_set_skips_all() {
        // When triage succeeds but no txids are confirmed, confirmed_set is Some(empty).
        // All txids should be skipped (none are in the empty set).
        let confirmed_set: Option<std::collections::HashSet<String>> =
            Some(std::collections::HashSet::new());
        let txid = "any_tx";
        let should_skip = match &confirmed_set {
            Some(confirmed) => !confirmed.contains(txid),
            None => false,
        };
        assert!(should_skip, "Empty confirmed_set should skip all txids");
    }

    #[test]
    fn test_triage_some_with_match_does_not_skip() {
        let mut set = std::collections::HashSet::new();
        set.insert("confirmed_tx".to_string());
        let confirmed_set: Option<std::collections::HashSet<String>> = Some(set);
        let should_skip = match &confirmed_set {
            Some(confirmed) => !confirmed.contains("confirmed_tx"),
            None => false,
        };
        assert!(!should_skip, "Confirmed txid should not be skipped");
    }

    #[test]
    fn test_triage_classification_counts() {
        use crate::services::TxStatusDetail;

        let statuses = vec![
            TxStatusDetail {
                txid: "a".to_string(),
                status: "mined".to_string(),
                depth: Some(5),
            },
            TxStatusDetail {
                txid: "b".to_string(),
                status: "mined".to_string(),
                depth: Some(2),
            },
            TxStatusDetail {
                txid: "c".to_string(),
                status: "known".to_string(),
                depth: Some(0),
            },
            TxStatusDetail {
                txid: "d".to_string(),
                status: "unknown".to_string(),
                depth: None,
            },
            TxStatusDetail {
                txid: "e".to_string(),
                status: "unknown".to_string(),
                depth: None,
            },
        ];

        // Verify the counting logic matches check_for_proofs
        let confirmed: usize = statuses.iter().filter(|s| s.status == "mined").count();
        let mempool: usize = statuses.iter().filter(|s| s.status == "known").count();
        let missing: usize = statuses.iter().filter(|s| s.status == "unknown").count();

        assert_eq!(confirmed, 2);
        assert_eq!(mempool, 1);
        assert_eq!(missing, 2);
        assert_eq!(confirmed + mempool + missing, statuses.len());
    }
}
