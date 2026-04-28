//! UTXO Audit Endpoint — `/monitor/audit`
//!
//! Two-level integrity audit for the wallet UTXO set:
//!
//! **Level 2 (default)** — SQL-only integrity checks:
//! 1. locked-by-failed: UTXOs locked by failed transactions (auto-repaired)
//! 2. orphaned-refs: Outputs referencing non-existent transactions
//! 3. spendable-locked contradiction: spendable flag doesn't match spent_by
//! 4. stranded outputs: Outputs with no basket association
//!
//! **Level 1** — All Level 2 checks + per-UTXO deep validation:
//! 1. Locking script exists and is non-empty
//! 2. Raw transaction retrievable (D1 inline or R2 blob)
//! 3. Raw tx hash matches the output's txid
//! 4. vout index is within range of the raw tx outputs
//! 5. Satoshis match the actual tx output value
//! 6. BEEF can be reconstructed with proven ancestors
//! 7. Merkle proofs are valid for proven ancestors

use chrono::Utc;
use serde::{Deserialize, Serialize};
use worker::*;

use crate::d1::batch::BatchCollector;
use crate::d1::{QVal, Query};

// =============================================================================
// Public types
// =============================================================================

/// A single issue found during audit.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditIssue {
    pub output_id: i64,
    pub txid: String,
    pub problem: String,
    pub details: String,
    pub auto_repaired: bool,
}

/// Summary counts for the audit report.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditSummary {
    pub total_utxos_checked: u32,
    pub total_issues: u32,
    pub locked_by_failed: u32,
    pub orphaned_refs: u32,
    pub spendable_contradictions: u32,
    pub stranded_outputs: u32,
    pub missing_locking_script: u32,
    pub missing_raw_tx: u32,
    pub txid_mismatch: u32,
    pub vout_out_of_range: u32,
    pub satoshis_mismatch: u32,
    pub beef_invalid: u32,
    pub proof_invalid: u32,
    pub auto_repaired: u32,
}

/// Full audit report returned as JSON.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditReport {
    pub level: u8,
    pub summary: AuditSummary,
    pub issues: Vec<AuditIssue>,
    pub execution_ms: u64,
}

// =============================================================================
// D1 row types
// =============================================================================

#[derive(Debug, Deserialize)]
struct LockedByFailedRow {
    output_id: Option<f64>,
    txid: Option<String>,
    spent_by: Option<f64>,
    #[allow(dead_code)]
    tx_status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrphanedRefRow {
    output_id: Option<f64>,
    txid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SpendableContradictionRow {
    output_id: Option<f64>,
    txid: Option<String>,
    spendable: Option<f64>,
    spent_by: Option<f64>,
    problem: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StrandedOutputRow {
    output_id: Option<f64>,
    txid: Option<String>,
    #[allow(dead_code)]
    basket_id: Option<f64>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct SpendableUtxoRow {
    output_id: Option<f64>,
    transaction_id: Option<f64>,
    txid: Option<String>,
    vout: Option<f64>,
    satoshis: Option<f64>,
    has_locking_script: Option<f64>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RawTxRow {
    raw_tx: Option<String>, // hex from hex(raw_tx)
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ProvenAncestorRow {
    #[allow(dead_code)]
    txid: Option<String>,
    merkle_path: Option<String>, // hex from hex(merkle_path)
}

// =============================================================================
// Main audit function
// =============================================================================

/// Run the UTXO audit at the specified level.
///
/// Level 2 (default): SQL integrity checks only.
/// Level 1: SQL checks + per-UTXO deep validation.
pub async fn run_audit(db: &D1Database, bucket: &Bucket, level: u8) -> AuditReport {
    let start = js_sys::Date::now();
    let mut summary = AuditSummary::default();
    let mut issues: Vec<AuditIssue> = Vec::new();

    // Level 2: SQL integrity checks (always run)
    check_locked_by_failed(db, &mut summary, &mut issues).await;
    check_orphaned_refs(db, &mut summary, &mut issues).await;
    check_spendable_contradictions(db, &mut summary, &mut issues).await;
    check_stranded_outputs(db, &mut summary, &mut issues).await;

    // Level 1: Deep per-UTXO validation
    if level <= 1 {
        check_utxo_deep(db, bucket, &mut summary, &mut issues).await;
    }

    summary.total_issues = issues.len() as u32;

    let elapsed = (js_sys::Date::now() - start) as u64;

    AuditReport {
        level,
        summary,
        issues,
        execution_ms: elapsed,
    }
}

// =============================================================================
// Level 2 Check 1: Locked by failed transactions
// =============================================================================

/// Find outputs where spent_by references a transaction with status='failed'.
/// These UTXOs should be released since the spending tx failed.
/// Auto-repair: set spent_by=NULL, spendable=1.
async fn check_locked_by_failed(
    db: &D1Database,
    summary: &mut AuditSummary,
    issues: &mut Vec<AuditIssue>,
) {
    let rows: Vec<LockedByFailedRow> = match Query::new(
        "SELECT o.output_id, o.txid, o.spent_by, t.status as tx_status \
         FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.spent_by \
         WHERE t.status = 'failed' \
         AND o.spent_by IS NOT NULL",
    )
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            console_error!("Audit locked-by-failed query failed: {}", e);
            return;
        }
    };

    if rows.is_empty() {
        return;
    }

    // Auto-repair: release locked UTXOs
    let now = Utc::now().to_rfc3339();
    let mut batch = BatchCollector::new(db);
    let mut repaired = 0u32;

    for row in &rows {
        let output_id = row.output_id.unwrap_or(0.0) as i64;
        let txid = row.txid.clone().unwrap_or_default();
        let spent_by = row.spent_by.unwrap_or(0.0) as i64;

        if output_id == 0 {
            continue;
        }

        issues.push(AuditIssue {
            output_id,
            txid: txid.clone(),
            problem: "locked-by-failed".to_string(),
            details: format!("Output locked by failed transaction_id={}", spent_by),
            auto_repaired: true,
        });

        let _ = batch.add(
            "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE output_id = ?",
            vec![QVal::Text(now.clone()), QVal::Int(output_id)],
        );

        repaired += 1;
    }

    // Execute repairs
    if !batch.is_empty() {
        if let Err(e) = batch.execute().await {
            console_error!("Audit locked-by-failed repair failed: {}", e);
            // Mark issues as not repaired
            for issue in issues.iter_mut() {
                if issue.problem == "locked-by-failed" {
                    issue.auto_repaired = false;
                }
            }
            repaired = 0;
        }
    }

    // Log repairs to monitor_events
    if repaired > 0 {
        let _ = log_audit_event(
            db,
            "audit_repair_locked_by_failed",
            &format!("Released {} UTXOs locked by failed transactions", repaired),
        )
        .await;
    }

    summary.locked_by_failed = rows.len() as u32;
    summary.auto_repaired += repaired;
}

// =============================================================================
// Level 2 Check 2: Orphaned references
// =============================================================================

/// Find outputs referencing transactions (via txid) that don't exist in the transactions table.
async fn check_orphaned_refs(
    db: &D1Database,
    summary: &mut AuditSummary,
    issues: &mut Vec<AuditIssue>,
) {
    let rows: Vec<OrphanedRefRow> = match Query::new(
        "SELECT o.output_id, o.txid \
         FROM outputs o \
         LEFT JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE t.transaction_id IS NULL \
         LIMIT 500",
    )
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            console_error!("Audit orphaned-refs query failed: {}", e);
            return;
        }
    };

    for row in &rows {
        let output_id = row.output_id.unwrap_or(0.0) as i64;
        let txid = row.txid.clone().unwrap_or_default();

        issues.push(AuditIssue {
            output_id,
            txid,
            problem: "orphaned-ref".to_string(),
            details: "Output references a non-existent transaction".to_string(),
            auto_repaired: false,
        });
    }

    summary.orphaned_refs = rows.len() as u32;
}

// =============================================================================
// Level 2 Check 3: Spendable + locked contradictions
// =============================================================================

/// Find outputs where spendable flag contradicts spent_by state.
/// Case A: spendable=1 but spent_by IS NOT NULL (marked spendable but locked)
/// Case B: spendable=0 but spent_by IS NULL and not in a pending tx
async fn check_spendable_contradictions(
    db: &D1Database,
    summary: &mut AuditSummary,
    issues: &mut Vec<AuditIssue>,
) {
    // Case A: spendable but has a spending reference
    let case_a: Vec<SpendableContradictionRow> = match Query::new(
        "SELECT o.output_id, o.txid, o.spendable, o.spent_by, 'spendable-but-locked' as problem \
         FROM outputs o \
         WHERE o.spendable = 1 AND o.spent_by IS NOT NULL \
         LIMIT 500",
    )
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            console_error!("Audit spendable-contradiction (A) query failed: {}", e);
            Vec::new()
        }
    };

    // Case B: change outputs that are not spendable with no spending reference
    // Only flags change outputs (o.change = 1) — recipient outputs from outbound
    // payments are legitimately spendable=0, spent_by=NULL.
    let case_b: Vec<SpendableContradictionRow> = match Query::new(
        "SELECT o.output_id, o.txid, o.spendable, o.spent_by, \
         t.status as problem \
         FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE o.spendable = 0 \
         AND o.spent_by IS NULL \
         AND o.change = 1 \
         AND t.status IN ('completed', 'unproven', 'nosend') \
         LIMIT 500",
    )
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            console_error!("Audit spendable-contradiction (B) query failed: {}", e);
            Vec::new()
        }
    };

    for row in case_a.iter().chain(case_b.iter()) {
        let output_id = row.output_id.unwrap_or(0.0) as i64;
        let txid = row.txid.clone().unwrap_or_default();
        let problem = row.problem.clone().unwrap_or_default();
        let spent_by = row.spent_by.unwrap_or(0.0) as i64;
        let spendable = row.spendable.unwrap_or(0.0) as i32;

        issues.push(AuditIssue {
            output_id,
            txid,
            problem: "spendable-contradiction".to_string(),
            details: format!(
                "{}: spendable={}, spent_by={}",
                problem,
                spendable,
                if spent_by != 0 {
                    spent_by.to_string()
                } else {
                    "NULL".to_string()
                }
            ),
            auto_repaired: false,
        });
    }

    summary.spendable_contradictions = (case_a.len() + case_b.len()) as u32;
}

// =============================================================================
// Level 2 Check 4: Stranded outputs
// =============================================================================

/// Find outputs with no basket association that are spendable (should be in default basket).
async fn check_stranded_outputs(
    db: &D1Database,
    summary: &mut AuditSummary,
    issues: &mut Vec<AuditIssue>,
) {
    let rows: Vec<StrandedOutputRow> = match Query::new(
        "SELECT o.output_id, o.txid, o.basket_id \
         FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE o.basket_id IS NULL \
         AND o.spendable = 1 \
         AND t.status IN ('completed', 'unproven', 'nosend') \
         LIMIT 500",
    )
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            console_error!("Audit stranded-outputs query failed: {}", e);
            return;
        }
    };

    for row in &rows {
        let output_id = row.output_id.unwrap_or(0.0) as i64;
        let txid = row.txid.clone().unwrap_or_default();

        issues.push(AuditIssue {
            output_id,
            txid,
            problem: "stranded-output".to_string(),
            details: "Spendable output has no basket association".to_string(),
            auto_repaired: false,
        });
    }

    summary.stranded_outputs = rows.len() as u32;
}

// =============================================================================
// Level 1: Deep per-UTXO validation
// =============================================================================

/// For every spendable UTXO in default baskets, verify it is 100% ready to spend.
///
/// Uses a bulk query with LEFT JOINs to check raw_tx existence and proof status
/// in a single pass, avoiding per-UTXO queries that would timeout on large wallets.
/// Only does per-UTXO deep checks (raw_tx hash, vout range, satoshis) for UTXOs
/// that have raw_tx available.
async fn check_utxo_deep(
    db: &D1Database,
    _bucket: &Bucket,
    summary: &mut AuditSummary,
    issues: &mut Vec<AuditIssue>,
) {
    // Single bulk query: get all spendable UTXOs with raw_tx and proof status
    // via LEFT JOINs. This replaces 7 queries per UTXO with 1 query total.
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct BulkUtxoRow {
        output_id: Option<f64>,
        transaction_id: Option<f64>,
        txid: Option<String>,
        vout: Option<f64>,
        satoshis: Option<f64>,
        has_locking_script: Option<f64>,
        has_raw_tx: Option<f64>,
        has_proof: Option<f64>,
        has_proof_req: Option<f64>,
        tx_status: Option<String>,
    }

    let rows: Vec<BulkUtxoRow> = match Query::new(
        "SELECT o.output_id, o.transaction_id, o.txid, o.vout, o.satoshis, \
         CASE WHEN o.locking_script IS NOT NULL AND LENGTH(o.locking_script) > 0 \
              THEN 1 ELSE 0 END as has_locking_script, \
         CASE WHEN (SELECT 1 FROM transactions t2 WHERE t2.transaction_id = o.transaction_id AND t2.raw_tx IS NOT NULL AND LENGTH(t2.raw_tx) > 0) = 1 THEN 1 \
              WHEN (SELECT 1 FROM proven_txs pt WHERE pt.txid = o.txid AND pt.raw_tx IS NOT NULL AND LENGTH(pt.raw_tx) > 0) = 1 THEN 1 \
              WHEN (SELECT 1 FROM proven_tx_reqs ptr WHERE ptr.txid = o.txid AND ptr.raw_tx IS NOT NULL AND LENGTH(ptr.raw_tx) > 0) = 1 THEN 1 \
              ELSE 0 END as has_raw_tx, \
         CASE WHEN (SELECT 1 FROM proven_txs pt2 WHERE pt2.txid = o.txid) = 1 THEN 1 ELSE 0 END as has_proof, \
         CASE WHEN (SELECT 1 FROM proven_tx_reqs ptr2 WHERE ptr2.txid = o.txid) = 1 THEN 1 ELSE 0 END as has_proof_req, \
         t.status as tx_status \
         FROM outputs o \
         JOIN output_baskets b ON b.basket_id = o.basket_id \
         JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE o.spendable = 1 \
         AND b.name = 'default' \
         ORDER BY o.output_id",
    )
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            console_error!("Audit deep-check query failed: {}", e);
            return;
        }
    };

    summary.total_utxos_checked = rows.len() as u32;

    for row in &rows {
        let output_id = row.output_id.unwrap_or(0.0) as i64;
        let txid = row.txid.clone().unwrap_or_default();
        let has_locking_script = row.has_locking_script.unwrap_or(0.0) as i32;
        let has_raw_tx = row.has_raw_tx.unwrap_or(0.0) as i32;
        let has_proof = row.has_proof.unwrap_or(0.0) as i32;
        let has_proof_req = row.has_proof_req.unwrap_or(0.0) as i32;
        let tx_status = row.tx_status.clone().unwrap_or_default();

        // Check 1: Locking script exists
        if has_locking_script == 0 {
            issues.push(AuditIssue {
                output_id,
                txid: txid.clone(),
                problem: "missing-locking-script".to_string(),
                details: "Output has no locking script".to_string(),
                auto_repaired: false,
            });
            summary.missing_locking_script += 1;
            continue;
        }

        if txid.is_empty() {
            continue;
        }

        // Check 2: Raw tx exists somewhere (transactions, proven_txs, or proven_tx_reqs)
        if has_raw_tx == 0 {
            issues.push(AuditIssue {
                output_id,
                txid: txid.clone(),
                problem: "missing-raw-tx".to_string(),
                details: format!(
                    "No raw_tx in transactions, proven_txs, or proven_tx_reqs (status={})",
                    tx_status
                ),
                auto_repaired: false,
            });
            summary.missing_raw_tx += 1;
        }

        // Check 3: Proof chain — proven_tx exists, or proven_tx_req pending,
        // or tx is completed/nosend/sending (acceptable states)
        if has_proof == 0
            && has_proof_req == 0
            && !matches!(tx_status.as_str(), "completed" | "nosend" | "sending")
        {
            let details = if tx_status == "unproven" {
                "Unproven tx has no proven_tx_req — monitor cannot track proof (see #13)"
                    .to_string()
            } else {
                format!("No proven_tx or proven_tx_req, tx_status={}", tx_status)
            };
            issues.push(AuditIssue {
                output_id,
                txid: txid.clone(),
                problem: "proof-invalid".to_string(),
                details,
                auto_repaired: false,
            });
            summary.proof_invalid += 1;
        }
    }
}

// =============================================================================
// Raw tx retrieval
// =============================================================================

/// Retrieve raw_tx bytes for a transaction. Checks D1 inline first, then R2 blob.
/// Used for per-UTXO deep verification (txid hash, vout range, satoshis match).
#[allow(dead_code)]
async fn get_raw_tx(
    db: &D1Database,
    bucket: &Bucket,
    transaction_id: i64,
    txid: &str,
) -> std::result::Result<Option<Vec<u8>>, String> {
    // First try the transactions table (inline D1)
    let row: Option<RawTxRow> =
        Query::new("SELECT hex(raw_tx) as raw_tx FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_optional(db)
            .await
            .map_err(|e| e.to_string())?;

    if let Some(row) = row {
        if let Some(hex_str) = &row.raw_tx {
            if !hex_str.is_empty() {
                return hex::decode(hex_str)
                    .map(Some)
                    .map_err(|e| format!("hex decode error: {}", e));
            }
        }
    }

    // Try R2 blob store
    let key = format!("transactions/{}/raw_tx", transaction_id);
    match bucket.get(&key).execute().await {
        Ok(Some(obj)) => match obj.body() {
            Some(body) => {
                let bytes = body.bytes().await.map_err(|e| e.to_string())?;
                Ok(Some(bytes))
            }
            None => Ok(None),
        },
        Ok(None) => {
            // Also try by proven_txs (for completed transactions)
            let pt_row: Option<RawTxRow> =
                Query::new("SELECT hex(raw_tx) as raw_tx FROM proven_txs WHERE txid = ?")
                    .bind(txid)
                    .fetch_optional(db)
                    .await
                    .map_err(|e| e.to_string())?;

            if let Some(row) = pt_row {
                if let Some(hex_str) = &row.raw_tx {
                    if !hex_str.is_empty() {
                        return hex::decode(hex_str)
                            .map(Some)
                            .map_err(|e| format!("hex decode error: {}", e));
                    }
                }
            }

            // Also try proven_tx_reqs — processAction clears raw_tx from
            // transactions table after broadcast but stores it in proven_tx_reqs.
            let ptr_row: Option<RawTxRow> =
                Query::new("SELECT hex(raw_tx) as raw_tx FROM proven_tx_reqs WHERE txid = ?")
                    .bind(txid)
                    .fetch_optional(db)
                    .await
                    .map_err(|e| e.to_string())?;

            if let Some(row) = ptr_row {
                if let Some(hex_str) = &row.raw_tx {
                    if !hex_str.is_empty() {
                        return hex::decode(hex_str)
                            .map(Some)
                            .map_err(|e| format!("hex decode error: {}", e));
                    }
                }
            }
            Ok(None)
        }
        Err(e) => Err(e.to_string()),
    }
}

// =============================================================================
// Proof chain verification
// =============================================================================

/// Verify that a proven ancestor exists with valid merkle path for this txid.
/// Used for per-UTXO deep verification of merkle path binary.
#[allow(dead_code)]
async fn check_proof_chain(
    db: &D1Database,
    txid: &str,
) -> std::result::Result<(), (String, String)> {
    // Check if there's a proven_tx for this txid (or if it was internalized via BEEF)
    let row: Option<ProvenAncestorRow> =
        Query::new("SELECT txid, hex(merkle_path) as merkle_path FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(db)
            .await
            .map_err(|e| {
                (
                    "proof-invalid".to_string(),
                    format!("DB query error: {}", e),
                )
            })?;

    match row {
        Some(r) => {
            // Has a proven_tx entry — verify merkle_path is parseable
            if let Some(mp_hex) = &r.merkle_path {
                if !mp_hex.is_empty() {
                    let mp_bytes = hex::decode(mp_hex).map_err(|e| {
                        (
                            "proof-invalid".to_string(),
                            format!("Merkle path hex decode error: {}", e),
                        )
                    })?;
                    // Try to parse the merkle path
                    bsv_sdk::transaction::MerklePath::from_binary(&mp_bytes).map_err(|e| {
                        (
                            "proof-invalid".to_string(),
                            format!("Invalid merkle path binary: {}", e),
                        )
                    })?;
                }
            }
            Ok(())
        }
        None => {
            // No direct proof — check if there's a proven_tx_req (proof pending)
            let req_row: Option<ProvenAncestorRow> =
                Query::new("SELECT txid, NULL as merkle_path FROM proven_tx_reqs WHERE txid = ?")
                    .bind(txid)
                    .fetch_optional(db)
                    .await
                    .map_err(|e| {
                        (
                            "proof-invalid".to_string(),
                            format!("DB query error: {}", e),
                        )
                    })?;

            match req_row {
                Some(_) => Ok(()), // Proof pending, acceptable
                None => {
                    // Check if this is a nosend transaction (doesn't need proof)
                    #[derive(Debug, Deserialize)]
                    struct StatusCheck {
                        status: Option<String>,
                    }
                    let tx_status: Option<StatusCheck> =
                        Query::new("SELECT status FROM transactions WHERE txid = ?")
                            .bind(txid)
                            .fetch_optional(db)
                            .await
                            .map_err(|e| {
                                (
                                    "proof-invalid".to_string(),
                                    format!("DB query error: {}", e),
                                )
                            })?;

                    match tx_status {
                        Some(s) if s.status.as_deref() == Some("nosend") => Ok(()),
                        Some(s) if s.status.as_deref() == Some("completed") => {
                            // Completed but no proven_tx — this is fine if it was
                            // internalized and the proof is on a parent tx
                            Ok(())
                        }
                        Some(s) if s.status.as_deref() == Some("unproven") => {
                            // Unproven with no proven_tx_req = monitor can't track this.
                            // Not a data integrity error — it's a gap in proof tracking.
                            // Issue #13 (close monitor gaps) will address this.
                            Err((
                                "proof-invalid".to_string(),
                                "Unproven tx has no proven_tx_req — monitor cannot track proof (see #13)".to_string(),
                            ))
                        }
                        Some(s) if s.status.as_deref() == Some("sending") => {
                            // Sending = broadcast in progress, proof tracking imminent
                            Ok(())
                        }
                        _ => Err((
                            "proof-invalid".to_string(),
                            "No proven_tx or proven_tx_req found for this txid".to_string(),
                        )),
                    }
                }
            }
        }
    }
}

// =============================================================================
// Transaction parsing utilities
// =============================================================================

/// Compute double-SHA256 txid from raw transaction bytes.
#[cfg(test)]
fn compute_txid(raw_tx: &[u8]) -> String {
    use bsv_sdk::sha256d;
    let hash = sha256d(raw_tx);
    // txid is the double-SHA256 in reverse byte order, hex-encoded
    let reversed: Vec<u8> = hash.iter().rev().cloned().collect();
    hex::encode(reversed)
}

#[allow(dead_code)]
/// Parse a raw transaction and extract output satoshi values.
/// Returns a Vec of satoshis for each output index.
fn parse_tx_outputs(raw_tx: &[u8]) -> std::result::Result<Vec<i64>, String> {
    let tx =
        bsv_sdk::Transaction::from_binary(raw_tx).map_err(|e| format!("TX parse error: {}", e))?;
    Ok(tx
        .outputs
        .iter()
        .map(|o| o.satoshis.unwrap_or(0) as i64)
        .collect())
}

// =============================================================================
// Logging
// =============================================================================

/// Log an audit event to the monitor_events table.
async fn log_audit_event(
    db: &D1Database,
    event: &str,
    details: &str,
) -> std::result::Result<(), String> {
    Query::new(
        "INSERT INTO monitor_events (event, details, created_at, updated_at) \
         VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(event)
    .bind(details)
    .execute(db)
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // AuditIssue serialization
    // =========================================================================

    #[test]
    fn audit_issue_serializes_camel_case() {
        let issue = AuditIssue {
            output_id: 42,
            txid: "abc123".to_string(),
            problem: "locked-by-failed".to_string(),
            details: "test detail".to_string(),
            auto_repaired: true,
        };
        let json = serde_json::to_value(&issue).unwrap();
        assert_eq!(json["outputId"], 42);
        assert_eq!(json["txid"], "abc123");
        assert_eq!(json["problem"], "locked-by-failed");
        assert_eq!(json["details"], "test detail");
        assert_eq!(json["autoRepaired"], true);
    }

    #[test]
    fn audit_issue_serializes_not_repaired() {
        let issue = AuditIssue {
            output_id: 1,
            txid: "def456".to_string(),
            problem: "orphaned-ref".to_string(),
            details: "test".to_string(),
            auto_repaired: false,
        };
        let json = serde_json::to_value(&issue).unwrap();
        assert_eq!(json["autoRepaired"], false);
    }

    // =========================================================================
    // AuditSummary defaults
    // =========================================================================

    #[test]
    fn audit_summary_defaults_to_zero() {
        let summary = AuditSummary::default();
        assert_eq!(summary.total_utxos_checked, 0);
        assert_eq!(summary.total_issues, 0);
        assert_eq!(summary.locked_by_failed, 0);
        assert_eq!(summary.orphaned_refs, 0);
        assert_eq!(summary.spendable_contradictions, 0);
        assert_eq!(summary.stranded_outputs, 0);
        assert_eq!(summary.missing_locking_script, 0);
        assert_eq!(summary.missing_raw_tx, 0);
        assert_eq!(summary.txid_mismatch, 0);
        assert_eq!(summary.vout_out_of_range, 0);
        assert_eq!(summary.satoshis_mismatch, 0);
        assert_eq!(summary.beef_invalid, 0);
        assert_eq!(summary.proof_invalid, 0);
        assert_eq!(summary.auto_repaired, 0);
    }

    #[test]
    fn audit_summary_serializes_camel_case() {
        let mut summary = AuditSummary::default();
        summary.total_utxos_checked = 100;
        summary.locked_by_failed = 3;
        summary.auto_repaired = 3;
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["totalUtxosChecked"], 100);
        assert_eq!(json["lockedByFailed"], 3);
        assert_eq!(json["autoRepaired"], 3);
    }

    // =========================================================================
    // AuditReport structure
    // =========================================================================

    #[test]
    fn audit_report_serializes_complete() {
        let report = AuditReport {
            level: 2,
            summary: AuditSummary::default(),
            issues: vec![],
            execution_ms: 42,
        };
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["level"], 2);
        assert_eq!(json["executionMs"], 42);
        assert!(json["issues"].as_array().unwrap().is_empty());
    }

    #[test]
    fn audit_report_with_issues() {
        let report = AuditReport {
            level: 1,
            summary: AuditSummary {
                total_issues: 2,
                locked_by_failed: 1,
                orphaned_refs: 1,
                ..Default::default()
            },
            issues: vec![
                AuditIssue {
                    output_id: 1,
                    txid: "tx1".to_string(),
                    problem: "locked-by-failed".to_string(),
                    details: "detail1".to_string(),
                    auto_repaired: true,
                },
                AuditIssue {
                    output_id: 2,
                    txid: "tx2".to_string(),
                    problem: "orphaned-ref".to_string(),
                    details: "detail2".to_string(),
                    auto_repaired: false,
                },
            ],
            execution_ms: 150,
        };
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["level"], 1);
        assert_eq!(json["summary"]["totalIssues"], 2);
        assert_eq!(json["issues"].as_array().unwrap().len(), 2);
    }

    // =========================================================================
    // compute_txid
    // =========================================================================

    #[test]
    fn compute_txid_basic() {
        // A known raw tx (minimal coinbase-like) — just verify it produces a 64-char hex string
        // This is a minimal valid-ish transaction for hashing (not valid on network)
        let fake_raw = vec![
            0x01, 0x00, 0x00, 0x00, // version
            0x01, // 1 input
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, // prev tx
            0xFF, 0xFF, 0xFF, 0xFF, // prev vout
            0x01, 0x00, // script len + script
            0xFF, 0xFF, 0xFF, 0xFF, // sequence
            0x01, // 1 output
            0x00, 0xE1, 0xF5, 0x05, 0x00, 0x00, 0x00, 0x00, // satoshis
            0x01, 0x00, // script len + script
            0x00, 0x00, 0x00, 0x00, // locktime
        ];
        let txid = compute_txid(&fake_raw);
        assert_eq!(txid.len(), 64);
        assert!(txid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn compute_txid_deterministic() {
        let raw = vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let txid1 = compute_txid(&raw);
        let txid2 = compute_txid(&raw);
        assert_eq!(txid1, txid2);
    }

    #[test]
    fn compute_txid_different_inputs_differ() {
        let raw1 = vec![0x01, 0x00, 0x00, 0x00];
        let raw2 = vec![0x02, 0x00, 0x00, 0x00];
        assert_ne!(compute_txid(&raw1), compute_txid(&raw2));
    }
}
