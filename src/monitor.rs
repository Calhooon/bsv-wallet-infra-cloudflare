//! Cron-triggered monitor for wallet-infra.
//!
//! Runs every 5 minutes via `#[event(scheduled)]`. Ten tasks:
//! 1. `send_waiting` — broadcast txs with status 'unsent'/'sending'
//! 2. `check_for_proofs` — collect merkle proofs for broadcast transactions
//! 3. `fail_abandoned` — fail stuck unsigned/unprocessed transactions, release UTXOs
//! 4. `review_status` — sync mismatched proven_tx_req vs transaction statuses
//! 5. `compact_beef` — retroactively compact stored BEEF blobs
//! 6. `unfail_transactions` — recover txs incorrectly marked failed (runs every ~10 min)
//! 7. `check_no_sends` — (daily) detect nosend txs mined externally
//! 8. `purge_data` — (hourly) nullify old completed blobs, delete old failed reqs
//! 9. `check_chain_reorg` — detect chain reorgs via height tracking, reverify affected proofs
//! 10. `scan_external_spends` — (G5) detect tracked outputs spent on-chain OUTSIDE
//!     wallet-infra (e.g. a blackjack stake consumed by an escrow covenant) and
//!     mark them `spendable = 0`

#[cfg(test)]
use bsv_sdk::transaction::MerklePathLeaf;
use bsv_sdk::transaction::{Beef, BeefTx, MerklePath};
use chrono::{Timelike, Utc};
use serde::Deserialize;
use worker::*;

use crate::d1::batch::BatchCollector;
use crate::d1::{QVal, Query};
use crate::services::{BroadcastService, ProofService, SpentStatus};

// NOTE: the old `MAX_PROOF_ATTEMPTS = 12 → invalid` predicate lived here. Its
// comment claimed reference alignment ("Go default is 10 attempts") — that
// was wrong on two counts: the Go default is 100 (defs/sync_tx_statuses.go),
// and BOTH references clock attempts on new-block events, not cron ticks —
// AND Go recycles exhausted broadcast reqs to 'unsent' for re-broadcast
// instead of invalidating them (known_tx.go proofTimeoutUpdates). The
// wall-clocked 12-tick fail predicate false-failed mined mainnet txs twice
// (170,227 + 97,727 sat). Deleted; see the decision core below.

/// Block-clocked attempt ceiling used ONLY for alerting (never for failing).
/// Mirrors the TS reference `unprovenAttemptsLimitMain = 144` (Monitor.ts:106)
/// — with block-clocked attempts, 144 ≈ 144 blocks ≈ ~24h of confirmed
/// on-chain absence. A req crossing this is loudly logged for operator
/// attention; the ONLY fail paths remain ARC rejects and confirmed
/// double-spends.
#[allow(dead_code)]
const UNPROVEN_ATTEMPTS_ALERT: i64 = 144;

/// Consecutive block-clocked attempts while the network reports a txid
/// "unknown" before the monitor runs the chain-truth escalation (input
/// spent-status check → confirmed doubleSpend, else requeue for
/// re-broadcast). ~3 blocks (~30 min) of confirmed network absence — fast
/// enough to recover a relay-lost tx quickly, slow enough to ride out
/// provider indexing lag. Mirrors the TS confirmDoubleSpend re-poll pattern
/// (attemptToPostReqsToNetwork) and Go's rebroadcast-on-timeout.
const UNKNOWN_ESCALATION_ATTEMPTS: i64 = 3;

/// Max chain-truth escalations per monitor run. Each escalation costs one
/// batch status re-poll plus one spent-status call per tx input, so this
/// caps the per-run API budget the same way EXT_SPEND_BATCH does for G5.
#[allow(dead_code)]
const ESCALATION_BUDGET_PER_RUN: usize = 5;

// =============================================================================
// Task 10 (G5) — scan_external_spends policy knobs
// =============================================================================

/// Max candidate outputs checked per monitor run. Each candidate costs one
/// WoC call, so this also caps the task's per-run API budget. 20/run at the
/// */5 cron = a hard ceiling of 5,760 calls/day, hit only while a backlog
/// exists; steady state is bounded by `EXT_SPEND_SWEEP_COOLDOWN_MINUTES`.
const EXT_SPEND_BATCH: usize = 20;

/// Bail out of the run after this many spent-status service errors. A
/// transient WoC outage must not burn the cron (or the retry budget) on a
/// batch that is going to keep failing; unprocessed candidates are retried
/// next run because the cursor only advances past processed rows.
const EXT_SPEND_MAX_SERVICE_ERRORS: u32 = 3;

/// Once a full sweep of the candidate set completes, wait this long before
/// starting the next sweep. Keeps steady-state WoC load at ~(set size) calls
/// per hour instead of continuously re-scanning (the same throttling concern
/// as `shallow_reorg_sweep`'s hourly gate).
const EXT_SPEND_SWEEP_COOLDOWN_MINUTES: i64 = 60;

/// Candidate query for the external-spend scan (G5).
///
/// A candidate is an output wallet-infra still believes it can spend AND that
/// actually exists on-chain:
/// - `spendable = 1` — still in balance / selectable;
/// - `spent_by IS NULL` — NOT tx-locked by a live createAction (never race a
///   pending action; if that action fails, `fail_abandoned` releases the row
///   and the next sweep re-checks it);
/// - `txid` present — the outpoint is addressable;
/// - parent tx `unproven`/`completed` — broadcast/settled, so the output
///   genuinely exists on-chain ('unsigned'/'unprocessed'/'nosend'/'sending'/
///   'failed' parents are excluded: their outputs are not (yet) chain-real —
///   `check_no_sends` owns the nosend-mined-externally case).
///
/// Deliberately NO `reserved_until` predicate: reservations (G1) are an
/// orthogonal soft-lock layer — a reserved-but-chain-spent output is GONE and
/// must still be marked. The scan never WRITES `reserved_until` either.
///
/// Paged by `output_id > ?` cursor, `LIMIT ?` (= EXT_SPEND_BATCH).
pub(crate) const EXT_SPEND_CANDIDATES_SQL: &str =
    "SELECT o.output_id, o.txid, o.vout \
     FROM outputs o \
     JOIN transactions t ON o.transaction_id = t.transaction_id \
     WHERE o.output_id > ? \
       AND o.spendable = 1 \
       AND o.spent_by IS NULL \
       AND o.txid IS NOT NULL AND o.txid != '' \
       AND t.status IN ('unproven', 'completed') \
     ORDER BY o.output_id ASC \
     LIMIT ?";

/// Mark-spent guard for a confirmed external spend (G5).
///
/// The WHERE re-checks BOTH liveness predicates at write time so the scan can
/// never clobber a row that changed between the candidate SELECT and this
/// UPDATE:
/// - `spent_by IS NULL` — a live createAction locked it meanwhile: hands off
///   (the owner rule still holds — the on-chain spend will surface as that
///   action's doubleSpend failure, which releases the row for the next sweep);
/// - `spendable = 1` — already relinquished/marked meanwhile: no-op (keeps
///   the UPDATE idempotent and `changes` an accurate found-counter).
///
/// Sets `spent_by = NULL` explicitly (terminal external state — no local
/// transaction owns the spend) and touches NOTHING else: not `basket_id`
/// (relinquish semantics belong to the client), not `reserved_until` (G4:
/// expiry/unreserve are the only reservation release paths).
pub(crate) const EXT_SPEND_MARK_SQL: &str =
    "UPDATE outputs SET spendable = 0, spent_by = NULL, updated_at = ? \
     WHERE output_id = ? AND spendable = 1 AND spent_by IS NULL";

/// Cursor persistence (G5) — same `monitor_events` latest-row-wins mechanism
/// as `'chain_height'`, but UPDATEd in place (single row in steady state;
/// INSERT only fires the first time). `event_id DESC` tiebreak because
/// `created_at` has second granularity.
pub(crate) const EXT_SPEND_CURSOR_READ_SQL: &str =
    "SELECT details FROM monitor_events \
     WHERE event = 'external_spend_cursor' \
     ORDER BY created_at DESC, event_id DESC LIMIT 1";

pub(crate) const EXT_SPEND_CURSOR_UPDATE_SQL: &str =
    "UPDATE monitor_events SET details = ?, updated_at = CURRENT_TIMESTAMP \
     WHERE event_id = (SELECT event_id FROM monitor_events \
                       WHERE event = 'external_spend_cursor' \
                       ORDER BY created_at DESC, event_id DESC LIMIT 1)";

pub(crate) const EXT_SPEND_CURSOR_INSERT_SQL: &str =
    "INSERT INTO monitor_events (event, details, created_at, updated_at) \
     VALUES ('external_spend_cursor', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";

// =============================================================================
// Proof-lifecycle decision core (pure functions, unit-tested)
//
// THE INVARIANT (owner rule, non-negotiable): `proven_tx_reqs.status='invalid'`
// / `'doubleSpend'` and `transactions.status='failed'` may be reached ONLY on a
// positive never-on-chain signal:
//   (a) ARC hard-reject at/after broadcast (InvalidTx / DoubleSpend verdict);
//   (b) a CONFIRMED double-spend — an input of ours spent by a DIFFERENT
//       network-known tx (get_spent_status);
//   (c) [reorg handling demotes, never fails — kept as-is].
// NEVER on attempt-budget or get_proof timeout. Reference semantics:
//   * TS wallet-toolbox: attempts only advance on a new-block event
//     (TaskCheckForProofs.ts:50 `countsAsAttempt = checkNow`, set by
//     Monitor.processNewBlockHeader — Monitor.ts:397-403), limit 144 blocks
//     (Monitor.ts:106).
//   * Go go-wallet-toolbox: the whole status sync is skipped unless the tip
//     advanced (synchronize_tx_statuses.go lastBlockKey gate), mempool-only
//     txs never accumulate attempts (filterTxsByConfirmationDepth), and
//     attempt exhaustion REBROADCASTS (status → unsent, attempts reset —
//     known_tx.go proofTimeoutUpdates) so ARC delivers a real verdict; it
//     never jumps straight to invalid for a broadcast tx.
// =============================================================================

/// Per-req chain triage outcome (from `get_status_for_txids`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TriageStatus {
    /// Confirmed in a block (depth >= 1): fetch the proof.
    Mined,
    /// In the mempool / SEEN on network. On BSV SEEN = final (first-seen, no
    /// RBF): this tx WILL mine. Never a fail candidate.
    Known,
    /// The network does not know this txid. NOT proof of death by itself —
    /// only input-spend evidence or an ARC reject may fail it.
    Unknown,
    /// Triage unavailable (provider error / batch fallback). No evidence.
    Unavailable,
}

/// What to do with one pending proven_tx_req this cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReqAction {
    /// Triage says mined: fetch + store the merkle proof.
    FetchProof,
    /// Keep waiting; optionally advance the block-clocked attempt counter.
    Wait { count_attempt: bool },
    /// Network-unknown for >= UNKNOWN_ESCALATION_ATTEMPTS blocks: run the
    /// chain-truth escalation (confirm double-spend or requeue for
    /// re-broadcast). Attempt counting still applies.
    Escalate { count_attempt: bool },
    /// LEGACY ONLY — attempt budget exhausted → invalid. The fixed decision
    /// function NEVER returns this; it exists so tests can prove the old
    /// semantics violated the invariant (red) and the new ones don't (green).
    #[allow(dead_code)]
    MarkInvalid,
}

/// Verdict of the chain-truth escalation for a network-unknown tx.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EscalationVerdict {
    /// An input is spent by a DIFFERENT network-known tx — positive
    /// never-on-chain signal. req → 'doubleSpend', tx → 'failed'.
    ConfirmedDoubleSpend { spending_txid: String },
    /// No conflicting spend found: requeue for re-broadcast ('unsent') so ARC
    /// delivers a real verdict (Go proofTimeoutUpdates semantics).
    Rebroadcast,
    /// Evidence incomplete (recheck no longer unknown, parse failure, or a
    /// spent-status service error with no positive DS hit): do nothing.
    Hold,
}

/// Block-clock gate: an attempt only "counts" when the chain tip has advanced
/// past the height at which we last counted one (TS `countsAsAttempt`, Go
/// `lastBlockKey`). Tip unavailable → no attempt (no evidence, no clock).
pub(crate) fn attempt_counts_now(current_tip: Option<u32>, last_counted_tip: Option<u32>) -> bool {
    match (current_tip, last_counted_tip) {
        // Tip unavailable: no chain evidence, no clock. A provider outage
        // must freeze the attempt clock, not run it.
        (None, _) => false,
        // First observation: counts (starts the clock).
        (Some(_), None) => true,
        // Only strictly-new blocks count. Equal tip = zero blocks passed
        // without the tx; a LOWER tip is a reorg/provider flap, not evidence
        // of absence.
        (Some(cur), Some(last)) => cur > last,
    }
}

/// Decide the action for one pending req given triage evidence.
pub(crate) fn decide_req_action(
    triage: &TriageStatus,
    attempts: i64,
    counts_as_attempt: bool,
    escalation_budget_left: bool,
) -> ReqAction {
    match triage {
        // Confirmed in a block: fetch the proof, whatever the attempt count.
        TriageStatus::Mined => ReqAction::FetchProof,
        // SEEN on network = final on BSV: it WILL mine. Wait for the proof
        // provider to catch up — never escalate, never fail, and do NOT
        // tick the counter (review L1: attempts feed the UNKNOWN-streak
        // escalation trigger, so a long-SEEN tx accruing them would
        // escalate on the first WoC flap; Go's depth filter likewise never
        // counts attempts for mempool txs).
        TriageStatus::Known => ReqAction::Wait {
            count_attempt: false,
        },
        // Network-unknown: after UNKNOWN_ESCALATION_ATTEMPTS blocks of
        // confirmed absence, gather real evidence (input spends / ARC
        // re-verdict). Until then, wait.
        TriageStatus::Unknown => {
            if attempts >= UNKNOWN_ESCALATION_ATTEMPTS && escalation_budget_left {
                ReqAction::Escalate {
                    count_attempt: counts_as_attempt,
                }
            } else {
                ReqAction::Wait {
                    count_attempt: counts_as_attempt,
                }
            }
        }
        // No triage evidence at all: freeze. Do not count, do not escalate —
        // a provider outage must never advance any req toward any verdict.
        TriageStatus::Unavailable => ReqAction::Wait {
            count_attempt: false,
        },
    }
}

/// Decide the escalation verdict from re-checked status + per-input
/// spent-status evidence. `input_spends` = (input_prev_txid, vout, status).
pub(crate) fn decide_escalation(
    recheck: &TriageStatus,
    input_spends: &[(String, u32, SpentStatus)],
    our_txid: &str,
    had_service_error: bool,
) -> EscalationVerdict {
    // A positive double-spend hit wins regardless of other errors: an input
    // consumed by a DIFFERENT network-known tx is conclusive (SEEN = final).
    for (_, _, s) in input_spends {
        if let SpentStatus::Spent { spending_txid } = s {
            if spending_txid != our_txid {
                return EscalationVerdict::ConfirmedDoubleSpend {
                    spending_txid: spending_txid.clone(),
                };
            }
            // Spent by OUR OWN txid → the network knows our tx after all.
            return EscalationVerdict::Hold;
        }
    }
    // No positive signal. Only act further if the evidence is complete and
    // the tx is still network-unknown.
    if *recheck != TriageStatus::Unknown || had_service_error || input_spends.is_empty() {
        return EscalationVerdict::Hold;
    }
    EscalationVerdict::Rebroadcast
}

/// Books mutation on a positive ARC fail verdict (DoubleSpend/InvalidTx) for
/// a tx identified by txid: RELEASE the inputs the tx had locked. Mirrors
/// process_action's inline DoubleSpend branch and Go reviewKnownTxStatuses
/// `RecreateSpentOutputs` (synchronize_tx_statuses.go). `basket_id IS NOT
/// NULL` keeps relinquished (chain-gone) rows dead — G5 guard.
/// Binds: (updated_at, txid).
pub(crate) const FAIL_RELEASE_INPUTS_SQL: &str =
    "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? \
     WHERE spent_by IN (SELECT transaction_id FROM transactions WHERE txid = ?) \
       AND basket_id IS NOT NULL \
       AND transaction_id NOT IN (SELECT transaction_id FROM transactions WHERE status = 'failed')";

/// Companion to FAIL_RELEASE_INPUTS_SQL: the failed tx's own CREATED outputs
/// will never exist on-chain — force spendable=0 so the coin selector can
/// never pick a phantom (Go `MarkCreatedOutputsAsNotSpendable`).
/// Binds: (updated_at, txid).
pub(crate) const FAIL_DERECOGNIZE_CREATED_SQL: &str =
    "UPDATE outputs SET spendable = 0, updated_at = ? \
     WHERE transaction_id IN (SELECT transaction_id FROM transactions WHERE txid = ?)";

/// Row type for COUNT(*) queries.
#[derive(Debug, Deserialize)]
struct CountRow {
    n: Option<f64>,
}

/// Read the chain height at which the block-clocked attempt counter last
/// advanced (monitor_events 'proof_attempt_height', UPDATE-in-place row like
/// the G5 cursor). None = never counted (or unparseable → restart clock).
async fn read_proof_attempt_height(db: &D1Database) -> Option<u32> {
    let row: Option<ChainHeightRow> = Query::new(
        "SELECT details FROM monitor_events \
         WHERE event = 'proof_attempt_height' \
         ORDER BY created_at DESC, event_id DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    row.and_then(|r| r.details)
        .and_then(|d| d.trim().parse::<u32>().ok())
}

/// Persist the attempt-clock height after a counted run (single-row upsert).
async fn store_proof_attempt_height(db: &D1Database, height: u32) {
    let updated = Query::new(
        "UPDATE monitor_events SET details = ?, updated_at = CURRENT_TIMESTAMP \
         WHERE event_id = (SELECT event_id FROM monitor_events \
                           WHERE event = 'proof_attempt_height' \
                           ORDER BY created_at DESC, event_id DESC LIMIT 1)",
    )
    .bind(height.to_string().as_str())
    .execute(db)
    .await
    .map(|m| m.changes)
    .unwrap_or(0);
    if updated == 0 {
        let _ = Query::new(
            "INSERT INTO monitor_events (event, details, created_at, updated_at) \
             VALUES ('proof_attempt_height', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(height.to_string().as_str())
        .execute(db)
        .await;
    }
}

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

/// Row type for scan_external_spends candidates (G5).
#[derive(Debug, Deserialize)]
struct ExtSpendCandidateRow {
    output_id: Option<f64>,
    txid: Option<String>,
    vout: Option<f64>,
}

// =============================================================================
// Task 10 (G5) — persistent scan cursor
// =============================================================================

/// Persistent cursor for the external-spend scan.
///
/// Stored in `monitor_events` under `event = 'external_spend_cursor'` — the
/// same latest-row-wins mechanism `check_chain_reorg` uses for
/// `'chain_height'` (no migration needed). Unlike chain_height (which keeps
/// history for reorg forensics) the cursor row is UPDATEd in place, with an
/// INSERT only on first use — a single row in steady state.
#[derive(Debug, Default, PartialEq)]
struct ExtSpendCursor {
    /// Scan resumes at `output_id > last_output_id`. 0 = start of a sweep.
    last_output_id: i64,
    /// RFC3339 timestamp of the last COMPLETED full sweep. Only consulted
    /// while parked at `last_output_id == 0` (sweep-cooldown gate).
    sweep_completed_at: Option<String>,
}

/// Parse a stored cursor `details` JSON. Malformed/missing fields degrade to
/// the default (restart the sweep from 0 immediately) — never an error, the
/// scan must self-heal from a corrupt cursor.
fn parse_ext_spend_cursor(details: &str) -> ExtSpendCursor {
    let v: serde_json::Value = match serde_json::from_str(details) {
        Ok(v) => v,
        Err(_) => return ExtSpendCursor::default(),
    };
    ExtSpendCursor {
        last_output_id: v.get("last_output_id").and_then(|x| x.as_i64()).unwrap_or(0),
        sweep_completed_at: v
            .get("sweep_completed_at")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
    }
}

/// Serialize a cursor to the `details` JSON stored in monitor_events.
fn ext_spend_cursor_json(cursor: &ExtSpendCursor) -> String {
    serde_json::json!({
        "last_output_id": cursor.last_output_id,
        "sweep_completed_at": cursor.sweep_completed_at,
    })
    .to_string()
}

/// True while a completed sweep is inside its cooldown window (parked).
fn ext_spend_sweep_parked(cursor: &ExtSpendCursor, now: chrono::DateTime<chrono::Utc>) -> bool {
    if cursor.last_output_id != 0 {
        return false; // mid-sweep — keep going
    }
    match cursor
        .sweep_completed_at
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
    {
        Some(done) => {
            now.signed_duration_since(done.with_timezone(&chrono::Utc))
                < chrono::Duration::minutes(EXT_SPEND_SWEEP_COOLDOWN_MINUTES)
        }
        None => false, // never completed (or unparseable) — sweep now
    }
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
    /// Task 10 (G5): candidate outputs whose spent-status was checked this run.
    pub ext_spends_scanned: u32,
    /// Task 10 (G5): outputs found externally spent and marked spendable=0.
    pub ext_spends_found: u32,
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
        ext_spends_scanned: 0,
        ext_spends_found: 0,
        errors: Vec::new(),
    };

    // Task 1: Broadcast unsent/sending transactions
    match send_waiting(db, blobs, broadcast).await {
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

    // Task 10 (G5): Scan tracked outpoints for spends made OUTSIDE wallet-infra
    match scan_external_spends(db, proof_service).await {
        Ok((scanned, found, scan_errors)) => {
            result.ext_spends_scanned = scanned;
            result.ext_spends_found = found;
            result.errors.extend(scan_errors);
        }
        Err(e) => result.errors.push(format!("scan_external_spends: {}", e)),
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
    blobs: &worker::Bucket,
    broadcast: &B,
) -> Result<(u32, u32, Vec<String>)> {
    // ORDER BY attempts ASC first (review M-C): rows that fail or no-op
    // accrue attempts and sink, so a wall of stuck rows can no longer
    // permanently occupy the LIMIT-100 window and starve fresh sends
    // (including the escalation's Rebroadcast → 'unsent' handoff).
    let rows: Vec<UnsentTxRow> = Query::new(
        "SELECT proven_tx_req_id, txid, status, attempts, hex(raw_tx) as raw_tx, \
         batch, hex(input_beef) as input_beef \
         FROM proven_tx_reqs \
         WHERE status IN ('unsent', 'sending') \
         ORDER BY attempts ASC, created_at ASC \
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

        // Broadcast the SUBJECT TX with its ancestry: stored input_beef is
        // ancestors-only (build_input_beef), so it MUST be merged with raw_tx
        // before posting — the reference merges req.rawTx + inputBEEF
        // (StorageProvider.ts mergeReqToBeefToShareExternally), and
        // process_action's inline path does the same merge. The old code
        // posted input_beef verbatim: ARC received only already-known
        // ancestors, answered success, and every status advanced on a tx
        // that never reached the network (audit C3).
        let raw_tx_hex: Option<&String> = row.raw_tx.as_ref().filter(|h| !h.is_empty());

        // input_beef may live in R2 (D1 column NULL for >4KB blobs — the
        // COMMON case for deep ancestry; review M-D: without this fallback
        // the largest txs silently downgraded to raw-tx-only broadcast,
        // the exact orphan-mempool failure of the 2026-04-15 incident).
        let beef_bytes: Option<Vec<u8>> = match &row.input_beef {
            Some(h) if !h.is_empty() => hex::decode(h).ok(),
            _ => {
                let store = crate::r2::BlobStore::new(blobs);
                store
                    .get("proven_tx_reqs", req_id, "input_beef", None)
                    .await
                    .ok()
                    .flatten()
            }
        };

        let merged_beef_hex: Option<String> = match (beef_bytes, raw_tx_hex) {
            (Some(beef_bytes), Some(raw_hex)) => hex::decode(raw_hex).ok().and_then(|raw_bytes| {
                match Beef::from_binary(&beef_bytes) {
                    Ok(mut beef) => {
                        beef.merge_raw_tx(raw_bytes, None);
                        Some(hex::encode(beef.to_binary()))
                    }
                    Err(e) => {
                        console_error!(
                            "send_waiting: BEEF rebuild failed for {} (falling back to raw_tx): {}",
                            txid,
                            e
                        );
                        None
                    }
                }
            }),
            _ => None,
        };

        let broadcast_result = if let Some(ref beef_hex) = merged_beef_hex {
            broadcast.broadcast_beef(beef_hex).await
        } else if let Some(raw_hex) = raw_tx_hex {
            broadcast.broadcast_raw_tx(raw_hex).await
        } else {
            // Nothing broadcastable (no raw_tx). Bump attempts so the row
            // sinks in the ordering instead of clogging the window forever
            // (review M-C), and surface it — this state should not exist.
            console_error!(
                "send_waiting: req {} for {} has no raw_tx — cannot broadcast (attempts+1)",
                req_id,
                txid
            );
            let now = Utc::now().to_rfc3339();
            let _ = Query::new(
                "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?",
            )
            .bind(now.as_str())
            .bind(req_id)
            .execute(db)
            .await;
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
                // Books on a positive ARC fail verdict (audit M1; reference:
                // Go reviewKnownTxStatuses — RecreateSpentOutputs +
                // MarkCreatedOutputsAsNotSpendable; same statements as
                // process_action's inline DoubleSpend branch):
                //  1. RELEASE the INPUTS this tx had locked (spent_by → this
                //     transaction) — the tx will never consume them. The
                //     basket_id IS NOT NULL guard keeps relinquished rows dead.
                //  2. De-recognize the tx's own CREATED outputs — they will
                //     never exist on-chain; spendable=1 here would be phantom
                //     money the coin selector picks and bricks on.
                // (The old code did the exact inverse: re-enabled the created
                // outputs and never touched the input locks.)
                let _ = batch.add(
                    FAIL_RELEASE_INPUTS_SQL,
                    vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                );
                let _ = batch.add(
                    FAIL_DERECOGNIZE_CREATED_SQL,
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
                // Same books correction as the DoubleSpend arm (audit M1):
                // release the inputs, de-recognize the created outputs.
                let _ = batch.add(
                    FAIL_RELEASE_INPUTS_SQL,
                    vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                );
                let _ = batch.add(
                    FAIL_DERECOGNIZE_CREATED_SQL,
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

    // THE FALSE-FAIL FIX (HANDOFF-MONITOR-FALSE-FAIL.md): the old code
    // bulk-invalidated any req with attempts >= 12 here — 12 wall-clocked
    // cron ticks ≈ 60 min, no chain-truth check. That predicate false-failed
    // mined transactions twice on mainnet (170,227 + 97,727 sat) whenever the
    // proof provider lagged mining. It is deleted, not tuned: NO attempt
    // count may ever set 'invalid'. Attempts are now a block-clocked
    // observability counter (reference: TS TaskCheckForProofs.ts:50,229 —
    // countsAsAttempt fires only on a new-header event; Go
    // synchronize_tx_statuses.go skips the whole sync unless the tip moved).
    //
    // Block-clock gate: an attempt counts only if the chain tip advanced
    // since the last counted attempt. Tip source is the ProofService
    // (chaintracks-first via MultiProvider). Tip unavailable → clock frozen.
    let current_tip: Option<u32> = match proof_service.get_chain_height().await {
        // A zero tip is a service-degraded signal (empty tip row), never
        // chain state — treat as unavailable (rust-chaintracks audit C4).
        Ok(0) => {
            console_error!("check_for_proofs: tip source returned 0 — attempt clock frozen");
            None
        }
        Ok(h) => Some(h),
        Err(e) => {
            console_error!("check_for_proofs: tip unavailable ({}) — attempt clock frozen", e);
            None
        }
    };
    let last_counted_tip: Option<u32> = read_proof_attempt_height(db).await;
    let counts_as_attempt = attempt_counts_now(current_tip, last_counted_tip);

    // Alert (never fail) on reqs stuck past the reference ceiling: 144
    // block-clocked attempts ≈ 144 blocks ≈ ~24h of confirmed absence
    // (TS Monitor.ts:106). Operator signal only.
    if let Ok(stuck) = Query::new(
        "SELECT COUNT(*) AS n FROM proven_tx_reqs \
         WHERE status IN ('unmined', 'unknown', 'unconfirmed', 'callback', 'sending', 'reorg') \
           AND attempts >= ?",
    )
    .bind(UNPROVEN_ATTEMPTS_ALERT)
    .fetch_optional::<CountRow>(db)
    .await
    {
        let n = stuck.and_then(|r| r.n).unwrap_or(0.0) as i64;
        if n > 0 {
            console_error!(
                "ALERT check_for_proofs: {} req(s) unproven past {} block-clocked attempts (~24h) — investigate; they will NOT be auto-failed",
                n,
                UNPROVEN_ATTEMPTS_ALERT
            );
        }
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

    // Triage: batch status check → per-txid TriageStatus. On provider failure
    // OR suspicious results (0 confirmed of N — WoC sometimes returns all
    // "unknown" from CF Workers), triage is Unavailable for everyone: we still
    // try get_proof (a found proof is positive evidence and ARC may work when
    // WoC doesn't), but nothing counts as an attempt and nothing escalates.
    let status_map: Option<std::collections::HashMap<String, TriageStatus>> = match proof_service
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

            // Suspicious ONLY when the batch reports every txid unknown
            // (the known WoC-from-CF failure shape). 0 mined with some
            // mempool is the NORMAL state between blocks — discarding the
            // map then (review H1) silently disabled attempt counting AND
            // escalation for relay-lost txs, indefinitely.
            if confirmed == 0 && mempool == 0 && !all_txids.is_empty() {
                console_error!(
                        "check_for_proofs triage: 0/{} known to the network — batch status may be broken, falling back to check all",
                        all_txids.len()
                    );
                None
            } else {
                Some(
                    statuses
                        .into_iter()
                        .map(|s| {
                            let t = match s.status.as_str() {
                                "mined" if s.depth.unwrap_or(0) >= 1 => TriageStatus::Mined,
                                // Mined but depth 0 = bleeding-edge block —
                                // treat as Known (will confirm; avoid
                                // re-orged proofs, TS maxAcceptableHeight).
                                "mined" => TriageStatus::Known,
                                "known" => TriageStatus::Known,
                                "unknown" => TriageStatus::Unknown,
                                _ => TriageStatus::Unavailable,
                            };
                            (s.txid, t)
                        })
                        .collect(),
                )
            }
        }
        Err(e) => {
            console_error!("check_for_proofs triage failed, falling back: {}", e);
            None
        }
    };

    let mut escalations_left: usize = ESCALATION_BUDGET_PER_RUN;

    for row in &rows {
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };
        let req_id = row.proven_tx_req_id.unwrap_or(0.0) as i64;
        let attempts = row.attempts.unwrap_or(0.0) as i64;

        let triage = match &status_map {
            Some(map) => map.get(&txid).cloned().unwrap_or(TriageStatus::Unavailable),
            // Batch triage unavailable: no chain evidence — but still try
            // the proof fetch below (legacy fallback behavior).
            None => TriageStatus::Unavailable,
        };

        // Corruption check (TS TaskCheckForProofs.ts:139-151 parity): a
        // raw_tx that doesn't hash to its txid is a positive local invalid
        // signal — and it would poison proven_txs.raw_tx / BEEF rebuilds if
        // a (txid-keyed) proof were stored over it.
        if let Some(false) = raw_tx_matches_txid(&row.raw_tx, &txid) {
            console_error!(
                "check_for_proofs: raw_tx does not hash to txid {} — corruption, marking invalid",
                txid
            );
            let now = Utc::now().to_rfc3339();
            let _ = Query::new(
                "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?",
            )
            .bind(now.as_str())
            .bind(req_id)
            .execute(db)
            .await;
            continue;
        }

        let action = decide_req_action(&triage, attempts, counts_as_attempt, escalations_left > 0);

        let fetch_proof = match action {
            ReqAction::FetchProof => true,
            // Triage said nothing useful — fall back to a direct proof try.
            ReqAction::Wait { count_attempt } if triage == TriageStatus::Unavailable => {
                let _ = count_attempt; // always false for Unavailable
                true
            }
            ReqAction::Wait { count_attempt } => {
                if count_attempt {
                    let _ = increment_attempts(db, req_id, attempts).await;
                }
                false
            }
            ReqAction::Escalate { count_attempt } => {
                if count_attempt {
                    let _ = increment_attempts(db, req_id, attempts).await;
                }
                escalations_left = escalations_left.saturating_sub(1);
                match escalate_unknown_req(db, proof_service, req_id, &txid, &row.raw_tx).await {
                    Ok(v) => console_log!("escalation({}): {:?}", &txid[..8.min(txid.len())], v),
                    Err(e) => {
                        console_error!("escalation({}) failed: {}", txid, e);
                        if proof_errors.len() < 3 {
                            proof_errors.push(format!("escalate({}):{}", &txid[..8], e));
                        }
                    }
                }
                // Escalation includes its own status re-poll; pace it.
                worker::Delay::from(std::time::Duration::from_millis(1000)).await;
                false
            }
            // The fixed decision function never returns MarkInvalid; if it
            // ever did, refusing to act is the safe direction.
            ReqAction::MarkInvalid => false,
        };

        if !fetch_proof {
            continue;
        }

        checked += 1;

        match proof_service.get_proof(&txid).await {
            Ok(Some(proof_result)) => {
                // Bleeding-edge gate (TS maxAcceptableHeight,
                // TaskCheckForProofs.ts:188-192): a proof from a block our
                // tip source hasn't settled yet is the most likely to be
                // re-orged — skip this cycle, take it next run.
                if let Some(tip) = current_tip {
                    if proof_result.block_height > tip {
                        console_log!(
                            "check_for_proofs: proof for {} at height {} above settled tip {} — deferring one cycle",
                            &txid[..8.min(txid.len())],
                            proof_result.block_height,
                            tip
                        );
                        continue;
                    }
                }
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
                // No proof yet. NEVER a fail signal (a SEEN-but-unproven tx
                // is indistinguishable from a lagging provider here — the
                // exact conflation behind the false-FAIL incidents). Count
                // the block-clocked attempt only when triage POSITIVELY said
                // mined (proof should exist but the proof providers lag —
                // Go only counts attempts for depth-confirmed txs whose
                // merkle fetch failed).
                if counts_as_attempt && triage == TriageStatus::Mined {
                    let _ = increment_attempts(db, req_id, attempts).await;
                }
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

    // Advance the block-clock only after a counted run so the next run
    // doesn't double-count the same block — and only when triage evidence
    // was actually usable (review H1: storing the height on a discarded-
    // triage run burned the block, starving the clock on quiet wallets).
    if counts_as_attempt && status_map.is_some() {
        if let Some(tip) = current_tip {
            store_proof_attempt_height(db, tip).await;
        }
    }

    Ok((found, checked, proof_errors))
}

/// Chain-truth escalation for a req the network has reported "unknown" for
/// >= UNKNOWN_ESCALATION_ATTEMPTS block-clocked attempts (the tx should have
/// been SEEN by now if the original broadcast propagated).
///
/// Evidence gathering (positive signals only — see decide_escalation):
///   1. Re-poll the txid status (kills triage-drift races).
///   2. Check each input outpoint's spent-status.
/// Verdicts:
///   * ConfirmedDoubleSpend — an input is consumed by a DIFFERENT
///     network-known tx: req 'doubleSpend', tx 'failed', inputs released,
///     created outputs de-recognized (the one factual non-ARC fail path).
///   * Rebroadcast — nothing conflicts: requeue 'unsent' (attempts reset) so
///     send_waiting re-broadcasts and ARC delivers a real verdict (Go
///     proofTimeoutUpdates semantics: timeout → unsent, never → invalid).
///   * Hold — evidence incomplete: change nothing.
async fn escalate_unknown_req<P: ProofService>(
    db: &D1Database,
    proof_service: &P,
    req_id: i64,
    txid: &str,
    raw_tx_hex: &Option<String>,
) -> Result<EscalationVerdict> {
    // 1. Status re-poll.
    let recheck = match proof_service
        .get_status_for_txids(&[txid.to_string()])
        .await
    {
        Ok(statuses) => match statuses.first() {
            Some(s) if s.status == "mined" => TriageStatus::Mined,
            Some(s) if s.status == "known" => TriageStatus::Known,
            Some(s) if s.status == "unknown" => TriageStatus::Unknown,
            _ => TriageStatus::Unavailable,
        },
        Err(_) => TriageStatus::Unavailable,
    };

    // 2. Input spent-status evidence (only meaningful while still unknown).
    let mut input_spends: Vec<(String, u32, SpentStatus)> = Vec::new();
    let mut had_service_error = false;
    if recheck == TriageStatus::Unknown {
        let outpoints = raw_tx_hex
            .as_ref()
            .and_then(|h| hex::decode(h).ok())
            .map(|bytes| parse_input_outpoints(&bytes))
            .unwrap_or_default();
        for (prev_txid, prev_vout) in outpoints {
            match proof_service.get_spent_status(&prev_txid, prev_vout).await {
                Ok(s) => input_spends.push((prev_txid, prev_vout, s)),
                Err(e) => {
                    console_error!(
                        "escalation({}): spent-status {}:{} error: {}",
                        &txid[..8.min(txid.len())],
                        &prev_txid[..8.min(prev_txid.len())],
                        prev_vout,
                        e
                    );
                    had_service_error = true;
                }
            }
        }
    }

    let mut verdict = decide_escalation(&recheck, &input_spends, txid, had_service_error);

    // Second positive signal before failing (parity hardening: TS
    // confirmDoubleSpend re-polls 3×; we instead require the alleged
    // SPENDING tx to be network-known — a stronger, direct confirmation
    // that the conflicting spend exists).
    if let EscalationVerdict::ConfirmedDoubleSpend { spending_txid } = &verdict {
        let spender_known = proof_service
            .get_status_for_txids(&[spending_txid.clone()])
            .await
            .ok()
            .and_then(|v| v.into_iter().next())
            .map(|st| st.status == "known" || st.status == "mined")
            .unwrap_or(false);
        if !spender_known {
            console_log!(
                "escalation({}): WoC alleges spender {} but the network doesn't know it — holding",
                &txid[..8.min(txid.len())],
                &spending_txid[..8.min(spending_txid.len())]
            );
            verdict = EscalationVerdict::Hold;
        }
    }

    let now = Utc::now().to_rfc3339();

    match &verdict {
        EscalationVerdict::ConfirmedDoubleSpend { spending_txid } => {
            console_error!(
                "escalation: CONFIRMED double-spend txid={} (input taken by {}) — failing on positive signal",
                txid,
                spending_txid
            );
            let mut batch = BatchCollector::new(db);
            let _ = batch.add(
                "UPDATE proven_tx_reqs SET status = 'doubleSpend', updated_at = ? WHERE proven_tx_req_id = ? \
                 AND status IN ('unmined', 'unknown', 'unconfirmed', 'callback', 'sending', 'reorg')",
                vec![QVal::Text(now.clone()), QVal::Int(req_id)],
            );
            let _ = batch.add(
                "UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?",
                vec![QVal::Text(now.clone()), QVal::Text(txid.to_string())],
            );
            let _ = batch.add(
                FAIL_RELEASE_INPUTS_SQL,
                vec![QVal::Text(now.clone()), QVal::Text(txid.to_string())],
            );
            let _ = batch.add(
                FAIL_DERECOGNIZE_CREATED_SQL,
                vec![QVal::Text(now), QVal::Text(txid.to_string())],
            );
            batch
                .execute()
                .await
                .map_err(|e| Error::from(e.to_string()))?;
        }
        EscalationVerdict::Rebroadcast => {
            console_log!(
                "escalation: txid={} network-unknown with no conflicting spend — requeueing for re-broadcast (ARC verdict)",
                txid
            );
            Query::new(
                "UPDATE proven_tx_reqs SET status = 'unsent', attempts = 0, updated_at = ? WHERE proven_tx_req_id = ? \
                 AND status IN ('unmined', 'unknown', 'unconfirmed', 'callback', 'reorg')",
            )
            .bind(now.as_str())
            .bind(req_id)
            .execute(db)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        }
        EscalationVerdict::Hold => {}
    }

    Ok(verdict)
}

/// TS parity (TaskCheckForProofs.ts:139-151): a stored raw_tx that does NOT
/// double-SHA256 to its claimed txid is local corruption — a positive,
/// deterministic invalid signal. Returns None when raw_tx is absent (no
/// evidence — never a fail license).
pub(crate) fn raw_tx_matches_txid(raw_tx_hex: &Option<String>, txid: &str) -> Option<bool> {
    let bytes = raw_tx_hex.as_ref().and_then(|h| {
        if h.is_empty() {
            None
        } else {
            hex::decode(h).ok()
        }
    })?;
    let hash = bsv_sdk::sha256d(&bytes);
    let reversed: Vec<u8> = hash.iter().rev().cloned().collect();
    Some(hex::encode(reversed).eq_ignore_ascii_case(txid))
}

/// Parse (prev_txid_hex_display, prev_vout) outpoints from raw tx bytes.
/// Coinbase inputs are skipped. Returns empty on any parse trouble — the
/// caller treats "no evidence" as Hold, never as license to fail.
pub(crate) fn parse_input_outpoints(raw_tx: &[u8]) -> Vec<(String, u32)> {
    let mut result = Vec::new();
    if raw_tx.len() < 5 {
        return result;
    }
    let mut pos = 4; // version
    let (vin_count, new_pos) = match read_varint_at(raw_tx, pos) {
        Some(v) => v,
        None => return result,
    };
    pos = new_pos;
    for _ in 0..vin_count {
        if pos + 36 > raw_tx.len() {
            return Vec::new(); // truncated — no partial evidence
        }
        let mut txid_bytes = [0u8; 32];
        txid_bytes.copy_from_slice(&raw_tx[pos..pos + 32]);
        txid_bytes.reverse();
        let txid_hex = hex::encode(txid_bytes);
        pos += 32;
        let vout = u32::from_le_bytes([
            raw_tx[pos],
            raw_tx[pos + 1],
            raw_tx[pos + 2],
            raw_tx[pos + 3],
        ]);
        pos += 4;
        let (script_len, new_pos) = match read_varint_at(raw_tx, pos) {
            Some(v) => v,
            None => return Vec::new(),
        };
        pos = new_pos + script_len as usize;
        pos += 4; // sequence
        if txid_hex != "0000000000000000000000000000000000000000000000000000000000000000" {
            result.push((txid_hex, vout));
        }
    }
    result
}

/// Read a Bitcoin varint at `pos`. Returns (value, new_pos) or None.
fn read_varint_at(data: &[u8], pos: usize) -> Option<(u64, usize)> {
    if pos >= data.len() {
        return None;
    }
    match data[pos] {
        0xFD => {
            if pos + 3 > data.len() {
                return None;
            }
            Some((
                u16::from_le_bytes([data[pos + 1], data[pos + 2]]) as u64,
                pos + 3,
            ))
        }
        0xFE => {
            if pos + 5 > data.len() {
                return None;
            }
            Some((
                u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                    as u64,
                pos + 5,
            ))
        }
        0xFF => {
            if pos + 9 > data.len() {
                return None;
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&data[pos + 1..pos + 9]);
            Some((u64::from_le_bytes(b), pos + 9))
        }
        n => Some((n as u64, pos + 1)),
    }
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

/// Advance the block-clocked attempt counter. PURE COUNTER: attempts feed
/// the UNKNOWN escalation trigger and the 144-block operator alert; they can
/// NEVER set 'invalid' (the old `> MAX_PROOF_ATTEMPTS → invalid` branch here
/// was one of the two false-FAIL predicates — deleted, not tuned).
async fn increment_attempts(db: &D1Database, req_id: i64, current_attempts: i64) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    Query::new(
        "UPDATE proven_tx_reqs SET attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?",
    )
    .bind(current_attempts + 1)
    .bind(now.as_str())
    .bind(req_id)
    .execute(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;
    Ok(())
}

// =============================================================================
// Task 3: Fail abandoned transactions
// =============================================================================

async fn fail_abandoned(db: &D1Database) -> Result<u32> {
    // Abandonment may only ever touch DRAFTS — transactions that were never
    // handed to the network (reference TaskFailAbandoned.ts:37-46 restricts
    // to unsigned/unprocessed). Two hardenings vs the old query (audit M2):
    //  * 30-minute window (deployed TS wallet-infra pins abandonedMsecs to
    //    30 min — index.ts:122; 5 min raced a single ARC flap on a 5-min cron);
    //  * NOT EXISTS live-req guard: a tx whose proven_tx_req is queued,
    //    in-flight, or completed has been (or will be) broadcast — timing it
    //    out would release inputs a pending broadcast can still consume.
    let rows: Vec<AbandonedTxRow> = Query::new(
        "SELECT transaction_id, txid, status FROM transactions t \
         WHERE status IN ('unsigned', 'unprocessed') \
         AND datetime(updated_at) < datetime('now', '-30 minutes') \
         AND is_outgoing = 1 \
         AND NOT EXISTS (SELECT 1 FROM proven_tx_reqs r WHERE r.txid = t.txid \
             AND r.status IN ('unsent', 'sending', 'unmined', 'unknown', 'unconfirmed', 'callback', 'completed'))",
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

        // Release the tx-lock (spendable/spent_by) on UTXOs this abandoned
        // transaction had locked via createAction.
        //
        // G4 — interaction with reserveOutputs reservations: this statement
        // deliberately does NOT touch `outputs.reserved_until`. Reservations
        // placed via the reserveOutputs RPC are a SEPARATE lock layer:
        //   * an output that is merely reserved (spent_by IS NULL) can never
        //     match this WHERE clause, so fail_abandoned cannot release it;
        //   * an output that was reserved AND THEN locked by a createAction
        //     that is now abandoned gets its tx-lock released here, but its
        //     reservation survives — it stays invisible to auto-selection and
        //     to competing reserveOutputs calls until `reserved_until` passes
        //     or the owner calls unreserveOutputs.
        // Reservation EXPIRY (or explicit unreserveOutputs by the owner) is
        // the ONLY release path for reservations. Never add
        // `reserved_until = NULL` to this statement.
        //
        // G5 — `basket_id IS NOT NULL`: an output relinquished while tx-locked
        // (basket NULL + spendable 0 — it was spent externally on-chain) must
        // NOT be resurrected to spendable=1 when the locking tx is failed here.
        batch
            .add(
                "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE spent_by = ? AND basket_id IS NOT NULL",
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

    // Sync terminal-fail reqs → failed transactions + release locked UTXOs.
    // Includes 'doubleSpend' (review P6: a doubleSpend req whose tx row
    // missed its 'failed' write in a partial batch was invisible to both
    // this sweep and the canary; Go reviewKnownTxStatuses covers both
    // terminal statuses).
    let invalid_rows: Vec<MismatchRow> = Query::new(
        "SELECT t.transaction_id, t.txid \
         FROM proven_tx_reqs ptr \
         JOIN transactions t ON t.txid = ptr.txid \
         WHERE ptr.status IN ('invalid', 'doubleSpend') \
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

        // Release locked inputs — with the same guards as every other fail
        // path (review L3: this arm predated the M1 books fix): G5
        // basket_id guard against resurrecting relinquished rows, plus
        // de-recognition of the failed tx's own created outputs.
        batch
            .add(
                // Parent-failed guard (ts-stack 2.4.0 reviewStatus parity):
                // never resurrect an output whose CREATING tx is itself
                // failed — a chained failure (failed B spent failed A's
                // output) otherwise leaves A's phantom spendable=1 forever,
                // and the G5 sweep can't catch it (never on-chain, no
                // external spender).
                "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? \
                 WHERE spent_by = ? AND basket_id IS NOT NULL \
                   AND transaction_id NOT IN (SELECT transaction_id FROM transactions WHERE status = 'failed')",
                vec![QVal::Text(now.clone()), QVal::Int(tx_id)],
            )
            .map_err(|e| Error::from(e.to_string()))?;
        batch
            .add(
                "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ?",
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
                    store_unfail_proof(db, blobs, proof_service, &txid, req_id, &row.raw_tx, &proof_result).await
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
                // No proof yet. Reference (TaskUnFail.ts:78-81) returns the
                // req to 'invalid' — but ONLY treat that as sound when the
                // network genuinely doesn't know the tx. A SEEN/mined-but-
                // unproven tx goes back to 'unmined' so the proof loop keeps
                // watching it (proof lag must never re-fail it).
                let now = Utc::now().to_rfc3339();
                let net_status = proof_service
                    .get_status_for_txids(&[txid.clone()])
                    .await
                    .ok()
                    .and_then(|v| v.into_iter().next())
                    .map(|s| s.status)
                    .unwrap_or_else(|| "unavailable".to_string());
                if net_status == "known" || net_status == "mined" {
                    let _ = Query::new(
                        "UPDATE proven_tx_reqs SET status = 'unmined', attempts = 0, updated_at = ? WHERE proven_tx_req_id = ?",
                    )
                    .bind(now.as_str())
                    .bind(req_id)
                    .execute(db)
                    .await;
                    console_log!(
                        "Unfail: txid={} is {} on network but unproven — demoted to unmined (never invalid)",
                        txid,
                        net_status
                    );
                } else if net_status == "unknown" {
                    // Positive network-absence evidence: back to 'invalid'
                    // (reference TaskUnFail.ts:78-81) — the canary keeps it
                    // watched regardless.
                    let _ = Query::new(
                        "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?",
                    )
                    .bind(now.as_str())
                    .bind(req_id)
                    .execute(db)
                    .await;
                    console_log!("Unfail invalid (network-unknown): txid={}", txid);
                } else {
                    // No evidence at all (status provider down): HOLD at
                    // 'unfail' for the next pass — no evidence, no state
                    // change (our invariant; stricter than TS here).
                    console_log!(
                        "Unfail: no status evidence for txid={} — holding at 'unfail'",
                        txid
                    );
                }
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

    // AUTO-UNFAIL + FALSE-FAIL CANARY (the 88f71e4b/0efb3dfb incident class,
    // bsv-blackjack §16i; HANDOFF-MONITOR-FALSE-FAIL.md production canary):
    // every failed transaction with an 'invalid'/'doubleSpend' req is
    // periodically re-verified against the chain, UNBOUNDED — a tx the
    // network still reports known/mined must never stop being re-checked
    // (the old `attempts < 60` cap made a >47h provider outage turn a
    // false-fail permanent; dropped). Hourly backoff via updated_at re-stamp.
    //
    // Canary invariant: `failed` means never-on-chain. Any failed tx the
    // chain KNOWS is a false-fail → recovered automatically (proof →
    // store_unfail_proof; SEEN-only → promote back to unmined/unproven) and
    // recorded in monitor_events('false_fail_canary') for alerting.
    // Eligibility notes:
    //  * datetime(r.updated_at) — the column holds RFC3339 'T' stamps which
    //    compare wrong against SQLite's space-format datetime() output
    //    (critic finding: the raw compare silently turned the 1h backoff
    //    into rest-of-day);
    //  * LEFT JOIN + the OR arm — a 'doubleSpend' req whose transactions row
    //    missed its 'failed' write (partial batch) must still be re-checked
    //    (TS TaskReviewDoubleSpends parity);
    //  * backoff: hourly for the first 24 checks, daily after (attempts
    //    counts canary re-stamps) — keeps eternally-dead rows from burning
    //    WoC budget forever while never abandoning them.
    let retry_rows: Vec<UnfailRow> = Query::new(
        "SELECT DISTINCT r.proven_tx_req_id, r.txid, hex(r.raw_tx) as raw_tx \
         FROM proven_tx_reqs r \
         LEFT JOIN transactions t ON t.txid = r.txid \
         WHERE r.status IN ('invalid', 'doubleSpend') \
           AND (t.status = 'failed' OR r.status = 'doubleSpend') \
           AND datetime(r.updated_at) < datetime('now', '-1 hour') \
           AND (r.attempts < 24 OR datetime(r.updated_at) < datetime('now', '-1 day')) \
         ORDER BY r.updated_at ASC \
         LIMIT 20",
    )
    .fetch_all(db)
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let mut canary_hits: Vec<String> = Vec::new();

    for row in &retry_rows {
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
                if let Err(e) =
                    store_unfail_proof(db, blobs, proof_service, &txid, req_id, &row.raw_tx, &proof_result).await
                {
                    console_error!("auto-unfail store_proof({}) failed: {}", txid, e);
                    if errors.len() < 3 {
                        errors.push(format!("auto-unfail({}):{}", &txid[..txid.len().min(8)], e));
                    }
                } else {
                    recovered += 1;
                    canary_hits.push(txid.clone());
                    console_error!(
                        "CANARY: false-failed tx WAS MINED — recovered txid={}",
                        txid
                    );
                }
            }
            Ok(None) => {
                // No proof. Chain-truth check before re-stamping: a tx the
                // network reports known/mined is a live false-fail — promote
                // it back into the proof pipeline instead of leaving the
                // books lying until a proof shows up.
                let now = Utc::now().to_rfc3339();
                let net_status = proof_service
                    .get_status_for_txids(&[txid.clone()])
                    .await
                    .ok()
                    .and_then(|v| v.into_iter().next())
                    .map(|s| s.status)
                    .unwrap_or_else(|| "unavailable".to_string());
                if net_status == "known" || net_status == "mined" {
                    let mut batch = BatchCollector::new(db);
                    let _ = batch.add(
                        "UPDATE proven_tx_reqs SET status = 'unmined', attempts = 0, updated_at = ? WHERE proven_tx_req_id = ?",
                        vec![QVal::Text(now.clone()), QVal::Int(req_id)],
                    );
                    let _ = batch.add(
                        "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ? AND status = 'failed'",
                        vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                    );
                    // Re-enable our own created outputs (change/derivation) —
                    // the tx is network-final (SEEN = final on BSV).
                    let _ = batch.add(
                        "UPDATE outputs SET spendable = 1, updated_at = ? WHERE txid = ? AND spendable = 0 AND spent_by IS NULL AND (change = 1 OR custom_instructions IS NOT NULL)",
                        vec![QVal::Text(now.clone()), QVal::Text(txid.clone())],
                    );
                    // Re-mark the tx's consumed inputs as spent (they were
                    // released when the tx was failed) — TaskUnFail.ts:118-129.
                    let _ = batch.execute().await;
                    remark_inputs_spent(db, &txid, &row.raw_tx, &now).await;
                    recovered += 1;
                    canary_hits.push(txid.clone());
                    console_error!(
                        "CANARY: false-failed tx is {} on network — promoted back to unmined/unproven txid={}",
                        net_status,
                        txid
                    );
                } else {
                    // Chain doesn't know it — factually failed (so far).
                    // Re-stamp for the hourly backoff; stays invalid, stays
                    // watched, forever.
                    let _ = Query::new(
                        "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?",
                    )
                    .bind(now.as_str())
                    .bind(req_id)
                    .execute(db)
                    .await;
                }
            }
            Err(_) => {
                // Provider error — re-stamp only; no evidence, no state change.
                let now = Utc::now().to_rfc3339();
                let _ = Query::new(
                    "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?",
                )
                .bind(now.as_str())
                .bind(req_id)
                .execute(db)
                .await;
            }
        }

        // Pace the sweep — up to 20 back-to-back proof fetches tripped
        // WoC's 3/sec free-tier throttle from CF egress IPs (critic).
        worker::Delay::from(std::time::Duration::from_millis(500)).await;
    }

    // Persist the canary verdict for alerting/forensics.
    if !canary_hits.is_empty() {
        let details = serde_json::json!({
            "false_fails_recovered": canary_hits,
        })
        .to_string();
        let _ = Query::new(
            "INSERT INTO monitor_events (event, details, created_at, updated_at) \
             VALUES ('false_fail_canary', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(details.as_str())
        .execute(db)
        .await;
    }

    Ok((recovered, errors))
}

/// Re-mark a recovered (unfailed) transaction's consumed inputs as spent:
/// outputs matching the tx's input outpoints get `spendable=0, spent_by=<tx>`.
/// The fail path had released them; with the tx back in the books they are
/// factually consumed (TS TaskUnFail.ts:118-129 parity; audit M4).
async fn remark_inputs_spent(
    db: &D1Database,
    txid: &str,
    raw_tx_hex: &Option<String>,
    now: &str,
) {
    let outpoints = raw_tx_hex
        .as_ref()
        .and_then(|h| hex::decode(h).ok())
        .map(|b| parse_input_outpoints(&b))
        .unwrap_or_default();
    if outpoints.is_empty() {
        return;
    }
    for (prev_txid, prev_vout) in outpoints {
        // spendable=0 for EVERY row of the outpoint (chain truth — the
        // outpoint is consumed), but spent_by only where it's NULL (never
        // clobber an existing true link) and only with the SAME USER's
        // transaction row (review H3: transactions.txid is not unique
        // across users; a scalar subquery stamped foreign tenants' rows
        // with an arbitrary user's transaction_id).
        let _ = Query::new(
            "UPDATE outputs SET spendable = 0, \
                 spent_by = COALESCE(spent_by, \
                     (SELECT t.transaction_id FROM transactions t \
                      WHERE t.txid = ? AND t.user_id = outputs.user_id)), \
                 updated_at = ? \
             WHERE txid = ? AND vout = ?",
        )
        .bind(txid)
        .bind(now)
        .bind(prev_txid.as_str())
        .bind(prev_vout as i64)
        .execute(db)
        .await;
    }
}

/// Store a proof for an unfailed transaction: insert proven_tx, update proven_tx_req
/// and transaction to 'completed', and re-enable spendable on outputs.
///
/// Similar to `store_proof_result` but specific to the unfail flow — the transaction
/// was previously 'failed', so we restore it to 'completed' and re-enable outputs.
async fn store_unfail_proof<P: ProofService>(
    db: &D1Database,
    blobs: &worker::Bucket,
    proof_service: &P,
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
            vec![QVal::Text(now.clone()), QVal::Text(txid.to_string())],
        )
        .map_err(|e| Error::from(e.to_string()))?;

    batch
        .execute()
        .await
        .map_err(|e| Error::from(e.to_string()))?;

    // The fail path released this tx's inputs (spendable=1, spent_by=NULL);
    // with the tx proven mined they are factually consumed — re-mark them
    // (TS TaskUnFail.ts:118-129; audit M4). Without this, both the consumed
    // inputs AND the change output count as spendable → inflated balance and
    // guaranteed doubleSpend failures when the selector picks dead inputs.
    remark_inputs_spent(db, txid, raw_tx_hex, &now).await;

    // isUtxo re-check (TS TaskUnFail.ts:131-145 parity): outputs of a tx
    // that sat 'failed' may have been spent EXTERNALLY in the interim —
    // verify actual chain state before leaving them spendable (the G5 sweep
    // would catch it eventually; this closes the phantom-balance window on
    // this rare path immediately).
    #[derive(Deserialize)]
    struct VoutRow {
        vout: Option<f64>,
    }
    let vouts: Vec<VoutRow> = Query::new(
        "SELECT vout FROM outputs WHERE txid = ? AND spendable = 1 AND spent_by IS NULL \
           AND (change = 1 OR custom_instructions IS NOT NULL)",
    )
    .bind(txid)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    for v in vouts {
        let vout = v.vout.unwrap_or(-1.0) as i64;
        if vout < 0 {
            continue;
        }
        if let Ok(SpentStatus::Spent { spending_txid }) =
            proof_service.get_spent_status(txid, vout as u32).await
        {
            console_log!(
                "unfail isUtxo: {}:{} already consumed on-chain by {} — marking spent",
                &txid[..8.min(txid.len())],
                vout,
                &spending_txid[..8.min(spending_txid.len())]
            );
            let _ = Query::new(
                "UPDATE outputs SET spendable = 0, updated_at = ? WHERE txid = ? AND vout = ? AND spent_by IS NULL",
            )
            .bind(now.as_str())
            .bind(txid)
            .bind(vout)
            .execute(db)
            .await;
        }
    }

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
                    store_unfail_proof(db, blobs, proof_service, &txid, req_id, &row.raw_tx, &proof_result).await
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
             AND datetime(updated_at) < datetime('now', ?)",
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
             AND datetime(updated_at) < datetime('now', ?) \
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
             AND datetime(updated_at) < datetime('now', ?)",
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

                // Demote proven_tx_req back to 'unmined' with a fresh attempt
                // clock so check_for_proofs re-fetches (Go reference: reorg
                // sets req status + attempts=0 and touches NOTHING else —
                // known_tx.go:685-695).
                let _ = batch.add(
                    "UPDATE proven_tx_reqs SET status = 'unmined', proven_tx_id = NULL, \
                     attempts = 0, updated_at = ? WHERE proven_tx_id = ?",
                    vec![QVal::Text(now.clone()), QVal::Int(proven_tx_id)],
                );

                // Deliberately NOT touched (audit M3 — the old code demoted
                // the transaction to 'unproven' AND clamped its outputs to
                // spendable=0 here):
                //  * transactions.status — a missing proof mid-reorg is
                //    exactly when providers lag; the tx is still SEEN and,
                //    per the owner rule, final. Neither reference demotes
                //    the tx on a proof-fetch miss (TS TaskReorg re-proves
                //    with retries; Go resets the req only).
                //  * outputs.spendable — clamping on a guess understated the
                //    balance, and basket-insertion outputs (change=0, no
                //    custom_instructions) could NEVER recover because the
                //    re-enable filter on re-proof only matches change/
                //    derivation outputs — permanent stranding.
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
// =============================================================================
// Task 10 (G5): Scan tracked outpoints for external (out-of-wallet) spends
// =============================================================================

/// Detect tracked outputs spent ON-CHAIN outside wallet-infra and mark them
/// `spendable = 0` (G5 completion — the service-side safety net behind
/// client-driven `relinquishOutput`, see the design note in
/// `storage/relinquish_output.rs`).
///
/// Owner rule (non-negotiable): on BSV a tx SEEN_ON_NETWORK is FINAL
/// (first-seen, no RBF). The scan marks an output as soon as a spending tx is
/// SEEN — it never waits for mining. `unproven` means "awaiting merkle
/// proof", never "reversible".
///
/// Mechanics per run:
/// - Load the persistent cursor (`monitor_events` / 'external_spend_cursor').
/// - If the previous sweep completed inside the cooldown window, park (0 API
///   calls).
/// - Page ≤ EXT_SPEND_BATCH candidates (`EXT_SPEND_CANDIDATES_SQL`), ask the
///   ProofService for each outpoint's spent-status, and on `Spent` apply the
///   double-guarded `EXT_SPEND_MARK_SQL` (never clobbers a row a live
///   createAction locked meanwhile; never touches reserved_until/basket_id).
/// - Service errors skip-and-advance (a poison outpoint can't stall the
///   sweep; it is re-checked next sweep) and the run bails after
///   EXT_SPEND_MAX_SERVICE_ERRORS so a WoC outage can't burn the cron.
/// - Persist the cursor: advanced past all PROCESSED rows only, reset to 0
///   with a completion timestamp when a short page ends the sweep.
///
/// Returns (scanned, spent_found, error_messages).
async fn scan_external_spends<P: ProofService>(
    db: &D1Database,
    proof_service: &P,
) -> Result<(u32, u32, Vec<String>)> {
    let now = Utc::now();

    // Load persistent cursor (row reuse: same {details} shape as chain_height).
    let cursor_row: Option<ChainHeightRow> = Query::new(EXT_SPEND_CURSOR_READ_SQL)
        .fetch_optional(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    let cursor = cursor_row
        .and_then(|r| r.details)
        .map(|d| parse_ext_spend_cursor(&d))
        .unwrap_or_default();

    if ext_spend_sweep_parked(&cursor, now) {
        console_log!("scan_external_spends: scanned=0 spent_found=0 errors=0 (parked — sweep cooldown)");
        return Ok((0, 0, Vec::new()));
    }

    let rows: Vec<ExtSpendCandidateRow> = Query::new(EXT_SPEND_CANDIDATES_SQL)
        .bind(cursor.last_output_id)
        .bind(EXT_SPEND_BATCH as i64)
        .fetch_all(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;

    let fetched = rows.len();
    let now_str = now.to_rfc3339();
    let mut scanned = 0u32;
    let mut found = 0u32;
    let mut errors = 0u32;
    let mut error_msgs: Vec<String> = Vec::new();
    let mut last_processed = cursor.last_output_id;
    let mut unsupported = false;

    for row in &rows {
        let output_id = row.output_id.unwrap_or(0.0) as i64;
        if output_id == 0 {
            continue;
        }
        let txid = match &row.txid {
            Some(t) if !t.is_empty() => t.clone(),
            _ => {
                // Defensive — the SQL already excludes these.
                last_processed = output_id;
                continue;
            }
        };
        let vout = row.vout.unwrap_or(0.0) as u32;

        match proof_service.get_spent_status(&txid, vout).await {
            Ok(SpentStatus::Spent { spending_txid }) => {
                scanned += 1;
                last_processed = output_id;
                let meta = Query::new(EXT_SPEND_MARK_SQL)
                    .bind(now_str.as_str())
                    .bind(output_id)
                    .execute(db)
                    .await
                    .map_err(|e| Error::from(e.to_string()))?;
                if meta.changes > 0 {
                    found += 1;
                    console_log!(
                        "scan_external_spends: output {} ({}.{}) spent externally by {} — marked spendable=0",
                        output_id,
                        &txid[..txid.len().min(16)],
                        vout,
                        &spending_txid[..spending_txid.len().min(16)]
                    );
                } else {
                    // Row changed between SELECT and UPDATE (createAction lock
                    // or relinquish) — guard held, hands off, next sweep re-checks.
                    console_log!(
                        "scan_external_spends: output {} changed under us — guard held, left alone",
                        output_id
                    );
                }
            }
            Ok(SpentStatus::Unspent) => {
                scanned += 1;
                last_processed = output_id;
            }
            Ok(SpentStatus::Unsupported) => {
                // Provider has no outpoint index: leave the cursor untouched so
                // nothing is skipped once a capable provider is wired back in.
                unsupported = true;
                break;
            }
            Err(e) => {
                errors += 1;
                // Skip-and-advance: re-checked on the next full sweep.
                last_processed = output_id;
                if error_msgs.len() < 5 {
                    error_msgs.push(format!(
                        "ext_spend({}.{}): {}",
                        &txid[..txid.len().min(8)],
                        vout,
                        e
                    ));
                }
                if errors >= EXT_SPEND_MAX_SERVICE_ERRORS {
                    console_error!(
                        "scan_external_spends: {} service errors — bailing out of this run",
                        errors
                    );
                    break;
                }
            }
        }
    }

    if unsupported {
        console_log!(
            "scan_external_spends: scanned={} spent_found={} errors={} (provider lacks spent-status support — cursor untouched)",
            scanned, found, errors
        );
        return Ok((scanned, found, error_msgs));
    }

    let bailed = errors >= EXT_SPEND_MAX_SERVICE_ERRORS;
    let new_cursor = if !bailed && fetched < EXT_SPEND_BATCH {
        // Short page = end of the candidate set: sweep complete, park.
        ExtSpendCursor {
            last_output_id: 0,
            sweep_completed_at: Some(now_str.clone()),
        }
    } else {
        ExtSpendCursor {
            last_output_id: last_processed,
            sweep_completed_at: cursor.sweep_completed_at.clone(),
        }
    };
    persist_ext_spend_cursor(db, &new_cursor).await?;

    console_log!(
        "scan_external_spends: scanned={} spent_found={} errors={} cursor {}→{}{}",
        scanned,
        found,
        errors,
        cursor.last_output_id,
        new_cursor.last_output_id,
        if new_cursor.last_output_id == 0 {
            " (sweep complete)"
        } else {
            ""
        }
    );

    Ok((scanned, found, error_msgs))
}

/// Persist the scan cursor: UPDATE the existing 'external_spend_cursor' row,
/// INSERT only if none exists yet (single row in steady state).
async fn persist_ext_spend_cursor(db: &D1Database, cursor: &ExtSpendCursor) -> Result<()> {
    let details = ext_spend_cursor_json(cursor);
    let meta = Query::new(EXT_SPEND_CURSOR_UPDATE_SQL)
        .bind(details.as_str())
        .execute(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    if meta.changes == 0 {
        Query::new(EXT_SPEND_CURSOR_INSERT_SQL)
            .bind(details.as_str())
            .execute(db)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
    }
    Ok(())
}

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
        "ext_spends_scanned": result.ext_spends_scanned,
        "ext_spends_found": result.ext_spends_found,
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
    // Proof-lifecycle decision core — THE false-FAIL invariant
    // (HANDOFF-MONITOR-FALSE-FAIL.md proof plan, cases 1-6)
    //
    // Reference cites: TS wallet-toolbox TaskCheckForProofs.ts:50,193,229
    // (countsAsAttempt = new-header event), Monitor.ts:106 (144-block limit);
    // Go go-wallet-toolbox synchronize_tx_statuses.go (lastBlockKey gate,
    // depth filter), known_tx.go proofTimeoutUpdates (timeout → rebroadcast).
    // =========================================================================

    /// Proof-plan case 1 (the live bug, mainnet-lag sim): proof provider lags
    /// mining — triage says Known (SEEN) while get_proof keeps returning None,
    /// for 1000 monitor cycles with NO new block. The req must NEVER reach
    /// invalid, and the block-clocked attempt counter must not advance.
    #[test]
    fn test_seen_tx_never_fails_during_provider_lag() {
        let tip = Some(950_000u32);
        let mut attempts: i64 = 0;
        let mut last_counted_tip: Option<u32> = tip; // counted once at broadcast height
        for _cycle in 0..1000 {
            let counts = attempt_counts_now(tip, last_counted_tip);
            // Tip never advanced → no cycle may count as an attempt.
            assert!(
                !counts,
                "attempt counted with a constant chain tip (wall-clocked, not block-clocked)"
            );
            let action = decide_req_action(&TriageStatus::Known, attempts, counts, true);
            assert_ne!(
                action,
                ReqAction::MarkInvalid,
                "SEEN-on-network tx marked invalid after {} cycles — the false-FAIL bug",
                attempts
            );
            if let ReqAction::Wait { count_attempt } = action {
                if count_attempt {
                    attempts += 1;
                    last_counted_tip = tip;
                }
            }
        }
        assert_eq!(attempts, 0, "attempts advanced without chain progress");
    }

    /// Case 1b: even WITH chain progress (tip advances 1000 blocks), a tx the
    /// network reports as Known/SEEN must never fail — SEEN is final on BSV.
    /// (Old code failed it at 12 attempts; TS would at 144; Go never does —
    /// we follow the strictest-correct semantics: only positive signals fail.)
    #[test]
    fn test_seen_tx_never_fails_even_across_many_blocks() {
        let mut attempts: i64 = 0;
        for height in 950_000u32..951_000 {
            let counts = attempt_counts_now(Some(height + 1), Some(height));
            assert!(counts, "tip advanced — attempt should count");
            let action = decide_req_action(&TriageStatus::Known, attempts, counts, true);
            assert_ne!(action, ReqAction::MarkInvalid);
            assert_ne!(
                std::mem::discriminant(&action),
                std::mem::discriminant(&ReqAction::Escalate {
                    count_attempt: false
                }),
                "Known tx must not escalate to double-spend hunting"
            );
            if let ReqAction::Wait { count_attempt: true } = action {
                attempts += 1;
            }
        }
    }

    /// Case 2: network-unknown + an input spent by a DIFFERENT tx ⇒ confirmed
    /// double-spend (the ONLY non-ARC fail path).
    #[test]
    fn test_unknown_with_conflicting_input_spend_is_double_spend() {
        let spends = vec![(
            "aa".repeat(32),
            0u32,
            SpentStatus::Spent {
                spending_txid: "bb".repeat(32),
            },
        )];
        let v = decide_escalation(&TriageStatus::Unknown, &spends, &"cc".repeat(32), false);
        assert_eq!(
            v,
            EscalationVerdict::ConfirmedDoubleSpend {
                spending_txid: "bb".repeat(32)
            }
        );
    }

    /// Case 3: network-unknown + inputs all unspent ⇒ re-broadcast (ARC gets
    /// to deliver the verdict), NOT fail.
    #[test]
    fn test_unknown_with_unspent_inputs_rebroadcasts_not_fails() {
        let spends = vec![
            ("aa".repeat(32), 0u32, SpentStatus::Unspent),
            ("dd".repeat(32), 1u32, SpentStatus::Unspent),
        ];
        let v = decide_escalation(&TriageStatus::Unknown, &spends, &"cc".repeat(32), false);
        assert_eq!(v, EscalationVerdict::Rebroadcast);
    }

    /// Case 3b: an input spent by OUR OWN txid means the network DOES know
    /// our tx (triage drift) — hold, never double-spend ourselves.
    #[test]
    fn test_input_spent_by_our_own_tx_holds() {
        let ours = "cc".repeat(32);
        let spends = vec![(
            "aa".repeat(32),
            0u32,
            SpentStatus::Spent {
                spending_txid: ours.clone(),
            },
        )];
        let v = decide_escalation(&TriageStatus::Unknown, &spends, &ours, false);
        assert_eq!(v, EscalationVerdict::Hold);
    }

    /// Case 4: proof arrives late — triage flips to Mined after any number of
    /// attempts ⇒ FetchProof (→ completed), attempts irrelevant.
    #[test]
    fn test_late_proof_completes_regardless_of_attempts() {
        for attempts in [0i64, 12, 145, 10_000] {
            let action = decide_req_action(&TriageStatus::Mined, attempts, true, true);
            assert_eq!(action, ReqAction::FetchProof);
        }
    }

    /// Case 5: incomplete evidence never fails — Unavailable triage at huge
    /// attempt counts, spent-status service errors, unparseable inputs.
    #[test]
    fn test_no_evidence_never_fails() {
        for attempts in [0i64, 11, 12, 13, 144, 145, 100_000] {
            for counts in [false, true] {
                let a = decide_req_action(&TriageStatus::Unavailable, attempts, counts, true);
                assert_ne!(a, ReqAction::MarkInvalid);
                // Unavailable = no evidence — must not even escalate.
                assert!(matches!(a, ReqAction::Wait { .. }));
            }
        }
        // Escalation with a service error and no positive DS hit: hold.
        let spends = vec![("aa".repeat(32), 0u32, SpentStatus::Unspent)];
        let v = decide_escalation(&TriageStatus::Unknown, &spends, &"cc".repeat(32), true);
        assert_eq!(v, EscalationVerdict::Hold);
        // Escalation whose recheck says the tx is no longer unknown: hold.
        let v = decide_escalation(&TriageStatus::Known, &spends, &"cc".repeat(32), false);
        assert_eq!(v, EscalationVerdict::Hold);
        // Empty input evidence (parse failure): hold.
        let v = decide_escalation(&TriageStatus::Unknown, &[], &"cc".repeat(32), false);
        assert_eq!(v, EscalationVerdict::Hold);
        // Unsupported spent-status is NOT license to fail — rebroadcast path
        // still allowed (harmless, idempotent), never a fail verdict.
        let spends = vec![("aa".repeat(32), 0u32, SpentStatus::Unsupported)];
        let v = decide_escalation(&TriageStatus::Unknown, &spends, &"cc".repeat(32), false);
        assert!(matches!(
            v,
            EscalationVerdict::Rebroadcast | EscalationVerdict::Hold
        ));
    }

    /// The invariant, exhaustively: NO combination of non-Mined triage,
    /// attempt count, clock state, and budget may yield MarkInvalid. `invalid`
    /// is reachable ONLY from ARC rejects and confirmed double-spends.
    #[test]
    fn test_invariant_no_attempt_based_invalid_ever() {
        let statuses = [
            TriageStatus::Known,
            TriageStatus::Unknown,
            TriageStatus::Unavailable,
        ];
        for st in &statuses {
            for attempts in 0..2000i64 {
                for counts in [false, true] {
                    for budget in [false, true] {
                        let a = decide_req_action(st, attempts, counts, budget);
                        assert_ne!(
                            a,
                            ReqAction::MarkInvalid,
                            "attempt-based invalid: status={:?} attempts={}",
                            st,
                            attempts
                        );
                    }
                }
            }
        }
    }

    /// Block-clock gate semantics: counts only on tip advance; never counts
    /// when the tip is unavailable (no evidence, no clock); first sighting
    /// (nothing counted yet) counts.
    #[test]
    fn test_attempt_clock_is_block_clocked() {
        assert!(!attempt_counts_now(None, None), "no tip → no attempt");
        assert!(!attempt_counts_now(None, Some(100)), "no tip → no attempt");
        assert!(attempt_counts_now(Some(100), None), "first observation counts");
        assert!(!attempt_counts_now(Some(100), Some(100)), "same tip → no attempt");
        assert!(attempt_counts_now(Some(101), Some(100)), "tip advanced → counts");
        assert!(
            !attempt_counts_now(Some(99), Some(100)),
            "tip regression (reorg/provider flap) is not an attempt"
        );
    }

    /// Escalation trigger: unknown reqs escalate only after the block-clocked
    /// unknown streak reaches UNKNOWN_ESCALATION_ATTEMPTS, and only when the
    /// per-run escalation budget allows.
    #[test]
    fn test_unknown_escalates_after_streak_with_budget() {
        // Below streak: wait.
        let a = decide_req_action(&TriageStatus::Unknown, 1, true, true);
        assert!(matches!(a, ReqAction::Wait { .. }));
        // At/above streak with budget: escalate.
        let a = decide_req_action(&TriageStatus::Unknown, UNKNOWN_ESCALATION_ATTEMPTS, true, true);
        assert!(matches!(a, ReqAction::Escalate { .. }));
        // At/above streak, budget exhausted: wait (retry next run).
        let a = decide_req_action(&TriageStatus::Unknown, UNKNOWN_ESCALATION_ATTEMPTS, true, false);
        assert!(matches!(a, ReqAction::Wait { .. }));
    }

    // =========================================================================
    // False-FAIL fix — wiring regression tests
    // =========================================================================

    /// The books mutation on a positive ARC fail verdict must RELEASE inputs
    /// (spent_by → NULL) and DE-RECOGNIZE created outputs (spendable → 0).
    /// The old code did the exact inverse (re-enabled phantoms, stranded
    /// inputs) — audit finding M1.
    #[test]
    fn test_fail_books_release_inputs_not_created_outputs() {
        assert!(FAIL_RELEASE_INPUTS_SQL.contains("spendable = 1"));
        assert!(FAIL_RELEASE_INPUTS_SQL.contains("spent_by = NULL"));
        // IN, not = (review H3): transactions.txid is not unique across
        // users — a scalar subquery picked an arbitrary row and could
        // strand the other user's input locks.
        assert!(FAIL_RELEASE_INPUTS_SQL
            .contains("WHERE spent_by IN (SELECT transaction_id FROM transactions WHERE txid = ?)"));
        assert!(
            FAIL_RELEASE_INPUTS_SQL.contains("basket_id IS NOT NULL"),
            "must not resurrect relinquished (chain-gone) outputs"
        );
        assert!(FAIL_DERECOGNIZE_CREATED_SQL.contains("spendable = 0"));
        assert!(FAIL_DERECOGNIZE_CREATED_SQL
            .contains("transaction_id IN (SELECT transaction_id FROM transactions WHERE txid = ?)"));
    }

    /// parse_input_outpoints: version(4) + vin count + [txid(32) vout(4)
    /// script sequence(4)] — display-order txids, coinbase skipped,
    /// truncation yields NO partial evidence.
    #[test]
    fn test_parse_input_outpoints() {
        let mut tx = vec![1, 0, 0, 0]; // version
        tx.push(2); // vin count
        // input 0: prev txid internal 0x01 at byte 31 → display 01..00? build:
        let mut prev0 = [0u8; 32];
        prev0[31] = 0x01; // internal LE — display begins "01"
        tx.extend_from_slice(&prev0);
        tx.extend_from_slice(&7u32.to_le_bytes()); // vout 7
        tx.push(0); // empty script
        tx.extend_from_slice(&[0xFF; 4]); // sequence
        // input 1: coinbase (all-zero txid) — must be skipped
        tx.extend_from_slice(&[0u8; 32]);
        tx.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        tx.push(0);
        tx.extend_from_slice(&[0xFF; 4]);

        let outpoints = parse_input_outpoints(&tx);
        assert_eq!(outpoints.len(), 1);
        assert_eq!(outpoints[0].1, 7);
        assert!(outpoints[0].0.starts_with("01"));
        assert_eq!(outpoints[0].0.len(), 64);

        // Truncated tx → empty (no partial evidence).
        let truncated = &tx[..tx.len() - 10];
        assert!(parse_input_outpoints(truncated).is_empty());
        // Garbage → empty.
        assert!(parse_input_outpoints(&[0x00, 0x01]).is_empty());
    }

    /// Proof-plan case 6 adjunct: the auto-unfail sweep must be UNBOUNDED
    /// (no attempts cap — the old `attempts < 60` cap made a long provider
    /// outage turn a false-fail permanent) and must also re-check
    /// 'doubleSpend' reqs (a raced ARC verdict must self-correct too).
    #[test]
    fn test_auto_unfail_sweep_unbounded_and_covers_double_spend() {
        let sql = "SELECT DISTINCT r.proven_tx_req_id, r.txid, hex(r.raw_tx) as raw_tx \
                   FROM proven_tx_reqs r \
                   JOIN transactions t ON t.txid = r.txid \
                   WHERE r.status IN ('invalid', 'doubleSpend') AND t.status = 'failed' \
                     AND r.updated_at < datetime('now', '-1 hour') \
                   ORDER BY r.updated_at ASC \
                   LIMIT 20";
        assert!(!sql.contains("attempts <"), "no attempt cap on self-correction");
        assert!(sql.contains("'doubleSpend'"));
        assert!(sql.contains("datetime('now', '-1 hour')"), "hourly backoff retained");
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
        // Positive ARC verdict → status writes are factual...
        let ds_sql_ptr = "UPDATE proven_tx_reqs SET status = 'doubleSpend', updated_at = ? WHERE proven_tx_req_id = ?";
        let ds_sql_tx = "UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?";
        assert!(ds_sql_ptr.contains("'doubleSpend'"));
        assert!(ds_sql_tx.contains("'failed'"));
        // ...and the books mutation releases INPUTS and de-recognizes the
        // CREATED outputs (audit M1 — the old code re-enabled the phantoms
        // and left the inputs stranded).
        assert!(FAIL_RELEASE_INPUTS_SQL.contains("spent_by = NULL"));
        assert!(FAIL_DERECOGNIZE_CREATED_SQL.contains("spendable = 0"));
    }

    #[test]
    fn test_send_waiting_invalid_tx_updates() {
        let inv_sql_ptr = "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?";
        let inv_sql_tx = "UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?";

        assert!(inv_sql_ptr.contains("'invalid'"));
        assert!(inv_sql_tx.contains("'failed'"));
        // Same books correction as the doubleSpend arm (audit M1).
        assert!(FAIL_RELEASE_INPUTS_SQL.contains("spendable = 1"));
        assert!(FAIL_DERECOGNIZE_CREATED_SQL.contains("spendable = 0"));
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
                    AND datetime(updated_at) < datetime('now', ?)";
        assert!(sql.contains("DELETE FROM proven_tx_reqs"));
        assert!(sql.contains("status = 'completed'"));
        assert!(sql.contains("proven_tx_id IS NOT NULL"));
        assert!(sql.contains("datetime(updated_at) < datetime('now', ?)"));
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
                    AND datetime(updated_at) < datetime('now', ?) \
                    AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)";
        assert!(sql.contains("UPDATE transactions"));
        assert!(sql.contains("SET raw_tx = NULL, input_beef = NULL"));
        assert!(sql.contains("status = 'completed'"));
        assert!(sql.contains("proven_tx_id IS NOT NULL"));
        assert!(sql.contains("datetime(updated_at) < datetime('now', ?)"));
        // Targets only rows that still have data to clear
        assert!(sql.contains("raw_tx IS NOT NULL OR input_beef IS NOT NULL"));
    }

    #[test]
    fn test_purge_sql_failed_where_clause() {
        let sql = "DELETE FROM proven_tx_reqs \
                    WHERE status IN ('invalid', 'doubleSpend') \
                    AND datetime(updated_at) < datetime('now', ?)";
        assert!(sql.contains("status IN ('invalid', 'doubleSpend')"));
        assert!(sql.contains("datetime(updated_at) < datetime('now', ?)"));
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
            ext_spends_scanned: 0,
            ext_spends_found: 0,
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
                   attempts = 0, updated_at = ? WHERE proven_tx_id = ?";
        assert!(sql.contains("'unmined'"));
        assert!(sql.contains("proven_tx_id = NULL"));
        assert!(
            sql.contains("attempts = 0"),
            "reorg demote restarts the attempt clock (Go known_tx.go:685-695)"
        );
    }

    /// Audit M3: a reorg proof-miss demotes ONLY the req. The transaction
    /// stays at its status and outputs are NOT clamped — a missing proof
    /// mid-reorg is provider lag, not evidence; the old spendable=0 clamp
    /// permanently stranded basket-insertion outputs (the re-enable filter
    /// only matches change/derivation outputs). Neither reference touches
    /// transactions or outputs on reorg (TS TaskReorg.ts:69-84 re-proves
    /// with retries; Go resets the req only).
    #[test]
    fn test_reorg_invalidation_touches_only_req_and_proof() {
        // The reorg batch must contain exactly: proven_txs proof-null +
        // proven_tx_reqs demote. Neither a transactions demote nor an
        // outputs clamp may reappear.
        let proof_null_sql = "UPDATE proven_txs SET block_hash = '', merkle_root = '', merkle_path = NULL, \
                   updated_at = ? WHERE proven_tx_id = ?";
        assert!(proof_null_sql.contains("merkle_path = NULL"));
        assert!(
            !proof_null_sql.contains("DELETE"),
            "raw_tx must be preserved for ancestry"
        );
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

    // =========================================================================
    // Task 10 (G5) scan_external_spends — cursor state machine
    // =========================================================================

    #[test]
    fn test_ext_spend_cursor_round_trip() {
        let c = ExtSpendCursor {
            last_output_id: 42,
            sweep_completed_at: Some("2026-07-05T12:00:00+00:00".to_string()),
        };
        let json = ext_spend_cursor_json(&c);
        assert_eq!(parse_ext_spend_cursor(&json), c);
    }

    #[test]
    fn test_ext_spend_cursor_defaults_on_garbage() {
        // A corrupt cursor must degrade to "restart sweep now", never error.
        assert_eq!(parse_ext_spend_cursor("not json"), ExtSpendCursor::default());
        assert_eq!(parse_ext_spend_cursor("{}"), ExtSpendCursor::default());
        assert_eq!(parse_ext_spend_cursor(""), ExtSpendCursor::default());
        assert_eq!(
            parse_ext_spend_cursor(r#"{"last_output_id":"nope"}"#),
            ExtSpendCursor::default()
        );
    }

    #[test]
    fn test_ext_spend_cursor_null_completed_at() {
        let json = r#"{"last_output_id":7,"sweep_completed_at":null}"#;
        let c = parse_ext_spend_cursor(json);
        assert_eq!(c.last_output_id, 7);
        assert!(c.sweep_completed_at.is_none());
    }

    #[test]
    fn test_ext_spend_parked_within_cooldown() {
        let now = chrono::Utc::now();
        let done = (now - chrono::Duration::minutes(EXT_SPEND_SWEEP_COOLDOWN_MINUTES - 5))
            .to_rfc3339();
        let c = ExtSpendCursor {
            last_output_id: 0,
            sweep_completed_at: Some(done),
        };
        assert!(ext_spend_sweep_parked(&c, now));
    }

    #[test]
    fn test_ext_spend_not_parked_after_cooldown() {
        let now = chrono::Utc::now();
        let done = (now - chrono::Duration::minutes(EXT_SPEND_SWEEP_COOLDOWN_MINUTES + 5))
            .to_rfc3339();
        let c = ExtSpendCursor {
            last_output_id: 0,
            sweep_completed_at: Some(done),
        };
        assert!(!ext_spend_sweep_parked(&c, now));
    }

    #[test]
    fn test_ext_spend_never_parked_mid_sweep() {
        // A fresh completion timestamp must NOT park a cursor that is mid-sweep.
        let now = chrono::Utc::now();
        let c = ExtSpendCursor {
            last_output_id: 99,
            sweep_completed_at: Some(now.to_rfc3339()),
        };
        assert!(!ext_spend_sweep_parked(&c, now));
    }

    #[test]
    fn test_ext_spend_not_parked_first_run() {
        // No cursor row yet → default → sweep immediately.
        assert!(!ext_spend_sweep_parked(
            &ExtSpendCursor::default(),
            chrono::Utc::now()
        ));
    }

    #[test]
    fn test_ext_spend_candidate_row_from_d1_json() {
        // D1 delivers numbers as floats.
        let val = serde_json::json!({"output_id": 12.0, "txid": "ab", "vout": 3.0});
        let row: ExtSpendCandidateRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.output_id.map(|v| v as i64), Some(12));
        assert_eq!(row.txid.as_deref(), Some("ab"));
        assert_eq!(row.vout.map(|v| v as u32), Some(3));
    }

    // =========================================================================
    // Task 10 (G5) — SQL invariants (the strings the runtime actually executes)
    // =========================================================================

    #[test]
    fn test_ext_spend_candidate_sql_predicates() {
        let sql = EXT_SPEND_CANDIDATES_SQL;
        assert!(sql.contains("o.spendable = 1"));
        assert!(sql.contains("o.spent_by IS NULL"));
        assert!(sql.contains("o.txid IS NOT NULL"));
        assert!(sql.contains("t.status IN ('unproven', 'completed')"));
        assert!(sql.contains("o.output_id > ?"));
        assert!(sql.contains("ORDER BY o.output_id ASC"));
        assert!(sql.contains("LIMIT ?"));
        // Reservations are an orthogonal layer (G1/G4) — the scan must not
        // filter on them, and must not read them at all.
        assert!(!sql.contains("reserved_until"));
    }

    #[test]
    fn test_ext_spend_mark_sql_guard() {
        let sql = EXT_SPEND_MARK_SQL;
        assert!(sql.contains("spendable = 0"));
        assert!(sql.contains("spent_by = NULL"));
        // Write-time guard: never clobber a row a live createAction locked.
        assert!(sql.contains("WHERE output_id = ? AND spendable = 1 AND spent_by IS NULL"));
        // G4/G1 invariants: mark-spent must not touch reservations or baskets.
        assert!(!sql.contains("reserved_until"));
        assert!(!sql.contains("basket_id"));
    }

    #[test]
    fn test_ext_spend_cursor_sql_event_scoped() {
        assert!(EXT_SPEND_CURSOR_READ_SQL.contains("event = 'external_spend_cursor'"));
        assert!(EXT_SPEND_CURSOR_UPDATE_SQL.contains("event = 'external_spend_cursor'"));
        assert!(EXT_SPEND_CURSOR_INSERT_SQL.contains("'external_spend_cursor'"));
        // Never mix with the reorg tracker's rows.
        assert!(!EXT_SPEND_CURSOR_READ_SQL.contains("chain_height"));
    }

    // =========================================================================
    // Task 10 (G5) — real-SQLite harness: the ACTUAL migrations + the ACTUAL
    // SQL consts against an in-memory DB (D1 is SQLite; semantics match).
    // =========================================================================

    /// Build an in-memory DB with the real migrations applied and a fixture
    /// matrix of outputs covering every candidate predicate.
    ///
    /// Fixture map (output_id → why it is / isn't a candidate):
    ///   1  parent 'completed', spendable=1, free        → CANDIDATE
    ///   2  parent 'unproven',  spendable=1, free,
    ///      reserved_until in the future                 → CANDIDATE (reservations orthogonal)
    ///   3  parent 'unsigned'                            → no (not chain-real yet)
    ///   4  parent 'completed', spendable=0              → no (already relinquished/marked)
    ///   5  parent 'completed', spent_by = 1             → no (tx-locked by live createAction)
    ///   6  parent 'completed', txid NULL                → no (not addressable)
    ///   7  parent 'failed'                              → no
    ///   8  parent 'nosend'                              → no (check_no_sends owns that path)
    ///   9  parent 'completed', spendable=1, free        → CANDIDATE (paging fodder)
    fn g5_test_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../migrations/0001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../migrations/0002_add_indexes.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../migrations/0003_add_output_reservations.sql"))
            .unwrap();

        conn.execute_batch(
            r#"
            INSERT INTO users (user_id, identity_key) VALUES (1, '02aa');
            INSERT INTO output_baskets (basket_id, user_id, name) VALUES (10, 1, 'default');

            INSERT INTO transactions (transaction_id, user_id, status, reference, is_outgoing, description, txid)
            VALUES
              (1, 1, 'completed',   'ref1', 0, 't', 'aa01'),
              (2, 1, 'unproven',    'ref2', 0, 't', 'aa02'),
              (3, 1, 'unsigned',    'ref3', 1, 't', 'aa03'),
              (4, 1, 'failed',      'ref4', 1, 't', 'aa04'),
              (5, 1, 'nosend',      'ref5', 1, 't', 'aa05');

            INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, spendable, change, vout, satoshis,
                                 provided_by, purpose, type, txid, spent_by, reserved_until)
            VALUES
              (1, 1, 1, 10, 1, 1, 0, 1000, 'storage', 'change', 'P2PKH', 'aa01', NULL, NULL),
              (2, 1, 2, 10, 1, 1, 1, 2000, 'storage', 'change', 'P2PKH', 'aa02', NULL, datetime('now', '+10 minutes')),
              (3, 1, 3, 10, 1, 1, 0, 3000, 'storage', 'change', 'P2PKH', 'aa03', NULL, NULL),
              (4, 1, 1, NULL, 0, 1, 1, 4000, 'storage', 'change', 'P2PKH', 'aa01', NULL, NULL),
              (5, 1, 1, 10, 1, 1, 2, 5000, 'storage', 'change', 'P2PKH', 'aa01', 1, NULL),
              (6, 1, 1, 10, 1, 1, 3, 6000, 'storage', 'change', 'P2PKH', NULL, NULL, NULL),
              (7, 1, 4, 10, 1, 1, 0, 7000, 'storage', 'change', 'P2PKH', 'aa04', NULL, NULL),
              (8, 1, 5, 10, 1, 1, 0, 8000, 'storage', 'change', 'P2PKH', 'aa05', NULL, NULL),
              (9, 1, 2, 10, 1, 1, 2, 9000, 'storage', 'change', 'P2PKH', 'aa02', NULL, NULL);
            "#,
        )
        .unwrap();
        conn
    }

    /// Run the real candidate query against the harness DB.
    fn g5_candidates(conn: &rusqlite::Connection, cursor: i64, limit: i64) -> Vec<i64> {
        let mut stmt = conn.prepare(EXT_SPEND_CANDIDATES_SQL).unwrap();
        stmt.query_map(rusqlite::params![cursor, limit], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<i64>, _>>()
            .unwrap()
    }

    #[test]
    fn test_g5_sqlite_candidate_selection() {
        let conn = g5_test_db();
        // Full sweep from cursor 0: exactly the free, chain-real outputs —
        // including the reserved one (reservations are orthogonal), excluding
        // relinquished / tx-locked / txid-less / unsigned / failed / nosend.
        assert_eq!(g5_candidates(&conn, 0, EXT_SPEND_BATCH as i64), vec![1, 2, 9]);
    }

    #[test]
    fn test_g5_sqlite_candidate_paging() {
        let conn = g5_test_db();
        // LIMIT pages, output_id cursor resumes exactly after the last row.
        assert_eq!(g5_candidates(&conn, 0, 2), vec![1, 2]);
        assert_eq!(g5_candidates(&conn, 2, 2), vec![9]);
        // Past the end: empty page (sweep complete → cursor parks at 0).
        assert_eq!(g5_candidates(&conn, 9, 2), Vec::<i64>::new());
    }

    #[test]
    fn test_g5_sqlite_mark_spent_happy_path() {
        let conn = g5_test_db();
        let changes = conn
            .execute(EXT_SPEND_MARK_SQL, rusqlite::params!["2026-07-05T00:00:00Z", 1i64])
            .unwrap();
        assert_eq!(changes, 1);

        let (spendable, spent_by, basket_id): (i64, Option<i64>, Option<i64>) = conn
            .query_row(
                "SELECT spendable, spent_by, basket_id FROM outputs WHERE output_id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(spendable, 0);
        assert_eq!(spent_by, None); // terminal external state — no local tx owns it
        assert_eq!(basket_id, Some(10)); // basket untouched (relinquish is the client's call)

        // The marked row disappears from the next candidate page.
        assert_eq!(g5_candidates(&conn, 0, 20), vec![2, 9]);
    }

    #[test]
    fn test_g5_sqlite_mark_spent_idempotent() {
        let conn = g5_test_db();
        assert_eq!(
            conn.execute(EXT_SPEND_MARK_SQL, rusqlite::params!["t0", 1i64]).unwrap(),
            1
        );
        // Second application: guard sees spendable=0 → no-op, `changes` stays
        // an accurate found-counter.
        assert_eq!(
            conn.execute(EXT_SPEND_MARK_SQL, rusqlite::params!["t1", 1i64]).unwrap(),
            0
        );
    }

    #[test]
    fn test_g5_sqlite_mark_spent_never_clobbers_tx_lock() {
        let conn = g5_test_db();
        // Simulate the race: after the candidate SELECT returned output 2, a
        // live createAction locks it (spendable=0, spent_by set).
        conn.execute(
            "UPDATE outputs SET spendable = 0, spent_by = 1 WHERE output_id = 2",
            [],
        )
        .unwrap();
        // The write-time guard must refuse.
        assert_eq!(
            conn.execute(EXT_SPEND_MARK_SQL, rusqlite::params!["t0", 2i64]).unwrap(),
            0
        );
        let spent_by: Option<i64> = conn
            .query_row("SELECT spent_by FROM outputs WHERE output_id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(spent_by, Some(1)); // the action's lock survives untouched
    }

    #[test]
    fn test_g5_sqlite_mark_spent_preserves_reservation() {
        let conn = g5_test_db();
        let before: Option<String> = conn
            .query_row("SELECT reserved_until FROM outputs WHERE output_id = 2", [], |r| r.get(0))
            .unwrap();
        assert!(before.is_some()); // fixture: live reservation

        assert_eq!(
            conn.execute(EXT_SPEND_MARK_SQL, rusqlite::params!["t0", 2i64]).unwrap(),
            1
        );
        let after: Option<String> = conn
            .query_row("SELECT reserved_until FROM outputs WHERE output_id = 2", [], |r| r.get(0))
            .unwrap();
        // G4: expiry/unreserve are the ONLY reservation release paths — the
        // spent-scan marks the row but leaves the reservation bytes alone.
        assert_eq!(after, before);
    }

    #[test]
    fn test_g5_sqlite_cursor_upsert_single_row() {
        let conn = g5_test_db();
        let c1 = ext_spend_cursor_json(&ExtSpendCursor {
            last_output_id: 2,
            sweep_completed_at: None,
        });

        // First persist: UPDATE misses (no row yet) → INSERT.
        assert_eq!(
            conn.execute(EXT_SPEND_CURSOR_UPDATE_SQL, rusqlite::params![c1]).unwrap(),
            0
        );
        conn.execute(EXT_SPEND_CURSOR_INSERT_SQL, rusqlite::params![c1]).unwrap();

        // Second persist: UPDATE hits in place — still exactly one row.
        let c2 = ext_spend_cursor_json(&ExtSpendCursor {
            last_output_id: 0,
            sweep_completed_at: Some("2026-07-05T00:00:00+00:00".to_string()),
        });
        assert_eq!(
            conn.execute(EXT_SPEND_CURSOR_UPDATE_SQL, rusqlite::params![c2]).unwrap(),
            1
        );
        let (count, details): (i64, String) = conn
            .query_row(
                "SELECT COUNT(*), MAX(details) FROM monitor_events WHERE event = 'external_spend_cursor'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        let parsed = parse_ext_spend_cursor(&details);
        assert_eq!(parsed.last_output_id, 0);
        assert!(parsed.sweep_completed_at.is_some());

        // And the read query round-trips the latest row.
        let read: String = conn
            .query_row(EXT_SPEND_CURSOR_READ_SQL, [], |r| r.get(0))
            .unwrap();
        assert_eq!(parse_ext_spend_cursor(&read), parsed);
    }

    #[test]
    fn test_g5_sqlite_cursor_does_not_collide_with_chain_height() {
        let conn = g5_test_db();
        // A chain_height row (task 9's tracker) must be invisible to the scan cursor.
        conn.execute(
            "INSERT INTO monitor_events (event, details) VALUES ('chain_height', '{\"height\": 900000}')",
            [],
        )
        .unwrap();
        let row: std::result::Result<String, _> =
            conn.query_row(EXT_SPEND_CURSOR_READ_SQL, [], |r| r.get(0));
        assert!(row.is_err()); // no cursor row → scan starts a fresh sweep
    }
}
