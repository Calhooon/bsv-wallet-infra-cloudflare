//! Process Action — after the client signs, updates DB with txid, script offsets,
//! and creates proven_tx_req for broadcast tracking.
//!
//! Ported from rust-wallet-toolbox/src/storage/sqlx/process_action.rs (1,884 lines).
//! Adapted for D1.
//!
//! Flow:
//! 1. Find transaction by reference, validate status (must be unsigned/unprocessed)
//! 2. Verify txid matches raw_tx hash
//! 3. Parse raw_tx to extract script offsets for all outputs
//! 4. Update transaction: set txid, clear raw_tx/input_beef, update status
//! 5. Update each output: set txid, script_offset, script_length, locking_script (for change)
//! 6. Create proven_tx_req for broadcast queue
//! 7. Determine broadcast status based on noSend/delayed flags

use crate::d1::batch::BatchCollector;
use crate::d1::{QVal, Query};
use crate::error::{Error, Result};
use crate::types::{SendWithResult, StorageProcessActionArgs, StorageProcessActionResults};
use chrono::Utc;
use serde::Deserialize;

use super::StorageD1;

// =============================================================================
// D1 Row Types
// =============================================================================

#[derive(Debug, Deserialize)]
struct TransactionForProcessRow {
    transaction_id: Option<f64>,
    status: Option<String>,
    is_outgoing: Option<f64>,
    input_beef: Option<String>, // hex from hex(input_beef)
}

#[derive(Debug, Deserialize)]
struct OutputForProcessRow {
    output_id: Option<f64>,
    vout: Option<f64>,
    change: Option<f64>,
    custom_instructions: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProvenTxReqIdRow {
    proven_tx_req_id: Option<f64>,
    status: Option<String>,
}

// =============================================================================
// Script offset extraction from raw transaction
// =============================================================================

/// Parsed script location within a raw transaction.
#[derive(Debug, Clone)]
struct ScriptInfo {
    offset: usize,
    length: usize,
}

/// Parse a raw BSV transaction and extract output script offsets.
///
/// BSV transaction format:
/// - 4 bytes: version
/// - varint: input count
/// - inputs (each: 32 txid + 4 vout + varint script_len + script + 4 sequence)
/// - varint: output count
/// - outputs (each: 8 satoshis + varint script_len + script)
/// - 4 bytes: locktime
fn parse_output_scripts(raw_tx: &[u8]) -> Result<Vec<ScriptInfo>> {
    let mut pos = 4; // Skip version

    // Skip inputs
    let (input_count, bytes_read) = read_varint(raw_tx, pos)?;
    pos += bytes_read;

    for _ in 0..input_count {
        pos += 32 + 4; // txid + vout
        let (script_len, bytes_read) = read_varint(raw_tx, pos)?;
        pos += bytes_read + script_len as usize + 4; // script + sequence
    }

    // Parse outputs
    let (output_count, bytes_read) = read_varint(raw_tx, pos)?;
    pos += bytes_read;

    let mut scripts = Vec::with_capacity(output_count as usize);

    for _ in 0..output_count {
        pos += 8; // satoshis
        let (script_len, bytes_read) = read_varint(raw_tx, pos)?;
        pos += bytes_read;
        scripts.push(ScriptInfo {
            offset: pos,
            length: script_len as usize,
        });
        pos += script_len as usize;
    }

    Ok(scripts)
}

/// Read a Bitcoin varint from a byte slice at the given position.
fn read_varint(data: &[u8], pos: usize) -> Result<(u64, usize)> {
    if pos >= data.len() {
        return Err(Error::ValidationError(
            "Unexpected end of transaction data".to_string(),
        ));
    }

    let first = data[pos];
    match first {
        0..=0xFC => Ok((first as u64, 1)),
        0xFD => {
            if pos + 3 > data.len() {
                return Err(Error::ValidationError("Truncated varint".to_string()));
            }
            let val = u16::from_le_bytes([data[pos + 1], data[pos + 2]]);
            Ok((val as u64, 3))
        }
        0xFE => {
            if pos + 5 > data.len() {
                return Err(Error::ValidationError("Truncated varint".to_string()));
            }
            let val =
                u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]);
            Ok((val as u64, 5))
        }
        0xFF => {
            if pos + 9 > data.len() {
                return Err(Error::ValidationError("Truncated varint".to_string()));
            }
            let val = u64::from_le_bytes([
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
                data[pos + 8],
            ]);
            Ok((val, 9))
        }
    }
}

/// Compute txid from raw transaction bytes: SHA256d, reversed.
fn compute_txid(raw_tx: &[u8]) -> String {
    use bsv_sdk::sha256d;
    let hash = sha256d(raw_tx);
    // Reverse for display (Bitcoin's display convention)
    let reversed: Vec<u8> = hash.iter().rev().cloned().collect();
    hex::encode(reversed)
}

// =============================================================================
// Implementation
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// Process a signed transaction: update DB with txid, script offsets,
    /// create broadcast queue entry.
    pub async fn process_action(
        &self,
        user_id: i64,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        let mut bench = crate::bench::BenchTimer::new("process_action");
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        // =====================================================================
        // Step 1: Validate args
        // =====================================================================
        let reference = args.reference.as_deref().ok_or_else(|| {
            Error::ValidationError("processAction requires a reference".to_string())
        })?;

        let provided_txid = args
            .txid
            .as_deref()
            .ok_or_else(|| Error::ValidationError("processAction requires a txid".to_string()))?;

        let raw_tx = args
            .raw_tx
            .as_deref()
            .ok_or_else(|| Error::ValidationError("processAction requires raw_tx".to_string()))?;

        if raw_tx.is_empty() {
            return Err(Error::ValidationError(
                "processAction raw_tx must not be empty".to_string(),
            ));
        }

        // =====================================================================
        // Step 2: Verify txid matches raw_tx
        // =====================================================================
        let computed_txid = compute_txid(raw_tx);
        if computed_txid != provided_txid {
            return Err(Error::ValidationError(format!(
                "txid mismatch: provided {} but raw_tx hashes to {}",
                provided_txid, computed_txid
            )));
        }

        // =====================================================================
        // Step 3: Find transaction by reference
        // =====================================================================
        let tx_row: TransactionForProcessRow = Query::new(
            r#"SELECT transaction_id, status, is_outgoing, hex(input_beef) as input_beef
               FROM transactions WHERE user_id = ? AND reference = ?"#,
        )
        .bind(user_id)
        .bind(reference)
        .fetch_one(self.db)
        .await
        .map_err(|_| {
            Error::NotFound(format!(
                "Transaction with reference '{}' not found",
                reference
            ))
        })?;

        let tx_id = tx_row
            .transaction_id
            .map(|v| v as i64)
            .ok_or_else(|| Error::NotFound("Transaction ID missing".to_string()))?;

        let status = tx_row.status.as_deref().unwrap_or("unknown");
        let is_outgoing = tx_row.is_outgoing.map(|v| v != 0.0).unwrap_or(false);

        // =====================================================================
        // Step 4: Validate transaction state
        // =====================================================================
        if !is_outgoing {
            return Err(Error::ValidationError(
                "Cannot process an incoming transaction".to_string(),
            ));
        }

        match status {
            "unsigned" | "unprocessed" => {} // OK
            _ => {
                return Err(Error::ValidationError(format!(
                    "Cannot process transaction with status '{}'. Must be 'unsigned' or 'unprocessed'",
                    status
                )));
            }
        }

        // =====================================================================
        // Step 5: Parse raw_tx to get output script offsets
        // =====================================================================
        let output_scripts = parse_output_scripts(raw_tx)?;

        // =====================================================================
        // Step 6: Get the stored input_beef for proven_tx_req
        // =====================================================================
        // input_beef may live in R2 (D1 column NULL for >4KB blobs — the COMMON case
        // for deep ancestry). Without this fallback the largest txs silently downgrade
        // to raw-tx-only inline broadcast — the exact orphan-mempool failure of the
        // 2026-04-15 incident (the monitor's send_waiting had this fix; the inline
        // path did not until the 2026-07-13 Arcade migration surfaced it, since Arcade
        // rejects bare raw txs outright instead of quietly accepting them).
        let input_beef_bytes = match tx_row.input_beef.as_deref().filter(|h| !h.is_empty()) {
            Some(h) => hex::decode(h).ok(),
            None => {
                let store = crate::r2::BlobStore::new(self.blobs);
                store
                    .get("transactions", tx_id, "input_beef", None)
                    .await
                    .ok()
                    .flatten()
            }
        };

        // =====================================================================
        // Step 7: Determine target status
        // =====================================================================
        let (tx_status, req_status) = if args.is_no_send && !args.is_send_with {
            ("nosend", "nosend")
        } else if args.is_delayed {
            // 'sending', NOT 'unprocessed' (audit M2): the TS reference
            // promotes a queued delayed send to 'sending' immediately
            // (processAction.ts:177-186), which keeps it structurally out of
            // TaskFailAbandoned's unsigned/unprocessed sweep. Leaving it
            // 'unprocessed' let fail_abandoned kill a fully signed,
            // queued-for-broadcast tx on a pure timeout whenever ARC flapped
            // past the window — releasing inputs that the pending req could
            // still broadcast later.
            ("sending", "unsent")
        } else {
            ("sending", "unprocessed")
        };

        // =====================================================================
        // Step 8: Load output records for this transaction
        // =====================================================================
        let output_rows: Vec<OutputForProcessRow> = Query::new(
            "SELECT output_id, vout, change, custom_instructions FROM outputs WHERE transaction_id = ? ORDER BY vout",
        )
        .bind(tx_id)
        .fetch_all(self.db)
        .await?;

        bench.lap("setup");

        // =====================================================================
        // Step 9: Atomic write batch — update transaction + outputs
        // =====================================================================
        let mut batch = BatchCollector::new(self.db);

        // Update transaction: set txid, status, store raw_tx, clear input_beef.
        //
        // 2026-04-15 FIX (data loss bug): previously this cleared raw_tx = NULL,
        // diverging from bsv-wallet-toolbox-rs/src/storage/sqlx/process_action.rs:516
        // which keeps raw_tx. The reference comment explains why:
        //   "Store raw_tx on the transaction record so child transactions can find
        //    it during BEEF construction. ... input_beef is cleared because the
        //    proven_tx_req record now holds the authoritative copy."
        //
        // Our bug caused every x402 payment's raw_tx to be destroyed at broadcast
        // time. When later txs spent from those parents, BEEF reconstruction was
        // impossible, broadcasts sent only raw_tx (no parent context), miners put
        // the children into orphan mempool, and they never mined. This created
        // the ~800 stuck backlog diagnosed on 2026-04-15. See UNPROVEN-BACKLOG-DESIGN.md.
        // Route raw_tx through BlobStore so >4KB blobs land in R2 instead
        // of blowing up the D1 1MB row limit on multi-input drain/consolidate
        // txs. tx_id is known here so single-phase works.
        let store = crate::r2::BlobStore::new(self.blobs);
        let (raw_tx_d1, _) = store
            .put("transactions", tx_id, "raw_tx", raw_tx)
            .await?;
        batch.add(
            "UPDATE transactions SET txid = ?, status = ?, raw_tx = ?, input_beef = NULL, updated_at = ? WHERE transaction_id = ?",
            vec![
                QVal::Text(provided_txid.to_string()),
                QVal::Text(tx_status.to_string()),
                raw_tx_d1.map(QVal::Blob).unwrap_or(QVal::Null),
                QVal::Text(now_str.clone()),
                QVal::Int(tx_id),
            ],
        )?;

        // Update each output with txid and script offset/length.
        //
        // Ownership predicate (matches the post-proof monitor filter):
        //   is_ours = change = 1 OR custom_instructions IS NOT NULL
        //
        // - change = 1               → storage-generated change; we sign via derivation_*
        // - custom_instructions != ? → user-provided self-send (BRC-29 derivation in JSON)
        // - external recipient       → change = 0 AND custom_instructions = NULL
        //
        // Previously we keyed the spendable flip on `is_change` alone, which
        // meant self-send outputs (change = 0, custom_instructions populated)
        // stayed non-spendable after a successful broadcast and the funds
        // appeared "lost" from the wallet's perspective.
        for row in &output_rows {
            let output_id = row.output_id.map(|v| v as i64).unwrap_or(0);
            let vout = row.vout.map(|v| v as usize).unwrap_or(0);
            let is_change = row.change.map(|v| v != 0.0).unwrap_or(false);
            let has_custom_instructions = row.custom_instructions.is_some();
            let is_ours = is_change || has_custom_instructions;

            if vout >= output_scripts.len() {
                continue; // Skip if vout is out of range (shouldn't happen)
            }

            let script_info = &output_scripts[vout];

            if is_change {
                // Change output: extract locking script from raw_tx and store it.
                // Route through BlobStore — P2PKH scripts are 25 B so stay inline,
                // but non-P2PKH change (if ever used) would overflow D1 without this.
                let script_bytes =
                    &raw_tx[script_info.offset..script_info.offset + script_info.length];
                let (script_d1, _) = store
                    .put("outputs", output_id, "locking_script", script_bytes)
                    .await?;

                batch.add(
                    "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, locking_script = ?, spendable = 1, updated_at = ? WHERE output_id = ?",
                    vec![
                        QVal::Text(provided_txid.to_string()),
                        QVal::Int(script_info.offset as i64),
                        QVal::Int(script_info.length as i64),
                        script_d1.map(QVal::Blob).unwrap_or(QVal::Null),
                        QVal::Text(now_str.clone()),
                        QVal::Int(output_id),
                    ],
                )?;
            } else if is_ours {
                // Self-send: locking_script was already stored at create_action
                // time (user supplied the P2PKH to their own derived address).
                // Record the on-chain offset/length and flip spendable = 1 so
                // future create_action calls can select this UTXO.
                batch.add(
                    "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, spendable = 1, updated_at = ? WHERE output_id = ?",
                    vec![
                        QVal::Text(provided_txid.to_string()),
                        QVal::Int(script_info.offset as i64),
                        QVal::Int(script_info.length as i64),
                        QVal::Text(now_str.clone()),
                        QVal::Int(output_id),
                    ],
                )?;
            } else {
                // External recipient: set txid and script offset only.
                // Keep spendable = 0 — these outputs belong to the recipient.
                batch.add(
                    "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, updated_at = ? WHERE output_id = ?",
                    vec![
                        QVal::Text(provided_txid.to_string()),
                        QVal::Int(script_info.offset as i64),
                        QVal::Int(script_info.length as i64),
                        QVal::Text(now_str.clone()),
                        QVal::Int(output_id),
                    ],
                )?;
            }
        }

        batch.execute().await?;

        // =====================================================================
        // Step 10: Create or update proven_tx_req for broadcast tracking
        // =====================================================================
        let existing_req: Option<ProvenTxReqIdRow> =
            Query::new("SELECT proven_tx_req_id, status FROM proven_tx_reqs WHERE txid = ?")
                .bind(provided_txid)
                .fetch_optional(self.db)
                .await?;

        if let Some(req) = existing_req {
            let req_id = req.proven_tx_req_id.map(|v| v as i64).unwrap_or(0);
            let existing_status = req.status.as_deref().unwrap_or("unknown");

            // raw_tx is NOT NULL in schema AND always present — bind directly
            // (matches reference, keeps constraint honest). Only input_beef
            // (nullable, potentially huge) goes through BlobStore.
            let req_ib_d1 = match input_beef_bytes.as_deref() {
                Some(bytes) => {
                    store.put("proven_tx_reqs", req_id, "input_beef", bytes).await?.0
                }
                None => None,
            };

            // Update with raw_tx + input_beef even if status is terminal.
            // A prior network-fallback INSERT may have created this entry
            // WITHOUT input_beef. We must fill it in now so future
            // build_input_beef calls can reconstruct the BEEF chain.
            // (Matches Go toolbox's upsertKnownTx which always overwrites.)
            if matches!(existing_status, "completed" | "unmined" | "unproven") {
                // Terminal state — preserve status but fill in missing data
                Query::new(
                    "UPDATE proven_tx_reqs SET raw_tx = ?, input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?",
                )
                .bind(raw_tx)
                .bind(req_ib_d1)
                .bind(now)
                .bind(req_id)
                .execute(self.db)
                .await?;
            } else {
                Query::new(
                    "UPDATE proven_tx_reqs SET status = ?, raw_tx = ?, input_beef = ?, attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?",
                )
                .bind(req_status)
                .bind(raw_tx)
                .bind(req_ib_d1)
                .bind(now)
                .bind(req_id)
                .execute(self.db)
                .await?;
            }
        } else {
            // Two-phase: INSERT with blob columns = NULL to reserve the PK,
            // then BlobStore.put() with the assigned req_id, then UPDATE to
            // fill in either the inline blobs or the R2-routed NULLs.
            let notify_json = format!("{{\"transactionIds\":[{}]}}", tx_id);

            // Single-phase INSERT for raw_tx (NOT NULL, always available here —
            // the client's signed tx). Reference binds raw_tx directly; we do
            // the same. For input_beef (nullable + potentially large), use
            // two-phase so R2 overflow works.
            let req_meta = Query::new(
                r#"INSERT INTO proven_tx_reqs
                    (txid, status, attempts, history, notify, notified, raw_tx, input_beef, created_at, updated_at)
                   VALUES (?, ?, 1, '{}', ?, 0, ?, NULL, ?, ?)"#,
            )
            .bind(provided_txid)
            .bind(req_status)
            .bind(notify_json.as_str())
            .bind(raw_tx)
            .bind(now)
            .bind(now)
            .execute(self.db)
            .await?;
            let req_id = req_meta.last_row_id;
            if let Some(ib_bytes) = input_beef_bytes.as_deref() {
                let (ib_d1, _) = store
                    .put("proven_tx_reqs", req_id, "input_beef", ib_bytes)
                    .await?;
                Query::new(
                    "UPDATE proven_tx_reqs SET input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?",
                )
                .bind(ib_d1)
                .bind(now)
                .bind(req_id)
                .execute(self.db)
                .await?;
            }
        }
        bench.lap("write_batch_and_req");

        // =====================================================================
        // Step 11: Broadcast transaction to BSV network
        //
        // Preference order (matches bsv-wallet-toolbox-rs `post_beef` pattern):
        //  1. Build a BEEF from stored input_beef (ancestors) + new raw_tx and
        //     call broadcast_beef so miners receive full ancestry — prevents
        //     orphan mempool issues when our parents haven't propagated yet.
        //  2. Fall back to broadcast_raw_tx only if input_beef is unavailable
        //     or the BEEF merge fails.
        //
        // This is the fix for the 2026-04-15 incident (805 stuck txs). See
        // UNPROVEN-BACKLOG-DESIGN.md for full context.
        // =====================================================================
        // Delayed actions NEVER broadcast inline (review M-A: the M2 status
        // change made tx_status 'sending' for delayed too, which
        // accidentally routed them through this inline block — blocking the
        // RPC on ARC and defeating the whole point of is_delayed; the
        // monitor's send_waiting owns delayed broadcast via the 'unsent'
        // req).
        if tx_status == "sending" && !args.is_delayed {
            let beef_hex_opt: Option<String> = input_beef_bytes.as_ref().and_then(|ib| {
                use bsv_sdk::transaction::Beef;
                match Beef::from_binary(ib) {
                    Ok(mut beef) => {
                        beef.merge_raw_tx(raw_tx.to_vec(), None);
                        Some(hex::encode(beef.to_binary()))
                    }
                    Err(e) => {
                        worker::console_error!(
                            "processAction: could not rebuild BEEF for {} (falling back to raw_tx): {}",
                            provided_txid,
                            e
                        );
                        None
                    }
                }
            });

            let broadcast_result = match &beef_hex_opt {
                Some(beef_hex) => self.broadcast.broadcast_beef(beef_hex).await,
                None => {
                    let raw_tx_hex = hex::encode(raw_tx);
                    self.broadcast.broadcast_raw_tx(&raw_tx_hex).await
                }
            };

            match broadcast_result {
                Ok(result) => {
                    if result.seen_on_network {
                        worker::console_log!(
                            "processAction: broadcast seen on network for txid={}",
                            provided_txid
                        );
                    } else {
                        worker::console_log!(
                            "processAction: broadcast OK for txid={}",
                            provided_txid
                        );
                    }
                    // Update to unproven/unmined (broadcast succeeded)
                    let mut bcast_batch = BatchCollector::new(self.db);
                    let _ = bcast_batch.add(
                        "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE transaction_id = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    let _ = bcast_batch.add(
                        "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE txid = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Text(provided_txid.to_string())],
                    );
                    let _ = bcast_batch.execute().await;
                }
                Err(crate::services::BroadcastError::DoubleSpend(msg)) => {
                    // DOUBLE-SPEND: Input already spent by a competing tx.
                    // Permanent failure -- mark as failed, release locked inputs.
                    worker::console_log!(
                        "processAction: broadcast double-spend for txid={}: {}",
                        provided_txid,
                        msg
                    );
                    let mut fail_batch = BatchCollector::new(self.db);
                    let _ = fail_batch.add(
                        "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    let _ = fail_batch.add(
                        "UPDATE proven_tx_reqs SET status = 'doubleSpend', updated_at = ? WHERE txid = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Text(provided_txid.to_string())],
                    );
                    let _ = fail_batch.add(
                        "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? WHERE spent_by = ? AND basket_id IS NOT NULL",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    // Clean up the outputs this tx was going to create: since the
                    // tx failed and never hit chain, they're phantoms. Without
                    // this the coin selector will keep picking them as inputs
                    // and bricking subsequent consolidation rounds.
                    let _ = fail_batch.add(
                        "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    let _ = fail_batch.execute().await;
                }
                Err(crate::services::BroadcastError::InvalidTx(msg)) => {
                    // INVALID TX: Malformed transaction, script verification failed, etc.
                    // Permanent failure -- mark as failed, release locked inputs.
                    worker::console_log!(
                        "processAction: broadcast invalid tx for txid={}: {}",
                        provided_txid,
                        msg
                    );

                    let mut fail_batch = BatchCollector::new(self.db);
                    let _ = fail_batch.add(
                        "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    let _ = fail_batch.add(
                        "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE txid = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Text(provided_txid.to_string())],
                    );
                    let _ = fail_batch.add(
                        "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? WHERE spent_by = ? AND basket_id IS NOT NULL",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    // Clean up the outputs this tx was going to create — phantoms
                    // if we don't (see comment in the DoubleSpend branch above).
                    let _ = fail_batch.add(
                        "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    let _ = fail_batch.execute().await;
                }
                Err(crate::services::BroadcastError::ServiceError(msg)) => {
                    // SERVICE ERROR: All ARC endpoints returned ambiguous (timeout,
                    // 5xx, or tx accepted but not SEEN_ON_NETWORK within waitFor).
                    // The tx may still have propagated — e.g. WoC mempool picks it up
                    // while ARC takes longer to flip its status. Marking 'failed' +
                    // releasing inputs here created the phantom-UTXO cascade.
                    //
                    // Canonical wallet-toolbox serviceError semantics (TS
                    // `attemptToPostReqsToNetwork.ts:240-244`): both statuses go to
                    // 'sending', increment attempts, DO NOT release inputs, DO NOT
                    // mark future outputs unspendable. Monitor's `check_for_proofs`
                    // polls `proven_tx_reqs.status='sending'` and reconciles on
                    // proof arrival. True zombies eventually drain via
                    // the canary/escalation machinery (never by attempt count).
                    worker::console_log!(
                        "processAction: broadcast service error for txid={} — leaving 'sending' for monitor: {}",
                        provided_txid,
                        msg
                    );
                    let mut retry_batch = BatchCollector::new(self.db);
                    let _ = retry_batch.add(
                        "UPDATE transactions SET status = 'sending', updated_at = ? WHERE transaction_id = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                    );
                    let _ = retry_batch.add(
                        "UPDATE proven_tx_reqs SET status = 'sending', attempts = attempts + 1, updated_at = ? WHERE txid = ?",
                        vec![QVal::Text(now_str.clone()), QVal::Text(provided_txid.to_string())],
                    );
                    let _ = retry_batch.execute().await;
                }
            }
        }
        bench.lap("broadcast");

        // =====================================================================
        // Step 12: Build send_with_results
        // =====================================================================
        let mut send_results = Vec::new();

        if !args.is_no_send || args.is_send_with {
            // The main transaction
            send_results.push(SendWithResult {
                txid: provided_txid.to_string(),
                status: "sending".to_string(),
            });

            // Process send_with txids (batch broadcast)
            for sw_txid in &args.send_with {
                send_results.push(SendWithResult {
                    txid: sw_txid.clone(),
                    status: "sending".to_string(),
                });

                // Update the send_with transaction's status
                Query::new(
                    "UPDATE transactions SET status = 'sending', updated_at = ? WHERE txid = ? AND user_id = ?",
                )
                .bind(now)
                .bind(sw_txid.as_str())
                .bind(user_id)
                .execute(self.db)
                .await?;

                // Queue the companion's proven_tx_req for ACTUAL broadcast
                // (audit M6): the reference posts the whole sendWith batch
                // (processAction.ts shareReqsWithWorld). The old code set the
                // tx 'sending' but left the req at 'nosend' — a status
                // claiming broadcast activity that never happens (send_waiting
                // only selects 'unsent'/'sending' reqs). 'nosend' → 'unsent'
                // hands it to the next monitor cycle; ARC's verdict then
                // drives the real status.
                Query::new(
                    "UPDATE proven_tx_reqs SET status = 'unsent', updated_at = ? \
                     WHERE txid = ? AND status = 'nosend' \
                       AND EXISTS (SELECT 1 FROM transactions tx \
                                   WHERE tx.txid = proven_tx_reqs.txid AND tx.user_id = ?)",
                )
                .bind(now)
                .bind(sw_txid.as_str())
                .bind(user_id)
                .execute(self.db)
                .await?;
            }
        }
        bench.done();

        Ok(StorageProcessActionResults {
            send_with_results: Some(send_results),
            not_delayed_results: None,
            log: None,
        })
    }

    /// Called after a broadcast attempt to update transaction status.
    ///
    /// On success: transaction → 'unproven', proven_tx_req → 'unmined'
    /// On failure: restore inputs (spendable), transaction → 'failed', proven_tx_req → 'invalid'
    pub async fn update_transaction_status_after_broadcast(
        &self,
        user_id: i64,
        txid: &str,
        success: bool,
    ) -> Result<()> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        if success {
            // Chain-truth gate (audit minor-2): 'unproven'/'unmined' are
            // statements that the tx IS on the network — a client claim is
            // not evidence. Verify with the status service before promoting;
            // if the network doesn't know the txid (or the check fails),
            // leave the current status alone — the server's own broadcast
            // path and the monitor own the truth.
            let net_status = self
                .broadcast
                .get_status_for_txids(&[txid.to_string()])
                .await
                .ok()
                .and_then(|v| v.into_iter().next())
                .map(|s| s.status)
                .unwrap_or_else(|| "unavailable".to_string());
            if net_status != "known" && net_status != "mined" {
                worker::console_log!(
                    "update_transaction_status_after_broadcast: client claims success for {} but network says '{}' — not promoting",
                    txid,
                    net_status
                );
                return Ok(());
            }

            let mut batch = BatchCollector::new(self.db);

            batch.add(
                "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ? AND user_id = ?",
                vec![
                    QVal::Text(now_str.clone()),
                    QVal::Text(txid.to_string()),
                    QVal::Int(user_id),
                ],
            )?;

            batch.add(
                "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE txid = ?",
                vec![QVal::Text(now_str), QVal::Text(txid.to_string())],
            )?;

            batch.execute().await?;
        } else {
            // Reconcile guard: the server-side `processAction` broadcasts too,
            // and the client's re-broadcast can race the server's. If the
            // server already transitioned this tx to `unproven` / `completed`
            // (broadcast succeeded) or `sending` (broadcast in flight), do
            // NOT overwrite to `failed` on the client's say-so — that would
            // release inputs we may already have consumed on chain, creating
            // a real double-spend risk. Log a warning and return Ok.
            #[derive(Deserialize)]
            struct TxRow {
                transaction_id: Option<f64>,
                status: Option<String>,
            }

            let tx_row: Option<TxRow> = Query::new(
                "SELECT transaction_id, status FROM transactions WHERE txid = ? AND user_id = ?",
            )
            .bind(txid)
            .bind(user_id)
            .fetch_optional(self.db)
            .await?;

            if let Some(row) = tx_row {
                let tx_id = row.transaction_id.map(|v| v as i64).unwrap_or(0);
                let current_status = row.status.as_deref().unwrap_or("");

                // Only fail-close when the tx is still in a pre-broadcast or
                // actively-sending state. After `unproven`/`completed`, the
                // server owns the truth and the monitor can retry/verify.
                const FAILABLE_FROM: &[&str] =
                    &["unsigned", "unprocessed", "nosend", "nonfinal", "sending"];
                // Chain-truth gate, mirroring the success branch (parity
                // audit Q1a): a client's failure claim must not fail a tx
                // the network already knows — the server's own broadcast
                // may have raced ahead of the client's.
                let net_status = self
                    .broadcast
                    .get_status_for_txids(&[txid.to_string()])
                    .await
                    .ok()
                    .and_then(|v| v.into_iter().next())
                    .map(|s| s.status)
                    .unwrap_or_else(|| "unavailable".to_string());
                if net_status == "known" || net_status == "mined" {
                    worker::console_log!(
                        "update_transaction_status_after_broadcast: client claims failure for {} but network says '{}' — refusing to fail",
                        txid,
                        net_status
                    );
                    return Ok(());
                }
                if !FAILABLE_FROM.contains(&current_status) {
                    worker::console_log!(
                        "update_transaction_status_after_broadcast: refusing to mark \
                         tx {} as failed — current server status is '{}'. Trusting server.",
                        txid,
                        current_status
                    );
                    return Ok(());
                }

                let mut batch = BatchCollector::new(self.db);

                // Restore locked inputs. All three input-release statements in
                // this file share two invariants with abort_action/fail_abandoned:
                //   G4 — never clear `reserved_until` (reserveOutputs
                //        reservations release only by expiry/unreserveOutputs);
                //   G5 — `basket_id IS NOT NULL` so relinquished (externally
                //        spent) outputs are not resurrected to spendable=1.
                batch.add(
                    "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? WHERE spent_by = ? AND basket_id IS NOT NULL",
                    vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                )?;

                // Mark transaction failed
                batch.add(
                    "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                    vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
                )?;

                // Mark proven_tx_req invalid
                batch.add(
                    "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE txid = ?",
                    vec![QVal::Text(now_str.clone()), QVal::Text(txid.to_string())],
                )?;

                // Clean up the outputs this tx was going to create — phantoms
                // if we don't (match the DoubleSpend/InvalidTx branches above).
                batch.add(
                    "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ?",
                    vec![QVal::Text(now_str), QVal::Int(tx_id)],
                )?;

                batch.execute().await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // read_varint
    // =========================================================================

    #[test]
    fn read_varint_single_byte_zero() {
        let data = [0x00];
        let (val, size) = read_varint(&data, 0).unwrap();
        assert_eq!(val, 0);
        assert_eq!(size, 1);
    }

    #[test]
    fn read_varint_single_byte_max() {
        let data = [0xFC];
        let (val, size) = read_varint(&data, 0).unwrap();
        assert_eq!(val, 0xFC);
        assert_eq!(size, 1);
    }

    #[test]
    fn read_varint_single_byte_midrange() {
        let data = [42];
        let (val, size) = read_varint(&data, 0).unwrap();
        assert_eq!(val, 42);
        assert_eq!(size, 1);
    }

    #[test]
    fn read_varint_two_byte() {
        // 0xFD prefix + 2 bytes LE
        let data = [0xFD, 0x00, 0x01]; // 256
        let (val, size) = read_varint(&data, 0).unwrap();
        assert_eq!(val, 256);
        assert_eq!(size, 3);
    }

    #[test]
    fn read_varint_two_byte_max() {
        let data = [0xFD, 0xFF, 0xFF]; // 65535
        let (val, size) = read_varint(&data, 0).unwrap();
        assert_eq!(val, 65535);
        assert_eq!(size, 3);
    }

    #[test]
    fn read_varint_four_byte() {
        let data = [0xFE, 0x00, 0x00, 0x01, 0x00]; // 65536
        let (val, size) = read_varint(&data, 0).unwrap();
        assert_eq!(val, 65536);
        assert_eq!(size, 5);
    }

    #[test]
    fn read_varint_eight_byte() {
        let data = [0xFF, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00]; // 2^32
        let (val, size) = read_varint(&data, 0).unwrap();
        assert_eq!(val, 0x100000000);
        assert_eq!(size, 9);
    }

    #[test]
    fn read_varint_at_offset() {
        let data = [0x00, 0x00, 42]; // varint at position 2
        let (val, size) = read_varint(&data, 2).unwrap();
        assert_eq!(val, 42);
        assert_eq!(size, 1);
    }

    #[test]
    fn read_varint_empty_data() {
        let data: [u8; 0] = [];
        let result = read_varint(&data, 0);
        assert!(result.is_err());
    }

    #[test]
    fn read_varint_out_of_bounds() {
        let data = [0x42];
        let result = read_varint(&data, 5);
        assert!(result.is_err());
    }

    #[test]
    fn read_varint_truncated_two_byte() {
        let data = [0xFD, 0x00];
        let result = read_varint(&data, 0);
        assert!(result.is_err());
    }

    #[test]
    fn read_varint_truncated_four_byte() {
        let data = [0xFE, 0x00, 0x00, 0x01];
        let result = read_varint(&data, 0);
        assert!(result.is_err());
    }

    #[test]
    fn read_varint_truncated_eight_byte() {
        let data = [0xFF, 0x00, 0x00, 0x00, 0x00];
        let result = read_varint(&data, 0);
        assert!(result.is_err());
    }

    // =========================================================================
    // parse_output_scripts
    // =========================================================================

    /// Build a minimal valid BSV transaction with given outputs (no inputs).
    fn build_raw_tx(outputs: &[(u64, &[u8])]) -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1u32.to_le_bytes()); // version
        tx.push(0x00); // 0 inputs
        tx.push(outputs.len() as u8); // output count
        for (sats, script) in outputs {
            tx.extend_from_slice(&sats.to_le_bytes());
            tx.push(script.len() as u8);
            tx.extend_from_slice(script);
        }
        tx.extend_from_slice(&0u32.to_le_bytes()); // locktime
        tx
    }

    /// Build a raw tx with 1 input and N outputs.
    fn build_raw_tx_with_input(outputs: &[(u64, &[u8])]) -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1u32.to_le_bytes()); // version
        tx.push(0x01); // 1 input
        tx.extend_from_slice(&[0u8; 32]); // prev txid
        tx.extend_from_slice(&0u32.to_le_bytes()); // prev vout
        tx.push(0x00); // empty unlock script
        tx.extend_from_slice(&0xFFFFFFFFu32.to_le_bytes()); // sequence
        tx.push(outputs.len() as u8);
        for (sats, script) in outputs {
            tx.extend_from_slice(&sats.to_le_bytes());
            tx.push(script.len() as u8);
            tx.extend_from_slice(script);
        }
        tx.extend_from_slice(&0u32.to_le_bytes()); // locktime
        tx
    }

    #[test]
    fn parse_output_scripts_single_output() {
        let script = vec![0x76u8, 0xa9, 0x14]; // 3-byte script
        let raw_tx = build_raw_tx(&[(50000, &script)]);
        let scripts = parse_output_scripts(&raw_tx).unwrap();
        assert_eq!(scripts.len(), 1);
        assert_eq!(scripts[0].length, 3);
        assert_eq!(
            &raw_tx[scripts[0].offset..scripts[0].offset + scripts[0].length],
            script.as_slice()
        );
    }

    #[test]
    fn parse_output_scripts_two_outputs() {
        let script1 = [0x76, 0xa9, 0x14];
        let script2 = [0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef];
        let raw_tx = build_raw_tx(&[(1000, &script1), (0, &script2)]);
        let scripts = parse_output_scripts(&raw_tx).unwrap();
        assert_eq!(scripts.len(), 2);
        assert_eq!(scripts[0].length, 3);
        assert_eq!(scripts[1].length, 6);
    }

    #[test]
    fn parse_output_scripts_with_input() {
        let script = vec![0xab; 25];
        let raw_tx = build_raw_tx_with_input(&[(10000, &script)]);
        let scripts = parse_output_scripts(&raw_tx).unwrap();
        assert_eq!(scripts.len(), 1);
        assert_eq!(scripts[0].length, 25);
        assert_eq!(
            &raw_tx[scripts[0].offset..scripts[0].offset + 25],
            script.as_slice()
        );
    }

    #[test]
    fn parse_output_scripts_zero_outputs() {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1u32.to_le_bytes());
        tx.push(0x00); // 0 inputs
        tx.push(0x00); // 0 outputs
        tx.extend_from_slice(&0u32.to_le_bytes());
        let scripts = parse_output_scripts(&tx).unwrap();
        assert!(scripts.is_empty());
    }

    #[test]
    fn parse_output_scripts_empty_script() {
        let raw_tx = build_raw_tx(&[(0, &[])]);
        let scripts = parse_output_scripts(&raw_tx).unwrap();
        assert_eq!(scripts.len(), 1);
        assert_eq!(scripts[0].length, 0);
    }

    #[test]
    fn parse_output_scripts_offsets_correct() {
        let script1 = [0x01, 0x02, 0x03];
        let script2 = [0x04, 0x05];
        let raw_tx = build_raw_tx(&[(100, &script1), (200, &script2)]);
        let scripts = parse_output_scripts(&raw_tx).unwrap();
        assert_eq!(
            &raw_tx[scripts[0].offset..scripts[0].offset + scripts[0].length],
            &script1
        );
        assert_eq!(
            &raw_tx[scripts[1].offset..scripts[1].offset + scripts[1].length],
            &script2
        );
    }

    // =========================================================================
    // compute_txid
    // =========================================================================

    #[test]
    fn compute_txid_is_deterministic() {
        let raw_tx = build_raw_tx(&[(50000, &[0x76, 0xa9])]);
        assert_eq!(compute_txid(&raw_tx), compute_txid(&raw_tx));
    }

    #[test]
    fn compute_txid_is_64_hex_chars() {
        let raw_tx = build_raw_tx(&[(50000, &[0x76, 0xa9])]);
        let txid = compute_txid(&raw_tx);
        assert_eq!(txid.len(), 64);
        assert!(txid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn compute_txid_different_for_different_tx() {
        let tx1 = build_raw_tx(&[(50000, &[0x76, 0xa9])]);
        let tx2 = build_raw_tx(&[(60000, &[0x76, 0xa9])]);
        assert_ne!(compute_txid(&tx1), compute_txid(&tx2));
    }

    #[test]
    fn compute_txid_mismatch_detected() {
        let raw_tx = build_raw_tx(&[(50000, &[0x76, 0xa9])]);
        let computed = compute_txid(&raw_tx);
        let wrong = "0".repeat(64);
        assert_ne!(computed, wrong);
    }

    // =========================================================================
    // ScriptInfo
    // =========================================================================

    #[test]
    fn script_info_clone() {
        let info = ScriptInfo {
            offset: 100,
            length: 25,
        };
        let cloned = info.clone();
        assert_eq!(cloned.offset, 100);
        assert_eq!(cloned.length, 25);
    }

    // =========================================================================
    // Status validation (mirrors process_action Step 4)
    // =========================================================================

    fn validate_process_status(status: &str, is_outgoing: bool) -> std::result::Result<(), String> {
        if !is_outgoing {
            return Err("Cannot process an incoming transaction".to_string());
        }
        match status {
            "unsigned" | "unprocessed" => Ok(()),
            _ => Err(format!(
                "Cannot process transaction with status '{}'",
                status
            )),
        }
    }

    #[test]
    fn status_unsigned_outgoing_ok() {
        assert!(validate_process_status("unsigned", true).is_ok());
    }

    #[test]
    fn status_unprocessed_outgoing_ok() {
        assert!(validate_process_status("unprocessed", true).is_ok());
    }

    #[test]
    fn status_completed_rejected() {
        assert!(validate_process_status("completed", true).is_err());
    }

    #[test]
    fn status_sending_rejected() {
        assert!(validate_process_status("sending", true).is_err());
    }

    #[test]
    fn status_unproven_rejected() {
        assert!(validate_process_status("unproven", true).is_err());
    }

    #[test]
    fn status_failed_rejected() {
        assert!(validate_process_status("failed", true).is_err());
    }

    #[test]
    fn status_nosend_rejected() {
        assert!(validate_process_status("nosend", true).is_err());
    }

    #[test]
    fn status_incoming_rejected() {
        assert!(validate_process_status("unsigned", false).is_err());
    }

    // =========================================================================
    // Target status determination (mirrors Step 7)
    // =========================================================================

    fn determine_target_status(
        is_no_send: bool,
        is_send_with: bool,
        is_delayed: bool,
    ) -> (&'static str, &'static str) {
        if is_no_send && !is_send_with {
            ("nosend", "nosend")
        } else if is_delayed {
            // M2: delayed commits as tx 'sending' (TS processAction.ts:177-186
            // parity) — structurally outside fail_abandoned's draft sweep.
            // Inline broadcast is separately gated on !is_delayed (M-A).
            ("sending", "unsent")
        } else {
            ("sending", "unprocessed")
        }
    }

    #[test]
    fn target_status_normal() {
        let (tx, req) = determine_target_status(false, false, false);
        assert_eq!(tx, "sending");
        assert_eq!(req, "unprocessed");
    }

    #[test]
    fn target_status_no_send() {
        let (tx, req) = determine_target_status(true, false, false);
        assert_eq!(tx, "nosend");
        assert_eq!(req, "nosend");
    }

    #[test]
    fn target_status_delayed() {
        let (tx, req) = determine_target_status(false, false, true);
        assert_eq!(tx, "sending"); // M2: TS parity — see determine_target_status
        assert_eq!(req, "unsent");
    }

    #[test]
    fn target_status_no_send_with_send_with() {
        // is_no_send=true + is_send_with=true => NOT nosend
        let (tx, req) = determine_target_status(true, true, false);
        assert_eq!(tx, "sending");
        assert_eq!(req, "unprocessed");
    }

    // =========================================================================
    // D1 row type deserialization
    // =========================================================================

    #[test]
    fn transaction_for_process_row_full() {
        let val = serde_json::json!({
            "transaction_id": 100.0,
            "status": "unsigned",
            "is_outgoing": 1.0,
            "input_beef": "deadbeef"
        });
        let row: TransactionForProcessRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.transaction_id, Some(100.0));
        assert_eq!(row.status, Some("unsigned".to_string()));
        assert_eq!(row.is_outgoing, Some(1.0));
        assert_eq!(row.input_beef, Some("deadbeef".to_string()));
    }

    #[test]
    fn transaction_for_process_row_nulls() {
        let val = serde_json::json!({
            "transaction_id": null,
            "status": null,
            "is_outgoing": null,
            "input_beef": null
        });
        let row: TransactionForProcessRow = serde_json::from_value(val).unwrap();
        assert!(row.transaction_id.is_none());
        assert!(row.status.is_none());
        assert!(row.is_outgoing.is_none());
        assert!(row.input_beef.is_none());
    }

    #[test]
    fn output_for_process_row_change() {
        let val = serde_json::json!({
            "output_id": 50.0,
            "vout": 0.0,
            "change": 1.0
        });
        let row: OutputForProcessRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.change.map(|v| v != 0.0), Some(true));
    }

    #[test]
    fn output_for_process_row_not_change() {
        let val = serde_json::json!({
            "output_id": 51.0,
            "vout": 1.0,
            "change": 0.0
        });
        let row: OutputForProcessRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.change.map(|v| v != 0.0), Some(false));
    }

    // =========================================================================
    // Terminal status check for proven_tx_req
    // =========================================================================

    fn is_terminal_proven_tx_req_status(status: &str) -> bool {
        matches!(status, "completed" | "unmined" | "unproven")
    }

    #[test]
    fn terminal_completed() {
        assert!(is_terminal_proven_tx_req_status("completed"));
    }

    #[test]
    fn terminal_unmined() {
        assert!(is_terminal_proven_tx_req_status("unmined"));
    }

    #[test]
    fn terminal_unproven() {
        assert!(is_terminal_proven_tx_req_status("unproven"));
    }

    #[test]
    fn non_terminal_unprocessed() {
        assert!(!is_terminal_proven_tx_req_status("unprocessed"));
    }

    #[test]
    fn non_terminal_sending() {
        assert!(!is_terminal_proven_tx_req_status("sending"));
    }

    #[test]
    fn non_terminal_nosend() {
        assert!(!is_terminal_proven_tx_req_status("nosend"));
    }
}
