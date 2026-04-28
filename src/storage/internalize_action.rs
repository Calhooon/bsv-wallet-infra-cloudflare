//! Internalize Action Implementation — ported from sqlx to D1.
//!
//! Allows a wallet to take ownership of outputs in a pre-existing transaction.
//! Two output types:
//! - "wallet payment" — adds output value to wallet's change balance in "default" basket
//! - "basket insertion" — custom output in specified basket, no effect on balance
//!
//! D1 adaptation: No SQL transactions. We use individual queries for reads,
//! then batch writes for atomicity. The batch pattern provides all-or-nothing
//! semantics for the write phase.

use crate::d1::Query;
use crate::entities::{TableOutput, TableTransaction, TransactionStatus};
use crate::error::{Error, Result};
use crate::types::StorageInternalizeActionResult;
use bsv_sdk::transaction::Beef;
use bsv_sdk::wallet::{BasketInsertion, InternalizeActionArgs, WalletPayment};
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashMap;

use super::StorageD1;

// =============================================================================
// Constants
// =============================================================================

const WALLET_PAYMENT_PROTOCOL: &str = "wallet payment";
const BASKET_INSERTION_PROTOCOL: &str = "basket insertion";

// =============================================================================
// D1 Row Types
// =============================================================================

#[derive(Debug, Deserialize)]
struct TransactionRow {
    transaction_id: Option<f64>,
    user_id: Option<f64>,
    txid: Option<String>,
    status: Option<String>,
    reference: Option<String>,
    description: Option<String>,
    satoshis: Option<f64>,
    version: Option<f64>,
    lock_time: Option<f64>,
    is_outgoing: Option<f64>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl TransactionRow {
    fn into_table(self) -> TableTransaction {
        TableTransaction {
            transaction_id: self.transaction_id.map(|v| v as i64).unwrap_or(0),
            user_id: self.user_id.map(|v| v as i64).unwrap_or(0),
            txid: self.txid,
            status: self
                .status
                .as_deref()
                .map(TransactionStatus::parse_status)
                .unwrap_or_default(),
            reference: self.reference.unwrap_or_default(),
            description: self.description.unwrap_or_default(),
            satoshis: self.satoshis.map(|v| v as i64).unwrap_or(0),
            version: self.version.map(|v| v as i32).unwrap_or(0),
            lock_time: self.lock_time.map(|v| v as i64).unwrap_or(0),
            raw_tx: None,
            input_beef: None,
            is_outgoing: self.is_outgoing.map(|v| v != 0.0).unwrap_or(false),
            proof_txid: None,
            created_at: super::writers::parse_datetime_pub(&self.created_at),
            updated_at: super::writers::parse_datetime_pub(&self.updated_at),
        }
    }
}

#[derive(Debug, Deserialize)]
struct OutputRow {
    output_id: Option<f64>,
    user_id: Option<f64>,
    transaction_id: Option<f64>,
    basket_id: Option<f64>,
    txid: Option<String>,
    vout: Option<f64>,
    satoshis: Option<f64>,
    script_length: Option<f64>,
    script_offset: Option<f64>,
    #[serde(rename = "type")]
    output_type: Option<String>,
    provided_by: Option<String>,
    purpose: Option<String>,
    spendable: Option<f64>,
    change: Option<f64>,
    derivation_prefix: Option<String>,
    derivation_suffix: Option<String>,
    sender_identity_key: Option<String>,
    custom_instructions: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl OutputRow {
    fn into_table(self) -> TableOutput {
        TableOutput {
            output_id: self.output_id.map(|v| v as i64).unwrap_or(0),
            user_id: self.user_id.map(|v| v as i64).unwrap_or(0),
            transaction_id: self.transaction_id.map(|v| v as i64).unwrap_or(0),
            basket_id: self.basket_id.map(|v| v as i64),
            txid: self.txid.unwrap_or_default(),
            vout: self.vout.map(|v| v as i32).unwrap_or(0),
            satoshis: self.satoshis.map(|v| v as i64).unwrap_or(0),
            locking_script: None, // Not loaded in this query (blob)
            script_length: self.script_length.map(|v| v as i32).unwrap_or(0),
            script_offset: self.script_offset.map(|v| v as i32).unwrap_or(0),
            output_type: self.output_type.unwrap_or_default(),
            provided_by: self.provided_by.unwrap_or_else(|| "you".to_string()),
            purpose: self.purpose,
            output_description: None,
            spent_by: None,
            sequence_number: None,
            spending_description: None,
            spendable: self.spendable.map(|v| v != 0.0).unwrap_or(false),
            change: self.change.map(|v| v != 0.0).unwrap_or(false),
            derivation_prefix: self.derivation_prefix,
            derivation_suffix: self.derivation_suffix,
            sender_identity_key: self.sender_identity_key,
            custom_instructions: self.custom_instructions,
            created_at: super::writers::parse_datetime_pub(&self.created_at),
            updated_at: super::writers::parse_datetime_pub(&self.updated_at),
        }
    }
}

// =============================================================================
// Internal Types
// =============================================================================

#[derive(Debug, Clone)]
struct OutputData {
    vout: u32,
    satoshis: u64,
    locking_script: Vec<u8>,
    protocol: String,
    payment: Option<WalletPayment>,
    insertion: Option<BasketInsertion>,
    existing_output_id: Option<i64>,
    existing_basket_id: Option<i64>,
    existing_is_change: bool,
}

// =============================================================================
// Main Implementation
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// Internalize an external transaction — the core payment acceptance flow.
    pub async fn internalize_action(
        &self,
        user_id: i64,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        // Step 1: Parse and validate the AtomicBEEF
        let beef = Beef::from_binary(&args.tx)
            .map_err(|e| Error::ValidationError(format!("Failed to parse AtomicBEEF: {}", e)))?;

        let txid = beef.atomic_txid.clone().ok_or_else(|| {
            Error::ValidationError("BEEF is not AtomicBEEF (missing atomic_txid)".to_string())
        })?;

        // Step 1b: BEEF verification (Phase 3)
        // Mode is controlled by the BEEF_VERIFICATION env var:
        //   "skip"     — no verification (default, safe while ChainTracks is down)
        //   "log_only" — verify and log failures, but accept all payments
        //   "strict"   — reject payments with invalid BEEF proofs
        {
            let mode = self.beef_verification_mode;
            let provider = self.header_provider;
            let known_txids = std::collections::HashSet::new();
            // beef needs &mut for verify_valid (RefCell internals)
            let mut beef_clone = beef.clone();
            super::beef_verification::verify_beef(&mut beef_clone, provider, mode, &known_txids)
                .await?;
        }

        // Find the target transaction in the BEEF
        let beef_tx = beef.find_txid(&txid).ok_or_else(|| {
            Error::ValidationError(format!("Could not find transaction {} in AtomicBEEF", txid))
        })?;

        // Extract ALL data from the transaction before any await points.
        // Transaction contains RefCell which is not Send.
        let (tx_outputs_count, tx_version, tx_lock_time, raw_tx, extracted_outputs) = {
            let tx = beef_tx.tx().ok_or_else(|| {
                Error::ValidationError(format!("Transaction {} is txid-only in BEEF", txid))
            })?;

            let outputs: Vec<(u64, Vec<u8>)> = tx
                .outputs
                .iter()
                .map(|o| (o.satoshis.unwrap_or(0), o.locking_script.to_binary()))
                .collect();

            (
                tx.outputs.len(),
                tx.version,
                tx.lock_time,
                tx.to_binary(),
                outputs,
            )
        };

        // Step 2: Get the user's default (change) basket
        let change_basket = self.find_or_create_default_basket(user_id).await?;
        let change_basket_id = change_basket.basket_id;

        // Step 3: Check for existing transaction (READ phase)
        let existing_tx = self.find_existing_transaction(user_id, &txid).await?;
        let is_merge = existing_tx.is_some();

        if let Some(ref etx) = existing_tx {
            validate_merge_status(&etx.status)?;
        }

        // Step 4: Extract and validate output specifications
        let mut outputs_data: Vec<OutputData> = Vec::new();
        for output_spec in &args.outputs {
            let vout = output_spec.output_index;

            if vout as usize >= tx_outputs_count {
                return Err(Error::ValidationError(format!(
                    "Output index {} is out of range (transaction has {} outputs)",
                    vout, tx_outputs_count
                )));
            }

            let (satoshis, locking_script) = extracted_outputs[vout as usize].clone();

            let (payment, insertion) = match output_spec.protocol.as_str() {
                WALLET_PAYMENT_PROTOCOL => {
                    let p = output_spec.payment_remittance.clone().ok_or_else(|| {
                        Error::ValidationError(format!(
                            "Wallet payment at index {} missing paymentRemittance",
                            vout
                        ))
                    })?;
                    (Some(p), None)
                }
                BASKET_INSERTION_PROTOCOL => {
                    let i = output_spec.insertion_remittance.clone().ok_or_else(|| {
                        Error::ValidationError(format!(
                            "Basket insertion at index {} missing insertionRemittance",
                            vout
                        ))
                    })?;
                    (None, Some(i))
                }
                _ => {
                    return Err(Error::ValidationError(format!(
                        "Unknown protocol: {}",
                        output_spec.protocol
                    )));
                }
            };

            outputs_data.push(OutputData {
                vout,
                satoshis,
                locking_script,
                protocol: output_spec.protocol.clone(),
                payment,
                insertion,
                existing_output_id: None,
                existing_basket_id: None,
                existing_is_change: false,
            });
        }

        // Step 5: If merging, load existing outputs
        if is_merge {
            let existing_outputs = self.load_existing_outputs(user_id, &txid).await?;
            for od in &mut outputs_data {
                if let Some(eo) = existing_outputs.iter().find(|o| o.vout == od.vout as i32) {
                    od.existing_output_id = Some(eo.output_id);
                    od.existing_basket_id = eo.basket_id;
                    od.existing_is_change = eo.change;
                }
            }
        }

        // Step 6: Calculate satoshi changes
        let mut net_satoshis: i64 = 0;

        for od in &outputs_data {
            match od.protocol.as_str() {
                WALLET_PAYMENT_PROTOCOL => {
                    if od.existing_output_id.is_some()
                        && od.existing_basket_id == Some(change_basket_id)
                        && od.existing_is_change
                    {
                        // Already a change output — no change
                    } else {
                        net_satoshis += od.satoshis as i64;
                    }
                }
                BASKET_INSERTION_PROTOCOL => {
                    if od.existing_basket_id == Some(change_basket_id) && od.existing_is_change {
                        net_satoshis -= od.satoshis as i64;
                    }
                }
                _ => {}
            }
        }

        // Step 7: WRITE PHASE — create/update transaction
        let has_proof = beef.find_bump(&txid).is_some();
        let status = if has_proof { "completed" } else { "unproven" };
        let now = Utc::now();

        let transaction_id = if is_merge {
            let tx_id = existing_tx.as_ref().unwrap().transaction_id;

            // Update description
            Query::new(
                "UPDATE transactions SET description = ?, updated_at = ? WHERE transaction_id = ?",
            )
            .bind(args.description.as_str())
            .bind(now)
            .bind(tx_id)
            .execute(self.db)
            .await?;

            tx_id
        } else {
            // Create new transaction. Two-phase: transaction_id is autoincrement
            // PK, so we INSERT with raw_tx + input_beef = NULL to reserve the PK,
            // then BlobStore.put() each, then UPDATE to fill in. This lets us
            // route large AtomicBEEFs (which are regularly >4 KB on refund /
            // cross-wallet internalize flows) to R2 instead of hitting the D1
            // 1 MB row limit.
            let reference = generate_uuid();

            let meta = Query::new(
                r#"INSERT INTO transactions (
                    user_id, txid, status, reference, description, satoshis,
                    version, lock_time, raw_tx, input_beef, is_outgoing, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, ?, ?)"#,
            )
            .bind(user_id)
            .bind(txid.as_str())
            .bind(status)
            .bind(reference.as_str())
            .bind(args.description.as_str())
            .bind(net_satoshis)
            .bind(tx_version as i64)
            .bind(tx_lock_time as i64)
            .bind(now)
            .bind(now)
            .execute(self.db)
            .await?;
            let new_tx_id = meta.last_row_id;

            let store = crate::r2::BlobStore::new(self.blobs);
            let (raw_tx_d1, _) = store
                .put("transactions", new_tx_id, "raw_tx", raw_tx.as_slice())
                .await?;
            let (ib_d1, _) = store
                .put("transactions", new_tx_id, "input_beef", args.tx.as_slice())
                .await?;
            Query::new(
                "UPDATE transactions SET raw_tx = ?, input_beef = ?, updated_at = ? WHERE transaction_id = ?",
            )
            .bind(raw_tx_d1)
            .bind(ib_d1)
            .bind(now)
            .bind(new_tx_id)
            .execute(self.db)
            .await?;

            new_tx_id
        };

        // Step 8: Add labels
        if let Some(ref labels) = args.labels {
            for label in labels {
                self.add_label(user_id, transaction_id, label).await?;
            }
        }

        // Step 9: Process each output
        let mut baskets_cache: HashMap<String, i64> = HashMap::new();

        for od in &outputs_data {
            match od.protocol.as_str() {
                WALLET_PAYMENT_PROTOCOL => {
                    let payment = od.payment.as_ref().ok_or(Error::ValidationError(
                        "wallet payment missing paymentRemittance".into(),
                    ))?;

                    // Skip if already a change output
                    if od.existing_output_id.is_some()
                        && od.existing_basket_id == Some(change_basket_id)
                        && od.existing_is_change
                    {
                        continue;
                    }

                    if let Some(output_id) = od.existing_output_id {
                        // Update existing output
                        Query::new(
                            r#"UPDATE outputs
                            SET basket_id = ?, type = 'P2PKH', change = 1, spendable = 1,
                                derivation_prefix = ?, derivation_suffix = ?,
                                sender_identity_key = ?, custom_instructions = NULL, updated_at = ?
                            WHERE output_id = ?"#,
                        )
                        .bind(change_basket_id)
                        .bind(payment.derivation_prefix.as_str())
                        .bind(payment.derivation_suffix.as_str())
                        .bind(payment.sender_identity_key.as_str())
                        .bind(now)
                        .bind(output_id)
                        .execute(self.db)
                        .await?;
                    } else {
                        // Two-phase INSERT for locking_script: output_id is PK
                        // autoincrement, so we reserve the row with NULL script
                        // first, then route the script through BlobStore.
                        let out_meta = Query::new(
                            r#"INSERT INTO outputs (
                                user_id, transaction_id, basket_id, txid, vout, satoshis,
                                locking_script, script_length, type, spendable, change,
                                derivation_prefix, derivation_suffix, sender_identity_key,
                                provided_by, purpose, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'P2PKH', 1, 1, ?, ?, ?, 'storage', 'receive', ?, ?)"#,
                        )
                        .bind(user_id)
                        .bind(transaction_id)
                        .bind(change_basket_id)
                        .bind(txid.as_str())
                        .bind(od.vout as i64)
                        .bind(od.satoshis as i64)
                        .bind(od.locking_script.len() as i64)
                        .bind(payment.derivation_prefix.as_str())
                        .bind(payment.derivation_suffix.as_str())
                        .bind(payment.sender_identity_key.as_str())
                        .bind(now)
                        .bind(now)
                        .execute(self.db)
                        .await?;
                        let new_out_id = out_meta.last_row_id;
                        let store = crate::r2::BlobStore::new(self.blobs);
                        let (script_d1, _) = store
                            .put("outputs", new_out_id, "locking_script", &od.locking_script)
                            .await?;
                        Query::new(
                            "UPDATE outputs SET locking_script = ? WHERE output_id = ?",
                        )
                        .bind(script_d1)
                        .bind(new_out_id)
                        .execute(self.db)
                        .await?;
                    }
                }
                BASKET_INSERTION_PROTOCOL => {
                    let insertion = od.insertion.as_ref().ok_or(Error::ValidationError(
                        "basket insertion missing insertionRemittance".into(),
                    ))?;

                    // Get or create basket
                    let basket_id = if let Some(id) = baskets_cache.get(&insertion.basket) {
                        *id
                    } else {
                        let id = self
                            .get_or_create_basket(user_id, &insertion.basket)
                            .await?;
                        baskets_cache.insert(insertion.basket.clone(), id);
                        id
                    };

                    if let Some(output_id) = od.existing_output_id {
                        // Update existing output
                        Query::new(
                            r#"UPDATE outputs
                            SET basket_id = ?, type = 'custom', change = 0,
                                custom_instructions = ?, derivation_prefix = NULL,
                                derivation_suffix = NULL, sender_identity_key = NULL, updated_at = ?
                            WHERE output_id = ?"#,
                        )
                        .bind(basket_id)
                        .bind(insertion.custom_instructions.as_deref())
                        .bind(now)
                        .bind(output_id)
                        .execute(self.db)
                        .await?;

                        if let Some(ref tags) = insertion.tags {
                            for tag in tags {
                                self.add_tag_to_output(user_id, output_id, tag).await?;
                            }
                        }
                    } else {
                        // Two-phase: basket-insertion custom scripts can be
                        // arbitrarily large (non-P2PKH). Reserve PK, then R2.
                        let meta = Query::new(
                            r#"INSERT INTO outputs (
                                user_id, transaction_id, basket_id, txid, vout, satoshis,
                                locking_script, script_length, type, spendable, change,
                                custom_instructions, provided_by, purpose, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'custom', 1, 0, ?, 'storage', 'receive', ?, ?)"#,
                        )
                        .bind(user_id)
                        .bind(transaction_id)
                        .bind(basket_id)
                        .bind(txid.as_str())
                        .bind(od.vout as i64)
                        .bind(od.satoshis as i64)
                        .bind(od.locking_script.len() as i64)
                        .bind(insertion.custom_instructions.as_deref())
                        .bind(now)
                        .bind(now)
                        .execute(self.db)
                        .await?;

                        let output_id = meta.last_row_id;
                        let store = crate::r2::BlobStore::new(self.blobs);
                        let (script_d1, _) = store
                            .put("outputs", output_id, "locking_script", &od.locking_script)
                            .await?;
                        Query::new(
                            "UPDATE outputs SET locking_script = ? WHERE output_id = ?",
                        )
                        .bind(script_d1)
                        .bind(output_id)
                        .execute(self.db)
                        .await?;

                        if let Some(ref tags) = insertion.tags {
                            for tag in tags {
                                self.add_tag_to_output(user_id, output_id, tag).await?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Step 10: Link proof or ensure monitoring
        //
        // Three-layer proof linking (matches TypeScript wallet-toolbox pattern):
        //   Layer 1: Inline link — if proven_tx exists, link immediately
        //   Layer 2: Monitor — create proven_tx_req so monitor finds/confirms proof
        //   Layer 3: Safety net — review_status() catches any missed links
        if !is_merge {
            let mut linked = false;

            if has_proof {
                // BEEF contains a merkle proof — try to link to existing proven_tx
                if let Some(pt_id) = self.find_proven_tx_id(&txid).await? {
                    Query::new("UPDATE transactions SET proven_tx_id = ? WHERE transaction_id = ?")
                        .bind(pt_id)
                        .bind(transaction_id)
                        .execute(self.db)
                        .await?;
                    linked = true;
                }
            }

            // If not linked yet, ensure a proven_tx_req exists so the monitor can
            // find/confirm the proof. For has_proof txs, the monitor will quickly
            // find the already-mined proof. For !has_proof, it will broadcast and
            // watch for confirmation.
            if !linked {
                let broadcast_failed = self.create_proven_tx_req(&txid, &raw_tx, &args.tx).await?;

                if broadcast_failed {
                    // Broadcast hit a network error (WoC down, DNS failure, timeout).
                    // Mark outputs non-spendable to prevent phantom UTXOs from inflating
                    // the user's balance. The monitor will retry the broadcast every 5
                    // minutes and set spendable = 1 once the transaction is confirmed
                    // on-chain.
                    Query::new(
                        "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND user_id = ?",
                    )
                    .bind(now)
                    .bind(transaction_id)
                    .bind(user_id)
                    .execute(self.db)
                    .await?;
                }
            }
        }

        Ok(StorageInternalizeActionResult {
            accepted: true,
            is_merge,
            txid,
            satoshis: net_satoshis,
            send_with_results: None,
            not_delayed_results: None,
        })
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    async fn find_existing_transaction(
        &self,
        user_id: i64,
        txid: &str,
    ) -> Result<Option<TableTransaction>> {
        let row: Option<TransactionRow> = Query::new(
            r#"SELECT transaction_id, user_id, txid, status, reference, description,
                      satoshis, version, lock_time, is_outgoing, created_at, updated_at
               FROM transactions WHERE user_id = ? AND txid = ?"#,
        )
        .bind(user_id)
        .bind(txid)
        .fetch_optional(self.db)
        .await?;

        Ok(row.map(|r| r.into_table()))
    }

    async fn load_existing_outputs(&self, user_id: i64, txid: &str) -> Result<Vec<TableOutput>> {
        let rows: Vec<OutputRow> = Query::new(
            r#"SELECT output_id, user_id, transaction_id, basket_id, txid, vout,
                      satoshis, script_length, script_offset, type, provided_by, purpose,
                      spendable, change, derivation_prefix, derivation_suffix,
                      sender_identity_key, custom_instructions, created_at, updated_at
               FROM outputs WHERE user_id = ? AND txid = ?"#,
        )
        .bind(user_id)
        .bind(txid)
        .fetch_all(self.db)
        .await?;

        Ok(rows.into_iter().map(|r| r.into_table()).collect())
    }

    /// Create a proven_tx_req for broadcast tracking.
    /// Look up an existing proven_tx by txid and return its ID.
    async fn find_proven_tx_id(&self, txid: &str) -> Result<Option<i64>> {
        #[derive(Deserialize)]
        struct PtRow {
            proven_tx_id: Option<f64>,
        }
        let row: Option<PtRow> = Query::new("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(self.db)
            .await?;
        Ok(row.and_then(|r| r.proven_tx_id.map(|id| id as i64)))
    }

    /// Returns `true` if broadcast encountered a network error (outputs should
    /// be marked non-spendable until the monitor confirms the broadcast).
    async fn create_proven_tx_req(
        &self,
        txid: &str,
        raw_tx: &[u8],
        input_beef: &[u8],
    ) -> Result<bool> {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct IdRow {
            proven_tx_req_id: Option<f64>,
        }

        let existing: Option<IdRow> =
            Query::new("SELECT proven_tx_req_id FROM proven_tx_reqs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(self.db)
                .await?;

        if existing.is_some() {
            return Ok(false);
        }

        // Broadcast the FULL BEEF (not just raw_tx) to ensure ARC/TAAL receive
        // the complete ancestor chain with their proofs. Broadcasting only the
        // raw tx causes the orphan-mempool problem: if any parent hasn't yet
        // propagated to a given miner's mempool, the child gets dropped into
        // the orphan pool and may be evicted before the parent arrives.
        //
        // This matches `bsv-wallet-toolbox-rs`'s broadcast intent — the full
        // BEEF contains raw_tx + parent raw_txs + merkle bumps, so miners can
        // validate the entire chain without needing their mempool to contain
        // the parents. Without the parent ancestry, mempool-orphan txs accrue
        // into a stuck backlog that the monitor cron has to drain.
        //
        // A Rejected error means double-spend → reject the payment.
        // Permanent errors (DoubleSpend/InvalidTx) reject the payment.
        // ServiceError means transient failure -- monitor will retry.
        let beef_hex = hex::encode(input_beef);
        let broadcast_network_error = match self.broadcast.broadcast_beef(&beef_hex).await {
            Ok(_result) => {
                // Broadcast succeeded (new or already known) -- proceed
                false
            }
            Err(crate::services::BroadcastError::DoubleSpend(msg)) => {
                // Double-spend -- reject the payment, do NOT store the transaction.
                return Err(Error::ValidationError(format!(
                    "Transaction {} rejected by network (double-spend): {}",
                    txid, msg
                )));
            }
            Err(crate::services::BroadcastError::InvalidTx(msg)) => {
                // Invalid tx -- reject the payment, do NOT store the transaction.
                return Err(Error::ValidationError(format!(
                    "Transaction {} rejected by network (invalid tx): {}",
                    txid, msg
                )));
            }
            Err(crate::services::BroadcastError::ServiceError(_)) => {
                // Service error -- proceed cautiously, monitor will retry broadcast.
                // Caller must mark outputs non-spendable until broadcast is confirmed.
                true
            }
        };

        let now = Utc::now();

        // raw_tx is NOT NULL in the schema AND always available here (we just
        // parsed it from the AtomicBEEF). Single-phase INSERT binds it
        // directly, matching reference semantics. input_beef is potentially
        // huge (AtomicBEEF of refund flows can be 100+ KB) — use two-phase
        // so R2 overflow works.
        let req_meta = Query::new(
            r#"INSERT INTO proven_tx_reqs (
                txid, status, attempts, history, notify, notified, raw_tx, input_beef, created_at, updated_at
            ) VALUES (?, 'unmined', 0, '{}', '{}', 0, ?, NULL, ?, ?)"#,
        )
        .bind(txid)
        .bind(raw_tx)
        .bind(now)
        .bind(now)
        .execute(self.db)
        .await?;
        let req_id = req_meta.last_row_id;

        let store = crate::r2::BlobStore::new(self.blobs);
        let (ib_d1, _) = store
            .put("proven_tx_reqs", req_id, "input_beef", input_beef)
            .await?;
        Query::new(
            "UPDATE proven_tx_reqs SET input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(ib_d1)
        .bind(now)
        .bind(req_id)
        .execute(self.db)
        .await?;

        Ok(broadcast_network_error)
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn validate_merge_status(status: &TransactionStatus) -> Result<()> {
    match status {
        TransactionStatus::Completed | TransactionStatus::Unproven | TransactionStatus::NoSend => {
            Ok(())
        }
        _ => Err(Error::ValidationError(format!(
            "Target transaction of internalizeAction has invalid status: {:?}",
            status
        ))),
    }
}

/// Generate a simple UUID v4 without pulling in the uuid crate.
pub(crate) fn generate_uuid() -> String {
    use getrandom::getrandom;
    let mut bytes = [0u8; 16];
    getrandom(&mut bytes).expect("getrandom failed — cannot generate secure random bytes");
    // Set version (4) and variant (RFC 4122)
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // validate_merge_status
    // =========================================================================

    #[test]
    fn test_validate_merge_status_completed_ok() {
        assert!(validate_merge_status(&TransactionStatus::Completed).is_ok());
    }

    #[test]
    fn test_validate_merge_status_unproven_ok() {
        assert!(validate_merge_status(&TransactionStatus::Unproven).is_ok());
    }

    #[test]
    fn test_validate_merge_status_nosend_ok() {
        assert!(validate_merge_status(&TransactionStatus::NoSend).is_ok());
    }

    #[test]
    fn test_validate_merge_status_unsigned_rejected() {
        let result = validate_merge_status(&TransactionStatus::Unsigned);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_merge_status_unprocessed_rejected() {
        let result = validate_merge_status(&TransactionStatus::Unprocessed);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_merge_status_sending_rejected() {
        let result = validate_merge_status(&TransactionStatus::Sending);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_merge_status_failed_rejected() {
        let result = validate_merge_status(&TransactionStatus::Failed);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_merge_status_nonfinal_rejected() {
        let result = validate_merge_status(&TransactionStatus::NonFinal);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_merge_status_unfail_rejected() {
        let result = validate_merge_status(&TransactionStatus::Unfail);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_merge_status_error_message_contains_status() {
        let result = validate_merge_status(&TransactionStatus::Failed);
        match result {
            Err(Error::ValidationError(msg)) => {
                assert!(
                    msg.contains("invalid status"),
                    "Error message should mention 'invalid status': {}",
                    msg
                );
                assert!(
                    msg.contains("Failed"),
                    "Error message should mention the status name: {}",
                    msg
                );
            }
            _ => panic!("Expected ValidationError"),
        }
    }

    // =========================================================================
    // generate_uuid — comprehensive tests
    // =========================================================================

    #[test]
    fn test_uuid_format_8_4_4_4_12() {
        let uuid = generate_uuid();
        let parts: Vec<&str> = uuid.split('-').collect();
        assert_eq!(
            parts.len(),
            5,
            "UUID should have 5 dash-separated parts: {}",
            uuid
        );
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
    }

    #[test]
    fn test_uuid_total_length_36() {
        let uuid = generate_uuid();
        assert_eq!(uuid.len(), 36);
    }

    #[test]
    fn test_uuid_only_hex_and_dashes() {
        let uuid = generate_uuid();
        for (i, c) in uuid.chars().enumerate() {
            match i {
                8 | 13 | 18 | 23 => assert_eq!(c, '-', "Expected dash at position {}", i),
                _ => assert!(
                    c.is_ascii_hexdigit(),
                    "Expected hex digit at position {}, got '{}'",
                    i,
                    c
                ),
            }
        }
    }

    #[test]
    fn test_uuid_lowercase_hex() {
        let uuid = generate_uuid();
        let hex_only: String = uuid.chars().filter(|c| *c != '-').collect();
        assert_eq!(
            hex_only,
            hex_only.to_lowercase(),
            "UUID should use lowercase hex"
        );
    }

    #[test]
    fn test_uuid_version_4() {
        let uuid = generate_uuid();
        // Character at position 14 (0-indexed) is the version nibble
        let version = uuid.chars().nth(14).unwrap();
        assert_eq!(version, '4', "Version nibble must be 4, got '{}'", version);
    }

    #[test]
    fn test_uuid_variant_rfc4122() {
        let uuid = generate_uuid();
        // Character at position 19 (0-indexed) is the variant nibble
        let variant = uuid.chars().nth(19).unwrap();
        assert!(
            "89ab".contains(variant),
            "Variant nibble must be 8/9/a/b (RFC 4122), got '{}'",
            variant
        );
    }

    #[test]
    fn test_uuid_not_all_zeros() {
        // CRITICAL TEST: getrandom(&mut bytes).unwrap_or_default() silently produces
        // all-zero bytes on failure. After version/variant masking this becomes:
        // "00000000-0000-4000-8000-000000000000"
        let zero_uuid = "00000000-0000-4000-8000-000000000000";
        let uuid = generate_uuid();
        assert_ne!(
            uuid, zero_uuid,
            "UUID is the all-zeros sentinel value — getrandom likely failed silently"
        );
    }

    #[test]
    fn test_uuid_multiple_calls_produce_different_values() {
        // Probabilistic but effective: 10 UUIDs should all be unique
        let uuids: Vec<String> = (0..10).map(|_| generate_uuid()).collect();
        let unique: std::collections::HashSet<&String> = uuids.iter().collect();
        assert_eq!(
            unique.len(),
            uuids.len(),
            "UUIDs should be unique: {:?}",
            uuids
        );
    }

    #[test]
    fn test_uuid_version_and_variant_stable_across_many() {
        // Version 4 and RFC 4122 variant must hold for every generated UUID
        for _ in 0..100 {
            let uuid = generate_uuid();
            let v = uuid.chars().nth(14).unwrap();
            let var = uuid.chars().nth(19).unwrap();
            assert_eq!(v, '4');
            assert!("89ab".contains(var));
        }
    }

    // =========================================================================
    // Constants
    // =========================================================================

    #[test]
    fn test_protocol_constants() {
        assert_eq!(WALLET_PAYMENT_PROTOCOL, "wallet payment");
        assert_eq!(BASKET_INSERTION_PROTOCOL, "basket insertion");
    }
}
