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

use crate::beef_at_rest::{AtRestBucket, BeefAtRest};
use crate::d1::Query;
use crate::entities::{TableOutput, TableTransaction, TransactionStatus};
use crate::error::{Error, Result};
use crate::types::{
    merge_noop, MergeResult, ReviewActionResult, ReviewActionResultStatus, SendWithResult,
    StorageInternalizeActionResult,
};
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
// Spent-input marking SQL (rust-wallet-infra #22 / btc-relay #16)
// =============================================================================
//
// internalizeAction must mark the internalized tx's consumed INPUTS spent —
// otherwise the consumed coins stay spendable=1 and the next listOutputs
// re-serves an already-spent coin to an externally-built tx (the 2026-07-22
// btc-beacon double-spend outage). Reference semantic: ts-stack
// wallet-toolbox `storage/methods/internalizeAction.ts` markUserInputsSpent /
// restoreInputsToSpendable. The consts live at module scope so the rusqlite
// harness below runs the REAL SQL against the REAL migrations (g5 pattern).

/// The internalizing user's own rows at one consumed-input outpoint. A call
/// writes no row of another user: another user's coin at that outpoint is
/// marked where the subject's checked proof is stored, from the proven bytes
/// (`crate::monitor::PROOF_MARK_SPENT_INPUT_SQL`). `user_tx_id` is the
/// subject tx's transaction_id in the user's scope (review H3:
/// transactions.txid is not unique across users).
/// Binds: subject_txid, prev_txid, prev_vout, user_id.
const INTERNALIZE_INPUT_ROWS_SQL: &str = "SELECT o.output_id, o.spendable, o.spent_by, \
        (SELECT t.transaction_id FROM transactions t \
         WHERE t.txid = ? AND t.user_id = o.user_id) AS user_tx_id \
     FROM outputs o WHERE o.txid = ? AND o.vout = ? AND o.user_id = ?";

/// Flip one eligible row of the internalizing user to consumed
/// (internalizeAction.ts:89-93). spent_by uses the
/// COALESCE-with-per-user-scalar-subquery idiom
/// (monitor.rs::remark_inputs_spent): never clobbers an existing link. The
/// row must be the caller's own (`user_id = ?`). NEVER writes
/// spending_description (contrast createAction). The WHERE guard re-checks
/// eligibility write-time: only a live spendable row whose spent_by is NULL
/// or already the subject tx (idempotence; a competing spend is never
/// overwritten, internalizeAction.ts:86-87). Guarded no-ops keep audit
/// Case A (spendable=1 AND spent_by set) unreachable from this path.
/// Binds: subject_txid, updated_at, output_id, user_id, subject_txid.
const INTERNALIZE_MARK_INPUT_SQL: &str = "UPDATE outputs SET spendable = 0, \
        spent_by = COALESCE(spent_by, \
            (SELECT t.transaction_id FROM transactions t \
             WHERE t.txid = ? AND t.user_id = outputs.user_id)), \
        updated_at = ? \
     WHERE output_id = ? AND user_id = ? AND spendable = 1 \
       AND (spent_by IS NULL OR spent_by = \
            (SELECT t.transaction_id FROM transactions t \
             WHERE t.txid = ? AND t.user_id = outputs.user_id))";

/// Restore (broadcast non-success only, internalizeAction.ts:120-128,
/// :626-636) for a transition that SET spent_by: undo exactly what this call
/// wrote. `spent_by = ?` (the value we stamped) means a competing spend
/// recorded since is never clobbered, and a double-restore is a no-op.
/// `basket_id IS NOT NULL` is the repo's standing anti-resurrection guard
/// (abort_action.rs G5): a row relinquished since must stay GONE.
/// Binds: updated_at, output_id, stamped_spent_by.
const INTERNALIZE_RESTORE_SPENT_BY_SQL: &str = "UPDATE outputs \
     SET spendable = 1, spent_by = NULL, updated_at = ? \
     WHERE output_id = ? AND spent_by = ? AND basket_id IS NOT NULL";

/// Restore for a transition that flipped ONLY spendable (foreign rows / rows
/// whose spent_by predated this call). `spent_by IS NULL` refuses to
/// resurrect a row some other actor has since linked to a spend (re-enabling
/// it would mint audit Case A); same relinquish guard as above.
/// Binds: updated_at, output_id.
const INTERNALIZE_RESTORE_SPENDABLE_SQL: &str = "UPDATE outputs \
     SET spendable = 1, updated_at = ? \
     WHERE output_id = ? AND spent_by IS NULL AND basket_id IS NOT NULL";

// =============================================================================
// A refused internalize: the caller's own row, and nothing proven
// =============================================================================

/// The reference's message for an AtomicBEEF that is not valid
/// (`WERR_INVALID_PARAMETER` of `tx`, "valid AtomicBEEF",
/// `internalizeActionCore.ts` validateAtomicBeef, the text of
/// `WERR_errors.ts:105`). Answered here for a subject that carries no proof
/// and names an input whose parent the BEEF does not hold: a
/// `ValidationError`, `-32602` on the wire.
const VALID_ATOMIC_BEEF_TEXT: &str = "The tx parameter must be valid AtomicBEEF";

/// The internalizing call's own transactions row, failed on the network's
/// refusal: never a row that is `completed` or carries a proof.
/// Binds: updated_at, transaction_id, user_id.
const COMPENSATE_FAIL_TX_SQL: &str = "UPDATE transactions SET status = 'failed', updated_at = ? \
     WHERE transaction_id = ? AND user_id = ? \
       AND status != 'completed' AND proven_tx_id IS NULL";

/// The outputs of that row, and only while the row reads `failed`.
/// Binds: updated_at, transaction_id, user_id.
const COMPENSATE_ZERO_OUTPUTS_SQL: &str = "UPDATE outputs SET spendable = 0, updated_at = ? \
     WHERE transaction_id = ? AND user_id = ? \
       AND EXISTS (SELECT 1 FROM transactions t \
             WHERE t.transaction_id = outputs.transaction_id AND t.status = 'failed')";

/// The canary's request for a refused txid, when none stands (review H2c).
/// Binds: txid, raw_tx, created_at, updated_at, txid.
const COMPENSATE_REQ_SQL: &str = "INSERT INTO proven_tx_reqs ( \
        txid, status, attempts, history, notify, notified, raw_tx, input_beef, created_at, updated_at \
     ) SELECT ?, 'invalid', 0, '{}', '{}', 0, ?, NULL, ?, ? \
       WHERE NOT EXISTS (SELECT 1 FROM proven_tx_reqs WHERE txid = ?)";

/// A refusal held on the request for its txid: the answer the call gave,
/// kept under the history's own `refusalHeld` key (the notes, which keep the
/// newest 32, are not where it lives). Never on a `completed` request.
/// Binds: when, answer, txid.
const RECORD_REFUSAL_HELD_SQL: &str = "UPDATE proven_tx_reqs SET history = json_set(\
    CASE WHEN json_valid(history) AND json_type(history) = 'object' THEN history \
         ELSE json_object('previous', history) END, \
    '$.refusalHeld', json_object('when', ?, 'answer', ?)) \
     WHERE txid = ? AND status != 'completed'";

/// The answer a held refusal gave, while the request for the txid is not
/// `completed` and the caller's row (bound second) is neither `completed`
/// nor proven. `REFUSAL_HELD_ON_MERGE_SQL` holds while none of that row's
/// outputs is credited; `REFUSAL_HELD_ON_NEW_ROW_SQL`, for a row just
/// written, while no other row's output of the txid is.
/// Binds: txid, transaction_id, transaction_id.
macro_rules! refusal_held_answer_sql {
    ($outputs:literal) => {
        concat!(
            "SELECT json_extract(r.history, '$.refusalHeld.answer') AS answer \
             FROM proven_tx_reqs r \
             WHERE r.txid = ? AND r.status != 'completed' \
               AND json_valid(r.history) AND json_type(r.history, '$.refusalHeld.answer') = 'text' \
               AND NOT EXISTS (SELECT 1 FROM transactions t \
                     WHERE t.transaction_id = ? AND (t.status = 'completed' OR t.proven_tx_id IS NOT NULL)) \
               AND NOT EXISTS (SELECT 1 FROM outputs o \
                     WHERE o.txid = r.txid AND o.spendable = 1 AND o.transaction_id ",
            $outputs,
            " ?) LIMIT 1"
        )
    };
}
const REFUSAL_HELD_ON_MERGE_SQL: &str = refusal_held_answer_sql!("=");
const REFUSAL_HELD_ON_NEW_ROW_SQL: &str = refusal_held_answer_sql!("!=");

/// What the second source says of a txid the broadcaster refused.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum NetworkWord {
    /// Known or mined.
    Knows,
    /// Answered, and not known.
    DoesNotKnow,
    /// No answer (an error): a hold, never "not known".
    Unavailable,
}

fn network_word(
    answer: std::result::Result<Vec<crate::services::TxStatusDetail>, String>,
) -> NetworkWord {
    match answer.ok().and_then(|v| v.into_iter().next()) {
        Some(s) if s.status == "known" || s.status == "mined" => NetworkWord::Knows,
        Some(s) if s.status == "unknown" => NetworkWord::DoesNotKnow,
        _ => NetworkWord::Unavailable,
    }
}

/// A subject with no proof of its own whose input names a parent the BEEF
/// does not hold (no entry, or a txid alone with no BUMP): the reference's
/// BEEF validity refuses it (`Beef.verifyValid`, missing inputs).
fn unproven_subject_lacks_a_parent(beef: &Beef, txid: &str, inputs: &[String]) -> bool {
    if beef.find_bump(txid).is_some() {
        return false;
    }
    inputs.iter().any(|parent| {
        if parent.chars().all(|c| c == '0') {
            return false; // a coinbase input names no parent
        }
        match beef.find_txid(parent) {
            None => true,
            Some(entry) => entry.tx().is_none() && beef.find_bump(parent).is_none(),
        }
    })
}

// =============================================================================
// One row per payment: the subject's rows are written by guarded inserts
// =============================================================================
//
// D1 runs a statement whole, and a call's statements are separate, so calls
// for one transaction meet at the statement. Each insert of the subject's
// rows therefore carries its own condition: the transactions row is inserted
// only when the read that decides "new" or "merge" would still find none
// (the same predicate, inside the statement), and an outputs row or a proof
// request only when the table's own unique key is free. An insert that
// writes nothing (`changes == 0`) says a row stands, and the call goes on as
// the merge it is. So a user holds ONE transactions row for an internalized
// txid and ONE outputs row for an outpoint, in whatever order the statements
// of several calls reach the database, and each of those calls is answered
// as accepted. A call names each output index once (step 4), so an outputs
// row it finds standing is never one it wrote itself. The consts live at
// module scope so the rusqlite harness below runs the REAL SQL against the
// REAL migrations.

/// The reference's message for a call whose `outputs` name one output index
/// more than once: `WERR_INVALID_PARAMETER` of `outputs`, "unique outputIndex
/// values" (`internalizeActionCore.ts:445`, the text of `WERR_errors.ts:105`).
/// Answered as every refusal of that class is here: a `ValidationError`,
/// `-32602` on the wire.
const UNIQUE_OUTPUT_INDEX_TEXT: &str = "The outputs parameter must be unique outputIndex values";

/// A call names each output index once. One named more than once, under
/// whichever protocols and whatever the storage holds, is refused where the
/// specifications are read (step 4): before a row of the transaction, of an
/// output or of a proof request is written, and before anything is announced.
fn require_unique_output_indexes(indexes: impl IntoIterator<Item = u32>) -> Result<()> {
    let mut named = std::collections::HashSet::new();
    for index in indexes {
        if !named.insert(index) {
            return Err(Error::ValidationError(UNIQUE_OUTPUT_INDEX_TEXT.to_string()));
        }
    }
    Ok(())
}

/// The user's row for the subject txid, if one stands: the read that decides
/// "new" or "merge".
/// Binds: user_id, txid.
const INTERNALIZE_FIND_TX_SQL: &str = "SELECT transaction_id, user_id, txid, status, reference, \
        description, satoshis, version, lock_time, is_outgoing, created_at, updated_at \
     FROM transactions WHERE user_id = ? AND txid = ?";

/// The subject's transactions row, inserted only when the read above would
/// still find none: its predicate, word for word, inside the statement.
/// `changes == 0` means a row stands and the call is a merge onto it.
/// raw_tx and input_beef follow by a second statement (the blob store needs
/// the row's id).
/// Binds: user_id, txid, status, reference, description, satoshis, version,
/// lock_time, created_at, updated_at, then the predicate's user_id, txid.
const INTERNALIZE_INSERT_TX_SQL: &str = "INSERT INTO transactions ( \
        user_id, txid, status, reference, description, satoshis, \
        version, lock_time, raw_tx, input_beef, is_outgoing, created_at, updated_at \
     ) SELECT ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, ?, ? \
     WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE user_id = ? AND txid = ?)";

/// The user's rows for the subject's outputs: what a merge reads to find the
/// row that stands for an output index.
/// Binds: user_id, txid.
const INTERNALIZE_EXISTING_OUTPUTS_SQL: &str = "SELECT output_id, user_id, transaction_id, \
        basket_id, txid, vout, satoshis, script_length, script_offset, type, provided_by, \
        purpose, spendable, change, derivation_prefix, derivation_suffix, \
        sender_identity_key, custom_instructions, created_at, updated_at \
     FROM outputs WHERE user_id = ? AND txid = ?";

/// A wallet payment's outputs row. The schema holds one row to
/// (transaction_id, vout, user_id); when one stands the statement writes
/// nothing (`changes == 0`) and the output is merged onto that row. The
/// script follows by a second statement (the blob store needs the row's id).
/// Binds: user_id, transaction_id, basket_id, txid, vout, satoshis,
/// script_length, derivation_prefix, derivation_suffix, sender_identity_key,
/// created_at, updated_at.
const INTERNALIZE_INSERT_PAYMENT_OUTPUT_SQL: &str = "INSERT INTO outputs ( \
        user_id, transaction_id, basket_id, txid, vout, satoshis, \
        locking_script, script_length, type, spendable, change, \
        derivation_prefix, derivation_suffix, sender_identity_key, \
        provided_by, purpose, created_at, updated_at \
     ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'P2PKH', 1, 1, ?, ?, ?, 'storage', 'receive', ?, ?) \
     ON CONFLICT(transaction_id, vout, user_id) DO NOTHING";

/// A basket insertion's outputs row, under the same key and the same rule.
/// Binds: user_id, transaction_id, basket_id, txid, vout, satoshis,
/// script_length, custom_instructions, created_at, updated_at.
const INTERNALIZE_INSERT_BASKET_OUTPUT_SQL: &str = "INSERT INTO outputs ( \
        user_id, transaction_id, basket_id, txid, vout, satoshis, \
        locking_script, script_length, type, spendable, change, \
        custom_instructions, provided_by, purpose, created_at, updated_at \
     ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'custom', 1, 0, ?, 'storage', 'receive', ?, ?) \
     ON CONFLICT(transaction_id, vout, user_id) DO NOTHING";

/// The subject's proof request. `proven_tx_reqs.txid` is unique: when a
/// request stands (another writer's) the statement writes nothing
/// (`changes == 0`) and that request serves this call too. input_beef
/// follows by a second statement (the blob store needs the row's id).
/// Binds: txid, status, raw_tx, created_at, updated_at.
const INTERNALIZE_INSERT_REQ_SQL: &str = "INSERT INTO proven_tx_reqs ( \
        txid, status, attempts, history, notify, notified, raw_tx, input_beef, \
        created_at, updated_at \
     ) VALUES (?, ?, 0, '{}', '{}', 0, ?, NULL, ?, ?) \
     ON CONFLICT(txid) DO NOTHING";

/// What an internalize broadcast left for the caller.
#[derive(Clone, PartialEq, Eq)]
enum PostOutcome {
    /// Accepted, already known, or proven: nothing to hold.
    Sent,
    /// A transient fault: outputs demoted, the payer's inputs restored.
    Transient,
    /// A policy refusal (B5, #50): outputs demoted, inputs held, the req
    /// queued `unsent` for the monitor's re-announce.
    PolicyHeld,
    /// The broadcaster refused it (double-spend or invalid) and the second
    /// source could not be asked (an error): the rows are held, as a
    /// transient fault's are, but never credited, whatever
    /// INTERNALIZE_ZERO_CONF says, and the call is answered with this
    /// message, the refusal's own.
    RefusalHeld(String),
    /// The broadcaster refused it (double-spend or invalid) and the second
    /// source answered that it does not know it: the call is refused with
    /// this message.
    Rejected(String),
}

/// The subject of an AtomicBEEF, as the storage needs it.
struct Subject {
    txid: String,
    raw_tx: Vec<u8>,
    version: u32,
    lock_time: u32,
    /// Each output's satoshis and locking script.
    outputs: Vec<(u64, Vec<u8>)>,
    /// The BEEF carries a BUMP for the subject itself.
    has_proof: bool,
}

/// Where the whole BEEF is: in the argument, or at rest in R2 (NL-7).
enum BeefBlob<'b> {
    Inline(&'b [u8]),
    AtRest {
        store: &'b AtRestBucket<'b>,
        reference: &'b BeefAtRest,
        /// The signed-in caller, the one the object must name (NL-7c).
        caller: &'b str,
    },
}

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

/// One row at a consumed-input outpoint (see INTERNALIZE_INPUT_ROWS_SQL).
#[derive(Debug, Deserialize)]
struct InputOutpointRow {
    output_id: Option<f64>,
    spendable: Option<f64>,
    spent_by: Option<f64>,
    user_tx_id: Option<f64>,
}

// =============================================================================
// Internal Types
// =============================================================================

/// Record of one spent-input flip this internalize call performed — enough
/// state to roll back the EXACT change on broadcast failure
/// (internalizeAction.ts:33-37 SpentInputTransition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpentInputTransition {
    output_id: i64,
    /// `Some(tx_id)` when THIS call stamped `spent_by = tx_id` (the subject
    /// tx's transaction_id in the row-owner's scope); `None` when only
    /// `spendable` was flipped. Restore clears spent_by only for `Some`.
    set_spent_by: Option<i64>,
}

/// Per-row eligibility verdict for marking a consumed input spent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMarkDecision {
    /// Row is not eligible — already unspendable (idempotence) or spent by a
    /// DIFFERENT tx (competing spend; never overwritten).
    Skip,
    /// Flip the row; `set_spent_by` is what this call will stamp into
    /// spent_by (None = leave it as-is: either already stamped, or the
    /// row's user has no transactions row for the subject txid).
    Mark { set_spent_by: Option<i64> },
}

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

impl OutputData {
    /// The row that stands for this output is change in `basket_id`.
    fn is_change_in(&self, basket_id: i64) -> bool {
        self.existing_basket_id == Some(basket_id) && self.existing_is_change
    }
}

/// The inline door's parse of a caller's AtomicBEEF: measured before it is
/// parsed (`crate::door::beef_measure`), so a BEEF whose parse would pass the
/// budget is refused as too large; one the measure cannot read meets the
/// parser's own refusal.
pub fn parse_inline_beef(tx: &[u8]) -> Result<Beef> {
    crate::door::beef_measure::admit_beef(tx)?;
    Beef::from_binary(tx)
        .map_err(|e| Error::ValidationError(format!("Failed to parse AtomicBEEF: {}", e)))
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
        // Step 1: Parse and validate the AtomicBEEF: at most the payment
        // figure of it (`crate::door::payment`), then measured, then parsed.
        crate::door::payment::admit(args.tx.len() as u64, self.payment_budget())?;
        let beef = parse_inline_beef(&args.tx)?;

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
        let subject = {
            let tx = beef_tx.tx().ok_or_else(|| {
                Error::ValidationError(format!("Transaction {} is txid-only in BEEF", txid))
            })?;

            // A subject that carries no proof of its own is internalized only
            // with the parent of each of its inputs: the reference's BEEF
            // validity (`validateAtomicBeef`) refuses one that lacks a parent,
            // before any write and any broadcast.
            let parents: Vec<String> = tx
                .inputs
                .iter()
                .filter_map(|i| i.get_source_txid().ok())
                .collect();
            if unproven_subject_lacks_a_parent(&beef, &txid, &parents) {
                return Err(Error::ValidationError(VALID_ATOMIC_BEEF_TEXT.to_string()));
            }

            // The local script check (#72, #50): each input against its parent
            // in this BEEF. A script that fails under every era is refused here,
            // before any write and any broadcast; a missing parent is no verdict.
            // The at-rest form checks the scripts as it streams.
            let sources: Vec<_> = tx
                .inputs
                .iter()
                .map_while(|i| {
                    let parent = beef.find_txid(&i.get_source_txid().ok()?)?.tx()?;
                    parent.outputs.get(i.source_output_index as usize).cloned()
                })
                .collect();
            if let Some(reason) = super::tracker_rule::script_refusal(tx, &sources) {
                return Err(Error::ValidationError(format!(
                    "internalize: the local script check refuses {txid}: {reason}"
                )));
            }

            Subject {
                outputs: tx
                    .outputs
                    .iter()
                    .map(|o| (o.satoshis.unwrap_or(0), o.locking_script.to_binary()))
                    .collect(),
                version: tx.version,
                lock_time: tx.lock_time,
                raw_tx: tx.to_binary(),
                has_proof: beef.find_bump(&txid).is_some(),
                txid,
            }
        };
        drop(beef);

        self.internalize_subject(user_id, &args, subject, BeefBlob::Inline(&args.tx), false)
            .await
    }

    /// Internalize an AtomicBEEF at rest in R2 (NL-7): the object the
    /// reference names, held to its bucket, etag and size, is read as a
    /// stream and verified one element in hand (`crate::beef_at_rest`); the
    /// storage is handed the subject alone, and the BEEF is stored by a
    /// streamed copy. The reference form is verified in full whatever
    /// `BEEF_VERIFICATION` says: the structure, the scripts and every root.
    /// The object is read only for the caller it names (`caller`, the
    /// BRC-31 identity key; NL-7c): another's is refused before its body is
    /// asked for.
    pub async fn internalize_action_at_rest(
        &self,
        user_id: i64,
        caller: &str,
        args: InternalizeActionArgs,
        reference: BeefAtRest,
    ) -> Result<StorageInternalizeActionResult> {
        // At most the payment figure of a payment is read
        // (`crate::door::payment`): the length the reference names, which the
        // object's head must match, is held to it before the object is opened.
        crate::door::payment::admit(reference.size, self.payment_budget())?;
        let store = self.beef_at_rest.ok_or_else(|| {
            Error::ValidationError(
                "beefAtRest: no bucket is bound here for bytes at rest (BEEF_AT_REST)".to_string(),
            )
        })?;
        let headers = self.header_provider.ok_or_else(|| {
            Error::InternalError(
                "beefAtRest: no header service is configured to verify the roots".to_string(),
            )
        })?;
        let object = crate::beef_at_rest::open(store, &reference, caller).await?;
        let mut source = crate::beef_at_rest::body_of(&object)?;
        let reading =
            crate::beef_at_rest::read_at_rest(&mut source, headers, reference.size).await?;
        drop(source);
        drop(object);
        let subject = Subject {
            txid: reading.txid,
            raw_tx: reading.raw_tx,
            version: reading.version,
            lock_time: reading.lock_time,
            outputs: reading.outputs,
            has_proof: reading.has_proof,
        };
        if reference.size <= crate::r2::THRESHOLD as u64 {
            // Small enough for D1: verified above, stored and broadcast the
            // inline way.
            let bytes = crate::beef_at_rest::read_small(store, &reference, caller).await?;
            return self
                .internalize_subject(user_id, &args, subject, BeefBlob::Inline(&bytes), false)
                .await;
        }
        self.internalize_subject(
            user_id,
            &args,
            subject,
            BeefBlob::AtRest {
                store,
                reference: &reference,
                caller,
            },
            false,
        )
        .await
    }

    /// Steps 2 to 10 of internalizeAction: the subject, verified, into the
    /// user's storage; `beef` is where the whole BEEF is (inline or at rest).
    /// `again` is set on the one pass a call makes after its insert of the
    /// transactions row wrote nothing (step 7): that pass reads the row that
    /// stands and is the merge.
    async fn internalize_subject(
        &self,
        user_id: i64,
        args: &InternalizeActionArgs,
        subject: Subject,
        beef: BeefBlob<'_>,
        again: bool,
    ) -> Result<StorageInternalizeActionResult> {
        let Subject {
            txid,
            raw_tx,
            version: tx_version,
            lock_time: tx_lock_time,
            outputs: extracted_outputs,
            has_proof,
        } = subject;
        let tx_outputs_count = extracted_outputs.len();

        // Step 2: Get the user's default (change) basket
        let change_basket = self.find_or_create_default_basket(user_id).await?;
        let change_basket_id = change_basket.basket_id;

        // Step 3: Check for existing transaction (READ phase)
        let existing_tx = self.find_existing_transaction(user_id, &txid).await?;
        let is_merge = existing_tx.is_some();

        if let Some(ref etx) = existing_tx {
            validate_merge_status(&etx.status)?;
            // A row held on a refusal, with nothing credited: the call is
            // answered as the presentation that wrote it was, before any
            // write.
            if let Some(answer) = self
                .refusal_held_answer(REFUSAL_HELD_ON_MERGE_SQL, &txid, etx.transaction_id)
                .await?
            {
                return Err(Error::ValidationError(answer));
            }
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

        // Each output index is named once. Asked when every specification
        // has passed its own refusals above (an index out of range, a
        // missing remittance, an unknown protocol), which are answered first.
        require_unique_output_indexes(outputs_data.iter().map(|od| od.vout))?;

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

        // Step 6: Calculate satoshi changes (from what step 5 read; step 9
        // corrects the sum for an output whose row another call wrote since)
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
                BASKET_INSERTION_PROTOCOL
                    if od.existing_basket_id == Some(change_basket_id) && od.existing_is_change =>
                {
                    net_satoshis -= od.satoshis as i64;
                }
                _ => {}
            }
        }

        // Step 7: WRITE PHASE — create/update transaction
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

            // nosend lifecycle advance (ts-stack 2.4.0
            // internalizeAction.ts:526-553): someone internalizing a payment
            // from our own nosend tx proves it was broadcast EXTERNALLY —
            // leaving it 'nosend' kept it out of the balance and out of the
            // proof pipeline forever (funds invisible). BUMP present ⇒
            // 'completed'; else 'unproven' and the req flips to 'unmined' so
            // the monitor proves it. Chain-factual: the counterparty holds
            // the tx (and possibly its proof) in hand.
            if matches!(
                existing_tx.as_ref().unwrap().status,
                TransactionStatus::NoSend
            ) {
                let promoted = if has_proof { "completed" } else { "unproven" };
                Query::new(
                    "UPDATE transactions SET status = ?, updated_at = ? WHERE transaction_id = ? AND status = 'nosend'",
                )
                .bind(promoted)
                .bind(now)
                .bind(tx_id)
                .execute(self.db)
                .await?;
                Query::new(
                    "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE txid = ? AND status = 'nosend'",
                )
                .bind(now)
                .bind(txid.as_str())
                .execute(self.db)
                .await?;
                worker::console_log!(
                    "internalize: nosend tx {} externally broadcast — promoted to {}",
                    txid,
                    promoted
                );
            }

            tx_id
        } else {
            // Create new transaction. Two-phase: transaction_id is autoincrement
            // PK, so we INSERT with raw_tx + input_beef = NULL to reserve the PK,
            // then BlobStore.put() each, then UPDATE to fill in. This lets us
            // route large AtomicBEEFs (which are regularly >4 KB on refund /
            // cross-wallet internalize flows) to R2 instead of hitting the D1
            // 1 MB row limit.
            let reference = generate_uuid();

            // The insert carries the step-3 read's predicate: it writes the
            // row only when that read would still find none.
            let meta = Query::new(INTERNALIZE_INSERT_TX_SQL)
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
                .bind(user_id)
                .bind(txid.as_str())
                .execute(self.db)
                .await?;
            if meta.changes == 0 {
                // A row for this user and txid stands, so this call is a
                // merge onto it. Nothing of the subject has been written by
                // this call yet: it starts again at the read, once, and
                // takes the merge path a call arriving later takes.
                if again {
                    return Err(Error::InternalError(format!(
                        "internalize: no transactions row could be read or inserted for {txid}"
                    )));
                }
                worker::console_log!(
                    "internalize: a transactions row for {} stands; this call merges onto it",
                    txid
                );
                let subject = Subject {
                    txid,
                    raw_tx,
                    version: tx_version,
                    lock_time: tx_lock_time,
                    outputs: extracted_outputs,
                    has_proof,
                };
                return Box::pin(self.internalize_subject(user_id, args, subject, beef, true))
                    .await;
            }
            let new_tx_id = meta.last_row_id;

            let store = crate::r2::BlobStore::new(self.blobs);
            let (raw_tx_d1, _) = store
                .put("transactions", new_tx_id, "raw_tx", raw_tx.as_slice())
                .await?;
            let ib_d1 = self
                .put_input_beef("transactions", new_tx_id, &beef)
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

        // Step 7b (#22, btc-relay #16): mark the internalizing user's own
        // consumed INPUTS spent (another user's coin is marked where the
        // subject's checked proof is stored) — AFTER the subject
        // transactions row exists (spent_by
        // needs its transaction_id) and BEFORE the broadcast attempt in
        // step 10 (internalizeAction.ts:593-595). The merge path also lands
        // here and marks idempotently — a nosend can be merged from a sender
        // that doesn't share storage (internalizeAction.ts:496-504) — but
        // never restores (only step 10's outcomes other than a take do).
        // Without this, the consumed coins stay spendable=1 and the next
        // listOutputs re-serves an already-spent coin (the 2026-07-22
        // double-spend outage class).
        let input_transitions = self
            .mark_internalized_inputs_spent(user_id, &txid, &raw_tx, now)
            .await?;

        // Step 8: Add labels
        if let Some(ref labels) = args.labels {
            for label in labels {
                self.add_label(user_id, transaction_id, label).await?;
            }
        }

        // Step 9: Process each output
        let mut baskets_cache: HashMap<String, i64> = HashMap::new();
        // #56 fix 13: per-output merge outcomes — a guarded merge UPDATE
        // that changes 0 rows (relinquished/terminal row) must be visible
        // in the result, never a silent accepted:true.
        let mut merge_results: Vec<MergeResult> = Vec::new();
        // #56 fix 14: the broadcast-failure demotion (spendable=0 pending
        // the monitor) must be visible in the result.
        let mut outputs_demoted = false;

        // Each output is taken from a queue. An output whose insert writes
        // nothing (the schema holds one row to (transaction_id, vout,
        // user_id), and one stands) goes back to the front carrying that
        // row, and is merged onto it as an output step 5 had read.
        let mut queue: std::collections::VecDeque<OutputData> = outputs_data.into();
        while let Some(od) = queue.pop_front() {
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
                        let meta = Query::new(
                            // spendable: only re-enable when the row is not
                            // already consumed by a live createAction
                            // (spent_by set) — the TS merge never rewrites
                            // spendable at all (internalizeAction.ts:494-509);
                            // unconditionally forcing 1 here could resurrect
                            // a locked/spent output (audit minor-6).
                            //
                            // WHERE (dkls23 #180 fix-c): a RELINQUISHED row
                            // (basket NULL) that no createAction ever consumed
                            // (spent_by NULL) MAY be re-credited by a wallet
                            // payment — the owned-coin reclaim path (a wallet's
                            // own standing-child output mis-relinquished by a
                            // client-side scan was unreachable without a manual
                            // D1 restore; the TS reference merge re-baskets
                            // unconditionally, internalizeAction.ts:690-705).
                            // A relinquished row WITH spent_by stays terminal.
                            // ⚠️ The basket-insertion merge below keeps the
                            // STRICT basket-guard: its consumers spend via
                            // client-built txs the server never sees (bj hands
                            // — spent_by stays NULL on genuinely spent stakes),
                            // so this relaxation there would re-open the
                            // 2026-07-08 resurrection incident.
                            r#"UPDATE outputs
                            SET basket_id = ?, type = 'P2PKH', change = 1,
                                spendable = CASE WHEN spent_by IS NULL THEN 1 ELSE spendable END,
                                derivation_prefix = ?, derivation_suffix = ?,
                                sender_identity_key = ?, custom_instructions = NULL, updated_at = ?
                            WHERE output_id = ? AND (basket_id IS NOT NULL OR spent_by IS NULL)"#,
                        )
                        .bind(change_basket_id)
                        .bind(payment.derivation_prefix.as_str())
                        .bind(payment.derivation_suffix.as_str())
                        .bind(payment.sender_identity_key.as_str())
                        .bind(now)
                        .bind(output_id)
                        .execute(self.db)
                        .await?;
                        // #56 fix 13: changes == 0 ⇒ the terminal-row guard
                        // refused the merge — reflect it per-output instead
                        // of a silent accepted:true.
                        merge_results.push(MergeResult {
                            output_index: od.vout,
                            merged: meta.changes > 0,
                        });
                    } else {
                        // Two-phase INSERT for locking_script: output_id is PK
                        // autoincrement, so we reserve the row with NULL script
                        // first, then route the script through BlobStore.
                        let out_meta = Query::new(INTERNALIZE_INSERT_PAYMENT_OUTPUT_SQL)
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
                        if out_meta.changes == 0 {
                            worker::console_log!(
                                "internalize: an outputs row for {}:{} stands; the output merges onto it",
                                txid,
                                od.vout
                            );
                            let standing = self
                                .standing_output(user_id, &txid, transaction_id, od)
                                .await?;
                            if standing.is_change_in(change_basket_id) {
                                net_satoshis -= standing.satoshis as i64;
                            }
                            queue.push_front(standing);
                            continue;
                        }
                        let new_out_id = out_meta.last_row_id;
                        let store = crate::r2::BlobStore::new(self.blobs);
                        let (script_d1, _) = store
                            .put("outputs", new_out_id, "locking_script", &od.locking_script)
                            .await?;
                        Query::new("UPDATE outputs SET locking_script = ? WHERE output_id = ?")
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
                        // Update existing output. GUARD (2026-07-08 incident,
                        // the resurrection bug's second head): `basket_id IS
                        // NOT NULL` — a RELINQUISHED row (basket NULL,
                        // spendable 0) is terminally untracked ("goes to
                        // GONE", relinquish_output.rs) and a late re-credit
                        // must NOT re-basket it. Measured live: a bj-house
                        // pool output was credited at SEEN, staked + spent by
                        // a hand, relinquished — then the crediting cron's
                        // MINED-gated internalize re-basketed it and the
                        // monitor's confirmation pass re-enabled spendable,
                        // resurrecting a spent stake that a later hand picked
                        // (double-spend orphan). Same guard the wallet-payment
                        // merge above already carries (audit minor-6).
                        let meta = Query::new(
                            r#"UPDATE outputs
                            SET basket_id = ?, type = 'custom', change = 0,
                                custom_instructions = ?, derivation_prefix = NULL,
                                derivation_suffix = NULL, sender_identity_key = NULL, updated_at = ?
                            WHERE output_id = ? AND basket_id IS NOT NULL"#,
                        )
                        .bind(basket_id)
                        .bind(insertion.custom_instructions.as_deref())
                        .bind(now)
                        .bind(output_id)
                        .execute(self.db)
                        .await?;
                        // #56 fix 13: the strict basket-guard refusing the
                        // merge (0 rows) must reach the caller.
                        merge_results.push(MergeResult {
                            output_index: od.vout,
                            merged: meta.changes > 0,
                        });

                        if let Some(ref tags) = insertion.tags {
                            for tag in tags {
                                self.add_tag_to_output(user_id, output_id, tag).await?;
                            }
                        }
                    } else {
                        // Two-phase: basket-insertion custom scripts can be
                        // arbitrarily large (non-P2PKH). Reserve PK, then R2.
                        let meta = Query::new(INTERNALIZE_INSERT_BASKET_OUTPUT_SQL)
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
                        if meta.changes == 0 {
                            worker::console_log!(
                                "internalize: an outputs row for {}:{} stands; the output merges onto it",
                                txid,
                                od.vout
                            );
                            let standing = self
                                .standing_output(user_id, &txid, transaction_id, od)
                                .await?;
                            if standing.is_change_in(change_basket_id) {
                                net_satoshis -= standing.satoshis as i64;
                            }
                            queue.push_front(standing);
                            continue;
                        }

                        let output_id = meta.last_row_id;
                        let store = crate::r2::BlobStore::new(self.blobs);
                        let (script_d1, _) = store
                            .put("outputs", output_id, "locking_script", &od.locking_script)
                            .await?;
                        Query::new("UPDATE outputs SET locking_script = ? WHERE output_id = ?")
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
                let post = match self
                    .create_proven_tx_req(&txid, &raw_tx, &beef, has_proof, transaction_id)
                    .await
                {
                    Ok(post) => post,
                    Err(e) => {
                        // The call answers the first error; a restore that
                        // also fails is logged.
                        if let Err(restore_err) = self
                            .restore_internalized_inputs(&input_transitions, now)
                            .await
                        {
                            worker::console_error!(
                                "internalize: restore of inputs for txid={} failed: {}",
                                txid,
                                restore_err
                            );
                        }
                        return Err(e);
                    }
                };
                // Every outcome but a take gives back what step 7b
                // transitioned: the refusals, the policy hold and the
                // transient fault under either INTERNALIZE_ZERO_CONF setting.
                // The restore and the steps after it are independent: when the
                // restore errs, the credit of a transaction that was not taken
                // is still taken back below, and the call then answers the
                // restore's error.
                let restore = if post != PostOutcome::Sent {
                    self.restore_internalized_inputs(&input_transitions, now)
                        .await
                } else {
                    Ok(())
                };
                if let PostOutcome::Rejected(reason) = &post {
                    // The call already wrote its transaction row and its
                    // spendable=1 outputs, and D1 has no rollback: the
                    // compensation takes this call's credit back explicitly
                    // (the reference internalizes in one transaction,
                    // internalizeAction.ts:417-437).
                    self.compensate_rejected_internalize(&txid, &raw_tx, transaction_id, user_id)
                        .await;
                    restore?;
                    return Err(Error::ValidationError(reason.clone()));
                }
                // A policy refusal (B5, #50) demotes whatever INTERNALIZE_ZERO_CONF
                // says: Arcade files a script failure under 461 and an input sum
                // short of the outputs under 465, so a refused payment is not
                // credited until the tracker rule's evidence, the broadcaster's
                // SEEN_ON_NETWORK on a later announce or a checked proof (the
                // monitor's promotions), makes its outputs spendable.
                let demote = match post {
                    PostOutcome::Sent | PostOutcome::Rejected(_) => false,
                    PostOutcome::Transient => !self.internalize_zero_conf,
                    PostOutcome::PolicyHeld | PostOutcome::RefusalHeld(_) => true,
                };

                if demote {
                    outputs_demoted = true; // #56 fix 14: canonical fields below
                                            // Broadcast hit a network error (WoC down, DNS failure, timeout)
                                            // or a policy refusal. Mark outputs non-spendable to prevent
                                            // phantom UTXOs from inflating the user's balance. The monitor
                                            // will retry the broadcast every 5 minutes and set spendable = 1
                                            // once the broadcaster sees the transaction or it is confirmed
                                            // on-chain.
                                            //
                                            // ZERO-CONF dev lever (env INTERNALIZE_ZERO_CONF=true): SKIP this
                                            // demotion on a transient fault only. The operator funds from
                                            // their own already-broadcast wallet, so the funding tx is
                                            // genuinely on-network and the only reason it would land here is
                                            // a transient provider hiccup (ARC/WoC timeout). Keeping
                                            // spendable = 1 makes the deposit usable immediately (0-conf)
                                            // instead of waiting ~1 block for the monitor to reconcile. The
                                            // proven_tx_req still exists, so the monitor fetches the proof
                                            // and completes the status normally. NOTE: a DoubleSpend/InvalidTx
                                            // hard-rejects upstream in create_proven_tx_req, and a policy
                                            // refusal is demoted under either setting; only the transient
                                            // ServiceError path keeps its outputs here.
                    if let Err(e) = Query::new(
                        "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND user_id = ?",
                    )
                    .bind(now)
                    .bind(transaction_id)
                    .bind(user_id)
                    .execute(self.db)
                    .await
                    {
                        // The first error is answered.
                        restore?;
                        return Err(e);
                    }
                    // #22: the step-7b flips were rolled back above
                    // (internalizeAction.ts:120-128, :626-636). A policy
                    // refusal is re-announced by the monitor: the row reads
                    // `sending` (`unsent` is the req's word, no transaction
                    // status) until send_waiting's accepted arm moves it to
                    // `unproven` and marks the caller's inputs again.
                    if post == PostOutcome::PolicyHeld {
                        if let Err(e) = Query::new(
                            "UPDATE transactions SET status = 'sending', updated_at = ? WHERE transaction_id = ? AND status = 'unproven'",
                        )
                        .bind(now)
                        .bind(transaction_id)
                        .execute(self.db)
                        .await
                        {
                            restore?;
                            return Err(e);
                        }
                    }
                } else if post == PostOutcome::Transient {
                    worker::console_log!(
                        "internalize: broadcast ServiceError for {} but INTERNALIZE_ZERO_CONF=true — keeping outputs spendable (0-conf)",
                        txid
                    );
                }
                restore?;
                if let PostOutcome::RefusalHeld(reason) = post {
                    // The rows stay held without credit; the caller is
                    // answered the refusal.
                    return Err(Error::ValidationError(reason));
                }
            }
        }

        let noop = merge_noop(&merge_results);
        // btc-relay #56 fix 14, CONFORMANT form (wallet-toolbox
        // WalletStorage.interfaces.ts:272-303): a ServiceError demotion is
        // expressed via the canonical fields — sendWithResults 'unproven'
        // (a TS wallet's throwIfUnsuccessfulInternalizeAction tolerates it,
        // matching accepted-and-spendable-once-confirmed) and
        // notDelayedResults 'serviceError' (queued for delayed retries by
        // the monitor). Valid on this path by construction: the demotion
        // only fires for a non-merge tx unknown to storage (!linked).
        let (send_with_results, not_delayed_results) = if outputs_demoted {
            (
                Some(vec![SendWithResult {
                    txid: txid.clone(),
                    status: "unproven".to_string(),
                }]),
                Some(vec![ReviewActionResult {
                    txid: txid.clone(),
                    status: ReviewActionResultStatus::ServiceError,
                    competing_txs: None,
                    competing_beef: None,
                }]),
            )
        } else {
            (None, None)
        };
        Ok(StorageInternalizeActionResult {
            accepted: true,
            is_merge,
            txid,
            satoshis: net_satoshis,
            send_with_results,
            not_delayed_results,
            merge_results: if merge_results.is_empty() {
                None
            } else {
                Some(merge_results)
            },
            noop,
        })
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    /// Store a BEEF as the `input_beef` blob of a row: inline bytes through
    /// the blob store (D1 up to 4,096 bytes, R2 above), a BEEF at rest by a
    /// streamed copy into this Worker's bucket at the blob store's key. The
    /// D1 column's value is returned (`None` when the bytes are in R2).
    async fn put_input_beef(
        &self,
        table: &str,
        id: i64,
        beef: &BeefBlob<'_>,
    ) -> Result<Option<Vec<u8>>> {
        match beef {
            BeefBlob::Inline(bytes) => {
                let store = crate::r2::BlobStore::new(self.blobs);
                Ok(store.put(table, id, "input_beef", bytes).await?.0)
            }
            BeefBlob::AtRest {
                store,
                reference,
                caller,
            } => {
                let key = crate::r2::r2_key(table, id, "input_beef");
                crate::beef_at_rest::copy_to(store, reference, caller, self.blobs, &key).await?;
                Ok(None)
            }
        }
    }

    async fn find_existing_transaction(
        &self,
        user_id: i64,
        txid: &str,
    ) -> Result<Option<TableTransaction>> {
        let row: Option<TransactionRow> = Query::new(INTERNALIZE_FIND_TX_SQL)
            .bind(user_id)
            .bind(txid)
            .fetch_optional(self.db)
            .await?;

        Ok(row.map(|r| r.into_table()))
    }

    async fn load_existing_outputs(&self, user_id: i64, txid: &str) -> Result<Vec<TableOutput>> {
        let rows: Vec<OutputRow> = Query::new(INTERNALIZE_EXISTING_OUTPUTS_SQL)
            .bind(user_id)
            .bind(txid)
            .fetch_all(self.db)
            .await?;

        Ok(rows.into_iter().map(|r| r.into_table()).collect())
    }

    /// The output specification `od`, whose insert wrote nothing, carrying
    /// the row that stands for its index: read as step 5 reads it, so the
    /// output is merged exactly as one found there. A row the schema's key
    /// holds and that read does not return is answered as the fault the
    /// insert itself gave.
    async fn standing_output(
        &self,
        user_id: i64,
        txid: &str,
        transaction_id: i64,
        mut od: OutputData,
    ) -> Result<OutputData> {
        let existing = self.load_existing_outputs(user_id, txid).await?;
        let row = existing
            .iter()
            .find(|o| o.vout == od.vout as i32)
            .ok_or_else(|| {
                Error::DatabaseError(format!(
                    "internalize: outputs holds a row at ({transaction_id}, {}, {user_id}) that is not read under {txid}",
                    od.vout
                ))
            })?;
        od.existing_output_id = Some(row.output_id);
        od.existing_basket_id = row.basket_id;
        od.existing_is_change = row.change;
        Ok(od)
    }

    /// Mark the internalizing user's own rows at each of the subject tx's
    /// consumed-input outpoints as spent (internalizeAction.ts
    /// markUserInputsSpent, :73-93, for the caller's rows; another user's
    /// row waits for the subject's checked proof). Returns the exact
    /// per-row transitions so `restore_internalized_inputs` can roll them
    /// back on every outcome but a take. Idempotent: already-unspendable rows and
    /// rows spent by a different tx are skipped. D1 has no transactions —
    /// best-effort sequential writes, matching this file's existing idiom;
    /// each write is individually guarded so a partial pass never produces
    /// an inconsistent row.
    async fn mark_internalized_inputs_spent(
        &self,
        user_id: i64,
        txid: &str,
        raw_tx: &[u8],
        now: chrono::DateTime<Utc>,
    ) -> Result<Vec<SpentInputTransition>> {
        // Coinbase / unparseable inputs are skipped by the parser; nothing
        // consumed ⇒ nothing to do (internalizeAction.ts:73-76).
        let outpoints = crate::monitor::parse_input_outpoints(raw_tx);
        if outpoints.is_empty() {
            return Ok(Vec::new());
        }

        let mut transitions: Vec<SpentInputTransition> = Vec::new();
        for (prev_txid, prev_vout) in outpoints {
            let rows: Vec<InputOutpointRow> = Query::new(INTERNALIZE_INPUT_ROWS_SQL)
                .bind(txid)
                .bind(prev_txid.as_str())
                .bind(prev_vout as i64)
                .bind(user_id)
                .fetch_all(self.db)
                .await?;

            for row in rows {
                let output_id = match row.output_id.map(|v| v as i64) {
                    Some(id) => id,
                    None => continue,
                };
                let decision = input_mark_decision(
                    row.spendable.map(|v| v != 0.0).unwrap_or(false),
                    row.spent_by.map(|v| v as i64),
                    row.user_tx_id.map(|v| v as i64),
                );
                let set_spent_by = match decision {
                    InputMarkDecision::Skip => continue,
                    InputMarkDecision::Mark { set_spent_by } => set_spent_by,
                };
                let meta = Query::new(INTERNALIZE_MARK_INPUT_SQL)
                    .bind(txid)
                    .bind(now)
                    .bind(output_id)
                    .bind(user_id)
                    .bind(txid)
                    .execute(self.db)
                    .await?;
                // changes == 0 ⇒ the write-time guard saw a state change
                // since the read (raced) — the row was NOT flipped by this
                // call, so it must not be restorable by this call.
                if meta.changes > 0 {
                    transitions.push(SpentInputTransition {
                        output_id,
                        set_spent_by,
                    });
                }
            }
        }
        Ok(transitions)
    }

    /// Revert the spendable→0 transitions performed by
    /// `mark_internalized_inputs_spent` (internalizeAction.ts
    /// restoreInputsToSpendable, :120-128). Called on every outcome of the
    /// post but a take. Undoes exactly what was changed: spendable=1 always;
    /// spent_by cleared only when the original transition stamped it. The
    /// SQL guards make a double restore a no-op and refuse to clobber a
    /// competing spend or resurrect a since-relinquished row.
    async fn restore_internalized_inputs(
        &self,
        transitions: &[SpentInputTransition],
        now: chrono::DateTime<Utc>,
    ) -> Result<()> {
        for t in transitions {
            match t.set_spent_by {
                Some(stamped_tx_id) => {
                    Query::new(INTERNALIZE_RESTORE_SPENT_BY_SQL)
                        .bind(now)
                        .bind(t.output_id)
                        .bind(stamped_tx_id)
                        .execute(self.db)
                        .await?;
                }
                None => {
                    Query::new(INTERNALIZE_RESTORE_SPENDABLE_SQL)
                        .bind(now)
                        .bind(t.output_id)
                        .execute(self.db)
                        .await?;
                }
            }
        }
        Ok(())
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

    /// Returns what the broadcast left: anything but `Sent` marks the outputs
    /// non-spendable until the monitor confirms the broadcast.
    async fn create_proven_tx_req(
        &self,
        txid: &str,
        raw_tx: &[u8],
        input_beef: &BeefBlob<'_>,
        has_verified_proof: bool,
        transaction_id: i64,
    ) -> Result<PostOutcome> {
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
            // A refusal held on the standing request, with nothing credited:
            // this call's rows are held as that call's were, and it is
            // answered the same.
            if !has_verified_proof {
                if let Some(answer) = self
                    .refusal_held_answer(REFUSAL_HELD_ON_NEW_ROW_SQL, txid, transaction_id)
                    .await?
                {
                    return Ok(PostOutcome::RefusalHeld(answer));
                }
            }
            return Ok(PostOutcome::Sent);
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
        // the parents. See UNPROVEN-BACKLOG-DESIGN.md for the incident that
        // made this critical (2026-04-15 bulk cleanup of 805 stuck txs).
        //
        // A Rejected error means double-spend → reject the payment.
        // Permanent errors (DoubleSpend/InvalidTx) reject the payment.
        // ServiceError means transient failure -- monitor will retry.
        //
        // PROVEN TXS ARE NOT RE-BROADCAST (adversarial review H2): a payment
        // whose BUMP root was strict-verified against a real header is a
        // chain fact. Re-posting an old mined tx to ARC can come back
        // "missing inputs" (spent by ITSELF, past the cache window), which
        // maps to DoubleSpend/InvalidTx and used to de-credit a genuinely
        // mined payment with no proven_tx_req row for the canary to rescue.
        let broadcast_result = if has_verified_proof {
            Ok(crate::services::BroadcastResult {
                txid: txid.to_string(),
                tx_status: "proof-verified, broadcast skipped".to_string(),
                seen_on_network: true,
            })
        } else {
            match input_beef {
                // ARC is handed the BEEF behind the AtomicBEEF's prefix, as
                // bytes (`arc_beef_request`, bsv-stack-lean #63); Arcade, EF.
                BeefBlob::Inline(bytes) => self.broadcast.broadcast_beef(&hex::encode(bytes)).await,
                // A BEEF at rest is never made whole to be posted (NL-7). The
                // subject the network already holds needs no broadcast; one it
                // does not is left to the monitor as a transient fault is,
                // and the caller treats it so (INTERNALIZE_ZERO_CONF decides
                // whether the outputs stay spendable meanwhile).
                BeefBlob::AtRest { .. } => {
                    if self.network_knows_txid(txid).await == NetworkWord::Knows {
                        Ok(crate::services::BroadcastResult {
                            txid: txid.to_string(),
                            tx_status: "known to the network, broadcast skipped".to_string(),
                            seen_on_network: true,
                        })
                    } else {
                        worker::console_log!(
                            "internalize: {} rests in R2 and the network does not know it; the broadcast is the monitor's",
                            txid
                        );
                        Err(crate::services::BroadcastError::ServiceError(
                            "a BEEF at rest is not posted from the request".to_string(),
                        ))
                    }
                }
            }
        };
        let mut policy_hint = None;
        let post = match broadcast_result {
            Ok(_result) => {
                // Broadcast succeeded (new or already known) -- proceed
                PostOutcome::Sent
            }
            Err(crate::services::BroadcastError::DoubleSpend(msg)) => {
                // Double-spend verdict from ARC. CHAIN-TRUTH GATE first
                // (review H2): an old-but-mined tx re-validated fresh can
                // come back "inputs missing/spent" — spent by ITSELF. Only
                // compensate when the network genuinely doesn't know the
                // txid; a known/mined tx proceeds as an accepted payment.
                match self.network_knows_txid(txid).await {
                    NetworkWord::Knows => {
                        worker::console_log!(
                            "internalize: ARC said double-spend for {} but the network knows it — treating as accepted",
                            txid
                        );
                        PostOutcome::Sent
                    }
                    // The second source gave no answer: the rows are held, as
                    // a transient fault's are, and the caller is answered the
                    // refusal; the monitor re-asks.
                    NetworkWord::Unavailable => PostOutcome::RefusalHeld(format!(
                        "Transaction {} rejected by network (double-spend): {}",
                        txid, msg
                    )),
                    NetworkWord::DoesNotKnow => {
                        return Ok(PostOutcome::Rejected(format!(
                            "Transaction {} rejected by network (double-spend): {}",
                            txid, msg
                        )))
                    }
                }
            }
            Err(crate::services::BroadcastError::InvalidTx(msg)) => {
                // Same chain-truth gate + compensation as the double-spend
                // arm (audit C1 + review H2).
                match self.network_knows_txid(txid).await {
                    NetworkWord::Knows => {
                        worker::console_log!(
                            "internalize: ARC said invalid-tx for {} but the network knows it — treating as accepted",
                            txid
                        );
                        PostOutcome::Sent
                    }
                    NetworkWord::Unavailable => PostOutcome::RefusalHeld(format!(
                        "Transaction {} rejected by network (invalid tx): {}",
                        txid, msg
                    )),
                    NetworkWord::DoesNotKnow => {
                        return Ok(PostOutcome::Rejected(format!(
                            "Transaction {} rejected by network (invalid tx): {}",
                            txid, msg
                        )))
                    }
                }
            }
            Err(crate::services::BroadcastError::ServiceError(_)) => {
                // Service error -- proceed cautiously, monitor will retry broadcast.
                // Caller must mark outputs non-spendable until broadcast is confirmed.
                PostOutcome::Transient
            }
            Err(crate::services::BroadcastError::PolicyRefusal(refusal)) => {
                // B5 (#50): the instance's policy answer or conflict hint is a hint,
                // never invalidity ("an Arcade REJECTED is not necessarily terminal",
                // ts-stack@edf6e03 e2e/storage/README.md:113). No compensation; the
                // req is queued `unsent` with the hint so send_waiting re-announces it.
                policy_hint = Some(serde_json::json!({
                    "when": Utc::now().to_rfc3339(), "what": "internalizeBroadcastHint",
                    "outcome": refusal.outcome(), "words": refusal.hint_words(), "nextReaskMinutes": 5,
                }));
                PostOutcome::PolicyHeld
            }
        };

        let now = Utc::now();

        // raw_tx is NOT NULL in the schema AND always available here (we just
        // parsed it from the AtomicBEEF). Single-phase INSERT binds it
        // directly, matching reference semantics. input_beef is potentially
        // huge (AtomicBEEF of refund flows can be 100+ KB) — use two-phase
        // so R2 overflow works.
        let req_meta = Query::new(INTERNALIZE_INSERT_REQ_SQL)
            .bind(txid)
            .bind(if policy_hint.is_some() {
                "unsent"
            } else {
                "unmined"
            })
            .bind(raw_tx)
            .bind(now)
            .bind(now)
            .execute(self.db)
            .await?;
        // `changes == 0`: a request for this txid stands, made by another
        // writer since the read above. It serves this call as it is, and its
        // BEEF is its writer's to store; what this call's broadcast left is
        // still what the caller is told.
        if req_meta.changes > 0 {
            let req_id = req_meta.last_row_id;

            let ib_d1 = self
                .put_input_beef("proven_tx_reqs", req_id, input_beef)
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
        if let Some(note) = policy_hint {
            super::tracker_rule::append_note(self.db, txid, note).await?;
        }
        if let PostOutcome::RefusalHeld(answer) = &post {
            // Best-effort: the caller's rows are held below whatever this
            // write returns.
            if let Err(e) = Query::new(RECORD_REFUSAL_HELD_SQL)
                .bind(now.to_rfc3339())
                .bind(answer.as_str())
                .bind(txid)
                .execute(self.db)
                .await
            {
                worker::console_error!(
                    "internalize: the held refusal of {} was not recorded on its request: {}",
                    txid,
                    e
                );
            }
        }

        Ok(post)
    }

    /// The answer of a refusal held on the request for `txid`, for the
    /// caller's row `transaction_id` (`refusal_held_answer_sql!`).
    async fn refusal_held_answer(
        &self,
        sql: &str,
        txid: &str,
        transaction_id: i64,
    ) -> Result<Option<String>> {
        #[derive(Deserialize)]
        struct AnswerRow {
            answer: Option<String>,
        }
        let row: Option<AnswerRow> = Query::new(sql)
            .bind(txid)
            .bind(transaction_id)
            .bind(transaction_id)
            .fetch_optional(self.db)
            .await?;
        Ok(row.and_then(|r| r.answer))
    }

    /// The second source's word on a txid the broadcaster refused: known,
    /// not known, or no answer (an error), which is a hold.
    async fn network_knows_txid(&self, txid: &str) -> NetworkWord {
        network_word(
            self.broadcast
                .get_status_for_txids(&[txid.to_string()])
                .await,
        )
    }

    /// Compensation for an internalize whose broadcast came back with a
    /// positive ARC reject (DoubleSpend/InvalidTx), the second source
    /// answering that it does not know the txid, AFTER the transaction and
    /// output rows were written (audit C1). D1 exposes no real transactions
    /// to roll back, so undo this call's credit explicitly:
    ///   * this call's transactions row → 'failed', unless it is
    ///     `completed` or carries a proof;
    ///   * that row's outputs → spendable=0, only while it reads `failed`.
    ///
    /// No row of another user is touched. Best-effort (errors logged, not
    /// propagated — the caller is already returning the reject).
    async fn compensate_rejected_internalize(
        &self,
        txid: &str,
        raw_tx: &[u8],
        transaction_id: i64,
        user_id: i64,
    ) {
        let now = Utc::now();
        let r1 = Query::new(COMPENSATE_FAIL_TX_SQL)
            .bind(now)
            .bind(transaction_id)
            .bind(user_id)
            .execute(self.db)
            .await;
        let r2 = Query::new(COMPENSATE_ZERO_OUTPUTS_SQL)
            .bind(now)
            .bind(transaction_id)
            .bind(user_id)
            .execute(self.db)
            .await;
        // Canary coverage (review H2c): the auto-unfail sweep JOINs on
        // proven_tx_reqs — without a row here, a wrong verdict would be
        // permanently invisible to self-correction. INSERT OR IGNORE keyed
        // on the txid uniqueness in practice (existing row = already
        // covered).
        let _ = Query::new(COMPENSATE_REQ_SQL)
            .bind(txid)
            .bind(raw_tx)
            .bind(now)
            .bind(now)
            .bind(txid)
            .execute(self.db)
            .await;
        if r1.is_err() || r2.is_err() {
            worker::console_error!(
                "internalize compensation for rejected txid={} incomplete: tx={:?} outputs={:?}",
                txid,
                r1.err().map(|e| e.to_string()),
                r2.err().map(|e| e.to_string())
            );
        } else {
            worker::console_log!(
                "internalize compensation: txid={} de-credited after network reject",
                txid
            );
        }
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Pure eligibility/transition decision for one row at a consumed outpoint
/// (internalizeAction.ts:86-93):
///   * `spendable == false` → Skip (idempotence — a prior mark, a live
///     createAction lock, or a relinquish already accounts for it);
///   * `spent_by` set to a DIFFERENT tx than the subject tx's id in the
///     row-owner's scope → Skip (competing spend recorded; never overwrite);
///   * `spent_by` already the subject tx → Mark, spendable only (this call
///     did not stamp it, so restore must not clear it);
///   * `spent_by` NULL → Mark, stamping `user_tx_id` when the row's user has
///     their own transactions row for the subject txid (None for foreign
///     users without one — their spent_by stays NULL).
fn input_mark_decision(
    spendable: bool,
    spent_by: Option<i64>,
    user_tx_id: Option<i64>,
) -> InputMarkDecision {
    if !spendable {
        return InputMarkDecision::Skip;
    }
    match spent_by {
        Some(existing) if user_tx_id != Some(existing) => InputMarkDecision::Skip,
        Some(_) => InputMarkDecision::Mark { set_spent_by: None },
        None => InputMarkDecision::Mark {
            set_spent_by: user_tx_id,
        },
    }
}

fn validate_merge_status(status: &TransactionStatus) -> Result<()> {
    match status {
        // 'sending' accepted per 2.4.0 (internalizeAction.ts:327) — our M2
        // delayed-commit design parks queued txs at 'sending', so rejecting
        // it here would bounce legitimate payment merges onto our own
        // in-flight transactions.
        TransactionStatus::Completed
        | TransactionStatus::Unproven
        | TransactionStatus::NoSend
        | TransactionStatus::Sending => Ok(()),
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
    fn test_validate_merge_status_sending_accepted() {
        // 2.4.0 parity (internalizeAction.ts:327): 'sending' is a legal
        // merge target — our delayed txs park there (M2), so rejecting it
        // bounced payment merges onto our own in-flight transactions.
        let result = validate_merge_status(&TransactionStatus::Sending);
        assert!(result.is_ok());
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

    // =========================================================================
    // The merge-guard ASYMMETRY pin (dkls23 #180 fix-c vs the 2026-07-08
    // resurrection incident) — the two existing-output merge UPDATEs must
    // carry DIFFERENT guards, and a refactor must not symmetrize them:
    //   • wallet payment: relinquished-but-never-createAction-consumed rows
    //     (basket NULL, spent_by NULL) MAY be re-credited — the owned-coin
    //     reclaim path (TS reference merges unconditionally).
    //   • basket insertion: STRICT basket_id IS NOT NULL — its consumers (bj
    //     hands) spend via client-built txs the server never sees, so
    //     spent_by NULL does NOT mean unspent there; relaxing it re-opens
    //     the measured double-spend resurrection.
    // Pinned against the compiled source so the guard can't drift silently.
    // =========================================================================

    #[test]
    fn test_merge_guard_asymmetry_pin() {
        let src = include_str!("internalize_action.rs");
        // concat! keeps these literals from matching THEMSELVES in the included source (the
        // reviewer-caught vacuous-pin trap): each assertion can only match the real SQL.
        let relaxed = concat!(
            "WHERE output_id = ? AND (basket_id IS NOT ",
            "NULL OR spent_by IS NULL)"
        );
        let strict = concat!("WHERE output_id = ? AND basket_id IS NOT ", "NULL\"#");
        assert_eq!(
            src.matches(relaxed).count(),
            1,
            "the wallet-payment merge lost its #180 fix-c reclaim clause \
             (relinquished-but-unspent rows must be re-creditable) — or the \
             clause spread beyond the ONE wallet-payment head"
        );
        assert_eq!(
            src.matches(strict).count(),
            1,
            "the basket-insertion merge lost its STRICT anti-resurrection \
             guard (the 2026-07-08 incident class — do NOT relax it), or a \
             second head went strict (a guard SWAP would be invisible here \
             without the exact-count pins)"
        );
        // Head-BINDING (a guard SWAP passes both counts): each clause must sit inside ITS
        // protocol's merge arm. The merge-loop arms are the LAST occurrences (the earlier ones
        // are the validation passes); rfind + the concat-split literals keep this test's own
        // text out of the search.
        let wp_arm = src
            .rfind(concat!("WALLET_PAYMENT_", "PROTOCOL => {"))
            .expect("wallet-payment merge arm");
        let bi_arm = src
            .rfind(concat!("BASKET_INSERTION_", "PROTOCOL => {"))
            .expect("basket-insertion merge arm");
        let relaxed_at = src.find(relaxed).expect("relaxed clause");
        let strict_at = src.find(strict).expect("strict clause");
        assert!(
            wp_arm < relaxed_at && relaxed_at < bi_arm && bi_arm < strict_at,
            "guard/head binding broken: the relaxed clause must live in the \
             wallet-payment merge arm and the strict clause in the \
             basket-insertion merge arm (positions wp_arm={wp_arm} \
             relaxed={relaxed_at} bi_arm={bi_arm} strict={strict_at})"
        );
    }

    // =========================================================================
    // #22 — spent-input marking: real-SQLite harness (monitor.rs g5 pattern).
    // The ACTUAL migrations + the ACTUAL SQL consts against an in-memory DB
    // (D1 is SQLite; semantics match), driven by the ACTUAL pure decision fn
    // — the same read → decide → guarded-write sequence as
    // mark_internalized_inputs_spent / restore_internalized_inputs.
    // =========================================================================

    /// The subject (internalized) tx in the fixtures.
    const I22_SUBJECT_TXID: &str = "cc01";
    /// Its consumed-input outpoints (what parse_input_outpoints would yield).
    /// dd01:0 has no storage rows anywhere — the foreign-outpoint no-op case.
    const I22_OUTPOINTS: &[(&str, i64)] = &[("aa01", 0), ("aa01", 1), ("aa02", 0), ("dd01", 0)];

    /// Fixture map (output_id → role):
    ///   1  user 1 (internalizer), aa01:0, spendable=1, free
    ///        → owned candidate: spent_by must become 10 (user 1's cc01 row)
    ///   2  user 2, aa01:0, spendable=1, free; user 2 has NO cc01 record
    ///        → cross-user flip: spendable=0 only, spent_by stays NULL
    ///   3  user 3, aa01:0, spendable=1, free; user 3 HAS a cc01 record (12)
    ///        → cross-user flip stamped with THEIR tx id 12, never 10 (H3)
    ///   4  user 1, aa01:1, spendable=0, spent_by=3 (live createAction lock)
    ///        → competing spend: untouched, never overwritten
    ///   5  user 1, aa02:0, spendable=0, free
    ///        → already unspendable: skipped (idempotence), never restored
    ///   6  user 1, aa02:5, spendable=1, free
    ///        → NOT a consumed outpoint: untouched by mark AND restore
    fn i22_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../migrations/0001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../migrations/0002_add_indexes.sql"))
            .unwrap();
        conn.execute_batch(include_str!(
            "../../migrations/0003_add_output_reservations.sql"
        ))
        .unwrap();

        conn.execute_batch(
            r#"
            INSERT INTO users (user_id, identity_key) VALUES (1, '02aa'), (2, '02bb'), (3, '02cc');
            INSERT INTO output_baskets (basket_id, user_id, name)
            VALUES (10, 1, 'default'), (20, 2, 'default'), (30, 3, 'default');

            INSERT INTO transactions (transaction_id, user_id, status, reference, is_outgoing, description, txid)
            VALUES
              (1,  1, 'completed', 'r1',  0, 'fund aa01 (user 1)',   'aa01'),
              (2,  2, 'completed', 'r2',  0, 'fund aa01 (user 2)',   'aa01'),
              (3,  1, 'completed', 'r3',  1, 'competing spender',    'bb01'),
              (4,  1, 'completed', 'r4',  0, 'fund aa02 (user 1)',   'aa02'),
              (5,  3, 'completed', 'r5',  0, 'fund aa01 (user 3)',   'aa01'),
              (10, 1, 'unproven',  'r10', 0, 'subject (user 1)',     'cc01'),
              (12, 3, 'unproven',  'r12', 0, 'subject (user 3)',     'cc01');

            INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, spendable, change, vout, satoshis,
                                 provided_by, purpose, type, txid, spent_by)
            VALUES
              (1, 1, 1, 10, 1, 1, 0, 1000, 'storage', 'change', 'P2PKH', 'aa01', NULL),
              (2, 2, 2, 20, 1, 1, 0, 2000, 'storage', 'change', 'P2PKH', 'aa01', NULL),
              (3, 3, 5, 30, 1, 1, 0, 3000, 'storage', 'change', 'P2PKH', 'aa01', NULL),
              (4, 1, 1, 10, 0, 1, 1, 4000, 'storage', 'change', 'P2PKH', 'aa01', 3),
              (5, 1, 4, 10, 0, 1, 0, 5000, 'storage', 'change', 'P2PKH', 'aa02', NULL),
              (6, 1, 4, 10, 1, 1, 5, 6000, 'storage', 'change', 'P2PKH', 'aa02', NULL);
            "#,
        )
        .unwrap();
        conn
    }

    /// The internalizing user of the #22 fixtures.
    const I22_USER: i64 = 1;

    /// Native mirror of mark_internalized_inputs_spent for the fixtures'
    /// internalizer (user 1).
    fn i22_mark(conn: &rusqlite::Connection) -> Vec<SpentInputTransition> {
        mark_inputs_for(conn, I22_USER, I22_SUBJECT_TXID, I22_OUTPOINTS)
    }

    /// Native mirror of mark_internalized_inputs_spent: the real SELECT →
    /// the real decision fn → the real guarded UPDATE, for the rows of
    /// `user` alone, recording transitions only when the write changed the
    /// row.
    fn mark_inputs_for(
        conn: &rusqlite::Connection,
        user: i64,
        subject: &str,
        outpoints: &[(&str, i64)],
    ) -> Vec<SpentInputTransition> {
        let mut transitions = Vec::new();
        for (prev_txid, prev_vout) in outpoints {
            let rows: Vec<(i64, bool, Option<i64>, Option<i64>)> = {
                let mut stmt = conn.prepare(INTERNALIZE_INPUT_ROWS_SQL).unwrap();
                let rows = stmt
                    .query_map(
                        rusqlite::params![subject, prev_txid, prev_vout, user],
                        |r| {
                            Ok((
                                r.get::<_, i64>(0)?,
                                r.get::<_, i64>(1)? != 0,
                                r.get(2)?,
                                r.get(3)?,
                            ))
                        },
                    )
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap();
                rows
            };
            for (output_id, spendable, spent_by, user_tx_id) in rows {
                let set_spent_by = match input_mark_decision(spendable, spent_by, user_tx_id) {
                    InputMarkDecision::Skip => continue,
                    InputMarkDecision::Mark { set_spent_by } => set_spent_by,
                };
                let changes = conn
                    .execute(
                        INTERNALIZE_MARK_INPUT_SQL,
                        rusqlite::params![subject, "t-mark", output_id, user, subject],
                    )
                    .unwrap();
                if changes > 0 {
                    transitions.push(SpentInputTransition {
                        output_id,
                        set_spent_by,
                    });
                }
            }
        }
        transitions
    }

    /// Native mirror of restore_internalized_inputs.
    fn i22_restore(conn: &rusqlite::Connection, transitions: &[SpentInputTransition]) {
        for t in transitions {
            match t.set_spent_by {
                Some(stamped) => {
                    conn.execute(
                        INTERNALIZE_RESTORE_SPENT_BY_SQL,
                        rusqlite::params!["t-restore", t.output_id, stamped],
                    )
                    .unwrap();
                }
                None => {
                    conn.execute(
                        INTERNALIZE_RESTORE_SPENDABLE_SQL,
                        rusqlite::params!["t-restore", t.output_id],
                    )
                    .unwrap();
                }
            }
        }
    }

    fn i22_row(conn: &rusqlite::Connection, output_id: i64) -> (i64, Option<i64>) {
        conn.query_row(
            "SELECT spendable, spent_by FROM outputs WHERE output_id = ?",
            [output_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    }

    /// audit.rs Case A: spendable=1 AND spent_by set is a flagged
    /// contradiction — no path here may ever produce it.
    fn i22_assert_no_case_a(conn: &rusqlite::Connection) {
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outputs WHERE spendable = 1 AND spent_by IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 0,
            "audit Case A produced (spendable=1 with spent_by set)"
        );
    }

    #[test]
    fn test_i22_owned_input_marked_spent() {
        // (a) The internalizing user's own consumed coin: spendable=0 and
        // spent_by = the subject tx's transaction_id in THEIR scope (10).
        let conn = i22_db();
        let transitions = i22_mark(&conn);
        assert_eq!(i22_row(&conn, 1), (0, Some(10)));
        assert!(transitions.contains(&SpentInputTransition {
            output_id: 1,
            set_spent_by: Some(10)
        }));
        i22_assert_no_case_a(&conn);
    }

    #[test]
    fn test_i22_foreign_outpoint_and_unrelated_rows_noop() {
        // (b) dd01:0 has no rows → contributes nothing; rows at outpoints
        // the tx does not consume (output 6) are untouched, and so are the
        // rows of users 2 and 3 at aa01:0. Exactly the internalizer's one
        // eligible row transitions — nothing else.
        let conn = i22_db();
        let transitions = i22_mark(&conn);
        let mut flipped: Vec<i64> = transitions.iter().map(|t| t.output_id).collect();
        flipped.sort_unstable();
        assert_eq!(flipped, vec![1]);
        assert_eq!(i22_row(&conn, 2), (1, None));
        assert_eq!(i22_row(&conn, 3), (1, None));
        assert_eq!(i22_row(&conn, 6), (1, None));
    }

    #[test]
    fn test_i22_competing_spend_never_overwritten() {
        // (c) A row a live createAction locked (spendable=0, spent_by=3)
        // is skipped by the decision fn AND refused by the SQL guard even
        // when driven directly (defense in depth against races).
        let conn = i22_db();
        let transitions = i22_mark(&conn);
        assert_eq!(i22_row(&conn, 4), (0, Some(3)));
        assert!(!transitions.iter().any(|t| t.output_id == 4));

        // Direct guarded UPDATE against the locked row: no-op.
        let changes = conn
            .execute(
                INTERNALIZE_MARK_INPUT_SQL,
                rusqlite::params![
                    I22_SUBJECT_TXID,
                    "t-direct",
                    4i64,
                    I22_USER,
                    I22_SUBJECT_TXID
                ],
            )
            .unwrap();
        assert_eq!(changes, 0);

        // Anomalous Case-A pre-state (spendable=1 with a FOREIGN spent_by —
        // audit's job to flag, never ours to compound): both layers skip it.
        conn.execute_batch(
            "INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, spendable, change,
                                  vout, satoshis, provided_by, purpose, type, txid, spent_by)
             VALUES (7, 2, 2, 20, 1, 1, 1, 7000, 'storage', 'change', 'P2PKH', 'aa01', 2)",
        )
        .unwrap();
        let transitions = i22_mark(&conn);
        assert!(transitions.is_empty()); // everything else already marked
        assert_eq!(i22_row(&conn, 7), (1, Some(2)));
        let changes = conn
            .execute(
                INTERNALIZE_MARK_INPUT_SQL,
                rusqlite::params![I22_SUBJECT_TXID, "t-direct", 7i64, 2i64, I22_SUBJECT_TXID],
            )
            .unwrap();
        assert_eq!(changes, 0);
        // Driven directly at another user's row under the internalizer's id:
        // the statement's own user guard refuses it.
        let changes = conn
            .execute(
                INTERNALIZE_MARK_INPUT_SQL,
                rusqlite::params![
                    I22_SUBJECT_TXID,
                    "t-direct",
                    2i64,
                    I22_USER,
                    I22_SUBJECT_TXID
                ],
            )
            .unwrap();
        assert_eq!(changes, 0);
        assert_eq!(i22_row(&conn, 2), (1, None));
    }

    #[test]
    fn test_i22_cross_user_semantics() {
        // (d) The call writes no row of another user. The spend reaches
        // every user's row at the store of the subject's checked proof
        // (monitor::PROOF_MARK_SPENT_INPUT_SQL): a user without their own
        // record of the subject tx gets spendable=0 only (spent_by must
        // reference THEIR transactions); a user WITH their own record gets
        // THEIR transaction_id — never the internalizer's (review H3).
        let conn = i22_db();
        let transitions = i22_mark(&conn);
        assert_eq!(i22_row(&conn, 2), (1, None));
        assert_eq!(i22_row(&conn, 3), (1, None));
        assert!(!transitions
            .iter()
            .any(|t| t.output_id == 2 || t.output_id == 3));
        for (prev_txid, prev_vout) in I22_OUTPOINTS {
            conn.execute(
                crate::monitor::PROOF_MARK_SPENT_INPUT_SQL,
                rusqlite::params![
                    I22_SUBJECT_TXID,
                    "t-proof",
                    prev_txid,
                    prev_vout,
                    I22_SUBJECT_TXID
                ],
            )
            .unwrap();
        }
        assert_eq!(i22_row(&conn, 1), (0, Some(10)));
        assert_eq!(i22_row(&conn, 2), (0, None));
        assert_eq!(i22_row(&conn, 3), (0, Some(12)));
        assert_eq!(i22_row(&conn, 4), (0, Some(3))); // a competing lock intact
        assert_eq!(i22_row(&conn, 6), (1, None)); // not an outpoint the bytes spend
        i22_assert_no_case_a(&conn);
    }

    #[test]
    fn test_i22_restore_reverses_exact_transitions() {
        // (e) Restore undoes exactly what mark did: same-user rows get
        // spendable=1 + spent_by cleared; cross-user spendable-only rows
        // get spendable=1 only; untouched rows stay untouched.
        let conn = i22_db();
        let transitions = i22_mark(&conn);
        i22_restore(&conn, &transitions);
        assert_eq!(i22_row(&conn, 1), (1, None));
        assert_eq!(i22_row(&conn, 2), (1, None));
        assert_eq!(i22_row(&conn, 3), (1, None));
        assert_eq!(i22_row(&conn, 4), (0, Some(3))); // competing lock intact
        assert_eq!(i22_row(&conn, 6), (1, None)); // unrelated row intact
        i22_assert_no_case_a(&conn);
    }

    #[test]
    fn test_i22_restore_never_resurrects_unflipped_or_moved_rows() {
        // (f) Restore touches ONLY rows this call flipped, and refuses rows
        // whose state moved underneath it.
        let conn = i22_db();
        let transitions = i22_mark(&conn);

        // Row 5 was already unspendable before the call — no transition, so
        // restore must not resurrect it.
        assert!(!transitions.iter().any(|t| t.output_id == 5));

        // Row 1: a competitor overwrote our spent_by stamp in the window —
        // the spent_by = <stamped> guard refuses to clobber it.
        conn.execute_batch("UPDATE outputs SET spent_by = 3 WHERE output_id = 1")
            .unwrap();

        i22_restore(&conn, &transitions);
        assert_eq!(i22_row(&conn, 1), (0, Some(3)));
        assert_eq!(i22_row(&conn, 5), (0, None)); // never resurrected
        i22_assert_no_case_a(&conn);

        // A spendable-only flip (the row already named the subject) that the
        // monitor re-stamped in the window: the spent_by IS NULL guard
        // refuses (re-enabling would mint Case A).
        let conn = i22_db();
        conn.execute_batch("UPDATE outputs SET spent_by = 10 WHERE output_id = 1")
            .unwrap();
        let transitions = i22_mark(&conn);
        assert_eq!(
            transitions,
            vec![SpentInputTransition {
                output_id: 1,
                set_spent_by: None
            }]
        );
        conn.execute_batch("UPDATE outputs SET spent_by = 3 WHERE output_id = 1")
            .unwrap();
        i22_restore(&conn, &transitions);
        assert_eq!(i22_row(&conn, 1), (0, Some(3)));

        // Relinquished in the window — the basket_id guard refuses (a GONE
        // row stays GONE, abort_action G5 idiom).
        let conn = i22_db();
        let transitions = i22_mark(&conn);
        conn.execute_batch("UPDATE outputs SET basket_id = NULL WHERE output_id = 1")
            .unwrap();
        i22_restore(&conn, &transitions);
        assert_eq!(i22_row(&conn, 1), (0, Some(10)));
        i22_assert_no_case_a(&conn);
    }

    #[test]
    fn test_i22_double_restore_harmless() {
        // Restore twice: the second pass is a pure no-op (spent_by already
        // NULL fails the Some-arm guard; the None-arm re-write is
        // idempotent), and a lock acquired between restores survives.
        let conn = i22_db();
        let transitions = i22_mark(&conn);
        i22_restore(&conn, &transitions);
        let after_first: Vec<(i64, Option<i64>)> = (1..=6).map(|id| i22_row(&conn, id)).collect();
        i22_restore(&conn, &transitions);
        let after_second: Vec<(i64, Option<i64>)> = (1..=6).map(|id| i22_row(&conn, id)).collect();
        assert_eq!(after_first, after_second);

        // A createAction re-locked row 1 after the first restore — a further
        // restore must not release it.
        conn.execute_batch("UPDATE outputs SET spendable = 0, spent_by = 3 WHERE output_id = 1")
            .unwrap();
        i22_restore(&conn, &transitions);
        assert_eq!(i22_row(&conn, 1), (0, Some(3)));
        i22_assert_no_case_a(&conn);
    }

    #[test]
    fn test_i22_merge_remark_idempotent() {
        // The merge path re-marks (internalizeAction.ts:496-504): a second
        // pass finds every row already unspendable, produces NO transitions
        // and changes nothing — so a merge can never restore what it didn't
        // flip.
        let conn = i22_db();
        let first = i22_mark(&conn);
        assert_eq!(first.len(), 1);
        let state: Vec<(i64, Option<i64>)> = (1..=6).map(|id| i22_row(&conn, id)).collect();
        let second = i22_mark(&conn);
        assert!(second.is_empty());
        let state_after: Vec<(i64, Option<i64>)> = (1..=6).map(|id| i22_row(&conn, id)).collect();
        assert_eq!(state, state_after);
        i22_assert_no_case_a(&conn);
    }

    #[test]
    fn test_i22_mark_decision_matrix() {
        // (g-adjacent) The pure eligibility fn, all branches
        // (internalizeAction.ts:86-93).
        use InputMarkDecision::{Mark, Skip};
        // Already unspendable — idempotence.
        assert_eq!(input_mark_decision(false, None, Some(10)), Skip);
        assert_eq!(input_mark_decision(false, Some(3), Some(10)), Skip);
        // Competing spend — never overwritten (even when the row's user has
        // no record of the subject tx).
        assert_eq!(input_mark_decision(true, Some(3), Some(10)), Skip);
        assert_eq!(input_mark_decision(true, Some(3), None), Skip);
        // Already stamped by the subject tx — flip spendable only; restore
        // must not clear a stamp this call didn't write.
        assert_eq!(
            input_mark_decision(true, Some(10), Some(10)),
            Mark { set_spent_by: None }
        );
        // Fresh flips.
        assert_eq!(
            input_mark_decision(true, None, Some(10)),
            Mark {
                set_spent_by: Some(10)
            }
        );
        assert_eq!(
            input_mark_decision(true, None, None),
            Mark { set_spent_by: None }
        );
    }

    // =========================================================================
    // One row per payment: real-SQLite harness (the #22 pattern above). The
    // ACTUAL migrations and the ACTUAL SQL consts; a call's deciding
    // statements run one at a time, as D1 runs them, so a test places the
    // statements of two calls in an order it names, or in every order.
    // =========================================================================

    /// The subject transaction of these fixtures and what each output pays.
    const ONE_TXID: &str = "ee01";
    const ONE_SATS: i64 = 1000;
    /// The internalizing user and their default basket.
    const ONE_USER: i64 = 1;
    const ONE_BASKET: i64 = 10;

    fn one_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for migration in [
            include_str!("../../migrations/0001_initial.sql"),
            include_str!("../../migrations/0002_add_indexes.sql"),
            include_str!("../../migrations/0003_add_output_reservations.sql"),
            include_str!("../../migrations/0004_proven_txs_height_pair_index.sql"),
        ] {
            conn.execute_batch(migration).unwrap();
        }
        // D1 enforces foreign keys.
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            INSERT INTO users (user_id, identity_key) VALUES (1, '02aa'), (2, '02bb');
            INSERT INTO output_baskets (basket_id, user_id, name)
            VALUES (10, 1, 'default'), (20, 2, 'default');
            "#,
        )
        .unwrap();
        conn
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum OneStep {
        /// Step 3: the read that decides "new" or "merge".
        ReadTx,
        /// Step 7: the transactions row.
        InsertTx,
        /// Step 5: a merge reads the outputs that stand.
        ReadOutputs,
        /// Step 9: the outputs row.
        InsertOutput,
        /// Step 9, after an insert that wrote nothing: the row that stands.
        ReadStanding,
        Done,
    }

    /// One "wallet payment" call for output `vout` of ONE_TXID by ONE_USER:
    /// the native mirror of internalize_subject's deciding statements, with
    /// the code's own decision on what each returns. `step` runs exactly one
    /// statement.
    #[derive(Clone, Debug)]
    struct OneCall {
        vout: i64,
        /// The output indexes the call's `outputs` name, in their order.
        named: Vec<u32>,
        /// The refusal of the specifications (step 4), as the wire gives it:
        /// the call's answer, and its end.
        refusal: Option<(i32, String)>,
        reference: &'static str,
        next: OneStep,
        again: bool,
        transaction_id: i64,
        /// The answer's `isMerge` and `satoshis`.
        is_merge: bool,
        satoshis: i64,
        /// An insert of this call wrote nothing because a row stood.
        tx_stood: bool,
        output_stood: bool,
    }

    impl OneCall {
        fn new(reference: &'static str, vout: i64) -> Self {
            Self::naming(reference, &[vout as u32])
        }

        /// A call whose `outputs` name `named`; the output it goes on to
        /// write is the first of them.
        fn naming(reference: &'static str, named: &[u32]) -> Self {
            OneCall {
                vout: named[0] as i64,
                named: named.to_vec(),
                refusal: None,
                reference,
                next: OneStep::ReadTx,
                again: false,
                transaction_id: 0,
                is_merge: false,
                satoshis: 0,
                tx_stood: false,
                output_stood: false,
            }
        }

        fn done(&self) -> bool {
            self.next == OneStep::Done
        }

        /// The row that stands for this call's output index, as step 5 reads
        /// it: (basket_id, change).
        fn standing(&self, conn: &rusqlite::Connection) -> Option<(Option<i64>, bool)> {
            let mut stmt = conn.prepare(INTERNALIZE_EXISTING_OUTPUTS_SQL).unwrap();
            let rows = stmt
                .query_map(rusqlite::params![ONE_USER, ONE_TXID], |r| {
                    Ok((
                        r.get::<_, i64>("vout")?,
                        r.get::<_, Option<i64>>("basket_id")?,
                        r.get::<_, i64>("change")? != 0,
                    ))
                })
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            rows.into_iter()
                .find(|row| row.0 == self.vout)
                .map(|row| (row.1, row.2))
        }

        fn step(&mut self, conn: &rusqlite::Connection) {
            use rusqlite::OptionalExtension;
            match self.next {
                OneStep::ReadTx => {
                    let row: Option<(i64, String)> = conn
                        .query_row(
                            INTERNALIZE_FIND_TX_SQL,
                            rusqlite::params![ONE_USER, ONE_TXID],
                            |r| Ok((r.get("transaction_id")?, r.get("status")?)),
                        )
                        .optional()
                        .unwrap();
                    match row {
                        Some((transaction_id, status)) => {
                            validate_merge_status(&TransactionStatus::parse_status(&status))
                                .unwrap();
                            self.transaction_id = transaction_id;
                            self.is_merge = true;
                            self.next = OneStep::ReadOutputs;
                        }
                        None => self.next = OneStep::InsertTx,
                    }
                    // Step 4, between the read and the first write: the
                    // specifications, no statement.
                    if let Err(refusal) = require_unique_output_indexes(self.named.iter().copied())
                    {
                        self.refusal = Some(crate::door::errors::public_parts(&refusal));
                        self.next = OneStep::Done;
                    }
                }
                OneStep::InsertTx => {
                    let changes = conn
                        .execute(
                            INTERNALIZE_INSERT_TX_SQL,
                            rusqlite::params![
                                ONE_USER,
                                ONE_TXID,
                                "unproven",
                                self.reference,
                                "payment",
                                ONE_SATS,
                                1i64,
                                0i64,
                                "t-insert",
                                "t-insert",
                                ONE_USER,
                                ONE_TXID
                            ],
                        )
                        .unwrap();
                    if changes == 0 {
                        // A row stands: once more from the read, as the merge.
                        assert!(!self.again, "a second pass read no row and inserted none");
                        self.again = true;
                        self.tx_stood = true;
                        self.next = OneStep::ReadTx;
                    } else {
                        self.transaction_id = conn.last_insert_rowid();
                        self.satoshis = ONE_SATS;
                        self.next = OneStep::InsertOutput;
                    }
                }
                OneStep::ReadOutputs => match self.standing(conn) {
                    // Change in the default basket already: nothing to write
                    // and nothing to count.
                    Some((Some(ONE_BASKET), true)) => {
                        self.satoshis = 0;
                        self.next = OneStep::Done;
                    }
                    Some(other) => panic!("no such row in these fixtures: {other:?}"),
                    None => {
                        self.satoshis = ONE_SATS;
                        self.next = OneStep::InsertOutput;
                    }
                },
                OneStep::InsertOutput => {
                    let changes = conn
                        .execute(
                            INTERNALIZE_INSERT_PAYMENT_OUTPUT_SQL,
                            rusqlite::params![
                                ONE_USER,
                                self.transaction_id,
                                ONE_BASKET,
                                ONE_TXID,
                                self.vout,
                                ONE_SATS,
                                25i64,
                                "prefix",
                                "suffix",
                                "02cc",
                                "t-insert",
                                "t-insert"
                            ],
                        )
                        .unwrap();
                    if changes == 0 {
                        self.output_stood = true;
                        self.next = OneStep::ReadStanding;
                    } else {
                        self.next = OneStep::Done;
                    }
                }
                OneStep::ReadStanding => match self.standing(conn) {
                    // Merged as a row step 5 had read: nothing to write, and
                    // what step 6 counted for it comes off.
                    Some((Some(ONE_BASKET), true)) => {
                        self.satoshis -= ONE_SATS;
                        self.next = OneStep::Done;
                    }
                    other => panic!("the insert wrote nothing and the read gave {other:?}"),
                },
                OneStep::Done => panic!("the call is over"),
            }
        }

        /// The whole call, alone: the statements it ran.
        fn run(&mut self, conn: &rusqlite::Connection) -> Vec<OneStep> {
            let mut ran = Vec::new();
            while !self.done() {
                ran.push(self.next);
                self.step(conn);
            }
            ran
        }
    }

    /// For ONE_USER and ONE_TXID: the transactions rows, and at `vout` the
    /// outputs rows and the spendable ones.
    fn one_rows(conn: &rusqlite::Connection, vout: i64) -> (i64, i64, i64) {
        let txs = conn
            .query_row(
                "SELECT COUNT(*) FROM transactions WHERE user_id = ? AND txid = ?",
                rusqlite::params![ONE_USER, ONE_TXID],
                |r| r.get(0),
            )
            .unwrap();
        let (outputs, spendable) = conn
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(spendable), 0) FROM outputs \
                 WHERE user_id = ? AND txid = ? AND vout = ?",
                rusqlite::params![ONE_USER, ONE_TXID, vout],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        (txs, outputs, spendable)
    }

    /// ONE_USER's balance as getBalance sums it (readers.rs): the spendable
    /// outputs of the transactions that count.
    fn one_balance(conn: &rusqlite::Connection) -> i64 {
        conn.query_row(
            "SELECT COALESCE(SUM(o.satoshis), 0) FROM outputs o \
             JOIN transactions t ON o.transaction_id = t.transaction_id \
             WHERE o.user_id = ? AND o.spendable = 1 \
               AND t.status IN ('completed', 'unproven', 'sending')",
            [ONE_USER],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Every order of the two calls' statements, each from the same start;
    /// `end` sees the database and the two calls when both are over. Returns
    /// the number of orders.
    fn one_every_order(
        conn: &rusqlite::Connection,
        a: &OneCall,
        b: &OneCall,
        end: &mut dyn FnMut(&rusqlite::Connection, &OneCall, &OneCall),
    ) -> usize {
        if a.done() && b.done() {
            end(conn, a, b);
            return 1;
        }
        let mut orders = 0;
        for a_moves in [true, false] {
            let (mut a, mut b) = (a.clone(), b.clone());
            let mover = if a_moves { &mut a } else { &mut b };
            if mover.done() {
                continue;
            }
            conn.execute_batch("SAVEPOINT one_order").unwrap();
            mover.step(conn);
            orders += one_every_order(conn, &a, &b, end);
            conn.execute_batch("ROLLBACK TO one_order; RELEASE one_order")
                .unwrap();
        }
        orders
    }

    #[test]
    fn test_one_row_reads_first_then_inserts() {
        // Two calls for one payment, their statements in an order named
        // here: both step-3 reads, then both inserts of the transactions
        // row; the second call then goes on to the insert of its output,
        // the first call runs to its end, and the second ends.
        let conn = one_db();
        let (mut a, mut b) = (OneCall::new("ref-a", 0), OneCall::new("ref-b", 0));
        a.step(&conn);
        b.step(&conn);
        assert_eq!((a.next, b.next), (OneStep::InsertTx, OneStep::InsertTx));
        a.step(&conn);
        b.step(&conn);
        while !matches!(b.next, OneStep::InsertOutput | OneStep::Done) {
            b.step(&conn);
        }
        a.run(&conn);
        b.run(&conn);

        // One transactions row, one spendable outputs row, the balance of
        // one payment; both calls are answered, one as new and one as the
        // merge, and their satoshis sum to the payment.
        assert_eq!(one_rows(&conn, 0), (1, 1, 1));
        assert_eq!(one_balance(&conn), ONE_SATS);
        assert!(a.done() && b.done());
        assert_eq!((a.is_merge, b.is_merge), (false, true));
        assert_eq!(a.satoshis + b.satoshis, ONE_SATS);
        // The way there: the second call's insert of the transactions row
        // wrote nothing and it read the first call's row; its insert of the
        // output wrote nothing too, and it counted nothing.
        assert!(b.tx_stood && !a.tx_stood);
        assert_eq!(b.transaction_id, a.transaction_id);
        assert!(b.output_stood && !a.output_stood);
        assert_eq!((a.satoshis, b.satoshis), (ONE_SATS, 0));
    }

    #[test]
    fn test_one_row_same_output_every_order() {
        // Two calls for one payment, in every order of their statements: one
        // transactions row, one spendable outputs row, the balance of one
        // payment; one call answers as new and one as a merge, and the
        // satoshis of the two answers sum to the payment.
        let conn = one_db();
        let (mut tx_stood, mut output_stood) = (0, 0);
        let orders = one_every_order(
            &conn,
            &OneCall::new("ref-a", 0),
            &OneCall::new("ref-b", 0),
            &mut |conn, a, b| {
                assert_eq!(one_rows(conn, 0), (1, 1, 1), "{a:?} {b:?}");
                assert_eq!(one_balance(conn), ONE_SATS, "{a:?} {b:?}");
                assert!(a.is_merge != b.is_merge, "{a:?} {b:?}");
                assert_eq!(a.satoshis + b.satoshis, ONE_SATS, "{a:?} {b:?}");
                tx_stood += usize::from(a.tx_stood || b.tx_stood);
                output_stood += usize::from(a.output_stood || b.output_stood);
            },
        );
        // The start is as it was, and the orders include those in which an
        // insert met a row that stood.
        assert_eq!(one_rows(&conn, 0), (0, 0, 0));
        println!("same output: {orders} orders, {tx_stood} with a transactions insert that wrote nothing, {output_stood} with an outputs insert that wrote nothing");
        assert!(orders > 20 && tx_stood > 0 && output_stood > 0);
    }

    #[test]
    fn test_one_row_other_output_index_every_order() {
        // The second call names another output of the same transaction: one
        // transactions row, each output once, the balance of the two.
        let conn = one_db();
        let mut tx_stood = 0;
        let orders = one_every_order(
            &conn,
            &OneCall::new("ref-a", 0),
            &OneCall::new("ref-b", 1),
            &mut |conn, a, b| {
                assert_eq!(one_rows(conn, 0), (1, 1, 1), "{a:?} {b:?}");
                assert_eq!(one_rows(conn, 1), (1, 1, 1), "{a:?} {b:?}");
                assert_eq!(one_balance(conn), 2 * ONE_SATS, "{a:?} {b:?}");
                assert!(a.is_merge != b.is_merge, "{a:?} {b:?}");
                assert_eq!((a.satoshis, b.satoshis), (ONE_SATS, ONE_SATS));
                assert!(!a.output_stood && !b.output_stood);
                tx_stood += usize::from(a.tx_stood || b.tx_stood);
            },
        );
        println!("other output index: {orders} orders, {tx_stood} with a transactions insert that wrote nothing");
        assert!(orders > 20 && tx_stood > 0);
    }

    #[test]
    fn test_one_row_one_call_after_another() {
        // A call alone runs the read, the insert and the output's insert,
        // and nothing more; the same call again is a merge that writes no
        // row; a later call for another output inserts that output alone.
        let conn = one_db();
        let mut first = OneCall::new("ref-a", 0);
        assert_eq!(
            first.run(&conn),
            [OneStep::ReadTx, OneStep::InsertTx, OneStep::InsertOutput]
        );
        assert_eq!((first.is_merge, first.satoshis), (false, ONE_SATS));

        let mut again = OneCall::new("ref-b", 0);
        assert_eq!(again.run(&conn), [OneStep::ReadTx, OneStep::ReadOutputs]);
        assert_eq!((again.is_merge, again.satoshis), (true, 0));
        assert_eq!(one_rows(&conn, 0), (1, 1, 1));
        assert_eq!(one_balance(&conn), ONE_SATS);

        let mut other = OneCall::new("ref-c", 1);
        assert_eq!(
            other.run(&conn),
            [OneStep::ReadTx, OneStep::ReadOutputs, OneStep::InsertOutput]
        );
        assert_eq!((other.is_merge, other.satoshis), (true, ONE_SATS));
        assert_eq!(one_rows(&conn, 1), (1, 1, 1));
        assert_eq!(one_balance(&conn), 2 * ONE_SATS);
        assert!(!first.tx_stood && !again.tx_stood && !other.tx_stood);
        assert!(!first.output_stood && !again.output_stood && !other.output_stood);
    }

    /// The wire answer of a call naming an output index more than once.
    fn one_index_refusal() -> Option<(i32, String)> {
        Some((
            crate::json_rpc::INVALID_PARAMS,
            "The outputs parameter must be unique outputIndex values".to_string(),
        ))
    }

    #[test]
    fn test_one_index_once_the_refusal_and_its_wire_answer() {
        // An index named more than once, wherever it stands among the
        // outputs, is the reference's refusal on the wire; indexes named
        // once pass.
        for named in [vec![0, 0], vec![1, 0, 1], vec![0, 1, 2, 2], vec![7, 7, 7]] {
            let refusal = require_unique_output_indexes(named.iter().copied()).unwrap_err();
            assert!(matches!(refusal, Error::ValidationError(_)), "{named:?}");
            assert_eq!(
                Some(crate::door::errors::public_parts(&refusal)),
                one_index_refusal(),
                "{named:?}"
            );
        }
        for named in [vec![], vec![0], vec![0, 1], vec![2, 0, 1]] {
            assert!(
                require_unique_output_indexes(named.iter().copied()).is_ok(),
                "{named:?}"
            );
        }
    }

    #[test]
    fn test_one_index_once_refused_before_the_first_write() {
        // A call naming output 0 twice, for a transaction the storage does
        // not hold: it runs the step-3 read and is refused; no statement
        // follows and no row is written.
        let conn = one_db();
        let written = conn.total_changes();
        let mut new = OneCall::naming("ref-a", &[0, 0]);
        assert_eq!(new.run(&conn), [OneStep::ReadTx]);
        assert_eq!(new.refusal, one_index_refusal());
        assert_eq!(conn.total_changes(), written);
        assert_eq!(one_rows(&conn, 0), (0, 0, 0));

        // The transaction held (its output 0, by a call alone): a call
        // naming output 1 twice, and one naming the held output 0 twice,
        // are refused the same, the rows as they were.
        let mut first = OneCall::new("ref-b", 0);
        first.run(&conn);
        assert_eq!((first.refusal.clone(), first.satoshis), (None, ONE_SATS));
        let written = conn.total_changes();
        for named in [[1, 1], [0, 0]] {
            let mut held = OneCall::naming("ref-c", &named);
            assert_eq!(held.run(&conn), [OneStep::ReadTx], "{named:?}");
            assert_eq!(held.refusal, one_index_refusal(), "{named:?}");
        }
        assert_eq!(conn.total_changes(), written);
        assert_eq!(one_rows(&conn, 0), (1, 1, 1));
        assert_eq!(one_rows(&conn, 1), (1, 0, 0));
        assert_eq!(one_balance(&conn), ONE_SATS);

        // Output 1 named once is then the merge that inserts it.
        let mut once = OneCall::new("ref-d", 1);
        assert_eq!(
            once.run(&conn),
            [OneStep::ReadTx, OneStep::ReadOutputs, OneStep::InsertOutput]
        );
        assert_eq!(
            (once.refusal.clone(), once.is_merge, once.satoshis),
            (None, true, ONE_SATS)
        );
        assert!(!once.output_stood);
    }

    #[test]
    fn test_one_index_once_a_refused_call_beside_another_every_order() {
        // A call naming output 0 twice beside a call naming it once, in
        // every order of their statements: the first is refused and writes
        // nothing, the second is answered as a call alone is.
        let conn = one_db();
        let orders = one_every_order(
            &conn,
            &OneCall::naming("ref-a", &[0, 0]),
            &OneCall::new("ref-b", 0),
            &mut |conn, a, b| {
                assert_eq!(a.refusal, one_index_refusal(), "{a:?} {b:?}");
                assert_eq!(b.refusal, None, "{a:?} {b:?}");
                assert_eq!((b.is_merge, b.satoshis), (false, ONE_SATS), "{b:?}");
                assert!(!b.tx_stood && !b.output_stood, "{b:?}");
                assert_eq!(one_rows(conn, 0), (1, 1, 1), "{a:?} {b:?}");
                assert_eq!(one_balance(conn), ONE_SATS, "{a:?} {b:?}");
            },
        );
        assert_eq!(orders, 4);
    }

    #[test]
    fn test_one_row_insert_carries_the_reads_predicate() {
        // The insert's condition is the read's predicate, word for word.
        let predicate = INTERNALIZE_FIND_TX_SQL.rsplit_once(" WHERE ").unwrap().1;
        assert_eq!(predicate, "user_id = ? AND txid = ?");
        assert!(INTERNALIZE_INSERT_TX_SQL.ends_with(&format!(
            "WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE {predicate})"
        )));

        // It holds one row to a user and a txid: another user and another
        // txid are not held back.
        let conn = one_db();
        let insert = |user: i64, txid: &str, reference: &str| {
            conn.execute(
                INTERNALIZE_INSERT_TX_SQL,
                rusqlite::params![
                    user, txid, "unproven", reference, "payment", ONE_SATS, 1i64, 0i64, "t", "t",
                    user, txid
                ],
            )
            .unwrap()
        };
        assert_eq!(insert(1, ONE_TXID, "ref-1"), 1);
        assert_eq!(insert(1, ONE_TXID, "ref-2"), 0);
        assert_eq!(insert(2, ONE_TXID, "ref-3"), 1);
        assert_eq!(insert(1, "ee02", "ref-4"), 1);
        assert_eq!(one_rows(&conn, 0).0, 1);

        // The condition is this statement's alone: the schema is as it was,
        // and takes the rows other writers insert for a txid they repeat.
        for reference in ["sent-1", "sent-2"] {
            conn.execute(
                "INSERT INTO transactions (user_id, txid, status, reference, is_outgoing, description) \
                 VALUES (1, 'ee03', 'failed', ?, 1, 'outgoing')",
                [reference],
            )
            .unwrap();
        }
    }

    #[test]
    fn test_one_row_output_inserts_hold_the_schema_key() {
        // One outputs row to (transaction_id, vout, user_id), whichever
        // protocol's insert comes second; another index is free.
        let conn = one_db();
        let mut call = OneCall::new("ref-a", 0);
        call.run(&conn);
        let basket_insert = |vout: i64| {
            conn.execute(
                INTERNALIZE_INSERT_BASKET_OUTPUT_SQL,
                rusqlite::params![
                    ONE_USER,
                    call.transaction_id,
                    ONE_BASKET,
                    ONE_TXID,
                    vout,
                    ONE_SATS,
                    1i64,
                    Option::<&str>::None,
                    "t",
                    "t"
                ],
            )
            .unwrap()
        };
        assert_eq!(basket_insert(0), 0);
        assert_eq!(basket_insert(1), 1);
        assert_eq!(basket_insert(1), 0);
        let mut payment = OneCall::new("ref-b", 1);
        payment.transaction_id = call.transaction_id;
        payment.next = OneStep::InsertOutput;
        payment.step(&conn);
        assert!(payment.output_stood);
        assert_eq!(one_rows(&conn, 0), (1, 1, 1));
        assert_eq!(one_rows(&conn, 1), (1, 1, 1));

        // A row the key does not cover is still refused: an insert naming a
        // transaction that does not exist is a fault, not a row that stands.
        let orphan = conn.execute(
            INTERNALIZE_INSERT_PAYMENT_OUTPUT_SQL,
            rusqlite::params![
                ONE_USER, 999i64, ONE_BASKET, ONE_TXID, 0i64, ONE_SATS, 25i64, "p", "s", "02cc",
                "t", "t"
            ],
        );
        assert!(orphan.is_err());
    }

    #[test]
    fn test_one_row_proof_request_insert() {
        // One proof request to a txid: the second insert writes nothing and
        // raises nothing, and the request that stands is as it was.
        let conn = one_db();
        let insert = |status: &str| {
            conn.execute(
                INTERNALIZE_INSERT_REQ_SQL,
                rusqlite::params![ONE_TXID, status, vec![1u8], "t", "t"],
            )
            .unwrap()
        };
        assert_eq!(insert("unmined"), 1);
        assert_eq!(insert("unsent"), 0);
        let (rows, status): (i64, String) = conn
            .query_row(
                "SELECT COUNT(*), MAX(status) FROM proven_tx_reqs WHERE txid = ?",
                [ONE_TXID],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((rows, status.as_str()), (1, "unmined"));
    }

    // =========================================================================
    // A coin's row is marked from a checked proof of its spender; a call
    // takes back what it wrote unless its transaction is taken. The
    // statements one internalize call runs, over the real migrations, in the
    // order it runs them, for each outcome of its post.
    // =========================================================================

    /// The other user (B), who holds the coin, and the caller (A).
    const MK_OWNER: i64 = 1;
    const MK_CALLER: i64 = 2;
    const MK_OWNER_BASKET: i64 = 10;
    const MK_CALLER_BASKET: i64 = 20;

    /// One transaction's bytes: one input spending `prev` (display hex) at
    /// `vout` with an empty unlocking script, one output of 1,000 sats, and
    /// `marker` as its lock time. Returns (raw hex, txid display hex).
    fn mk_raw(prev: &str, vout: u32, marker: u32) -> (String, String) {
        let mut prev_le = hex::decode(prev).unwrap();
        prev_le.reverse();
        let mut b = Vec::new();
        b.extend_from_slice(&1u32.to_le_bytes());
        b.push(1);
        b.extend_from_slice(&prev_le);
        b.extend_from_slice(&vout.to_le_bytes());
        b.push(0);
        b.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        b.push(1);
        b.extend_from_slice(&1000u64.to_le_bytes());
        b.extend_from_slice(&[1, 0x51]);
        b.extend_from_slice(&marker.to_le_bytes());
        let mut id = bsv_sdk::sha256d(&b).to_vec();
        id.reverse();
        (hex::encode(&b), hex::encode(id))
    }

    fn mk_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            include_str!("../../migrations/0001_initial.sql"),
            include_str!("../../migrations/0002_add_indexes.sql"),
            include_str!("../../migrations/0003_add_output_reservations.sql"),
            include_str!("../../migrations/0004_proven_txs_height_pair_index.sql"),
        ] {
            conn.execute_batch(sql).unwrap();
        }
        conn.execute_batch(&format!(
            "INSERT INTO users (user_id, identity_key) VALUES ({MK_OWNER}, '02b0'), ({MK_CALLER}, '02a0');
             INSERT INTO output_baskets (basket_id, user_id, name)
             VALUES ({MK_OWNER_BASKET}, {MK_OWNER}, 'default'), ({MK_CALLER_BASKET}, {MK_CALLER}, 'default');"
        ))
        .unwrap();
        conn
    }

    /// A user's balance as getBalance sums it (readers.rs,
    /// BALANCE_OUTPUT_STATUSES, every basket).
    fn mk_balance(conn: &rusqlite::Connection, user: i64) -> i64 {
        conn.query_row(
            "SELECT COALESCE(SUM(o.satoshis), 0) FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id
             WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ('completed', 'unproven', 'sending')",
            [user],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Every row of `user`, as a stable text, to compare before and after.
    fn mk_rows(conn: &rusqlite::Connection, user: i64) -> Vec<String> {
        let mut out = Vec::new();
        let mut stmt = conn
            .prepare("SELECT transaction_id, status, proven_tx_id FROM transactions WHERE user_id = ? ORDER BY transaction_id")
            .unwrap();
        for r in stmt
            .query_map([user], |r| {
                Ok(format!(
                    "tx {} {} {:?}",
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<i64>>(2)?
                ))
            })
            .unwrap()
        {
            out.push(r.unwrap());
        }
        let mut stmt = conn
            .prepare("SELECT output_id, spendable, spent_by, satoshis FROM outputs WHERE user_id = ? ORDER BY output_id")
            .unwrap();
        for r in stmt
            .query_map([user], |r| {
                Ok(format!(
                    "out {} {} {:?} {}",
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                    r.get::<_, i64>(3)?
                ))
            })
            .unwrap()
        {
            out.push(r.unwrap());
        }
        out
    }

    /// The owner's coin: a confirmed parent (`completed`) of 50,000 sats at
    /// output 0, in the owner's default basket. Returns its output_id.
    fn mk_owner_coin(conn: &rusqlite::Connection, parent: &str) -> i64 {
        conn.execute(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, description, txid)
             VALUES (?, 'completed', 'ref-parent', 0, 'parent', ?)",
            rusqlite::params![MK_OWNER, parent],
        )
        .unwrap();
        let tx = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis,
                                  provided_by, purpose, type, txid)
             VALUES (?, ?, ?, 1, 1, 0, 50000, 'storage', 'change', 'P2PKH', ?)",
            rusqlite::params![MK_OWNER, tx, MK_OWNER_BASKET, parent],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// What the post left, as the call reads it.
    #[derive(Clone, Copy, Debug)]
    enum MkPost {
        /// Taken.
        Sent,
        /// Double-spend or invalid, the second source answering "not known".
        Rejected,
        /// A policy refusal (460 to 475 but 466).
        PolicyHeld,
        /// No verdict, under INTERNALIZE_ZERO_CONF = true.
        TransientZeroConf,
        /// No verdict, under INTERNALIZE_ZERO_CONF = false.
        Transient,
    }

    /// What `create_proven_tx_req` makes of a broadcaster's refusal
    /// (double-spend or invalid) given the second source's answer.
    fn mk_post_after_refusal(
        answer: std::result::Result<Vec<crate::services::TxStatusDetail>, String>,
        zero_conf: bool,
    ) -> MkPost {
        let _ = zero_conf; // a refusal held is demoted under either setting
        match network_word(answer) {
            NetworkWord::Knows => MkPost::Sent,
            NetworkWord::DoesNotKnow => MkPost::Rejected,
            NetworkWord::Unavailable => MkPost::Transient,
        }
    }

    /// The caller's internalizeAction of `subject` (spending `outpoints`),
    /// as a wallet payment of its output 0, statement after statement as
    /// `internalize_subject` runs them (step 7, 7b, 9, 10), for the outcome
    /// `post`. Returns the caller's transactions row.
    fn mk_internalize(
        conn: &rusqlite::Connection,
        subject: &str,
        raw_hex: &str,
        outpoints: &[(&str, i64)],
        post: MkPost,
    ) -> i64 {
        // Step 7.
        let changes = conn
            .execute(
                INTERNALIZE_INSERT_TX_SQL,
                rusqlite::params![
                    MK_CALLER,
                    subject,
                    "unproven",
                    "ref-subject",
                    "pay",
                    1000,
                    1,
                    0,
                    "t7",
                    "t7",
                    MK_CALLER,
                    subject
                ],
            )
            .unwrap();
        assert_eq!(changes, 1);
        let tx = conn.last_insert_rowid();
        // Step 7b.
        let transitions = mark_inputs_for(conn, MK_CALLER, subject, outpoints);
        // Step 9.
        conn.execute(
            INTERNALIZE_INSERT_PAYMENT_OUTPUT_SQL,
            rusqlite::params![
                MK_CALLER,
                tx,
                MK_CALLER_BASKET,
                subject,
                0,
                1000,
                25,
                "p",
                "s",
                "02cc",
                "t9",
                "t9"
            ],
        )
        .unwrap();
        // Step 10.
        if !matches!(post, MkPost::Sent) {
            i22_restore(conn, &transitions);
        }
        match post {
            MkPost::Sent | MkPost::TransientZeroConf => {}
            MkPost::Rejected => {
                conn.execute(
                    COMPENSATE_FAIL_TX_SQL,
                    rusqlite::params!["tc", tx, MK_CALLER],
                )
                .unwrap();
                conn.execute(
                    COMPENSATE_ZERO_OUTPUTS_SQL,
                    rusqlite::params!["tc", tx, MK_CALLER],
                )
                .unwrap();
                conn.execute(
                    COMPENSATE_REQ_SQL,
                    rusqlite::params![subject, hex::decode(raw_hex).unwrap(), "tc", "tc", subject],
                )
                .unwrap();
            }
            MkPost::PolicyHeld | MkPost::Transient => {
                conn.execute(
                    "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND user_id = ?",
                    rusqlite::params!["td", tx, MK_CALLER],
                )
                .unwrap();
                if matches!(post, MkPost::PolicyHeld) {
                    conn.execute(
                        "UPDATE transactions SET status = 'sending', updated_at = ? WHERE transaction_id = ? AND status = 'unproven'",
                        rusqlite::params!["td", tx],
                    )
                    .unwrap();
                }
            }
        }
        tx
    }

    /// The store of the subject's checked proof (monitor::store_proof_result's
    /// batch): the subject's rows completed, and every user's coin at an
    /// outpoint the stored bytes spend marked.
    fn mk_store_proof(conn: &rusqlite::Connection, subject: &str, raw_hex: &str) {
        conn.execute(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx)
             VALUES (?, 900000, 0, 'bh', 'root', X'00', ?)",
            rusqlite::params![subject, hex::decode(raw_hex).unwrap()],
        )
        .unwrap();
        let proven = conn.last_insert_rowid();
        conn.execute(
            "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
            rusqlite::params![proven, "tp", subject],
        )
        .unwrap();
        for (prev_txid, prev_vout) in
            crate::monitor::outpoints_spent_by(&Some(raw_hex.to_string()), subject)
        {
            conn.execute(
                crate::monitor::PROOF_MARK_SPENT_INPUT_SQL,
                rusqlite::params![subject, "tp", prev_txid, prev_vout as i64, subject],
            )
            .unwrap();
        }
    }

    #[test]
    fn test_marks_another_users_coin_is_left_on_every_outcome() {
        // The caller names the owner's coin as an input
        // of its subject; the call writes nothing of the owner's on any
        // outcome of its post, and its own transitions are taken back on
        // every outcome but a take.
        for post in [
            MkPost::Rejected,
            MkPost::PolicyHeld,
            MkPost::TransientZeroConf,
            MkPost::Transient,
            MkPost::Sent,
        ] {
            let conn = mk_db();
            let parent = "aa".repeat(32);
            let coin = mk_owner_coin(&conn, &parent);
            let before = mk_rows(&conn, MK_OWNER);
            assert_eq!(mk_balance(&conn, MK_OWNER), 50000);
            let (raw, subject) = mk_raw(&parent, 0, 1);
            mk_internalize(&conn, &subject, &raw, &[(parent.as_str(), 0)], post);
            assert_eq!(
                mk_rows(&conn, MK_OWNER),
                before,
                "{post:?}: the owner's rows moved"
            );
            assert_eq!(
                mk_balance(&conn, MK_OWNER),
                50000,
                "{post:?}: the owner's balance"
            );
            assert_eq!(i22_row(&conn, coin), (1, None), "{post:?}");
        }
    }

    #[test]
    fn test_marks_the_callers_own_coin_is_restored_unless_taken() {
        // The caller's own coin at the subject's input: marked by the call,
        // given back on every outcome but a take.
        for (post, end) in [
            (MkPost::Rejected, (1, None)),
            (MkPost::PolicyHeld, (1, None)),
            (MkPost::TransientZeroConf, (1, None)),
            (MkPost::Transient, (1, None)),
            (MkPost::Sent, (0, Some(2))),
        ] {
            let conn = mk_db();
            let parent = "ab".repeat(32);
            conn.execute(
                "INSERT INTO transactions (transaction_id, user_id, status, reference, is_outgoing, description, txid)
                 VALUES (1, ?, 'completed', 'ref-own', 0, 'own parent', ?)",
                rusqlite::params![MK_CALLER, parent],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, spendable, change, vout, satoshis,
                                      provided_by, purpose, type, txid)
                 VALUES (500, ?, 1, ?, 1, 1, 0, 7000, 'storage', 'change', 'P2PKH', ?)",
                rusqlite::params![MK_CALLER, MK_CALLER_BASKET, parent],
            )
            .unwrap();
            let (raw, subject) = mk_raw(&parent, 0, 2);
            let tx = mk_internalize(&conn, &subject, &raw, &[(parent.as_str(), 0)], post);
            assert_eq!(tx, 2);
            assert_eq!(i22_row(&conn, 500), end, "{post:?}");
            i22_assert_no_case_a(&conn);
        }
    }

    #[test]
    fn test_marks_a_real_spend_is_marked_at_its_proof() {
        // The legitimate spend of the owner's coin: the call leaves the
        // owner's row; the store of the subject's checked proof marks it,
        // from the stored bytes that hash to the proven txid.
        let conn = mk_db();
        let parent = "ac".repeat(32);
        let coin = mk_owner_coin(&conn, &parent);
        let (raw, subject) = mk_raw(&parent, 0, 3);
        mk_internalize(&conn, &subject, &raw, &[(parent.as_str(), 0)], MkPost::Sent);
        assert_eq!(i22_row(&conn, coin), (1, None));
        mk_store_proof(&conn, &subject, &raw);
        assert_eq!(i22_row(&conn, coin), (0, None));
        assert_eq!(mk_balance(&conn, MK_OWNER), 0);
        i22_assert_no_case_a(&conn);
    }

    #[test]
    fn test_marks_the_proof_marks_only_from_bytes_that_hash_to_its_txid() {
        let parent = "ad".repeat(32);
        let (raw, subject) = mk_raw(&parent, 0, 4);
        assert_eq!(
            crate::monitor::outpoints_spent_by(&Some(raw.clone()), &subject),
            vec![(parent.clone(), 0)]
        );
        // Bytes of another transaction, or none: nothing to mark.
        let (other, _) = mk_raw(&parent, 0, 5);
        assert!(crate::monitor::outpoints_spent_by(&Some(other), &subject).is_empty());
        assert!(crate::monitor::outpoints_spent_by(&None, &subject).is_empty());
        // A proof store whose bytes are not the subject's marks no coin.
        let conn = mk_db();
        let coin = mk_owner_coin(&conn, &parent);
        let (wrong, _) = mk_raw(&parent, 0, 6);
        mk_store_proof(&conn, &subject, &wrong);
        assert_eq!(i22_row(&conn, coin), (1, None));
    }

    #[test]
    fn test_marks_the_proof_never_overwrites_a_competing_spend() {
        // The owner's coin locked by the owner's own payment (spent_by set):
        // a proof of another transaction spending the outpoint leaves it.
        let conn = mk_db();
        let parent = "ae".repeat(32);
        let coin = mk_owner_coin(&conn, &parent);
        conn.execute(
            "INSERT INTO transactions (transaction_id, user_id, status, reference, is_outgoing, description, txid)
             VALUES (77, ?, 'completed', 'ref-own-pay', 1, 'own payment', 'ff01')",
            [MK_OWNER],
        )
        .unwrap();
        conn.execute(
            "UPDATE outputs SET spendable = 0, spent_by = 77 WHERE output_id = ?",
            [coin],
        )
        .unwrap();
        let (raw, subject) = mk_raw(&parent, 0, 7);
        mk_store_proof(&conn, &subject, &raw);
        assert_eq!(i22_row(&conn, coin), (0, Some(77)));
    }

    #[test]
    fn test_marks_a_proven_row_of_another_user_is_left() {
        // The owner's own mined payment T, `completed` with a
        // checked proof and its request purged; the caller internalizes T's
        // bytes; the broadcaster refuses it.
        let setup = || {
            let conn = mk_db();
            let (raw, t) = mk_raw(&"af".repeat(32), 0, 8);
            conn.execute(
                "INSERT INTO proven_txs (proven_tx_id, txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx)
                 VALUES (7, ?, 900000, 1, 'bh', 'root', X'00', ?)",
                rusqlite::params![t, hex::decode(&raw).unwrap()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO transactions (transaction_id, user_id, proven_tx_id, status, reference, is_outgoing, description, txid)
                 VALUES (100, ?, 7, 'completed', 'ref-T', 1, 'pay', ?)",
                rusqlite::params![MK_OWNER, t],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, spendable, change, vout, satoshis,
                                      provided_by, purpose, type, txid)
                 VALUES (1001, ?, 100, ?, 1, 1, 1, 40000, 'storage', 'change', 'P2PKH', ?)",
                rusqlite::params![MK_OWNER, MK_OWNER_BASKET, t],
            )
            .unwrap();
            (conn, raw, t)
        };
        // The second source errs (a hold, the transient road), or answers
        // "not known" (the caller's own row is failed).
        let error = || Err("WoC get_status_for_txids HTTP 429".to_string());
        let unknown = || {
            Ok(vec![crate::services::TxStatusDetail {
                txid: "t".into(),
                status: "unknown".into(),
                depth: None,
            }])
        };
        for post in [
            mk_post_after_refusal(error(), true),
            mk_post_after_refusal(error(), false),
            mk_post_after_refusal(unknown(), true),
        ] {
            let (conn, raw, t) = setup();
            let before = mk_rows(&conn, MK_OWNER);
            let tx = mk_internalize(&conn, &t, &raw, &[], post);
            assert_eq!(
                mk_rows(&conn, MK_OWNER),
                before,
                "{post:?}: the owner's rows moved"
            );
            assert_eq!(mk_balance(&conn, MK_OWNER), 40000, "{post:?}");
            if matches!(post, MkPost::Rejected) {
                // "Not known": the caller's own row alone is failed.
                let status: String = conn
                    .query_row(
                        "SELECT status FROM transactions WHERE transaction_id = ?",
                        [tx],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(status, "failed");
                assert_eq!(mk_balance(&conn, MK_CALLER), 0);
            }
        }
        // Never a row that is completed or carries a proof, even the caller's.
        let (conn, _, _) = setup();
        assert_eq!(
            conn.execute(
                COMPENSATE_FAIL_TX_SQL,
                rusqlite::params!["tc", 100, MK_OWNER]
            )
            .unwrap(),
            0
        );
        assert_eq!(
            conn.execute(
                COMPENSATE_ZERO_OUTPUTS_SQL,
                rusqlite::params!["tc", 100, MK_OWNER]
            )
            .unwrap(),
            0
        );
        conn.execute(
            "UPDATE transactions SET status = 'unproven' WHERE transaction_id = 100",
            [],
        )
        .unwrap();
        assert_eq!(
            conn.execute(
                COMPENSATE_FAIL_TX_SQL,
                rusqlite::params!["tc", 100, MK_OWNER]
            )
            .unwrap(),
            0,
            "a row carrying a proof is never failed"
        );
    }

    #[test]
    fn test_a_held_refusal_is_answered_while_nothing_is_credited() {
        let conn = mk_db();
        let answer = "Transaction t rejected by network (invalid tx): refused";
        conn.execute_batch(&format!(
            "INSERT INTO transactions (transaction_id, user_id, reference, status, is_outgoing, description, txid)
             VALUES (100, {MK_CALLER}, 'held', 'unproven', 0, 'held', 't'),
                    (101, {MK_OWNER}, 'other', 'unproven', 0, 'other', 't');
             INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, vout, satoshis, provided_by, purpose, type, txid, spendable, change)
             VALUES (1000, {MK_CALLER}, 100, {MK_CALLER_BASKET}, 0, 1000, 'storage', 'change', 'P2PKH', 't', 0, 1),
                    (1001, {MK_OWNER}, 101, {MK_OWNER_BASKET}, 0, 1000, 'storage', 'change', 'P2PKH', 't', 1, 1);
             INSERT INTO proven_tx_reqs (txid, status, raw_tx, history) VALUES ('t', 'unmined', x'00', 'attempted 3 times');"
        ))
        .unwrap();
        let read = |sql: &str, tx: i64| -> Option<String> {
            conn.query_row(sql, rusqlite::params!["t", tx, tx], |r| r.get(0))
                .ok()
        };
        // Nothing recorded: nothing answered.
        assert_eq!(read(REFUSAL_HELD_ON_MERGE_SQL, 100), None);
        conn.execute(
            RECORD_REFUSAL_HELD_SQL,
            rusqlite::params!["now", answer, "t"],
        )
        .unwrap();
        // A history that was not an object is kept under `previous`.
        let history: String = conn
            .query_row(
                "SELECT history FROM proven_tx_reqs WHERE txid = 't'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let history: serde_json::Value = serde_json::from_str(&history).unwrap();
        assert_eq!(history["previous"], "attempted 3 times");
        assert_eq!(history["refusalHeld"]["answer"], answer);
        // The caller's row, its outputs not credited: answered.
        assert_eq!(
            read(REFUSAL_HELD_ON_MERGE_SQL, 100).as_deref(),
            Some(answer)
        );
        // A new row while another row's output of the txid is credited: not.
        assert_eq!(read(REFUSAL_HELD_ON_NEW_ROW_SQL, 100), None);
        conn.execute(
            "UPDATE outputs SET spendable = 0 WHERE output_id = 1001",
            [],
        )
        .unwrap();
        assert_eq!(
            read(REFUSAL_HELD_ON_NEW_ROW_SQL, 100).as_deref(),
            Some(answer)
        );
        // The caller's own output credited: the merge is not answered so.
        conn.execute(
            "UPDATE outputs SET spendable = 1 WHERE output_id = 1000",
            [],
        )
        .unwrap();
        assert_eq!(read(REFUSAL_HELD_ON_MERGE_SQL, 100), None);
        assert_eq!(
            read(REFUSAL_HELD_ON_NEW_ROW_SQL, 100).as_deref(),
            Some(answer)
        );
        conn.execute(
            "UPDATE outputs SET spendable = 0 WHERE output_id = 1000",
            [],
        )
        .unwrap();
        // The caller's row proven: not answered.
        conn.execute(
            "UPDATE transactions SET status = 'completed' WHERE transaction_id = 100",
            [],
        )
        .unwrap();
        assert_eq!(read(REFUSAL_HELD_ON_MERGE_SQL, 100), None);
        conn.execute(
            "UPDATE transactions SET status = 'unproven' WHERE transaction_id = 100",
            [],
        )
        .unwrap();
        assert_eq!(
            read(REFUSAL_HELD_ON_MERGE_SQL, 100).as_deref(),
            Some(answer)
        );
        // The request completed: not answered, and never recorded again.
        conn.execute(
            "UPDATE proven_tx_reqs SET status = 'completed' WHERE txid = 't'",
            [],
        )
        .unwrap();
        assert_eq!(read(REFUSAL_HELD_ON_MERGE_SQL, 100), None);
        assert_eq!(read(REFUSAL_HELD_ON_NEW_ROW_SQL, 100), None);
        let changed = conn
            .execute(
                RECORD_REFUSAL_HELD_SQL,
                rusqlite::params!["later", answer, "t"],
            )
            .unwrap();
        assert_eq!(changed, 0);
    }

    #[test]
    fn test_marks_network_word() {
        use crate::services::TxStatusDetail;
        let one = |s: &str| {
            Ok(vec![TxStatusDetail {
                txid: "t".into(),
                status: s.into(),
                depth: None,
            }])
        };
        assert_eq!(network_word(one("mined")), NetworkWord::Knows);
        assert_eq!(network_word(one("known")), NetworkWord::Knows);
        assert_eq!(network_word(one("unknown")), NetworkWord::DoesNotKnow);
        assert_eq!(network_word(Ok(vec![])), NetworkWord::Unavailable);
        assert_eq!(network_word(Err("down".into())), NetworkWord::Unavailable);
    }

    #[test]
    fn test_marks_a_subject_without_proof_or_parents_is_refused() {
        // BEEF V2: no BUMP, the subject alone; then with its parent.
        let (parent_hex, parent) = mk_raw(&"b0".repeat(32), 0, 9);
        let (subject_hex, subject) = mk_raw(&parent, 0, 10);
        let beef = |txs: &[(u8, &str)]| {
            let mut b = vec![0x02, 0x00, 0xbe, 0xef, 0, txs.len() as u8];
            for (format, body) in txs {
                b.push(*format);
                b.extend(hex::decode(body).unwrap());
            }
            Beef::from_binary(&b).unwrap()
        };
        let lone = beef(&[(0, &subject_hex)]);
        assert!(unproven_subject_lacks_a_parent(
            &lone,
            &subject,
            std::slice::from_ref(&parent)
        ));
        let whole = beef(&[(0, &parent_hex), (0, &subject_hex)]);
        assert!(!unproven_subject_lacks_a_parent(
            &whole,
            &subject,
            std::slice::from_ref(&parent)
        ));
        // A coinbase input names no parent.
        assert!(!unproven_subject_lacks_a_parent(
            &lone,
            &subject,
            &["00".repeat(32)]
        ));
        assert_eq!(
            VALID_ATOMIC_BEEF_TEXT,
            "The tx parameter must be valid AtomicBEEF"
        );
    }
}
