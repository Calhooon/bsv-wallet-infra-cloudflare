//! Abort Action — cancels an unsigned/unprocessed transaction, releases locked UTXOs.
//!
//! Ported from rust-wallet-toolbox/src/storage/sqlx/abort_action.rs.
//! Adapted for D1: uses BatchCollector for atomic writes.

use crate::d1::batch::BatchCollector;
use crate::d1::{QVal, Query};
use crate::error::{Error, Result};
use chrono::Utc;
use serde::Deserialize;

use super::StorageD1;

// =============================================================================
// D1 Row Types
// =============================================================================

#[derive(Debug, Deserialize)]
struct TransactionLookupRow {
    transaction_id: Option<f64>,
    status: Option<String>,
    is_outgoing: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SpentCountRow {
    cnt: Option<f64>,
}

// =============================================================================
// Abortable statuses
// =============================================================================

const ABORTABLE_STATUSES: &[&str] = &["unsigned", "unprocessed", "nosend", "nonfinal", "unfail"];

fn is_abortable(status: &str) -> bool {
    ABORTABLE_STATUSES.contains(&status)
}

// =============================================================================
// Implementation
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// Abort an outgoing transaction: release locked UTXOs, mark transaction failed.
    ///
    /// The reference can be a transaction reference string or a txid (64 hex chars).
    pub async fn abort_action(&self, user_id: i64, reference: &str) -> Result<bool> {
        // Step 1: Find the transaction by reference or txid
        let tx = self.find_transaction_for_abort(user_id, reference).await?;

        let tx_id = tx
            .transaction_id
            .map(|v| v as i64)
            .ok_or_else(|| Error::NotFound("Transaction not found".to_string()))?;

        let status = tx.status.as_deref().unwrap_or("unknown");
        let is_outgoing = tx.is_outgoing.map(|v| v != 0.0).unwrap_or(false);

        // Step 2: Validate — must be outgoing
        if !is_outgoing {
            return Err(Error::ValidationError(
                "Cannot abort an incoming transaction".to_string(),
            ));
        }

        // Step 3: Validate — must be in an abortable status
        if !is_abortable(status) {
            return Err(Error::ValidationError(format!(
                "Cannot abort transaction with status '{}'. Abortable statuses: {:?}",
                status, ABORTABLE_STATUSES
            )));
        }

        // Step 4: Check that no outputs of this transaction have been spent elsewhere
        let spent_count: SpentCountRow = Query::new(
            "SELECT COUNT(*) as cnt FROM outputs WHERE transaction_id = ? AND spent_by IS NOT NULL",
        )
        .bind(tx_id)
        .fetch_one(self.db)
        .await?;

        let spent = spent_count.cnt.map(|v| v as i64).unwrap_or(0);
        if spent > 0 {
            return Err(Error::ValidationError(format!(
                "Cannot abort: {} output(s) of this transaction have already been spent",
                spent
            )));
        }

        // Step 5: Atomic write batch — release inputs + mark transaction failed
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        let mut batch = BatchCollector::new(self.db);

        // Release locked inputs (outputs that were marked as spent by this transaction)
        batch.add(
            "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? WHERE spent_by = ?",
            vec![QVal::Text(now_str.clone()), QVal::Int(tx_id)],
        )?;

        // Mark the transaction as failed
        batch.add(
            "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
            vec![QVal::Text(now_str), QVal::Int(tx_id)],
        )?;

        batch.execute().await?;

        Ok(true)
    }

    /// Find a transaction by reference or txid for abort.
    async fn find_transaction_for_abort(
        &self,
        user_id: i64,
        reference: &str,
    ) -> Result<TransactionLookupRow> {
        // First try by reference
        let row: Option<TransactionLookupRow> = Query::new(
            "SELECT transaction_id, status, is_outgoing FROM transactions WHERE user_id = ? AND reference = ?",
        )
        .bind(user_id)
        .bind(reference)
        .fetch_optional(self.db)
        .await?;

        if let Some(row) = row {
            return Ok(row);
        }

        // If reference looks like a txid (64 hex chars), try by txid
        if reference.len() == 64 && reference.chars().all(|c| c.is_ascii_hexdigit()) {
            let row: Option<TransactionLookupRow> = Query::new(
                "SELECT transaction_id, status, is_outgoing FROM transactions WHERE user_id = ? AND txid = ?",
            )
            .bind(user_id)
            .bind(reference)
            .fetch_optional(self.db)
            .await?;

            if let Some(row) = row {
                return Ok(row);
            }
        }

        Err(Error::NotFound(format!(
            "Transaction with reference '{}' not found",
            reference
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // ABORTABLE_STATUSES constant
    // =========================================================================

    #[test]
    fn abortable_statuses_contains_unsigned() {
        assert!(ABORTABLE_STATUSES.contains(&"unsigned"));
    }

    #[test]
    fn abortable_statuses_contains_unprocessed() {
        assert!(ABORTABLE_STATUSES.contains(&"unprocessed"));
    }

    #[test]
    fn abortable_statuses_contains_nosend() {
        assert!(ABORTABLE_STATUSES.contains(&"nosend"));
    }

    #[test]
    fn abortable_statuses_contains_nonfinal() {
        assert!(ABORTABLE_STATUSES.contains(&"nonfinal"));
    }

    #[test]
    fn abortable_statuses_contains_unfail() {
        assert!(ABORTABLE_STATUSES.contains(&"unfail"));
    }

    #[test]
    fn abortable_statuses_does_not_contain_completed() {
        assert!(!ABORTABLE_STATUSES.contains(&"completed"));
    }

    #[test]
    fn abortable_statuses_does_not_contain_sending() {
        assert!(!ABORTABLE_STATUSES.contains(&"sending"));
    }

    #[test]
    fn abortable_statuses_does_not_contain_unproven() {
        assert!(!ABORTABLE_STATUSES.contains(&"unproven"));
    }

    #[test]
    fn abortable_statuses_does_not_contain_failed() {
        assert!(!ABORTABLE_STATUSES.contains(&"failed"));
    }

    #[test]
    fn abortable_statuses_count() {
        assert_eq!(
            ABORTABLE_STATUSES.len(),
            5,
            "Expected 5 abortable statuses, got {}",
            ABORTABLE_STATUSES.len()
        );
    }

    // =========================================================================
    // is_abortable
    // =========================================================================

    #[test]
    fn is_abortable_unsigned() {
        assert!(is_abortable("unsigned"));
    }

    #[test]
    fn is_abortable_unprocessed() {
        assert!(is_abortable("unprocessed"));
    }

    #[test]
    fn is_abortable_nosend() {
        assert!(is_abortable("nosend"));
    }

    #[test]
    fn is_abortable_nonfinal() {
        assert!(is_abortable("nonfinal"));
    }

    #[test]
    fn is_abortable_unfail() {
        assert!(is_abortable("unfail"));
    }

    #[test]
    fn not_abortable_completed() {
        assert!(!is_abortable("completed"));
    }

    #[test]
    fn not_abortable_sending() {
        assert!(!is_abortable("sending"));
    }

    #[test]
    fn not_abortable_unproven() {
        assert!(!is_abortable("unproven"));
    }

    #[test]
    fn not_abortable_failed() {
        assert!(!is_abortable("failed"));
    }

    #[test]
    fn not_abortable_empty_string() {
        assert!(!is_abortable(""));
    }

    #[test]
    fn not_abortable_unknown_status() {
        assert!(!is_abortable("bogus"));
    }

    #[test]
    fn not_abortable_case_sensitive() {
        // Status matching is case-sensitive — "Unsigned" (capitalized) is NOT abortable.
        assert!(!is_abortable("Unsigned"));
        assert!(!is_abortable("UNSIGNED"));
        assert!(!is_abortable("Unprocessed"));
    }

    // =========================================================================
    // TransactionLookupRow deserialization
    // =========================================================================

    #[test]
    fn transaction_lookup_row_deserialize_full() {
        let val = serde_json::json!({
            "transaction_id": 42.0,
            "status": "unsigned",
            "is_outgoing": 1.0
        });
        let row: TransactionLookupRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.transaction_id, Some(42.0));
        assert_eq!(row.status, Some("unsigned".to_string()));
        assert_eq!(row.is_outgoing, Some(1.0));
    }

    #[test]
    fn transaction_lookup_row_deserialize_nulls() {
        let val = serde_json::json!({
            "transaction_id": null,
            "status": null,
            "is_outgoing": null
        });
        let row: TransactionLookupRow = serde_json::from_value(val).unwrap();
        assert!(row.transaction_id.is_none());
        assert!(row.status.is_none());
        assert!(row.is_outgoing.is_none());
    }

    #[test]
    fn transaction_lookup_row_is_outgoing_zero_means_incoming() {
        let val = serde_json::json!({
            "transaction_id": 1.0,
            "status": "unsigned",
            "is_outgoing": 0.0
        });
        let row: TransactionLookupRow = serde_json::from_value(val).unwrap();
        let is_outgoing = row.is_outgoing.map(|v| v != 0.0).unwrap_or(false);
        assert!(!is_outgoing);
    }

    #[test]
    fn transaction_lookup_row_is_outgoing_one_means_outgoing() {
        let val = serde_json::json!({
            "transaction_id": 1.0,
            "status": "unsigned",
            "is_outgoing": 1.0
        });
        let row: TransactionLookupRow = serde_json::from_value(val).unwrap();
        let is_outgoing = row.is_outgoing.map(|v| v != 0.0).unwrap_or(false);
        assert!(is_outgoing);
    }

    #[test]
    fn transaction_lookup_row_missing_is_outgoing_defaults_false() {
        let val = serde_json::json!({
            "transaction_id": 1.0,
            "status": "unsigned",
            "is_outgoing": null
        });
        let row: TransactionLookupRow = serde_json::from_value(val).unwrap();
        let is_outgoing = row.is_outgoing.map(|v| v != 0.0).unwrap_or(false);
        assert!(!is_outgoing);
    }

    // =========================================================================
    // SpentCountRow deserialization
    // =========================================================================

    #[test]
    fn spent_count_row_zero() {
        let val = serde_json::json!({"cnt": 0.0});
        let row: SpentCountRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.cnt.map(|v| v as i64).unwrap_or(0), 0);
    }

    #[test]
    fn spent_count_row_positive() {
        let val = serde_json::json!({"cnt": 3.0});
        let row: SpentCountRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.cnt.map(|v| v as i64).unwrap_or(0), 3);
    }

    #[test]
    fn spent_count_row_null_defaults_to_zero() {
        let val = serde_json::json!({"cnt": null});
        let row: SpentCountRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.cnt.map(|v| v as i64).unwrap_or(0), 0);
    }

    // =========================================================================
    // Abort validation logic (unit-tested via status + outgoing checks)
    // =========================================================================

    /// Simulate the validation logic from abort_action to verify error paths.
    fn validate_abort_preconditions(
        status: &str,
        is_outgoing: bool,
        spent_count: i64,
    ) -> std::result::Result<(), String> {
        if !is_outgoing {
            return Err("Cannot abort an incoming transaction".to_string());
        }
        if !is_abortable(status) {
            return Err(format!("Cannot abort transaction with status '{}'", status));
        }
        if spent_count > 0 {
            return Err(format!(
                "Cannot abort: {} output(s) already spent",
                spent_count
            ));
        }
        Ok(())
    }

    #[test]
    fn validate_abort_unsigned_outgoing_no_spent_ok() {
        assert!(validate_abort_preconditions("unsigned", true, 0).is_ok());
    }

    #[test]
    fn validate_abort_unprocessed_outgoing_no_spent_ok() {
        assert!(validate_abort_preconditions("unprocessed", true, 0).is_ok());
    }

    #[test]
    fn validate_abort_nosend_outgoing_no_spent_ok() {
        assert!(validate_abort_preconditions("nosend", true, 0).is_ok());
    }

    #[test]
    fn validate_abort_nonfinal_outgoing_no_spent_ok() {
        assert!(validate_abort_preconditions("nonfinal", true, 0).is_ok());
    }

    #[test]
    fn validate_abort_unfail_outgoing_no_spent_ok() {
        assert!(validate_abort_preconditions("unfail", true, 0).is_ok());
    }

    #[test]
    fn validate_abort_incoming_transaction_rejected() {
        let result = validate_abort_preconditions("unsigned", false, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("incoming"));
    }

    #[test]
    fn validate_abort_completed_status_rejected() {
        let result = validate_abort_preconditions("completed", true, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("completed"));
    }

    #[test]
    fn validate_abort_sending_status_rejected() {
        let result = validate_abort_preconditions("sending", true, 0);
        assert!(result.is_err());
    }

    #[test]
    fn validate_abort_unproven_status_rejected() {
        let result = validate_abort_preconditions("unproven", true, 0);
        assert!(result.is_err());
    }

    #[test]
    fn validate_abort_failed_status_rejected() {
        let result = validate_abort_preconditions("failed", true, 0);
        assert!(result.is_err());
    }

    #[test]
    fn validate_abort_spent_outputs_rejected() {
        let result = validate_abort_preconditions("unsigned", true, 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already spent"));
    }

    #[test]
    fn validate_abort_multiple_spent_outputs_rejected() {
        let result = validate_abort_preconditions("unsigned", true, 5);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("5 output(s)"));
    }

    #[test]
    fn validate_abort_incoming_checked_before_status() {
        // Even if status is abortable, incoming transaction is rejected first.
        let result = validate_abort_preconditions("unsigned", false, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("incoming"));
    }

    // =========================================================================
    // txid reference detection (64 hex chars)
    // =========================================================================

    #[test]
    fn txid_reference_valid_64_hex() {
        let txid = "a".repeat(64);
        assert_eq!(txid.len(), 64);
        assert!(txid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn txid_reference_mixed_hex_chars() {
        let txid = "0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF";
        assert_eq!(txid.len(), 64);
        assert!(txid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn txid_reference_63_chars_not_txid() {
        let reference = "a".repeat(63);
        assert_ne!(reference.len(), 64);
    }

    #[test]
    fn txid_reference_65_chars_not_txid() {
        let reference = "a".repeat(65);
        assert_ne!(reference.len(), 64);
    }

    #[test]
    fn txid_reference_with_non_hex_not_txid() {
        let mut reference = "a".repeat(63);
        reference.push('g'); // 'g' is not hex
        assert_eq!(reference.len(), 64);
        assert!(!reference.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn normal_reference_not_txid() {
        let reference = "my-transaction-ref";
        assert_ne!(reference.len(), 64);
    }

    #[test]
    fn uuid_reference_not_txid() {
        let reference = "550e8400-e29b-41d4-a716-446655440000"; // 36 chars
        assert_ne!(reference.len(), 64);
    }
}
