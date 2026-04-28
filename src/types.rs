//! Query argument and result types.
//!
//! Copied from rust-wallet-toolbox/src/storage/traits.rs (types only, no traits).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entities::*;

// =============================================================================
// Authentication
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthId {
    pub identity_key: String,
    pub user_id: Option<i64>,
    pub is_active: Option<bool>,
}

impl AuthId {
    pub fn new(identity_key: impl Into<String>) -> Self {
        Self {
            identity_key: identity_key.into(),
            user_id: None,
            is_active: None,
        }
    }

    pub fn with_user_id(identity_key: impl Into<String>, user_id: i64) -> Self {
        Self {
            identity_key: identity_key.into(),
            user_id: Some(user_id),
            is_active: None,
        }
    }
}

// =============================================================================
// Query Arguments
// =============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Paged {
    pub offset: Option<u32>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindSincePagedArgs {
    pub since: Option<DateTime<Utc>>,
    pub paged: Option<Paged>,
    pub order_descending: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindCertificatesArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub user_id: Option<i64>,
    pub certifiers: Option<Vec<String>>,
    pub types: Option<Vec<String>>,
    pub include_fields: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindOutputBasketsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub user_id: Option<i64>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindOutputsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub user_id: Option<i64>,
    pub basket_id: Option<i64>,
    pub txid: Option<String>,
    pub vout: Option<u32>,
    pub no_script: Option<bool>,
    pub tx_status: Option<Vec<TransactionStatus>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindProvenTxReqsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub status: Option<Vec<ProvenTxReqStatus>>,
    pub txids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindTransactionsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub status: Option<Vec<TransactionStatus>>,
    pub no_raw_tx: Option<bool>,
}

// =============================================================================
// Storage Results
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct StorageCreateActionResult {
    pub input_beef: Option<Vec<u8>>,
    #[serde(default)]
    pub inputs: Vec<StorageCreateTransactionInput>,
    #[serde(default)]
    pub outputs: Vec<StorageCreateTransactionOutput>,
    pub no_send_change_output_vouts: Option<Vec<u32>>,
    #[serde(default)]
    pub derivation_prefix: String,
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub lock_time: u32,
    #[serde(default)]
    pub reference: String,
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct StorageCreateTransactionInput {
    pub vin: u32,
    pub source_txid: String,
    pub source_vout: u32,
    pub source_satoshis: u64,
    pub source_locking_script: String,
    pub source_transaction: Option<Vec<u8>>,
    pub unlocking_script_length: u32,
    #[serde(default)]
    pub provided_by: StorageProvidedBy,
    #[serde(default)]
    pub input_type: String,
    pub spending_description: Option<String>,
    pub derivation_prefix: Option<String>,
    pub derivation_suffix: Option<String>,
    pub sender_identity_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageCreateTransactionOutput {
    pub vout: u32,
    pub satoshis: u64,
    pub locking_script: String,
    pub provided_by: StorageProvidedBy,
    pub purpose: Option<String>,
    pub derivation_suffix: Option<String>,
    pub basket: Option<String>,
    pub tags: Vec<String>,
    pub output_description: Option<String>,
    pub custom_instructions: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum StorageProvidedBy {
    #[default]
    You,
    Storage,
    YouAndStorage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageProcessActionArgs {
    pub is_new_tx: bool,
    pub is_send_with: bool,
    pub is_no_send: bool,
    pub is_delayed: bool,
    pub reference: Option<String>,
    pub txid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_tx: Option<Vec<u8>>,
    pub send_with: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageProcessActionResults {
    pub send_with_results: Option<Vec<SendWithResult>>,
    pub not_delayed_results: Option<Vec<ReviewActionResult>>,
    pub log: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendWithResult {
    pub txid: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewActionResult {
    pub txid: String,
    pub status: ReviewActionResultStatus,
    pub competing_txs: Option<Vec<String>>,
    pub competing_beef: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReviewActionResultStatus {
    Success,
    DoubleSpend,
    ServiceError,
    InvalidTx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageInternalizeActionResult {
    pub accepted: bool,
    pub is_merge: bool,
    pub txid: String,
    pub satoshis: i64,
    pub send_with_results: Option<Vec<SendWithResult>>,
    pub not_delayed_results: Option<Vec<ReviewActionResult>>,
}

// =============================================================================
// Sync Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestSyncChunkArgs {
    pub from_storage_identity_key: String,
    pub to_storage_identity_key: String,
    pub identity_key: String,
    pub since: Option<DateTime<Utc>>,
    pub max_rough_size: u32,
    pub max_items: u32,
    pub offsets: Vec<SyncOffset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncOffset {
    pub name: String,
    pub offset: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncChunk {
    pub from_storage_identity_key: String,
    pub to_storage_identity_key: String,
    pub user_identity_key: String,
    pub user: Option<TableUser>,
    pub proven_txs: Option<Vec<TableProvenTx>>,
    pub proven_tx_reqs: Option<Vec<TableProvenTxReq>>,
    pub output_baskets: Option<Vec<TableOutputBasket>>,
    pub tx_labels: Option<Vec<TableTxLabel>>,
    pub output_tags: Option<Vec<TableOutputTag>>,
    pub transactions: Option<Vec<TableTransaction>>,
    pub tx_label_maps: Option<Vec<TableTxLabelMap>>,
    pub commissions: Option<Vec<TableCommission>>,
    pub outputs: Option<Vec<TableOutput>>,
    pub output_tag_maps: Option<Vec<TableOutputTagMap>>,
    pub certificates: Option<Vec<TableCertificate>>,
    pub certificate_fields: Option<Vec<TableCertificateField>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessSyncChunkResult {
    pub done: bool,
    pub max_updated_at: Option<DateTime<Utc>>,
    pub updates: u32,
    pub inserts: u32,
    pub error: Option<String>,
}

// =============================================================================
// Operation Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeParams {
    pub max_age_days: u32,
    pub purge_completed: bool,
    pub purge_failed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeResults {
    pub count: u32,
    pub log: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewStatusResult {
    pub log: String,
}

// =============================================================================
// BEEF Verification
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeefVerificationMode {
    /// Reject payments with invalid BEEF proofs (default).
    #[default]
    Strict,
    /// Attempt verification but only log failures — never reject.
    LogOnly,
    /// Skip verification entirely.
    Skip,
}

impl BeefVerificationMode {
    /// Parse from the `BEEF_VERIFICATION` env var string.
    /// Accepted values: "strict", "log_only", "skip" (case-insensitive).
    /// Returns `Strict` for unrecognized values.
    pub fn from_env_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "strict" => Self::Strict,
            "log_only" | "logonly" | "log-only" => Self::LogOnly,
            "skip" | "disabled" => Self::Skip,
            _ => Self::Strict,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // AuthId
    // =========================================================================

    #[test]
    fn auth_id_new() {
        let auth = AuthId::new("key123");
        assert_eq!(auth.identity_key, "key123");
        assert!(auth.user_id.is_none());
        assert!(auth.is_active.is_none());
    }

    #[test]
    fn auth_id_with_user_id() {
        let auth = AuthId::with_user_id("key456", 42);
        assert_eq!(auth.identity_key, "key456");
        assert_eq!(auth.user_id, Some(42));
        assert!(auth.is_active.is_none());
    }

    #[test]
    fn auth_id_deserialize_camel_case() {
        let val = json!({
            "identityKey": "abc",
            "userId": 7,
            "isActive": true
        });
        let auth: AuthId = serde_json::from_value(val).unwrap();
        assert_eq!(auth.identity_key, "abc");
        assert_eq!(auth.user_id, Some(7));
        assert_eq!(auth.is_active, Some(true));
    }

    #[test]
    fn auth_id_deserialize_minimal() {
        let val = json!({"identityKey": "key_only"});
        let auth: AuthId = serde_json::from_value(val).unwrap();
        assert_eq!(auth.identity_key, "key_only");
        assert!(auth.user_id.is_none());
        assert!(auth.is_active.is_none());
    }

    #[test]
    fn auth_id_roundtrip() {
        let auth = AuthId::with_user_id("round_trip", 99);
        let json_val = serde_json::to_value(&auth).unwrap();
        assert_eq!(json_val["identityKey"], "round_trip");
        assert_eq!(json_val["userId"], 99);
        let back: AuthId = serde_json::from_value(json_val).unwrap();
        assert_eq!(back.identity_key, "round_trip");
        assert_eq!(back.user_id, Some(99));
    }

    // =========================================================================
    // Paged
    // =========================================================================

    #[test]
    fn paged_default() {
        let p = Paged::default();
        assert!(p.offset.is_none());
        assert!(p.limit.is_none());
    }

    #[test]
    fn paged_deserialize() {
        let val = json!({"offset": 10, "limit": 25});
        let p: Paged = serde_json::from_value(val).unwrap();
        assert_eq!(p.offset, Some(10));
        assert_eq!(p.limit, Some(25));
    }

    #[test]
    fn paged_deserialize_empty() {
        let val = json!({});
        let p: Paged = serde_json::from_value(val).unwrap();
        assert!(p.offset.is_none());
        assert!(p.limit.is_none());
    }

    // =========================================================================
    // FindSincePagedArgs
    // =========================================================================

    #[test]
    fn find_since_paged_default() {
        let args = FindSincePagedArgs::default();
        assert!(args.since.is_none());
        assert!(args.paged.is_none());
        assert!(args.order_descending.is_none());
    }

    #[test]
    fn find_since_paged_deserialize_with_values() {
        let val = json!({
            "since": "2024-01-01T00:00:00Z",
            "paged": {"offset": 0, "limit": 50},
            "orderDescending": true
        });
        let args: FindSincePagedArgs = serde_json::from_value(val).unwrap();
        assert!(args.since.is_some());
        assert!(args.paged.is_some());
        assert_eq!(args.order_descending, Some(true));
    }

    // =========================================================================
    // FindCertificatesArgs
    // =========================================================================

    #[test]
    fn find_certificates_args_empty() {
        let val = json!({});
        let args: FindCertificatesArgs = serde_json::from_value(val).unwrap();
        assert!(args.certifiers.is_none());
        assert!(args.types.is_none());
        assert!(args.include_fields.is_none());
        assert!(args.user_id.is_none());
    }

    #[test]
    fn find_certificates_args_with_certifiers() {
        let val = json!({
            "certifiers": ["certifier1", "certifier2"],
            "types": ["type_a"],
            "includeFields": true
        });
        let args: FindCertificatesArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.certifiers.as_ref().unwrap().len(), 2);
        assert_eq!(args.types.as_ref().unwrap().len(), 1);
        assert_eq!(args.include_fields, Some(true));
    }

    // =========================================================================
    // FindOutputsArgs
    // =========================================================================

    #[test]
    fn find_outputs_args_empty() {
        let val = json!({});
        let args: FindOutputsArgs = serde_json::from_value(val).unwrap();
        assert!(args.txid.is_none());
        assert!(args.vout.is_none());
        assert!(args.basket_id.is_none());
        assert!(args.no_script.is_none());
        assert!(args.tx_status.is_none());
    }

    #[test]
    fn find_outputs_args_with_status_filter() {
        let val = json!({
            "txid": "abc123",
            "vout": 0,
            "txStatus": ["completed", "unproven"]
        });
        let args: FindOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.txid, Some("abc123".to_string()));
        assert_eq!(args.vout, Some(0));
        let statuses = args.tx_status.unwrap();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0], TransactionStatus::Completed);
        assert_eq!(statuses[1], TransactionStatus::Unproven);
    }

    // =========================================================================
    // FindProvenTxReqsArgs
    // =========================================================================

    #[test]
    fn find_proven_tx_reqs_args_empty() {
        let val = json!({});
        let args: FindProvenTxReqsArgs = serde_json::from_value(val).unwrap();
        assert!(args.status.is_none());
        assert!(args.txids.is_none());
    }

    #[test]
    fn find_proven_tx_reqs_args_with_values() {
        let val = json!({
            "status": ["pending", "completed"],
            "txids": ["txid1", "txid2"]
        });
        let args: FindProvenTxReqsArgs = serde_json::from_value(val).unwrap();
        let statuses = args.status.unwrap();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0], ProvenTxReqStatus::Pending);
        assert_eq!(statuses[1], ProvenTxReqStatus::Completed);
        assert_eq!(args.txids.unwrap().len(), 2);
    }

    // =========================================================================
    // StorageProvidedBy
    // =========================================================================

    #[test]
    fn provided_by_default() {
        let p = StorageProvidedBy::default();
        assert_eq!(p, StorageProvidedBy::You);
    }

    #[test]
    fn provided_by_deserialize_kebab_case() {
        let val = json!("you");
        let p: StorageProvidedBy = serde_json::from_value(val).unwrap();
        assert_eq!(p, StorageProvidedBy::You);

        let val = json!("storage");
        let p: StorageProvidedBy = serde_json::from_value(val).unwrap();
        assert_eq!(p, StorageProvidedBy::Storage);

        let val = json!("you-and-storage");
        let p: StorageProvidedBy = serde_json::from_value(val).unwrap();
        assert_eq!(p, StorageProvidedBy::YouAndStorage);
    }

    #[test]
    fn provided_by_roundtrip() {
        let original = StorageProvidedBy::YouAndStorage;
        let json_val = serde_json::to_value(original).unwrap();
        assert_eq!(json_val, json!("you-and-storage"));
        let back: StorageProvidedBy = serde_json::from_value(json_val).unwrap();
        assert_eq!(back, StorageProvidedBy::YouAndStorage);
    }

    // =========================================================================
    // StorageProcessActionArgs
    // =========================================================================

    #[test]
    fn process_action_args_deserialize() {
        let val = json!({
            "isNewTx": true,
            "isSendWith": false,
            "isNoSend": false,
            "isDelayed": false,
            "reference": "ref-123",
            "txid": "deadbeef",
            "sendWith": ["txid_a", "txid_b"]
        });
        let args: StorageProcessActionArgs = serde_json::from_value(val).unwrap();
        assert!(args.is_new_tx);
        assert!(!args.is_send_with);
        assert!(!args.is_no_send);
        assert!(!args.is_delayed);
        assert_eq!(args.reference, Some("ref-123".to_string()));
        assert_eq!(args.txid, Some("deadbeef".to_string()));
        assert_eq!(args.send_with.len(), 2);
    }

    #[test]
    fn process_action_args_minimal() {
        let val = json!({
            "isNewTx": false,
            "isSendWith": false,
            "isNoSend": true,
            "isDelayed": false,
            "sendWith": []
        });
        let args: StorageProcessActionArgs = serde_json::from_value(val).unwrap();
        assert!(args.is_no_send);
        assert!(args.reference.is_none());
        assert!(args.txid.is_none());
        assert!(args.raw_tx.is_none());
        assert!(args.send_with.is_empty());
    }

    // =========================================================================
    // StorageCreateActionResult
    // =========================================================================

    #[test]
    fn create_action_result_defaults() {
        // Note: #[derive(Default)] uses u32::default() = 0 for version.
        // The serde(default = "default_version") only applies during deserialization.
        let result = StorageCreateActionResult::default();
        assert!(result.input_beef.is_none());
        assert!(result.inputs.is_empty());
        assert!(result.outputs.is_empty());
        assert!(result.no_send_change_output_vouts.is_none());
        assert_eq!(result.derivation_prefix, "");
        assert_eq!(result.version, 0); // derive(Default) gives u32::default()
        assert_eq!(result.lock_time, 0);
        assert_eq!(result.reference, "");
    }

    #[test]
    fn create_action_result_serde_default_version_is_1() {
        // When deserialized from JSON with missing "version", serde uses default_version() = 1.
        let val = json!({});
        let result: StorageCreateActionResult = serde_json::from_value(val).unwrap();
        assert_eq!(result.version, 1);
    }

    #[test]
    fn create_action_result_deserialize_from_json() {
        let val = json!({
            "derivationPrefix": "m/1/2",
            "version": 2,
            "lockTime": 500000,
            "reference": "ref-xyz",
            "inputs": [],
            "outputs": []
        });
        let result: StorageCreateActionResult = serde_json::from_value(val).unwrap();
        assert_eq!(result.derivation_prefix, "m/1/2");
        assert_eq!(result.version, 2);
        assert_eq!(result.lock_time, 500000);
        assert_eq!(result.reference, "ref-xyz");
    }

    // =========================================================================
    // ReviewActionResultStatus
    // =========================================================================

    #[test]
    fn review_action_result_status_variants() {
        let success: ReviewActionResultStatus = serde_json::from_value(json!("success")).unwrap();
        assert_eq!(success, ReviewActionResultStatus::Success);

        let ds: ReviewActionResultStatus = serde_json::from_value(json!("doubleSpend")).unwrap();
        assert_eq!(ds, ReviewActionResultStatus::DoubleSpend);

        let se: ReviewActionResultStatus = serde_json::from_value(json!("serviceError")).unwrap();
        assert_eq!(se, ReviewActionResultStatus::ServiceError);

        let inv: ReviewActionResultStatus = serde_json::from_value(json!("invalidTx")).unwrap();
        assert_eq!(inv, ReviewActionResultStatus::InvalidTx);
    }

    // =========================================================================
    // BeefVerificationMode
    // =========================================================================

    #[test]
    fn beef_verification_mode_default_is_strict() {
        let mode = BeefVerificationMode::default();
        assert_eq!(mode, BeefVerificationMode::Strict);
    }

    #[test]
    fn beef_verification_mode_deserialize() {
        let strict: BeefVerificationMode = serde_json::from_value(json!("strict")).unwrap();
        assert_eq!(strict, BeefVerificationMode::Strict);

        let log_only: BeefVerificationMode = serde_json::from_value(json!("log_only")).unwrap();
        assert_eq!(log_only, BeefVerificationMode::LogOnly);

        let skip: BeefVerificationMode = serde_json::from_value(json!("skip")).unwrap();
        assert_eq!(skip, BeefVerificationMode::Skip);
    }

    #[test]
    fn beef_verification_mode_roundtrip() {
        let original = BeefVerificationMode::LogOnly;
        let json_val = serde_json::to_value(original).unwrap();
        assert_eq!(json_val, json!("log_only"));
        let back: BeefVerificationMode = serde_json::from_value(json_val).unwrap();
        assert_eq!(back, BeefVerificationMode::LogOnly);
    }

    #[test]
    fn beef_verification_mode_from_env_str() {
        assert_eq!(
            BeefVerificationMode::from_env_str("strict"),
            BeefVerificationMode::Strict
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("STRICT"),
            BeefVerificationMode::Strict
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("log_only"),
            BeefVerificationMode::LogOnly
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("LOG_ONLY"),
            BeefVerificationMode::LogOnly
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("logonly"),
            BeefVerificationMode::LogOnly
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("log-only"),
            BeefVerificationMode::LogOnly
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("skip"),
            BeefVerificationMode::Skip
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("disabled"),
            BeefVerificationMode::Skip
        );
        assert_eq!(
            BeefVerificationMode::from_env_str("unknown_value"),
            BeefVerificationMode::Strict
        );
        assert_eq!(
            BeefVerificationMode::from_env_str(""),
            BeefVerificationMode::Strict
        );
    }

    // =========================================================================
    // SendWithResult
    // =========================================================================

    #[test]
    fn send_with_result_roundtrip() {
        let swr = SendWithResult {
            txid: "abc123".to_string(),
            status: "unproven".to_string(),
        };
        let val = serde_json::to_value(&swr).unwrap();
        assert_eq!(val["txid"], "abc123");
        assert_eq!(val["status"], "unproven");
        let back: SendWithResult = serde_json::from_value(val).unwrap();
        assert_eq!(back.txid, "abc123");
        assert_eq!(back.status, "unproven");
    }

    // =========================================================================
    // PurgeParams / PurgeResults
    // =========================================================================

    #[test]
    fn purge_params_deserialize() {
        let val = json!({
            "maxAgeDays": 30,
            "purgeCompleted": true,
            "purgeFailed": false
        });
        let p: PurgeParams = serde_json::from_value(val).unwrap();
        assert_eq!(p.max_age_days, 30);
        assert!(p.purge_completed);
        assert!(!p.purge_failed);
    }

    #[test]
    fn purge_results_serialize() {
        let r = PurgeResults {
            count: 5,
            log: "purged 5 records".to_string(),
        };
        let val = serde_json::to_value(&r).unwrap();
        assert_eq!(val["count"], 5);
        assert_eq!(val["log"], "purged 5 records");
    }

    // =========================================================================
    // SyncChunk defaults
    // =========================================================================

    #[test]
    fn sync_chunk_default() {
        let chunk = SyncChunk::default();
        assert_eq!(chunk.from_storage_identity_key, "");
        assert_eq!(chunk.to_storage_identity_key, "");
        assert_eq!(chunk.user_identity_key, "");
        assert!(chunk.user.is_none());
        assert!(chunk.proven_txs.is_none());
        assert!(chunk.transactions.is_none());
        assert!(chunk.outputs.is_none());
        assert!(chunk.certificates.is_none());
    }

    // =========================================================================
    // FindOutputBasketsArgs
    // =========================================================================

    #[test]
    fn find_output_baskets_args_empty() {
        let val = json!({});
        let args: FindOutputBasketsArgs = serde_json::from_value(val).unwrap();
        assert!(args.user_id.is_none());
        assert!(args.name.is_none());
    }

    #[test]
    fn find_output_baskets_args_with_name() {
        let val = json!({"name": "default", "userId": 42});
        let args: FindOutputBasketsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.name, Some("default".to_string()));
        assert_eq!(args.user_id, Some(42));
    }

    // =========================================================================
    // FindTransactionsArgs
    // =========================================================================

    #[test]
    fn find_transactions_args_empty() {
        let val = json!({});
        let args: FindTransactionsArgs = serde_json::from_value(val).unwrap();
        assert!(args.status.is_none());
        assert!(args.no_raw_tx.is_none());
    }

    #[test]
    fn find_transactions_args_with_status() {
        let val = json!({
            "status": ["completed", "failed"],
            "noRawTx": true
        });
        let args: FindTransactionsArgs = serde_json::from_value(val).unwrap();
        let statuses = args.status.unwrap();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0], TransactionStatus::Completed);
        assert_eq!(statuses[1], TransactionStatus::Failed);
        assert_eq!(args.no_raw_tx, Some(true));
    }
}
