//! Database entity definitions.
//!
//! Copied from rust-wallet-toolbox/src/storage/entities/mod.rs.
//! These structs represent the 18 tables in the wallet storage schema.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn default_datetime() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

// =============================================================================
// Transaction Status
// =============================================================================

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransactionStatus {
    #[default]
    Completed,
    Unprocessed,
    Sending,
    Unproven,
    Unsigned,
    #[serde(alias = "nosend")]
    NoSend,
    #[serde(alias = "nonfinal")]
    NonFinal,
    Failed,
    Unfail,
}

impl TransactionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionStatus::Completed => "completed",
            TransactionStatus::Unprocessed => "unprocessed",
            TransactionStatus::Sending => "sending",
            TransactionStatus::Unproven => "unproven",
            TransactionStatus::Unsigned => "unsigned",
            TransactionStatus::NoSend => "nosend",
            TransactionStatus::NonFinal => "nonfinal",
            TransactionStatus::Failed => "failed",
            TransactionStatus::Unfail => "unfail",
        }
    }

    pub fn parse_status(s: &str) -> Self {
        match s {
            "completed" => TransactionStatus::Completed,
            "unprocessed" => TransactionStatus::Unprocessed,
            "sending" => TransactionStatus::Sending,
            "unproven" => TransactionStatus::Unproven,
            "unsigned" => TransactionStatus::Unsigned,
            "nosend" => TransactionStatus::NoSend,
            "nonfinal" => TransactionStatus::NonFinal,
            "failed" => TransactionStatus::Failed,
            "unfail" => TransactionStatus::Unfail,
            _ => TransactionStatus::Unprocessed,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProvenTxReqStatus {
    #[default]
    Pending,
    InProgress,
    Completed,
    Failed,
    NotFound,
    Unsent,
    Sending,
    Unmined,
    Unknown,
    Callback,
    Unconfirmed,
    Unfail,
    #[serde(alias = "noSend")]
    #[serde(rename = "nosend")]
    NoSend,
    Invalid,
    DoubleSpend,
    #[serde(alias = "nonFinal")]
    #[serde(rename = "nonfinal")]
    NonFinal,
    Unprocessed,
}

impl ProvenTxReqStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "inProgress",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::NotFound => "notFound",
            Self::Unsent => "unsent",
            Self::Sending => "sending",
            Self::Unmined => "unmined",
            Self::Unknown => "unknown",
            Self::Callback => "callback",
            Self::Unconfirmed => "unconfirmed",
            Self::Unfail => "unfail",
            Self::NoSend => "nosend",
            Self::Invalid => "invalid",
            Self::DoubleSpend => "doubleSpend",
            Self::NonFinal => "nonfinal",
            Self::Unprocessed => "unprocessed",
        }
    }
}

// =============================================================================
// Core Tables
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableUser {
    pub user_id: i64,
    pub identity_key: String,
    pub active_storage: Option<String>,
    #[serde(alias = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(alias = "updated_at")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSettings {
    #[serde(default = "default_settings_id")]
    pub settings_id: i64,
    pub storage_identity_key: String,
    pub storage_name: String,
    pub chain: String,
    pub max_output_script: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dbtype: Option<String>,
    #[serde(alias = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(alias = "updated_at")]
    pub updated_at: DateTime<Utc>,
}

fn default_settings_id() -> i64 {
    1
}

impl Default for TableSettings {
    fn default() -> Self {
        Self {
            settings_id: 1,
            storage_identity_key: String::new(),
            storage_name: String::new(),
            chain: "mainnet".to_string(),
            max_output_script: 10_000,
            dbtype: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TableTransaction {
    pub transaction_id: i64,
    pub user_id: i64,
    pub txid: Option<String>,
    pub status: TransactionStatus,
    pub reference: String,
    pub description: String,
    pub satoshis: i64,
    pub version: i32,
    pub lock_time: i64,
    pub raw_tx: Option<Vec<u8>>,
    pub input_beef: Option<Vec<u8>>,
    pub is_outgoing: bool,
    pub proof_txid: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Default for TableTransaction {
    fn default() -> Self {
        Self {
            transaction_id: 0,
            user_id: 0,
            txid: None,
            status: TransactionStatus::default(),
            reference: String::new(),
            description: String::new(),
            satoshis: 0,
            version: 0,
            lock_time: 0,
            raw_tx: None,
            input_beef: None,
            is_outgoing: false,
            proof_txid: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutput {
    #[serde(default)]
    pub output_id: i64,
    #[serde(default)]
    pub user_id: i64,
    #[serde(default)]
    pub transaction_id: i64,
    pub basket_id: Option<i64>,
    #[serde(default)]
    pub txid: String,
    #[serde(default)]
    pub vout: i32,
    #[serde(default)]
    pub satoshis: i64,
    pub locking_script: Option<Vec<u8>>,
    #[serde(default)]
    pub script_length: i32,
    #[serde(default)]
    pub script_offset: i32,
    #[serde(default)]
    pub output_type: String,
    #[serde(default)]
    pub provided_by: String,
    pub purpose: Option<String>,
    pub output_description: Option<String>,
    pub spent_by: Option<i64>,
    pub sequence_number: Option<u32>,
    pub spending_description: Option<String>,
    #[serde(default)]
    pub spendable: bool,
    #[serde(default)]
    pub change: bool,
    pub derivation_prefix: Option<String>,
    pub derivation_suffix: Option<String>,
    pub sender_identity_key: Option<String>,
    pub custom_instructions: Option<String>,
    #[serde(default = "default_datetime")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_datetime")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutputBasket {
    pub basket_id: i64,
    pub user_id: i64,
    pub name: String,
    pub number_of_desired_utxos: i32,
    pub minimum_desired_utxo_value: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutputTag {
    pub tag_id: i64,
    pub user_id: i64,
    pub tag: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutputTagMap {
    pub output_tag_map_id: i64,
    pub output_id: i64,
    pub tag_id: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableTxLabel {
    pub label_id: i64,
    pub user_id: i64,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableTxLabelMap {
    pub tx_label_map_id: i64,
    pub transaction_id: i64,
    pub label_id: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Proof Tables
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableProvenTx {
    pub proven_tx_id: i64,
    pub txid: String,
    pub height: i64,
    pub index: i64,
    pub block_hash: String,
    pub merkle_root: String,
    pub merkle_path: Vec<u8>,
    pub raw_tx: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableProvenTxReq {
    #[serde(default)]
    pub proven_tx_req_id: i64,
    #[serde(default)]
    pub txid: String,
    #[serde(default)]
    pub status: ProvenTxReqStatus,
    #[serde(default)]
    pub attempts: i32,
    #[serde(default)]
    pub history: String,
    #[serde(default)]
    pub notified: bool,
    #[serde(default)]
    pub notify: String,
    pub raw_tx: Option<Vec<u8>>,
    pub input_beef: Option<Vec<u8>>,
    pub proven_tx_id: Option<i64>,
    pub batch: Option<String>,
    #[serde(default = "default_datetime")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_datetime")]
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Certificate Tables
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCertificate {
    pub certificate_id: i64,
    pub user_id: i64,
    pub cert_type: String,
    pub serial_number: String,
    pub certifier: String,
    pub subject: String,
    pub verifier: Option<String>,
    pub revocation_outpoint: String,
    pub signature: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCertificateField {
    pub certificate_field_id: i64,
    pub certificate_id: i64,
    pub user_id: i64,
    pub field_name: String,
    pub field_value: String,
    pub master_key: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Sync Tables
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSyncState {
    pub sync_state_id: i64,
    pub user_id: i64,
    pub storage_identity_key: String,
    pub storage_name: String,
    pub status: String,
    pub init: bool,
    pub ref_num: String,
    pub sync_map: String,
    pub when_last_sync_started: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub satoshis: Option<i64>,
    pub error_local: Option<String>,
    pub error_other: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Other Tables
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCommission {
    pub commission_id: i64,
    pub user_id: i64,
    pub transaction_id: i64,
    pub satoshis: i64,
    pub payer_locking_script: Vec<u8>,
    pub key_offset: String,
    pub is_redeemed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMonitorEvent {
    pub event_id: i64,
    pub event_type: String,
    pub event_data: String,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // TransactionStatus
    // =========================================================================

    #[test]
    fn transaction_status_default_is_completed() {
        assert_eq!(TransactionStatus::default(), TransactionStatus::Completed);
    }

    #[test]
    fn transaction_status_as_str_roundtrip() {
        let variants = [
            (TransactionStatus::Completed, "completed"),
            (TransactionStatus::Unprocessed, "unprocessed"),
            (TransactionStatus::Sending, "sending"),
            (TransactionStatus::Unproven, "unproven"),
            (TransactionStatus::Unsigned, "unsigned"),
            (TransactionStatus::NoSend, "nosend"),
            (TransactionStatus::NonFinal, "nonfinal"),
            (TransactionStatus::Failed, "failed"),
            (TransactionStatus::Unfail, "unfail"),
        ];
        for (variant, expected_str) in &variants {
            assert_eq!(variant.as_str(), *expected_str);
            assert_eq!(TransactionStatus::parse_status(expected_str), *variant);
        }
    }

    #[test]
    fn transaction_status_from_str_unknown_defaults_to_unprocessed() {
        let status = TransactionStatus::parse_status("bogus_status");
        assert_eq!(status, TransactionStatus::Unprocessed);
    }

    #[test]
    fn transaction_status_serde_camel_case() {
        // Serde uses camelCase for most variants.
        let completed: TransactionStatus = serde_json::from_value(json!("completed")).unwrap();
        assert_eq!(completed, TransactionStatus::Completed);

        let unproven: TransactionStatus = serde_json::from_value(json!("unproven")).unwrap();
        assert_eq!(unproven, TransactionStatus::Unproven);
    }

    #[test]
    fn transaction_status_nosend_alias() {
        // "nosend" alias should work alongside the camelCase "noSend".
        let nosend: TransactionStatus = serde_json::from_value(json!("nosend")).unwrap();
        assert_eq!(nosend, TransactionStatus::NoSend);

        let no_send_camel: TransactionStatus = serde_json::from_value(json!("noSend")).unwrap();
        assert_eq!(no_send_camel, TransactionStatus::NoSend);
    }

    #[test]
    fn transaction_status_nonfinal_alias() {
        let nonfinal: TransactionStatus = serde_json::from_value(json!("nonfinal")).unwrap();
        assert_eq!(nonfinal, TransactionStatus::NonFinal);

        let non_final_camel: TransactionStatus = serde_json::from_value(json!("nonFinal")).unwrap();
        assert_eq!(non_final_camel, TransactionStatus::NonFinal);
    }

    #[test]
    fn transaction_status_serialize_roundtrip() {
        let original = TransactionStatus::Sending;
        let json_val = serde_json::to_value(original).unwrap();
        assert_eq!(json_val, json!("sending"));
        let back: TransactionStatus = serde_json::from_value(json_val).unwrap();
        assert_eq!(back, TransactionStatus::Sending);
    }

    // =========================================================================
    // ProvenTxReqStatus
    // =========================================================================

    #[test]
    fn proven_tx_req_status_default_is_pending() {
        assert_eq!(ProvenTxReqStatus::default(), ProvenTxReqStatus::Pending);
    }

    #[test]
    fn proven_tx_req_status_as_str() {
        assert_eq!(ProvenTxReqStatus::Pending.as_str(), "pending");
        assert_eq!(ProvenTxReqStatus::InProgress.as_str(), "inProgress");
        assert_eq!(ProvenTxReqStatus::Completed.as_str(), "completed");
        assert_eq!(ProvenTxReqStatus::Failed.as_str(), "failed");
        assert_eq!(ProvenTxReqStatus::NotFound.as_str(), "notFound");
        assert_eq!(ProvenTxReqStatus::Unsent.as_str(), "unsent");
        assert_eq!(ProvenTxReqStatus::Sending.as_str(), "sending");
        assert_eq!(ProvenTxReqStatus::Unmined.as_str(), "unmined");
        assert_eq!(ProvenTxReqStatus::Unknown.as_str(), "unknown");
        assert_eq!(ProvenTxReqStatus::Callback.as_str(), "callback");
        assert_eq!(ProvenTxReqStatus::Unconfirmed.as_str(), "unconfirmed");
        assert_eq!(ProvenTxReqStatus::Unfail.as_str(), "unfail");
        assert_eq!(ProvenTxReqStatus::NoSend.as_str(), "nosend");
        assert_eq!(ProvenTxReqStatus::Invalid.as_str(), "invalid");
        assert_eq!(ProvenTxReqStatus::DoubleSpend.as_str(), "doubleSpend");
        assert_eq!(ProvenTxReqStatus::NonFinal.as_str(), "nonfinal");
        assert_eq!(ProvenTxReqStatus::Unprocessed.as_str(), "unprocessed");
    }

    #[test]
    fn proven_tx_req_status_serde_roundtrip() {
        let original = ProvenTxReqStatus::InProgress;
        let json_val = serde_json::to_value(original).unwrap();
        assert_eq!(json_val, json!("inProgress"));
        let back: ProvenTxReqStatus = serde_json::from_value(json_val).unwrap();
        assert_eq!(back, ProvenTxReqStatus::InProgress);
    }

    #[test]
    fn proven_tx_req_status_nosend_alias() {
        // The "noSend" alias should deserialize to NoSend.
        let ns: ProvenTxReqStatus = serde_json::from_value(json!("noSend")).unwrap();
        assert_eq!(ns, ProvenTxReqStatus::NoSend);

        // The canonical serialized form is "nosend" (via serde rename).
        let serialized = serde_json::to_value(ProvenTxReqStatus::NoSend).unwrap();
        assert_eq!(serialized, json!("nosend"));
    }

    #[test]
    fn proven_tx_req_status_nonfinal_alias() {
        let nf: ProvenTxReqStatus = serde_json::from_value(json!("nonFinal")).unwrap();
        assert_eq!(nf, ProvenTxReqStatus::NonFinal);

        let serialized = serde_json::to_value(ProvenTxReqStatus::NonFinal).unwrap();
        assert_eq!(serialized, json!("nonfinal"));
    }

    // =========================================================================
    // TableUser
    // =========================================================================

    #[test]
    fn table_user_deserialize_camel_case() {
        let val = json!({
            "userId": 1,
            "identityKey": "abc123",
            "activeStorage": "wallet-infra",
            "createdAt": "2024-06-01T12:00:00Z",
            "updatedAt": "2024-06-01T12:00:00Z"
        });
        let user: TableUser = serde_json::from_value(val).unwrap();
        assert_eq!(user.user_id, 1);
        assert_eq!(user.identity_key, "abc123");
        assert_eq!(user.active_storage, Some("wallet-infra".to_string()));
    }

    #[test]
    fn table_user_null_active_storage() {
        let val = json!({
            "userId": 2,
            "identityKey": "key2",
            "activeStorage": null,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let user: TableUser = serde_json::from_value(val).unwrap();
        assert!(user.active_storage.is_none());
    }

    #[test]
    fn table_user_serialize_roundtrip() {
        let val = json!({
            "userId": 5,
            "identityKey": "roundtrip_key",
            "activeStorage": null,
            "createdAt": "2024-06-15T10:30:00Z",
            "updatedAt": "2024-06-15T10:30:00Z"
        });
        let user: TableUser = serde_json::from_value(val).unwrap();
        let back = serde_json::to_value(&user).unwrap();
        assert_eq!(back["userId"], 5);
        assert_eq!(back["identityKey"], "roundtrip_key");
    }

    // =========================================================================
    // TableSettings
    // =========================================================================

    #[test]
    fn table_settings_default() {
        let s = TableSettings::default();
        assert_eq!(s.settings_id, 1);
        assert_eq!(s.chain, "mainnet");
        assert_eq!(s.max_output_script, 10_000);
        assert!(s.dbtype.is_none());
    }

    #[test]
    fn table_settings_deserialize() {
        let val = json!({
            "settingsId": 1,
            "storageIdentityKey": "key",
            "storageName": "test-store",
            "chain": "testnet",
            "maxOutputScript": 5000,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let s: TableSettings = serde_json::from_value(val).unwrap();
        assert_eq!(s.storage_name, "test-store");
        assert_eq!(s.chain, "testnet");
        assert_eq!(s.max_output_script, 5000);
    }

    #[test]
    fn table_settings_missing_settings_id_uses_default() {
        let val = json!({
            "storageIdentityKey": "key",
            "storageName": "s",
            "chain": "main",
            "maxOutputScript": 100,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let s: TableSettings = serde_json::from_value(val).unwrap();
        assert_eq!(s.settings_id, 1);
    }

    // =========================================================================
    // TableTransaction
    // =========================================================================

    #[test]
    fn table_transaction_default() {
        let tx = TableTransaction::default();
        assert_eq!(tx.transaction_id, 0);
        assert_eq!(tx.user_id, 0);
        assert!(tx.txid.is_none());
        assert_eq!(tx.status, TransactionStatus::Completed); // Default
        assert_eq!(tx.satoshis, 0);
        assert!(!tx.is_outgoing);
        assert!(tx.raw_tx.is_none());
        assert!(tx.input_beef.is_none());
    }

    #[test]
    fn table_transaction_deserialize_full() {
        let val = json!({
            "transactionId": 42,
            "userId": 7,
            "txid": "deadbeef",
            "status": "unproven",
            "reference": "ref-1",
            "description": "test tx",
            "satoshis": 50000,
            "version": 1,
            "lockTime": 0,
            "isOutgoing": true,
            "createdAt": "2024-06-01T00:00:00Z",
            "updatedAt": "2024-06-01T00:00:00Z"
        });
        let tx: TableTransaction = serde_json::from_value(val).unwrap();
        assert_eq!(tx.transaction_id, 42);
        assert_eq!(tx.user_id, 7);
        assert_eq!(tx.txid, Some("deadbeef".to_string()));
        assert_eq!(tx.status, TransactionStatus::Unproven);
        assert_eq!(tx.satoshis, 50000);
        assert!(tx.is_outgoing);
    }

    #[test]
    fn table_transaction_from_empty_json_uses_defaults() {
        // TableTransaction derives Default, and serde(default) on the struct.
        let val = json!({});
        let tx: TableTransaction = serde_json::from_value(val).unwrap();
        assert_eq!(tx.transaction_id, 0);
        assert_eq!(tx.status, TransactionStatus::Completed);
    }

    // =========================================================================
    // TableOutput
    // =========================================================================

    #[test]
    fn table_output_deserialize() {
        let val = json!({
            "outputId": 100,
            "userId": 3,
            "transactionId": 50,
            "basketId": 1,
            "txid": "aabb",
            "vout": 0,
            "satoshis": 1000,
            "scriptLength": 25,
            "scriptOffset": 0,
            "outputType": "P2PKH",
            "providedBy": "you",
            "spendable": true,
            "change": false,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let output: TableOutput = serde_json::from_value(val).unwrap();
        assert_eq!(output.output_id, 100);
        assert_eq!(output.txid, "aabb");
        assert_eq!(output.satoshis, 1000);
        assert!(output.spendable);
        assert!(!output.change);
        assert_eq!(output.basket_id, Some(1));
    }

    #[test]
    fn table_output_null_optional_fields() {
        let val = json!({
            "outputId": 1,
            "userId": 1,
            "transactionId": 1,
            "basketId": null,
            "txid": "tx",
            "vout": 0,
            "satoshis": 0,
            "scriptLength": 0,
            "scriptOffset": 0,
            "outputType": "",
            "providedBy": "",
            "spendable": false,
            "change": false,
            "purpose": null,
            "outputDescription": null,
            "spentBy": null,
            "sequenceNumber": null,
            "spendingDescription": null,
            "derivationPrefix": null,
            "derivationSuffix": null,
            "senderIdentityKey": null,
            "customInstructions": null,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let output: TableOutput = serde_json::from_value(val).unwrap();
        assert!(output.basket_id.is_none());
        assert!(output.purpose.is_none());
        assert!(output.spent_by.is_none());
        assert!(output.derivation_prefix.is_none());
        assert!(output.custom_instructions.is_none());
    }

    // =========================================================================
    // TableOutputBasket
    // =========================================================================

    #[test]
    fn table_output_basket_deserialize() {
        let val = json!({
            "basketId": 1,
            "userId": 1,
            "name": "default",
            "numberOfDesiredUtxos": 6,
            "minimumDesiredUtxoValue": 10000,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let basket: TableOutputBasket = serde_json::from_value(val).unwrap();
        assert_eq!(basket.name, "default");
        assert_eq!(basket.number_of_desired_utxos, 6);
        assert_eq!(basket.minimum_desired_utxo_value, 10000);
    }

    // =========================================================================
    // TableProvenTxReq
    // =========================================================================

    #[test]
    fn table_proven_tx_req_defaults() {
        let val = json!({
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let req: TableProvenTxReq = serde_json::from_value(val).unwrap();
        assert_eq!(req.proven_tx_req_id, 0);
        assert_eq!(req.txid, "");
        assert_eq!(req.status, ProvenTxReqStatus::Pending);
        assert_eq!(req.attempts, 0);
        assert!(!req.notified);
        assert!(req.raw_tx.is_none());
        assert!(req.proven_tx_id.is_none());
    }

    #[test]
    fn table_proven_tx_req_full() {
        let val = json!({
            "provenTxReqId": 10,
            "txid": "abc",
            "status": "completed",
            "attempts": 3,
            "history": "attempted 3 times",
            "notified": true,
            "notify": "webhook",
            "provenTxId": 5,
            "batch": "batch-1",
            "createdAt": "2024-06-01T00:00:00Z",
            "updatedAt": "2024-06-01T00:00:00Z"
        });
        let req: TableProvenTxReq = serde_json::from_value(val).unwrap();
        assert_eq!(req.proven_tx_req_id, 10);
        assert_eq!(req.status, ProvenTxReqStatus::Completed);
        assert_eq!(req.attempts, 3);
        assert!(req.notified);
        assert_eq!(req.proven_tx_id, Some(5));
        assert_eq!(req.batch, Some("batch-1".to_string()));
    }

    // =========================================================================
    // TableCertificate
    // =========================================================================

    #[test]
    fn table_certificate_deserialize() {
        let val = json!({
            "certificateId": 1,
            "userId": 2,
            "certType": "identity",
            "serialNumber": "sn-001",
            "certifier": "certifier_key",
            "subject": "subject_key",
            "verifier": null,
            "revocationOutpoint": "txid.0",
            "signature": "sig_hex",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let cert: TableCertificate = serde_json::from_value(val).unwrap();
        assert_eq!(cert.certificate_id, 1);
        assert_eq!(cert.cert_type, "identity");
        assert_eq!(cert.serial_number, "sn-001");
        assert!(cert.verifier.is_none());
    }

    // =========================================================================
    // TableCertificateField
    // =========================================================================

    #[test]
    fn table_certificate_field_deserialize() {
        let val = json!({
            "certificateFieldId": 10,
            "certificateId": 1,
            "userId": 2,
            "fieldName": "name",
            "fieldValue": "encrypted_value",
            "masterKey": "mk_hex",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let field: TableCertificateField = serde_json::from_value(val).unwrap();
        assert_eq!(field.field_name, "name");
        assert_eq!(field.field_value, "encrypted_value");
    }

    // =========================================================================
    // TableTxLabel / TableOutputTag
    // =========================================================================

    #[test]
    fn table_tx_label_deserialize() {
        let val = json!({
            "labelId": 1,
            "userId": 1,
            "label": "payment",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let label: TableTxLabel = serde_json::from_value(val).unwrap();
        assert_eq!(label.label, "payment");
    }

    #[test]
    fn table_output_tag_deserialize() {
        let val = json!({
            "tagId": 1,
            "userId": 1,
            "tag": "redeemable",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let tag: TableOutputTag = serde_json::from_value(val).unwrap();
        assert_eq!(tag.tag, "redeemable");
    }

    // =========================================================================
    // TableCommission
    // =========================================================================

    #[test]
    fn table_commission_deserialize() {
        let val = json!({
            "commissionId": 1,
            "userId": 1,
            "transactionId": 10,
            "satoshis": 500,
            "payerLockingScript": [118, 169],
            "keyOffset": "offset_hex",
            "isRedeemed": false,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let c: TableCommission = serde_json::from_value(val).unwrap();
        assert_eq!(c.satoshis, 500);
        assert!(!c.is_redeemed);
        assert_eq!(c.payer_locking_script, vec![118, 169]);
    }

    // =========================================================================
    // TableMonitorEvent
    // =========================================================================

    #[test]
    fn table_monitor_event_deserialize() {
        let val = json!({
            "eventId": 1,
            "eventType": "proof_found",
            "eventData": "{\"txid\":\"abc\"}",
            "createdAt": "2024-06-01T00:00:00Z"
        });
        let event: TableMonitorEvent = serde_json::from_value(val).unwrap();
        assert_eq!(event.event_type, "proof_found");
        assert_eq!(event.event_data, "{\"txid\":\"abc\"}");
    }

    // =========================================================================
    // D1 float behavior: i64 fields from f64 JSON values
    // =========================================================================

    #[test]
    fn table_user_accepts_integer_json() {
        // D1 returns numbers as floats, but serde_json integers deserialize to i64 fine.
        // This tests that normal integer JSON values work with our i64 fields.
        let val = json!({
            "userId": 999,
            "identityKey": "key",
            "activeStorage": null,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let user: TableUser = serde_json::from_value(val).unwrap();
        assert_eq!(user.user_id, 999);
    }

    // =========================================================================
    // Datetime alias: created_at / updated_at accept snake_case
    // =========================================================================

    #[test]
    fn table_user_accepts_snake_case_datetime() {
        let val = json!({
            "userId": 1,
            "identityKey": "key",
            "activeStorage": null,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        });
        let user: TableUser = serde_json::from_value(val).unwrap();
        assert_eq!(user.user_id, 1);
    }

    // =========================================================================
    // TableSyncState
    // =========================================================================

    #[test]
    fn table_sync_state_deserialize() {
        let val = json!({
            "syncStateId": 1,
            "userId": 1,
            "storageIdentityKey": "key",
            "storageName": "wallet-infra",
            "status": "active",
            "init": true,
            "refNum": "ref-1",
            "syncMap": "{}",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        });
        let state: TableSyncState = serde_json::from_value(val).unwrap();
        assert_eq!(state.storage_name, "wallet-infra");
        assert!(state.init);
        assert!(state.when_last_sync_started.is_none());
        assert!(state.satoshis.is_none());
    }
}
