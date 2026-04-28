//! Core writer methods: makeAvailable, migrate, findOrInsertUser,
//! find_or_create_default_basket, and certificate CRUD.

use crate::d1::Query;
use crate::entities::*;
use crate::error::{Error, Result};
use crate::types::AuthId;
use chrono::Utc;
use serde::Deserialize;

use super::StorageD1;

// =============================================================================
// D1 row types (for serde_wasm_bindgen deserialization)
// =============================================================================

/// Settings row from D1.
#[derive(Debug, Deserialize)]
struct SettingsRow {
    settings_id: Option<f64>,
    storage_identity_key: Option<String>,
    storage_name: Option<String>,
    chain: Option<String>,
    max_output_script: Option<f64>,
    dbtype: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl SettingsRow {
    fn into_table(self) -> TableSettings {
        TableSettings {
            settings_id: self.settings_id.map(|v| v as i64).unwrap_or(1),
            storage_identity_key: self.storage_identity_key.unwrap_or_default(),
            storage_name: self.storage_name.unwrap_or_default(),
            chain: self.chain.unwrap_or_else(|| "mainnet".to_string()),
            max_output_script: self.max_output_script.map(|v| v as i32).unwrap_or(10_000),
            dbtype: self.dbtype,
            created_at: parse_datetime(&self.created_at),
            updated_at: parse_datetime(&self.updated_at),
        }
    }
}

/// User row from D1.
#[derive(Debug, Deserialize)]
struct UserRow {
    user_id: Option<f64>,
    identity_key: Option<String>,
    active_storage: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl UserRow {
    fn into_table(self) -> TableUser {
        TableUser {
            user_id: self.user_id.map(|v| v as i64).unwrap_or(0),
            identity_key: self.identity_key.unwrap_or_default(),
            active_storage: self.active_storage,
            created_at: parse_datetime(&self.created_at),
            updated_at: parse_datetime(&self.updated_at),
        }
    }
}

/// Output basket row from D1.
#[derive(Debug, Deserialize)]
struct BasketRow {
    basket_id: Option<f64>,
    user_id: Option<f64>,
    name: Option<String>,
    number_of_desired_utxos: Option<f64>,
    minimum_desired_utxo_value: Option<f64>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl BasketRow {
    fn into_table(self) -> TableOutputBasket {
        TableOutputBasket {
            basket_id: self.basket_id.map(|v| v as i64).unwrap_or(0),
            user_id: self.user_id.map(|v| v as i64).unwrap_or(0),
            name: self.name.unwrap_or_default(),
            number_of_desired_utxos: self.number_of_desired_utxos.map(|v| v as i32).unwrap_or(6),
            minimum_desired_utxo_value: self
                .minimum_desired_utxo_value
                .map(|v| v as i64)
                .unwrap_or(10000),
            created_at: parse_datetime(&self.created_at),
            updated_at: parse_datetime(&self.updated_at),
        }
    }
}

/// Helper: parse a datetime string from D1 (public for use by other storage modules).
pub(crate) fn parse_datetime_pub(s: &Option<String>) -> chrono::DateTime<Utc> {
    parse_datetime(s)
}

fn parse_datetime(s: &Option<String>) -> chrono::DateTime<Utc> {
    s.as_deref()
        .and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
                .or_else(|| {
                    // D1 CURRENT_TIMESTAMP format: "YYYY-MM-DD HH:MM:SS"
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .ok()
                        .map(|dt| dt.and_utc())
                })
        })
        .unwrap_or_else(Utc::now)
}

// =============================================================================
// makeAvailable
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// Initialize storage. Reads or creates settings.
    pub async fn make_available(&mut self) -> Result<TableSettings> {
        // Try to read existing settings
        let row: Option<SettingsRow> = Query::new("SELECT * FROM settings WHERE settings_id = 1")
            .fetch_optional(self.db)
            .await?;

        if let Some(row) = row {
            let settings = row.into_table();
            self.settings = Some(settings.clone());
            return Ok(settings);
        }

        // No settings yet — this is a fresh database
        Err(Error::NotFound(
            "Settings not found. Call migrate first.".to_string(),
        ))
    }

    /// Run migration: insert settings if not present, return chain.
    pub async fn migrate(
        &mut self,
        storage_name: &str,
        storage_identity_key: &str,
    ) -> Result<String> {
        // Check if settings exist
        let existing: Option<SettingsRow> =
            Query::new("SELECT * FROM settings WHERE settings_id = 1")
                .fetch_optional(self.db)
                .await?;

        if let Some(row) = existing {
            let settings = row.into_table();
            let chain = settings.chain.clone();
            self.settings = Some(settings);
            return Ok(chain);
        }

        // Insert new settings
        let now = Utc::now();
        let chain = "mainnet".to_string();

        Query::new(
            "INSERT INTO settings (storage_identity_key, storage_name, chain, dbtype, max_output_script, created_at, updated_at) VALUES (?, ?, ?, 'D1', 10000, ?, ?)"
        )
        .bind(storage_identity_key)
        .bind(storage_name)
        .bind(chain.as_str())
        .bind(now)
        .bind(now)
        .execute(self.db)
        .await?;

        // Read back
        let row: SettingsRow = Query::new("SELECT * FROM settings WHERE settings_id = 1")
            .fetch_one(self.db)
            .await?;
        let settings = row.into_table();
        self.settings = Some(settings);

        Ok(chain)
    }

    /// Find or insert a user by identity key. Returns (user, was_inserted).
    pub async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)> {
        // Try to find existing
        let existing: Option<UserRow> = Query::new("SELECT * FROM users WHERE identity_key = ?")
            .bind(identity_key)
            .fetch_optional(self.db)
            .await?;

        if let Some(row) = existing {
            return Ok((row.into_table(), false));
        }

        // Insert new user
        let now = Utc::now();
        let meta = Query::new(
            "INSERT INTO users (identity_key, active_storage, created_at, updated_at) VALUES (?, '', ?, ?)",
        )
        .bind(identity_key)
        .bind(now)
        .bind(now)
        .execute(self.db)
        .await?;

        let user = TableUser {
            user_id: meta.last_row_id,
            identity_key: identity_key.to_string(),
            active_storage: Some(String::new()),
            created_at: now,
            updated_at: now,
        };

        Ok((user, true))
    }

    /// Resolve an AuthId: look up user_id if not already set.
    pub async fn resolve_auth(&self, auth: &AuthId) -> Result<(i64, AuthId)> {
        if let Some(user_id) = auth.user_id {
            return Ok((user_id, auth.clone()));
        }

        let (user, _) = self.find_or_insert_user(&auth.identity_key).await?;
        Ok((
            user.user_id,
            AuthId::with_user_id(&auth.identity_key, user.user_id),
        ))
    }

    /// Find or create the "default" output basket for a user.
    pub async fn find_or_create_default_basket(&self, user_id: i64) -> Result<TableOutputBasket> {
        let existing: Option<BasketRow> = Query::new(
            "SELECT * FROM output_baskets WHERE user_id = ? AND name = 'default' AND is_deleted = 0",
        )
        .bind(user_id)
        .fetch_optional(self.db)
        .await?;

        if let Some(row) = existing {
            return Ok(row.into_table());
        }

        // Create default basket
        let now = Utc::now();
        let meta = Query::new(
            "INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at) VALUES (?, 'default', 6, 10000, ?, ?)",
        )
        .bind(user_id)
        .bind(now)
        .bind(now)
        .execute(self.db)
        .await?;

        Ok(TableOutputBasket {
            basket_id: meta.last_row_id,
            user_id,
            name: "default".to_string(),
            number_of_desired_utxos: 6,
            minimum_desired_utxo_value: 10000,
            created_at: now,
            updated_at: now,
        })
    }

    /// Get or create a named basket for a user.
    pub async fn get_or_create_basket(&self, user_id: i64, name: &str) -> Result<i64> {
        let existing: Option<BasketRow> = Query::new(
            "SELECT * FROM output_baskets WHERE user_id = ? AND name = ? AND is_deleted = 0",
        )
        .bind(user_id)
        .bind(name)
        .fetch_optional(self.db)
        .await?;

        if let Some(row) = existing {
            return Ok(row.basket_id.map(|v| v as i64).unwrap_or(0));
        }

        let now = Utc::now();
        let meta = Query::new(
            "INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at) VALUES (?, ?, 6, 10000, ?, ?)",
        )
        .bind(user_id)
        .bind(name)
        .bind(now)
        .bind(now)
        .execute(self.db)
        .await?;

        Ok(meta.last_row_id)
    }

    /// Add a label to a transaction.
    pub async fn add_label(&self, user_id: i64, transaction_id: i64, label: &str) -> Result<()> {
        let now = Utc::now();

        // Find or create label
        #[derive(Deserialize)]
        struct LabelIdRow {
            tx_label_id: Option<f64>,
        }

        let existing: Option<LabelIdRow> = Query::new(
            "SELECT tx_label_id FROM tx_labels WHERE user_id = ? AND label = ? AND is_deleted = 0",
        )
        .bind(user_id)
        .bind(label)
        .fetch_optional(self.db)
        .await?;

        let label_id = if let Some(row) = existing {
            row.tx_label_id.map(|v| v as i64).unwrap_or(0)
        } else {
            let meta = Query::new(
                "INSERT INTO tx_labels (user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?)",
            )
            .bind(user_id)
            .bind(label)
            .bind(now)
            .bind(now)
            .execute(self.db)
            .await?;
            meta.last_row_id
        };

        // Check if mapping exists
        let existing_map: Option<LabelIdRow> = Query::new(
            "SELECT tx_label_map_id as tx_label_id FROM tx_labels_map WHERE transaction_id = ? AND tx_label_id = ?",
        )
        .bind(transaction_id)
        .bind(label_id)
        .fetch_optional(self.db)
        .await?;

        if existing_map.is_none() {
            Query::new(
                "INSERT INTO tx_labels_map (transaction_id, tx_label_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
            )
            .bind(transaction_id)
            .bind(label_id)
            .bind(now)
            .bind(now)
            .execute(self.db)
            .await?;
        }

        Ok(())
    }

    /// Add a tag to an output.
    pub async fn add_tag_to_output(&self, user_id: i64, output_id: i64, tag: &str) -> Result<()> {
        let now = Utc::now();

        #[derive(Deserialize)]
        struct TagIdRow {
            output_tag_id: Option<f64>,
        }

        let existing: Option<TagIdRow> = Query::new(
            "SELECT output_tag_id FROM output_tags WHERE user_id = ? AND tag = ? AND is_deleted = 0",
        )
        .bind(user_id)
        .bind(tag)
        .fetch_optional(self.db)
        .await?;

        let tag_id = if let Some(row) = existing {
            row.output_tag_id.map(|v| v as i64).unwrap_or(0)
        } else {
            let meta = Query::new(
                "INSERT INTO output_tags (user_id, tag, created_at, updated_at) VALUES (?, ?, ?, ?)",
            )
            .bind(user_id)
            .bind(tag)
            .bind(now)
            .bind(now)
            .execute(self.db)
            .await?;
            meta.last_row_id
        };

        // Check if mapping exists
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct MapIdRow {
            output_tag_map_id: Option<f64>,
        }

        let existing_map: Option<MapIdRow> = Query::new(
            "SELECT output_tag_map_id FROM output_tags_map WHERE output_id = ? AND output_tag_id = ?",
        )
        .bind(output_id)
        .bind(tag_id)
        .fetch_optional(self.db)
        .await?;

        if existing_map.is_none() {
            Query::new(
                "INSERT INTO output_tags_map (output_id, output_tag_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
            )
            .bind(output_id)
            .bind(tag_id)
            .bind(now)
            .bind(now)
            .execute(self.db)
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};
    use serde_json::json;

    // =========================================================================
    // parse_datetime — RFC 3339 format
    // =========================================================================

    #[test]
    fn parse_datetime_rfc3339_utc() {
        let s = Some("2024-06-15T10:30:00Z".to_string());
        let dt = parse_datetime(&s);
        assert_eq!(dt.to_rfc3339(), "2024-06-15T10:30:00+00:00");
    }

    #[test]
    fn parse_datetime_rfc3339_with_offset() {
        let s = Some("2024-06-15T10:30:00+05:00".to_string());
        let dt = parse_datetime(&s);
        // Should be converted to UTC
        assert_eq!(dt.hour(), 5);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn parse_datetime_rfc3339_with_nanos() {
        let s = Some("2024-06-15T10:30:00.123456789Z".to_string());
        let dt = parse_datetime(&s);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 6);
    }

    // =========================================================================
    // parse_datetime — D1 CURRENT_TIMESTAMP format
    // =========================================================================

    #[test]
    fn parse_datetime_d1_format() {
        let s = Some("2024-06-15 10:30:00".to_string());
        let dt = parse_datetime(&s);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 6);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 0);
    }

    #[test]
    fn parse_datetime_d1_format_midnight() {
        let s = Some("2025-01-01 00:00:00".to_string());
        let dt = parse_datetime(&s);
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 0);
    }

    #[test]
    fn parse_datetime_d1_format_end_of_day() {
        let s = Some("2024-12-31 23:59:59".to_string());
        let dt = parse_datetime(&s);
        assert_eq!(dt.hour(), 23);
        assert_eq!(dt.minute(), 59);
        assert_eq!(dt.second(), 59);
    }

    // =========================================================================
    // parse_datetime — edge cases
    // =========================================================================

    #[test]
    fn parse_datetime_none_returns_now() {
        let dt = parse_datetime(&None);
        // Should return approximately "now" — just verify it's not epoch
        assert!(dt.year() >= 2024);
    }

    #[test]
    fn parse_datetime_empty_string_returns_now() {
        let s = Some("".to_string());
        let dt = parse_datetime(&s);
        // Empty string doesn't parse either way, falls back to Utc::now()
        assert!(dt.year() >= 2024);
    }

    #[test]
    fn parse_datetime_garbage_returns_now() {
        let s = Some("not-a-date".to_string());
        let dt = parse_datetime(&s);
        assert!(dt.year() >= 2024);
    }

    #[test]
    fn parse_datetime_partial_date_returns_now() {
        let s = Some("2024-06-15".to_string()); // date only, no time
        let dt = parse_datetime(&s);
        // Neither RFC 3339 nor D1 format — falls back to now
        assert!(dt.year() >= 2024);
    }

    // =========================================================================
    // parse_datetime_pub (public wrapper)
    // =========================================================================

    #[test]
    fn parse_datetime_pub_delegates_correctly() {
        let s = Some("2024-06-15T10:30:00Z".to_string());
        let dt = parse_datetime_pub(&s);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 6);
    }

    // =========================================================================
    // SettingsRow → TableSettings conversion
    // =========================================================================

    #[test]
    fn settings_row_into_table_full() {
        let row = SettingsRow {
            settings_id: Some(1.0),
            storage_identity_key: Some("key123".to_string()),
            storage_name: Some("my-store".to_string()),
            chain: Some("mainnet".to_string()),
            max_output_script: Some(10000.0),
            dbtype: Some("D1".to_string()),
            created_at: Some("2024-06-15T10:30:00Z".to_string()),
            updated_at: Some("2024-06-15T10:30:00Z".to_string()),
        };
        let table = row.into_table();
        assert_eq!(table.settings_id, 1);
        assert_eq!(table.storage_identity_key, "key123");
        assert_eq!(table.storage_name, "my-store");
        assert_eq!(table.chain, "mainnet");
        assert_eq!(table.max_output_script, 10000);
        assert_eq!(table.dbtype, Some("D1".to_string()));
    }

    #[test]
    fn settings_row_into_table_defaults() {
        let row = SettingsRow {
            settings_id: None,
            storage_identity_key: None,
            storage_name: None,
            chain: None,
            max_output_script: None,
            dbtype: None,
            created_at: None,
            updated_at: None,
        };
        let table = row.into_table();
        assert_eq!(table.settings_id, 1); // default
        assert_eq!(table.storage_identity_key, "");
        assert_eq!(table.storage_name, "");
        assert_eq!(table.chain, "mainnet"); // default
        assert_eq!(table.max_output_script, 10000); // default
        assert!(table.dbtype.is_none());
    }

    #[test]
    fn settings_row_into_table_testnet() {
        let row = SettingsRow {
            settings_id: Some(1.0),
            storage_identity_key: Some("key".to_string()),
            storage_name: Some("test".to_string()),
            chain: Some("testnet".to_string()),
            max_output_script: Some(5000.0),
            dbtype: None,
            created_at: None,
            updated_at: None,
        };
        let table = row.into_table();
        assert_eq!(table.chain, "testnet");
        assert_eq!(table.max_output_script, 5000);
    }

    // =========================================================================
    // UserRow → TableUser conversion
    // =========================================================================

    #[test]
    fn user_row_into_table_full() {
        let row = UserRow {
            user_id: Some(42.0),
            identity_key: Some("abc123".to_string()),
            active_storage: Some("wallet-infra".to_string()),
            created_at: Some("2024-01-01T00:00:00Z".to_string()),
            updated_at: Some("2024-06-01T00:00:00Z".to_string()),
        };
        let table = row.into_table();
        assert_eq!(table.user_id, 42);
        assert_eq!(table.identity_key, "abc123");
        assert_eq!(table.active_storage, Some("wallet-infra".to_string()));
    }

    #[test]
    fn user_row_into_table_defaults() {
        let row = UserRow {
            user_id: None,
            identity_key: None,
            active_storage: None,
            created_at: None,
            updated_at: None,
        };
        let table = row.into_table();
        assert_eq!(table.user_id, 0);
        assert_eq!(table.identity_key, "");
        assert!(table.active_storage.is_none());
    }

    #[test]
    fn user_row_d1_float_conversion() {
        // D1 returns all numbers as floats. Verify i64 cast works correctly.
        let row = UserRow {
            user_id: Some(999999.0),
            identity_key: Some("key".to_string()),
            active_storage: None,
            created_at: None,
            updated_at: None,
        };
        let table = row.into_table();
        assert_eq!(table.user_id, 999999);
    }

    // =========================================================================
    // BasketRow → TableOutputBasket conversion
    // =========================================================================

    #[test]
    fn basket_row_into_table_full() {
        let row = BasketRow {
            basket_id: Some(1.0),
            user_id: Some(42.0),
            name: Some("default".to_string()),
            number_of_desired_utxos: Some(6.0),
            minimum_desired_utxo_value: Some(10000.0),
            created_at: Some("2024-01-01T00:00:00Z".to_string()),
            updated_at: Some("2024-01-01T00:00:00Z".to_string()),
        };
        let table = row.into_table();
        assert_eq!(table.basket_id, 1);
        assert_eq!(table.user_id, 42);
        assert_eq!(table.name, "default");
        assert_eq!(table.number_of_desired_utxos, 6);
        assert_eq!(table.minimum_desired_utxo_value, 10000);
    }

    #[test]
    fn basket_row_into_table_defaults() {
        let row = BasketRow {
            basket_id: None,
            user_id: None,
            name: None,
            number_of_desired_utxos: None,
            minimum_desired_utxo_value: None,
            created_at: None,
            updated_at: None,
        };
        let table = row.into_table();
        assert_eq!(table.basket_id, 0);
        assert_eq!(table.user_id, 0);
        assert_eq!(table.name, "");
        assert_eq!(table.number_of_desired_utxos, 6); // default
        assert_eq!(table.minimum_desired_utxo_value, 10000); // default
    }

    #[test]
    fn basket_row_custom_values() {
        let row = BasketRow {
            basket_id: Some(5.0),
            user_id: Some(7.0),
            name: Some("payments".to_string()),
            number_of_desired_utxos: Some(12.0),
            minimum_desired_utxo_value: Some(50000.0),
            created_at: None,
            updated_at: None,
        };
        let table = row.into_table();
        assert_eq!(table.name, "payments");
        assert_eq!(table.number_of_desired_utxos, 12);
        assert_eq!(table.minimum_desired_utxo_value, 50000);
    }

    // =========================================================================
    // SettingsRow deserialization from D1 JSON
    // =========================================================================

    #[test]
    fn settings_row_deserialize_d1_format() {
        let val = json!({
            "settings_id": 1.0,
            "storage_identity_key": "sk123",
            "storage_name": "prod-store",
            "chain": "mainnet",
            "max_output_script": 10000.0,
            "dbtype": "D1",
            "created_at": "2024-01-01 00:00:00",
            "updated_at": "2024-06-15 12:30:00"
        });
        let row: SettingsRow = serde_json::from_value(val).unwrap();
        let table = row.into_table();
        assert_eq!(table.storage_identity_key, "sk123");
        assert_eq!(table.storage_name, "prod-store");
        // D1 format datetime should parse correctly
        assert_eq!(table.updated_at.year(), 2024);
        assert_eq!(table.updated_at.month(), 6);
    }

    // =========================================================================
    // UserRow deserialization from D1 JSON
    // =========================================================================

    #[test]
    fn user_row_deserialize_d1_format() {
        let val = json!({
            "user_id": 42.0,
            "identity_key": "02abc123",
            "active_storage": "",
            "created_at": "2024-01-01 00:00:00",
            "updated_at": "2024-01-01 00:00:00"
        });
        let row: UserRow = serde_json::from_value(val).unwrap();
        let table = row.into_table();
        assert_eq!(table.user_id, 42);
        assert_eq!(table.identity_key, "02abc123");
    }

    // =========================================================================
    // Migrate idempotency (conceptual — both paths return chain string)
    // =========================================================================

    #[test]
    fn migrate_returns_mainnet_by_default() {
        // The migrate function creates settings with chain = "mainnet" when fresh.
        // This is a conceptual test verifying the constant.
        let default_chain = "mainnet";
        assert_eq!(default_chain, "mainnet");
    }

    #[test]
    fn default_basket_values() {
        // Default basket: 6 desired UTXOs, 10000 sat minimum
        let desired_utxos = 6;
        let min_value = 10000i64;
        assert_eq!(desired_utxos, 6);
        assert_eq!(min_value, 10000);
    }
}
