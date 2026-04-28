//! Certificate CRUD methods: listCertificates, insertCertificate, relinquishCertificate.

use crate::d1::batch::BatchCollector;
use crate::d1::{QVal, Query, WhereBuilder};
use crate::entities::{TableCertificate, TableCertificateField};
use crate::error::{Error, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::StorageD1;

// =============================================================================
// D1 row types (D1 returns all numbers as JS floats)
// =============================================================================

/// Certificate row from D1. The DB column is `type` but Rust reserves that keyword,
/// so we deserialize it via `#[serde(rename)]`.
#[derive(Debug, Deserialize)]
struct CertificateRow {
    certificate_id: Option<f64>,
    user_id: Option<f64>,
    #[serde(rename = "type")]
    cert_type: Option<String>,
    serial_number: Option<String>,
    certifier: Option<String>,
    subject: Option<String>,
    verifier: Option<String>,
    revocation_outpoint: Option<String>,
    signature: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl CertificateRow {
    fn into_table(self) -> TableCertificate {
        TableCertificate {
            certificate_id: self.certificate_id.map(|v| v as i64).unwrap_or(0),
            user_id: self.user_id.map(|v| v as i64).unwrap_or(0),
            cert_type: self.cert_type.unwrap_or_default(),
            serial_number: self.serial_number.unwrap_or_default(),
            certifier: self.certifier.unwrap_or_default(),
            subject: self.subject.unwrap_or_default(),
            verifier: self.verifier,
            revocation_outpoint: self.revocation_outpoint.unwrap_or_else(|| {
                "0000000000000000000000000000000000000000000000000000000000000000.0".to_string()
            }),
            signature: self.signature.unwrap_or_default(),
            created_at: super::writers::parse_datetime_pub(&self.created_at),
            updated_at: super::writers::parse_datetime_pub(&self.updated_at),
        }
    }
}

/// Certificate field row from D1.
#[derive(Debug, Deserialize)]
struct CertificateFieldRow {
    certificate_field_id: Option<f64>,
    certificate_id: Option<f64>,
    user_id: Option<f64>,
    field_name: Option<String>,
    field_value: Option<String>,
    master_key: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl CertificateFieldRow {
    fn into_table(self) -> TableCertificateField {
        TableCertificateField {
            certificate_field_id: self.certificate_field_id.map(|v| v as i64).unwrap_or(0),
            certificate_id: self.certificate_id.map(|v| v as i64).unwrap_or(0),
            user_id: self.user_id.map(|v| v as i64).unwrap_or(0),
            field_name: self.field_name.unwrap_or_default(),
            field_value: self.field_value.unwrap_or_default(),
            master_key: self.master_key.unwrap_or_default(),
            created_at: super::writers::parse_datetime_pub(&self.created_at),
            updated_at: super::writers::parse_datetime_pub(&self.updated_at),
        }
    }
}

// =============================================================================
// Input / output types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertCertificateArgs {
    /// The DB column is `type`. Callers send `"type"` in JSON.
    #[serde(alias = "type", alias = "certType")]
    pub cert_type: String,
    pub serial_number: String,
    pub certifier: String,
    pub subject: String,
    pub verifier: Option<String>,
    pub revocation_outpoint: Option<String>,
    pub signature: Option<String>,
    pub fields: Option<Vec<CertificateFieldInput>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateFieldInput {
    pub field_name: String,
    pub field_value: String,
    pub master_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateResult {
    #[serde(flatten)]
    pub certificate: TableCertificate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<TableCertificateField>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCertificatesResult {
    pub certificates: Vec<CertificateResult>,
    pub total_certificates: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelinquishCertificateArgs {
    pub serial_number: Option<String>,
    pub certificate_id: Option<i64>,
}

// =============================================================================
// Storage methods
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// List certificates for a user, optionally filtered by certifiers and types.
    pub async fn list_certificates(
        &self,
        user_id: i64,
        args: crate::types::FindCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        let include_fields = args.include_fields.unwrap_or(false);
        let order_desc = args.base.order_descending.unwrap_or(true);
        let order_dir = if order_desc { "DESC" } else { "ASC" };

        // Build WHERE clause
        let mut wb = WhereBuilder::new()
            .eq("user_id", user_id)
            .eq("is_deleted", 0i32);

        if let Some(ref certifiers) = args.certifiers {
            if !certifiers.is_empty() {
                let vals: Vec<QVal> = certifiers.iter().map(|c| QVal::Text(c.clone())).collect();
                wb = wb.in_list("certifier", vals);
            }
        }

        if let Some(ref types) = args.types {
            if !types.is_empty() {
                let vals: Vec<QVal> = types.iter().map(|t| QVal::Text(t.clone())).collect();
                wb = wb.in_list("type", vals);
            }
        }

        if let Some(ref since) = args.base.since {
            wb = wb.gte("updated_at", *since);
        }

        let (where_clause, params) = wb.build();

        // Count query
        let count_sql = format!("SELECT COUNT(*) as total FROM certificates{}", where_clause);
        let mut count_query = Query::new(&count_sql);
        for p in &params {
            count_query = count_query.bind(match p {
                QVal::Int(v) => QVal::Int(*v),
                QVal::Text(v) => QVal::Text(v.clone()),
                QVal::Bool(v) => QVal::Bool(*v),
                QVal::Float(v) => QVal::Float(*v),
                QVal::Null => QVal::Null,
                QVal::Blob(v) => QVal::Blob(v.clone()),
            });
        }
        let total: f64 = count_query.fetch_scalar(self.db).await?;
        let total_certificates = total as u32;

        // Main query
        let mut sql = format!(
            "SELECT * FROM certificates{} ORDER BY created_at {}",
            where_clause, order_dir
        );

        if let Some(ref paged) = args.base.paged {
            let limit = paged.limit.unwrap_or(100);
            let offset = paged.offset.unwrap_or(0);
            sql.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));
        }

        let mut query = Query::new(&sql);
        for p in params {
            query = query.bind(p);
        }

        let rows: Vec<CertificateRow> = query.fetch_all(self.db).await?;
        let certs: Vec<TableCertificate> = rows.into_iter().map(|r| r.into_table()).collect();

        // Optionally fetch fields for each certificate
        let mut results = Vec::with_capacity(certs.len());
        if include_fields && !certs.is_empty() {
            // Batch fetch all fields for all cert IDs at once
            let cert_ids: Vec<i64> = certs.iter().map(|c| c.certificate_id).collect();
            let fields_map = self.fetch_certificate_fields_batch(&cert_ids).await?;

            for cert in certs {
                let fields = fields_map
                    .get(&cert.certificate_id)
                    .cloned()
                    .unwrap_or_default();
                results.push(CertificateResult {
                    certificate: cert,
                    fields: Some(fields),
                });
            }
        } else {
            for cert in certs {
                results.push(CertificateResult {
                    certificate: cert,
                    fields: None,
                });
            }
        }

        Ok(ListCertificatesResult {
            certificates: results,
            total_certificates,
        })
    }

    /// Fetch certificate fields for a batch of certificate IDs.
    async fn fetch_certificate_fields_batch(
        &self,
        cert_ids: &[i64],
    ) -> Result<std::collections::HashMap<i64, Vec<TableCertificateField>>> {
        use std::collections::HashMap;

        if cert_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders: Vec<&str> = cert_ids.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT * FROM certificate_fields WHERE certificate_id IN ({})",
            placeholders.join(", ")
        );

        let mut query = Query::new(&sql);
        for id in cert_ids {
            query = query.bind(*id);
        }

        let rows: Vec<CertificateFieldRow> = query.fetch_all(self.db).await?;
        let mut map: HashMap<i64, Vec<TableCertificateField>> = HashMap::new();
        for row in rows {
            let field = row.into_table();
            map.entry(field.certificate_id).or_default().push(field);
        }

        Ok(map)
    }

    /// Insert a new certificate and optionally its fields.
    pub async fn insert_certificate(
        &self,
        user_id: i64,
        args: InsertCertificateArgs,
    ) -> Result<CertificateResult> {
        let now = Utc::now();
        let revocation_outpoint = args.revocation_outpoint.unwrap_or_else(|| {
            "0000000000000000000000000000000000000000000000000000000000000000.0".to_string()
        });
        let signature = args.signature.unwrap_or_default();

        let mut batch = BatchCollector::new(self.db);

        // INSERT certificate
        batch.add(
            "INSERT INTO certificates (user_id, type, serial_number, certifier, subject, verifier, revocation_outpoint, signature, is_deleted, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
            vec![
                QVal::Int(user_id),
                QVal::Text(args.cert_type.clone()),
                QVal::Text(args.serial_number.clone()),
                QVal::Text(args.certifier.clone()),
                QVal::Text(args.subject.clone()),
                args.verifier.clone().map(QVal::Text).unwrap_or(QVal::Null),
                QVal::Text(revocation_outpoint.clone()),
                QVal::Text(signature.clone()),
                QVal::from(now),
                QVal::from(now),
            ],
        )?;

        // Execute the certificate insert first to get the ID
        let cert_id = batch.execute_returning_last_id().await?;

        // Insert fields if provided
        let fields = if let Some(ref field_inputs) = args.fields {
            if !field_inputs.is_empty() {
                let mut field_batch = BatchCollector::new(self.db);
                let mut fields = Vec::with_capacity(field_inputs.len());

                for input in field_inputs {
                    let master_key = input.master_key.clone().unwrap_or_default();
                    field_batch.add(
                        "INSERT INTO certificate_fields (certificate_id, user_id, field_name, field_value, master_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        vec![
                            QVal::Int(cert_id),
                            QVal::Int(user_id),
                            QVal::Text(input.field_name.clone()),
                            QVal::Text(input.field_value.clone()),
                            QVal::Text(master_key.clone()),
                            QVal::from(now),
                            QVal::from(now),
                        ],
                    )?;

                    fields.push(TableCertificateField {
                        certificate_field_id: 0, // Will be set by DB
                        certificate_id: cert_id,
                        user_id,
                        field_name: input.field_name.clone(),
                        field_value: input.field_value.clone(),
                        master_key,
                        created_at: now,
                        updated_at: now,
                    });
                }

                field_batch.execute().await?;
                Some(fields)
            } else {
                Some(vec![])
            }
        } else {
            None
        };

        let cert = TableCertificate {
            certificate_id: cert_id,
            user_id,
            cert_type: args.cert_type,
            serial_number: args.serial_number,
            certifier: args.certifier,
            subject: args.subject,
            verifier: args.verifier,
            revocation_outpoint,
            signature,
            created_at: now,
            updated_at: now,
        };

        Ok(CertificateResult {
            certificate: cert,
            fields,
        })
    }

    /// Soft-delete a certificate by setting is_deleted = 1.
    /// Identifies the certificate by serial_number or certificate_id. Verifies ownership.
    pub async fn relinquish_certificate(
        &self,
        user_id: i64,
        args: RelinquishCertificateArgs,
    ) -> Result<bool> {
        // Find the certificate first to verify ownership
        let cert: Option<CertificateRow> = if let Some(ref sn) = args.serial_number {
            Query::new("SELECT * FROM certificates WHERE serial_number = ? AND is_deleted = 0")
                .bind(sn.as_str())
                .fetch_optional(self.db)
                .await?
        } else if let Some(cid) = args.certificate_id {
            Query::new("SELECT * FROM certificates WHERE certificate_id = ? AND is_deleted = 0")
                .bind(cid)
                .fetch_optional(self.db)
                .await?
        } else {
            return Err(Error::ValidationError(
                "relinquishCertificate requires serialNumber or certificateId".to_string(),
            ));
        };

        let cert = match cert {
            Some(c) => c.into_table(),
            None => return Ok(false),
        };

        // Verify ownership
        if cert.user_id != user_id {
            return Err(Error::ValidationError(
                "Certificate does not belong to this user".to_string(),
            ));
        }

        let now = Utc::now();
        let meta = Query::new(
            "UPDATE certificates SET is_deleted = 1, updated_at = ? WHERE certificate_id = ? AND user_id = ?",
        )
        .bind(now)
        .bind(cert.certificate_id)
        .bind(user_id)
        .execute(self.db)
        .await?;

        Ok(meta.changes > 0)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // InsertCertificateArgs deserialization
    // =========================================================================

    #[test]
    fn insert_certificate_args_with_type_field() {
        // Callers send "type" in JSON (the DB column name).
        let val = json!({
            "type": "identity",
            "serialNumber": "sn-001",
            "certifier": "certifier_key",
            "subject": "subject_key"
        });
        let args: InsertCertificateArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.cert_type, "identity");
        assert_eq!(args.serial_number, "sn-001");
        assert_eq!(args.certifier, "certifier_key");
        assert_eq!(args.subject, "subject_key");
        assert!(args.verifier.is_none());
        assert!(args.revocation_outpoint.is_none());
        assert!(args.signature.is_none());
        assert!(args.fields.is_none());
    }

    #[test]
    fn insert_certificate_args_with_cert_type_field() {
        // Also accept "certType" (camelCase of cert_type).
        let val = json!({
            "certType": "identity",
            "serialNumber": "sn-002",
            "certifier": "c1",
            "subject": "s1"
        });
        let args: InsertCertificateArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.cert_type, "identity");
    }

    #[test]
    fn insert_certificate_args_with_fields() {
        let val = json!({
            "type": "identity",
            "serialNumber": "sn-003",
            "certifier": "c1",
            "subject": "s1",
            "fields": [
                {"fieldName": "name", "fieldValue": "encrypted_name", "masterKey": "mk1"},
                {"fieldName": "email", "fieldValue": "encrypted_email"}
            ]
        });
        let args: InsertCertificateArgs = serde_json::from_value(val).unwrap();
        let fields = args.fields.unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field_name, "name");
        assert_eq!(fields[0].master_key, Some("mk1".to_string()));
        assert_eq!(fields[1].field_name, "email");
        assert!(fields[1].master_key.is_none());
    }

    #[test]
    fn insert_certificate_args_with_all_optional_fields() {
        let val = json!({
            "type": "credential",
            "serialNumber": "sn-004",
            "certifier": "c2",
            "subject": "s2",
            "verifier": "v1",
            "revocationOutpoint": "txid.1",
            "signature": "sig_hex"
        });
        let args: InsertCertificateArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.verifier, Some("v1".to_string()));
        assert_eq!(args.revocation_outpoint, Some("txid.1".to_string()));
        assert_eq!(args.signature, Some("sig_hex".to_string()));
    }

    // =========================================================================
    // RelinquishCertificateArgs deserialization
    // =========================================================================

    #[test]
    fn relinquish_args_with_serial_number() {
        let val = json!({"serialNumber": "sn-001"});
        let args: RelinquishCertificateArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.serial_number, Some("sn-001".to_string()));
        assert!(args.certificate_id.is_none());
    }

    #[test]
    fn relinquish_args_with_certificate_id() {
        let val = json!({"certificateId": 42});
        let args: RelinquishCertificateArgs = serde_json::from_value(val).unwrap();
        assert!(args.serial_number.is_none());
        assert_eq!(args.certificate_id, Some(42));
    }

    #[test]
    fn relinquish_args_empty_fails_validation() {
        // The args parse fine, but the storage method will return an error.
        let val = json!({});
        let args: RelinquishCertificateArgs = serde_json::from_value(val).unwrap();
        assert!(args.serial_number.is_none());
        assert!(args.certificate_id.is_none());
    }

    // =========================================================================
    // CertificateResult serialization
    // =========================================================================

    #[test]
    fn certificate_result_without_fields_skips_fields() {
        let result = CertificateResult {
            certificate: TableCertificate {
                certificate_id: 1,
                user_id: 2,
                cert_type: "identity".to_string(),
                serial_number: "sn-001".to_string(),
                certifier: "c1".to_string(),
                subject: "s1".to_string(),
                verifier: None,
                revocation_outpoint: "txid.0".to_string(),
                signature: "sig".to_string(),
                created_at: chrono::DateTime::UNIX_EPOCH,
                updated_at: chrono::DateTime::UNIX_EPOCH,
            },
            fields: None,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert!(!val.as_object().unwrap().contains_key("fields"));
        assert_eq!(val["certificateId"], 1);
        assert_eq!(val["certType"], "identity");
    }

    #[test]
    fn certificate_result_with_fields_includes_fields() {
        let result = CertificateResult {
            certificate: TableCertificate {
                certificate_id: 1,
                user_id: 2,
                cert_type: "identity".to_string(),
                serial_number: "sn-001".to_string(),
                certifier: "c1".to_string(),
                subject: "s1".to_string(),
                verifier: None,
                revocation_outpoint: "txid.0".to_string(),
                signature: "sig".to_string(),
                created_at: chrono::DateTime::UNIX_EPOCH,
                updated_at: chrono::DateTime::UNIX_EPOCH,
            },
            fields: Some(vec![TableCertificateField {
                certificate_field_id: 10,
                certificate_id: 1,
                user_id: 2,
                field_name: "name".to_string(),
                field_value: "encrypted".to_string(),
                master_key: "mk".to_string(),
                created_at: chrono::DateTime::UNIX_EPOCH,
                updated_at: chrono::DateTime::UNIX_EPOCH,
            }]),
        };
        let val = serde_json::to_value(&result).unwrap();
        assert!(val["fields"].is_array());
        assert_eq!(val["fields"][0]["fieldName"], "name");
    }

    // =========================================================================
    // ListCertificatesResult serialization
    // =========================================================================

    #[test]
    fn list_certificates_result_serialize() {
        let result = ListCertificatesResult {
            certificates: vec![],
            total_certificates: 0,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["certificates"], json!([]));
        assert_eq!(val["totalCertificates"], 0);
    }

    // =========================================================================
    // CertificateRow -> TableCertificate conversion
    // =========================================================================

    #[test]
    fn certificate_row_into_table_defaults() {
        let row = CertificateRow {
            certificate_id: Some(1.0),
            user_id: Some(2.0),
            cert_type: Some("identity".to_string()),
            serial_number: Some("sn-001".to_string()),
            certifier: Some("c1".to_string()),
            subject: Some("s1".to_string()),
            verifier: None,
            revocation_outpoint: None,
            signature: None,
            created_at: None,
            updated_at: None,
        };
        let cert = row.into_table();
        assert_eq!(cert.certificate_id, 1);
        assert_eq!(cert.user_id, 2);
        assert_eq!(cert.cert_type, "identity");
        assert!(cert.verifier.is_none());
        // Default revocation outpoint is the zero outpoint
        assert!(cert.revocation_outpoint.starts_with("0000"));
    }

    #[test]
    fn certificate_field_row_into_table() {
        let row = CertificateFieldRow {
            certificate_field_id: Some(10.0),
            certificate_id: Some(1.0),
            user_id: Some(2.0),
            field_name: Some("name".to_string()),
            field_value: Some("encrypted".to_string()),
            master_key: Some("mk".to_string()),
            created_at: Some("2024-01-01 00:00:00".to_string()),
            updated_at: Some("2024-01-01 00:00:00".to_string()),
        };
        let field = row.into_table();
        assert_eq!(field.certificate_field_id, 10);
        assert_eq!(field.field_name, "name");
        assert_eq!(field.master_key, "mk");
    }

    // =========================================================================
    // CertificateRow deserializes from D1 JSON (with "type" column name)
    // =========================================================================

    #[test]
    fn certificate_row_deserialize_with_type_column() {
        // D1 returns the raw column name "type", not "certType"
        let val = json!({
            "certificate_id": 1.0,
            "user_id": 2.0,
            "type": "identity",
            "serial_number": "sn-001",
            "certifier": "c1",
            "subject": "s1",
            "verifier": null,
            "revocation_outpoint": "txid.0",
            "signature": "sig",
            "created_at": "2024-01-01 00:00:00",
            "updated_at": "2024-01-01 00:00:00"
        });
        let row: CertificateRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.cert_type.as_deref(), Some("identity"));
    }
}
