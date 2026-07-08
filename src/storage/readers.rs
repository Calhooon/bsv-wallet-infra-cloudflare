//! Reader methods: listOutputs, listActions, getBalance, getAnalyticsSummary.
//!
//! Ported from rust-wallet-toolbox/src/storage/sqlx/storage_sqlx.rs.
//! Adapted from sqlx to D1 query patterns.

use std::collections::HashMap;

use crate::d1::Query;
use crate::error::Result;
use serde::{Deserialize, Serialize};

use super::StorageD1;

// =============================================================================
// D1 row types
// =============================================================================

#[derive(Debug, Deserialize)]
struct OutputRow {
    output_id: Option<f64>,
    transaction_id: Option<f64>,
    txid: Option<String>,
    vout: Option<f64>,
    satoshis: Option<f64>,
    spendable: Option<f64>,
    locking_script: Option<String>,
    custom_instructions: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    total: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct BasketIdRow {
    basket_id: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TagIdRow {
    output_tag_id: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TagRow {
    tag: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LabelIdRow {
    tx_label_id: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TransactionRow {
    transaction_id: Option<f64>,
    txid: Option<String>,
    satoshis: Option<f64>,
    status: Option<String>,
    is_outgoing: Option<f64>,
    description: Option<String>,
    version: Option<f64>,
    lock_time: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct BalanceRow {
    balance: Option<f64>,
    total: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct AnalyticsRow {
    satoshis: Option<f64>,
    sender: Option<String>,
    ts_ms: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct BatchLabelRow {
    transaction_id: Option<f64>,
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BatchTagRow {
    output_id: Option<f64>,
    tag: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ActionOutputRow {
    output_id: Option<f64>,
    vout: Option<f64>,
    satoshis: Option<f64>,
    spendable: Option<f64>,
    locking_script: Option<String>,
    custom_instructions: Option<String>,
    output_description: Option<String>,
    basket_name: Option<String>,
}

// =============================================================================
// Result types (match SDK's JSON serialization)
// =============================================================================

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListOutputsResult {
    pub total_outputs: u32,
    pub outputs: Vec<OutputItem>,
    /// One aggregate BEEF covering every returned output's transaction plus
    /// its full ancestry (mirrors wallet-toolbox `listOutputsKnex`). Present
    /// only when `include: "entire transactions"` was requested. BRC-100
    /// serializes this under the uppercase `BEEF` key as a number array.
    #[serde(rename = "BEEF", skip_serializing_if = "Option::is_none")]
    pub beef: Option<Vec<u8>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutputItem {
    pub satoshis: u64,
    pub spendable: bool,
    pub outpoint: OutpointItem,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locking_script: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutpointItem {
    pub txid: String,
    pub vout: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListActionsResult {
    pub total_actions: u32,
    pub actions: Vec<ActionItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionItem {
    pub txid: String,
    pub satoshis: i64,
    pub status: String,
    pub is_outgoing: bool,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    pub version: u32,
    pub lock_time: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Vec<ActionOutputItem>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionOutputItem {
    pub satoshis: u64,
    pub spendable: bool,
    pub output_index: u32,
    pub output_description: String,
    pub basket: String,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locking_script: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_instructions: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBalanceResult {
    pub balance: u64,
    pub total_outputs: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsSenderItem {
    pub sender: String,
    pub tx_count: u64,
    pub satoshis: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsWindowResult {
    pub total_revenue: u64,
    pub total_transactions: u64,
    pub unique_senders: u64,
    pub senders: Vec<AnalyticsSenderItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAnalyticsSummaryResult {
    pub windows: GetAnalyticsWindows,
    pub total_actions: u64,
}

#[derive(Debug, Serialize)]
pub struct GetAnalyticsWindows {
    #[serde(rename = "24h")]
    pub h24: AnalyticsWindowResult,
    #[serde(rename = "7d")]
    pub d7: AnalyticsWindowResult,
    #[serde(rename = "30d")]
    pub d30: AnalyticsWindowResult,
    #[serde(rename = "allTime")]
    pub all_time: AnalyticsWindowResult,
}

// =============================================================================
// Input arg types (deserialized from JSON-RPC params)
// =============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListOutputsArgs {
    #[serde(default = "default_basket")]
    pub basket: String,
    pub tags: Option<Vec<String>>,
    pub tag_query_mode: Option<String>,
    pub include: Option<String>,
    pub include_custom_instructions: Option<bool>,
    pub include_tags: Option<bool>,
    pub include_labels: Option<bool>,
    pub limit: Option<u32>,
    pub offset: Option<i32>,
}

fn default_basket() -> String {
    "default".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListActionsArgs {
    #[serde(default)]
    pub labels: Vec<String>,
    pub label_query_mode: Option<String>,
    pub include_labels: Option<bool>,
    pub include_inputs: Option<bool>,
    pub include_outputs: Option<bool>,
    pub include_output_locking_scripts: Option<bool>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBalanceArgs {
    #[serde(default = "default_basket")]
    pub basket: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAnalyticsSummaryArgs {
    /// Current time in milliseconds (sent by caller for time-window boundaries).
    pub now_ms: Option<f64>,
}

// =============================================================================
// listOutputs
// =============================================================================

const VALID_OUTPUT_STATUSES: &str = "'completed', 'unproven', 'nosend', 'sending'";

/// Status set for BALANCE computation — excludes 'nosend' (deliberately
/// unbroadcast txs are not money until sent; TS specOpWalletBalance parity).
const BALANCE_OUTPUT_STATUSES: &str = "'completed', 'unproven', 'sending'";

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// List spendable outputs for a user, filtered by basket and optional tags.
    pub async fn list_outputs(
        &self,
        user_id: i64,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        let limit = args.limit.unwrap_or(10).min(10000);
        let offset = args.offset.unwrap_or(0);
        let order_by = if offset < 0 { "DESC" } else { "ASC" };
        let actual_offset = if offset < 0 {
            (-offset - 1) as u32
        } else {
            offset as u32
        };

        let tag_query_mode = args.tag_query_mode.as_deref().unwrap_or("any");
        let include_custom_instructions = args.include_custom_instructions.unwrap_or(false);
        let include_tags = args.include_tags.unwrap_or(false);
        let include_labels = args.include_labels.unwrap_or(false);
        let include_locking_scripts = args.include.as_deref() == Some("locking scripts");
        let include_transactions = args.include.as_deref() == Some("entire transactions");

        // Find basket ID
        let basket_id: Option<i64> = if !args.basket.is_empty() {
            let row: Option<BasketIdRow> = Query::new(
                "SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = ? AND is_deleted = 0",
            )
            .bind(user_id)
            .bind(args.basket.as_str())
            .fetch_optional(self.db)
            .await?;

            match row {
                Some(r) => Some(r.basket_id.map(|v| v as i64).unwrap_or(0)),
                None => {
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        outputs: vec![],
                        beef: None,
                    });
                }
            }
        } else {
            None
        };

        // Look up tag IDs if tags provided
        let tag_ids = self.resolve_tag_ids(user_id, &args.tags).await?;
        if let Some(ref tags) = args.tags {
            if !tags.is_empty() {
                if tag_query_mode == "all" && tag_ids.len() < tags.len() {
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        outputs: vec![],
                        beef: None,
                    });
                }
                if tag_query_mode == "any" && tag_ids.is_empty() {
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        outputs: vec![],
                        beef: None,
                    });
                }
            }
        }

        // Build main query
        let (output_rows, total_count) = if tag_ids.is_empty() {
            self.query_outputs_no_tags(
                user_id,
                basket_id,
                order_by,
                limit,
                actual_offset,
                include_locking_scripts,
            )
            .await?
        } else {
            let required = if tag_query_mode == "all" {
                tag_ids.len() as i64
            } else {
                1
            };
            self.query_outputs_with_tags(
                user_id,
                basket_id,
                &tag_ids,
                required,
                order_by,
                limit,
                actual_offset,
                include_locking_scripts,
            )
            .await?
        };

        // Batch-fetch tags and labels to avoid N+1 queries
        let output_ids: Vec<i64> = output_rows
            .iter()
            .map(|r| r.output_id.map(|v| v as i64).unwrap_or(0))
            .collect();
        let transaction_ids: Vec<i64> = output_rows
            .iter()
            .map(|r| r.transaction_id.map(|v| v as i64).unwrap_or(0))
            .collect();

        let tags_map = if include_tags {
            self.batch_get_output_tags(&output_ids).await?
        } else {
            HashMap::new()
        };

        let labels_map = if include_labels {
            self.batch_get_transaction_labels(&transaction_ids).await?
        } else {
            HashMap::new()
        };

        // Convert to result items
        let mut outputs = Vec::with_capacity(output_rows.len());
        for row in &output_rows {
            let output_id = row.output_id.map(|v| v as i64).unwrap_or(0);
            let transaction_id = row.transaction_id.map(|v| v as i64).unwrap_or(0);
            let txid = row.txid.clone().unwrap_or_default();
            let vout = row.vout.map(|v| v as u32).unwrap_or(0);
            let satoshis = row.satoshis.map(|v| v as u64).unwrap_or(0);
            let spendable = row.spendable.map(|v| v as i32 != 0).unwrap_or(false);

            let locking_script = if include_locking_scripts {
                row.locking_script.clone()
            } else {
                None
            };
            let custom_instructions = if include_custom_instructions {
                row.custom_instructions.clone()
            } else {
                None
            };

            let tags = if include_tags {
                Some(tags_map.get(&output_id).cloned().unwrap_or_default())
            } else {
                None
            };

            let labels = if include_labels {
                Some(labels_map.get(&transaction_id).cloned().unwrap_or_default())
            } else {
                None
            };

            outputs.push(OutputItem {
                satoshis,
                spendable,
                outpoint: OutpointItem { txid, vout },
                custom_instructions,
                tags,
                labels,
                locking_script,
            });
        }

        let total_outputs = if (outputs.len() as u32) < limit {
            actual_offset + outputs.len() as u32
        } else {
            total_count
        };

        // `include: "entire transactions"` — build ONE aggregate BEEF covering
        // every distinct subject txid in the returned page plus full ancestry
        // (mirrors wallet-toolbox `listOutputsKnex`). Subject txids enter the
        // BFS walk at depth 0, so an unresolvable wallet-owned output txid is
        // a hard error (strict — same semantics as createAction's input BEEF).
        let beef = if include_transactions {
            let mut distinct_txids: Vec<String> = Vec::new();
            for output in &outputs {
                let txid = &output.outpoint.txid;
                // Defensive: a NULL txid row is already tolerated above (it
                // serializes with an empty outpoint.txid); there is no tx for
                // the BEEF to cover, so skip it rather than hard-error.
                if !txid.is_empty() && !distinct_txids.iter().any(|t| t == txid) {
                    distinct_txids.push(txid.clone());
                }
            }
            self.build_input_beef(&distinct_txids).await?
        } else {
            None
        };

        Ok(ListOutputsResult {
            total_outputs,
            outputs,
            beef,
        })
    }

    async fn resolve_tag_ids(&self, user_id: i64, tags: &Option<Vec<String>>) -> Result<Vec<i64>> {
        let tags = match tags {
            Some(t) if !t.is_empty() => t,
            _ => return Ok(vec![]),
        };

        let placeholders: Vec<&str> = tags.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT output_tag_id FROM output_tags WHERE user_id = ? AND is_deleted = 0 AND tag IN ({})",
            placeholders.join(",")
        );

        let mut query = Query::new(&sql).bind(user_id);
        for tag in tags {
            query = query.bind(tag.as_str());
        }

        let rows: Vec<TagIdRow> = query.fetch_all(self.db).await?;
        Ok(rows
            .iter()
            .map(|r| r.output_tag_id.map(|v| v as i64).unwrap_or(0))
            .collect())
    }

    async fn query_outputs_no_tags(
        &self,
        user_id: i64,
        basket_id: Option<i64>,
        order_by: &str,
        limit: u32,
        offset: u32,
        include_locking_scripts: bool,
    ) -> Result<(Vec<OutputRow>, u32)> {
        let basket_filter = match basket_id {
            Some(bid) => format!(" AND o.basket_id = {}", bid),
            None => String::new(),
        };

        // Use hex() for blob columns so D1 returns them as strings, not binary
        let locking_script_col = if include_locking_scripts {
            "hex(o.locking_script) as locking_script"
        } else {
            "NULL as locking_script"
        };

        let sql = format!(
            "SELECT o.output_id, o.transaction_id, o.txid, o.vout, o.satoshis, o.spendable, \
             {}, o.custom_instructions \
             FROM outputs o \
             JOIN transactions t ON o.transaction_id = t.transaction_id \
             WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){}  \
             ORDER BY o.output_id {} LIMIT {} OFFSET {}",
            locking_script_col, VALID_OUTPUT_STATUSES, basket_filter, order_by, limit, offset
        );

        let rows: Vec<OutputRow> = Query::new(&sql).bind(user_id).fetch_all(self.db).await?;

        let count_sql = format!(
            "SELECT COUNT(*) as total FROM outputs o \
             JOIN transactions t ON o.transaction_id = t.transaction_id \
             WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){} ",
            VALID_OUTPUT_STATUSES, basket_filter
        );

        let count: CountRow = Query::new(&count_sql)
            .bind(user_id)
            .fetch_one(self.db)
            .await?;
        let total = count.total.map(|v| v as u32).unwrap_or(0);

        Ok((rows, total))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_outputs_with_tags(
        &self,
        user_id: i64,
        basket_id: Option<i64>,
        tag_ids: &[i64],
        required_count: i64,
        order_by: &str,
        limit: u32,
        offset: u32,
        include_locking_scripts: bool,
    ) -> Result<(Vec<OutputRow>, u32)> {
        let tag_id_list: String = tag_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let basket_filter = match basket_id {
            Some(bid) => format!(" AND o.basket_id = {}", bid),
            None => String::new(),
        };

        // Use hex() for blob columns so D1 returns them as strings, not binary
        let locking_script_col = if include_locking_scripts {
            "hex(o.locking_script) as locking_script"
        } else {
            "NULL as locking_script"
        };

        let sql = format!(
            "WITH outputs_with_tags AS ( \
                SELECT o.output_id, o.transaction_id, o.txid, o.vout, o.satoshis, o.spendable, \
                       {}, o.custom_instructions, \
                       (SELECT COUNT(*) FROM output_tags_map m \
                        WHERE m.output_id = o.output_id \
                        AND m.output_tag_id IN ({}) \
                        AND m.is_deleted = 0) as tag_count \
                FROM outputs o \
                JOIN transactions t ON o.transaction_id = t.transaction_id \
                WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){} \
            ) \
            SELECT output_id, transaction_id, txid, vout, satoshis, spendable, \
                   locking_script, custom_instructions \
            FROM outputs_with_tags WHERE tag_count >= ? \
            ORDER BY output_id {} LIMIT ? OFFSET ?",
            locking_script_col, tag_id_list, VALID_OUTPUT_STATUSES, basket_filter, order_by
        );

        let rows: Vec<OutputRow> = Query::new(&sql)
            .bind(user_id)
            .bind(required_count)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.db)
            .await?;

        let count_sql = format!(
            "WITH outputs_with_tags AS ( \
                SELECT o.output_id, \
                       (SELECT COUNT(*) FROM output_tags_map m \
                        WHERE m.output_id = o.output_id \
                        AND m.output_tag_id IN ({}) \
                        AND m.is_deleted = 0) as tag_count \
                FROM outputs o \
                JOIN transactions t ON o.transaction_id = t.transaction_id \
                WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){} \
            ) \
            SELECT COUNT(*) as total FROM outputs_with_tags WHERE tag_count >= ?",
            tag_id_list, VALID_OUTPUT_STATUSES, basket_filter
        );

        let count: CountRow = Query::new(&count_sql)
            .bind(user_id)
            .bind(required_count)
            .fetch_one(self.db)
            .await?;
        let total = count.total.map(|v| v as u32).unwrap_or(0);

        Ok((rows, total))
    }

    async fn get_output_tags(&self, output_id: i64) -> Result<Vec<String>> {
        let rows: Vec<TagRow> = Query::new(
            "SELECT t.tag FROM output_tags t \
             JOIN output_tags_map m ON t.output_tag_id = m.output_tag_id \
             WHERE m.output_id = ? AND m.is_deleted = 0 AND t.is_deleted = 0",
        )
        .bind(output_id)
        .fetch_all(self.db)
        .await?;

        Ok(rows.into_iter().filter_map(|r| r.tag).collect())
    }

    // =========================================================================
    // listActions
    // =========================================================================

    /// List transactions (actions) for a user, filtered by labels.
    pub async fn list_actions(
        &self,
        user_id: i64,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        let limit = args.limit.unwrap_or(10).min(10000);
        let offset = args.offset.unwrap_or(0);
        let label_query_mode = args.label_query_mode.as_deref().unwrap_or("any");
        let include_labels = args.include_labels.unwrap_or(false);
        let include_outputs = args.include_outputs.unwrap_or(false);
        let include_output_locking_scripts = args.include_output_locking_scripts.unwrap_or(false);

        let valid_statuses =
            "'completed', 'unprocessed', 'sending', 'unproven', 'unsigned', 'nosend', 'nonfinal'";

        // Look up label IDs if provided
        let label_ids = self.resolve_label_ids(user_id, &args.labels).await?;
        if !args.labels.is_empty() {
            if label_query_mode == "all" && label_ids.len() < args.labels.len() {
                return Ok(ListActionsResult {
                    total_actions: 0,
                    actions: vec![],
                });
            }
            if label_query_mode == "any" && label_ids.is_empty() {
                return Ok(ListActionsResult {
                    total_actions: 0,
                    actions: vec![],
                });
            }
        }

        // Build and execute query
        let (tx_rows, total_count) = if label_ids.is_empty() {
            self.query_actions_no_labels(user_id, valid_statuses, limit, offset)
                .await?
        } else {
            let required = if label_query_mode == "all" {
                label_ids.len() as i64
            } else {
                1
            };
            self.query_actions_with_labels(
                user_id,
                valid_statuses,
                &label_ids,
                required,
                limit,
                offset,
            )
            .await?
        };

        // Batch-fetch labels to avoid N+1 queries
        let action_tx_ids: Vec<i64> = tx_rows
            .iter()
            .map(|r| r.transaction_id.map(|v| v as i64).unwrap_or(0))
            .collect();
        let labels_map = if include_labels {
            self.batch_get_transaction_labels(&action_tx_ids).await?
        } else {
            HashMap::new()
        };

        // Convert to result items
        let mut actions = Vec::with_capacity(tx_rows.len());
        for row in &tx_rows {
            let transaction_id = row.transaction_id.map(|v| v as i64).unwrap_or(0);
            let txid = row.txid.clone().unwrap_or_default();
            let satoshis = row.satoshis.map(|v| v as i64).unwrap_or(0);
            let status = row
                .status
                .clone()
                .unwrap_or_else(|| "unprocessed".to_string());
            let is_outgoing = row.is_outgoing.map(|v| v as i32 != 0).unwrap_or(false);
            let description = row.description.clone().unwrap_or_default();
            let version = row.version.map(|v| v as u32).unwrap_or(1);
            let lock_time = row.lock_time.map(|v| v as u32).unwrap_or(0);

            let labels = if include_labels {
                Some(labels_map.get(&transaction_id).cloned().unwrap_or_default())
            } else {
                None
            };

            let outputs = if include_outputs {
                Some(
                    self.get_action_outputs(transaction_id, include_output_locking_scripts)
                        .await?,
                )
            } else {
                None
            };

            actions.push(ActionItem {
                txid,
                satoshis,
                status,
                is_outgoing,
                description,
                labels,
                version,
                lock_time,
                outputs,
            });
        }

        let total_actions = if (actions.len() as u32) < limit {
            offset + actions.len() as u32
        } else {
            total_count
        };

        Ok(ListActionsResult {
            total_actions,
            actions,
        })
    }

    async fn resolve_label_ids(&self, user_id: i64, labels: &[String]) -> Result<Vec<i64>> {
        if labels.is_empty() {
            return Ok(vec![]);
        }

        let placeholders: Vec<&str> = labels.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT tx_label_id FROM tx_labels WHERE user_id = ? AND is_deleted = 0 AND label IN ({})",
            placeholders.join(",")
        );

        let mut query = Query::new(&sql).bind(user_id);
        for label in labels {
            query = query.bind(label.as_str());
        }

        let rows: Vec<LabelIdRow> = query.fetch_all(self.db).await?;
        Ok(rows
            .iter()
            .map(|r| r.tx_label_id.map(|v| v as i64).unwrap_or(0))
            .collect())
    }

    async fn query_actions_no_labels(
        &self,
        user_id: i64,
        valid_statuses: &str,
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<TransactionRow>, u32)> {
        let sql = format!(
            "SELECT transaction_id, txid, satoshis, status, is_outgoing, description, version, lock_time \
             FROM transactions \
             WHERE user_id = ? AND status IN ({}) \
             ORDER BY transaction_id DESC LIMIT {} OFFSET {}",
            valid_statuses, limit, offset
        );

        let rows: Vec<TransactionRow> = Query::new(&sql).bind(user_id).fetch_all(self.db).await?;

        let count_sql = format!(
            "SELECT COUNT(*) as total FROM transactions WHERE user_id = ? AND status IN ({})",
            valid_statuses
        );

        let count: CountRow = Query::new(&count_sql)
            .bind(user_id)
            .fetch_one(self.db)
            .await?;
        let total = count.total.map(|v| v as u32).unwrap_or(0);

        Ok((rows, total))
    }

    async fn query_actions_with_labels(
        &self,
        user_id: i64,
        valid_statuses: &str,
        label_ids: &[i64],
        required_count: i64,
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<TransactionRow>, u32)> {
        let label_id_list: String = label_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!(
            "WITH txs_with_labels AS ( \
                SELECT t.transaction_id, t.txid, t.satoshis, t.status, t.is_outgoing, \
                       t.description, t.version, t.lock_time, \
                       (SELECT COUNT(*) FROM tx_labels_map m \
                        WHERE m.transaction_id = t.transaction_id \
                        AND m.tx_label_id IN ({}) \
                        AND m.is_deleted = 0) as label_count \
                FROM transactions t \
                WHERE t.user_id = ? AND t.status IN ({}) \
            ) \
            SELECT transaction_id, txid, satoshis, status, is_outgoing, description, version, lock_time \
            FROM txs_with_labels WHERE label_count >= ? \
            ORDER BY transaction_id DESC LIMIT ? OFFSET ?",
            label_id_list, valid_statuses
        );

        let rows: Vec<TransactionRow> = Query::new(&sql)
            .bind(user_id)
            .bind(required_count)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.db)
            .await?;

        let count_sql = format!(
            "WITH txs_with_labels AS ( \
                SELECT t.transaction_id, \
                       (SELECT COUNT(*) FROM tx_labels_map m \
                        WHERE m.transaction_id = t.transaction_id \
                        AND m.tx_label_id IN ({}) \
                        AND m.is_deleted = 0) as label_count \
                FROM transactions t \
                WHERE t.user_id = ? AND t.status IN ({}) \
            ) \
            SELECT COUNT(*) as total FROM txs_with_labels WHERE label_count >= ?",
            label_id_list, valid_statuses
        );

        let count: CountRow = Query::new(&count_sql)
            .bind(user_id)
            .bind(required_count)
            .fetch_one(self.db)
            .await?;
        let total = count.total.map(|v| v as u32).unwrap_or(0);

        Ok((rows, total))
    }

    // =========================================================================
    // getBalance
    // =========================================================================

    /// Return total spendable balance and UTXO count in a single SQL aggregate.
    pub async fn get_balance(
        &self,
        user_id: i64,
        args: GetBalanceArgs,
    ) -> Result<GetBalanceResult> {
        // Find basket ID
        let basket_id: Option<i64> = if !args.basket.is_empty() {
            let row: Option<BasketIdRow> = Query::new(
                "SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = ? AND is_deleted = 0",
            )
            .bind(user_id)
            .bind(args.basket.as_str())
            .fetch_optional(self.db)
            .await?;

            match row {
                Some(r) => Some(r.basket_id.map(|v| v as i64).unwrap_or(0)),
                None => {
                    return Ok(GetBalanceResult {
                        balance: 0,
                        total_outputs: 0,
                    });
                }
            }
        } else {
            None
        };

        let basket_filter = match basket_id {
            Some(bid) => format!(" AND o.basket_id = {}", bid),
            None => String::new(),
        };

        // Balance statuses EXCLUDE 'nosend' (audit minor-1): a nosend tx is
        // deliberately unbroadcast — its outputs are listable (TS listOutputs
        // includes nosend too) but must not count as spendable balance (TS
        // wallet balance uses completed/unproven/sending only —
        // StorageProvider.ts:183-198 specOpWalletBalance).
        let sql = format!(
            "SELECT COALESCE(SUM(o.satoshis), 0) as balance, COUNT(*) as total \
             FROM outputs o \
             JOIN transactions t ON o.transaction_id = t.transaction_id \
             WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){} ",
            BALANCE_OUTPUT_STATUSES, basket_filter
        );

        let row: BalanceRow = Query::new(&sql).bind(user_id).fetch_one(self.db).await?;

        Ok(GetBalanceResult {
            balance: row.balance.map(|v| v as u64).unwrap_or(0),
            total_outputs: row.total.map(|v| v as u32).unwrap_or(0),
        })
    }

    // =========================================================================
    // getAnalyticsSummary
    // =========================================================================

    /// Server-side analytics aggregation: revenue, tx count, and unique senders
    /// per time window (24h, 7d, 30d, allTime), grouped by sender identity key.
    pub async fn get_analytics_summary(
        &self,
        user_id: i64,
        args: GetAnalyticsSummaryArgs,
    ) -> Result<GetAnalyticsSummaryResult> {
        let now_ms = args.now_ms.unwrap_or(0.0) as u64;

        // Single query: join transactions with labels, extract sender: and ts: prefixes,
        // return one row per completed incoming transaction.
        let sql = "SELECT \
                t.satoshis, \
                MAX(CASE WHEN l.label LIKE 'sender:%' THEN SUBSTR(l.label, 8) END) as sender, \
                MAX(CASE WHEN l.label LIKE 'ts:%' THEN CAST(SUBSTR(l.label, 4) AS INTEGER) END) as ts_ms \
            FROM transactions t \
            JOIN tx_labels_map m ON t.transaction_id = m.transaction_id AND m.is_deleted = 0 \
            JOIN tx_labels l ON m.tx_label_id = l.tx_label_id AND l.is_deleted = 0 \
            WHERE t.user_id = ? AND t.status = 'completed' AND t.satoshis > 0 \
            GROUP BY t.transaction_id";

        let rows: Vec<AnalyticsRow> = Query::new(sql).bind(user_id).fetch_all(self.db).await?;

        let ms_24h = 24 * 60 * 60 * 1000u64;
        let ms_7d = 7 * ms_24h;
        let ms_30d = 30 * ms_24h;

        // sender → (tx_count, satoshis)
        let mut w_24h: HashMap<String, (u64, u64)> = HashMap::new();
        let mut w_7d: HashMap<String, (u64, u64)> = HashMap::new();
        let mut w_30d: HashMap<String, (u64, u64)> = HashMap::new();
        let mut w_all: HashMap<String, (u64, u64)> = HashMap::new();
        let mut total_actions = 0u64;

        for row in &rows {
            let sats = row.satoshis.map(|v| v as u64).unwrap_or(0);
            if sats == 0 {
                continue;
            }

            let sender = row.sender.clone().unwrap_or_else(|| "unknown".to_string());
            total_actions += 1;

            // Always add to all-time
            let e = w_all.entry(sender.clone()).or_insert((0, 0));
            e.0 += 1;
            e.1 += sats;

            // Time-windowed buckets
            if let Some(ts) = row.ts_ms.map(|v| v as u64) {
                if now_ms > 0 {
                    let age = now_ms.saturating_sub(ts);
                    if age <= ms_24h {
                        let e = w_24h.entry(sender.clone()).or_insert((0, 0));
                        e.0 += 1;
                        e.1 += sats;
                    }
                    if age <= ms_7d {
                        let e = w_7d.entry(sender.clone()).or_insert((0, 0));
                        e.0 += 1;
                        e.1 += sats;
                    }
                    if age <= ms_30d {
                        let e = w_30d.entry(sender.clone()).or_insert((0, 0));
                        e.0 += 1;
                        e.1 += sats;
                    }
                }
            }
        }

        fn build_window(map: &HashMap<String, (u64, u64)>) -> AnalyticsWindowResult {
            let mut senders: Vec<AnalyticsSenderItem> = map
                .iter()
                .map(|(k, (count, sats))| AnalyticsSenderItem {
                    sender: k.clone(),
                    tx_count: *count,
                    satoshis: *sats,
                })
                .collect();
            senders.sort_by(|a, b| b.satoshis.cmp(&a.satoshis));

            let total_revenue = senders.iter().map(|s| s.satoshis).sum();
            let total_transactions = senders.iter().map(|s| s.tx_count).sum();
            let unique_senders = senders.len() as u64;

            AnalyticsWindowResult {
                total_revenue,
                total_transactions,
                unique_senders,
                senders,
            }
        }

        Ok(GetAnalyticsSummaryResult {
            windows: GetAnalyticsWindows {
                h24: build_window(&w_24h),
                d7: build_window(&w_7d),
                d30: build_window(&w_30d),
                all_time: build_window(&w_all),
            },
            total_actions,
        })
    }

    // =========================================================================
    // Batch label/tag helpers (replaces per-row N+1 queries)
    // =========================================================================

    /// Fetch labels for multiple transactions in a single query.
    async fn batch_get_transaction_labels(
        &self,
        transaction_ids: &[i64],
    ) -> Result<HashMap<i64, Vec<String>>> {
        if transaction_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders: Vec<&str> = transaction_ids.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT m.transaction_id, l.label FROM tx_labels l \
             JOIN tx_labels_map m ON l.tx_label_id = m.tx_label_id \
             WHERE m.transaction_id IN ({}) AND m.is_deleted = 0 AND l.is_deleted = 0",
            placeholders.join(",")
        );

        let mut query = Query::new(&sql);
        for id in transaction_ids {
            query = query.bind(*id);
        }

        let rows: Vec<BatchLabelRow> = query.fetch_all(self.db).await?;

        let mut map: HashMap<i64, Vec<String>> = HashMap::new();
        for row in rows {
            if let (Some(tid), Some(label)) = (row.transaction_id.map(|v| v as i64), row.label) {
                map.entry(tid).or_default().push(label);
            }
        }
        Ok(map)
    }

    /// Fetch tags for multiple outputs in a single query.
    async fn batch_get_output_tags(&self, output_ids: &[i64]) -> Result<HashMap<i64, Vec<String>>> {
        if output_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders: Vec<&str> = output_ids.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT m.output_id, t.tag FROM output_tags t \
             JOIN output_tags_map m ON t.output_tag_id = m.output_tag_id \
             WHERE m.output_id IN ({}) AND m.is_deleted = 0 AND t.is_deleted = 0",
            placeholders.join(",")
        );

        let mut query = Query::new(&sql);
        for id in output_ids {
            query = query.bind(*id);
        }

        let rows: Vec<BatchTagRow> = query.fetch_all(self.db).await?;

        let mut map: HashMap<i64, Vec<String>> = HashMap::new();
        for row in rows {
            if let (Some(oid), Some(tag)) = (row.output_id.map(|v| v as i64), row.tag) {
                map.entry(oid).or_default().push(tag);
            }
        }
        Ok(map)
    }

    async fn get_action_outputs(
        &self,
        transaction_id: i64,
        include_locking_scripts: bool,
    ) -> Result<Vec<ActionOutputItem>> {
        // Use hex() for blob columns so D1 returns them as strings, not binary
        let locking_col = if include_locking_scripts {
            "hex(o.locking_script) as locking_script"
        } else {
            "NULL as locking_script"
        };
        let sql = format!(
            "SELECT o.output_id, o.vout, o.satoshis, o.spendable, {}, \
             o.custom_instructions, o.output_description, ob.name as basket_name \
             FROM outputs o \
             LEFT JOIN output_baskets ob ON o.basket_id = ob.basket_id \
             WHERE o.transaction_id = ? \
             ORDER BY o.vout ASC",
            locking_col
        );
        let rows: Vec<ActionOutputRow> = Query::new(&sql)
            .bind(transaction_id)
            .fetch_all(self.db)
            .await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let output_id = row.output_id.map(|v| v as i64).unwrap_or(0);
            let tags = self.get_output_tags(output_id).await?;

            result.push(ActionOutputItem {
                satoshis: row.satoshis.map(|v| v as u64).unwrap_or(0),
                spendable: row.spendable.map(|v| v as i32 != 0).unwrap_or(false),
                output_index: row.vout.map(|v| v as u32).unwrap_or(0),
                output_description: row.output_description.unwrap_or_default(),
                basket: row.basket_name.unwrap_or_default(),
                tags,
                locking_script: if include_locking_scripts {
                    row.locking_script
                } else {
                    None
                },
                custom_instructions: row.custom_instructions,
            });
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // VALID_OUTPUT_STATUSES constant
    // =========================================================================

    #[test]
    fn valid_output_statuses_contains_expected() {
        assert!(VALID_OUTPUT_STATUSES.contains("completed"));
        assert!(VALID_OUTPUT_STATUSES.contains("unproven"));
        assert!(VALID_OUTPUT_STATUSES.contains("nosend"));
        assert!(VALID_OUTPUT_STATUSES.contains("sending"));
    }

    #[test]
    fn valid_output_statuses_does_not_contain_failed() {
        assert!(!VALID_OUTPUT_STATUSES.contains("failed"));
    }

    #[test]
    fn valid_output_statuses_does_not_contain_unsigned() {
        assert!(!VALID_OUTPUT_STATUSES.contains("unsigned"));
    }

    // =========================================================================
    // ListOutputsArgs deserialization
    // =========================================================================

    #[test]
    fn list_outputs_args_defaults() {
        let val = json!({});
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "default");
        assert!(args.tags.is_none());
        assert!(args.tag_query_mode.is_none());
        assert!(args.include.is_none());
        assert!(args.include_custom_instructions.is_none());
        assert!(args.include_tags.is_none());
        assert!(args.include_labels.is_none());
        assert!(args.limit.is_none());
        assert!(args.offset.is_none());
    }

    #[test]
    fn list_outputs_args_custom_basket() {
        let val = json!({"basket": "payments"});
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "payments");
    }

    #[test]
    fn list_outputs_args_with_tags() {
        let val = json!({
            "tags": ["redeemable", "payment"],
            "tagQueryMode": "all"
        });
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.tags.as_ref().unwrap().len(), 2);
        assert_eq!(args.tag_query_mode.as_deref(), Some("all"));
    }

    #[test]
    fn list_outputs_args_with_pagination() {
        let val = json!({"limit": 50, "offset": 10});
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.limit, Some(50));
        assert_eq!(args.offset, Some(10));
    }

    #[test]
    fn list_outputs_args_negative_offset() {
        let val = json!({"offset": -5});
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.offset, Some(-5));
    }

    #[test]
    fn list_outputs_args_include_locking_scripts() {
        let val = json!({"include": "locking scripts"});
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.include.as_deref(), Some("locking scripts"));
    }

    #[test]
    fn list_outputs_args_include_entire_transactions() {
        let val = json!({"include": "entire transactions"});
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.include.as_deref(), Some("entire transactions"));

        // Same expressions as list_outputs: "entire transactions" flips ONLY
        // the transactions flag — locking scripts stays off (and vice versa).
        let include_locking_scripts = args.include.as_deref() == Some("locking scripts");
        let include_transactions = args.include.as_deref() == Some("entire transactions");
        assert!(!include_locking_scripts);
        assert!(include_transactions);

        let val = json!({"include": "locking scripts"});
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        let include_locking_scripts = args.include.as_deref() == Some("locking scripts");
        let include_transactions = args.include.as_deref() == Some("entire transactions");
        assert!(include_locking_scripts);
        assert!(!include_transactions);
    }

    #[test]
    fn list_outputs_args_include_flags() {
        let val = json!({
            "includeCustomInstructions": true,
            "includeTags": true,
            "includeLabels": true
        });
        let args: ListOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.include_custom_instructions, Some(true));
        assert_eq!(args.include_tags, Some(true));
        assert_eq!(args.include_labels, Some(true));
    }

    // =========================================================================
    // ListActionsArgs deserialization
    // =========================================================================

    #[test]
    fn list_actions_args_defaults() {
        let val = json!({});
        let args: ListActionsArgs = serde_json::from_value(val).unwrap();
        assert!(args.labels.is_empty());
        assert!(args.label_query_mode.is_none());
        assert!(args.include_labels.is_none());
        assert!(args.include_inputs.is_none());
        assert!(args.include_outputs.is_none());
        assert!(args.limit.is_none());
        assert!(args.offset.is_none());
    }

    #[test]
    fn list_actions_args_with_labels() {
        let val = json!({
            "labels": ["payment", "invoice"],
            "labelQueryMode": "all"
        });
        let args: ListActionsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.labels.len(), 2);
        assert_eq!(args.label_query_mode.as_deref(), Some("all"));
    }

    #[test]
    fn list_actions_args_with_pagination() {
        let val = json!({"limit": 100, "offset": 20});
        let args: ListActionsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.limit, Some(100));
        assert_eq!(args.offset, Some(20));
    }

    #[test]
    fn list_actions_args_include_outputs() {
        let val = json!({
            "includeOutputs": true,
            "includeOutputLockingScripts": true
        });
        let args: ListActionsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.include_outputs, Some(true));
        assert_eq!(args.include_output_locking_scripts, Some(true));
    }

    // =========================================================================
    // GetBalanceArgs deserialization
    // =========================================================================

    #[test]
    fn get_balance_args_default_basket() {
        let val = json!({});
        let args: GetBalanceArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "default");
    }

    #[test]
    fn get_balance_args_custom_basket() {
        let val = json!({"basket": "savings"});
        let args: GetBalanceArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "savings");
    }

    // =========================================================================
    // GetAnalyticsSummaryArgs deserialization
    // =========================================================================

    #[test]
    fn analytics_args_with_now_ms() {
        let val = json!({"nowMs": 1700000000000.0});
        let args: GetAnalyticsSummaryArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.now_ms, Some(1700000000000.0));
    }

    #[test]
    fn analytics_args_empty() {
        let val = json!({});
        let args: GetAnalyticsSummaryArgs = serde_json::from_value(val).unwrap();
        assert!(args.now_ms.is_none());
    }

    // =========================================================================
    // ListOutputsResult serialization
    // =========================================================================

    #[test]
    fn list_outputs_result_empty() {
        let result = ListOutputsResult {
            total_outputs: 0,
            outputs: vec![],
            beef: None,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["totalOutputs"], 0);
        assert_eq!(val["outputs"], json!([]));
    }

    #[test]
    fn list_outputs_result_with_output() {
        let result = ListOutputsResult {
            total_outputs: 1,
            outputs: vec![OutputItem {
                satoshis: 50000,
                spendable: true,
                outpoint: OutpointItem {
                    txid: "abc123".to_string(),
                    vout: 0,
                },
                custom_instructions: None,
                tags: None,
                labels: None,
                locking_script: None,
            }],
            beef: None,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["totalOutputs"], 1);
        assert_eq!(val["outputs"][0]["satoshis"], 50000);
        assert_eq!(val["outputs"][0]["spendable"], true);
        assert_eq!(val["outputs"][0]["outpoint"]["txid"], "abc123");
        assert_eq!(val["outputs"][0]["outpoint"]["vout"], 0);
        // Optional fields should be absent (skip_serializing_if)
        assert!(val["outputs"][0].get("customInstructions").is_none());
        assert!(val["outputs"][0].get("tags").is_none());
        assert!(val["outputs"][0].get("labels").is_none());
        assert!(val["outputs"][0].get("lockingScript").is_none());
    }

    #[test]
    fn list_outputs_result_with_optional_fields() {
        let result = ListOutputsResult {
            total_outputs: 1,
            outputs: vec![OutputItem {
                satoshis: 10000,
                spendable: false,
                outpoint: OutpointItem {
                    txid: "def456".to_string(),
                    vout: 1,
                },
                custom_instructions: Some("redeem at x402".to_string()),
                tags: Some(vec!["payment".to_string()]),
                labels: Some(vec!["invoice".to_string()]),
                locking_script: Some("76a914abcd1234".to_string()),
            }],
            beef: None,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["outputs"][0]["customInstructions"], "redeem at x402");
        assert_eq!(val["outputs"][0]["tags"], json!(["payment"]));
        assert_eq!(val["outputs"][0]["labels"], json!(["invoice"]));
        assert_eq!(val["outputs"][0]["lockingScript"], "76a914abcd1234");
    }

    #[test]
    fn list_outputs_result_with_beef_serializes_uppercase_key() {
        let result = ListOutputsResult {
            total_outputs: 1,
            outputs: vec![OutputItem {
                satoshis: 50000,
                spendable: true,
                outpoint: OutpointItem {
                    txid: "abc123".to_string(),
                    vout: 0,
                },
                custom_instructions: None,
                tags: None,
                labels: None,
                locking_script: None,
            }],
            beef: Some(vec![1, 2, 3, 4]),
        };
        let val = serde_json::to_value(&result).unwrap();
        // BRC-100 standard key is uppercase BEEF; Vec<u8> serializes as a
        // number array (same convention as createAction's inputBeef).
        assert_eq!(val["BEEF"], json!([1, 2, 3, 4]));
        assert!(val.get("beef").is_none());
    }

    #[test]
    fn list_outputs_result_without_beef_omits_key() {
        let result = ListOutputsResult {
            total_outputs: 0,
            outputs: vec![],
            beef: None,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert!(val.get("BEEF").is_none());
        assert!(val.get("beef").is_none());
    }

    // =========================================================================
    // ListActionsResult serialization
    // =========================================================================

    #[test]
    fn list_actions_result_empty() {
        let result = ListActionsResult {
            total_actions: 0,
            actions: vec![],
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["totalActions"], 0);
        assert_eq!(val["actions"], json!([]));
    }

    #[test]
    fn list_actions_result_with_action() {
        let result = ListActionsResult {
            total_actions: 1,
            actions: vec![ActionItem {
                txid: "deadbeef".to_string(),
                satoshis: -50000,
                status: "completed".to_string(),
                is_outgoing: true,
                description: "Payment to service".to_string(),
                labels: Some(vec!["payment".to_string()]),
                version: 1,
                lock_time: 0,
                outputs: None,
            }],
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["totalActions"], 1);
        assert_eq!(val["actions"][0]["txid"], "deadbeef");
        assert_eq!(val["actions"][0]["satoshis"], -50000);
        assert_eq!(val["actions"][0]["status"], "completed");
        assert_eq!(val["actions"][0]["isOutgoing"], true);
        assert_eq!(val["actions"][0]["description"], "Payment to service");
        assert_eq!(val["actions"][0]["labels"], json!(["payment"]));
        assert_eq!(val["actions"][0]["version"], 1);
        assert_eq!(val["actions"][0]["lockTime"], 0);
        assert!(val["actions"][0].get("outputs").is_none());
    }

    // =========================================================================
    // GetBalanceResult serialization
    // =========================================================================

    #[test]
    fn get_balance_result_serializes() {
        let result = GetBalanceResult {
            balance: 150000,
            total_outputs: 3,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["balance"], 150000);
        assert_eq!(val["totalOutputs"], 3);
    }

    #[test]
    fn get_balance_result_zero_balance() {
        let result = GetBalanceResult {
            balance: 0,
            total_outputs: 0,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["balance"], 0);
        assert_eq!(val["totalOutputs"], 0);
    }

    // =========================================================================
    // Pagination logic (mirrors list_outputs offset/limit processing)
    // =========================================================================

    fn compute_pagination(limit: Option<u32>, offset: Option<i32>) -> (u32, u32, &'static str) {
        let limit = limit.unwrap_or(10).min(10000);
        let offset_val = offset.unwrap_or(0);
        let order_by = if offset_val < 0 { "DESC" } else { "ASC" };
        let actual_offset = if offset_val < 0 {
            (-offset_val - 1) as u32
        } else {
            offset_val as u32
        };
        (limit, actual_offset, order_by)
    }

    #[test]
    fn pagination_defaults() {
        let (limit, offset, order) = compute_pagination(None, None);
        assert_eq!(limit, 10);
        assert_eq!(offset, 0);
        assert_eq!(order, "ASC");
    }

    #[test]
    fn pagination_custom_limit() {
        let (limit, _, _) = compute_pagination(Some(50), None);
        assert_eq!(limit, 50);
    }

    #[test]
    fn pagination_limit_capped_at_10000() {
        let (limit, _, _) = compute_pagination(Some(20000), None);
        assert_eq!(limit, 10000);
    }

    #[test]
    fn pagination_positive_offset() {
        let (_, offset, order) = compute_pagination(None, Some(5));
        assert_eq!(offset, 5);
        assert_eq!(order, "ASC");
    }

    #[test]
    fn pagination_negative_offset_reverses_order() {
        let (_, offset, order) = compute_pagination(None, Some(-1));
        assert_eq!(offset, 0);
        assert_eq!(order, "DESC");
    }

    #[test]
    fn pagination_negative_offset_minus_5() {
        let (_, offset, order) = compute_pagination(None, Some(-5));
        assert_eq!(offset, 4);
        assert_eq!(order, "DESC");
    }

    #[test]
    fn pagination_negative_offset_minus_10() {
        let (_, offset, order) = compute_pagination(None, Some(-10));
        assert_eq!(offset, 9);
        assert_eq!(order, "DESC");
    }

    // =========================================================================
    // Tag query mode logic
    // =========================================================================

    fn should_return_empty_for_tags(
        tag_query_mode: &str,
        requested_tags: usize,
        resolved_tag_ids: usize,
    ) -> bool {
        if requested_tags > 0 {
            if tag_query_mode == "all" && resolved_tag_ids < requested_tags {
                return true;
            }
            if tag_query_mode == "any" && resolved_tag_ids == 0 {
                return true;
            }
        }
        false
    }

    #[test]
    fn tag_mode_all_missing_tag_returns_empty() {
        // Requested 3 tags, only 2 found in DB
        assert!(should_return_empty_for_tags("all", 3, 2));
    }

    #[test]
    fn tag_mode_all_all_found_does_not_return_empty() {
        assert!(!should_return_empty_for_tags("all", 3, 3));
    }

    #[test]
    fn tag_mode_any_none_found_returns_empty() {
        assert!(should_return_empty_for_tags("any", 3, 0));
    }

    #[test]
    fn tag_mode_any_some_found_does_not_return_empty() {
        assert!(!should_return_empty_for_tags("any", 3, 1));
    }

    #[test]
    fn tag_mode_no_tags_requested_never_empty() {
        assert!(!should_return_empty_for_tags("all", 0, 0));
        assert!(!should_return_empty_for_tags("any", 0, 0));
    }

    // =========================================================================
    // AnalyticsWindowResult serialization
    // =========================================================================

    #[test]
    fn analytics_window_result_empty() {
        let window = AnalyticsWindowResult {
            total_revenue: 0,
            total_transactions: 0,
            unique_senders: 0,
            senders: vec![],
        };
        let val = serde_json::to_value(&window).unwrap();
        assert_eq!(val["totalRevenue"], 0);
        assert_eq!(val["totalTransactions"], 0);
        assert_eq!(val["uniqueSenders"], 0);
        assert_eq!(val["senders"], json!([]));
    }

    #[test]
    fn analytics_window_result_with_senders() {
        let window = AnalyticsWindowResult {
            total_revenue: 150000,
            total_transactions: 5,
            unique_senders: 2,
            senders: vec![
                AnalyticsSenderItem {
                    sender: "agent_a".to_string(),
                    tx_count: 3,
                    satoshis: 100000,
                },
                AnalyticsSenderItem {
                    sender: "agent_b".to_string(),
                    tx_count: 2,
                    satoshis: 50000,
                },
            ],
        };
        let val = serde_json::to_value(&window).unwrap();
        assert_eq!(val["totalRevenue"], 150000);
        assert_eq!(val["senders"][0]["sender"], "agent_a");
        assert_eq!(val["senders"][0]["txCount"], 3);
        assert_eq!(val["senders"][1]["satoshis"], 50000);
    }

    // =========================================================================
    // GetAnalyticsSummaryResult serialization
    // =========================================================================

    #[test]
    fn analytics_summary_result_serializes() {
        let empty_window = AnalyticsWindowResult {
            total_revenue: 0,
            total_transactions: 0,
            unique_senders: 0,
            senders: vec![],
        };
        let result = GetAnalyticsSummaryResult {
            windows: GetAnalyticsWindows {
                h24: empty_window.clone(),
                d7: empty_window.clone(),
                d30: empty_window.clone(),
                all_time: AnalyticsWindowResult {
                    total_revenue: 500000,
                    total_transactions: 10,
                    unique_senders: 3,
                    senders: vec![],
                },
            },
            total_actions: 10,
        };
        let val = serde_json::to_value(&result).unwrap();
        assert_eq!(val["totalActions"], 10);
        assert_eq!(val["windows"]["allTime"]["totalRevenue"], 500000);
        assert_eq!(val["windows"]["24h"]["totalRevenue"], 0);
        assert_eq!(val["windows"]["7d"]["totalTransactions"], 0);
    }

    // =========================================================================
    // D1 row type deserialization
    // =========================================================================

    #[test]
    fn output_row_full() {
        let val = json!({
            "output_id": 100.0,
            "transaction_id": 50.0,
            "txid": "abc123",
            "vout": 0.0,
            "satoshis": 10000.0,
            "spendable": 1.0,
            "locking_script": "76a914",
            "custom_instructions": "redeem"
        });
        let row: OutputRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.output_id, Some(100.0));
        assert_eq!(row.transaction_id, Some(50.0));
        assert_eq!(row.txid, Some("abc123".to_string()));
        assert_eq!(row.satoshis.map(|v| v as u64), Some(10000));
        assert_eq!(row.spendable.map(|v| v as i32 != 0), Some(true));
    }

    #[test]
    fn output_row_nulls() {
        let val = json!({
            "output_id": null,
            "transaction_id": null,
            "txid": null,
            "vout": null,
            "satoshis": null,
            "spendable": null,
            "locking_script": null,
            "custom_instructions": null
        });
        let row: OutputRow = serde_json::from_value(val).unwrap();
        assert!(row.output_id.is_none());
        assert!(row.txid.is_none());
        assert!(row.satoshis.is_none());
    }

    #[test]
    fn balance_row_with_values() {
        let val = json!({"balance": 150000.0, "total": 5.0});
        let row: BalanceRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.balance.map(|v| v as u64), Some(150000));
        assert_eq!(row.total.map(|v| v as u32), Some(5));
    }

    #[test]
    fn balance_row_nulls_default_to_zero() {
        let val = json!({"balance": null, "total": null});
        let row: BalanceRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.balance.map(|v| v as u64).unwrap_or(0), 0);
        assert_eq!(row.total.map(|v| v as u32).unwrap_or(0), 0);
    }

    #[test]
    fn count_row_value() {
        let val = json!({"total": 42.0});
        let row: CountRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.total.map(|v| v as u32), Some(42));
    }

    #[test]
    fn transaction_row_full() {
        let val = json!({
            "transaction_id": 1.0,
            "txid": "deadbeef",
            "satoshis": -50000.0,
            "status": "completed",
            "is_outgoing": 1.0,
            "description": "test",
            "version": 1.0,
            "lock_time": 0.0
        });
        let row: TransactionRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.txid, Some("deadbeef".to_string()));
        assert_eq!(row.satoshis.map(|v| v as i64), Some(-50000));
        assert_eq!(row.is_outgoing.map(|v| v as i32 != 0), Some(true));
    }

    // =========================================================================
    // total_outputs computation logic
    // =========================================================================

    fn compute_total_outputs(
        returned_count: u32,
        limit: u32,
        actual_offset: u32,
        db_total: u32,
    ) -> u32 {
        if returned_count < limit {
            actual_offset + returned_count
        } else {
            db_total
        }
    }

    #[test]
    fn total_outputs_fewer_than_limit() {
        // Only 3 results, limit was 10 => total = offset + count
        assert_eq!(compute_total_outputs(3, 10, 0, 100), 3);
    }

    #[test]
    fn total_outputs_at_limit_uses_db_total() {
        // 10 results at limit 10 => use DB total
        assert_eq!(compute_total_outputs(10, 10, 0, 42), 42);
    }

    #[test]
    fn total_outputs_with_offset() {
        // 3 results at offset 5 => total = 5 + 3 = 8
        assert_eq!(compute_total_outputs(3, 10, 5, 100), 8);
    }

    // =========================================================================
    // ActionOutputItem serialization
    // =========================================================================

    #[test]
    fn action_output_item_serialize() {
        let item = ActionOutputItem {
            satoshis: 10000,
            spendable: true,
            output_index: 0,
            output_description: "payment".to_string(),
            basket: "default".to_string(),
            tags: vec!["p2p".to_string()],
            locking_script: Some("76a914abcd".to_string()),
            custom_instructions: None,
        };
        let val = serde_json::to_value(&item).unwrap();
        assert_eq!(val["satoshis"], 10000);
        assert_eq!(val["spendable"], true);
        assert_eq!(val["outputIndex"], 0);
        assert_eq!(val["outputDescription"], "payment");
        assert_eq!(val["basket"], "default");
        assert_eq!(val["tags"], json!(["p2p"]));
        assert_eq!(val["lockingScript"], "76a914abcd");
        assert!(val.get("customInstructions").is_none());
    }

    // =========================================================================
    // AnalyticsSenderItem serialization
    // =========================================================================

    #[test]
    fn analytics_sender_item_serializes() {
        let item = AnalyticsSenderItem {
            sender: "agent_x".to_string(),
            tx_count: 10,
            satoshis: 250000,
        };
        let val = serde_json::to_value(&item).unwrap();
        assert_eq!(val["sender"], "agent_x");
        assert_eq!(val["txCount"], 10);
        assert_eq!(val["satoshis"], 250000);
    }

    // =========================================================================
    // Analytics time window bucketing (mirrors get_analytics_summary logic)
    // =========================================================================

    #[test]
    fn analytics_time_windows() {
        let ms_24h = 24 * 60 * 60 * 1000u64;
        let ms_7d = 7 * ms_24h;
        let ms_30d = 30 * ms_24h;

        // A transaction 12h ago should be in 24h, 7d, and 30d windows
        let now_ms = 1_700_000_000_000u64;
        let ts = now_ms - (12 * 60 * 60 * 1000); // 12 hours ago
        let age = now_ms - ts;
        assert!(age <= ms_24h);
        assert!(age <= ms_7d);
        assert!(age <= ms_30d);

        // A transaction 2 days ago should NOT be in 24h, but should be in 7d and 30d
        let ts_2d = now_ms - (2 * ms_24h);
        let age_2d = now_ms - ts_2d;
        assert!(age_2d > ms_24h);
        assert!(age_2d <= ms_7d);
        assert!(age_2d <= ms_30d);

        // A transaction 10 days ago: not in 24h or 7d, but in 30d
        let ts_10d = now_ms - (10 * ms_24h);
        let age_10d = now_ms - ts_10d;
        assert!(age_10d > ms_7d);
        assert!(age_10d <= ms_30d);

        // A transaction 31 days ago: not in any time window
        let ts_31d = now_ms - (31 * ms_24h);
        let age_31d = now_ms - ts_31d;
        assert!(age_31d > ms_30d);
    }

    // =========================================================================
    // AnalyticsWindowResult Clone
    // =========================================================================

    #[test]
    fn analytics_window_clone() {
        let window = AnalyticsWindowResult {
            total_revenue: 100,
            total_transactions: 5,
            unique_senders: 2,
            senders: vec![],
        };
        let cloned = window.clone();
        assert_eq!(cloned.total_revenue, 100);
    }

    // Implement Clone for AnalyticsWindowResult if needed by checking if it derives Clone
    // Actually we test it's available for the test assertion — it's used in
    // analytics_summary_result_serializes above.
}
