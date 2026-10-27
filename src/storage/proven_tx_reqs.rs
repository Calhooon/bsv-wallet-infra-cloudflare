//! Authenticated request history reads (#73), scoped through wallet transactions.
use super::StorageD1;
use crate::d1::Query;
use crate::entities::TableProvenTxReq;
use crate::error::{Error, Result};
use crate::types::FindProvenTxReqsArgs;
use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Deserialize)]
struct Row {
    proven_tx_req_id: f64,
    txid: String,
    status: String,
    attempts: f64,
    history: String,
    notified: f64,
    notify: String,
    raw_tx: String,
    input_beef: String,
    proven_tx_id: Option<f64>,
    batch: Option<String>,
    created_at: String,
    updated_at: String,
}
fn date(s: &str) -> Result<DateTime<Utc>> {
    Ok(super::writers::parse_datetime_pub(&Some(s.to_string())))
}
impl<B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'_, B> {
    pub async fn find_proven_tx_reqs(
        &self,
        user_id: i64,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        let mut sql = String::from("SELECT r.proven_tx_req_id, r.txid, r.status, r.attempts, r.history, r.notified, r.notify, hex(r.raw_tx) AS raw_tx, hex(r.input_beef) AS input_beef, r.proven_tx_id, r.batch, r.created_at, r.updated_at FROM proven_tx_reqs r WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.txid = r.txid AND t.user_id = ?)");
        let mut values = vec![crate::d1::QVal::Int(user_id)];
        for (column, filter) in [
            ("r.txid", args.txids),
            (
                "r.status",
                args.status
                    .map(|s| s.into_iter().map(|s| s.as_str().to_string()).collect()),
            ),
        ] {
            if let Some(filter) = filter {
                if filter.is_empty() {
                    continue;
                }
                sql.push_str(&format!(
                    " AND {column} IN ({})",
                    vec!["?"; filter.len()].join(",")
                ));
                values.extend(filter.into_iter().map(crate::d1::QVal::Text));
            }
        }
        if let Some(since) = args.base.since {
            sql.push_str(" AND julianday(r.updated_at) > julianday(?)");
            values.push(crate::d1::QVal::Text(since.to_rfc3339()));
        }
        let direction = if args.base.order_descending.unwrap_or(false) {
            "DESC"
        } else {
            "ASC"
        };
        sql.push_str(&format!(
            " ORDER BY julianday(r.updated_at) {direction}, r.proven_tx_req_id {direction} LIMIT ? OFFSET ?"
        ));
        let paged = args.base.paged.unwrap_or_default();
        values.push(crate::d1::QVal::Int(i64::from(
            paged
                .limit
                .unwrap_or(crate::door::bounds::DEFAULT_LIST_LIMIT),
        )));
        values.push(crate::d1::QVal::Int(paged.offset.unwrap_or(0) as i64));
        let mut query = Query::new(sql);
        for value in values {
            query = query.bind(value);
        }
        let rows: Vec<Row> = query.fetch_all(self.db).await?;
        let store = crate::r2::BlobStore::new(self.blobs);
        let mut result = Vec::with_capacity(rows.len());
        for r in rows {
            let id = r.proven_tx_req_id as i64;
            let decode = |s: &str| -> Result<Option<Vec<u8>>> {
                if s.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(
                        hex::decode(s).map_err(|e| Error::InternalError(e.to_string()))?,
                    ))
                }
            };
            result.push(TableProvenTxReq {
                proven_tx_req_id: id,
                txid: r.txid,
                status: serde_json::from_value(serde_json::Value::String(r.status))?,
                attempts: r.attempts as i32,
                history: r.history,
                notified: r.notified != 0.0,
                notify: r.notify,
                raw_tx: store
                    .get("proven_tx_reqs", id, "raw_tx", decode(&r.raw_tx)?)
                    .await?,
                input_beef: store
                    .get("proven_tx_reqs", id, "input_beef", decode(&r.input_beef)?)
                    .await?,
                proven_tx_id: r.proven_tx_id.map(|v| v as i64),
                batch: r.batch,
                created_at: date(&r.created_at)?,
                updated_at: date(&r.updated_at)?,
            });
        }
        Ok(result)
    }
}
