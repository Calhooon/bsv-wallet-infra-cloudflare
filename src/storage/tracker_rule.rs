//! The server's script verdict and request history (#72, #73).
//! A post result is a hint. Missing bytes, era context or resources never fail a tx.
use bsv_sdk::primitives::bsv::sighash::{TxInput, TxOutput};
use bsv_sdk::script::{ProtocolEra, ScriptFlags, Spend, SpendParams};
use bsv_sdk::transaction::{Transaction, TransactionOutput};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use super::StorageD1;
use crate::d1::{batch::BatchCollector, QVal, Query};
use crate::error::{Error, Result};

/// With no stored creation heights, refuse only a script that fails under every
/// supported block era and both UTXO eras. Policy flags are never applied.
/// A local resource refusal or absent input context is no consensus verdict.
pub fn script_refusal(tx: &Transaction, sources: &[TransactionOutput]) -> Option<String> {
    if tx.inputs.is_empty() || sources.len() != tx.inputs.len() {
        return None;
    }
    let inputs: Vec<TxInput> = tx
        .inputs
        .iter()
        .map(|i| {
            Some(TxInput {
                txid: i.get_source_txid_bytes().ok()?,
                output_index: i.source_output_index,
                script: i.unlocking_script.as_ref()?.to_binary(),
                sequence: i.sequence,
            })
        })
        .collect::<Option<_>>()?;
    let outputs: Vec<TxOutput> = tx
        .outputs
        .iter()
        .map(|o| {
            Some(TxOutput {
                satoshis: o.satoshis?,
                script: o.locking_script.to_binary(),
            })
        })
        .collect::<Option<_>>()?;
    for (vin, (input, source)) in tx.inputs.iter().zip(sources).enumerate() {
        let mut refused = true;
        let mut reason = String::new();
        for era in [ProtocolEra::PostGenesis, ProtocolEra::PostChronicle] {
            for after in [false, true] {
                let mut spend = Spend::new(SpendParams {
                    source_txid: inputs[vin].txid,
                    source_output_index: input.source_output_index,
                    source_satoshis: source.satoshis?,
                    locking_script: source.locking_script.clone(),
                    transaction_version: tx.version as i32,
                    other_inputs: inputs
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != vin)
                        .map(|(_, i)| i.clone())
                        .collect(),
                    outputs: outputs.clone(),
                    input_index: vin,
                    unlocking_script: input.unlocking_script.as_ref()?.clone(),
                    input_sequence: input.sequence,
                    lock_time: tx.lock_time,
                    memory_limit: None,
                });
                spend.set_flags(ScriptFlags::block(era));
                spend.set_utxo_after_chronicle(after);
                match spend.validate() {
                    Ok(true) => refused = false,
                    Ok(false) => reason = "script did not succeed".into(),
                    Err(e) if e.is_resource_limit() => return None,
                    Err(e) => reason = e.message,
                }
            }
        }
        if refused {
            return Some(format!("input {vin}: {reason}"));
        }
    }
    None
}

/// Append against the current DB value, so concurrent posts cannot lose notes.
/// Preserve other history keys and keep the newest 32 notes, as toolbox v0.7.4.
pub(crate) const APPEND_NOTE_SQL: &str = "UPDATE proven_tx_reqs SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), history = json_set(\
    CASE WHEN json_valid(history) AND json_type(history) = 'object' THEN history \
         ELSE json_object('previous', history) END, '$.notes', \
    json((SELECT json_group_array(json(value)) FROM (\
        SELECT value FROM json_each(CASE WHEN json_valid(history) AND json_type(history, '$.notes') = 'array' \
             THEN json_extract(history, '$.notes') ELSE '[]' END) \
        WHERE key >= json_array_length(CASE WHEN json_valid(history) AND json_type(history, '$.notes') = 'array' \
             THEN json_extract(history, '$.notes') ELSE '[]' END) - 31 \
        UNION ALL SELECT ?)))) WHERE txid = ?";

pub(crate) async fn append_note(db: &worker::D1Database, txid: &str, note: Value) -> Result<()> {
    Query::new(APPEND_NOTE_SQL)
        .bind(note.to_string())
        .bind(txid)
        .execute(db)
        .await?;
    Ok(())
}

impl<B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'_, B> {
    /// Only locally stored parents are read, never a network index or explorer.
    pub(super) async fn local_script_refusal(&self, raw: &[u8]) -> Result<Option<String>> {
        let Ok(tx) = Transaction::from_binary(raw) else {
            return Ok(None);
        };
        let mut sources = Vec::with_capacity(tx.inputs.len());
        #[derive(Deserialize)]
        struct Source {
            transaction_id: f64,
            raw_tx: String,
        }
        for input in &tx.inputs {
            let Some(txid) = input.source_txid.as_deref() else {
                return Ok(None);
            };
            let row: Option<Source> = Query::new("SELECT transaction_id, hex(raw_tx) AS raw_tx FROM transactions WHERE txid = ? ORDER BY transaction_id LIMIT 1")
                .bind(txid).fetch_optional(self.db).await?;
            let Some(row) = row else { return Ok(None) };
            let inline =
                hex::decode(&row.raw_tx).map_err(|e| Error::InternalError(e.to_string()))?;
            let bytes = if inline.is_empty() {
                crate::r2::BlobStore::new(self.blobs)
                    .get("transactions", row.transaction_id as i64, "raw_tx", None)
                    .await?
            } else {
                Some(inline)
            };
            let Some(bytes) = bytes else { return Ok(None) };
            let Ok(parent) = Transaction::from_binary(&bytes) else {
                return Ok(None);
            };
            if parent.id() != txid {
                return Ok(None);
            }
            let Some(output) = parent.outputs.get(input.source_output_index as usize) else {
                return Ok(None);
            };
            sources.push(output.clone());
        }
        Ok(script_refusal(&tx, &sources))
    }

    pub(super) async fn record_hint(
        &self,
        txid: &str,
        what: &str,
        outcome: &str,
        words: Value,
        retry: bool,
    ) -> Result<()> {
        let mut batch = BatchCollector::new(self.db);
        batch.add(
            APPEND_NOTE_SQL,
            vec![
                QVal::Text(
                    json!({
                        "when": Utc::now().to_rfc3339(), "what": what, "outcome": outcome,
                        "words": words, "nextReaskMinutes": if retry { Some(5) } else { None },
                    })
                    .to_string(),
                ),
                QVal::Text(txid.into()),
            ],
        )?;
        if retry {
            batch.add("UPDATE proven_tx_reqs SET status = 'unsent', attempts = attempts + 1, updated_at = ? WHERE txid = ? AND status IN ('unsent', 'sending', 'unprocessed')", vec![QVal::Text(Utc::now().to_rfc3339()), QVal::Text(txid.into())])?;
        }
        batch.execute().await?;
        Ok(())
    }

    pub(super) async fn fail_local_script(
        &self,
        user_id: i64,
        txid: &str,
        reason: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut batch = BatchCollector::new(self.db);
        batch.add(APPEND_NOTE_SQL, vec![QVal::Text(json!({"when": now, "what": "localScriptVerdict", "outcome": "invalid", "words": [reason]}).to_string()), QVal::Text(txid.into())])?;
        // A checked proof or an earlier host retirement stands. All book writes
        // share the same status guard, in one D1 batch.
        const IDS: &str = "SELECT transaction_id FROM transactions WHERE txid = ? AND user_id = ? AND status IN ('unsigned', 'unprocessed', 'sending', 'nosend', 'nonfinal') AND NOT EXISTS (SELECT 1 FROM proven_tx_reqs r WHERE r.txid = transactions.txid AND r.status IN ('completed', 'doubleSpend'))";
        batch.add(&format!("UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? WHERE spent_by IN ({IDS}) AND basket_id IS NOT NULL"), vec![QVal::Text(now.clone()), QVal::Text(txid.into()), QVal::Int(user_id)])?;
        batch.add(
            &format!(
                "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id IN ({IDS})"
            ),
            vec![
                QVal::Text(now.clone()),
                QVal::Text(txid.into()),
                QVal::Int(user_id),
            ],
        )?;
        batch.add(&format!("UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE txid = ? AND status NOT IN ('completed', 'doubleSpend') AND EXISTS (SELECT 1 FROM transactions WHERE transaction_id IN ({IDS}))"), vec![QVal::Text(now.clone()), QVal::Text(txid.into()), QVal::Text(txid.into()), QVal::Int(user_id)])?;
        batch.add(&format!("UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id IN ({IDS})"), vec![QVal::Text(now), QVal::Text(txid.into()), QVal::Int(user_id)])?;
        batch.execute().await?;
        Ok(())
    }
}
