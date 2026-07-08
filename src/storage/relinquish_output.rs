//! relinquishOutput — remove an output from wallet tracking.
//!
//! Sets `basket_id = NULL` AND `spendable = 0` (G5 — external-spend safety).
//!
//! The TS reference (`wallet-toolbox/src/storage/StorageProvider.ts:652`) only
//! nulls basketId; we deliberately go further. In our deployments tracked
//! outpoints get spent ON-CHAIN outside wallet-infra's view (e.g. a blackjack
//! stake consumed by the escrow covenant). Basket-null alone kept the row
//! `spendable = 1`, so it still inflated getBalance and no-basket listOutputs,
//! and remained explicitly lockable by createAction — a phantom UTXO. Every
//! known caller (blackjack pool reconcile, refund middleware, agents cmd_send)
//! relinquishes precisely BECAUSE the output has left the wallet, so
//! spendable=0 is the correct terminal state for all of them.
//!
//! G5 monitor spent-scan — IMPLEMENTED (2026-07-05): `monitor.rs` task 10,
//! `scan_external_spends`. Relinquish stays the client-driven fast path; the
//! monitor cron is the service-side safety net for clients that skip it. It
//! pages candidates (`spendable = 1`, unlocked, chain-real parent) against
//! WoC's outpoint-spent endpoint (`GET /tx/{txid}/{vout}/spent` — the Bitails
//! equivalent guessed below turned out not to exist; probed 500 on
//! 2026-07-05) and sets `spendable = 0` under a spent_by-guard. SEEN = final
//! (owner rule): a mempool-only spending tx counts, no waiting for mining.

use crate::d1::Query;
use crate::error::{Error, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::StorageD1;

// =============================================================================
// Input types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelinquishOutputArgs {
    pub basket: String,
    pub output: OutputRef,
}

/// Outpoint reference — accepts EITHER:
/// - Object: `{"txid": "abc...", "vout": 0}`
/// - String: `"abc...def.0"` (bsv-rs SDK format)
#[derive(Debug, Clone, Serialize)]
pub struct OutputRef {
    pub txid: String,
    pub vout: u32,
}

impl<'de> serde::Deserialize<'de> for OutputRef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};

        struct OutputRefVisitor;

        impl<'de> Visitor<'de> for OutputRefVisitor {
            type Value = OutputRef;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("string \"txid.vout\" or object {txid, vout}")
            }

            fn visit_str<E: de::Error>(self, s: &str) -> std::result::Result<Self::Value, E> {
                let dot = s.rfind('.').ok_or_else(|| de::Error::custom("missing '.' in outpoint string"))?;
                let txid = &s[..dot];
                let vout: u32 = s[dot + 1..].parse().map_err(de::Error::custom)?;
                Ok(OutputRef { txid: txid.to_string(), vout })
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> std::result::Result<Self::Value, M::Error> {
                let mut txid = None;
                let mut vout = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "txid" => txid = Some(map.next_value()?),
                        "vout" => vout = Some(map.next_value()?),
                        _ => { let _ = map.next_value::<serde::de::IgnoredAny>()?; }
                    }
                }
                Ok(OutputRef {
                    txid: txid.ok_or_else(|| de::Error::missing_field("txid"))?,
                    vout: vout.ok_or_else(|| de::Error::missing_field("vout"))?,
                })
            }
        }

        deserializer.deserialize_any(OutputRefVisitor)
    }
}

// =============================================================================
// D1 row type for the lookup query
// =============================================================================

#[derive(Debug, Deserialize)]
struct OutputBasketRow {
    output_id: Option<f64>,
    basket_name: Option<String>,
}

// =============================================================================
// Storage method
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// Relinquish an output: set basket_id = NULL AND spendable = 0 (G5).
    ///
    /// Verifies the output exists, belongs to the user, currently has a basket,
    /// and that the basket matches the one specified in the args.
    /// Returns true if a row was updated, false if not found.
    pub async fn relinquish_output(
        &self,
        user_id: i64,
        args: RelinquishOutputArgs,
    ) -> Result<bool> {
        let now = Utc::now().to_rfc3339();

        // Look up the output and its basket name in one query
        let row: Option<OutputBasketRow> = Query::new(
            "SELECT o.output_id, ob.name as basket_name \
             FROM outputs o \
             LEFT JOIN output_baskets ob ON o.basket_id = ob.basket_id \
             WHERE o.user_id = ? AND o.txid = ? AND o.vout = ? AND o.basket_id IS NOT NULL",
        )
        .bind(user_id)
        .bind(args.output.txid.as_str())
        .bind(args.output.vout as i64)
        .fetch_optional(self.db())
        .await?;

        let row = match row {
            Some(r) => r,
            None => return Ok(false),
        };

        // Verify the basket matches
        let basket_name = row.basket_name.unwrap_or_default();
        if basket_name != args.basket {
            return Err(Error::ValidationError(format!(
                "Output is in basket '{}', not '{}'",
                basket_name, args.basket
            )));
        }

        let output_id = row.output_id.map(|v| v as i64).unwrap_or(0);

        // Relinquish = terminal untracking: basket_id = NULL (out of every
        // basket-scoped view) AND spendable = 0 (out of balance, listOutputs,
        // and explicit createAction locking — G5). Also clear any leftover
        // reservation: a relinquished output can never return to the free
        // pool, so its reservation is moot (this does NOT weaken the G4 rule —
        // expiry stays the only path back to FREE; this path goes to GONE).
        let meta = Query::new(
            "UPDATE outputs SET basket_id = NULL, spendable = 0, reserved_until = NULL, updated_at = ? WHERE output_id = ? AND user_id = ?",
        )
        .bind(now.as_str())
        .bind(output_id)
        .bind(user_id)
        .execute(self.db())
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
    // RelinquishOutputArgs deserialization
    // =========================================================================

    #[test]
    fn relinquish_output_args_from_json() {
        let val = json!({
            "basket": "default",
            "output": {
                "txid": "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb",
                "vout": 0
            }
        });
        let args: RelinquishOutputArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "default");
        assert_eq!(
            args.output.txid,
            "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb"
        );
        assert_eq!(args.output.vout, 0);
    }

    #[test]
    fn relinquish_output_args_camel_case() {
        // Verify camelCase serde works (though all fields here are single-word,
        // the OutputRef fields are also single-word; this confirms the struct
        // deserializes from the expected JSON shape).
        let val = json!({
            "basket": "tokens",
            "output": {"txid": "ff00", "vout": 3}
        });
        let args: RelinquishOutputArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "tokens");
        assert_eq!(args.output.txid, "ff00");
        assert_eq!(args.output.vout, 3);
    }

    // =========================================================================
    // OutputRef deserialization
    // =========================================================================

    #[test]
    fn output_ref_from_json() {
        let val = json!({
            "txid": "deadbeef",
            "vout": 42
        });
        let oref: OutputRef = serde_json::from_value(val).unwrap();
        assert_eq!(oref.txid, "deadbeef");
        assert_eq!(oref.vout, 42);
    }

    #[test]
    fn output_ref_vout_zero() {
        let val = json!({"txid": "0000", "vout": 0});
        let oref: OutputRef = serde_json::from_value(val).unwrap();
        assert_eq!(oref.vout, 0);
    }

    #[test]
    fn output_ref_from_string() {
        // bsv-rs SDK serializes Outpoint as "txid.vout"
        let val = json!("aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb.0");
        let oref: OutputRef = serde_json::from_value(val).unwrap();
        assert_eq!(oref.txid, "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb");
        assert_eq!(oref.vout, 0);
    }

    #[test]
    fn output_ref_string_vout_nonzero() {
        let val = json!("deadbeef.42");
        let oref: OutputRef = serde_json::from_value(val).unwrap();
        assert_eq!(oref.txid, "deadbeef");
        assert_eq!(oref.vout, 42);
    }

    #[test]
    fn relinquish_args_with_string_output() {
        let val = json!({
            "basket": "default",
            "output": "aabb.0"
        });
        let args: RelinquishOutputArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.output.txid, "aabb");
        assert_eq!(args.output.vout, 0);
    }

    // =========================================================================
    // Error cases: missing fields
    // =========================================================================

    #[test]
    fn missing_basket_field_fails() {
        let val = json!({
            "output": {"txid": "aa", "vout": 0}
        });
        let result = serde_json::from_value::<RelinquishOutputArgs>(val);
        assert!(result.is_err());
    }

    #[test]
    fn missing_output_field_fails() {
        let val = json!({
            "basket": "default"
        });
        let result = serde_json::from_value::<RelinquishOutputArgs>(val);
        assert!(result.is_err());
    }

    #[test]
    fn missing_txid_in_output_fails() {
        let val = json!({
            "basket": "default",
            "output": {"vout": 0}
        });
        let result = serde_json::from_value::<RelinquishOutputArgs>(val);
        assert!(result.is_err());
    }

    #[test]
    fn missing_vout_in_output_fails() {
        let val = json!({
            "basket": "default",
            "output": {"txid": "aa"}
        });
        let result = serde_json::from_value::<RelinquishOutputArgs>(val);
        assert!(result.is_err());
    }

    // =========================================================================
    // Serialization round-trip
    // =========================================================================

    #[test]
    fn relinquish_output_args_round_trip() {
        let args = RelinquishOutputArgs {
            basket: "default".to_string(),
            output: OutputRef {
                txid: "abcd1234".to_string(),
                vout: 1,
            },
        };
        let val = serde_json::to_value(&args).unwrap();
        assert_eq!(val["basket"], "default");
        assert_eq!(val["output"]["txid"], "abcd1234");
        assert_eq!(val["output"]["vout"], 1);

        // Deserialize back
        let args2: RelinquishOutputArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args2.basket, args.basket);
        assert_eq!(args2.output.txid, args.output.txid);
        assert_eq!(args2.output.vout, args.output.vout);
    }

    // =========================================================================
    // OutputBasketRow deserialization (D1 format)
    // =========================================================================

    #[test]
    fn output_basket_row_from_d1_json() {
        let val = json!({
            "output_id": 42.0,
            "basket_name": "default"
        });
        let row: OutputBasketRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.output_id.map(|v| v as i64), Some(42));
        assert_eq!(row.basket_name.as_deref(), Some("default"));
    }

    #[test]
    fn output_basket_row_null_basket_name() {
        let val = json!({
            "output_id": 1.0,
            "basket_name": null
        });
        let row: OutputBasketRow = serde_json::from_value(val).unwrap();
        assert!(row.basket_name.is_none());
    }
}
