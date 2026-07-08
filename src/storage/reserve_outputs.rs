//! reserveOutputs / unreserveOutputs — strict atomic UTXO reservation (G1).
//!
//! Purpose: two concurrent callers (e.g. two blackjack `/new` requests landing
//! on different Worker isolates) must never select the same stake UTXO. The
//! previous KV-based soft reservation was eventually consistent and racy; this
//! is the hard, D1-backed replacement.
//!
//! Semantics
//! ---------
//! * A reservation is a `reserved_until` timestamp on the `outputs` row
//!   (migration 0003). An output is RESERVED iff `reserved_until IS NOT NULL
//!   AND datetime(reserved_until) > datetime('now')`.
//! * `reserveOutputs` atomically transitions free → reserved using the same
//!   single-statement `UPDATE ... WHERE output_id = (SELECT ...) RETURNING`
//!   pattern as `create_action.rs::allocate_change_input`. SQLite serializes
//!   writes, so of two concurrent calls naming the same outpoint exactly one
//!   sees it free and wins; the loser's subquery returns no row.
//! * Outpoints that were already reserved / spent / absent are SKIPPED, not an
//!   error — the result lists only the sublist actually reserved. Callers
//!   diff request vs. result to learn what they got.
//! * Expiry is the ONLY automatic release path. Nothing in the monitor cron
//!   (`fail_abandoned`), `abort_action`, or `process_action` failure handling
//!   touches `reserved_until` — see the comments at those release sites. The
//!   manual release path is `unreserveOutputs` (owner only).
//! * A reserved output remains spendable when EXPLICITLY named as a
//!   createAction input by the same auth-scoped user — that is how the
//!   reserver consumes its own reservation. Only auto-selection and competing
//!   reserveOutputs calls are gated.
//!
//! Wire shapes (FROZEN — the blackjack worker calls exactly this):
//!   reserveOutputs   params `[auth, {"basket": "<name>",
//!                                    "outputs": ["txid.vout", ...],
//!                                    "ttlSeconds": <u32, optional, default 300>}]`
//!                    → result `{"reserved": ["txid.vout", ...]}`
//!   unreserveOutputs params `[auth, {"basket": "<name>",
//!                                    "outputs": ["txid.vout", ...]}]`
//!                    → result `{"unreserved": ["txid.vout", ...]}`

use crate::d1::Query;
use crate::error::Result;
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use super::relinquish_output::OutputRef;
use super::StorageD1;

// =============================================================================
// TTL policy
// =============================================================================

/// Default reservation TTL when the caller omits `ttlSeconds`.
pub const DEFAULT_TTL_SECONDS: u32 = 300;

/// Upper clamp on `ttlSeconds`. A buggy caller passing u32::MAX must not lock
/// its own pool for 136 years; 24h is generous for any legitimate hold and
/// still recoverable via unreserveOutputs.
pub const MAX_TTL_SECONDS: u32 = 86_400;

/// Clamp a requested TTL into [1, MAX_TTL_SECONDS], defaulting when absent.
fn clamp_ttl(requested: Option<u32>) -> i64 {
    let ttl = requested.unwrap_or(DEFAULT_TTL_SECONDS);
    let ttl = ttl.clamp(1, MAX_TTL_SECONDS);
    ttl as i64
}

/// Canonical "txid.vout" outpoint string (the wire format of the result lists).
fn format_outpoint(txid: &str, vout: u32) -> String {
    format!("{}.{}", txid, vout)
}

// =============================================================================
// Args
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReserveOutputsArgs {
    pub basket: String,
    /// Outpoints as `"txid.vout"` strings (also accepts `{txid, vout}` objects).
    pub outputs: Vec<OutputRef>,
    /// Seconds until the reservation self-expires. Default 300, clamped to 24h.
    #[serde(default)]
    pub ttl_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnreserveOutputsArgs {
    pub basket: String,
    pub outputs: Vec<OutputRef>,
}

// =============================================================================
// D1 row type
// =============================================================================

#[derive(Debug, Deserialize)]
struct ReservedRow {
    txid: Option<String>,
    vout: Option<f64>,
}

// =============================================================================
// Storage methods
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// Atomically reserve the named outputs for `ttl_seconds`.
    ///
    /// Returns the sublist of outpoints (as `"txid.vout"`) that transitioned
    /// free → reserved. Outpoints skipped because they were already reserved,
    /// spent, spendable=0, in a non-spendable tx status, in a different
    /// basket, or unknown are simply absent from the result — NOT an error.
    pub async fn reserve_outputs(
        &self,
        user_id: i64,
        args: ReserveOutputsArgs,
    ) -> Result<Vec<String>> {
        let now = Utc::now();
        let until = now + Duration::seconds(clamp_ttl(args.ttl_seconds));

        let mut reserved: Vec<String> = Vec::with_capacity(args.outputs.len());

        for op in &args.outputs {
            // Single atomic statement, same pattern as allocate_change_input:
            // the subquery finds the row iff it is currently FREE (spendable,
            // unspent, spendable tx status, right basket, no live reservation),
            // the UPDATE stamps the reservation, RETURNING confirms the win.
            // All under one SQLite write lock — no race window. The outer
            // WHERE re-states the free predicates defensively (mirrors
            // create_action.rs's repeated `spent_by IS NULL` guard).
            let rows: Vec<ReservedRow> = Query::new(
                r#"UPDATE outputs SET reserved_until = ?, updated_at = ?
                   WHERE output_id = (
                       SELECT o.output_id FROM outputs o
                       JOIN output_baskets ob ON o.basket_id = ob.basket_id
                       JOIN transactions t ON o.transaction_id = t.transaction_id
                       WHERE o.user_id = ? AND o.txid = ? AND o.vout = ?
                         AND ob.name = ?
                         AND o.spendable = 1 AND o.spent_by IS NULL
                         AND t.status IN ('completed', 'unproven', 'nosend', 'sending')
                         AND (o.reserved_until IS NULL
                              OR datetime(o.reserved_until) <= datetime('now'))
                       LIMIT 1
                   )
                   AND spendable = 1 AND spent_by IS NULL
                   AND (reserved_until IS NULL
                        OR datetime(reserved_until) <= datetime('now'))
                   RETURNING txid, vout"#,
            )
            .bind(until)
            .bind(now)
            .bind(user_id)
            .bind(op.txid.as_str())
            .bind(op.vout as i64)
            .bind(args.basket.as_str())
            .fetch_all(self.db())
            .await?;

            if let Some(row) = rows.into_iter().next() {
                let txid = row.txid.unwrap_or_else(|| op.txid.clone());
                let vout = row.vout.map(|v| v as u32).unwrap_or(op.vout);
                reserved.push(format_outpoint(&txid, vout));
            }
        }

        Ok(reserved)
    }

    /// Release reservations the caller placed with `reserve_outputs`.
    ///
    /// Idempotent: clears `reserved_until` (live OR already expired) on the
    /// named outputs and returns the sublist actually cleared. Outputs with no
    /// reservation, in another basket, or unknown are skipped — NOT an error.
    pub async fn unreserve_outputs(
        &self,
        user_id: i64,
        args: UnreserveOutputsArgs,
    ) -> Result<Vec<String>> {
        let now = Utc::now();

        let mut unreserved: Vec<String> = Vec::with_capacity(args.outputs.len());

        for op in &args.outputs {
            let rows: Vec<ReservedRow> = Query::new(
                r#"UPDATE outputs SET reserved_until = NULL, updated_at = ?
                   WHERE output_id = (
                       SELECT o.output_id FROM outputs o
                       JOIN output_baskets ob ON o.basket_id = ob.basket_id
                       WHERE o.user_id = ? AND o.txid = ? AND o.vout = ?
                         AND ob.name = ?
                         AND o.reserved_until IS NOT NULL
                       LIMIT 1
                   )
                   AND reserved_until IS NOT NULL
                   RETURNING txid, vout"#,
            )
            .bind(now)
            .bind(user_id)
            .bind(op.txid.as_str())
            .bind(op.vout as i64)
            .bind(args.basket.as_str())
            .fetch_all(self.db())
            .await?;

            if let Some(row) = rows.into_iter().next() {
                let txid = row.txid.unwrap_or_else(|| op.txid.clone());
                let vout = row.vout.map(|v| v as u32).unwrap_or(op.vout);
                unreserved.push(format_outpoint(&txid, vout));
            }
        }

        Ok(unreserved)
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
    // TTL clamping (the reservation state machine's only pure-Rust arm —
    // the free→reserved transition itself lives in the atomic SQL above)
    // =========================================================================

    #[test]
    fn ttl_defaults_to_300() {
        assert_eq!(clamp_ttl(None), 300);
    }

    #[test]
    fn ttl_passes_through_in_range() {
        assert_eq!(clamp_ttl(Some(60)), 60);
        assert_eq!(clamp_ttl(Some(1)), 1);
        assert_eq!(clamp_ttl(Some(86_400)), 86_400);
    }

    #[test]
    fn ttl_zero_clamps_to_one() {
        // ttl=0 would create an instantly-expired (useless) reservation and
        // make "reserved" ambiguous — clamp to the minimum meaningful hold.
        assert_eq!(clamp_ttl(Some(0)), 1);
    }

    #[test]
    fn ttl_clamps_to_24h_max() {
        assert_eq!(clamp_ttl(Some(u32::MAX)), 86_400);
        assert_eq!(clamp_ttl(Some(86_401)), 86_400);
    }

    // =========================================================================
    // Outpoint formatting (result wire shape)
    // =========================================================================

    #[test]
    fn outpoint_format_is_txid_dot_vout() {
        assert_eq!(format_outpoint("abcd", 0), "abcd.0");
        assert_eq!(format_outpoint("abcd", 42), "abcd.42");
    }

    // =========================================================================
    // Args deserialization — the FROZEN wire shape
    // =========================================================================

    #[test]
    fn reserve_args_frozen_wire_shape() {
        // Exactly what the blackjack worker sends.
        let val = json!({
            "basket": "bj-pool",
            "outputs": [
                "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb.0",
                "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb.3"
            ],
            "ttlSeconds": 120
        });
        let args: ReserveOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "bj-pool");
        assert_eq!(args.outputs.len(), 2);
        assert_eq!(args.outputs[0].vout, 0);
        assert_eq!(args.outputs[1].vout, 3);
        assert_eq!(args.ttl_seconds, Some(120));
    }

    #[test]
    fn reserve_args_ttl_optional() {
        let val = json!({
            "basket": "default",
            "outputs": ["deadbeef.1"]
        });
        let args: ReserveOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.ttl_seconds, None);
        assert_eq!(clamp_ttl(args.ttl_seconds), 300);
    }

    #[test]
    fn reserve_args_accepts_object_outpoints() {
        // OutputRef also accepts {txid, vout} objects (same as relinquishOutput).
        let val = json!({
            "basket": "default",
            "outputs": [{"txid": "ff00", "vout": 7}]
        });
        let args: ReserveOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.outputs[0].txid, "ff00");
        assert_eq!(args.outputs[0].vout, 7);
    }

    #[test]
    fn reserve_args_empty_outputs_ok() {
        // Empty list is legal — result will just be an empty "reserved" list.
        let val = json!({"basket": "default", "outputs": []});
        let args: ReserveOutputsArgs = serde_json::from_value(val).unwrap();
        assert!(args.outputs.is_empty());
    }

    #[test]
    fn reserve_args_missing_basket_fails() {
        let val = json!({"outputs": ["aa.0"]});
        assert!(serde_json::from_value::<ReserveOutputsArgs>(val).is_err());
    }

    #[test]
    fn reserve_args_missing_outputs_fails() {
        let val = json!({"basket": "default"});
        assert!(serde_json::from_value::<ReserveOutputsArgs>(val).is_err());
    }

    #[test]
    fn unreserve_args_frozen_wire_shape() {
        let val = json!({
            "basket": "bj-pool",
            "outputs": ["deadbeef.2"]
        });
        let args: UnreserveOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.basket, "bj-pool");
        assert_eq!(args.outputs[0].txid, "deadbeef");
        assert_eq!(args.outputs[0].vout, 2);
    }

    #[test]
    fn unreserve_args_rejects_ttl_free_extra_fields_ignored() {
        // Extra/unknown fields are ignored (serde default) — forward-compatible.
        let val = json!({
            "basket": "default",
            "outputs": ["aa.0"],
            "ttlSeconds": 500
        });
        let args: UnreserveOutputsArgs = serde_json::from_value(val).unwrap();
        assert_eq!(args.outputs.len(), 1);
    }

    // =========================================================================
    // D1 RETURNING row deserialization (floats, as D1 delivers numbers)
    // =========================================================================

    #[test]
    fn reserved_row_from_d1_json() {
        let val = json!({"txid": "abcd", "vout": 5.0});
        let row: ReservedRow = serde_json::from_value(val).unwrap();
        assert_eq!(row.txid.as_deref(), Some("abcd"));
        assert_eq!(row.vout.map(|v| v as u32), Some(5));
    }
}
