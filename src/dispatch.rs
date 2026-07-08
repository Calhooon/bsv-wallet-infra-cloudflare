//! JSON-RPC method dispatch.
//!
//! Routes incoming JSON-RPC method calls to the appropriate StorageD1 method.
//! Phase 1: makeAvailable, migrate, findOrInsertUser, internalizeAction.
//! Phase 2: listOutputs, listActions.
//! Phase 3: abortAction, createAction, processAction, updateTransactionStatusAfterBroadcast.
//! Phase 4: reviewStatus (monitor sync).
//!
//! Params format: The toolbox StorageClient sends positional params (JSON arrays)
//! while the x402 skill sends named params (JSON objects). Handlers accept both.

use serde_json::Value;

use crate::error::Error;
use crate::json_rpc::{JsonRpcError, JsonRpcResponse};
use crate::storage::certificates::{InsertCertificateArgs, RelinquishCertificateArgs};
use crate::storage::readers::{
    GetAnalyticsSummaryArgs, GetBalanceArgs, ListActionsArgs, ListOutputsArgs,
};
use crate::storage::relinquish_output::RelinquishOutputArgs;
use crate::storage::reserve_outputs::{ReserveOutputsArgs, UnreserveOutputsArgs};
use crate::storage::StorageD1;
use crate::types::{AuthId, FindCertificatesArgs};

use bsv_sdk::wallet::{AbortActionArgs, CreateActionArgs, InternalizeActionArgs};

use crate::types::StorageProcessActionArgs;

/// Extract the actual args from params, handling both positional and named formats.
///
/// - Toolbox StorageClient sends: `[auth, args]` for auth'd methods, `[arg]` for others
/// - x402 skill / direct callers send: `{field: value}` (object)
///
/// For auth'd methods (listOutputs, listActions, internalizeAction):
///   - If array with 2+ elements: return index 1 (index 0 is auth, ignored — we use BRC-31)
///   - If array with 1 element: return index 0
///   - If object: return as-is
///
/// For non-auth'd methods (findOrInsertUser, migrate):
///   - If array with 1+ elements: return index 0
///   - If object: return as-is
fn extract_args(params: &Value, auth_method: bool) -> Value {
    match params {
        Value::Array(arr) => {
            if auth_method && arr.len() >= 2 {
                arr[1].clone()
            } else if !arr.is_empty() {
                arr[0].clone()
            } else {
                Value::Null
            }
        }
        _ => params.clone(),
    }
}

/// Dispatch a JSON-RPC method call.
///
/// The `auth` parameter is the authenticated identity from BRC-31.
/// Some methods require auth (most do), some don't (makeAvailable, migrate).
pub async fn dispatch<B: crate::services::BroadcastService + crate::services::ProofService>(
    storage: &mut StorageD1<'_, B>,
    method: &str,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Value {
    let result = match method {
        // Phase 1: Core methods
        "makeAvailable" => handle_make_available(storage, id.clone()).await,
        "migrate" => handle_migrate(storage, params, id.clone()).await,
        "findOrInsertUser" => handle_find_or_insert_user(storage, params, id.clone()).await,
        "internalizeAction" => handle_internalize_action(storage, params, id.clone(), auth).await,

        // Phase 2: Reader methods
        "listOutputs" => handle_list_outputs(storage, params, id.clone(), auth).await,
        "listActions" => handle_list_actions(storage, params, id.clone(), auth).await,
        "getBalance" => handle_get_balance(storage, params, id.clone(), auth).await,
        "getAnalyticsSummary" => {
            handle_get_analytics_summary(storage, params, id.clone(), auth).await
        }

        // Phase 3: Heavy writers
        "abortAction" => handle_abort_action(storage, params, id.clone(), auth).await,
        "createAction" => handle_create_action(storage, params, id.clone(), auth).await,
        "processAction" => handle_process_action(storage, params, id.clone(), auth).await,
        "updateTransactionStatusAfterBroadcast" => {
            handle_update_tx_status(storage, params, id.clone(), auth).await
        }

        // Certificate CRUD
        "listCertificates" => handle_list_certificates(storage, params, id.clone(), auth).await,
        "insertCertificate" => handle_insert_certificate(storage, params, id.clone(), auth).await,
        "relinquishCertificate" => {
            handle_relinquish_certificate(storage, params, id.clone(), auth).await
        }

        // Output management
        "relinquishOutput" => handle_relinquish_output(storage, params, id.clone(), auth).await,
        "reserveOutputs" => handle_reserve_outputs(storage, params, id.clone(), auth).await,
        "unreserveOutputs" => handle_unreserve_outputs(storage, params, id.clone(), auth).await,

        // Phase 4: Monitor
        "reviewStatus" => handle_review_status(storage, id.clone(), auth).await,

        // Transaction token stubs (D1 doesn't use real transactions)
        "beginStorageTransaction" => Ok(serde_json::to_value(JsonRpcResponse::success(
            id.clone(),
            serde_json::json!({ "token": 0 }),
        ))
        .unwrap()),
        "commitStorageTransaction" => {
            Ok(serde_json::to_value(JsonRpcResponse::success(id.clone(), Value::Null)).unwrap())
        }
        "rollbackStorageTransaction" => {
            Ok(serde_json::to_value(JsonRpcResponse::success(id.clone(), Value::Null)).unwrap())
        }

        // Not yet implemented
        _ => {
            return serde_json::to_value(JsonRpcError::method_not_found(id, method)).unwrap();
        }
    };

    match result {
        Ok(val) => val,
        Err(e) => {
            let (code, msg) = match &e {
                Error::ValidationError(m) => (-32602, m.clone()),
                Error::NotFound(m) => (-32001, m.clone()),
                Error::DatabaseError(m) => (-32603, m.clone()),
                Error::InternalError(m) => (-32603, m.clone()),
            };
            serde_json::to_value(JsonRpcError::new(id, code, msg)).unwrap()
        }
    }
}

// =============================================================================
// Handlers
// =============================================================================

async fn handle_make_available<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &mut StorageD1<'_, B>,
    id: Value,
) -> Result<Value, Error> {
    let settings = storage.make_available().await?;
    let result = serde_json::to_value(&settings)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

async fn handle_migrate<B: crate::services::BroadcastService + crate::services::ProofService>(
    storage: &mut StorageD1<'_, B>,
    params: Value,
    id: Value,
) -> Result<Value, Error> {
    let args = extract_args(&params, false);

    // Positional: ["storage_name"] → just a string
    // Named: {"storageName": "...", "storageIdentityKey": "..."}
    let (storage_name, storage_identity_key) = if let Some(s) = args.as_str() {
        (s.to_string(), String::new())
    } else {
        let name = args
            .get("storageName")
            .and_then(|v| v.as_str())
            .unwrap_or("wallet-infra")
            .to_string();
        let key = args
            .get("storageIdentityKey")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        (name, key)
    };

    let chain = storage
        .migrate(&storage_name, &storage_identity_key)
        .await?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, serde_json::json!(chain))).unwrap())
}

async fn handle_find_or_insert_user<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
) -> Result<Value, Error> {
    let args = extract_args(&params, false);

    // Positional: ["identity_key"] → just a string
    // Named: {"identityKey": "..."}
    let identity_key = if let Some(s) = args.as_str() {
        s.to_string()
    } else {
        args.get("identityKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ValidationError("missing identityKey".to_string()))?
            .to_string()
    };

    let (user, inserted) = storage.find_or_insert_user(&identity_key).await?;
    let result = serde_json::json!({
        "user": serde_json::to_value(&user)?,
        "isNew": inserted,
    });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

async fn handle_internalize_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("internalizeAction requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let mut args_val = extract_args(&params, true);

    // The bsv-sdk InternalizeActionArgs expects `tx` as a hex string (via #[serde(with = "hex_bytes")]).
    // But the bsv-auth-cloudflare payment middleware sends `tx` as a JSON array of byte values
    // (Vec<u8> serialized). Convert array → hex string so deserialization works for both formats.
    if let Some(tx_val) = args_val.get("tx") {
        if tx_val.is_array() {
            let bytes: Vec<u8> = tx_val
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|v| v.as_u64().map(|n| n as u8))
                .collect();
            args_val["tx"] = Value::String(hex::encode(&bytes));
        }
    }

    let args: InternalizeActionArgs = serde_json::from_value(args_val)?;
    let result = storage.internalize_action(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_list_outputs<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("listOutputs requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: ListOutputsArgs = serde_json::from_value(args_val)?;
    let result = storage.list_outputs(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_list_actions<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("listActions requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: ListActionsArgs = serde_json::from_value(args_val)?;
    let result = storage.list_actions(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_get_balance<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("getBalance requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: GetBalanceArgs = serde_json::from_value(args_val)?;
    let result = storage.get_balance(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_get_analytics_summary<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("getAnalyticsSummary requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: GetAnalyticsSummaryArgs = serde_json::from_value(args_val)?;
    let result = storage.get_analytics_summary(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

// =============================================================================
// Phase 3: Heavy writer handlers
// =============================================================================

async fn handle_abort_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("abortAction requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: AbortActionArgs = serde_json::from_value(args_val)?;
    let aborted = storage.abort_action(user_id, &args.reference).await?;
    let result = serde_json::json!({ "aborted": aborted });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

async fn handle_create_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("createAction requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: CreateActionArgs = serde_json::from_value(args_val)?;
    let result = storage.create_action(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_process_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("processAction requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: StorageProcessActionArgs = serde_json::from_value(args_val)?;
    let result = storage.process_action(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_update_tx_status<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError(
            "updateTransactionStatusAfterBroadcast requires authentication".to_string(),
        )
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;

    // This method doesn't wrap params in [auth, args] like other methods.
    // Toolbox sends: params = [txid_string, success_bool] (bare array, no auth element)
    // Named: {"txid": "...", "success": true/false}
    let (txid, success) = if let Some(arr) = params.as_array() {
        let txid = arr
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ValidationError("missing txid".to_string()))?
            .to_string();
        let success = arr.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
        (txid, success)
    } else {
        let txid = params
            .get("txid")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ValidationError("missing txid".to_string()))?
            .to_string();
        let success = params
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        (txid, success)
    };

    storage
        .update_transaction_status_after_broadcast(user_id, &txid, success)
        .await?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, Value::Null)).unwrap())
}

// =============================================================================
// Phase 4: Monitor handlers
// =============================================================================

async fn handle_review_status<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let _auth = auth.ok_or_else(|| {
        Error::ValidationError("reviewStatus requires authentication".to_string())
    })?;

    // reviewStatus runs the same logic as the cron monitor's review_status task
    let count = crate::monitor::review_status(storage.db())
        .await
        .map_err(|e| Error::InternalError(e.to_string()))?;

    let result = serde_json::json!({ "status_synced": count });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

// =============================================================================
// Certificate CRUD handlers
// =============================================================================

async fn handle_list_certificates<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("listCertificates requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: FindCertificatesArgs = serde_json::from_value(args_val)?;
    let result = storage.list_certificates(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_insert_certificate<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("insertCertificate requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: InsertCertificateArgs = serde_json::from_value(args_val)?;
    let result = storage.insert_certificate(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_relinquish_certificate<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("relinquishCertificate requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: RelinquishCertificateArgs = serde_json::from_value(args_val)?;
    let relinquished = storage.relinquish_certificate(user_id, args).await?;
    let result = serde_json::json!({ "relinquished": relinquished });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

// =============================================================================
// Output management handlers
// =============================================================================

async fn handle_relinquish_output<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("relinquishOutput requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: RelinquishOutputArgs = serde_json::from_value(args_val)?;
    let relinquished = storage.relinquish_output(user_id, args).await?;
    let result = serde_json::json!({ "relinquished": relinquished });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

/// G1 — atomic UTXO reservation. Params: `[auth, {basket, outputs, ttlSeconds?}]`.
/// Result: `{"reserved": ["txid.vout", ...]}` — the sublist that transitioned
/// free → reserved. Already-reserved/spent/absent outpoints are skipped, not
/// an error. See `storage/reserve_outputs.rs` for the full semantics.
async fn handle_reserve_outputs<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("reserveOutputs requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: ReserveOutputsArgs = serde_json::from_value(args_val)?;
    let reserved = storage.reserve_outputs(user_id, args).await?;
    let result = serde_json::json!({ "reserved": reserved });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

/// G1 — release reservations. Params: `[auth, {basket, outputs}]`.
/// Result: `{"unreserved": ["txid.vout", ...]}`. Idempotent.
async fn handle_unreserve_outputs<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("unreserveOutputs requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: UnreserveOutputsArgs = serde_json::from_value(args_val)?;
    let unreserved = storage.unreserve_outputs(user_id, args).await?;
    let result = serde_json::json!({ "unreserved": unreserved });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    // Re-import the function under test. It's private, so we test via super.
    use super::extract_args;

    // =========================================================================
    // Auth method = true (listOutputs, listActions, internalizeAction, etc.)
    // =========================================================================

    #[test]
    fn auth_positional_array_two_elements_returns_second() {
        // Toolbox sends [auth_obj, args_obj] for authenticated methods.
        let params = json!([{"identityKey": "abc"}, {"basket": "default", "limit": 10}]);
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"basket": "default", "limit": 10}));
    }

    #[test]
    fn auth_positional_array_three_elements_returns_second() {
        // Extra elements beyond index 1 are ignored.
        let params = json!(["auth", {"args": true}, "extra"]);
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"args": true}));
    }

    #[test]
    fn auth_positional_array_one_element_returns_first() {
        // Array with only 1 element: no auth prefix, so return index 0.
        let params = json!([{"basket": "default"}]);
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"basket": "default"}));
    }

    #[test]
    fn auth_empty_array_returns_null() {
        let params = json!([]);
        let result = extract_args(&params, true);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn auth_object_params_returned_as_is() {
        // Direct callers / x402 skill send named objects.
        let params = json!({"basket": "default", "limit": 5});
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"basket": "default", "limit": 5}));
    }

    // =========================================================================
    // Auth method = false (findOrInsertUser, migrate)
    // =========================================================================

    #[test]
    fn non_auth_positional_array_two_elements_returns_first() {
        // Non-auth methods always take index 0, even with 2 elements.
        let params = json!(["identity_key_123", "ignored"]);
        let result = extract_args(&params, false);
        assert_eq!(result, json!("identity_key_123"));
    }

    #[test]
    fn non_auth_positional_array_one_element_returns_first() {
        let params = json!(["identity_key_123"]);
        let result = extract_args(&params, false);
        assert_eq!(result, json!("identity_key_123"));
    }

    #[test]
    fn non_auth_empty_array_returns_null() {
        let params = json!([]);
        let result = extract_args(&params, false);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn non_auth_object_params_returned_as_is() {
        let params = json!({"identityKey": "abc123"});
        let result = extract_args(&params, false);
        assert_eq!(result, json!({"identityKey": "abc123"}));
    }

    // =========================================================================
    // Edge cases: non-array, non-object param types
    // =========================================================================

    #[test]
    fn null_params_returned_as_is() {
        let params = Value::Null;
        let result = extract_args(&params, true);
        assert_eq!(result, Value::Null);

        let result = extract_args(&params, false);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn string_params_returned_as_is() {
        // A raw string is not an array, so the match falls through to the default.
        let params = json!("just_a_string");
        let result = extract_args(&params, true);
        assert_eq!(result, json!("just_a_string"));
    }

    #[test]
    fn number_params_returned_as_is() {
        let params = json!(42);
        let result = extract_args(&params, false);
        assert_eq!(result, json!(42));
    }

    #[test]
    fn bool_params_returned_as_is() {
        let params = json!(true);
        let result = extract_args(&params, true);
        assert_eq!(result, json!(true));
    }

    // =========================================================================
    // Verify auth flag makes a difference with 2-element arrays
    // =========================================================================

    #[test]
    fn auth_flag_selects_different_index_for_two_element_array() {
        let params = json!(["first", "second"]);

        // auth=true -> index 1
        let auth_result = extract_args(&params, true);
        assert_eq!(auth_result, json!("second"));

        // auth=false -> index 0
        let non_auth_result = extract_args(&params, false);
        assert_eq!(non_auth_result, json!("first"));
    }

    #[test]
    fn nested_objects_in_array_preserved() {
        let params = json!([
            {"identityKey": "auth_key"},
            {"basket": "default", "tags": ["tag1", "tag2"], "nested": {"deep": true}}
        ]);
        let result = extract_args(&params, true);
        assert_eq!(
            result,
            json!({"basket": "default", "tags": ["tag1", "tag2"], "nested": {"deep": true}})
        );
    }
}
