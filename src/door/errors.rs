//! The error map on the wire: a storage refusal keeps its text; a database or
//! internal fault is collapsed to `WERR_INTERNAL`'s stable text and its own
//! text goes to the log only (`StorageServer.ts:113-127`;
//! `WERR_errors.ts:19-23`).

use serde_json::Value;

use crate::error::Error;
use crate::json_rpc::{JsonRpcError, INTERNAL_ERROR, INVALID_PARAMS};

/// The reference's text for an internal fault (`WERR_errors.ts:21`).
pub const INTERNAL_TEXT: &str = "An internal error has occurred.";

/// The JSON-RPC code of a `NotFound` refusal.
pub const NOT_FOUND: i32 = -32001;

/// The JSON-RPC code and the public text of `e`.
pub fn public_parts(e: &Error) -> (i32, String) {
    match e {
        Error::ValidationError(m) => (INVALID_PARAMS, m.clone()),
        Error::NotFound(m) => (NOT_FOUND, m.clone()),
        Error::DatabaseError(_) | Error::InternalError(_) => {
            (INTERNAL_ERROR, INTERNAL_TEXT.to_string())
        }
    }
}

/// The JSON-RPC error answer of the call `id` failing with `e`; a collapsed
/// fault is logged with its own text.
pub fn rpc_error(id: Value, e: &Error) -> Value {
    #[cfg(target_arch = "wasm32")]
    if matches!(e, Error::DatabaseError(_) | Error::InternalError(_)) {
        worker::console_error!("rpc fault (collapsed on the wire): {e}");
    }
    let (code, message) = public_parts(e);
    serde_json::to_value(JsonRpcError::new(id, code, message)).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_fault_is_collapsed_and_a_refusal_keeps_its_text() {
        let d1 = Error::DatabaseError("D1_ERROR: no such table: certificates".into());
        let answer = rpc_error(json!(9), &d1);
        assert_eq!(answer["error"]["code"], INTERNAL_ERROR);
        assert_eq!(answer["error"]["message"], INTERNAL_TEXT);
        assert_eq!(answer["id"], 9);
        assert!(!answer.to_string().contains("certificates"));
        let internal = Error::InternalError("R2 get failed: 500".into());
        assert_eq!(public_parts(&internal).1, INTERNAL_TEXT);
        assert_eq!(
            public_parts(&Error::ValidationError("missing identityKey".into())),
            (INVALID_PARAMS, "missing identityKey".to_string())
        );
        assert_eq!(
            public_parts(&Error::NotFound("output 99".into())),
            (NOT_FOUND, "output 99".to_string())
        );
    }
}
