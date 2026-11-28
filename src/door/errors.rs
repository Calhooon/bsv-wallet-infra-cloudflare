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
        Error::PaymentSize { size, budget } => {
            (INVALID_PARAMS, super::payment::description(*size, *budget))
        }
    }
}

/// The JSON-RPC error answer of the call `id` failing with `e`; a collapsed
/// fault is logged with its own text. A payment over the budget carries
/// `data`: the code `ERR_PAYMENT_SIZE`, the `budget` and the `size` where known.
pub fn rpc_error(id: Value, e: &Error) -> Value {
    #[cfg(target_arch = "wasm32")]
    if matches!(e, Error::DatabaseError(_) | Error::InternalError(_)) {
        worker::console_error!("rpc fault (collapsed on the wire): {e}");
    }
    let (code, message) = public_parts(e);
    let mut answer = JsonRpcError::new(id, code, message);
    if let Error::PaymentSize { size, budget } = e {
        answer.error.data = Some(super::payment::data(*size, *budget));
    }
    serde_json::to_value(answer).unwrap_or(Value::Null)
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

    #[test]
    fn a_payment_over_the_budget_is_a_refused_argument_with_its_data() {
        let answer = rpc_error(
            json!(3),
            &Error::PaymentSize {
                size: Some(614_401),
                budget: 614_400,
            },
        );
        assert_eq!(
            answer,
            json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32602,
                    "message": crate::door::payment::description(Some(614_401), 614_400),
                    "data": { "code": "ERR_PAYMENT_SIZE", "budget": 614_400, "size": 614_401 },
                },
                "id": 3,
            })
        );
        let unknown = rpc_error(
            json!(3),
            &Error::PaymentSize {
                size: None,
                budget: 614_400,
            },
        );
        assert_eq!(
            unknown["error"]["data"],
            json!({ "code": "ERR_PAYMENT_SIZE", "budget": 614_400 })
        );
        assert!(unknown["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("Not judged: this payment is more than 614400 bytes"));
    }
}
