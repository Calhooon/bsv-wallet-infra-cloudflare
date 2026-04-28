//! JSON-RPC 2.0 request/response types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC 2.0 request.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Value,
}

/// JSON-RPC 2.0 success response.
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub result: Value,
    pub id: Value,
}

/// JSON-RPC 2.0 error response.
#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub jsonrpc: &'static str,
    pub error: JsonRpcErrorBody,
    pub id: Value,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcErrorBody {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// Standard JSON-RPC error codes
pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;

impl JsonRpcResponse {
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            result,
            id,
        }
    }
}

impl JsonRpcError {
    pub fn new(id: Value, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            error: JsonRpcErrorBody {
                code,
                message: message.into(),
                data: None,
            },
            id,
        }
    }

    pub fn method_not_found(id: Value, method: &str) -> Self {
        Self::new(
            id,
            METHOD_NOT_FOUND,
            format!("Method not found: {}", method),
        )
    }

    pub fn invalid_params(id: Value, msg: impl Into<String>) -> Self {
        Self::new(id, INVALID_PARAMS, msg)
    }

    pub fn internal_error(id: Value, msg: impl Into<String>) -> Self {
        Self::new(id, INTERNAL_ERROR, msg)
    }

    pub fn parse_error() -> Self {
        Self::new(Value::Null, PARSE_ERROR, "Parse error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // JsonRpcRequest deserialization
    // =========================================================================

    #[test]
    fn deserialize_request_with_object_params() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "listOutputs",
            "params": {"basket": "default"},
            "id": 1
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.method, "listOutputs");
        assert_eq!(req.params, json!({"basket": "default"}));
        assert_eq!(req.id, json!(1));
    }

    #[test]
    fn deserialize_request_with_array_params() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "findOrInsertUser",
            "params": ["identity_key_abc"],
            "id": "req-1"
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.method, "findOrInsertUser");
        assert_eq!(req.params, json!(["identity_key_abc"]));
        assert_eq!(req.id, json!("req-1"));
    }

    #[test]
    fn deserialize_request_without_params_defaults_to_null() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "makeAvailable",
            "id": 42
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.params, Value::Null);
    }

    #[test]
    fn deserialize_request_with_null_params() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "makeAvailable",
            "params": null,
            "id": 1
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.params, Value::Null);
    }

    #[test]
    fn deserialize_request_with_null_id() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "test",
            "id": null
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.id, Value::Null);
    }

    #[test]
    fn deserialize_request_with_string_id() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "test",
            "id": "uuid-abc-123"
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.id, json!("uuid-abc-123"));
    }

    // =========================================================================
    // JsonRpcResponse serialization
    // =========================================================================

    #[test]
    fn serialize_success_response() {
        let resp = JsonRpcResponse::success(json!(1), json!({"status": "ok"}));
        let val = serde_json::to_value(&resp).unwrap();
        assert_eq!(val["jsonrpc"], "2.0");
        assert_eq!(val["result"]["status"], "ok");
        assert_eq!(val["id"], 1);
        // Success responses must not have an "error" field.
        assert!(val.get("error").is_none());
    }

    #[test]
    fn serialize_success_response_with_null_result() {
        let resp = JsonRpcResponse::success(json!(2), Value::Null);
        let val = serde_json::to_value(&resp).unwrap();
        assert_eq!(val["result"], Value::Null);
        assert_eq!(val["id"], 2);
    }

    #[test]
    fn serialize_success_response_with_string_id() {
        let resp = JsonRpcResponse::success(json!("req-99"), json!(true));
        let val = serde_json::to_value(&resp).unwrap();
        assert_eq!(val["id"], "req-99");
    }

    // =========================================================================
    // JsonRpcError serialization
    // =========================================================================

    #[test]
    fn serialize_error_response() {
        let err = JsonRpcError::new(json!(5), -32602, "Invalid params");
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["jsonrpc"], "2.0");
        assert_eq!(val["error"]["code"], -32602);
        assert_eq!(val["error"]["message"], "Invalid params");
        assert_eq!(val["id"], 5);
        // data should be absent when None.
        assert!(val["error"].get("data").is_none());
    }

    #[test]
    fn method_not_found_error() {
        let err = JsonRpcError::method_not_found(json!(10), "unknownMethod");
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["error"]["code"], METHOD_NOT_FOUND);
        assert_eq!(val["error"]["message"], "Method not found: unknownMethod");
        assert_eq!(val["id"], 10);
    }

    #[test]
    fn invalid_params_error() {
        let err = JsonRpcError::invalid_params(json!(11), "missing basket field");
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["error"]["code"], INVALID_PARAMS);
        assert_eq!(val["error"]["message"], "missing basket field");
    }

    #[test]
    fn internal_error_construction() {
        let err = JsonRpcError::internal_error(json!(12), "database timeout");
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["error"]["code"], INTERNAL_ERROR);
        assert_eq!(val["error"]["message"], "database timeout");
    }

    #[test]
    fn parse_error_has_null_id() {
        let err = JsonRpcError::parse_error();
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["error"]["code"], PARSE_ERROR);
        assert_eq!(val["error"]["message"], "Parse error");
        assert_eq!(val["id"], Value::Null);
    }

    // =========================================================================
    // Error code constants
    // =========================================================================

    #[test]
    fn error_code_constants() {
        assert_eq!(PARSE_ERROR, -32700);
        assert_eq!(INVALID_REQUEST, -32600);
        assert_eq!(METHOD_NOT_FOUND, -32601);
        assert_eq!(INVALID_PARAMS, -32602);
        assert_eq!(INTERNAL_ERROR, -32603);
    }

    // =========================================================================
    // JsonRpcErrorBody data field skipped when None
    // =========================================================================

    #[test]
    fn error_body_data_skipped_when_none() {
        let body = JsonRpcErrorBody {
            code: -32602,
            message: "test".to_string(),
            data: None,
        };
        let val = serde_json::to_value(&body).unwrap();
        assert!(val.get("data").is_none());
    }

    #[test]
    fn error_body_data_present_when_some() {
        let body = JsonRpcErrorBody {
            code: -32603,
            message: "test".to_string(),
            data: Some(json!({"detail": "extra info"})),
        };
        let val = serde_json::to_value(&body).unwrap();
        assert_eq!(val["data"]["detail"], "extra info");
    }
}
