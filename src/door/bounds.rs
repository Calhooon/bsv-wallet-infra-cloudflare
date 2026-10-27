//! The bounds with the reference's `standard` numbers
//! (`StorageServer.ts:341-398, 496-515`): the request body, the response,
//! the list limits and offsets, the array items and the nesting of the
//! parameters.

use serde_json::Value;

use super::Refusal;

/// The JSON body bound, 8 MiB (`StorageServer.ts:391-398, 496-507`).
pub const MAX_JSON_BODY_BYTES: u64 = 8 * 1024 * 1024;

/// The binary (`application/octet-stream`) body bound, 8 MiB (`StorageServer.ts:510-515`).
pub const MAX_BINARY_BODY_BYTES: u64 = 8 * 1024 * 1024;

/// The response bound, 8 MiB (`StorageServer.ts:369-379`).
pub const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;

/// The largest list limit a caller may ask for (`StorageServer.ts:348-354`).
pub const MAX_LIST_LIMIT: u64 = 1_000;

/// The list limit when the caller names none, for the methods whose default
/// was unbounded (`StorageServer.ts:341-347`); `findProvenTxReqs` is one.
pub const DEFAULT_LIST_LIMIT: u32 = 1_000;

/// The largest list offset (`StorageServer.ts:355-361`).
pub const MAX_LIST_OFFSET: u64 = 1_000_000;

/// The most items in any one array of the parameters (`StorageServer.ts:362-368`).
pub const MAX_ARRAY_ITEMS: usize = 1_000_000;

/// The deepest nesting of the parameters (`StorageServer.ts:803`).
pub const MAX_NESTING: usize = 64;

/// The body bound for a request's content type: binary for
/// `application/octet-stream`, JSON for everything else.
pub fn body_limit(content_type: Option<&str>) -> u64 {
    match content_type {
        Some(t) if t.trim().starts_with("application/octet-stream") => MAX_BINARY_BODY_BYTES,
        _ => MAX_JSON_BODY_BYTES,
    }
}

/// The reference's `413 ERR_BODY_TOO_LARGE` when `length` is over the bound
/// for `content_type` (`edgePolicy.ts:624-633`).
pub fn check_body(length: u64, content_type: Option<&str>) -> Result<(), Refusal> {
    if length > body_limit(content_type) {
        return Err(Refusal::plain(
            413,
            "ERR_BODY_TOO_LARGE",
            "The request body exceeds the endpoint limit.",
        ));
    }
    Ok(())
}

/// The length of `value` as JSON, counted without building the bytes.
pub fn json_len(value: &Value) -> u64 {
    struct Count(u64);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len() as u64;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    // Writing a `Value` to a counter cannot fail: no I/O error, no non-string key.
    let _ = serde_json::to_writer(&mut count, value);
    count.0
}

/// The answer as it goes out: `result` within the bound, or the reference's
/// `413 -32005` naming the request's `id` (`StorageServer.ts:666-668, 737-746`).
pub fn bound_response(result: Value, id: &Value) -> (u16, Value) {
    if json_len(&result) > MAX_RESPONSE_BYTES {
        return (
            413,
            serde_json::json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32005,
                    "message": "The requested response exceeds the configured service limit."
                },
                "id": id,
            }),
        );
    }
    (200, result)
}

/// The array and nesting budget over the whole parameters
/// (`StorageServer.ts:796-816`), walked without recursion.
pub fn check_params_shape(params: &Value) -> Result<(), String> {
    let mut pending: Vec<(&Value, usize)> = vec![(params, 0)];
    while let Some((value, depth)) = pending.pop() {
        if depth > MAX_NESTING {
            return Err(format!(
                "RPC parameter nesting exceeds {MAX_NESTING} levels"
            ));
        }
        match value {
            Value::Array(items) => {
                if items.len() > MAX_ARRAY_ITEMS {
                    return Err(format!(
                        "RPC arrays must not exceed {MAX_ARRAY_ITEMS} items"
                    ));
                }
                pending.extend(items.iter().map(|v| (v, depth + 1)));
            }
            Value::Object(fields) => pending.extend(fields.values().map(|v| (v, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

/// A list limit, when present, is a positive integer at most 1,000
/// (`StorageServer.ts:831-843`).
pub fn check_limit(value: Option<&Value>) -> Result<(), String> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(());
    };
    match value.as_u64() {
        Some(n) if n >= 1 && n <= MAX_LIST_LIMIT => Ok(()),
        Some(n) if n > MAX_LIST_LIMIT => Err(format!(
            "RPC list limits must not exceed {MAX_LIST_LIMIT}"
        )),
        _ => Err("RPC list limits must be positive safe integers".to_string()),
    }
}

/// A list offset, when present, is an integer of magnitude at most 1,000,000
/// (`StorageServer.ts:845-855`). A negative offset is kept: `listOutputs` and
/// `listActions` read it here as counted from the end (`src/storage/readers.rs:294-300`).
pub fn check_offset(value: Option<&Value>) -> Result<(), String> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(());
    };
    match value.as_i64() {
        Some(n) if n.unsigned_abs() <= MAX_LIST_OFFSET => Ok(()),
        Some(_) => Err(format!(
            "RPC list offsets must not exceed {MAX_LIST_OFFSET}"
        )),
        None => Err("RPC list offsets must be safe integers".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_body_over_eight_mib_is_413() {
        assert!(check_body(MAX_JSON_BODY_BYTES, Some("application/json")).is_ok());
        let r = check_body(9 * 1024 * 1024, Some("application/json")).unwrap_err();
        assert_eq!(r.status, 413);
        assert_eq!(r.body["code"], "ERR_BODY_TOO_LARGE");
        assert!(check_body(MAX_BINARY_BODY_BYTES + 1, Some("application/octet-stream")).is_err());
        assert!(check_body(MAX_JSON_BODY_BYTES + 1, None).is_err());
    }

    #[test]
    fn json_len_counts_the_serialized_bytes() {
        let v = json!({"a": [1, 2, "three"], "b": null});
        assert_eq!(json_len(&v), serde_json::to_vec(&v).unwrap().len() as u64);
    }

    #[test]
    fn a_response_over_the_bound_is_413_minus_32005() {
        let small = json!({"jsonrpc": "2.0", "result": 1, "id": 7});
        assert_eq!(bound_response(small.clone(), &json!(7)), (200, small));
        let big = json!({"result": "x".repeat(MAX_RESPONSE_BYTES as usize)});
        let (status, body) = bound_response(big, &json!(7));
        assert_eq!(status, 413);
        assert_eq!(body["error"]["code"], -32005);
        assert_eq!(body["id"], 7);
    }

    #[test]
    fn nesting_over_sixty_four_is_refused() {
        let mut deep = json!(1);
        for _ in 0..MAX_NESTING {
            deep = json!([deep]);
        }
        assert!(check_params_shape(&deep).is_ok());
        let deeper = json!([deep]);
        assert!(check_params_shape(&deeper)
            .unwrap_err()
            .contains("nesting exceeds 64"));
    }

    #[test]
    fn an_array_over_a_million_items_is_refused() {
        let at = Value::Array(vec![Value::Null; MAX_ARRAY_ITEMS]);
        assert!(check_params_shape(&json!([at])).is_ok());
        let over = Value::Array(vec![Value::Null; MAX_ARRAY_ITEMS + 1]);
        assert!(check_params_shape(&json!([{"txids": over}]))
            .unwrap_err()
            .contains("1000000 items"));
    }

    #[test]
    fn limits_and_offsets() {
        assert!(check_limit(None).is_ok());
        assert!(check_limit(Some(&json!(null))).is_ok());
        assert!(check_limit(Some(&json!(1000))).is_ok());
        assert!(check_limit(Some(&json!(1001)))
            .unwrap_err()
            .contains("must not exceed 1000"));
        assert!(check_limit(Some(&json!(0))).is_err());
        assert!(check_limit(Some(&json!(-1))).is_err());
        assert!(check_limit(Some(&json!("10"))).is_err());
        assert!(check_offset(Some(&json!(1_000_000))).is_ok());
        assert!(check_offset(Some(&json!(-5))).is_ok());
        assert!(check_offset(Some(&json!(1_000_001))).is_err());
        assert!(check_offset(Some(&json!(-1_000_001))).is_err());
        assert!(check_offset(Some(&json!(1.5))).is_err());
    }
}
