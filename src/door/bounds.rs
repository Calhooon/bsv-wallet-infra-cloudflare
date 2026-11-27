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
pub const MAX_LIST_LIMIT: u32 = 1_000;

/// The list limit when the caller names none, for the methods whose default
/// was unbounded (`StorageServer.ts:341-347`); `findProvenTxReqs` is one.
pub const DEFAULT_LIST_LIMIT: u32 = 1_000;

/// The largest list offset (`StorageServer.ts:355-361`).
pub const MAX_LIST_OFFSET: u64 = 1_000_000;

/// The most items in any one array of the parameters (`StorageServer.ts:362-368`).
pub const MAX_ARRAY_ITEMS: usize = 1_000_000;

/// The most array items the whole parameters may hold together, counted across
/// every array in the request. `MAX_ARRAY_ITEMS` caps one array; this caps
/// their sum, so a body that spreads its items over several arrays is held to
/// the same parse-heap envelope as the largest single array the door admits.
/// It sits comfortably above the largest body a caller sends (one BEEF as a
/// byte array is at most `MAX_ARRAY_ITEMS`, plus the small lists around it).
pub const MAX_TOTAL_ARRAY_ITEMS: usize = 1_500_000;

/// The deepest nesting of the parameters (`StorageServer.ts:803`).
pub const MAX_NESTING: usize = 64;

/// The deepest nesting the raw-body pre-scan tracks. Well above `MAX_NESTING`
/// (the parameter nesting limit `check_params_shape` applies to the parsed
/// value), so the pre-scan never refuses a body that the parsed-value check
/// would accept; its only purpose is to keep the pre-scan's own stack bounded.
const PRESCAN_MAX_DEPTH: usize = 256;

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

/// A structural pass over the raw JSON body, before it is parsed into a
/// `Value`, that holds the request's arrays to `MAX_TOTAL_ARRAY_ITEMS`. A body
/// over the bound (or nested deeper than the pre-scan tracks) is refused here
/// rather than built into a `Value` first; a body within it parses exactly as
/// before, and the parsed value still meets `check_params_shape`'s per-array
/// and nesting limits. Malformed JSON is left for the parser to reject.
pub fn prescan_body(body: &[u8]) -> Result<(), Refusal> {
    match scan_array_items(body) {
        Some(total) if total <= MAX_TOTAL_ARRAY_ITEMS => Ok(()),
        _ => Err(Refusal::plain(
            413,
            "ERR_PARAMS_TOO_LARGE",
            "The request parameters exceed the endpoint limit.",
        )),
    }
}

/// The total number of array items across every array in `body`, read straight
/// from the bytes without building a value: for each array, the count of its
/// items (nested arrays add their own items too). Strings and their escapes are
/// skipped, so brackets and commas inside a string are not miscounted. Returns
/// `None` only when the nesting is deeper than `PRESCAN_MAX_DEPTH` (so the
/// stack stays bounded). Allocates only that depth stack.
fn scan_array_items(body: &[u8]) -> Option<usize> {
    // Each open container: an array carries whether it holds any item yet and
    // how many item separators it has seen; an object carries nothing (its
    // values that are arrays are counted when they close).
    enum Container {
        Array { has_item: bool, commas: usize },
        Object,
    }
    let mut stack: Vec<Container> = Vec::new();
    let mut total_items: usize = 0;
    let mut in_string = false;
    let mut escaped = false;

    // Mark the innermost array, if any, as holding an item.
    fn mark_item(stack: &mut [Container]) {
        if let Some(Container::Array { has_item, .. }) = stack.last_mut() {
            *has_item = true;
        }
    }

    for &byte in body {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b' ' | b'\t' | b'\n' | b'\r' | b':' => {}
            b'"' => {
                mark_item(&mut stack);
                in_string = true;
            }
            b'[' => {
                mark_item(&mut stack);
                if stack.len() >= PRESCAN_MAX_DEPTH {
                    return None;
                }
                stack.push(Container::Array {
                    has_item: false,
                    commas: 0,
                });
            }
            b'{' => {
                mark_item(&mut stack);
                if stack.len() >= PRESCAN_MAX_DEPTH {
                    return None;
                }
                stack.push(Container::Object);
            }
            b']' => {
                if let Some(Container::Array { has_item, commas }) = stack.last() {
                    total_items += if *has_item { commas + 1 } else { 0 };
                    stack.pop();
                }
                // A `]` with no open array is malformed; leave it for the parser.
            }
            b'}' => {
                if matches!(stack.last(), Some(Container::Object)) {
                    stack.pop();
                }
            }
            b',' => {
                if let Some(Container::Array { commas, .. }) = stack.last_mut() {
                    *commas += 1;
                }
            }
            _ => mark_item(&mut stack),
        }
    }
    Some(total_items)
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
        Some(n) if n >= 1 && n <= u64::from(MAX_LIST_LIMIT) => Ok(()),
        Some(n) if n > u64::from(MAX_LIST_LIMIT) => {
            Err(format!("RPC list limits must not exceed {MAX_LIST_LIMIT}"))
        }
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

    /// Sum of the lengths of every array in a parsed value — the quantity the
    /// raw pre-scan counts without building the value.
    fn total_array_items(v: &Value) -> usize {
        match v {
            Value::Array(items) => items.len() + items.iter().map(total_array_items).sum::<usize>(),
            Value::Object(fields) => fields.values().map(total_array_items).sum(),
            _ => 0,
        }
    }

    #[test]
    fn prescan_counts_match_the_parsed_value() {
        // Including strings that carry [ ] , and escapes, which must not be
        // miscounted, and nested arrays/objects.
        let cases: [&[u8]; 7] = [
            br#"{}"#,
            br#"[]"#,
            br#"[1,2,3]"#,
            br#"{"a":[1,[2,3],{"b":[4,5,6]}],"c":"]["}"#,
            br#"["a,b","c]d","e\"f","g\\h"]"#,
            br#"{"tx":[0,0,0,0,0],"labels":["x","y"],"nested":[[1],[2],[3]]}"#,
            br#"{"jsonrpc":"2.0","method":"listOutputs","params":[{"tags":["a","b"]}],"id":1}"#,
        ];
        for raw in cases {
            let v: Value = serde_json::from_slice(raw).unwrap();
            assert_eq!(
                scan_array_items(raw),
                Some(total_array_items(&v)),
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[test]
    fn prescan_passes_an_ordinary_body_and_a_full_single_array() {
        let ordinary =
            br#"{"jsonrpc":"2.0","method":"internalizeAction","params":[{"outputs":[]}],"id":1}"#;
        assert!(prescan_body(ordinary).is_ok());
        // One array at the single-array limit (a ~1 MB BEEF as a byte array) is
        // under the cumulative bound, so it is admitted as before.
        let mut big = br#"{"params":["#.to_vec();
        for i in 0..MAX_ARRAY_ITEMS {
            if i > 0 {
                big.push(b',');
            }
            big.push(b'0');
        }
        big.extend_from_slice(br#"]}"#);
        assert_eq!(scan_array_items(&big), Some(MAX_ARRAY_ITEMS));
        assert!(prescan_body(&big).is_ok());
    }

    #[test]
    fn prescan_refuses_a_body_over_the_cumulative_bound() {
        // One oversize array.
        let mut one = Vec::from(&b"["[..]);
        for i in 0..=MAX_TOTAL_ARRAY_ITEMS {
            if i > 0 {
                one.push(b',');
            }
            one.push(b'0');
        }
        one.push(b']');
        let r = prescan_body(&one).unwrap_err();
        assert_eq!(r.status, 413);
        assert_eq!(r.body["code"], "ERR_PARAMS_TOO_LARGE");
        // Several arrays each within the single-array limit but over the sum.
        let arr = {
            let mut a = Vec::from(&b"["[..]);
            for i in 0..MAX_ARRAY_ITEMS {
                if i > 0 {
                    a.push(b',');
                }
                a.push(b'0');
            }
            a.push(b']');
            a
        };
        let mut many = Vec::from(&b"["[..]);
        many.extend_from_slice(&arr);
        many.push(b',');
        many.extend_from_slice(&arr); // 2 * MAX_ARRAY_ITEMS > MAX_TOTAL_ARRAY_ITEMS
        many.push(b']');
        assert!(prescan_body(&many).is_err());
    }

    #[test]
    fn prescan_accepts_the_parameter_nesting_limit_and_bounds_its_own_stack() {
        // A value nested as deep as check_params_shape accepts still passes the
        // pre-scan (its depth cap is well above the parameter nesting limit).
        let mut deep = vec![b'['; MAX_NESTING];
        deep.push(b'0');
        deep.resize(deep.len() + MAX_NESTING, b']');
        assert!(prescan_body(&deep).is_ok());
        // Deeper than the pre-scan tracks is refused (the parsed-value check
        // would refuse it too); the stack never grows past the cap.
        let deeper = vec![b'['; PRESCAN_MAX_DEPTH + 1];
        assert!(prescan_body(&deeper).is_err());
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
