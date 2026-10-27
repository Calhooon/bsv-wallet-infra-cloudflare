//! The checks on a JSON-RPC call before it is dispatched: the request budgets
//! (`StorageServer.ts:752-855`) on every call, and `findOrInsertUser` bound to
//! the session's identity (`:1046-1050`).

use serde_json::Value;

use super::bounds::{check_limit, check_offset, check_params_shape};
use super::Refusal;
use crate::json_rpc::{JsonRpcError, INVALID_PARAMS};

/// The methods whose arguments carry a list limit and offset, at the top
/// level (`limit`, `offset`) or paged (`paged.limit`, `paged.offset`)
/// (`StorageServer.ts:175-187`).
pub const LIST_METHODS: [&str; 4] = [
    "listOutputs",
    "listActions",
    "listCertificates",
    "findProvenTxReqs",
];

/// The JSON-RPC code of a call refused for the caller's identity, answered 401.
pub const UNAUTHORIZED: i32 = -32003;

/// The reference's text for `findOrInsertUser` with another key (`StorageServer.ts:1048`).
pub const ANOTHER_USER: &str = "function may only access authenticated user.";

/// A refusal of the call `id` as a JSON-RPC error, answered 200 as the
/// dispatch answers its own refusals.
fn invalid(id: &Value, message: String) -> Refusal {
    Refusal {
        status: 200,
        body: serde_json::to_value(JsonRpcError::new(id.clone(), INVALID_PARAMS, message))
            .unwrap_or(Value::Null),
    }
}

/// The budgets of one call: the array items and nesting of the whole
/// parameters, and the list limit and offset of a list method.
pub fn check_budgets(method: &str, params: &Value, id: &Value) -> Result<(), Refusal> {
    check_params_shape(params).map_err(|m| invalid(id, m))?;
    if !LIST_METHODS.contains(&method) {
        return Ok(());
    }
    let args = crate::dispatch::extract_args(params, true);
    let paged = match args.get("paged") {
        None | Some(Value::Null) => None,
        Some(p @ Value::Object(_)) => Some(p),
        Some(_) => return Err(invalid(id, "paged must be an object".to_string())),
    };
    for scope in std::iter::once(&args).chain(paged) {
        check_limit(scope.get("limit")).map_err(|m| invalid(id, m))?;
        check_offset(scope.get("offset")).map_err(|m| invalid(id, m))?;
    }
    Ok(())
}

/// Every check of one call from the session of `identity_key`.
pub fn check_request(
    method: &str,
    params: &Value,
    identity_key: &str,
    id: &Value,
) -> Result<(), Refusal> {
    check_budgets(method, params, id)?;
    check_user_bound(method, params, identity_key, id)
}

/// `findOrInsertUser` names the session's own identity key or is refused 401
/// with `WERR_UNAUTHORIZED` (`StorageServer.ts:1046-1050`). The key is read as
/// the dispatch reads it (`["key"]` or `{identityKey}`); a call naming none is
/// left to the dispatch's own refusal.
pub fn check_user_bound(
    method: &str,
    params: &Value,
    identity_key: &str,
    id: &Value,
) -> Result<(), Refusal> {
    if method != "findOrInsertUser" {
        return Ok(());
    }
    let args = crate::dispatch::extract_args(params, false);
    let named = args
        .as_str()
        .or_else(|| args.get("identityKey").and_then(Value::as_str));
    match named {
        Some(key) if key != identity_key => {
            let mut body =
                serde_json::to_value(JsonRpcError::new(id.clone(), UNAUTHORIZED, ANOTHER_USER))
                    .unwrap_or(Value::Null);
            body["error"]["data"] = serde_json::json!({ "name": "WERR_UNAUTHORIZED" });
            Err(Refusal { status: 401, body })
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message(r: Refusal) -> String {
        assert_eq!(r.status, 200);
        assert_eq!(r.body["error"]["code"], INVALID_PARAMS);
        r.body["error"]["message"]
            .as_str()
            .unwrap_or("")
            .to_string()
    }

    #[test]
    fn a_limit_of_1001_is_refused_on_every_list_method() {
        for method in LIST_METHODS {
            let top = json!([{}, {"limit": 1001}]);
            assert!(message(check_budgets(method, &top, &json!(1)).unwrap_err())
                .contains("must not exceed 1000"));
            let paged = json!([{}, {"paged": {"limit": 1001}}]);
            assert!(
                message(check_budgets(method, &paged, &json!(1)).unwrap_err())
                    .contains("must not exceed 1000")
            );
            let named = json!({"paged": {"limit": 10, "offset": 2_000_000}});
            assert!(
                message(check_budgets(method, &named, &json!(1)).unwrap_err())
                    .contains("offsets must not exceed")
            );
            assert!(check_budgets(method, &json!([{}, {"limit": 1000}]), &json!(1)).is_ok());
        }
    }

    #[test]
    fn find_or_insert_user_only_for_the_sessions_key() {
        let me = "02aa";
        for params in [json!(["02bb"]), json!({"identityKey": "02bb"})] {
            let r = check_request("findOrInsertUser", &params, me, &json!(3)).unwrap_err();
            assert_eq!(r.status, 401);
            assert_eq!(r.body["error"]["code"], UNAUTHORIZED);
            assert_eq!(r.body["error"]["message"], ANOTHER_USER);
            assert_eq!(r.body["error"]["data"]["name"], "WERR_UNAUTHORIZED");
            assert_eq!(r.body["id"], 3);
        }
        assert!(check_request("findOrInsertUser", &json!([me]), me, &json!(3)).is_ok());
        assert!(check_request(
            "findOrInsertUser",
            &json!({"identityKey": me}),
            me,
            &json!(3)
        )
        .is_ok());
        assert!(check_request("findOrInsertUser", &json!([]), me, &json!(3)).is_ok());
        assert!(check_request("listOutputs", &json!(["02bb"]), me, &json!(3)).is_ok());
    }

    #[test]
    fn paged_must_be_an_object() {
        let r = check_budgets("findProvenTxReqs", &json!([{"paged": 5}]), &json!(2));
        assert_eq!(message(r.unwrap_err()), "paged must be an object");
    }

    #[test]
    fn other_methods_carry_only_the_shape_budget() {
        assert!(check_budgets("createAction", &json!([{}, {"limit": 5000}]), &json!(1)).is_ok());
        let mut deep = json!(0);
        for _ in 0..70 {
            deep = json!([deep]);
        }
        assert!(
            message(check_budgets("createAction", &deep, &json!(1)).unwrap_err())
                .contains("nesting")
        );
    }
}
