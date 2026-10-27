//! The checks on a JSON-RPC call before it is dispatched: the request budgets
//! (`StorageServer.ts:752-855`) on every call.

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
