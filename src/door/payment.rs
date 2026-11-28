//! A payment's bytes at the door: at most [`MAX_PAYMENT_BYTES`] of them are read.
//!
//! A door that takes a payment's bytes from a caller reads at most one
//! mebibyte of them (the BEEF or the transaction as bytes, whatever encoding
//! carried it) and refuses a larger one before any of it is parsed or
//! verified and before anything is written. A Worker's isolate is
//! memory-limited and a door bounds what it reads. The doors:
//!
//! - `internalizeAction` inline: the AtomicBEEF in `tx` (a JSON array of
//!   bytes or a hex string), bounded as the request is decoded
//!   ([`parse_request`]) and again on its bytes ([`admit`]);
//! - `processAction`: the signed transaction in `rawTx`, the same way;
//! - `internalizeAction` at rest: the length the reference names, which the
//!   object's head must match, before the object is opened.
//!
//! The refusal is the JSON-RPC error of a refused argument (`-32602`) with
//! [`description`] as its message and [`data`] beside it, and it is counted
//! by [`log_line`]. The bound is this host's: the reference's servers run
//! under a process with no isolate ceiling and name no such figure; this host
//! differs on purpose.
//!
//! The figure is provisional: it is read again at this crate's next bsv-rs pin and raised to the door's natural figure then.

use std::cell::Cell;
use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::Error;
use crate::json_rpc::JsonRpcRequest;

/// The most bytes of a payment a door reads, one mebibyte.
///
/// The figure is provisional: it is read again at this crate's next bsv-rs pin and raised to the door's natural figure then.
pub const MAX_PAYMENT_BYTES: u64 = 1_048_576;

/// The typed code of the refusal.
pub const CODE: &str = "ERR_PAYMENT_SIZE";

const TAIL: &str = "a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block.";

/// The refusal's sentence: with the payment's size where the door knows it.
pub fn description(size: Option<u64>) -> String {
    match size {
        Some(size) => format!(
            "Not judged: this payment is {size} bytes, and this host reads at most {MAX_PAYMENT_BYTES} bytes of a payment: {TAIL}"
        ),
        None => format!(
            "Not judged: this payment is more than {MAX_PAYMENT_BYTES} bytes, the most this host reads of a payment: {TAIL}"
        ),
    }
}

/// The refusal's `data`: the code, the budget, and the size where known.
pub fn data(size: Option<u64>) -> Value {
    let mut data = serde_json::json!({ "code": CODE, "budget": MAX_PAYMENT_BYTES });
    if let Some(size) = size {
        data["size"] = Value::from(size);
    }
    data
}

/// A payment of `size` bytes is admitted when it is at most the budget.
pub fn admit(size: u64) -> Result<(), Error> {
    if size > MAX_PAYMENT_BYTES {
        return Err(Error::PaymentSize { size: Some(size) });
    }
    Ok(())
}

/// The line that counts a refusal, beside the payer's public identity key.
pub fn log_line(size: Option<u64>, identity_key: &str) -> String {
    let size = size.map_or_else(|| "none".to_string(), |s| s.to_string());
    format!(
        "payment.not_judged code={CODE} resource=Size size={size} budget={MAX_PAYMENT_BYTES} identity_key={identity_key}"
    )
}

/// Count a refusal in the Worker's log.
pub fn log(size: Option<u64>, identity_key: &str) {
    #[cfg(target_arch = "wasm32")]
    worker::console_log!("{}", log_line(size, identity_key));
    #[cfg(not(target_arch = "wasm32"))]
    let _ = (size, identity_key);
}

/// The argument that carries a method's payment, where the method takes one.
pub fn payment_field(method: &str) -> Option<&'static str> {
    match method {
        "internalizeAction" => Some("tx"),
        "processAction" => Some("rawTx"),
        _ => None,
    }
}

/// A request body read by [`parse_request`].
#[derive(Debug)]
pub enum Parsed {
    /// The request, its payment within the budget.
    Request(JsonRpcRequest),
    /// The request's payment is over the budget: decoded no further than it.
    Refused { id: Value, size: Option<u64> },
    /// Not a JSON-RPC request.
    Malformed,
}

/// The request's own fields, its parameters passed over unread.
#[derive(Deserialize)]
struct Head {
    jsonrpc: String,
    method: String,
    #[serde(default)]
    #[allow(dead_code)]
    params: IgnoredAny,
    id: Value,
}

/// Read a JSON-RPC body. For a method that takes a payment, its argument is
/// decoded under the budget: an array of more than [`MAX_PAYMENT_BYTES`]
/// items is refused at the first item past it, and a hex string of more than
/// that many bytes is refused on its length, before it is decoded.
pub fn parse_request(body: &[u8]) -> Parsed {
    let Ok(head) = serde_json::from_slice::<Head>(body) else {
        return Parsed::Malformed;
    };
    let Some(field) = payment_field(&head.method) else {
        return match serde_json::from_slice::<JsonRpcRequest>(body) {
            Ok(request) => Parsed::Request(request),
            Err(_) => Parsed::Malformed,
        };
    };
    let over = Cell::new(None);
    let mut de = serde_json::Deserializer::from_slice(body);
    let params = RequestSeed { field, over: &over }
        .deserialize(&mut de)
        .and_then(|params| de.end().map(|()| params));
    match (params, over.get()) {
        (_, Some(size)) => Parsed::Refused { id: head.id, size },
        (Ok(params), None) => Parsed::Request(JsonRpcRequest {
            jsonrpc: head.jsonrpc,
            method: head.method,
            params,
            id: head.id,
        }),
        (Err(_), None) => Parsed::Malformed,
    }
}

/// Set when the payment is over the budget: its size where it is known.
type Over<'a> = &'a Cell<Option<Option<u64>>>;

macro_rules! scalars {
    () => {
        fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
            Ok(Value::Bool(v))
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
            Ok(Value::from(v))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
            Ok(Value::from(v))
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
            Ok(Value::from(v))
        }
        fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
            Ok(Value::Null)
        }
        fn visit_none<E: de::Error>(self) -> Result<Value, E> {
            Ok(Value::Null)
        }
    };
}

macro_rules! strings {
    () => {
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
            Ok(Value::String(v.to_owned()))
        }
        fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
            Ok(Value::String(v))
        }
    };
}

/// The request object: its `params` read by [`ParamsSeed`], the rest passed over.
struct RequestSeed<'a> {
    field: &'static str,
    over: Over<'a>,
}

impl<'de> DeserializeSeed<'de> for RequestSeed<'_> {
    type Value = Value;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for RequestSeed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON-RPC request")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut params = Value::Null;
        while let Some(key) = map.next_key::<String>()? {
            if key == "params" {
                params = map.next_value_seed(ParamsSeed {
                    field: self.field,
                    over: self.over,
                    top: true,
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(params)
    }
}

/// The parameters (`top`), named or positional, or one positional argument:
/// in an object, the payment's field is read by [`PaymentSeed`].
#[derive(Clone, Copy)]
struct ParamsSeed<'a> {
    field: &'static str,
    over: Over<'a>,
    top: bool,
}

impl<'de> DeserializeSeed<'de> for ParamsSeed<'_> {
    type Value = Value;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ParamsSeed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON-RPC parameters")
    }
    scalars!();
    strings!();
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        if self.top {
            let element = ParamsSeed { top: false, ..self };
            while let Some(item) = seq.next_element_seed(element)? {
                items.push(item);
            }
        } else {
            while let Some(item) = seq.next_element::<Value>()? {
                items.push(item);
            }
        }
        Ok(Value::Array(items))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut fields = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let value = if key == self.field {
                map.next_value_seed(PaymentSeed { over: self.over })?
            } else {
                map.next_value::<Value>()?
            };
            fields.insert(key, value);
        }
        Ok(Value::Object(fields))
    }
}

/// The payment's argument, read no further than the budget.
struct PaymentSeed<'a> {
    over: Over<'a>,
}

impl PaymentSeed<'_> {
    fn refuse<E: de::Error>(&self, size: Option<u64>) -> E {
        self.over.set(Some(size));
        E::custom(CODE)
    }

    /// A hex string carries half its length in bytes.
    fn hex<E: de::Error>(&self, v: &str) -> Result<(), E> {
        let size = (v.len() / 2) as u64;
        if size > MAX_PAYMENT_BYTES {
            return Err(self.refuse(Some(size)));
        }
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for PaymentSeed<'_> {
    type Value = Value;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for PaymentSeed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a payment's bytes")
    }
    scalars!();
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        self.hex(v)?;
        Ok(Value::String(v.to_owned()))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        self.hex(&v)?;
        Ok(Value::String(v))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element::<Value>()? {
            if items.len() as u64 == MAX_PAYMENT_BYTES {
                return Err(self.refuse(None));
            }
            items.push(item);
        }
        Ok(Value::Array(items))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut fields = Map::new();
        while let Some((key, value)) = map.next_entry::<String, Value>()? {
            fields.insert(key, value);
        }
        Ok(Value::Object(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const CAP: usize = MAX_PAYMENT_BYTES as usize;

    fn body(method: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(
            &json!({ "jsonrpc": "2.0", "method": method, "params": params, "id": 7 }),
        )
        .unwrap()
    }

    fn request(parsed: Parsed) -> JsonRpcRequest {
        match parsed {
            Parsed::Request(r) => r,
            other => panic!("not a request: {other:?}"),
        }
    }

    #[test]
    fn the_figure_is_one_mebibyte() {
        assert_eq!(MAX_PAYMENT_BYTES, 1024 * 1024);
        assert!(admit(MAX_PAYMENT_BYTES).is_ok());
        match admit(MAX_PAYMENT_BYTES + 1) {
            Err(Error::PaymentSize { size }) => assert_eq!(size, Some(MAX_PAYMENT_BYTES + 1)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_sentences_are_pinned() {
        assert_eq!(
            description(Some(1_048_577)),
            "Not judged: this payment is 1048577 bytes, and this host reads at most 1048576 bytes of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block."
        );
        assert_eq!(
            description(None),
            "Not judged: this payment is more than 1048576 bytes, the most this host reads of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block."
        );
        assert_eq!(
            data(Some(9)),
            json!({ "code": "ERR_PAYMENT_SIZE", "budget": 1_048_576, "size": 9 })
        );
        assert_eq!(
            data(None),
            json!({ "code": "ERR_PAYMENT_SIZE", "budget": 1_048_576 })
        );
    }

    #[test]
    fn the_log_line_is_pinned() {
        assert_eq!(
            log_line(Some(1_048_577), "02ab"),
            "payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=1048577 budget=1048576 identity_key=02ab"
        );
        assert_eq!(
            log_line(None, "02ab"),
            "payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=none budget=1048576 identity_key=02ab"
        );
    }

    #[test]
    fn a_payment_at_the_budget_is_read_as_before() {
        let at = vec![7u8; CAP];
        for (method, field) in [("internalizeAction", "tx"), ("processAction", "rawTx")] {
            for params in [
                json!([{ "identityKey": "02ab" }, { field: at, "description": "d" }]),
                json!({ field: at, "description": "d" }),
                json!([{ field: hex::encode(&at) }]),
            ] {
                let b = body(method, params);
                let ours = request(parse_request(&b));
                let theirs: JsonRpcRequest = serde_json::from_slice(&b).unwrap();
                assert_eq!(ours.params, theirs.params, "{method}");
                assert_eq!((ours.method, ours.id), (theirs.method, theirs.id));
            }
        }
    }

    #[test]
    fn one_byte_over_is_refused_where_it_is_carried() {
        let over = vec![7u8; CAP + 1];
        for (method, field) in [("internalizeAction", "tx"), ("processAction", "rawTx")] {
            let cases = [
                (json!([{ "identityKey": "02ab" }, { field: over }]), None),
                (json!({ field: over }), None),
                (
                    json!([{ "identityKey": "02ab" }, { field: hex::encode(&over) }]),
                    Some(MAX_PAYMENT_BYTES + 1),
                ),
            ];
            for (params, size) in cases {
                match parse_request(&body(method, params)) {
                    Parsed::Refused { id, size: got } => {
                        assert_eq!(id, json!(7));
                        assert_eq!(got, size, "{method}");
                    }
                    other => panic!("{method}: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn another_method_and_another_argument_keep_their_own_limits() {
        let over = vec![7u8; CAP + 1];
        for b in [
            body("listOutputs", json!([{ "tx": over }])),
            body("internalizeAction", json!([{ "outputs": over }])),
        ] {
            let ours = request(parse_request(&b));
            let theirs: JsonRpcRequest = serde_json::from_slice(&b).unwrap();
            assert_eq!(ours.params, theirs.params);
        }
    }

    #[test]
    fn a_malformed_body_is_malformed() {
        assert!(matches!(parse_request(b"{"), Parsed::Malformed));
        assert!(matches!(
            parse_request(br#"{"jsonrpc":"2.0","method":"processAction","id":1,"params":[1,]}"#),
            Parsed::Malformed
        ));
        let no_params = request(parse_request(
            br#"{"jsonrpc":"2.0","method":"processAction","id":1}"#,
        ));
        assert_eq!(no_params.params, Value::Null);
    }
}
