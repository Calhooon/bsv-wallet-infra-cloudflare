//! A payment's bytes at the door: at most the door's figure of them are read.
//!
//! A door that takes a payment's bytes from a caller reads at most the
//! figure in force of them (the BEEF or the transaction as bytes, whatever
//! encoding carried it) and refuses a larger one before any of it is parsed
//! or verified and before anything is written. The figure is a setting of
//! the Worker, [`SETTING`], read by [`budget_from`]: 614,400 bytes (600 KiB,
//! [`DEFAULT_MAX_PAYMENT_BYTES`]) where it is missing, zero or not a number.
//! A Worker's isolate is memory-limited and a door bounds what it reads. The
//! doors:
//!
//! - `internalizeAction` inline: the AtomicBEEF in `tx` (a JSON array of
//!   bytes or a hex string), bounded as the request is decoded
//!   ([`parse_request`]), whether the arguments are named (`tx`) or
//!   positional (the first), and again on its bytes ([`admit`]);
//! - `processAction`: the signed transaction in `rawTx` (named, or the
//!   seventh positional argument), the same way;
//! - `internalizeAction` at rest: the length the reference names, which the
//!   object's head must match, before the caller's user is looked up
//!   (`dispatch`) and again before the object is opened.
//!
//! The refusal is the JSON-RPC error of a refused argument (`-32602`) with
//! [`description`] as its message and [`data`] beside it, and it is counted
//! by [`log_line`]. The bound is this host's. The reference names no such
//! figure: its storage server holds a request's JSON and binary bodies at
//! 8 MiB and its arrays at 1,000,000 items in the standard profile
//! (bsv-blockchain/ts-stack@57c5f13, `StorageServer.ts:547-558, 561-566,
//! 402-408`); this host differs on purpose.
//!
//! An answer a provider sends that carries a proof, a BEEF, a transaction, a
//! transaction's status or a broadcast's outcome (a merkle proof, a raw
//! transaction, a stored transaction record, a status, the answer to a
//! posted transaction), and the broadcaster's callback, is read no further than
//! [`answer_bound`] ([`read_answer`], [`read_request`]) and is then held to
//! the same figure before it is parsed ([`admit_answer`]): over either, the
//! answer is not read and the provider is taken as unavailable at that
//! moment.
//!
//! The figure is provisional: it is read again at this crate's next bsv-rs pin and raised to the door's natural figure then.

use std::cell::Cell;
use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::Error;
use crate::json_rpc::JsonRpcRequest;

/// The most bytes of a payment a door reads where the setting names no
/// other figure: 614,400 bytes, 600 KiB. An ordinary payment is about a
/// kilobyte; an operator who must take a larger one raises the setting.
pub const DEFAULT_MAX_PAYMENT_BYTES: u64 = 614_400;

/// The Worker's setting (a var in `[vars]`) that names the figure in bytes.
pub const SETTING: &str = "MAX_PAYMENT_BYTES";

/// The figure in force from the setting's value: a positive whole number of
/// bytes, or [`DEFAULT_MAX_PAYMENT_BYTES`] where the value is missing, zero
/// or malformed. Never "no figure".
pub fn budget_from(value: Option<&str>) -> u64 {
    setting_from(value).figure
}

/// The setting as read: the figure in force, and whether a value was given
/// that names no figure (zero or malformed), so the default stands for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting {
    pub figure: u64,
    pub malformed: bool,
}

/// Read the setting's value. A missing value is the default; a value that is
/// not a positive whole number of bytes is the default too, marked malformed.
pub fn setting_from(value: Option<&str>) -> Setting {
    let Some(value) = value else {
        return Setting {
            figure: DEFAULT_MAX_PAYMENT_BYTES,
            malformed: false,
        };
    };
    match value.trim().parse::<u64>() {
        Ok(n) if n > 0 => Setting {
            figure: n,
            malformed: false,
        },
        _ => Setting {
            figure: DEFAULT_MAX_PAYMENT_BYTES,
            malformed: true,
        },
    }
}

/// The line that names a malformed setting and the figure in force.
pub fn setting_log_line(figure: u64) -> String {
    format!(
        "setting.malformed {SETTING} is not a positive whole number of bytes; the figure in force is {figure}"
    )
}

/// The line to log for this reading of the setting: for a malformed value,
/// once (the first reading that finds `said` unset sets it), else none.
pub fn setting_note(setting: Setting, said: &std::sync::atomic::AtomicBool) -> Option<String> {
    use std::sync::atomic::Ordering;
    if setting.malformed && !said.swap(true, Ordering::Relaxed) {
        Some(setting_log_line(setting.figure))
    } else {
        None
    }
}

/// The typed code of the refusal.
pub const CODE: &str = "ERR_PAYMENT_SIZE";

const TAIL: &str = "a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block.";

/// The refusal's sentence under the figure `budget`: with the payment's size
/// where the door knows it.
pub fn description(size: Option<u64>, budget: u64) -> String {
    match size {
        Some(size) => format!(
            "Not judged: this payment is {size} bytes, and this host reads at most {budget} bytes of a payment: {TAIL}"
        ),
        None => format!(
            "Not judged: this payment is more than {budget} bytes, the most this host reads of a payment: {TAIL}"
        ),
    }
}

/// The refusal's `data`: the code, the budget, and the size where known.
pub fn data(size: Option<u64>, budget: u64) -> Value {
    let mut data = serde_json::json!({ "code": CODE, "budget": budget });
    if let Some(size) = size {
        data["size"] = Value::from(size);
    }
    data
}

/// A payment of `size` bytes is admitted when it is at most the budget.
pub fn admit(size: u64, budget: u64) -> Result<(), Error> {
    if size > budget {
        return Err(Error::PaymentSize {
            size: Some(size),
            budget,
        });
    }
    Ok(())
}

/// The line that counts a refusal, beside the payer's public identity key.
pub fn log_line(size: Option<u64>, budget: u64, identity_key: &str) -> String {
    let size = size.map_or_else(|| "none".to_string(), |s| s.to_string());
    format!(
        "payment.not_judged code={CODE} resource=Size size={size} budget={budget} identity_key={identity_key}"
    )
}

/// Count a refusal in the Worker's log.
pub fn log(size: Option<u64>, budget: u64, identity_key: &str) {
    #[cfg(target_arch = "wasm32")]
    worker::console_log!("{}", log_line(size, budget, identity_key));
    #[cfg(not(target_arch = "wasm32"))]
    let _ = (size, budget, identity_key);
}

/// The line that counts a provider's answer not read for its size.
pub fn answer_log_line(provider: &str, what: &str, size: u64, budget: u64) -> String {
    format!(
        "provider.answer_not_read provider={provider} answer={what} size={size} budget={budget}"
    )
}

/// A provider's answer of `size` bytes, about to be parsed, is read when it
/// is at most the budget. Over it the answer is not read: one log line names
/// the provider and the size, and the `Err` is the provider's error, which
/// each caller already takes as that provider unavailable at that moment.
pub fn admit_answer(provider: &str, what: &str, size: u64, budget: u64) -> Result<(), String> {
    if size <= budget {
        return Ok(());
    }
    let line = answer_log_line(provider, what, size, budget);
    #[cfg(target_arch = "wasm32")]
    worker::console_log!("{}", line);
    Err(line)
}

/// The allowance beside a payment's bytes in an answer for its JSON wrapping
/// (the field names, the txid, the status, the block's hash and height):
/// 64 KiB.
pub const ANSWER_WRAPPING: u64 = 64 * 1024;

/// The most bytes of a provider's answer read under the figure `budget`: the
/// figure as hex (a proof's or a transaction's bytes travel as two characters
/// a byte) and [`ANSWER_WRAPPING`]. [`admit_answer`] then holds the bytes the
/// answer carries to the figure itself.
pub fn answer_bound(budget: u64) -> u64 {
    budget.saturating_mul(2).saturating_add(ANSWER_WRAPPING)
}

/// Why an answer's body was not read whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyFault {
    /// The answer declares a length over the bound: nothing of it is read.
    Declared(u64),
    /// The body passed the bound as it arrived: the read stopped there, with
    /// this many bytes seen.
    Past(u64),
    /// The body could not be read (the platform's own error).
    Read(String),
}

/// The line that counts an answer not read past the bound.
pub fn answer_body_log_line(provider: &str, what: &str, fault: &BodyFault, bound: u64) -> String {
    let seen = match fault {
        BodyFault::Declared(n) => format!("declared={n}"),
        BodyFault::Past(n) => format!("read={n}"),
        BodyFault::Read(_) => "read=error".to_string(),
    };
    format!("provider.answer_not_read provider={provider} answer={what} {seen} bound={bound}")
}

/// A `Content-Length` value, where one is given and is a number.
pub fn declared_length(value: Option<&str>) -> Option<u64> {
    value.and_then(|v| v.trim().parse::<u64>().ok())
}

/// Read a body no further than `bound`: refused unread where `declared` is
/// over it, else read as it arrives and stopped once the bytes pass it (the
/// rest is not read and not held).
pub async fn read_bounded<S, E>(
    declared: Option<u64>,
    stream: S,
    bound: u64,
) -> Result<Vec<u8>, BodyFault>
where
    S: futures_util::Stream<Item = Result<Vec<u8>, E>>,
    E: fmt::Display,
{
    use futures_util::StreamExt;
    if let Some(n) = declared.filter(|&n| n > bound) {
        return Err(BodyFault::Declared(n));
    }
    let mut stream = std::pin::pin!(stream);
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| BodyFault::Read(e.to_string()))?;
        let seen = (body.len() as u64).saturating_add(chunk.len() as u64);
        if seen > bound {
            return Err(BodyFault::Past(seen));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Why an answer was not given as text ([`read_answer`], [`read_request`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnswerError {
    /// Not read past the bound: the line that counts it.
    Over(String),
    /// The body could not be read.
    Read(String),
}

impl From<AnswerError> for String {
    fn from(e: AnswerError) -> String {
        match e {
            AnswerError::Over(line) | AnswerError::Read(line) => line,
        }
    }
}

/// The body read by [`read_bounded`] as text. Past the bound one line names
/// the provider and the `Err` is that line, the provider's error; a body that
/// cannot be read or is not UTF-8 is the platform's error, as `text()` gives
/// it.
fn answer_text(
    provider: &str,
    what: &str,
    read: Result<Vec<u8>, BodyFault>,
    bound: u64,
) -> Result<String, AnswerError> {
    match read {
        Ok(bytes) => String::from_utf8(bytes).map_err(|e| AnswerError::Read(e.to_string())),
        Err(BodyFault::Read(e)) => Err(AnswerError::Read(e)),
        Err(fault) => {
            let line = answer_body_log_line(provider, what, &fault, bound);
            #[cfg(target_arch = "wasm32")]
            worker::console_log!("{}", line);
            Err(AnswerError::Over(line))
        }
    }
}

/// A fetched provider answer's body, read no further than
/// [`answer_bound`]`(budget)`.
pub async fn read_answer(
    response: &mut worker::Response,
    provider: &str,
    what: &str,
    budget: u64,
) -> Result<String, AnswerError> {
    let bound = answer_bound(budget);
    let declared = declared_length(
        response
            .headers()
            .get("Content-Length")
            .ok()
            .flatten()
            .as_deref(),
    );
    let read = match response.stream() {
        Ok(stream) => read_bounded(declared, stream, bound).await,
        // A body that is not a stream is already whole (or empty).
        Err(_) => match response.bytes().await {
            Ok(bytes) => {
                read_bounded(
                    declared,
                    futures_util::stream::iter([Ok::<_, String>(bytes)]),
                    bound,
                )
                .await
            }
            Err(e) => Err(BodyFault::Read(e.to_string())),
        },
    };
    answer_text(provider, what, read, bound)
}

/// A request's body sent by a provider (the broadcaster's callback), read no
/// further than [`answer_bound`]`(budget)`.
pub async fn read_request(
    request: &mut worker::Request,
    provider: &str,
    what: &str,
    budget: u64,
) -> Result<String, AnswerError> {
    let bound = answer_bound(budget);
    let declared = declared_length(
        request
            .headers()
            .get("Content-Length")
            .ok()
            .flatten()
            .as_deref(),
    );
    let read = match request.stream() {
        Ok(stream) => read_bounded(declared, stream, bound).await,
        // No body: nothing to read.
        Err(_) => Ok(Vec::new()),
    };
    answer_text(provider, what, read, bound)
}

/// The argument that carries a method's payment, where the method takes one.
pub fn payment_field(method: &str) -> Option<&'static str> {
    match method {
        "internalizeAction" => Some("tx"),
        "processAction" => Some("rawTx"),
        _ => None,
    }
}

/// The position of a method's payment among its arguments when they are
/// given as an array (the struct's field order): `tx` is the first of
/// `internalizeAction`'s, `rawTx` the seventh of `processAction`'s.
pub fn payment_position(method: &str) -> Option<usize> {
    match method {
        "internalizeAction" => Some(0),
        "processAction" => Some(6),
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
/// decoded under the figure `budget`, whether the arguments are named (the
/// payment's field) or positional (the payment's position): an array of more
/// than `budget` items is refused at the first item past it, and a hex string
/// of more than that many bytes is refused on its length, before it is
/// decoded.
pub fn parse_request(body: &[u8], budget: u64) -> Parsed {
    let Ok(head) = serde_json::from_slice::<Head>(body) else {
        return Parsed::Malformed;
    };
    let (Some(field), Some(position)) =
        (payment_field(&head.method), payment_position(&head.method))
    else {
        return match serde_json::from_slice::<JsonRpcRequest>(body) {
            Ok(request) => Parsed::Request(request),
            Err(_) => Parsed::Malformed,
        };
    };
    let over = Cell::new(None);
    let mut de = serde_json::Deserializer::from_slice(body);
    let params = RequestSeed {
        field,
        position,
        budget,
        over: &over,
    }
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
    position: usize,
    budget: u64,
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
                    position: self.position,
                    budget: self.budget,
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
/// in an object, the payment's field is read by [`PaymentSeed`]; in an array
/// (the method's arguments given by position), the item at the payment's
/// position is.
#[derive(Clone, Copy)]
struct ParamsSeed<'a> {
    field: &'static str,
    position: usize,
    budget: u64,
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
            loop {
                let item = if items.len() == self.position {
                    seq.next_element_seed(PaymentSeed {
                        budget: self.budget,
                        over: self.over,
                    })?
                } else {
                    seq.next_element::<Value>()?
                };
                let Some(item) = item else { break };
                items.push(item);
            }
        }
        Ok(Value::Array(items))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut fields = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let value = if key == self.field {
                map.next_value_seed(PaymentSeed {
                    budget: self.budget,
                    over: self.over,
                })?
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
    budget: u64,
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
        if size > self.budget {
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
            if items.len() as u64 == self.budget {
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

    const BUDGET: u64 = DEFAULT_MAX_PAYMENT_BYTES;
    const CAP: usize = BUDGET as usize;

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
    fn the_default_figure_is_600_kib() {
        assert_eq!(DEFAULT_MAX_PAYMENT_BYTES, 600 * 1024);
        assert_eq!(DEFAULT_MAX_PAYMENT_BYTES, 614_400);
        assert!(admit(BUDGET, BUDGET).is_ok());
        match admit(BUDGET + 1, BUDGET) {
            Err(Error::PaymentSize { size, budget }) => {
                assert_eq!((size, budget), (Some(BUDGET + 1), BUDGET))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_setting_names_the_figure_and_anything_else_is_the_default() {
        assert_eq!(budget_from(Some("1000000")), 1_000_000);
        assert_eq!(budget_from(Some(" 2000 ")), 2000);
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("-5"),
            Some("600k"),
            Some("1.5"),
            Some("none"),
            Some("99999999999999999999999"),
        ] {
            assert_eq!(budget_from(value), DEFAULT_MAX_PAYMENT_BYTES, "{value:?}");
        }
    }

    #[test]
    fn the_sentences_are_pinned() {
        assert_eq!(
            description(Some(614_401), BUDGET),
            "Not judged: this payment is 614401 bytes, and this host reads at most 614400 bytes of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block."
        );
        assert_eq!(
            description(None, BUDGET),
            "Not judged: this payment is more than 614400 bytes, the most this host reads of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block."
        );
        assert_eq!(
            data(Some(9), BUDGET),
            json!({ "code": "ERR_PAYMENT_SIZE", "budget": 614_400, "size": 9 })
        );
        assert_eq!(
            data(None, BUDGET),
            json!({ "code": "ERR_PAYMENT_SIZE", "budget": 614_400 })
        );
    }

    #[test]
    fn the_log_lines_are_pinned() {
        assert_eq!(
            log_line(Some(614_401), BUDGET, "02ab"),
            "payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=614401 budget=614400 identity_key=02ab"
        );
        assert_eq!(
            log_line(None, BUDGET, "02ab"),
            "payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=none budget=614400 identity_key=02ab"
        );
        assert_eq!(
            answer_log_line("woc", "raw_tx", 614_401, BUDGET),
            "provider.answer_not_read provider=woc answer=raw_tx size=614401 budget=614400"
        );
    }

    #[test]
    fn a_provider_answer_over_the_figure_is_not_read() {
        assert!(admit_answer("arc", "proof", BUDGET, BUDGET).is_ok());
        assert_eq!(
            admit_answer("arc", "proof", BUDGET + 1, BUDGET),
            Err(answer_log_line("arc", "proof", BUDGET + 1, BUDGET))
        );
        assert!(admit_answer("arc", "proof", 2001, 2000).is_err());
        assert!(admit_answer("arc", "proof", 2000, 2000).is_ok());
    }

    #[test]
    fn a_payment_at_the_budget_decodes_as_serde_json_decodes_it() {
        let at = vec![7u8; CAP];
        for (method, field) in [("internalizeAction", "tx"), ("processAction", "rawTx")] {
            for params in [
                json!([{ "identityKey": "02ab" }, { field: at, "description": "d" }]),
                json!({ field: at, "description": "d" }),
                json!([{ field: hex::encode(&at) }]),
            ] {
                let b = body(method, params);
                let ours = request(parse_request(&b, BUDGET));
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
                    Some(BUDGET + 1),
                ),
            ];
            for (params, size) in cases {
                match parse_request(&body(method, params), BUDGET) {
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
    fn a_door_follows_another_figure() {
        // A host that names 2,000 bytes: exactly 2,000 decodes as before, one
        // byte over is refused, and the refusal names the figure in force.
        let figure = budget_from(Some("2000"));
        for (method, field) in [("internalizeAction", "tx"), ("processAction", "rawTx")] {
            let at = body(method, json!([{ field: vec![7u8; 2000] }]));
            let theirs: JsonRpcRequest = serde_json::from_slice(&at).unwrap();
            assert_eq!(request(parse_request(&at, figure)).params, theirs.params);
            let over_hex = body(method, json!([{ field: hex::encode(vec![7u8; 2001]) }]));
            assert!(matches!(
                parse_request(&over_hex, figure),
                Parsed::Refused {
                    size: Some(2001),
                    ..
                }
            ));
            let over_array = body(method, json!([{ field: vec![7u8; 2001] }]));
            assert!(matches!(
                parse_request(&over_array, figure),
                Parsed::Refused { size: None, .. }
            ));
            // The same 2,001 bytes pass under the default.
            assert!(matches!(
                parse_request(&over_array, BUDGET),
                Parsed::Request(_)
            ));
        }
        assert!(admit(2000, figure).is_ok());
        assert!(matches!(
            admit(2001, figure),
            Err(Error::PaymentSize {
                size: Some(2001),
                budget: 2000
            })
        ));
        assert!(description(Some(2001), figure)
            .starts_with("Not judged: this payment is 2001 bytes, and this host reads at most 2000 bytes of a payment: "));
        assert_eq!(
            data(None, figure),
            json!({ "code": "ERR_PAYMENT_SIZE", "budget": 2000 })
        );
    }

    #[test]
    fn another_method_and_another_argument_keep_their_own_limits() {
        let over = vec![7u8; CAP + 1];
        for b in [
            body("listOutputs", json!([{ "tx": over }])),
            body("internalizeAction", json!([{ "outputs": over }])),
        ] {
            let ours = request(parse_request(&b, BUDGET));
            let theirs: JsonRpcRequest = serde_json::from_slice(&b).unwrap();
            assert_eq!(ours.params, theirs.params);
        }
    }

    #[test]
    fn positional_arguments_are_held_at_the_payment_position() {
        // internalizeAction(tx, outputs, description, labels, seekPermission)
        // and processAction(isNewTx, isSendWith, isNoSend, isDelayed,
        // reference, txid, rawTx, sendWith), as `[auth, [args...]]`.
        let auth = json!({ "identityKey": "02ab" });
        let internalize = |tx: Value| json!([auth, [tx, [], "d", null, null]]);
        let process = |raw: Value| json!([auth, [true, false, false, false, "r", "t", raw, []]]);
        let at = vec![7u8; CAP];
        let over = vec![7u8; CAP + 1];
        for (method, params) in [
            ("internalizeAction", internalize(json!(hex::encode(&at)))),
            ("internalizeAction", internalize(json!(at))),
            ("processAction", process(json!(at))),
            ("processAction", process(Value::Null)),
        ] {
            let b = body(method, params);
            let theirs: JsonRpcRequest = serde_json::from_slice(&b).unwrap();
            assert_eq!(
                request(parse_request(&b, BUDGET)).params,
                theirs.params,
                "{method}"
            );
        }
        for (method, params, size) in [
            (
                "internalizeAction",
                internalize(json!(hex::encode(&over))),
                Some(BUDGET + 1),
            ),
            ("internalizeAction", internalize(json!(over)), None),
            ("processAction", process(json!(over)), None),
            // The arguments alone, as `[[args...]]`.
            (
                "processAction",
                json!([[true, false, false, false, "ref", "txid", over, []]]),
                None,
            ),
        ] {
            match parse_request(&body(method, params), BUDGET) {
                Parsed::Refused { id, size: got } => {
                    assert_eq!(id, json!(7));
                    assert_eq!(got, size, "{method}");
                }
                other => panic!("{method}: {other:?}"),
            }
        }
        // Another position is not the payment's: another method's array of the
        // same length, and processAction's array at another position, pass.
        for b in [
            body("listOutputs", json!([{}, [over]])),
            body("processAction", json!([{}, [over]])),
        ] {
            assert!(matches!(parse_request(&b, BUDGET), Parsed::Request(_)));
        }
    }

    #[test]
    fn a_malformed_setting_is_the_default_and_said_once() {
        use std::sync::atomic::AtomicBool;
        assert_eq!(
            setting_from(None),
            Setting {
                figure: DEFAULT_MAX_PAYMENT_BYTES,
                malformed: false
            }
        );
        assert_eq!(
            setting_from(Some("2000")),
            Setting {
                figure: 2000,
                malformed: false
            }
        );
        for value in ["", "0", "600k", "1.5", "-5", "99999999999999999999999"] {
            assert_eq!(
                setting_from(Some(value)),
                Setting {
                    figure: DEFAULT_MAX_PAYMENT_BYTES,
                    malformed: true
                },
                "{value:?}"
            );
        }
        let said = AtomicBool::new(false);
        let malformed = setting_from(Some("600k"));
        assert_eq!(
            setting_note(malformed, &said),
            Some("setting.malformed MAX_PAYMENT_BYTES is not a positive whole number of bytes; the figure in force is 614400".to_string())
        );
        assert_eq!(setting_note(malformed, &said), None);
        let quiet = AtomicBool::new(false);
        assert_eq!(setting_note(setting_from(None), &quiet), None);
        assert_eq!(setting_note(setting_from(Some("2000")), &quiet), None);
    }

    #[tokio::test]
    async fn an_answer_is_read_no_further_than_its_bound() {
        assert_eq!(ANSWER_WRAPPING, 65_536);
        assert_eq!(answer_bound(BUDGET), 2 * 614_400 + 65_536);
        let chunks = |sizes: &[usize]| {
            sizes
                .iter()
                .map(|&n| Ok::<_, String>(vec![b'a'; n]))
                .collect::<Vec<_>>()
        };
        // Exactly the bound, in chunks: read whole.
        let whole = read_bounded(None, futures_util::stream::iter(chunks(&[40, 60])), 100).await;
        assert_eq!(whole.map(|b| b.len()), Ok(100));
        let declared =
            read_bounded(Some(100), futures_util::stream::iter(chunks(&[100])), 100).await;
        assert_eq!(declared.map(|b| b.len()), Ok(100));
        // A declared length over the bound: no chunk is asked for.
        let polled = Cell::new(0);
        let counted = |sizes: &[usize]| {
            futures_util::StreamExt::inspect(futures_util::stream::iter(chunks(sizes)), |_| {
                polled.set(polled.get() + 1)
            })
        };
        assert_eq!(
            read_bounded(Some(101), counted(&[10]), 100).await,
            Err(BodyFault::Declared(101))
        );
        assert_eq!(polled.get(), 0);
        // No length declared (or a smaller one): stopped at the chunk that
        // passes the bound; the chunks after it are not read.
        assert_eq!(
            read_bounded(None, counted(&[60, 41, 500, 500]), 100).await,
            Err(BodyFault::Past(101))
        );
        assert_eq!(polled.get(), 2);
        assert_eq!(
            read_bounded(Some(5), counted(&[60, 60]), 100).await,
            Err(BodyFault::Past(120))
        );
        assert_eq!(
            read_bounded(
                None,
                futures_util::stream::iter([Err::<Vec<u8>, _>("gone")]),
                100
            )
            .await,
            Err(BodyFault::Read("gone".to_string()))
        );
        assert_eq!(
            answer_body_log_line("woc", "proof", &BodyFault::Declared(101), 100),
            "provider.answer_not_read provider=woc answer=proof declared=101 bound=100"
        );
        assert_eq!(
            answer_body_log_line("arcade", "callback", &BodyFault::Past(120), 100),
            "provider.answer_not_read provider=arcade answer=callback read=120 bound=100"
        );
        assert_eq!(declared_length(Some(" 42 ")), Some(42));
        assert_eq!(declared_length(Some("x")), None);
        assert_eq!(declared_length(None), None);
    }

    #[test]
    fn a_malformed_body_is_malformed() {
        assert!(matches!(parse_request(b"{", BUDGET), Parsed::Malformed));
        assert!(matches!(
            parse_request(
                br#"{"jsonrpc":"2.0","method":"processAction","id":1,"params":[1,]}"#,
                BUDGET
            ),
            Parsed::Malformed
        ));
        let no_params = request(parse_request(
            br#"{"jsonrpc":"2.0","method":"processAction","id":1}"#,
            BUDGET,
        ));
        assert_eq!(no_params.params, Value::Null);
    }
}
