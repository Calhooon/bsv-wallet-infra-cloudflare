//! Arcade's policy refusal as a hint (#50; the broadcaster charter's B5:
//! "A policy refusal is a hint, never a word").
//!
//! A `400` from Arcade's `POST /tx` or `/txs` whose body carries `status` 460
//! to 475 is the instance's own policy answer, recorded on the request's
//! history with the code, the instance and the fee its `GET /policy` asks.
//! The verdict is to retry at the fee asked; the transaction keeps its state
//! and its inputs. The codes are Arcade's ARC taxonomy at `arcade@85bfe5d`
//! `errors/errors.go:25-53` (470 unassigned there); 476, the finality gate's
//! answer, is outside this table.

use serde_json::{json, Value};

/// Arcade's ARC codes 460 to 475, each a policy refusal under B5.
pub const ARCADE_POLICY_CODES: &[(u16, &str)] = &[
    (460, "TxFormat"),
    (461, "UnlockingScripts"),
    (462, "Inputs"),
    (463, "Malformed"),
    (464, "Outputs"),
    (465, "Fees"),
    (466, "Conflict"),
    (467, "Generic"),
    (468, "BeefInvalid"),
    (469, "MerkleRoots"),
    (471, "FrozenPolicy"),
    (472, "FrozenConsensus"),
    (473, "CumulativeFees"),
    (474, "TxSize"),
    (475, "MinedAncestorsNotInBUMP"),
];

/// The code and its name when an HTTP answer is Arcade's `400` carrying a
/// `status` of [`ARCADE_POLICY_CODES`]; `None` for any other answer.
pub fn arcade_policy_code(http_status: u16, body: &str) -> Option<(u16, &'static str)> {
    if http_status != 400 {
        return None;
    }
    let code = serde_json::from_str::<Value>(body)
        .ok()?
        .get("status")?
        .as_u64()?;
    ARCADE_POLICY_CODES
        .iter()
        .find(|(c, _)| u64::from(*c) == code)
        .copied()
}

/// The fee an instance's policy asks: `satoshis` per `bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MiningFee {
    /// Satoshis asked per `bytes`.
    pub satoshis: u64,
    /// The size the satoshis are asked for.
    pub bytes: u64,
}

/// Read `policy.miningFee` from a `GET /policy` body (Arcade's
/// `PolicyResponse`, `openapi/arcade.openapi.yaml:142-176` at `85bfe5d`).
pub fn parse_policy_fee(body: &str) -> Option<MiningFee> {
    let v: Value = serde_json::from_str(body).ok()?;
    let fee = v.get("policy")?.get("miningFee")?;
    Some(MiningFee {
        satoshis: fee.get("satoshis")?.as_u64()?,
        bytes: fee.get("bytes")?.as_u64()?,
    })
}

/// One policy refusal, as recorded on the request's history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRefusal {
    /// The ARC code Arcade answered.
    pub code: u16,
    /// The code's name in Arcade's taxonomy.
    pub name: &'static str,
    /// The instance that answered (its host).
    pub instance: String,
    /// The fee its policy asks, when the policy read answered.
    pub fee_asked: Option<MiningFee>,
    /// The instance's own text.
    pub text: String,
}

impl PolicyRefusal {
    /// The hint's words for the history note: the code, the instance, the
    /// fee asked and the verdict, never invalidity.
    pub fn hint_words(&self) -> Value {
        json!({
            "instance": self.instance,
            "code": self.code,
            "name": self.name,
            "feeAsked": self.fee_asked.map(|f| json!({"satoshis": f.satoshis, "bytes": f.bytes})),
            "text": self.text,
            "verdict": "retry at the fee asked",
        })
    }
}

impl std::fmt::Display for PolicyRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} policy refusal {} ({}): {}",
            self.instance, self.code, self.name, self.text
        )?;
        if let Some(fee) = self.fee_asked {
            write!(
                f,
                "; policy asks {} sats per {} bytes",
                fee.satoshis, fee.bytes
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_460_to_475_but_470_is_a_policy_refusal() {
        for code in 455u16..=480 {
            let body = format!(r#"{{"error":"transaction failed validation","status":{code}}}"#);
            let got = arcade_policy_code(400, &body).map(|(c, _)| c);
            let want = ((460..=475).contains(&code) && code != 470).then_some(code);
            assert_eq!(got, want, "code {code}");
        }
    }

    #[test]
    fn only_a_400_with_a_status_is_read() {
        assert_eq!(arcade_policy_code(422, r#"{"status":465}"#), None);
        assert_eq!(arcade_policy_code(400, r#"{"error":"decode"}"#), None);
        assert_eq!(arcade_policy_code(400, "not json"), None);
        assert_eq!(
            arcade_policy_code(400, r#"{"status":465}"#),
            Some((465, "Fees"))
        );
    }

    #[test]
    fn the_policy_fee_is_read_from_the_openapi_example() {
        let body = r#"{"policy":{"miningFee":{"satoshis":100,"bytes":1000},"maxtxsizepolicy":10485760},"timestamp":"2026-08-07T12:00:00Z"}"#;
        assert_eq!(
            parse_policy_fee(body),
            Some(MiningFee {
                satoshis: 100,
                bytes: 1000
            })
        );
        assert_eq!(parse_policy_fee(r#"{"policy":{}}"#), None);
    }

    #[test]
    fn the_hint_carries_code_instance_and_fee() {
        let r = PolicyRefusal {
            code: 465,
            name: "Fees",
            instance: "arcade.test".into(),
            fee_asked: Some(MiningFee {
                satoshis: 100,
                bytes: 1000,
            }),
            text: "fee too low".into(),
        };
        let w = r.hint_words();
        assert_eq!(w["code"], 465);
        assert_eq!(w["instance"], "arcade.test");
        assert_eq!(w["feeAsked"]["satoshis"], 100);
        assert_eq!(w["feeAsked"]["bytes"], 1000);
        assert!(r
            .to_string()
            .contains("policy asks 100 sats per 1000 bytes"));
        let bare = PolicyRefusal {
            fee_asked: None,
            ..r
        };
        assert!(bare.hint_words()["feeAsked"].is_null());
    }
}
