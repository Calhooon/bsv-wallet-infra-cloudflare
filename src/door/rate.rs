//! The two rate limits (`StorageServer.ts:477-487, 540-549`;
//! `RateLimitPolicy.ts:10-37`): 300 a minute per IP before auth, skipping
//! GET, HEAD and OPTIONS, and 1,000 a minute per identity after it, both
//! answered `429 ERR_RATE_LIMITED`.
//!
//! The counters are the Workers rate-limit bindings `RATE_LIMIT_IP` and
//! `RATE_LIMIT_IDENTITY` (`wrangler.toml`; the numbers live there, as
//! `simple = { limit, period }`). A binding counts per Cloudflare location
//! and needs no D1 statement and no account id. A Worker without the binding
//! is not limited (the operator's choice; `GET /` names each binding `bound`
//! or `absent`); a limiter that faults admits the request and logs, so an
//! outage of the counter is never an outage of the door.

use std::net::{IpAddr, Ipv6Addr};

use super::Refusal;

/// The binding counting requests per IP before auth (300 a minute).
pub const IP_BINDING: &str = "RATE_LIMIT_IP";

/// The binding counting requests per identity after auth (1,000 a minute).
pub const IDENTITY_BINDING: &str = "RATE_LIMIT_IDENTITY";

/// The pre-auth limit skips the reads (`StorageServer.ts:482`).
pub fn pre_auth_skips(method: &worker::Method) -> bool {
    matches!(
        method,
        worker::Method::Get | worker::Method::Head | worker::Method::Options
    )
}

/// The pre-auth key: `ip:` and the caller's address (`CF-Connecting-IP`),
/// an IPv6 address by its /56 as `express-rate-limit`'s `ipKeyGenerator` does.
pub fn ip_key(connecting_ip: Option<&str>) -> String {
    let Some(raw) = connecting_ip.map(str::trim).filter(|s| !s.is_empty()) else {
        return "ip:unknown".to_string();
    };
    match raw.parse::<IpAddr>() {
        Ok(IpAddr::V6(v6)) => {
            let mut octets = v6.octets();
            octets[7..].fill(0);
            format!("ip:{}/56", Ipv6Addr::from(octets))
        }
        Ok(IpAddr::V4(v4)) => format!("ip:{v4}"),
        Err(_) => format!("ip:{raw}"),
    }
}

/// The post-auth key: `identity:` and the session's identity key, or the IP
/// key when there is none (`RateLimitPolicy.ts:29-37`).
pub fn identity_key(identity: &str, connecting_ip: Option<&str>) -> String {
    let identity = identity.trim();
    if identity.is_empty() || identity == "unknown" {
        return ip_key(connecting_ip);
    }
    format!("identity:{identity}")
}

/// The reference's 429 (`RateLimitPolicy.ts:18-24`).
pub fn limited() -> Refusal {
    Refusal::plain(
        429,
        "ERR_RATE_LIMITED",
        "Too many requests. Please retry later.",
    )
}

/// The limit and window the reference sets on a binding (`StorageServer.ts:480,
/// 542`), for the draft-8 headers; the binding counts by `wrangler.toml`'s
/// `simple` and reports neither number.
pub fn reference_limit(binding: &str) -> (u32, u32) {
    if binding == IP_BINDING {
        (300, 60)
    } else {
        (1_000, 60)
    }
}

/// The policy's name as `express-rate-limit` forms it: the limit, `-in-`,
/// and the window in its largest whole unit (`300-in-1min`).
fn policy_name(limit: u32, window_secs: u32) -> String {
    let duration = match window_secs {
        s if s < 60 => format!("{s}sec"),
        s if s < 3_600 => format!("{}min", s / 60),
        s if s < 86_400 => {
            let h = s / 3_600;
            format!("{h}hr{}", if h > 1 { "s" } else { "" })
        }
        s => {
            let d = s / 86_400;
            format!("{d}day{}", if d > 1 { "s" } else { "" })
        }
    };
    format!("{limit}-in-{duration}")
}

/// The headers of a 429 on `binding` for the counted `key`, the draft-8 set
/// the reference enables (`RateLimitPolicy.ts:16`, `standardHeaders:
/// 'draft-8'`): `RateLimit`, `RateLimit-Policy` and `Retry-After`. The
/// remaining count of a refused request is 0. The binding gives no reset
/// time, so `t` and `Retry-After` are the whole window, as
/// `express-rate-limit` sends when it has none. The partition key is its
/// `pk`: the base64 of the leading 12 hex digits of the key's SHA-256, the
/// key being the bare address before auth (the reference's default key).
pub fn limited_headers(binding: &str, key: &str) -> Vec<(String, String)> {
    let (limit, window) = reference_limit(binding);
    let name = policy_name(limit, window);
    let partition = if binding == IP_BINDING {
        key.strip_prefix("ip:").unwrap_or(key)
    } else {
        key
    };
    let digest = hex::encode(bsv_sdk::primitives::sha256(partition.as_bytes()));
    let pk = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        digest.get(..12).unwrap_or(&digest),
    );
    vec![
        (
            "RateLimit".to_string(),
            format!("\"{name}\"; r=0; t={window}"),
        ),
        (
            "RateLimit-Policy".to_string(),
            format!("\"{name}\"; q={limit}; w={window}; pk=:{pk}:"),
        ),
        ("Retry-After".to_string(), window.to_string()),
    ]
}

/// Whether `binding` is bound on this Worker: `"bound"` or `"absent"`.
pub fn binding_state(env: &worker::Env, binding: &str) -> &'static str {
    if env.rate_limiter(binding).is_ok() {
        "bound"
    } else {
        "absent"
    }
}

/// The two limits as `GET /` reports them, each `bound` or `absent`, so a
/// Worker deployed without a binding (and so not limited) shows it.
pub fn surface(env: &worker::Env) -> serde_json::Value {
    serde_json::json!({
        IP_BINDING: binding_state(env, IP_BINDING),
        IDENTITY_BINDING: binding_state(env, IDENTITY_BINDING),
    })
}

/// Count one request under `key` on `binding`: `false` when the limit is
/// reached. No binding, or a fault of the limiter, admits.
pub async fn admit(env: &worker::Env, binding: &str, key: String) -> bool {
    let Ok(limiter) = env.rate_limiter(binding) else {
        return true;
    };
    match limiter.limit(key).await {
        Ok(outcome) => outcome.success,
        Err(e) => {
            worker::console_error!("rate limit {binding}: {e}; admitted");
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keys() {
        assert_eq!(ip_key(Some("203.0.113.7")), "ip:203.0.113.7");
        assert_eq!(
            ip_key(Some("2001:db8:1234:5678:9abc::1")),
            "ip:2001:db8:1234:5600::/56"
        );
        assert_eq!(ip_key(None), "ip:unknown");
        assert_eq!(ip_key(Some("  ")), "ip:unknown");
        assert_eq!(identity_key("02ab", Some("203.0.113.7")), "identity:02ab");
        assert_eq!(
            identity_key("unknown", Some("203.0.113.7")),
            "ip:203.0.113.7"
        );
        assert_eq!(identity_key("", None), "ip:unknown");
    }

    #[test]
    fn reads_skip_the_pre_auth_limit() {
        assert!(pre_auth_skips(&worker::Method::Get));
        assert!(pre_auth_skips(&worker::Method::Head));
        assert!(pre_auth_skips(&worker::Method::Options));
        assert!(!pre_auth_skips(&worker::Method::Post));
    }

    #[test]
    fn the_draft_8_headers() {
        let ip = limited_headers(IP_BINDING, "ip:203.0.113.7");
        assert_eq!(
            ip[0],
            ("RateLimit".into(), "\"300-in-1min\"; r=0; t=60".into())
        );
        // The pk of "203.0.113.7" by express-rate-limit's getPartitionKey, in Node:
        // Buffer.from(sha256hex.slice(0, 12)).toString("base64").
        let pk = "ZmVjNTI1NjVhYTBj";
        assert_eq!(
            ip[1],
            (
                "RateLimit-Policy".into(),
                format!("\"300-in-1min\"; q=300; w=60; pk=:{pk}:")
            )
        );
        assert_eq!(ip[2], ("Retry-After".into(), "60".into()));
        let identity = limited_headers(IDENTITY_BINDING, "identity:02ab");
        assert_eq!(identity[0].1, "\"1000-in-1min\"; r=0; t=60");
        assert!(identity[1]
            .1
            .starts_with("\"1000-in-1min\"; q=1000; w=60; pk=:"));
        assert_eq!(policy_name(30, 30), "30-in-30sec");
        assert_eq!(policy_name(5, 7_200), "5-in-2hrs");
        assert_eq!(policy_name(5, 86_400), "5-in-1day");
    }

    #[test]
    fn the_refusal_is_the_references() {
        let r = limited();
        assert_eq!(r.status, 429);
        assert_eq!(r.body["code"], "ERR_RATE_LIMITED");
        assert_eq!(
            r.body["description"],
            "Too many requests. Please retry later."
        );
    }
}
