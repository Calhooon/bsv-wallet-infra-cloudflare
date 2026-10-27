//! The two rate limits (`StorageServer.ts:477-487, 540-549`;
//! `RateLimitPolicy.ts:10-37`): 300 a minute per IP before auth, skipping
//! GET, HEAD and OPTIONS, and 1,000 a minute per identity after it, both
//! answered `429 ERR_RATE_LIMITED`.
//!
//! The counters are the Workers rate-limit bindings `RATE_LIMIT_IP` and
//! `RATE_LIMIT_IDENTITY` (`wrangler.toml`; the numbers live there, as
//! `simple = { limit, period }`). A binding counts per Cloudflare location
//! and needs no D1 statement and no account id. A Worker without the binding
//! is not limited (the operator's choice); a limiter that faults admits the
//! request and logs, so an outage of the counter is never an outage of the door.

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
    fn the_refusal_is_the_references() {
        let r = limited();
        assert_eq!(r.status, 429);
        assert_eq!(r.body["code"], "ERR_RATE_LIMITED");
        assert_eq!(r.body["description"], "Too many requests. Please retry later.");
    }
}
