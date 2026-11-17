//! What GET /healthz answers: the status and the build object, so a deployer can read what a
//! deployed Worker runs. The object is the one the stack's other Workers serve (the shared
//! capture of bsv-worker-build-info, run by build.rs) with this crate's own version beside
//! the locked bsv-rs and middleware versions. No secret, no id, no hostname.
use serde_json::{json, Value};

/// The build object: `lib_rev` (the source revision, or `unknown`), `built_at` (UTC),
/// `dirty` (whether the tree held changes when built) and `crates` (the locked versions).
pub fn build() -> Value {
    let shared = bsv_worker_build_info::build_json!();
    json!({
        "lib_rev": shared["lib_rev"],
        "built_at": shared["built_at"],
        "dirty": shared["dirty"],
        "crates": {
            "bsv-rs": shared["crates"]["bsv-rs"],
            "bsv-middleware-cloudflare": shared["crates"]["bsv-middleware-cloudflare"],
            "bsv-middleware-core": shared["crates"]["bsv-middleware-core"],
            "bsv-wallet-infra-cloudflare": env!("CARGO_PKG_VERSION"),
        },
    })
}

/// The GET /healthz body.
pub fn healthz() -> Value {
    json!({ "status": "ok", "build": build() })
}

#[cfg(test)]
mod tests {
    /// The version the lock names for `name`, which must be named once.
    fn locked(name: &str) -> String {
        let lock = include_str!("../Cargo.lock");
        let versions: Vec<&str> = lock
            .split("[[package]]")
            .filter(|block| block.contains(&format!("\nname = \"{name}\"\n")))
            .filter_map(|block| block.lines().find_map(|l| l.strip_prefix("version = ")))
            .map(|v| v.trim_matches('"'))
            .collect();
        assert_eq!(versions.len(), 1, "{name}: {versions:?}");
        versions[0].to_string()
    }

    #[test]
    fn healthz_serves_the_build_object() {
        let body = super::healthz();
        let mut keys: Vec<_> = body.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["build", "status"]);
        assert_eq!(body["status"], "ok");

        let build = &body["build"];
        let mut keys: Vec<_> = build.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["built_at", "crates", "dirty", "lib_rev"]);
        let rev = build["lib_rev"].as_str().unwrap();
        assert!(
            rev == "unknown" || (rev.len() == 40 && rev.bytes().all(|b| b.is_ascii_hexdigit())),
            "{rev}"
        );
        assert!(build["dirty"].is_boolean());
        let built_at = build["built_at"].as_str().unwrap();
        assert!(built_at.ends_with('Z'), "{built_at}");
        assert!(chrono::DateTime::parse_from_rfc3339(built_at).is_ok());

        let crates = build["crates"].as_object().unwrap();
        assert_eq!(crates.len(), 4);
        for name in [
            "bsv-rs",
            "bsv-middleware-cloudflare",
            "bsv-middleware-core",
            "bsv-wallet-infra-cloudflare",
        ] {
            assert_eq!(crates[name], locked(name), "{name}");
        }
        println!("{body}");
    }
}
