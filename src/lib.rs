//! rust-wallet-infra: BSV Wallet Storage Server on Cloudflare Workers.
//!
//! Self-hosted replacement for storage.babbage.systems.
//! Backed by D1 (SQLite) + R2 (blob storage).
//!
//! Endpoints:
//! - GET  /                   -> health check (no auth)
//! - POST /.well-known/auth   -> BRC-31 handshake (middleware)
//! - POST /                   -> JSON-RPC 2.0 dispatch (authenticated)

pub mod arcade_callback;
pub mod audit;
pub mod beef_at_rest;
pub mod bench;
pub mod d1;
pub mod dispatch;
pub mod entities;
pub mod error;
pub mod json_rpc;
pub mod monitor;
pub mod r2;
pub mod services;
pub mod storage;
pub mod types;

use bsv_middleware_cloudflare::{
    add_cors_headers,
    middleware::{
        auth::handle_cors_preflight, process_auth, sign_json_response, AuthMiddlewareOptions,
        AuthResult,
    },
};
use worker::*;

use crate::json_rpc::{JsonRpcError, JsonRpcRequest};
use crate::storage::StorageD1;
use crate::types::AuthId;

/// Read an env value that may live as either a secret or a plain var.
fn env_value(env: &Env, name: &str) -> Option<String> {
    env.secret(name)
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var(name).ok().map(|v| v.to_string()))
}

/// ChainTracker for the monitor: ChainTracks (CHAINTRACKS_URL) with WoC fallback.
fn build_monitor_headers(env: &Env) -> crate::services::chaintracker::HeaderProvider {
    let chaintracks_url = env
        .var("CHAINTRACKS_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty());
    crate::services::chaintracker::build_header_provider(
        chaintracks_url,
        env_value(env, "WOC_API_KEY"),
    )
}

/// `?key=` must equal MONITOR_TRIGGER_KEY (secret or var); unset key ⇒ always refused.
fn monitor_key_ok(env: &Env, url: &Url) -> bool {
    let provided = url
        .query_pairs()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.to_string())
        .unwrap_or_default();
    let expected = env_value(env, "MONITOR_TRIGGER_KEY").unwrap_or_default();
    !expected.is_empty() && provided == expected
}

/// Build the env-selected broadcast/proof provider (`BROADCASTER`: absent/`arc` = today's
/// ARC→WoC MultiProvider path; `arcade` = Arcade V2 with ARC/WoC as the OUTAGE fallback).
/// A selector typo is a hard configuration error — callers surface it loudly and refuse
/// to run, never guessing a broadcaster.
fn build_provider(
    env: &Env,
) -> std::result::Result<crate::services::selected::SelectedProvider, String> {
    let arc_api_key = env_value(env, "ARC_API_KEY");
    let woc_api_key = env_value(env, "WOC_API_KEY");
    let chaintracks_url = env
        .var("CHAINTRACKS_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty());
    let multi = crate::services::multi::MultiProvider::with_chaintracks(
        arc_api_key,
        woc_api_key,
        chaintracks_url,
    );
    let choice = crate::services::selected::BroadcasterChoice::parse(
        env_value(env, "BROADCASTER").as_deref(),
    )?;
    let arcade_url = env.var("ARCADE_URL").ok().map(|v| v.to_string());
    // Webhook proof delivery: when BOTH are set, submits register X-CallbackUrl and
    // Arcade POSTs status (+ the free MINED merklePath) to /arcade/callback, Bearer-authed
    // with the token. Either missing ⇒ no registration (SSE-verdict-only, as before).
    let callback = match (
        env.var("ARCADE_CALLBACK_URL").ok().map(|v| v.to_string()),
        env_value(env, "ARCADE_CALLBACK_TOKEN"),
    ) {
        (Some(url), Some(token)) if !url.trim().is_empty() && !token.trim().is_empty() => {
            Some((url, token))
        }
        _ => None,
    };
    Ok(crate::services::selected::SelectedProvider::with_callback(
        choice, arcade_url, callback, multi,
    ))
}

/// HTTP status and JSON body for a `process_auth` fault, from the crate's own mapping
/// (KV/SDK/transport/config → 500; unreadable JSON → 400). Since middleware 0.4.1 an
/// authentication refusal is never an `Err`: the middleware answers it itself (401,
/// CORS) as `AuthResult::Response`, which the worker passes through unchanged.
fn auth_fault_parts(e: &bsv_middleware_cloudflare::AuthCloudflareError) -> (u16, String) {
    (e.status_code(), e.to_json())
}

#[event(fetch)]
pub async fn main(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // No `init_panic_hook()`: worker 0.8's `#[wasm_bindgen(start)]` installs a logging
    // panic hook that also flags the instance for re-init; replacing it would lose that.

    // CORS preflight
    if req.method() == Method::Options {
        return handle_cors_preflight();
    }

    // Health check (no auth). Surfaces the active broadcaster selection so a deploy
    // can be gate-checked (and a BROADCASTER typo is visible here, not just in logs).
    if req.path() == "/" && req.method() == Method::Get {
        let broadcaster = match crate::services::selected::BroadcasterChoice::parse(
            env_value(&env, "BROADCASTER").as_deref(),
        ) {
            Ok(crate::services::selected::BroadcasterChoice::Arc) => "arc".to_string(),
            Ok(crate::services::selected::BroadcasterChoice::Arcade) => "arcade".to_string(),
            Err(e) => format!("MISCONFIGURED: {e}"),
        };
        let response = Response::from_json(&serde_json::json!({
            "status": "ok",
            "service": "wallet-infra",
            "broadcaster": broadcaster
        }))?;
        return Ok(add_cors_headers(response));
    }

    // Arcade V2 webhook — the push-native proof path. Registered at submit via
    // X-CallbackUrl; Arcade POSTs TransactionStatus JSON authed with
    // `Authorization: Bearer <ARCADE_CALLBACK_TOKEN>`. Fail-closed: no configured
    // token ⇒ the route does not exist (404); bad/missing bearer ⇒ 401 (Arcade's
    // reaper retries those). Once authed, ALWAYS 200 — per-tx problems are logged
    // and left to the cron monitor's fallback, never turned into retry storms.
    if req.path() == "/arcade/callback" && req.method() == Method::Post {
        let Some(secret) = env_value(&env, "ARCADE_CALLBACK_TOKEN").filter(|s| !s.is_empty())
        else {
            return Response::error("Not Found", 404);
        };
        let presented = req.headers().get("Authorization").ok().flatten();
        if presented.as_deref() != Some(format!("Bearer {secret}").as_str()) {
            return Response::error("unauthorized", 401);
        }
        let body: serde_json::Value = match req.json().await {
            Ok(b) => b,
            Err(_) => {
                // Unparseable body: ack (200) so a malformed event is not retried forever.
                console_error!("arcade-callback: unparseable JSON body");
                return Response::from_json(&serde_json::json!({ "ok": true, "processed": 0 }));
            }
        };
        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let blobs = env
            .bucket("BLOBS")
            .map_err(|e| Error::from(e.to_string()))?;
        let outcome = arcade_callback::handle(&env, &db, &blobs, body).await;
        return Response::from_json(&outcome);
    }

    // Monitor status — aggregate counts only, no PII
    if req.path() == "/monitor/status" && req.method() == Method::Get {
        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let response = Response::from_json(&monitor::get_status(&db).await)?;
        return Ok(add_cors_headers(response));
    }

    // Manual monitor trigger — for debugging. Gated by ?key=<MONITOR_TRIGGER_KEY>
    // so rando HTTP callers can't cause WoC rate-limit bursts. Runs the full
    // run_monitor() pipeline synchronously and returns the MonitorResult as
    // JSON so you can see checked/found/errors without waiting for the
    // next cron cycle.
    if req.path() == "/monitor/run" && req.method() == Method::Post {
        let url = req.url().map_err(|e| Error::from(e.to_string()))?;
        if !monitor_key_ok(&env, &url) {
            let response = Response::from_json(&serde_json::json!({
                "error": "unauthorized — set MONITOR_TRIGGER_KEY secret and pass ?key=<value>"
            }))?
            .with_status(401);
            return Ok(add_cors_headers(response));
        }

        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let blobs = env
            .bucket("BLOBS")
            .map_err(|e| Error::from(e.to_string()))?;
        let provider = match build_provider(&env) {
            Ok(p) => p,
            Err(e) => {
                let response =
                    Response::from_json(&serde_json::json!({ "error": e }))?.with_status(500);
                return Ok(add_cors_headers(response));
            }
        };
        let headers = build_monitor_headers(&env);
        let result = monitor::run_monitor(&db, &blobs, &provider, &provider, &headers).await;

        let response = Response::from_json(&serde_json::json!({
            "sent": result.sent,
            "send_errors": result.send_errors,
            "proofs_found": result.proofs_found,
            "proofs_checked": result.proofs_checked,
            "abandoned_failed": result.abandoned_failed,
            "status_synced": result.status_synced,
            "beef_compacted": result.beef_compacted,
            "unfail_recovered": result.unfail_recovered,
            "purged": result.purged,
            "nosend_found": result.nosend_found,
            "reorg_detected": result.reorg_detected,
            "reorg_depth": result.reorg_depth,
            "proofs_reverified": result.proofs_reverified,
            "ext_spends_scanned": result.ext_spends_scanned,
            "ext_spends_found": result.ext_spends_found,
            "deep_sweep_checked": result.deep_sweep_checked,
            "deep_sweep_mismatched": result.deep_sweep_mismatched,
            "deep_sweep_repaired": result.deep_sweep_repaired,
            "errors": result.errors,
        }))?;
        return Ok(add_cors_headers(response));
    }

    // Deep reorg sweep over a height range, now — POST
    // /monitor/reorg-sweep?key=<MONITOR_TRIGGER_KEY>&from=<h>&to=<h>. Ignores the
    // cron cursor; at most 500 heights. Every group's root is verified against
    // the ChainTracker and orphan groups are re-proved (monitor/deep_reorg.rs).
    if req.path() == "/monitor/reorg-sweep" && req.method() == Method::Post {
        let url = req.url().map_err(|e| Error::from(e.to_string()))?;
        if !monitor_key_ok(&env, &url) {
            let response = Response::from_json(&serde_json::json!({
                "error": "unauthorized — set MONITOR_TRIGGER_KEY secret and pass ?key=<value>"
            }))?
            .with_status(401);
            return Ok(add_cors_headers(response));
        }
        let param = |name: &str| {
            url.query_pairs()
                .find(|(k, _)| k == name)
                .and_then(|(_, v)| v.parse::<u32>().ok())
        };
        let (Some(from), Some(to)) = (param("from"), param("to")) else {
            let response = Response::from_json(&serde_json::json!({
                "error": "from and to (block heights) are required"
            }))?
            .with_status(400);
            return Ok(add_cors_headers(response));
        };
        if to < from {
            let response =
                Response::from_json(&serde_json::json!({ "error": "to must be >= from" }))?
                    .with_status(400);
            return Ok(add_cors_headers(response));
        }
        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let blobs = env
            .bucket("BLOBS")
            .map_err(|e| Error::from(e.to_string()))?;
        let provider = match build_provider(&env) {
            Ok(p) => p,
            Err(e) => {
                let response =
                    Response::from_json(&serde_json::json!({ "error": e }))?.with_status(500);
                return Ok(add_cors_headers(response));
            }
        };
        let headers = build_monitor_headers(&env);
        let out =
            monitor::deep_reorg::deep_reorg_sweep_range(&db, &blobs, &provider, &headers, from, to)
                .await;
        let mut body = out.to_json();
        body["from"] = serde_json::json!(from);
        body["to"] = serde_json::json!(
            to.min(from.saturating_add(monitor::deep_reorg::DEEP_SWEEP_RANGE_MAX_HEIGHTS - 1))
        );
        return Ok(add_cors_headers(Response::from_json(&body)?));
    }

    // Debug: raw WoC TSC proof probe — GET /monitor/probe-woc?txid=<txid>&key=<MONITOR_TRIGGER_KEY>
    // Calls WoC exactly as the monitor would and returns the raw status + body.
    if req.path() == "/monitor/probe-woc" && req.method() == Method::Get {
        let url = req.url().map_err(|e| Error::from(e.to_string()))?;
        let provided = url
            .query_pairs()
            .find(|(k, _)| k == "key")
            .map(|(_, v)| v.to_string())
            .unwrap_or_default();
        let expected = env
            .secret("MONITOR_TRIGGER_KEY")
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_default();
        if expected.is_empty() || provided != expected {
            return Ok(add_cors_headers(
                Response::from_json(&serde_json::json!({"error":"unauthorized"}))?.with_status(401),
            ));
        }
        let txid = url
            .query_pairs()
            .find(|(k, _)| k == "txid")
            .map(|(_, v)| v.to_string())
            .unwrap_or_default();
        if txid.len() != 64 {
            return Ok(add_cors_headers(
                Response::from_json(&serde_json::json!({"error":"txid must be 64-char hex"}))?
                    .with_status(400),
            ));
        }
        let woc_key = env.secret("WOC_API_KEY").ok().map(|s| s.to_string());
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        if let Some(ref key) = woc_key {
            let headers = worker::Headers::new();
            let _ = headers.set("woc-api-key", key);
            init.with_headers(headers);
        }
        let url_str = format!(
            "https://api.whatsonchain.com/v1/bsv/main/tx/{}/proof/tsc",
            txid
        );
        let request = worker::Request::new_with_init(&url_str, &init)
            .map_err(|e| Error::from(e.to_string()))?;
        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        let status = response.status_code();
        let body = response.text().await.unwrap_or_default();
        return Ok(add_cors_headers(Response::from_json(&serde_json::json!({
            "url": url_str,
            "has_api_key": woc_key.is_some(),
            "api_key_len": woc_key.as_ref().map(|k| k.len()),
            "status": status,
            "body_len": body.len(),
            "body_preview": &body[..body.len().min(500)],
        }))?));
    }

    // UTXO audit — integrity checks and optional deep validation
    if req.path().starts_with("/monitor/audit") && req.method() == Method::Get {
        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let blobs = env
            .bucket("BLOBS")
            .map_err(|e| Error::from(e.to_string()))?;

        // Parse ?level= query parameter (default: 2)
        let url = req.url().map_err(|e| Error::from(e.to_string()))?;
        let level: u8 = url
            .query_pairs()
            .find(|(k, _)| k == "level")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(2);

        let report = audit::run_audit(&db, &blobs, level).await;
        let response = Response::from_json(&report)?;
        return Ok(add_cors_headers(response));
    }

    // Get server key
    let server_key = env
        .secret("SERVER_PRIVATE_KEY")
        .map_err(|e| Error::from(format!("SERVER_PRIVATE_KEY not set: {}", e)))?
        .to_string();

    // Auth options — all requests require authentication
    let auth_options = AuthMiddlewareOptions {
        server_private_key: server_key,
        allow_unauthenticated: false,
        session_ttl_seconds: 3600,
        ..Default::default()
    };

    // Process auth (handles BRC-31 handshake + session validation)
    // A refusal (no auth headers, bad signature, header identity not the session's,
    // unknown session, replayed or missing nonce) is the middleware's own 401
    // `AuthResult::Response` (0.4.1+), passed through below so the client
    // re-handshakes. An `Err` is a fault only.
    let auth_result = match process_auth(req, &env, &auth_options).await {
        Ok(r) => r,
        Err(e) => {
            console_error!("auth: {}", e);
            let (status, body) = auth_fault_parts(&e);
            let resp = Response::from_bytes(body.into_bytes())?.with_status(status);
            resp.headers().set("Content-Type", "application/json")?;
            return Ok(add_cors_headers(resp));
        }
    };

    let (auth_context, req, session, request_body) = match auth_result {
        AuthResult::Authenticated {
            context,
            request,
            session,
            body,
        } => (context, request, session, body),
        AuthResult::Response(response) => return Ok(response),
    };

    // Require session for response signing
    let session = match session {
        Some(s) => s,
        None => {
            let resp = Response::from_json(&serde_json::json!({
                "status": "error",
                "code": "ERR_NO_SESSION",
                "description": "Authentication required"
            }))?
            .with_status(401);
            return Ok(add_cors_headers(resp));
        }
    };

    // Only POST / is valid for JSON-RPC
    if req.path() != "/" || req.method() != Method::Post {
        let body = serde_json::json!({
            "status": "error",
            "code": "NOT_FOUND",
            "description": "Unknown endpoint. Use POST / for JSON-RPC."
        });
        return sign_json_response(&body, 404, &[], &session)
            .map_err(|e| Error::from(e.to_string()));
    }

    // Parse JSON-RPC request
    let rpc_request: JsonRpcRequest = match serde_json::from_slice(&request_body) {
        Ok(r) => r,
        Err(_) => {
            let error = JsonRpcError::parse_error();
            return sign_json_response(&error, 200, &[], &session)
                .map_err(|e| Error::from(e.to_string()));
        }
    };

    // Get D1 and R2 bindings
    let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
    let blobs = env
        .bucket("BLOBS")
        .map_err(|e| Error::from(e.to_string()))?;

    // Read WoC API key (optional — if set, sent as `woc-api-key` header on all
    // WoC requests to bypass anonymous IP-based rate limiting).
    let woc_api_key = env_value(&env, "WOC_API_KEY");

    // Read BEEF verification mode (default: "strict" — verifies merkle roots via ChainTracks/WoC)
    let beef_mode = env
        .var("BEEF_VERIFICATION")
        .ok()
        .map(|v| crate::types::BeefVerificationMode::from_env_str(&v.to_string()))
        .unwrap_or_default();

    // Read ChainTracks URL (optional — if set, uses ChainTracks with WoC fallback)
    let chaintracks_url = env
        .var("CHAINTRACKS_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty());

    // Build header provider for BEEF verification
    let header_provider = crate::services::chaintracker::build_header_provider(
        chaintracks_url.clone(),
        woc_api_key.clone(),
    );

    // Build the env-selected broadcast/proof provider (BROADCASTER: arc|arcade; proofs
    // always ride ARC → WoC → Bitails with ChainTracks as the canonical-chain authority).
    let provider = match build_provider(&env) {
        Ok(p) => p,
        Err(e) => {
            let response =
                Response::from_json(&serde_json::json!({ "error": e }))?.with_status(500);
            return Ok(add_cors_headers(response));
        }
    };
    // ZERO-CONF dev lever: when INTERNALIZE_ZERO_CONF=true, a freshly-internalized
    // deposit stays spendable even if the internalize-time broadcast hits a transient
    // ServiceError — removing the ~1-block wait for the monitor to reconcile spendable.
    let internalize_zero_conf = env
        .var("INTERNALIZE_ZERO_CONF")
        .ok()
        .map(|v| {
            matches!(
                v.to_string().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false);

    // NL-7: the bucket a reference to a BEEF at rest may name. The binding
    // `BEEF_AT_REST` and `BEEF_AT_REST_BUCKET`, the name of the bucket bound
    // there (a binding does not say it); either missing, the reference form is
    // refused and the inline form is unchanged.
    let at_rest_binding = env.bucket("BEEF_AT_REST").ok();
    let at_rest = match (
        at_rest_binding.as_ref(),
        env_value(&env, "BEEF_AT_REST_BUCKET").filter(|s| !s.is_empty()),
    ) {
        (Some(bucket), Some(name)) => Some(crate::beef_at_rest::AtRestBucket { bucket, name }),
        _ => None,
    };

    let mut storage = StorageD1::new(&db, &blobs, &provider)
        .with_beef_verification(beef_mode, &header_provider)
        .with_internalize_zero_conf(internalize_zero_conf);
    if let Some(at_rest) = at_rest.as_ref() {
        storage = storage.with_beef_at_rest(at_rest);
    }

    // Build auth ID from BRC-31 context
    let auth = AuthId::new(&auth_context.identity_key);

    // Dispatch
    let result = dispatch::dispatch(
        &mut storage,
        &rpc_request.method,
        rpc_request.params,
        rpc_request.id,
        Some(&auth),
    )
    .await;

    // Return signed JSON-RPC response
    sign_json_response(&result, 200, &[], &session).map_err(|e| Error::from(e.to_string()))
}

#[event(scheduled)]
pub async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    let db = match env.d1("DB") {
        Ok(db) => db,
        Err(e) => {
            console_error!("Monitor: failed to get DB binding: {}", e);
            return;
        }
    };
    let blobs = match env.bucket("BLOBS") {
        Ok(b) => b,
        Err(e) => {
            console_error!("Monitor: failed to get BLOBS binding: {}", e);
            return;
        }
    };

    let provider = match build_provider(&env) {
        Ok(p) => p,
        Err(e) => {
            // A BROADCASTER typo must not silently run the monitor with a guessed
            // broadcaster — refuse the run loudly; /health shows the misconfiguration.
            console_error!("Monitor: refusing to run — {}", e);
            return;
        }
    };
    let headers = build_monitor_headers(&env);
    let result = monitor::run_monitor(&db, &blobs, &provider, &provider, &headers).await;

    console_log!(
        "Monitor: {} sent, {} send errors, {} proofs found, {} checked, {} abandoned failed, {} status synced, {} beef compacted, {} unfail recovered, {} purged, {} nosend found, reorg={} depth={} reverified={}, ext_spends {}/{} scanned/found, deep_sweep {}/{}/{} checked/mismatched/repaired, {} errors",
        result.sent,
        result.send_errors,
        result.proofs_found,
        result.proofs_checked,
        result.abandoned_failed,
        result.status_synced,
        result.beef_compacted,
        result.unfail_recovered,
        result.purged,
        result.nosend_found,
        result.reorg_detected,
        result.reorg_depth,
        result.proofs_reverified,
        result.ext_spends_scanned,
        result.ext_spends_found,
        result.deep_sweep_checked,
        result.deep_sweep_mismatched,
        result.deep_sweep_repaired,
        result.errors.len()
    );
    for err in &result.errors {
        console_error!("Monitor error: {}", err);
    }
}

#[cfg(test)]
mod tests {
    use super::auth_fault_parts;
    use bsv_middleware_cloudflare::AuthCloudflareError;

    fn parts(e: AuthCloudflareError) -> (u16, serde_json::Value) {
        let (status, body) = auth_fault_parts(&e);
        (
            status,
            serde_json::from_str(&body).expect("error body is JSON"),
        )
    }

    /// The source of `main` between two markers.
    fn main_src(from: &str, to: &str) -> &'static str {
        let src = include_str!("lib.rs");
        let start = src.find(from).expect("start marker");
        let len = src[start..].find(to).expect("end marker");
        &src[start..start + len]
    }

    /// A refusal is the middleware's own 401 answer (`UNAUTHORIZED`,
    /// `ERR_SESSION_NOT_FOUND`, `ERR_INVALID_AUTH`, `ERR_REPLAYED_REQUEST`, CORS set by
    /// the middleware): `AuthResult::Response` must reach the client untouched, and the
    /// fault arm must build no 401 of its own. `worker::Response` needs the JS runtime,
    /// so this holds the code shape; the bodies are checked live in
    /// `tests/e2e/auth_refusals.sh`.
    #[test]
    fn the_middlewares_401_reaches_the_client_unchanged() {
        let gate = main_src(
            "let auth_result = match process_auth(",
            "// Require session for response signing",
        );
        assert!(gate.contains("AuthResult::Response(response) => return Ok(response),"));
        let fault_arm =
            &gate[gate.find("Err(e) =>").unwrap()..gate.find("let (auth_context").unwrap()];
        assert!(!fault_arm.contains("401"), "{fault_arm}");
        assert!(!fault_arm.contains("Unauthorized"), "{fault_arm}");
        assert!(!fault_arm.contains("InvalidAuthentication"), "{fault_arm}");
        assert!(!fault_arm.contains("SessionNotFound"), "{fault_arm}");
    }

    /// Infrastructure failures stay 5xx: a KV outage must not tell every client
    /// to re-handshake into the same outage.
    #[test]
    fn infrastructure_errors_stay_500() {
        for e in [
            AuthCloudflareError::KvError("429".into()),
            AuthCloudflareError::SdkError("x".into()),
            AuthCloudflareError::TransportError("t".into()),
            AuthCloudflareError::ConfigError("bad key".into()),
        ] {
            let (status, body) = parts(e);
            assert_eq!(status, 500);
            assert_eq!(body["status"], "error");
        }
    }

    /// Unreadable JSON (the crate's `From<serde_json::Error>`) keeps the crate's 400.
    #[test]
    fn unreadable_json_is_400() {
        let (status, body) = parts(AuthCloudflareError::SerializationError("eof".into()));
        assert_eq!(status, 400);
        assert_eq!(body["status"], "error");
    }
}
