//! ARC broadcast and proof provider (Phase 2b).
//!
//! Implements `BroadcastService` and `ProofService` using the ARC REST API
//! with round-robin failover across multiple endpoints.

use super::{BroadcastError, BroadcastResult, BroadcastService, ProofResult, ProofService};
use serde::Deserialize;

// =============================================================================
// Constants
// =============================================================================

const ARC_ENDPOINTS: &[&str] = &["https://arc.taal.com", "https://arc.gorillapool.io"];

// =============================================================================
// ARC API response types
// =============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ArcBroadcastResponse {
    txid: Option<String>,
    tx_status: Option<String>,
    extra_info: Option<String>,
    block_hash: Option<String>,
    block_height: Option<u64>,
    merkle_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ArcTxStatusResponse {
    txid: Option<String>,
    tx_status: Option<String>,
    merkle_path: Option<String>,
    block_height: Option<u64>,
    block_hash: Option<String>,
}

// =============================================================================
// TX Status Classification
// =============================================================================

/// Statuses that indicate the broadcast succeeded. `STORED` is included because
/// ARC has persisted the tx — per canonical wallet-toolbox behavior (TS
/// `ARC.ts:290-306` and bsv-wallet-toolbox-rs `arc.rs:299-302`), a persisted tx
/// will propagate and treating it as success avoids the phantom cascade caused
/// by forcing retry when ARC is merely slow to flip to `SEEN_ON_NETWORK`.
const SEEN_STATUSES: &[&str] = &["SEEN_ON_NETWORK", "STORED", "MINED"];

/// Statuses that indicate the tx was accepted but not yet confirmed on the network.
const ACCEPTED_STATUSES: &[&str] = &[
    "SENT_TO_NETWORK",
    "ACCEPTED_BY_NETWORK",
    "REQUESTED_BY_NETWORK",
    "ANNOUNCED_TO_NETWORK",
    "RECEIVED",
    "QUEUED",
];

/// Statuses that indicate a double-spend (permanent).
const DOUBLE_SPEND_STATUSES: &[&str] = &["DOUBLE_SPEND_ATTEMPTED"];

/// Statuses that indicate a definitive rejection (permanent, not double-spend).
const REJECTED_STATUSES: &[&str] = &["REJECTED"];

/// Classify an ARC tx_status string into a broadcast outcome.
fn classify_tx_status(status: &str) -> TxStatusClass {
    let upper = status.to_uppercase();
    if SEEN_STATUSES.iter().any(|s| *s == upper) {
        TxStatusClass::SeenOnNetwork
    } else if ACCEPTED_STATUSES.iter().any(|s| *s == upper) {
        TxStatusClass::Accepted
    } else if DOUBLE_SPEND_STATUSES.iter().any(|s| *s == upper) {
        TxStatusClass::DoubleSpend
    } else if REJECTED_STATUSES.iter().any(|s| *s == upper) {
        TxStatusClass::Rejected
    } else {
        TxStatusClass::Accepted
    }
}

#[derive(Debug, PartialEq)]
enum TxStatusClass {
    SeenOnNetwork,
    Accepted,
    DoubleSpend,
    Rejected,
}

/// Check if a response body contains "already known" patterns.
fn is_already_known(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("txn-already-known") || lower.contains("already in mempool")
}

/// Check if the response body contains double-spend indicators.
fn is_double_spend_body(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("double spend")
        || lower.contains("double-spend")
        || lower.contains("txn-mempool-conflict")
        || lower.contains("missing inputs")
        || lower.contains("already spent")
}

/// Check if the response body contains invalid-tx indicators.
fn is_invalid_tx_body(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("mandatory-script-verify-flag-failed")
        || lower.contains("bad-txns-inputs-missingorspent")
        || lower.contains("non-mandatory-script-verify-flag")
        || lower.contains("scriptnumber overflow")
        || lower.contains("tx-size-small")
        || lower.contains("bad-txns-oversize")
        || lower.contains("bad-txns-vout-negative")
        || lower.contains("bad-txns-txouttotal-toolarge")
        || lower.contains("bad-txns-inputs-duplicate")
        || lower.contains("bad-txns-prevout-null")
}

// =============================================================================
// ArcProvider
// =============================================================================

pub struct ArcProvider {
    pub(crate) api_key: Option<String>,
}

impl ArcProvider {
    pub fn new(api_key: Option<String>) -> Self {
        Self { api_key }
    }

    fn build_headers(&self) -> worker::Headers {
        let headers = worker::Headers::new();
        let _ = headers.set("Content-Type", "application/json");
        let _ = headers.set("X-WaitFor", "SEEN_ON_NETWORK");
        let _ = headers.set("X-MaxTimeout", "15");
        if let Some(ref key) = self.api_key {
            let _ = headers.set("Authorization", &format!("Bearer {}", key));
        }
        headers
    }

    async fn post_tx(
        &self,
        base_url: &str,
        json_body: &str,
    ) -> std::result::Result<(u16, String), String> {
        let url = format!("{}/v1/tx", base_url);
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);
        init.with_headers(self.build_headers());
        init.with_body(Some(wasm_bindgen::JsValue::from_str(json_body)));

        let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| format!("ARC fetch {}: {}", base_url, e))?;

        let status = response.status_code();
        let text = response.text().await.unwrap_or_default();
        Ok((status, text))
    }

    async fn get_tx_status(
        &self,
        base_url: &str,
        txid: &str,
    ) -> std::result::Result<(u16, String), String> {
        let url = format!("{}/v1/tx/{}", base_url, txid);
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        let headers = worker::Headers::new();
        if let Some(ref key) = self.api_key {
            let _ = headers.set("Authorization", &format!("Bearer {}", key));
        }
        init.with_headers(headers);

        let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| format!("ARC fetch {}: {}", base_url, e))?;

        let status = response.status_code();
        let text = response.text().await.unwrap_or_default();
        Ok((status, text))
    }
}

// =============================================================================
// Broadcast response processing (pure logic, testable without HTTP)
// =============================================================================

#[derive(Debug)]
enum BroadcastResponseError {
    DoubleSpend(String),
    InvalidTx(String),
    TryNext(String),
}

fn process_broadcast_response(
    status: u16,
    text: &str,
) -> std::result::Result<BroadcastResult, BroadcastResponseError> {
    if is_already_known(text) {
        return Ok(BroadcastResult {
            txid: String::new(),
            tx_status: "SEEN_ON_NETWORK".to_string(),
            seen_on_network: true,
        });
    }

    if status == 466 {
        return Err(BroadcastResponseError::DoubleSpend(format!(
            "ARC {} : {}",
            status,
            &text[..std::cmp::min(200, text.len())]
        )));
    }

    if matches!(status, 461..=463) {
        if is_double_spend_body(text) {
            return Err(BroadcastResponseError::DoubleSpend(format!(
                "ARC {} : {}",
                status,
                &text[..std::cmp::min(200, text.len())]
            )));
        }
        return Err(BroadcastResponseError::InvalidTx(format!(
            "ARC {} : {}",
            status,
            &text[..std::cmp::min(200, text.len())]
        )));
    }

    if (400..500).contains(&status) {
        if is_double_spend_body(text) {
            return Err(BroadcastResponseError::DoubleSpend(format!(
                "ARC {} : {}",
                status,
                &text[..std::cmp::min(200, text.len())]
            )));
        }
        if is_invalid_tx_body(text) {
            return Err(BroadcastResponseError::InvalidTx(format!(
                "ARC {} : {}",
                status,
                &text[..std::cmp::min(200, text.len())]
            )));
        }
        return Err(BroadcastResponseError::TryNext(format!(
            "ARC {} : {}",
            status,
            &text[..std::cmp::min(200, text.len())]
        )));
    }

    if status >= 500 {
        return Err(BroadcastResponseError::TryNext(format!(
            "ARC {} : {}",
            status,
            &text[..std::cmp::min(200, text.len())]
        )));
    }

    let resp: ArcBroadcastResponse = serde_json::from_str(text)
        .map_err(|e| BroadcastResponseError::TryNext(format!("ARC response parse error: {}", e)))?;

    let tx_status_str = resp.tx_status.unwrap_or_default();
    let classification = classify_tx_status(&tx_status_str);

    match classification {
        TxStatusClass::DoubleSpend => Err(BroadcastResponseError::DoubleSpend(format!(
            "ARC tx double-spend: {}",
            tx_status_str
        ))),
        TxStatusClass::Rejected => Err(BroadcastResponseError::InvalidTx(format!(
            "ARC tx rejected: {}",
            tx_status_str
        ))),
        TxStatusClass::SeenOnNetwork => Ok(BroadcastResult {
            txid: resp.txid.unwrap_or_default(),
            tx_status: tx_status_str,
            seen_on_network: true,
        }),
        // ARC only reaches here after X-WaitFor: SEEN_ON_NETWORK + X-MaxTimeout: 15.
        // If the tx is still merely Accepted (QUEUED / ANNOUNCED / RECEIVED / etc.)
        // after that window, it hasn't actually propagated — telling the caller
        // "success" turns into a zombie tx that never mines. Treat as retry-worthy;
        // if all endpoints return Accepted, process_action's ServiceError path
        // will cleanly mark the tx failed and revert the locked inputs.
        TxStatusClass::Accepted => Err(BroadcastResponseError::TryNext(format!(
            "ARC tx accepted but not SEEN_ON_NETWORK within waitFor window: {}",
            tx_status_str
        ))),
    }
}

fn process_proof_response(
    txid: &str,
    http_status: u16,
    text: &str,
) -> std::result::Result<Option<ProofResult>, String> {
    if http_status == 404 {
        return Ok(None);
    }
    if http_status >= 400 {
        return Err(format!(
            "ARC proof API error {}: {}",
            http_status,
            &text[..std::cmp::min(200, text.len())]
        ));
    }

    let resp: ArcTxStatusResponse =
        serde_json::from_str(text).map_err(|e| format!("ARC proof parse error: {}", e))?;

    let tx_status = resp.tx_status.unwrap_or_default();
    if tx_status.to_uppercase() != "MINED" {
        return Ok(None);
    }

    let merkle_path_hex = match resp.merkle_path {
        Some(ref mp) if !mp.is_empty() => mp,
        _ => return Ok(None),
    };

    let merkle_path_binary = hex::decode(merkle_path_hex)
        .map_err(|e| format!("ARC merklePath hex decode error: {}", e))?;

    let block_height = resp.block_height.unwrap_or(0) as u32;

    Ok(Some(ProofResult {
        txid: txid.to_string(),
        merkle_path_binary,
        block_height,
        block_hash: resp.block_hash.unwrap_or_default(),
        merkle_root: String::new(),
    }))
}

// =============================================================================
// BroadcastService
// =============================================================================

/// Broadcast to all ARC endpoints in parallel. First definitive result wins:
///   - First success → return Ok immediately (other future is dropped).
///   - First DoubleSpend / InvalidTx → return immediately (deterministic
///     property of the tx, applies regardless of endpoint).
///   - TryNext / transport error → wait for the other endpoint.
///   - All failed → ServiceError with all collected reasons.
///
/// Why parallel: TAAL ARC frequently waits 10-15s for SEEN_ON_NETWORK
/// confirmation. GorillaPool sees propagation independently. Racing them
/// drops user-perceived latency to the faster endpoint with no semantic
/// change — both are still asked to wait for SEEN_ON_NETWORK.
async fn arc_broadcast_with_failover(
    provider: &ArcProvider,
    json_body: &str,
) -> std::result::Result<BroadcastResult, BroadcastError> {
    use futures_util::stream::{FuturesUnordered, StreamExt};

    let race_t0 = js_sys::Date::now();
    let mut in_flight: FuturesUnordered<_> = ARC_ENDPOINTS
        .iter()
        .map(|&base_url| async move {
            let host = base_url.strip_prefix("https://").unwrap_or(base_url);
            let t0 = js_sys::Date::now();
            let post_result = provider.post_tx(base_url, json_body).await;
            let post_ms = js_sys::Date::now() - t0;
            (host, post_ms, post_result)
        })
        .collect();

    let mut errors: Vec<String> = Vec::new();
    while let Some((host, post_ms, post_result)) = in_flight.next().await {
        match post_result {
            Ok((status, text)) => {
                worker::console_log!(
                    "BENCH arc.post_tx[{},http={}]: {:.0} ms",
                    host,
                    status,
                    post_ms
                );
                match process_broadcast_response(status, &text) {
                    Ok(result) => {
                        worker::console_log!(
                            "BENCH arc.race_winner[{}]: {:.0} ms (race total)",
                            host,
                            js_sys::Date::now() - race_t0
                        );
                        return Ok(result);
                    }
                    Err(BroadcastResponseError::DoubleSpend(msg)) => {
                        return Err(BroadcastError::DoubleSpend(msg));
                    }
                    Err(BroadcastResponseError::InvalidTx(msg)) => {
                        return Err(BroadcastError::InvalidTx(msg));
                    }
                    Err(BroadcastResponseError::TryNext(msg)) => {
                        worker::console_log!(
                            "BENCH arc.post_tx[{}]: try_next ({})",
                            host,
                            &msg[..msg.len().min(80)]
                        );
                        errors.push(format!("{}: {}", host, msg));
                    }
                }
            }
            Err(e) => {
                worker::console_log!(
                    "BENCH arc.post_tx[{},err]: {:.0} ms ({})",
                    host,
                    post_ms,
                    e
                );
                errors.push(format!("{}: {}", host, e));
            }
        }
    }

    Err(BroadcastError::ServiceError(format!(
        "All ARC endpoints failed: {}",
        errors.join("; ")
    )))
}

impl BroadcastService for ArcProvider {
    async fn broadcast_raw_tx(
        &self,
        raw_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        let body = serde_json::json!({ "rawTx": raw_hex }).to_string();
        arc_broadcast_with_failover(self, &body).await
    }

    async fn broadcast_beef(
        &self,
        beef_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        let body = serde_json::json!({ "rawTx": beef_hex }).to_string();
        arc_broadcast_with_failover(self, &body).await
    }
}

impl ProofService for ArcProvider {
    async fn get_chain_height(&self) -> std::result::Result<u32, String> {
        Err("ARC does not provide chain height".to_string())
    }

    async fn get_proof(&self, txid: &str) -> std::result::Result<Option<ProofResult>, String> {
        let mut last_error = String::new();

        for base_url in ARC_ENDPOINTS {
            let (status, text) = match self.get_tx_status(base_url, txid).await {
                Ok(r) => r,
                Err(e) => {
                    last_error = e;
                    continue;
                }
            };

            match process_proof_response(txid, status, &text) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = e;
                    continue;
                }
            }
        }

        Err(format!(
            "All ARC endpoints failed for proof: {}",
            last_error
        ))
    }

    async fn get_raw_tx(&self, _txid: &str) -> std::result::Result<Option<Vec<u8>>, String> {
        Ok(None)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_seen_on_network() {
        assert_eq!(
            classify_tx_status("SEEN_ON_NETWORK"),
            TxStatusClass::SeenOnNetwork
        );
        assert_eq!(classify_tx_status("MINED"), TxStatusClass::SeenOnNetwork);
        // STORED counts as seen — canonical wallet-toolbox behavior
        // (TS ARC.ts:290-306, bsv-wallet-toolbox-rs arc.rs:299-302).
        assert_eq!(classify_tx_status("STORED"), TxStatusClass::SeenOnNetwork);
    }

    #[test]
    fn test_classify_accepted() {
        assert_eq!(
            classify_tx_status("SENT_TO_NETWORK"),
            TxStatusClass::Accepted
        );
        assert_eq!(classify_tx_status("QUEUED"), TxStatusClass::Accepted);
        assert_eq!(
            classify_tx_status("ANNOUNCED_TO_NETWORK"),
            TxStatusClass::Accepted
        );
    }

    #[test]
    fn test_classify_double_spend() {
        assert_eq!(
            classify_tx_status("DOUBLE_SPEND_ATTEMPTED"),
            TxStatusClass::DoubleSpend
        );
    }

    #[test]
    fn test_classify_rejected() {
        assert_eq!(classify_tx_status("REJECTED"), TxStatusClass::Rejected);
    }

    #[test]
    fn test_classify_unknown_is_accepted() {
        assert_eq!(classify_tx_status(""), TxStatusClass::Accepted);
    }

    #[test]
    fn test_already_known_patterns() {
        assert!(is_already_known("txn-already-known"));
        assert!(is_already_known("already in mempool"));
    }

    #[test]
    fn test_not_already_known() {
        assert!(!is_already_known("double spend"));
        assert!(!is_already_known(""));
    }

    #[test]
    fn test_double_spend_body_patterns() {
        assert!(is_double_spend_body("double spend detected"));
        assert!(is_double_spend_body("txn-mempool-conflict"));
        assert!(is_double_spend_body("missing inputs for tx"));
    }

    #[test]
    fn test_invalid_tx_body_patterns() {
        assert!(is_invalid_tx_body("mandatory-script-verify-flag-failed"));
        assert!(is_invalid_tx_body("bad-txns-inputs-missingorspent"));
    }

    #[test]
    fn test_broadcast_success_seen_on_network() {
        let body = r#"{"txid":"abc123","txStatus":"SEEN_ON_NETWORK"}"#;
        let result = process_broadcast_response(200, body).unwrap();
        assert!(result.seen_on_network);
    }

    #[test]
    fn test_broadcast_double_spend_status() {
        let body = r#"{"txid":"abc123","txStatus":"DOUBLE_SPEND_ATTEMPTED"}"#;
        assert!(matches!(
            process_broadcast_response(200, body),
            Err(BroadcastResponseError::DoubleSpend(_))
        ));
    }

    #[test]
    fn test_broadcast_http_466_double_spend() {
        assert!(matches!(
            process_broadcast_response(466, "conflict"),
            Err(BroadcastResponseError::DoubleSpend(_))
        ));
    }

    #[test]
    fn test_broadcast_rejected_status() {
        let body = r#"{"txid":"abc123","txStatus":"REJECTED"}"#;
        assert!(matches!(
            process_broadcast_response(200, body),
            Err(BroadcastResponseError::InvalidTx(_))
        ));
    }

    #[test]
    fn test_broadcast_http_462_invalid_tx() {
        assert!(matches!(
            process_broadcast_response(462, "script error"),
            Err(BroadcastResponseError::InvalidTx(_))
        ));
    }

    #[test]
    fn test_broadcast_http_500_try_next() {
        assert!(matches!(
            process_broadcast_response(500, "server error"),
            Err(BroadcastResponseError::TryNext(_))
        ));
    }

    #[test]
    fn test_broadcast_http_400_generic_try_next() {
        assert!(matches!(
            process_broadcast_response(400, "bad request"),
            Err(BroadcastResponseError::TryNext(_))
        ));
    }

    #[test]
    fn test_broadcast_already_known_at_400() {
        let result = process_broadcast_response(400, "txn-already-known").unwrap();
        assert!(result.seen_on_network);
    }

    #[test]
    fn test_broadcast_missing_fields_is_try_next() {
        // Empty response body → default (empty) tx_status → Accepted class → TryNext.
        // Previously treated as Ok(seen_on_network=false), which caused zombie txs
        // when ARC returned a QUEUED-like status without tx_status set. Now we
        // require an explicit SEEN_ON_NETWORK / MINED to call it a success.
        assert!(matches!(
            process_broadcast_response(200, r#"{}"#),
            Err(BroadcastResponseError::TryNext(_))
        ));
    }

    #[test]
    fn test_proof_mined_with_merkle_path() {
        let mp_hex = hex::encode(vec![1u8, 2, 3, 4, 5]);
        let body = format!(
            r#"{{"txid":"abc","txStatus":"MINED","merklePath":"{}","blockHeight":800123}}"#,
            mp_hex
        );
        let result = process_proof_response("abc", 200, &body).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_proof_404_returns_none() {
        assert!(process_proof_response("abc", 404, "").unwrap().is_none());
    }

    #[test]
    fn test_proof_500_returns_error() {
        assert!(process_proof_response("abc", 500, "err").is_err());
    }

    #[test]
    fn test_arc_provider_with_api_key() {
        let p = ArcProvider::new(Some("key".to_string()));
        assert_eq!(p.api_key, Some("key".to_string()));
    }

    #[test]
    fn test_broadcast_error_permanent() {
        assert!(BroadcastError::DoubleSpend("ds".into()).is_permanent());
        assert!(BroadcastError::InvalidTx("iv".into()).is_permanent());
        assert!(!BroadcastError::ServiceError("se".into()).is_permanent());
    }

    #[test]
    fn test_broadcast_error_display() {
        assert_eq!(
            format!("{}", BroadcastError::DoubleSpend("ds".into())),
            "Double-spend: ds"
        );
        assert_eq!(
            format!("{}", BroadcastError::InvalidTx("iv".into())),
            "Invalid tx: iv"
        );
        assert_eq!(
            format!("{}", BroadcastError::ServiceError("se".into())),
            "Service error: se"
        );
    }
}
