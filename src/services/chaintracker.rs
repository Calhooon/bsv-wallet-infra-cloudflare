//! Chain tracker implementations for BEEF verification.
//!
//! Provides the `HeaderService` trait for verifying merkle roots against
//! the blockchain. Two providers:
//!
//! - `WocChainTracker` — WhatsOnChain block header API (always available)
//! - `ChainTracksProvider` — ChainTracks header service (requires running instance)
//!
//! The bsv-sdk `ChainTracker` trait requires `Send + Sync`, which is incompatible
//! with Cloudflare Workers (worker::Fetch returns non-Send futures). So we define
//! our own `HeaderService` trait without Send bounds and verify roots manually
//! in `beef_verification.rs`.
//!
//! The active provider is selected at startup based on the `CHAINTRACKS_URL`
//! env var. If set, ChainTracks is primary with WoC fallback. If unset, WoC only.

use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;

// =============================================================================
// HeaderService trait
// =============================================================================

/// Service for verifying block headers against the blockchain.
///
/// This is our own trait (not bsv-sdk's `ChainTracker`) because Cloudflare
/// Workers' `worker::Fetch` returns non-Send futures, making it incompatible
/// with the `Send + Sync` bound on `ChainTracker`.
pub trait HeaderService {
    /// Check if a merkle root is valid for a given block height.
    ///
    /// Returns `Ok(true)` if the root matches, `Ok(false)` if it doesn't,
    /// or `Err` on network/parse failures.
    fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> impl std::future::Future<Output = std::result::Result<bool, String>>;
}

// =============================================================================
// Constants
// =============================================================================

const WOC_BASE: &str = "https://api.whatsonchain.com/v1/bsv/main";

// =============================================================================
// Root cache — persists across requests in the same CF Worker isolate
// =============================================================================

/// In-memory cache for verified merkle roots (height -> root hex).
///
/// Matches Go toolbox's `rootCache map[uint32]*chainhash.Hash` pattern.
/// CF Workers are single-threaded, so RefCell is safe.
pub struct RootCache {
    inner: RefCell<HashMap<u32, String>>,
    max_entries: usize,
}

impl RootCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: RefCell::new(HashMap::new()),
            max_entries,
        }
    }

    /// Get cached merkle root for a block height.
    pub fn get(&self, height: u32) -> Option<String> {
        self.inner.borrow().get(&height).cloned()
    }

    /// Cache a verified merkle root. Evicts lowest height if at capacity.
    pub fn insert(&self, height: u32, root: String) {
        let mut map = self.inner.borrow_mut();
        if map.len() >= self.max_entries && !map.contains_key(&height) {
            // Evict the lowest height (oldest blocks are least likely to be re-queried)
            if let Some(&min_height) = map.keys().min() {
                map.remove(&min_height);
            }
        }
        map.insert(height, root);
    }

    pub fn len(&self) -> usize {
        self.inner.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }
}

// =============================================================================
// Retry configuration
// =============================================================================

/// Configuration for retry behavior on transient failures (429, 5xx, etc.).
/// Matches Go toolbox's `rootForHeightRetries` pattern.
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries).
    pub max_retries: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        // 2026-04-15: reduced from 5 → 1 after WoC blanket-429'd us.
        // With 5 retries × LIMIT 1000 × */2 cron we were making 30K+
        // WoC calls/hour when rate-limited, triggering a blacklist.
        // 1 retry still catches transient blips without amplifying
        // sustained rate-limits into call storms.
        Self { max_retries: 1 }
    }
}

impl RetryConfig {
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }

    /// Returns true if the given HTTP status code is retryable.
    ///
    /// BUG-005 FIX: `500 Internal Server Error` is now retryable. WhatsOnChain
    /// occasionally returns 500 on otherwise-valid block header lookups
    /// (observed 2026-04-12 during E2E handshake at height 944381). The error
    /// is transient — a few seconds later the same request succeeds. Leaving
    /// 500 out of this set caused the error to propagate up, failing the
    /// entire x402 payment verification and killing an in-flight LLM call.
    ///
    /// We retry on all 5xx plus 429 (rate limit). 4xx errors (other than 429)
    /// are NOT retried because they indicate client-side issues (bad request,
    /// not found, unauthorized) that retrying won't fix.
    pub fn is_retryable_status(status: u16) -> bool {
        matches!(status, 429 | 500 | 502 | 503 | 504)
    }
}

// =============================================================================
// WoC block header response
// =============================================================================

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct WocBlockHeaderByHeight {
    hash: Option<String>,
    height: Option<u32>,
    merkleroot: Option<String>,
}

// =============================================================================
// WocChainTracker
// =============================================================================

/// HeaderService backed by WhatsOnChain block header API.
///
/// Verifies merkle roots by fetching the block header at the given height
/// and comparing the merkle root. Uses Worker Fetch API (no TCP sockets).
///
/// Includes retry logic for transient failures (429 rate limits, 502/503/504).
/// CF Workers are single-threaded so retries are immediate (no delay) -- the
/// value is in hitting a different WoC backend on the next attempt.
pub struct WocChainTracker {
    retry: RetryConfig,
    /// Optional WoC API key sent as `Authorization` header to bypass anonymous
    /// rate limiting. Loaded from the `WOC_API_KEY` CF Worker secret.
    api_key: Option<String>,
}

impl Default for WocChainTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl WocChainTracker {
    pub fn new() -> Self {
        Self {
            retry: RetryConfig::default(),
            api_key: None,
        }
    }

    pub fn with_retries(max_retries: u32) -> Self {
        Self {
            retry: RetryConfig::new(max_retries),
            api_key: None,
        }
    }

    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key;
        self
    }
}

impl HeaderService for WocChainTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, String> {
        let url = format!("{}/block/height/{}", WOC_BASE, height);
        let mut last_err = String::new();

        for attempt in 0..=self.retry.max_retries {
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Get);
            if let Some(ref key) = self.api_key {
                let headers = worker::Headers::new();
                let _ = headers.set("woc-api-key", key);
                init.with_headers(headers);
            }

            let request = match worker::Request::new_with_init(&url, &init) {
                Ok(r) => r,
                Err(e) => return Err(format!("WoC request error: {}", e)),
            };

            let response = match worker::Fetch::Request(request).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("WoC chain tracker fetch: {}", e);
                    if attempt < self.retry.max_retries {
                        continue;
                    }
                    return Err(last_err);
                }
            };

            let status = response.status_code();
            if RetryConfig::is_retryable_status(status) && attempt < self.retry.max_retries {
                last_err = format!(
                    "WoC block header API error {} for height {} (attempt {}/{})",
                    status,
                    height,
                    attempt + 1,
                    self.retry.max_retries + 1
                );
                continue;
            }

            if status == 404 {
                return Err(format!("Block not found at height {}", height));
            }
            if status >= 400 {
                return Err(format!(
                    "WoC block header API error {} for height {}",
                    status, height
                ));
            }

            let mut response = response;
            let header: WocBlockHeaderByHeight = response
                .json()
                .await
                .map_err(|e| format!("Failed to parse WoC block header: {}", e))?;

            return match header.merkleroot {
                Some(ref mr) => Ok(mr.eq_ignore_ascii_case(root)),
                None => Err(format!(
                    "WoC block header at height {} missing merkleroot",
                    height
                )),
            };
        }

        Err(last_err)
    }
}

// =============================================================================
// ChainTracks response types
// =============================================================================

/// Wrapper for ChainTracks API responses: `{"status":"success","value":{...}}`
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ChainTracksApiResponse<T> {
    status: String,
    value: Option<T>,
    code: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ChainTracksHeaderResponse {
    merkle_root: Option<String>,
    merkleroot: Option<String>,
    height: Option<u32>,
}

impl ChainTracksHeaderResponse {
    /// Extract the merkle root from the response, handling both camelCase and
    /// lowercase field names that different ChainTracks versions may use.
    fn get_merkle_root(&self) -> Option<&str> {
        self.merkle_root.as_deref().or(self.merkleroot.as_deref())
    }
}

// =============================================================================
// ChainTracksProvider
// =============================================================================

/// HeaderService backed by a ChainTracks instance.
///
/// ChainTracks is a self-hosted header verification service that maintains
/// a complete chain of block headers. It is the preferred verification
/// method but requires a running instance (Issue #6).
///
/// API: `GET {base_url}/findHeaderHexForHeight?height={height}`
/// Response: `{"status":"success","value":{"merkleRoot":"...","height":...,...}}`
pub struct ChainTracksProvider {
    base_url: String,
}

impl ChainTracksProvider {
    pub fn new(base_url: String) -> Self {
        // Strip trailing slash for consistent URL construction
        let base_url = base_url.trim_end_matches('/').to_string();
        Self { base_url }
    }
}

impl HeaderService for ChainTracksProvider {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, String> {
        let url = format!("{}/findHeaderHexForHeight?height={}", self.base_url, height);
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);

        let request = worker::Request::new_with_init(&url, &init)
            .map_err(|e| format!("ChainTracks request error: {}", e))?;
        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| format!("ChainTracks fetch error for height {}: {}", height, e))?;

        let status = response.status_code();
        if status == 404 {
            return Err(format!("ChainTracks: block not found at height {}", height));
        }
        if status >= 400 {
            return Err(format!(
                "ChainTracks API error {} for height {}",
                status, height
            ));
        }

        let api_resp: ChainTracksApiResponse<ChainTracksHeaderResponse> = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse ChainTracks response: {}", e))?;

        if api_resp.status != "success" {
            return Err(format!(
                "ChainTracks API returned status '{}' for height {}",
                api_resp.status, height
            ));
        }

        let header = match api_resp.value {
            Some(h) => h,
            None => {
                return Err(format!("ChainTracks: no header found at height {}", height));
            }
        };

        match header.get_merkle_root() {
            Some(mr) => Ok(mr.eq_ignore_ascii_case(root)),
            None => Err("ChainTracks response missing merkleRoot field".to_string()),
        }
    }
}

// =============================================================================
// FallbackChainTracker
// =============================================================================

/// A header service that tries a primary provider first, falling back to WoC.
///
/// Used when ChainTracks is configured but may be unreliable. If the primary
/// provider returns an error, the fallback (WoC) is tried. Includes an
/// in-memory root cache to avoid redundant lookups.
pub struct FallbackChainTracker<P: HeaderService> {
    primary: P,
    fallback: WocChainTracker,
    cache: RootCache,
}

impl<P: HeaderService> FallbackChainTracker<P> {
    pub fn new(primary: P) -> Self {
        Self {
            primary,
            fallback: WocChainTracker::new(),
            cache: RootCache::new(1000),
        }
    }

    pub fn with_woc_api_key(mut self, api_key: Option<String>) -> Self {
        self.fallback = self.fallback.with_api_key(api_key);
        self
    }
}

impl<P: HeaderService> HeaderService for FallbackChainTracker<P> {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, String> {
        // Check cache first
        if let Some(cached_root) = self.cache.get(height) {
            return Ok(cached_root.eq_ignore_ascii_case(root));
        }

        // Try primary
        match self.primary.is_valid_root_for_height(root, height).await {
            Ok(valid) => {
                if valid {
                    self.cache.insert(height, root.to_lowercase());
                }
                Ok(valid)
            }
            Err(_primary_err) => {
                // Primary failed, try fallback
                let result = self.fallback.is_valid_root_for_height(root, height).await;
                // Cache successful fallback results too
                if let Ok(true) = &result {
                    self.cache.insert(height, root.to_lowercase());
                }
                result
            }
        }
    }
}

// =============================================================================
// Enum wrapper for dynamic dispatch
// =============================================================================

/// Runtime-selected header service provider.
///
/// We use an enum instead of `Box<dyn HeaderService>` because `HeaderService`
/// uses `impl Future` return types (not object-safe). The enum dispatches
/// to the correct implementation at runtime.
///
/// Both variants include a `RootCache` for avoiding redundant lookups.
pub enum HeaderProvider {
    /// WoC only with cache (no ChainTracks configured)
    Woc(WocChainTracker, RootCache),
    /// ChainTracks primary with WoC fallback and cache
    ChainTracksWithFallback(FallbackChainTracker<ChainTracksProvider>),
}

impl HeaderService for HeaderProvider {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, String> {
        match self {
            HeaderProvider::Woc(woc, cache) => {
                // Check cache first
                if let Some(cached_root) = cache.get(height) {
                    return Ok(cached_root.eq_ignore_ascii_case(root));
                }
                let result = woc.is_valid_root_for_height(root, height).await;
                if let Ok(true) = &result {
                    cache.insert(height, root.to_lowercase());
                }
                result
            }
            HeaderProvider::ChainTracksWithFallback(ct) => {
                ct.is_valid_root_for_height(root, height).await
            }
        }
    }
}

// =============================================================================
// Factory function
// =============================================================================

/// Build the appropriate header provider based on configuration.
///
/// - If `chaintracks_url` is provided, uses ChainTracks with WoC fallback.
/// - Otherwise, uses WoC directly.
///
/// Both paths include an in-memory root cache (up to 1000 entries).
pub fn build_header_provider(
    chaintracks_url: Option<String>,
    woc_api_key: Option<String>,
) -> HeaderProvider {
    match chaintracks_url {
        Some(url) if !url.is_empty() => {
            let primary = ChainTracksProvider::new(url);
            HeaderProvider::ChainTracksWithFallback(
                FallbackChainTracker::new(primary).with_woc_api_key(woc_api_key),
            )
        }
        _ => HeaderProvider::Woc(
            WocChainTracker::new().with_api_key(woc_api_key),
            RootCache::new(1000),
        ),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // RootCache tests
    // =========================================================================

    #[test]
    fn cache_starts_empty() {
        let cache = RootCache::new(100);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cache_insert_and_get() {
        let cache = RootCache::new(100);
        cache.insert(942949, "abc123".to_string());
        assert_eq!(cache.get(942949), Some("abc123".to_string()));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_miss_returns_none() {
        let cache = RootCache::new(100);
        assert_eq!(cache.get(999999), None);
    }

    #[test]
    fn cache_overwrites_same_height() {
        let cache = RootCache::new(100);
        cache.insert(100, "old".to_string());
        cache.insert(100, "new".to_string());
        assert_eq!(cache.get(100), Some("new".to_string()));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_evicts_lowest_height_when_full() {
        let cache = RootCache::new(3);
        cache.insert(300, "c".to_string());
        cache.insert(100, "a".to_string());
        cache.insert(200, "b".to_string());
        assert_eq!(cache.len(), 3);

        // Insert 4th -- should evict height 100 (lowest)
        cache.insert(400, "d".to_string());
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get(100), None); // evicted
        assert_eq!(cache.get(200), Some("b".to_string()));
        assert_eq!(cache.get(300), Some("c".to_string()));
        assert_eq!(cache.get(400), Some("d".to_string()));
    }

    #[test]
    fn cache_no_eviction_when_updating_existing() {
        let cache = RootCache::new(2);
        cache.insert(100, "a".to_string());
        cache.insert(200, "b".to_string());
        // Update existing -- should NOT evict
        cache.insert(100, "a_updated".to_string());
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(100), Some("a_updated".to_string()));
        assert_eq!(cache.get(200), Some("b".to_string()));
    }

    #[test]
    fn cache_large_capacity() {
        let cache = RootCache::new(10000);
        for i in 0..5000 {
            cache.insert(i, format!("root_{}", i));
        }
        assert_eq!(cache.len(), 5000);
        assert_eq!(cache.get(0), Some("root_0".to_string()));
        assert_eq!(cache.get(4999), Some("root_4999".to_string()));
    }

    #[test]
    fn cache_zero_capacity_always_evicts() {
        // Edge case: max_entries = 0 means every insert evicts immediately.
        // Actually, with 0 capacity the insert checks len >= max_entries (0 >= 0 = true)
        // and tries to evict, but there's nothing to evict on first insert, so it
        // still inserts. Then on second insert it evicts the first.
        let cache = RootCache::new(0);
        cache.insert(100, "a".to_string());
        // len is 1, but max is 0 -- next insert should evict
        cache.insert(200, "b".to_string());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(100), None);
        assert_eq!(cache.get(200), Some("b".to_string()));
    }

    #[test]
    fn cache_single_capacity() {
        let cache = RootCache::new(1);
        cache.insert(100, "a".to_string());
        assert_eq!(cache.len(), 1);
        cache.insert(200, "b".to_string());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(100), None);
        assert_eq!(cache.get(200), Some("b".to_string()));
    }

    // =========================================================================
    // RetryConfig tests
    // =========================================================================

    #[test]
    fn retry_config_default_is_1() {
        // 2026-04-15: reduced from 5 → 1 after WoC blanket-429'd us.
        // High retry counts amplify rate-limits into call storms.
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 1);
    }

    #[test]
    fn retry_config_custom() {
        let config = RetryConfig::new(5);
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn retry_config_zero() {
        let config = RetryConfig::new(0);
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn retryable_status_429() {
        assert!(RetryConfig::is_retryable_status(429));
    }

    #[test]
    fn retryable_status_500() {
        // BUG-005 FIX: WoC intermittently returns 500 on block header
        // lookups. This was causing x402 payment verification to fail
        // catastrophically instead of retrying. Must be retryable.
        assert!(RetryConfig::is_retryable_status(500));
    }

    #[test]
    fn retryable_status_502() {
        assert!(RetryConfig::is_retryable_status(502));
    }

    #[test]
    fn retryable_status_503() {
        assert!(RetryConfig::is_retryable_status(503));
    }

    #[test]
    fn retryable_status_504() {
        assert!(RetryConfig::is_retryable_status(504));
    }

    #[test]
    fn non_retryable_status_200() {
        assert!(!RetryConfig::is_retryable_status(200));
    }

    #[test]
    fn non_retryable_status_400() {
        assert!(!RetryConfig::is_retryable_status(400));
    }

    #[test]
    fn non_retryable_status_401() {
        assert!(!RetryConfig::is_retryable_status(401));
    }

    #[test]
    fn non_retryable_status_404() {
        // Block not found is NOT transient — don't retry.
        assert!(!RetryConfig::is_retryable_status(404));
    }

    #[test]
    fn non_retryable_status_403() {
        // Forbidden is NOT transient — retrying won't help.
        assert!(!RetryConfig::is_retryable_status(403));
    }

    #[test]
    fn all_5xx_retryable_except_501_and_505() {
        // Sanity: the 5xx codes we care about all retry.
        for status in [500, 502, 503, 504] {
            assert!(
                RetryConfig::is_retryable_status(status),
                "{status} should be retryable"
            );
        }
        // 501 (Not Implemented) and 505 (HTTP Version Not Supported) are
        // permanent failures — retrying is pointless.
        assert!(!RetryConfig::is_retryable_status(501));
        assert!(!RetryConfig::is_retryable_status(505));
    }

    // =========================================================================
    // WocChainTracker construction tests
    // =========================================================================

    #[test]
    fn woc_default_has_1_retry() {
        // 2026-04-15: reduced from 5 → 1 after WoC rate-limit incident.
        let woc = WocChainTracker::new();
        assert_eq!(woc.retry.max_retries, 1);
    }

    #[test]
    fn woc_with_custom_retries() {
        let woc = WocChainTracker::with_retries(5);
        assert_eq!(woc.retry.max_retries, 5);
    }

    #[test]
    fn woc_with_zero_retries() {
        let woc = WocChainTracker::with_retries(0);
        assert_eq!(woc.retry.max_retries, 0);
    }

    // =========================================================================
    // Existing tests (preserved)
    // =========================================================================

    #[test]
    fn test_chaintracks_provider_strips_trailing_slash() {
        let p = ChainTracksProvider::new("https://example.com/".to_string());
        assert_eq!(p.base_url, "https://example.com");
    }

    #[test]
    fn test_chaintracks_provider_no_trailing_slash() {
        let p = ChainTracksProvider::new("https://example.com".to_string());
        assert_eq!(p.base_url, "https://example.com");
    }

    #[test]
    fn test_chaintracks_response_merkle_root_camel_case() {
        let resp: ChainTracksHeaderResponse =
            serde_json::from_str(r#"{"merkleRoot": "abc123", "height": 800000}"#).unwrap();
        assert_eq!(resp.get_merkle_root(), Some("abc123"));
    }

    #[test]
    fn test_chaintracks_response_merkle_root_lowercase() {
        let resp: ChainTracksHeaderResponse =
            serde_json::from_str(r#"{"merkleroot": "def456", "height": 800001}"#).unwrap();
        assert_eq!(resp.get_merkle_root(), Some("def456"));
    }

    #[test]
    fn test_chaintracks_response_both_fields_prefers_camel_case() {
        let resp: ChainTracksHeaderResponse = serde_json::from_str(
            r#"{"merkleRoot": "camel", "merkleroot": "lower", "height": 800003}"#,
        )
        .unwrap();
        // camelCase (merkle_root) is checked first via serde rename
        assert_eq!(resp.get_merkle_root(), Some("camel"));
    }

    #[test]
    fn test_chaintracks_response_missing_merkle_root() {
        let resp: ChainTracksHeaderResponse =
            serde_json::from_str(r#"{"height": 800002}"#).unwrap();
        assert_eq!(resp.get_merkle_root(), None);
    }

    #[test]
    fn test_woc_block_header_deserialize() {
        let header: WocBlockHeaderByHeight = serde_json::from_str(
            r#"{"hash": "000abc", "height": 800000, "merkleroot": "root123"}"#,
        )
        .unwrap();
        assert_eq!(header.merkleroot, Some("root123".to_string()));
        assert_eq!(header.height, Some(800000));
    }

    #[test]
    fn test_build_header_provider_no_url() {
        let provider = build_header_provider(None, None);
        assert!(matches!(provider, HeaderProvider::Woc(..)));
    }

    #[test]
    fn test_build_header_provider_empty_url() {
        let provider = build_header_provider(Some(String::new()), None);
        assert!(matches!(provider, HeaderProvider::Woc(..)));
    }

    #[test]
    fn test_build_header_provider_with_url() {
        let provider = build_header_provider(Some("https://chaintracks.example.com".to_string()), None);
        assert!(matches!(
            provider,
            HeaderProvider::ChainTracksWithFallback(_)
        ));
    }

    #[test]
    fn test_chaintracks_api_response_wrapper_deserialize() {
        let json = r#"{
            "status": "success",
            "value": {
                "merkleRoot": "abc123def",
                "height": 800000
            }
        }"#;
        let resp: ChainTracksApiResponse<ChainTracksHeaderResponse> =
            serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "success");
        let header = resp.value.unwrap();
        assert_eq!(header.get_merkle_root(), Some("abc123def"));
        assert_eq!(header.height, Some(800000));
    }

    #[test]
    fn test_chaintracks_api_response_null_value() {
        let json = r#"{"status": "success", "value": null}"#;
        let resp: ChainTracksApiResponse<ChainTracksHeaderResponse> =
            serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "success");
        assert!(resp.value.is_none());
    }

    #[test]
    fn test_chaintracks_api_response_error() {
        let json = r#"{
            "status": "error",
            "code": "ERR_INTERNAL",
            "description": "Something went wrong"
        }"#;
        let resp: ChainTracksApiResponse<ChainTracksHeaderResponse> =
            serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "error");
        assert_eq!(resp.code.as_deref(), Some("ERR_INTERNAL"));
        assert!(resp.value.is_none());
    }

    #[test]
    fn test_merkle_root_case_insensitive_comparison() {
        // Simulate what the code does: eq_ignore_ascii_case
        let lower = "abc123def456";
        let upper = "ABC123DEF456";
        let mixed = "Abc123Def456";
        assert!(lower.eq_ignore_ascii_case(upper));
        assert!(lower.eq_ignore_ascii_case(mixed));
        assert!(upper.eq_ignore_ascii_case(mixed));
    }

    // =========================================================================
    // Cache integration with FallbackChainTracker (mock-based)
    // =========================================================================

    /// A mock HeaderService that always succeeds with a configurable root.
    struct MockHeaderService {
        root: String,
        call_count: RefCell<u32>,
    }

    impl MockHeaderService {
        fn new(root: &str) -> Self {
            Self {
                root: root.to_string(),
                call_count: RefCell::new(0),
            }
        }

        fn calls(&self) -> u32 {
            *self.call_count.borrow()
        }
    }

    impl HeaderService for MockHeaderService {
        async fn is_valid_root_for_height(
            &self,
            root: &str,
            _height: u32,
        ) -> std::result::Result<bool, String> {
            *self.call_count.borrow_mut() += 1;
            Ok(self.root.eq_ignore_ascii_case(root))
        }
    }

    #[tokio::test]
    async fn fallback_cache_hit_skips_providers() {
        let fallback = FallbackChainTracker {
            primary: MockHeaderService::new("abc123"),
            fallback: WocChainTracker::with_retries(0),
            cache: RootCache::new(100),
        };

        // Pre-populate cache
        fallback.cache.insert(800000, "abc123".to_string());

        // Should hit cache, not call primary
        let result = fallback
            .is_valid_root_for_height("ABC123", 800000)
            .await
            .unwrap();
        assert!(result);
        assert_eq!(fallback.primary.calls(), 0);
    }

    #[tokio::test]
    async fn fallback_cache_miss_calls_primary() {
        let fallback = FallbackChainTracker {
            primary: MockHeaderService::new("abc123"),
            fallback: WocChainTracker::with_retries(0),
            cache: RootCache::new(100),
        };

        let result = fallback
            .is_valid_root_for_height("abc123", 800000)
            .await
            .unwrap();
        assert!(result);
        assert_eq!(fallback.primary.calls(), 1);

        // Should now be cached
        assert_eq!(fallback.cache.get(800000), Some("abc123".to_string()));
    }

    #[tokio::test]
    async fn fallback_caches_on_valid_root() {
        let fallback = FallbackChainTracker {
            primary: MockHeaderService::new("deadbeef"),
            fallback: WocChainTracker::with_retries(0),
            cache: RootCache::new(100),
        };

        let result = fallback
            .is_valid_root_for_height("DEADBEEF", 900000)
            .await
            .unwrap();
        assert!(result);

        // Cache stores lowercase
        assert_eq!(fallback.cache.get(900000), Some("deadbeef".to_string()));
    }

    #[tokio::test]
    async fn fallback_does_not_cache_invalid_root() {
        let fallback = FallbackChainTracker {
            primary: MockHeaderService::new("abc123"),
            fallback: WocChainTracker::with_retries(0),
            cache: RootCache::new(100),
        };

        let result = fallback
            .is_valid_root_for_height("wrong_root", 800000)
            .await
            .unwrap();
        assert!(!result);

        // Should NOT be cached since the root didn't match
        assert!(fallback.cache.is_empty());
    }

    #[tokio::test]
    async fn fallback_second_lookup_uses_cache() {
        let fallback = FallbackChainTracker {
            primary: MockHeaderService::new("abc123"),
            fallback: WocChainTracker::with_retries(0),
            cache: RootCache::new(100),
        };

        // First call -- hits primary
        let _ = fallback
            .is_valid_root_for_height("abc123", 800000)
            .await
            .unwrap();
        assert_eq!(fallback.primary.calls(), 1);

        // Second call -- should hit cache
        let result = fallback
            .is_valid_root_for_height("ABC123", 800000)
            .await
            .unwrap();
        assert!(result);
        assert_eq!(fallback.primary.calls(), 1); // still 1, no new call
    }

    // =========================================================================
    // BUG-005: Primary error → fallback path regression tests
    // =========================================================================
    //
    // These tests cover a real failure mode observed in production: the
    // primary ChainTracks endpoint returned an error (200-OK with empty
    // `value` at recent heights — semantically a failure), and the WoC
    // fallback then 500'd. We need to guarantee:
    //
    //   1. When primary errors, the fallback is still consulted
    //   2. The fallback's result is returned (even if primary failed)
    //   3. Primary error does NOT cause a short-circuit failure
    //   4. Successful fallback results ARE cached (prevents repeated WoC hits)

    /// A mock that always returns an error — simulates an upstream that's
    /// up but broken (200 success with empty value, or partial response).
    struct ErrorHeaderService {
        err_msg: String,
        call_count: RefCell<u32>,
    }

    impl ErrorHeaderService {
        fn new(err: &str) -> Self {
            Self {
                err_msg: err.to_string(),
                call_count: RefCell::new(0),
            }
        }

        fn calls(&self) -> u32 {
            *self.call_count.borrow()
        }
    }

    impl HeaderService for ErrorHeaderService {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, String> {
            *self.call_count.borrow_mut() += 1;
            Err(self.err_msg.clone())
        }
    }

    /// A mock fallback that returns a valid result (simulates WoC being healthy).
    /// Unlike `MockHeaderService`, this one can be wired in as the fallback slot.
    struct HealthyFallbackService {
        root: String,
        call_count: RefCell<u32>,
    }

    impl HealthyFallbackService {
        fn new(root: &str) -> Self {
            Self {
                root: root.to_string(),
                call_count: RefCell::new(0),
            }
        }

        fn calls(&self) -> u32 {
            *self.call_count.borrow()
        }
    }

    impl HeaderService for HealthyFallbackService {
        async fn is_valid_root_for_height(
            &self,
            root: &str,
            _height: u32,
        ) -> std::result::Result<bool, String> {
            *self.call_count.borrow_mut() += 1;
            Ok(self.root.eq_ignore_ascii_case(root))
        }
    }

    /// Generic two-service fallback chain (primary + custom fallback type).
    /// We use this instead of `FallbackChainTracker` because that type
    /// hardcodes `WocChainTracker` as the fallback, which can't be mocked.
    struct GenericFallback<P: HeaderService, F: HeaderService> {
        primary: P,
        fallback: F,
        cache: RootCache,
    }

    impl<P: HeaderService, F: HeaderService> HeaderService for GenericFallback<P, F> {
        async fn is_valid_root_for_height(
            &self,
            root: &str,
            height: u32,
        ) -> std::result::Result<bool, String> {
            if let Some(cached) = self.cache.get(height) {
                return Ok(cached.eq_ignore_ascii_case(root));
            }
            match self.primary.is_valid_root_for_height(root, height).await {
                Ok(valid) => {
                    if valid {
                        self.cache.insert(height, root.to_lowercase());
                    }
                    Ok(valid)
                }
                Err(_) => {
                    let result = self.fallback.is_valid_root_for_height(root, height).await;
                    if let Ok(true) = &result {
                        self.cache.insert(height, root.to_lowercase());
                    }
                    result
                }
            }
        }
    }

    #[tokio::test]
    async fn bug005_primary_error_falls_through_to_fallback() {
        // This is the exact scenario that caused the 2026-04-12 outage:
        // ChainTracks primary returns an error, and the verification
        // MUST continue to the fallback rather than failing hard.
        let fb = GenericFallback {
            primary: ErrorHeaderService::new("ChainTracks: no header found at height 944381"),
            fallback: HealthyFallbackService::new("deadbeef"),
            cache: RootCache::new(100),
        };

        let result = fb.is_valid_root_for_height("deadbeef", 944381).await;
        assert_eq!(result, Ok(true), "fallback result must be returned");
        assert_eq!(fb.primary.calls(), 1, "primary must be tried once");
        assert_eq!(
            fb.fallback.calls(),
            1,
            "fallback must be tried after primary error"
        );
    }

    #[tokio::test]
    async fn bug005_primary_error_fallback_mismatch_returns_false() {
        // If primary errors AND fallback returns a mismatch, we should get
        // Ok(false) — not an error. The verification layer will reject the
        // BEEF as invalid, but the process doesn't crash.
        let fb = GenericFallback {
            primary: ErrorHeaderService::new("primary down"),
            fallback: HealthyFallbackService::new("expected_root"),
            cache: RootCache::new(100),
        };

        let result = fb.is_valid_root_for_height("wrong_root", 944381).await;
        assert_eq!(result, Ok(false));
    }

    #[tokio::test]
    async fn bug005_successful_fallback_is_cached() {
        // After a primary-error → fallback-success, the result should be
        // cached so subsequent calls skip both providers. This prevents
        // repeated hammering on WoC during an outage of the primary.
        let fb = GenericFallback {
            primary: ErrorHeaderService::new("primary down"),
            fallback: HealthyFallbackService::new("deadbeef"),
            cache: RootCache::new(100),
        };

        // First call: primary errors, fallback succeeds
        fb.is_valid_root_for_height("deadbeef", 944381)
            .await
            .unwrap();
        assert_eq!(fb.primary.calls(), 1);
        assert_eq!(fb.fallback.calls(), 1);

        // Second call: should hit cache, skip BOTH providers
        fb.is_valid_root_for_height("DEADBEEF", 944381)
            .await
            .unwrap();
        assert_eq!(fb.primary.calls(), 1, "primary must not be called again");
        assert_eq!(fb.fallback.calls(), 1, "fallback must not be called again");
    }

    #[tokio::test]
    async fn bug005_cache_hit_before_primary_even_when_primary_is_broken() {
        // If the cache has a valid entry, neither primary nor fallback
        // should be consulted. This is critical during upstream outages:
        // once we've verified a root, we should never re-verify it.
        let fb = GenericFallback {
            primary: ErrorHeaderService::new("primary always fails"),
            fallback: HealthyFallbackService::new("unused"),
            cache: RootCache::new(100),
        };
        fb.cache.insert(944381, "cached_root".to_string());

        let result = fb
            .is_valid_root_for_height("CACHED_ROOT", 944381)
            .await
            .unwrap();
        assert!(result);
        assert_eq!(
            fb.primary.calls(),
            0,
            "primary must be skipped on cache hit"
        );
        assert_eq!(
            fb.fallback.calls(),
            0,
            "fallback must be skipped on cache hit"
        );
    }

    #[tokio::test]
    async fn bug005_both_providers_error_returns_fallback_error() {
        // Worst case: primary errors AND fallback errors. The fallback's
        // error should be returned (it's the more recent one). The caller
        // can then decide whether to retry at a higher level.
        let fb = GenericFallback {
            primary: ErrorHeaderService::new("primary error"),
            fallback: ErrorHeaderService::new("fallback error too"),
            cache: RootCache::new(100),
        };

        let result = fb.is_valid_root_for_height("root", 944381).await;
        assert!(result.is_err());
        assert!(
            result.as_ref().unwrap_err().contains("fallback error"),
            "should return the fallback's error message"
        );
        assert_eq!(fb.primary.calls(), 1);
        assert_eq!(fb.fallback.calls(), 1);
    }
}
