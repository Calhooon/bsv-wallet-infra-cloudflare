//! Service abstractions for broadcast and proof providers.
//!
//! Replaces hardcoded WhatsOnChain calls with trait-based dispatch.
//! Providers: WoC (legacy), ARC (primary, added in Phase 2b).

pub mod arc;
pub mod arcade;
pub mod bitails;
pub mod chaintracker;
pub mod multi;
pub mod selected;
pub mod woc;

// =============================================================================
// Broadcast types
// =============================================================================

/// Result of a transaction broadcast attempt.
pub struct BroadcastResult {
    /// The transaction ID.
    pub txid: String,
    /// Status string for the transaction (e.g. "sent", "seen_on_network").
    pub tx_status: String,
    /// Whether the provider confirmed the tx was seen on the P2P network.
    pub seen_on_network: bool,
}

/// Error from a broadcast or proof service.
///
/// 4-way classification matching the reference wallet-toolbox pattern:
/// - **DoubleSpend**: Input already spent by a competing tx -- permanent failure.
/// - **InvalidTx**: Malformed/invalid transaction -- permanent failure.
/// - **ServiceError**: ARC/network temporarily unavailable -- keep inputs locked for retry.
///
/// Both `DoubleSpend` and `InvalidTx` are permanent failures that release inputs.
/// `ServiceError` is transient -- inputs stay locked so the monitor can retry every 5 min.
pub enum BroadcastError {
    /// Input already spent -- permanent failure, release inputs.
    DoubleSpend(String),
    /// Malformed/invalid transaction -- permanent failure, release inputs.
    InvalidTx(String),
    /// ARC/network temporarily unavailable -- keep inputs LOCKED for retry.
    ServiceError(String),
}

impl BroadcastError {
    /// Returns true if this is a permanent failure (inputs should be released).
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            BroadcastError::DoubleSpend(_) | BroadcastError::InvalidTx(_)
        )
    }
}

impl std::fmt::Display for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BroadcastError::DoubleSpend(msg) => write!(f, "Double-spend: {}", msg),
            BroadcastError::InvalidTx(msg) => write!(f, "Invalid tx: {}", msg),
            BroadcastError::ServiceError(msg) => write!(f, "Service error: {}", msg),
        }
    }
}

impl std::fmt::Debug for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BroadcastError::DoubleSpend(msg) => {
                write!(f, "BroadcastError::DoubleSpend({})", msg)
            }
            BroadcastError::InvalidTx(msg) => {
                write!(f, "BroadcastError::InvalidTx({})", msg)
            }
            BroadcastError::ServiceError(msg) => {
                write!(f, "BroadcastError::ServiceError({})", msg)
            }
        }
    }
}

// =============================================================================
// Broadcast trait
// =============================================================================

/// Transaction broadcast service.
pub trait BroadcastService {
    /// Broadcast a raw transaction hex. Returns txid and propagation status.
    fn broadcast_raw_tx(
        &self,
        raw_hex: &str,
    ) -> impl std::future::Future<Output = std::result::Result<BroadcastResult, BroadcastError>>;

    /// Broadcast a BEEF hex (for providers that support it, falls back to raw tx extraction).
    fn broadcast_beef(
        &self,
        beef_hex: &str,
    ) -> impl std::future::Future<Output = std::result::Result<BroadcastResult, BroadcastError>>;
}

// =============================================================================
// Proof types
// =============================================================================

/// Merkle proof from a provider.
pub struct ProofResult {
    /// The transaction ID this proof is for.
    pub txid: String,
    /// BRC-74 binary merkle path.
    pub merkle_path_binary: Vec<u8>,
    /// Block height where the transaction was mined.
    pub block_height: u32,
    /// Block hash where the transaction was mined.
    pub block_hash: String,
    /// Merkle root of the block (empty if not available from provider).
    pub merkle_root: String,
}

/// Status of a single transaction from batch status check.
pub struct TxStatusDetail {
    pub txid: String,
    /// "mined", "known" (mempool), or "unknown" (not found)
    pub status: String,
    /// Confirmation depth (Some for mined/known, None for unknown)
    pub depth: Option<u32>,
}

/// Spent-status of a single outpoint from a chain-index provider (G5).
///
/// Owner rule (non-negotiable): on BSV a tx SEEN_ON_NETWORK is FINAL
/// (first-seen, no RBF). A mempool-only spend therefore counts as `Spent` —
/// callers must NOT wait for the spending tx to be mined before acting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpentStatus {
    /// A spending transaction exists (mined OR merely seen — both final).
    Spent {
        /// txid of the spending transaction, as reported by the provider.
        spending_txid: String,
    },
    /// The provider reports the outpoint unspent — or does not know the
    /// outpoint at all (indistinguishable on WoC's endpoint; both mean
    /// "take no action").
    Unspent,
    /// The provider cannot answer outpoint-spent queries (default impl).
    /// Callers must treat this as "no information", never as unspent.
    Unsupported,
}

/// Transaction proof service.
pub trait ProofService {
    /// Fetch merkle proof for a transaction. Returns None if not yet proven.
    fn get_proof(
        &self,
        txid: &str,
    ) -> impl std::future::Future<Output = std::result::Result<Option<ProofResult>, String>>;

    /// Fetch raw transaction bytes by txid. Returns None if not found.
    fn get_raw_tx(
        &self,
        txid: &str,
    ) -> impl std::future::Future<Output = std::result::Result<Option<Vec<u8>>, String>>;

    /// Fetch current chain tip height. Used for reorg detection.
    fn get_chain_height(
        &self,
    ) -> impl std::future::Future<Output = std::result::Result<u32, String>>;

    /// Batch status check for multiple txids (Go pattern: filterTxsByConfirmationDepth).
    /// Returns status for each txid: "mined" (confirmed), "known" (mempool), "unknown" (not found).
    /// Default implementation returns all unknown (providers that don't support batch can override).
    fn get_status_for_txids(
        &self,
        txids: &[String],
    ) -> impl std::future::Future<Output = std::result::Result<Vec<TxStatusDetail>, String>> {
        std::future::ready(Ok(txids
            .iter()
            .map(|t| TxStatusDetail {
                txid: t.clone(),
                status: "unknown".to_string(),
                depth: None,
            })
            .collect()))
    }

    /// Spent-status lookup for a single outpoint (G5 — external-spend scan).
    ///
    /// Default implementation returns `Unsupported` so existing providers stay
    /// source-compatible (additive change). Providers with an outpoint-spent
    /// index (WoC `GET /tx/{txid}/{vout}/spent`) override this.
    fn get_spent_status(
        &self,
        _txid: &str,
        _vout: u32,
    ) -> impl std::future::Future<Output = std::result::Result<SpentStatus, String>> {
        std::future::ready(Ok(SpentStatus::Unsupported))
    }

    /// Reset any per-monitor-run internal cache state before a new check_for_proofs run.
    ///
    /// Providers that maintain in-run caches (e.g. WocProvider's block hash→height cache)
    /// override this to clear. Default is a no-op for providers with no cache state.
    fn reset_run_cache(&self) {}
}
