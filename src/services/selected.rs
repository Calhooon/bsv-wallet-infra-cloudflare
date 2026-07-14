//! Env-selected broadcaster: today's ARC→WoC `MultiProvider` path, or the Arcade V2
//! provider with ARC/WoC as the documented OUTAGE fallback.
//!
//! Selection rides the `BROADCASTER` env var:
//! - absent / empty / `arc` → [`SelectedProvider::Arc`] — the pre-migration path,
//!   byte-identical behavior (the no-regression default).
//! - `arcade` → [`SelectedProvider::Arcade`] — Arcade V2 primary; ARC/WoC take over ONLY
//!   when Arcade never accepted the submission (an OUTAGE). A definitive network reject
//!   (async `REJECTED` / `DOUBLE_SPEND_ATTEMPTED`) is a VERDICT and is never retried on
//!   another provider — a swallowed reject would fake a success (the money-rails law).
//! - anything else → a hard configuration error. A selector typo must STOP the worker
//!   loudly, never silently pick a default (`BROADCASTER=arcane` quietly meaning "arc"
//!   would un-ship the migration without anyone noticing).
//!
//! Proof/status lookups (`ProofService`) are unchanged: both arms delegate to the inner
//! `MultiProvider` (ARC → WoC → Bitails; ChainTracks tip). Arcade's free MINED-webhook
//! merklePath is a possible future monitor integration, deliberately NOT part of this
//! migration.

use super::arcade::{ArcadeFailure, ArcadeProvider};
use super::multi::MultiProvider;
use super::{
    BroadcastError, BroadcastResult, BroadcastService, ProofResult, ProofService, SpentStatus,
    TxStatusDetail,
};

// =============================================================================
// Selection
// =============================================================================

/// Parsed value of the `BROADCASTER` env var.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcasterChoice {
    Arc,
    Arcade,
}

impl BroadcasterChoice {
    /// Parse the raw env value. Absent/empty/`arc` = the ARC path; `arcade` = Arcade V2;
    /// anything else is a hard STOP (never a silent default).
    pub fn parse(raw: Option<&str>) -> std::result::Result<Self, String> {
        match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("arc") => Ok(BroadcasterChoice::Arc),
            Some("arcade") => Ok(BroadcasterChoice::Arcade),
            Some(other) => Err(format!(
                "BROADCASTER={other:?} is not a recognized selector (expected \"arc\" or \
                 \"arcade\") — refusing to guess a broadcaster; STOP"
            )),
        }
    }
}

// =============================================================================
// SelectedProvider
// =============================================================================

/// The provider every consumer (`StorageD1`, `run_monitor`) actually holds. Generic call
/// sites are untouched — this implements both `BroadcastService` and `ProofService`.
pub enum SelectedProvider {
    /// Pre-migration path: ARC primary, WoC broadcast fallback (unchanged).
    Arc(MultiProvider),
    /// Arcade V2 primary; `fallback` (ARC→WoC) takes broadcasts only on Arcade OUTAGE,
    /// and keeps all proof/status duties.
    Arcade {
        arcade: ArcadeProvider,
        fallback: MultiProvider,
    },
}

impl SelectedProvider {
    pub fn new(choice: BroadcasterChoice, arcade_url: Option<String>, multi: MultiProvider) -> Self {
        match choice {
            BroadcasterChoice::Arc => SelectedProvider::Arc(multi),
            BroadcasterChoice::Arcade => SelectedProvider::Arcade {
                arcade: ArcadeProvider::new(arcade_url),
                fallback: multi,
            },
        }
    }

    fn multi(&self) -> &MultiProvider {
        match self {
            SelectedProvider::Arc(m) => m,
            SelectedProvider::Arcade { fallback, .. } => fallback,
        }
    }
}

/// Fold an Arcade failure into the fallback decision (shared by both broadcast methods).
/// Returns `Ok(msg)` when the ARC/WoC fallback should run (outage), `Err` otherwise.
fn fallback_or_error(failure: ArcadeFailure) -> std::result::Result<String, BroadcastError> {
    match failure {
        ArcadeFailure::Outage(msg) => Ok(msg),
        // A reject is definitive — never retried on another provider.
        ArcadeFailure::Permanent(e) => Err(e),
        // Submitted but unverified: the tx IS at Arcade. Keep inputs locked (ServiceError
        // semantics) and let the monitor re-broadcast — Arcade dedupes and replays status.
        ArcadeFailure::Unverified(msg) => Err(BroadcastError::ServiceError(msg)),
    }
}

impl BroadcastService for SelectedProvider {
    async fn broadcast_raw_tx(
        &self,
        raw_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        match self {
            SelectedProvider::Arc(m) => m.broadcast_raw_tx(raw_hex).await,
            SelectedProvider::Arcade { arcade, fallback } => {
                match arcade.broadcast_raw_tx_arcade(raw_hex).await {
                    Ok(r) => Ok(r),
                    Err(failure) => {
                        let outage = fallback_or_error(failure)?;
                        worker::console_error!(
                            "ARCADE OUTAGE (raw path) — falling back to ARC/WoC: {}",
                            outage
                        );
                        fallback.broadcast_raw_tx(raw_hex).await
                    }
                }
            }
        }
    }

    async fn broadcast_beef(
        &self,
        beef_hex: &str,
    ) -> std::result::Result<BroadcastResult, BroadcastError> {
        match self {
            SelectedProvider::Arc(m) => m.broadcast_beef(beef_hex).await,
            SelectedProvider::Arcade { arcade, fallback } => {
                match arcade.broadcast_beef_arcade(beef_hex).await {
                    Ok(r) => Ok(r),
                    Err(failure) => {
                        let outage = fallback_or_error(failure)?;
                        worker::console_error!(
                            "ARCADE OUTAGE (beef path) — falling back to ARC/WoC: {}",
                            outage
                        );
                        fallback.broadcast_beef(beef_hex).await
                    }
                }
            }
        }
    }
}

impl ProofService for SelectedProvider {
    async fn get_proof(&self, txid: &str) -> std::result::Result<Option<ProofResult>, String> {
        self.multi().get_proof(txid).await
    }

    async fn get_raw_tx(&self, txid: &str) -> std::result::Result<Option<Vec<u8>>, String> {
        self.multi().get_raw_tx(txid).await
    }

    async fn get_chain_height(&self) -> std::result::Result<u32, String> {
        self.multi().get_chain_height().await
    }

    async fn get_status_for_txids(
        &self,
        txids: &[String],
    ) -> std::result::Result<Vec<TxStatusDetail>, String> {
        self.multi().get_status_for_txids(txids).await
    }

    async fn get_spent_status(
        &self,
        txid: &str,
        vout: u32,
    ) -> std::result::Result<SpentStatus, String> {
        self.multi().get_spent_status(txid, vout).await
    }

    fn reset_run_cache(&self) {
        self.multi().reset_run_cache();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn choice_defaults_to_arc() {
        assert_eq!(BroadcasterChoice::parse(None), Ok(BroadcasterChoice::Arc));
        assert_eq!(
            BroadcasterChoice::parse(Some("")),
            Ok(BroadcasterChoice::Arc)
        );
        assert_eq!(
            BroadcasterChoice::parse(Some("arc")),
            Ok(BroadcasterChoice::Arc)
        );
        assert_eq!(
            BroadcasterChoice::parse(Some("  ARC ")),
            Ok(BroadcasterChoice::Arc)
        );
    }

    #[test]
    fn choice_selects_arcade() {
        assert_eq!(
            BroadcasterChoice::parse(Some("arcade")),
            Ok(BroadcasterChoice::Arcade)
        );
        assert_eq!(
            BroadcasterChoice::parse(Some("Arcade")),
            Ok(BroadcasterChoice::Arcade)
        );
    }

    #[test]
    fn choice_typo_is_a_hard_stop_never_a_silent_default() {
        for typo in ["arcane", "arcade2", "woc", "taal", "arc,arcade"] {
            let err = BroadcasterChoice::parse(Some(typo)).expect_err("typo must STOP");
            assert!(err.contains("STOP"), "{err}");
            assert!(err.contains(typo), "names the bad value: {err}");
        }
    }

    #[test]
    fn permanent_and_unverified_never_fall_back() {
        // REJECTED is a verdict — the fallback path must not run.
        let e = fallback_or_error(ArcadeFailure::Permanent(BroadcastError::InvalidTx(
            "REJECTED".into(),
        )))
        .expect_err("permanent must not fall back");
        assert!(e.is_permanent());

        // Unverified = submitted but not gated — inputs stay locked (ServiceError), and the
        // fallback must NOT double-submit through another provider.
        let e = fallback_or_error(ArcadeFailure::Unverified("no verdict in 20s".into()))
            .expect_err("unverified must not fall back");
        assert!(matches!(e, BroadcastError::ServiceError(_)));

        // Only a genuine outage routes to ARC/WoC.
        let msg = fallback_or_error(ArcadeFailure::Outage("unreachable".into()))
            .expect("outage falls back");
        assert_eq!(msg, "unreachable");
    }
}
