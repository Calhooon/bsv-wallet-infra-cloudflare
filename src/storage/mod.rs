//! StorageD1 — the core storage implementation backed by Cloudflare D1 + R2.

pub mod abort_action;
pub mod beef_verification;
pub mod certificates;
pub mod create_action;
pub mod internalize_action;
pub mod process_action;
pub mod readers;
pub mod relinquish_output;
pub mod reserve_outputs;
pub mod writers;

use worker::{Bucket, D1Database};

use crate::entities::TableSettings;
use crate::services::chaintracker::HeaderProvider;
use crate::services::{BroadcastService, ProofService};
use crate::types::BeefVerificationMode;

/// Main storage struct. Holds references to D1, R2, and broadcast service bindings.
pub struct StorageD1<'a, B: BroadcastService + ProofService = crate::services::woc::WocProvider> {
    db: &'a D1Database,
    blobs: &'a Bucket,
    broadcast: &'a B,
    settings: Option<TableSettings>,
    beef_verification_mode: BeefVerificationMode,
    header_provider: Option<&'a HeaderProvider>,
    /// When true, a freshly-internalized deposit stays `spendable = 1` even if
    /// the internalize-time broadcast hits a transient ServiceError — i.e. it is
    /// spendable at ZERO confirmations. Dev lever (env `INTERNALIZE_ZERO_CONF`):
    /// the operator funds from their own already-broadcast wallet, so the funding
    /// tx is on-network; this removes the ~1-block (≈10 min) wait for the monitor
    /// to flip spendable after a transient broadcast hiccup. The monitor still
    /// fetches the proof and reconciles status later — only the spendable-demotion
    /// on a transient error is skipped.
    internalize_zero_conf: bool,
}

impl<'a, B: BroadcastService + ProofService> StorageD1<'a, B> {
    pub fn new(db: &'a D1Database, blobs: &'a Bucket, broadcast: &'a B) -> Self {
        Self {
            db,
            blobs,
            broadcast,
            settings: None,
            beef_verification_mode: BeefVerificationMode::default(),
            header_provider: None,
            internalize_zero_conf: false,
        }
    }

    /// Enable/disable 0-conf spendable on internalize (env `INTERNALIZE_ZERO_CONF`).
    pub fn with_internalize_zero_conf(mut self, enabled: bool) -> Self {
        self.internalize_zero_conf = enabled;
        self
    }

    pub fn internalize_zero_conf(&self) -> bool {
        self.internalize_zero_conf
    }

    /// Set the BEEF verification mode and header provider for SPV verification.
    pub fn with_beef_verification(
        mut self,
        mode: BeefVerificationMode,
        header_provider: &'a HeaderProvider,
    ) -> Self {
        self.beef_verification_mode = mode;
        self.header_provider = Some(header_provider);
        self
    }

    pub fn db(&self) -> &D1Database {
        self.db
    }

    pub fn blobs(&self) -> &Bucket {
        self.blobs
    }

    pub fn broadcast(&self) -> &B {
        self.broadcast
    }

    pub fn settings(&self) -> Option<&TableSettings> {
        self.settings.as_ref()
    }

    pub fn set_settings(&mut self, settings: TableSettings) {
        self.settings = Some(settings);
    }

    pub fn beef_verification_mode(&self) -> BeefVerificationMode {
        self.beef_verification_mode
    }

    pub fn header_provider(&self) -> Option<&HeaderProvider> {
        self.header_provider
    }
}
