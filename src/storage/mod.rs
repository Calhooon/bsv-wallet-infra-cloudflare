//! StorageD1 — the core storage implementation backed by Cloudflare D1 + R2.

pub mod abort_action;
pub mod beef_verification;
pub mod certificates;
pub mod create_action;
pub mod internalize_action;
pub mod process_action;
pub mod readers;
pub mod relinquish_output;
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
        }
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
