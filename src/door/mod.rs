//! The door's policies for strangers (#22): the bounds every caller meets
//! before the storage sees a byte, with the reference's numbers
//! (ts-stack@edf6e03 `packages/wallet/wallet-toolbox/src/storage/remoting/StorageServer.ts`).
//!
//! Each policy is its own module; the fetch handler in `lib.rs` calls them in
//! the reference's order: the in-flight ceiling, the body bound, the rate
//! limits, the request budgets, the response bound.

pub mod bounds;
pub mod errors;
pub mod inflight;
pub mod policy;
pub mod rate;

use serde_json::Value;

/// A refusal at the door: the HTTP status and the JSON body to answer with.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal {
    /// The HTTP status of the answer.
    pub status: u16,
    /// The JSON body of the answer.
    pub body: Value,
}

impl Refusal {
    /// The reference's shape for a refusal before the JSON-RPC layer:
    /// `{status: "error", code, description}` (`RateLimitPolicy.ts:18-24`,
    /// `edgePolicy.ts:629-633, 669-673`).
    pub fn plain(status: u16, code: &str, description: &str) -> Self {
        Self {
            status,
            body: serde_json::json!({
                "status": "error",
                "code": code,
                "description": description,
            }),
        }
    }

    /// The refusal as an unsigned answer with CORS, for the routes before the
    /// BRC-31 session exists.
    pub fn to_response(&self) -> worker::Result<worker::Response> {
        let response = worker::Response::from_json(&self.body)?.with_status(self.status);
        Ok(bsv_middleware_cloudflare::add_cors_headers(response))
    }
}
