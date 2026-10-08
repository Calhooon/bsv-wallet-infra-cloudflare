//! Task 11 — deep reorg sweep + store-time proof gate.
//!
//! WHY (2026-10-07 incident): 14 proven_txs rows kept proofs from the ORPHANED
//! block 965771 (`0000…153e10f4…fbb1`) for a month after a 2-block reorg re-mined
//! their txs in 965773. `shallow_reorg_sweep` only looks at the last 200 blocks
//! and only acts when `get_proof` hands back a canonical proof with a different
//! block hash; for these Arcade-broadcast txs ARC answers 404 and WoC was
//! rate-limiting (HTTP 429 in 22 of the 34 hourly sweeps that covered 965771), so
//! every sweep silently `continue`d and the rows aged out of the 200-block window
//! unrepaired. `check_chain_reorg` never fired either: the tip went
//! 965771 → 965773, never LOWER.
//!
//! The deep sweep does not depend on the proof providers to DETECT an orphan: it
//! walks every distinct (height, block_hash, merkle_root) in proven_txs and asks
//! the ChainTracker (ChainTracks → WoC) whether the root is canonical for the
//! height. Only on a mismatch does it call `get_proof`, and a replacement is
//! written only after ITS root verifies too. Bounded per cron run
//! (`DEEP_SWEEP_MAX_HEIGHTS` header lookups), resumable via a cursor in
//! monitor_events, wraps forever — so an orphan proof at ANY depth is found
//! within one full pass and retried every pass until repaired.

use std::collections::{HashMap, HashSet};

use bsv_sdk::transaction::MerklePath;
use chrono::Utc;
use serde::Deserialize;
use worker::*;

use crate::d1::Query;
use crate::services::chaintracker::HeaderService;
use crate::services::{ProofResult, ProofService};

/// Distinct heights verified per cron run (one header lookup each, cached).
pub(crate) const DEEP_SWEEP_MAX_HEIGHTS: usize = 40;

/// Pair rows fetched per cursor page. Comfortably above 40 heights' worth of
/// pairs (orphan + canonical at the same height is the worst normal case).
const DEEP_SWEEP_PAIR_PAGE: usize = 400;

/// Hard bound on the admin range trigger (`/monitor/reorg-sweep`).
pub(crate) const DEEP_SWEEP_RANGE_MAX_HEIGHTS: u32 = 500;

/// Max rows re-proved (`get_proof`) in one run — caps subrequests when a
/// whole busy block turns out orphaned. Leftover rows are picked up next run
/// (the cursor stops before the unfinished pair).
const DEEP_SWEEP_MAX_REPAIR_ROWS: usize = 60;

/// Cursor persistence — same latest-row-wins, UPDATE-in-place mechanism as
/// `'external_spend_cursor'` (no migration needed for the row itself).
const DEEP_CURSOR_READ_SQL: &str = "SELECT details FROM monitor_events \
     WHERE event = 'deep_reorg_cursor' \
     ORDER BY created_at DESC, event_id DESC LIMIT 1";

const DEEP_CURSOR_UPDATE_SQL: &str =
    "UPDATE monitor_events SET details = ?, updated_at = CURRENT_TIMESTAMP \
     WHERE event_id = (SELECT event_id FROM monitor_events \
                       WHERE event = 'deep_reorg_cursor' \
                       ORDER BY created_at DESC, event_id DESC LIMIT 1)";

const DEEP_CURSOR_INSERT_SQL: &str =
    "INSERT INTO monitor_events (event, details, created_at, updated_at) \
     VALUES ('deep_reorg_cursor', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";

/// Distinct proof groups above the cursor, ascending. Served by
/// `idx_proven_txs_height_pair` (migration 0004) when present; correct without it.
/// Rows nulled by `handle_reorg` (block_hash = '') are owned by check_for_proofs.
const DEEP_PAIRS_AFTER_SQL: &str = "SELECT height, block_hash, merkle_root, COUNT(*) AS n \
     FROM proven_txs \
     WHERE height > ? AND block_hash IS NOT NULL AND block_hash != '' \
     GROUP BY height, block_hash, merkle_root \
     ORDER BY height ASC, block_hash ASC, merkle_root ASC \
     LIMIT ?";

const DEEP_PAIRS_RANGE_SQL: &str = "SELECT height, block_hash, merkle_root, COUNT(*) AS n \
     FROM proven_txs \
     WHERE height >= ? AND height <= ? AND block_hash IS NOT NULL AND block_hash != '' \
     GROUP BY height, block_hash, merkle_root \
     ORDER BY height ASC, block_hash ASC, merkle_root ASC";

/// A few member paths — only read when the group has no stored root.
const DEEP_PAIR_SAMPLE_SQL: &str = "SELECT proven_tx_id, txid, hex(merkle_path) AS merkle_path \
     FROM proven_txs \
     WHERE height = ? AND block_hash = ? AND merkle_root = ? \
     ORDER BY proven_tx_id ASC LIMIT 3";

/// Every member of an ORPHAN group (ids only — the repair re-proves each).
const DEEP_PAIR_ROWS_SQL: &str = "SELECT proven_tx_id, txid, NULL AS merkle_path \
     FROM proven_txs \
     WHERE height = ? AND block_hash = ? AND merkle_root = ? \
     ORDER BY proven_tx_id ASC";

// =============================================================================
// Pure parts (unit-tested)
// =============================================================================

/// One distinct (height, block_hash, merkle_root) group of proven_txs rows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProofPair {
    pub height: u32,
    pub block_hash: String,
    /// May be '' — ARC-sourced proofs were stored without a root; the sweep
    /// then computes it from a member row's merkle_path.
    pub merkle_root: String,
    pub rows: u32,
}

/// Trim a height-ordered page of pairs to at most `max_heights` distinct
/// heights. When the page was full (`page_full`) its last height may be cut
/// off mid-way, so that height is dropped too (it heads the next run) —
/// unless it is the only height on the page, which is then processed as-is.
pub(crate) fn select_pairs(
    pairs: Vec<ProofPair>,
    max_heights: usize,
    page_full: bool,
) -> Vec<ProofPair> {
    let mut out: Vec<ProofPair> = Vec::new();
    let mut heights = 0usize;
    let mut truncated = page_full;
    for p in pairs {
        if out.last().map(|l: &ProofPair| l.height) != Some(p.height) {
            if heights == max_heights {
                truncated = false; // the next height is entirely unconsumed
                break;
            }
            heights += 1;
        }
        out.push(p);
    }
    if truncated && heights > 1 {
        let last = out.last().map(|p| p.height);
        out.retain(|p| Some(p.height) != last);
    }
    out
}

/// Persistent deep-sweep cursor (`monitor_events` / 'deep_reorg_cursor').
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct DeepCursor {
    /// Next run resumes at `height > last_height`. 0 = start of a pass.
    pub last_height: u32,
    /// Completed full passes (wraps).
    pub passes: u64,
    /// RFC3339 time the last full pass completed.
    pub pass_completed_at: Option<String>,
}

/// Malformed/missing → default (restart from the lowest height). Never errors.
pub(crate) fn parse_deep_cursor(details: &str) -> DeepCursor {
    let v: serde_json::Value = match serde_json::from_str(details) {
        Ok(v) => v,
        Err(_) => return DeepCursor::default(),
    };
    DeepCursor {
        last_height: v
            .get("last_height")
            .and_then(|x| x.as_u64())
            .map(|h| h as u32)
            .unwrap_or(0),
        passes: v.get("passes").and_then(|x| x.as_u64()).unwrap_or(0),
        pass_completed_at: v
            .get("pass_completed_at")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
    }
}

pub(crate) fn deep_cursor_json(c: &DeepCursor) -> String {
    serde_json::json!({
        "last_height": c.last_height,
        "passes": c.passes,
        "pass_completed_at": c.pass_completed_at,
    })
    .to_string()
}

/// The cursor after a pass wrapped (page above the cursor came back empty).
pub(crate) fn wrap_cursor(c: &DeepCursor, now: &str) -> DeepCursor {
    DeepCursor {
        last_height: 0,
        passes: c.passes + 1,
        pass_completed_at: Some(now.to_string()),
    }
}

/// The cursor after a run that fully processed every height `<= done`.
/// `None` (nothing completed, e.g. tracker outage on the first pair) keeps it.
pub(crate) fn advance_cursor(c: &DeepCursor, done: Option<u32>) -> DeepCursor {
    match done {
        Some(h) if h > c.last_height => DeepCursor {
            last_height: h,
            ..c.clone()
        },
        _ => c.clone(),
    }
}

/// Cursor to keep when the per-run repair budget runs out inside an orphan
/// group. Progress made → resume this height next run (repaired rows have left
/// the group). No progress (every tried row unrepairable) → move past it so one
/// unrepairable block can never wedge the sweep; the next pass retries it.
pub(crate) fn budget_stop_cursor(done: Option<u32>, height: u32, progressed: bool) -> Option<u32> {
    if progressed {
        done
    } else {
        Some(height)
    }
}

/// Root, height and tx index proved by a BRC-74 path for `txid`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PathFacts {
    pub height: u32,
    pub root: String,
    pub idx: u64,
}

/// Parse a BRC-74 path and compute the root for `txid` (errs if the path does
/// not contain the txid — the same check `compute_root` applies at BEEF build).
pub(crate) fn path_facts(merkle_path: &[u8], txid: &str) -> std::result::Result<PathFacts, String> {
    let mp = MerklePath::from_binary(merkle_path).map_err(|e| format!("BUMP parse: {e:?}"))?;
    let root = mp
        .compute_root(Some(txid))
        .map_err(|e| format!("BUMP root for {txid}: {e:?}"))?;
    let idx = mp
        .path
        .first()
        .and_then(|lvl| lvl.iter().find(|l| l.hash.as_deref() == Some(txid)))
        .map(|l| l.offset)
        .unwrap_or(0);
    Ok(PathFacts {
        height: mp.block_height,
        root,
        idx,
    })
}

/// ChainTracker verdict for one (root, height).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RootVerdict {
    Canonical,
    Orphan,
    /// Tracker unavailable — no evidence either way.
    Unknown(String),
}

/// Per-run root cache: at most ONE tracker lookup per height in the common
/// case. Once a height's canonical root is known, every other root at that
/// height is an orphan without another lookup.
#[derive(Default)]
pub(crate) struct RunRootCache {
    canonical: HashMap<u32, String>,
    orphan: HashSet<(u32, String)>,
    pub lookups: u32,
}

impl RunRootCache {
    pub async fn verdict<H: HeaderService>(
        &mut self,
        headers: &H,
        root: &str,
        height: u32,
    ) -> RootVerdict {
        let root_lc = root.to_ascii_lowercase();
        if let Some(c) = self.canonical.get(&height) {
            return if *c == root_lc {
                RootVerdict::Canonical
            } else {
                RootVerdict::Orphan
            };
        }
        if self.orphan.contains(&(height, root_lc.clone())) {
            return RootVerdict::Orphan;
        }
        self.lookups += 1;
        match headers.is_valid_root_for_height(&root_lc, height).await {
            Ok(true) => {
                self.canonical.insert(height, root_lc);
                RootVerdict::Canonical
            }
            Ok(false) => {
                self.orphan.insert((height, root_lc));
                RootVerdict::Orphan
            }
            Err(e) => RootVerdict::Unknown(e),
        }
    }
}

/// Store-time gate decision for a freshly fetched proof.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StoreGate {
    /// Root verified canonical — store (with the computed root/idx).
    Store(PathFacts),
    /// Tracker unavailable — store as today (never block proofs on a
    /// header-service outage). Facts are still the path's own.
    StoreUnverified(PathFacts, String),
    /// Do not store; the tx stays pending and is re-checked next cycle.
    Reject(String),
}

/// Decide whether a fetched proof may be persisted. Rejects a path that does
/// not prove `txid`, a height that disagrees with the provider's, and a root
/// the ChainTracker says is NOT canonical for its height (proof fetched
/// during/after a reorg from a lagging provider).
pub(crate) async fn gate_proof_for_store<H: HeaderService>(
    headers: &H,
    txid: &str,
    proof: &ProofResult,
) -> StoreGate {
    let facts = match path_facts(&proof.merkle_path_binary, txid) {
        Ok(f) => f,
        Err(e) => return StoreGate::Reject(e),
    };
    if proof.block_height != facts.height {
        return StoreGate::Reject(format!(
            "provider height {} != BUMP height {}",
            proof.block_height, facts.height
        ));
    }
    match headers
        .is_valid_root_for_height(&facts.root, facts.height)
        .await
    {
        Ok(true) => StoreGate::Store(facts),
        Ok(false) => StoreGate::Reject(format!(
            "root {} is NOT canonical at height {}",
            facts.root, facts.height
        )),
        Err(e) => StoreGate::StoreUnverified(facts, e),
    }
}

// =============================================================================
// Sweep (D1 + network)
// =============================================================================

#[derive(Debug, Default)]
pub struct DeepSweepOutcome {
    pub pairs_checked: u32,
    pub heights_checked: u32,
    /// Pairs whose root is NOT canonical for their height.
    pub mismatched: u32,
    /// Rows rewritten with a verified canonical proof.
    pub repaired: u32,
    /// Txids of orphan rows left as-is (no verified canonical proof yet).
    pub unrepaired: Vec<String>,
    pub header_lookups: u32,
    pub errors: Vec<String>,
}

impl DeepSweepOutcome {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "pairs_checked": self.pairs_checked,
            "heights_checked": self.heights_checked,
            "mismatched": self.mismatched,
            "repaired": self.repaired,
            "unrepaired": self.unrepaired,
            "header_lookups": self.header_lookups,
            "errors": self.errors,
        })
    }
}

#[derive(Debug, Deserialize)]
struct PairRow {
    height: Option<f64>,
    block_hash: Option<String>,
    merkle_root: Option<String>,
    n: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PairMemberRow {
    proven_tx_id: Option<f64>,
    txid: Option<String>,
    merkle_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DetailsRow {
    details: Option<String>,
}

fn to_pairs(rows: Vec<PairRow>) -> Vec<ProofPair> {
    rows.into_iter()
        .filter_map(|r| {
            Some(ProofPair {
                height: r.height? as u32,
                block_hash: r.block_hash?,
                merkle_root: r.merkle_root.unwrap_or_default(),
                rows: r.n.unwrap_or(0.0) as u32,
            })
        })
        .collect()
}

/// Cron entry: check up to `DEEP_SWEEP_MAX_HEIGHTS` heights above the cursor,
/// wrapping to the lowest height at the end of a pass.
pub async fn deep_reorg_sweep<P: ProofService, H: HeaderService>(
    db: &D1Database,
    blobs: &Bucket,
    proof_service: &P,
    headers: &H,
) -> Result<DeepSweepOutcome> {
    let cursor = Query::new(DEEP_CURSOR_READ_SQL)
        .fetch_optional::<DetailsRow>(db)
        .await
        .map_err(|e| Error::from(e.to_string()))?
        .and_then(|r| r.details)
        .map(|d| parse_deep_cursor(&d));
    let had_row = cursor.is_some();
    let mut cursor = cursor.unwrap_or_default();

    let fetch = |after: u32| {
        Query::new(DEEP_PAIRS_AFTER_SQL)
            .bind(after as i64)
            .bind(DEEP_SWEEP_PAIR_PAGE as i64)
            .fetch_all::<PairRow>(db)
    };
    let mut page = to_pairs(
        fetch(cursor.last_height)
            .await
            .map_err(|e| Error::from(e.to_string()))?,
    );
    if page.is_empty() && cursor.last_height > 0 {
        cursor = wrap_cursor(&cursor, &Utc::now().to_rfc3339());
        page = to_pairs(fetch(0).await.map_err(|e| Error::from(e.to_string()))?);
    }
    let page_full = page.len() >= DEEP_SWEEP_PAIR_PAGE;
    let pairs = select_pairs(page, DEEP_SWEEP_MAX_HEIGHTS, page_full);

    let mut out = DeepSweepOutcome::default();
    let done = sweep_pairs(db, blobs, proof_service, headers, &pairs, &mut out).await;
    let next = advance_cursor(&cursor, done);

    let details = deep_cursor_json(&next);
    if had_row {
        Query::new(DEEP_CURSOR_UPDATE_SQL)
            .bind(details.as_str())
            .execute(db)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
    } else {
        Query::new(DEEP_CURSOR_INSERT_SQL)
            .bind(details.as_str())
            .execute(db)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
    }
    Ok(out)
}

/// Admin entry (`POST /monitor/reorg-sweep`): every pair with
/// `from <= height <= to` (clamped to `DEEP_SWEEP_RANGE_MAX_HEIGHTS`),
/// ignoring and not touching the cursor.
pub async fn deep_reorg_sweep_range<P: ProofService, H: HeaderService>(
    db: &D1Database,
    blobs: &Bucket,
    proof_service: &P,
    headers: &H,
    from: u32,
    to: u32,
) -> DeepSweepOutcome {
    let mut out = DeepSweepOutcome::default();
    let to = to.min(from.saturating_add(DEEP_SWEEP_RANGE_MAX_HEIGHTS - 1));
    let rows = Query::new(DEEP_PAIRS_RANGE_SQL)
        .bind(from as i64)
        .bind(to as i64)
        .fetch_all::<PairRow>(db)
        .await;
    match rows {
        Ok(rows) => {
            let pairs = to_pairs(rows);
            sweep_pairs(db, blobs, proof_service, headers, &pairs, &mut out).await;
        }
        Err(e) => out.errors.push(format!("pairs query: {e}")),
    }
    out
}

/// Verify each pair's root; repair orphan pairs. Returns the highest height
/// whose pairs were ALL fully handled (cursor-safe), stopping early on a
/// tracker outage or an exhausted repair budget.
async fn sweep_pairs<P: ProofService, H: HeaderService>(
    db: &D1Database,
    blobs: &Bucket,
    proof_service: &P,
    headers: &H,
    pairs: &[ProofPair],
    out: &mut DeepSweepOutcome,
) -> Option<u32> {
    let mut cache = RunRootCache::default();
    let mut repair_budget = DEEP_SWEEP_MAX_REPAIR_ROWS;
    let mut done: Option<u32> = None;
    let mut current: Option<u32> = None;
    let mut heights_seen: HashSet<u32> = HashSet::new();

    for pair in pairs {
        // Entering a new height: everything at the previous one is complete.
        if current != Some(pair.height) {
            if current.is_some() {
                done = current;
            }
            current = Some(pair.height);
        }
        heights_seen.insert(pair.height);

        // The root to verify: the stored one, else computed from a member path
        // (ARC-sourced rows were stored with merkle_root = '').
        let root = if !pair.merkle_root.is_empty() {
            Some(pair.merkle_root.clone())
        } else {
            match pair_members(db, DEEP_PAIR_SAMPLE_SQL, pair).await {
                Ok(sample) => first_member_root(blobs, &sample).await,
                Err(e) => {
                    out.errors
                        .push(format!("pair sample h={}: {e}", pair.height));
                    return finish(out, &cache, &heights_seen, done);
                }
            }
        };

        let verdict = match &root {
            Some(r) => cache.verdict(headers, r, pair.height).await,
            // No member path proves its txid — broken proofs: re-prove them.
            None => RootVerdict::Orphan,
        };
        out.pairs_checked += 1;
        match verdict {
            RootVerdict::Canonical => continue,
            RootVerdict::Unknown(e) => {
                out.errors
                    .push(format!("tracker unavailable at h={}: {e}", pair.height));
                // Not advancing past this height: it is retried next run.
                return finish(out, &cache, &heights_seen, done);
            }
            RootVerdict::Orphan => {}
        }

        out.mismatched += 1;
        let members = match pair_members(db, DEEP_PAIR_ROWS_SQL, pair).await {
            Ok(m) => m,
            Err(e) => {
                out.errors.push(format!("pair rows h={}: {e}", pair.height));
                return finish(out, &cache, &heights_seen, done);
            }
        };
        console_error!(
            "deep_reorg_sweep: ORPHAN proof group h={} hash={} root={} rows={}",
            pair.height,
            pair.block_hash,
            root.as_deref().unwrap_or("?"),
            members.len()
        );
        let take = members.len().min(repair_budget);
        let repaired_before = out.repaired;
        for m in members.iter().take(take) {
            repair_row(db, blobs, proof_service, headers, &mut cache, pair, m, out).await;
        }
        repair_budget -= take;
        if take < members.len() {
            out.errors.push(format!(
                "repair budget exhausted at h={} ({} of {} rows tried)",
                pair.height,
                take,
                members.len()
            ));
            let next = budget_stop_cursor(done, pair.height, out.repaired > repaired_before);
            return finish(out, &cache, &heights_seen, next);
        }
    }
    finish(out, &cache, &heights_seen, current)
}

fn finish(
    out: &mut DeepSweepOutcome,
    cache: &RunRootCache,
    heights_seen: &HashSet<u32>,
    done: Option<u32>,
) -> Option<u32> {
    out.header_lookups = cache.lookups;
    out.heights_checked = heights_seen.len() as u32;
    done
}

async fn pair_members(
    db: &D1Database,
    sql: &str,
    pair: &ProofPair,
) -> std::result::Result<Vec<PairMemberRow>, String> {
    Query::new(sql)
        .bind(pair.height as i64)
        .bind(pair.block_hash.as_str())
        .bind(pair.merkle_root.as_str())
        .fetch_all(db)
        .await
        .map_err(|e| e.to_string())
}

async fn member_path(blobs: &Bucket, m: &PairMemberRow) -> Option<Vec<u8>> {
    let id = m.proven_tx_id? as i64;
    let d1 = m
        .merkle_path
        .as_deref()
        .filter(|h| !h.is_empty())
        .and_then(|h| hex::decode(h).ok());
    crate::r2::BlobStore::new(blobs)
        .get("proven_txs", id, "merkle_path", d1)
        .await
        .ok()
        .flatten()
}

async fn first_member_root(blobs: &Bucket, members: &[PairMemberRow]) -> Option<String> {
    for m in members {
        let (Some(txid), Some(path)) = (m.txid.as_deref(), member_path(blobs, m).await) else {
            continue;
        };
        if let Ok(f) = path_facts(&path, txid) {
            return Some(f.root);
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn repair_row<P: ProofService, H: HeaderService>(
    db: &D1Database,
    blobs: &Bucket,
    proof_service: &P,
    headers: &H,
    cache: &mut RunRootCache,
    pair: &ProofPair,
    m: &PairMemberRow,
    out: &mut DeepSweepOutcome,
) {
    let (Some(id), Some(txid)) = (m.proven_tx_id.map(|v| v as i64), m.txid.clone()) else {
        return;
    };
    let unrepaired = |out: &mut DeepSweepOutcome, reason: String| {
        console_error!("deep_reorg_sweep: {} left unrepaired — {}", txid, reason);
        out.unrepaired.push(txid.clone());
        reason
    };

    let reason = match proof_service.get_proof(&txid).await {
        Ok(Some(p)) => match path_facts(&p.merkle_path_binary, &txid) {
            Ok(f) if f.height != p.block_height => Some(unrepaired(
                out,
                format!(
                    "provider height {} != BUMP height {}",
                    p.block_height, f.height
                ),
            )),
            Ok(f) => match cache.verdict(headers, &f.root, f.height).await {
                RootVerdict::Canonical => {
                    match write_repair(db, blobs, id, &txid, pair, &p, &f).await {
                        Ok(()) => {
                            out.repaired += 1;
                            None
                        }
                        Err(e) => Some(unrepaired(out, format!("write: {e}"))),
                    }
                }
                RootVerdict::Orphan => Some(unrepaired(
                    out,
                    format!(
                        "provider proof root {} not canonical at {}",
                        f.root, f.height
                    ),
                )),
                RootVerdict::Unknown(e) => Some(unrepaired(
                    out,
                    format!("tracker unavailable verifying new proof: {e}"),
                )),
            },
            Err(e) => Some(unrepaired(out, format!("provider proof unusable: {e}"))),
        },
        Ok(None) => Some(unrepaired(out, "no canonical proof available".to_string())),
        Err(e) => Some(unrepaired(out, format!("get_proof: {e}"))),
    };

    if let Some(reason) = reason {
        let details = serde_json::json!({
            "txid": txid,
            "proven_tx_id": id,
            "height": pair.height,
            "block_hash": pair.block_hash,
            "merkle_root": pair.merkle_root,
            "reason": reason,
        })
        .to_string();
        let _ = Query::new(
            "INSERT INTO monitor_events (event, details, created_at, updated_at) \
             VALUES ('deep_reorg_unrepaired', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(details.as_str())
        .execute(db)
        .await;
    }
}

/// Replace the proof fields exactly like `shallow_reorg_sweep` (merkle_path via
/// BlobStore), plus idx and the VERIFIED root. Guarded on the old
/// height/block_hash so a concurrent writer's newer proof is never clobbered.
async fn write_repair(
    db: &D1Database,
    blobs: &Bucket,
    id: i64,
    txid: &str,
    pair: &ProofPair,
    p: &ProofResult,
    f: &PathFacts,
) -> std::result::Result<(), String> {
    let now = Utc::now().to_rfc3339();
    let (mp_d1, _) = crate::r2::BlobStore::new(blobs)
        .put("proven_txs", id, "merkle_path", &p.merkle_path_binary)
        .await
        .map_err(|e| e.to_string())?;
    let meta = Query::new(
        "UPDATE proven_txs SET height = ?, idx = ?, block_hash = ?, merkle_root = ?, \
         merkle_path = ?, updated_at = ? \
         WHERE proven_tx_id = ? AND height = ? AND block_hash = ?",
    )
    .bind(f.height as i64)
    .bind(f.idx as i64)
    .bind(p.block_hash.as_str())
    .bind(f.root.as_str())
    .bind(mp_d1)
    .bind(now.as_str())
    .bind(id)
    .bind(pair.height as i64)
    .bind(pair.block_hash.as_str())
    .execute(db)
    .await
    .map_err(|e| e.to_string())?;
    if meta.changes == 0 {
        return Err("row changed concurrently (0 rows updated)".to_string());
    }

    let details = serde_json::json!({
        "txid": txid,
        "proven_tx_id": id,
        "old_height": pair.height,
        "old_block_hash": pair.block_hash,
        "old_merkle_root": pair.merkle_root,
        "new_height": f.height,
        "new_block_hash": p.block_hash,
        "new_merkle_root": f.root,
    })
    .to_string();
    let _ = Query::new(
        "INSERT INTO monitor_events (event, details, created_at, updated_at) \
         VALUES ('deep_reorg_repair', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(details.as_str())
    .execute(db)
    .await;
    console_log!(
        "deep_reorg_sweep: REPAIRED {} {}→{} ({}→{})",
        txid,
        pair.height,
        f.height,
        &pair.block_hash[..16.min(pair.block_hash.len())],
        &p.block_hash[..16.min(p.block_hash.len())]
    );
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_sdk::transaction::MerklePathLeaf;
    use std::cell::RefCell;

    fn pair(h: u32, hash: &str, root: &str) -> ProofPair {
        ProofPair {
            height: h,
            block_hash: hash.to_string(),
            merkle_root: root.to_string(),
            rows: 1,
        }
    }

    /// Tracker with a fixed canonical root per height; counts lookups.
    struct MapTracker {
        canonical: HashMap<u32, String>,
        calls: RefCell<u32>,
        fail: bool,
    }
    impl MapTracker {
        fn new(entries: &[(u32, &str)]) -> Self {
            Self {
                canonical: entries.iter().map(|(h, r)| (*h, r.to_string())).collect(),
                calls: RefCell::new(0),
                fail: false,
            }
        }
        fn down() -> Self {
            Self {
                canonical: HashMap::new(),
                calls: RefCell::new(0),
                fail: true,
            }
        }
    }
    impl HeaderService for MapTracker {
        async fn is_valid_root_for_height(
            &self,
            root: &str,
            height: u32,
        ) -> std::result::Result<bool, String> {
            *self.calls.borrow_mut() += 1;
            if self.fail {
                return Err("chaintracks + woc unreachable".into());
            }
            Ok(self
                .canonical
                .get(&height)
                .is_some_and(|c| c.eq_ignore_ascii_case(root)))
        }
    }

    // ---- pair selection -----------------------------------------------------

    #[test]
    fn select_caps_distinct_heights_not_pairs() {
        // orphan + canonical at 100 count as ONE height
        let page = vec![
            pair(100, "a", "r1"),
            pair(100, "b", "r2"),
            pair(101, "c", "r3"),
            pair(102, "d", "r4"),
        ];
        let sel = select_pairs(page, 2, false);
        assert_eq!(
            sel.iter().map(|p| p.height).collect::<Vec<_>>(),
            vec![100, 100, 101]
        );
    }

    #[test]
    fn select_full_page_drops_possibly_partial_last_height() {
        let page = vec![
            pair(100, "a", "r"),
            pair(101, "b", "r"),
            pair(101, "c", "r"),
        ];
        let sel = select_pairs(page, 40, true);
        assert_eq!(sel.iter().map(|p| p.height).collect::<Vec<_>>(), vec![100]);
    }

    #[test]
    fn select_full_page_single_height_is_kept() {
        let page = vec![pair(100, "a", "r"), pair(100, "b", "r")];
        assert_eq!(select_pairs(page, 40, true).len(), 2);
    }

    #[test]
    fn select_full_page_cut_by_height_cap_keeps_last_height() {
        // the cap stopped us BEFORE the page end, so height 101 is complete
        let page = vec![
            pair(100, "a", "r"),
            pair(101, "b", "r"),
            pair(102, "c", "r"),
        ];
        let sel = select_pairs(page, 2, true);
        assert_eq!(
            sel.iter().map(|p| p.height).collect::<Vec<_>>(),
            vec![100, 101]
        );
    }

    #[test]
    fn select_empty_page() {
        assert!(select_pairs(vec![], 40, false).is_empty());
    }

    // ---- cursor -------------------------------------------------------------

    #[test]
    fn cursor_roundtrip_and_garbage() {
        let c = DeepCursor {
            last_height: 965_771,
            passes: 3,
            pass_completed_at: Some("2026-10-07T00:00:00Z".into()),
        };
        assert_eq!(parse_deep_cursor(&deep_cursor_json(&c)), c);
        assert_eq!(parse_deep_cursor("not json"), DeepCursor::default());
        assert_eq!(parse_deep_cursor("{}"), DeepCursor::default());
    }

    #[test]
    fn cursor_advances_only_forward() {
        let c = DeepCursor {
            last_height: 500,
            ..Default::default()
        };
        assert_eq!(advance_cursor(&c, Some(540)).last_height, 540);
        assert_eq!(advance_cursor(&c, None).last_height, 500); // outage: hold
        assert_eq!(advance_cursor(&c, Some(400)).last_height, 500);
    }

    #[test]
    fn budget_stop_resumes_on_progress_and_never_wedges() {
        assert_eq!(budget_stop_cursor(Some(99), 100, true), Some(99));
        assert_eq!(budget_stop_cursor(None, 100, true), None);
        assert_eq!(budget_stop_cursor(Some(99), 100, false), Some(100));
    }

    #[test]
    fn cursor_wrap_resets_and_counts_pass() {
        let c = DeepCursor {
            last_height: 970_000,
            passes: 7,
            pass_completed_at: None,
        };
        let w = wrap_cursor(&c, "2026-10-07T12:00:00Z");
        assert_eq!(w.last_height, 0);
        assert_eq!(w.passes, 8);
        assert_eq!(w.pass_completed_at.as_deref(), Some("2026-10-07T12:00:00Z"));
    }

    // ---- mismatch decision (run cache) --------------------------------------

    #[tokio::test]
    async fn verdict_detects_orphan_root_for_height() {
        // The incident: 965771 stored with the orphan block's root.
        let t = MapTracker::new(&[(965_771, "canon771")]);
        let mut cache = RunRootCache::default();
        assert_eq!(
            cache.verdict(&t, "709df569orphan", 965_771).await,
            RootVerdict::Orphan
        );
        assert_eq!(
            cache.verdict(&t, "CANON771", 965_771).await,
            RootVerdict::Canonical
        );
    }

    #[tokio::test]
    async fn verdict_one_lookup_per_height_once_canonical_known() {
        let t = MapTracker::new(&[(100, "good")]);
        let mut cache = RunRootCache::default();
        assert_eq!(cache.verdict(&t, "good", 100).await, RootVerdict::Canonical);
        assert_eq!(cache.verdict(&t, "good", 100).await, RootVerdict::Canonical);
        // a second (orphan) root at the same height needs no lookup
        assert_eq!(cache.verdict(&t, "bad", 100).await, RootVerdict::Orphan);
        assert_eq!(*t.calls.borrow(), 1);
        assert_eq!(cache.lookups, 1);
    }

    #[tokio::test]
    async fn verdict_tracker_down_is_unknown_not_orphan() {
        let t = MapTracker::down();
        let mut cache = RunRootCache::default();
        assert!(matches!(
            cache.verdict(&t, "x", 1).await,
            RootVerdict::Unknown(_)
        ));
    }

    // ---- store-time gate ----------------------------------------------------

    /// Two-leaf block: txid A at offset 0, sibling B at offset 1.
    fn two_leaf_proof(height: u32) -> (String, ProofResult, String) {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let mp = MerklePath::new(
            height,
            vec![vec![
                MerklePathLeaf {
                    offset: 0,
                    hash: Some(a.clone()),
                    txid: true,
                    duplicate: false,
                },
                MerklePathLeaf {
                    offset: 1,
                    hash: Some(b),
                    txid: false,
                    duplicate: false,
                },
            ]],
        )
        .unwrap();
        let root = mp.compute_root(Some(&a)).unwrap();
        let proof = ProofResult {
            txid: a.clone(),
            merkle_path_binary: mp.to_binary(),
            block_height: height,
            block_hash: "00".repeat(32),
            merkle_root: String::new(), // ARC-style: provider gave no root
        };
        (a, proof, root)
    }

    #[tokio::test]
    async fn gate_stores_canonical_proof_with_computed_root() {
        let (txid, proof, root) = two_leaf_proof(965_773);
        let t = MapTracker::new(&[(965_773, &root)]);
        match gate_proof_for_store(&t, &txid, &proof).await {
            StoreGate::Store(f) => {
                assert_eq!(f.root, root);
                assert_eq!(f.height, 965_773);
                assert_eq!(f.idx, 0);
            }
            other => panic!("expected Store, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn gate_rejects_orphan_root() {
        let (txid, proof, _) = two_leaf_proof(965_771);
        let t = MapTracker::new(&[(965_771, "someotherroot")]);
        assert!(matches!(
            gate_proof_for_store(&t, &txid, &proof).await,
            StoreGate::Reject(_)
        ));
    }

    #[tokio::test]
    async fn gate_tracker_unavailable_keeps_todays_behaviour() {
        let (txid, proof, root) = two_leaf_proof(965_771);
        let t = MapTracker::down();
        match gate_proof_for_store(&t, &txid, &proof).await {
            StoreGate::StoreUnverified(f, _) => assert_eq!(f.root, root),
            other => panic!("expected StoreUnverified, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn gate_rejects_path_not_proving_txid_and_height_disagreement() {
        let (_, proof, root) = two_leaf_proof(100);
        let t = MapTracker::new(&[(100, &root)]);
        let other_txid = "cc".repeat(32);
        assert!(matches!(
            gate_proof_for_store(&t, &other_txid, &proof).await,
            StoreGate::Reject(_)
        ));
        let (txid, mut proof, _) = two_leaf_proof(100);
        proof.block_height = 101;
        assert!(matches!(
            gate_proof_for_store(&t, &txid, &proof).await,
            StoreGate::Reject(_)
        ));
        assert_eq!(*t.calls.borrow(), 0, "rejected before any header lookup");
    }

    #[test]
    fn path_facts_reports_idx_of_txid() {
        let (_, proof, _) = two_leaf_proof(5);
        let b = "bb".repeat(32);
        let f = path_facts(&proof.merkle_path_binary, &b).unwrap();
        assert_eq!(f.idx, 1);
        assert_eq!(f.height, 5);
    }
}
