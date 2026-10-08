#!/usr/bin/env node
/**
 * One-time BEEF repair script.
 * Fixes orphaned bump txids (proofs in bumps but no raw_tx in BEEF).
 *
 * Phase 1: Collect all unique orphan txids
 * Phase 2: Fetch raw_txs from proven_txs (local) + WoC (3 req/sec)
 * Phase 3: Fix each corrupted BEEF and write back to D1
 */

const CF_ACCOUNT = '<your-account-id>';
const CF_DB_ID = '<your-d1-database-id>';
const CF_TOKEN = '61rJhtnCk2IhD04qIPT6-bkC1Io4h-jaGhfMglYS';
const WOC_BASE = 'https://api.whatsonchain.com/v1/bsv/main';

// Dynamic import for ESM
const { Beef } = await import('/Users/johncalhoun/bsv/ts-sdk/dist/esm/mod.js');

// ── Helpers ─────────────────────────────────────────────────────────────────

function hexToBytes(hex) {
    const b = [];
    for (let i = 0; i < hex.length; i += 2) b.push(parseInt(hex.substr(i, 2), 16));
    return b;
}

function bytesToHex(bytes) {
    return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function d1Query(sql, params = []) {
    const resp = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT}/d1/database/${CF_DB_ID}/query`,
        {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${CF_TOKEN}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, params }),
        }
    );
    const data = await resp.json();
    if (!data.success) throw new Error(`D1: ${JSON.stringify(data.errors)}`);
    return data.result[0];
}

let lastWoc = 0;
async function wocGet(url) {
    const wait = Math.max(0, 340 - (Date.now() - lastWoc));
    if (wait > 0) await new Promise(r => setTimeout(r, wait));
    lastWoc = Date.now();
    return fetch(url);
}

function getOrphanTxids(beef) {
    const bumpTxids = new Set();
    for (const bump of beef.bumps) {
        if (bump.path?.[0]) {
            for (const leaf of bump.path[0]) {
                if (leaf.txid && leaf.hash) bumpTxids.add(leaf.hash);
            }
        }
    }
    const beefTxids = new Set();
    for (const [, btx] of Object.entries(beef.txs)) {
        if (btx.tx) beefTxids.add(btx.tx.id('hex'));
    }
    return [...bumpTxids].filter(t => !beefTxids.has(t));
}

// ── Phase 1: Discover orphans and corrupted BEEFs ───────────────────────────

console.log('Phase 1: Scanning for corrupted BEEFs...');

const allOrphans = new Set();
const corruptedTxids = [];
let offset = 0, scanned = 0;

while (true) {
    const result = await d1Query(
        `SELECT p.txid, hex(p.input_beef) as beef_hex
         FROM proven_tx_reqs p JOIN transactions t ON p.txid = t.txid
         WHERE t.is_outgoing = 1 AND p.input_beef IS NOT NULL
         ORDER BY p.proven_tx_req_id LIMIT 100 OFFSET ?`, [offset]
    );
    if (result.results.length === 0) break;

    for (const row of result.results) {
        scanned++;
        try {
            const beef = Beef.fromBinary(hexToBytes(row.beef_hex));
            const orphans = getOrphanTxids(beef);
            if (orphans.length > 0) {
                corruptedTxids.push(row.txid);
                orphans.forEach(o => allOrphans.add(o));
            }
        } catch {}
    }
    offset += result.results.length;
    if (scanned % 500 === 0) process.stdout.write(`\r  ${scanned} scanned, ${corruptedTxids.length} corrupted, ${allOrphans.size} orphans`);
}

console.log(`\n  Done: ${corruptedTxids.length} corrupted BEEFs, ${allOrphans.size} unique orphan txids.\n`);

if (corruptedTxids.length === 0) { console.log('Nothing to fix!'); process.exit(0); }

// ── Phase 2: Fetch raw_txs for orphans ──────────────────────────────────────

console.log('Phase 2: Fetching raw transactions...');

const orphanRawTxHex = new Map();
let fromLocal = 0, fromWoc = 0, failed = 0;

for (const txid of allOrphans) {
    // Try proven_txs first (local, no rate limit)
    const local = await d1Query('SELECT hex(raw_tx) as h FROM proven_txs WHERE txid = ?', [txid]);
    if (local.results.length > 0 && local.results[0].h) {
        orphanRawTxHex.set(txid, local.results[0].h);
        fromLocal++;
        continue;
    }

    // Try proven_tx_reqs
    const localReq = await d1Query('SELECT hex(raw_tx) as h FROM proven_tx_reqs WHERE txid = ?', [txid]);
    if (localReq.results.length > 0 && localReq.results[0].h) {
        orphanRawTxHex.set(txid, localReq.results[0].h);
        fromLocal++;
        continue;
    }

    // WoC fallback (rate limited)
    try {
        const resp = await wocGet(`${WOC_BASE}/tx/${txid}/hex`);
        if (resp.ok) {
            const hex = (await resp.text()).trim().replace(/"/g, '');
            if (hex.length > 0) {
                orphanRawTxHex.set(txid, hex);
                fromWoc++;
                if (fromWoc % 10 === 0) process.stdout.write(`\r  local=${fromLocal} woc=${fromWoc} failed=${failed}`);
                continue;
            }
        }
    } catch {}
    failed++;
    console.log(`\n  FAILED: ${txid.substring(0, 16)}...`);
}

console.log(`\n  Fetched: ${orphanRawTxHex.size}/${allOrphans.size} (local=${fromLocal}, woc=${fromWoc}, failed=${failed})\n`);

// ── Phase 3: Fix corrupted BEEFs ────────────────────────────────────────────

console.log(`Phase 3: Fixing ${corruptedTxids.length} BEEFs...`);

let fixed = 0, fixFailed = 0;

for (let i = 0; i < corruptedTxids.length; i += 50) {
    const batch = corruptedTxids.slice(i, i + 50);
    const placeholders = batch.map(() => '?').join(',');

    const result = await d1Query(
        `SELECT txid, hex(input_beef) as beef_hex FROM proven_tx_reqs WHERE txid IN (${placeholders})`,
        batch
    );

    for (const row of result.results) {
        try {
            const beef = Beef.fromBinary(hexToBytes(row.beef_hex));
            const orphans = getOrphanTxids(beef);

            let addedAny = false;
            for (const orphanTxid of orphans) {
                const rawHex = orphanRawTxHex.get(orphanTxid);
                if (rawHex) {
                    beef.mergeRawTx(hexToBytes(rawHex));
                    addedAny = true;
                }
            }

            if (addedAny) {
                // Verify the fix worked
                const remaining = getOrphanTxids(beef);
                if (remaining.length > 0) {
                    console.log(`\n  WARN: ${row.txid.substring(0,16)}... still has ${remaining.length} orphans after fix`);
                }

                const fixedHex = bytesToHex(beef.toBinary());
                // D1 API: use hex() in SQL for reading, but for writing we need to use X'hex' literal
                await d1Query(
                    `UPDATE proven_tx_reqs SET input_beef = X'${fixedHex}', updated_at = datetime('now') WHERE txid = ?`,
                    [row.txid]
                );
                fixed++;
            }
        } catch (e) {
            fixFailed++;
            console.log(`\n  ERROR fixing ${row.txid.substring(0,16)}...: ${e.message?.substring(0,80)}`);
        }
    }
    process.stdout.write(`\r  ${fixed} fixed, ${fixFailed} failed / ${corruptedTxids.length}`);
}

console.log(`\n\nDone! Fixed: ${fixed}, Failed: ${fixFailed}, Total corrupted: ${corruptedTxids.length}`);
