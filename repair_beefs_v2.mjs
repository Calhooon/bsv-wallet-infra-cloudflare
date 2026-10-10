#!/usr/bin/env node
/**
 * BEEF repair v2 — uses base64 blob params for large BEEFs.
 * Fixes orphaned bump txids (proofs in bumps but no raw_tx in BEEF).
 */

const CF_ACCOUNT = '<your-account-id>';
const CF_DB_ID = '<your-d1-database-id>';
const CF_TOKEN = process.env.CLOUDFLARE_API_TOKEN; if (!CF_TOKEN) { console.error('set CLOUDFLARE_API_TOKEN in the environment'); process.exit(2); }
const WOC_BASE = 'https://api.whatsonchain.com/v1/bsv/main';

const { Beef } = await import('/Users/johncalhoun/bsv/ts-sdk/dist/esm/mod.js');

function h2b(h) { const b=[]; for(let i=0;i<h.length;i+=2) b.push(parseInt(h.substr(i,2),16)); return b; }

async function d1Query(sql, params = []) {
    const resp = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT}/d1/database/${CF_DB_ID}/query`,
        { method: 'POST', headers: { 'Authorization': `Bearer ${CF_TOKEN}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql, params }) });
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

function getOrphans(beef) {
    const bT = new Set(), eT = new Set();
    for (const b of beef.bumps) if (b.path?.[0]) for (const l of b.path[0]) if (l.txid && l.hash) bT.add(l.hash);
    for (const [, x] of Object.entries(beef.txs)) if (x.tx) eT.add(x.tx.id('hex'));
    return [...bT].filter(t => !eT.has(t));
}

// Phase 1: Collect all orphan txids
console.log('Phase 1: Scanning...');
const allOrphans = new Set();
const corruptedTxids = [];
let offset = 0, scanned = 0;
while (true) {
    const r = await d1Query('SELECT p.txid, hex(p.input_beef) as h FROM proven_tx_reqs p WHERE p.input_beef IS NOT NULL ORDER BY p.proven_tx_req_id LIMIT 100 OFFSET ?', [offset]);
    if (!r.results.length) break;
    for (const row of r.results) {
        scanned++;
        try {
            const beef = Beef.fromBinary(h2b(row.h));
            const orphans = getOrphans(beef);
            if (orphans.length > 0) {
                corruptedTxids.push(row.txid);
                orphans.forEach(o => allOrphans.add(o));
            }
        } catch {}
    }
    offset += r.results.length;
    if (scanned % 1000 === 0) process.stdout.write(`\r  ${scanned} scanned, ${corruptedTxids.length} corrupted, ${allOrphans.size} orphans`);
}
console.log(`\n  ${corruptedTxids.length} corrupted, ${allOrphans.size} unique orphans\n`);

if (!corruptedTxids.length) { console.log('Nothing to fix!'); process.exit(0); }

// Phase 2: Fetch orphan raw txs
console.log('Phase 2: Fetching raw txs...');
const orphanRawHex = new Map();
let local = 0, woc = 0;
for (const txid of allOrphans) {
    // proven_txs
    const pt = await d1Query('SELECT hex(raw_tx) as h FROM proven_txs WHERE txid = ?', [txid]);
    if (pt.results.length && pt.results[0].h) { orphanRawHex.set(txid, pt.results[0].h); local++; continue; }
    // proven_tx_reqs
    const pr = await d1Query('SELECT hex(raw_tx) as h FROM proven_tx_reqs WHERE txid = ?', [txid]);
    if (pr.results.length && pr.results[0].h) { orphanRawHex.set(txid, pr.results[0].h); local++; continue; }
    // WoC
    try {
        const resp = await wocGet(`${WOC_BASE}/tx/${txid}/hex`);
        if (resp.ok) {
            const hex = (await resp.text()).trim().replace(/"/g, '');
            if (hex.length > 0) { orphanRawHex.set(txid, hex); woc++; continue; }
        }
    } catch {}
    console.log(`  FAILED: ${txid.substring(0,16)}...`);
}
console.log(`  ${orphanRawHex.size}/${allOrphans.size} fetched (local=${local}, woc=${woc})\n`);

// Phase 3: Fix BEEFs — use wrangler CLI for large updates
console.log(`Phase 3: Fixing ${corruptedTxids.length} BEEFs...`);
let fixed = 0, failed = 0;

for (let i = 0; i < corruptedTxids.length; i += 20) {
    const batch = corruptedTxids.slice(i, i + 20);
    const ph = batch.map(() => '?').join(',');
    const r = await d1Query(`SELECT txid, hex(input_beef) as h FROM proven_tx_reqs WHERE txid IN (${ph})`, batch);

    for (const row of r.results) {
        try {
            const beef = Beef.fromBinary(h2b(row.h));
            const orphans = getOrphans(beef);
            let addedAny = false;
            for (const oid of orphans) {
                const rawHex = orphanRawHex.get(oid);
                if (rawHex) { beef.mergeRawTx(h2b(rawHex)); addedAny = true; }
            }
            if (!addedAny) { failed++; continue; }

            // Verify fix worked
            const remaining = getOrphans(beef);
            if (remaining.length > 0) {
                console.log(`  WARN: ${row.txid.substring(0,16)}... still has ${remaining.length} orphans`);
            }

            // Serialize fixed BEEF to base64 for D1 blob param
            const fixedBytes = beef.toBinary();
            const fixedB64 = Buffer.from(fixedBytes).toString('base64');

            // Use parameterized query with base64 blob
            // D1 REST API: blobs can be passed as base64 strings with type hint
            const updateResult = await d1Query(
                'UPDATE proven_tx_reqs SET input_beef = ?, updated_at = datetime(\'now\') WHERE txid = ?',
                [fixedB64, row.txid]
            );

            if (updateResult.meta?.changes > 0) {
                fixed++;
            } else {
                // Try hex literal as fallback for smaller BEEFs
                const fixedHex = Array.from(fixedBytes).map(b => b.toString(16).padStart(2, '0')).join('');
                if (fixedHex.length < 900000) { // ~450KB hex = ~225KB blob
                    await d1Query(`UPDATE proven_tx_reqs SET input_beef = X'${fixedHex}', updated_at = datetime('now') WHERE txid = ?`, [row.txid]);
                    fixed++;
                } else {
                    failed++;
                    console.log(`  TOO LARGE: ${row.txid.substring(0,16)}... (${fixedBytes.length} bytes)`);
                }
            }
        } catch (e) {
            failed++;
            if (failed <= 5) console.log(`  ERR: ${row.txid.substring(0,16)}... ${e.message?.substring(0,80)}`);
        }
    }
    process.stdout.write(`\r  ${fixed} fixed, ${failed} failed / ${corruptedTxids.length}`);
}
console.log(`\n\nDone! Fixed: ${fixed}, Failed: ${failed}`);
