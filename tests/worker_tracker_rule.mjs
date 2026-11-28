// Local compiled-Worker witnesses for bsv-stack-lean #72 and #73, and #50: Arcade's 400
// carrying status 460 to 475 is a policy refusal, a hint, never invalidity (B5).
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'
const { Miniflare } = await import(process.env.MINIFLARE_MODULE)
const { AuthFetch, ProtoWallet, PrivateKey } = await import(process.env.BSV_SDK_MODULE)
const sha = b => createHash('sha256').update(b).digest()
const hash = b => sha(sha(b))
const txid = b => Buffer.from(hash(b)).reverse().toString('hex')
const u32 = n => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const raw = (prev, script, marker, outputs = 1) => Buffer.concat([u32(1), Buffer.from([1]), prev, u32(0), Buffer.from([0]), u32(0xffffffff), Buffer.from([outputs]), ...Array.from({ length: outputs }, () => Buffer.concat([Buffer.from('e803000000000000', 'hex'), Buffer.from([1, script])])), u32(marker)])
// A BEEF V2 head with one bump: `parent` alone in a block at `height` (its root is its txid).
const bumped = (parent, height) => Buffer.concat([u32(0xefbe0002), Buffer.from([1, 0xfe]), u32(height), Buffer.from([1, 1, 0, 2]), hash(parent)])
const provenBeef = (parent, height) => Buffer.concat([bumped(parent, height), Buffer.from([1, 1, 0]), parent])
let posts = 0
const boot = async (bindings, outboundService) => {
  const mf = new Miniflare({ modules: true, scriptPath: resolve('build/worker/shim.mjs'),
    modulesRules: [{ type: 'ESModule', include: ['**/*.js', '**/*.mjs'] }, { type: 'CompiledWasm', include: ['**/*.wasm'] }],
    compatibilityDate: '2024-01-01', host: '127.0.0.1', port: 0,
    bindings: { SERVER_PRIVATE_KEY: PrivateKey.fromRandom().toHex(), MONITOR_TRIGGER_KEY: 'fixture-monitor', ...bindings },
    d1Databases: ['DB'], kvNamespaces: ['AUTH_SESSIONS'], r2Buckets: ['BLOBS'], outboundService })
  const base = await mf.ready
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    const split = spawnSync('python3', ['-c', `import json, sqlite3, sys
statements, pending = [], ''
for char in sys.stdin.read():
 pending += char
 if char == ';' and sqlite3.complete_statement(pending):
  statements.append(pending); pending = ''
print(json.dumps(statements))`], { input: readFileSync('migrations/' + name, 'utf8'), encoding: 'utf8' })
    assert.equal(split.status, 0)
    await db.batch(JSON.parse(split.stdout).map(sql => db.prepare(sql)))
  }
  const key = PrivateKey.fromRandom()
  let id = 0
  // A session of the fixture's user, or of the user of another key: its own BRC-31 client.
  const session = (of = key) => {
    const client = new AuthFetch(new ProtoWallet(of))
    return async (method, params) => {
      const response = await client.fetch(new URL('/', base).toString(), { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }) })
      assert.equal(response.status, 200)
      return JSON.parse(await response.text())
    }
  }
  const rpc = session()
  const user = (await rpc('findOrInsertUser', [key.toPublicKey().toString()])).result.user.userId
  const exec = (sql, ...args) => db.prepare(sql).bind(...args.map(a => Buffer.isBuffer(a) ? new Uint8Array(a) : a)).run()
  await exec("INSERT INTO output_baskets (user_id, name) VALUES (?, 'default')", user)
  const basket = (await db.prepare('SELECT basket_id FROM output_baskets WHERE user_id = ?').bind(user).first()).basket_id
  const seed = async (n, invalid = false, status = 'unsigned', beef = null) => {
    const parent = raw(Buffer.alloc(32, n), invalid ? 0x00 : 0x51, n)
    const child = raw(hash(parent), 0x51, n + 100)
    await exec("INSERT INTO transactions (user_id, reference, status, is_outgoing, description, txid, raw_tx) VALUES (?, ?, 'completed', 0, 'fixture', ?, ?)", user, `parent-${n}`, txid(parent), parent)
    const parentId = (await db.prepare('SELECT transaction_id FROM transactions WHERE reference = ?').bind(`parent-${n}`).first()).transaction_id
    await exec("INSERT INTO transactions (user_id, reference, status, is_outgoing, description, txid, raw_tx, input_beef) VALUES (?, ?, ?, 1, 'fixture', ?, ?, ?)", user, `child-${n}`, status, txid(child), child, beef && provenBeef(parent, 800000 + n))
    const childId = (await db.prepare('SELECT transaction_id FROM transactions WHERE reference = ?').bind(`child-${n}`).first()).transaction_id
    await exec("INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, provided_by, purpose, type, txid, locking_script, spendable, spent_by) VALUES (?, ?, ?, 0, 1000, 'storage', 'fixture', 'custom', ?, ?, 0, ?)", user, parentId, basket, txid(parent), Buffer.from([invalid ? 0x00 : 0x51]), childId)
    await exec("INSERT INTO proven_tx_reqs (txid, raw_tx, status) VALUES (?, ?, 'unprocessed')", txid(child), child)
    return { child, childId, txid: txid(child), reference: `child-${n}` }
  }
  const row = s => db.prepare('SELECT t.status AS tx_status, r.status AS req_status, r.history, r.attempts FROM transactions t JOIN proven_tx_reqs r ON r.txid = t.txid WHERE t.transaction_id = ?').bind(s.childId).first()
  const locked = async s => {
    const o = await db.prepare('SELECT spendable, spent_by FROM outputs WHERE spent_by = ?').bind(s.childId).first()
    assert.ok(o, 'input was released'); assert.equal(o.spendable, 0)
  }
  return { mf, base, db, rpc, session, exec, seed, row, locked }
}
let failed = 0, ran = 0
const check = async (name, f) => { ran++; try { await f(); console.log(`PASS ${name}`) } catch (e) { failed++; console.log(`FAIL ${name}: ${e.message}`) } }
const { mf, base, db, rpc, exec, seed, row, locked } = await boot({ BROADCASTER: 'arc' }, async request => {
  const url = new URL(request.url)
  if (request.method === 'POST' && url.pathname === '/v1/tx') {
    posts++
    return new Response(JSON.stringify({ status: 465, detail: 'local fixture policy refusal: non-mandatory-script-verify-flag' }), { status: 465, headers: { 'content-type': 'application/json' } })
  }
  return new Response('not found', { status: 404 })
})
try {
  const server = await seed(1)
  await check('a server refusal keeps status and inputs, records word, schedules re-ask', async () => {
    const r = await rpc('processAction', [{ identityKey: '' }, { isNewTx: true, isSendWith: false, isNoSend: false, isDelayed: false, reference: server.reference, txid: server.txid, rawTx: [...server.child], sendWith: [] }])
    assert.ok('result' in r, JSON.stringify(r)); assert.ok(posts > 0)
    const r0 = await row(server); assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'unsent'); await locked(server)
    assert.match(r0.history, /policy refusal/); assert.match(r0.history, /immediateBroadcastHint/)
  })
  const remote = await seed(2, false, 'sending')
  await check('b legacy client false keeps status and inputs, notes and schedules', async () => {
    const r = await rpc('updateTransactionStatusAfterBroadcast', [remote.txid, false]); assert.ok('result' in r, JSON.stringify(r))
    const r0 = await row(remote); assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'unsent'); await locked(remote); assert.match(r0.history, /clientBroadcastHint/)
  })
  const invalid = await seed(3, true)
  await check('c local script refusal alone fails and releases inputs', async () => {
    const before = posts
    await rpc('processAction', [{}, { isNewTx: true, isSendWith: false, isNoSend: false, isDelayed: false, reference: invalid.reference, txid: invalid.txid, rawTx: [...invalid.child], sendWith: [] }])
    const r0 = await row(invalid); assert.match(r0.history, /localScriptVerdict/); assert.equal(r0.tx_status, 'failed'); assert.equal(r0.req_status, 'invalid'); assert.equal(posts, before)
    assert.equal((await db.prepare('SELECT spendable FROM outputs WHERE transaction_id = (SELECT transaction_id FROM transactions WHERE reference = ?)').bind('parent-3').first()).spendable, 1)
  })
  await check('d findProvenTxReqs returns history by txid', async () => {
    const r = await rpc('findProvenTxReqs', [{}, { txids: [server.txid] }]); assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.length, 1); assert.equal(r.result[0].txid, server.txid); assert.match(r.result[0].history, /policy refusal/)
  })
  await check('e client post notes append and reader returns same history', async () => {
    const notes = [{ when: new Date().toISOString(), what: 'immediateBroadcastHint', outcome: 'invalidTx', words: ['fixture client 465 refusal'] }]
    const r = await rpc('updateTransactionStatusAfterBroadcast', [remote.txid, false, { notes }]); assert.ok('result' in r, JSON.stringify(r))
    const found = await rpc('findProvenTxReqs', { txids: [remote.txid] }); assert.ok(found.result, JSON.stringify(found)); assert.match(found.result[0].history, /fixture client 465 refusal/); await locked(remote)
    assert.ok(JSON.parse(found.result[0].history).notes.length >= 3)
  })
  await check('f another identity cannot read or write request history', async () => {
    const stranger = new AuthFetch(new ProtoWallet(PrivateKey.fromRandom()))
    const body = method => JSON.stringify({ jsonrpc: '2.0', id: 99, method, params: method === 'findProvenTxReqs' ? { txids: [server.txid] } : [server.txid, false, { notes: [{ what: 'forged' }] }] })
    for (const method of ['findProvenTxReqs', 'updateTransactionStatusAfterBroadcast']) {
      const response = await stranger.fetch(new URL('/', base).toString(), { method: 'POST', headers: { 'content-type': 'application/json' }, body: body(method) })
      const r = JSON.parse(await response.text())
      if (method === 'findProvenTxReqs') assert.deepEqual(r.result, [])
      else assert.ok(r.error, JSON.stringify(r))
    }
    assert.doesNotMatch((await row(server)).history, /forged/)
  })
  await check('g monitor re-ask treats refusal as a hint and keeps locks', async () => {
    const pending = await seed(4, false, 'sending')
    await exec("UPDATE proven_tx_reqs SET status = 'unsent' WHERE txid = ?", pending.txid)
    const before = posts
    const response = await mf.dispatchFetch(new URL('/monitor/run?key=fixture-monitor', base), { method: 'POST' })
    assert.equal(response.status, 200, await response.text()); assert.ok(posts > before)
    const r0 = await row(pending); assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'unsent'); assert.match(r0.history, /monitorBroadcastHint/); await locked(pending)
  })
  await check('h reader filters, pagination and both argument shapes', async () => {
    const empty = await rpc('findProvenTxReqs', { txids: [] }); assert.ok(empty.result.length > 0)
    const future = await rpc('findProvenTxReqs', { since: '2099-01-01T00:00:00Z' }); assert.deepEqual(future.result, [])
    const page = await rpc('findProvenTxReqs', [{ userId: 999 }, { paged: { limit: 1, offset: 0 }, orderDescending: true, status: ['unsent'] }]); assert.equal(page.result.length, 1)
    const r = await rpc('findProvenTxReqs', { txids: [server.txid], status: ['invalid'] }); assert.deepEqual(r.result, [])
  })
  await check('i client false runs local evaluator against stored parent', async () => {
    const bad = await seed(5, true, 'sending')
    const before = posts
    const r = await rpc('updateTransactionStatusAfterBroadcast', [bad.txid, false]); assert.ok('result' in r, JSON.stringify(r))
    const r0 = await row(bad); assert.equal(r0.tx_status, 'failed'); assert.equal(r0.req_status, 'invalid'); assert.match(r0.history, /localScriptVerdict/); assert.equal(posts, before)
  })
  await check('j concurrent notes preserve both writes and bound history', async () => {
    await Promise.all(['one', 'two'].map(word => rpc('updateTransactionStatusAfterBroadcast', { txid: remote.txid, success: false, notes: [{ when: new Date().toISOString(), what: word }] })))
    let history = JSON.parse((await row(remote)).history); assert.ok(history.notes.some(n => n.what === 'one')); assert.ok(history.notes.some(n => n.what === 'two'))
    const notes = Array.from({ length: 40 }, (_, i) => ({ what: 'bounded', index: i }))
    await rpc('updateTransactionStatusAfterBroadcast', [remote.txid, false, { notes }])
    history = JSON.parse((await row(remote)).history); assert.equal(history.notes.length, 32); assert.ok(history.notes.some(n => n.index === 39)); await locked(remote)
  })
  await check('k a completed request blocks local failure and release', async () => {
    const completed = await seed(6, true, 'sending')
    await exec("UPDATE proven_tx_reqs SET status = 'completed' WHERE txid = ?", completed.txid)
    await rpc('updateTransactionStatusAfterBroadcast', [completed.txid, false])
    const r0 = await row(completed); assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'completed'); await locked(completed)
  })
  await check('l a missing or mismatched local parent gives no failure verdict', async () => {
    for (const [n, bytes] of [[7, null], [8, raw(Buffer.alloc(32, 0xcc), 0x00, 8)]]) {
      const pending = await seed(n, true, 'sending')
      await exec('UPDATE transactions SET raw_tx = ? WHERE reference = ?', bytes, `parent-${n}`)
      await rpc('updateTransactionStatusAfterBroadcast', [pending.txid, false])
      const r0 = await row(pending); assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'unsent'); assert.doesNotMatch(r0.history, /localScriptVerdict/); await locked(pending)
    }
  })
  await check('m toolbox reader orders by update time and since is strict', async () => {
    await exec("UPDATE proven_tx_reqs SET updated_at = '2030-01-01T00:00:00Z' WHERE txid = ?", server.txid)
    const r = await rpc('findProvenTxReqs', [{ partial: {}, orderDescending: true, paged: { limit: 1 } }]); assert.equal(r.result[0].txid, server.txid)
    const exact = await rpc('findProvenTxReqs', { txids: [server.txid], since: '2030-01-01T00:00:00Z' }); assert.deepEqual(exact.result, [])
    await exec("UPDATE proven_tx_reqs SET updated_at = '2000-01-01T00:00:00Z', status = 'completed' WHERE txid = ?", server.txid)
    await rpc('updateTransactionStatusAfterBroadcast', [server.txid, true, { notes: [{ what: 'sync-history' }] }])
    const updated = await rpc('findProvenTxReqs', { txids: [server.txid], since: '2020-01-01T00:00:00Z' }); assert.equal(updated.result.length, 1); assert.match(updated.result[0].history, /sync-history/)
  })
  await check('n reader returns inline raw bytes and R2 input BEEF', async () => {
    const pending = await seed(9, false, 'sending')
    const req = await db.prepare('SELECT proven_tx_req_id FROM proven_tx_reqs WHERE txid = ?').bind(pending.txid).first()
    const bucket = await mf.getR2Bucket('BLOBS')
    const bytes = new Uint8Array(5000).fill(42)
    await bucket.put(`proven_tx_reqs/${req.proven_tx_req_id}/input_beef`, bytes)
    const r = await rpc('findProvenTxReqs', { txids: [pending.txid] }); assert.deepEqual(r.result[0].rawTx, [...pending.child]); assert.deepEqual(r.result[0].inputBeef, [...bytes])
  })
} finally { await mf.dispose() }

// #50 under BROADCASTER arcade: Arcade's 400 carrying status 460 to 475 but 466 is the
// instance's policy answer (B5); 466 is a conflict hint, no word written. The fixture answers every submission with `arcadeCode` and its
// policy with 100 sats per 1,000 bytes; the header service carries each fixture's root.
const POLICY_CODES = [460, 461, 462, 463, 464, 465, 467, 468, 469, 471, 472, 473, 474, 475]
let arcadeCode = 465, arcadePosts = 0, policyReads = 0
const roots = new Map()
const arcade = await boot({ BROADCASTER: 'arcade', ARCADE_URL: 'https://arcade.test', CHAINTRACKS_URL: 'https://headers.test', BEEF_VERIFICATION: 'strict', INTERNALIZE_ZERO_CONF: 'false' }, async request => {
  const url = new URL(request.url)
  const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
  if (url.host === 'arcade.test' && request.method === 'POST' && ['/tx', '/txs'].includes(url.pathname)) {
    arcadePosts++
    return json({ error: 'transaction failed validation', reason: `local fixture policy refusal ${arcadeCode}`, status: arcadeCode, title: 'fixture' }, 400)
  }
  if (url.host === 'arcade.test' && url.pathname === '/policy') {
    policyReads++
    return json({ policy: { miningFee: { satoshis: 100, bytes: 1000 }, maxtxsizepolicy: 10485760 }, timestamp: '2026-10-27T00:00:00Z' })
  }
  if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
    const height = Number(url.searchParams.get('height'))
    return json({ status: 'success', value: { merkleRoot: roots.get(height) ?? '00'.repeat(32), height } })
  }
  return new Response('not found', { status: 404 })
})
const hints = (history, what) => JSON.parse(history).notes.filter(n => n.what === what && n.outcome === 'policyRefusal')
const policyHint = (history, what, code) => {
  const found = hints(history, what).at(-1)
  assert.ok(found, `no ${what} policyRefusal note: ${history}`)
  assert.equal(found.words.code, code); assert.equal(found.words.instance, 'arcade.test')
  assert.deepEqual(found.words.feeAsked, { satoshis: 100, bytes: 1000 })
  assert.equal(found.words.verdict, 'retry at the fee asked')
}
const processTx = s => arcade.rpc('processAction', [{}, { isNewTx: true, isSendWith: false, isNoSend: false, isDelayed: false, reference: s.reference, txid: s.txid, rawTx: [...s.child], sendWith: [] }])
const monitorRun = async () => {
  const response = await arcade.mf.dispatchFetch(new URL('/monitor/run?key=fixture-monitor', arcade.base), { method: 'POST' })
  assert.equal(response.status, 200, await response.text())
}
try {
  const fee = await arcade.seed(21, false, 'unsigned', true)
  await check('o arcade 400 status 465: a hint with the code and the fee asked, status and inputs kept', async () => {
    const r = await processTx(fee); assert.ok('result' in r, JSON.stringify(r)); assert.ok(arcadePosts > 0); assert.ok(policyReads > 0)
    const r0 = await arcade.row(fee); assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'unsent'); await arcade.locked(fee)
    policyHint(r0.history, 'immediateBroadcastHint', 465)
  })
  await check('p the next monitor run re-announces the refused transaction', async () => {
    const before = arcadePosts
    await monitorRun()
    assert.ok(arcadePosts > before, 'the monitor did not re-announce')
    const r0 = await arcade.row(fee); assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'unsent'); await arcade.locked(fee)
    policyHint(r0.history, 'monitorBroadcastHint', 465)
  })
  await check('q the codes 460 to 475 as a table: never failed, inputs held, the code recorded', async () => {
    try {
      for (const [i, code] of POLICY_CODES.entries()) {
        arcadeCode = code
        const s = await arcade.seed(40 + i, false, 'unsigned', true)
        await processTx(s)
        const r0 = await arcade.row(s)
        assert.equal(r0.tx_status, 'sending', `code ${code}`); assert.equal(r0.req_status, 'unsent', `code ${code}`); await arcade.locked(s)
        policyHint(r0.history, 'immediateBroadcastHint', code)
      }
    } finally { arcadeCode = 465 }
  })
  await check('q2 arcade 400 status 466: a conflict hint, no fee read, never doubleSpend or failed, inputs held', async () => {
    try {
      arcadeCode = 466
      const s = await arcade.seed(60, false, 'unsigned', true)
      const reads = policyReads
      await processTx(s)
      const r0 = await arcade.row(s)
      assert.equal(r0.tx_status, 'sending'); assert.equal(r0.req_status, 'unsent'); await arcade.locked(s)
      assert.equal(policyReads, reads, 'a conflict hint read /policy')
      const note = JSON.parse(r0.history).notes.filter(n => n.what === 'immediateBroadcastHint').at(-1)
      assert.equal(note.outcome, 'conflictHint'); assert.equal(note.words.code, 466); assert.equal(note.words.feeAsked, null)
      assert.match(note.words.verdict, /conflict hint/)
    } finally { arcadeCode = 465 }
  })
  await check('q3 a monitor run reads /policy once however many refused rows it re-announces', async () => {
    const unsent = (await arcade.db.prepare("SELECT COUNT(*) AS n FROM proven_tx_reqs WHERE status = 'unsent'").first()).n
    assert.ok(unsent >= 10, `only ${unsent} refused rows`)
    const posts = arcadePosts, reads = policyReads
    await monitorRun()
    assert.ok(arcadePosts - posts >= unsent, `re-announced ${arcadePosts - posts} of ${unsent}`)
    assert.equal(policyReads - reads, 1, `${policyReads - reads} /policy reads in one run`)
  })
  await check('r internalizeAction refused at 465 is not failed; the req is queued with the hint and re-announced', async () => {
    const funding = raw(Buffer.alloc(32, 0xaa), 0x51, 77)
    const subject = raw(hash(funding), 0x51, 78)
    roots.set(800077, Buffer.from(hash(funding)).reverse().toString('hex'))
    const atomic = Buffer.concat([u32(0x01010101), hash(subject), bumped(funding, 800077), Buffer.from([2, 1, 0]), funding, Buffer.from([0]), subject])
    const r = await arcade.rpc('internalizeAction', [{ identityKey: '' }, { tx: atomic.toString('hex'), outputs: [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'fee' } }], description: 'fee refusal', labels: [], seekPermission: false }])
    assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
    const t = await arcade.db.prepare('SELECT status FROM transactions WHERE txid = ?').bind(txid(subject)).first()
    assert.equal(t.status, 'sending')
    let req = await arcade.db.prepare('SELECT status, history FROM proven_tx_reqs WHERE txid = ?').bind(txid(subject)).first()
    assert.equal(req.status, 'unsent'); policyHint(req.history, 'internalizeBroadcastHint', 465)
    const before = arcadePosts
    await monitorRun()
    assert.ok(arcadePosts > before, 'the monitor did not re-announce')
    req = await arcade.db.prepare('SELECT status, history FROM proven_tx_reqs WHERE txid = ?').bind(txid(subject)).first()
    assert.equal(req.status, 'unsent'); policyHint(req.history, 'monitorBroadcastHint', 465)
    assert.equal((await arcade.db.prepare('SELECT status FROM transactions WHERE txid = ?').bind(txid(subject)).first()).status, 'sending')
  })
} finally { await arcade.mf.dispose() }

// #50 under production's settings (INTERNALIZE_ZERO_CONF=true, BROADCASTER=arcade): an
// inbound payment Arcade refuses is never credited as spendable; one it accepts (202, then
// SEEN_ON_NETWORK on its event stream) is credited as before, and a refused one becomes
// spendable when a later announce is seen. And one row per inbound payment: two calls for one
// payment at the same moment leave one transaction row and one spendable output (checks w, x, y).
let zeroCode = 461, zeroPosts = 0
// A height listed in `together` has its header answered when two calls have asked for it, so the
// two calls of one payment are let on into the storage at the same moment; `met` counts the
// heights at which two calls met. The two answers are given while D1 is busy with a statement of
// the fixture's own (HOLD, `holding`): workerd finds both answers waiting when the statement
// ends, and the two calls go on from there statement for statement.
const together = new Map()
let met = 0, holding = null
const HOLD = 'WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 800000) SELECT COUNT(*) AS c FROM n'
const zero = await boot({ BROADCASTER: 'arcade', ARCADE_URL: 'https://arcade.test', CHAINTRACKS_URL: 'https://headers.test', BEEF_VERIFICATION: 'strict', INTERNALIZE_ZERO_CONF: 'true' }, async request => {
  const url = new URL(request.url)
  const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
  if (url.host === 'arcade.test' && request.method === 'POST' && ['/tx', '/txs'].includes(url.pathname)) {
    zeroPosts++
    if (zeroCode === 0) return json({ txStatus: 'RECEIVED' }, 202)
    return json({ error: 'transaction failed validation', reason: `local fixture refusal ${zeroCode}`, status: zeroCode, title: 'fixture' }, 400)
  }
  if (url.host === 'arcade.test' && url.pathname === '/events') {
    const frame = { txid: url.searchParams.get('callbackToken'), txStatus: 'SEEN_ON_NETWORK' }
    return new Response(`data: ${JSON.stringify(frame)}\n\n`, { headers: { 'content-type': 'text/event-stream' } })
  }
  if (url.host === 'arcade.test' && url.pathname === '/policy') return json({ policy: { miningFee: { satoshis: 100, bytes: 1000 } } })
  if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
    const height = Number(url.searchParams.get('height'))
    const waiting = together.get(height)
    if (waiting) {
      await new Promise(go => {
        waiting.push(go)
        if (waiting.length === 2) {
          met++
          holding = zero.db.prepare(HOLD).first()
          setTimeout(() => { for (const each of waiting) each() }, 25)
        } else setTimeout(() => { together.delete(height); go() }, 5000)
      })
    }
    return json({ status: 'success', value: { merkleRoot: roots.get(height) ?? '00'.repeat(32), height } })
  }
  return new Response('not found', { status: 404 })
})
const sender = PrivateKey.fromRandom().toPublicKey().toString()
// An AtomicBEEF of a fresh subject of `outputs` outputs, internalized as a wallet payment of its
// output `index`; its funding is proven at 800000 + n.
const payment = (n, lock = 0x51, outputs = 1) => {
  const funding = raw(Buffer.alloc(32, n), lock, n)
  const subject = raw(hash(funding), 0x51, n + 1, outputs)
  roots.set(800000 + n, Buffer.from(hash(funding)).reverse().toString('hex'))
  const atomic = Buffer.concat([u32(0x01010101), hash(subject), bumped(funding, 800000 + n), Buffer.from([2, 1, 0]), funding, Buffer.from([0]), subject])
  return { atomic, txid: txid(subject) }
}
const internalize = (rpc, n, atomic, index = 0) => rpc('internalizeAction', [{ identityKey: '' }, { tx: atomic.toString('hex'), outputs: [{ outputIndex: index, protocol: 'wallet payment', paymentRemittance: { derivationPrefix: 'cHJlZml4', derivationSuffix: Buffer.from(`s${n + index}`).toString('base64'), senderIdentityKey: sender } }], description: 'zero-conf payment', labels: [], seekPermission: false }])
const inbound = async (n, lock = 0x51) => {
  const { atomic, txid: t } = payment(n, lock)
  return { r: await internalize(zero.rpc, n, atomic), txid: t }
}
const credit = async t => (await zero.db.prepare('SELECT COALESCE(SUM(spendable), 0) AS n, COUNT(*) AS c FROM outputs WHERE txid = ?').bind(t).first())
const zeroReq = t => zero.db.prepare('SELECT r.status, r.history, t.status AS tx_status FROM proven_tx_reqs r JOIN transactions t ON t.txid = r.txid WHERE r.txid = ?').bind(t).first()
const zeroMonitor = async () => {
  const response = await zero.mf.dispatchFetch(new URL('/monitor/run?key=fixture-monitor', zero.base), { method: 'POST' })
  assert.equal(response.status, 200, await response.text())
}
let held465
try {
  for (const [n, code] of [[120, 461], [130, 465]]) {
    await check(`s zero-conf: a payment refused with ${code} leaves no spendable output, the req unsent with the hint`, async () => {
      zeroCode = code
      const { r, txid: t } = await inbound(n)
      assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
      const c = await credit(t); assert.equal(c.c, 1); assert.equal(c.n, 0, `code ${code}: the refused payment was credited spendable`)
      const req = await zeroReq(t); assert.equal(req.status, 'unsent'); assert.equal(req.tx_status, 'sending')
      const hint = hints(req.history, 'internalizeBroadcastHint').at(-1); assert.ok(hint, req.history); assert.equal(hint.words.code, code)
      if (code === 465) held465 = t
    })
  }
  await check('t zero-conf: a payment Arcade accepts is credited spendable as before', async () => {
    zeroCode = 0
    const { r, txid: t } = await inbound(140)
    assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
    const c = await credit(t); assert.equal(c.c, 1); assert.equal(c.n, 1)
    const req = await zeroReq(t); assert.equal(req.status, 'unmined'); assert.equal(req.tx_status, 'unproven')
  })
  await check('u zero-conf: the refused payment becomes spendable when a later announce is seen', async () => {
    assert.ok(held465, 'no held payment from s')
    zeroCode = 465; await zeroMonitor(); assert.equal((await credit(held465)).n, 0)
    zeroCode = 0; await zeroMonitor()
    const req = await zeroReq(held465); assert.equal((await credit(held465)).n, 1); assert.equal(req.status, 'unmined'); assert.equal(req.tx_status, 'unproven')
  })
  await check('v internalize: a script the local check refuses is refused before any write or announce', async () => {
    const before = zeroPosts
    const { r, txid: t } = await inbound(150, 0x00)
    assert.ok(r.error, JSON.stringify(r)); assert.match(JSON.stringify(r.error), /local script check/)
    assert.equal(zeroPosts, before); assert.equal((await credit(t)).c, 0)
    assert.equal(await zero.db.prepare('SELECT 1 FROM proven_tx_reqs WHERE txid = ?').bind(t).first(), null)
  })
  // One AtomicBEEF posted by two sessions of the user at the same moment, payment after payment.
  // The header service answers the two calls of a payment together, so both are in the storage
  // at once and their statements reach D1 in whatever order they fall.
  const other = zero.session()
  const balance = async rpc => (await rpc('getBalance', [{ identityKey: '' }, { basket: 'default' }])).result.balance
  const count = async (sql, ...binds) => (await zero.db.prepare(sql).bind(...binds).first()).c
  const atOnce = async (n, calls) => {
    const before = met
    together.set(800000 + n, [])
    const answers = await Promise.all(calls.map(call => call()))
    assert.equal(met - before, 1, `payment ${n}: the two calls were not in flight together`)
    assert.equal((await holding).c, 800000)
    for (const r of answers) { assert.ok(r.result, `payment ${n}: ${JSON.stringify(r)}`); assert.equal(r.result.accepted, true, `payment ${n}`) }
    return answers.map(r => r.result)
  }
  const PAIRS = 40
  await check(`w two calls at once for one payment, ${PAIRS} payments: one transaction row, one spendable output, the balance of one payment, both accepted`, async () => {
    zeroCode = 0
    let expected = await balance(zero.rpc)
    assert.equal(await balance(other), expected)
    for (let i = 0; i < PAIRS; i++) {
      const n = 1000 + 2 * i
      const { atomic, txid: t } = payment(n)
      const answers = await atOnce(n, [zero.rpc, other].map(rpc => () => internalize(rpc, n, atomic, 0)))
      assert.equal(await count('SELECT COUNT(*) AS c FROM transactions WHERE txid = ?', t), 1, `payment ${n}: transaction rows`)
      const c = await credit(t); assert.equal(c.c, 1, `payment ${n}: output rows`); assert.equal(c.n, 1, `payment ${n}: spendable outputs`)
      expected += 1000
      assert.equal(await balance(zero.rpc), expected, `payment ${n}: the balance`)
      assert.deepEqual(answers.map(a => a.isMerge).sort(), [false, true], `payment ${n}: one answer new, one a merge`)
      assert.equal(answers[0].satoshis + answers[1].satoshis, 1000, `payment ${n}: the satoshis of the two answers`)
      const req = await zeroReq(t); assert.equal(req.status, 'unmined', `payment ${n}`); assert.equal(req.tx_status, 'unproven', `payment ${n}`)
    }
  })
  // The transaction is held already (its output 0, by a call alone); its output 1 is then
  // internalized by two calls at once, both merges onto the one transaction row.
  const SECOND = 20
  await check(`x two calls at once for another output of a transaction held, ${SECOND} payments: one row for the output, one payment more on the balance, both accepted as merges`, async () => {
    zeroCode = 0
    let expected = await balance(zero.rpc)
    for (let i = 0; i < SECOND; i++) {
      const n = 2000 + 2 * i
      const { atomic, txid: t } = payment(n, 0x51, 2)
      const first = await internalize(zero.rpc, n, atomic, 0)
      assert.ok(first.result, JSON.stringify(first)); assert.equal(first.result.isMerge, false); assert.equal(first.result.satoshis, 1000)
      const answers = await atOnce(n, [zero.rpc, other].map(rpc => () => internalize(rpc, n, atomic, 1)))
      assert.equal(await count('SELECT COUNT(*) AS c FROM transactions WHERE txid = ?', t), 1, `payment ${n}: transaction rows`)
      const rows = await zero.db.prepare('SELECT vout, COUNT(*) AS c, SUM(spendable) AS n FROM outputs WHERE txid = ? GROUP BY vout ORDER BY vout').bind(t).all()
      assert.deepEqual(rows.results, [{ vout: 0, c: 1, n: 1 }, { vout: 1, c: 1, n: 1 }], `payment ${n}: output rows`)
      expected += 2000
      assert.equal(await balance(zero.rpc), expected, `payment ${n}: the balance`)
      assert.deepEqual(answers.map(a => a.isMerge), [true, true], `payment ${n}: both merges`)
      assert.equal(answers[0].satoshis + answers[1].satoshis, 1000, `payment ${n}: the satoshis of the two answers`)
    }
  })
  // A new user's first two calls, at once: a basket insertion into a basket, under a label and
  // with a tag, none of which the storage holds yet (nor the user's default basket).
  const USERS = 10
  await check(`y the first two calls of a new user at once, ${USERS} users: a basket insertion with a new basket, label and tag leaves one row of each, both accepted`, async () => {
    zeroCode = 0
    for (let i = 0; i < USERS; i++) {
      const n = 3000 + 2 * i
      const key = PrivateKey.fromRandom()
      const sessions = [zero.session(key), zero.session(key)]
      let user
      for (const rpc of sessions) user = (await rpc('findOrInsertUser', [key.toPublicKey().toString()])).result.user.userId
      const { atomic, txid: t } = payment(n)
      const args = { tx: atomic.toString('hex'), outputs: [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'kept', customInstructions: 'fixture', tags: ['seat'] } }], description: 'kept output', labels: ['paid'], seekPermission: false }
      await atOnce(n, sessions.map(rpc => () => rpc('internalizeAction', [{ identityKey: '' }, args])))
      assert.equal(await count('SELECT COUNT(*) AS c FROM transactions WHERE user_id = ? AND txid = ?', user, t), 1, `user ${i}: transaction rows`)
      assert.equal(await count('SELECT COUNT(*) AS c FROM outputs WHERE user_id = ? AND txid = ? AND spendable = 1', user, t), 1, `user ${i}: output rows`)
      assert.equal(await count('SELECT COUNT(*) AS c FROM outputs WHERE user_id = ?', user), 1, `user ${i}: output rows`)
      const baskets = await zero.db.prepare('SELECT name FROM output_baskets WHERE user_id = ? ORDER BY name').bind(user).all()
      assert.deepEqual(baskets.results.map(b => b.name), ['default', 'kept'], `user ${i}: baskets`)
      assert.equal(await count('SELECT COUNT(*) AS c FROM tx_labels WHERE user_id = ?', user), 1, `user ${i}: labels`)
      assert.equal(await count('SELECT COUNT(*) AS c FROM tx_labels_map m JOIN tx_labels l ON l.tx_label_id = m.tx_label_id WHERE l.user_id = ?', user), 1, `user ${i}: label map rows`)
      assert.equal(await count('SELECT COUNT(*) AS c FROM output_tags WHERE user_id = ?', user), 1, `user ${i}: tags`)
      assert.equal(await count('SELECT COUNT(*) AS c FROM output_tags_map m JOIN output_tags g ON g.output_tag_id = m.output_tag_id WHERE g.user_id = ?', user), 1, `user ${i}: tag map rows`)
    }
  })
  // A call names each output index once. One call, alone, whose `outputs` name an index twice is
  // refused where its specifications are read: `-32602` with the reference's message, every
  // table of the storage as it was, and nothing announced.
  const UNIQUE = 'The outputs parameter must be unique outputIndex values'
  const pay = (index, word) => ({ outputIndex: index, protocol: 'wallet payment', paymentRemittance: { derivationPrefix: 'cHJlZml4', derivationSuffix: Buffer.from(word).toString('base64'), senderIdentityKey: sender } })
  const insert = (index, basket) => ({ outputIndex: index, protocol: 'basket insertion', insertionRemittance: { basket, customInstructions: 'fixture', tags: ['seat'] } })
  const named = (atomic, outputs, rpc = zero.rpc) => rpc('internalizeAction', [{ identityKey: '' }, { tx: atomic.toString('hex'), outputs, description: 'named outputs', labels: ['paid'], seekPermission: false }])
  const bytes = (_, v) => v instanceof ArrayBuffer ? Buffer.from(v).toString('hex') : ArrayBuffer.isView(v) ? Buffer.from(v.buffer, v.byteOffset, v.byteLength).toString('hex') : v
  // Every row of every table of the storage.
  const stored = async () => {
    const tables = await zero.db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name").all()
    const rows = {}
    for (const { name } of tables.results) rows[name] = JSON.stringify((await zero.db.prepare(`SELECT * FROM ${name}`).all()).results, bytes)
    return rows
  }
  const refused = async (what, atomic, outputs, message = UNIQUE) => {
    const before = await stored(), posts = zeroPosts, held = await balance(zero.rpc)
    const r = await named(atomic, outputs)
    assert.deepEqual(r.error, { code: -32602, message }, `${what}: ${JSON.stringify(r)}`)
    assert.deepEqual(await stored(), before, `${what}: the storage is not as it was`)
    assert.equal(zeroPosts, posts, `${what}: announced`)
    assert.equal(await balance(zero.rpc), held, `${what}: the balance`)
  }
  const accepted = async (what, atomic, outputs, isMerge, satoshis) => {
    const r = await named(atomic, outputs)
    assert.ok(r.result, `${what}: ${JSON.stringify(r)}`)
    assert.deepEqual([r.result.accepted, r.result.isMerge, r.result.satoshis], [true, isMerge, satoshis], what)
  }
  const outputRows = async t => (await zero.db.prepare('SELECT o.vout, o.spendable, o.change, o.type, b.name AS basket FROM outputs o JOIN output_baskets b ON b.basket_id = o.basket_id WHERE o.txid = ? ORDER BY o.vout').bind(t).all()).results
  await check('z1 one call naming output 0 of a new transaction twice as a wallet payment: refused, no row written, nothing announced; named once it is then accepted as new', async () => {
    zeroCode = 0
    const { atomic, txid: t } = payment(4000)
    const held = await balance(zero.rpc)
    await refused('twice', atomic, [pay(0, 'one'), pay(0, 'one')])
    await refused('twice, another derivation', atomic, [pay(0, 'one'), pay(0, 'two')])
    assert.equal(await count('SELECT COUNT(*) AS c FROM transactions WHERE txid = ?', t), 0)
    await accepted('once', atomic, [pay(0, 'one')], false, 1000)
    assert.equal(await count('SELECT COUNT(*) AS c FROM transactions WHERE txid = ? AND satoshis = 1000', t), 1)
    assert.deepEqual(await outputRows(t), [{ vout: 0, spendable: 1, change: 1, type: 'P2PKH', basket: 'default' }])
    const req = await zeroReq(t); assert.equal(req.status, 'unmined'); assert.equal(req.tx_status, 'unproven')
    assert.equal(await balance(zero.rpc), held + 1000)
  })
  await check('z2 one call naming output 0 of a new transaction twice as a basket insertion, two baskets: refused, no row and no basket written; named once it is then accepted as new', async () => {
    zeroCode = 0
    const { atomic, txid: t } = payment(4010)
    await refused('twice', atomic, [insert(0, 'first shelf'), insert(0, 'second shelf')])
    assert.equal(await count("SELECT COUNT(*) AS c FROM output_baskets WHERE name IN ('first shelf', 'second shelf')"), 0)
    await accepted('once', atomic, [insert(0, 'first shelf')], false, 0)
    assert.deepEqual(await outputRows(t), [{ vout: 0, spendable: 1, change: 0, type: 'custom', basket: 'first shelf' }])
    assert.equal((await zeroReq(t)).status, 'unmined')
  })
  await check('z3 a transaction held, one call naming its output 1 twice as a wallet payment: refused, the rows as they were; named once it is then accepted as a merge', async () => {
    zeroCode = 0
    const { atomic, txid: t } = payment(4020, 0x51, 2)
    const held = await balance(zero.rpc)
    await accepted('output 0, a call alone', atomic, [pay(0, 'one')], false, 1000)
    await refused('twice', atomic, [pay(1, 'two'), pay(1, 'two')])
    assert.deepEqual((await outputRows(t)).map(o => o.vout), [0])
    await accepted('once', atomic, [pay(1, 'two')], true, 1000)
    assert.equal(await count('SELECT COUNT(*) AS c FROM transactions WHERE txid = ?', t), 1)
    assert.deepEqual((await outputRows(t)).map(o => [o.vout, o.spendable, o.basket]), [[0, 1, 'default'], [1, 1, 'default']])
    assert.equal(await balance(zero.rpc), held + 2000)
  })
  await check('z4 one call naming output 0 of a new transaction as a wallet payment and as a basket insertion: refused in either order, no row written; the wallet payment alone is then accepted as new', async () => {
    zeroCode = 0
    const { atomic, txid: t } = payment(4030)
    const held = await balance(zero.rpc)
    await refused('the payment first', atomic, [pay(0, 'one'), insert(0, 'third shelf')])
    await refused('the insertion first', atomic, [insert(0, 'third shelf'), pay(0, 'one')])
    assert.equal(await count("SELECT COUNT(*) AS c FROM output_baskets WHERE name = 'third shelf'"), 0)
    await accepted('once', atomic, [pay(0, 'one')], false, 1000)
    assert.deepEqual(await outputRows(t), [{ vout: 0, spendable: 1, change: 1, type: 'P2PKH', basket: 'default' }])
    assert.equal(await balance(zero.rpc), held + 1000)
  })
  await check('z5 an index named twice is refused whatever the storage holds and wherever it stands among the outputs; an index out of range is answered first; indexes named once are accepted', async () => {
    zeroCode = 0
    const { atomic, txid: t } = payment(4040, 0x51, 3)
    await accepted('outputs 0 and 1, each once', atomic, [pay(0, 'one'), pay(1, 'two')], false, 2000)
    await refused('an output held, twice', atomic, [pay(0, 'one'), pay(0, 'one')])
    await refused('an output held, as a payment and as an insertion', atomic, [pay(1, 'two'), insert(1, 'fourth shelf')])
    await refused('twice, another between', atomic, [pay(2, 'three'), pay(0, 'one'), insert(2, 'fourth shelf')])
    await refused('an index out of range among them', atomic, [pay(2, 'three'), pay(2, 'three'), pay(3, 'four')], 'Output index 3 is out of range (transaction has 3 outputs)')
    await accepted('output 2, once, with one held', atomic, [pay(0, 'one'), pay(2, 'three')], true, 1000)
    assert.deepEqual((await outputRows(t)).map(o => [o.vout, o.spendable, o.basket]), [[0, 1, 'default'], [1, 1, 'default'], [2, 1, 'default']])
  })
  await check('z6 the first call of a new user naming an index twice: refused, nothing announced; the one row written is the default basket a call finds or makes for its user first', async () => {
    zeroCode = 0
    const key = PrivateKey.fromRandom()
    const rpc = zero.session(key)
    const user = (await rpc('findOrInsertUser', [key.toPublicKey().toString()])).result.user.userId
    const { atomic } = payment(4050)
    const before = await stored(), posts = zeroPosts
    const r = await named(atomic, [pay(0, 'one'), pay(0, 'one')], rpc)
    assert.deepEqual(r.error, { code: -32602, message: UNIQUE }, JSON.stringify(r))
    const after = await stored()
    assert.deepEqual(Object.keys(after).filter(name => after[name] !== before[name]), ['output_baskets'])
    const baskets = await zero.db.prepare('SELECT name FROM output_baskets WHERE user_id = ?').bind(user).all()
    assert.deepEqual(baskets.results.map(b => b.name), ['default'])
    assert.equal(JSON.parse(after.output_baskets).length, JSON.parse(before.output_baskets).length + 1)
    assert.equal(zeroPosts, posts)
  })
} finally { await zero.mf.dispose() }

// A coin's row is marked from a checked proof of its spender, and a call takes back what it
// wrote unless its transaction is taken: two users, production's settings. The owner (B, the
// fixture's user) holds a confirmed coin; the caller (A, another identity) internalizes a
// transaction naming it. Every row of the owner and its balance are read before and after.
let markArcade = 'accept', markWoc = 'unknown', markPosts = 0
const markProofs = new Map(), markMined = new Set()
const marks = await boot({ BROADCASTER: 'arcade', ARCADE_URL: 'https://arcade.test', CHAINTRACKS_URL: 'https://headers.test', BEEF_VERIFICATION: 'strict', INTERNALIZE_ZERO_CONF: 'true' }, async request => {
  const url = new URL(request.url)
  const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
  if (url.host === 'arcade.test' && request.method === 'POST' && ['/tx', '/txs'].includes(url.pathname)) {
    markPosts++
    if (markArcade === 'invalid') return json({ error: 'local fixture structural refusal' }, 400)
    if (markArcade === 'policy') return json({ error: 'transaction failed validation', reason: 'local fixture policy refusal 465', status: 465, title: 'fixture' }, 400)
    return json({ txStatus: 'RECEIVED' }, 202)
  }
  if (url.host === 'arcade.test' && url.pathname === '/events') {
    const txStatus = { accept: 'SEEN_ON_NETWORK', double: 'DOUBLE_SPEND_ATTEMPTED', unverified: 'RECEIVED' }[markArcade] ?? 'RECEIVED'
    return new Response(`data: ${JSON.stringify({ txid: url.searchParams.get('callbackToken'), txStatus })}\n\n`, { headers: { 'content-type': 'text/event-stream' } })
  }
  if (url.host === 'arcade.test' && url.pathname === '/policy') return json({ policy: { miningFee: { satoshis: 100, bytes: 1000 } } })
  if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
    const height = Number(url.searchParams.get('height'))
    return json({ status: 'success', value: { merkleRoot: roots.get(height) ?? '00'.repeat(32), height } })
  }
  if (url.host === 'api.whatsonchain.com' && request.method === 'POST' && url.pathname.endsWith('/txs/status')) {
    const { txids } = JSON.parse(await request.text())
    if (markWoc === 'error' && !txids.some(t => markMined.has(t))) return new Response('local fixture refusal', { status: 400 })
    return json(txids.map(t => markMined.has(t) ? { txid: t, blockhash: 'cd'.repeat(32), blockheight: 900100, confirmations: 1 } : { txid: t, error: 'unknown' }))
  }
  const m = url.pathname.match(/^\/v1\/tx\/([0-9a-f]{64})$/)
  if (url.host.startsWith('arc.') && request.method === 'GET' && m && markProofs.has(m[1])) {
    const p = markProofs.get(m[1])
    return json({ txid: m[1], txStatus: 'MINED', blockHeight: p.height, blockHash: 'cd'.repeat(32), merklePath: p.hex })
  }
  return new Response('not found', { status: 404 })
})
try {
  const display = b => Buffer.from(b).reverse().toString('hex')
  const owner = (await marks.db.prepare('SELECT user_id FROM users ORDER BY user_id LIMIT 1').first()).user_id
  const ownerBasket = (await marks.db.prepare("SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = 'default'").bind(owner).first()).basket_id
  const callerKey = PrivateKey.fromRandom()
  const caller = marks.session(callerKey)
  await caller('findOrInsertUser', [callerKey.toPublicKey().toString()])
  const ownerBalance = async () => (await marks.rpc('getBalance', [{ identityKey: '' }, { basket: 'default' }])).result.balance
  const ownerRows = async () => JSON.stringify([
    (await marks.db.prepare('SELECT transaction_id, status, proven_tx_id, txid FROM transactions WHERE user_id = ? ORDER BY transaction_id').bind(owner).all()).results,
    (await marks.db.prepare('SELECT output_id, spendable, spent_by, satoshis, basket_id FROM outputs WHERE user_id = ? ORDER BY output_id').bind(owner).all()).results])
  // The owner's confirmed coin at output 0 of a parent proven at 810000 + n.
  const coin = async n => {
    const parent = raw(Buffer.alloc(32, n), 0x51, n)
    roots.set(810000 + n, display(hash(parent)))
    await marks.exec("INSERT INTO transactions (user_id, reference, status, is_outgoing, description, txid, raw_tx) VALUES (?, ?, 'completed', 0, 'owner coin', ?, ?)", owner, `coin-${n}`, txid(parent), parent)
    const tx = (await marks.db.prepare('SELECT transaction_id FROM transactions WHERE reference = ?').bind(`coin-${n}`).first()).transaction_id
    await marks.exec("INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, provided_by, purpose, type, txid, locking_script, spendable, change) VALUES (?, ?, ?, 0, 1000, 'storage', 'change', 'P2PKH', ?, ?, 1, 1)", owner, tx, ownerBasket, txid(parent), Buffer.from([0x51]))
    const output = (await marks.db.prepare('SELECT output_id FROM outputs WHERE transaction_id = ?').bind(tx).first()).output_id
    return { parent, height: 810000 + n, output }
  }
  const spendOf = (c, marker) => raw(hash(c.parent), 0x51, marker)
  const withParent = (c, subject) => Buffer.concat([u32(0x01010101), hash(subject), bumped(c.parent, c.height), Buffer.from([2, 1, 0]), c.parent, Buffer.from([0]), subject])
  const bare = subject => Buffer.concat([u32(0x01010101), hash(subject), u32(0xefbe0002), Buffer.from([0, 1, 0]), subject])
  const take = (atomic, n) => caller('internalizeAction', [{ identityKey: '' }, { tx: atomic.toString('hex'), outputs: [{ outputIndex: 0, protocol: 'wallet payment', paymentRemittance: { derivationPrefix: 'cHJlZml4', derivationSuffix: Buffer.from(`m${n}`).toString('base64'), senderIdentityKey: sender } }], description: 'marks', labels: [], seekPermission: false }])
  const coinRow = c => marks.db.prepare('SELECT spendable, spent_by FROM outputs WHERE output_id = ?').bind(c.output).first()
  const callerTx = t => marks.db.prepare('SELECT t.status FROM transactions t JOIN users u ON u.user_id = t.user_id WHERE t.txid = ? AND u.identity_key = ?').bind(t, callerKey.toPublicKey().toString()).first()
  const unchanged = async (label, f) => {
    const rows = await ownerRows(), balance = await ownerBalance()
    const r = await f()
    assert.equal(await ownerRows(), rows, `${label}: the owner's rows moved`)
    assert.equal(await ownerBalance(), balance, `${label}: the owner's balance moved`)
    return r
  }
  await check('mk1 B1: a subject with neither proof nor parent, naming another user\'s coin, refused before any write as the reference refuses it; the owner\'s rows and balance as they were', async () => {
    markArcade = 'invalid'; markWoc = 'unknown'
    const c = await coin(201); const subject = spendOf(c, 202)
    const posts = markPosts
    const r = await unchanged('B1', () => take(bare(subject), 201))
    assert.deepEqual(r.error, { code: -32602, message: 'The tx parameter must be valid AtomicBEEF' }, JSON.stringify(r))
    assert.equal(await callerTx(txid(subject)), null); assert.equal(markPosts, posts)
  })
  for (const [label, arcadeAnswer] of [['invalid', 'invalid'], ['double-spend', 'double']]) {
    await check(`mk2 B1 with the parent: the broadcaster's ${label} and the second source's "not known" leave the owner's rows and balance as they were; the caller's own row is failed`, async () => {
      markArcade = arcadeAnswer; markWoc = 'unknown'
      const c = await coin(arcadeAnswer === 'invalid' ? 211 : 221); const subject = spendOf(c, arcadeAnswer === 'invalid' ? 212 : 222)
      const r = await unchanged(`B1 ${label}`, () => take(withParent(c, subject), arcadeAnswer === 'invalid' ? 211 : 221))
      assert.ok(r.error, JSON.stringify(r)); assert.match(r.error.message, /rejected by network/)
      assert.equal((await callerTx(txid(subject))).status, 'failed')
    })
  }
  await check('mk3 B1\': a policy refusal (465) leaves the owner\'s rows and balance as they were', async () => {
    markArcade = 'policy'; markWoc = 'unknown'
    const c = await coin(231); const subject = spendOf(c, 232)
    const r = await unchanged('B1\' policy', () => take(withParent(c, subject), 231))
    assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
    assert.equal((await callerTx(txid(subject))).status, 'sending')
  })
  await check('mk4 B1\': no verdict under INTERNALIZE_ZERO_CONF=true leaves the owner\'s rows and balance as they were; the caller\'s payment is credited as before', async () => {
    markArcade = 'unverified'; markWoc = 'unknown'
    const c = await coin(241); const subject = spendOf(c, 242)
    const r = await unchanged('B1\' transient', () => take(withParent(c, subject), 241))
    assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
    const credit = await marks.db.prepare('SELECT spendable FROM outputs WHERE txid = ?').bind(txid(subject)).first()
    assert.equal(credit.spendable, 1)
  })
  for (const [label, woc] of [['errs', 'error'], ['answers "not known"', 'unknown']]) {
    await check(`mk5 B4: the owner's mined payment, proven, its request purged; another user internalizes its bytes, the broadcaster refuses, the second source ${label}: the owner's rows and balance as they were`, async () => {
      markArcade = 'invalid'; markWoc = woc
      const n = woc === 'error' ? 251 : 261
      const c = await coin(n)
      // The owner's own payment T spends the coin; T is mined and proven, its change is the owner's.
      const t = spendOf(c, n + 1)
      await marks.exec('UPDATE outputs SET spendable = 0 WHERE output_id = ?', c.output)
      await marks.exec("INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) VALUES (?, 900200, 0, ?, ?, ?, ?)", txid(t), 'cd'.repeat(32), display(hash(t)), Buffer.from([0]), t)
      const proven = (await marks.db.prepare('SELECT proven_tx_id FROM proven_txs WHERE txid = ?').bind(txid(t)).first()).proven_tx_id
      await marks.exec("INSERT INTO transactions (user_id, reference, status, is_outgoing, description, txid, raw_tx, proven_tx_id) VALUES (?, ?, 'completed', 1, 'owner payment', ?, ?, ?)", owner, `pay-${n}`, txid(t), t, proven)
      const tx = (await marks.db.prepare('SELECT transaction_id FROM transactions WHERE reference = ?').bind(`pay-${n}`).first()).transaction_id
      await marks.exec('UPDATE outputs SET spent_by = ? WHERE output_id = ?', tx, c.output)
      await marks.exec("INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, provided_by, purpose, type, txid, locking_script, spendable, change) VALUES (?, ?, ?, 0, 1000, 'storage', 'change', 'P2PKH', ?, ?, 1, 1)", owner, tx, ownerBasket, txid(t), Buffer.from([0x51]))
      const r = await unchanged(`B4 ${label}`, () => take(withParent(c, t), n))
      if (woc === 'error') {
        // A refusal the second source could not answer: held, the caller's output not credited.
        assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
        const held = await marks.db.prepare('SELECT o.spendable FROM outputs o JOIN users u ON u.user_id = o.user_id WHERE o.txid = ? AND u.identity_key = ?').bind(txid(t), callerKey.toPublicKey().toString()).first()
        assert.equal(held.spendable, 0)
      }
      else { assert.ok(r.error, JSON.stringify(r)); assert.equal((await callerTx(txid(t))).status, 'failed') }
    })
  }
  let real
  await check('mk6 a real spend of another user\'s coin: the call leaves the owner\'s coin as it was', async () => {
    markArcade = 'accept'; markWoc = 'unknown'
    const c = await coin(271); const subject = spendOf(c, 272)
    real = { c, subject }
    const r = await unchanged('real spend, at the call', () => take(withParent(c, subject), 271))
    assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
  })
  await check('mk7 the same spend is marked when its checked proof is stored', async () => {
    assert.ok(real, 'no spend from mk6')
    const id = txid(real.subject), leaf = hash(real.subject), sibling = Buffer.alloc(32, 0x5b)
    const height = 900100
    const path = Buffer.concat([Buffer.from([0xfe]), u32(height), Buffer.from([1, 2, 0, 2]), leaf, Buffer.from([1, 0]), sibling])
    roots.set(height, display(hash(Buffer.concat([leaf, sibling]))))
    markProofs.set(id, { hex: path.toString('hex'), height }); markMined.add(id)
    const balance = await ownerBalance()
    const response = await marks.mf.dispatchFetch(new URL('/monitor/run?key=fixture-monitor', marks.base), { method: 'POST' })
    assert.equal(response.status, 200, await response.text())
    assert.ok(await marks.db.prepare('SELECT 1 FROM proven_txs WHERE txid = ?').bind(id).first(), 'the proof was not stored')
    assert.deepEqual({ ...(await coinRow(real.c)) }, { spendable: 0, spent_by: null })
    assert.equal(await ownerBalance(), balance - 1000)
  })
} finally { await marks.mf.dispose() }
console.log(`Tracker Worker witnesses: ${ran - failed} passed; ${failed} failed`)
if (failed) process.exitCode = 1
