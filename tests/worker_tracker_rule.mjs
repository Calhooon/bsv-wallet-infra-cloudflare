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
const raw = (prev, script, marker) => Buffer.concat([u32(1), Buffer.from([1]), prev, u32(0), Buffer.from([0]), u32(0xffffffff), Buffer.from([1]), Buffer.from('e803000000000000', 'hex'), Buffer.from([1, script]), u32(marker)])
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
  const client = new AuthFetch(new ProtoWallet(key))
  let id = 0
  const rpc = async (method, params) => {
    const response = await client.fetch(new URL('/', base).toString(), { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }) })
    assert.equal(response.status, 200)
    return JSON.parse(await response.text())
  }
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
  return { mf, base, db, rpc, exec, seed, row, locked }
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

// #50 under BROADCASTER arcade: Arcade's 400 carrying status 460 to 475 is the instance's
// policy answer (B5). The fixture answers every submission with `arcadeCode` and its
// policy with 100 sats per 1,000 bytes; the header service carries each fixture's root.
const POLICY_CODES = [460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 471, 472, 473, 474, 475]
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
  await check('r internalizeAction refused at 465 is not failed; the req is queued with the hint and re-announced', async () => {
    const funding = raw(Buffer.alloc(32, 0xaa), 0x51, 77)
    const subject = raw(hash(funding), 0x51, 78)
    roots.set(800077, Buffer.from(hash(funding)).reverse().toString('hex'))
    const atomic = Buffer.concat([u32(0x01010101), hash(subject), bumped(funding, 800077), Buffer.from([2, 1, 0]), funding, Buffer.from([0]), subject])
    const r = await arcade.rpc('internalizeAction', [{ identityKey: '' }, { tx: atomic.toString('hex'), outputs: [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'fee' } }], description: 'fee refusal', labels: [], seekPermission: false }])
    assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
    const t = await arcade.db.prepare('SELECT status FROM transactions WHERE txid = ?').bind(txid(subject)).first()
    assert.notEqual(t.status, 'failed')
    let req = await arcade.db.prepare('SELECT status, history FROM proven_tx_reqs WHERE txid = ?').bind(txid(subject)).first()
    assert.equal(req.status, 'unsent'); policyHint(req.history, 'internalizeBroadcastHint', 465)
    const before = arcadePosts
    await monitorRun()
    assert.ok(arcadePosts > before, 'the monitor did not re-announce')
    req = await arcade.db.prepare('SELECT status, history FROM proven_tx_reqs WHERE txid = ?').bind(txid(subject)).first()
    assert.equal(req.status, 'unsent'); policyHint(req.history, 'monitorBroadcastHint', 465)
    assert.notEqual((await arcade.db.prepare('SELECT status FROM transactions WHERE txid = ?').bind(txid(subject)).first()).status, 'failed')
  })
} finally { await arcade.mf.dispose() }

// #50 under production's settings (INTERNALIZE_ZERO_CONF=true, BROADCASTER=arcade): an
// inbound payment Arcade refuses is never credited as spendable; one it accepts (202, then
// SEEN_ON_NETWORK on its event stream) is credited as before, and a refused one becomes
// spendable when a later announce is seen.
let zeroCode = 461
const zero = await boot({ BROADCASTER: 'arcade', ARCADE_URL: 'https://arcade.test', CHAINTRACKS_URL: 'https://headers.test', BEEF_VERIFICATION: 'strict', INTERNALIZE_ZERO_CONF: 'true' }, async request => {
  const url = new URL(request.url)
  const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
  if (url.host === 'arcade.test' && request.method === 'POST' && ['/tx', '/txs'].includes(url.pathname)) {
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
    return json({ status: 'success', value: { merkleRoot: roots.get(height) ?? '00'.repeat(32), height } })
  }
  return new Response('not found', { status: 404 })
})
const sender = PrivateKey.fromRandom().toPublicKey().toString()
// An AtomicBEEF paying output 0 of a fresh subject; its funding is proven at 800000 + n.
const inbound = async (n, lock = 0x51) => {
  const funding = raw(Buffer.alloc(32, n), lock, n)
  const subject = raw(hash(funding), 0x51, n + 1)
  roots.set(800000 + n, Buffer.from(hash(funding)).reverse().toString('hex'))
  const atomic = Buffer.concat([u32(0x01010101), hash(subject), bumped(funding, 800000 + n), Buffer.from([2, 1, 0]), funding, Buffer.from([0]), subject])
  const r = await zero.rpc('internalizeAction', [{ identityKey: '' }, { tx: atomic.toString('hex'), outputs: [{ outputIndex: 0, protocol: 'wallet payment', paymentRemittance: { derivationPrefix: 'cHJlZml4', derivationSuffix: Buffer.from(`s${n}`).toString('base64'), senderIdentityKey: sender } }], description: 'zero-conf payment', labels: [], seekPermission: false }])
  return { r, txid: txid(subject) }
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
      const req = await zeroReq(t); assert.equal(req.status, 'unsent'); assert.notEqual(req.tx_status, 'failed')
      const hint = hints(req.history, 'internalizeBroadcastHint').at(-1); assert.ok(hint, req.history); assert.equal(hint.words.code, code)
      if (code === 465) held465 = t
    })
  }
  await check('t zero-conf: a payment Arcade accepts is credited spendable as before', async () => {
    zeroCode = 0
    const { r, txid: t } = await inbound(140)
    assert.ok(r.result, JSON.stringify(r)); assert.equal(r.result.accepted, true)
    const c = await credit(t); assert.equal(c.c, 1); assert.equal(c.n, 1)
    assert.equal((await zeroReq(t)).status, 'unmined')
  })
  await check('u zero-conf: the refused payment becomes spendable when a later announce is seen', async () => {
    assert.ok(held465, 'no held payment from s')
    zeroCode = 465; await zeroMonitor(); assert.equal((await credit(held465)).n, 0)
    zeroCode = 0; await zeroMonitor()
    assert.equal((await credit(held465)).n, 1); assert.equal((await zeroReq(held465)).status, 'unmined')
  })
} finally { await zero.mf.dispose() }
console.log(`Tracker Worker witnesses: ${ran - failed} passed; ${failed} failed`)
if (failed) process.exitCode = 1
