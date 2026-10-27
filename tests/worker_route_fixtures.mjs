// Issue #15, the oracle: every HTTP route and every served JSON-RPC method of the
// compiled Worker, driven in a fixed order against a fresh Miniflare (D1, KV, both
// buckets in memory; every outbound request answered here, nothing leaves the
// process), each answer compared with its recorded fixture under
// tests/fixtures/routes/. A refactor shows no diff; a behaviour change rewrites a
// fixture in its own commit.
//
// worker-build --release
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js \
// BSV_SDK_MODULE=<@bsv/sdk>/dist/esm/mod.js node tests/worker_route_fixtures.mjs
// UPDATE_FIXTURES=1 (same line) records the fixtures instead of comparing.
//
// A fixture is the request and the canonical answer: the status, the content-type
// and access-control-allow-origin headers, and the body (JSON parsed and printed
// back in the server's key order, or text). The keys and every input are fixed, so
// ids and txids are the same on every run. Two canonicalization rules, applied to
// the answer only (CANON below), each by key name and value type: a string under a
// key in TIME_KEYS that reads as a wall-clock time becomes "<time>"; a number under
// a key in DURATION_KEYS (the audit's own run time, the minutes since the oldest
// unproven request) becomes "<duration>". The key, its place and the value's type
// are still compared. Any other difference fails the case by its name.
import { readFileSync, readdirSync, writeFileSync, mkdirSync, rmSync } from 'node:fs'
import { resolve, join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'

const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const { AuthFetch, ProtoWallet, PrivateKey } = await import(process.env.BSV_SDK_MODULE ?? '@bsv/sdk')

const DIR = resolve('tests/fixtures/routes')
const UPDATE = process.env.UPDATE_FIXTURES === '1'
const sha = b => createHash('sha256').update(b).digest()
const sha256d = b => sha(sha(b))
const display = h => Buffer.from(h).reverse().toString('hex')
// Fixed keys from fixed labels, so identity keys and signatures' subjects repeat.
const keyOf = label => PrivateKey.fromHex(sha(label).toString('hex'))
const MONITOR_KEY = 'route-fixtures-monitor'
const CALLBACK_TOKEN = 'route-fixtures-callback'

// The chain of tests/worker_beef_at_rest.mjs, one proven transaction: an AtomicBEEF
// whose one root the header stub below carries at HEIGHT.
const HEIGHT = 800000
const u32 = n => { const b = Buffer.alloc(4); b.writeUInt32LE(n >>> 0); return b }
const spend = (prev, outputs) => {
  const parts = [u32(1), Buffer.from([1]), prev, u32(0), Buffer.from([0]), u32(0xffffffff), Buffer.from([outputs])]
  for (let i = 0; i < outputs; i++) {
    const sats = Buffer.alloc(8); sats.writeBigUInt64LE(1000n)
    parts.push(sats, Buffer.from([1, 0x51]))
  }
  parts.push(u32(0))
  return Buffer.concat(parts)
}
const funding = spend(Buffer.alloc(32, 0xaa), 2)
const fundingRoot = sha256d(funding)
const atomicBeef = Buffer.concat([
  u32(0x01010101), fundingRoot,
  u32(0xefbe0002), Buffer.from([1]), Buffer.from([0xfe]), u32(HEIGHT), Buffer.from([1, 1, 0, 2]), fundingRoot,
  Buffer.from([1]), Buffer.from([1, 0]), funding,
])
// A transaction seeded straight into D1 as the caller's spendable change.
const seeded = spend(Buffer.alloc(32, 0xbb), 1)
const seededTxid = display(sha256d(seeded))

// Every outbound request: the header service knows the one root; anything else is 404.
const outbound = async request => {
  const url = new URL(request.url)
  if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
    const height = Number(url.searchParams.get('height'))
    const merkleRoot = height === HEIGHT ? display(fundingRoot) : '00'.repeat(32)
    return new Response(JSON.stringify({ status: 'success', value: { merkleRoot, height } }),
      { headers: { 'content-type': 'application/json' } })
  }
  return new Response('not found', { status: 404 })
}

const mf = new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    { type: 'ESModule', include: ['**/*.js', '**/*.mjs'] },
    { type: 'CompiledWasm', include: ['**/*.wasm'] },
  ],
  compatibilityDate: '2024-01-01',
  host: '127.0.0.1',
  port: 0,
  bindings: {
    SERVER_PRIVATE_KEY: keyOf('route-fixtures server').toHex(),
    CHAINTRACKS_URL: 'https://headers.test',
    BEEF_VERIFICATION: 'strict',
    INTERNALIZE_ZERO_CONF: 'false',
    BROADCASTER: 'arc',
    MONITOR_TRIGGER_KEY: MONITOR_KEY,
    ARCADE_CALLBACK_TOKEN: CALLBACK_TOKEN,
  },
  d1Databases: ['DB'],
  kvNamespaces: ['AUTH_SESSIONS'],
  r2Buckets: ['BLOBS'],
  outboundService: outbound,
})

// CANON: the two rules. A wall-clock time is "YYYY-MM-DD", a "T" or a space,
// "HH:MM:SS", an optional fraction and an optional zone.
const TIME_KEYS = new Set(['created_at', 'updated_at', 'createdAt', 'updatedAt', 'when', 'timestamp'])
const TIME = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$/
const DURATION_KEYS = new Set(['executionMs', 'oldestMinutes'])
const canon = (value, key) => {
  if (Array.isArray(value)) return value.map(v => canon(v))
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, canon(v, k)]))
  }
  if (typeof value === 'string' && TIME_KEYS.has(key) && TIME.test(value)) return '<time>'
  if (typeof value === 'number' && DURATION_KEYS.has(key)) return '<duration>'
  return value
}

const answer = async response => {
  const text = await response.text()
  const type = response.headers.get('content-type')
  let body
  try { body = canon(JSON.parse(text)) } catch { body = text }
  return { status: response.status, contentType: type, allowOrigin: response.headers.get('access-control-allow-origin'), body }
}

const cases = []
let failed = 0
const record = async (name, request, run) => {
  const n = String(cases.length + 1).padStart(2, '0')
  const file = `${n}-${name}.json`
  cases.push(file)
  let got
  try { got = await run() } catch (e) { got = { thrown: String(e?.message ?? e) } }
  const text = JSON.stringify({ case: name, request, response: got }, null, 2) + '\n'
  if (UPDATE) { writeFileSync(join(DIR, file), text); console.log(`RECORDED ${name}`); return got }
  let want = null
  try { want = readFileSync(join(DIR, file), 'utf8') } catch { /* no fixture */ }
  if (want === text) console.log(`PASS ${name}`)
  else {
    failed++
    console.log(`FAIL ${name}: ${want === null ? `no fixture ${file}` : `answer differs from ${file}`}`)
    if (want !== null) {
      const a = want.split('\n'), b = text.split('\n')
      const i = a.findIndex((line, j) => line !== b[j])
      console.log(`  line ${i + 1}\n  fixture: ${a[i]}\n  now:     ${b[i]}`)
    }
  }
  return got
}

try {
  const base = await mf.ready
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    // D1.exec splits on newlines; let SQLite find the complete statements.
    const split = spawnSync('python3', ['-c', `import json, sqlite3, sys
statements, pending = [], ''
for char in sys.stdin.read():
 pending += char
 if char == ';' and sqlite3.complete_statement(pending):
  statements.append(pending); pending = ''
print(json.dumps(statements))`], { input: readFileSync('migrations/' + name, 'utf8'), encoding: 'utf8' })
    assert.equal(split.status, 0, split.stderr)
    await db.batch(JSON.parse(split.stdout).map(sql => db.prepare(sql)))
  }
  if (UPDATE) { rmSync(DIR, { recursive: true, force: true }); mkdirSync(DIR, { recursive: true }) }

  // ---- the open routes and the door, no session -----------------------------
  const http = (name, method, path, { headers = {}, body } = {}) =>
    record(name, { method, path, headers, body: body ?? null },
      async () => answer(await mf.dispatchFetch(new URL(path, base), { method, headers, body })))

  await http('options-preflight', 'OPTIONS', '/')
  await http('get-health', 'GET', '/')
  await http('get-monitor-status-empty', 'GET', '/monitor/status')
  await http('post-monitor-run-no-key', 'POST', '/monitor/run')
  await http('post-monitor-run-wrong-key', 'POST', '/monitor/run?key=wrong')
  await http('post-monitor-reorg-sweep-no-key', 'POST', '/monitor/reorg-sweep?from=1&to=1')
  await http('post-monitor-reorg-sweep-no-range', 'POST', `/monitor/reorg-sweep?key=${MONITOR_KEY}`)
  await http('post-monitor-reorg-sweep-to-below-from', 'POST', `/monitor/reorg-sweep?key=${MONITOR_KEY}&from=10&to=5`)
  await http('get-monitor-probe-woc-no-key', 'GET', `/monitor/probe-woc?txid=${seededTxid}`)
  await http('get-monitor-probe-woc-bad-txid', 'GET', `/monitor/probe-woc?key=${MONITOR_KEY}&txid=abc`)
  await http('get-monitor-audit-no-key', 'GET', '/monitor/audit')
  await http('get-monitor-audit-wrong-key', 'GET', '/monitor/audit?level=1&key=wrong')
  await http('post-arcade-callback-no-bearer', 'POST', '/arcade/callback', { body: '{}' })
  await http('post-arcade-callback-wrong-bearer', 'POST', '/arcade/callback', { headers: { authorization: 'Bearer wrong' }, body: '{}' })
  await http('post-arcade-callback-unparseable', 'POST', '/arcade/callback', { headers: { authorization: `Bearer ${CALLBACK_TOKEN}` }, body: 'not json' })
  await http('post-arcade-callback-empty-event', 'POST', '/arcade/callback', { headers: { authorization: `Bearer ${CALLBACK_TOKEN}`, 'content-type': 'application/json' }, body: '{}' })
  await http('post-root-no-session', 'POST', '/', { headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'makeAvailable', params: [] }) })
  await http('get-unknown-path-no-session', 'GET', '/nowhere')

  // ---- JSON-RPC through the BRC-31 door (@bsv/sdk's AuthFetch) ---------------
  const callerKey = keyOf('route-fixtures caller')
  const caller = callerKey.toPublicKey().toString()
  const client = new AuthFetch(new ProtoWallet(callerKey))
  let id = 0
  const authed = async (path, method, body) =>
    answer(await client.fetch(new URL(path, base).toString(), { method, headers: { 'content-type': 'application/json' }, body }))
  const rpc = (name, method, params) => {
    const request = { jsonrpc: '2.0', id: ++id, method, params }
    return record(name, { method: 'POST', path: '/', session: 'caller', body: request },
      () => authed('/', 'POST', JSON.stringify(request)))
  }
  const A = { identityKey: caller }

  await rpc('makeAvailable-before-migrate', 'makeAvailable', [])
  // The operator initializes the store once, as the README's INSERT (#22): migrate writes nothing.
  await db.prepare("INSERT INTO settings (storage_identity_key, storage_name, chain, dbtype, max_output_script) VALUES (?, 'route-fixtures', 'mainnet', 'D1', 10000)")
    .bind(keyOf('route-fixtures server').toPublicKey().toString()).run()
  await rpc('migrate', 'migrate', ['route-fixtures', 'route-fixtures-storage'])
  await rpc('makeAvailable', 'makeAvailable', [])
  await rpc('findOrInsertUser-new', 'findOrInsertUser', [caller])
  await rpc('findOrInsertUser-existing', 'findOrInsertUser', [caller])
  await rpc('findOrInsertUser-missing-key', 'findOrInsertUser', [{}])

  // Seed: the caller's default basket and one spendable change output.
  const user = (await db.prepare('SELECT user_id FROM users WHERE identity_key = ?').bind(caller).first()).user_id
  const exec = (sql, ...args) => db.prepare(sql).bind(...args.map(a => Buffer.isBuffer(a) ? new Uint8Array(a) : a)).run()
  const at = '2026-01-01 00:00:00'
  await exec("INSERT OR IGNORE INTO output_baskets (user_id, name, created_at, updated_at) VALUES (?, 'default', ?, ?)", user, at, at)
  const basket = (await db.prepare("SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = 'default'").bind(user).first()).basket_id
  await exec("INSERT INTO transactions (user_id, reference, status, is_outgoing, description, txid, raw_tx, satoshis, created_at, updated_at) VALUES (?, 'seeded', 'completed', 0, 'route fixture', ?, ?, 1000, ?, ?)", user, seededTxid, seeded, at, at)
  const seededId = (await db.prepare("SELECT transaction_id FROM transactions WHERE reference = 'seeded'").first()).transaction_id
  await exec("INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, locking_script, script_length, script_offset, created_at, updated_at) VALUES (?, ?, ?, 1, 1, 0, 1000, 'storage', 'change', 'P2PKH', ?, ?, 1, 0, ?, ?)", user, seededId, basket, seededTxid, Buffer.from([0x51]), at, at)
  await exec("INSERT INTO proven_tx_reqs (txid, raw_tx, status, created_at, updated_at) VALUES (?, ?, 'unmined', ?, ?)", seededTxid, seeded, at, at)

  await rpc('getBalance', 'getBalance', [A, { basket: 'default' }])
  await rpc('listOutputs', 'listOutputs', [A, { basket: 'default', include: 'locking scripts', limit: 10 }])
  await rpc('listOutputs-bad-args', 'listOutputs', [A, { basket: 7 }])
  await rpc('listActions', 'listActions', [A, { labels: [], limit: 10 }])
  await rpc('getAnalyticsSummary', 'getAnalyticsSummary', [A, { nowMs: Date.parse('2026-06-01T00:00:00Z') }])
  await rpc('findProvenTxReqs', 'findProvenTxReqs', [A, { txids: [seededTxid] }])
  await rpc('getBeefForTxid', 'getBeefForTxid', [A, { txid: seededTxid }])
  await rpc('getBeefForTxid-bad-txid', 'getBeefForTxid', [A, { txid: 'abc' }])
  await rpc('reserveOutputs', 'reserveOutputs', [A, { basket: 'default', outputs: [{ txid: seededTxid, vout: 0 }], ttlSeconds: 60 }])
  await rpc('unreserveOutputs', 'unreserveOutputs', [A, { basket: 'default', outputs: [{ txid: seededTxid, vout: 0 }] }])
  await rpc('internalizeAction', 'internalizeAction', [A, { tx: atomicBeef.toString('hex'), outputs: [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'fixtures' } }], description: 'route fixture', labels: [], seekPermission: false }])
  await rpc('internalizeAction-bad-beef', 'internalizeAction', [A, { tx: '00', outputs: [], description: 'route fixture', labels: [] }])
  await rpc('createAction-missing-args', 'createAction', [A, {}])
  await rpc('abortAction-unknown-reference', 'abortAction', [A, { reference: 'cm91dGUtZml4dHVyZXM=' }])
  await rpc('processAction-unknown-reference', 'processAction', [A, { isNewTx: true, isSendWith: false, isNoSend: false, isDelayed: false, reference: 'cm91dGUtZml4dHVyZXM=', txid: seededTxid, rawTx: [...seeded], sendWith: [] }])
  await rpc('updateTransactionStatusAfterBroadcast-unknown-txid', 'updateTransactionStatusAfterBroadcast', ['00'.repeat(32), true])
  await rpc('updateTransactionStatusAfterBroadcast-missing-txid', 'updateTransactionStatusAfterBroadcast', [])
  const certifier = keyOf('route-fixtures certifier').toPublicKey().toString()
  await rpc('insertCertificate', 'insertCertificate', [A, { type: 'cm91dGUtZml4dHVyZS10eXBl', serialNumber: 'cm91dGUtZml4dHVyZS1zZXJpYWw=', certifier, subject: caller, revocationOutpoint: `${seededTxid}.0`, signature: '3006020101020101', fields: [{ fieldName: 'name', fieldValue: 'fixture', masterKey: 'a2V5' }] }])
  await rpc('listCertificates', 'listCertificates', [A, { certifiers: [certifier], includeFields: true }])
  await rpc('relinquishCertificate', 'relinquishCertificate', [A, { serialNumber: 'cm91dGUtZml4dHVyZS1zZXJpYWw=' }])
  await rpc('relinquishOutput', 'relinquishOutput', [A, { basket: 'fixtures', output: `${display(fundingRoot)}.0` }])
  await rpc('reviewStatus', 'reviewStatus', [A, {}])
  await rpc('beginStorageTransaction', 'beginStorageTransaction', [])
  await rpc('commitStorageTransaction', 'commitStorageTransaction', [0])
  await rpc('rollbackStorageTransaction', 'rollbackStorageTransaction', [0])
  await rpc('unknown-method', 'noSuchMethod', [])
  await record('parse-error', { method: 'POST', path: '/', session: 'caller', body: 'not json' }, () => authed('/', 'POST', 'not json'))
  await record('authed-unknown-path', { method: 'POST', path: '/nowhere', session: 'caller', body: '{}' }, () => authed('/nowhere', 'POST', '{}'))

  // ---- the keyed operator routes, on the state above --------------------------
  await http('get-monitor-status', 'GET', `/monitor/status?key=${MONITOR_KEY}`)
  await http('get-monitor-audit-level-1', 'GET', `/monitor/audit?level=1&key=${MONITOR_KEY}`)
  await http('get-monitor-probe-woc', 'GET', `/monitor/probe-woc?key=${MONITOR_KEY}&txid=${seededTxid}`)
  await http('post-monitor-reorg-sweep', 'POST', `/monitor/reorg-sweep?key=${MONITOR_KEY}&from=${HEIGHT}&to=${HEIGHT}`)
  await http('post-monitor-run', 'POST', `/monitor/run?key=${MONITOR_KEY}`)

  if (!UPDATE) {
    const extra = readdirSync(DIR).filter(f => f.endsWith('.json') && !cases.includes(f))
    for (const f of extra) { failed++; console.log(`FAIL ${f}: a fixture no case recorded`) }
  }
  console.log(`Route fixtures: ${cases.length} cases, ${UPDATE ? 'recorded' : `${cases.length - failed} passed; ${failed} failed`}`)
} finally { await mf.dispose() }
if (failed) process.exitCode = 1
