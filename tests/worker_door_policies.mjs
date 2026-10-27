// #22, the door's policies for strangers, on the compiled Worker, fully local:
// the in-flight ceiling, the body bound, the two rate limits (the numbers read
// from wrangler.toml), the list budgets, findOrInsertUser bound to the session,
// migrate without a write, a D1 fault collapsed on the wire, the status route
// behind the trigger key, healthz and robots.txt. Miniflare with D1, KV, R2
// and the rate-limit bindings; @bsv/sdk's AuthFetch for the BRC-31 door.
// Nothing leaves this process. No deploy.
//
// worker-build --release   (worker-build ^0.8, the wrangler.toml [build] line)
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js \
// BSV_SDK_MODULE=<@bsv/sdk>/dist/esm/mod.js node tests/worker_door_policies.mjs
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import assert from 'node:assert/strict'

const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const { AuthFetch, ProtoWallet, PrivateKey } = await import(process.env.BSV_SDK_MODULE ?? '@bsv/sdk')

const MONITOR_KEY = 'w24-door-local'
const INTERNAL_TEXT = 'An internal error has occurred.'

// The two limits as wrangler.toml declares them, so the witness proves the file's numbers.
const wrangler = readFileSync('wrangler.toml', 'utf8')
const limitOf = name => {
  const m = wrangler.match(new RegExp(`name = "${name}"[\\s\\S]*?simple = \\{ limit = (\\d+), period = (\\d+) \\}`))
  assert.ok(m, `wrangler.toml declares no ${name}`)
  return { limit: Number(m[1]), period: Number(m[2]) }
}
const IP_LIMIT = limitOf('RATE_LIMIT_IP')
const IDENTITY_LIMIT = limitOf('RATE_LIMIT_IDENTITY')
assert.deepEqual([IP_LIMIT, IDENTITY_LIMIT], [{ limit: 300, period: 60 }, { limit: 1000, period: 60 }])

// The WhatsOnChain probe's outbound call is held while `hold` is set: the
// requests that reach it stay in flight (the ceiling, step 8).
let hold = null
let held = 0
const outbound = async request => {
  const url = new URL(request.url)
  if (url.host === 'api.whatsonchain.com' && hold) {
    held++
    await hold.promise
    return new Response('{}', { headers: { 'content-type': 'application/json' } })
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
    SERVER_PRIVATE_KEY: PrivateKey.fromRandom().toHex(),
    BROADCASTER: 'arc',
    MONITOR_TRIGGER_KEY: MONITOR_KEY,
  },
  d1Databases: ['DB'],
  kvNamespaces: ['AUTH_SESSIONS'],
  r2Buckets: ['BLOBS'],
  ratelimits: {
    RATE_LIMIT_IP: { simple: IP_LIMIT },
    RATE_LIMIT_IDENTITY: { simple: IDENTITY_LIMIT },
  },
  outboundService: outbound,
})

const say = line => console.log(line)

try {
  const base = await mf.ready
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    // D1.exec splits on newlines; let SQLite find the complete statements.
    const split = spawnSync('python3', ['-c', `
import json, sqlite3, sys
statements, pending = [], ''
for char in sys.stdin.read():
    pending += char
    if char == ';' and sqlite3.complete_statement(pending):
        statements.append(pending)
        pending = ''
print(json.dumps(statements))
`], { input: readFileSync('migrations/' + name, 'utf8'), encoding: 'utf8' })
    assert.equal(split.status, 0, split.stderr)
    await db.batch(JSON.parse(split.stdout).map(sql => db.prepare(sql)))
  }

  // Each AuthFetch client sends each request from the address `from()` names then.
  const clientFrom = (key, from) => new AuthFetch(new ProtoWallet(key), undefined, undefined, undefined, {},
    (url, init = {}) => {
      const headers = new Headers(init.headers)
      headers.set('CF-Connecting-IP', from())
      return fetch(url, { ...init, headers })
    })
  const callerKey = PrivateKey.fromRandom()
  const caller = callerKey.toPublicKey().toString()
  const another = PrivateKey.fromRandom().toPublicKey().toString()
  const client = clientFrom(callerKey, () => '192.0.2.10')
  let id = 0
  const call = async (who, method, params) => {
    const body = JSON.stringify({ jsonrpc: '2.0', method, id: ++id, params })
    const response = await who.fetch(new URL('/', base).toString(), {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })
    const text = await response.text()
    return { status: response.status, body: JSON.parse(text) }
  }
  const rpc = async (method, params) => {
    const r = await call(client, method, params)
    assert.equal(r.status, 200, JSON.stringify(r.body))
    return r.body
  }

  // 1. healthz and robots.txt, as the reference.
  {
    const health = await fetch(new URL('/healthz', base))
    assert.equal(health.status, 200)
    assert.equal(health.headers.get('cache-control'), 'no-store')
    assert.deepEqual(await health.json(), { status: 'ok' })
    const robots = await fetch(new URL('/robots.txt', base))
    assert.equal(robots.status, 200)
    assert.match(robots.headers.get('content-type'), /^text\/plain/)
    assert.equal(await robots.text(), 'User-agent: *\nDisallow: /')
    say('healthz: 200 {"status":"ok"}, no-store; robots.txt: User-agent: * / Disallow: /')
  }

  // 2. The status route behind the trigger key.
  {
    const refused = await fetch(new URL('/monitor/status', base))
    assert.equal(refused.status, 401)
    assert.deepEqual(await refused.json(), { error: 'unauthorized — set MONITOR_TRIGGER_KEY secret and pass ?key=<value>' })
    const keyed = await fetch(new URL(`/monitor/status?key=${MONITOR_KEY}`, base))
    assert.equal(keyed.status, 200, await keyed.clone().text())
    say('monitor/status, no key: 401; trigger key: 200')
  }

  // 3. migrate writes nothing: an uninitialized store stays without settings;
  // an initialized one answers its chain and its row is unchanged.
  {
    const count = async () => db.prepare('SELECT COUNT(*) AS n FROM settings').first('n')
    const empty = await rpc('migrate', ['a-stranger-store'])
    assert.ok(empty.error, JSON.stringify(empty))
    assert.equal(await count(), 0, 'migrate wrote a settings row')
    await db.prepare("INSERT INTO settings (storage_identity_key, storage_name, chain, dbtype, max_output_script) VALUES ('02' || hex(randomblob(32)), 'wallet-infra', 'mainnet', 'D1', 10000)").run()
    const before = await db.prepare('SELECT * FROM settings').all()
    const r = await rpc('migrate', ['a-stranger-store'])
    assert.equal(r.result, 'mainnet', JSON.stringify(r))
    const named = await rpc('migrate', { storageName: 'another', storageIdentityKey: another })
    assert.equal(named.result, 'mainnet', JSON.stringify(named))
    assert.deepEqual((await db.prepare('SELECT * FROM settings').all()).results, before.results)
    say(`migrate: no settings row written on an uninitialized store (${empty.error.message}); on an initialized one, answered "mainnet", the row unchanged`)
  }

  // 4. findOrInsertUser for the session's key only.
  {
    const mine = await rpc('findOrInsertUser', [caller])
    assert.equal(mine.result.user.identityKey, caller, JSON.stringify(mine))
    for (const params of [[another], { identityKey: another }]) {
      const r = await call(client, 'findOrInsertUser', params)
      assert.equal(r.status, 401, JSON.stringify(r.body))
      assert.equal(r.body.error.message, 'function may only access authenticated user.')
      assert.equal(r.body.error.data.name, 'WERR_UNAUTHORIZED')
    }
    assert.equal(await db.prepare('SELECT COUNT(*) AS n FROM users WHERE identity_key = ?').bind(another).first('n'), 0)
    say('findOrInsertUser: the session\'s key 200; another key 401 WERR_UNAUTHORIZED, no row for it')
  }

  // 5. The list budgets: a limit of 1,001 is refused, 1,000 is served.
  {
    for (const [method, args] of [
      ['listOutputs', { basket: 'default', limit: 1001 }],
      ['listActions', { labels: [], limit: 1001 }],
      ['findProvenTxReqs', { paged: { limit: 1001 } }],
      ['listCertificates', { certifiers: [], types: [], limit: 1001 }],
    ]) {
      const r = await rpc(method, [{ identityKey: '' }, args])
      assert.ok(r.error, `${method}: ${JSON.stringify(r)}`)
      assert.equal(r.error.code, -32602)
      assert.equal(r.error.message, 'RPC list limits must not exceed 1000')
    }
    const served = await rpc('listOutputs', [{ identityKey: '' }, { basket: 'default', limit: 1000 }])
    assert.ok(served.result, JSON.stringify(served))
    const reqs = await rpc('findProvenTxReqs', [{ identityKey: '' }, {}])
    assert.ok(Array.isArray(reqs.result), JSON.stringify(reqs))
    say('list budgets: limit 1001 refused on listOutputs, listActions, findProvenTxReqs (paged), listCertificates; 1000 served')
  }

  // 6. A body of 9 MiB is refused unread, 413.
  {
    const body = JSON.stringify({ jsonrpc: '2.0', method: 'listOutputs', id: 1, params: ['x'.repeat(9 * 1024 * 1024)] })
    const response = await fetch(new URL('/', base), { method: 'POST', headers: { 'content-type': 'application/json' }, body })
    assert.equal(response.status, 413)
    assert.deepEqual(await response.json(), { status: 'error', code: 'ERR_BODY_TOO_LARGE', description: 'The request body exceeds the endpoint limit.' })
    say(`body of ${body.length} bytes: 413 ERR_BODY_TOO_LARGE`)
  }

  // 7. A D1 fault answers WERR_INTERNAL's text, never the database's.
  {
    await db.prepare('ALTER TABLE certificates RENAME TO certificates_away').run()
    const r = await rpc('listCertificates', [{ identityKey: '' }, { certifiers: [], types: [] }])
    await db.prepare('ALTER TABLE certificates_away RENAME TO certificates').run()
    assert.ok(r.error, JSON.stringify(r))
    assert.deepEqual([r.error.code, r.error.message], [-32603, INTERNAL_TEXT])
    assert.ok(!JSON.stringify(r).includes('certificates'), JSON.stringify(r))
    say(`D1 fault (no such table): -32603 "${r.error.message}"`)
  }

  // 8. The in-flight ceiling: 30 requests at once, 24 held in flight, 6 refused 503.
  {
    let release
    hold = { promise: new Promise(r => { release = r }) }
    held = 0
    const txid = '00'.repeat(32)
    const pending = Array.from({ length: 30 }, () =>
      fetch(new URL(`/monitor/probe-woc?key=${MONITOR_KEY}&txid=${txid}`, base)))
    const deadline = Date.now() + 20000
    while (held < 24 && Date.now() < deadline) await new Promise(r => setTimeout(r, 20))
    await new Promise(r => setTimeout(r, 300))
    assert.equal(held, 24, `held in flight: ${held}`)
    release()
    const answers = await Promise.all(pending)
    hold = null
    const busy = answers.filter(a => a.status === 503)
    assert.equal(busy.length, 6, answers.map(a => a.status).join(','))
    assert.equal(answers.filter(a => a.status === 200).length, 24)
    assert.equal(busy[0].headers.get('retry-after'), '1')
    assert.equal((await busy[0].json()).code, 'ERR_SERVER_BUSY')
    const after = await fetch(new URL('/healthz', base))
    assert.equal(after.status, 200, 'the slots were not given back')
    say('in flight: 30 at once, 24 served, 6 answered 503 ERR_SERVER_BUSY with Retry-After 1; the slots given back')
  }

  // 9. The two rate limits, in one window of the limiter (aligned to its period).
  {
    const periodMs = IP_LIMIT.period * 1000
    const left = periodMs - (Date.now() % periodMs)
    if (left < 45000) await new Promise(r => setTimeout(r, left + 50))
    const windowStart = Date.now()

    // 9a. A stranger from one address: the limit's requests reach the door (401,
    // the middleware's), the next is 429; a read from it and another address pass.
    const post = ip => fetch(new URL('/', base), {
      method: 'POST', headers: { 'content-type': 'application/json', 'CF-Connecting-IP': ip },
      body: JSON.stringify({ jsonrpc: '2.0', method: 'listOutputs', id: 1, params: [] }),
    })
    const statuses = []
    for (let i = 0; i < IP_LIMIT.limit; i += 50) {
      const batch = await Promise.all(Array.from({ length: Math.min(50, IP_LIMIT.limit - i) }, () => post('198.51.100.7')))
      statuses.push(...batch.map(r => r.status))
    }
    assert.ok(statuses.every(s => s === 401), [...new Set(statuses)].join(','))
    const over = await post('198.51.100.7')
    assert.equal(over.status, 429)
    assert.deepEqual(await over.json(), { status: 'error', code: 'ERR_RATE_LIMITED', description: 'Too many requests. Please retry later.' })
    const read = await fetch(new URL('/healthz', base), { headers: { 'CF-Connecting-IP': '198.51.100.7' } })
    assert.equal(read.status, 200)
    assert.equal((await post('198.51.100.8')).status, 401)
    say(`pre-auth: ${IP_LIMIT.limit} POSTs from one address reach the door, the next is 429 ERR_RATE_LIMITED; a GET from it and a POST from another pass`)

    // 9b. One identity from eight addresses (each under the address limit):
    // the limit's calls are served, the next is 429, signed.
    const strangerKey = PrivateKey.fromRandom()
    const addresses = Array.from({ length: 8 }, (_, i) => `203.0.113.${i + 1}`)
    let n = 0
    const stranger = clientFrom(strangerKey, () => addresses[n++ % addresses.length])
    const one = () => call(stranger, 'beginStorageTransaction', [])
    let served = 0
    for (let i = 0; i < IDENTITY_LIMIT.limit; i += 20) {
      const batch = await Promise.all(Array.from({ length: Math.min(20, IDENTITY_LIMIT.limit - i) }, one))
      for (const r of batch) {
        assert.equal(r.status, 200, `call ${served + 1}: ${JSON.stringify(r.body)}`)
        served++
      }
    }
    const limited = await one()
    assert.equal(limited.status, 429, JSON.stringify(limited.body))
    assert.deepEqual(limited.body, { status: 'error', code: 'ERR_RATE_LIMITED', description: 'Too many requests. Please retry later.' })
    const elapsed = Date.now() - windowStart
    assert.ok(elapsed < periodMs - 1000, `the drive left the limiter's window (${elapsed} ms)`)
    say(`identity: ${served} calls of one identity from ${addresses.length} addresses served, the next is 429 ERR_RATE_LIMITED (signed), in ${elapsed} ms`)
  }

  console.log('Local compiled Worker GREEN (#22): the in-flight ceiling, the body bound, both rate limits, the list budgets, findOrInsertUser bound to the session, migrate without a write, the collapsed fault, the status route behind the key, healthz and robots.txt')
} finally {
  await mf.dispose()
}
