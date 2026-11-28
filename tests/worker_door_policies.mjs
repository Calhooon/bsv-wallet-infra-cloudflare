// #22, the door's policies for strangers, on the compiled Worker, fully local:
// the in-flight ceiling, the body bound, the two rate limits (the numbers read
// from wrangler.toml; GET / naming each binding bound or absent; the 429's
// draft-8 headers), the list budgets, findOrInsertUser bound to the session,
// migrate without a write, a D1 fault collapsed on the wire, the status route
// behind the trigger key, healthz (with the build object) and robots.txt,
// a BEEF measured before it is parsed (internalizeAction, processAction), and
// a payment's bytes held to the figure in force, named or positional, with
// the figure shown on GET / and a malformed setting named once in the log.
// Miniflare with D1, KV, R2 and the rate-limit bindings; @bsv/sdk's AuthFetch
// for the BRC-31 door.
// Nothing leaves this process. No deploy.
//
// worker-build --release   (worker-build ^0.8, the wrangler.toml [build] line)
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js \
// BSV_SDK_MODULE=<@bsv/sdk>/dist/esm/mod.js node tests/worker_door_policies.mjs
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
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

// express-rate-limit's draft-8 headers on a 429 (`standardHeaders: 'draft-8'`,
// RateLimitPolicy.ts:16): the policy name, r=0, t and Retry-After the window
// (the binding gives no reset time), pk by its getPartitionKey.
const draft8 = ({ limit, period }, key) => {
  const name = `${limit}-in-${period / 60}min`
  const pk = Buffer.from(createHash('sha256').update(key).digest('hex').slice(0, 12)).toString('base64')
  return { ratelimit: `"${name}"; r=0; t=${period}`, 'ratelimit-policy': `"${name}"; q=${limit}; w=${period}; pk=:${pk}:`, 'retry-after': String(period) }
}
const headersOf = (headers, names) => Object.fromEntries(names.map(n => [n, headers.get(n)]))

const options = {
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
  handleStructuredLogs: ({ message }) => { logs.push(String(message).trim()) },
}
// The Worker's log lines (each refusal of a payment's size is counted by one).
const logs = []
const mf = new Miniflare(options)

const say = line => console.log(line)

try {
  const base = await mf.ready
  const db = await mf.getD1Database('DB')
  // The migrations, applied to a Worker's database (this one, and 8d's).
  const migrate = async db => {
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
  }
  await migrate(db)

  // Each AuthFetch client sends each request from the address `from()` names then.
  // AuthFetch's Response carries only the signed headers; `wire` keeps the
  // headers of the latest HTTP answer as they arrived.
  let wire = null
  const clientFrom = (key, from) => new AuthFetch(new ProtoWallet(key), undefined, undefined, undefined, {},
    async (url, init = {}) => {
      const headers = new Headers(init.headers)
      headers.set('CF-Connecting-IP', from())
      const response = await fetch(url, { ...init, headers })
      wire = response.headers
      return response
    })
  const callerKey = PrivateKey.fromRandom()
  const caller = callerKey.toPublicKey().toString()
  const another = PrivateKey.fromRandom().toPublicKey().toString()
  const client = clientFrom(callerKey, () => '192.0.2.10')
  let id = 0
  const call = async (who, method, params, at = base) => {
    const body = JSON.stringify({ jsonrpc: '2.0', method, id: ++id, params })
    const response = await who.fetch(new URL('/', at).toString(), {
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
    const { status, build, ...rest } = await health.json()
    assert.equal(status, 'ok')
    assert.deepEqual(rest, {})
    // The build object: the revision, the build time, the dirty flag, and the
    // versions the lock names (one entry each), this crate's among them.
    assert.deepEqual(Object.keys(build).sort(), ['built_at', 'crates', 'dirty', 'lib_rev'])
    assert.match(build.lib_rev, /^([0-9a-f]{40}|unknown)$/)
    assert.equal(typeof build.dirty, 'boolean')
    assert.match(build.built_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/)
    const lock = readFileSync('Cargo.lock', 'utf8')
    const locked = name => {
      const found = [...lock.matchAll(new RegExp(`\\[\\[package\\]\\]\\nname = "${name}"\\nversion = "([^"]+)"`, 'g'))]
      assert.equal(found.length, 1, `the lock names ${name} once`)
      return found[0][1]
    }
    const names = ['bsv-rs', 'bsv-middleware-cloudflare', 'bsv-middleware-core', 'bsv-wallet-infra-cloudflare']
    assert.deepEqual(build.crates, Object.fromEntries(names.map(n => [n, locked(n)])))
    const head = spawnSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).stdout.trim()
    if (head) assert.equal(build.lib_rev, head)
    const robots = await fetch(new URL('/robots.txt', base))
    assert.equal(robots.status, 200)
    assert.match(robots.headers.get('content-type'), /^text\/plain/)
    assert.equal(await robots.text(), 'User-agent: *\nDisallow: /')
    say(`healthz: 200 {"status":"ok","build":${JSON.stringify(build)}}, no-store; robots.txt: User-agent: * / Disallow: /`)
  }

  // 1b. GET / names each rate-limit binding: bound here, absent on a Worker
  // without them; and the payment figure in force, a number (614,400 where
  // MAX_PAYMENT_BYTES is unset).
  {
    const root = await fetch(new URL('/', base))
    assert.equal(root.status, 200)
    const rootBody = await root.json()
    assert.deepEqual(rootBody, { status: 'ok', service: 'wallet-infra', broadcaster: 'arc', rate_limits: { RATE_LIMIT_IP: 'bound', RATE_LIMIT_IDENTITY: 'bound' }, max_payment_bytes: 614400 })
    say(`GET /: ${JSON.stringify(rootBody)}`)
    const { ratelimits, ...unbound } = options
    const bare = new Miniflare(unbound)
    try {
      const answer = await fetch(new URL('/', await bare.ready))
      assert.equal(answer.status, 200)
      assert.deepEqual((await answer.json()).rate_limits, { RATE_LIMIT_IP: 'absent', RATE_LIMIT_IDENTITY: 'absent' })
    } finally {
      await bare.dispose()
    }
    say('GET /: rate_limits both bound with the bindings, both absent on a Worker without them')
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

  // 8b. A BEEF is measured before it is parsed: internalizeAction's AtomicBEEF
  // and processAction's signed transaction over the measure's budget are
  // refused with the storage's refusal of a BEEF (-32602, as a BEEF the parser
  // refuses), and nothing is written: every table holds the rows it held.
  {
    const counts = async () => {
      const tables = (await db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'").all()).results.map(r => r.name)
      const out = {}
      for (const t of tables.sort()) out[t] = await db.prepare(`SELECT COUNT(*) AS n FROM "${t}"`).first('n')
      return out
    }
    const varint = n => n < 0xfd ? [n] : [0xfd, n & 0xff, n >> 8]
    const le32 = n => [n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, n >>> 24]
    await rpc('findOrInsertUser', [caller])
    const userId = await db.prepare('SELECT user_id FROM users WHERE identity_key = ?').bind(caller).first('user_id')

    // internalizeAction: an AtomicBEEF of 10,000 txid-only entries (330 KB).
    const n = 10000
    const beef = [...le32(0x01010101), ...new Array(32).fill(0x11), ...le32(0xefbe0002), 0, ...varint(n)]
    for (let k = 0; k < n; k++) beef.push(2, ...le32(k), ...new Array(28).fill(0x22))
    const internalize = tx => rpc('internalizeAction', [{ identityKey: caller }, {
      tx, outputs: [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'measure' } }],
      description: 'a payment over the measure',
    }])
    const before = await counts()
    const refused = await internalize(Buffer.from(beef).toString('hex'))
    assert.deepEqual(refused.error, { code: -32602, message: 'the BEEF is too large to process' }, JSON.stringify(refused))
    assert.deepEqual(Object.keys(refused).sort(), ['error', 'id', 'jsonrpc'])
    const asArray = await internalize(beef)
    assert.deepEqual(asArray.error, refused.error)
    const malformed = await internalize('0100beef01')
    assert.equal(malformed.error.code, refused.error.code)
    assert.match(malformed.error.message, /^Failed to parse AtomicBEEF/)
    assert.deepEqual(Object.keys(malformed).sort(), Object.keys(refused).sort())
    assert.deepEqual(await counts(), before)

    // processAction: a signed transaction merged with its action's stored
    // input BEEF (the 10,000 entries above, as the action keeps one), the pair
    // over the measure's budget. A lone transaction within the payment figure
    // (614,400 bytes) stays under the measure's budget, so the stored BEEF
    // carries the weight here.
    const reference = 'measure-reference'
    await db.prepare("INSERT INTO transactions (user_id, status, reference, is_outgoing, description, input_beef) VALUES (?, 'unsigned', ?, 1, 'an action', ?)").bind(userId, reference, new Uint8Array(beef)).run()
    const raw = [...le32(1), 1, ...new Array(36).fill(0x33), 0, ...le32(0xffffffff), 1, 0xe8, 0x03, 0, 0, 0, 0, 0, 0, 1, 0x51, ...le32(0)]
    const sha = b => createHash('sha256').update(b).digest()
    const txid = Buffer.from(sha(sha(Buffer.from(raw)))).reverse().toString('hex')
    const beforeProcess = await counts()
    const processRefused = await rpc('processAction', [{ identityKey: caller }, {
      isNewTx: true, isSendWith: false, isNoSend: false, isDelayed: false, reference, txid, rawTx: raw, sendWith: [],
    }])
    assert.deepEqual(processRefused.error, { code: -32602, message: 'the transaction is too large to process' }, JSON.stringify(processRefused))
    assert.deepEqual(await counts(), beforeProcess)
    const row = await db.prepare('SELECT status, txid, raw_tx FROM transactions WHERE reference = ?').bind(reference).first()
    assert.deepEqual(row, { status: 'unsigned', txid: null, raw_tx: null })
    say(`measure: internalizeAction's AtomicBEEF of ${n} txid-only entries (hex and array) -32602 "${refused.error.message}", as a malformed BEEF is (-32602 "${malformed.error.message.slice(0, 26)}..."); processAction's ${raw.length}-byte transaction with its action's stored ${beef.length}-byte input BEEF -32602 "${processRefused.error.message}"; no table gained or lost a row, the action stays unsigned`)

    // 8c. The payment budget (door::payment, 614,400 bytes where the setting
    // MAX_PAYMENT_BYTES names no other): processAction's signed transaction
    // one byte over, as the array of bytes a client sends, is refused before
    // it is hashed or parsed (-32602, the pinned sentence, ERR_PAYMENT_SIZE
    // with the budget; its size is not known where the array is read no
    // further than the budget), counted by one log line, and nothing is
    // written. Exactly the figure passes to the storage's own handling. An
    // array between the figure and the request's array budget (1,000,000
    // items, door::bounds) is refused by the figure.
    const CAP = 614400
    const pinnedMore = (budget = CAP) => `Not judged: this payment is more than ${budget} bytes, the most this host reads of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block.`
    const pinnedIs = (size, budget = CAP) => `Not judged: this payment is ${size} bytes, and this host reads at most ${budget} bytes of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block.`
    // A signed transaction of `length` bytes: one input, one output whose
    // script is one push filling it.
    const signed = length => {
      const data = length - 64 - 5
      return [...le32(1), 1, ...new Array(36).fill(0x44), 0, ...le32(0xffffffff), 1,
        ...new Array(8).fill(0), 0xfe, ...le32(data + 5), 0x4e, ...le32(data), ...new Array(data).fill(0x5a), ...le32(0)]
    }
    const budgetRef = 'budget-reference'
    await db.prepare("INSERT INTO transactions (user_id, status, reference, is_outgoing, description) VALUES (?, 'unsigned', ?, 1, 'an action')").bind(userId, budgetRef).run()
    const processOf = rawTx => {
      const txid = Buffer.from(sha(sha(Buffer.from(rawTx)))).reverse().toString('hex')
      return rpc('processAction', [{ identityKey: caller }, {
        isNewTx: true, isSendWith: false, isNoSend: false, isDelayed: false, reference: budgetRef, txid, rawTx, sendWith: [],
      }])
    }
    const logsBefore = logs.length
    const beforeBudget = await counts()
    const over = signed(CAP + 1)
    assert.equal(over.length, CAP + 1)
    const overRefused = await processOf(over)
    assert.deepEqual(overRefused.error, { code: -32602, message: pinnedMore(), data: { code: 'ERR_PAYMENT_SIZE', budget: CAP } }, JSON.stringify(overRefused).slice(0, 600))
    assert.deepEqual(Object.keys(overRefused).sort(), ['error', 'id', 'jsonrpc'])
    // internalizeAction's AtomicBEEF one byte over, as hex: its size known.
    const overBeef = Buffer.concat([Buffer.from(beef.slice(0, 44)), Buffer.alloc(CAP + 1 - 44, 0x22)])
    const beefRefused = await internalize(overBeef.toString('hex'))
    assert.deepEqual(beefRefused.error, { code: -32602, message: pinnedIs(CAP + 1), data: { code: 'ERR_PAYMENT_SIZE', budget: CAP, size: CAP + 1 } }, JSON.stringify(beefRefused).slice(0, 600))
    // An array of 800,000 bytes: under the request's array budget, over the figure.
    const between = signed(800000)
    const betweenRefused = await processOf(between)
    assert.deepEqual(betweenRefused.error, { code: -32602, message: pinnedMore(), data: { code: 'ERR_PAYMENT_SIZE', budget: CAP } }, JSON.stringify(betweenRefused).slice(0, 600))
    assert.deepEqual(await counts(), beforeBudget)
    assert.deepEqual(await db.prepare('SELECT status, txid, raw_tx FROM transactions WHERE reference = ?').bind(budgetRef).first(), { status: 'unsigned', txid: null, raw_tx: null })
    const expected = [
      `payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=none budget=${CAP} identity_key=${caller}`,
      `payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=${CAP + 1} budget=${CAP} identity_key=${caller}`,
      `payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=none budget=${CAP} identity_key=${caller}`,
    ]
    const notJudged = () => logs.slice(logsBefore).filter(l => l.startsWith('payment.not_judged'))
    const deadline = Date.now() + 5000
    while (notJudged().length < expected.length && Date.now() < deadline) await new Promise(r => setTimeout(r, 50))
    assert.deepEqual(notJudged(), expected)
    const exact = signed(CAP)
    assert.equal(exact.length, CAP)
    const handled = await processOf(exact)
    assert.notEqual(handled.error?.data?.code, 'ERR_PAYMENT_SIZE', JSON.stringify(handled).slice(0, 600))
    assert.notEqual(handled.error?.message, 'RPC arrays must not exceed 1000000 items', JSON.stringify(handled).slice(0, 600))
    assert.notEqual(handled.error?.message, 'the transaction is too large to process', JSON.stringify(handled).slice(0, 600))
    assert.equal(notJudged().length, expected.length)
    // 8c'. The positional form: the arguments as an array in the method's
    // field order (internalizeAction: tx first; processAction: rawTx
    // seventh), from a caller with no user row yet. One byte over is refused
    // at the decode door with the pinned sentence and code, before the
    // caller's user is looked up: no table gains a row, the users table
    // included. Exactly the figure is handled as the named form is.
    const freshKey = PrivateKey.fromRandom()
    const fresh = freshKey.toPublicKey().toString()
    const freshClient = clientFrom(freshKey, () => '192.0.2.12')
    const send = async (method, params) => {
      const r = await call(freshClient, method, params)
      assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 600))
      return r.body
    }
    const outputs = [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'measure' } }]
    const positionalInternalize = tx => send('internalizeAction', [{ identityKey: fresh }, [tx, outputs, 'a positional payment', null, null]])
    const positionalProcess = rawTx => {
      const txid = Buffer.from(sha(sha(Buffer.from(rawTx)))).reverse().toString('hex')
      return send('processAction', [{ identityKey: fresh }, [true, false, false, false, budgetRef, txid, rawTx, []]])
    }
    const beforePositional = await counts()
    assert.equal(await db.prepare('SELECT COUNT(*) AS n FROM users WHERE identity_key = ?').bind(fresh).first('n'), 0)
    const positionalLogs = logs.length
    const positionalBeef = await positionalInternalize(overBeef.toString('hex'))
    assert.deepEqual(positionalBeef.error, { code: -32602, message: pinnedIs(CAP + 1), data: { code: 'ERR_PAYMENT_SIZE', budget: CAP, size: CAP + 1 } }, JSON.stringify(positionalBeef).slice(0, 600))
    const positionalRaw = await positionalProcess(over)
    assert.deepEqual(positionalRaw.error, { code: -32602, message: pinnedMore(), data: { code: 'ERR_PAYMENT_SIZE', budget: CAP } }, JSON.stringify(positionalRaw).slice(0, 600))
    assert.deepEqual(await counts(), beforePositional)
    assert.equal(await db.prepare('SELECT COUNT(*) AS n FROM users WHERE identity_key = ?').bind(fresh).first('n'), 0)
    const positionalExpected = [
      `payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=${CAP + 1} budget=${CAP} identity_key=${fresh}`,
      `payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=none budget=${CAP} identity_key=${fresh}`,
    ]
    const positionalLines = () => logs.slice(positionalLogs).filter(l => l.startsWith('payment.not_judged'))
    const positionalDeadline = Date.now() + 5000
    while (positionalLines().length < positionalExpected.length && Date.now() < positionalDeadline) await new Promise(r => setTimeout(r, 50))
    assert.deepEqual(positionalLines(), positionalExpected)
    // Exactly the figure: the positional AtomicBEEF meets what the named one
    // meets, and the positional transaction passes to the storage.
    const atBeef = Buffer.concat([Buffer.from(beef.slice(0, 44)), Buffer.alloc(CAP - 44, 0x22)]).toString('hex')
    const namedAt = await internalize(atBeef)
    const positionalAt = await positionalInternalize(atBeef)
    assert.notEqual(positionalAt.error?.data?.code, 'ERR_PAYMENT_SIZE', JSON.stringify(positionalAt).slice(0, 600))
    assert.deepEqual(positionalAt.error, namedAt.error, JSON.stringify(positionalAt).slice(0, 600))
    const positionalHandled = await positionalProcess(exact)
    assert.notEqual(positionalHandled.error?.data?.code, 'ERR_PAYMENT_SIZE', JSON.stringify(positionalHandled).slice(0, 600))
    assert.notEqual(positionalHandled.error?.message, 'RPC arrays must not exceed 1000000 items', JSON.stringify(positionalHandled).slice(0, 600))
    assert.equal(positionalLines().length, positionalExpected.length)
    say(`positional: internalizeAction's ${overBeef.length}-byte AtomicBEEF (hex, first) -32602 ${JSON.stringify(positionalBeef.error.data)}; processAction's ${over.length}-byte transaction (array, seventh) -32602 ${JSON.stringify(positionalRaw.error.data)}; no table gained a row, the users table included; at the figure the AtomicBEEF meets what the named form meets (${JSON.stringify(namedAt.error ?? namedAt.result).slice(0, 80)}) and the transaction passed to the storage: ${JSON.stringify(positionalHandled.error ?? positionalHandled.result).slice(0, 120)}`)

    say(`payment budget: processAction's ${over.length}-byte transaction (array) -32602 ${JSON.stringify(overRefused.error.data)}; internalizeAction's ${overBeef.length}-byte AtomicBEEF (hex) -32602 ${JSON.stringify(beefRefused.error.data)}; the ${between.length}-byte array, under the array budget, -32602 ${JSON.stringify(betweenRefused.error.data)}; counted "${expected[0]}"; no table gained or lost a row; the ${exact.length}-byte transaction passed to the storage: ${JSON.stringify(handled.error ?? handled.result).slice(0, 160)}`)

    // 8d. Another figure: a Worker whose setting MAX_PAYMENT_BYTES is 2000.
    // Exactly 2,000 bytes are handled as before (here a BEEF the parser
    // refuses, as the default Worker refuses it); one byte over is refused
    // with the figure in force in the sentence, the data and the log line.
    // The default Worker takes the same 2,001 bytes to the parser.
    const FIGURE = 2000
    const figured = new Miniflare({ ...options, bindings: { ...options.bindings, MAX_PAYMENT_BYTES: String(FIGURE) } })
    try {
      const figuredBase = await figured.ready
      await migrate(await figured.getD1Database('DB'))
      const who = clientFrom(callerKey, () => '192.0.2.11')
      const send = async (method, params) => {
        const r = await call(who, method, params, figuredBase)
        assert.equal(r.status, 200, JSON.stringify(r.body))
        return r.body
      }
      const figuredRoot = await (await fetch(new URL('/', figuredBase))).json()
      assert.equal(figuredRoot.max_payment_bytes, FIGURE)
      await send('findOrInsertUser', [caller])
      const payment = n => ({ tx: Buffer.concat([Buffer.from(beef.slice(0, 44)), Buffer.alloc(n - 44, 0x22)]).toString('hex'),
        outputs: [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'measure' } }], description: 'another figure' })
      const figuredLogs = logs.length
      const overFigure = await send('internalizeAction', [{ identityKey: caller }, payment(FIGURE + 1)])
      assert.deepEqual(overFigure.error, { code: -32602, message: pinnedIs(FIGURE + 1, FIGURE), data: { code: 'ERR_PAYMENT_SIZE', budget: FIGURE, size: FIGURE + 1 } }, JSON.stringify(overFigure).slice(0, 600))
      const atFigure = await send('internalizeAction', [{ identityKey: caller }, payment(FIGURE)])
      assert.match(atFigure.error?.message ?? '', /^Failed to parse AtomicBEEF/, JSON.stringify(atFigure).slice(0, 600))
      const atDefault = await internalize(payment(FIGURE + 1).tx)
      assert.match(atDefault.error?.message ?? '', /^Failed to parse AtomicBEEF/, JSON.stringify(atDefault).slice(0, 600))
      const line = `payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=${FIGURE + 1} budget=${FIGURE} identity_key=${caller}`
      const until = Date.now() + 5000
      const figuredLines = () => logs.slice(figuredLogs).filter(l => l.startsWith('payment.not_judged'))
      while (figuredLines().length < 1 && Date.now() < until) await new Promise(r => setTimeout(r, 50))
      assert.deepEqual(figuredLines(), [line])
      say(`another figure (MAX_PAYMENT_BYTES=${FIGURE}): ${FIGURE + 1} bytes -32602 ${JSON.stringify(overFigure.error.data)} "${overFigure.error.message.slice(0, 80)}..."; ${FIGURE} bytes to the parser ("${atFigure.error.message.slice(0, 32)}..."), as the default Worker takes ${FIGURE + 1}; counted "${line}"`)
    } finally {
      await figured.dispose()
    }

    // 8e. A malformed setting: MAX_PAYMENT_BYTES = "600k" is the default, the
    // Worker serves, GET / shows the figure in force, and one log line names
    // the variable and the figure, once for the isolate however many requests
    // read it. The default Worker (no setting) logs no such line.
    const misset = new Miniflare({ ...options, bindings: { ...options.bindings, MAX_PAYMENT_BYTES: '600k' } })
    try {
      const since = logs.length
      const malformedBase = await misset.ready
      const first = await fetch(new URL('/', malformedBase))
      assert.equal(first.status, 200)
      const shown = await first.json()
      assert.equal(shown.max_payment_bytes, 614400)
      const second = await fetch(new URL('/', malformedBase))
      assert.equal((await second.json()).max_payment_bytes, 614400)
      assert.equal((await fetch(new URL('/healthz', malformedBase))).status, 200)
      const line = 'setting.malformed MAX_PAYMENT_BYTES is not a positive whole number of bytes; the figure in force is 614400'
      const seen = () => logs.slice(since).filter(l => l.startsWith('setting.malformed'))
      const until = Date.now() + 5000
      while (seen().length < 1 && Date.now() < until) await new Promise(r => setTimeout(r, 50))
      await new Promise(r => setTimeout(r, 200))
      assert.deepEqual(seen(), [line])
      say(`malformed setting (MAX_PAYMENT_BYTES="600k"): GET / ${JSON.stringify(shown)}; logged once "${line}"`)
    } finally {
      await misset.dispose()
    }
    assert.deepEqual(logs.filter(l => l.startsWith('setting.malformed')).length, 1)
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
    const ipHeaders = draft8(IP_LIMIT, '198.51.100.7')
    assert.deepEqual(headersOf(over.headers, Object.keys(ipHeaders)), ipHeaders)
    const read = await fetch(new URL('/healthz', base), { headers: { 'CF-Connecting-IP': '198.51.100.7' } })
    assert.equal(read.status, 200)
    assert.equal((await post('198.51.100.8')).status, 401)
    say(`pre-auth: ${IP_LIMIT.limit} POSTs from one address reach the door, the next is 429 ERR_RATE_LIMITED with RateLimit ${ipHeaders.ratelimit}; a GET from it and a POST from another pass`)

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
    const identityHeaders = draft8(IDENTITY_LIMIT, `identity:${strangerKey.toPublicKey().toString()}`)
    assert.deepEqual(headersOf(wire, Object.keys(identityHeaders)), identityHeaders)
    const elapsed = Date.now() - windowStart
    assert.ok(elapsed < periodMs - 1000, `the drive left the limiter's window (${elapsed} ms)`)
    say(`identity: ${served} calls of one identity from ${addresses.length} addresses served, the next is 429 ERR_RATE_LIMITED (signed) with RateLimit ${identityHeaders.ratelimit}, in ${elapsed} ms`)
  }

  console.log('Local compiled Worker GREEN (#22): the in-flight ceiling, the body bound, both rate limits (bound or absent on GET /, draft-8 headers on the 429), the list budgets, findOrInsertUser bound to the session, migrate without a write, the collapsed fault, the status route behind the key, healthz and robots.txt, a BEEF measured before it is parsed')
} finally {
  await mf.dispose()
}
