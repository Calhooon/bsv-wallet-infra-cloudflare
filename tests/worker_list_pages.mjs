// The list methods past 100 rows, on the compiled Worker with workerd's D1, which
// binds at most 100 parameters a statement: a wallet seeded straight into D1 with
// 250 actions (each with its own label and a shared one) and 250 outputs (each with
// its own tag and the shared one, and its own custom instructions), 250 certificates
// of two fields and 250 proof requests; listActions and listOutputs at limits 100,
// 101, 250 and 1000 with and without every include, label and tag filters of 150
// names, listCertificates and findProvenTxReqs over filters of 260 names. Every
// row once, the stated total, each row's labels, tags and outputs its own. A limit
// of 1001 is still refused -32602.
//
// worker-build --release
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js \
// BSV_SDK_MODULE=<@bsv/sdk>/dist/esm/mod.js node tests/worker_list_pages.mjs
// LIST_PAGES_DUMP=<file> (same line) also writes every answer of a page of 100 or
// fewer rows, to compare two builds byte for byte.
import { readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'

const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const { AuthFetch, ProtoWallet, PrivateKey } = await import(process.env.BSV_SDK_MODULE ?? '@bsv/sdk')

const sha = s => createHash('sha256').update(s).digest()
const keyOf = label => PrivateKey.fromHex(sha(label).toString('hex'))
// LIST_PAGES_ROWS (default 250, a multiple of 10) seeds a larger wallet, to measure a page of 1000.
const N = Number(process.env.LIST_PAGES_ROWS ?? 250)
const txidOf = i => sha(`list-pages tx ${i}`).toString('hex')
// The first 80% of the actions have one output, the next 10% two, the last 10% none: N outputs.
const voutsOf = i => (i <= N * 0.8 ? [0] : i <= N * 0.9 ? [0, 1] : [])
// The shared label (tag) first on an even action (output), last on an odd one.
const labelsOf = i => (i % 2 === 0 ? ['shared', `label-${i}`] : [`label-${i}`, 'shared'])
const tagsOf = o => (o % 2 === 0 ? ['shared', `tag-${o}`] : [`tag-${o}`, 'shared'])

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
    SERVER_PRIVATE_KEY: keyOf('list-pages server').toHex(),
    CHAINTRACKS_URL: 'https://headers.test',
    BEEF_VERIFICATION: 'strict',
    INTERNALIZE_ZERO_CONF: 'false',
    BROADCASTER: 'arc',
  },
  d1Databases: ['DB'],
  kvNamespaces: ['AUTH_SESSIONS'],
  r2Buckets: ['BLOBS'],
  outboundService: async () => new Response('not found', { status: 404 }),
})

let failed = 0
let note = ''
let passed = 0
const dump = {}
const check = async (name, fn) => {
  try {
    note = ''
    await fn()
    passed++
    console.log(`PASS ${name}${note}`)
  } catch (e) {
    failed++
    console.log(`FAIL ${name}: ${String(e?.message ?? e).split('\n').slice(0, 6).join(' | ')}`)
  }
}

try {
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
    assert.equal(split.status, 0, split.stderr)
    await db.batch(JSON.parse(split.stdout).map(sql => db.prepare(sql)))
  }
  await db.prepare("INSERT INTO settings (storage_identity_key, storage_name, chain, dbtype, max_output_script) VALUES (?, 'list-pages', 'mainnet', 'D1', 10000)")
    .bind(keyOf('list-pages server').toPublicKey().toString()).run()

  const callerKey = keyOf('list-pages caller')
  const caller = callerKey.toPublicKey().toString()
  const client = new AuthFetch(new ProtoWallet(callerKey))
  let id = 0
  const call = async (method, params) => {
    const r = await client.fetch(new URL('/', base).toString(), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }),
    })
    return { status: r.status, text: await r.text() }
  }
  const A = { identityKey: caller }
  const ok = async (method, args) => {
    const started = Date.now()
    const r = await call(method, [A, args])
    note = ` (${r.text.length} bytes, ${Date.now() - started} ms)`
    const body = JSON.parse(r.text)
    assert.ok(body.result !== undefined, `${r.status} ${r.text.slice(0, 300)}`)
    return { result: body.result, text: r.text }
  }
  await call('findOrInsertUser', [caller])
  const user = (await db.prepare('SELECT user_id FROM users WHERE identity_key = ?').bind(caller).first()).user_id

  // ---- the seed, straight into D1 ---------------------------------------------
  const at = '2026-01-01 00:00:00'
  const statements = []
  const add = (sql, ...args) => statements.push(db.prepare(sql).bind(...args))
  add("INSERT INTO output_baskets (basket_id, user_id, name, created_at, updated_at) VALUES (1, ?, 'default', ?, ?)", user, at, at)
  add("INSERT INTO tx_labels (tx_label_id, user_id, label, created_at, updated_at) VALUES (100000, ?, 'shared', ?, ?)", user, at, at)
  add("INSERT INTO output_tags (output_tag_id, user_id, tag, created_at, updated_at) VALUES (100000, ?, 'shared', ?, ?)", user, at, at)
  let oid = 0
  const outputsOf = {}
  for (let i = 1; i <= N; i++) {
    add("INSERT INTO transactions (transaction_id, user_id, reference, status, is_outgoing, description, txid, satoshis, version, lock_time, created_at, updated_at) VALUES (?, ?, ?, 'completed', 0, ?, ?, ?, 1, 0, ?, ?)",
      i, user, `lp-${i}`, `action ${i}`, txidOf(i), i, at, at)
    add('INSERT INTO tx_labels (tx_label_id, user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?, ?)', i, user, `label-${i}`, at, at)
    for (const label of labelsOf(i)) {
      add('INSERT INTO tx_labels_map (tx_label_id, transaction_id, created_at, updated_at) VALUES (?, ?, ?, ?)', label === 'shared' ? 100000 : i, i, at, at)
    }
    outputsOf[i] = []
    for (const vout of voutsOf(i)) {
      oid++
      outputsOf[i].push(oid)
      add("INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, locking_script, script_length, script_offset, custom_instructions, output_description, created_at, updated_at) VALUES (?, ?, ?, 1, 1, 0, ?, ?, 'you', '', 'custom', ?, ?, 2, 0, ?, ?, ?, ?)",
        oid, user, i, vout, 1000 + oid, txidOf(i), new Uint8Array([0x51, oid & 0xff]), `ci-${oid}`, `out ${oid}`, at, at)
      add('INSERT INTO output_tags (output_tag_id, user_id, tag, created_at, updated_at) VALUES (?, ?, ?, ?, ?)', oid, user, `tag-${oid}`, at, at)
      for (const tag of tagsOf(oid)) {
        add('INSERT INTO output_tags_map (output_tag_id, output_id, created_at, updated_at) VALUES (?, ?, ?, ?)', tag === 'shared' ? 100000 : oid, oid, at, at)
      }
    }
    add("INSERT INTO certificates (certificate_id, user_id, serial_number, type, certifier, subject, revocation_outpoint, signature, created_at, updated_at) VALUES (?, ?, ?, 'type', ?, 'subject', 'outpoint', 'sig', ?, ?)",
      i, user, `serial-${i}`, `certifier-${i}`, `2026-01-01 00:${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}`, at)
    for (const field of ['name', 'email']) {
      add('INSERT INTO certificate_fields (user_id, certificate_id, field_name, field_value, master_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)', user, i, field, `${field}-${i}`, `key-${i}`, at, at)
    }
    add("INSERT INTO proven_tx_reqs (txid, raw_tx, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
      txidOf(i), new Uint8Array([i & 0xff]), i % 2 === 0 ? 'completed' : 'unmined', at, at)
  }
  for (let k = 0; k < statements.length; k += 100) await db.batch(statements.slice(k, k + 100))
  assert.equal(oid, N, `the seed holds ${N} outputs`)
  console.log(`seeded ${N} actions, ${oid} outputs, ${N} certificates, ${N} proof requests (${statements.length} statements)`)

  const actionOf = new Map(Array.from({ length: N }, (_, k) => [txidOf(k + 1), k + 1]))
  // The answer's bytes but its id: a failed call on one build shifts the ids after it.
  const record = (name, rows, text) => { if (rows <= 100) dump[name] = text.replace(/"id":\d+/g, '"id":0') }

  // ---- listActions ----------------------------------------------------------------
  const actionIncludes = {
    none: {},
    labels: { includeLabels: true },
    outputs: { includeOutputs: true },
    everything: { includeLabels: true, includeOutputs: true, includeOutputLockingScripts: true, includeInputs: true },
  }
  for (const limit of [100, 101, 250, 1000]) {
    for (const [inc, flags] of Object.entries(actionIncludes)) {
      const name = `listActions limit ${limit} ${inc}`
      await check(name, async () => {
        const { result, text } = await ok('listActions', { labels: [], limit, ...flags })
        const want = Math.min(limit, N)
        assert.equal(result.actions.length, want)
        assert.equal(result.totalActions, N)
        const seen = new Set()
        result.actions.forEach((a, k) => {
          const i = actionOf.get(a.txid)
          assert.ok(i, `an unknown txid ${a.txid}`)
          assert.ok(!seen.has(i), `action ${i} twice`)
          seen.add(i)
          assert.equal(i, N - k, 'newest first')
          assert.equal(a.description, `action ${i}`)
          assert.equal(a.satoshis, i)
          if (flags.includeLabels) assert.deepEqual(a.labels, labelsOf(i), `action ${i}'s labels`)
          else assert.equal(a.labels, undefined)
          if (flags.includeOutputs) {
            assert.deepEqual(a.outputs.map(o => o.outputIndex), voutsOf(i), `action ${i}'s outputs`)
            a.outputs.forEach((o, v) => {
              const own = outputsOf[i][v]
              assert.equal(o.satoshis, 1000 + own)
              assert.deepEqual(o.tags, tagsOf(own), `output ${own}'s tags`)
              assert.equal(o.customInstructions, `ci-${own}`)
              assert.equal(o.outputDescription, `out ${own}`)
              assert.equal(o.basket, 'default')
              if (flags.includeOutputLockingScripts) assert.equal(o.lockingScript, `51${(own & 0xff).toString(16).padStart(2, '0').toUpperCase()}`)
              else assert.equal(o.lockingScript, undefined)
            })
          } else assert.equal(a.outputs, undefined)
        })
        record(name, want, text)
      })
    }
  }
  await check('listActions labels any, 150 names', async () => {
    const labels = Array.from({ length: 150 }, (_, k) => `label-${k + 1}`)
    const { result } = await ok('listActions', { labels, labelQueryMode: 'any', limit: 1000, includeLabels: true })
    assert.equal(result.actions.length, 150)
    assert.equal(result.totalActions, 150)
    assert.deepEqual(result.actions.map(a => actionOf.get(a.txid)), Array.from({ length: 150 }, (_, k) => 150 - k))
    for (const a of result.actions) assert.deepEqual(a.labels, labelsOf(actionOf.get(a.txid)))
  })
  await check('listActions labels all, 120 names of which one holds them all: none', async () => {
    const labels = ['shared', ...Array.from({ length: 119 }, (_, k) => `label-${k + 1}`)]
    const { result } = await ok('listActions', { labels, labelQueryMode: 'all', limit: 1000 })
    assert.equal(result.actions.length, 0)
  })
  await check('listActions labels all, shared and label-7', async () => {
    const { result } = await ok('listActions', { labels: ['shared', 'label-7'], labelQueryMode: 'all', limit: 1000 })
    assert.deepEqual(result.actions.map(a => actionOf.get(a.txid)), [7])
  })

  // ---- listOutputs ----------------------------------------------------------------
  const outputIncludes = {
    none: {},
    tags: { includeTags: true },
    labels: { includeLabels: true },
    custom: { includeCustomInstructions: true },
    everything: { includeTags: true, includeLabels: true, includeCustomInstructions: true, include: 'locking scripts' },
  }
  const ownerOf = {}
  for (const [i, oids] of Object.entries(outputsOf)) for (const o of oids) ownerOf[o] = Number(i)
  const checkOutputs = (result, flags, wantIds) => {
    assert.equal(result.outputs.length, wantIds.length)
    result.outputs.forEach((o, k) => {
      const own = wantIds[k]
      const i = ownerOf[own]
      assert.equal(o.satoshis, 1000 + own, `row ${k}`)
      assert.equal(o.outpoint.txid, txidOf(i))
      assert.equal(o.outpoint.vout, outputsOf[i].indexOf(own))
      if (flags.includeTags) assert.deepEqual(o.tags, tagsOf(own), `output ${own}'s tags`)
      else assert.equal(o.tags, undefined)
      if (flags.includeLabels) assert.deepEqual(o.labels, labelsOf(i), `output ${own}'s labels`)
      else assert.equal(o.labels, undefined)
      if (flags.includeCustomInstructions) assert.equal(o.customInstructions, `ci-${own}`)
      else assert.equal(o.customInstructions, undefined)
      if (flags.include) assert.equal(o.lockingScript, `51${(own & 0xff).toString(16).padStart(2, '0').toUpperCase()}`)
      else assert.equal(o.lockingScript, undefined)
    })
  }
  for (const limit of [100, 101, 250, 1000]) {
    for (const [inc, flags] of Object.entries(outputIncludes)) {
      const name = `listOutputs limit ${limit} ${inc}`
      await check(name, async () => {
        const { result, text } = await ok('listOutputs', { basket: 'default', limit, ...flags })
        const want = Math.min(limit, N)
        assert.equal(result.totalOutputs, N)
        checkOutputs(result, flags, Array.from({ length: want }, (_, k) => k + 1))
        record(name, want, text)
      })
    }
  }
  await check('listOutputs tags any, 150 names', async () => {
    const tags = Array.from({ length: 150 }, (_, k) => `tag-${k + 1}`)
    const { result } = await ok('listOutputs', { basket: 'default', tags, tagQueryMode: 'any', limit: 1000, includeTags: true })
    assert.equal(result.totalOutputs, 150)
    checkOutputs(result, { includeTags: true }, Array.from({ length: 150 }, (_, k) => k + 1))
  })
  await check('listOutputs tags all, shared and tag-9', async () => {
    const { result } = await ok('listOutputs', { basket: 'default', tags: ['shared', 'tag-9'], tagQueryMode: 'all', limit: 1000 })
    assert.deepEqual(result.outputs.map(o => o.satoshis), [1009])
  })

  // ---- listCertificates and findProvenTxReqs -------------------------------------
  const certifiers = Array.from({ length: N + 10 }, (_, k) => `certifier-${k + 1}`)
  for (const limit of [100, 1000]) {
    await check(`listCertificates 260 certifiers, fields, limit ${limit}`, async () => {
      const { result, text } = await ok('listCertificates', { certifiers, types: ['type'], includeFields: true, paged: { limit } })
      const want = Math.min(limit, N)
      assert.equal(result.totalCertificates, N)
      assert.equal(result.certificates.length, want)
      const seen = new Set()
      result.certificates.forEach((c, k) => {
        assert.equal(c.serialNumber, `serial-${N - k}`, 'newest first')
        assert.ok(!seen.has(c.serialNumber))
        seen.add(c.serialNumber)
        assert.deepEqual(c.fields.map(f => [f.certificateId, f.fieldName, f.fieldValue]).sort(),
          [[N - k, 'email', `email-${N - k}`], [N - k, 'name', `name-${N - k}`]])
      })
      record(`listCertificates limit ${limit}`, want, text)
    })
  }
  await check('findProvenTxReqs 260 txids, two statuses', async () => {
    const txids = [...Array.from({ length: N }, (_, k) => txidOf(k + 1)), ...Array.from({ length: 10 }, (_, k) => sha(`absent ${k}`).toString('hex'))]
    const { result } = await ok('findProvenTxReqs', { txids, status: ['completed', 'unmined'], paged: { limit: 1000 } })
    assert.equal(result.length, N)
    assert.equal(new Set(result.map(r => r.txid)).size, N)
    const done = await ok('findProvenTxReqs', { txids, status: ['completed'], paged: { limit: 1000 } })
    assert.equal(done.result.length, N / 2)
    assert.ok(done.result.every(r => r.status === 'completed'))
  })

  // ---- filters of 100 names or fewer: the same answers as before, byte for byte ----
  await check('small filters', async () => {
    const calls = [
      ['listActions', { labels: Array.from({ length: 50 }, (_, k) => `label-${k + 1}`), limit: 100, includeLabels: true, includeOutputs: true }],
      ['listOutputs', { basket: 'default', tags: Array.from({ length: 50 }, (_, k) => `tag-${k + 1}`), limit: 100, includeTags: true, includeLabels: true }],
      ['listCertificates', { certifiers: certifiers.slice(0, 50), includeFields: true, paged: { limit: 100 } }],
      ['findProvenTxReqs', { txids: Array.from({ length: 50 }, (_, k) => txidOf(k + 1)), status: ['completed'], paged: { limit: 100 } }],
    ]
    for (const [method, args] of calls) {
      const { text } = await ok(method, args)
      record(`${method} small filter`, 0, text)
    }
  })

  // ---- the door's limit, unchanged -------------------------------------------------
  for (const [method, args] of [
    ['listActions', { labels: [], limit: 1001 }],
    ['listOutputs', { basket: 'default', limit: 1001 }],
    ['listCertificates', { paged: { limit: 1001 } }],
    ['findProvenTxReqs', { paged: { limit: 1001 } }],
  ]) {
    await check(`${method} limit 1001 refused -32602`, async () => {
      const r = await call(method, [A, args])
      const body = JSON.parse(r.text)
      assert.equal(body.error?.code, -32602, r.text.slice(0, 300))
      assert.equal(body.error.message, 'RPC list limits must not exceed 1000')
    })
  }

  if (process.env.LIST_PAGES_DUMP) writeFileSync(process.env.LIST_PAGES_DUMP, JSON.stringify(dump, null, 1) + '\n')
  console.log(`${passed} passed, ${failed} failed`)
  if (failed) process.exitCode = 1
  else console.log('Local compiled Worker GREEN: the list methods past 100 rows, every row once with its own labels, tags and outputs; limit 1001 refused')
} finally {
  await mf.dispose()
}

