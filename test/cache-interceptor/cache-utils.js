'use strict'

const { test } = require('node:test')
const { tspl } = require('@matteo.collina/tspl')
const { normalizeHeaders } = require('../../lib/util/cache')

test('normalizeHeaders preserves repeated iterable headers', (t) => {
  const { ok, strictEqual } = tspl(t, { plan: 4 })

  const headers = normalizeHeaders({
    headers: [
      ['Cache-Control', 'no-store'],
      ['cache-control', 'max-age=60']
    ]
  })

  ok(Array.isArray(headers['cache-control']))
  strictEqual(headers['cache-control'][0], 'no-store')
  strictEqual(headers['cache-control'][1], 'max-age=60')
  strictEqual(headers['cache-control'].length, 2)
})

test('normalizeHeaders preserves repeated plain-object headers with different casing', (t) => {
  const { ok, strictEqual } = tspl(t, { plan: 4 })

  const headers = normalizeHeaders({
    headers: {
      'Cache-Control': 'no-store',
      'cache-control': 'max-age=60'
    }
  })

  ok(Array.isArray(headers['cache-control']))
  strictEqual(headers['cache-control'][0], 'no-store')
  strictEqual(headers['cache-control'][1], 'max-age=60')
  strictEqual(headers['cache-control'].length, 2)
})
