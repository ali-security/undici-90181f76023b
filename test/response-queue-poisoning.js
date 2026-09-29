'use strict'

const assert = require('node:assert')
const { once } = require('node:events')
const { createServer } = require('node:http')
const net = require('node:net')
const { after, test } = require('node:test')
const { Client, buildConnector } = require('..')

function readBody (body) {
  return new Promise((resolve, reject) => {
    let data = ''
    body.setEncoding('latin1')
    body.on('data', chunk => { data += chunk })
    body.on('end', () => resolve(data))
    body.on('error', reject)
  })
}

test('should not reuse an idle socket with buffered unsolicited response bytes', async () => {
  let evilServerSocket

  const server = createServer((req, res) => {
    if (!evilServerSocket) {
      evilServerSocket = req.socket
    }

    res.end(req.url)
  })
  after(() => server.close())

  await new Promise(resolve => server.listen(0, resolve))

  const client = new Client(`http://localhost:${server.address().port}`, {
    keepAliveTimeout: 300e3
  })
  after(() => client.close())

  const response1 = await client.request({ path: '/request1', method: 'GET' })
  assert.strictEqual(await readBody(response1.body), '/request1')

  const disconnected = once(client, 'disconnect')

  evilServerSocket.write(
    'HTTP/1.1 200 OK\r\n' +
    'Poison-Free-Socket: true\r\n' +
    'Connection: keep-alive\r\n' +
    'Keep-Alive: timeout=300\r\n' +
    'Content-Length: 0\r\n' +
    '\r\n'
  )

  await disconnected

  const response2 = await client.request({ path: '/request2', method: 'GET' })
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')
})

test('should process unsolicited bytes buffered on an idle socket before reusing it', async () => {
  const server = createServer((req, res) => {
    res.end(req.url)
  })
  after(() => server.close())

  await new Promise(resolve => server.listen(0, resolve))

  const sockets = []
  const connector = buildConnector({})
  const client = new Client(`http://localhost:${server.address().port}`, {
    keepAliveTimeout: 300e3,
    connect (opts, cb) {
      connector(opts, (err, socket) => {
        if (socket) {
          sockets.push(socket)
        }
        cb(err, socket)
      })
    }
  })
  after(() => client.close())

  const response1 = await client.request({ path: '/request1', method: 'GET' })
  assert.strictEqual(await readBody(response1.body), '/request1')

  // The unsolicited response is already buffered on the idle keep-alive
  // socket, but not yet read by the parser, when the next request is
  // dispatched in the same tick.
  sockets[0].unshift(Buffer.from(
    'HTTP/1.1 200 OK\r\n' +
    'Poison-Free-Socket: true\r\n' +
    'Connection: keep-alive\r\n' +
    'Keep-Alive: timeout=300\r\n' +
    'Content-Length: 0\r\n' +
    '\r\n'
  ))
  const response2 = await client.request({ path: '/request2', method: 'GET' })

  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')

  // The poisoned socket must have been discarded instead of reused.
  assert.strictEqual(sockets.length, 2)
  assert.strictEqual(sockets[0].destroyed, true)
})

test('should not deliver an unsolicited response to a queued request', async () => {
  const unsolicited =
    'HTTP/1.1 200 OK\r\n' +
    'Poisoned: true\r\n' +
    'Connection: keep-alive\r\n' +
    'Keep-Alive: timeout=300\r\n' +
    'Content-Length: 6\r\n' +
    '\r\n' +
    'poison'

  let connections = 0
  const sockets = new Set()

  const server = net.createServer((socket) => {
    connections++
    sockets.add(socket)
    socket.on('close', () => sockets.delete(socket))
    socket.setEncoding('latin1')

    let buffered = ''
    socket.on('data', (chunk) => {
      buffered += chunk

      for (;;) {
        const end = buffered.indexOf('\r\n\r\n')
        if (end === -1) {
          break
        }

        const path = buffered.slice(0, end).split(' ')[1]
        buffered = buffered.slice(end + 4)

        const response =
          'HTTP/1.1 200 OK\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          `Content-Length: ${Buffer.byteLength(path)}\r\n` +
          '\r\n' +
          path

        // The genuine response to the first request and an unsolicited extra
        // response are written in a single chunk, while the next request is
        // still queued on the client and has not been written yet.
        socket.write(path === '/request1' ? response + unsolicited : response)
      }
    })
  })
  after(() => {
    for (const socket of sockets) {
      socket.destroy()
    }
    server.close()
  })

  await once(server.listen(0), 'listening')

  const client = new Client(`http://localhost:${server.address().port}`, {
    keepAliveTimeout: 300e3
  })
  after(() => client.close())

  // With the default pipelining of 1, request2 is queued but not written
  // while request1 is in flight.
  const [response1, response2] = await Promise.all([
    client.request({ path: '/request1', method: 'GET' }),
    client.request({ path: '/request2', method: 'GET' })
  ])

  assert.strictEqual(await readBody(response1.body), '/request1')

  assert.strictEqual(response2.headers.poisoned, undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')

  // The socket carrying the unsolicited response must not be reused.
  assert.strictEqual(connections, 2)
})
