'use strict'

const { test, after } = require('node:test')
const { WebSocketServer } = require('ws')
const { Agent, BalancedPool, Client, Pool, RoundRobinPool, WebSocket } = require('../..')
const diagnosticsChannel = require('node:diagnostics_channel')

test('Fragmented frame with a ping frame in the middle of it', (t) => {
  const server = new WebSocketServer({ port: 0 })

  server.on('connection', (ws) => {
    const socket = ws._socket

    socket.write(Buffer.from([0x01, 0x03, 0x48, 0x65, 0x6c])) // Text frame "Hel"
    socket.write(Buffer.from([0x89, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f])) // ping "Hello"
    socket.write(Buffer.from([0x80, 0x02, 0x6c, 0x6f])) // Text frame "lo"
  })

  after(() => {
    for (const client of server.clients) {
      client.close()
    }

    server.close()
  })

  const ws = new WebSocket(`ws://localhost:${server.address().port}`)

  diagnosticsChannel.channel('undici:websocket:ping').subscribe(
    ({ payload }) => t.assert.deepStrictEqual(payload, Buffer.from('Hello'))
  )

  return new Promise((resolve) => {
    ws.addEventListener('message', ({ data }) => {
      t.assert.strictEqual(data, 'Hello')

      ws.close()
      resolve()
    })
  })
})

test('Too many fragments (uncompressed)', (t, done) => {
  t.plan(4)

  // Tear down only once both peers have observed the close, otherwise the
  // server may finish first and the client-side assertions are lost.
  let closing = 2
  const closed = () => {
    if (--closing === 0) {
      agent.close()
      server.close(done)
    }
  }

  const agent = new Agent({
    webSocket: {
      maxFragments: 3
    }
  })

  const server = new WebSocketServer({ port: 0 }, () => {
    const { port } = server.address()
    const client = new WebSocket(`ws://127.0.0.1:${port}`, {
      dispatcher: agent
    })

    client.addEventListener('error', (event) => {
      t.assert.ok(true)
    })

    client.addEventListener('close', (event) => {
      t.assert.deepStrictEqual(event.code, 1006)
      closed()
    })
  })

  server.on('connection', (ws) => {
    ws.on('close', (code, reason) => {
      t.assert.deepStrictEqual(code, 1008)
      t.assert.deepStrictEqual(reason.toString(), 'Too many message fragments')
      closed()
    })

    const fragment = Buffer.from('a')
    const options = { fin: false }

    ws.send(fragment, options)
    ws.send(fragment, options)
    ws.send(fragment, options)
    ws.send(fragment, options)
  })
})

test('Too many fragments (compressed)', (t, done) => {
  t.plan(4)

  // Tear down only once both peers have observed the close, otherwise the
  // server may finish first and the client-side assertions are lost.
  let closing = 2
  const closed = () => {
    if (--closing === 0) {
      agent.close()
      server.close(done)
    }
  }

  const agent = new Agent({
    webSocket: {
      maxFragments: 3
    }
  })

  const server = new WebSocketServer({
    perMessageDeflate: { threshold: 0 },
    port: 0
  }, () => {
    const { port } = server.address()
    const client = new WebSocket(`ws://127.0.0.1:${port}`, {
      dispatcher: agent
    })

    client.addEventListener('error', (event) => {
      t.assert.ok(true)
    })

    client.addEventListener('close', (event) => {
      t.assert.deepStrictEqual(event.code, 1006)
      closed()
    })
  })

  server.on('connection', (ws) => {
    ws.on('close', (code, reason) => {
      t.assert.deepStrictEqual(code, 1008)
      t.assert.deepStrictEqual(reason.toString(), 'Too many message fragments')
      closed()
    })

    const fragment = Buffer.from('a')
    const options = { fin: false }

    ws.send(fragment, options)
    ws.send(fragment, options)
    ws.send(fragment, options)
    ws.send(fragment, options)
  })
})

test('Too many empty fragments triggers close 1008', (t, done) => {
  // Zero-byte fragments must still count toward maxFragments, otherwise a
  // peer can flood empty continuation frames forever.
  t.plan(4)

  let closing = 2
  const closed = () => {
    if (--closing === 0) {
      agent.close()
      server.close(done)
    }
  }

  const agent = new Agent({
    webSocket: {
      maxFragments: 3
    }
  })

  const server = new WebSocketServer({ port: 0 }, () => {
    const { port } = server.address()
    const client = new WebSocket(`ws://127.0.0.1:${port}`, {
      dispatcher: agent
    })

    client.addEventListener('error', (event) => {
      t.assert.ok(true)
    })

    client.addEventListener('close', (event) => {
      t.assert.deepStrictEqual(event.code, 1006)
      closed()
    })
  })

  server.on('connection', (ws) => {
    ws.on('close', (code, reason) => {
      t.assert.deepStrictEqual(code, 1008)
      t.assert.deepStrictEqual(reason.toString(), 'Too many message fragments')
      closed()
    })

    const fragment = ''
    const options = { fin: false }

    ws.send(fragment, options) // Text frame fin=0, len=0
    ws.send(fragment, options) // Continuation fin=0, len=0
    ws.send(fragment, options) // Continuation fin=0, len=0
    ws.send(fragment, options) // Continuation fin=0, len=0
  })
})

test('Empty first fragment is still part of the message', (t) => {
  // RFC 6455 §5.4 allows zero-byte fragments: a message opened with an empty
  // frame must still be reassembled once its continuation arrives.
  const server = new WebSocketServer({ port: 0 })

  server.on('connection', (ws) => {
    ws.send('', { fin: false }) // Text frame fin=0, len=0
    ws.send('hello', { fin: true }) // Continuation fin=1, "hello"
  })

  t.after(() => {
    for (const client of server.clients) {
      client.close()
    }

    server.close()
  })

  const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}`)

  return new Promise((resolve, reject) => {
    ws.addEventListener('message', ({ data }) => {
      t.assert.strictEqual(data, 'hello')

      ws.close()
      resolve()
    })

    ws.addEventListener('close', (event) => {
      reject(new Error(`connection closed before the message was delivered (${event.code})`))
    })
  })
})

test('Default maxFragments limit is enforced', (t, done) => {
  t.plan(5)

  let closing = 2
  const closed = () => {
    if (--closing === 0) {
      agent.close()
      server.close(done)
    }
  }

  // No webSocket options: the default limit must apply.
  const agent = new Agent()
  const { maxFragments } = agent.webSocketOptions
  t.assert.strictEqual(maxFragments, 131072)

  const server = new WebSocketServer({ port: 0 }, () => {
    const { port } = server.address()
    const client = new WebSocket(`ws://127.0.0.1:${port}`, {
      dispatcher: agent
    })

    client.addEventListener('error', (event) => {
      t.assert.ok(true)
    })

    client.addEventListener('close', (event) => {
      t.assert.deepStrictEqual(event.code, 1006)
      closed()
    })
  })

  server.on('connection', (ws) => {
    ws.on('close', (code, reason) => {
      t.assert.deepStrictEqual(code, 1008)
      t.assert.deepStrictEqual(reason.toString(), 'Too many message fragments')
      closed()
    })

    // One empty text frame (fin=0) followed by `maxFragments` empty
    // continuation frames (fin=0): one fragment more than allowed.
    const frames = Buffer.alloc(2 * (maxFragments + 1))
    frames[0] = 0x01
    ws._socket.write(frames)
  })
})

test('webSocket options are forwarded by every dispatcher', async (t) => {
  const webSocket = { maxFragments: 7 }
  const dispatchers = [
    new Client('http://localhost', { webSocket }),
    new Pool('http://localhost', { webSocket }),
    new RoundRobinPool('http://localhost', { webSocket }),
    new BalancedPool(['http://localhost'], { webSocket }),
    new Agent({ webSocket })
  ]

  t.after(() => Promise.all(dispatchers.map((dispatcher) => dispatcher.close())))

  for (const dispatcher of dispatchers) {
    t.assert.strictEqual(dispatcher.webSocketOptions.maxFragments, 7, dispatcher.constructor.name)
  }
})
