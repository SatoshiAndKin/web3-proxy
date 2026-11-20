import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { connect } from 'node:net';
import { makeGateway } from './gateway.mjs';

const token = randomBytes(32).toString('base64url');
const other = randomBytes(32).toString('base64url');
const hash = (v) => createHash('sha256').update(v).digest('hex');
const rpc = (method = 'eth_chainId', params = [], id = 1) =>
  ({ jsonrpc: '2.0', method, params, id });
const address = '0x' + '1'.repeat(40);

async function listen(server, t) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve) => {
    server.close(resolve);
    server.closeAllConnections();
  }));
  return 'http://127.0.0.1:' + server.address().port;
}

async function setup(t, opts = {}) {
  const received = [];
  const upstream = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const data = JSON.parse(Buffer.concat(chunks));
    received.push({ url: req.url, headers: req.headers, data });
    if (opts.reply) return opts.reply(req, res);
    const result = ({ id }) => ({ jsonrpc: '2.0', id, result: '0x1' });
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify(Array.isArray(data) ? data.map(result) : result(data)));
  });
  const endpoint = await listen(upstream, t);
  const root = await listen(makeGateway({
    upstream: endpoint + '/fixed',
    tokenHashes: [hash(token), hash(other)],
    origins: opts.origins ?? ['https://wallet.example'],
    limits: opts.limits,
  }), t);
  const post = (path, body = rpc(), headers = {}) => fetch(root + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });
  return { received, root, post };
}

test('header and URL clients reach only the fixed upstream without caller headers', async (t) => {
  const { post, received } = await setup(t);
  let r = await post('/rpc', rpc(), { authorization: 'Bearer ' + token, cookie: 'secret' });
  assert.equal(r.status, 200);
  assert.equal((await r.json()).result, '0x1');
  r = await post('/rpc/' + token, [rpc(), rpc('eth_blockNumber', [], 'head')]);
  assert.equal(r.status, 200);
  assert.deepEqual((await r.json()).map(({ id }) => id), [1, 'head']);
  assert.equal(received.length, 2);
  for (const req of received) {
    assert.equal(req.url, '/fixed');
    assert.equal(req.headers.authorization, undefined);
    assert.equal(req.headers.cookie, undefined);
    assert(!JSON.stringify(req).includes(token));
  }
});

test('missing and ambiguous credentials never reach the upstream', async (t) => {
  const { post, received } = await setup(t);
  assert.equal((await post('/rpc')).status, 401);
  assert.equal((await post('/rpc/' + 'A'.repeat(43))).status, 401);
  assert.equal((await post('/rpc/' + token, rpc(), { authorization: 'Bearer ' + other })).status, 400);
  assert.ok((await post('/rpc?key=' + token)).status >= 400);
  assert.equal(received.length, 0);
});

test('reject dangerous items and request shapes before forwarding any batch item', async (t) => {
  const { post, received } = await setup(t);
  const path = '/rpc/' + token;
  const malicious = [
    rpc('eth_sendRawTransaction', ['0x12']),
    rpc('personal_sign', []),
    rpc('debug_traceTransaction', []),
    rpc('eth_getLogs', [{}]),
    rpc('eth_call', [{ to: address }, 'latest', {}]),
    rpc('eth_call', [{ to: address, gas: '0xffffffffffff' }, 'latest']),
    rpc('eth_estimateGas', [{ to: address, data: '0x' + 'ab'.repeat(40000) }]),
    rpc('eth_getBlockByNumber', ['latest', true]),
    { jsonrpc: '2.0', method: 'eth_chainId', params: [] },
  ];
  for (const body of malicious) {
    const r = await post(path, [rpc('eth_chainId', [], 'first'), body]);
    assert.ok(r.status >= 400, JSON.stringify(body).slice(0, 180));
  }
  assert.ok((await post(path, '{')).status >= 400);
  assert.equal(received.length, 0);
});

test('CORS is local, restricted, and never replaces authentication', async (t) => {
  const { post, received, root } = await setup(t);
  const path = '/rpc/' + token;
  const r = await fetch(root + path, {
    method: 'OPTIONS',
    headers: {
      origin: 'https://wallet.example',
      'access-control-request-method': 'POST',
      'access-control-request-headers': 'content-type',
    },
  });
  assert.equal(r.status, 204);
  assert.equal(r.headers.get('access-control-allow-origin'), 'https://wallet.example');
  assert.equal(received.length, 0);
  assert.equal((await post(path, rpc(), { origin: 'https://untrusted.example' })).status, 403);
  assert.equal((await post('/rpc', rpc(), { origin: 'https://wallet.example' })).status, 401);
  const allowed = await post(path, rpc(), { origin: 'https://wallet.example' });
  assert.equal(allowed.status, 200);
  assert.equal(allowed.headers.get('access-control-allow-origin'), 'https://wallet.example');
  assert.equal(allowed.headers.get('cache-control'), 'no-store');
});

test('caps are applied before retries or upstream work; a second token has its own budget', async (t) => {
  const { post, received } = await setup(t, {
    limits: { burstUnits: 1, rateUnitsPerMinute: 1, maxRequestBytes: 256 },
  });
  assert.equal((await post('/rpc/' + token, '{' + ' '.repeat(300))).status, 413);
  assert.equal((await post('/rpc/' + token)).status, 200);
  assert.equal((await post('/rpc/' + token)).status, 429);
  assert.equal((await post('/rpc/' + other)).status, 200);
  assert.equal(received.length, 2);
});

test('redirects are not followed and oversized responses are not returned', async (t) => {
  let followed = 0;
  const sink = await listen(createServer((_req, res) => { followed++; res.end('{}'); }), t);
  const redirect = await setup(t, {
    reply: (_req, res) => { res.writeHead(307, { location: sink + '/other' }); res.end(); },
  });
  const rejected = await redirect.post('/rpc/' + token);
  assert.equal(rejected.status, 502);
  assert.equal(rejected.headers.get('location'), null);
  assert.equal(redirect.received.length, 1);
  assert.equal(followed, 0);
  const oversized = await setup(t, {
    limits: { maxResponseBytes: 64 },
    reply: (_req, res) => { res.setHeader('content-type', 'application/json'); res.end('"'+ 'x'.repeat(500) +'"'); },
  });
  const r = await oversized.post('/rpc/' + token);
  assert.equal(r.status, 502);
  assert(!(await r.text()).includes('x'.repeat(20)));
});

test('a slow peer does not hold the gateway indefinitely', async (t) => {
  const { post, received } = await setup(t, {
    limits: { upstreamTimeoutMs: 150 },
    reply: () => {},
  });
  assert.equal((await post('/rpc/' + token)).status, 504);
  assert.equal(received.length, 1);
});

test('accepts a wallet extension explicitly listed and caps call gas automatically', async (t) => {
  const origin = 'chrome-extension://' + 'a'.repeat(32);
  const { post, received } = await setup(t, { origins: [origin] });
  const r = await post('/rpc/' + token, rpc('eth_call', [{ to: address }]), { origin });
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('access-control-allow-origin'), origin);
  assert.deepEqual(received[0].data.params, [{ to: address, gas: '0xf4240' }, 'latest']);
});

test('concurrency cap rejects a new authenticated caller without a second upstream request', async (t) => {
  let admitted;
  let release;
  const firstAdmitted = new Promise((resolve) => { admitted = resolve; });
  const canFinish = new Promise((resolve) => { release = resolve; });
  const { post, received } = await setup(t, {
    limits: { maxInflight: 1 },
    reply: async (_req, res) => {
      admitted();
      await canFinish;
      res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: '0x1' }));
    },
  });
  const first = post('/rpc/' + token);
  try {
    await firstAdmitted;
    assert.equal((await post('/rpc/' + other)).status, 429);
    assert.equal(received.length, 1);
  } finally {
    release();
    assert.equal((await first).status, 200);
  }
});

test('keeps useful revert data without leaking upstream HTTP or RPC error details', async (t) => {
  const { post } = await setup(t, {
    reply: (_req, res) => {
      res.setHeader('set-cookie', 'internal=secret');
      res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, error: {
        code: 3, message: 'internal.example/private', data: '0x1234',
      }}));
    },
  });
  const r = await post('/rpc/' + token);
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('set-cookie'), null);
  assert.deepEqual((await r.json()).error, {
    code: 3, message: 'RPC request failed upstream', data: '0x1234',
  });
});

test('an admitted partial body cannot force the gateway to ingest more concurrent bodies', async (t) => {
  const { post, received, root } = await setup(t, {
    limits: { maxInflight: 1, requestTimeoutMs: 500 },
  });
  const socket = connect(Number(new URL(root).port), '127.0.0.1');
  t.after(() => socket.destroy());
  await once(socket, 'connect');
  socket.write([
    'POST /rpc/' + token + ' HTTP/1.1', 'Host: localhost',
    'Content-Type: application/json', 'Content-Length: 100', '', '{',
  ].join('\r\n'));
  // Give the local HTTP server a turn to receive and admit the partial body.
  let overCapacity = false;
  for (let retry = 0; retry < 10; retry++) {
    const r = await post('/rpc/' + other, '{');
    if (r.status === 429) { overCapacity = true; break; }
    assert.equal(r.status, 400);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.equal(overCapacity, true);
  assert.equal(received.length, 0);
  socket.destroy();
});
