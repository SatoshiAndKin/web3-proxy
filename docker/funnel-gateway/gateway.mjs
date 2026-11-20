import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { pathToFileURL } from 'node:url';

const defaults = Object.freeze({
  maxRequestBytes: 64 * 1024,
  maxResponseBytes: 1024 * 1024,
  maxBatchItems: 16,
  maxGas: 1_000_000,
  maxCalldataBytes: 16 * 1024,
  maxInflight: 8,
  rateUnitsPerMinute: 120,
  burstUnits: 48,
  requestTimeoutMs: 5_000,
  upstreamTimeoutMs: 8_000,
});
const tokenPattern = /^[A-Za-z0-9_-]{43}$/;
const authPattern = /^Bearer ([A-Za-z0-9_-]{43})$/i;
const addressPattern = /^0x[0-9a-fA-F]{40}$/;
const hashPattern = /^0x[0-9a-fA-F]{64}$/;
const quantityPattern = /^0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})$/;
const dataPattern = /^0x(?:[0-9a-fA-F]{2})*$/;
const tags = new Set(['latest', 'earliest', 'safe', 'finalized', 'pending']);
const rpcFields = new Set(['jsonrpc', 'id', 'method', 'params']);
const txQuantityFields = new Set([
  'gas', 'gasPrice', 'value', 'maxFeePerGas', 'maxPriorityFeePerGas', 'nonce', 'chainId', 'type',
]);
const txInputFields = new Set(['from', 'to', 'data', 'input', 'accessList', ...txQuantityFields]);

class GatewayError extends Error {
  constructor(status) {
    super('RPC gateway rejected the request');
    this.status = status;
  }
}
function requireValid(condition) {
  if (!condition) throw new GatewayError(400);
}
function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function quantity(x) {
  requireValid(typeof x === 'string' && quantityPattern.test(x));
  return '0x' + BigInt(x).toString(16);
}
function hex(x, pattern) {
  requireValid(typeof x === 'string' && pattern.test(x));
  return x.toLowerCase();
}
function block(x, allowHashObject = false) {
  if (typeof x === 'string' && tags.has(x)) return x;
  if (allowHashObject && isObject(x)) {
    const keys = Object.keys(x);
    if (Object.hasOwn(x, 'blockHash')) {
      requireValid(keys.every((key) => key === 'blockHash' || key === 'requireCanonical'));
      const result = { blockHash: hex(x.blockHash, hashPattern) };
      if (Object.hasOwn(x, 'requireCanonical')) {
        requireValid(typeof x.requireCanonical === 'boolean');
        result.requireCanonical = x.requireCanonical;
      }
      return result;
    }
    requireValid(keys.length === 1 && keys[0] === 'blockNumber');
    return { blockNumber: quantity(x.blockNumber) };
  }
  return quantity(x);
}
function count(params, min, max = min) {
  requireValid(params.length >= min && params.length <= max);
}
function validId(id) {
  return id === null || Number.isSafeInteger(id)
    || (typeof id === 'string' && id.length <= 128);
}
function transaction(obj, limits) {
  requireValid(isObject(obj) && Object.keys(obj).every((key) => txInputFields.has(key)));
  requireValid(Object.hasOwn(obj, 'to'));
  const tx = { to: hex(obj.to, addressPattern) };
  for (const key of txQuantityFields) {
    if (Object.hasOwn(obj, key)) tx[key] = quantity(obj[key]);
  }
  if (Object.hasOwn(obj, 'from')) tx.from = hex(obj.from, addressPattern);
  requireValid(!(Object.hasOwn(obj, 'input') && Object.hasOwn(obj, 'data')));
  for (const key of ['input', 'data']) {
    if (Object.hasOwn(obj, key)) {
      requireValid(typeof obj[key] === 'string' && obj[key].length <= 2 + 2 * limits.maxCalldataBytes);
      tx.data = hex(obj[key], dataPattern);
    }
  }
  if (Object.hasOwn(obj, 'accessList')) {
    requireValid(Array.isArray(obj.accessList) && obj.accessList.length <= 16);
    tx.accessList = obj.accessList.map((item) => {
      requireValid(isObject(item) && Object.keys(item).length === 2
        && Object.hasOwn(item, 'address') && Object.hasOwn(item, 'storageKeys')
        && Array.isArray(item.storageKeys) && item.storageKeys.length <= 32);
      return {
        address: hex(item.address, addressPattern),
        storageKeys: item.storageKeys.map((key) => hex(key, hashPattern)),
      };
    });
  }
  if (Object.hasOwn(tx, 'gas')) {
    requireValid(BigInt(tx.gas) <= BigInt(limits.maxGas));
  } else {
    tx.gas = '0x' + limits.maxGas.toString(16);
  }
  return tx;
}
function logFilter(obj) {
  const keys = new Set(['address', 'blockHash', 'fromBlock', 'toBlock', 'topics']);
  requireValid(isObject(obj) && Object.keys(obj).every((key) => keys.has(key)));
  requireValid(Object.hasOwn(obj, 'address'));
  const result = {};
  if (Array.isArray(obj.address)) {
    requireValid(obj.address.length > 0 && obj.address.length <= 4);
    result.address = obj.address.map((addr) => hex(addr, addressPattern));
  } else {
    result.address = hex(obj.address, addressPattern);
  }
  if (Object.hasOwn(obj, 'blockHash')) {
    requireValid(!Object.hasOwn(obj, 'fromBlock') && !Object.hasOwn(obj, 'toBlock'));
    result.blockHash = hex(obj.blockHash, hashPattern);
  } else {
    requireValid(Object.hasOwn(obj, 'fromBlock') && Object.hasOwn(obj, 'toBlock'));
    result.fromBlock = quantity(obj.fromBlock);
    result.toBlock = quantity(obj.toBlock);
    const span = BigInt(result.toBlock) - BigInt(result.fromBlock);
    requireValid(span >= 0n && span <= 127n);
  }
  if (Object.hasOwn(obj, 'topics')) {
    requireValid(Array.isArray(obj.topics) && obj.topics.length <= 4);
    result.topics = obj.topics.map((item) => {
      if (item === null) return null;
      if (!Array.isArray(item)) return hex(item, hashPattern);
      requireValid(item.length > 0 && item.length <= 8);
      return item.map((value) => hex(value, hashPattern));
    });
  }
  return result;
}
function canonicalizeMethod(method, args, limits) {
  switch (method) {
    case 'eth_chainId': case 'eth_blockNumber': case 'net_version': case 'eth_gasPrice':
    case 'eth_maxPriorityFeePerGas': case 'web3_clientVersion':
      count(args, 0);
      return [args, 1];
    case 'eth_getBalance': case 'eth_getTransactionCount': case 'eth_getCode':
      count(args, 1, 2);
      return [[hex(args[0], addressPattern), block(args.length === 2 ? args[1] : 'latest', true)], 1];
    case 'eth_getStorageAt':
      count(args, 2, 3);
      return [[hex(args[0], addressPattern), quantity(args[1]), block(args.length === 3 ? args[2] : 'latest', true)], 1];
    case 'eth_getTransactionByHash': case 'eth_getTransactionReceipt':
    case 'eth_getBlockTransactionCountByHash':
      count(args, 1);
      return [[hex(args[0], hashPattern)], 1];
    case 'eth_getBlockTransactionCountByNumber':
      count(args, 1);
      return [[block(args[0])], 1];
    case 'eth_getBlockByNumber': case 'eth_getBlockByHash':
      count(args, 2);
      requireValid(args[1] === false); // Transaction hashes only.
      return [[method === 'eth_getBlockByHash' ? hex(args[0], hashPattern) : block(args[0]), false], 2];
    case 'eth_getTransactionByBlockNumberAndIndex': case 'eth_getTransactionByBlockHashAndIndex':
      count(args, 2);
      return [[method === 'eth_getTransactionByBlockHashAndIndex'
        ? hex(args[0], hashPattern) : block(args[0]), quantity(args[1])], 2];
    case 'eth_feeHistory': {
      count(args, 3);
      const n = quantity(args[0]);
      requireValid(BigInt(n) > 0n && BigInt(n) <= 64n && Array.isArray(args[2]) && args[2].length <= 16);
      let previous = -1;
      for (const percentile of args[2]) {
        requireValid(typeof percentile === 'number' && Number.isFinite(percentile)
          && percentile >= 0 && percentile <= 100 && percentile > previous);
        previous = percentile;
      }
      return [[n, block(args[1]), args[2]], 2 + Math.ceil(Number(BigInt(n)) / 16)];
    }
    case 'eth_call': case 'eth_estimateGas': {
      count(args, 1, 2); // Never admit a state or block override argument.
      const tx = transaction(args[0], limits);
      const params = [tx];
      if (args.length === 2) params.push(block(args[1], method === 'eth_call'));
      if (method === 'eth_call' && params.length === 1) params.push('latest');
      const gasFactor = Math.max(1, Math.ceil(Number(BigInt(tx.gas)) / 250_000));
      return [params, (method === 'eth_estimateGas' ? 3 : 1) + gasFactor];
    }
    case 'eth_getLogs':
      count(args, 1);
      return [[logFilter(args[0])], 12];
    default:
      throw new GatewayError(400);
  }
}
function canonicalize(payload, limits) {
  const batch = Array.isArray(payload);
  const requests = batch ? payload : [payload];
  requireValid(requests.length > 0 && requests.length <= limits.maxBatchItems);
  const ids = new Set();
  let units = 0;
  const result = requests.map((request) => {
    requireValid(isObject(request) && Object.keys(request).every((key) => rpcFields.has(key))
      && request.jsonrpc === '2.0' && Object.hasOwn(request, 'id') && validId(request.id)
      && typeof request.method === 'string' && !ids.has(request.id));
    const args = Object.hasOwn(request, 'params') ? request.params : [];
    requireValid(Array.isArray(args));
    const [params, cost] = canonicalizeMethod(request.method, args, limits);
    ids.add(request.id);
    units += cost;
    return { jsonrpc: '2.0', id: request.id, method: request.method, params };
  });
  return { batch, ids, units, payload: batch ? result : result[0] };
}

function header(req, key) {
  const values = req.headersDistinct[key];
  if (values?.length > 1) throw new GatewayError(400);
  return values?.[0];
}
function validToken(token) {
  return typeof token === 'string' && tokenPattern.test(token)
    && Buffer.from(token, 'base64url').toString('base64url') === token;
}
function upstreamUrl(value) {
  if (typeof value !== 'string' || value.trim() !== value || /[?#]/.test(value)) {
    throw new Error('Invalid RPC_UPSTREAM');
  }
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password
    || !/^https?:\/\/[^/@]+(?:\/|$)/i.test(value)) throw new Error('Invalid RPC_UPSTREAM');
  return url;
}
function checkedOrigins(origins) {
  if (!Array.isArray(origins)) throw new Error('Invalid RPC_ORIGINS');
  for (const origin of origins) {
    if (typeof origin !== 'string') throw new Error('Invalid RPC_ORIGINS');
    // WHATWG URL reports a null origin for extension schemes; validate their
    // serialized Origin header explicitly and do not accept paths or patterns.
    if (/^chrome-extension:\/\/[a-p]{32}$/.test(origin)
      || /^moz-extension:\/\/[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(origin)) {
      continue;
    }
    const url = new URL(origin);
    if (!['http:', 'https:'].includes(url.protocol) || url.origin !== origin) {
      throw new Error('Invalid RPC_ORIGINS');
    }
  }
  return new Set(origins);
}
function checkedKeys(hashes) {
  if (!(Array.isArray(hashes) || hashes instanceof Set)) throw new Error('Invalid RPC_TOKEN_FILE');
  const keys = new Set(hashes);
  if (!keys.size || keys.size > 1024
    || [...keys].some((s) => typeof s !== 'string' || !/^[0-9a-f]{64}$/i.test(s))) {
    throw new Error('Invalid RPC_TOKEN_FILE');
  }
  return new Set([...keys].map((key) => key.toLowerCase()));
}
function readRequest(req, max, timeout) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    const timer = setTimeout(() => finish(new GatewayError(408)), timeout);
    function finish(error) {
      clearTimeout(timer);
      req.removeListener('data', onData);
      req.removeListener('end', onEnd);
      req.removeListener('aborted', onAbort);
      req.removeListener('error', onAbort);
      if (error) {
        req.pause();
        reject(error);
      } else {
        try {
          const data = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks, size));
          resolve(JSON.parse(data));
        } catch {
          reject(new GatewayError(400));
        }
      }
    }
    function onData(chunk) {
      size += chunk.length;
      if (size > max) finish(new GatewayError(413));
      else chunks.push(chunk);
    }
    function onEnd() { finish(); }
    function onAbort() { finish(new GatewayError(400)); }
    req.on('data', onData);
    req.once('end', onEnd);
    req.once('aborted', onAbort);
    req.once('error', onAbort);
  });
}
function sanitizeUpstream(data, request) {
  const batch = Array.isArray(data);
  if (batch !== request.batch) throw new GatewayError(502);
  const items = batch ? data : [data];
  const idsLeft = new Set(request.ids);
  const sanitized = items.map((item) => {
    if (!isObject(item) || item.jsonrpc !== '2.0' || !idsLeft.delete(item.id)
      || Object.hasOwn(item, 'result') === Object.hasOwn(item, 'error')) throw new GatewayError(502);
    if (Object.hasOwn(item, 'result')) return { jsonrpc: '2.0', id: item.id, result: item.result };
    if (!isObject(item.error) || !Number.isSafeInteger(item.error.code)) throw new GatewayError(502);
    const error = { code: item.error.code, message: 'RPC request failed upstream' };
    // Revert data is useful to wallets; never pass through upstream URLs, stack traces, or metadata.
    if (typeof item.error.data === 'string' && dataPattern.test(item.error.data)) {
      error.data = item.error.data;
    }
    return { jsonrpc: '2.0', id: item.id, error };
  });
  if (idsLeft.size) throw new GatewayError(502);
  return batch ? sanitized : sanitized[0];
}
async function forward(upstream, request, limits, res) {
  const client = new AbortController();
  const timeout = AbortSignal.timeout(limits.upstreamTimeoutMs);
  const close = () => client.abort();
  res.once('close', close);
  try {
    const response = await fetch(upstream, {
      method: 'POST',
      redirect: 'error',
      signal: AbortSignal.any([client.signal, timeout]),
      headers: { 'content-type': 'application/json', accept: 'application/json', 'accept-encoding': 'identity' },
      body: JSON.stringify(request.payload),
    });
    if (!response.ok || Number(response.headers.get('content-length')) > limits.maxResponseBytes
      || !response.body || (response.headers.get('content-encoding') ?? 'identity') !== 'identity') {
      if (response.body) await response.body.cancel();
      throw new GatewayError(502);
    }
    const reader = response.body.getReader();
    const chunks = [];
    let size = 0;
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > limits.maxResponseBytes) {
          await reader.cancel();
          throw new GatewayError(502);
        }
        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }
    const sanitized = sanitizeUpstream(
      JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks, size))), request,
    );
    const serialized = JSON.stringify(sanitized);
    if (Buffer.byteLength(serialized) > limits.maxResponseBytes) throw new GatewayError(502);
    return serialized;
  } catch {
    throw new GatewayError(timeout.aborted ? 504 : 502);
  } finally {
    res.removeListener('close', close);
  }
}

function writeJson(res, status, serialized) {
  if (res.destroyed || res.writableEnded) return;
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(serialized);
}
function writeError(req, res, status) {
  if (res.destroyed || res.writableEnded) return;
  if (!req.complete) res.setHeader('Connection', 'close');
  if (status === 401) res.setHeader('WWW-Authenticate', 'Bearer');
  if (status === 429) res.setHeader('Retry-After', '1');
  if (status === 405) res.setHeader('Allow', 'POST, OPTIONS');
  writeJson(res, status, JSON.stringify({ jsonrpc: '2.0', id: null, error: {
    code: -32000,
    message: ({
      400: 'Invalid or unsupported RPC request', 401: 'Unauthorized', 403: 'Forbidden',
      404: 'Not found', 405: 'Method not allowed', 408: 'Request timed out',
      413: 'Request too large', 415: 'Expected application/json', 429: 'Request limit reached',
      502: 'Upstream RPC unavailable', 504: 'Upstream RPC timed out',
    })[status] ?? 'RPC unavailable',
  } }));
}

export function makeGateway({ upstream, tokenHashes, origins = [], limits: options = {} }) {
  const target = upstreamUrl(upstream);
  const cors = checkedOrigins(origins);
  const keys = checkedKeys(tokenHashes);
  if (!isObject(options) || Object.keys(options).some((key) => !Object.hasOwn(defaults, key))) {
    throw new Error('Invalid RPC limits');
  }
  const limits = { ...defaults, ...options };
  if (Object.values(limits).some((n) => !Number.isSafeInteger(n) || n <= 0)
    || limits.maxGas > 5_000_000 || limits.maxBatchItems > 64 || limits.maxInflight > 128) {
    throw new Error('Invalid RPC limits');
  }
  const buckets = new Map();
  let inflight = 0;
  let admitting = 0;

  const server = createServer({
    requestTimeout: Math.max(1_000, limits.requestTimeoutMs),
    headersTimeout: Math.max(1_000, limits.requestTimeoutMs),
    connectionsCheckingInterval: 1_000,
    maxHeaderSize: 8 * 1024,
    requireHostHeader: true,
  }, async (req, res) => {
    // Creating a short error response may close a partially received request.
    req.on('error', () => {});
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Referrer-Policy', 'no-referrer');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Vary', 'Origin, Access-Control-Request-Method, Access-Control-Request-Headers');
    try {
      const pathToken = req.url.startsWith('/rpc/') ? req.url.slice(5) : undefined;
      if (req.url.includes('?') || req.url.includes('#')) throw new GatewayError(400);
      if (req.url !== '/rpc' && !validToken(pathToken)) throw new GatewayError(404);
      const origin = header(req, 'origin');
      if (origin !== undefined) {
        if (!cors.has(origin)) throw new GatewayError(403);
        res.setHeader('Access-Control-Allow-Origin', origin);
      }
      if (req.method === 'OPTIONS') {
        const requested = header(req, 'access-control-request-headers');
        if (!origin || header(req, 'access-control-request-method') !== 'POST'
          || (requested && requested.split(',').some(
            (name) => !['authorization', 'content-type'].includes(name.trim().toLowerCase())))) {
          throw new GatewayError(403);
        }
        res.writeHead(204, {
          'Access-Control-Allow-Methods': 'POST',
          'Access-Control-Allow-Headers': 'authorization, content-type',
          'Access-Control-Max-Age': '600',
        });
        res.end();
        return;
      }
      if (req.method !== 'POST') throw new GatewayError(405);
      const authorization = header(req, 'authorization');
      if (pathToken && authorization !== undefined) throw new GatewayError(400);
      const token = pathToken ?? authPattern.exec(authorization ?? '')?.[1];
      if (!validToken(token)) throw new GatewayError(401);
      const key = createHash('sha256').update(token).digest('hex');
      if (!keys.has(key)) throw new GatewayError(401);
      const contentType = header(req, 'content-type') ?? '';
      const encoding = header(req, 'content-encoding');
      if (!/^application\/json(?:\s*;\s*charset\s*=\s*(?:utf-8|"utf-8"))?$/i.test(contentType)
        || (encoding && encoding.toLowerCase() !== 'identity')) throw new GatewayError(415);
      const length = header(req, 'content-length');
      if (length !== undefined && Number(length) > limits.maxRequestBytes) throw new GatewayError(413);
      if (admitting >= limits.maxInflight) throw new GatewayError(429);
      admitting++;
      let request;
      try {
        request = canonicalize(await readRequest(req, limits.maxRequestBytes, limits.requestTimeoutMs), limits);
      } finally {
        admitting--;
      }
      if (res.destroyed) return;
      if (inflight >= limits.maxInflight) throw new GatewayError(429);
      const now = performance.now();
      const previous = buckets.get(key);
      const available = previous ? Math.min(limits.burstUnits,
        previous.available + (now - previous.at) * limits.rateUnitsPerMinute / 60_000) : limits.burstUnits;
      if (available < request.units) throw new GatewayError(429);
      buckets.set(key, { available: available - request.units, at: now });
      inflight++;
      try {
        writeJson(res, 200, await forward(target, request, limits, res));
      } finally {
        inflight--;
      }
    } catch (error) {
      writeError(req, res, error instanceof GatewayError ? error.status : 502);
    }
  });
  server.keepAliveTimeout = 5_000;
  server.maxConnections = 128;
  server.maxRequestsPerSocket = 100;
  server.on('clientError', (_error, socket) => {
    if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
  });
  return server;
}

function deploymentLimit(name, max, fallback) {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!/^[1-9][0-9]*$/.test(raw) || !Number.isSafeInteger(n) || n > max) {
    throw new Error('Invalid deployment limit');
  }
  return n;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  function failStartup() {
    console.error('RPC gateway failed to start');
    process.exitCode = 1;
  }
  try {
    const port = Number(process.env.PORT ?? '8080');
    if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('Invalid PORT');
    const tokenHashes = JSON.parse(readFileSync(process.env.RPC_TOKEN_FILE, 'utf8'));
    if (!Array.isArray(tokenHashes)) throw new Error('Invalid RPC_TOKEN_FILE');
    const origins = (process.env.RPC_ORIGINS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
    const limits = {
      rateUnitsPerMinute: deploymentLimit('RPC_RATE_UNITS_PER_MINUTE', 6_000, defaults.rateUnitsPerMinute),
      burstUnits: deploymentLimit('RPC_BURST_UNITS', 480, defaults.burstUnits),
      maxInflight: deploymentLimit('RPC_MAX_INFLIGHT', 32, defaults.maxInflight),
    };
    const server = makeGateway({ upstream: process.env.RPC_UPSTREAM, tokenHashes, origins, limits });
    server.on('error', failStartup);
    const host = process.env.RPC_BIND_HOST ?? '127.0.0.1';
    if (!['127.0.0.1', '0.0.0.0'].includes(host)) throw new Error('Invalid RPC_BIND_HOST');
    server.listen(port, host);
  } catch {
    // Configuration may contain private endpoint identifiers; do not log its values.
    failStartup();
  }
}
