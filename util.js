// Small shared helpers: caches, rate limits, safe fetching.

/** Tiny in-memory cache with an expiry time. */
function ttlCache(ttlMs, max = 500) {
  const m = new Map();
  return {
    get(k) {
      const e = m.get(k);
      if (!e) return undefined;
      if (e.t < Date.now()) { m.delete(k); return undefined; }
      return e.v;
    },
    set(k, v, ms = ttlMs) {
      if (m.size >= max) m.delete(m.keys().next().value);
      m.set(k, { v, t: Date.now() + ms });
    },
    delete(k) { m.delete(k); },
    size() { return m.size; },
  };
}

/** Fixed-window limiter: returns allow(id) -> true/false. max = 0 or Infinity means unlimited. */
function limiter(max, windowMs) {
  const hits = new Map();
  if (Number.isFinite(max) && max > 0) setInterval(() => hits.clear(), windowMs).unref();
  return (id) => {
    if (!max || !Number.isFinite(max)) return true;
    const n = (hits.get(id) || 0) + 1;
    hits.set(id, n);
    return n <= max;
  };
}

/** True for localhost / private-network / link-local hosts (so the API never fetches the owner's internal network). */
function isPrivateHost(hostname) {
  const h = String(hostname || '').toLowerCase().replace(/^\[|\]$/g, '');
  if (!h || h === 'localhost' || h.endsWith('.localhost') || h.endsWith('.local') || h.endsWith('.internal')) return true;
  if (/^\d+\.\d+\.\d+\.\d+$/.test(h)) {
    const [a, b] = h.split('.').map(Number);
    return a === 0 || a === 10 || a === 127 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || a >= 224;
  }
  if (h.includes(':')) return h === '::1' || h === '::' || h.startsWith('fc') || h.startsWith('fd') || h.startsWith('fe80') || h.startsWith('::ffff:');
  return false;
}

/** fetch() with a timeout. Throws Error with .status on non-2xx. Returns parsed JSON (or text with expect:'text'). */
async function request(url, { method = 'GET', headers = {}, body, timeoutMs = 15000, fetchImpl = (...a) => fetch(...a), expect = 'json' } = {}) {
  const h = { ...headers };
  let payload;
  if (body != null) {
    if (typeof body === 'string') payload = body;
    else { payload = JSON.stringify(body); if (!Object.keys(h).some((k) => k.toLowerCase() === 'content-type')) h['Content-Type'] = 'application/json'; }
  }
  const res = await fetchImpl(url, { method, headers: h, body: payload, signal: AbortSignal.timeout(timeoutMs) });
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return expect === 'text' ? res.text() : res.json();
}

const httpError = (status, message) => Object.assign(new Error(message), { status });

module.exports = { ttlCache, limiter, isPrivateHost, request, httpError };
