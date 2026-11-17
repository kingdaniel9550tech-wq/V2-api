/**
 * KING AI API
 * One self-hosted AI endpoint for your bot. It rotates many API keys, falls back
 * across providers automatically, and keeps short per-user chat memory.
 * OpenAI-compatible, so it can replace Groq with a one-line URL change.
 */
require('dotenv').config();
const express = require('express');

const PORT = process.env.PORT || 3000;
// 0 means unlimited
const num = (v, d) => { const n = Number(v === undefined || v === '' ? d : v); return n === 0 ? Infinity : n; };
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 120000); // self-hosted models can be slow
const MAX_TOKENS_CAP = num(process.env.MAX_TOKENS_CAP, 1024);   // longest reply you allow
const MAX_INPUT_CHARS = num(process.env.MAX_INPUT_CHARS, 4000); // longest question you allow
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT || 2);      // how many requests the model runs at once
const DEFAULT_SYSTEM =
  process.env.DEFAULT_SYSTEM ||
  'You are KING AI, a helpful, friendly assistant inside a WhatsApp bot. Keep answers clear and concise.';

const list = (v) =>
  String(v || '')
    .split(/[,\n]/)
    .map((s) => s.trim())
    .filter(Boolean);

/* ---------------- Key pool with rotation + cooldown ---------------- */
class KeyPool {
  constructor(keys) {
    this.keys = keys.map((k) => ({ k, until: 0 }));
    this.i = 0;
  }
  size() { return this.keys.length; }
  available() { return this.keys.filter((x) => x.until <= Date.now()).length; }
  next() {
    for (let n = 0; n < this.keys.length; n++) {
      const slot = this.keys[this.i++ % this.keys.length];
      if (slot.until <= Date.now()) return slot;
    }
    return null;
  }
  report(slot, status, retryAfterSec) {
    if (status === 429) slot.until = Date.now() + (retryAfterSec ? retryAfterSec * 1000 : 60000);
    else if (status === 401 || status === 403) slot.until = Date.now() + 3600000;
    else if (status >= 500 || !status) slot.until = Date.now() + 5000;
  }
}

/* ---------------- Providers (all OpenAI-compatible) ---------------- */
// Model names change over time. Override any of them in .env (e.g. GROQ_MODEL=...).
const PROVIDER_DEFS = {
  groq: {
    url: 'https://api.groq.com/openai/v1/chat/completions',
    model: 'llama-3.3-70b-versatile',
  },
  gemini: {
    url: 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions',
    model: 'gemini-2.5-flash',
  },
  cerebras: {
    url: 'https://api.cerebras.ai/v1/chat/completions',
    model: 'llama-3.3-70b',
  },
  openrouter: {
    url: 'https://openrouter.ai/api/v1/chat/completions',
    model: 'meta-llama/llama-3.3-70b-instruct:free',
  },
  mistral: {
    url: 'https://api.mistral.ai/v1/chat/completions',
    model: 'mistral-small-latest',
  },
  // YOUR OWN model running on your server (Ollama, llama.cpp, vLLM). No external key needed.
  local: {
    url: 'http://localhost:11434/v1/chat/completions',
    model: 'qwen2.5:3b',
    keyless: true,
  },
  // Any other OpenAI-compatible service: set CUSTOM_URL, CUSTOM_MODEL, CUSTOM_KEYS
  custom: { url: process.env.CUSTOM_URL, model: 'default' },
};

const ORDER = list(process.env.PROVIDER_ORDER || 'local');

const providers = ORDER.map((name) => {
  const def = PROVIDER_DEFS[name];
  if (!def) return null;
  const U = name.toUpperCase();
  let keys = list(process.env[`${U}_KEYS`] || process.env[`${U}_KEY`]);
  if (!keys.length && def.keyless) keys = ['none'];
  if (!keys.length) return null;
  return {
    name,
    url: process.env[`${U}_URL`] || def.url,
    model: process.env[`${U}_MODEL`] || def.model,
    pool: new KeyPool(keys),
  };
}).filter(Boolean);

if (!providers.length) console.warn('[WARN] No provider keys found. Fill in .env first.');

async function callProvider(p, slot, messages, opts) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const body = { model: opts.model || p.model, messages };
    if (opts.temperature != null) body.temperature = opts.temperature;
    if (opts.max_tokens != null) body.max_tokens = opts.max_tokens;
    const res = await fetch(p.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(slot.k !== 'none' && { Authorization: `Bearer ${slot.k}` }) },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const detail = (await res.text().catch(() => '')).slice(0, 200);
      const err = new Error(`${p.name} HTTP ${res.status} ${detail}`);
      err.status = res.status;
      err.retryAfter = Number(res.headers.get('retry-after')) || 0;
      throw err;
    }
    const data = await res.json();
    const text = data?.choices?.[0]?.message?.content;
    if (!text) throw Object.assign(new Error(`${p.name} empty reply`), { status: 502 });
    return { text, usage: data.usage, model: data.model || body.model };
  } finally {
    clearTimeout(timer);
  }
}

let running = 0;
const waiting = [];
async function withSlot(fn) {
  if (running >= MAX_CONCURRENT) {
    if (waiting.length >= Number(process.env.MAX_QUEUE || 20)) throw Object.assign(new Error('Server busy, try again shortly'), { status: 503 });
    await new Promise((r) => waiting.push(r));
  }
  running++;
  try { return await fn(); } finally { running--; const nxt = waiting.shift(); if (nxt) nxt(); }
}

function complete(messages, opts = {}) {
  const total = messages.reduce((n, m) => n + String(m.content || '').length, 0);
  if (total > MAX_INPUT_CHARS * 3) throw Object.assign(new Error('Input too long'), { status: 413 });
  const mt = Math.min(Number(opts.max_tokens) || MAX_TOKENS_CAP, MAX_TOKENS_CAP);
  opts.max_tokens = Number.isFinite(mt) ? mt : undefined;
  return withSlot(() => completeInner(messages, opts));
}

async function completeInner(messages, opts = {}) {
  const errors = [];
  const chain = opts.provider ? providers.filter((p) => p.name === opts.provider) : providers;
  for (const p of chain) {
    const tries = Math.min(p.pool.size(), 4);
    for (let i = 0; i < tries; i++) {
      const slot = p.pool.next();
      if (!slot) break;
      try {
        const r = await callProvider(p, slot, messages, opts);
        return { ...r, provider: p.name };
      } catch (e) {
        p.pool.report(slot, e.status, e.retryAfter);
        errors.push(e.name === 'AbortError' ? `${p.name} timeout` : e.message);
        // 400-level errors other than auth/rate-limit are request problems: try next provider
        if (e.status && e.status >= 400 && e.status < 500 && ![401, 403, 429].includes(e.status)) break;
      }
    }
  }
  const err = new Error('All providers failed: ' + (errors.join('; ') || 'no providers configured'));
  err.status = 503;
  throw err;
}

/* ---------------- Per-user memory ---------------- */
const MEM_TURNS = Number(process.env.MEMORY_MESSAGES || 12);
const MEM_TTL = Number(process.env.MEMORY_TTL_MIN || 60) * 60000;
const memory = new Map(); // userId -> { msgs, t }
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of memory) if (now - v.t > MEM_TTL) memory.delete(k);
}, 300000).unref();

/* ---------------- App ---------------- */
const app = express();
app.use(express.json({ limit: '1mb' }));

// KING_API_KEYS=keyA:30,keyB:100  (number = requests per minute for that key; default RATE_LIMIT)
const API_KEYS = new Map(list(process.env.KING_API_KEYS).map((e) => { const [k, l] = e.split(':'); return [k, Number(l) || null]; }));
if (!API_KEYS.size) console.warn('[WARN] KING_API_KEYS is empty: your API is open to anyone.');

app.use((req, res, next) => {
  if (req.path === '/' || req.path === '/health') return next();
  if (!API_KEYS.size) return next();
  const given = (req.headers.authorization || '').replace(/^Bearer\s+/i, '') || req.headers['x-api-key'] || req.query.key;
  if (!API_KEYS.has(given)) return res.status(401).json({ error: 'Invalid API key' });
  req.apiKey = given;
  next();
});

// Simple rate limit: RATE_LIMIT requests per minute per caller
const RATE = num(process.env.RATE_LIMIT, 60);
const USER_RATE = num(process.env.USER_RATE_LIMIT, 10); // per WhatsApp user per minute on /ask
const userHits = new Map();
setInterval(() => userHits.clear(), 60000).unref();
const hits = new Map();
setInterval(() => hits.clear(), 60000).unref();
app.use((req, res, next) => {
  const id = req.apiKey || req.ip;
  const n = (hits.get(id) || 0) + 1;
  hits.set(id, n);
  if (n > (API_KEYS.get(req.apiKey) || RATE)) return res.status(429).json({ error: 'Rate limit exceeded, slow down' });
  next();
});

app.get('/', (_, res) => res.json({ name: 'KING AI API', status: 'online' }));

app.get('/health', (_, res) =>
  res.json({
    status: 'ok',
    uptime: Math.round(process.uptime()),
    providers: providers.map((p) => ({ name: p.name, model: p.model, keys: p.pool.size(), ready: p.pool.available() })),
  })
);

// Simple endpoint for the bot: GET /ask?q=hello&user=2348012345678  or POST {q, user, system}
async function ask(req, res) {
  const src = req.method === 'GET' ? req.query : req.body || {};
  const q = String(src.q || src.text || '').trim();
  if (!q) return res.status(400).json({ error: 'Missing q' });
  const user = String(src.user || 'anon');
  const uh = (userHits.get(user) || 0) + 1;
  userHits.set(user, uh);
  if (uh > USER_RATE) return res.status(429).json({ error: 'You are sending too many requests, wait a minute' });
  if (q.length > MAX_INPUT_CHARS) return res.status(413).json({ error: `Question too long (max ${MAX_INPUT_CHARS} chars)` });
  const system = src.system || DEFAULT_SYSTEM;
  const mem = memory.get(user) || { msgs: [], t: 0 };
  const messages = [{ role: 'system', content: system }, ...mem.msgs, { role: 'user', content: q }];
  try {
    const r = await complete(messages, { provider: src.provider, model: src.model });
    mem.msgs.push({ role: 'user', content: q }, { role: 'assistant', content: r.text });
    mem.msgs = mem.msgs.slice(-MEM_TURNS);
    mem.t = Date.now();
    memory.set(user, mem);
    res.json({ reply: r.text, provider: r.provider, model: r.model });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
}
app.get('/ask', ask);
app.post('/ask', ask);

app.post('/reset', (req, res) => {
  memory.delete(String((req.body || {}).user || 'anon'));
  res.json({ ok: true });
});

// OpenAI-compatible endpoint (drop-in for Groq / OpenAI SDKs)
app.post('/v1/chat/completions', async (req, res) => {
  const { messages, temperature, max_tokens, model } = req.body || {};
  if (!Array.isArray(messages) || !messages.length) return res.status(400).json({ error: 'messages required' });
  try {
    // "model" can be "auto" (default) or a provider name like "groq" / "gemini"
    const provider = providers.some((p) => p.name === model) ? model : undefined;
    const r = await complete(messages, { temperature, max_tokens, provider });
    res.json({
      id: 'chatcmpl-' + Date.now(),
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: r.model,
      choices: [{ index: 0, message: { role: 'assistant', content: r.text }, finish_reason: 'stop' }],
      usage: r.usage || {},
    });
  } catch (e) {
    res.status(e.status || 500).json({ error: { message: e.message } });
  }
});

app.listen(PORT, () => console.log(`KING AI API running on :${PORT} with providers: ${providers.map((p) => p.name).join(', ') || 'none'}`));
