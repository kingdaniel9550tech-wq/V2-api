#!/usr/bin/env node
/**
 * KING WORKER: the part of your downloader that runs on YOUR OWN device (Android phone with Termux, a PC, a laptop).
 * It uses yt-dlp, so nobody else's API is involved, and a normal home / phone connection is much less likely to be
 * blocked than a cloud server. It only makes outgoing requests to your KING API, so it needs no domain or open port.
 *
 *   1. Put HUB_URL and WORKER_TOKEN in worker.env (see worker.env.example)
 *   2. node king-worker.js
 */
const fs = require('fs');
const os = require('os');
const net = require('net');
const path = require('path');
const dns = require('dns').promises;
const { spawn } = require('child_process');
const { Readable } = require('stream');

// ------------------------------------------------------------------ settings
function parseEnvFile(text) {
  const out = {};
  for (const line of String(text || '').split(/\r?\n/)) {
    const m = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/.exec(line);
    if (!m || line.trim().startsWith('#')) continue;
    out[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
  return out;
}

function loadConfig(env = process.env, dir = __dirname) {
  let file = {};
  try { file = parseEnvFile(fs.readFileSync(path.join(dir, 'worker.env'), 'utf8')); } catch {}
  const e = { ...file, ...env }; // real environment variables win over worker.env
  const num = (v, d) => { const n = Number(v); return Number.isFinite(n) && n > 0 ? n : d; };
  return {
    hubUrl: String(e.HUB_URL || '').trim().replace(/\/+$/, ''),
    token: String(e.WORKER_TOKEN || '').trim(),
    name: e.WORKER_NAME || os.hostname(),
    ytdlp: e.YTDLP_PATH || 'yt-dlp',
    ytdlpExplicit: !!e.YTDLP_PATH,
    ytdlpUrl: e.YTDLP_URL || '',
    autoInstall: String(e.YTDLP_AUTO_INSTALL ?? '1') !== '0', // download yt-dlp by itself if it is not installed
    ffmpeg: e.FFMPEG_PATH || 'ffmpeg',
    jsRuntime: e.JS_RUNTIME === undefined ? `node:${process.execPath}` : e.JS_RUNTIME, // yt-dlp needs a JavaScript runtime for YouTube
    concurrency: Math.min(4, Math.round(num(e.CONCURRENCY, 1))),
    maxFileMb: num(e.MAX_FILE_MB, 50),
    maxDiskMb: num(e.MAX_DISK_MB, num(e.MAX_FILE_MB, 50) * 2), // the most one job may ever hold on disk, including half-finished parts
    tmpDir: e.TMP_DIR || '',
    maxHeight: num(e.MAX_HEIGHT, 720),
    jobTimeoutMs: num(e.JOB_TIMEOUT_MS, 70000),
    extraArgs: String(e.YTDLP_EXTRA_ARGS || '').split(/\s+/).filter(Boolean),
    cookies: e.YTDLP_COOKIES || '',
    proxy: e.PROXY_URL || '',                                                           // optional: a proxy for yt-dlp
    ytClients: String(e.YT_CLIENTS ?? 'mweb,tv').split(',').map((x) => x.trim()).filter(Boolean), // extra YouTube modes tried when YouTube shows its bot check
    searchSources: String(e.SEARCH_SOURCES ?? 'youtube,soundcloud').split(',').map((x) => x.trim().toLowerCase()).filter(Boolean),
    update: e.YTDLP_UPDATE || 'self', // self = yt-dlp -U | pip = pip install -U yt-dlp | off
    pip: e.PIP_PATH || 'pip',
    pollTimeoutMs: num(e.POLL_TIMEOUT_MS, 40000),
  };
}

// ------------------------------------------------------------------ helpers
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

/** Runs a program (no shell), returns { code, stdout, stderr }. Never throws. */
function run(cmd, args, { timeoutMs = 60000, cwd, signal } = {}) {
  return new Promise((resolve) => {
    let out = '', err = '', done = false;
    let child;
    try { child = spawn(cmd, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] }); } catch (e) { return resolve({ code: -1, stdout: '', stderr: String(e.message) }); }
    const finish = (code) => { if (done) return; done = true; clearTimeout(timer); resolve({ code, stdout: out, stderr: err }); };
    const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch {} err += '\nTIMEOUT'; finish(-2); }, timeoutMs);
    if (signal) signal.addEventListener('abort', () => { err += '\nDISK_LIMIT'; try { child.kill('SIGKILL'); } catch {} }, { once: true });
    child.stdout.on('data', (d) => { out = (out + d).slice(-20000); });
    child.stderr.on('data', (d) => { err = (err + d).slice(-20000); });
    child.on('error', (e) => { err += String(e.message); finish(-1); });
    child.on('close', (code) => finish(code));
  });
}

function isPrivateIp(ip) {
  const h = String(ip).toLowerCase().replace(/^::ffff:/, '');
  if (/^\d+\.\d+\.\d+\.\d+$/.test(h)) {
    const [a, b] = h.split('.').map(Number);
    return a === 0 || a === 10 || a === 127 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127) || a >= 224;
  }
  return h === '::1' || h === '::' || h.startsWith('fc') || h.startsWith('fd') || h.startsWith('fe80');
}

/** Total size of a folder (including sub-folders), in bytes. */
function dirSize(dir) {
  let total = 0;
  let entries = [];
  try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return 0; }
  for (const e of entries) {
    const f = path.join(dir, e.name);
    try { total += e.isDirectory() ? dirSize(f) : fs.statSync(f).size; } catch {}
  }
  return total;
}

class JobError extends Error { constructor(message, code = 'failed') { super(message); this.code = code; } }

/** Never let a link point yt-dlp at your own network (router, other devices). */
async function assertPublicUrl(raw, lookup = (h) => dns.lookup(h, { all: true })) {
  let u;
  try { u = new URL(raw); } catch { throw new JobError('That link is not valid', 'bad_url'); }
  if (!/^https?:$/.test(u.protocol)) throw new JobError('Only http(s) links are allowed', 'bad_url');
  const host = u.hostname.replace(/^\[|\]$/g, '');
  let addrs;
  try { addrs = net.isIP(host) ? [{ address: host }] : await lookup(host); } catch { throw new JobError('I could not find that website', 'bad_url'); }
  if (!addrs.length || addrs.some((a) => isPrivateIp(a.address))) throw new JobError('That link is not allowed', 'bad_url');
}

function buildArgs(job, cfg, { dir, hasFfmpeg, target, client, impersonate }) {
  const a = ['--no-playlist', '--no-warnings', '--no-progress', '--newline', '--restrict-filenames', '--write-info-json', '--match-filter', '!is_live',
    '--max-filesize', `${cfg.maxFileMb}M`, '-o', path.join(dir, 'out.%(ext)s')];
  if (cfg.jsRuntime && cfg.jsRuntime !== 'none') a.push('--js-runtimes', cfg.jsRuntime);
  if (cfg.cookies) a.push('--cookies', cfg.cookies);
  if (cfg.proxy) a.push('--proxy', cfg.proxy);
  if (client) a.push('--extractor-args', `youtube:player_client=${client}`);
  if (impersonate) a.push('--impersonate', 'chrome'); // makes yt-dlp look like a real Chrome browser (helps with sites that block scripts)
  const h = cfg.maxHeight;
  if (job.type === 'audio') {
    if (hasFfmpeg) a.push('-f', 'bestaudio/best', '-x', '--audio-format', 'mp3', '--audio-quality', '0');
    else a.push('-f', 'bestaudio[ext=m4a]/bestaudio');
  } else if (hasFfmpeg) {
    a.push('-f', `bv*[height<=${h}][ext=mp4]+ba[ext=m4a]/b[height<=${h}][ext=mp4]/b[height<=${h}]/b`, '--merge-output-format', 'mp4');
  } else {
    a.push('-f', `b[ext=mp4][height<=${h}]/b[ext=mp4]/b`);
  }
  a.push(...cfg.extraArgs);
  a.push('--', target || job.url || `ytsearch1:${job.query}`); // "--" so a link can never be read as an option
  return a;
}

/** Turns yt-dlp's long error text into one short sentence for the user. */
function explain(stderr) {
  const s = String(stderr);
  if (/DISK_LIMIT/.test(s)) return new JobError('That file is too big', 'too_big');
  if (/TIMEOUT/.test(s)) return new JobError('The download took too long', 'timeout');
  if (/confirm you.?re not a bot/i.test(s)) return new JobError('YouTube asked this device to sign in (bot check). Try again later, or use a different network.', 'blocked');
  if (/Private video|Video unavailable|This video is (not available|unavailable)|has been removed|account.*terminated/i.test(s)) return new JobError('That video is private or unavailable', 'unavailable');
  if (/Unsupported URL/i.test(s)) return new JobError('That website is not supported', 'unsupported');
  if (/Failed to parse JSON|Unable to download (JSON|webpage)|HTTP Error 403|Forbidden|cloudflare|captcha|Just a moment/i.test(s)) return new JobError("That website is not answering this server (it may be blocking this server's address)", 'blocked');
  if (/Requested format is not available/i.test(s)) return new JobError('No suitable format was found for that video', 'format');
  if (/larger than max-filesize/i.test(s)) return new JobError('That file is too big', 'too_big');
  if (/HTTP Error 429|Too Many Requests/i.test(s)) return new JobError('Too many requests, try again in a few minutes', 'rate_limited');
  const last = s.split('\n').map((l) => l.trim()).filter((l) => /^ERROR/i.test(l)).pop();
  return new JobError(last ? last.replace(/^ERROR:\s*(\[[^\]]+\]\s*[^\s:]+:\s*)?/i, '').slice(0, 200) : 'yt-dlp could not download that', 'failed');
}

// ------------------------------------------------------------------ the worker
function createWorker(cfg, { fetchImpl = (...a) => fetch(...a), lookup, logger = log, binDir = path.join(__dirname, 'bin'), tmpBase = cfg.tmpDir || path.join(__dirname, 'king-tmp'), watchMs = 300 } = {}) {
  let stopped = false;
  let ytdlp = cfg.ytdlp; // may become the copy this worker downloaded itself
  let hasFfmpeg = false;
  let version = '';
  const headers = () => ({ 'x-worker-token': cfg.token });
  const hub = (p) => `${cfg.hubUrl}${p}`;

  // ---- yt-dlp: use the one that is installed, or download the official ready-made program next to this file
  const isMusl = () => process.platform === 'linux' && (() => { try { return !process.report.getReport().header.glibcVersionRuntime; } catch { return fs.existsSync('/etc/alpine-release'); } })();
  function binaryUrls() {
    if (cfg.ytdlpUrl) return [cfg.ytdlpUrl];
    const base = 'https://github.com/yt-dlp/yt-dlp/releases/latest/download/';
    const arm = process.arch === 'arm64' ? '_aarch64' : '';
    if (process.platform === 'win32') return [base + 'yt-dlp.exe'];
    if (process.platform === 'darwin') return [base + 'yt-dlp_macos'];
    const glibc = base + 'yt-dlp_linux' + arm, musl = base + 'yt-dlp_musllinux' + arm;
    return [...(isMusl() ? [musl, glibc] : [glibc, musl]), base + 'yt-dlp']; // last one is the Python version
  }
  async function downloadBinary(url, dest) {
    const res = await fetchImpl(url, { redirect: 'follow', signal: AbortSignal.timeout(180000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    const tmp = dest + '.download';
    await require('stream/promises').pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmp));
    if (fs.statSync(tmp).size < 1000) { fs.rmSync(tmp, { force: true }); throw new Error('the downloaded file is too small'); }
    fs.renameSync(tmp, dest);
    fs.chmodSync(dest, 0o755);
  }
  async function findYtdlp() {
    const works = async (cmd) => (await run(cmd, ['--version'], { timeoutMs: 20000 })).code === 0;
    if (await works(ytdlp)) return;
    if (cfg.ytdlpExplicit) return; // the user pointed at a specific file: do not guess
    const local = path.join(binDir, process.platform === 'win32' ? 'yt-dlp.exe' : 'yt-dlp');
    if (fs.existsSync(local)) { try { fs.chmodSync(local, 0o755); } catch {} if (await works(local)) { ytdlp = local; return; } }
    if (!cfg.autoInstall) return;
    for (const url of binaryUrls()) {
      try {
        logger(`yt-dlp is not installed: downloading it (${path.basename(new URL(url).pathname)})...`);
        await downloadBinary(url, local);
        if (await works(local)) { ytdlp = local; logger('yt-dlp installed'); return; }
        logger('that version does not run here, trying another...');
      } catch (e) { logger(`download failed (${e.message})`); }
    }
  }

  async function checkTools() {
    await findYtdlp();
    const v = await run(ytdlp, ['--version'], { timeoutMs: 20000 });
    if (v.code !== 0) throw new Error(`yt-dlp was not found or does not run (${ytdlp}). Install it first (see README).`);
    version = v.stdout.trim().split('\n')[0];
    hasFfmpeg = (await run(cfg.ffmpeg, ['-version'], { timeoutMs: 20000 })).code === 0;
    return { version, hasFfmpeg };
  }

  async function updateYtdlp() {
    if (cfg.update === 'off') return;
    const r = cfg.update === 'pip' ? await run(cfg.pip, ['install', '-U', 'yt-dlp', 'yt-dlp-ejs'], { timeoutMs: 300000 }) : await run(ytdlp, ['-U'], { timeoutMs: 120000 });
    logger(r.code === 0 ? 'yt-dlp update check done' : 'yt-dlp update check failed (continuing)');
    const v = await run(ytdlp, ['--version'], { timeoutMs: 20000 });
    if (v.code === 0) version = v.stdout.trim().split('\n')[0];
  }

  /** Runs yt-dlp for one job. Returns { file, ext, meta } and a cleanup function. */
  async function download(job) {
    const target = job.url || job.query;
    if (!target || (job.url && job.query)) throw new JobError('Bad job', 'bad_job');
    if (job.url) await assertPublicUrl(job.url, lookup);
    if (job.query && /[\r\n]/.test(job.query)) throw new JobError('Bad search text', 'bad_job');
    fs.mkdirSync(tmpBase, { recursive: true });
    const dir = fs.mkdtempSync(path.join(tmpBase, 'job-'));
    const cleanup = () => fs.rmSync(dir, { recursive: true, force: true });
    // watchdog: yt-dlp's own size limit does not work for every kind of video, so we watch the folder ourselves
    const ac = new AbortController();
    const watch = setInterval(() => { if (dirSize(dir) > cfg.maxDiskMb * 1048576) ac.abort(); }, watchMs);
    try {
      // The layers, in order. A "bot check" from YouTube moves on to the next layer; any other problem with the link itself stops here.
      const isYt = (u) => { try { return /(^|\.)(youtube\.com|youtu\.be|youtube-nocookie\.com)$/.test(new URL(u).hostname); } catch { return false; } };
      const layers = [];
      if (job.url) {
        for (const client of isYt(job.url) ? [null, ...cfg.ytClients] : [null]) layers.push({ target: job.url, client, label: client ? `YouTube mode ${client}` : 'normal' });
        if (!isYt(job.url)) layers.push({ target: job.url, client: null, impersonate: true, label: 'browser mode' });
      } else {
        for (const src of cfg.searchSources) {
          if (src === 'youtube') for (const client of [null, ...cfg.ytClients]) layers.push({ target: `ytsearch1:${job.query}`, client, label: client ? `YouTube search (${client})` : 'YouTube search' });
          if (src === 'soundcloud' && job.type === 'audio') layers.push({ target: `scsearch1:${job.query}`, client: null, label: 'SoundCloud search' });
        }
      }
      const deadline = Date.now() + cfg.jobTimeoutMs;
      const errors = [];
      let r = null;
      for (const layer of layers) {
        const left = deadline - Date.now();
        if (left < 4000) break;
        for (const f of fs.readdirSync(dir)) fs.rmSync(path.join(dir, f), { recursive: true, force: true }); // each layer starts clean
        const res = await run(ytdlp, buildArgs(job, cfg, { dir, hasFfmpeg, target: layer.target, client: layer.client, impersonate: layer.impersonate }), { timeoutMs: left, cwd: dir, signal: ac.signal });
        const produced = res.code === 0 && fs.readdirSync(dir).some((f) => !/\.(info\.json|part|ytdl)$/i.test(f));
        if (produced) { r = res; if (layer.label !== 'normal') logger(`job ${job.id}: worked with ${layer.label}`); break; }
        const why = res.code === 0 ? explain(res.stdout + '\n' + res.stderr) : explain(res.stderr + '\n' + res.stdout);
        errors.push(res.code === 0 && why.code === 'failed' ? new JobError('Nothing was downloaded (the video may be too long or too big)', 'too_big') : why);
        if (why.code === 'too_big' || why.code === 'timeout' || why.code === 'unsupported' || (why.code === 'unavailable' && job.url)) break; // the next layer would not help
      }
      if (!r && job.url && job.type === 'audio' && isAudiomack(job.url)) {
        try { await scrapeAudiomack(job.url, dir); r = { code: 0, stdout: '', stderr: '' }; logger(`job ${job.id}: worked with the Audiomack page reader`); }
        catch (e) { logger(`job ${job.id}: Audiomack page reader failed: ${e.message}`); errors.push(e instanceof JobError ? e : new JobError(`Audiomack: ${e.message}`, 'failed')); }
      }
      clearInterval(watch);
      if (!r) throw errors.find((x) => x.code === 'blocked') || errors[0] || new JobError('Nothing was downloaded', 'failed');
      const files = fs.readdirSync(dir).filter((f) => !/\.(info\.json|part|ytdl|jpg|jpeg|png|webp|vtt|srt)$/i.test(f));
      if (!files.length) {
        const why = explain(r.stdout + '\n' + r.stderr);
        throw why.code === 'failed' ? new JobError('Nothing was downloaded (the video may be too long or too big)', 'too_big') : why;
      }
      const file = files.map((f) => ({ f, size: fs.statSync(path.join(dir, f)).size })).sort((x, y) => y.size - x.size)[0].f;
      let info = {};
      try { info = JSON.parse(fs.readFileSync(path.join(dir, 'out.info.json'), 'utf8')); } catch {}
      return {
        file: path.join(dir, file), ext: path.extname(file).slice(1).toLowerCase() || 'mp4', cleanup,
        meta: { title: info.title || '', thumbnail: info.thumbnail || '', duration: info.duration_string || '', author: info.uploader || info.channel || '' },
      };
    } catch (e) { cleanup(); throw e; } finally { clearInterval(watch); }
  }

  const isAudiomack = (u) => { try { return /(^|\.)audiomack\.com$/.test(new URL(u).hostname); } catch { return false; } };

  /** Last resort for Audiomack: read the song page like a browser and download the audio file it points to. */
  async function scrapeAudiomack(url, dir) {
    const headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', Accept: 'text/html,application/xhtml+xml,*/*;q=0.8', Referer: 'https://audiomack.com/' };
    const page = await fetchImpl(url, { headers, redirect: 'follow', signal: AbortSignal.timeout(30000) });
    if (!page.ok) throw new JobError(`Audiomack answered ${page.status}`, 'blocked');
    const html = await page.text();
    const clean = (u) => u.replace(/\\u002F/gi, '/').replace(/\\\//g, '/').replace(/&amp;/g, '&');
    const found = [...clean(html).matchAll(/https?:\/\/[^"'\\\s<>]+\.(?:mp3|m4a|aac|ogg)[^"'\\\s<>]*/gi)].map((m) => m[0])[0]; // clean the whole page first: sites write slashes as \u002F
    if (!found) throw new JobError('No audio file was found on that Audiomack page', 'failed');
    await assertPublicUrl(found, lookup);
    const meta = /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)/i.exec(html) || /<title>([^<]+)<\/title>/i.exec(html);
    const title = (meta ? meta[1] : 'Audiomack').replace(/&amp;/g, '&').replace(/&#39;/g, "'").trim().slice(0, 150);
    const audio = await fetchImpl(found, { headers, redirect: 'follow', signal: AbortSignal.timeout(120000) });
    if (!audio.ok) throw new JobError(`Audiomack audio answered ${audio.status}`, 'blocked');
    const ext = ((/\.(mp3|m4a|aac|ogg)/i.exec(found) || [])[1] || 'mp3').toLowerCase();
    const cap = cfg.maxFileMb * 1048576;
    let size = 0;
    const counter = new (require('stream').Transform)({ transform(chunk, enc, cb) { size += chunk.length; cb(size > cap ? new JobError('That file is too big', 'too_big') : null, chunk); } });
    await require('stream/promises').pipeline(Readable.fromWeb(audio.body), counter, fs.createWriteStream(path.join(dir, `out.${ext}`)));
    if (size < 10000) throw new JobError('The Audiomack audio file was too small', 'failed');
    fs.writeFileSync(path.join(dir, 'out.info.json'), JSON.stringify({ title }));
  }

  async function upload(jobId, out) {
    const q = new URLSearchParams({ ext: out.ext, title: out.meta.title, thumbnail: out.meta.thumbnail, duration: out.meta.duration, author: out.meta.author });
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const res = await fetchImpl(hub(`/worker/file/${jobId}?${q}`), {
          method: 'PUT', headers: { ...headers(), 'Content-Type': 'application/octet-stream' },
          body: Readable.toWeb(fs.createReadStream(out.file)), duplex: 'half', signal: AbortSignal.timeout(600000),
        });
        if (res.ok) return;
        if (res.status >= 400 && res.status < 500) throw new JobError(`The API refused the file (${res.status})`, 'upload_refused');
      } catch (e) { if (e instanceof JobError || attempt === 3) throw e; }
      await sleep(1500 * attempt);
    }
    throw new JobError('Could not send the file to your API', 'upload_failed');
  }

  async function fail(jobId, err) {
    try {
      await fetchImpl(hub(`/worker/fail/${jobId}`), { method: 'POST', headers: { ...headers(), 'Content-Type': 'application/json' }, body: JSON.stringify({ error: err.message, code: err.code || 'failed', name: cfg.name }), signal: AbortSignal.timeout(20000) });
    } catch {}
  }

  async function handle(job) {
    logger(`job ${job.id}: ${job.type} ${job.url || '"' + job.query + '"'}`);
    let out;
    try {
      out = await download(job);
      await upload(job.id, out);
      logger(`job ${job.id}: sent "${out.meta.title}" (${out.ext})`);
    } catch (e) {
      logger(`job ${job.id}: failed: ${e.message}`);
      await fail(job.id, e);
    } finally { out?.cleanup(); }
  }

  async function loop() {
    let wait = 2000;
    while (!stopped) {
      try {
        const res = await fetchImpl(hub('/worker/poll'), {
          method: 'POST', headers: { ...headers(), 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: cfg.name, ytdlp: version, ffmpeg: hasFfmpeg }), signal: AbortSignal.timeout(cfg.pollTimeoutMs),
        });
        if (res.status === 401) { logger('The API rejected WORKER_TOKEN. Check it matches on both sides.'); await sleep(30000); continue; }
        if (res.status === 503) { logger('The API says the worker is not set up (WORKER_TOKEN missing there).'); await sleep(30000); continue; }
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        wait = 2000;
        const { job } = await res.json();
        if (job) await handle(job);
      } catch (e) {
        if (stopped) break;
        logger(`cannot reach the API (${e.message}); retrying in ${Math.round(wait / 1000)}s`);
        await sleep(wait);
        wait = Math.min(wait * 2, 30000);
      }
    }
  }

  let running = [];
  let updateTimer;
  return {
    checkTools, updateYtdlp, download, buildArgs: (job, o) => buildArgs(job, cfg, o),
    async start() {
      if (!cfg.hubUrl || !cfg.token) throw new Error('Set HUB_URL and WORKER_TOKEN in worker.env first.');
      const t = await this.checkTools();
      // leftovers from a crash or a restart are removed now, so storage can never creep up
      fs.rmSync(tmpBase, { recursive: true, force: true });
      fs.mkdirSync(tmpBase, { recursive: true });
      try { fs.rmSync(path.join(binDir, 'yt-dlp.download'), { force: true }); } catch {}
      logger(`yt-dlp ${t.version} | ffmpeg: ${t.hasFfmpeg ? 'yes' : 'no (audio will be m4a, video single-file mp4)'} | JS runtime: ${cfg.jsRuntime}`);
      await this.updateYtdlp();
      updateTimer = setInterval(() => this.updateYtdlp(), 12 * 3600 * 1000); updateTimer.unref?.();
      logger(`connected to ${cfg.hubUrl} as "${cfg.name}" with ${cfg.concurrency} slot(s). Waiting for jobs...`);
      running = Array.from({ length: cfg.concurrency }, () => loop());
    },
    async stop() { stopped = true; clearInterval(updateTimer); },
    get running() { return running; },
  };
}

/** One-line start for use inside another program (your bot's index.js): does nothing unless HUB_URL and WORKER_TOKEN are set. */
async function startFromEnv(env = process.env, dir = __dirname, deps = {}) {
  const cfg = loadConfig(env, dir);
  if (!cfg.hubUrl || !cfg.token) return null;
  const worker = createWorker(cfg, deps);
  await worker.start();
  return worker;
}

module.exports = { loadConfig, parseEnvFile, createWorker, startFromEnv, buildArgs, explain, assertPublicUrl, isPrivateIp, dirSize, JobError };

if (require.main === module) {
  const cfg = loadConfig();
  const w = createWorker(cfg);
  w.start().catch((e) => { console.error('\n' + e.message + '\n'); process.exit(1); });
  process.on('SIGINT', () => { console.log('\nStopping...'); w.stop().then(() => process.exit(0)); });
    }
