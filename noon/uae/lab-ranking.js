#!/usr/bin/env node
// noon/uae/lab-ranking.js
//
// TEST-ONLY Noon ranking worker (noon_lab). Started by Supabase on the cnj-alytics org
// machines. Measures what gets blocked; it does NOT save any ranking data.
//
// Identity (from the claim's identity_mode):
//   fresh-lower / fresh-upper  new random device + visitor id on every mint (lower / UPPER case)
//   poolN-*                    a saved "phone" from Supabase (same ids every time it is used)
// Flow (from the claim's flow; pacing / call-order experiment):
//   A  5 setup calls back to back, first page at once, 0.5 s between pages (as production)
//   B  3 setup calls only (no repeated set-location + whoami-food), otherwise as A
//   C  B + waits: 2-4 s after launch, 1-2 s after location, 4-8 s before the list
//   D  C + 2-6 s between pages (varied)
//   E  C with 10-14 s before the list and 4-10 s between pages
//   Any flow stops paging after 9 minutes (outcome ok, stop_reason time-cap).
// Akamai log: for every setup call and the first search page (and any page that sets
// cookies) it records the status, Server header and the cookies set (name, size, lifetime;
// value only for Akamai cookies ak_bmsc / bm_* / _abck). On 403/429 it records the
// response headers and the first 400 characters of the body.
//
// Rules for one machine:
//   * Up to MAX_AREAS areas per machine, minting a fresh token for each area.
//   * Each machine gets its own proxy port (sticky IP) via MACHINE_NO.
//   * Walk /search page by page with the test's page gap (page_delay_ms) until a page is
//     empty, adds nothing new, or has no next-page token (cap 150 pages).
//   * 403 or 429 or any other error -> report and continue to next area.
//   * Reports: outcome, failing step/page, pages reached, cards, setup time, time per page.
//
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TEST_ID, MACHINE_NO, GITHUB_RUN_ID

'use strict';

const crypto = require('node:crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TEST_ID   = Number(process.env.TEST_ID);
const MACHINE   = Number(process.env.MACHINE_NO);
const GH_RUN_ID = process.env.GITHUB_RUN_ID || '';
if (!SUPABASE_URL || !SUPABASE_KEY || !TEST_ID || !MACHINE) {
  console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / TEST_ID / MACHINE_NO');
  process.exit(1);
}

// -- Proxy support (Decodo residential) ---------------------------------------
// Each machine gets its own port for a unique sticky IP.
// Decodo UAE ports: 20001-20010. PROXY_URL base port is the starting port.
const PROXY_URL    = process.env.PROXY_URL || '';
const HTTP1_ONLY   = process.env.HTTP1_ONLY   === '1';  // force HTTP/1.1 (no HTTP/2)
const HTTP10_MODE  = process.env.HTTP10_MODE  === '1';  // HTTP/1.0 style: fresh TCP+TLS per request
const ADAPTIVE_HTTP = process.env.ADAPTIVE_HTTP === '1'; // smart cascade: h2 → h1.1 → h1.0
const HTTP3_ONLY   = process.env.HTTP3_ONLY   === '1';  // HTTP/3 via curl (QUIC/UDP)
const undici = require('undici');
const { execFileSync: _execFile } = require('child_process');

// -- Protocol level (mutable in adaptive mode) --------------------------------
// 'h2' = Node default (negotiates HTTP/2), 'h1.1' = HTTP/1.1 only, 'h1.0' = fresh TCP per request
const ALL_PROTOCOLS = ['h2', 'h1.1', 'h1.0'];
let _httpLevel = HTTP10_MODE ? 'h1.0' : (HTTP1_ONLY ? 'h1.1' : 'h2');
let _h11Agent  = null;           // lazy-created, reused within one level stint
let proxyDispatcher = null;

if (PROXY_URL) {
  const pu = new URL(PROXY_URL);
  const basePort = parseInt(pu.port, 10) || 20001;
  const machinePort = basePort + MACHINE - 1;
  const proxyOrigin = `${pu.protocol}//${pu.hostname}:${machinePort}`;
  const agentOpts = { uri: proxyOrigin };
  if (pu.username) {
    agentOpts.token = `Basic ${Buffer.from(
      `${decodeURIComponent(pu.username)}:${decodeURIComponent(pu.password)}`
    ).toString('base64')}`;
  }
  proxyDispatcher = new undici.ProxyAgent(agentOpts);
  console.log(`[proxy] machine ${MACHINE} routing through ${proxyOrigin} (auth: ${pu.username ? 'yes' : 'none'})`);
} else if (ADAPTIVE_HTTP) {
  console.log(`[http] adaptive mode — will cascade h2 → h1.1 → h1.0 on 403`);
} else if (HTTP10_MODE) {
  console.log(`[http] forcing HTTP/1.0 style (fresh TCP+TLS per request, no keep-alive)`);
} else if (HTTP1_ONLY) {
  console.log(`[http] forcing HTTP/1.1 (no H2)`);
} else {
  console.log(`[http] default — HTTP/2 (Node built-in)`);
}

// Dynamic fetch + dispatcher based on current _httpLevel (or proxy)
const proxyFetch = (url, opts) => {
  if (proxyDispatcher) return undici.fetch(url, opts);   // proxy mode
  if (_httpLevel === 'h2') return globalThis.fetch(url, opts); // Node default (H2)
  return undici.fetch(url, opts);                        // h1.1 / h1.0
};
const proxyOpt = () => {
  if (proxyDispatcher) return { dispatcher: proxyDispatcher };
  if (_httpLevel === 'h2') return {};
  if (_httpLevel === 'h1.1') {
    if (!_h11Agent) _h11Agent = new undici.Agent({ allowH2: false });
    return { dispatcher: _h11Agent };
  }
  // h1.0: throwaway single-use Agent → fresh TCP+TLS each time
  return { dispatcher: new undici.Agent({ allowH2: false, connections: 1, pipelining: 0,
    keepAliveTimeout: 10, keepAliveMaxTimeout: 10 }) };
};

// -- HTTP/3 via curl (QUIC/UDP) -----------------------------------------------
// Node.js doesn't support HTTP/3 natively, so we shell out to curl --http3-only.
// Returns a Response-like object the existing code can consume.
function _curlH3(url, opts) {
  const args = ['--http3-only', '-s', '-i', '--max-time', '30'];
  const headers = opts.headers || {};
  for (const [k, v] of Object.entries(headers)) args.push('-H', `${k}: ${v}`);
  if (opts.method && opts.method.toUpperCase() === 'POST') {
    args.push('-X', 'POST');
    if (opts.body) args.push('--data-raw', opts.body);
  }
  args.push('--', url);
  const raw = _execFile('curl', args, { encoding: 'utf-8', timeout: 35000 });

  // Parse curl -i output: status line + headers + \r\n\r\n + body
  const sep = raw.indexOf('\r\n\r\n') >= 0 ? '\r\n\r\n' : '\n\n';
  const si = raw.indexOf(sep);
  const hBlock = si >= 0 ? raw.substring(0, si) : raw;
  const body   = si >= 0 ? raw.substring(si + sep.length) : '';
  const hLines = hBlock.split(/\r?\n/);
  const status = parseInt((hLines[0] || '').split(' ')[1]) || 0;
  const hMap = new Map();
  const setCookies = [];
  for (const ln of hLines.slice(1)) {
    const ci = ln.indexOf(': ');
    if (ci < 0) continue;
    const k = ln.substring(0, ci), v = ln.substring(ci + 2);
    if (k.toLowerCase() === 'set-cookie') setCookies.push(v);
    else hMap.set(k.toLowerCase(), v);
  }
  return {
    status, ok: status >= 200 && status < 300,
    headers: {
      get: (k) => hMap.get(k.toLowerCase()) || null,
      getSetCookie: () => setCookies,
      forEach: (fn) => { hMap.forEach((v, k) => fn(v, k)); setCookies.forEach(c => fn(c, 'set-cookie')); },
    },
    json: async () => JSON.parse(body),
    text: async () => body,
  };
}

// -- Error diagnostics --------------------------------------------------------
function errorChain(e) {
  const parts = [];
  let cur = e;
  while (cur) {
    if (cur.message) parts.push(cur.message);
    if (cur.code) parts.push(`[${cur.code}]`);
    cur = cur.cause;
  }
  return parts.join(' → ') || String(e);
}

const UA    = 'noon/22098 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const BUILD = '22098';
const WHOAMI_URL       = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami';
const SET_LOCATION_URL = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng';
const WHOAMI_FOOD_URL  = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1';
const SEARCH_URL       = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';
const PAGE_LIMIT = 60;
const PAGE_CAP   = 150;
const TIME_CAP_MS = 9 * 60 * 1000;
const MAX_AREAS  = Number(process.env.MAX_AREAS) || 10;
const FLOWS = {
  A: { locate: true,  launch: null,         loc: null,         list: null,          gap: [500, 500] },
  B: { locate: false, launch: null,         loc: null,         list: null,          gap: [500, 500] },
  C: { locate: false, launch: [2000, 4000], loc: [1000, 2000], list: [4000, 8000],  gap: [500, 500] },
  D: { locate: false, launch: [2000, 4000], loc: [1000, 2000], list: [4000, 8000],  gap: [2000, 6000] },
  E: { locate: false, launch: [2000, 4000], loc: [1000, 2000], list: [10000, 14000], gap: [4000, 10000] },
};
const between = (r) => (r ? r[0] + Math.floor(Math.random() * (r[1] - r[0] + 1)) : 0);

// -- Per-area mutable state (reset between areas) -----------------------------
let requests = 0;
let session = null;
let blockInfo = null;
let searchPagesLogged = 0;
const reqLog = [];

function resetAreaState() {
  requests = 0;
  session = null;
  blockInfo = null;
  searchPagesLogged = 0;
  reqLog.length = 0;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (m) => console.log(`[lab t${TEST_ID} m${MACHINE}] ${m}`);

async function rpc(name, params) {
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
        signal: AbortSignal.timeout(20000),
      });
      const text = await resp.text();
      if (!resp.ok) throw new Error(`rpc(${name}) ${resp.status}: ${text.slice(0, 300)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      if (attempt >= 3) throw e;
      log(`rpc(${name}) attempt ${attempt} failed: ${e.message} - retrying`);
      await sleep(2000 * attempt);
    }
  }
}

// -- Noon session -----------------------------------------------------------

function mergeSetCookies(jar, resp) {
  const list = typeof resp.headers.getSetCookie === 'function' ? resp.headers.getSetCookie() : [];
  for (const c of list) { const m = /^([^=]+)=([^;]+)/.exec(c); if (m) jar[m[1]] = m[2]; }
}

// -- Akamai / cookie log ------------------------------------------------------
const AKAMAI = /^(ak_bmsc|bm_|_abck)/;

function describeSetCookies(resp) {
  const list = typeof resp.headers.getSetCookie === 'function' ? resp.headers.getSetCookie() : [];
  return list.map((c) => {
    const parts = c.split(';').map((x) => x.trim());
    const m = /^([^=]+)=(.*)$/.exec(parts[0]) || [];
    const name = m[1] || '?', value = m[2] || '';
    const attr = {};
    for (const p of parts.slice(1)) { const [k, ...v] = p.split('='); attr[k.toLowerCase()] = v.join('=') || true; }
    const out = { name, len: value.length, max_age: attr['max-age'] || null, expires: attr.expires || null,
                  domain: attr.domain || null };
    if (AKAMAI.test(name)) out.value = value.slice(0, 300);
    return out;
  });
}

function logRequest(step, url, opts, resp, ms) {
  const set = describeSetCookies(resp);
  if (step === 'search') { searchPagesLogged++; if (searchPagesLogged > 1 && set.length === 0 && resp.ok) return; }
  const sent = String((opts.headers && opts.headers.Cookie) || '').split(';').map((x) => x.split('=')[0].trim()).filter(Boolean);
  reqLog.push({ step, host: new URL(url).host, status: resp.status, ms, server: resp.headers.get('server'),
                sent_cookies: sent, set });
}

async function captureBlock(step, resp) {
  const headers = {};
  resp.headers.forEach((v, k) => { if (k !== 'set-cookie') headers[k] = String(v).slice(0, 200); });
  let body = '';
  try { body = (await resp.text()).slice(0, 400); } catch (_) {}
  blockInfo = { step, status: resp.status, headers, set: describeSetCookies(resp), body };
}
const cookieStr = (jar) => Object.entries(jar).map(([k, v]) => `${k}=${v}`).join('; ');

function fail(step, status, extra = {}) {
  const kind = status === 403 ? 'blocked' : status === 429 ? 'rate_limited' : 'error';
  return Object.assign(new Error(`${step} ${status ? 'HTTP ' + status : ''}`.trim()), { step, status, kind, ...extra });
}
async function noonFetch(step, url, opts) {
  // -- HTTP/3 via curl (completely separate transport: QUIC/UDP) ---------------
  if (HTTP3_ONLY) {
    let r; const t0 = Date.now(); requests++;
    try { r = _curlH3(url, opts); }
    catch (e) {
      log(`${step} curl h3 error (${Date.now() - t0}ms): ${e.message.split('\n')[0]}`);
      throw Object.assign(fail(step, null), { message: `${step} h3: ${e.message.split('\n')[0]}` });
    }
    try { logRequest(step, url, opts, r, Date.now() - t0); } catch (_) {}
    if (r.status === 403 || r.status === 429) { await captureBlock(step, r); throw fail(step, r.status); }
    return r;
  }
  // -- Adaptive / fixed protocol (TCP-based: h2, h1.1, h1.0) -----------------
  // In adaptive mode: if a request gets 403, retry it with the next protocol down.
  // Keeps the same cookies/session — only the transport changes.
  // h2 → h1.1 → h1.0. Once a lower level works, it sticks for subsequent calls.
  const levels = ADAPTIVE_HTTP
    ? ALL_PROTOCOLS.slice(ALL_PROTOCOLS.indexOf(_httpLevel))   // from current level down
    : [_httpLevel];

  for (let i = 0; i < levels.length; i++) {
    const level = levels[i];
    if (i > 0) {                                               // protocol downgrade
      _httpLevel = level; _h11Agent = null;
      log(`${step} retrying on ${level}...`);
    }
    let r; const t0 = Date.now(); requests++;
    try { r = await proxyFetch(url, { ...opts, signal: AbortSignal.timeout(30000), ...proxyOpt() }); }
    catch (e) {
      const chain = errorChain(e);
      log(`${step} fetch error on ${level} (${Date.now() - t0}ms): ${chain}`);
      if (ADAPTIVE_HTTP && i < levels.length - 1) continue;   // network error → try next protocol
      throw Object.assign(fail(step, null), { message: `${step} network: ${chain}` });
    }
    try { logRequest(step, url, opts, r, Date.now() - t0); } catch (_) {}
    if ((r.status === 403 || r.status === 429) && ADAPTIVE_HTTP && i < levels.length - 1) {
      log(`${step} ${r.status} on ${level} — dropping to ${levels[i + 1]}...`);
      continue;                                                // 403 → try next protocol
    }
    if (r.status === 403 || r.status === 429) { await captureBlock(step, r); throw fail(step, r.status); }
    return r;
  }
}

// identity = saved phone from the claim, or null for a fresh random one
function pickIds(mode, identity) {
  if (identity && identity.device_id) return { deviceId: identity.device_id, visitorId: identity.visitor_id };
  const up = String(mode || '').endsWith('-upper');
  const f = (u) => (up ? u.toUpperCase() : u);
  return { deviceId: f(crypto.randomUUID()), visitorId: f(crypto.randomUUID()) };
}

async function mintToken(lat, lng, ids, flow) {
  const { deviceId, visitorId } = ids;
  const jar = {};
  const base = () => ({
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD, 'x-mp': 'noon', 'x-content': 'mobile',
    Accept: 'application/json', 'x-device-id': deviceId, 'x-visitor-id': visitorId,
  });
  const r1 = await noonFetch('mint_whoami', WHOAMI_URL, {
    headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() } });
  if (!r1.ok) throw fail('mint_whoami', r1.status);
  mergeSetCookies(jar, r1);
  await sleep(between(flow.launch));

  const r2 = await noonFetch('mint_set_location', SET_LOCATION_URL, {
    method: 'POST',
    headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat),
      'x-device-lng': String(lng), 'Content-Type': 'application/json', Cookie: cookieStr(jar) },
    body: JSON.stringify({ location: { lat, lng }, lang: 'en' }) });
  if (!r2.ok) throw fail('mint_set_location', r2.status);
  mergeSetCookies(jar, r2);
  await sleep(between(flow.loc));

  const r3 = await noonFetch('mint_whoami_food', WHOAMI_FOOD_URL, {
    headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat),
      'x-device-lng': String(lng), 'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(jar) } });
  if (!r3.ok) throw fail('mint_whoami_food', r3.status);
  const who = await r3.json().catch(() => ({}));
  mergeSetCookies(jar, r3);
  if (!jar.nguestv2) throw fail('mint_whoami_food', null, { message: 'mint produced no nguestv2' });
  session = { deviceId, visitorId, jar, mintedAt: Date.now() };
  return who && who.headers && typeof who.headers === 'object' ? who.headers : {};
}

function hdrs(lat, lng) {
  return {
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD, 'x-mp': 'noon', 'x-content': 'mobile',
    'x-locale': 'en-ae', 'x-device-id': session.deviceId, 'x-visitor-id': session.visitorId,
    'x-device-lat': String(lat), 'x-device-lng': String(lng), Accept: 'application/json',
  };
}

async function locate(lat, lng) {
  const jar = { ...session.jar };
  const h = { ...hdrs(lat, lng), 'x-experience': 'ecom' };
  const r1 = await noonFetch('locate_set_location', SET_LOCATION_URL, {
    method: 'POST', headers: { ...h, 'Content-Type': 'application/json', Cookie: cookieStr(jar) },
    body: JSON.stringify({ location: { lat, lng }, lang: 'en' }) });
  if (!r1.ok) throw fail('locate_set_location', r1.status);
  mergeSetCookies(jar, r1);
  const r2 = await noonFetch('locate_whoami', WHOAMI_FOOD_URL, {
    headers: { ...h, 'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(jar) } });
  if (!r2.ok) throw fail('locate_whoami', r2.status);
  const who = await r2.json().catch(() => ({}));
  mergeSetCookies(jar, r2);
  const zh = who && who.headers && typeof who.headers === 'object' ? who.headers : {};
  return { lat, lng, zh, cookie: cookieStr(jar) };
}

async function fetchPage(loc, searchToken) {
  const { lat, lng } = loc;
  const headers = {
    ...hdrs(lat, lng), 'x-mp-country': 'ae', 'x-experience': 'food',
    'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)),
    'x-border-enabled': 'true', 'x-rocket-enabled': 'true', 'x-whoami-request-id': crypto.randomUUID(),
    Cookie: loc.cookie, 'Content-Type': 'application/json',
  };
  for (const [k, v] of Object.entries(loc.zh)) {
    if (v != null && typeof v !== 'object' && !k.toLowerCase().includes('ab-test')) headers[k] = String(v);
  }
  const body = {
    queryEntity: null, q: null, f: {}, contexts: [], withContent: true, excludeOutletCodes: [],
    limit: PAGE_LIMIT, page: 1, sort: { by: 'popularity', dir: 'desc' }, type: 'outlet',
    qType: 'search_query', getFallback: true,
  };
  if (searchToken) body.searchToken = searchToken;
  const resp = await noonFetch('search', SEARCH_URL, { method: 'POST', headers, body: JSON.stringify(body) });
  if (resp.status === 404) return { outlets: [], token: null };
  if (!resp.ok) throw fail('search', resp.status);
  const json = await resp.json();
  const outlets = [];
  (function walk(x, d) {
    if (d > 8 || !x) return;
    if (Array.isArray(x)) { for (const v of x) walk(v, d + 1); return; }
    if (typeof x === 'object') {
      if (typeof x.outletCode === 'string' && x.outletCode) { outlets.push(x); return; }
      for (const k of Object.keys(x)) walk(x[k], d + 1);
    }
  })(json.results || [], 0);
  return { outlets, token: json.searchToken || null };
}

// -- Main -------------------------------------------------------------------

(async () => {
  // -- HTTP/3 diagnostic (once per machine) -----------------------------------
  if (HTTP3_ONLY) {
    try {
      const ver = _execFile('curl', ['--version'], { encoding: 'utf-8', timeout: 5000 });
      console.log(`[h3] ${ver.split('\n')[0]}`);
      console.log(`[h3] HTTP/3: ${/http3|HTTP3|nghttp3|quiche/.test(ver) ? 'YES' : 'not detected in features (will try anyway)'}`);
    } catch (e) { console.log(`[h3] curl check failed: ${e.message}`); }
    // Quick test: can curl talk HTTP/3 to Noon?
    try {
      const t0 = Date.now();
      const out = _execFile('curl', ['--http3-only', '-s', '-o', '/dev/null', '-w', '%{http_code} %{http_version}',
        '--max-time', '10', '-H', `User-Agent: ${UA}`, 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami'],
        { encoding: 'utf-8', timeout: 15000 });
      console.log(`[h3] probe noon whoami: ${out.trim()} (${Date.now() - t0}ms)`);
    } catch (e) { console.log(`[h3] probe failed: ${e.message.split('\n')[0]}`); }
  }

  // -- Proxy connectivity check (once per machine) ----------------------------
  let ip = null;
  try {
    const t0 = Date.now();
    if (ADAPTIVE_HTTP) _httpLevel = 'h2';  // start ipify check with h2
    ip = (await (await proxyFetch('https://api.ipify.org', { signal: AbortSignal.timeout(8000), ...proxyOpt() })).text()).trim();
    console.log(`[proxy-check] ipify ok ${Date.now() - t0}ms ip=${ip}`);
  } catch (e) {
    console.log(`[proxy-check] ipify failed: ${errorChain(e)}`);
  }

  // -- Area loop: up to MAX_AREAS claims per machine --------------------------
  let areasCompleted = 0;
  for (let areaNum = 1; areaNum <= MAX_AREAS; areaNum++) {
    // In adaptive mode, reset to H2 so each area gets a fresh shot at the fastest protocol.
    // noonFetch will auto-downgrade per-request if a step gets 403.
    if (ADAPTIVE_HTTP) { _httpLevel = 'h2'; _h11Agent = null; }
    resetAreaState();

    const claim = await rpc('noon_lab_claim', { p_test_id: TEST_ID, p_machine_no: MACHINE, p_github_run_id: GH_RUN_ID, p_runner_ip: ip });
    if (!claim || claim.stop) { log(`area ${areaNum}: nothing to do — ${claim && claim.reason}`); break; }

    const { item_id: itemId, lat, lng, page_delay_ms: delay } = claim;
    const mode = claim.identity_mode || 'fresh-lower';
    const flowName = FLOWS[claim.flow] ? claim.flow : 'A';
    const flow = FLOWS[flowName];
    const ids = pickIds(mode, claim.identity);
    log(`area ${areaNum}/${MAX_AREAS}: id=${claim.noon_area_id} (${claim.area_name}, usually ${claim.usual_pages}p) delay=${delay}ms attempt=${claim.attempt}`);
    log(`flow ${flowName} identity ${mode} device=${ids.deviceId}${ADAPTIVE_HTTP ? ' [adaptive h2→h1.1→h1.0]' : ''}`);

    const t0 = Date.now();
    const pageMs = [];
    const seen = new Set();
    let setupMs = null, page = 0, stopReason = null;
    const result = {};
    try {
      const mintZh = await mintToken(lat, lng, ids, flow);
      const loc = flow.locate ? await locate(lat, lng) : { lat, lng, zh: mintZh, cookie: cookieStr(session.jar) };
      await sleep(between(flow.list));
      setupMs = Date.now() - t0;
      log(`setup done in ${setupMs}ms [${_httpLevel}]`);
      let searchToken = null;
      while (page < PAGE_CAP) {
        page++;
        const p0 = Date.now();
        const r = await fetchPage(loc, searchToken);
        pageMs.push(Date.now() - p0);
        const fresh = r.outlets.filter((o) => !seen.has(o.outletCode));
        for (const o of fresh) seen.add(o.outletCode);
        if (r.outlets.length === 0) { stopReason = `empty@${page}`; break; }
        if (fresh.length === 0)     { stopReason = `no-new@${page}`; break; }
        if (!r.token)               { stopReason = `no-token@${page}`; break; }
        searchToken = r.token;
        if (Date.now() - t0 > TIME_CAP_MS) { stopReason = `time-cap@${page}`; break; }
        await sleep(flow === FLOWS.A ? delay : between(flow.gap));
      }
      if (!stopReason) stopReason = `cap(${PAGE_CAP})`;
      Object.assign(result, { outcome: 'ok', pages_done: page, stop_reason: stopReason });
    } catch (e) {
      Object.assign(result, {
        outcome: e.kind || 'error', fail_step: e.step || 'other', http_status: e.status || null,
        fail_page: e.step === 'search' ? page : null, pages_done: e.step === 'search' ? page - 1 : 0,
        error: String(e.message).slice(0, 500),
      });
    }
    Object.assign(result, {
      cards: seen.size, setup_ms: setupMs, elapsed_ms: Date.now() - t0, page_ms: pageMs,
      token_age_ms: session ? Date.now() - session.mintedAt : null,
      device_id: ids.deviceId, cookies: reqLog, block_info: blockInfo, flow: flowName, requests,
      http_protocol: _httpLevel,   // the level that was active at the end (lowest used)
    });
    log(`area ${areaNum} → ${result.outcome} ${result.fail_step || ''} pages=${result.pages_done} cards=${result.cards} ${result.elapsed_ms}ms [${_httpLevel}]`);
    await rpc('noon_lab_report', { p_test_id: TEST_ID, p_machine_no: MACHINE, p_item_id: itemId, p_result: result });
    areasCompleted++;

    // Brief pause between areas to avoid hammering
    if (areaNum < MAX_AREAS) await sleep(2000);
  }
  log(`done — ${areasCompleted} area(s) completed`);
})().catch(async (e) => {
  console.error(`[lab t${TEST_ID} m${MACHINE}] fatal: ${e.message}`);
  try { await rpc('noon_lab_report', { p_test_id: TEST_ID, p_machine_no: MACHINE, p_item_id: null, p_result: { outcome: 'error', error: e.message } }); } catch (_) {}
  process.exit(1);
});
