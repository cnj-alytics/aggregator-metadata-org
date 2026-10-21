#!/usr/bin/env node
// cross-platform/uae/v2-detail-scrape.js
//
// v2 restaurant detail scrape. One GitHub Actions run = one machine.
// Supabase (v2_detail_* functions) is the controller: it hands out one restaurant
// at a time from v2.detail_queue; this machine fetches that restaurant's FULL raw
// page from the app (viewed from the fixed point where it was listed) and saves it
// untouched to v2.detail_raw immediately. After jobs_per_machine restaurants it
// exits and Supabase starts a fresh machine (fresh IP). 403/429 = stop this machine.
//
// Endpoints (full responses, nothing trimmed):
//   talabat   GET  api.talabat.com/menubff/v4/branches/{bid}/menu
//   careem    GET  apigateway.careemdash.com/v1/restaurants/{merchant_id}   (v2's own token pool)
//   deliveroo GET  {menu page}?geohash={point}  -> __NEXT_DATA__ (menuPage + query saved)
//   noon      POST outlet/details/guest/partial (guest token minted on this machine)
//
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RUN_ID, MACHINE_NO, GITHUB_RUN_ID

'use strict';

const crypto = require('node:crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const RUN_ID = Number(process.env.RUN_ID);
const MACHINE = Number(process.env.MACHINE_NO);
const GH_RUN_ID = process.env.GITHUB_RUN_ID || '';
if (!SUPABASE_URL || !SUPABASE_KEY || !RUN_ID || !MACHINE) {
  console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / RUN_ID / MACHINE_NO');
  process.exit(1);
}

const START = Date.now();
const WALL_LIMIT_MS = 25 * 60 * 1000;
const HTTP_TIMEOUT_MS = 30_000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (m) => console.log(`[${new Date().toISOString().slice(11, 19)}] [m${MACHINE}] ${m}`);

async function rpc(name, params) {
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(params), signal: AbortSignal.timeout(90_000),
      });
      const text = await resp.text();
      if (!resp.ok) throw new Error(`rpc(${name}) ${resp.status}: ${text.slice(0, 200)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      if (attempt >= 4) throw e;
      log(`rpc(${name}) attempt ${attempt} failed: ${e.message} - retrying`);
      await sleep(3_000 * attempt);
    }
  }
}

class Blocked extends Error { constructor(m, s) { super(m); this.status = s; } }

async function http(url, opts = {}) {
  const t0 = Date.now();
  const resp = await fetch(url, { ...opts, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  const text = await resp.text();
  if (resp.status === 429 || resp.status === 403) throw new Blocked(`${new URL(url).host} HTTP ${resp.status}`, resp.status);
  return { resp, status: resp.status, text, elapsed: Date.now() - t0 };
}

// save -> v2.detail_raw ; returns parsed json (or null)
async function save(item, endpoint, r, request, jsonOverride) {
  let json = jsonOverride !== undefined ? jsonOverride : null;
  if (jsonOverride === undefined) { try { json = r.text ? JSON.parse(r.text) : null; } catch (_) { json = null; } }
  await rpc('v2_detail_save', {
    p_run: RUN_ID, p_machine: MACHINE, p_platform: item.platform, p_key: item.branch_key, p_endpoint: endpoint,
    p_http: r.status, p_body: json, p_body_text: json === null ? (r.text || null) : null,
    p_bytes: r.text ? Buffer.byteLength(r.text) : 0, p_ms: r.elapsed, p_request: request,
  });
  return json;
}

// -- geohash (Deliveroo views the page from the listing point) --------------
const GH32 = '0123456789bcdefghjkmnpqrstuvwxyz';
function geohash(lat, lng, p = 12) {
  const la = [-90, 90], lo = [-180, 180]; let even = true, bit = 0, ch = 0, s = '';
  while (s.length < p) {
    const r = even ? lo : la, v = even ? lng : lat, mid = (r[0] + r[1]) / 2;
    if (v >= mid) { ch |= 16 >> bit; r[0] = mid; } else r[1] = mid;
    even = !even;
    if (++bit === 5) { s += GH32[ch]; bit = 0; ch = 0; }
  }
  return s;
}

// -- Talabat ----------------------------------------------------------------
async function talabat(item) {
  const url = `https://api.talabat.com/menubff/v4/branches/${item.branch_key}/menu`;
  const r = await http(url, { headers: {
    'User-Agent': 'talabat/8701 CFNetwork/3896.100.1.2.1 Darwin/27.0.0', Accept: '*/*', 'Accept-Language': 'en-US',
    'Accept-Encoding': 'gzip, deflate, br', appbrand: '1', 'x-country': 'ae', 'x-app-version': '13.93.0', 'x-device-version': '13.93.0',
    'x-device-source': '4', 'x-device-framework': 'flutter', 'x-marshmallow-version': 'mm3', 'http2-enabled': 'true', tokentypekey: 'guest' } });
  const j = await save(item, 'menubff_v4', r, { method: 'GET', url });
  if (r.status === 404) return 'unavailable';
  if (r.status !== 200 || !j) throw new Error(`talabat HTTP ${r.status}`);
  return 'done';
}

// -- Careem (v2's own token pool) -------------------------------------------
let careemTok = null;
async function careem(item) {
  if (!careemTok) careemTok = await rpc('v2_careem_token', {});
  if (!careemTok) throw new Error('no v2 Careem token');
  let sub = '';
  try { sub = JSON.parse(Buffer.from(careemTok.split('.')[1].replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8')).sub || ''; } catch (_) {}
  const lat = String(item.lat), lng = String(item.lng);
  const sess = 'V2DS-' + crypto.randomBytes(4).toString('hex').toUpperCase();
  const url = `https://apigateway.careemdash.com/v1/restaurants/${item.branch_key}`;
  const r = await http(url, { headers: {
    SESSION_ID: sess, 'X-Careem-Beta': 'false', 'User-Agent': 'ICMA/26.39.0', 'X-Careem-Agent': 'ICMA', 'X-Careem-Session-Id': sess,
    Agent: 'ICMA', 'Time-Zone': 'Asia/Dubai', lat, lng, 'x-careem-userid': String(sub), Version: '26.39.0', 'X-Careem-Version': '26.39.0',
    'x-careem-user-location': `${lat},${lng}`, 'x-careem-appengine-api-version': '2026-09-17', 'X-Careem-Operating-System': 'iOS/27.0.1',
    Authorization: `Bearer ${careemTok}`, 'x-careem-permissions': 'location:granted', 'Accept-Language': 'en',
    'x-careem-device-id': 'D0O8gpXoJdQ2L5lC', Accept: '*/*' } });
  const j = await save(item, 'restaurant', r, { method: 'GET', url, lat, lng });
  if (r.status === 404) return 'unavailable';
  if (r.status === 401) { careemTok = null; throw new Error('careem 401 (token)'); }
  if (r.status !== 200 || !j) throw new Error(`careem HTTP ${r.status}`);
  return 'done';
}

// -- Deliveroo ----------------------------------------------------------------
async function deliveroo(item) {
  if (!item.page_url) {
    await save(item, 'menu_page_next_data', { status: null, text: null, elapsed: 0 }, { note: 'no page url known' }, null);
    return 'unavailable';
  }
  const gh = geohash(Number(item.lat), Number(item.lng));
  const url = `${item.page_url}?geohash=${gh}`;
  const r = await http(url, { headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    Accept: 'text/html,application/xhtml+xml', 'Accept-Language': 'en-US,en;q=0.5' }, redirect: 'follow' });
  if (r.status === 404) { await save(item, 'menu_page_next_data', r, { method: 'GET', url }, null); return 'unavailable'; }
  const m = /<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/.exec(r.text || '');
  let d = null; try { d = m ? JSON.parse(m[1]) : null; } catch (_) { d = null; }
  const body = d ? { menuPage: d?.props?.initialState?.menuPage ?? null, query: d.query ?? null, page: d.page ?? null, buildId: d.buildId ?? null } : null;
  await save(item, 'menu_page_next_data', { ...r, text: d ? null : r.text }, { method: 'GET', url, geohash: gh, final_url: r.resp.url }, body);
  if (r.status !== 200) throw new Error(`deliveroo HTTP ${r.status}`);
  if (!body || !body.menuPage) return 'unavailable';
  return 'done';
}

// -- Noon (token minted on this machine, re-located per outlet) --------------
const NOON_UA = 'noon/22098 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const NOON_SET_LOCATION = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng';
const NOON_WHOAMI_FOOD = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1';
let noon = null;
function mergeCookies(jar, resp) {
  const list = typeof resp.headers.getSetCookie === 'function' ? resp.headers.getSetCookie() : [];
  for (const c of list) { const m = /^([^=]+)=([^;]+)/.exec(c); if (m) jar[m[1]] = m[2]; }
}
const cookieStr = (jar) => Object.entries(jar).map(([k, v]) => `${k}=${v}`).join('; ');
const noonBase = (s) => ({ 'User-Agent': NOON_UA, 'x-platform': 'ios', 'x-build': '22098', 'x-mp': 'noon', 'x-content': 'mobile',
  Accept: 'application/json', 'x-device-id': s.deviceId, 'x-visitor-id': s.visitorId });

async function noonLocate(s, lat, lng) {
  const loc = { 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng) };
  let r = await http(NOON_SET_LOCATION, { method: 'POST', headers: { ...noonBase(s), ...loc, 'Content-Type': 'application/json', Cookie: cookieStr(s.jar) },
    body: JSON.stringify({ location: { lat, lng }, lang: 'en' }) });
  if (r.status !== 200) throw new Error(`noon set-location HTTP ${r.status}`);
  mergeCookies(s.jar, r.resp);
  r = await http(NOON_WHOAMI_FOOD, { headers: { ...noonBase(s), ...loc, 'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(s.jar) } });
  if (r.status !== 200) throw new Error(`noon whoami-food HTTP ${r.status}`);
  mergeCookies(s.jar, r.resp);
  let who = {}; try { who = JSON.parse(r.text); } catch (_) {}
  s.zones = who && typeof who.headers === 'object' ? who.headers : {};
}

async function noonSession(lat, lng) {
  if (noon && Date.now() - noon.mintedAt < 240_000) { await noonLocate(noon, lat, lng); return noon; }
  const s = { deviceId: crypto.randomUUID(), visitorId: crypto.randomUUID(), jar: {}, zones: {}, mintedAt: Date.now() };
  const r = await http('https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami',
    { headers: { ...noonBase(s), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() } });
  if (r.status !== 200) throw new Error(`noon whoami HTTP ${r.status}`);
  mergeCookies(s.jar, r.resp);
  await noonLocate(s, lat, lng);
  if (!s.jar.nguestv2) throw new Error('noon mint produced no nguestv2');
  noon = s;
  return s;
}

async function noonOutlet(item) {
  const lat = Number(item.lat), lng = Number(item.lng);
  const s = await noonSession(lat, lng);
  const h = { ...noonBase(s), 'x-locale': 'en-ae', 'x-mp-country': 'ae', 'x-experience': 'food', 'x-device-lat': String(lat), 'x-device-lng': String(lng),
    'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)), 'x-whoami-request-id': crypto.randomUUID(),
    Cookie: cookieStr(s.jar), 'Content-Type': 'application/json' };
  for (const [k, v] of Object.entries(s.zones || {})) {
    if (v == null || typeof v !== 'string' || k.toLowerCase().includes('ab-test') || v.startsWith('{') || v.startsWith('[')) continue;
    h[k] = v;
  }
  const url = 'https://api-app-fd.noon.com/_svc/mp-food-api-mpnoon/consumer/restaurant/outlet/details/guest/partial';
  const body = { outletCode: item.branch_key, lat: Math.round(lat * 1e7), lng: Math.round(lng * 1e7) };
  const r = await http(url, { method: 'POST', headers: h, body: JSON.stringify(body) });
  const j = await save(item, 'outlet_details_partial', r, { method: 'POST', url, body, zone: s.zones && s.zones['x-food-zonecode'] });
  if (r.status === 404) return 'unavailable';
  if (r.status !== 200 || !j) throw new Error(`noon HTTP ${r.status}`);
  return 'done';
}

const FETCH = { talabat, careem, deliveroo, noon: noonOutlet };

// -- Main -------------------------------------------------------------------
let current = null, ending = false;
async function endMachine(reason) {
  if (ending) return; ending = true;
  try { await rpc('v2_detail_machine_end', { p_run: RUN_ID, p_machine: MACHINE, p_reason: reason }); } catch (e) { log(`machine_end failed: ${e.message}`); }
}
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => { await endMachine(`cancelled (${sig})`); process.exit(0); });

(async () => {
  const st = await rpc('v2_detail_machine_start', { p_run: RUN_ID, p_machine: MACHINE, p_gh: GH_RUN_ID });
  if (!st || st.stop) { log(`not starting: ${st && st.reason}`); return; }
  const jobs = Number(st.jobs) || 40, delay = Number(st.delay_ms) || 800;
  let exitReason = 'job quota reached', done = 0;
  for (let j = 0; j < jobs; j++) {
    if (Date.now() - START > WALL_LIMIT_MS) { exitReason = 'wall limit'; break; }
    const item = await rpc('v2_detail_claim', { p_run: RUN_ID, p_machine: MACHINE });
    if (!item || item.stop) { exitReason = (item && item.reason) || 'nothing to claim'; break; }
    current = item;
    let outcome, err = null;
    try { outcome = await FETCH[item.platform](item); }
    catch (e) { outcome = e instanceof Blocked ? 'blocked' : 'error'; err = String(e.message).slice(0, 400); }
    const rep = await rpc('v2_detail_report', { p_run: RUN_ID, p_machine: MACHINE, p_platform: item.platform, p_key: item.branch_key, p_outcome: outcome, p_error: err });
    current = null; done++;
    log(`${item.platform} ${item.branch_key}: ${outcome}${err ? ' | ' + err : ''}`);
    if (outcome === 'blocked' || (rep && rep.action === 'stop')) { exitReason = 'blocked (403/429)'; break; }
    if (j < jobs - 1) await sleep(delay);
  }
  await endMachine(exitReason);
  log(`=== ${done} restaurants, ${Math.round((Date.now() - START) / 1000)}s, exit "${exitReason}"`);
})().catch(async (e) => { console.error(`[m${MACHINE}] FATAL: ${e.message}`); await endMachine(`fatal: ${String(e.message).slice(0, 200)}`); process.exit(1); });
