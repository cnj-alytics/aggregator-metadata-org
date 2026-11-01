#!/usr/bin/env node
// noon-mint-lab/mint-lab.js — STANDALONE MINT EXPERIMENT (not production)
//
// One GitHub runner = one attempt. The machine claims a variant + area from
// Supabase (noon_mint_lab_claim), runs that variant's SETUP (how it obtains a
// guest session), then runs a few /search pages exactly like production, and
// reports every call back (noon_mint_lab_report).
//
// Variants live in noon_lab.mint_variant (spec JSONB). Setup routines:
//   v3_mint     — production 4-call mint (whoami → set-location → whoami-food
//                 → fd/whoami) with telemetry. Knobs:
//                   timing: "fast" (production) | "app" (iPhone spacing ~20s)
//                   etracker_init: true → GET etracker /init like the app
//                   filler_calls: true → 3 benign app calls between identity calls
//                   mint_retry: {max_attempts, backoff_ms, swap: device|visitor|both}
//                     → on 403 at a mint step, swap ids, pause, retry that step
//   whoami_only — 1 identity call (st whoami → nguestv2), zone headers from cache
//   none        — no identity calls, no cookies; zone_headers: "cache" | "none"
//   donor       — no identity calls; cookies minted by another lab machine
//
// Network helpers (curl-impersonate, headers, etracker events) are copied
// verbatim from the production scraper so the lab measures the same stack.
//
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TEST_ID, MACHINE_NO, GITHUB_RUN_ID

'use strict';

const { execFileSync } = require('node:child_process');
const crypto = require('node:crypto');
const fs = require('node:fs');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TEST_ID = Number(process.env.TEST_ID);
const MACHINE = Number(process.env.MACHINE_NO);
const GH_RUN_ID = process.env.GITHUB_RUN_ID || '';
if (!SUPABASE_URL || !SUPABASE_KEY || !TEST_ID || !MACHINE) {
  console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / TEST_ID / MACHINE_NO');
  process.exit(1);
}
const prefix = `[mintlab t${TEST_ID} m${MACHINE}]`;
const say = (m) => console.log(`${prefix} ${m}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rnd = (lo, hi) => lo + Math.random() * (hi - lo);

// ---- BEGIN helpers copied verbatim from noon/uae/ranking-scrape.js (production; not modified) ----
const CI_VERSION = 'v0.6.1';
const CI_URL = `https://github.com/lwthiker/curl-impersonate/releases/download/${CI_VERSION}/curl-impersonate-${CI_VERSION}.x86_64-linux-gnu.tar.gz`;
const CI_DIR = '/tmp/curl-impersonate';
const WRAPPER = process.env.IMPERSONATE_WRAPPER || 'curl_safari15_3';

function ensureCurl() {
  for (const c of [WRAPPER, `/opt/ci/${WRAPPER}`, `${CI_DIR}/${WRAPPER}`]) {
    try { execFileSync(c, ['--version'], { stdio: 'ignore', timeout: 10000 }); return c; } catch (_) {}
  }
  fs.mkdirSync(CI_DIR, { recursive: true });
  execFileSync('curl', ['-sSL', '--fail', '--max-time', '120', '-o', `${CI_DIR}/ci.tar.gz`, CI_URL], { timeout: 130000 });
  execFileSync('tar', ['-xzf', `${CI_DIR}/ci.tar.gz`, '-C', CI_DIR], { timeout: 60000 });
  const p = `${CI_DIR}/${WRAPPER}`;
  execFileSync(p, ['--version'], { stdio: 'ignore', timeout: 10000 });
  return p;
}

let CURL = null; // resolved at start
let NOON_REQ = 0;

// Per-area visibility: every Noon call is recorded here and saved to
// noon_scrape_audit.req_log (plus counters) when the area ends.
let AREA_LOG = null;
const REQ_LOG_CAP = 600;
function newAreaLog() {
  return { t0: Date.now(), log: [], rotations: 0, recoveries: 0, recoveries_ok: 0, remints: 0,
    search_403: 0, search_429: 0, et_posts: 0, et_events: 0, et_non2xx: 0 };
}
function logReq(entry) {
  if (!AREA_LOG) return;
  if (AREA_LOG.log.length < REQ_LOG_CAP) AREA_LOG.log.push({ t: Date.now() - AREA_LOG.t0, ...entry });
}

// curl-impersonate call -> {step, status, ms, headers, setCookies, body}. (v3 verbatim)
function curlCall(step, url, { method = 'GET', headers = {}, body = null, cookieJar = null } = {}) {
  const args = ['-sS', '-i', '--max-time', '20', '-X', method];
  for (const [k, v] of Object.entries(headers)) args.push('-H', `${k}: ${v}`);
  if (cookieJar) {
    const cookieStr = Object.entries(cookieJar).map(([k, v]) => `${k}=${v}`).join('; ');
    if (cookieStr) args.push('-H', `Cookie: ${cookieStr}`);
  }
  if (body != null) args.push('--data-raw', body);
  args.push('--', url);

  if (url.includes('noon.com')) NOON_REQ++;
  const t0 = Date.now();
  let raw;
  try {
    raw = execFileSync(CURL, args, { encoding: 'utf-8', timeout: 22000, maxBuffer: 20 * 1024 * 1024 });
  } catch (e) {
    const err = `exit=${e.status ?? '?'} ${String(e.stderr || e.message || e).replace(/\s+/g, ' ').trim()}`.slice(0, 300);
    logReq({ step, st: null, ms: Date.now() - t0, err: err.slice(0, 200) });
    return { step, status: null, ms: Date.now() - t0, error: err };
  }
  const ms = Date.now() - t0;

  const sep = raw.lastIndexOf('\r\n\r\n');
  const alt = raw.lastIndexOf('\n\n');
  const splitAt = sep >= 0 ? sep + 4 : alt >= 0 ? alt + 2 : -1;
  if (splitAt < 0) return { step, status: null, ms, error: 'no header separator' };

  const headerText = raw.slice(0, splitAt);
  const bodyText = raw.slice(splitAt);
  const blocks = headerText.split(/\r?\n\r?\n/).filter(Boolean);
  const lines = blocks[blocks.length - 1].split(/\r?\n/);
  const status = parseInt(((lines[0] || '').split(' ')[1] || '0'), 10);

  const headersMap = new Map();
  const setCookies = [];
  for (const ln of lines.slice(1)) {
    const ci = ln.indexOf(': ');
    if (ci < 0) continue;
    const k = ln.slice(0, ci).toLowerCase(), v = ln.slice(ci + 2);
    if (k === 'set-cookie') setCookies.push(v); else headersMap.set(k, v);
  }
  if (cookieJar) {
    for (const c of setCookies) {
      const m = /^([^=]+)=([^;]+)/.exec(c);
      if (m) cookieJar[m[1]] = m[2];
    }
  }
  logReq({ step, st: status, ms, ck: setCookies.length || undefined });
  return { step, status, ms, headers: headersMap, setCookies, body: bodyText };
}

// One retry on 403/429/503/null with 2–3s backoff (v3 verbatim).
async function curlCallWithRetry(step, url, opts, { retries = 1, backoffMs = 2000 } = {}) {
  let r = curlCall(step, url, opts);
  r.step = step;
  let attempt = 1;
  while (attempt <= retries && (r.status === 403 || r.status === 429 || r.status === 503 || r.status === null)) {
    await sleep(backoffMs + Math.random() * 1000);
    attempt++;
    const nextStep = `${step}_retry${attempt - 1}`;
    r = curlCall(nextStep, url, opts);
    r.step = nextStep;
  }
  return r;
}

function egressIp() {
  try {
    return execFileSync(CURL, ['-sS', '--max-time', '8', 'https://api.ipify.org'], { encoding: 'utf-8', timeout: 10000 }).trim();
  } catch (_) { return null; }
}

// -- Noon constants (v3) ------------------------------------------------------

const UA = 'noon/22895 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const BUILD = '22895';
const WHOAMI_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami';
const WHOAMI_FOOD_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1';
const SET_LOCATION_URL = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng';
const FD_WHOAMI_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/customer/whoami';
const SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';
const ETRACKER_URL = 'https://etracker.noon.com/track/v2/';

// -- etracker events (v3 verbatim — schemas from the iPhone Proxyman trace) ---

function deviceBlock(idfv) {
  return { dm: 'Apple iPhone 17 Pro Max', dos: 'ios 27.0.1', d: 'Handset', sr: '1320x2868', vps: '1320x2868', scl: 3,
    idfa: '00000000-0000-0000-0000-000000000000', idfv };
}
function evt(ev, em, visitorId, idfv) {
  return { em, ev, ts: Date.now(), pc: 'noon', vid: visitorId, lo: 'en-ae', pf: 'ios', d: deviceBlock(idfv) };
}
const NOON_APP = { mc: 'noon', an: 'noon', av: '5.261024', ab: '22895', mpe: 'noon' };
const FOOD_APP = { mc: 'food', an: 'noon', av: '5.261024', ab: '22895', mpe: 'food' };

function evLaunch(v, i) {
  return evt('launch', { fsv: '1.00', isNA: false, nps: 'denied',
    perf: { NTJS: { d: 700 }, GWA: { d: 250 }, SR: { d: 280, m: { fsmps: 10 } }, TJS: { d: 15500 }, TAS: { d: 16200 } }, ...NOON_APP }, v, i);
}
function evLaunchInfo(v, i) {
  const now = Date.now();
  return evt('launch_info', { nsa: now - 10000, jsa: now - 9400, ds: 0.6, s: 'app_launch', dl: 'en', ...NOON_APP }, v, i);
}
function evAppForeground(v, i) { return evt('app_foreground', { ...FOOD_APP }, v, i); }
function evLocationFetch(v, i) { return evt('location_fetch', { GPS_allow: true, ...NOON_APP }, v, i); }
function evPageHome(v, i) { return evt('page_home', { curl: '/', ...NOON_APP }, v, i); }
function evSelectedZone(zh, v, i) {
  return evt('selected_zone', {
    szn: String(zh['x-ecom-zonecode'] || 'AE_DXB-S1'),
    szf: String(zh['x-food-zonecode'] || 'FOOD-AE-DXB-HUB4-DUBAI_MARINA'),
    szi: String(zh['x-nooninstant-zonecode'] || ''),
    szr: String(zh['x-rocket-zonecode'] || ''),
    sznow: String(zh['x-noonnownow-zonecode'] || ''),
    szs: String(zh['x-services-zonecode'] || 'SERVICES-AE-DUBAI'),
    szmpr: String(zh['x-rocket-mp-zonecode'] || ''),
    szg: String(zh['x-ecom-zonecode'] || 'AE_DXB-S1'),
    address_codes: [{ mp: 'ecom' }, { mp: 'rocket' }, { mp: 'food' }, { mp: 'nooninstant' }, { mp: 'services' }, { mp: 'noonnownow' }, { mp: 'ping' }, { mp: 'out' }, { mp: 'aster' }, { mp: 'money' }],
    ...FOOD_APP,
  }, v, i);
}
function evWhoamiAbConfig(zh, v, i) {
  let abIds = [];
  const raw = zh['x-ab-test'];
  if (typeof raw === 'string') abIds = raw.split(',').map((s) => parseInt(s.trim(), 10)).filter((n) => !isNaN(n));
  else if (Array.isArray(raw)) abIds = raw.filter((n) => typeof n === 'number');
  return evt('whoami_ab_config', { abIds: abIds.length ? abIds : [1052, 1471, 1531, 1731, 1750], ...NOON_APP }, v, i);
}
function evPageCatalog(pageNum, v, i) {
  return evt('page_catalog', { hid: crypto.randomUUID(), nbh: 60, pn: pageNum, sb: 'browse', ...FOOD_APP }, v, i);
}
function evOutletImpression(o, idx, v, i) {
  return evt('outlet_impression', { oc: o.outletCode, wid: `search_outlet/${idx}`, aid: o.outletCode, ix: `0/${idx}`, otg: false,
    df: o.distance || 5.0, fav: false, isp: true, sla: o.sla || '30-40', wn: 'search_results', wtc: 'search_results_tab',
    curl: '/search?type=outlet', src: 'search/results', pn: 'Search_Results', ...FOOD_APP }, v, i);
}
function evOutletImpressionV2(o, idx, v, i) {
  return evt('outlet_impression_v2', { pg: 'SR', wt: `SR/search_results/0/search_results_tab/${idx}/ROW/${idx}/ITM/0`,
    ul: '/search?type=outlet', cf: '{}', isp: true, oc: o.outletCode, df: o.distance || 5.0, sla: o.sla || '30-40', ...FOOD_APP }, v, i);
}
function evPromotionImpression(o, idx, v, i) {
  return evt('promotion_impression', { aid: o.outletCode, wid: `search_results/${idx}`,
    aidMap: { [o.outletCode]: `search_results/${idx}` }, ismMap: { [o.outletCode]: true },
    curl: '/search?type=outlet', toUrl: `/outlet/${o.outletCode}`, ed: { eIds: [], reqId: crypto.randomUUID() },
    cmsEids: {}, wp: idx, ar: 100, adType: 'display', isAd: false, ...FOOD_APP }, v, i);
}
function impressionTripletsBatch(outlets, startIdx, v, i) {
  const events = [];
  outlets.forEach((o, k) => {
    const idx = startIdx + k;
    events.push(evOutletImpression(o, idx, v, i), evOutletImpressionV2(o, idx, v, i), evPromotionImpression(o, idx, v, i));
  });
  return events;
}
// ---- END helpers ----

// -- Lab-specific ---------------------------------------------------------------

const FILLER_CALLS = [
  ['filler_membership', 'https://api-app.noon.com/_svc/mp-vip-api-v1/membership?primaryUphoneCode=none'],
  ['filler_splash', 'https://api-app-instant.noon.com/_svc/catalog/splash-screen-config'],
  ['filler_configs', 'https://api-app.noon.com/_svc/configs-v1/configs'],
];
const ETRACKER_INIT_URL = 'https://etracker.noon.com/init';
const IDENTITY_RE = /whoami|set-location/;

async function rpc(name, params) {
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      });
      const text = await resp.text();
      if (!resp.ok) throw new Error(`rpc(${name}) ${resp.status}: ${text.slice(0, 300)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      if (attempt >= 3) throw e;
      await sleep(2000 * attempt);
    }
  }
}

(async () => {
  CURL = ensureCurl();
  AREA_LOG = newAreaLog();          // reuse production's per-call log
  const ip = egressIp();
  const claim = await rpc('noon_mint_lab_claim', { p_test_id: TEST_ID, p_machine_no: MACHINE, p_github_run_id: GH_RUN_ID, p_runner_ip: ip });
  if (!claim || claim.stop) { say(`nothing to do: ${claim && claim.reason}`); return; }

  const spec = claim.spec || {};
  const lat = Number(claim.lat), lng = Number(claim.lng);
  const SEARCH_PAGES = Number(claim.search_pages) || 3;
  say(`variant=${claim.variant_id} area=${claim.area_name} (${lat},${lng}) ip=${ip} spec=${JSON.stringify(spec)}`);

  // Stagger start so the lab never bursts its own identity calls.
  const jitter = Math.floor(Math.random() * (Number(claim.start_jitter_ms) || 0));
  if (jitter) { say(`start jitter ${jitter}ms`); await sleep(jitter); }
  AREA_LOG = newAreaLog();

  let deviceId = crypto.randomUUID().toUpperCase();
  let visitorId = crypto.randomUUID().toLowerCase();
  const deviceIdfv = deviceId;
  const jar = {};
  let whoHeaders = {};
  let identityCalls = 0, identity403 = 0, swaps = 0;
  const shortId = (id) => String(id).slice(0, 8);

  const sentryTrace = () => {
    const trace = crypto.randomBytes(16).toString('hex');
    const span = crypto.randomBytes(8).toString('hex');
    return { 'sentry-trace': `${trace}-${span}`,
      baggage: `sentry-environment=production,sentry-public_key=d61e4de762b3c36e7c14f2d7cf6030c1,sentry-trace_id=${trace}` };
  };
  const base = () => ({
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD, 'x-mp': 'noon', 'x-content': 'mobile',
    Accept: 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br', 'Cache-Control': 'no-cache', Connection: 'keep-alive',
    'x-device-id': deviceId, 'x-visitor-id': visitorId, ...sentryTrace(),
  });
  const et = (step, events) => {
    if (spec.telemetry === false) return null;
    return curlCall(step, ETRACKER_URL, { method: 'POST', headers: { ...base(), 'Content-Type': 'text/plain;charset=UTF-8' },
      body: JSON.stringify(events), cookieJar: jar });
  };
  const etInit = (step) => {
    if (!spec.etracker_init) return;
    const r = curlCall(step, ETRACKER_INIT_URL, { headers: { 'User-Agent': UA, Accept: '*/*', 'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br', Connection: 'keep-alive', ...sentryTrace() } });
    say(`  ${step}: ${r.status}`);
  };
  const filler = () => {
    if (!spec.filler_calls) return;
    for (const [step, url] of FILLER_CALLS) {
      const r = curlCall(step, url, { headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae' }, cookieJar: jar });
      say(`  ${step}: ${r.status}`);
    }
  };
  const buildFdHeaders = () => {
    const h = { ...base(), 'x-experience': 'food', 'x-locale': 'en-ae',
      'x-device-lat': String(lat), 'x-device-lng': String(lng), 'x-mp-country': 'ae',
      'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)),
      'x-border-enabled': 'true', 'x-rocket-enabled': 'true' };
    for (const [k, v] of Object.entries(whoHeaders)) if (v != null && typeof v !== 'object') h[k] = String(v);
    return h;
  };
  const mergeWho = (body) => {
    try { const j = JSON.parse(body); if (j && j.headers && typeof j.headers === 'object')
      for (const [k, v] of Object.entries(j.headers)) if (v != null && typeof v !== 'object') whoHeaders[k] = v; } catch (_) {}
  };
  const doSwap = (mode) => {
    if (mode === 'device' || mode === 'both') deviceId = crypto.randomUUID().toUpperCase();
    if (mode === 'visitor' || mode === 'both') visitorId = crypto.randomUUID().toLowerCase();
    swaps++;
  };

  // One identity call with production's 1 plain retry, plus optional swap-and-retry (mint_retry).
  const identity = async (step, url, optsFn) => {
    const mr = spec.mint_retry;
    const maxA = mr ? Number(mr.max_attempts) || 0 : 0;
    for (let a = 0; ; a++) {
      const r = await curlCallWithRetry(a === 0 ? step : `${step}_swap${a}`, url, optsFn());
      say(`  ${r.step}: ${r.status} ${r.ms}ms dev=${shortId(deviceId)}`);
      if (r.status === 200) return r;
      if (!mr || a >= maxA || (r.status !== 403 && r.status !== 429)) return r;
      doSwap(mr.swap || 'both');
      await sleep((Number(mr.backoff_ms) || 6000) + Math.random() * 1000);
    }
  };

  const result = { steps: null, identity_calls: 0, identity_403: 0, swaps: 0, got_token: false,
    search_ok: 0, search_tried: 0, search_403: 0, search_other: 0, page1_outlets: 0, page1_codes: [] };
  const setupStart = Date.now();
  let setupFail = null;

  try {
    // ------------------------------- SETUP -----------------------------------
    const app = spec.timing === 'app';
    if (spec.setup === 'v3_mint') {
      etInit('et_init_1');
      let r = await identity('mint_whoami', WHOAMI_URL, () => ({
        headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() }, cookieJar: jar }));
      if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: 'mint_whoami', status: r.status });
      await sleep(app ? rnd(1800, 2200) : rnd(300, 600));
      et('et_mint_launch', [evLaunch(visitorId, deviceIdfv), evLaunchInfo(visitorId, deviceIdfv), evAppForeground(visitorId, deviceIdfv)]);
      await sleep(app ? rnd(3700, 5000) : rnd(1200, 2000));

      r = await identity('mint_set_location', SET_LOCATION_URL, () => ({ method: 'POST',
        headers: { ...base(), 'x-experience': 'ecom', ...(app ? {} : { 'x-locale': 'en-ae' }), 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'Content-Type': 'application/json' },
        body: JSON.stringify({ location: { lat, lng }, lang: 'en' }), cookieJar: jar }));
      if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: 'mint_set_location', status: r.status });
      await sleep(app ? rnd(200, 400) : rnd(200, 500));
      et('et_mint_location', [evLocationFetch(visitorId, deviceIdfv), evPageHome(visitorId, deviceIdfv)]);
      if (app) etInit('et_init_2');
      await sleep(app ? rnd(300, 500) : rnd(400, 700));

      r = await identity('mint_whoami_food', WHOAMI_FOOD_URL, () => ({
        headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'x-whoami-request-id': crypto.randomUUID() },
        cookieJar: jar }));
      if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: 'mint_whoami_food', status: r.status });
      try { const w = JSON.parse(r.body); whoHeaders = (w && w.headers && typeof w.headers === 'object') ? { ...w.headers } : {}; } catch (_) {}
      await sleep(rnd(150, 400));
      et('et_mint_zone', [evWhoamiAbConfig(whoHeaders, visitorId, deviceIdfv), evSelectedZone(whoHeaders, visitorId, deviceIdfv)]);

      if (app) { await sleep(rnd(200, 600)); filler(); await sleep(rnd(8000, 12000)); }
      else { filler(); await sleep(rnd(800, 2000)); }

      r = await identity('mint_fd_whoami', FD_WHOAMI_URL, () => ({ headers: buildFdHeaders(), cookieJar: jar }));
      if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: 'mint_fd_whoami', status: r.status });
      mergeWho(r.body);
      result.zone_headers = Object.fromEntries(Object.entries(whoHeaders).filter(([, v]) => v != null && typeof v !== 'object'));
    } else if (spec.setup === 'whoami_only') {
      const r = await identity('mint_whoami', WHOAMI_URL, () => ({
        headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() }, cookieJar: jar }));
      if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: 'mint_whoami', status: r.status });
      await sleep(rnd(300, 600));
      et('et_mint_launch', [evLaunch(visitorId, deviceIdfv), evLaunchInfo(visitorId, deviceIdfv), evAppForeground(visitorId, deviceIdfv)]);
      whoHeaders = { ...(claim.zone_headers || {}) };
    } else if (spec.setup === 'none') {
      whoHeaders = spec.zone_headers === 'cache' ? { ...(claim.zone_headers || {}) } : {};
      et('et_mint_launch', [evLaunch(visitorId, deviceIdfv), evLaunchInfo(visitorId, deviceIdfv), evAppForeground(visitorId, deviceIdfv)]);
    } else if (spec.setup === 'donor') {
      if (!claim.donor_cookies) throw Object.assign(new Error('no donor token available'), { step: 'donor', status: null });
      Object.assign(jar, claim.donor_cookies);
      whoHeaders = { ...(claim.zone_headers || {}) };
      et('et_mint_launch', [evLaunch(visitorId, deviceIdfv), evLaunchInfo(visitorId, deviceIdfv), evAppForeground(visitorId, deviceIdfv)]);
    } else {
      throw Object.assign(new Error(`unknown setup ${spec.setup}`), { step: 'config', status: null });
    }
    result.got_token = !!jar.nguestv2;
    if (spec.setup === 'v3_mint' && jar.nguestv2) result.donor_cookies = { ...jar };
    result.setup_outcome = 'ok';
    say(`SETUP ok in ${Date.now() - setupStart}ms | token=${result.got_token} zone_headers=${Object.keys(whoHeaders).length}`);
  } catch (e) {
    setupFail = e;
    result.setup_outcome = e.status === 403 || e.status === 429 ? 'blocked' : 'error';
    result.setup_fail_step = e.step || null;
    result.setup_http = e.status || null;
    say(`SETUP FAILED at ${e.step}: ${e.message}`);
  }
  result.setup_ms = Date.now() - setupStart;

  // ------------------------------- SEARCH ------------------------------------
  let searchFail = null;
  if (!setupFail) {
    let searchToken = null;
    for (let p = 1; p <= SEARCH_PAGES; p++) {
      if (p > 1) await sleep(rnd(0, 3750));     // production pacing (1.25–5s incl. telemetry)
      const body = {
        queryEntity: null, q: null, f: {}, contexts: [], withContent: true,
        excludeOutletCodes: [], limit: 60, scopes: null, page: 1, dishTags: [],
        ratingMin: null, sort: { by: 'popularity', dir: 'desc', slab: null },
        widgets: null, content: null, withHits: false, filteredByWidget: null,
        internalFilteredByWidget: 'homepage_navpill_filters_widget',
        getFallback: true, type: 'outlet', qType: 'search_query',
        minCount: 0, groupDishes: false, isSsc: false,
        hideSearchBar: false, hideSearchOptions: false,
        isOutletAdProcessingEnabled: true, withDishFetchQuery: null,
      };
      if (searchToken) body.searchToken = searchToken;
      let sr;
      for (let a = 0; a <= 12; a++) {
        sr = curlCall(a === 0 ? `search_p${p}` : `search_p${p}_rec${a}`, SEARCH_URL, {
          method: 'POST', headers: { ...buildFdHeaders(), 'Content-Type': 'application/json' }, body: JSON.stringify(body), cookieJar: jar });
        result.search_tried++;
        if (sr.status === 403 || sr.status === 429) result.search_403++;
        else if (sr.status !== 200) result.search_other++;
        say(`  search p${p}${a ? ` rec${a}` : ''}: ${sr.status} ${sr.ms}ms dev=${shortId(deviceId)}`);
        if (sr.status === 200) { result.search_ok++; break; }
        if (sr.status !== 403 && sr.status !== 429) break;
        doSwap('device');
        await sleep(6000 + Math.random() * 1000);
      }
      if (sr.status !== 200) { searchFail = { step: `search_p${p}`, status: sr.status }; break; }
      let json = {};
      try { json = JSON.parse(sr.body); } catch (_) {}
      const outlets = [];
      (function walk(x, d) {
        if (d > 8 || !x) return;
        if (Array.isArray(x)) { for (const v of x) walk(v, d + 1); return; }
        if (typeof x === 'object') {
          if (typeof x.outletCode === 'string' && x.outletCode) { outlets.push(x); return; }
          for (const k of Object.keys(x)) walk(x[k], d + 1);
        }
      })(json.results || [], 0);
      if (p === 1) { result.page1_outlets = outlets.length; result.page1_codes = outlets.slice(0, 15).map((o) => o.outletCode); }
      say(`  page ${p}: ${outlets.length} outlets${json.searchToken ? '' : ' (no next token)'}`);
      if (!outlets.length || !json.searchToken) break;
      searchToken = json.searchToken;
      await sleep(rnd(150, 350));
      et('et_page_catalog', [evPageCatalog(p, visitorId, deviceIdfv)]);
      await sleep(rnd(500, 1400));
      et('et_impressions', impressionTripletsBatch(outlets.slice(0, 6), 0, visitorId, deviceIdfv));
    }
  }

  const idLog = AREA_LOG.log.filter((e) => /^mint_/.test(e.step));
  identityCalls = idLog.length;
  identity403 = idLog.filter((e) => e.st === 403 || e.st === 429).length;
  result.identity_calls = identityCalls;
  result.identity_403 = identity403;
  result.swaps = swaps;
  result.outcome = setupFail ? 'setup_failed' : searchFail ? 'search_failed' : (result.page1_outlets > 0 ? 'ok' : 'empty_results');
  if (setupFail) result.error = String(setupFail.message).slice(0, 300);
  else if (searchFail) result.error = `${searchFail.step} ${searchFail.status}`;
  result.steps = AREA_LOG.log;
  await rpc('noon_mint_lab_report', { p_attempt_id: claim.attempt_id, p_result: result });
  say(`=== DONE outcome=${result.outcome} setup=${result.setup_outcome} identity_calls=${identityCalls} identity_403=${identity403} searches ok ${result.search_ok}/${result.search_tried} page1=${result.page1_outlets}`);
})().catch((e) => {
  console.error(`${prefix} FATAL: ${e.message}`);
  process.exit(1);
});
