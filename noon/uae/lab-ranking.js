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
//   * ONE area per machine, then exit.
//   * ONE token per area: whoami -> set-location -> whoami-food, then locate
//     (set-location + whoami-food) — the same 5 setup calls as production.
//   * Walk /search page by page with the test's page gap (page_delay_ms) until a page is
//     empty, adds nothing new, or has no next-page token (cap 150 pages).
//   * 403 or 429 or any other error -> stop immediately and report. Supabase decides
//     whether the area is tried again on a fresh machine.
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

const UA    = 'noon/22098 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const BUILD = '22098';
const WHOAMI_URL       = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami';
const SET_LOCATION_URL = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng';
const WHOAMI_FOOD_URL  = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1';
const SEARCH_URL       = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';
const PAGE_LIMIT = 60;
const PAGE_CAP   = 150;
const TIME_CAP_MS = 9 * 60 * 1000;
const FLOWS = {
  A: { locate: true,  launch: null,         loc: null,         list: null,          gap: [500, 500] },
  B: { locate: false, launch: null,         loc: null,         list: null,          gap: [500, 500] },
  C: { locate: false, launch: [2000, 4000], loc: [1000, 2000], list: [4000, 8000],  gap: [500, 500] },
  D: { locate: false, launch: [2000, 4000], loc: [1000, 2000], list: [4000, 8000],  gap: [2000, 6000] },
  E: { locate: false, launch: [2000, 4000], loc: [1000, 2000], list: [10000, 14000], gap: [4000, 10000] },
};
const between = (r) => (r ? r[0] + Math.floor(Math.random() * (r[1] - r[0] + 1)) : 0);
let requests = 0;

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
const reqLog = [];
let blockInfo = null;
let searchPagesLogged = 0;

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
  let r; const t0 = Date.now(); requests++;
  try { r = await fetch(url, { ...opts, signal: AbortSignal.timeout(30000) }); }
  catch (e) { throw Object.assign(fail(step, null), { message: `${step} network: ${e.message}` }); }
  try { logRequest(step, url, opts, r, Date.now() - t0); } catch (_) {}
  if (r.status === 403 || r.status === 429) { await captureBlock(step, r); throw fail(step, r.status); }
  return r;
}

let session = null;

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
  let ip = null;
  try { ip = (await (await fetch('https://api.ipify.org', { signal: AbortSignal.timeout(4000) })).text()).trim(); } catch (_) {}

  const claim = await rpc('noon_lab_claim', { p_test_id: TEST_ID, p_machine_no: MACHINE, p_github_run_id: GH_RUN_ID, p_runner_ip: ip });
  if (!claim || claim.stop) { log(`nothing to do: ${claim && claim.reason}`); return; }

  const { item_id: itemId, lat, lng, page_delay_ms: delay } = claim;
  const mode = claim.identity_mode || 'fresh-lower';
  const flowName = FLOWS[claim.flow] ? claim.flow : 'A';
  const flow = FLOWS[flowName];
  const ids = pickIds(mode, claim.identity);
  log(`ip=${ip} area=${claim.noon_area_id} (${claim.area_name}, usually ${claim.usual_pages}p) delay=${delay}ms attempt=${claim.attempt}`);
  log(`flow ${flowName} ${JSON.stringify(flow)}`);
  log(`identity ${mode}${claim.identity ? ` #${claim.identity.identity_id} (used ${claim.identity.prior_uses}x, blocked ${claim.identity.prior_blocked}x)` : ''} device=${ids.deviceId}`);

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
      error: String(e.message).slice(0, 300),
    });
  }
  Object.assign(result, {
    cards: seen.size, setup_ms: setupMs, elapsed_ms: Date.now() - t0, page_ms: pageMs,
    token_age_ms: session ? Date.now() - session.mintedAt : null,
    device_id: ids.deviceId, cookies: reqLog, block_info: blockInfo, flow: flowName, requests,
  });
  log(`${result.outcome} ${result.fail_step || ''} pages=${result.pages_done} cards=${result.cards} ${result.elapsed_ms}ms`);
  await rpc('noon_lab_report', { p_test_id: TEST_ID, p_machine_no: MACHINE, p_item_id: itemId, p_result: result });
})().catch(async (e) => {
  console.error(`[lab t${TEST_ID} m${MACHINE}] fatal: ${e.message}`);
  try { await rpc('noon_lab_report', { p_test_id: TEST_ID, p_machine_no: MACHINE, p_item_id: null, p_result: { outcome: 'error', error: e.message } }); } catch (_) {}
  process.exit(1);
});
