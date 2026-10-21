#!/usr/bin/env node
// cross-platform/uae/v2-listing-scrape.js
//
// v2 phase 1: listing scrape. One GitHub Actions run = one machine that keeps
// claiming area jobs (one app at one fixed Talabat area pin) from Supabase until
// none are left or its time is up.
//   - every raw response is gzipped and parked in Supabase Storage (v2-staging),
//     with one row per file in v2.raw_file (archived to Iceberg later);
//   - the restaurant cards found are extracted here and saved compactly to v2.seen.
// Nothing raw is written to Postgres. Request logic is unchanged from the BB/JLT test.
//
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RUN_ID, MACHINE_NO, GITHUB_RUN_ID

'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const zlib = require('node:zlib');

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
const WALL_LIMIT_MS = 100 * 60 * 1000; // stop claiming new jobs after 100 min (job timeout is 120)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let JOB_NO = 0;
const log = (m) => console.log(`[${new Date().toISOString().slice(11, 19)}] [m${MACHINE} j${JOB_NO}] ${m}`);
const HTTP_TIMEOUT_MS = 60_000;
let BUCKET = 'v2-staging';

// -- Supabase ---------------------------------------------------------------

async function rpc(name, params) {
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
        signal: AbortSignal.timeout(120_000),
      });
      const text = await resp.text();
      if (!resp.ok) throw new Error(`rpc(${name}) ${resp.status}: ${text.slice(0, 300)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      if (attempt >= 3) throw e;
      log(`rpc(${name}) attempt ${attempt} failed: ${e.message} - retrying`);
      await sleep(3_000 * attempt);
    }
  }
}

// Upload one gzipped raw response to Storage (upsert).
async function putRaw(objectPath, text) {
  const gz = zlib.gzipSync(Buffer.from(text || '', 'utf8'));
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/storage/v1/object/${BUCKET}/${objectPath}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/gzip', 'x-upsert': 'true' },
        body: gz, signal: AbortSignal.timeout(180_000),
      });
      if (!resp.ok) throw new Error(`storage ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
      return gz.length;
    } catch (e) {
      if (attempt >= 4) throw e;
      log(`upload attempt ${attempt} failed: ${e.message} - retrying`);
      await sleep(4_000 * attempt);
    }
  }
}

let job = null, seq = 0, pagesSaved = 0;
const cards = new Map(); // listing_id -> card (first appearance wins the rank)
let ord = 0;
function addCard(c) {
  if (!c || !c.id) return;
  const id = String(c.id);
  const prev = cards.get(id);
  if (prev) { for (const k of ['page_url', 'name', 'lat', 'lng']) if (prev[k] == null && c[k] != null) prev[k] = c[k];
    prev.extra = { ...(c.extra || {}), ...(prev.extra || {}) }; return; }
  ord += 1;
  cards.set(id, { ...c, id, ord });
}

// Park one raw response in Storage + record it. Returns parsed JSON (or null).
async function save(kind, pageNo, status, request, text) {
  seq += 1;
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch (_) { json = null; }
  const objectPath = `listing/run${RUN_ID}/${job.platform}/a${job.area_id}-j${JOB_NO}/${String(seq).padStart(3, '0')}-${kind}.json.gz`;
  const gzBytes = await putRaw(objectPath, text);
  await rpc('v2_raw_record', {
    p_phase: 'listing', p_run: RUN_ID, p_job: JOB_NO, p_machine: MACHINE, p_platform: job.platform, p_listing_id: null,
    p_seq: seq, p_kind: kind, p_http: status, p_bytes: text ? Buffer.byteLength(text) : 0, p_gz: gzBytes, p_path: objectPath,
    p_request: { ...request, page: pageNo },
  });
  pagesSaved += 1;
  log(`saved #${seq} ${kind}${pageNo != null ? ' p' + pageNo : ''} http ${status} ${text ? Math.round(Buffer.byteLength(text) / 1024) : 0} KB`);
  return json;
}

async function http(url, opts = {}) {
  const t0 = Date.now();
  const resp = await fetch(url, { ...opts, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  const text = await resp.text();
  return { resp, status: resp.status, text, elapsed: Date.now() - t0 };
}

function walk(x, fn, d = 0) {
  if (!x || d > 40) return;
  if (Array.isArray(x)) { for (const v of x) walk(v, fn, d + 1); return; }
  if (typeof x !== 'object') return;
  if (fn(x) === 'stop') return;
  for (const k of Object.keys(x)) if (x[k] && typeof x[k] === 'object') walk(x[k], fn, d + 1);
}
const num = (v) => (v === null || v === undefined || v === '' || isNaN(Number(v)) ? null : Number(v));

// -- Talabat: composite-list, page size 1000, pages until empty ------------

const TALABAT_HEADERS = {
  'User-Agent': 'talabat/8701 CFNetwork/3896.100.1.2.1 Darwin/27.0.0', Accept: '*/*', 'Accept-Language': 'en-US',
  'Accept-Encoding': 'gzip, deflate, br', appbrand: '1', 'x-country': 'ae', 'x-app-version': '13.93.0',
  'x-device-version': '13.93.0', 'x-device-source': '4', 'x-device-framework': 'flutter', 'x-marshmallow-version': 'mm3',
  'http2-enabled': 'true', tokentypekey: 'jwt', 'Content-Length': '0',
};

async function talabat() {
  for (let page = 1; page <= 50; page++) {
    const url = `https://api.talabat.com/vendor-list/v1/composite-list/${job.lat}/${job.lng}` +
      `?countrycode=4&areaid=${job.app_area_id}&vertical_id=0&isCustomerPro=false&page=${page}&size=1000`;
    const r = await http(url, { headers: TALABAT_HEADERS });
    const j = await save('listing_page', page, r.status, { method: 'GET', url }, r.text);
    if (r.status !== 200) throw new Error(`talabat HTTP ${r.status}`);
    const list = j?.vendors || j?.data?.vendors || j?.result?.vendors || j?.restaurants || j?.data?.restaurants || [];
    for (const v of list) addCard({ id: v.bid, name: v.bna || v.na, lat: num(v.latitude), lng: num(v.longitude), extra: { rid: v.id, brand: v.na } });
    if (!list.length) break;
    await sleep(1500);
  }
}

// -- Careem: all-restaurants component, following Careem's next_page_url ----

function careemCards(node, out = [], d = 0) {
  if (!node || d > 20) return out;
  if (Array.isArray(node)) { for (const n of node) careemCards(n, out, d + 1); return out; }
  if (typeof node !== 'object') return out;
  if ((node.merchant_id || node.outlet_id || node.restaurant_id) && node.brand_id && (node.merchant_name || node.brand_name)) out.push(node);
  for (const k of Object.keys(node)) if (typeof node[k] === 'object') careemCards(node[k], out, d + 1);
  return out;
}

async function careem() {
  const tok = await rpc('v2_careem_token', {});
  if (!tok) throw new Error('no live Careem token in pool');
  let sub = '';
  try { sub = JSON.parse(Buffer.from(tok.split('.')[1].replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8')).sub || ''; } catch (_) {}
  let next = '/v1/page/food-discovery-home/component/v/1/food-discovery-all-restaurants-v2?sp_page=1&sp_offset=0&sp_category=food_disc_all_restaurants';
  for (let page = 1; page <= 400 && next; page++) {
    const sess = 'V2LS-' + crypto.randomBytes(4).toString('hex').toUpperCase();
    const url = 'https://appengine.careemapis.com' + next;
    const headers = {
      SESSION_ID: sess, 'X-Careem-Beta': 'false', 'User-Agent': 'ICMA/26.39.0', 'X-Careem-Agent': 'ICMA',
      'X-Careem-Session-Id': sess, Agent: 'ICMA', 'Time-Zone': 'Asia/Dubai', lng: String(job.lng), lat: String(job.lat),
      'x-careem-userid': String(sub), Version: '26.39.0', 'X-Careem-Version': '26.39.0', 'x-careem-user-location': `${job.lat},${job.lng}`,
      'x-careem-appengine-api-version': '2026-09-17', 'X-Careem-Operating-System': 'iOS/27.0.1', Authorization: 'Bearer ' + tok,
      'x-careem-permissions': 'location:granted', 'Accept-Language': 'en', 'x-careem-device-id': 'D0O8gpXoJdQ2L5lC', Accept: '*/*',
      'Accept-Encoding': 'gzip, deflate, br',
    };
    const r = await http(url, { headers, redirect: 'follow' });
    if (r.status === 204 || (r.status === 200 && !r.text)) { await save('listing_page', page, r.status, { method: 'GET', url }, r.text); break; }
    const j = await save('listing_page', page, r.status, { method: 'GET', url }, r.text);
    if (r.status !== 200) throw new Error(`careem HTTP ${r.status}`);
    // restaurant tiles: id "<merchant_id>-food-discovery-all-restaurants-v2"
    let found = 0;
    walk(j, (x) => {
      const m = typeof x.id === 'string' && /^(\d+)-food-discovery/.exec(x.id);
      if (!m) return;
      found += 1;
      const brand = x.favorite?.action?.metadata?.brandId || null;
      const title = (() => { try { return decodeURIComponent((/item_title=([^&]+)/.exec(x.action?.url || '') || [])[1] || '').replace(/\+/g, ' ') || null; } catch (_) { return null; } })();
      addCard({ id: m[1], name: title, extra: { brand_id: brand } });
      return 'stop';
    });
    if (!found) found = careemCards(j).length; // older shape fallback, keeps paging logic
    next = j && typeof j.next_page_url === 'string' && j.next_page_url ? j.next_page_url : null;
    if (!found) break;
    await sleep(1000);
  }
}

// -- Deliveroo: mobile FeedV2 at the pin + the nearest Deliveroo area web page --

const ROO_BODY = JSON.parse(fs.readFileSync(path.join(__dirname, 'deliveroo-feedv2-body.json'), 'utf8'));
const rooUuid = () => crypto.randomUUID().toUpperCase();
function rooPageUrl(href) {
  if (!href) return null;
  const p = String(href).split('?')[0];
  const m = /^\/menu\/([^/]+)\/([^/]+)\/([^/]+)$/.exec(p);
  return m ? `https://deliveroo.ae/en/menu/${m[1].toLowerCase()}/${m[2]}/${m[3]}` : null;
}

async function deliveroo() {
  const guid = rooUuid();
  const headers = {
    'User-Agent': 'Deliveroo-OrderApp/3.342.0 (iPhone18,2; iOS27.0.1; Release; en_US; 697347)', 'X-Roo-App-Version': '3.342.0',
    'X-Roo-Sticky-Guid': guid, 'X-Roo-Guid': guid, 'X-Roo-Country': 'ae', 'X-Roo-Platform': 'iOS', 'X-Roo-External-Device-Id': rooUuid(),
    'X-Roo-Rooblocks-Version': '5.3.0', 'apollographql-client-name': 'com.deliveroo.orderapp-apollo-ios',
    'apollographql-client-version': '3.342.0-697347', 'X-APOLLO-OPERATION-NAME': 'FeedV2', 'X-APOLLO-OPERATION-TYPE': 'query',
    Accept: 'multipart/mixed;deferSpec=20220824,application/json', 'Content-Type': 'application/json', 'Accept-Language': 'en-US',
  };
  const body = JSON.parse(JSON.stringify(ROO_BODY));
  if (body.variables?.location) { body.variables.location.lat = job.lat; body.variables.location.lon = job.lng; }
  if (body.variables) body.variables.uuid = rooUuid();
  const url = 'https://co-m.ae.deliveroo.com/consumer/graphql/';
  const r = await http(url, { method: 'POST', headers, body: JSON.stringify(body) });
  const j = await save('mobile_feedv2', 1, r.status, { method: 'POST', url, operation: 'FeedV2', lat: job.lat, lng: job.lng }, r.text);
  if (r.status !== 200) throw new Error(`deliveroo FeedV2 HTTP ${r.status}`);
  walk(j, (x) => { if (x.__typename === 'Restaurant' && x.drn_id) addCard({ id: x.drn_id, name: x.name, extra: { rid: x.id } }); });

  if (job.app_area_url) {
    await sleep(1500);
    const h = await http(job.app_area_url, { headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.5' }, redirect: 'follow' });
    const m = /<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/.exec(h.text || '');
    const nd = await save('html_listing_next_data', 1, h.status, { method: 'GET', url: job.app_area_url, note: m ? '__NEXT_DATA__ extracted' : 'raw html' },
      m ? m[1] : h.text);
    // page addresses: navigate_to_restaurant actions carry restaurant_id, partner_drn_id and restaurant_href
    walk(nd, (x) => {
      if (x.partner_drn_id && x.restaurant_href) addCard({ id: x.partner_drn_id, name: x.restaurant_name, page_url: rooPageUrl(x.restaurant_href), extra: { rid: x.restaurant_id, html: true } });
    });
  }
}

// -- Noon: 3-step guest mint, then search chained by searchToken -----------

const NOON_UA = 'noon/22098 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const NOON_BUILD = '22098';
function mergeCookies(jar, resp) {
  const list = typeof resp.headers.getSetCookie === 'function' ? resp.headers.getSetCookie() : [];
  for (const c of list) { const m = /^([^=]+)=([^;]+)/.exec(c); if (m) jar[m[1]] = m[2]; }
}
const cookieStr = (jar) => Object.entries(jar).map(([k, v]) => `${k}=${v}`).join('; ');

async function noon() {
  const { lat, lng } = job;
  const s = { deviceId: crypto.randomUUID(), visitorId: crypto.randomUUID(), jar: {} };
  const base = { 'User-Agent': NOON_UA, 'x-platform': 'ios', 'x-build': NOON_BUILD, 'x-mp': 'noon', 'x-content': 'mobile',
    Accept: 'application/json', 'x-device-id': s.deviceId, 'x-visitor-id': s.visitorId };
  const loc = { 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng) };

  let u = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami';
  let r = await http(u, { headers: { ...base, 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() } });
  mergeCookies(s.jar, r.resp);
  await save('noon_whoami', null, r.status, { method: 'GET', url: u }, r.text);
  if (r.status !== 200) throw new Error(`noon whoami HTTP ${r.status}`);

  u = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng';
  r = await http(u, { method: 'POST', headers: { ...base, ...loc, 'Content-Type': 'application/json', Cookie: cookieStr(s.jar) },
    body: JSON.stringify({ location: { lat, lng }, lang: 'en' }) });
  mergeCookies(s.jar, r.resp);
  await save('noon_set_location', null, r.status, { method: 'POST', url: u, lat, lng }, r.text);
  if (r.status !== 200) throw new Error(`noon set-location HTTP ${r.status}`);

  u = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1';
  r = await http(u, { headers: { ...base, ...loc, 'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(s.jar) } });
  mergeCookies(s.jar, r.resp);
  const who = await save('noon_whoami_food', null, r.status, { method: 'GET', url: u }, r.text);
  if (r.status !== 200) throw new Error(`noon whoami-food HTTP ${r.status}`);
  if (!s.jar.nguestv2) throw new Error('noon mint produced no nguestv2');
  const zones = who && typeof who.headers === 'object' ? who.headers : {};
  log(`noon zone ${zones['x-food-zonecode'] || '-'}`);

  const url = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';
  let token = null;
  const mintedAt = Date.now();
  for (let page = 1; page <= 500; page++) {
    if (Date.now() - mintedAt > 270_000) throw new Error('noon token near expiry before end of pages');
    const headers = { ...base, 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'x-mp-country': 'ae',
      'x-experience': 'food', 'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)), 'x-border-enabled': 'true',
      'x-rocket-enabled': 'true', 'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(s.jar), 'Content-Type': 'application/json' };
    for (const [k, v] of Object.entries(zones)) if (v != null && typeof v !== 'object' && !k.toLowerCase().includes('ab-test')) headers[k] = String(v);
    const body = { queryEntity: null, q: null, f: {}, contexts: [], withContent: true, excludeOutletCodes: [], limit: 60, page: 1,
      sort: { by: 'popularity', dir: 'desc' }, type: 'outlet', qType: 'search_query', getFallback: true };
    if (token) body.searchToken = token;
    r = await http(url, { method: 'POST', headers, body: JSON.stringify(body) });
    const j = await save('listing_page', page, r.status, { method: 'POST', url, page, has_search_token: !!token }, r.text);
    if (r.status === 404) break;
    if (r.status !== 200) throw new Error(`noon search HTTP ${r.status}`);
    let n = 0;
    (function w(x, d) {
      if (d > 8 || !x) return;
      if (Array.isArray(x)) { for (const v of x) w(v, d + 1); return; }
      if (typeof x === 'object') {
        if (typeof x.outletCode === 'string' && x.outletCode) {
          n += 1; addCard({ id: x.outletCode, name: x.name, extra: { rc: x.restaurantCode, mc: x.menuCode } }); return;
        }
        for (const k of Object.keys(x)) w(x[k], d + 1);
      }
    })(j?.results || [], 0);
    token = j?.searchToken || null;
    if (!n || !token || n < 60) break;
    await sleep(500);
  }
}

// -- Main: claim jobs until none are left ------------------------------------

const RUNNERS = { talabat, careem, deliveroo, noon };

async function flushCards() {
  const all = [...cards.values()];
  for (let i = 0; i < all.length; i += 500) await rpc('v2_listing_save_cards', { p_run: RUN_ID, p_job: JOB_NO, p_cards: all.slice(i, i + 500) });
  return all.length;
}

let ending = false;
async function endMachine(reason) {
  if (ending) return; ending = true;
  try { await rpc('v2_listing_machine_end', { p_run: RUN_ID, p_machine: MACHINE, p_reason: reason }); } catch (e) { log(`machine_end failed: ${e.message}`); }
}
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => { await endMachine(`cancelled (${sig})`); process.exit(0); });

(async () => {
  const st = await rpc('v2_listing_machine_start', { p_run: RUN_ID, p_machine: MACHINE, p_gh: GH_RUN_ID });
  if (!st || st.stop) { log(`not starting: ${st && st.reason}`); await endMachine('not started'); return; }
  if (st.staging_bucket) BUCKET = st.staging_bucket;
  let exitReason = 'wall limit', done = 0;
  while (Date.now() - START < WALL_LIMIT_MS) {
    const c = await rpc('v2_listing_claim', { p_run: RUN_ID, p_machine: MACHINE });
    if (!c || c.stop) { exitReason = (c && c.reason) || 'nothing to claim'; break; }
    job = c.job; JOB_NO = job.job_no; seq = 0; pagesSaved = 0; cards.clear(); ord = 0;
    log(`${job.platform} area ${job.area_id} ${job.app_area_name} @ ${job.lat},${job.lng}`);
    try {
      await RUNNERS[job.platform]();
      const n = await flushCards();
      await rpc('v2_listing_job_finish', { p_run: RUN_ID, p_job: JOB_NO, p_status: 'done', p_pages: pagesSaved, p_cards: n, p_error: null });
      log(`done: ${pagesSaved} responses, ${n} restaurants`);
    } catch (e) {
      log(`FAILED: ${e.message}`);
      try { await flushCards(); } catch (_) {}
      await rpc('v2_listing_job_finish', { p_run: RUN_ID, p_job: JOB_NO, p_status: 'failed', p_pages: pagesSaved, p_cards: cards.size,
        p_error: String(e.message).slice(0, 500) }).catch(() => {});
    }
    done += 1;
    await sleep(1000);
  }
  await endMachine(exitReason);
  log(`=== ${done} jobs, ${Math.round((Date.now() - START) / 1000)}s, exit "${exitReason}"`);
})().catch(async (e) => { console.error(`[m${MACHINE}] FATAL: ${e.message}`); await endMachine(`fatal: ${String(e.message).slice(0, 200)}`); process.exit(1); });
