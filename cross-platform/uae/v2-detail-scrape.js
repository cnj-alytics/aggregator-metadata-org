#!/usr/bin/env node
// cross-platform/uae/v2-detail-scrape.js
//
// v2 phase 3: restaurant page scrape. One GitHub Actions run = one machine.
// Supabase (v2_detail_* functions) is the controller: it hands out one restaurant
// at a time from v2.detail_queue (unique app listings only); this machine fetches
// that restaurant's FULL raw page (viewed from the area pin where it was listed),
// parks it gzipped in Supabase Storage (v2-staging, recorded in v2.raw_file) and
// saves only the extracted fields + menu item names to v2.detail_extract.
// After jobs_per_machine restaurants it exits and Supabase starts a fresh machine.
// 429/403 = as the v1 ranking scrape does: this machine stops at once, the restaurant goes back in the
// queue, and Supabase starts a fresh machine (new runner, new IP) in its place.
// Pace between restaurants comes from Supabase (delay_ms on every claim) with +-30% jitter.
//
// Endpoints (full responses, nothing trimmed):
//   talabat   GET  api.talabat.com/menubff/v4/branches/{bid}/menu
//   careem    GET  apigateway.careemdash.com/v1/restaurants/{merchant_id}   (v2's own token pool)
//   deliveroo POST co-m.ae.deliveroo.com/consumer/graphql/ (app: is it listed at its location?) then, if live,
//             GET  {menu page from the app}?geohash={point}  -> __NEXT_DATA__ (menuPage + query saved)
//   noon      POST outlet/details/guest/partial (guest token minted on this machine)
//
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RUN_ID, MACHINE_NO, GITHUB_RUN_ID

'use strict';

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

// http() with the v1 back-off: on 429/403 wait 60 s, 120 s, 180 s, 240 s on this machine and try again.
// Gives up (throws Blocked) only after the last wait, or when the machine's time is nearly used.
const BACKOFF_MS = []; // no waiting on this machine: a 429 ends it and Supabase replaces it
async function httpBackoff(url, opts = {}) {
  for (let i = 0; ; i++) {
    try { return await http(url, opts); }
    catch (e) {
      if (!(e instanceof Blocked) || i >= BACKOFF_MS.length) throw e;
      if (Date.now() - START + BACKOFF_MS[i] > WALL_LIMIT_MS - 60_000) throw e;
      log(`${e.message} - waiting ${BACKOFF_MS[i] / 1000}s before retrying (back-off ${i + 1}/${BACKOFF_MS.length})`);
      // wait in 50 s steps with a heartbeat, so Supabase does not take this machine for dead (5 min silence)
      for (let left = BACKOFF_MS[i]; left > 0; left -= 50_000) {
        await sleep(Math.min(50_000, left));
        try { await rpc('v2_detail_heartbeat', { p_run: RUN_ID, p_machine: MACHINE }); } catch (_) {}
      }
    }
  }
}

let BUCKET = 'v2-staging';
const lastRaw = new Map(); // `${platform}:${key}` -> { status, path, json }

async function putRaw(objectPath, text) {
  const gz = zlib.gzipSync(Buffer.from(text || '', 'utf8'));
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/storage/v1/object/${BUCKET}/${objectPath}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/gzip', 'x-upsert': 'true' },
        body: gz, signal: AbortSignal.timeout(120_000),
      });
      if (!resp.ok) throw new Error(`storage ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
      return gz.length;
    } catch (e) {
      if (attempt >= 4) throw e;
      log(`upload attempt ${attempt} failed: ${e.message} - retrying`);
      await sleep(3_000 * attempt);
    }
  }
}
const safe = (k) => String(k).replace(/[^A-Za-z0-9_.-]/g, '_');

// save -> raw page parked in Storage (+ v2.raw_file row); returns parsed json (or null)
async function save(item, endpoint, r, request, jsonOverride) {
  let json = jsonOverride !== undefined ? jsonOverride : null;
  if (jsonOverride === undefined) { try { json = r.text ? JSON.parse(r.text) : null; } catch (_) { json = null; } }
  const text = json !== null ? JSON.stringify(json) : (r.text || '');
  const objectPath = `detail/${item.platform}/${safe(item.branch_key)}/run${RUN_ID}-${endpoint}.json.gz`;
  const gzBytes = await putRaw(objectPath, text);
  await rpc('v2_raw_record', {
    p_phase: 'detail', p_run: RUN_ID, p_job: null, p_machine: MACHINE, p_platform: item.platform, p_listing_id: String(item.branch_key),
    p_seq: 1, p_kind: endpoint, p_http: r.status, p_bytes: Buffer.byteLength(text), p_gz: gzBytes, p_path: objectPath,
    p_request: { ...request, ms: r.elapsed },
  });
  lastRaw.set(`${item.platform}:${item.branch_key}`, { status: r.status, path: objectPath, json });
  return json;
}

// -- field extraction (what the build needs; the full page stays in the archive) --
const str = (v) => (v === null || v === undefined || v === '' ? null : String(v));
const e7 = (v) => { const n = fnum(v); return n === null ? null : (Math.abs(n) > 1000 ? n / 1e7 : n); };
const fnum = (v) => (v === null || v === undefined || v === '' || isNaN(Number(v)) ? null : Number(v));
function menuNames(names) {
  const out = new Set();
  for (const n of names) {
    if (typeof n !== 'string') continue;
    const t = n.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    if (t.length >= 3) out.add(t);
    if (out.size >= 800) break;
  }
  return [...out];
}
function findPin(x, d = 0) {
  if (!x || d > 40 || typeof x !== 'object') return null;
  if (Array.isArray(x)) { for (const v of x) { const p = findPin(v, d + 1); if (p) return p; } return null; }
  if (Array.isArray(x.pins) && x.pins.length && x.pins[0] && x.pins[0].lat != null) return { lat: fnum(x.pins[0].lat), lng: fnum(x.pins[0].lon ?? x.pins[0].lng) };
  for (const k of Object.keys(x)) { if (x[k] && typeof x[k] === 'object') { const p = findPin(x[k], d + 1); if (p) return p; } }
  return null;
}
function extract(platform, j) {
  if (!j) return null;
  if (platform === 'talabat') {
    const R = j?.vendor?.result?.restaurant; if (!R) return null;
    const items = [];
    for (const sec of j?.menu?.result?.menu?.menuSection || []) for (const it of sec.itm || []) items.push(it.nm);
    return { branch_key: str(R.bid), name: str(R.bna), brand_id: R.gid && Number(R.gid) !== 0 ? str(R.gid) : null, brand_name: str(R.na),
      account_id: str(R.id), owner_name: str(R.brandLegalName), lat: fnum(R.lat), lng: fnum(R.lon), area: str(R.an), menu: menuNames(items) };
  }
  if (platform === 'careem') {
    const items = [];
    for (const g of j?.menu?.groups || []) for (const it of g.items || []) items.push(it.item);
    return { branch_key: str(j.id), name: str(j.name), brand_id: str(j.brand?.id ?? j.brand_id), brand_name: str(j.brand?.name),
      account_id: str(j.contract_id), phone: str(j.contact?.phone), lat: fnum(j.coordinate?.lat), lng: fnum(j.coordinate?.lng),
      area: str(j.location), menu: menuNames(items) };
  }
  if (platform === 'deliveroo') {
    const root = j?.menuPage?.menu?.metas?.root; const R = root?.restaurant;
    if (!R) {
      const A = j?.app?.found; if (!A) return null;
      return { branch_key: null, name: str(A.name), account_id: str(A.id), lat: fnum(A.lat), lng: fnum(A.lng), menu: [],
        extra: { live_in_app: true, delivery_status: A.delivery_status ?? null, app_href: A.href ?? null, app_check: j.app.how } };
    }
    const pin = findPin(j?.menuPage?.menu?.layoutGroups) || {};
    const loc = R.location || {};
    // dishes only: an item filed under one of the menu's categories (add-on options sit in the same items list)
    const cats = new Set((root.categories || []).map((c) => c && c.id));
    const seen = new Set(), dishes = [];
    for (const i of root.items || []) { if (i && cats.has(i.categoryId) && !seen.has(i.id)) { seen.add(i.id); dishes.push(i.name); } }
    return { branch_key: str(R.drnId), name: str(R.name), brand_id: str(R.brandDrnId), account_id: str(R.id),
      address: str(loc.address?.address1), area: str(loc.address?.neighborhood), zone: str(loc.zoneId), lat: pin.lat ?? null, lng: pin.lng ?? null,
      menu: menuNames(dishes), extra: { menu_disabled: R.menuDisabled ?? null, uname: R.uname ?? null, live_in_app: j?.app?.found ? true : null,
        delivery_status: j?.app?.found?.delivery_status ?? null, app_href: j?.app?.found?.href ?? null } };
  }
  if (platform === 'noon') {
    const D = j?.data; if (!D) return null;
    const logo = typeof D.logoImage === 'string' ? D.logoImage : (D.media?.logoImage || '');
    const partner = (/partner_(\d+)/.exec(logo || '') || [])[1] || null;
    const addr = typeof D.address === 'string' ? D.address : (D.address?.text || D.address?.addressLine || (D.address ? JSON.stringify(D.address) : null));
    return { branch_key: str(D.outletCode), name: str(D.name), brand_id: str(D.restaurantCode), brand_name: str(D.name), account_id: str(D.menuCode),
      owner_id: partner, phone: str(D.phone), lat: e7(D.outletLat), lng: e7(D.outletLng), address: str(addr),
      menu: menuNames((D.menu?.items || []).map((i) => i.name)) };
  }
  return null;
}
// images + cuisines + description, branch level and brand level
const NOON_CDN = 'https://f.nooncdn.com/';
const noonImg = (p) => (typeof p === 'string' && p ? (/^https?:/.test(p) ? p : NOON_CDN + p.replace(/^\//, '')) : null);
const strList = (a, f) => (Array.isArray(a) ? a.map(f).filter((x) => typeof x === 'string' && x).slice(0, 20) : null);
function media(platform, j) {
  if (!j) return {};
  if (platform === 'talabat') {
    const V = j?.vendor?.result || {}, R = V.restaurant || {};
    return { logo_url: str(R.logoUrl), cover_url: str(V.hImgUrl), brand_logo_url: str(R.logoUrl),
      cuisines: strList(R.cus, (c) => c && c.na) || (R.css ? String(R.css).split(',').map((s) => s.trim()).filter(Boolean) : null) };
  }
  if (platform === 'careem') {
    return { logo_url: str(j.logo_url), cover_url: str(j.image_url), brand_logo_url: str(j.brand?.image_url),
      cuisines: strList(j.cuisines, (c) => c && c.name), description: str(j.description) };
  }
  if (platform === 'deliveroo') {
    const M = j?.menuPage?.menu || {};
    return { cover_url: str(M.header?.image?.url), logo_url: null, brand_logo_url: null,
      description: str(M.metas?.root?.metatags?.descriptionSocial) };
  }
  if (platform === 'noon') {
    const D = j?.data || {};
    const logo = noonImg(D.media?.logoImage || D.logoImage);
    return { logo_url: logo, cover_url: noonImg(D.media?.heroImage || (D.media?.images || [])[0] || (D.images || [])[0]), brand_logo_url: logo,
      cuisines: strList(D.cuisines, (c) => (typeof c === 'string' ? c : c && c.name)) };
  }
  return {};
}
async function saveExtract(item, outcome) {
  const raw = lastRaw.get(`${item.platform}:${item.branch_key}`) || {};
  let f = null;
  try { f = extract(item.platform, raw.json); } catch (e) { log(`extract failed: ${e.message}`); }
  if (f) { try { Object.assign(f, media(item.platform, raw.json)); } catch (e) { log(`media failed: ${e.message}`); } }
  await rpc('v2_detail_save_extract', { p_run: RUN_ID, p_machine: MACHINE, p_platform: item.platform, p_key: String(item.branch_key),
    p_http: raw.status ?? null, p_outcome: f ? outcome : (outcome === 'done' ? 'no_fields' : outcome), p_f: f || {}, p_raw_path: raw.path || null });
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
// Step 1 is the Deliveroo app (mobile GraphQL, the same endpoint and headers as the v1 hourly ranking scrape,
// which runs on 100+ machines a day without 429s): is the restaurant in the app at its own location?
//   a) text search for its name there, then b) if not found, the full app listing there (unavailable ones included).
// Found -> live: the app gives its web page path, which is opened (step 2) for the menu.
// Not found by either -> not live on Deliveroo (no website request at all).
const ROO_GQL = 'https://co-m.ae.deliveroo.com/consumer/graphql/';
const ROO_CAPS = {"fulfillment_methods": ["DELIVERY", "COLLECTION"], "ui_actions": ["CHANGE_DELIVERY_TIME", "CLEAR_FILTERS", "NO_DELIVERY_YET", "SHOW_MEAL_CARD_ISSUERS", "SHOW_PLUS_SIGN_UP", "TOGGLE_FAVOURITE", "COPY_TO_CLIPBOARD", "REFRESH", "SUBSCRIBE_FREE_PLUS", "REQUEST_RESTAURANT"], "ui_blocks": ["BANNER", "SHORTCUT", "CARD", "BUTTON", "MERCHANDISING_CARD", "DIVIDER", "STORY_CARD", "ROO_BLOCK"], "ui_controls": ["APPLIED_FILTER", "FILTER", "LAYOUT_GROUP", "QUERY_RESULT", "SORT", "TABS", "SORTING_AND_FILTERING", "TABBAR"], "ui_features": ["UI_CAROUSEL_COLOR", "UI_CARD_BORDER", "UI_PROMOTION_TAG", "ILLUSTRATION_BADGES", "SCHEDULED_RANGES", "LIMIT_QUERY_RESULTS", "UI_SPAN_COUNTDOWN", "HOME_MAP_VIEW", "UNAVAILABLE_RESTAURANTS", "UI_CARD_BADGES", "CARD_ILLUSTRATION_BADGE", "UI_CAROUSEL_BACKGROUND_IMAGE", "UI_SPAN_TAGS"], "ui_layout_carousel_styles": ["DEFAULT"], "ui_layouts": ["LIST", "CAROUSEL"], "ui_lines": ["TEXT", "TITLE", "BULLET"], "ui_targets": ["ACTION", "LAYOUT_GROUP", "PARAMS", "RESTAURANT", "WEB_PAGE", "DEEP_LINK", "MENU_ITEM", "EDITORIAL_CONTENT"], "ui_themes": ["BANNER_EMPTY", "BANNER_MARKETING_A", "BANNER_MARKETING_B", "BANNER_MARKETING_C", "BANNER_PICKUP_SHOWCASE", "BANNER_SERVICE_ADVISORY", "SHORTCUT_DEFAULT", "SHORTCUT_STACKED", "CARD_LARGE_V2", "CARD_MEDIUM_V2", "CARD_MEDIUM_HORIZONTAL", "CARD_SMALL", "CARD_SMALL_DIAGONAL", "CARD_SMALL_HORIZONTAL", "CARD_WIDE", "CARD_ROW_LARGE", "BUTTON_PRIMARY", "BUTTON_SECONDARY", "MODAL_DEFAULT", "MODAL_PLUS", "MODAL_PLUS_INVITE", "MODAL_BUTTON_PRIMARY", "MODAL_BUTTON_SECONDARY", "MODAL_BUTTON_TERTIARY", "BANNER_CARD", "CARD_TALL", "CARD_TALL_GRADIENT", "MERCHCARD_DEFAULT", "MERCHCARD_FULL_IMAGE", "MODAL_MERCHANDISING", "MODAL_PROMOTIONAL", "MODAL_ROOBLOCK", "ANY_MODAL", "MODAL_PLUS_FULL_SCREEN"]};
const ROO_R = 'id drn_id name delivery_status lat lon distance_meters links { self { href } }';
const ROO_B = `ui_blocks { __typename ... on UICard { target { __typename ... on UITargetRestaurant { restaurant { ${ROO_R} } } } } }`;
const rooQuery = (op) => `query T($options: SearchOptionsInput, $location: LocationInput!, $uuid: String!, $capabilities: Capabilities) { ${op}(location: $location, options: $options, uuid: $uuid, capabilities: $capabilities) { ui_layout_groups { ui_layouts { __typename ... on UILayoutList { ${ROO_B} } ... on UILayoutCarousel { ${ROO_B} } } } } }`;
const ROO_BASE = { fulfillment_include_asap_days: true, web_column_count: 1 };
function rooHeaders() {
  const g = crypto.randomUUID().toUpperCase();
  return { 'User-Agent': 'Deliveroo-OrderApp/3.342.0 (iPhone18,2; iOS27.0.1; Release; en_US; 697347)', 'X-Roo-App-Version': '3.342.0',
    'X-Roo-Sticky-Guid': g, 'X-Roo-Guid': g, 'X-Roo-Country': 'ae', 'X-Roo-Platform': 'iOS', 'X-Roo-External-Device-Id': crypto.randomUUID().toUpperCase(),
    'X-Roo-Rooblocks-Version': '5.3.0', 'apollographql-client-name': 'com.deliveroo.orderapp-apollo-ios', 'apollographql-client-version': '3.342.0-697347',
    'X-APOLLO-OPERATION-NAME': 'T', 'X-APOLLO-OPERATION-TYPE': 'query', Accept: 'multipart/mixed;deferSpec=20220824,application/json',
    'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
}
async function rooSearch(op, lat, lng, options) {
  const body = { operationName: 'T', query: rooQuery(op), variables: { uuid: crypto.randomUUID().toUpperCase(), capabilities: ROO_CAPS,
    location: { country_iso_code: 'AE', lat, lon: lng }, options } };
  const r = await http(ROO_GQL, { method: 'POST', headers: rooHeaders(), body: JSON.stringify(body) });
  if (r.status !== 200) throw new Error(`deliveroo app HTTP ${r.status}`);
  let j; try { j = JSON.parse(r.text); } catch (_) { throw new Error('deliveroo app: unreadable reply'); }
  if (j.errors && !j.data?.[op]) throw new Error(`deliveroo app: ${String(j.errors[0]?.message || 'error').slice(0, 120)}`);
  const out = [];
  for (const g of j.data?.[op]?.ui_layout_groups || []) for (const l of g.ui_layouts || []) for (const b of l.ui_blocks || []) {
    const x = b?.target?.restaurant; if (x && x.drn_id) out.push(x);
  }
  return out;
}

async function deliveroo(item) {
  const lat = Number(item.lat), lng = Number(item.lng);
  const app = { checked_at: new Date().toISOString(), lat, lng, name_searched: item.name || null, results: 0, how: null, found: null };
  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    let found = null;
    if (item.name) {
      const rs = await rooSearch('text_search', lat, lng, { ...ROO_BASE, query: String(item.name), search_button_pressed: true });
      app.results += rs.length; found = rs.find((x) => x.drn_id === item.branch_key); app.how = 'name search';
    }
    if (!found) {
      if (item.name) await sleep(1_500 + Math.random() * 1_000);
      const rs = await rooSearch('search', lat, lng, { ...ROO_BASE, params: [{ id: 'collection', value: ['restaurants', 'all-restaurants'] }] });
      app.results += rs.length; found = rs.find((x) => x.drn_id === item.branch_key); app.how = 'area listing';
    }
    if (found) app.found = { id: str(found.id), name: found.name, delivery_status: found.delivery_status, lat: found.lat, lng: found.lon,
      distance_m: found.distance_meters, href: found.links?.self?.href || null };
    if (!found) {
      await save(item, 'app_search', { status: 200, text: null, elapsed: 0 }, { method: 'POST', url: ROO_GQL, lat, lng, name: item.name || null }, { app });
      return 'unavailable';
    }
  }
  // Step 2: live in the app (or no coordinates) -> the web menu page, at the path the app gives (falls back to the stored URL)
  const href = app.found?.href ? String(app.found.href).split('?')[0] : null;
  const pageUrl = href ? 'https://deliveroo.ae/en' + href.split('/').map((p) => encodeURIComponent(decodeURIComponent(p).toLowerCase().replace(/\s+/g, '-'))).join('/') : item.page_url;
  if (!pageUrl) { await save(item, 'app_search', { status: 200, text: null, elapsed: 0 }, { note: 'no page url' }, { app }); return app.found ? 'done' : 'unavailable'; }
  const ghs = [geohash(lat, lng), ...(Array.isArray(item.geohashes) ? item.geohashes.slice(0, 2) : [])].filter((g) => g && !g.startsWith('7zzzz'));
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.5' };
  let last = null;
  for (let i = 0; i < ghs.length; i++) {
    if (i > 0) await sleep(3_000 + Math.random() * 2_000);
    const gh = ghs[i];
    const url = `${pageUrl}?geohash=${encodeURIComponent(gh)}`;
    const r = await http(url, { headers, redirect: 'follow' });
    const m = /<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/.exec(r.text || '');
    let d = null; try { d = m ? JSON.parse(m[1]) : null; } catch (_) { d = null; }
    const body = { app, menuPage: d?.props?.initialState?.menuPage ?? null, query: d?.query ?? null, page: d?.page ?? null, buildId: d?.buildId ?? null };
    const root = body.menuPage?.menu?.metas?.root;
    const isMenu = r.status === 200 && root?.restaurant && Array.isArray(root.categories) && root.categories.length > 0;
    last = { r, body, url, gh };
    if (isMenu) {
      await save(item, 'menu_page_next_data', { ...r, text: null }, { method: 'GET', url, geohash: gh, final_url: r.resp.url, area_try: i + 1 }, body);
      return 'done';
    }
  }
  // in the app but its web page showed no menu: still live (the app result is the evidence), menu left empty
  const { r, body, url, gh } = last;
  await save(item, 'menu_page_next_data', { ...r, text: null }, { method: 'GET', url, geohash: gh, final_url: r.resp.url, area_tries: ghs.length }, body);
  return app.found ? 'done' : 'unavailable';
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
  try { const b = await rpc('v2_setting', { p_key: 'staging_bucket' }); if (typeof b === 'string' && b) BUCKET = b; } catch (_) {}
  const jobs = Number(st.jobs) || 40, delay = Number(st.delay_ms) || 800;
  let exitReason = 'job quota reached', done = 0;
  const skip = new Set(); // apps that refused this machine's IP (403/429): keep working on the others
  for (let j = 0; j < jobs; j++) {
    if (Date.now() - START > WALL_LIMIT_MS) { exitReason = 'wall limit'; break; }
    if (skip.size >= 4) { exitReason = 'blocked (403/429) on all apps'; break; }
    const item = await rpc('v2_detail_claim', { p_run: RUN_ID, p_machine: MACHINE, p_skip: [...skip] });
    if (!item || item.stop) { exitReason = (item && item.reason) || 'nothing to claim'; break; }
    current = item;
    let outcome, err = null;
    try { outcome = await FETCH[item.platform](item); }
    catch (e) { outcome = e instanceof Blocked ? 'blocked' : 'error'; err = String(e.message).slice(0, 400); }
    if (outcome === 'done' || outcome === 'unavailable') {
      try { await saveExtract(item, outcome); } catch (e) { outcome = 'error'; err = `save extract: ${String(e.message).slice(0, 300)}`; }
    }
    lastRaw.delete(`${item.platform}:${item.branch_key}`);
    const rep = await rpc('v2_detail_report', { p_run: RUN_ID, p_machine: MACHINE, p_platform: item.platform, p_key: item.branch_key, p_outcome: outcome, p_error: err });
    current = null; done++;
    log(`${item.platform} ${item.branch_key}: ${outcome}${err ? ' | ' + err : ''}`);
    if (outcome === 'blocked') { exitReason = `${err} - replaced`; log('refused - ending this machine; Supabase starts a fresh one'); break; }
    if (rep && rep.action === 'stop') { exitReason = 'stopped by controller'; break; }
    // pace set by Supabase (it slows the run after a refusal), with +-30% jitter so machines don't fire in step
    const pace = Number(item.delay_ms) || delay;
    if (j < jobs - 1) await sleep(pace * (0.7 + Math.random() * 0.6));
  }
  await endMachine(exitReason);
  log(`=== ${done} restaurants, ${Math.round((Date.now() - START) / 1000)}s, exit "${exitReason}"`);
})().catch(async (e) => { console.error(`[m${MACHINE}] FATAL: ${e.message}`); await endMachine(`fatal: ${String(e.message).slice(0, 200)}`); process.exit(1); });
