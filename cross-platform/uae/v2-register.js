#!/usr/bin/env node
// cross-platform/uae/v2-register.js
//
// v2 registration runner. One GitHub Actions run = one runner for ONE app (lane).
// Supabase is the controller (schema reg): the Edge Function reg-dispatch starts this run, reg_claim hands it
// one new listing at a time, and reg_report saves what the restaurant's own page shows. Linking the listing
// into v2 (branch, site, storefront, owner) happens afterwards in Supabase, one listing at a time.
// Supabase never calls the delivery apps; only this runner does.
//
// Endpoints (unchanged from the v2 detail scrape that registered the first 100k listings):
//   talabat    GET  api.talabat.com/menubff/v4/branches/{bid}/menu
//   careem     GET  apigateway.careemdash.com/v1/restaurants/{merchant_id}   (token from the v1 pool via reg_careem_token)
//   deliveroo  POST co-m.ae.deliveroo.com/consumer/graphql/ (is it listed at its location?) then, if live,
//              GET  {menu page}?geohash={point} -> __NEXT_DATA__
//   noon       POST outlet/details/guest/partial (guest token minted on this runner)
// 429/403: this runner stops at once, the listing goes back in the queue (not counted as a failed attempt),
// and Supabase starts a fresh runner (new machine, new IP).
//
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RUNNER_ID, APP, GITHUB_RUN_ID

'use strict';

const crypto = require('node:crypto');
const zlib = require('node:zlib');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const RUNNER = Number(process.env.RUNNER_ID);
const APP = String(process.env.APP || '');
const GH_RUN_ID = process.env.GITHUB_RUN_ID || '';
if (!SUPABASE_URL || !SUPABASE_KEY || !RUNNER || !APP) {
  console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / RUNNER_ID / APP');
  process.exit(1);
}

const START = Date.now();
const WALL_LIMIT_MS = 25 * 60 * 1000;
const HTTP_TIMEOUT_MS = 30_000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (m) => console.log(`[${new Date().toISOString().slice(11, 19)}] [${APP} r${RUNNER}] ${m}`);

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

let BUCKET = 'reg-raw';
const lastRaw = new Map(); // `${app}:${id}` -> { status, path, json }

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
      if (attempt >= 3) { log(`raw upload skipped: ${e.message}`); return null; } // the raw copy is evidence only; never fail a listing for it
      await sleep(3_000 * attempt);
    }
  }
}
const safe = (k) => String(k).replace(/[^A-Za-z0-9_.-]/g, '_');

// raw page parked in Storage; returns parsed json (or null)
async function save(item, endpoint, r, request, jsonOverride) {
  let json = jsonOverride !== undefined ? jsonOverride : null;
  if (jsonOverride === undefined) { try { json = r.text ? JSON.parse(r.text) : null; } catch (_) { json = null; } }
  const text = json !== null ? JSON.stringify(json) : (r.text || '');
  const objectPath = `${item.platform}/${safe(item.branch_key)}/${new Date().toISOString().replace(/[:.]/g, '-')}-${endpoint}.json.gz`;
  const gz = await putRaw(objectPath, JSON.stringify({ request: { ...request, ms: r.elapsed, http: r.status }, body: text }));
  lastRaw.set(`${item.platform}:${item.branch_key}`, { status: r.status, path: gz === null ? null : objectPath, json });
  return json;
}

// -- field extraction (unchanged from the v2 detail scrape) -------------------
const str = (v) => (v === null || v === undefined || v === '' ? null : String(v));
const fnum = (v) => (v === null || v === undefined || v === '' || isNaN(Number(v)) ? null : Number(v));
const e7 = (v) => { const n = fnum(v); return n === null ? null : (Math.abs(n) > 1000 ? n / 1e7 : n); };
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
      address: str(loc.address?.address1), area: str(loc.address?.neighborhood), zone: str(loc.zoneId),
      lat: pin.lat ?? fnum(j?.app?.found?.lat) ?? null, lng: pin.lng ?? fnum(j?.app?.found?.lng) ?? null,
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

// -- Careem (token from the v1 pool, refreshed every 12 h by v1) -------------
let careemTok = null;
async function careem(item) {
  if (!careemTok) careemTok = await rpc('reg_careem_token', {});
  if (!careemTok) throw new Error('no live Careem token in careem_scraper_token_pool');
  let sub = '';
  try { sub = JSON.parse(Buffer.from(careemTok.split('.')[1].replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8')).sub || ''; } catch (_) {}
  const lat = String(item.lat ?? '25.1972'), lng = String(item.lng ?? '55.2744');
  const sess = 'V2RG-' + crypto.randomBytes(4).toString('hex').toUpperCase();
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
// Step 1: the Deliveroo app (mobile GraphQL, as the v1 hourly ranking scrape): is the restaurant listed at its own location?
// a) text search for its name there, then b) if not found, the full app listing there (unavailable ones included).
// Found -> live: the app gives its web page path, which is opened (step 2) for the menu. Not found -> not live.
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
  if (item.lat != null && item.lng != null && Number.isFinite(lat) && Number.isFinite(lng)) {
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
  const ghs = [Number.isFinite(lat) && item.lat != null ? geohash(lat, lng) : null, ...(Array.isArray(item.geohashes) ? item.geohashes.slice(0, 2) : [])]
    .filter((g) => g && !g.startsWith('7zzzz'));
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
  if (!last) { await save(item, 'app_search', { status: 200, text: null, elapsed: 0 }, { note: 'no geohash to open the page from' }, { app }); return app.found ? 'done' : 'unavailable'; }
  // in the app but its web page showed no menu: still live (the app result is the evidence), menu left empty
  const { r, body, url, gh } = last;
  await save(item, 'menu_page_next_data', { ...r, text: null }, { method: 'GET', url, geohash: gh, final_url: r.resp.url, area_tries: ghs.length }, body);
  return app.found ? 'done' : 'unavailable';
}

// -- Noon (token minted on this runner, re-located per outlet) ---------------
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
  if (noon && Date.now() - noon.mintedAt < 240_000) return noon; // as v1 registration: one guest session per ~4 min, no re-locate per outlet
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
  if (item.lat == null || item.lng == null) throw new Error('noon needs a location to open the outlet; none queued');
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

// one result back to Supabase: the extracted fields (if any) and the outcome
async function report(item, outcome, err) {
  const raw = lastRaw.get(`${item.platform}:${item.branch_key}`) || {};
  let f = null;
  if (outcome === 'done' || outcome === 'unavailable') {
    try { f = extract(item.platform, raw.json); } catch (e) { log(`extract failed: ${e.message}`); }
    if (f) { try { Object.assign(f, media(item.platform, raw.json)); } catch (e) { log(`media failed: ${e.message}`); } }
    if (outcome === 'done' && !f) outcome = 'no_fields';
  }
  lastRaw.delete(`${item.platform}:${item.branch_key}`);
  return rpc('reg_report', { p_runner: RUNNER, p_app: item.platform, p_listing_id: String(item.branch_key), p_outcome: outcome,
    p_http: raw.status ?? null, p_fields: f || {}, p_raw_path: raw.path || null, p_error: err });
}

// -- Main -------------------------------------------------------------------
let ending = false;
async function endRunner(reason) {
  if (ending) return; ending = true;
  try { await rpc('reg_runner_end', { p_runner: RUNNER, p_reason: reason }); } catch (e) { log(`runner_end failed: ${e.message}`); }
}
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => { await endRunner(`cancelled (${sig})`); process.exit(0); });

(async () => {
  if (!FETCH[APP]) { log(`unknown app ${APP}`); await endRunner(`unknown app ${APP}`); return; }
  const st = await rpc('reg_runner_start', { p_runner: RUNNER, p_gh: GH_RUN_ID });
  if (!st || st.stop) { log(`not starting: ${st && st.reason}`); ending = true; return; }
  if (typeof st.bucket === 'string' && st.bucket) BUCKET = st.bucket;
  const jobs = Number(st.jobs) || 30, delay = Number(st.delay_ms) || 1500;
  let exitReason = 'job quota reached', done = 0;
  const hb = setInterval(() => { rpc('reg_heartbeat', { p_runner: RUNNER }).catch(() => {}); }, 60_000);
  for (let j = 0; j < jobs; j++) {
    if (Date.now() - START > WALL_LIMIT_MS) { exitReason = 'wall limit'; break; }
    const c = await rpc('reg_claim', { p_runner: RUNNER });
    if (!c || c.stop) { exitReason = (c && c.reason) || 'nothing to claim'; break; }
    const item = { ...c, platform: c.app, branch_key: c.listing_id };
    let outcome, err = null;
    try { outcome = await FETCH[APP](item); }
    catch (e) { outcome = e instanceof Blocked ? 'blocked' : 'error'; err = String(e.message).slice(0, 400); }
    let rep = null;
    try { rep = await report(item, outcome, err); }
    catch (e) { log(`report failed: ${e.message}`); exitReason = 'could not report to Supabase'; break; }
    done++;
    log(`${item.branch_key}: ${outcome} -> ${rep && rep.status}${err ? ' | ' + err : ''}`);
    if (outcome === 'blocked') { exitReason = `${err} - replaced`; log('refused - ending this runner; Supabase starts a fresh one'); break; }
    if (rep && rep.action === 'stop') { exitReason = 'stopped by controller'; break; }
    // pace from Supabase (per app), with +-30% jitter
    const pace = Number(c.delay_ms) || delay;
    if (j < jobs - 1) await sleep(pace * (0.7 + Math.random() * 0.6));
  }
  clearInterval(hb);
  await endRunner(exitReason);
  log(`=== ${done} listings, ${Math.round((Date.now() - START) / 1000)}s, exit "${exitReason}"`);
})().catch(async (e) => { console.error(`[${APP} r${RUNNER}] FATAL: ${e.message}`); await endRunner(`fatal: ${String(e.message).slice(0, 200)}`); process.exit(1); });
