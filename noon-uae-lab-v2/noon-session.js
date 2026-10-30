// Everything that talks to Noon's API: HTTP/1.1 dispatcher, headers that match
// the real iPhone trace, mint phases, pagination, decoys.
//
// No decisions live here. The worker hands this module a resolved spec from
// Supabase and this module executes it. If you want to change behaviour, change
// the spec in Supabase (noon_lab.variant.spec), not this file.

'use strict';
const crypto = require('node:crypto');
const undici = require('undici');

// ---------- HTTP/1.1 dispatcher (matches real app ALPN) ---------------------
// Real Noon iOS traffic is HTTP/1.1 across every api-app*.noon.com call.
// Node's built-in fetch negotiates H2, which no real iOS client does here.
const h11Agent = new undici.Agent({
  allowH2: false,
  keepAliveTimeout: 60_000,
  keepAliveMaxTimeout: 60_000,
  connections: 4,
});

async function doFetch(url, opts) {
  return undici.fetch(url, { ...opts, dispatcher: h11Agent });
}

// ---------- URLs & constants from the real trace ----------------------------
const UA    = 'noon/22895 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const BUILD = '22895';

const APP_BASE     = 'https://api-app.noon.com';
const APP_ST_BASE  = 'https://api-app-st.noon.com';
const FD_BASE      = 'https://api-app-fd.noon.com';
const INST_BASE    = 'https://api-app-instant.noon.com';

const WHOAMI_URL       = `${APP_ST_BASE}/_vs/st/st-whoami-api/whoami`;
const SET_LOCATION_URL = `${APP_BASE}/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng`;
const WHOAMI_FOOD_URL  = `${APP_ST_BASE}/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1`;
const SEARCH_URL       = `${FD_BASE}/_svc/mp-food-api-catalog/api/search`;

const PAGE_LIMIT = 60;

// ---------- Header builders — fingerprint-matched to the Proxyman trace -----
// Key corrections vs v1:
//  * sentry-trace has NO '-0' sampled-flag suffix
//  * baggage does NOT include sentry-release
//  * first whoami sends NO x-locale and NO x-device-lat/lng
//  * identity is mixed-case: device_id UPPER (CFNetwork), visitor_id lower (Noon SDK)

function sentryTrace() {
  const trace = crypto.randomBytes(16).toString('hex');
  const span  = crypto.randomBytes(8).toString('hex');
  return {
    'sentry-trace': `${trace}-${span}`,                                           // no -0 suffix
    baggage: `sentry-environment=production,sentry-public_key=d61e4de762b3c36e7c14f2d7cf6030c1,sentry-trace_id=${trace}`,
  };
}

// Headers are built via session.* wrappers below so they can honour minimalHeaders.
// The raw helpers take an explicit `minimal` flag; keep them for direct use.

function baseHeaders(deviceId, visitorId, minimal = false) {
  const h = {
    'User-Agent': UA,
    'x-platform': 'ios',
    'x-build': BUILD,
    'x-mp': 'noon',
    'x-content': 'mobile',
    Accept: 'application/json, text/plain, */*',
    'x-device-id': deviceId,
    'x-visitor-id': visitorId,
  };
  if (!minimal) {
    h['Accept-Language'] = 'en-US,en;q=0.9';
    h['Accept-Encoding'] = 'gzip, deflate, br';
    h['Cache-Control'] = 'no-cache';
    h['Connection'] = 'keep-alive';
    Object.assign(h, sentryTrace());
  }
  return h;
}

function headersFirstWhoami(deviceId, visitorId, minimal = false, experience = 'ecom') {
  // Matches the real first call exactly: no x-locale, no device lat/lng, no cookies.
  return {
    ...baseHeaders(deviceId, visitorId, minimal),
    'x-experience': experience,
    'x-whoami-request-id': crypto.randomUUID(),
  };
}

function headersEcom(deviceId, visitorId, lat, lng, cookie, locale = 'en-ae', minimal = false) {
  return {
    ...baseHeaders(deviceId, visitorId, minimal),
    'x-experience': 'ecom',
    'x-locale': locale,
    'x-device-lat': String(lat),
    'x-device-lng': String(lng),
    Cookie: cookie,
  };
}

function headersFood(deviceId, visitorId, lat, lng, cookie, locale = 'en-ae', minimal = false) {
  return {
    ...baseHeaders(deviceId, visitorId, minimal),
    'x-experience': 'food',
    'x-locale': locale,
    'x-device-lat': String(lat),
    'x-device-lng': String(lng),
    Cookie: cookie,
  };
}

function headersSearch(deviceId, visitorId, lat, lng, cookie, whoHeaders, locale = 'en-ae', minimal = false) {
  const h = {
    ...headersFood(deviceId, visitorId, lat, lng, cookie, locale, minimal),
    'x-mp-country': 'ae',
    'x-lat': String(Math.round(lat * 1e7)),
    'x-lng': String(Math.round(lng * 1e7)),
    'x-border-enabled': 'true',
    'x-rocket-enabled': 'true',
    'Content-Type': 'application/json',
  };
  for (const [k, v] of Object.entries(whoHeaders || {})) {
    if (v != null && typeof v !== 'object') h[k] = String(v);
  }
  return h;
}

// ---------- Cookie handling -------------------------------------------------
function mergeSetCookies(jar, resp) {
  const list = typeof resp.headers.getSetCookie === 'function' ? resp.headers.getSetCookie() : [];
  for (const c of list) {
    const m = /^([^=]+)=([^;]+)/.exec(c);
    if (m) jar[m[1]] = m[2];
  }
}
const cookieStr = (jar) => Object.entries(jar).map(([k, v]) => `${k}=${v}`).join('; ');

// ---------- Request logging + Akamai inspection -----------------------------
const AKAMAI = /^(ak_bmsc|bm_|_abck)/;
function describeSetCookies(resp) {
  const list = typeof resp.headers.getSetCookie === 'function' ? resp.headers.getSetCookie() : [];
  return list.map((c) => {
    const parts = c.split(';').map((x) => x.trim());
    const m = /^([^=]+)=(.*)$/.exec(parts[0]) || [];
    const name = m[1] || '?', value = m[2] || '';
    const attr = {};
    for (const p of parts.slice(1)) { const [k, ...v] = p.split('='); attr[k.toLowerCase()] = v.join('=') || true; }
    const out = { name, len: value.length, max_age: attr['max-age'] || null,
                  expires: attr.expires || null, domain: attr.domain || null };
    if (AKAMAI.test(name)) out.value = value.slice(0, 300);
    return out;
  });
}

async function captureBlock(step, resp) {
  const headers = {};
  resp.headers.forEach((v, k) => { if (k !== 'set-cookie') headers[k] = String(v).slice(0, 200); });
  let body = '';
  try { body = (await resp.text()).slice(0, 400); } catch (_) {}
  return { step, status: resp.status, headers, set: describeSetCookies(resp), body };
}

// ---------- Session object --------------------------------------------------
// Holds the mutable state for ONE area: cookies, identity, timing, request log.
class Session {
  constructor({ deviceId, visitorId, locale = 'en-ae',
                minimalHeaders = false, firstExperience = 'ecom' }) {
    this.deviceId = deviceId;
    this.visitorId = visitorId;
    this.locale = locale;
    // minimalHeaders=true drops sentry-trace, baggage, Accept-Language,
    // Accept-Encoding, Cache-Control, Connection. Modeled on the production
    // menu-read Edge Function, which runs clean against Noon at scale.
    this.minimalHeaders = minimalHeaders;
    // 'ecom' = first whoami hits ecom whoami URL (full-launch user journey).
    // 'food' = first whoami hits food whoami URL (resumed-from-food-tab journey).
    this.firstExperience = firstExperience;
    this.jar = {};
    this.whoHeaders = {};
    this.mintedAt = null;
    this.requests = 0;
    this.reqLog = [];
    this.blockInfo = null;
    this.searchPagesLogged = 0;
    this.initialSearchToken = null;
  }

  async fetch(step, url, opts) {
    const t0 = Date.now();
    this.requests++;
    let resp;
    try {
      resp = await doFetch(url, { ...opts, signal: AbortSignal.timeout(30000) });
    } catch (e) {
      const chain = errorChain(e);
      throw Object.assign(new Error(`${step} network: ${chain}`), { step, status: null, kind: 'error' });
    }
    this._logRequest(step, url, opts, resp, Date.now() - t0);
    if (resp.status === 403 || resp.status === 429) {
      this.blockInfo = await captureBlock(step, resp);
      const kind = resp.status === 403 ? 'blocked' : 'rate_limited';
      throw Object.assign(new Error(`${step} HTTP ${resp.status}`), { step, status: resp.status, kind });
    }
    return resp;
  }

  _logRequest(step, url, opts, resp, ms) {
    const set = describeSetCookies(resp);
    if (step === 'search') {
      this.searchPagesLogged++;
      if (this.searchPagesLogged > 1 && set.length === 0 && resp.ok) return;
    }
    const sentCookies = String((opts.headers && opts.headers.Cookie) || '')
      .split(';').map(x => x.split('=')[0].trim()).filter(Boolean);
    this.reqLog.push({
      step, host: new URL(url).host, status: resp.status, ms,
      server: resp.headers.get('server'),
      sent_cookies: sentCookies, set,
    });
  }
}

// Attempt to read `searchToken` out of a successful JSON response and stash it on
// the session. Called after the food-home calls that are known to carry it. Clones
// the response so the main code path can still consume the body if needed.
async function captureInitialToken(session, resp, label) {
  try {
    const clone = resp.clone();
    const j = await clone.json();
    const tok = j && typeof j === 'object' ? j.searchToken : null;
    if (tok && typeof tok === 'string' && tok.length > 20) {
      session.initialSearchToken = tok;
      log(`captured initialSearchToken from ${label} (${tok.length} chars)`);
    }
  } catch (_) { /* body wasn't JSON or clone failed — ignore */ }
}

function errorChain(e) {
  const parts = []; let cur = e;
  while (cur) {
    if (cur.message) parts.push(cur.message);
    if (cur.code) parts.push(`[${cur.code}]`);
    cur = cur.cause;
  }
  return parts.join(' → ') || String(e);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const log = (m) => console.log(`[v2] ${m}`);

// ---------- Mint phases (data-driven by spec.mint_phases) -------------------

async function mint(session, lat, lng, spec) {
  const t = spec.mint_timing_ms || {};
  const phases = spec.mint_phases || ['ecom_warmup', 'food_tab', 'food_browse'];

  // The first whoami is ALWAYS the first call (matches real trace T+0).
  // firstExperience='food' sends it to the food-whoami URL — simulates
  // "user had food as last-selected experience and reopened the app".
  const firstUrl = session.firstExperience === 'food' ? WHOAMI_FOOD_URL : WHOAMI_URL;
  const r1 = await session.fetch('mint_whoami', firstUrl, {
    headers: headersFirstWhoami(session.deviceId, session.visitorId, session.minimalHeaders, session.firstExperience),
  });
  mergeSetCookies(session.jar, r1);

  // Splash / location-permission gap (real trace: 7.25s)
  await sleep(t.splash_gap || 7000);

  // set-location
  const r2 = await session.fetch('mint_set_location', SET_LOCATION_URL, {
    method: 'POST',
    headers: { ...headersEcom(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.locale, session.minimalHeaders),
               'Content-Type': 'application/json' },
    body: JSON.stringify({ location: { lat, lng }, lang: 'en' }),
  });
  mergeSetCookies(session.jar, r2);

  // whoami-food (gets nguestv2 token + ab-test headers).
  // If we already called food-whoami as the FIRST call (firstExperience='food'),
  // this second call still runs so we re-pull zone headers after set-location.
  await sleep(t.post_whoami_food || 500);
  const r3 = await session.fetch('mint_whoami_food', WHOAMI_FOOD_URL, {
    headers: { ...headersEcom(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.locale, session.minimalHeaders),
               'x-whoami-request-id': crypto.randomUUID() },
  });
  const whoResp = await r3.json().catch(() => ({}));
  mergeSetCookies(session.jar, r3);
  if (!session.jar.nguestv2) {
    throw Object.assign(new Error('mint produced no nguestv2'), { step: 'mint_whoami_food', status: null, kind: 'error' });
  }
  session.whoHeaders = (whoResp && whoResp.headers && typeof whoResp.headers === 'object') ? whoResp.headers : {};
  session.mintedAt = Date.now();

  for (const phase of phases) {
    if (phase === 'ecom_warmup') await ecomWarmup(session, lat, lng, t);
    else if (phase === 'food_tab') await foodTab(session, lat, lng, t);
    else if (phase === 'food_browse') await foodBrowse(session, lat, lng, t);
    else log(`unknown mint phase: ${phase}`);
  }

  // Pre-search pause (user deciding to tap "See all")
  await sleep(t.pre_search || 9000);
}

async function ecomWarmup(session, lat, lng, t) {
  const H = () => headersEcom(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.locale, session.minimalHeaders);

  await sleep(t.post_warmup1 || 300);
  await Promise.allSettled([
    session.fetch('warmup_membership', `${APP_BASE}/_svc/mp-vip-api-v1/membership?primaryUphoneCode=none`, { headers: H() }),
    session.fetch('warmup_splash',     `${INST_BASE}/_svc/catalog/splash-screen-config`,                   { headers: H() }),
    session.fetch('warmup_catalog',    `${APP_BASE}/_svc/catalog/api/v3/u/`,                               { headers: H() }),
  ]);

  await sleep(t.pre_warmup2 || 5500);
  await Promise.allSettled([
    session.fetch('warmup_configs', `${APP_BASE}/_svc/configs-v1/configs`, { headers: H() }),
    session.fetch('warmup_cart',    `${APP_BASE}/_svc/cart-v1/cart`,       { headers: H() }),
  ]);

  await sleep(t.post_warmup2 || 220);
  await Promise.allSettled([
    session.fetch('warmup_ads', `${APP_BASE}/_svc/mp-ads-api-adserver/ads/get-by-slot-codes`, {
      method: 'POST', headers: { ...H(), 'Content-Type': 'application/json' },
      body: JSON.stringify(['ocm_ae_mb_homepage_in_the_spotlight_1','ocm_ae_mb_homepage_in_the_spotlight_2']),
    }),
    session.fetch('warmup_catalog2', `${APP_BASE}/_svc/catalog/api/v3/u/?limit=18`, { headers: H() }),
  ]);
}

async function foodTab(session, lat, lng, t) {
  await sleep(t.food_tab_gap || 1600);
  const H = () => headersFood(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.locale, session.minimalHeaders);
  const foodHomeP = session.fetch('food_home', `${FD_BASE}/_svc/mp-food-api-catalog/api/`, { headers: H() });
  foodHomeP.then((r) => captureInitialToken(session, r, 'food_home')).catch(() => {});
  await Promise.allSettled([
    foodHomeP,
    session.fetch('food_popups',      `${FD_BASE}/_svc/mp-food-api-catalog/api/popups`,                    { headers: H() }),
    session.fetch('food_whoami',      `${FD_BASE}/_svc/mp-food-api-catalog/api/customer/whoami`,           { headers: H() }),
    session.fetch('food_suggestions', `${FD_BASE}/_svc/mp-food-api-catalog/api/page/suggestions`,          { headers: H() }),
    session.fetch('food_favorites',   `${FD_BASE}/_svc/mp-food-api-catalog/api/customer/favorite/restaurants`, { headers: H() }),
    session.fetch('food_navbar',      `${FD_BASE}/_svc/mp-food-api-catalog/api/customer/navbar-tabs-config`,   { headers: H() }),
  ]);

  await sleep(t.post_food_burst || 900);
  const S = () => headersSearch(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.whoHeaders, session.locale, session.minimalHeaders);
  await Promise.allSettled([
    session.fetch('food_homepage_dyn',    `${FD_BASE}/_svc/mp-food-api-mpnoon/order/homepage/dynamic`, { headers: S() }),
    session.fetch('food_homepage_static', `${FD_BASE}/_svc/mp-food-api-mpnoon/order/homepage/static`,  { headers: S() }),
    session.fetch('food_carousel1', `${FD_BASE}/_svc/mp-food-api-catalog/api/search?queryEntity%5BentityType%5D=outlet_carousel_tab&queryEntity%5BentityCode%5D=recommended_new_user_carousel_tab&f%5Bserviceable%5D=1&withContent=False&limit=20&content=home&type=outlet`, { headers: S() }),
    session.fetch('food_banners',   `${FD_BASE}/_svc/mp-food-api-catalog/api/admon/content-ad/banner-carousel?entity_type=food_banner_module&entity_code=homepage_hero_banner_ads_carousel_ae&limit=20&min_assets=2`, { headers: S() }),
    session.fetch('food_carousel2', `${FD_BASE}/_svc/mp-food-api-catalog/api/search?queryEntity%5BentityType%5D=outlet_carousel_tab&queryEntity%5BentityCode%5D=homepage_carousel_featured_tab&f%5Btags%5D=listing_ad&f%5Bserviceable%5D=1&withContent=False&limit=20&content=home&type=outlet`, { headers: S() }),
  ]);

  await sleep(t.post_carousel || 240);
  await Promise.allSettled([
    session.fetch('food_content', `${FD_BASE}/_svc/mp-food-api-catalog/api/search?content=noon_food_garnierkiwi&get_fallback=false`, { headers: S() }),
    session.fetch('food_reels',   `${FD_BASE}/_svc/mp-food-api-catalog/api/reels`, { headers: S() }),
  ]);
}

async function foodBrowse(session, lat, lng, t) {
  await sleep(t.pre_browse || 1200);
  const H = () => headersFood(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.locale, session.minimalHeaders);
  const S = () => headersSearch(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.whoHeaders, session.locale, session.minimalHeaders);

  await Promise.allSettled([
    session.fetch('browse_whoami',      WHOAMI_URL, { headers: { ...baseHeaders(session.deviceId, session.visitorId, session.minimalHeaders), 'x-experience': 'ecom', Cookie: cookieStr(session.jar) } }),
    session.fetch('browse_food_whoami', `${FD_BASE}/_svc/mp-food-api-catalog/api/customer/whoami`, { headers: H() }),
    session.fetch('browse_hp_dyn',      `${FD_BASE}/_svc/mp-food-api-mpnoon/order/homepage/dynamic`, { headers: S() }),
  ]);

  await sleep(t.post_browse_who || 3400);
  await session.fetch('browse_prefs_opt', `${FD_BASE}/_svc/mp-food-api-catalog/api/personalization/preferences/get-options`, {
    method: 'POST', headers: { ...S(), 'Content-Type': 'application/json' }, body: '{}',
  }).catch(() => {});
  await sleep(t.post_prefs_opt || 300);
  await session.fetch('browse_prefs_init', `${FD_BASE}/_svc/mp-food-api-catalog/api/personalization/preferences/init`, {
    method: 'POST', headers: { ...S(), 'Content-Type': 'application/json' }, body: '{}',
  }).catch(() => {});

  await sleep(t.post_prefs_init || 3000);
  const browseHome2P = session.fetch('browse_home2', `${FD_BASE}/_svc/mp-food-api-catalog/api/`, { headers: H() });
  // Second capture overwrites the first — matches the real-trace behaviour where
  // the token used on page 1 came from the phase-3 refresh, not the phase-2 call.
  browseHome2P.then((r) => captureInitialToken(session, r, 'browse_home2')).catch(() => {});
  await Promise.allSettled([
    session.fetch('browse_light', `${FD_BASE}/_svc/mp-food-api-catalog/api/light/`, { headers: S() }),
    browseHome2P,
  ]);
  await sleep(t.post_light || 450);
  await Promise.allSettled([
    session.fetch('browse_hp_dyn2',    `${FD_BASE}/_svc/mp-food-api-mpnoon/order/homepage/dynamic`, { headers: S() }),
    session.fetch('browse_carousel3',  `${FD_BASE}/_svc/mp-food-api-catalog/api/search?queryEntity%5BentityType%5D=outlet_carousel_tab&queryEntity%5BentityCode%5D=recommended_new_user_carousel_tab&f%5Bserviceable%5D=1&withContent=False&limit=20&content=home&type=outlet`, { headers: S() }),
    session.fetch('browse_reels2',     `${FD_BASE}/_svc/mp-food-api-catalog/api/reels`, { headers: S() }),
  ]);
}

// ---------- Search pagination ----------------------------------------------

async function fetchPage(session, lat, lng, searchToken, bodyOverrides) {
  const body = Object.assign({
    queryEntity: null, q: null, f: {}, contexts: [], withContent: true,
    excludeOutletCodes: [], limit: PAGE_LIMIT, scopes: null, page: 1, dishTags: [],
    ratingMin: null, sort: { by: 'popularity', dir: 'desc', slab: null },
    widgets: null, content: null, withHits: false, filteredByWidget: null,
    internalFilteredByWidget: 'homepage_navpill_filters_widget',
    getFallback: true, type: 'outlet', qType: 'search_query',
    minCount: 0, groupDishes: false, isSsc: false,
    hideSearchBar: false, hideSearchOptions: false,
    isOutletAdProcessingEnabled: true, withDishFetchQuery: null,
  }, bodyOverrides || {});
  if (searchToken) body.searchToken = searchToken;

  const resp = await session.fetch('search', SEARCH_URL, {
    method: 'POST',
    headers: headersSearch(session.deviceId, session.visitorId, lat, lng, cookieStr(session.jar), session.whoHeaders, session.locale, session.minimalHeaders),
    body: JSON.stringify(body),
  });
  if (resp.status === 404) return { outlets: [], token: null };
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

async function paginate(session, lat, lng, spec) {
  const pageCap   = spec.search.page_cap   || 60;
  const timeCapMs = spec.search.time_cap_ms || 540000;
  const gaps      = spec.search.page_gap_ms || [];
  const bodyOv    = spec.search.body_overrides || {};

  const seen = new Set();
  const pageMs = [];
  const t0 = Date.now();
  // Start with the token captured during mint (matches real-app behaviour where
  // page 1 carries a session searchToken, not an empty one). If capture failed,
  // falls back to null — page 1 will go without a token, as before.
  let searchToken = session.initialSearchToken || null;
  if (searchToken) log(`page 1 using initialSearchToken (${searchToken.length} chars)`);
  else             log('page 1 WITHOUT initialSearchToken (capture failed during mint)');
  let page = 0;
  let stopReason = null;
  let fail = null;

  while (page < pageCap) {
    page++;
    const p0 = Date.now();
    let r;
    try {
      r = await fetchPage(session, lat, lng, searchToken, bodyOv);
    } catch (e) {
      fail = e;
      break;
    }
    pageMs.push(Date.now() - p0);
    const fresh = r.outlets.filter(o => !seen.has(o.outletCode));
    for (const o of fresh) seen.add(o.outletCode);
    if (r.outlets.length === 0) { stopReason = `empty@${page}`; break; }
    if (fresh.length === 0)      { stopReason = `no-new@${page}`; break; }
    if (!r.token)                { stopReason = `no-token@${page}`; break; }
    searchToken = r.token;
    if (Date.now() - t0 > timeCapMs) { stopReason = `time-cap@${page}`; break; }

    const gap = gaps[page - 1] != null ? gaps[page - 1] : 750;
    await sleep(gap);
  }

  if (!stopReason && !fail) stopReason = `cap(${pageCap})`;
  return { page, pageMs, seen, stopReason, fail };
}

module.exports = {
  Session,
  mint,
  paginate,
  sleep,
  log,
  cookieStr,
};
