#!/usr/bin/env node
// noon/uae/ranking-scrape.js — PRODUCTION v2
//
// Hourly ranking worker. Dispatched by GitHub Actions via workflow_dispatch,
// one dispatch = one machine = one GitHub-runner IP. Claims up to TWO
// areas from the noon_ranking_run_area queue, walks the full /search
// listing for each area recording every outlet's ranking position, then
// saves the data via noon_ranking_save_area_hour.
//
// This version MERGES two codebases:
//   * The current production ranking-scrape.js (37-column row builder, promo
//     classifier, non-food cuisine filter, Supabase RPC lifecycle). The
//     downstream contract — what gets written to noon_ranking_analysis and
//     what gets queued into noon_restaurant_list_queue — is UNCHANGED.
//   * The v3 lab probe's resilience features: device_id swap on /search 403
//     recovery (up to 3 retries per page) and a one-shot token-age remint
//     when the 300s nguestv2 TTL approaches. These lifted lab success from
//     24% to 90% at noon peak hour against JLT.
//
// What changed vs the previous ranking-scrape.js
//   * search() is wrapped in a retry loop. On 403 we rotate x-device-id
//     (device_id was proven swappable mid-session without invalidating
//     the nguestv2 cookie) and retry the exact same /search call up to
//     RECOVERY_ATTEMPTS times. 429 drops through to the old "stop
//     machine" path because it's a legitimate rate signal from Noon,
//     not an Akamai score.
//   * Each area's token has a 300s TTL. If a session is still active at
//     ~240s we do a dual remint (st-whoami + fd/customer/whoami) which
//     refreshes the cookie without touching device/visitor. Rare in
//     practice — most areas finish well under 240s — but it prevents
//     expiry on the biggest areas (Al Safa, Jumeirah 3 at ~55 pages).
//   * Noon's 4th mint call (api-app-fd/customer/whoami) is now included
//     before pagination. This is what the real iPhone app does and v3
//     measurement showed it lifts the first-page pass rate.
//
// Noon-specific rules preserved from v1:
//   * Max 2 areas per machine, then exit (fresh IP = fresh machine).
//   * If area 1 >= 50 pages, don't claim area 2 (IP budget near spent).
//   * Non-restaurant outlets dropped at parse (noon_cuisines is_food=false).
//   * 429 from Noon stops the machine immediately — in-flight area goes
//     back to the queue for retry.
//
// Auth / mint: Noon's 4-step guest token minted locally on this runner's IP.
//   1. GET  whoami (ecom)         -> nguestv2 cookie, device/visitor ids
//   2. POST set-location          -> resolve lat/lng to zone
//   3. GET  whoami (food)         -> zone headers for food API (st host)
//   4. GET  customer/whoami (fd)  -> warms the food catalog session (fd host)
//
// Pagination: /search ignores `page`; chaining `searchToken` is the cursor.
// Each page returns up to 60 outlets. Walk until empty / no new outlets /
// no token / max_pages.
//
// Env vars (required):
//   SUPABASE_URL
//   SUPABASE_SERVICE_ROLE_KEY
//   RUN_ID                 Supabase ranking run id
//   MACHINE_NO             machine number in the run
// Env vars (set by workflow):
//   GITHUB_RUN_ID          GitHub Actions run id
//   WALL_LIMIT_SEC         wall-clock safety cap (default 4200 = 70 min)
//   RECOVERY_ATTEMPTS      max device_id swap retries per /search block (default 3)
//   TOKEN_REFRESH_AT_MS    threshold for mid-session remint (default 240000)

'use strict';

const crypto = require('node:crypto');

// -- Env --------------------------------------------------------------------

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}
const RUN_ID = Number(process.env.RUN_ID);
const MACHINE = Number(process.env.MACHINE_NO);
const GH_RUN_ID = process.env.GITHUB_RUN_ID || '';
if (!RUN_ID || !MACHINE) {
  console.error('Missing or invalid RUN_ID / MACHINE_NO');
  process.exit(1);
}
const WALL_LIMIT_MS = (Number(process.env.WALL_LIMIT_SEC) || 4200) * 1000;
const START_TIME = Date.now();

// v3 resilience knobs (safe defaults; override via workflow env for tuning)
const RECOVERY_ATTEMPTS = Number(process.env.RECOVERY_ATTEMPTS || 3);
const TOKEN_REFRESH_AT_MS = Number(process.env.TOKEN_REFRESH_AT_MS || 240000);
const RECOVERY_BACKOFF_MS = Number(process.env.RECOVERY_BACKOFF_MS || 1500);

// -- Audit log (403 pattern analysis). Never blocks or stops a scrape. ------

let NOON_REQ = 0; // Noon requests made from this machine's IP
let RUNNER_IP = null; // public IP of this GitHub runner
// HTTP/2 (Node's built-in fetch)
const rawFetch = globalThis.fetch.bind(globalThis);
globalThis.fetch = (u, o) => {
  if (String(u).includes('noon.com')) NOON_REQ++;
  return rawFetch(u, o);
};
const AREA_CTX = { page: 0 };

function failStep(msg) {
  const m = String(msg || '');
  if (m.startsWith('mint whoami-food')) return 'mint_whoami_food';
  if (m.startsWith('mint whoami')) return 'mint_whoami';
  if (m.startsWith('mint set-location')) return 'mint_set_location';
  if (m.startsWith('mint fd-whoami')) return 'mint_fd_whoami';
  if (m.startsWith('locate set-loc')) return 'locate_set_location';
  if (m.startsWith('locate whoami')) return 'locate_whoami';
  if (m.startsWith('search')) return 'search';
  if (m.startsWith('remint')) return 'remint';
  return 'other';
}

async function audit(row) {
  try {
    const r = await rawFetch(`${SUPABASE_URL}/rest/v1/noon_scrape_audit`, {
      method: 'POST',
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        Prefer: 'return=minimal',
      },
      body: JSON.stringify({
        run_id: RUN_ID, machine_no: MACHINE, github_run_id: GH_RUN_ID,
        runner_ip: RUNNER_IP,
        machine_started_at: new Date(START_TIME).toISOString(),
        machine_age_s: Math.round((Date.now() - START_TIME) / 1000),
        areas_cap: MAX_AREAS_PER_RUN,
        ...row,
      }),
      signal: AbortSignal.timeout(5000),
    });
    if (!r.ok) console.log(`[m${MACHINE}] audit log HTTP ${r.status}`);
  } catch (e) {
    console.log(`[m${MACHINE}] audit log failed: ${e.message}`);
  }
}

// -- Constants --------------------------------------------------------------

const UA = 'noon/22098 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const BUILD = '22098';

const WHOAMI_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami';
const SET_LOCATION_URL = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng';
const WHOAMI_FOOD_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1';
const FD_WHOAMI_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/customer/whoami';
const SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const PAGE_LIMIT = 60; // Noon's own page size
const PAGE_SAFETY_CAP = 500; // runaway guard
const MAX_AREAS_PER_RUN = 2; // Noon rule: hard cap, fresh IP for more
const EARLY_EXIT_PAGES = 50; // if area 1 >= this, don't claim area 2

// U+E001 is Noon's custom dirham glyph -> normalise to "AED"
const DIRHAM_RE = //g;
const cleanText = (s) => (typeof s === 'string' ? s.replace(DIRHAM_RE, 'AED').trim() || null : null);

// -- Supabase RPC helper ----------------------------------------------------

async function rpc(name, params) {
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(params),
      });
      const text = await resp.text();
      if (!resp.ok) throw new Error(`rpc(${name}) ${resp.status}: ${text.slice(0, 500)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      if (attempt >= 3) throw e;
      console.log(`[m${MACHINE}] rpc(${name}) attempt ${attempt} failed: ${e.message} - retrying`);
      await sleep(2_000 * attempt);
    }
  }
}

// -- Cookie helpers ---------------------------------------------------------

function mergeSetCookies(jar, resp) {
  const list = typeof resp.headers.getSetCookie === 'function'
    ? resp.headers.getSetCookie() : [];
  for (const c of list) {
    const m = /^([^=]+)=([^;]+)/.exec(c);
    if (m) jar[m[1]] = m[2];
  }
}
const cookieStr = (jar) =>
  Object.entries(jar).map(([k, v]) => `${k}=${v}`).join('; ');

// -- Session (mutable identity to support device_id swap recovery) ----------

// session.deviceId is MUTABLE — v3 lab proved Akamai does not invalidate the
// nguestv2 cookie when x-device-id changes mid-session, so we can swap it to
// a fresh UUID after a /search 403 and the retry succeeds roughly 90% of
// the time at peak hour. visitorId is kept stable per area (we never found
// a case where swapping it was needed and the real app never does so).
let session = null;
let tokenNumber = 0;
let identityRotations = 0;
let recoveriesTotal = 0;

/**
 * 4-step Noon guest auth. One token lives ~300s. Called once per area at
 * area start. mint_fd_whoami was added in v2 (the real iPhone app always
 * fires this; v1 was missing it and v3 measurement showed adding it bumps
 * the first-page pass rate).
 */
async function mintToken(lat, lng) {
  const deviceId = crypto.randomUUID();
  const visitorId = crypto.randomUUID();
  const jar = {};
  const base = () => ({
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD,
    'x-mp': 'noon', 'x-content': 'mobile', Accept: 'application/json',
    'x-device-id': session ? session.deviceId : deviceId,
    'x-visitor-id': session ? session.visitorId : visitorId,
  });

  // Step 1 — whoami (ecom)
  const r1 = await fetch(WHOAMI_URL, {
    headers: { ...base(), 'x-experience': 'ecom',
      'x-whoami-request-id': crypto.randomUUID() },
  });
  if (r1.status === 403) throw Object.assign(new Error('mint whoami 403'), { forbidden: true });
  if (!r1.ok) throw new Error(`mint whoami HTTP ${r1.status}`);
  mergeSetCookies(jar, r1);

  // Step 2 — set-location
  const r2 = await fetch(SET_LOCATION_URL, {
    method: 'POST',
    headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae',
      'x-device-lat': String(lat), 'x-device-lng': String(lng),
      'Content-Type': 'application/json', Cookie: cookieStr(jar) },
    body: JSON.stringify({ location: { lat, lng }, lang: 'en' }),
  });
  if (r2.status === 403) throw Object.assign(new Error('mint set-location 403'), { forbidden: true });
  if (!r2.ok) throw new Error(`mint set-location HTTP ${r2.status}`);
  mergeSetCookies(jar, r2);

  // Step 3 — whoami (food) on the st host
  const r3 = await fetch(WHOAMI_FOOD_URL, {
    headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae',
      'x-device-lat': String(lat), 'x-device-lng': String(lng),
      'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(jar) },
  });
  if (r3.status === 403) throw Object.assign(new Error('mint whoami-food 403'), { forbidden: true });
  if (!r3.ok) throw new Error(`mint whoami-food HTTP ${r3.status}`);
  const who = await r3.json().catch(() => ({}));
  mergeSetCookies(jar, r3);

  if (!jar.nguestv2) throw new Error('mint produced no nguestv2');

  const zones = who && who.headers && typeof who.headers === 'object' ? who.headers : {};

  // Step 4 (v2 new) — customer/whoami on fd host, warms food catalog session
  // Replays zone headers so Noon knows this session is bound to the resolved zone.
  const fdHeaders = {
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD, 'x-mp': 'noon',
    'x-content': 'mobile', 'x-locale': 'en-ae', Accept: 'application/json',
    'x-device-id': deviceId, 'x-visitor-id': visitorId,
    'x-mp-country': 'ae', 'x-experience': 'food',
    'x-device-lat': String(lat), 'x-device-lng': String(lng),
    'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)),
    'x-border-enabled': 'true', 'x-rocket-enabled': 'true',
    Cookie: cookieStr(jar),
  };
  for (const [k, v] of Object.entries(zones)) {
    if (v != null && typeof v !== 'object' && !k.toLowerCase().includes('ab-test')) fdHeaders[k] = String(v);
  }
  const r4 = await fetch(FD_WHOAMI_URL, { headers: fdHeaders });
  if (r4.status === 403) throw Object.assign(new Error('mint fd-whoami 403'), { forbidden: true });
  if (!r4.ok) throw new Error(`mint fd-whoami HTTP ${r4.status}`);
  mergeSetCookies(jar, r4);
  // Pull latest zone headers (fd/whoami can overwrite with fresher values)
  try {
    const fdResp = await r4.json();
    if (fdResp && fdResp.headers && typeof fdResp.headers === 'object') {
      for (const [k, v] of Object.entries(fdResp.headers)) {
        if (v != null && typeof v !== 'object') zones[k] = v;
      }
    }
  } catch (_) {}

  tokenNumber++;
  session = { deviceId, visitorId, jar, zones, mintedAt: Date.now() };
  console.log(`[m${MACHINE}] token #${tokenNumber} | zone=${zones['x-food-zonecode'] || '-'}`);
  return session;
}

/**
 * Mid-session token refresh. Fired automatically when the session is still
 * going at ~240s (the 300s nguestv2 TTL is 60s away). Fires the dual whoami
 * pair (st + fd), updating the cookie jar in place. deviceId and visitorId
 * are NOT changed — this is a token refresh, not an identity rotation.
 */
async function dualRemint(lat, lng) {
  if (!session) throw new Error('remint: no session');
  const base = () => ({
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD, 'x-mp': 'noon',
    'x-content': 'mobile', Accept: 'application/json',
    'x-device-id': session.deviceId, 'x-visitor-id': session.visitorId,
  });

  const r1 = await fetch(WHOAMI_URL, {
    headers: { ...base(), 'x-experience': 'ecom',
      'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(session.jar) },
  });
  if (r1.status === 403) throw Object.assign(new Error('remint whoami 403'), { forbidden: true });
  if (!r1.ok) throw new Error(`remint whoami HTTP ${r1.status}`);
  mergeSetCookies(session.jar, r1);

  await sleep(200 + Math.random() * 300);

  const fdHeaders = {
    ...base(), 'x-locale': 'en-ae', 'x-mp-country': 'ae', 'x-experience': 'food',
    'x-device-lat': String(lat), 'x-device-lng': String(lng),
    'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)),
    'x-border-enabled': 'true', 'x-rocket-enabled': 'true',
    Cookie: cookieStr(session.jar),
  };
  for (const [k, v] of Object.entries(session.zones)) {
    if (v != null && typeof v !== 'object' && !k.toLowerCase().includes('ab-test')) fdHeaders[k] = String(v);
  }
  const r2 = await fetch(FD_WHOAMI_URL, { headers: fdHeaders });
  if (r2.status === 403) throw Object.assign(new Error('remint fd-whoami 403'), { forbidden: true });
  if (!r2.ok) throw new Error(`remint fd-whoami HTTP ${r2.status}`);
  mergeSetCookies(session.jar, r2);

  session.mintedAt = Date.now();
  console.log(`[m${MACHINE}] token #${tokenNumber} refreshed (dual remint)`);
}

// -- Noon API calls ---------------------------------------------------------

function hdrs(lat, lng) {
  return {
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD,
    'x-mp': 'noon', 'x-content': 'mobile', 'x-locale': 'en-ae',
    'x-device-id': session.deviceId, 'x-visitor-id': session.visitorId,
    'x-device-lat': String(lat), 'x-device-lng': String(lng),
    Accept: 'application/json',
  };
}

async function locate(lat, lng) {
  const jar = { ...(session.jar || {}) };
  const h = { ...hdrs(lat, lng), 'x-experience': 'ecom' };

  const r1 = await fetch(SET_LOCATION_URL, {
    method: 'POST',
    headers: { ...h, 'Content-Type': 'application/json', Cookie: cookieStr(jar) },
    body: JSON.stringify({ location: { lat, lng }, lang: 'en' }),
  });
  if (r1.status === 429) throw Object.assign(new Error('locate set-loc 429'), { rateLimited: true });
  if (r1.status === 403) throw Object.assign(new Error('locate set-loc 403'), { forbidden: true });
  if (!r1.ok) throw new Error(`locate set-loc HTTP ${r1.status}`);
  mergeSetCookies(jar, r1);

  const r2 = await fetch(WHOAMI_FOOD_URL, {
    headers: { ...h, 'x-whoami-request-id': crypto.randomUUID(), Cookie: cookieStr(jar) },
  });
  if (r2.status === 429) throw Object.assign(new Error('locate whoami 429'), { rateLimited: true });
  if (r2.status === 403) throw Object.assign(new Error('locate whoami 403'), { forbidden: true });
  if (!r2.ok) throw new Error(`locate whoami HTTP ${r2.status}`);
  const who = await r2.json().catch(() => ({}));
  mergeSetCookies(jar, r2);

  const zh = who && who.headers && typeof who.headers === 'object' ? who.headers : {};
  return { lat, lng, zh, cookie: cookieStr(jar), jarRef: jar };
}

/**
 * Fire /search once. Returns {outlets, token, status}.
 * On non-200 responses, status carries the HTTP code and outlets is [].
 * This is the raw call; the retry/recovery loop sits in fetchPage().
 */
function searchHeadersFor(loc) {
  const { lat, lng } = loc;
  const headers = {
    ...hdrs(lat, lng),
    'x-mp-country': 'ae', 'x-experience': 'food',
    'x-lat': String(Math.round(lat * 1e7)),
    'x-lng': String(Math.round(lng * 1e7)),
    'x-border-enabled': 'true', 'x-rocket-enabled': 'true',
    'x-whoami-request-id': crypto.randomUUID(),
    Cookie: loc.cookie, 'Content-Type': 'application/json',
  };
  for (const [k, v] of Object.entries(loc.zh)) {
    if (v != null && typeof v !== 'object' && !k.toLowerCase().includes('ab-test'))
      headers[k] = String(v);
  }
  return headers;
}

async function fetchPageOnce(loc, searchToken) {
  const headers = searchHeadersFor(loc);
  const body = {
    queryEntity: null, q: null, f: {}, contexts: [], withContent: true,
    excludeOutletCodes: [], limit: PAGE_LIMIT, page: 1,
    sort: { by: 'popularity', dir: 'desc' },
    type: 'outlet', qType: 'search_query', getFallback: true,
  };
  if (searchToken) body.searchToken = searchToken;

  const resp = await fetch(SEARCH_URL, {
    method: 'POST', headers, body: JSON.stringify(body),
  });
  if (resp.status === 429) {
    return { outlets: [], token: null, status: 429 };
  }
  if (resp.status === 403) {
    return { outlets: [], token: null, status: 403 };
  }
  if (resp.status === 404) return { outlets: [], token: null, status: 404 };
  if (!resp.ok) throw new Error(`search HTTP ${resp.status}`);

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

  return { outlets, token: json.searchToken || null, status: 200 };
}

/**
 * Fetch /search with v3-style device_id swap recovery on 403.
 *   - 200: return result normally.
 *   - 429: hard fail — throw rateLimited (Noon's own rate signal, respect it).
 *   - 403: rotate x-device-id to a fresh UUID (same cookie, same visitor_id)
 *     and retry up to RECOVERY_ATTEMPTS times. Each retry has a 1.5s backoff.
 *   - any other error: throw up to the caller (treated as fatal per Noon rules).
 *
 * This is the single most important change from v1. The /search endpoint
 * is where Akamai scores and we lose most machines. Rotating device_id is
 * cheap, the cookie remains valid, and the recovered /search succeeds the
 * overwhelming majority of the time at both peak and off-peak hours.
 */
async function fetchPage(loc, searchToken) {
  for (let attempt = 0; attempt <= RECOVERY_ATTEMPTS; attempt++) {
    if (attempt > 0) {
      // Rotate the device header. loc.cookie was captured by locate() but
      // base() in hdrs() reads session.deviceId live, so this change
      // propagates to the next searchHeadersFor() call.
      const prev = session.deviceId;
      session.deviceId = crypto.randomUUID();
      identityRotations++;
      recoveriesTotal++;
      console.log(`[m${MACHINE}] search 403 — device_id ${prev.slice(0, 8)}... -> ${session.deviceId.slice(0, 8)}... (recovery ${attempt}/${RECOVERY_ATTEMPTS})`);
      await sleep(RECOVERY_BACKOFF_MS + Math.random() * 500);
    }
    const r = await fetchPageOnce(loc, searchToken);
    if (r.status === 200) return { outlets: r.outlets, token: r.token };
    if (r.status === 429) throw Object.assign(new Error('search 429'), { rateLimited: true });
    if (r.status === 404) return { outlets: [], token: null };
    if (r.status === 403) {
      if (attempt >= RECOVERY_ATTEMPTS) {
        throw Object.assign(new Error('search 403'), { forbidden: true });
      }
      continue; // retry with new device_id
    }
    throw new Error(`search HTTP ${r.status}`);
  }
  throw Object.assign(new Error('search 403'), { forbidden: true });
}

// -- Non-restaurant filter --------------------------------------------------

/**
 * Returns true if the outlet is a restaurant (at least one cuisine marked
 * is_food=true in noon_cuisines). Drops the outlet if ALL cuisines are
 * non-food (Flowers, Pharmacy, Butcher Shop, etc).
 */
function isRestaurant(outlet, nonFoodSet) {
  const cuisines = Array.isArray(outlet.cuisines) ? outlet.cuisines : [];
  if (cuisines.length === 0) return true; // unknown -> keep (register will decide)
  for (const c of cuisines) if (!nonFoodSet.has(c)) return true;
  return false;
}

// -- New-schema helpers (Noon changed /search response shape on 2026-10-14) --

const CDN_PREFIX = 'https://f.nooncdn.com/food_production/';
const cdnUrl = (key) =>
  (typeof key === 'string' && key.trim()) ? CDN_PREFIX + key.trim() + '?width=720&format=webp' : null;

function deriveRatingStatus(title) {
  if (title == null || String(title).trim() === '') return 'not_rated';
  if (/^new$/i.test(String(title).trim())) return 'new';
  return 'rated';
}

/**
 * Classify a promo text string into noon_promo_scope enum values and extract
 * numeric percent and cap when present. Any one of the three card tag fields
 * (tag / extendedTag / miniTag) can match. extendedTag often carries the
 * richest form (with "up to X" caps or "select items" scope).
 */
function classifyPromo(tag, extendedTag, miniTag) {
  const texts = [extendedTag, tag, miniTag].filter(
    (t) => typeof t === 'string' && t.trim()
  );
  if (texts.length === 0) return { scope: null, percent: null, cap: null };

  const joined = texts.join(' | ');

  if (/\bfree\s*delivery\b/i.test(joined)) {
    return { scope: 'free_delivery', percent: null, cap: null };
  }

  const percentMatch = joined.match(/(\d{1,3})\s*%/);
  const percent = percentMatch ? Math.min(100, parseInt(percentMatch[1], 10)) : null;

  const capMatch = joined.match(/up\s*to\s*[^0-9]*([\d.]+)/i);
  const cap = capMatch ? parseFloat(capMatch[1]) : null;

  if (/bestseller/i.test(joined)) return { scope: 'percent_off_bestsellers', percent, cap };
  if (/select\s*items?/i.test(joined)) return { scope: 'percent_off_select_items', percent, cap };
  if (/full\s*menu/i.test(joined)) return { scope: 'percent_off_full_menu', percent, cap };
  if (/\babove\b/i.test(joined)) {
    const aboveMatch = joined.match(/above\s*[^0-9]*([\d.]+)/i);
    const threshold = aboveMatch ? parseFloat(aboveMatch[1]) : null;
    return { scope: 'percent_off_above_min', percent, cap: threshold };
  }
  if (/\bflat\b/i.test(joined)) return { scope: 'flat_percent_off', percent, cap: null };
  if (/up\s*to/i.test(joined)) return { scope: 'percent_off_capped', percent, cap };
  if (percent != null) return { scope: 'percent_off', percent, cap: null };
  if (/^\s*offers?\s*$/i.test(joined)) return { scope: 'offers_generic', percent: null, cap: null };

  return { scope: 'offers_generic', percent: null, cap: null };
}

// -- Card -> ranking row ----------------------------------------------------

function parseCards(outlets, nonFoodSet) {
  const seen = new Set();
  const rows = [];
  let dropped = 0;

  for (let i = 0; i < outlets.length; i++) {
    const o = outlets[i];
    const code = o.outletCode;
    if (!code || seen.has(code)) continue;
    if (!isRestaurant(o, nonFoodSet)) { dropped++; continue; }
    seen.add(code);

    const rank = rows.length + 1;

    const rt = o.rating || {};
    const rating = rt.score != null ? rt.score : null;
    const ratingCount = rt.count != null ? rt.count : null;
    const ratingTitle = rt.title || null;
    const ratingDisplayCnt = rt.displayCount || null;
    const ratingStatus = deriveRatingStatus(ratingTitle);

    const tag = typeof o.tag === 'string' ? o.tag : null;
    const miniTag = typeof o.miniTag === 'string' ? o.miniTag : null;
    const extendedTag = typeof o.extendedTag === 'string' ? o.extendedTag : null;
    const hasPromo = !!(tag || miniTag || extendedTag);
    const promoText = cleanText(tag) || cleanText(extendedTag) || cleanText(miniTag);

    const classified = classifyPromo(tag, extendedTag, miniTag);
    const isFreeDeliv = classified.scope === 'free_delivery';
    const isNonFreeDel = hasPromo && !isFreeDeliv;

    const d = o.delivery || {};
    const bannerKey = (o.media && typeof o.media.heroImage === 'string') ? o.media.heroImage : null;
    const logoKey = (o.media && typeof o.media.logoImage === 'string') ? o.media.logoImage : null;
    const bannerUrl = cdnUrl(bannerKey);
    const logoUrl = cdnUrl(logoKey);

    rows.push([
      code,                                               // [0] noon_branch_id
      rank,                                               // [1] noon_listing_rank
      o.operatingStatusCode || null,                      // [2] noon_operating_status
      o.operatingType || null,                            // [3] noon_operating_type
      rating,                                             // [4] noon_rating
      ratingCount,                                        // [5] noon_rating_count
      ratingStatus,                                       // [6] noon_rating_status
      hasPromo,                                           // [7] noon_has_promo
      promoText,                                          // [8] noon_promo_text
      classified.scope,                                   // [9] noon_promo_scope
      classified.cap,                                     // [10] noon_promo_cap
      o.widgetGroup || null,                              // [11] noon_widget_group
      !!(o.isPersonalized),                               // [12] noon_is_personalized
      false,                                              // [13] noon_is_sponsored (deprecated)
      null,                                               // [14] noon_ad_slot (deprecated)
      ratingDisplayCnt,                                   // [15] noon_rating_display_count
      cleanText(miniTag),                                 // [16] noon_promo_mini_tag
      cleanText(extendedTag),                             // [17] noon_promo_extended_tag
      isFreeDeliv,                                        // [18] noon_promo_is_free_delivery
      isNonFreeDel,                                       // [19] noon_promo_is_non_free_delivery
      !!(o.isNearAndQuick),                               // [20] noon_is_near_and_quick
      o.name || null,                                     // [21] noon_card_name
      o.linkUrl || null,                                  // [22] noon_card_url
      bannerUrl,                                          // [23] noon_card_banner_url
      Array.isArray(o.cuisines) ? o.cuisines : [],        // [24] cuisines
      o.restaurantCode || null,                           // [25] noon_restaurant_code
      typeof o.price === 'string' ? o.price : null,       // [26] noon_card_price_tier
      typeof o.distance === 'number' ? o.distance : null, // [27] noon_card_distance_m
      ratingTitle,                                        // [28] noon_card_rating_title
      typeof d.fee === 'number' ? d.fee : null,           // [29] delivery_fee
      typeof d.originalFee === 'number' ? d.originalFee : null, // [30] delivery_original_fee
      typeof d.minOrder === 'number' ? d.minOrder : null, // [31] delivery_min_order
      typeof d.time === 'string' ? d.time : null,         // [32] delivery_time
      classified.percent,                                 // [33] noon_promo_percent
      logoUrl,                                            // [34] brand logo URL
      typeof o.isVip === 'boolean' ? o.isVip : null,      // [35] noon_is_vip
      o.vipMsg || null,                                   // [36] noon_vip_msg
    ]);
  }
  return { rows, dropped };
}

// -- Process one area -------------------------------------------------------

async function processArea(areaId, lat, lng, pageDelayMs, maxPages, nonFoodSet) {
  const t0 = Date.now();
  AREA_CTX.page = 0;
  session = null;

  // One mint per area. The dualRemint() helper refreshes the token in-place
  // if the session is still active at ~240s (see pagination loop below).
  await mintToken(lat, lng);

  // Locate: set-location + whoami-food
  const loc = await locate(lat, lng);

  const allOutlets = [];
  const seen = new Set();
  let searchToken = null;
  let page = 0;
  let stopReason = null;
  const cap = maxPages || PAGE_SAFETY_CAP;

  while (page < cap) {
    // Token-age remint: refresh when nguestv2 is approaching its 300s TTL.
    // This is rare in practice (most areas finish well under 240s) but
    // prevents expiry on the biggest areas (Al Safa ~55 pages).
    const tokenAge = Date.now() - session.mintedAt;
    if (tokenAge > TOKEN_REFRESH_AT_MS) {
      try {
        await dualRemint(lat, lng);
      } catch (e) {
        // If remint fails on 403, treat as a session block and let the
        // outer handler stop the machine (preserves original Noon rule).
        if (e.forbidden || e.rateLimited) throw e;
        console.log(`[m${MACHINE}] remint non-fatal error: ${e.message} — continuing on old token`);
      }
    }

    page++;
    AREA_CTX.page = page;
    const pt0 = Date.now();
    const r = await fetchPage(loc, searchToken);
    const pageMs = Date.now() - pt0;

    const fresh = r.outlets.filter((o) => !seen.has(o.outletCode));
    for (const o of fresh) { seen.add(o.outletCode); allOutlets.push(o); }

    console.log(
      `[m${MACHINE}] ${areaId} p${page}: ${r.outlets.length} outlets ` +
      `(${fresh.length} new, ${allOutlets.length} total) ${pageMs}ms` +
      `${r.token ? '' : ' no-token'}`
    );

    if (r.outlets.length === 0) { stopReason = `empty@${page}`; break; }
    if (fresh.length === 0) { stopReason = `no-new@${page}`; break; }
    if (!r.token) { stopReason = `no-token@${page}`; break; }

    searchToken = r.token;
    if (page < cap) await sleep(pageDelayMs);
  }
  if (!stopReason) stopReason = `max-pages(${cap})`;

  const parsed = parseCards(allOutlets, nonFoodSet);
  const elapsed = Date.now() - t0;
  const requests = page + 3; // search pages + locate(set-location + whoami-food) + fd/whoami in mint
  const areaRotations = identityRotations;

  return {
    pages: page,
    cards: allOutlets.length,
    rows: parsed.rows,
    dropped: parsed.dropped,
    elapsed,
    requests,
    stopReason,
    rotations: areaRotations,
  };
}

// -- Main -------------------------------------------------------------------

(async () => {
  if (typeof fetch !== 'function') {
    console.error('Node 18+ required (native fetch)');
    process.exit(1);
  }

  console.log(`[m${MACHINE}] starting | run=${RUN_ID} | gh=${GH_RUN_ID} | wall=${WALL_LIMIT_MS / 1000}s | recovery=${RECOVERY_ATTEMPTS} | refresh=${TOKEN_REFRESH_AT_MS}ms`);

  const start = await rpc('noon_ranking_machine_start', {
    p_run_id: RUN_ID, p_machine_no: MACHINE, p_github_run_id: GH_RUN_ID,
  });
  console.log(`[m${MACHINE}] start -> ${JSON.stringify(start)}`);

  if (start.stop) {
    console.log(`[m${MACHINE}] told to stop at check-in`);
    process.exit(0);
  }

  const scrapeDate = start.scrape_date;
  const scrapeHour = start.scrape_hour;
  const dryRun = !!start.dry_run;
  let totalAreas = 0, totalRows = 0, totalRequests = 0, totalDropped = 0;

  console.log(`[m${MACHINE}] date=${scrapeDate} hour=${scrapeHour} dry=${dryRun}`);

  const nonFoodList = await rpc('noon_get_non_food_cuisines', {});
  const nonFoodSet = new Set(Array.isArray(nonFoodList) ? nonFoodList : []);
  console.log(`[m${MACHINE}] non-food cuisines (${nonFoodSet.size}): ${[...nonFoodSet].join(', ')}`);

  try {
    const ipr = await rawFetch('https://api.ipify.org', { signal: AbortSignal.timeout(4000) });
    RUNNER_IP = (await ipr.text()).trim() || null;
  } catch (_) {}
  console.log(`[m${MACHINE}] runner ip=${RUNNER_IP || 'unknown'}`);

  let exitReason = 'areas_complete';

  for (let areaIdx = 0; areaIdx < MAX_AREAS_PER_RUN; areaIdx++) {
    if (Date.now() - START_TIME > WALL_LIMIT_MS) {
      console.log(`[m${MACHINE}] wall limit reached (${WALL_LIMIT_MS / 1000}s)`);
      exitReason = 'wall_limit';
      break;
    }

    const claim = await rpc('noon_ranking_claim', {
      p_run_id: RUN_ID, p_machine_no: MACHINE,
    });

    if (!claim || claim.noon_area_id == null) {
      if (claim && claim.wait_ms > 0) {
        console.log(`[m${MACHINE}] wait ${claim.wait_ms}ms (pacing)`);
        await sleep(claim.wait_ms);
        areaIdx--;
        continue;
      }
      console.log(`[m${MACHINE}] no more areas`);
      exitReason = 'no_more_areas';
      break;
    }

    const areaId = claim.noon_area_id;
    const areaName = claim.area_name || String(areaId);
    const lat = claim.lat;
    const lng = claim.lng;
    const pageDel = claim.page_delay_ms || 500;
    const maxPg = claim.max_pages || PAGE_SAFETY_CAP;

    console.log(`[m${MACHINE}] claimed ${areaId} (${areaName}) [area ${areaIdx + 1}/${MAX_AREAS_PER_RUN}] | delay=${pageDel}ms max=${maxPg}`);

    const claimedAt = Date.now();
    const reqBefore = NOON_REQ;
    const rotationsBefore = identityRotations;
    const recoveriesBefore = recoveriesTotal;
    const auditBase = () => ({
      area_seq: areaIdx + 1, noon_area_id: areaId, area_name: areaName,
      claimed_at: new Date(claimedAt).toISOString(), ended_at: new Date().toISOString(),
      gap_s: claim.gap_seconds || 0, page_delay_ms: pageDel, max_pages: maxPg,
      token_no: tokenNumber, token_age_ms: session ? Date.now() - session.mintedAt : null,
      req_before: reqBefore, req_area: NOON_REQ - reqBefore,
      elapsed_ms: Date.now() - claimedAt,
    });

    if (claim.gap_seconds > 0) {
      console.log(`[m${MACHINE}] gap ${claim.gap_seconds}s`);
      await sleep(claim.gap_seconds * 1000);
    }

    let result;
    try {
      result = await processArea(areaId, lat, lng, pageDel, maxPg, nonFoodSet);
      totalRequests += result.requests;
      totalDropped += result.dropped;

      let savedAnalysis = 0, savedPending = 0;
      if (!dryRun && result.rows.length > 0) {
        const sv = await rpc('noon_ranking_save_area_hour', {
          p_area_id: areaId, p_date: scrapeDate, p_hour: scrapeHour,
          p_rows: result.rows,
        });
        savedAnalysis = (sv && sv.inserted != null) ? sv.inserted : result.rows.length;
        savedPending = (sv && sv.pending != null) ? sv.pending : 0;
        console.log(`[m${MACHINE}] saved ${areaId}: analysis=${savedAnalysis} pending=${savedPending} dropped=${result.dropped}`);
      }

      const rep = await rpc('noon_ranking_report', {
        p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: areaId,
        p_result: {
          status: 'ok',
          pages_scraped: result.pages,
          cards: result.cards,
          ranking_rows: savedAnalysis,
          pending_rows: savedPending,
          dropped: result.dropped,
          bytes: 0,
          fetch_ms: result.elapsed,
          rotations: identityRotations - rotationsBefore,
          recoveries: recoveriesTotal - recoveriesBefore,
        },
      });

      totalAreas++;
      await audit({
        ...auditBase(), outcome: 'ok', pages: result.pages, cards: result.cards,
        rows_parsed: result.rows.length, rows_saved: savedAnalysis, stop_reason: result.stopReason,
      });
      totalRows += result.rows.length;
      console.log(
        `[m${MACHINE}] ${areaId} ok: ${result.cards} cards, ${result.rows.length} rows (${result.dropped} dropped), ` +
        `${result.pages}p, ${result.elapsed}ms, ${identityRotations - rotationsBefore} rotations | ${result.stopReason} | -> ${rep.action}`,
      );

      if (rep.action === 'stop_job') {
        console.log(`[m${MACHINE}] stop: ${rep.reason}`);
        exitReason = 'stop_job';
        break;
      }

      if (areaIdx === 0 && result.pages >= EARLY_EXIT_PAGES) {
        console.log(`[m${MACHINE}] area 1 was ${result.pages}p (>= ${EARLY_EXIT_PAGES}), skipping area 2 to spare IP`);
        exitReason = 'first_area_long';
        break;
      }

      if (rep.gap_seconds > 0) await sleep(rep.gap_seconds * 1000);

    } catch (e) {
      // 429 stops immediately (Noon's own rate limit — respect it).
      // 403 that reached here means device_id swap also failed — session is burnt.
      if (e.rateLimited || e.forbidden) {
        const kind = e.forbidden ? 'blocked' : 'rate_limited';
        await audit({
          ...auditBase(), outcome: kind, pages: Math.max(0, AREA_CTX.page - 1),
          fail_step: failStep(e.message), fail_page: AREA_CTX.page || null,
          http_status: e.forbidden ? 403 : 429, error: e.message.slice(0, 300),
        });
        console.log(`[m${MACHINE}] ${kind} on ${areaId}: ${e.message} - stopping machine immediately`);

        const rep = await rpc('noon_ranking_report', {
          p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: areaId,
          p_result: { status: kind },
        });
        console.log(`[m${MACHINE}] reported ${kind}: action=${rep.action} reason=${rep.reason || '-'}`);

        exitReason = kind;
        break;
      }

      // Non-throttle error -> report and stop
      console.log(`[m${MACHINE}] error on ${areaId}: ${e.message}`);
      await audit({
        ...auditBase(), outcome: 'error', pages: Math.max(0, AREA_CTX.page - 1),
        fail_step: failStep(e.message), fail_page: AREA_CTX.page || null,
        http_status: Number((e.message.match(/HTTP (\d{3})/) || [])[1]) || null,
        error: e.message.slice(0, 300),
      });
      await rpc('noon_ranking_report', {
        p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: areaId,
        p_result: { status: 'error', error: e.message.slice(0, 500) },
      });
      exitReason = 'error';
      break;
    }
  }

  const end = await rpc('noon_ranking_machine_end', {
    p_run_id: RUN_ID, p_machine_no: MACHINE, p_reason: exitReason,
  });

  const elapsed = Math.round((Date.now() - START_TIME) / 1000);
  console.log('');
  console.log('=== SUMMARY ===');
  console.log(`  run:        ${RUN_ID}`);
  console.log(`  machine:    ${MACHINE}`);
  console.log(`  areas:      ${totalAreas}`);
  console.log(`  rows:       ${totalRows}`);
  console.log(`  dropped:    ${totalDropped} (non-restaurant)`);
  console.log(`  requests:   ${totalRequests}`);
  console.log(`  tokens:     ${tokenNumber}`);
  console.log(`  rotations:  ${identityRotations}`);
  console.log(`  recoveries: ${recoveriesTotal}`);
  console.log(`  elapsed:    ${elapsed}s`);
  console.log(`  exit:       ${exitReason}`);
  console.log(`  finished:   ${JSON.stringify(end)}`);

})().catch(async (e) => {
  console.error(`[m${MACHINE}] FATAL: ${e.message}`);
  console.error(e.stack);
  try {
    await rpc('noon_ranking_machine_end', {
      p_run_id: RUN_ID, p_machine_no: MACHINE,
      p_reason: `fatal: ${e.message.slice(0, 200)}`,
    });
  } catch (_) {}
  process.exit(1);
});
