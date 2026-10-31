#!/usr/bin/env node
// noon/uae/ranking-scrape.js — PRODUCTION (v3 network layer + v1 output)
//
// One GitHub runner = one machine = one IP = one Noon guest session.
// Dispatched by Supabase (noon_ranking_dispatch_machine) to either the
// personal or the org repo; both run THIS file from the org repo.
//
// NETWORK BEHAVIOUR — ported verbatim from the lab's tls-probe-v3.js, the
// code that ran v3 Test A (variant 'v3-recover-device', 90% full captures):
//   * Every Noon call goes through curl-impersonate (Safari TLS fingerprint),
//     never Node's fetch.
//   * v3 header set (sentry-trace/baggage, Accept-Language, etc).
//   * 4-call mint WITH etracker telemetry (launch, launch_info,
//     app_foreground, location_fetch, page_home, whoami_ab_config,
//     selected_zone), one whoami retry, human-ish gaps between calls.
//   * ONE session (device_id/visitor_id/cookie jar) for both areas. Area 2
//     only re-sets delivery location (+ location_fetch event) and reminds
//     only if the token is old — exactly like v3.
//   * Pagination at real-app pace: per-session randomised gap bounds inside
//     5.5–7.8s, page_catalog + 4–10 impression triplets after every page.
//   * Token refresh at a per-session random 210–260s (dual whoami).
//   * On /search 403/429: recovery strategies from the spec (Test A:
//     swap_device_id, up to 3 attempts, 2s backoff + jitter).
//
// ALL of that is driven by noon_ranking_settings.scrape_spec in Supabase
// (same schema as noon_lab.variant.spec). Change behaviour there, not here.
//
// OUTPUT — unchanged from the previous production scraper:
//   * 37-column rows, promo classifier, non-food cuisine filter
//     (noon_get_non_food_cuisines), noon_ranking_save_area_hour,
//     noon_ranking_report / machine_start / machine_end, noon_scrape_audit.
//   * Any failure stops the machine; the in-flight area is reported and the
//     report RPC puts it back in the queue for another machine.
//
// Env (set by the workflow):
//   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RUN_ID, MACHINE_NO, GITHUB_RUN_ID
//   WALL_LIMIT_SEC (optional), IMPERSONATE_WRAPPER (optional, default curl_safari15_3)

'use strict';

const { execFileSync } = require('node:child_process');
const crypto = require('node:crypto');
const fs = require('node:fs');

// -- Env ----------------------------------------------------------------------

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
const WALL_LIMIT_MS = (Number(process.env.WALL_LIMIT_SEC) || 4000) * 1000;
const START_TIME = Date.now();

const prefix = `[m${MACHINE}]`;
const say = (m) => console.log(`${prefix} ${m}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const randInt = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo + 1));

// -- curl-impersonate ---------------------------------------------------------
// Installed on the fly so neither workflow needs to change when this does.
// Same release the lab workflows used.

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
    const err = String(e.message || e).slice(0, 300);
    logReq({ step, st: null, ms: Date.now() - t0, err: err.slice(0, 120) });
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

// -- Spec (from Supabase noon_ranking_settings.scrape_spec) -------------------

const DEFAULT_SPEC = {
  max_areas: 2,
  early_exit_pages: 50,
  pacing: { page_gap_ms_min: 5500, page_gap_ms_max: 7800, session_randomize: true, pause_probability: 0, pause_ms_min: 3000, pause_ms_max: 7000 },
  telemetry: { impressions_per_page_min: 4, impressions_per_page_max: 10 },
  token_refresh: { enabled: true, at_ms_min: 210000, at_ms_max: 260000 },
  on_search_block: { enabled: true, strategies: ['swap_device_id'], max_attempts: 3, backoff_ms: 2000 },
  periodic_identity_rotation: { enabled: false, every_pages: null, at_pages: [], field: 'device' },
};

function mergeSpec(defaults, spec) {
  if (!spec || typeof spec !== 'object') return defaults;
  const out = { ...defaults };
  for (const k of Object.keys(spec)) {
    if (spec[k] !== null && typeof spec[k] === 'object' && !Array.isArray(spec[k])) out[k] = mergeSpec(defaults[k] || {}, spec[k]);
    else out[k] = spec[k];
  }
  return out;
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

// -- Supabase -----------------------------------------------------------------

async function rpc(name, params) {
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      });
      const text = await resp.text();
      if (!resp.ok) throw new Error(`rpc(${name}) ${resp.status}: ${text.slice(0, 500)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      if (attempt >= 3) throw e;
      say(`rpc(${name}) attempt ${attempt} failed: ${e.message} - retrying`);
      await sleep(2000 * attempt);
    }
  }
}

let RUNNER_IP = null;
let MAX_AREAS = 2;

// Heartbeat: keeps noon_ranking_run_machine.last_seen_at fresh while an area
// is being paginated at real-app pace (an area can take 5–10 min with no
// other Supabase contact). Without it the tick's silent-timeout watchdog
// marks healthy machines 'lost' and re-queues their areas. Fire-and-forget.
let lastBeat = Date.now();
function heartbeat(force = false) {
  if (!force && Date.now() - lastBeat < 60000) return;
  lastBeat = Date.now();
  fetch(`${SUPABASE_URL}/rest/v1/noon_ranking_run_machine?run_id=eq.${RUN_ID}&machine_no=eq.${MACHINE}`, {
    method: 'PATCH',
    headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=minimal' },
    body: JSON.stringify({ last_seen_at: new Date().toISOString() }),
    signal: AbortSignal.timeout(5000),
  }).then((r) => { if (!r.ok) say(`heartbeat HTTP ${r.status}`); }).catch((e) => say(`heartbeat failed: ${e.message}`));
}

async function audit(row) {
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/noon_scrape_audit`, {
      method: 'POST',
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=minimal' },
      body: JSON.stringify({
        run_id: RUN_ID, machine_no: MACHINE, github_run_id: GH_RUN_ID, runner_ip: RUNNER_IP,
        machine_started_at: new Date(START_TIME).toISOString(),
        machine_age_s: Math.round((Date.now() - START_TIME) / 1000),
        areas_cap: MAX_AREAS,
        ...row,
      }),
      signal: AbortSignal.timeout(5000),
    });
    if (!r.ok) say(`audit log HTTP ${r.status}`);
  } catch (e) { say(`audit log failed: ${e.message}`); }
}

// Normalise a failing step name for the audit table.
function failStep(step) {
  const s = String(step || 'other').replace(/_retry\d+$/, '').replace(/^remint\d+_/, 'remint_');
  return s.startsWith('search') ? 'search' : s;
}

// -- Output side (unchanged from previous production) -------------------------

const DIRHAM_RE = //g;
const cleanText = (s) => (typeof s === 'string' ? s.replace(DIRHAM_RE, 'AED').trim() || null : null);

function isRestaurant(outlet, nonFoodSet) {
  const cuisines = Array.isArray(outlet.cuisines) ? outlet.cuisines : [];
  if (cuisines.length === 0) return true;
  for (const c of cuisines) if (!nonFoodSet.has(c)) return true;
  return false;
}

const CDN_PREFIX = 'https://f.nooncdn.com/food_production/';
const cdnUrl = (key) => (typeof key === 'string' && key.trim()) ? CDN_PREFIX + key.trim() + '?width=720&format=webp' : null;

function deriveRatingStatus(title) {
  if (title == null || String(title).trim() === '') return 'not_rated';
  if (/^new$/i.test(String(title).trim())) return 'new';
  return 'rated';
}

function classifyPromo(tag, extendedTag, miniTag) {
  const texts = [extendedTag, tag, miniTag].filter((t) => typeof t === 'string' && t.trim());
  if (texts.length === 0) return { scope: null, percent: null, cap: null };
  const joined = texts.join(' | ');
  if (/\bfree\s*delivery\b/i.test(joined)) return { scope: 'free_delivery', percent: null, cap: null };
  const percentMatch = joined.match(/(\d{1,3})\s*%/);
  const percent = percentMatch ? Math.min(100, parseInt(percentMatch[1], 10)) : null;
  const capMatch = joined.match(/up\s*to\s*[^0-9]*([\d.]+)/i);
  const cap = capMatch ? parseFloat(capMatch[1]) : null;
  if (/bestseller/i.test(joined)) return { scope: 'percent_off_bestsellers', percent, cap };
  if (/select\s*items?/i.test(joined)) return { scope: 'percent_off_select_items', percent, cap };
  if (/full\s*menu/i.test(joined)) return { scope: 'percent_off_full_menu', percent, cap };
  if (/\babove\b/i.test(joined)) {
    const aboveMatch = joined.match(/above\s*[^0-9]*([\d.]+)/i);
    return { scope: 'percent_off_above_min', percent, cap: aboveMatch ? parseFloat(aboveMatch[1]) : null };
  }
  if (/\bflat\b/i.test(joined)) return { scope: 'flat_percent_off', percent, cap: null };
  if (/up\s*to/i.test(joined)) return { scope: 'percent_off_capped', percent, cap };
  if (percent != null) return { scope: 'percent_off', percent, cap: null };
  return { scope: 'offers_generic', percent: null, cap: null };
}

function parseCards(outlets, nonFoodSet) {
  const seen = new Set();
  const rows = [];
  let dropped = 0;
  for (const o of outlets) {
    const code = o.outletCode;
    if (!code || seen.has(code)) continue;
    if (!isRestaurant(o, nonFoodSet)) { dropped++; continue; }
    seen.add(code);
    const rank = rows.length + 1;

    const rt = o.rating || {};
    const ratingTitle = rt.title || null;
    const tag = typeof o.tag === 'string' ? o.tag : null;
    const miniTag = typeof o.miniTag === 'string' ? o.miniTag : null;
    const extendedTag = typeof o.extendedTag === 'string' ? o.extendedTag : null;
    const hasPromo = !!(tag || miniTag || extendedTag);
    const classified = classifyPromo(tag, extendedTag, miniTag);
    const isFreeDeliv = classified.scope === 'free_delivery';
    const d = o.delivery || {};
    const bannerKey = (o.media && typeof o.media.heroImage === 'string') ? o.media.heroImage : null;
    const logoKey = (o.media && typeof o.media.logoImage === 'string') ? o.media.logoImage : null;

    rows.push([
      code,                                                     // [0] noon_branch_id
      rank,                                                     // [1] noon_listing_rank
      o.operatingStatusCode || null,                            // [2]
      o.operatingType || null,                                  // [3]
      rt.score != null ? rt.score : null,                       // [4] rating
      rt.count != null ? rt.count : null,                       // [5] rating count
      deriveRatingStatus(ratingTitle),                          // [6]
      hasPromo,                                                 // [7]
      cleanText(tag) || cleanText(extendedTag) || cleanText(miniTag), // [8]
      classified.scope,                                         // [9]
      classified.cap,                                           // [10]
      o.widgetGroup || null,                                    // [11]
      !!(o.isPersonalized),                                     // [12]
      false,                                                    // [13] is_sponsored (deprecated)
      null,                                                     // [14] ad_slot (deprecated)
      rt.displayCount || null,                                  // [15]
      cleanText(miniTag),                                       // [16]
      cleanText(extendedTag),                                   // [17]
      isFreeDeliv,                                              // [18]
      hasPromo && !isFreeDeliv,                                 // [19]
      !!(o.isNearAndQuick),                                     // [20]
      o.name || null,                                           // [21]
      o.linkUrl || null,                                        // [22]
      cdnUrl(bannerKey),                                        // [23]
      Array.isArray(o.cuisines) ? o.cuisines : [],              // [24]
      o.restaurantCode || null,                                 // [25]
      typeof o.price === 'string' ? o.price : null,             // [26]
      typeof o.distance === 'number' ? o.distance : null,       // [27]
      ratingTitle,                                              // [28]
      typeof d.fee === 'number' ? d.fee : null,                 // [29]
      typeof d.originalFee === 'number' ? d.originalFee : null, // [30]
      typeof d.minOrder === 'number' ? d.minOrder : null,       // [31]
      typeof d.time === 'string' ? d.time : null,               // [32]
      classified.percent,                                       // [33]
      cdnUrl(logoKey),                                          // [34]
      typeof o.isVip === 'boolean' ? o.isVip : null,            // [35]
      o.vipMsg || null,                                         // [36]
    ]);
  }
  return { rows, dropped };
}

// -- Main ---------------------------------------------------------------------

(async () => {
  CURL = ensureCurl();
  say(`starting | run=${RUN_ID} gh=${GH_RUN_ID} wrapper=${CURL}`);

  const start = await rpc('noon_ranking_machine_start', { p_run_id: RUN_ID, p_machine_no: MACHINE, p_github_run_id: GH_RUN_ID });
  if (!start || start.stop || start.error) { say(`told to stop at check-in: ${JSON.stringify(start)}`); return; }
  const scrapeDate = start.scrape_date, scrapeHour = start.scrape_hour, dryRun = !!start.dry_run;

  // Spec -> session-level values (v3 verbatim resolution)
  const SPEC = mergeSpec(DEFAULT_SPEC, await rpc('noon_ranking_scrape_spec', {}));
  MAX_AREAS = Number(SPEC.max_areas ?? 2);
  const EARLY_EXIT_PAGES = Number(SPEC.early_exit_pages ?? 50);
  let PAGE_GAP_MS_MIN, PAGE_GAP_MS_MAX;
  if (SPEC.pacing.session_randomize) {
    PAGE_GAP_MS_MIN = randInt(SPEC.pacing.page_gap_ms_min, SPEC.pacing.page_gap_ms_min + 1500);
    PAGE_GAP_MS_MAX = randInt(PAGE_GAP_MS_MIN + 1200, SPEC.pacing.page_gap_ms_max);
  } else {
    PAGE_GAP_MS_MIN = SPEC.pacing.page_gap_ms_min;
    PAGE_GAP_MS_MAX = SPEC.pacing.page_gap_ms_max;
  }
  const TOKEN_REFRESH_AT_MS = randInt(SPEC.token_refresh.at_ms_min, SPEC.token_refresh.at_ms_max);
  const IMPRESSIONS_PER_PAGE = randInt(SPEC.telemetry.impressions_per_page_min, SPEC.telemetry.impressions_per_page_max);
  const PAUSE_PROBABILITY = Number(SPEC.pacing.pause_probability || 0);
  const PAUSE_MS_MIN = SPEC.pacing.pause_ms_min, PAUSE_MS_MAX = SPEC.pacing.pause_ms_max;
  say(`spec: max_areas=${MAX_AREAS} gap=${PAGE_GAP_MS_MIN}-${PAGE_GAP_MS_MAX}ms refresh=${TOKEN_REFRESH_AT_MS}ms imp=${IMPRESSIONS_PER_PAGE} pause_prob=${PAUSE_PROBABILITY} on_block=${JSON.stringify(SPEC.on_search_block)}`);

  const nonFoodList = await rpc('noon_get_non_food_cuisines', {});
  const nonFoodSet = new Set(Array.isArray(nonFoodList) ? nonFoodList : []);
  say(`non-food cuisines (${nonFoodSet.size}): ${[...nonFoodSet].join(', ')}`);

  RUNNER_IP = egressIp();
  say(`egress ip ${RUNNER_IP || '(unknown)'} | date=${scrapeDate} hour=${scrapeHour} dry=${dryRun}`);

  // Session (one per machine, shared by both areas — v3)
  let deviceId = crypto.randomUUID().toUpperCase();
  let visitorId = crypto.randomUUID().toLowerCase();
  const deviceIdfv = deviceId; // etracker idfv stays the original device id (v3)
  const jar = {};
  let whoHeaders = {};
  let lastMintAt = 0;
  let remintCount = 0;
  let identityRotations = 0;
  let recoveryAttempts = 0;
  let sessionKilled = false;
  let totalAreas = 0, totalRows = 0, totalDropped = 0;
  let exitReason = 'areas_complete';

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
    const r = curlCall(step, ETRACKER_URL, {
      method: 'POST', headers: { ...base(), 'Content-Type': 'text/plain;charset=UTF-8' },
      body: JSON.stringify(events), cookieJar: jar,
    });
    const ok = r.status >= 200 && r.status < 300;
    if (AREA_LOG) { AREA_LOG.et_posts++; AREA_LOG.et_events += events.length; if (!ok) AREA_LOG.et_non2xx++; }
    say(`  telemetry ${step}: ${r.status} (${events.length} events: ${[...new Set(events.map((e) => e.ev))].join(',')}) ${r.ms}ms`);
    return r;
  };
  const shortId = (id) => String(id).slice(0, 8);
  const fail = (r) => Object.assign(new Error(`${r.step} ${r.status}`), { step: r.step, status: r.status });

  for (let areaIdx = 0; !sessionKilled && areaIdx < MAX_AREAS; areaIdx++) {
    if (Date.now() - START_TIME > WALL_LIMIT_MS) { exitReason = 'wall_limit'; break; }

    // Claim (Supabase pacing: wait_ms / gap_seconds — same as before)
    let claim;
    for (;;) {
      claim = await rpc('noon_ranking_claim', { p_run_id: RUN_ID, p_machine_no: MACHINE });
      if (claim && claim.noon_area_id == null && claim.wait_ms > 0) { heartbeat(true); await sleep(claim.wait_ms); continue; }
      break;
    }
    if (!claim || claim.noon_area_id == null) { say(`no area: ${claim && claim.reason}`); exitReason = 'no_more_areas'; break; }

    const areaId = claim.noon_area_id;
    const areaName = claim.area_name || String(areaId);
    const lat = Number(claim.lat), lng = Number(claim.lng);
    const maxPages = Number(claim.max_pages) || 120;
    if (claim.gap_seconds > 0) await sleep(claim.gap_seconds * 1000);
    say(`=== AREA ${areaIdx + 1}/${MAX_AREAS}: ${areaName} id=${areaId} (${lat},${lng}) ===`);

    const areaStart = Date.now();
    AREA_LOG = newAreaLog();
    const deviceAtStart = deviceId;
    const reqBefore = NOON_REQ;
    const rotBefore = identityRotations, recBefore = recoveryAttempts;
    let pagesDone = 0;
    let searchToken = null;
    let distractionPauses = 0;
    let stopReason = null;
    const seen = new Set();
    const captures = [];

    const auditVisibility = () => ({
      rotations: AREA_LOG.rotations, recoveries: AREA_LOG.recoveries, recoveries_ok: AREA_LOG.recoveries_ok,
      remints: AREA_LOG.remints, search_403: AREA_LOG.search_403, search_429: AREA_LOG.search_429,
      et_posts: AREA_LOG.et_posts, et_events: AREA_LOG.et_events, et_non2xx: AREA_LOG.et_non2xx,
      device_id_start: shortId(deviceAtStart), device_id_end: shortId(deviceId),
      req_log: AREA_LOG.log,
    });

    const buildFdHeaders = () => {
      const h = { ...base(), 'x-experience': 'food', 'x-locale': 'en-ae',
        'x-device-lat': String(lat), 'x-device-lng': String(lng), 'x-mp-country': 'ae',
        'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)),
        'x-border-enabled': 'true', 'x-rocket-enabled': 'true' };
      for (const [k, v] of Object.entries(whoHeaders)) if (v != null && typeof v !== 'object') h[k] = String(v);
      return h;
    };
    const mergeWho = (body) => {
      try {
        const j = JSON.parse(body);
        if (j && j.headers && typeof j.headers === 'object') {
          for (const [k, v] of Object.entries(j.headers)) if (v != null && typeof v !== 'object') whoHeaders[k] = v;
        }
      } catch (_) {}
    };

    const dualRemint = async (reason) => {
      remintCount++;
      const tag = `remint${remintCount}`;
      if (AREA_LOG) AREA_LOG.remints++;
      say(`REMINT ${tag}: ${reason} (device=${shortId(deviceId)} visitor=${shortId(visitorId)})`);
      let rr = await curlCallWithRetry(`${tag}_st_whoami`, WHOAMI_URL, {
        headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() }, cookieJar: jar });
      say(`  ${rr.step}: ${rr.status} in ${rr.ms}ms`);
      if (rr.status !== 200) throw fail(rr);
      await sleep(200 + Math.random() * 400);
      rr = await curlCallWithRetry(`${tag}_fd_whoami`, FD_WHOAMI_URL, { headers: buildFdHeaders(), cookieJar: jar });
      say(`  ${rr.step}: ${rr.status} in ${rr.ms}ms`);
      if (rr.status !== 200) throw fail(rr);
      mergeWho(rr.body);
      await sleep(100 + Math.random() * 200);
      et(`et_${tag}_zone`, [evWhoamiAbConfig(whoHeaders, visitorId, deviceIdfv), evSelectedZone(whoHeaders, visitorId, deviceIdfv)]);
      lastMintAt = Date.now();
    };

    const applyStrategy = async (name) => {
      const devBefore = deviceId, visBefore = visitorId;
      if (AREA_LOG) AREA_LOG.recoveries++;
      if (name === 'swap_device_id') { deviceId = crypto.randomUUID().toUpperCase(); identityRotations++; }
      else if (name === 'swap_visitor_id') { visitorId = crypto.randomUUID().toLowerCase(); identityRotations++; }
      else if (name === 'swap_both_ids') { deviceId = crypto.randomUUID().toUpperCase(); visitorId = crypto.randomUUID().toLowerCase(); identityRotations++; }
      else if (name === 'remint') { delete jar.nguestv2; await dualRemint(`recovery remint ${recoveryAttempts + 1}`); }
      else if (name === 'full_reset') {
        deviceId = crypto.randomUUID().toUpperCase(); visitorId = crypto.randomUUID().toLowerCase(); identityRotations++;
        for (const k of Object.keys(jar)) delete jar[k];
        whoHeaders = {};
        const r1 = await curlCallWithRetry('full_reset_mint_whoami', WHOAMI_URL, {
          headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() }, cookieJar: jar });
        if (r1.status === 200) {
          await sleep(400 + Math.random() * 600);
          const r2 = await curlCallWithRetry('full_reset_fd_whoami', FD_WHOAMI_URL, { headers: buildFdHeaders(), cookieJar: jar });
          mergeWho(r2.body);
          lastMintAt = Date.now();
        }
      } else say(`  WARNING: unknown recovery strategy "${name}"`);
      say(`  RECOVERY ${recoveryAttempts + 1}: ${name} | device ${shortId(devBefore)} -> ${shortId(deviceId)} | visitor ${shortId(visBefore)} -> ${shortId(visitorId)}`);
      logReq({ step: `recovery_${name}`, dev: `${shortId(devBefore)}>${shortId(deviceId)}`, vis: visBefore === visitorId ? undefined : `${shortId(visBefore)}>${shortId(visitorId)}` });
      if (AREA_LOG && deviceId !== devBefore) AREA_LOG.rotations++;
      recoveryAttempts++;
      if (SPEC.on_search_block.backoff_ms > 0) await sleep(SPEC.on_search_block.backoff_ms + Math.random() * 1000);
    };

    try {
      if (areaIdx === 0) {
        // --- 4-call mint with telemetry (v3 verbatim) ---
        say(`MINT start: device=${shortId(deviceId)} visitor=${shortId(visitorId)} ip=${RUNNER_IP || '?'}`);
        let r = await curlCallWithRetry('mint_whoami', WHOAMI_URL, {
          headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() }, cookieJar: jar });
        say(`${r.step}: ${r.status} in ${r.ms}ms`);
        if (r.status !== 200) throw fail(r);

        await sleep(300 + Math.random() * 300);
        et('et_mint_launch', [evLaunch(visitorId, deviceIdfv), evLaunchInfo(visitorId, deviceIdfv), evAppForeground(visitorId, deviceIdfv)]);

        await sleep(1200 + Math.random() * 800);
        r = await curlCallWithRetry('mint_set_location', SET_LOCATION_URL, {
          method: 'POST',
          headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'Content-Type': 'application/json' },
          body: JSON.stringify({ location: { lat, lng }, lang: 'en' }), cookieJar: jar });
        say(`${r.step}: ${r.status} in ${r.ms}ms`);
        if (r.status !== 200) throw fail(r);

        await sleep(200 + Math.random() * 300);
        et('et_mint_location', [evLocationFetch(visitorId, deviceIdfv), evPageHome(visitorId, deviceIdfv)]);

        await sleep(400 + Math.random() * 300);
        r = await curlCallWithRetry('mint_whoami_food', WHOAMI_FOOD_URL, {
          headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'x-whoami-request-id': crypto.randomUUID() },
          cookieJar: jar });
        say(`${r.step}: ${r.status} in ${r.ms}ms`);
        if (r.status !== 200) throw fail(r);
        try { const w = JSON.parse(r.body); whoHeaders = (w && w.headers && typeof w.headers === 'object') ? w.headers : {}; } catch (_) {}
        if (!jar.nguestv2) throw Object.assign(new Error('mint produced no nguestv2'), { step: 'mint_whoami_food', status: null });

        await sleep(150 + Math.random() * 250);
        et('et_mint_zone', [evWhoamiAbConfig(whoHeaders, visitorId, deviceIdfv), evSelectedZone(whoHeaders, visitorId, deviceIdfv)]);

        await sleep(800 + Math.random() * 1200);
        r = await curlCallWithRetry('mint_fd_whoami', FD_WHOAMI_URL, { headers: buildFdHeaders(), cookieJar: jar });
        say(`${r.step}: ${r.status} in ${r.ms}ms`);
        if (r.status !== 200) throw fail(r);
        mergeWho(r.body);
        lastMintAt = Date.now();
        say(`MINT done: nguestv2 set, zone=${whoHeaders['x-food-zonecode'] || '-'} in ${Math.round((Date.now() - areaStart) / 1000)}s`);
      } else {
        say(`AREA TRANSITION: same session, device=${shortId(deviceId)} visitor=${shortId(visitorId)}, token age ${Math.round((Date.now() - lastMintAt) / 1000)}s`);
        // --- Area transition: set-location + conditional remint (v3 verbatim) ---
        await sleep(800 + Math.random() * 700);
        const r = await curlCallWithRetry('area_set_location', SET_LOCATION_URL, {
          method: 'POST',
          headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'Content-Type': 'application/json' },
          body: JSON.stringify({ location: { lat, lng }, lang: 'en' }), cookieJar: jar });
        say(`${r.step}: ${r.status} in ${r.ms}ms`);
        if (r.status !== 200) throw fail(r);
        await sleep(200 + Math.random() * 300);
        et('et_area_transition', [evLocationFetch(visitorId, deviceIdfv)]);
        const age = Date.now() - lastMintAt;
        if (age > TOKEN_REFRESH_AT_MS) {
          await sleep(400 + Math.random() * 600);
          await dualRemint(`token age ${Math.round(age / 1000)}s > ${TOKEN_REFRESH_AT_MS / 1000}s`);
        }
      }

      // --- Pagination (v3 verbatim, page cap from Supabase claim) ---
      while (pagesDone < maxPages) {
        heartbeat();
        if (SPEC.periodic_identity_rotation.enabled && pagesDone > 0) {
          const every = SPEC.periodic_identity_rotation.every_pages;
          const atList = SPEC.periodic_identity_rotation.at_pages || [];
          if ((every && pagesDone % every === 0) || atList.includes(pagesDone)) {
            const field = SPEC.periodic_identity_rotation.field || 'device';
            if (field === 'device' || field === 'both') { deviceId = crypto.randomUUID().toUpperCase(); identityRotations++; }
            if (field === 'visitor' || field === 'both') { visitorId = crypto.randomUUID().toLowerCase(); identityRotations++; }
          }
        }
        if (pagesDone > 0 && PAUSE_PROBABILITY > 0 && Math.random() < PAUSE_PROBABILITY) {
          distractionPauses++;
          await sleep(PAUSE_MS_MIN + Math.random() * (PAUSE_MS_MAX - PAUSE_MS_MIN));
        }
        // Pre-search sleep: page gap minus ~1.5s telemetry overhead
        if (pagesDone > 0) {
          await sleep(Math.max(0, PAGE_GAP_MS_MIN - 1500) + Math.random() * Math.max(0, PAGE_GAP_MS_MAX - PAGE_GAP_MS_MIN));
        }
        const age = Date.now() - lastMintAt;
        if (SPEC.token_refresh.enabled && age > TOKEN_REFRESH_AT_MS) {
          await sleep(500 + Math.random() * 800);
          await dualRemint(`token age ${Math.round(age / 1000)}s at page ${pagesDone}`);
          await sleep(400 + Math.random() * 600);
        }

        const searchBody = {
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
        if (searchToken) searchBody.searchToken = searchToken;

        let sr;
        const maxRec = SPEC.on_search_block.enabled ? (SPEC.on_search_block.max_attempts || 0) : 0;
        for (let attempt = 0; attempt <= maxRec; attempt++) {
          const h = { ...buildFdHeaders(), 'Content-Type': 'application/json' };
          sr = curlCall(attempt === 0 ? 'search' : `search_recovery_${attempt}`, SEARCH_URL, {
            method: 'POST', headers: h, body: JSON.stringify(searchBody), cookieJar: jar });
          if (AREA_LOG && sr.status === 403) AREA_LOG.search_403++;
          if (AREA_LOG && sr.status === 429) AREA_LOG.search_429++;
          if (sr.status !== 200 || attempt > 0) {
            say(`search page ${pagesDone + 1}${attempt > 0 ? ` (recovery ${attempt})` : ''}: ${sr.status === null ? `NO RESPONSE (${sr.error && sr.error.slice(0, 80)})` : sr.status} in ${sr.ms}ms device=${shortId(deviceId)}`);
          }
          if (sr.status === 200 && attempt > 0 && AREA_LOG) { AREA_LOG.recoveries_ok++; say(`  RECOVERED on attempt ${attempt} with device ${shortId(deviceId)}`); }
          if (sr.status === 200) break;
          if (!SPEC.on_search_block.enabled || (sr.status !== 403 && sr.status !== 429) || attempt >= maxRec) break;
          const strategies = SPEC.on_search_block.strategies || [];
          if (strategies.length === 0) break;
          await applyStrategy(strategies[attempt % strategies.length]);
        }
        if (sr.status === 404) { stopReason = `404@${pagesDone + 1}`; break; }
        if (sr.status !== 200) throw Object.assign(new Error(`search ${sr.status}`), { step: 'search', status: sr.status });

        let json;
        try { json = JSON.parse(sr.body); } catch (_) { stopReason = `bad-json@${pagesDone + 1}`; break; }
        const outlets = [];
        (function walk(x, d) {
          if (d > 8 || !x) return;
          if (Array.isArray(x)) { for (const v of x) walk(v, d + 1); return; }
          if (typeof x === 'object') {
            if (typeof x.outletCode === 'string' && x.outletCode) { outlets.push(x); return; }
            for (const k of Object.keys(x)) walk(x[k], d + 1);
          }
        })(json.results || [], 0);
        pagesDone++;
        let fresh = 0;
        for (const o of outlets) if (!seen.has(o.outletCode)) { seen.add(o.outletCode); captures.push(o); fresh++; }
        say(`p${pagesDone}: ${outlets.length} outlets (${fresh} new, ${seen.size} total) ${sr.ms}ms`);
        if (outlets.length === 0) { stopReason = `empty@${pagesDone}`; break; }
        if (fresh === 0) { stopReason = `no-new@${pagesDone}`; break; }
        if (!json.searchToken) { stopReason = `no-token@${pagesDone}`; break; }
        searchToken = json.searchToken;

        // Telemetry after each page (v3 verbatim)
        await sleep(150 + Math.random() * 200);
        et('et_page_catalog', [evPageCatalog(pagesDone, visitorId, deviceIdfv)]);
        const visible = outlets.slice(0, IMPRESSIONS_PER_PAGE);
        if (visible.length > 0) {
          await sleep(500 + Math.random() * 900);
          et('et_impressions', impressionTripletsBatch(visible, 0, visitorId, deviceIdfv));
        }
      }
      if (!stopReason) stopReason = `max-pages(${maxPages})`;
    } catch (e) {
      // Any failure kills the session (v3 + previous production rule).
      sessionKilled = true;
      const kind = e.status === 429 ? 'rate_limited' : e.status === 403 ? 'blocked' : 'error';
      say(`area ${areaIdx + 1} ${kind} @ ${e.step || '?'}: ${e.message} — stopping machine`);
      await audit({
        area_seq: areaIdx + 1, noon_area_id: areaId, area_name: areaName,
        claimed_at: new Date(areaStart).toISOString(), ended_at: new Date().toISOString(),
        gap_s: claim.gap_seconds || 0, page_delay_ms: PAGE_GAP_MS_MIN, max_pages: maxPages,
        token_no: remintCount + 1, token_age_ms: lastMintAt ? Date.now() - lastMintAt : null,
        req_before: reqBefore, req_area: NOON_REQ - reqBefore, elapsed_ms: Date.now() - areaStart,
        outcome: kind, pages: pagesDone, fail_step: failStep(e.step), fail_page: e.step === 'search' ? pagesDone + 1 : null,
        http_status: e.status || null, error: String(e.message).slice(0, 300),
        ...auditVisibility(),
      });
      const rep = await rpc('noon_ranking_report', {
        p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: areaId,
        p_result: { status: kind, http_status: e.status || null, error: String(e.message).slice(0, 500) },
      });
      say(`reported ${kind}: action=${rep && rep.action} reason=${(rep && rep.reason) || '-'}`);
      exitReason = kind;
      break;
    }

    // --- Success: build rows, save, report (previous production contract) ---
    const parsed = parseCards(captures, nonFoodSet);
    let savedAnalysis = 0, savedPending = 0;
    if (!dryRun && parsed.rows.length > 0) {
      const sv = await rpc('noon_ranking_save_area_hour', { p_area_id: areaId, p_date: scrapeDate, p_hour: scrapeHour, p_rows: parsed.rows });
      savedAnalysis = (sv && sv.inserted != null) ? sv.inserted : parsed.rows.length;
      savedPending = (sv && sv.pending != null) ? sv.pending : 0;
    }
    const elapsed = Date.now() - areaStart;
    const rep = await rpc('noon_ranking_report', {
      p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: areaId,
      p_result: {
        status: 'ok', pages_scraped: pagesDone, cards: captures.length,
        ranking_rows: savedAnalysis, pending_rows: savedPending, dropped: parsed.dropped,
        bytes: 0, fetch_ms: elapsed,
        rotations: identityRotations - rotBefore, recoveries: recoveryAttempts - recBefore,
      },
    });
    await audit({
      area_seq: areaIdx + 1, noon_area_id: areaId, area_name: areaName,
      claimed_at: new Date(areaStart).toISOString(), ended_at: new Date().toISOString(),
      gap_s: claim.gap_seconds || 0, page_delay_ms: PAGE_GAP_MS_MIN, max_pages: maxPages,
      token_no: remintCount + 1, token_age_ms: lastMintAt ? Date.now() - lastMintAt : null,
      req_before: reqBefore, req_area: NOON_REQ - reqBefore, elapsed_ms: elapsed,
      outcome: 'ok', pages: pagesDone, cards: captures.length,
      rows_parsed: parsed.rows.length, rows_saved: savedAnalysis,
      stop_reason: `${stopReason} rot=${identityRotations - rotBefore} rec=${recoveryAttempts - recBefore} remints=${remintCount} pauses=${distractionPauses} gap=${PAGE_GAP_MS_MIN}-${PAGE_GAP_MS_MAX} imp=${IMPRESSIONS_PER_PAGE}`,
      ...auditVisibility(),
    });
    totalAreas++; totalRows += parsed.rows.length; totalDropped += parsed.dropped;
    say(`${areaName} ok: ${captures.length} cards, ${parsed.rows.length} rows (${parsed.dropped} non-food dropped), ${pagesDone}p, ${Math.round(elapsed / 1000)}s, recoveries=${recoveryAttempts - recBefore} | ${stopReason} | saved=${savedAnalysis} pending=${savedPending} -> ${rep && rep.action}`);

    if (rep && rep.action === 'stop_job') { exitReason = 'stop_job'; break; }
    if (areaIdx === 0 && pagesDone >= EARLY_EXIT_PAGES) { say(`area 1 was ${pagesDone}p (>= ${EARLY_EXIT_PAGES}) — not claiming area 2`); exitReason = 'first_area_long'; break; }
    if (rep && rep.gap_seconds > 0) await sleep(rep.gap_seconds * 1000);
  }

  const end = await rpc('noon_ranking_machine_end', { p_run_id: RUN_ID, p_machine_no: MACHINE, p_reason: exitReason });
  say(`=== DONE areas=${totalAreas} rows=${totalRows} dropped=${totalDropped} noon_requests=${NOON_REQ} remints=${remintCount} rotations=${identityRotations} recoveries=${recoveryAttempts} elapsed=${Math.round((Date.now() - START_TIME) / 1000)}s exit=${exitReason} end=${JSON.stringify(end)}`);
})().catch(async (e) => {
  console.error(`${prefix} FATAL: ${e.message}`);
  console.error(e.stack);
  try { await rpc('noon_ranking_machine_end', { p_run_id: RUN_ID, p_machine_no: MACHINE, p_reason: `fatal: ${String(e.message).slice(0, 200)}` }); } catch (_) {}
  process.exit(1);
});
