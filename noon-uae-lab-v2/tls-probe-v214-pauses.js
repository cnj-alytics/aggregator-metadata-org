#!/usr/bin/env node
// v2.14 — SAME as v2.13 but with RANDOM DISTRACTION PAUSES during pagination.
//
// Single change vs v2.13:
// (M) After every /search page, with probability PAUSE_PROBABILITY (default
//     0.20 = 20%), insert an extra 3,000-7,000ms "the user got distracted"
//     pause ON TOP of the normal page_gap. This breaks the uniform cadence
//     Akamai can detect across the pages 6-15 kill zone we observed in test 75.
//
// Everything else — mint sequence, telemetry schema, token-age remint,
// MAX_AREAS=2 cap, outlet capture — is identical to v2.13.
//
// Changes from v2.12:
// (K) Collects FULL outlet listing per page during pagination and sends it
//     with each per-area report. Edge Function v8 inserts rows into
//     noon_lab.outlet_capture (one row per outlet per page per area).
//     Each row carries listing_rank, outlet_code, name, rating, has_promo,
//     is_sponsored, distance, sla, and the raw outlet object for later
//     analysis. This is what makes the collected list reviewable.
// (L) MAX_AREAS capped at 2 (down from unlimited in v2.12). Area 3+ was
//     hitting HTTP 429 rate limits ~75% of the time, so the current ceiling
//     is 2 areas per visitor_id. Capping at 2 avoids wasted work.
//
// Everything else is preserved from v2.12.
//
//
// Design philosophy: match what the Oct 31 iPhone deep-scroll trace shows,
// across however many areas Akamai will tolerate for one session.
//
// Changes from v2.11:
// (H) Multi-area loop (from v2.10) with no MAX_AREAS cap. Keeps claiming
//     and scraping areas until (a) a fail stops the session or (b) queue
//     is empty. sessionKilled now triggers on ANY fail (blocked OR error),
//     fixing the test 71 cascade bug where network errors on remint didn't
//     set sessionKilled and areas 4-5 fired zero-page garbage.
// (I) Token-age remint: track mint time, fire ONE dual remint when
//     token_age > 240s (just before the 300s token TTL). This is the ONLY
//     mid-session whoami we fire — removing the every-3-pages remint that
//     didn't match real-app behavior.
// (J) Reduced impression telemetry: fire outlet_impression +
//     outlet_impression_v2 + promotion_impression triplets for only the
//     TOP 6 outlets per page (real app fires ~6-10 per page based on what
//     the user actually sees on screen, out of 60 in the response). Our
//     v2.9 fired for all 60 outlets, which is ~10x over-send.
//     Batch size is also smaller (3 events per POST), matching the real
//     app's ~3 events/POST average observed in Proxyman trace.
//
// Preserved from v2.11:
// (F) Real-pace pagination: 5.5-7.8s inter-page gap
// (A) 4-call initial mint with 1 whoami retry on failure
// (B) Full mint-phase etracker telemetry (launch, launch_info, app_foreground,
//     location_fetch, page_home, whoami_ab_config, selected_zone)
//
// Original design docs:
//
// Based on findings from the Oct 31 iPhone deep-scroll Proxyman trace:
// (F) Real app paginates /search with 5.66-7.80s inter-page gaps (mean 6.1s,
//     median 6.4s), consistent across 21 pages. Our prior v2.9/v2.10 used
//     700-1200ms sleeps → ~3s per page. We're 2× too fast. This probably
//     reads to Akamai as a bot signal.
// (G) Real app fires ZERO whoami/remint calls during 150s of pagination.
//     Only 3 whoami calls total in a 199s session, all in the first 43s
//     (mint phase). Our v2.8+ dual-remint-every-3-pages was based on a
//     misread of the SHORT original trace. We now know mid-pagination
//     reminting is wrong behavior. v2.11 removes it entirely.
//
// Preserved from v2.9:
// (A) 4-call initial mint (api-app-st + api-app set-location + api-app-st?experience=food + api-app-fd/customer/whoami)
// (B) Full mint-phase etracker telemetry (launch, launch_info, app_foreground, location_fetch, page_home, whoami_ab_config, selected_zone)
// (C) 3-event impression triplets per outlet during pagination
// (D) 1 retry on 403/429/503/null for every whoami call in the initial mint
//
// Schemas copied from the Oct 28 iPhone Proxyman trace.

'use strict';

const { execFileSync } = require('node:child_process');
const crypto = require('node:crypto');
const supa = require('./supabase.js');

const TEST_ID = Number(process.env.TEST_ID);
const MACHINE = Number(process.env.MACHINE_NO);
const GH_RUN_ID = process.env.GITHUB_RUN_ID || '';
// Which curl-impersonate WRAPPER SCRIPT to use. These are shell wrappers around
// curl-impersonate-chrome / -ff with the right --impersonate flag baked in.
// v0.6.1 build includes: curl_safari15_3, curl_chrome99_android, curl_chrome{99-116},
// curl_edge{99,101}, curl_ff{91esr,95-117}. No iOS preset in this build — the closest
// Apple BoringSSL fingerprint is curl_safari15_3 (desktop Safari macOS).
const CURL = process.env.IMPERSONATE_WRAPPER || 'curl_safari15_3';

if (!TEST_ID || !MACHINE) {
  console.error('TEST_ID and MACHINE_NO are required');
  process.exit(1);
}

const prefix = `[v2.14 t${TEST_ID} m${MACHINE}]`;
const say = (m) => console.log(`${prefix} ${m}`);

// v2.13: cap areas per machine to the known-good ceiling.
const MAX_AREAS = Number(process.env.MAX_AREAS || 2);

// v2.14 — random distraction pauses mid-pagination.
// After each /search page's telemetry, with PAUSE_PROBABILITY probability,
// add an extra 3-7s pause before the next /search. This breaks the uniform
// cadence that may be feeding Akamai's bot scorer in the pages 6-15 kill zone.
const PAUSE_PROBABILITY = Number(process.env.PAUSE_PROBABILITY || 0.20);
const PAUSE_MS_MIN = Number(process.env.PAUSE_MS_MIN || 3000);
const PAUSE_MS_MAX = Number(process.env.PAUSE_MS_MAX || 7000);

// Default ranges from Oct 31 Proxyman trace. These are the OUTER bounds —
// the actual values for THIS SESSION are randomly sampled from inside these
// bounds at startup so Akamai can't learn a single fixed pattern across
// 60 concurrent machines firing the same binary.
const PAGE_GAP_MS_MIN_FLOOR = Number(process.env.PAGE_GAP_MS_MIN || 4500);
const PAGE_GAP_MS_MAX_CEIL  = Number(process.env.PAGE_GAP_MS_MAX || 8500);

const TOKEN_TTL_MS = Number(process.env.TOKEN_TTL_MS || 300000);
const TOKEN_REFRESH_AT_FLOOR = Number(process.env.TOKEN_REFRESH_AT_MS_MIN || 210000);
const TOKEN_REFRESH_AT_CEIL  = Number(process.env.TOKEN_REFRESH_AT_MS_MAX || 260000);

const IMPRESSIONS_PER_PAGE_MIN = Number(process.env.IMPRESSIONS_PER_PAGE_MIN || 4);
const IMPRESSIONS_PER_PAGE_MAX = Number(process.env.IMPRESSIONS_PER_PAGE_MAX || 10);

// Per-session randomization: pick one base value per machine so each session
// looks slightly different to Akamai. We log these at startup so we can
// correlate pattern vs outcome in the attempt log.
const randInt = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo + 1));
const SESSION_PAGE_GAP_MIN = randInt(PAGE_GAP_MS_MIN_FLOOR, PAGE_GAP_MS_MIN_FLOOR + 1500);
const SESSION_PAGE_GAP_MAX = randInt(SESSION_PAGE_GAP_MIN + 1200, PAGE_GAP_MS_MAX_CEIL);
const SESSION_TOKEN_REFRESH_AT_MS = randInt(TOKEN_REFRESH_AT_FLOOR, TOKEN_REFRESH_AT_CEIL);
const SESSION_IMPRESSIONS_PER_PAGE = randInt(IMPRESSIONS_PER_PAGE_MIN, IMPRESSIONS_PER_PAGE_MAX);

// Aliases used throughout the rest of the file (kept similar to v2.12 names)
const PAGE_GAP_MS_MIN = SESSION_PAGE_GAP_MIN;
const PAGE_GAP_MS_MAX = SESSION_PAGE_GAP_MAX;
const TOKEN_REFRESH_AT_MS = SESSION_TOKEN_REFRESH_AT_MS;
const IMPRESSIONS_PER_PAGE = SESSION_IMPRESSIONS_PER_PAGE;

// v2.9 retry policy for whoami calls (initial mint and mid-session remints).
// One retry on 403/429/503/null, with 2–3s backoff plus jitter.
// NOTE: curlCall is defined later in this file; this helper is hoisted but
// invoked only inside the async IIFE where curlCall is in scope.
async function curlCallWithRetry(step, url, opts, { retries = 1, backoffMs = 2000 } = {}) {
  let r = curlCall(step, url, opts);
  let attempt = 1;
  while (
    attempt <= retries &&
    (r.status === 403 || r.status === 429 || r.status === 503 || r.status === null)
  ) {
    await new Promise(res => setTimeout(res, backoffMs + Math.random() * 1000));
    attempt++;
    const nextStep = `${step}_retry${attempt - 1}`;
    r = curlCall(nextStep, url, opts);
    r.step = nextStep;
  }
  return r;
}

const UA = 'noon/22895 CFNetwork/3896.100.1.2.1 Darwin/27.0.0';
const BUILD = '22895';

// Egress IP via curl-impersonate so we're measuring from the TLS stack we'll actually use.
function egressIp() {
  try {
    const out = execFileSync(CURL, [
      '-sS', '--max-time', '8',
      'https://api.ipify.org',
    ], { encoding: 'utf-8', timeout: 10000 });
    return out.trim();
  } catch (_) { return null; }
}

// curl-impersonate call that returns {status, headers, body, setCookies}.
// Uses -i to include headers in output, parses the HTTP response.
// The wrapper (e.g. curl_safari15_3) already has --impersonate baked in.
function curlCall(step, url, { method = 'GET', headers = {}, body = null, cookieJar = null } = {}) {
  const args = [
    '-sS', '-i',
    '--max-time', '20',
    '-X', method,
  ];
  // Headers
  for (const [k, v] of Object.entries(headers)) {
    args.push('-H', `${k}: ${v}`);
  }
  // Cookies
  if (cookieJar) {
    const cookieStr = Object.entries(cookieJar).map(([k, v]) => `${k}=${v}`).join('; ');
    if (cookieStr) args.push('-H', `Cookie: ${cookieStr}`);
  }
  // Body
  if (body != null) args.push('--data-raw', body);
  args.push('--', url);

  const t0 = Date.now();
  let raw;
  try {
    raw = execFileSync(CURL, args, { encoding: 'utf-8', timeout: 22000, maxBuffer: 20 * 1024 * 1024 });
  } catch (e) {
    const ms = Date.now() - t0;
    return { step, status: null, ms, error: String(e.message || e).slice(0, 300) };
  }
  const ms = Date.now() - t0;

  // Parse: headers end at first \r\n\r\n (or \n\n). curl-impersonate may emit
  // multiple header blocks if there was a redirect; take the LAST one.
  const sep = raw.lastIndexOf('\r\n\r\n');
  const alt = raw.lastIndexOf('\n\n');
  const splitAt = sep >= 0 ? sep + 4 : alt >= 0 ? alt + 2 : -1;
  if (splitAt < 0) return { step, status: null, ms, error: 'no header separator' };

  // The last block has the final response. Find the last status line in headers area.
  const headerText = raw.slice(0, splitAt);
  const bodyText = raw.slice(splitAt);
  const blocks = headerText.split(/\r?\n\r?\n/).filter(Boolean);
  const lastBlock = blocks[blocks.length - 1];
  const lines = lastBlock.split(/\r?\n/);
  const statusLine = lines[0] || '';
  const status = parseInt((statusLine.split(' ')[1] || '0'), 10);

  const headersMap = new Map();
  const setCookies = [];
  for (const ln of lines.slice(1)) {
    const ci = ln.indexOf(': ');
    if (ci < 0) continue;
    const k = ln.slice(0, ci).toLowerCase(), v = ln.slice(ci + 2);
    if (k === 'set-cookie') setCookies.push(v);
    else headersMap.set(k, v);
  }

  // Update cookie jar from Set-Cookie headers
  if (cookieJar) {
    for (const c of setCookies) {
      const m = /^([^=]+)=([^;]+)/.exec(c);
      if (m) cookieJar[m[1]] = m[2];
    }
  }

  return { step, status, ms, headers: headersMap, setCookies, body: bodyText };
}

// =========================================================================
// etracker helpers — matching event schemas from the real iPhone Proxyman trace
// =========================================================================
const ETRACKER_URL = 'https://etracker.noon.com/track/v2/';

// Standard device block that every event carries
function deviceBlock(idfv) {
  return {
    dm: 'Apple iPhone 17 Pro Max',
    dos: 'ios 27.0.1',
    d: 'Handset',
    sr: '1320x2868',
    vps: '1320x2868',
    scl: 3,
    idfa: '00000000-0000-0000-0000-000000000000',
    idfv: idfv,
  };
}

// Build a single event with the top-level envelope the real app uses
function evt(ev, em, visitorId, idfv) {
  return {
    em,
    ev,
    ts: Date.now(),
    pc: 'noon',
    vid: visitorId,
    lo: 'en-ae',
    pf: 'ios',
    d: deviceBlock(idfv),
  };
}

// --- mint-phase events (fired BEFORE first /search) ---
function evLaunch(visitorId, idfv) {
  return evt('launch', {
    fsv: '1.00',
    isNA: false,
    nps: 'denied',
    perf: { NTJS: { d: 700 }, GWA: { d: 250 }, SR: { d: 280, m: { fsmps: 10 } }, TJS: { d: 15500 }, TAS: { d: 16200 } },
    mc: 'noon', an: 'noon', av: '5.261024', ab: '22895', mpe: 'noon',
  }, visitorId, idfv);
}
function evLaunchInfo(visitorId, idfv) {
  const now = Date.now();
  return evt('launch_info', {
    nsa: now - 10000, jsa: now - 9400, ds: 0.6, s: 'app_launch', dl: 'en',
    mc: 'noon', an: 'noon', av: '5.261024', ab: '22895', mpe: 'noon',
  }, visitorId, idfv);
}
function evAppForeground(visitorId, idfv) {
  return evt('app_foreground', {
    mc: 'food', an: 'noon', av: '5.261024', ab: '22895', mpe: 'food',
  }, visitorId, idfv);
}
function evLocationFetch(visitorId, idfv) {
  return evt('location_fetch', {
    GPS_allow: true,
    mc: 'noon', an: 'noon', av: '5.261024', ab: '22895', mpe: 'noon',
  }, visitorId, idfv);
}
function evPageHome(visitorId, idfv) {
  return evt('page_home', {
    curl: '/',
    mc: 'noon', an: 'noon', av: '5.261024', ab: '22895', mpe: 'noon',
  }, visitorId, idfv);
}
function evSelectedZone(zh, visitorId, idfv) {
  // zh is the headers object from whoami-food response (zone codes)
  return evt('selected_zone', {
    szn: String(zh['x-ecom-zonecode'] || 'AE_DXB-S1'),
    szf: String(zh['x-food-zonecode'] || 'FOOD-AE-DXB-HUB4-DUBAI_MARINA'),
    szi: String(zh['x-nooninstant-zonecode'] || ''),
    szr: String(zh['x-rocket-zonecode'] || ''),
    sznow: String(zh['x-noonnownow-zonecode'] || ''),
    szs: String(zh['x-services-zonecode'] || 'SERVICES-AE-DUBAI'),
    szmpr: String(zh['x-rocket-mp-zonecode'] || ''),
    szg: String(zh['x-ecom-zonecode'] || 'AE_DXB-S1'),
    address_codes: [{mp:'ecom'},{mp:'rocket'},{mp:'food'},{mp:'nooninstant'},{mp:'services'},{mp:'noonnownow'},{mp:'ping'},{mp:'out'},{mp:'aster'},{mp:'money'}],
    mc: 'food', an: 'noon', av: '5.261024', ab: '22895', mpe: 'food',
  }, visitorId, idfv);
}
function evWhoamiAbConfig(zh, visitorId, idfv) {
  // Pull real x-ab-test ids from whoami-food response if available
  let abIds = [];
  const raw = zh['x-ab-test'];
  if (typeof raw === 'string') abIds = raw.split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n));
  else if (Array.isArray(raw)) abIds = raw.filter(n => typeof n === 'number');
  return evt('whoami_ab_config', {
    abIds: abIds.length ? abIds : [1052, 1471, 1531, 1731, 1750],
    mc: 'noon', an: 'noon', av: '5.261024', ab: '22895', mpe: 'noon',
  }, visitorId, idfv);
}
function evPageCatalog(pageNum, visitorId, idfv) {
  return evt('page_catalog', {
    hid: crypto.randomUUID(),
    nbh: 60, pn: pageNum, sb: 'browse',
    mc: 'food', an: 'noon', av: '5.261024', ab: '22895', mpe: 'food',
  }, visitorId, idfv);
}

// --- card-impression triplets fired during /search pagination ---
// For each outlet in viewport, real app sends outlet_impression +
// outlet_impression_v2 + promotion_impression PAIRED (same timestamp).
function evOutletImpression(outlet, idx, visitorId, idfv) {
  return evt('outlet_impression', {
    oc: outlet.outletCode,
    wid: `search_outlet/${idx}`,
    aid: outlet.outletCode,
    ix: `0/${idx}`,
    otg: false,
    df: outlet.distance || 5.0,
    fav: false,
    isp: true,
    sla: outlet.sla || '30-40',
    wn: 'search_results',
    wtc: 'search_results_tab',
    curl: '/search?type=outlet',
    src: 'search/results',
    pn: 'Search_Results',
    mc: 'food', an: 'noon', av: '5.261024', ab: '22895', mpe: 'food',
  }, visitorId, idfv);
}
function evOutletImpressionV2(outlet, idx, visitorId, idfv) {
  return evt('outlet_impression_v2', {
    pg: 'SR',
    wt: `SR/search_results/0/search_results_tab/${idx}/ROW/${idx}/ITM/0`,
    ul: '/search?type=outlet',
    cf: '{}',
    isp: true,
    oc: outlet.outletCode,
    df: outlet.distance || 5.0,
    sla: outlet.sla || '30-40',
    mc: 'food', an: 'noon', av: '5.261024', ab: '22895', mpe: 'food',
  }, visitorId, idfv);
}
function evPromotionImpression(outlet, idx, visitorId, idfv) {
  return evt('promotion_impression', {
    aid: outlet.outletCode,
    wid: `search_results/${idx}`,
    aidMap: { [outlet.outletCode]: `search_results/${idx}` },
    ismMap: { [outlet.outletCode]: true },
    curl: '/search?type=outlet',
    toUrl: `/outlet/${outlet.outletCode}`,
    ed: { eIds: [], reqId: crypto.randomUUID() },
    cmsEids: {},
    wp: idx,
    ar: 100,
    adType: 'display',
    isAd: false,
    mc: 'food', an: 'noon', av: '5.261024', ab: '22895', mpe: 'food',
  }, visitorId, idfv);
}

// Build an etracker batch of 3-event triplets (outlet_impression,
// outlet_impression_v2, promotion_impression) for a slice of outlets.
function impressionTripletsBatch(outlets, startIdx, visitorId, idfv) {
  const events = [];
  outlets.forEach((o, i) => {
    const idx = startIdx + i;
    events.push(evOutletImpression(o, idx, visitorId, idfv));
    events.push(evOutletImpressionV2(o, idx, visitorId, idfv));
    events.push(evPromotionImpression(o, idx, visitorId, idfv));
  });
  return events;
}

// Shared helper: fire an etracker POST and log it
function fireEtracker(session, step, events) {
  return curlCall(step, ETRACKER_URL, {
    method: 'POST',
    headers: {
      ...session.base(),
      'Content-Type': 'text/plain;charset=UTF-8',
    },
    body: JSON.stringify(events),
    cookieJar: session.jar,
  });
}

(async () => {
  const runnerIp = egressIp();
  say(`egress ip ${runnerIp || '(unknown)'}, TLS wrapper=${CURL}`);
  say(`session params: page_gap=${SESSION_PAGE_GAP_MIN}-${SESSION_PAGE_GAP_MAX}ms, token_refresh=${SESSION_TOKEN_REFRESH_AT_MS}ms, impressions=${SESSION_IMPRESSIONS_PER_PAGE}/page, max_areas=${MAX_AREAS}`);

  // Claim the FIRST area to establish an identity for the session.
  let firstClaim = await supa.claim({
    test_id: TEST_ID, machine_no: MACHINE, github_run_id: GH_RUN_ID, runner_ip: runnerIp,
  });
  if (!firstClaim || firstClaim.stop) {
    say(`nothing to do — ${firstClaim && firstClaim.reason}`);
    return;
  }
  const { variant_id, identity } = firstClaim;
  say(`machine identity: variant=${variant_id} device=${identity.device_id.slice(0, 8)}... visitor=${identity.visitor_id.slice(0, 8)}...`);

  // Session state (persists across all areas, same visitor_id)
  const deviceId = identity.device_id;
  const visitorId = identity.visitor_id;
  const jar = {};
  const reqLog = [];
  const sentryTrace = () => {
    const trace = crypto.randomBytes(16).toString('hex');
    const span = crypto.randomBytes(8).toString('hex');
    return {
      'sentry-trace': `${trace}-${span}`,
      baggage: `sentry-environment=production,sentry-public_key=d61e4de762b3c36e7c14f2d7cf6030c1,sentry-trace_id=${trace}`,
    };
  };
  const base = () => ({
    'User-Agent': UA, 'x-platform': 'ios', 'x-build': BUILD, 'x-mp': 'noon', 'x-content': 'mobile',
    Accept: 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'x-device-id': deviceId,
    'x-visitor-id': visitorId,
    ...sentryTrace(),
  });

  // Session-wide state
  const sessionStart = Date.now();
  let whoHeaders = {};
  let lastMintAt = 0; // Timestamp of last successful whoami pair (ms). Starts at 0; set on initial mint.
  let remintCount = 0;
  let sessionKilled = false;
  let areaIdx = 0;

  // deviceIdfv (uppercase UUID) used in every etracker event's d.idfv
  const deviceIdfv = (deviceId.includes('-') ? deviceId.toUpperCase() : crypto.randomUUID().toUpperCase());

  // helper for firing etracker (needs access to base/jar/visitorId/deviceIdfv)
  const sessionCtx = { base, jar };
  const et = (step, events) => fireEtracker(sessionCtx, step, events);

  const FD_WHOAMI_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/customer/whoami';
  const SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';

  // --- Outer area loop: scrape up to MAX_AREAS or until session killed/queue empty ---
  while (!sessionKilled && areaIdx < MAX_AREAS) {
    // Claim the current area (first iteration uses firstClaim already in hand).
    const claim = (areaIdx === 0) ? firstClaim : await supa.claim({
      test_id: TEST_ID, machine_no: MACHINE, github_run_id: GH_RUN_ID, runner_ip: runnerIp,
    });
    if (!claim || claim.stop) {
      say(`area ${areaIdx + 1}: no more items — ${claim && claim.reason}`);
      break;
    }
    const { item_id, area_name, lat, lng } = claim;
    say(`=== AREA ${areaIdx + 1}: ${area_name} id=${claim.noon_area_id} (${lat},${lng}) item_id=${item_id} ===`);

    // Per-area state
    let fail = null;
    let pagesDone = 0;
    let cards = 0;
    let setupMs = null;
    let searchToken = null;
    const seen = new Set();
    // v2.13: accumulate per-outlet rows for this area to upload with the report.
    // Only FRESH outlets (not previously seen in this area) are stored, with their
    // page_num and global listing_rank preserved. Memory: 60 pages × 60 outlets ≈
    // 3,600 objects per area worst case; well under any Edge Function payload cap.
    const outletCaptures = [];
    const areaStart = Date.now();

    // Build fdHeaders for THIS area
    const buildFdHeaders = () => {
      const h = {
        ...base(), 'x-experience': 'food', 'x-locale': 'en-ae',
        'x-device-lat': String(lat), 'x-device-lng': String(lng),
        'x-mp-country': 'ae',
        'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)),
        'x-border-enabled': 'true', 'x-rocket-enabled': 'true',
      };
      for (const [k, v] of Object.entries(whoHeaders)) {
        if (v != null && typeof v !== 'object') h[k] = String(v);
      }
      return h;
    };

    // Dual remint (ecom + food whoamis). Shared by token-age refresh.
    const dualRemint = async (reason) => {
      remintCount++;
      const tag = `remint${remintCount}`;
      say(`FIRING ${tag}: ${reason}`);
      let rr = await curlCallWithRetry(`${tag}_st_whoami`, 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami', {
        headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() },
        cookieJar: jar,
      });
      reqLog.push({ step: rr.step, status: rr.status, ms: rr.ms, set_count: (rr.setCookies || []).length });
      say(`${rr.step}: ${rr.status} in ${rr.ms}ms`);
      if (rr.status !== 200) throw Object.assign(new Error(`${rr.step} ${rr.status}`), { step: rr.step, status: rr.status, kind: rr.status === 403 ? 'blocked' : 'error' });
      await new Promise(res => setTimeout(res, 200 + Math.random() * 400));
      rr = await curlCallWithRetry(`${tag}_fd_whoami`, FD_WHOAMI_URL, { headers: buildFdHeaders(), cookieJar: jar });
      reqLog.push({ step: rr.step, status: rr.status, ms: rr.ms, set_count: (rr.setCookies || []).length });
      say(`${rr.step}: ${rr.status} in ${rr.ms}ms`);
      if (rr.status !== 200) throw Object.assign(new Error(`${rr.step} ${rr.status}`), { step: rr.step, status: rr.status, kind: rr.status === 403 ? 'blocked' : 'error' });
      try {
        const fdResp = JSON.parse(rr.body);
        if (fdResp && fdResp.headers && typeof fdResp.headers === 'object') {
          for (const [k, v] of Object.entries(fdResp.headers)) {
            if (v != null && typeof v !== 'object') whoHeaders[k] = v;
          }
        }
      } catch (_) {}
      await new Promise(res => setTimeout(res, 100 + Math.random() * 200));
      const re = et(`et_${tag}_zone`, [
        evWhoamiAbConfig(whoHeaders, visitorId, deviceIdfv),
        evSelectedZone(whoHeaders, visitorId, deviceIdfv),
      ]);
      reqLog.push({ step: `et_${tag}_zone`, status: re.status, ms: re.ms });
      lastMintAt = Date.now();
    };

    try {
      if (areaIdx === 0) {
        // --- First-area full 4-call mint ---
        let r = await curlCallWithRetry('mint_whoami', 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami', {
          headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() },
          cookieJar: jar,
        });
        reqLog.push({ step: r.step, status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
        say(`${r.step}: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
        if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: r.step, status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });

        await new Promise(res => setTimeout(res, 300 + Math.random() * 300));
        let etr = et('et_mint_launch', [
          evLaunch(visitorId, deviceIdfv), evLaunchInfo(visitorId, deviceIdfv), evAppForeground(visitorId, deviceIdfv),
        ]);
        reqLog.push({ step: 'et_mint_launch', status: etr.status, ms: etr.ms });

        await new Promise(res => setTimeout(res, 1200 + Math.random() * 800));
        r = await curlCallWithRetry('mint_set_location', 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng', {
          method: 'POST',
          headers: {
            ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae',
            'x-device-lat': String(lat), 'x-device-lng': String(lng),
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ location: { lat, lng }, lang: 'en' }),
          cookieJar: jar,
        });
        reqLog.push({ step: r.step, status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
        say(`${r.step}: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
        if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: r.step, status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });

        await new Promise(res => setTimeout(res, 200 + Math.random() * 300));
        etr = et('et_mint_location', [evLocationFetch(visitorId, deviceIdfv), evPageHome(visitorId, deviceIdfv)]);
        reqLog.push({ step: 'et_mint_location', status: etr.status, ms: etr.ms });

        await new Promise(res => setTimeout(res, 400 + Math.random() * 300));
        r = await curlCallWithRetry('mint_whoami_food', 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1', {
          headers: {
            ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae',
            'x-device-lat': String(lat), 'x-device-lng': String(lng),
            'x-whoami-request-id': crypto.randomUUID(),
          },
          cookieJar: jar,
        });
        reqLog.push({ step: r.step, status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
        say(`${r.step}: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
        if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: r.step, status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });
        try {
          const whoResp = JSON.parse(r.body);
          whoHeaders = (whoResp && whoResp.headers && typeof whoResp.headers === 'object') ? whoResp.headers : {};
        } catch (_) {}
        if (!jar.nguestv2) throw Object.assign(new Error('no nguestv2'), { step: 'mint_whoami_food', status: null, kind: 'error' });

        await new Promise(res => setTimeout(res, 150 + Math.random() * 250));
        etr = et('et_mint_zone', [evWhoamiAbConfig(whoHeaders, visitorId, deviceIdfv), evSelectedZone(whoHeaders, visitorId, deviceIdfv)]);
        reqLog.push({ step: 'et_mint_zone', status: etr.status, ms: etr.ms });

        await new Promise(res => setTimeout(res, 800 + Math.random() * 1200));
        r = await curlCallWithRetry('mint_fd_whoami', FD_WHOAMI_URL, { headers: buildFdHeaders(), cookieJar: jar });
        reqLog.push({ step: r.step, status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
        say(`${r.step}: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
        if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: r.step, status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });
        try {
          const fdResp = JSON.parse(r.body);
          if (fdResp && fdResp.headers && typeof fdResp.headers === 'object') {
            for (const [k, v] of Object.entries(fdResp.headers)) {
              if (v != null && typeof v !== 'object') whoHeaders[k] = v;
            }
          }
        } catch (_) {}

        lastMintAt = Date.now();
      } else {
        // --- Area transition: set-location + conditional remint ---
        say(`area ${areaIdx + 1} transition: set delivery location to (${lat},${lng})`);
        await new Promise(res => setTimeout(res, 800 + Math.random() * 700));
        let r = await curlCallWithRetry('area_set_location', 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng', {
          method: 'POST',
          headers: {
            ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae',
            'x-device-lat': String(lat), 'x-device-lng': String(lng),
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ location: { lat, lng }, lang: 'en' }),
          cookieJar: jar,
        });
        reqLog.push({ step: r.step, status: r.status, ms: r.ms });
        say(`${r.step}: ${r.status} in ${r.ms}ms`);
        if (r.status !== 200) throw Object.assign(new Error(`${r.step} ${r.status}`), { step: r.step, status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });

        await new Promise(res => setTimeout(res, 200 + Math.random() * 300));
        const etr = et('et_area_transition', [evLocationFetch(visitorId, deviceIdfv)]);
        reqLog.push({ step: 'et_area_transition', status: etr.status, ms: etr.ms });

        // Dual remint ONLY if token age > TOKEN_REFRESH_AT_MS
        const tokenAge = Date.now() - lastMintAt;
        if (tokenAge > TOKEN_REFRESH_AT_MS) {
          await new Promise(res => setTimeout(res, 400 + Math.random() * 600));
          await dualRemint(`token age ${Math.round(tokenAge/1000)}s > ${TOKEN_REFRESH_AT_MS/1000}s`);
        } else {
          say(`token age ${Math.round(tokenAge/1000)}s — remint not needed`);
        }
      }

      setupMs = Date.now() - areaStart;
      say(`area ${areaIdx + 1} setup done in ${setupMs}ms — pagination (real-pace ${PAGE_GAP_MS_MIN}-${PAGE_GAP_MS_MAX}ms)`);

      // Paginate /search until block or empty.
      // v2.14 corrections to v2.13:
      //   (a) BUG FIX: restore the pre-search sleep (lost in v2.12 refactor).
      //       Target net inter-page gap = PAGE_GAP_MS_MIN..PAGE_GAP_MS_MAX
      //       (telemetry fires AFTER each page cover ~1.5-2s, so we sleep
      //        the gap minus 1.5s here).
      //   (b) NEW: with PAUSE_PROBABILITY, add an extra 3-7s "the user got
      //       distracted" pause on top of the normal gap. Breaks uniform cadence.
      let distractionPauses = 0;
      while (pagesDone < 60) {
        // Only sleep before pages 2+ (page 1 flows straight from mint)
        if (pagesDone > 0) {
          const normalGap = Math.max(0, (PAGE_GAP_MS_MIN - 1500)) + Math.random() * Math.max(0, (PAGE_GAP_MS_MAX - PAGE_GAP_MS_MIN));
          let totalSleep = normalGap;
          if (Math.random() < PAUSE_PROBABILITY) {
            const distraction = PAUSE_MS_MIN + Math.random() * (PAUSE_MS_MAX - PAUSE_MS_MIN);
            totalSleep += distraction;
            distractionPauses++;
            say(`page ${pagesDone + 1}: DISTRACTION pause +${Math.round(distraction)}ms (total gap ${Math.round(totalSleep)}ms)`);
          }
          await new Promise(res => setTimeout(res, totalSleep));
        }

        // Token-age-based remint check: if we've been running > TOKEN_REFRESH_AT_MS since last mint, refresh
        const tokenAge = Date.now() - lastMintAt;
        if (tokenAge > TOKEN_REFRESH_AT_MS) {
          await new Promise(res => setTimeout(res, 500 + Math.random() * 800));
          await dualRemint(`token age ${Math.round(tokenAge/1000)}s at page ${pagesDone} > ${TOKEN_REFRESH_AT_MS/1000}s`);
          await new Promise(res => setTimeout(res, 400 + Math.random() * 600));
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
        const sHeaders = {
          ...base(), 'x-experience': 'food', 'x-locale': 'en-ae',
          'x-device-lat': String(lat), 'x-device-lng': String(lng),
          'x-mp-country': 'ae',
          'x-lat': String(Math.round(lat * 1e7)), 'x-lng': String(Math.round(lng * 1e7)),
          'x-border-enabled': 'true', 'x-rocket-enabled': 'true',
          'Content-Type': 'application/json',
        };
        for (const [k, v] of Object.entries(whoHeaders)) {
          if (v != null && typeof v !== 'object') sHeaders[k] = String(v);
        }
        const sr = curlCall('search', SEARCH_URL, {
          method: 'POST', headers: sHeaders, body: JSON.stringify(searchBody),
          cookieJar: jar,
        });
        reqLog.push({ step: 'search', status: sr.status, ms: sr.ms });
        say(`search page ${pagesDone + 1}: ${sr.status} in ${sr.ms}ms`);
        if (sr.status !== 200) throw Object.assign(new Error(`search ${sr.status}`), { step: 'search', status: sr.status, kind: sr.status === 403 ? 'blocked' : 'error' });
        let json;
        try { json = JSON.parse(sr.body); } catch (_) { break; }
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
        const fresh = outlets.filter(o => !seen.has(o.outletCode));
        // v2.13: record each FRESH outlet with its rank within the full listing.
        // Rank is 1-based across all pages (page 1 outlet 0 → rank 1, page 2 outlet 0 → rank 61, etc.)
        const prevSeenCount = seen.size;
        let rankWithinPage = 0;
        for (const o of outlets) {
          if (!seen.has(o.outletCode)) {
            seen.add(o.outletCode);
            const globalRank = prevSeenCount + rankWithinPage + 1;
            outletCaptures.push({
              ...o,
              _pageNum: pagesDone,
              _rank: globalRank,
            });
            rankWithinPage++;
          }
        }
        cards = seen.size;
        if (outlets.length === 0 || fresh.length === 0 || !json.searchToken) break;
        searchToken = json.searchToken;

        // v2.12 REDUCED telemetry: real app fires impressions for only the ~6 outlets
        // the user actually sees on screen while scrolling, not all 60 outlets in the
        // response. We match: cap at IMPRESSIONS_PER_PAGE outlets, in one small POST.
        if (outlets.length > 0) {
          await new Promise(res => setTimeout(res, 150 + Math.random() * 200));
          const etc = et('et_page_catalog', [evPageCatalog(pagesDone, visitorId, deviceIdfv)]);
          reqLog.push({ step: 'et_page_catalog', status: etc.status, ms: etc.ms });

          const visible = outlets.slice(0, IMPRESSIONS_PER_PAGE);
          if (visible.length > 0) {
            await new Promise(res => setTimeout(res, 500 + Math.random() * 900));
            const events = impressionTripletsBatch(visible, 0, visitorId, deviceIdfv);
            const batchResp = et('et_impressions', events);
            reqLog.push({ step: 'et_impressions', status: batchResp.status, ms: batchResp.ms, event_count: events.length });
          }
          say(`page ${pagesDone} telemetry: catalog + ${visible.length}-outlet impression triplets`);
        }
      }
    } catch (e) {
      fail = e;
      say(`area ${areaIdx + 1} fail @ ${e.step || '?'}: ${e.message}`);
      // v2.12: ANY fail kills the session — don't fire garbage into next area.
      sessionKilled = true;
    }

    // Per-area attempt report
    const result = {
      runner_ip: runnerIp,
      outcome: fail ? (fail.kind || 'error') : 'ok',
      fail_step: fail ? fail.step : null,
      fail_page: fail && fail.step === 'search' ? pagesDone + 1 : null,
      http_status: fail ? (fail.status || null) : null,
      pages_done: pagesDone,
      cards,
      setup_ms: setupMs,
      elapsed_ms: Date.now() - areaStart,
      stop_reason: fail ? null : `v214_area${areaIdx + 1}_done@${pagesDone}_remints=${remintCount}_pauses=${distractionPauses}_gap=${SESSION_PAGE_GAP_MIN}-${SESSION_PAGE_GAP_MAX}_imp=${SESSION_IMPRESSIONS_PER_PAGE}`,
      error: fail ? String(fail.message).slice(0, 500) : null,
      device_id: deviceId,
      cookies: reqLog.slice(-200),
      requests: reqLog.length,
    };

    // v2.13: keep_machine true only if next iteration will actually fire
    // (not killed AND we haven't hit MAX_AREAS yet).
    const willContinue = !sessionKilled && (areaIdx + 1 < MAX_AREAS);
    try {
      const rep = await supa.report({
        test_id: TEST_ID, machine_no: MACHINE, item_id,
        variant_id: variant_id || 'v2.14-pauses',
        result,
        keep_machine: willContinue,
        area_pos: areaIdx + 1,
        outlets: outletCaptures,
      });
      say(`area ${areaIdx + 1} report ok — outcome=${result.outcome} pages=${result.pages_done} cards=${result.cards} outlets_sent=${outletCaptures.length} outlets_inserted=${rep.outlets_inserted} keep_machine=${willContinue}`);
    } catch (e) {
      say(`area ${areaIdx + 1} report FAILED: ${e.message}`);
      process.exitCode = 1;
    }

    areaIdx++;
  } // end outer while

  say(`machine done — areas=${areaIdx} totalElapsed=${Date.now() - sessionStart}ms totalRemints=${remintCount}`);
})().catch(async (e) => {
  console.error(`${prefix} fatal: ${e.message}`);
  try {
    await supa.report({
      test_id: TEST_ID, machine_no: MACHINE, item_id: null, variant_id: 'v2.14-pauses',
      result: { outcome: 'error', error: e.message },
    });
  } catch (_) {}
  process.exit(1);
});
