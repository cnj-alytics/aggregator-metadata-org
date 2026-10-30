#!/usr/bin/env node
// Mint-only probe (isolation test).
// Runs the same v2.7 mint pattern (TLS impersonate + mint-phase etracker events)
// but STOPS after mint_whoami_food. No search, no pagination.
// Purpose: measure the baseline success rate of the mint flow across many IPs
// in parallel, isolating mint failures from search failures.

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

const prefix = `[tls t${TEST_ID} m${MACHINE}]`;
const say = (m) => console.log(`${prefix} ${m}`);

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
  say(`egress ip ${runnerIp || '(unknown)'}, TLS wrapper=${CURL} (v2.7 + full telemetry)`);

  // Claim an area to probe
  const claim = await supa.claim({
    test_id: TEST_ID, machine_no: MACHINE, github_run_id: GH_RUN_ID, runner_ip: runnerIp,
  });
  if (!claim || claim.stop) {
    say(`nothing to do — ${claim && claim.reason}`);
    return;
  }
  const { item_id, area_name, lat, lng, variant_id, identity } = claim;
  say(`area ${area_name} (${lat},${lng}) variant=${variant_id}`);

  // Minimal mint: 3 calls, iPhone-matched headers, cookie jar maintained manually.
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

  let fail = null;
  let pagesDone = 0;
  let cards = 0;
  const setupStart = Date.now();
  let setupMs = null;
  let searchToken = null;
  let whoHeaders = {};
  const seen = new Set();

  // deviceIdfv (uppercase UUID) used in every etracker event's d.idfv
  const deviceIdfv = (deviceId.includes('-') ? deviceId.toUpperCase() : crypto.randomUUID().toUpperCase());

  // helper for firing etracker (needs access to base/jar/visitorId/deviceIdfv)
  const sessionCtx = { base, jar };
  const et = (step, events) => fireEtracker(sessionCtx, step, events);

  try {
    // 1. whoami ecom
    let r = curlCall('mint_whoami', 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami', {
      headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() },
      cookieJar: jar,
    });
    reqLog.push({ step: r.step, status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
    say(`mint_whoami: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
    if (r.status !== 200) throw Object.assign(new Error(`mint_whoami ${r.status}`), { step: 'mint_whoami', status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });

    // --- v2.7 mint-phase telemetry: launch sequence ---
    await new Promise(res => setTimeout(res, 300 + Math.random() * 300));
    let etr = et('et_mint_launch', [
      evLaunch(visitorId, deviceIdfv),
      evLaunchInfo(visitorId, deviceIdfv),
      evAppForeground(visitorId, deviceIdfv),
    ]);
    reqLog.push({ step: 'et_mint_launch', status: etr.status, ms: etr.ms });
    say(`et_mint_launch: ${etr.status} in ${etr.ms}ms`);

    // 2. set-location
    await new Promise(res => setTimeout(res, 1200 + Math.random() * 800));
    r = curlCall('mint_set_location', 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng', {
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
    say(`mint_set_location: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
    if (r.status !== 200) throw Object.assign(new Error(`mint_set_location ${r.status}`), { step: 'mint_set_location', status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });

    // --- v2.7 mint-phase telemetry: location + home-page view ---
    await new Promise(res => setTimeout(res, 200 + Math.random() * 300));
    etr = et('et_mint_location', [
      evLocationFetch(visitorId, deviceIdfv),
      evPageHome(visitorId, deviceIdfv),
    ]);
    reqLog.push({ step: 'et_mint_location', status: etr.status, ms: etr.ms });
    say(`et_mint_location: ${etr.status} in ${etr.ms}ms`);

    // 3. whoami food
    await new Promise(res => setTimeout(res, 400 + Math.random() * 300));
    r = curlCall('mint_whoami_food', 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1', {
      headers: {
        ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae',
        'x-device-lat': String(lat), 'x-device-lng': String(lng),
        'x-whoami-request-id': crypto.randomUUID(),
      },
      cookieJar: jar,
    });
    reqLog.push({ step: r.step, status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
    say(`mint_whoami_food: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
    if (r.status !== 200) throw Object.assign(new Error(`mint_whoami_food ${r.status}`), { step: 'mint_whoami_food', status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });
    try {
      const whoResp = JSON.parse(r.body);
      whoHeaders = (whoResp && whoResp.headers && typeof whoResp.headers === 'object') ? whoResp.headers : {};
    } catch (_) {}
    if (!jar.nguestv2) throw Object.assign(new Error('no nguestv2'), { step: 'mint_whoami_food', status: null, kind: 'error' });

    // --- v2.7 mint-phase telemetry: zone selection + ab config (uses whoami-food response) ---
    await new Promise(res => setTimeout(res, 150 + Math.random() * 250));
    etr = et('et_mint_zone', [
      evWhoamiAbConfig(whoHeaders, visitorId, deviceIdfv),
      evSelectedZone(whoHeaders, visitorId, deviceIdfv),
    ]);
    reqLog.push({ step: 'et_mint_zone', status: etr.status, ms: etr.ms });
    say(`et_mint_zone: ${etr.status} in ${etr.ms}ms`);

    setupMs = Date.now() - setupStart;
    say(`mint complete in ${setupMs}ms — STOPPING here (mint-only probe)`);
  } catch (e) {
    fail = e;
    say(`fail @ ${e.step || '?'}: ${e.message}`);
  }

  // (search pagination intentionally omitted — this is a mint-only probe)
  if (false) {
    const SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';
    while (pagesDone < 60) {
      await new Promise(res => setTimeout(res, 700 + Math.random() * 500));
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
      r = curlCall('search', SEARCH_URL, {
        method: 'POST', headers: sHeaders, body: JSON.stringify(searchBody),
        cookieJar: jar,
      });
      reqLog.push({ step: 'search', status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
      say(`search page ${pagesDone + 1}: ${r.status} in ${r.ms}ms`);
      if (r.status !== 200) throw Object.assign(new Error(`search ${r.status}`), { step: 'search', status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });
      let json;
      try { json = JSON.parse(r.body); } catch (_) { break; }
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
      for (const o of fresh) seen.add(o.outletCode);
      cards = seen.size;
      if (outlets.length === 0 || fresh.length === 0 || !json.searchToken) break;
      searchToken = json.searchToken;

      // --- v2.7: fire catalog-page view + full impression triplets ---
      // Real app per card: outlet_impression + outlet_impression_v2 +
      // promotion_impression (all three, same timestamp). We send 3 batches
      // of 20 outlets each (60 outlets × 3 events = 180 events total,
      // distributed across the scroll time).
      if (outlets.length > 0) {
        // Catalog page view event (user is on /search/results screen)
        await new Promise(res => setTimeout(res, 150 + Math.random() * 200));
        let etc = et('et_page_catalog', [evPageCatalog(pagesDone, visitorId, deviceIdfv)]);
        reqLog.push({ step: 'et_page_catalog', status: etc.status, ms: etc.ms });

        // 3 batches of 20 outlets each
        const slices = [outlets.slice(0, 20), outlets.slice(20, 40), outlets.slice(40, 60)];
        for (let b = 0; b < slices.length; b++) {
          const slice = slices[b];
          if (slice.length === 0) continue;
          await new Promise(res => setTimeout(res, 500 + Math.random() * 900));
          const events = impressionTripletsBatch(slice, b * 20, visitorId, deviceIdfv);
          const batchResp = et(`et_impressions_b${b+1}`, events);
          reqLog.push({ step: `et_impressions_b${b+1}`, status: batchResp.status, ms: batchResp.ms, event_count: events.length });
        }
        say(`page ${pagesDone} telemetry: catalog + 3 impression triplet batches`);
      }
    }
  }

  const result = {
    runner_ip: runnerIp,
    outcome: fail ? (fail.kind || 'error') : 'ok',
    fail_step: fail ? fail.step : null,
    fail_page: null,
    http_status: fail ? (fail.status || null) : null,
    pages_done: 0,
    cards: 0,
    setup_ms: setupMs,
    elapsed_ms: Date.now() - setupStart,
    stop_reason: fail ? null : 'mint_probe_ok',
    error: fail ? String(fail.message).slice(0, 500) : null,
    device_id: deviceId,
    cookies: reqLog,
    requests: reqLog.length,
  };

  try {
    const r = await supa.report({ test_id: TEST_ID, machine_no: MACHINE, item_id, variant_id: variant_id || 'mint-probe', result });
    say(`report ok (${r.worker_version})`);
  } catch (e) {
    say(`report FAILED: ${e.message}`);
    process.exitCode = 1;
  }
  say(`done — outcome=${result.outcome} pages=${result.pages_done} cards=${result.cards} elapsed=${result.elapsed_ms}ms`);
})().catch(async (e) => {
  console.error(`${prefix} fatal: ${e.message}`);
  try {
    await supa.report({
      test_id: TEST_ID, machine_no: MACHINE, item_id: null, variant_id: 'mint-probe',
      result: { outcome: 'error', error: e.message },
    });
  } catch (_) {}
  process.exit(1);
});
