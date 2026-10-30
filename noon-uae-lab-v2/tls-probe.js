#!/usr/bin/env node
// TLS probe: do the real Noon flow but through curl-impersonate so the TLS
// handshake looks like iOS Safari 17.2 (same stack as the Noon app itself).
// Everything else (HTTP headers, request bodies, order, timing) matches v2.1.
//
// Writes the result through the same noon-lab-claim/noon-lab-report Edge
// Functions as the main worker — so the attempt row lands in the usual place.

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

(async () => {
  const runnerIp = egressIp();
  say(`egress ip ${runnerIp || '(unknown)'}, TLS wrapper=${CURL}`);

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

  try {
    // 1. whoami ecom
    let r = curlCall('mint_whoami', 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami', {
      headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() },
      cookieJar: jar,
    });
    reqLog.push({ step: r.step, status: r.status, ms: r.ms, set_count: (r.setCookies || []).length });
    say(`mint_whoami: ${r.status} in ${r.ms}ms, cookies set: ${(r.setCookies || []).length}`);
    if (r.status !== 200) throw Object.assign(new Error(`mint_whoami ${r.status}`), { step: 'mint_whoami', status: r.status, kind: r.status === 403 ? 'blocked' : 'error' });

    // 2. set-location
    await new Promise(res => setTimeout(res, 1500 + Math.random() * 1000));
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
    setupMs = Date.now() - setupStart;
    say(`mint complete in ${setupMs}ms — attempting /search pagination`);

    // 4. Paginate /search until block or empty
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
    }
  } catch (e) {
    fail = e;
    say(`fail @ ${e.step || '?'}: ${e.message}`);
  }

  const result = {
    runner_ip: runnerIp,
    outcome: fail ? (fail.kind || 'error') : 'ok',
    fail_step: fail ? fail.step : null,
    fail_page: fail && fail.step === 'search' ? pagesDone + 1 : null,
    http_status: fail ? (fail.status || null) : null,
    pages_done: pagesDone,
    cards,
    setup_ms: setupMs,
    elapsed_ms: Date.now() - setupStart,
    stop_reason: fail ? null : `tls_probe_done@${pagesDone}`,
    error: fail ? String(fail.message).slice(0, 500) : null,
    device_id: deviceId,
    cookies: reqLog,
    requests: reqLog.length,
  };

  try {
    const r = await supa.report({ test_id: TEST_ID, machine_no: MACHINE, item_id, variant_id: variant_id || 'v2.5-tls-ios', result });
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
      test_id: TEST_ID, machine_no: MACHINE, item_id: null, variant_id: 'v2.5-tls-ios',
      result: { outcome: 'error', error: e.message },
    });
  } catch (_) {}
  process.exit(1);
});
