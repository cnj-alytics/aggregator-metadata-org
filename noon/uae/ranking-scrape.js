#!/usr/bin/env node
// noon/uae/ranking-scrape.js — PRODUCTION (v3 network layer + v1 output)
// WARM-UP (2026-11-07): before the machine's first mint, a TLS handshake to mobile-sentry.noon.team
//   and GET etracker.noon.com/init, then a 1.8–2.6s pause (scrape_spec.warmup in Supabase).
// MINT v4 (2026-11-01): 1-call mint (st whoami) with zone headers from noon_lab.mint_zone_cache via claim;
//   swap-and-retry on blocked identity calls; no token refresh; area 2 skips set-location when cached.
//   Cache miss → full 4-call mint with swap-and-retry. Lab-tested as noon-mint-lab/ranking-candidate.js.
// HANDOVER (2026-11-01): a machine blocked on a page (after on_search_block.max_attempts swaps) saves a
//   checkpoint (pages done, Noon's next-page searchToken, cards so far) via noon_ranking_checkpoint_save,
//   reports blocked and shuts down. The next machine (new IP) gets the checkpoint from noon_ranking_claim
//   and continues from that page. Noon's searchToken holds no device/session/IP data (verified 2026-11-01).
// ROBUST (2026-11-02): Supabase calls retry for ~60s with backoff (and time out after 20s each) instead of
//   crashing after 3 quick tries; a background heartbeat every 30s; a checkpoint is saved on ANY failed page
//   (503 / no response too), not only on 403/429.
// RANDOMISED (2026-11-04): Supabase (noon_ranking_machine_start) gives each machine a random launch wait
//   (starting machines go out in random batches at random gaps) and a random session style:
//   whoami (1-call mint) | whoami_setloc (1-call mint + set-location) | full (4-call mint, ignores zone cache).
//   noon_ranking_claim returns lat/lng shifted 20-100 m inside the area (tested: rankings unchanged).
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

// ROBUST (2026-11-08): GitHub release downloads failed on and off from 13:01 (502s, 20-30 s
// responses) and every machine that missed it died at boot. Now: a copy bundled in the repo
// (noon/uae/vendor/) is used first; otherwise the download is retried for up to ~3 minutes.
const CI_BUNDLED = require('node:path').join(__dirname, 'vendor', `curl-impersonate-${CI_VERSION}.x86_64-linux-gnu.tar.gz`);

function ensureCurl() {
  for (const c of [WRAPPER, `/opt/ci/${WRAPPER}`, `${CI_DIR}/${WRAPPER}`]) {
    try { execFileSync(c, ['--version'], { stdio: 'ignore', timeout: 10000 }); return c; } catch (_) {}
  }
  fs.mkdirSync(CI_DIR, { recursive: true });
  const tgz = `${CI_DIR}/ci.tar.gz`;
  if (fs.existsSync(CI_BUNDLED)) {
    fs.copyFileSync(CI_BUNDLED, tgz);
  } else {
    let lastErr = null;
    for (let attempt = 1; attempt <= 6; attempt++) {
      try {
        execFileSync('curl', ['-sSL', '--fail', '--connect-timeout', '15', '--max-time', '45', '-o', tgz, CI_URL], { timeout: 55000 });
        lastErr = null; break;
      } catch (e) {
        lastErr = e;
        console.error(`curl-impersonate download attempt ${attempt}/6 failed: ${String(e.message).slice(0, 120)}`);
        if (attempt < 6) execFileSync('sleep', [String(Math.min(5 * attempt, 20) + Math.floor(Math.random() * 5))]);
      }
    }
    if (lastErr) throw lastErr;
  }
  execFileSync('tar', ['-xzf', tgz, '-C', CI_DIR], { timeout: 60000 });
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
  // WARM-UP (2026-11-07): what the app does at launch before its first whoami.
  // TLS handshake to mobile-sentry (no request sent) + GET etracker /init, then a short pause.
  warmup: { enabled: false, sentry: true, etracker_init: true, gap_ms_min: 1800, gap_ms_max: 2600 },
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

// APP VERSION (2026-11-05): build / app version / User-Agent come from Supabase
// noon_ranking_settings.scrape_spec.app — update them there when the Noon app updates.
// These are only the fallback if Supabase has no (or an invalid) value.
const APP_DEFAULT = { build: '23440', version: '5.261104', ua_tail: 'CFNetwork/3896.100.1.2.1 Darwin/27.0.0' };
let UA = `noon/${APP_DEFAULT.build} ${APP_DEFAULT.ua_tail}`;
let BUILD = APP_DEFAULT.build;
function applyAppIdentity(app) {
  const a = (app && typeof app === 'object') ? app : {};
  const build = /^[0-9]{4,7}$/.test(String(a.build ?? '')) ? String(a.build) : APP_DEFAULT.build;
  const version = /^[0-9]{1,3}(\.[0-9]{1,8}){1,3}$/.test(String(a.version ?? '')) ? String(a.version) : APP_DEFAULT.version;
  const tail = /^[A-Za-z0-9 ./_()-]{1,80}$/.test(String(a.ua_tail ?? '')) ? String(a.ua_tail) : APP_DEFAULT.ua_tail;
  BUILD = build; UA = `noon/${build} ${tail}`;
  NOON_APP.ab = build; NOON_APP.av = version; FOOD_APP.ab = build; FOOD_APP.av = version;
  return { build, version, ua: UA };
}
const WHOAMI_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami';
const WHOAMI_FOOD_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami?experience=food&select_nearby=1';
const SET_LOCATION_URL = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng';
const FD_WHOAMI_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/customer/whoami';
const SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search';
const ETRACKER_URL = 'https://etracker.noon.com/track/v2/';
const ETRACKER_INIT_URL = 'https://etracker.noon.com/init';
const SENTRY_HOST = 'mobile-sentry.noon.team';

// Warm-up: TCP+TLS handshake to mobile-sentry like the app's first connection at launch.
// No HTTP request is sent. Never throws; returns 'ok' or a short error.
function sentryHandshake(timeoutMs = 10000) {
  return new Promise((resolve) => {
    let done = false;
    const finish = (v, s) => { if (done) return; done = true; try { s && s.destroy(); } catch (_) {} resolve(v); };
    try {
      const s = require('node:tls').connect({ host: SENTRY_HOST, port: 443, servername: SENTRY_HOST, ALPNProtocols: ['h2', 'http/1.1'] });
      s.setTimeout(timeoutMs, () => finish('err:timeout', s));
      s.once('secureConnect', () => finish('ok', s));
      s.once('error', (e) => finish(`err:${String(e.code || e.message).slice(0, 40)}`, s));
    } catch (e) { finish(`err:${String(e.message).slice(0, 40)}`); }
  });
}

// -- etracker events (v3 verbatim — schemas from the iPhone Proxyman trace) ---

function deviceBlock(idfv) {
  return { dm: 'Apple iPhone 17 Pro Max', dos: 'ios 27.0.1', d: 'Handset', sr: '1320x2868', vps: '1320x2868', scl: 3,
    idfa: '00000000-0000-0000-0000-000000000000', idfv };
}
function evt(ev, em, visitorId, idfv) {
  return { em, ev, ts: Date.now(), pc: 'noon', vid: visitorId, lo: 'en-ae', pf: 'ios', d: deviceBlock(idfv) };
}
const NOON_APP = { mc: 'noon', an: 'noon', av: APP_DEFAULT.version, ab: APP_DEFAULT.build, mpe: 'noon' };
const FOOD_APP = { mc: 'food', an: 'noon', av: APP_DEFAULT.version, ab: APP_DEFAULT.build, mpe: 'food' };

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
  // Matched to real app Proxyman trace: pn=Home_page, isp=false, no src/wn/wtc/otg
  return evt('outlet_impression', { aid: o.outletCode, wid: `search_outlet/${idx}`, oc: o.outletCode, ix: `0/${idx}`,
    df: o.distance || 5.0, fav: false, isp: false, sla: o.sla || '30-40',
    curl: '/search?type=outlet', pn: 'Home_page', ...FOOD_APP }, v, i);
}
function evOutletImpressionV2(o, idx, v, i) {
  // Matched to real app Proxyman trace: no isp field
  return evt('outlet_impression_v2', { pg: 'SR', wt: `SR/search_results/0/search_results_tab/${idx}/ROW/${idx}/ITM/0`,
    ul: '/search?type=outlet', cf: '{}', oc: o.outletCode, df: o.distance || 5.0, sla: o.sla || '30-40', ...FOOD_APP }, v, i);
}
// promotion_impression removed — real app does not send it during scroll pagination
function impressionPairBatches(outlets, startIdx, v, i) {
  // Real app sends outlet_impression_v2 + outlet_impression in pairs, staggered across
  // 3-4 separate etracker POSTs per page (5 events each), not one big batch.
  const batches = [];
  const pairs = [];
  outlets.forEach((o, k) => {
    const idx = startIdx + k;
    pairs.push(evOutletImpressionV2(o, idx, v, i), evOutletImpression(o, idx, v, i));
  });
  for (let j = 0; j < pairs.length; j += 5) {
    batches.push(pairs.slice(j, j + 5));
  }
  return batches;
}

// -- Supabase -----------------------------------------------------------------

// ROBUST: keep retrying a busy/unreachable database for ~60s (1,2,4,8,15,15,15s pauses) instead of
// crashing after 3 quick tries. Each call times out after 20s so a stuck connection can't hang the machine.
// A 4xx (except 408/429) is a real error in the request and is not retried.
async function rpc(name, params) {
  const pauses = [1000, 2000, 4000, 8000, 15000, 15000, 15000];
  for (let attempt = 1; ; attempt++) {
    let status = null;
    try {
      const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
        signal: AbortSignal.timeout(20000),
      });
      status = resp.status;
      const text = await resp.text();
      if (!resp.ok) throw new Error(`rpc(${name}) ${resp.status}: ${text.slice(0, 500)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      const permanent = status != null && status >= 400 && status < 500 && status !== 408 && status !== 429;
      if (permanent || attempt > pauses.length) throw e;
      say(`rpc(${name}) attempt ${attempt} failed: ${e.message.slice(0, 160)} - retrying in ${pauses[attempt - 1] / 1000}s`);
      heartbeat(true);
      await sleep(pauses[attempt - 1] + Math.random() * 500);
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
// STOPPED (2026-11-08): Supabase closed this machine (fallback took over, deadline, run closed). The area is no
// longer ours, so the machine stops without saving or reporting it.
let STOPPED = false;
function heartbeat(force = false) {
  if (!force && Date.now() - lastBeat < 60000) return;
  lastBeat = Date.now();
  fetch(`${SUPABASE_URL}/rest/v1/noon_ranking_run_machine?run_id=eq.${RUN_ID}&machine_no=eq.${MACHINE}&select=status`, {
    method: 'PATCH',
    headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=representation' },
    body: JSON.stringify({ last_seen_at: new Date().toISOString() }),
    signal: AbortSignal.timeout(5000),
  }).then(async (r) => {
    if (!r.ok) { say(`heartbeat HTTP ${r.status}`); return; }
    const rows = await r.json().catch(() => null);
    const st = Array.isArray(rows) && rows[0] ? rows[0].status : null;
    if (st && st !== 'working' && st !== 'dispatched' && !STOPPED) { STOPPED = true; say(`machine closed by Supabase (status=${st}) — stopping`); }
  }).catch((e) => say(`heartbeat failed: ${e.message}`));
}
// Hard stop at the run deadline (:55): stop paging this long before it so the partial save lands in time.
const DEADLINE_MARGIN_MS = 20000;

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

// HANDOVER: keep only the outlet fields parseCards / isRestaurant read, so checkpoints stay small.
function trimOutlet(o) {
  const pick = (x, keys) => (x && typeof x === 'object') ? Object.fromEntries(keys.filter((k) => x[k] !== undefined).map((k) => [k, x[k]])) : undefined;
  const t = {};
  for (const k of ['outletCode', 'operatingStatusCode', 'operatingType', 'tag', 'miniTag', 'extendedTag', 'widgetGroup',
                   'isPersonalized', 'isNearAndQuick', 'name', 'linkUrl', 'cuisines', 'restaurantCode', 'price', 'distance',
                   'isVip', 'vipMsg']) if (o[k] !== undefined) t[k] = o[k];
  const rt = pick(o.rating, ['score', 'count', 'title', 'displayCount']); if (rt) t.rating = rt;
  const d = pick(o.delivery, ['fee', 'originalFee', 'minOrder', 'time']); if (d) t.delivery = d;
  const m = pick(o.media, ['heroImage', 'logoImage']); if (m) t.media = m;
  return t;
}

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
  // ROBUST: background heartbeat every 30s, whatever the machine is doing (unref: never keeps the process alive).
  setInterval(() => heartbeat(true), 30000).unref();
  // RANDOMISED: per-machine session style and launch wait chosen by Supabase.
  const SESSION_STYLE = ['whoami', 'whoami_setloc', 'full'].includes(start.session_style) ? start.session_style : 'whoami';
  const launchWait = Math.max(0, Math.min(Number(start.launch_wait_s) || 0, 900));
  say(`session style=${SESSION_STYLE} | launch wait ${launchWait}s`);
  if (launchWait > 0) await sleep(launchWait * 1000);
  const scrapeDate = start.scrape_date, scrapeHour = start.scrape_hour, dryRun = !!start.dry_run;

  // Spec -> session-level values (v3 verbatim resolution)
  const SPEC = mergeSpec(DEFAULT_SPEC, await rpc('noon_ranking_scrape_spec', {}));
  MAX_AREAS = Number(SPEC.max_areas ?? 2);
  const APP_ID = applyAppIdentity(SPEC.app);
  say(`app identity: build=${APP_ID.build} version=${APP_ID.version} ua="${APP_ID.ua}"`);
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

  // CANDIDATE: identity call = production's 1 plain retry, then swap ids + pause + retry on 403/429.
  const MINT_RETRY = (start && start.mint_retry) || { max_attempts: 6, backoff_ms: 6000, swap: 'both' };
  let mintSwaps = 0;
  const mintCall = async (step, url, optsFn) => {
    const maxA = Number(MINT_RETRY.max_attempts) || 0;
    for (let a = 0; ; a++) {
      const r = await curlCallWithRetry(a === 0 ? step : `${step}_swap${a}`, url, optsFn());
      say(`${r.step}: ${r.status} in ${r.ms}ms dev=${shortId(deviceId)}`);
      if (r.status === 200 || a >= maxA || (r.status !== 403 && r.status !== 429)) { r.baseStep = step; return r; }
      const devBefore = deviceId;
      const mode = MINT_RETRY.swap || 'both';
      if (mode === 'device' || mode === 'both') deviceId = crypto.randomUUID().toUpperCase();
      if (mode === 'visitor' || mode === 'both') visitorId = crypto.randomUUID().toLowerCase();
      mintSwaps++; identityRotations++;
      say(`  MINT SWAP ${a + 1}/${maxA} at ${step}: device ${shortId(devBefore)} -> ${shortId(deviceId)}`);
      logReq({ step: `mint_swap_${step}`, dev: `${shortId(devBefore)}>${shortId(deviceId)}` });
      await sleep((Number(MINT_RETRY.backoff_ms) || 6000) + Math.random() * 1000);
    }
  };
  const mintFail = (r) => Object.assign(new Error(`${r.step} ${r.status}`), { step: r.baseStep || r.step, status: r.status });

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
    const marginMs = claim.deadline_margin_s != null ? Number(claim.deadline_margin_s) * 1000 : DEADLINE_MARGIN_MS;  // settings.worker_deadline_margin_s
    const stopAtMs = claim.deadline_at ? Date.parse(claim.deadline_at) - marginMs : null;
    const pastDeadline = () => stopAtMs != null && Date.now() >= stopAtMs;
    if (claim.gap_seconds > 0) await sleep(claim.gap_seconds * 1000);
    if (STOPPED) { exitReason = 'stopped'; break; }
    if (pastDeadline()) {
      const rep = await rpc('noon_ranking_report', { p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: claim.noon_area_id, p_result: { status: 'deadline' } });
      say(`deadline before area ${claim.area_name || claim.noon_area_id} started — reported: ${JSON.stringify(rep)}`);
      exitReason = 'deadline'; break;
    }
    say(`=== AREA ${areaIdx + 1}/${MAX_AREAS}: ${areaName} id=${areaId} (${lat},${lng}) ===`);

    const areaStart = Date.now();
    AREA_LOG = newAreaLog();
    const zoneCached = SESSION_STYLE !== 'full' && !!(claim.zone_headers && typeof claim.zone_headers === 'object' && Object.keys(claim.zone_headers).length);
    let harvestedZone = null;
    const swapsBefore = mintSwaps;
    const mintStats = () => {
      const idl = AREA_LOG.log.filter((e) => /^(mint_|area_set_location)/.test(e.step) && !/^mint_swap_/.test(e.step));
      return { zone_cached: zoneCached, mint_calls: idl.length, mint_403: idl.filter((e) => e.st === 403 || e.st === 429).length,
               mint_swaps: mintSwaps - swapsBefore, ...(harvestedZone ? { zone_headers: harvestedZone } : {}) };
    };
    const deviceAtStart = deviceId;
    const reqBefore = NOON_REQ;
    const rotBefore = identityRotations, recBefore = recoveryAttempts;
    let pagesDone = 0;
    let searchToken = null;
    let distractionPauses = 0;
    let stopReason = null;
    const seen = new Set();
    const captures = [];

    // HANDOVER: resume from a checkpoint saved by a blocked machine (same run + area).
    const cp = (claim.checkpoint && typeof claim.checkpoint === 'object') ? claim.checkpoint : null;
    let resumedFrom = 0;
    if (cp && Number(cp.pages_done) > 0 && cp.search_token && Array.isArray(cp.captures)) {
      for (const o of cp.captures) if (o && o.outletCode && !seen.has(o.outletCode)) { seen.add(o.outletCode); captures.push(o); }
      pagesDone = Number(cp.pages_done); searchToken = cp.search_token; resumedFrom = pagesDone;
      say(`RESUME from checkpoint: page ${pagesDone + 1}, ${captures.length} cards carried over (handover #${cp.handovers || 1})`);
    }

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
        // --- WARM-UP before the first mint (spec.warmup, controlled in Supabase) ---
        if (SPEC.warmup && SPEC.warmup.enabled) {
          const w0 = Date.now();
          let sentry = 'skipped', init = 'skipped';
          if (SPEC.warmup.sentry !== false) {
            sentry = await sentryHandshake();
            logReq({ step: 'warmup_sentry', st: sentry === 'ok' ? 'ok' : null, ms: Date.now() - w0, err: sentry === 'ok' ? undefined : sentry });
          }
          if (SPEC.warmup.etracker_init !== false) {
            const ri = curlCall('warmup_etracker_init', ETRACKER_INIT_URL, {
              headers: { 'User-Agent': UA, Accept: '*/*', Connection: 'keep-alive' }, cookieJar: jar });
            init = ri.status;
          }
          const gap = randInt(Number(SPEC.warmup.gap_ms_min) || 1800, Math.max(Number(SPEC.warmup.gap_ms_min) || 1800, Number(SPEC.warmup.gap_ms_max) || 2600));
          say(`WARM-UP: sentry=${sentry} etracker_init=${init} | pause ${gap}ms before mint`);
          await sleep(gap);
        }
        // --- 4-call mint with telemetry (v3 verbatim) ---
        say(`MINT start (${zoneCached ? '1-call, zone headers from cache' : 'FULL — no cached zone headers'}): device=${shortId(deviceId)} visitor=${shortId(visitorId)} ip=${RUNNER_IP || '?'}`);
        let r = await mintCall('mint_whoami', WHOAMI_URL, () => ({
          headers: { ...base(), 'x-experience': 'ecom', 'x-whoami-request-id': crypto.randomUUID() }, cookieJar: jar }));
        if (r.status !== 200) throw mintFail(r);

        await sleep(300 + Math.random() * 300);
        et('et_mint_launch', [evLaunch(visitorId, deviceIdfv), evLaunchInfo(visitorId, deviceIdfv), evAppForeground(visitorId, deviceIdfv)]);

        if (zoneCached) {
          if (!jar.nguestv2) throw Object.assign(new Error('mint produced no nguestv2'), { step: 'mint_whoami', status: null });
          whoHeaders = { ...claim.zone_headers };
          lastMintAt = Date.now();
          say(`MINT done (1 call): nguestv2 set, zone=${whoHeaders['x-food-zonecode'] || '-'} in ${Math.round((Date.now() - areaStart) / 1000)}s`);
          if (SESSION_STYLE === 'whoami_setloc') {
            // RANDOMISED style: also set the delivery location, like the app does after launch.
            await sleep(1200 + Math.random() * 800);
            const rl = await mintCall('mint_set_location', SET_LOCATION_URL, () => ({
              method: 'POST',
              headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'Content-Type': 'application/json' },
              body: JSON.stringify({ location: { lat, lng }, lang: 'en' }), cookieJar: jar }));
            if (rl.status !== 200) throw mintFail(rl);
            await sleep(200 + Math.random() * 300);
            et('et_mint_location', [evLocationFetch(visitorId, deviceIdfv), evPageHome(visitorId, deviceIdfv)]);
          }
        } else {

        await sleep(1200 + Math.random() * 800);
        r = await mintCall('mint_set_location', SET_LOCATION_URL, () => ({
          method: 'POST',
          headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'Content-Type': 'application/json' },
          body: JSON.stringify({ location: { lat, lng }, lang: 'en' }), cookieJar: jar }));
        if (r.status !== 200) throw mintFail(r);

        await sleep(200 + Math.random() * 300);
        et('et_mint_location', [evLocationFetch(visitorId, deviceIdfv), evPageHome(visitorId, deviceIdfv)]);

        await sleep(400 + Math.random() * 300);
        r = await mintCall('mint_whoami_food', WHOAMI_FOOD_URL, () => ({
          headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'x-whoami-request-id': crypto.randomUUID() },
          cookieJar: jar }));
        if (r.status !== 200) throw mintFail(r);
        try { const w = JSON.parse(r.body); whoHeaders = (w && w.headers && typeof w.headers === 'object') ? w.headers : {}; } catch (_) {}
        if (!jar.nguestv2) throw Object.assign(new Error('mint produced no nguestv2'), { step: 'mint_whoami_food', status: null });

        await sleep(150 + Math.random() * 250);
        et('et_mint_zone', [evWhoamiAbConfig(whoHeaders, visitorId, deviceIdfv), evSelectedZone(whoHeaders, visitorId, deviceIdfv)]);

        await sleep(800 + Math.random() * 1200);
        r = await mintCall('mint_fd_whoami', FD_WHOAMI_URL, () => ({ headers: buildFdHeaders(), cookieJar: jar }));
        if (r.status !== 200) throw mintFail(r);
        mergeWho(r.body);
        lastMintAt = Date.now();
        harvestedZone = Object.fromEntries(Object.entries(whoHeaders).filter(([, v]) => v != null && typeof v !== 'object'));
        say(`MINT done (full): nguestv2 set, zone=${whoHeaders['x-food-zonecode'] || '-'} in ${Math.round((Date.now() - areaStart) / 1000)}s`);
        }
      } else if (zoneCached) {
        // CANDIDATE: same session, new location — no identity call, no refresh.
        whoHeaders = { ...claim.zone_headers };
        say(`AREA TRANSITION (${SESSION_STYLE === 'whoami_setloc' ? 'with' : 'no'} set-location): device=${shortId(deviceId)} zone=${whoHeaders['x-food-zonecode'] || '-'} token age ${Math.round((Date.now() - lastMintAt) / 1000)}s`);
        await sleep(800 + Math.random() * 700);
        if (SESSION_STYLE === 'whoami_setloc') {
          const rl = await mintCall('area_set_location', SET_LOCATION_URL, () => ({
            method: 'POST',
            headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'Content-Type': 'application/json' },
            body: JSON.stringify({ location: { lat, lng }, lang: 'en' }), cookieJar: jar }));
          if (rl.status !== 200) throw mintFail(rl);
          await sleep(200 + Math.random() * 300);
        }
        et('et_area_transition', [evLocationFetch(visitorId, deviceIdfv)]);
      } else {
        say(`AREA TRANSITION: same session, device=${shortId(deviceId)} visitor=${shortId(visitorId)}, token age ${Math.round((Date.now() - lastMintAt) / 1000)}s`);
        // --- Area transition: set-location + conditional remint (v3 verbatim) ---
        await sleep(800 + Math.random() * 700);
        const r = await mintCall('area_set_location', SET_LOCATION_URL, () => ({
          method: 'POST',
          headers: { ...base(), 'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': String(lat), 'x-device-lng': String(lng), 'Content-Type': 'application/json' },
          body: JSON.stringify({ location: { lat, lng }, lang: 'en' }), cookieJar: jar }));
        if (r.status !== 200) throw mintFail(r);
        await sleep(200 + Math.random() * 300);
        et('et_area_transition', [evLocationFetch(visitorId, deviceIdfv)]);
        // CANDIDATE: no token refresh.
      }

      // --- Pagination (v3 verbatim, page cap from Supabase claim) ---
      while (pagesDone < maxPages) {
        heartbeat();
        if (STOPPED) break;
        if (pastDeadline()) { stopReason = `deadline@${pagesDone}`; break; }
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
        if (false && SPEC.token_refresh.enabled && age > TOKEN_REFRESH_AT_MS) { // CANDIDATE: token refresh disabled
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
        if (sr.status !== 200) {
          // HANDOVER: save where we are so the next machine (new IP) continues from this page.
          if (searchToken && pagesDone > 0) {  // ROBUST: checkpoint on ANY failed page (403/429/503/no response)
            try {
              const slim = captures.map(trimOutlet);
              const cs = await rpc('noon_ranking_checkpoint_save', { p_run_id: RUN_ID, p_area_id: areaId, p_machine_no: MACHINE,
                p_pages_done: pagesDone, p_search_token: searchToken, p_captures: slim });
              say(`HANDOVER: page ${pagesDone + 1} failed (${sr.status}) — checkpoint saved (${pagesDone} pages, ${slim.length} cards): ${JSON.stringify(cs)}`);
            } catch (ce) { say(`HANDOVER checkpoint save failed: ${ce.message}`); }
          }
          throw Object.assign(new Error(`search ${sr.status}${pagesDone > 0 ? ` — handover at page ${pagesDone + 1}` : ''}`), { step: 'search', status: sr.status });
        }

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

        // Telemetry after each page — matched to real app Proxyman trace:
        //   POST 1: page_catalog (1 event)
        //   POST 2-4: outlet_impression_v2 + outlet_impression pairs (5 events each)
        //   Each POST staggered 200-500ms apart
        await sleep(150 + Math.random() * 200);
        et('et_page_catalog', [evPageCatalog(pagesDone, visitorId, deviceIdfv)]);
        const visible = outlets.slice(0, IMPRESSIONS_PER_PAGE);
        if (visible.length > 0) {
          const batches = impressionPairBatches(visible, 0, visitorId, deviceIdfv);
          for (let bi = 0; bi < batches.length; bi++) {
            await sleep(200 + Math.random() * 300);
            et(`et_imp_${bi}`, batches[bi]);
          }
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
        ...auditVisibility(), ...mintStats(),
      });
      const rep = await rpc('noon_ranking_report', {
        p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: areaId,
        p_result: { status: kind, http_status: e.status || null, error: String(e.message).slice(0, 500) },
      });
      say(`reported ${kind}: action=${rep && rep.action} reason=${(rep && rep.reason) || '-'}`);
      exitReason = kind;
      break;
    }

    if (STOPPED) { say(`stopped by Supabase at page ${pagesDone} — area not saved`); exitReason = 'stopped'; break; }
    if (String(stopReason || '').startsWith('deadline') && captures.length === 0) {
      const rep = await rpc('noon_ranking_report', { p_run_id: RUN_ID, p_machine_no: MACHINE, p_area_id: areaId, p_result: { status: 'deadline' } });
      say(`deadline before the first page of ${areaName} — reported: ${JSON.stringify(rep)}`);
      exitReason = 'deadline'; break;
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
        stop_reason: stopReason,
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
      stop_reason: `${stopReason} rot=${identityRotations - rotBefore} rec=${recoveryAttempts - recBefore} remints=${remintCount} pauses=${distractionPauses} gap=${PAGE_GAP_MS_MIN}-${PAGE_GAP_MS_MAX} imp=${IMPRESSIONS_PER_PAGE} resumed_from=${resumedFrom}`,
      ...auditVisibility(), ...mintStats(),
    });
    totalAreas++; totalRows += parsed.rows.length; totalDropped += parsed.dropped;
    say(`${areaName} ok: ${captures.length} cards, ${parsed.rows.length} rows (${parsed.dropped} non-food dropped), ${pagesDone}p, ${Math.round(elapsed / 1000)}s, recoveries=${recoveryAttempts - recBefore} | ${stopReason}${resumedFrom ? ` (resumed from page ${resumedFrom + 1})` : ''} | saved=${savedAnalysis} pending=${savedPending} -> ${rep && rep.action}`);

    if (String(stopReason || '').startsWith('deadline')) { exitReason = 'deadline'; break; }
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
