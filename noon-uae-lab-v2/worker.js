#!/usr/bin/env node
// noon-uae-lab-v2/worker.js
//
// Thin executor. Zero decision logic. Does exactly three things:
//   1. Ask Supabase (noon-lab-claim Edge Function) what to do for this machine.
//   2. Execute the returned spec against Noon's API.
//   3. Hand the result back (noon-lab-report Edge Function).
//
// All variant selection, timing randomisation, pi-digit routing and
// identity picking live in Supabase. Change them there, redeploy nothing.
//
// Env (set by the workflow): SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
//                            TEST_ID, MACHINE_NO, GITHUB_RUN_ID.

'use strict';

const undici = require('undici');
const supa = require('./supabase.js');
const { Session, mint, paginate, sleep, log } = require('./noon-session.js');

const TEST_ID   = Number(process.env.TEST_ID);
const MACHINE   = Number(process.env.MACHINE_NO);
const GH_RUN_ID = process.env.GITHUB_RUN_ID || '';

if (!TEST_ID || !MACHINE) {
  console.error('TEST_ID and MACHINE_NO are required');
  process.exit(1);
}

const prefix = `[v2 t${TEST_ID} m${MACHINE}]`;
const say = (m) => console.log(`${prefix} ${m}`);

(async () => {
  // Record our egress IP for the lab (so dashboards show where this ran).
  let runnerIp = null;
  try {
    const r = await undici.fetch('https://api.ipify.org', { signal: AbortSignal.timeout(8000) });
    runnerIp = (await r.text()).trim();
    say(`egress ip ${runnerIp}`);
  } catch (e) {
    say(`ipify failed: ${e.message}`);
  }

  // v2 scope: ONE area per machine (user: "one machine per area using only one token").
  const claim = await supa.claim({
    test_id: TEST_ID, machine_no: MACHINE, github_run_id: GH_RUN_ID, runner_ip: runnerIp,
  });

  if (!claim || claim.stop) {
    say(`nothing to do — ${claim && claim.reason}`);
    return;
  }

  const { item_id, noon_area_id, area_name, usual_pages, lat, lng,
          attempt, variant_id, spec, identity } = claim;

  say(`area ${area_name} (id=${noon_area_id}, usually ${usual_pages}p) attempt=${attempt}`);
  say(`variant=${variant_id} identity=${spec.identity_mode} locale=${spec.locale} cap=${spec.search.page_cap}`);

  const session = new Session({
    deviceId: identity.device_id,
    visitorId: identity.visitor_id,
    locale: spec.locale,
  });

  const t0 = Date.now();
  let setupMs = null, outcome = 'error', fail = null;
  let pageInfo = { page: 0, pageMs: [], seen: new Set(), stopReason: null, fail: null };

  try {
    await mint(session, lat, lng, spec);
    setupMs = Date.now() - t0;
    say(`mint done in ${setupMs}ms, token age 0, cookies=${Object.keys(session.jar).length}`);

    pageInfo = await paginate(session, lat, lng, spec);
    if (pageInfo.fail) {
      fail = pageInfo.fail;
      outcome = fail.kind || 'error';
    } else {
      outcome = 'ok';
    }
    say(`search done pages=${pageInfo.page} cards=${pageInfo.seen.size} stop=${pageInfo.stopReason || (fail && fail.message)}`);
  } catch (e) {
    fail = e;
    outcome = e.kind || 'error';
    say(`fail @ ${e.step || 'unknown'}: ${e.message}`);
  }

  const result = {
    runner_ip: runnerIp,
    outcome,
    fail_step: fail ? fail.step : null,
    fail_page: fail && fail.step === 'search' ? pageInfo.page : null,
    http_status: fail ? (fail.status || null) : null,
    pages_done: fail && fail.step === 'search' ? Math.max(0, pageInfo.page - 1) : pageInfo.page,
    cards: pageInfo.seen.size,
    setup_ms: setupMs,
    elapsed_ms: Date.now() - t0,
    token_age_ms: session.mintedAt ? Date.now() - session.mintedAt : null,
    page_ms: pageInfo.pageMs,
    stop_reason: pageInfo.stopReason,
    error: fail ? String(fail.message).slice(0, 500) : null,
    device_id: identity.device_id,
    cookies: session.reqLog,
    block_info: session.blockInfo,
    requests: session.requests,
  };

  try {
    const r = await supa.report({ test_id: TEST_ID, machine_no: MACHINE, item_id, variant_id, result });
    say(`report ok (worker_version=${r.worker_version})`);
  } catch (e) {
    say(`report FAILED: ${e.message}`);
    process.exitCode = 1;
  }

  say(`done — outcome=${outcome} pages=${result.pages_done} cards=${result.cards} elapsed=${result.elapsed_ms}ms`);
})().catch(async (e) => {
  console.error(`${prefix} fatal: ${e.message}`);
  try {
    await supa.report({
      test_id: TEST_ID, machine_no: MACHINE, item_id: null, variant_id: null,
      result: { outcome: 'error', error: e.message },
    });
  } catch (_) {}
  process.exit(1);
});
