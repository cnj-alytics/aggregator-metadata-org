// Thin client over the two Edge Functions the worker talks to.
// The worker calls NOTHING else on Supabase — no DB, no PG RPC, no PostgREST.

'use strict';
const undici = require('undici');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SERVICE_KEY) {
  throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set');
}

async function callFn(name, payload) {
  const url = `${SUPABASE_URL}/functions/v1/${name}`;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const resp = await undici.fetch(url, {
        method: 'POST',
        headers: {
          'authorization': `Bearer ${SERVICE_KEY}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(25000),
      });
      const text = await resp.text();
      if (!resp.ok) throw new Error(`${name} ${resp.status}: ${text.slice(0, 300)}`);
      return text ? JSON.parse(text) : null;
    } catch (e) {
      if (attempt === 3) throw e;
      await new Promise(r => setTimeout(r, 2000 * attempt));
    }
  }
}

exports.claim  = (payload) => callFn('noon-lab-claim',  payload);
exports.report = (payload) => callFn('noon-lab-report', payload);
