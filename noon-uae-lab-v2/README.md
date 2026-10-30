# noon-uae-lab-v2

A measurement-only worker for the Noon UAE ranking lab. One machine, one area,
one attempt. The worker is a thin executor; **all decisions live in Supabase**.

## Architecture

```
GitHub Actions  ────▶  Supabase Edge Function: noon-lab-claim
   (worker.js)                 picks variant via pi-digit randomiser,
       │                       resolves timing ranges, returns a frozen spec
       │
       │  executes mint phases + pagination against Noon's API,
       │  logging every request, cookie, and Akamai response
       │
       ▼
     Supabase Edge Function: noon-lab-report
                writes to noon_lab.attempt (worker_version='v2'),
                closes item, finishes test if drained
```

The worker calls **nothing else** on Supabase — no DB, no RPC, no PostgREST.
Change behaviour by editing rows in `noon_lab.variant` or `noon_lab.config`.

## Env

Set via workflow `secrets`:

| var                           | source                                     |
|-------------------------------|--------------------------------------------|
| `SUPABASE_URL`                | repo secret                                 |
| `SUPABASE_SERVICE_ROLE_KEY`   | repo secret                                 |
| `TEST_ID`                     | workflow input                              |
| `MACHINE_NO`                  | workflow input                              |
| `GITHUB_RUN_ID`               | `github.run_id` (auto)                      |

## Files

```
noon-uae-lab-v2/
├── worker.js        main entry — the thin executor
├── noon-session.js  everything that talks to Noon's API (HTTP/1.1 + headers + mint + search)
├── supabase.js      thin client over the two Edge Functions
├── package.json
└── README.md
```

## Changing behaviour

**Add a variant:** INSERT a row in `noon_lab.variant` with its spec and the
`pi_slots` (which digits of pi it answers to).

**Disable a variant:** `UPDATE noon_lab.variant SET enabled=false WHERE variant_id='v2.3'`.

**Change timings:** edit the `spec` JSONB of the variant row.

**Change the randomiser:** edit `noon_lab.config` (`randomiser.enabled`,
`randomiser.strategy`, `randomiser.hour_offset_enabled`).

No GitHub PR required for any of the above. The worker picks up the new spec
on its next claim.

## Fingerprint notes (vs v1)

The headers and transport are matched to a real iPhone Proxyman trace of a
guest Noon Food session:

- **HTTP/1.1 everywhere** (`undici.Agent({ allowH2: false })`). Real app uses
  H1.1; Node's default was H2, which no real client does against these hosts.
- **`sentry-trace`**: `{trace}-{span}` with no `-0` sampled-flag suffix.
- **`baggage`**: `sentry-environment,sentry-public_key,sentry-trace_id` only —
  no `sentry-release`.
- **UUIDs**: `x-device-id` UPPERCASE (CFNetwork style), `x-visitor-id`
  lowercase (Noon SDK). Different case per field is deliberate.
- **First whoami**: no `x-locale`, no `x-device-lat/lng`, no cookies — matches
  the first-call shape before location permission is granted.

## Running a test

From Supabase SQL editor (v1 start functions still work for dispatch):

```sql
SELECT noon_lab_start_flow('v2-sanity', ARRAY['v2.1']);
-- Then dispatch a machine via the GitHub Actions "Noon UAE – Lab Ranking v2" workflow.
```

The test row is set up and dispatch kicks off the workflow which runs
`worker.js`. Watch results in `noon_lab.attempt WHERE worker_version='v2'`.
