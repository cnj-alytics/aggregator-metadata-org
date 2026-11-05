#!/usr/bin/env python3
"""noon-mint-lab/fp-lab.py — TRANSPORT / FINGERPRINT EXPERIMENT (not production)

One GitHub runner = one attempt. Same session as production mint v4
(1-call whoami mint with swap-retry, cached zone headers, 20 search pages,
swap device id on a blocked page, up to 12 tries, 6s backoff) — ONLY the
network transport changes, set per variant (noon_lab.mint_variant.spec):

  impersonate : curl_cffi TLS/HTTP2 fingerprint, e.g. "safari15_3" (what
                production imitates today) or "safari270_ios" (iOS 27, which
                is what the app's User-Agent claims to be)
  persistent  : true  = one kept-alive connection for the whole session (like the app)
                false = new connection per request (like production today)
  app_headers : true  = also send x-ab-test like the iPhone capture
  http        : "1.1" or "2" — force the HTTP version (default: whatever the impersonation uses)
Build 23440 / app 5.261104 (iPhone capture 2026-11-05). Retries match production now:
mint 4 attempts / 3s, search 4 tries per page / 3s, device swap on each 403.

Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TEST_ID, MACHINE_NO, GITHUB_RUN_ID
"""
import json, os, random, sys, time, uuid, secrets
from curl_cffi import requests as cr
from curl_cffi.const import CurlHttpVersion

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
TEST_ID = int(os.environ['TEST_ID'])
MACHINE = int(os.environ['MACHINE_NO'])
GH_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
P = f'[fplab t{TEST_ID} m{MACHINE}]'
def say(m): print(f'{P} {m}', flush=True)

UA = 'noon/23440 CFNetwork/3896.100.1.2.1 Darwin/27.0.0'
BUILD = '23440'
WHOAMI_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami'
SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search'
AB_TEST = ('2451,2832,3771,4621,4921,4961,5032,1052,5061,5460,5470,2341,2922,4831,5311,5420,5652,3852,3971,4151,5582,5760,5901,5930,'
           '3792,4203,4461,4520,5812,4660,5271,5382,3641,4431,5202,5951,1731,2301,4751,4781,5122,5570,5621,3341,4111,4821,4871,5330')

def rpc(name, params):
    r = cr.post(f'{SUPABASE_URL}/rest/v1/rpc/{name}', json=params, timeout=20,
                headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'})
    if r.status_code >= 300: raise RuntimeError(f'rpc {name} HTTP {r.status_code}: {r.text[:200]}')
    return r.json() if r.text else None

def main():
    try: ip = cr.get('https://api.ipify.org', timeout=8).text.strip()
    except Exception: ip = ''
    claim = rpc('noon_mint_lab_claim', {'p_test_id': TEST_ID, 'p_machine_no': MACHINE, 'p_github_run_id': GH_RUN_ID, 'p_runner_ip': ip})
    if not claim or claim.get('stop'): say(f"nothing to do: {claim and claim.get('reason')}"); return
    spec = claim.get('spec') or {}
    IMP = spec.get('impersonate', 'safari15_3'); PERSIST = bool(spec.get('persistent')); APPH = bool(spec.get('app_headers'))
    HTTP = str(spec.get('http') or '')
    HV = {'1.1': CurlHttpVersion.V1_1, '2': CurlHttpVersion.V2_0}.get(HTTP)
    negotiated = {}
    lat, lng = float(claim['lat']), float(claim['lng'])
    pages = int(claim.get('search_pages') or 20)
    zone = dict(claim.get('zone_headers') or {})
    say(f"variant={claim['variant_id']} imp={IMP} http={HTTP or 'default'} persistent={PERSIST} app_headers={APPH} area={claim.get('area_name')} ip={ip}")
    time.sleep(random.random() * (int(claim.get('start_jitter_ms') or 0) / 1000))

    dev = str(uuid.uuid4()).upper(); vis = str(uuid.uuid4()).lower()
    jar = {}
    log = []; t0 = time.time()
    sess = cr.Session(impersonate=IMP, default_headers=False) if PERSIST else None

    def send(step, method, url, headers, body=None):
        h = dict(headers)
        if jar: h['Cookie'] = '; '.join(f'{k}={v}' for k, v in jar.items())
        s = sess or cr.Session(impersonate=IMP, default_headers=False)
        st0 = time.time()
        try:
            kw = {'http_version': HV} if HV is not None else {}
            r = s.request(method, url, headers=h, data=body, timeout=20, **kw)
            st, txt = r.status_code, r.text
            for k, v in r.cookies.items(): jar[k] = v
            if sess: sess.cookies.clear()   # we send cookies ourselves; stop the session adding duplicates
            hv = {1: '1.0', 2: '1.1', 3: '2', 30: '3'}.get(getattr(r, 'http_version', None), str(getattr(r, 'http_version', ''))); negotiated[hv] = negotiated.get(hv, 0) + 1
        except Exception as e:
            st, txt = 0, str(e)[:200]
        finally:
            if not sess: s.close()
        ms = int((time.time() - st0) * 1000)
        log.append({'t': int((time.time() - t0) * 1000), 'step': step, 'st': st, 'ms': ms})
        return st, txt, ms

    def sentry():
        tr = secrets.token_hex(16); sp = secrets.token_hex(8)
        return tr, sp

    def base(extra):
        tr, sp = sentry()
        h = {'x-experience': extra.pop('x-experience', 'ecom'), 'Accept': 'application/json, text/plain, */*', 'x-mp': 'noon',
             'baggage': f'sentry-environment=production,sentry-public_key=d61e4de762b3c36e7c14f2d7cf6030c1,sentry-trace_id={tr}'}
        h.update(extra)
        h.update({'Cache-Control': 'no-cache', 'x-content': 'mobile', 'sentry-trace': f'{tr}-{sp}', 'Accept-Language': 'en-US,en;q=0.9',
                  'Accept-Encoding': 'gzip, deflate, br', 'x-device-id': dev, 'x-platform': 'ios', 'x-build': BUILD,
                  'User-Agent': UA, 'Connection': 'keep-alive', 'x-visitor-id': vis})
        return h

    res = {'identity_calls': 0, 'identity_403': 0, 'swaps': 0, 'search_ok': 0, 'search_tried': 0, 'search_403': 0, 'search_other': 0,
           'page1_outlets': 0, 'page1_codes': [], 'got_token': False}
    # ---- mint: 1 whoami call, swap ids + 6s on 403, up to 6 swaps (production mint v4)
    s_start = time.time(); setup_fail = None
    for a in range(4):
        st, txt, ms = send('mint_whoami' if a == 0 else f'mint_whoami_swap{a}', 'GET', WHOAMI_URL, base({'x-whoami-request-id': str(uuid.uuid4())}))
        res['identity_calls'] += 1
        say(f'  whoami: {st} {ms}ms dev={dev[:8]}')
        if st == 200: break
        if st in (403, 429): res['identity_403'] += 1
        if a == 3 or st not in (403, 429): setup_fail = ('mint_whoami', st); break
        dev = str(uuid.uuid4()).upper(); vis = str(uuid.uuid4()).lower(); res['swaps'] += 1
        time.sleep(3 + random.random())
    res['setup_ms'] = int((time.time() - s_start) * 1000)
    res['got_token'] = 'nguestv2' in jar
    if setup_fail:
        res.update(setup_outcome='blocked' if setup_fail[1] in (403, 429) else 'error', setup_fail_step=setup_fail[0], setup_http=setup_fail[1],
                   outcome='setup_failed', error=f'{setup_fail[0]} {setup_fail[1]}')
    else:
        res['setup_outcome'] = 'ok'
        # ---- search: 20 pages, production pacing, swap device on 403 (12 tries, 6s)
        time.sleep(1 + random.random())
        token = None; recs = []; fail = None
        for p in range(1, pages + 1):
            if p > 1: time.sleep(1.25 + random.random() * 3.75)
            body = {'queryEntity': None, 'q': None, 'f': {}, 'contexts': [], 'withContent': True, 'excludeOutletCodes': [], 'limit': 60,
                    'scopes': None, 'page': 1, 'dishTags': [], 'ratingMin': None, 'sort': {'by': 'popularity', 'dir': 'desc', 'slab': None},
                    'widgets': None, 'content': None, 'withHits': False, 'filteredByWidget': None,
                    'internalFilteredByWidget': 'homepage_navpill_filters_widget', 'getFallback': True, 'type': 'outlet',
                    'qType': 'search_query', 'minCount': 0, 'groupDishes': False, 'isSsc': False, 'hideSearchBar': False,
                    'hideSearchOptions': False, 'isOutletAdProcessingEnabled': True, 'withDishFetchQuery': None}
            if token: body['searchToken'] = token
            a = 0
            while True:
                extra = {'x-experience': 'food', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng), 'x-mp-country': 'ae',
                         'x-lat': str(round(lat * 1e7)), 'x-lng': str(round(lng * 1e7)), 'x-border-enabled': 'true', 'x-rocket-enabled': 'true',
                         'Content-Type': 'application/json'}
                for k, v in zone.items():
                    if v is not None and not isinstance(v, (dict, list)): extra[k] = str(v).lower() if isinstance(v, bool) else str(v)
                if APPH: extra['x-ab-test'] = AB_TEST
                st, txt, ms = send(f'search_p{p}' if a == 0 else f'search_p{p}_rec{a}', 'POST', SEARCH_URL, base(extra), json.dumps(body))
                res['search_tried'] += 1
                if st == 200: res['search_ok'] += 1; break
                if st in (403, 429): res['search_403'] += 1
                else: res['search_other'] += 1
                say(f'  page {p} try {a}: {st} {ms}ms')
                if st not in (403, 429) or a >= 3: break
                a += 1; dev = str(uuid.uuid4()).upper(); res['swaps'] += 1
                time.sleep(3 + random.random())
            recs.append(a)
            if st != 200: fail = (f'search_p{p}', st); break
            try: j = json.loads(txt)
            except Exception: j = {}
            outlets = []
            def walk(x, d):
                if d > 8 or not x: return
                if isinstance(x, list):
                    for v in x: walk(v, d + 1)
                elif isinstance(x, dict):
                    if isinstance(x.get('outletCode'), str) and x['outletCode']: outlets.append(x); return
                    for v in x.values(): walk(v, d + 1)
            walk(j.get('results') or [], 0)
            if p == 1: res['page1_outlets'] = len(outlets); res['page1_codes'] = [o['outletCode'] for o in outlets[:15]]
            say(f'  page {p}: {len(outlets)} outlets (tries {a + 1})')
            if not outlets or not j.get('searchToken'): break
            token = j['searchToken']
        res['page_recoveries'] = recs
        res['outcome'] = 'search_failed' if fail else ('ok' if res['page1_outlets'] > 0 else 'empty_results')
        if fail: res['error'] = f'{fail[0]} {fail[1]}'
    res['steps'] = log
    res['recovery'] = f'{IMP}|http={HTTP or "default"}|persist={int(PERSIST)}|apph={int(APPH)}|negotiated={json.dumps(negotiated)}'
    res['bmsc_at_search'] = 'ak_bmsc' in jar
    res['cookies_at_search'] = sorted(jar.keys())
    rpc('noon_mint_lab_report', {'p_attempt_id': claim['attempt_id'], 'p_result': res})
    say(f"=== DONE outcome={res.get('outcome')} setup={res.get('setup_outcome')} pages {res['search_ok']}/{res['search_tried']} blocked={res['search_403']}")
    if sess: sess.close()

if __name__ == '__main__':
    try: main()
    except Exception as e:
        say(f'FATAL: {e}'); sys.exit(1)
