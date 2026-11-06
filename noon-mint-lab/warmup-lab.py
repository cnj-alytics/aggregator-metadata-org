#!/usr/bin/env python3
"""noon-mint-lab/warmup-lab.py — PRE-MINT WARMUP EXPERIMENT (not production).

Does the real app's launch sequence before mint (mobile-sentry TLS,
etracker/init) and/or its natural pacing / post-mint chatter reduce
mint block rate during a wave?

One GitHub runner = one attempt. Talks to lab RPCs noon_mint_lab_claim /
noon_mint_lab_report.

Variant spec (noon_lab.mint_variant.spec):
  warmup           : "none" | "sentry_etracker"
  pre_mint_pacing  : "fast" | "natural"      (2s/5s/1s natural gaps)
  pre_search_chatter : true | false          (splash + fd_whoami before search)
  impersonate      : curl_cffi fingerprint, default "safari15_3"
  app_headers      : include x-ab-test (true by default)

Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TEST_ID, MACHINE_NO, GITHUB_RUN_ID
"""
import json, os, random, sys, time, uuid, secrets, socket, ssl
from curl_cffi import requests as cr

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
TEST_ID = int(os.environ['TEST_ID'])
MACHINE = int(os.environ['MACHINE_NO'])
GH_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
P = f'[warmup t{TEST_ID} m{MACHINE}]'
def say(m): print(f'{P} {m}', flush=True)

UA = 'noon/23440 CFNetwork/3896.100.1.2.1 Darwin/27.0.0'
BUILD = '23440'
AB_TEST = ('2451,2832,3771,4621,4921,4961,5032,1052,5061,5460,5470,2341,2922,4831,5311,5420,5652,3852,3971,4151,5582,5760,5901,5930,'
           '3792,4203,4461,4520,5812,4660,5271,5382,3641,4431,5202,5951,1731,2301,4751,4781,5122,5570,5621,3341,4111,4821,4871,5330')
WHOAMI_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami'
SET_LOC_URL = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng'
FD_WHOAMI_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/customer/whoami'
SPLASH_URL = 'https://api-app-instant.noon.com/_svc/catalog/splash-screen-config'
SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search'
ETRACKER_INIT = 'https://etracker.noon.com/init'
ETRACKER_TRACK = 'https://etracker.noon.com/track/v2/'

def rpc(name, params):
    r = cr.post(f'{SUPABASE_URL}/rest/v1/rpc/{name}', json=params, timeout=20,
                headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'})
    if r.status_code >= 300: raise RuntimeError(f'rpc {name} HTTP {r.status_code}: {r.text[:200]}')
    return r.json() if r.text else None

def mobile_sentry_preflight():
    """TCP+TLS handshake to mobile-sentry.noon.team like the app's initial CONNECT."""
    try:
        ctx = ssl.create_default_context()
        sock = socket.create_connection(('mobile-sentry.noon.team', 443), timeout=10)
        tls = ctx.wrap_socket(sock, server_hostname='mobile-sentry.noon.team')
        tls.close(); return 'ok'
    except Exception as e:
        return f'err:{str(e)[:40]}'

def main():
    try: ip = cr.get('https://api.ipify.org', timeout=8).text.strip()
    except Exception: ip = ''
    claim = rpc('noon_mint_lab_claim', {'p_test_id': TEST_ID, 'p_machine_no': MACHINE, 'p_github_run_id': GH_RUN_ID, 'p_runner_ip': ip})
    if not claim or claim.get('stop'): say(f"nothing to do: {claim and claim.get('reason')}"); return
    spec = claim.get('spec') or {}
    IMP = spec.get('impersonate', 'safari15_3')
    WARMUP = spec.get('warmup', 'none')
    PACING = spec.get('pre_mint_pacing', 'fast')
    CHATTER = bool(spec.get('pre_search_chatter'))
    # Proxy source: env var NOON_PROXY_URL (from GitHub secret) wins; falls back to spec.proxy.
    # When the env var is set, routing all Noon calls through it; the auth part of the URL is not logged.
    PROXY = os.environ.get('NOON_PROXY_URL') or spec.get('proxy')
    PROXY_SCOPE = spec.get('proxy_scope', 'all')  # "all" or "mint_only" (search goes direct from the runner)
    if PROXY: say(f"using proxy: {PROXY.split('@')[-1] if '@' in PROXY else PROXY} (scope={PROXY_SCOPE})")
    APPH = bool(spec.get('app_headers', True))
    SEARCH_GAP_MIN = float(spec.get('search_gap_min_s', 1.5))
    SEARCH_GAP_MAX = float(spec.get('search_gap_max_s', 4.5))
    lat, lng = float(claim['lat']), float(claim['lng'])
    pages = int(claim.get('search_pages') or 10)
    zone = dict(claim.get('zone_headers') or {})
    say(f"variant={claim['variant_id']} warmup={WARMUP} pacing={PACING} chatter={CHATTER} area={claim.get('area_name')} ip={ip}")
    time.sleep(random.random() * (int(claim.get('start_jitter_ms') or 0) / 1000))

    dev = str(uuid.uuid4()).upper(); vis = str(uuid.uuid4()).lower()
    jar_etracker = {}; jar_noon = {}
    log = []; t0 = time.time()

    def send(step, url, headers, method='GET', body=None):
        s = cr.Session(impersonate=IMP, default_headers=False)
        # pick jar by host
        if 'etracker' in url: jar = jar_etracker
        elif 'noon.' in url and 'noon.team' not in url: jar = jar_noon
        else: jar = {}
        h = dict(headers)
        if jar: h['Cookie'] = '; '.join(f'{k}={v}' for k, v in jar.items())
        st0 = time.time()
        try:
            # Decide whether to proxy THIS request:
            #   scope "all"        → proxy every request
            #   scope "mint_only"  → proxy only mint / warmup / set_location; search goes direct from the runner
            use_proxy = bool(PROXY) and (PROXY_SCOPE != 'mint_only' or not step.startswith('search'))
            kw = {'proxies': {'http': PROXY, 'https': PROXY}, 'verify': False} if use_proxy else {}
            r = s.request(method, url, headers=h, data=body, timeout=30, **kw)
            st, txt = r.status_code, r.text
            for k, v in r.cookies.items(): jar[k] = v
        except Exception as e:
            st, txt = 0, str(e)[:160]
        finally: s.close()
        ms = int((time.time() - st0) * 1000)
        log.append({'t': int((time.time() - t0) * 1000), 'step': step, 'st': st, 'ms': ms})
        return st, txt

    def base(extra):
        tr = secrets.token_hex(16); sp = secrets.token_hex(8)
        xe = extra.pop('x-experience', 'ecom') if isinstance(extra, dict) else 'ecom'
        h = {'x-experience': xe, 'Accept': 'application/json, text/plain, */*', 'x-mp': 'noon',
             'baggage': f'sentry-environment=production,sentry-public_key=d61e4de762b3c36e7c14f2d7cf6030c1,sentry-trace_id={tr}'}
        if isinstance(extra, dict): h.update(extra)
        h.update({'Cache-Control': 'no-cache', 'x-content': 'mobile', 'sentry-trace': f'{tr}-{sp}',
                  'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'gzip, deflate, br',
                  'x-device-id': dev, 'x-platform': 'ios', 'x-build': BUILD,
                  'User-Agent': UA, 'Connection': 'keep-alive', 'x-visitor-id': vis})
        return h

    res = {'warmup_outcome': {}, 'mint': {}, 'searches': [], 'searches_ms': [], 'chatter': {}}

    # --- WARMUP (pre-mint) ---
    if WARMUP == 'sentry_etracker':
        res['warmup_outcome']['sentry'] = mobile_sentry_preflight()
        st, _ = send('etracker_init', ETRACKER_INIT, {'User-Agent': UA, 'Accept': '*/*', 'Connection': 'keep-alive'})
        res['warmup_outcome']['etracker_init'] = st
        if PACING == 'natural':
            time.sleep(1.8 + random.random() * 0.6)

    # --- MINT: whoami ---
    st, _ = send('mint_whoami', WHOAMI_URL, base({'x-whoami-request-id': str(uuid.uuid4())}))
    res['mint']['whoami'] = st
    if st != 200:
        res['outcome'] = 'mint_whoami_blocked'; res['setup_outcome'] = 'blocked'; res['got_token'] = False
        res['setup_fail_step'] = 'mint_whoami'; res['setup_http'] = st
        res['identity_calls'] = 1; res['identity_403'] = 1 if st in (403,429) else 0
        finish(claim, res, log)
        return

    # optional telemetry POST like the app
    if WARMUP == 'sentry_etracker':
        if PACING == 'natural': time.sleep(1.8 + random.random() * 0.6)
        st, _ = send('etracker_track', ETRACKER_TRACK, {'User-Agent': UA, 'Content-Type': 'application/json', 'Accept': '*/*'}, method='POST',
                     body=json.dumps([{'em':{'app':'noon','v':1}, 'ev':'launch', 'ts':int(time.time()*1000), 'pc':'noon', 'vid':vis}]))
        res['warmup_outcome']['etracker_track'] = st
        if PACING == 'natural': time.sleep(4.0 + random.random() * 1.2)
    elif PACING == 'natural':
        time.sleep(5.0 + random.random() * 1.0)

    # --- set-location (Akamai plants ak_bmsc here) ---
    extra = {'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng), 'Content-Type': 'application/json'}
    st, _ = send('set_location', SET_LOC_URL, base(extra), method='POST', body=json.dumps({'location': {'lat': lat, 'lng': lng}, 'lang': 'en'}))
    res['mint']['set_location'] = st
    if st != 200:
        res['outcome'] = 'set_location_blocked'; res['setup_outcome'] = 'blocked'; res['got_token'] = True
        res['setup_fail_step'] = 'set_location'; res['setup_http'] = st
        res['identity_calls'] = 2; res['identity_403'] = 1 if st in (403,429) else 0
        finish(claim, res, log); return

    res['got_token'] = True
    res['setup_outcome'] = 'ok'
    res['identity_calls'] = 2; res['identity_403'] = 0

    # --- optional post-mint chatter before search ---
    if CHATTER:
        if PACING == 'natural': time.sleep(0.8 + random.random() * 0.4)
        st, _ = send('chatter_splash', SPLASH_URL, base({'x-experience': 'ecom', 'x-locale': 'en-ae'}))
        res['chatter']['splash'] = st
        if PACING == 'natural': time.sleep(0.5 + random.random() * 0.3)
        st, _ = send('chatter_fd_whoami', FD_WHOAMI_URL, base({'x-experience': 'food', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng)}))
        res['chatter']['fd_whoami'] = st
        if PACING == 'natural': time.sleep(0.8 + random.random() * 0.4)

    # --- SEARCH pages ---
    if PACING == 'natural': time.sleep(1.0 + random.random() * 1.0)
    tok = None; search_ok = 0; search_tried = 0; search_403 = 0; search_other = 0; p1_outlets = 0; p1_codes = []
    for p in range(1, pages + 1):
        if p > 1: time.sleep(SEARCH_GAP_MIN + random.random() * max(0.0, SEARCH_GAP_MAX - SEARCH_GAP_MIN))
        body = {'queryEntity': None, 'q': None, 'f': {}, 'contexts': [], 'withContent': True, 'excludeOutletCodes': [],
                'limit': 60, 'scopes': None, 'page': 1, 'dishTags': [], 'ratingMin': None,
                'sort': {'by': 'popularity', 'dir': 'desc', 'slab': None}, 'widgets': None, 'content': None,
                'withHits': False, 'filteredByWidget': None, 'internalFilteredByWidget': 'homepage_navpill_filters_widget',
                'getFallback': True, 'type': 'outlet', 'qType': 'search_query', 'minCount': 0,
                'groupDishes': False, 'isSsc': False, 'hideSearchBar': False, 'hideSearchOptions': False,
                'isOutletAdProcessingEnabled': True, 'withDishFetchQuery': None}
        if tok: body['searchToken'] = tok
        extra = {'x-experience': 'food', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng),
                 'x-mp-country': 'ae', 'x-lat': str(round(lat * 1e7)), 'x-lng': str(round(lng * 1e7)),
                 'x-border-enabled': 'true', 'x-rocket-enabled': 'true', 'Content-Type': 'application/json'}
        for k, v in zone.items():
            if v is not None and not isinstance(v, (dict, list)):
                extra[k] = str(v).lower() if isinstance(v, bool) else str(v)
        if APPH: extra['x-ab-test'] = AB_TEST
        st, txt = send(f'search_p{p}', SEARCH_URL, base(extra), method='POST', body=json.dumps(body))
        search_tried += 1
        res['searches'].append(st)
        if st == 200:
            search_ok += 1
            try: j = json.loads(txt)
            except Exception: j = {}
            outlets = []
            def walk(x, d):
                if d > 8 or not x: return
                if isinstance(x, list):
                    for v in x: walk(v, d + 1)
                elif isinstance(x, dict):
                    if isinstance(x.get('outletCode'), str) and x['outletCode']: outlets.append(x['outletCode']); return
                    for v in x.values(): walk(v, d + 1)
            walk(j.get('results') or [], 0)
            if p == 1: p1_outlets = len(outlets); p1_codes = outlets[:15]
            tok = j.get('searchToken')
            if not outlets or not tok: break
        elif st in (403, 429):
            search_403 += 1; break
        else:
            search_other += 1; break

    res['outcome'] = 'ok' if (search_ok > 0 and search_403 == 0 and search_other == 0) else ('search_failed' if search_tried > 0 else 'no_search')
    res['search_ok'] = search_ok; res['search_tried'] = search_tried
    res['search_403'] = search_403; res['search_other'] = search_other
    res['page1_outlets'] = p1_outlets; res['page1_codes'] = p1_codes
    res['swaps'] = 0  # not applicable in this lab
    res['recovery'] = f'warmup={WARMUP}|pacing={PACING}|chatter={int(CHATTER)}|imp={IMP}'
    finish(claim, res, log)

def finish(claim, res, log):
    res['steps'] = log
    rpc('noon_mint_lab_report', {'p_attempt_id': claim['attempt_id'], 'p_result': res})
    say(f"=== DONE outcome={res.get('outcome')} setup={res.get('setup_outcome')} pages={res.get('search_ok',0)}/{res.get('search_tried',0)} blocked={res.get('search_403',0)}")

if __name__ == '__main__':
    try: main()
    except Exception as e:
        say(f'FATAL: {e}'); sys.exit(1)
