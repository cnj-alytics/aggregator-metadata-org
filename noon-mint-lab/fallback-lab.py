#!/usr/bin/env python3
"""noon-mint-lab/fallback-lab.py — TEST-ONLY fallback worker (independent machines, no shared rate limiting).

WHAT THIS IS
  Lab version of the fallback worker. Each machine is an independent device on
  public Wi-Fi — no shared IP page budget, no shared 429 pauses. A 429 on one
  machine does NOT affect any other machine.

  Reports to noon_lab schema (noon_lab_claim / noon_lab_report). Does NOT save
  ranking data.

CHANGES FROM PRODUCTION fallback-worker.py
  1. REMOVED: permit() — no shared per-IP page budget
  2. REMOVED: shared 429 pause (the whole-IP backoff via Supabase)
  3. On 429: this machine alone pauses briefly, then remints (new device + token)
  4. Reports to noon_lab_claim / noon_lab_report instead of noon_ranking_fb_*
  5. No ranking data saved (dry_run always true)
  6. No heartbeat (lab runs are short)
  7. No checkpoint/handover (test only)

Env (set by workflow):
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TEST_ID, MACHINE_NO, GITHUB_RUN_ID,
  FLEET (uae | decodo), PROXY_PORT (decodo only), NOON_PROXY_URL (secret),
  WALL_LIMIT_SEC (optional, default 3600)
"""
import base64, json, os, random, re, secrets, socket, ssl, sys, time, uuid
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote
from curl_cffi import requests as cr

# -- Env ------------------------------------------------------------------------

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
TEST_ID = int(os.environ.get('TEST_ID') or 0)
MACHINE = int(os.environ.get('MACHINE_NO') or 0)
GH_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
FLEET = (os.environ.get('FLEET') or '').strip().lower()
PROXY_PORT = (os.environ.get('PROXY_PORT') or '').strip()
WALL_LIMIT_S = int(os.environ.get('WALL_LIMIT_SEC') or 3600)
START = time.time()

P = f'[lab-fb t{TEST_ID} m{MACHINE} {FLEET}{":" + PROXY_PORT if PROXY_PORT else ""}]'
def say(m): print(f'{P} {m}', flush=True)
def now_iso(ts=None): return datetime.fromtimestamp(ts or time.time(), tz=timezone.utc).isoformat()

# -- Proxy (never direct) -------------------------------------------------------

def pick_proxy():
    raw = os.environ.get('NOON_PROXY_URL', '')
    urls = [u.strip() for u in raw.split(',') if u.strip()]
    if FLEET == 'decodo':
        if not PROXY_PORT: return None
        urls = [u for u in urls if str(urlparse(u).port) == PROXY_PORT]
    return random.choice(urls) if urls else None

def proxy_label(p):
    u = urlparse(p)
    return f'{u.hostname}:{u.port}'

PROXY = None

# -- Noon constants --------------------------------------------------------------

APP_DEFAULT = {'build': '23440', 'version': '5.261104', 'ua_tail': 'CFNetwork/3896.100.1.2.1 Darwin/27.0.0'}
BUILD = APP_DEFAULT['build']
UA = f"noon/{BUILD} {APP_DEFAULT['ua_tail']}"
IMP = 'safari15_3'
AB_TEST = ('2451,2832,3771,4621,4921,4961,5032,1052,5061,5460,5470,2341,2922,4831,5311,5420,5652,3852,3971,4151,5582,5760,5901,5930,'
           '3792,4203,4461,4520,5812,4660,5271,5382,3641,4431,5202,5951,1731,2301,4751,4781,5122,5570,5621,3341,4111,4821,4871,5330')
WHOAMI_URL = 'https://api-app-st.noon.com/_vs/st/st-whoami-api/whoami'
SET_LOC_URL = 'https://api-app.noon.com/_vs/st/mp-identity-api-geo/serviceable-geo-info/set-location-by-lat-lng'
FD_WHOAMI_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/customer/whoami'
SEARCH_URL = 'https://api-app-fd.noon.com/_svc/mp-food-api-catalog/api/search'
ETRACKER_INIT = 'https://etracker.noon.com/init'
ETRACKER_TRACK = 'https://etracker.noon.com/track/v2/'
SENTRY_HOST = 'mobile-sentry.noon.team'

VERSION = APP_DEFAULT['version']

def apply_app_identity(app):
    global BUILD, UA, VERSION
    a = app if isinstance(app, dict) else {}
    build = str(a.get('build', '')) if re.fullmatch(r'[0-9]{4,7}', str(a.get('build', ''))) else APP_DEFAULT['build']
    version = str(a.get('version', '')) if re.fullmatch(r'[0-9]{1,3}(\.[0-9]{1,8}){1,3}', str(a.get('version', ''))) else APP_DEFAULT['version']
    tail = str(a.get('ua_tail', '')) if re.fullmatch(r'[A-Za-z0-9 ./_()-]{1,80}', str(a.get('ua_tail', ''))) else APP_DEFAULT['ua_tail']
    BUILD, VERSION, UA = build, version, f'noon/{build} {tail}'
    return build, UA

# -- etracker telemetry -----------------------------------------------------------

def _ms(): return int(time.time() * 1000)
def noon_app(): return {'mc': 'noon', 'an': 'noon', 'av': VERSION, 'ab': BUILD, 'mpe': 'noon'}
def food_app(): return {'mc': 'food', 'an': 'noon', 'av': VERSION, 'ab': BUILD, 'mpe': 'food'}
def device_block(idfv):
    return {'dm': 'Apple iPhone 17 Pro Max', 'dos': 'ios 27.0.1', 'd': 'Handset', 'sr': '1320x2868', 'vps': '1320x2868', 'scl': 3,
            'idfa': '00000000-0000-0000-0000-000000000000', 'idfv': idfv}
def evt(ev, em, vid, idfv):
    return {'em': em, 'ev': ev, 'ts': _ms(), 'pc': 'noon', 'vid': vid, 'lo': 'en-ae', 'pf': 'ios', 'd': device_block(idfv)}
def ev_launch(v, i):
    return evt('launch', {'fsv': '1.00', 'isNA': False, 'nps': 'denied',
               'perf': {'NTJS': {'d': 700}, 'GWA': {'d': 250}, 'SR': {'d': 280, 'm': {'fsmps': 10}}, 'TJS': {'d': 15500}, 'TAS': {'d': 16200}},
               **noon_app()}, v, i)
def ev_launch_info(v, i):
    now = _ms()
    return evt('launch_info', {'nsa': now - 10000, 'jsa': now - 9400, 'ds': 0.6, 's': 'app_launch', 'dl': 'en', **noon_app()}, v, i)
def ev_app_foreground(v, i): return evt('app_foreground', {**food_app()}, v, i)
def ev_location_fetch(v, i): return evt('location_fetch', {'GPS_allow': True, **noon_app()}, v, i)
def ev_page_home(v, i): return evt('page_home', {'curl': '/', **noon_app()}, v, i)
def ev_selected_zone(zh, v, i):
    g = lambda k, d='': str(zh.get(k) if zh.get(k) is not None else d)
    return evt('selected_zone', {
        'szn': g('x-ecom-zonecode', 'AE_DXB-S1'), 'szf': g('x-food-zonecode', 'FOOD-AE-DXB-HUB4-DUBAI_MARINA'),
        'szi': g('x-nooninstant-zonecode'), 'szr': g('x-rocket-zonecode'), 'sznow': g('x-noonnownow-zonecode'),
        'szs': g('x-services-zonecode', 'SERVICES-AE-DUBAI'), 'szmpr': g('x-rocket-mp-zonecode'), 'szg': g('x-ecom-zonecode', 'AE_DXB-S1'),
        'address_codes': [{'mp': m} for m in ('ecom', 'rocket', 'food', 'nooninstant', 'services', 'noonnownow', 'ping', 'out', 'aster', 'money')],
        **food_app()}, v, i)
def ev_whoami_ab_config(zh, v, i):
    raw = zh.get('x-ab-test')
    ids = []
    if isinstance(raw, str): ids = [int(x.strip()) for x in raw.split(',') if x.strip().lstrip('-').isdigit()]
    elif isinstance(raw, list): ids = [x for x in raw if isinstance(x, int) and not isinstance(x, bool)]
    return evt('whoami_ab_config', {'abIds': ids or [1052, 1471, 1531, 1731, 1750], **noon_app()}, v, i)
def ev_page_catalog(pn, v, i): return evt('page_catalog', {'hid': str(uuid.uuid4()), 'nbh': 60, 'pn': pn, 'sb': 'browse', **food_app()}, v, i)
def impression_triplets(outlets, v, i):
    events = []
    for idx, o in enumerate(outlets):
        oc = o.get('outletCode'); df = o.get('distance') or 5.0; sla = o.get('sla') or '30-40'
        events.append(evt('outlet_impression', {'oc': oc, 'wid': f'search_outlet/{idx}', 'aid': oc, 'ix': f'0/{idx}', 'otg': False,
                          'df': df, 'fav': False, 'isp': True, 'sla': sla, 'wn': 'search_results', 'wtc': 'search_results_tab',
                          'curl': '/search?type=outlet', 'src': 'search/results', 'pn': 'Search_Results', **food_app()}, v, i))
        events.append(evt('outlet_impression_v2', {'pg': 'SR', 'wt': f'SR/search_results/0/search_results_tab/{idx}/ROW/{idx}/ITM/0',
                          'ul': '/search?type=outlet', 'cf': '{}', 'isp': True, 'oc': oc, 'df': df, 'sla': sla, **food_app()}, v, i))
        events.append(evt('promotion_impression', {'aid': oc, 'wid': f'search_results/{idx}', 'aidMap': {oc: f'search_results/{idx}'},
                          'ismMap': {oc: True}, 'curl': '/search?type=outlet', 'toUrl': f'/outlet/{oc}',
                          'ed': {'eIds': [], 'reqId': str(uuid.uuid4())}, 'cmsEids': {}, 'wp': idx, 'ar': 100,
                          'adType': 'display', 'isAd': False, **food_app()}, v, i))
    return events

# -- Supabase (lab RPCs) ----------------------------------------------------------

def rpc(name, params):
    pauses = [1, 2, 4, 8, 15, 15, 15]
    for attempt in range(1, len(pauses) + 2):
        status = None
        try:
            r = cr.post(f'{SUPABASE_URL}/rest/v1/rpc/{name}', json=params, timeout=20,
                        headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'})
            status = r.status_code
            if status >= 300: raise RuntimeError(f'rpc {name} HTTP {status}: {r.text[:300]}')
            return r.json() if r.text else None
        except Exception as e:
            permanent = status is not None and 400 <= status < 500 and status not in (408, 429)
            if permanent or attempt > len(pauses): raise
            say(f'rpc({name}) attempt {attempt} failed: {str(e)[:160]} - retrying in {pauses[attempt - 1]}s')
            time.sleep(pauses[attempt - 1] + random.random() * 0.5)

RUNNER_IP = None

# -- Network: one Noon "customer" --------------------------------------------------

def walk_outlets(results):
    out = []
    def walk(x, d):
        if d > 8 or not x: return
        if isinstance(x, list):
            for v in x: walk(v, d + 1)
        elif isinstance(x, dict):
            if isinstance(x.get('outletCode'), str) and x['outletCode']: out.append(x); return
            for v in x.values(): walk(v, d + 1)
    walk(results or [], 0)
    return out

class Customer:
    """One phone — fully independent. No shared state with other machines."""
    def __init__(self):
        self.dev = str(uuid.uuid4()).upper()
        self.vis = str(uuid.uuid4()).lower()
        self.jar_noon, self.jar_et = {}, {}
        self.t0 = time.time()
        self.log = []
        self.noon_req = 0
        self.et_posts = self.et_events = self.et_non2xx = 0

    def send(self, step, url, headers, method='GET', body=None):
        jar = self.jar_et if 'etracker' in url else self.jar_noon
        h = dict(headers)
        if jar: h['Cookie'] = '; '.join(f'{k}={v}' for k, v in jar.items())
        s = cr.Session(impersonate=IMP, default_headers=False)
        st0 = time.time(); st, txt = None, ''
        try:
            r = s.request(method, url, headers=h, data=body, timeout=30, proxies={'http': PROXY, 'https': PROXY})
            st, txt = r.status_code, r.text
            for k, v in r.cookies.items(): jar[k] = v
        except Exception as e:
            st, txt = None, str(e)[:160]
        finally:
            s.close()
        ms = int((time.time() - st0) * 1000)
        if 'noon.com' in url: self.noon_req += 1
        entry = {'t': int((time.time() - self.t0) * 1000), 'step': step, 'st': st, 'ms': ms}
        if st is None: entry['err'] = txt[:120]
        if len(self.log) < 600: self.log.append(entry)
        return st, txt

    def new_identity(self):
        """Fresh phone session — new device, visitor, cookies. Independent of all other machines."""
        self.dev = str(uuid.uuid4()).upper()
        self.vis = str(uuid.uuid4()).lower()
        self.jar_noon, self.jar_et = {}, {}

    def et(self, step, events):
        st, _ = self.send(step, ETRACKER_TRACK, {**self.base(), 'Content-Type': 'text/plain;charset=UTF-8'},
                          method='POST', body=json.dumps(events))
        self.et_posts += 1; self.et_events += len(events)
        if not (st and 200 <= st < 300): self.et_non2xx += 1
        return st

    def base(self, extra=None):
        extra = dict(extra or {})
        tr, sp = secrets.token_hex(16), secrets.token_hex(8)
        h = {'x-experience': extra.pop('x-experience', 'ecom'), 'Accept': 'application/json, text/plain, */*', 'x-mp': 'noon',
             'baggage': f'sentry-environment=production,sentry-public_key=d61e4de762b3c36e7c14f2d7cf6030c1,sentry-trace_id={tr}'}
        h.update(extra)
        h.update({'Cache-Control': 'no-cache', 'x-content': 'mobile', 'sentry-trace': f'{tr}-{sp}',
                  'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'gzip, deflate, br',
                  'x-device-id': self.dev, 'x-platform': 'ios', 'x-build': BUILD,
                  'User-Agent': UA, 'Connection': 'keep-alive', 'x-visitor-id': self.vis})
        return h

def sentry_preflight():
    try:
        u = urlparse(PROXY)
        sock = socket.create_connection((u.hostname, u.port), timeout=10)
        auth = ''
        if u.username:
            tok = base64.b64encode(f'{unquote(u.username)}:{unquote(u.password or "")}'.encode()).decode()
            auth = f'Proxy-Authorization: Basic {tok}\r\n'
        sock.sendall(f'CONNECT {SENTRY_HOST}:443 HTTP/1.1\r\nHost: {SENTRY_HOST}:443\r\n{auth}\r\n'.encode())
        resp = sock.recv(4096).decode('latin-1')
        if ' 200' not in resp.split('\r\n', 1)[0]:
            sock.close(); return f'connect:{resp[:20]!r}'
        tls = ssl.create_default_context().wrap_socket(sock, server_hostname=SENTRY_HOST)
        tls.close(); return 'ok'
    except Exception as e:
        return f'err:{str(e)[:40]}'

def egress_ip():
    for _ in range(3):
        try:
            r = cr.get('https://api.ipify.org', timeout=12, proxies={'http': PROXY, 'https': PROXY}, impersonate=IMP)
            if r.status_code == 200 and re.fullmatch(r'[0-9a-fA-F.:]{3,45}', r.text.strip()): return r.text.strip()
        except Exception as e:
            say(f'proxy check failed: {str(e)[:100]}')
        time.sleep(3 + random.random() * 3)
    return None

# -- One area -------------------------------------------------------------------

def scrape_area(claim, ctx):
    """Returns (kind, http_status). Each machine handles its own 429/403 independently."""
    area_id = claim['noon_area_id']; area_name = claim.get('area_name') or str(area_id)
    lat, lng = float(claim['lat']), float(claim['lng'])
    max_pages = int(claim.get('max_pages') or 120)
    zone = dict(claim.get('zone_headers') or {})
    c = Customer()
    area_start = time.time()
    pages_done, token, captures, seen = 0, None, [], set()
    stop_reason, search_403, search_429 = None, 0, 0
    mint_calls, mint_403 = 0, 0
    state = {'minted_at': None, 'refresh_at': 280.0, 'refreshes': 0, 'remints': 0,
             'remints_429': 0, 'remint429_ok': 0, 'after_remint429': []}
    dev_start = c.dev
    imp_n = int(ctx.get('impressions_per_page') or 6)

    # Pacing: this machine's persona decides the gap. No shared budget.
    pace_lo, pace_hi = ctx['pace']

    def new_refresh_at():
        lo, hi = float(claim.get('refresh_min_s') or 270), float(claim.get('refresh_max_s') or 290)
        return lo + random.random() * max(0.0, hi - lo)

    def refresh_token():
        st, _ = c.send('token_refresh', WHOAMI_URL, c.base({'x-whoami-request-id': str(uuid.uuid4())}))
        if st == 403:
            st, _ = remint_after_403('token_refresh')
        elif st == 200:
            state['minted_at'] = time.time(); state['refresh_at'] = new_refresh_at(); state['refreshes'] += 1
        return st

    say(f"=== AREA {area_name} id={area_id} | persona={ctx['persona']} | dev={c.dev[:8]} ===")

    def finish(kind, status=None, step=None, err=None, fail_page=None):
        token_age = int((time.time() - state['minted_at']) * 1000) if state['minted_at'] else None
        elapsed = int((time.time() - area_start) * 1000)
        result = {
            'outcome': kind, 'fail_step': step, 'http_status': status, 'error': (err or '')[:500] or None,
            'fail_page': fail_page,
            'pages_done': pages_done, 'cards': len(captures), 'stop_reason': stop_reason,
            'setup_ms': int((state['minted_at'] - area_start) * 1000) if state['minted_at'] else None,
            'elapsed_ms': elapsed, 'token_age_ms': token_age,
            'requests': c.noon_req,  # total Noon requests this machine made for this area
            'device_id': dev_start[:8],
            'device_id_end': c.dev[:8],
            'search_403': search_403, 'search_429': search_429,
            'mint_calls': mint_calls, 'mint_403': mint_403,
            'remints': state['remints'], 'refreshes': state['refreshes'],
            'remints_429': state['remints_429'], 'remint429_ok': state['remint429_ok'],
            'after_remint429': state['after_remint429'],
            'et_posts': c.et_posts, 'et_events': c.et_events, 'et_non2xx': c.et_non2xx,
            'persona': ctx['persona'], 'pace': f"{pace_lo}-{pace_hi}",
            'flow': 'fallback-lab', 'http_protocol': 'curl_cffi_safari15_3',
            'cookies': c.log,  # full request log: every request with step, status, ms, timestamp
        }
        say(f"area {area_name}: {kind}{f' ({status} @ {step})' if kind != 'ok' else ''}"
            f" pages={pages_done} cards={len(captures)} reqs={c.noon_req}"
            f" token_age={token_age}ms remints={state['remints']} r429={state['remints_429']}"
            f" elapsed={elapsed}ms")
        return result

    def kind_of(st): return 'rate_limited' if st == 429 else 'blocked' if st == 403 else 'error'

    loc_h = {'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng), 'Content-Type': 'application/json'}

    def mint(tag):
        """Fast mint: real app speed (~2-3s total), no artificial pauses."""
        nonlocal zone, mint_calls, mint_403
        mint_t0 = time.time()
        st, _ = c.send(f'{tag}whoami', WHOAMI_URL, c.base({'x-whoami-request-id': str(uuid.uuid4())}))
        mint_calls += 1
        if st != 200:
            mint_403 += 1 if st in (403, 429) else 0
            say(f'{tag}whoami: {st} ({int((time.time() - mint_t0) * 1000)}ms)')
            return st, f'{tag}whoami'
        state['minted_at'] = time.time(); state['refresh_at'] = new_refresh_at()
        c.et(f'et_{tag}launch', [ev_launch(c.vis, c.dev), ev_launch_info(c.vis, c.dev), ev_app_foreground(c.vis, c.dev)])
        time.sleep(0.1 + random.random() * 0.15)
        st, _ = c.send(f'{tag}set_location', SET_LOC_URL, c.base(loc_h), method='POST',
                        body=json.dumps({'location': {'lat': lat, 'lng': lng}, 'lang': 'en'}))
        mint_calls += 1
        if st != 200:
            mint_403 += 1 if st in (403, 429) else 0
            say(f'{tag}set_location: {st} ({int((time.time() - mint_t0) * 1000)}ms)')
            return st, f'{tag}set_location'
        c.et(f'et_{tag}location', [ev_location_fetch(c.vis, c.dev), ev_page_home(c.vis, c.dev)])
        if not zone:
            time.sleep(0.1 + random.random() * 0.1)
            fd_h = {'x-experience': 'food', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng),
                    'x-mp-country': 'ae', 'x-lat': str(round(lat * 1e7)), 'x-lng': str(round(lng * 1e7))}
            st, txt = c.send(f'{tag}fd_whoami', FD_WHOAMI_URL, c.base(fd_h))
            mint_calls += 1
            if st != 200:
                mint_403 += 1 if st in (403, 429) else 0
                say(f'{tag}fd_whoami: {st} ({int((time.time() - mint_t0) * 1000)}ms)')
                return st, f'{tag}fd_whoami'
            try:
                j = json.loads(txt)
                zone = {k: v for k, v in (j.get('headers') or {}).items() if v is not None and not isinstance(v, (dict, list))}
            except Exception:
                zone = {}
        c.et(f'et_{tag}zone', [ev_whoami_ab_config(zone, c.vis, c.dev), ev_selected_zone(zone, c.vis, c.dev)])
        say(f'{tag}DONE in {int((time.time() - mint_t0) * 1000)}ms | reqs={c.noon_req}')
        return 200, None

    def remint_after_403(where):
        limit = int(claim.get('remints_403') if claim.get('remints_403') is not None else 2)
        st, fstep = 403, where
        while st == 403 and state['remints'] < limit:
            state['remints'] += 1
            old = c.dev[:8]
            c.new_identity()
            say(f'403 at {fstep}: new identity #{state["remints"]} ({old} -> {c.dev[:8]})')
            time.sleep(3 + random.random() * 3)
            st, fstep = mint(f'remint{state["remints"]}_')
            if st == 200:
                return 200, None
        return st, fstep

    try:
        # --- warm-up (fast: no artificial pauses) ---
        sentry = sentry_preflight()
        c.send('etracker_init', ETRACKER_INIT, {'User-Agent': UA, 'Accept': '*/*', 'Connection': 'keep-alive'})
        time.sleep(0.3 + random.random() * 0.2)

        # --- mint (fast: real app speed) ---
        st, fstep = mint('mint_')
        if st == 403:
            st, fstep = remint_after_403(fstep)
        if st != 200:
            return finish(kind_of(st), st, fstep, f'{fstep} {st} (sentry={sentry})')
        say(f'MINT OK in {int((time.time() - area_start) * 1000)}ms | dev={c.dev[:8]} reqs={c.noon_req}')
        time.sleep(0.3 + random.random() * 0.3)

        # --- pagination (fast: ~1s per page, real app scroll speed) ---
        # Per-page verbose logging so you can see progress live
        search_body = {'queryEntity': None, 'q': None, 'f': {}, 'contexts': [], 'withContent': True, 'excludeOutletCodes': [],
                'limit': 60, 'scopes': None, 'page': 1, 'dishTags': [], 'ratingMin': None,
                'sort': {'by': 'popularity', 'dir': 'desc', 'slab': None}, 'widgets': None, 'content': None,
                'withHits': False, 'filteredByWidget': None, 'internalFilteredByWidget': 'homepage_navpill_filters_widget',
                'getFallback': True, 'type': 'outlet', 'qType': 'search_query', 'minCount': 0,
                'groupDishes': False, 'isSsc': False, 'hideSearchBar': False, 'hideSearchOptions': False,
                'isOutletAdProcessingEnabled': True, 'withDishFetchQuery': None}
        sh_base = {'x-experience': 'food', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng),
              'x-mp-country': 'ae', 'x-lat': str(round(lat * 1e7)), 'x-lng': str(round(lng * 1e7)),
              'x-border-enabled': 'true', 'x-rocket-enabled': 'true', 'Content-Type': 'application/json'}
        for k, v in zone.items():
            if v is not None and not isinstance(v, (dict, list)):
                sh_base[k] = str(v).lower() if isinstance(v, bool) else str(v)
        sh_base['x-ab-test'] = AB_TEST

        while pages_done < max_pages:
            # Token refresh if approaching 300s (refresh at 270-290s)
            if state['minted_at']:
                token_age = time.time() - state['minted_at']
                if token_age > state['refresh_at']:
                    say(f'TOKEN REFRESH at {int(token_age)}s (limit {int(state["refresh_at"])}s) reqs={c.noon_req}')
                    st = refresh_token()
                    if st != 200:
                        return finish(kind_of(st), st, 'token_refresh', f'token refresh {st} at page {pages_done + 1}', fail_page=pages_done + 1)
                    say(f'TOKEN REFRESHED OK | new age=0s reqs={c.noon_req}')

            # Page gap: ~1-2s like a real user scrolling (not 6-30s persona pacing)
            if pages_done > 0:
                time.sleep(0.8 + random.random() * 1.2)

            # Search
            body = dict(search_body)
            if token: body['searchToken'] = token
            page_t0 = time.time()
            st, txt = c.send('search', SEARCH_URL, c.base(sh_base), method='POST', body=json.dumps(body))
            page_ms = int((time.time() - page_t0) * 1000)

            # Flaky response: one retry
            if st in (None, 502, 503, 504):
                say(f'p{pages_done + 1}: {st} ({page_ms}ms) — retrying once')
                time.sleep(2 + random.random() * 1)
                st, txt = c.send('search_retry1', SEARCH_URL, c.base(sh_base), method='POST', body=json.dumps(body))
                page_ms = int((time.time() - page_t0) * 1000)

            if st == 403: search_403 += 1
            if st == 429: search_429 += 1

            # 403: new identity and retry
            if st == 403:
                say(f'p{pages_done + 1}: 403 ({page_ms}ms) reqs={c.noon_req} token_age={int((time.time() - state["minted_at"]) * 1000)}ms')
                rst, _ = remint_after_403(f'search p{pages_done + 1}')
                if rst == 200:
                    continue
                return finish(kind_of(rst), rst, 'search', f'403 at page {pages_done + 1}, remint failed', fail_page=pages_done + 1)

            # 429: KEEP SAME DEVICE. Wait to see how long 429 lasts, then retry with token refresh.
            if st == 429:
                token_age = int((time.time() - state['minted_at']) * 1000) if state['minted_at'] else 0
                say(f'p{pages_done + 1}: 429 ({page_ms}ms) reqs={c.noon_req} token_age={token_age}ms — WAITING to measure cooldown...')
                # Try waiting increasing intervals: 15s, 30s, 60s — same device, same IP
                recovered = False
                for wait_s in [15, 30, 60]:
                    say(f'  waiting {wait_s}s (same device {c.dev[:8]})...')
                    time.sleep(wait_s)
                    # Refresh token if it's near 300s
                    if state['minted_at'] and (time.time() - state['minted_at']) > 250:
                        say(f'  token near expiry ({int(time.time() - state["minted_at"])}s), refreshing...')
                        rst = refresh_token()
                        say(f'  token refresh: {rst}')
                        if rst != 200:
                            say(f'  token refresh failed, minting fresh...')
                            mst, mstep = mint(f'429wait_remint_')
                            if mst != 200:
                                say(f'  remint also failed: {mst}')
                                continue
                    # Retry the same search
                    retry_t0 = time.time()
                    st2, txt2 = c.send(f'search_after_{wait_s}s', SEARCH_URL, c.base(sh_base), method='POST', body=json.dumps(body))
                    retry_ms = int((time.time() - retry_t0) * 1000)
                    say(f'  after {wait_s}s: {st2} ({retry_ms}ms) reqs={c.noon_req}')
                    if st2 == 200:
                        st, txt = st2, txt2
                        recovered = True
                        say(f'  RECOVERED after {wait_s}s wait!')
                        break
                    if st2 == 429:
                        search_429 += 1
                        continue
                    if st2 == 403:
                        search_403 += 1
                        say(f'  switched to 403 after wait — need new identity')
                        break
                if not recovered:
                    return finish('rate_limited', 429, 'search', f'429 at page {pages_done + 1}, not recovered after 15+30+60s wait', fail_page=pages_done + 1)

            if st == 404: stop_reason = f'404@{pages_done + 1}'; break
            if st != 200:
                say(f'p{pages_done + 1}: {st} ({page_ms}ms) — stopping')
                return finish(kind_of(st), st, 'search', f'search {st} at page {pages_done + 1}', fail_page=pages_done + 1)

            try: j = json.loads(txt)
            except Exception: stop_reason = f'bad-json@{pages_done + 1}'; break
            outlets = walk_outlets(j.get('results'))
            pages_done += 1
            fresh = 0
            for o in outlets:
                if o['outletCode'] not in seen: seen.add(o['outletCode']); captures.append(o); fresh += 1

            # PER-PAGE LOG: every single page visible in the Actions log
            token_age = int((time.time() - state['minted_at']) * 1000) if state['minted_at'] else 0
            elapsed = int((time.time() - area_start) * 1000)
            say(f'p{pages_done}: {len(outlets)} outlets (+{fresh} new, {len(seen)} total) {page_ms}ms | reqs={c.noon_req} token_age={token_age}ms elapsed={elapsed}ms')

            if not outlets: stop_reason = f'empty@{pages_done}'; break
            if fresh == 0: stop_reason = f'no-new@{pages_done}'; break
            if not j.get('searchToken'): stop_reason = f'no-token@{pages_done}'; break
            token = j['searchToken']

            # Telemetry after each page (fast: minimal pauses)
            c.et('et_page_catalog', [ev_page_catalog(pages_done, c.vis, c.dev)])
            visible = outlets[:imp_n]
            if visible:
                time.sleep(0.1 + random.random() * 0.2)
                c.et('et_impressions', impression_triplets(visible, c.vis, c.dev))
        if not stop_reason: stop_reason = f'max-pages({max_pages})'
        return finish('ok')
    except Exception as e:
        say(f'area error: {str(e)[:200]}')
        return finish('error', None, 'other', str(e))

# -- Main -----------------------------------------------------------------------

def main():
    global PROXY, RUNNER_IP
    if not (SUPABASE_URL and SUPABASE_KEY and TEST_ID and MACHINE and FLEET in ('uae', 'decodo')):
        say('missing env (SUPABASE_URL/KEY, TEST_ID, MACHINE_NO, FLEET)'); sys.exit(1)
    PROXY = pick_proxy()
    if not PROXY:
        say('no proxy configured — refusing to run (never goes direct)')
        sys.exit(1)

    RUNNER_IP = egress_ip()
    if not RUNNER_IP:
        say('proxy unreachable'); sys.exit(1)
    say(f'egress {RUNNER_IP} via {proxy_label(PROXY)}')

    spec = rpc('noon_ranking_scrape_spec', {}) or {}
    build, ua = apply_app_identity(spec.get('app') if isinstance(spec, dict) else None)
    say(f'app {build} | ua {ua}')

    tel = (spec.get('telemetry') if isinstance(spec, dict) else None) or {}
    lo, hi = int(tel.get('impressions_per_page_min') or 4), int(tel.get('impressions_per_page_max') or 10)

    # Persona: each machine picks its own independently
    weights = {'scroller': 25, 'browser': 55, 'reader': 20}
    paces = {'scroller': [2, 6], 'browser': [6, 15], 'reader': [15, 30]}
    names = list(weights.keys())
    persona = random.choices(names, weights=[weights[n] for n in names])[0]
    plo, phi = paces[persona]
    say(f'persona {persona} (page gap {plo}-{phi}s) | impressions {lo}-{hi}')

    ctx = {'non_food': set(), 'dry_run': True, 'scrape_date': None, 'scrape_hour': None,
           'area_seq': 0, 'last_gap_s': 0, 'persona': persona, 'pace': (float(plo), float(phi)),
           'impressions_per_page': random.randint(min(lo, hi), max(lo, hi))}

    # Area loop: claim from noon_lab
    areas_done = 0
    max_areas = int(os.environ.get('MAX_AREAS') or 5)
    while areas_done < max_areas:
        if time.time() - START > WALL_LIMIT_S: say('wall limit'); break

        claim = rpc('noon_lab_claim', {'p_test_id': TEST_ID, 'p_machine_no': MACHINE,
                                        'p_github_run_id': GH_RUN_ID, 'p_runner_ip': RUNNER_IP})
        if not claim or claim.get('stop') or claim.get('noon_area_id') is None:
            say(f'no more areas: {(claim or {}).get("reason", "done")}'); break

        ctx['area_seq'] += 1
        say(f'area {areas_done + 1}/{max_areas}: {claim.get("area_name")} id={claim["noon_area_id"]}')
        result = scrape_area(claim, ctx)

        # Report to lab — full result with all diagnostics
        try:
            rpc('noon_lab_report', {'p_test_id': TEST_ID, 'p_machine_no': MACHINE,
                                     'p_item_id': claim.get('item_id'), 'p_result': result})
        except Exception as e:
            say(f'report failed: {str(e)[:120]}')

        areas_done += 1
        if areas_done < max_areas:
            gap = 15 + random.random() * 60
            say(f'gap {gap:.0f}s before next area')
            time.sleep(gap)

    say(f'=== done: {areas_done} areas ===')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        say(f'FATAL: {e}'); sys.exit(1)
