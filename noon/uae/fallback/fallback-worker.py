#!/usr/bin/env python3
"""noon/uae/fallback/fallback-worker.py — PRODUCTION fallback worker (UAE IP / Decodo).

WHAT THIS IS
  The peak-hour fallback fleet. Supabase launches these machines only when the decoy
  fleet (the old GitHub scraper, noon/uae/ranking-scrape.js) is being blocked
  (noon_ranking_fb_detect), or when fallback is forced on for an hour.

  Network behaviour = the lab script that passed tests 15, 16 and 18 through the UAE IP
  and Decodo (noon-mint-lab/warmup-lab.py, variant U5): curl_cffi Safari 15.3 fingerprint,
  mobile-sentry + etracker warm-up, whoami, launch telemetry, set-location, zone headers
  from noon_lab.mint_zone_cache, x-ab-test, persona-paced pagination, no identity swaps.

  Output = exactly what production writes (copied from noon/uae/ranking-scrape.js):
  37-column rows, promo classifier, non-food filter, noon_ranking_save_area_hour,
  noon_ranking_report, noon_scrape_audit, page checkpoints (handover) and heartbeat.

DIFFERENCES FROM THE DECOY MACHINES
  * Every Noon request goes through the proxy. If the proxy is unreachable the machine
    stops; it never falls back to the GitHub runner's own IP.
  * One machine works many areas until the deadline. Each area is a fresh identity
    (new device id, visitor id, cookies) with a random pause between areas, like
    different customers on the same Wi-Fi.
  * Areas come from noon_ranking_fb_claim (fallback machines get first pick).
  * All controls live in Supabase (noon_ranking_settings.fb_*); nothing to change here.

Env (set by .github/workflows/noon-uae-fallback.yml):
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RUN_ID, MACHINE_NO, GITHUB_RUN_ID,
  FLEET (uae | decodo), PROXY_PORT (decodo only), NOON_PROXY_URL (secret for this fleet),
  WALL_LIMIT_SEC (optional)
"""
import base64, json, os, random, re, secrets, socket, ssl, sys, threading, time, uuid
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote
from curl_cffi import requests as cr

# -- Env ------------------------------------------------------------------------

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
RUN_ID = int(os.environ.get('RUN_ID') or 0)
MACHINE = int(os.environ.get('MACHINE_NO') or 0)
GH_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
FLEET = (os.environ.get('FLEET') or '').strip().lower()
PROXY_PORT = (os.environ.get('PROXY_PORT') or '').strip()
WALL_LIMIT_S = int(os.environ.get('WALL_LIMIT_SEC') or 4000)
# Set by multi-worker.py (DC fleet): several workers share ONE run_machine row. The coordinator does
# machine_start / machine_end once; workers skip both and get the start payload via FB_START_JSON.
SHARED = os.environ.get('FB_SHARED_MACHINE') == '1'
START_JSON = os.environ.get('FB_START_JSON') or ''
# DC: each worker has its own IP, so page pacing is local to the worker (the Supabase bucket key
# for 'dc' is fleet-wide and would squeeze every DC IP into one IP's budget).
IP_PAGES_PER_MIN = float(os.environ.get('FB_IP_PAGES_PER_MIN') or 12)
DEADLINE_MARGIN_S = 20  # stop paging this long before the run deadline so the partial save lands by :55
START = time.time()

P = f'[fb m{MACHINE} {FLEET}{":" + PROXY_PORT if PROXY_PORT else ""}]'
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
    return f'{u.hostname}:{u.port}'  # never log credentials

PROXY = None  # set in main()

# -- Noon constants (lab U5 + production app identity) --------------------------

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

# -- etracker telemetry (ported from noon/uae/ranking-scrape.js, iPhone Proxyman schemas) --

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
def impression_pairs(outlets, v, i):
    """Lab-proven pattern: staggered v2+v1 pairs only (no promotion_impression).
    Matches real Noon app behaviour confirmed via Proxyman capture."""
    events = []
    for idx, o in enumerate(outlets):
        oc = o.get('outletCode'); df = o.get('distance') or 5.0; sla = o.get('sla') or '30-40'
        events.append(evt('outlet_impression_v2', {'pg': 'SR', 'wt': f'SR/search_results/0/search_results_tab/{idx}/ROW/{idx}/ITM/0',
                          'ul': '/search?type=outlet', 'cf': '{}', 'oc': oc, 'df': df, 'sla': sla, **food_app()}, v, i))
        events.append(evt('outlet_impression', {'oc': oc, 'wid': f'search_outlet/{idx}', 'aid': oc, 'ix': f'0/{idx}', 'otg': False,
                          'df': df, 'fav': False, 'isp': False, 'sla': sla, 'wn': 'search_results', 'wtc': 'search_results_tab',
                          'curl': '/search?type=outlet', 'src': 'search/results', 'pn': 'Home_page', **food_app()}, v, i))
    return events

# -- Supabase (direct, never through the proxy) ---------------------------------

def rpc(name, params):
    """Retry a busy/unreachable database for ~60s like production; 4xx (not 408/429) is permanent."""
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

def rest_patch(table, query, body):
    try:
        cr.patch(f'{SUPABASE_URL}/rest/v1/{table}?{query}', json=body, timeout=5,
                 headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}',
                          'Content-Type': 'application/json', 'Prefer': 'return=minimal'})
    except Exception as e:
        say(f'heartbeat failed: {str(e)[:120]}')

STOP = threading.Event()  # set when Supabase has closed this machine (fallback over, deadline, run closed)

def heartbeat_loop():
    while True:
        time.sleep(30)
        try:
            r = cr.patch(f'{SUPABASE_URL}/rest/v1/noon_ranking_run_machine?run_id=eq.{RUN_ID}&machine_no=eq.{MACHINE}&select=status',
                         json={'last_seen_at': now_iso()}, timeout=5,
                         headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}',
                                  'Content-Type': 'application/json', 'Prefer': 'return=representation'})
            rows = r.json() if r.status_code < 300 else None
            if isinstance(rows, list) and rows and rows[0].get('status') not in ('working', 'dispatched'):
                if not STOP.is_set(): say(f"machine closed by Supabase (status={rows[0].get('status')}) - stopping")
                STOP.set()
        except Exception as e:
            say(f'heartbeat failed: {str(e)[:120]}')

def permit(penalty_s=0):
    """Per-IP page budget (Supabase noon_ranking_fb_page_permit): seconds to wait before the next Noon page.
    penalty_s > 0 after a 429: every machine on this IP backs off that long. Never blocks scraping on a DB hiccup."""
    if FLEET == 'dc':
        return local_permit(penalty_s)
    try:
        r = cr.post(f'{SUPABASE_URL}/rest/v1/rpc/noon_ranking_fb_page_permit', timeout=8,
                    json={'p_run_id': RUN_ID, 'p_machine_no': MACHINE, 'p_penalty_s': int(penalty_s)},
                    headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'})
        if r.status_code < 300:
            return max(0, int((r.json() or {}).get('wait_ms') or 0)) / 1000.0
        say(f'permit HTTP {r.status_code}')
    except Exception as e:
        say(f'permit failed: {str(e)[:100]}')
    return float(penalty_s)

_local_next_at = 0.0
def local_permit(penalty_s=0):
    """Same token-bucket maths as noon_ranking_fb_page_permit, kept in-process (one worker = one IP)."""
    global _local_next_at
    now = time.time()
    if penalty_s and penalty_s > 0:
        _local_next_at = max(_local_next_at, now + penalty_s)
        return float(penalty_s)
    slot = max(_local_next_at, now)
    _local_next_at = slot + (60.0 / max(IP_PAGES_PER_MIN, 0.5)) * (0.7 + random.random() * 0.6)
    return max(0.0, slot - now)

RUNNER_IP = None
def audit(row):
    body = {'run_id': RUN_ID, 'machine_no': MACHINE, 'github_run_id': GH_RUN_ID, 'runner_ip': RUNNER_IP,
            'machine_started_at': now_iso(START), 'machine_age_s': round(time.time() - START), 'areas_cap': 0}
    body.update(row)
    try:
        r = cr.post(f'{SUPABASE_URL}/rest/v1/noon_scrape_audit', json=body, timeout=10,
                    headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}',
                             'Content-Type': 'application/json', 'Prefer': 'return=minimal'})
        if r.status_code >= 300: say(f'audit HTTP {r.status_code}: {r.text[:200]}')
    except Exception as e:
        say(f'audit failed: {str(e)[:120]}')

# -- Output side (ported 1:1 from noon/uae/ranking-scrape.js) -------------------

DIRHAM = ''
def js_truthy(x):
    if x is None or x is False: return False
    if isinstance(x, (int, float)) and not isinstance(x, bool): return x != 0 and x == x
    if isinstance(x, str): return x != ''
    return True  # objects/arrays (even empty) are truthy in JS
def js_or(x, default=None): return x if js_truthy(x) else default
def is_num(x): return isinstance(x, (int, float)) and not isinstance(x, bool)
def clean_text(s):
    if not isinstance(s, str): return None
    t = s.replace(DIRHAM, 'AED').strip()
    return t or None
def js_parse_float(s):
    m = re.match(r'\s*([+-]?(?:\d+\.?\d*|\.\d+))', s or '')
    return float(m.group(1)) if m else None

CDN_PREFIX = 'https://f.nooncdn.com/food_production/'
def cdn_url(key): return CDN_PREFIX + key.strip() + '?width=720&format=webp' if isinstance(key, str) and key.strip() else None

def derive_rating_status(title):
    if title is None or str(title).strip() == '': return 'not_rated'
    if re.fullmatch(r'new', str(title).strip(), re.I): return 'new'
    return 'rated'

def classify_promo(tag, extended_tag, mini_tag):
    texts = [t for t in (extended_tag, tag, mini_tag) if isinstance(t, str) and t.strip()]
    if not texts: return {'scope': None, 'percent': None, 'cap': None}
    joined = ' | '.join(texts)
    if re.search(r'\bfree\s*delivery\b', joined, re.I): return {'scope': 'free_delivery', 'percent': None, 'cap': None}
    pm = re.search(r'(\d{1,3})\s*%', joined)
    percent = min(100, int(pm.group(1))) if pm else None
    cm = re.search(r'up\s*to\s*[^0-9]*([\d.]+)', joined, re.I)
    cap = js_parse_float(cm.group(1)) if cm else None
    if re.search(r'bestseller', joined, re.I): return {'scope': 'percent_off_bestsellers', 'percent': percent, 'cap': cap}
    if re.search(r'select\s*items?', joined, re.I): return {'scope': 'percent_off_select_items', 'percent': percent, 'cap': cap}
    if re.search(r'full\s*menu', joined, re.I): return {'scope': 'percent_off_full_menu', 'percent': percent, 'cap': cap}
    if re.search(r'\babove\b', joined, re.I):
        am = re.search(r'above\s*[^0-9]*([\d.]+)', joined, re.I)
        return {'scope': 'percent_off_above_min', 'percent': percent, 'cap': js_parse_float(am.group(1)) if am else None}
    if re.search(r'\bflat\b', joined, re.I): return {'scope': 'flat_percent_off', 'percent': percent, 'cap': None}
    if re.search(r'up\s*to', joined, re.I): return {'scope': 'percent_off_capped', 'percent': percent, 'cap': cap}
    if percent is not None: return {'scope': 'percent_off', 'percent': percent, 'cap': None}
    return {'scope': 'offers_generic', 'percent': None, 'cap': None}

def is_restaurant(o, non_food):
    cuisines = o.get('cuisines') if isinstance(o.get('cuisines'), list) else []
    if not cuisines: return True
    return any(c not in non_food for c in cuisines)

def parse_cards(outlets, non_food):
    seen, rows, dropped = set(), [], 0
    for o in outlets:
        code = o.get('outletCode')
        if not js_truthy(code) or code in seen: continue
        if not is_restaurant(o, non_food): dropped += 1; continue
        seen.add(code)
        rank = len(rows) + 1
        rt = o.get('rating') if js_truthy(o.get('rating')) and isinstance(o.get('rating'), dict) else {}
        rating_title = js_or(rt.get('title'))
        tag = o.get('tag') if isinstance(o.get('tag'), str) else None
        mini = o.get('miniTag') if isinstance(o.get('miniTag'), str) else None
        ext = o.get('extendedTag') if isinstance(o.get('extendedTag'), str) else None
        has_promo = js_truthy(tag) or js_truthy(mini) or js_truthy(ext)
        cl = classify_promo(tag, ext, mini)
        free_deliv = cl['scope'] == 'free_delivery'
        d = o.get('delivery') if js_truthy(o.get('delivery')) and isinstance(o.get('delivery'), dict) else {}
        media = o.get('media') if isinstance(o.get('media'), dict) else {}
        banner = media.get('heroImage') if isinstance(media.get('heroImage'), str) else None
        logo = media.get('logoImage') if isinstance(media.get('logoImage'), str) else None
        rows.append([
            code, rank,
            js_or(o.get('operatingStatusCode')), js_or(o.get('operatingType')),
            rt.get('score'), rt.get('count'),
            derive_rating_status(rating_title), has_promo,
            clean_text(tag) or clean_text(ext) or clean_text(mini),
            cl['scope'], cl['cap'], js_or(o.get('widgetGroup')), js_truthy(o.get('isPersonalized')),
            False, None, js_or(rt.get('displayCount')), clean_text(mini), clean_text(ext),
            free_deliv, has_promo and not free_deliv, js_truthy(o.get('isNearAndQuick')),
            js_or(o.get('name')), js_or(o.get('linkUrl')), cdn_url(banner),
            o.get('cuisines') if isinstance(o.get('cuisines'), list) else [],
            js_or(o.get('restaurantCode')),
            o.get('price') if isinstance(o.get('price'), str) else None,
            o.get('distance') if is_num(o.get('distance')) else None,
            rating_title,
            d.get('fee') if is_num(d.get('fee')) else None,
            d.get('originalFee') if is_num(d.get('originalFee')) else None,
            d.get('minOrder') if is_num(d.get('minOrder')) else None,
            d.get('time') if isinstance(d.get('time'), str) else None,
            cl['percent'], cdn_url(logo),
            o.get('isVip') if isinstance(o.get('isVip'), bool) else None,
            js_or(o.get('vipMsg')),
        ])
    return rows, dropped

def trim_outlet(o):
    t = {k: o[k] for k in ('outletCode', 'operatingStatusCode', 'operatingType', 'tag', 'miniTag', 'extendedTag', 'widgetGroup',
                          'isPersonalized', 'isNearAndQuick', 'name', 'linkUrl', 'cuisines', 'restaurantCode', 'price', 'distance',
                          'isVip', 'vipMsg') if k in o and o[k] is not None}
    for src, keys in (('rating', ('score', 'count', 'title', 'displayCount')), ('delivery', ('fee', 'originalFee', 'minOrder', 'time')),
                      ('media', ('heroImage', 'logoImage'))):
        x = o.get(src)
        if isinstance(x, dict):
            t[src] = {k: x[k] for k in keys if k in x and x[k] is not None}
    return t

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

# -- Network: one Noon "customer" (lab U5 behaviour) -----------------------------

class Customer:
    """One phone on the shared connection: fresh device/visitor ids and cookies."""
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
        """A fresh phone session (production's swap: new device id, visitor id and cookies)."""
        self.dev = str(uuid.uuid4()).upper()
        self.vis = str(uuid.uuid4()).lower()
        self.jar_noon, self.jar_et = {}, {}

    def et(self, step, events):
        """POST events to etracker with the app's full headers (production et())."""
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
    """TCP+TLS handshake to mobile-sentry through the proxy tunnel (like the app's first CONNECT). No request is sent."""
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
    """Returns (kind, http_status). kind: ok | blocked | rate_limited | error."""
    area_id = claim['noon_area_id']; area_name = claim.get('area_name') or str(area_id)
    lat, lng = float(claim['lat']), float(claim['lng'])
    max_pages = int(claim.get('max_pages') or 120)
    zone = dict(claim.get('zone_headers') or {})
    zone_cached = bool(zone)
    c = Customer()
    area_start = time.time()
    pages_done, token, captures, seen = 0, None, [], set()
    stop_reason, search_403, search_429, et_posts, et_events, et_non2xx = None, 0, 0, 0, 0, 0
    mint_calls, mint_403 = 0, 0
    resumed_from = 0
    state = {'minted_at': None, 'paused': False, 'remints': 0,
             'remints_429': 0, 'remint429_ok': 0, 'after_remint429': []}
    on_429 = str(claim.get('on_429') or 'pause')  # Supabase: 'remint' = new device + new token on a search 429
    dev_start = c.dev
    imp_n = int(ctx.get('impressions_per_page') or 6)
    # Hard stop at the run deadline (:55): stop paging DEADLINE_MARGIN_S early and save what is scraped.
    try: stop_at = datetime.fromisoformat(str(claim.get('deadline_at')).replace('Z', '+00:00')).timestamp() - DEADLINE_MARGIN_S
    except Exception: stop_at = None
    def past_deadline(): return stop_at is not None and time.time() >= stop_at

    # Token refresh REMOVED: Proxyman confirmed the real Noon app never refreshes
    # the guest token mid-session. Refreshing was triggering 429s.

    def send429(step, url, headers_fn, method='GET', body=None):
        """One 30 s pause per area on a 429 (the whole IP backs off via Supabase), then one retry."""
        st, txt = c.send(step, url, headers_fn(), method=method, body=body)
        if st == 429 and step == 'search' and on_429 == 'remint':
            return st, txt  # the caller swaps to a new device + new token instead of pausing
        if st == 429 and not state['paused']:
            state['paused'] = True
            w = permit(int(claim.get('pause_429_s') or 30))
            say(f'429 at {step}: pausing {w:.0f}s (whole IP) then one retry')
            w = w + random.random() * 3
            time.sleep(w)
            st, txt = c.send(f'{step}_after429', url, headers_fn(), method=method, body=body)
            say(f'  after pause: {step} -> {st}')
        return st, txt

    say(f"=== AREA {area_name} id={area_id} | persona={ctx['persona']} | dev={c.dev[:8]} ===")
    cp = claim.get('checkpoint') if isinstance(claim.get('checkpoint'), dict) else None
    if cp and int(cp.get('pages_done') or 0) > 0 and cp.get('search_token') and isinstance(cp.get('captures'), list):
        for o in cp['captures']:
            if isinstance(o, dict) and o.get('outletCode') and o['outletCode'] not in seen:
                seen.add(o['outletCode']); captures.append(o)
        pages_done = int(cp['pages_done']); token = cp['search_token']; resumed_from = pages_done
        say(f'RESUME from checkpoint: page {pages_done + 1}, {len(captures)} cards carried over')

    def finish(kind, status=None, step=None, err=None, fail_page=None):
        rows, dropped = ([], 0)
        saved = pending = 0
        if kind == 'stopped':  # Supabase closed this machine: the area is no longer ours - no save, no report
            say(f'area {area_name}: stopped by Supabase at page {pages_done}, nothing saved')
            return kind, None
        if kind == 'ok' and pages_done == 0 and str(stop_reason or '').startswith('deadline'):
            kind = 'deadline'
        if kind == 'deadline':  # deadline before the first page: nothing to save
            rep = rpc('noon_ranking_report', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE, 'p_area_id': area_id,
                                              'p_result': {'status': 'deadline'}})
            say(f'area {area_name}: deadline before the first page | report={json.dumps(rep)[:100]}')
            return kind, None
        if kind == 'ok':
            rows, dropped = parse_cards(captures, ctx['non_food'])
            if not ctx['dry_run'] and rows:
                sv = rpc('noon_ranking_save_area_hour', {'p_area_id': area_id, 'p_date': ctx['scrape_date'],
                                                         'p_hour': ctx['scrape_hour'], 'p_rows': rows})
                saved = (sv or {}).get('inserted', len(rows)) if isinstance(sv, dict) else len(rows)
                pending = (sv or {}).get('pending', 0) if isinstance(sv, dict) else 0
            rep = rpc('noon_ranking_report', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE, 'p_area_id': area_id, 'p_result': {
                'status': 'ok', 'pages_scraped': pages_done, 'cards': len(captures), 'ranking_rows': saved,
                'pending_rows': pending, 'dropped': dropped, 'bytes': 0, 'fetch_ms': int((time.time() - area_start) * 1000),
                'rotations': 0, 'recoveries': 0, 'stop_reason': stop_reason}})
        else:
            if token and pages_done > 0:
                try:
                    cs = rpc('noon_ranking_checkpoint_save', {'p_run_id': RUN_ID, 'p_area_id': area_id, 'p_machine_no': MACHINE,
                                                              'p_pages_done': pages_done, 'p_search_token': token,
                                                              'p_captures': [trim_outlet(o) for o in captures]})
                    say(f'HANDOVER: checkpoint saved at page {pages_done} ({len(captures)} cards): {json.dumps(cs)[:120]}')
                except Exception as e:
                    say(f'checkpoint save failed: {str(e)[:120]}')
            rep = rpc('noon_ranking_report', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE, 'p_area_id': area_id,
                                              'p_result': {'status': kind, 'http_status': status, 'error': (err or '')[:500]}})
        audit({
            'area_seq': ctx['area_seq'], 'noon_area_id': area_id, 'area_name': area_name,
            'claimed_at': now_iso(area_start), 'ended_at': now_iso(), 'gap_s': ctx['last_gap_s'],
            'page_delay_ms': int(ctx['pace'][0] * 1000), 'max_pages': max_pages, 'token_no': 1 + state['remints'],
            'token_age_ms': int((time.time() - (state['minted_at'] or c.t0)) * 1000), 'req_before': 0, 'req_area': c.noon_req,
            'elapsed_ms': int((time.time() - area_start) * 1000), 'outcome': kind, 'pages': pages_done,
            'cards': len(captures) if kind == 'ok' else None, 'rows_parsed': len(rows) if kind == 'ok' else None,
            'rows_saved': saved if kind == 'ok' else None,
            'fail_step': step, 'fail_page': fail_page, 'http_status': status, 'error': (err or '')[:300] or None,
            'stop_reason': (f"{stop_reason} fleet={FLEET}{':' + PROXY_PORT if PROXY_PORT else ''} persona={ctx['persona']} "
                            f"pace={ctx['pace'][0]}-{ctx['pace'][1]}s resumed_from={resumed_from} no_refresh"
                            f" r429={state['remints_429']} r429_ok={state['remint429_ok']} after={state['after_remint429']}") if kind == 'ok' else
                           f"fallback fleet={FLEET}{':' + PROXY_PORT if PROXY_PORT else ''} persona={ctx['persona']}"
                           f" r429={state['remints_429']} r429_ok={state['remint429_ok']} after={state['after_remint429']}",
            'rotations': state['remints'], 'recoveries': 1 if state['paused'] else 0, 'recoveries_ok': state.get('remint_ok', 0),
            'remints': 0,  # token refresh removed — real app never refreshes
            'search_403': search_403, 'search_429': search_429, 'et_posts': c.et_posts, 'et_events': c.et_events, 'et_non2xx': c.et_non2xx,
            'device_id_start': dev_start[:8], 'device_id_end': c.dev[:8], 'req_log': c.log,
            'zone_cached': zone_cached, 'mint_calls': mint_calls, 'mint_403': mint_403, 'mint_swaps': 0,
        })
        say(f"area {area_name}: {kind}{f' ({status} @ {step})' if kind != 'ok' else ''} pages={pages_done} cards={len(captures)} "
            f"rows={saved} | report={json.dumps(rep)[:100]}")
        return kind, status

    def kind_of(st): return 'rate_limited' if st == 429 else 'blocked' if st == 403 else 'error'

    loc_h = {'x-experience': 'ecom', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng), 'Content-Type': 'application/json'}

    def mint(tag):
        """Full app mint with production telemetry: whoami -> launch events -> set-location -> location events
        -> (fd whoami if the zone is not cached) -> zone events. Returns (status, failing_step)."""
        nonlocal zone, mint_calls, mint_403
        time.sleep(permit())
        st, _ = send429(f'{tag}whoami', WHOAMI_URL, lambda: c.base({'x-whoami-request-id': str(uuid.uuid4())}))
        mint_calls += 1
        if st != 200:
            mint_403 += 1 if st in (403, 429) else 0
            return st, f'{tag}whoami'
        state['minted_at'] = time.time()
        time.sleep(0.3 + random.random() * 0.3)
        c.et(f'et_{tag}launch', [ev_launch(c.vis, c.dev), ev_launch_info(c.vis, c.dev), ev_app_foreground(c.vis, c.dev)])
        time.sleep(1.2 + random.random() * 0.8)
        st, _ = send429(f'{tag}set_location', SET_LOC_URL, lambda: c.base(loc_h), method='POST',
                        body=json.dumps({'location': {'lat': lat, 'lng': lng}, 'lang': 'en'}))
        mint_calls += 1
        if st != 200:
            mint_403 += 1 if st in (403, 429) else 0
            return st, f'{tag}set_location'
        time.sleep(0.2 + random.random() * 0.3)
        c.et(f'et_{tag}location', [ev_location_fetch(c.vis, c.dev), ev_page_home(c.vis, c.dev)])
        if not zone:  # zone headers not cached for this area: fetch once via fd whoami, like the app
            time.sleep(0.8 + random.random() * 0.6)
            fd_h = {'x-experience': 'food', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng),
                    'x-mp-country': 'ae', 'x-lat': str(round(lat * 1e7)), 'x-lng': str(round(lng * 1e7))}
            st, txt = send429(f'{tag}fd_whoami', FD_WHOAMI_URL, lambda: c.base(fd_h))
            mint_calls += 1
            if st != 200:
                mint_403 += 1 if st in (403, 429) else 0
                return st, f'{tag}fd_whoami'
            try:
                j = json.loads(txt)
                zone = {k: v for k, v in (j.get('headers') or {}).items() if v is not None and not isinstance(v, (dict, list))}
            except Exception:
                zone = {}
        time.sleep(0.15 + random.random() * 0.25)
        c.et(f'et_{tag}zone', [ev_whoami_ab_config(zone, c.vis, c.dev), ev_selected_zone(zone, c.vis, c.dev)])
        return 200, None

    def remint_after_403(where):
        """403: a new phone session (fresh device, visitor, cookies) mints a new token; up to remints_403 per area.
        Returns the final mint status (200 = new token ready) and the step that failed."""
        limit = int(claim.get('remints_403') if claim.get('remints_403') is not None else 2)
        st, fstep = 403, where
        while st == 403 and state['remints'] < limit:
            state['remints'] += 1
            old = c.dev[:8]
            c.new_identity()
            say(f'403 at {fstep}: new token #{state["remints"]} (device {old} -> {c.dev[:8]})')
            time.sleep(3 + random.random() * 3)
            st, fstep = mint(f'remint{state["remints"]}_')
            if st == 200:
                state['remint_ok'] = state.get('remint_ok', 0) + 1
                return 200, None
        return st, fstep

    try:
        # --- warm-up (lab U5: sentry + etracker init) ---
        sentry = sentry_preflight()
        c.send('etracker_init', ETRACKER_INIT, {'User-Agent': UA, 'Accept': '*/*', 'Connection': 'keep-alive'})
        time.sleep(1.8 + random.random() * 0.6)

        if STOP.is_set(): return finish('stopped')
        if past_deadline(): stop_reason = 'deadline@0'; return finish('deadline')
        # --- mint (retry with a new token on 403) ---
        st, fstep = mint('mint_')
        if st == 403:
            st, fstep = remint_after_403(fstep)
        if st != 200:
            return finish(kind_of(st), st, fstep, f'{fstep} {st} (sentry={sentry})')
        time.sleep(1.0 + random.random() * 1.0)

        # --- pagination (persona pace; production stop rules + production telemetry) ---
        first_page = True
        while pages_done < max_pages:
            gap = 0.0 if first_page else ctx['pace'][0] + random.random() * max(0.0, ctx['pace'][1] - ctx['pace'][0])
            first_page = False
            wait = max(gap, permit())  # persona pace, but never faster than this IP's page budget
            if stop_at is not None: wait = min(wait, max(0.0, stop_at - time.time()))
            # Token refresh REMOVED: real app never refreshes mid-session (Proxyman confirmed)
            time.sleep(max(0.0, wait))
            if STOP.is_set(): return finish('stopped')
            if past_deadline(): stop_reason = f'deadline@{pages_done}'; break
            body = {'queryEntity': None, 'q': None, 'f': {}, 'contexts': [], 'withContent': True, 'excludeOutletCodes': [],
                    'limit': 60, 'scopes': None, 'page': 1, 'dishTags': [], 'ratingMin': None,
                    'sort': {'by': 'popularity', 'dir': 'desc', 'slab': None}, 'widgets': None, 'content': None,
                    'withHits': False, 'filteredByWidget': None, 'internalFilteredByWidget': 'homepage_navpill_filters_widget',
                    'getFallback': True, 'type': 'outlet', 'qType': 'search_query', 'minCount': 0,
                    'groupDishes': False, 'isSsc': False, 'hideSearchBar': False, 'hideSearchOptions': False,
                    'isOutletAdProcessingEnabled': True, 'withDishFetchQuery': None}
            if token: body['searchToken'] = token
            sh = {'x-experience': 'food', 'x-locale': 'en-ae', 'x-device-lat': str(lat), 'x-device-lng': str(lng),
                  'x-mp-country': 'ae', 'x-lat': str(round(lat * 1e7)), 'x-lng': str(round(lng * 1e7)),
                  'x-border-enabled': 'true', 'x-rocket-enabled': 'true', 'Content-Type': 'application/json'}
            for k, v in zone.items():
                if v is not None and not isinstance(v, (dict, list)):
                    sh[k] = str(v).lower() if isinstance(v, bool) else str(v)
            sh['x-ab-test'] = AB_TEST
            st, txt = send429('search', SEARCH_URL, lambda: c.base(sh), method='POST', body=json.dumps(body))
            if st in (None, 502, 503, 504):  # one patient retry on a flaky response
                time.sleep(3 + random.random() * 2)
                st, txt = c.send('search_retry1', SEARCH_URL, c.base(sh), method='POST', body=json.dumps(body))
            if st == 403: search_403 += 1
            if st == 429: search_429 += 1
            if state.pop('await_429_result', False):
                state['after_remint429'].append(st)  # what the first search on the new device returned
                if st == 200: state['remint429_ok'] += 1
            if st == 403:  # new token (fresh phone session), then retry this same page from the saved search token
                rst, _ = remint_after_403(f'search p{pages_done + 1}')
                if rst == 200:
                    first_page = True
                    continue
            if st == 429 and on_429 == 'remint' and state['remints_429'] < int(claim.get('remints_429') or 3):
                # 429: switch to a brand-new device id / visitor / cookies and mint a new token, then retry this page
                state['remints_429'] += 1
                old = c.dev[:8]
                c.new_identity()
                say(f'429 at search p{pages_done + 1}: new device + new token #{state["remints_429"]} ({old} -> {c.dev[:8]})')
                time.sleep(2 + random.random() * 3)
                mst, mstep = mint(f'r429_{state["remints_429"]}_')
                if mst == 403:
                    mst, mstep = remint_after_403(mstep)
                if mst != 200:
                    return finish(kind_of(mst), mst, mstep, f'mint after 429 failed: {mstep} {mst}', pages_done + 1)
                state['await_429_result'] = True
                first_page = True
                continue
            if st == 404: stop_reason = f'404@{pages_done + 1}'; break
            if st != 200:
                return finish(kind_of(st), st, 'search', f'search {st} at page {pages_done + 1}', pages_done + 1)
            try: j = json.loads(txt)
            except Exception: stop_reason = f'bad-json@{pages_done + 1}'; break
            outlets = walk_outlets(j.get('results'))
            pages_done += 1
            fresh = 0
            for o in outlets:
                if o['outletCode'] not in seen: seen.add(o['outletCode']); captures.append(o); fresh += 1
            if not outlets: stop_reason = f'empty@{pages_done}'; break
            if fresh == 0: stop_reason = f'no-new@{pages_done}'; break
            if not j.get('searchToken'): stop_reason = f'no-token@{pages_done}'; break
            token = j['searchToken']
            # Telemetry after each page: 2-POST pattern matching real app (Proxyman-confirmed)
            # POST 1: page_catalog, POST 2: single batch of v2+v1 pairs (no promotion_impression)
            time.sleep(0.3 + random.random() * 0.2)
            c.et('et_page_catalog', [ev_page_catalog(pages_done, c.vis, c.dev)])
            visible = outlets[:imp_n]
            if visible:
                time.sleep(0.3 + random.random() * 0.3)
                c.et('et_impressions', impression_pairs(visible, c.vis, c.dev))
            if pages_done % 10 == 0: say(f'p{pages_done}: {len(seen)} cards so far')
        if not stop_reason: stop_reason = f'max-pages({max_pages})'
        return finish('ok')
    except Exception as e:
        say(f'area error: {str(e)[:200]}')
        return finish('error', None, 'other', str(e))

# -- Main -----------------------------------------------------------------------

def main():
    global PROXY, RUNNER_IP
    if not (SUPABASE_URL and SUPABASE_KEY and RUN_ID and MACHINE and FLEET in ('uae', 'decodo', 'dc')):
        say('missing env (SUPABASE_URL/KEY, RUN_ID, MACHINE_NO, FLEET)'); sys.exit(1)
    PROXY = pick_proxy()
    if not PROXY:
        say('no proxy configured for this fleet - refusing to run (fallback never goes direct)')
        if SHARED: sys.exit(1)  # coordinator owns machine_end
        try: rpc('noon_ranking_machine_end', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE, 'p_reason': 'no_proxy_configured'})
        except Exception: pass
        sys.exit(1)

    if SHARED:
        try: start = json.loads(START_JSON)
        except Exception: start = None
    else:
        start = rpc('noon_ranking_machine_start', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE, 'p_github_run_id': GH_RUN_ID})
    if not start or start.get('stop') or start.get('error'):
        say(f'told to stop at check-in: {json.dumps(start)[:200]}'); return
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    exit_reason = 'done'
    try:
        wait = max(0, min(int(start.get('launch_wait_s') or 0), 900))
        say(f'proxy {proxy_label(PROXY)} | launch wait {wait}s (randomised by Supabase)')
        if wait: time.sleep(wait)

        spec = rpc('noon_ranking_scrape_spec', {}) or {}
        build, ua = apply_app_identity(spec.get('app') if isinstance(spec, dict) else None)
        non_food = set(rpc('noon_get_non_food_cuisines', {}) or [])
        RUNNER_IP = egress_ip()
        if not RUNNER_IP:
            say('proxy unreachable - stopping (never scraping direct)'); exit_reason = 'proxy_unreachable'; return
        say(f'egress {RUNNER_IP} via {proxy_label(PROXY)} | app {build} | non-food {len(non_food)}')

        ctx = {'non_food': non_food, 'dry_run': bool(start.get('dry_run')), 'scrape_date': start.get('scrape_date'),
               'scrape_hour': start.get('scrape_hour'), 'area_seq': 0, 'last_gap_s': 0, 'persona': None, 'pace': (6.0, 15.0)}
        tel = (spec.get('telemetry') if isinstance(spec, dict) else None) or {}
        lo, hi = int(tel.get('impressions_per_page_min') or 4), int(tel.get('impressions_per_page_max') or 10)
        ctx['impressions_per_page'] = random.randint(min(lo, hi), max(lo, hi))  # per session, like production
        consecutive_blocks = 0
        while True:
            if time.time() - START > WALL_LIMIT_S: exit_reason = 'wall_limit'; break
            claim = rpc('noon_ranking_fb_claim', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE})
            if not claim or claim.get('stop') or claim.get('noon_area_id') is None:
                exit_reason = (claim or {}).get('reason') or 'no_claim'; say(f'stop: {exit_reason}'); break
            if ctx['persona'] is None:  # one persona per machine, weights/paces from Supabase
                weights = claim.get('persona_weights') or {'scroller': 25, 'browser': 55, 'reader': 20}
                paces = claim.get('persona_paces') or {'scroller': [2, 6], 'browser': [6, 15], 'reader': [15, 30]}
                names = [k for k in weights if k in paces and float(weights[k]) > 0] or ['browser']
                ctx['persona'] = random.choices(names, weights=[float(weights.get(n, 1)) for n in names])[0]
                lo, hi = paces.get(ctx['persona'], [6, 15])
                ctx['pace'] = (float(lo), float(hi))
                say(f"persona {ctx['persona']} (page gap {lo}-{hi}s)")
            ctx['area_seq'] += 1
            kind, st = scrape_area(claim, ctx)
            if kind in ('stopped', 'deadline'):
                exit_reason = kind; break
            if kind in ('blocked', 'rate_limited'):
                consecutive_blocks += 1
                if consecutive_blocks >= int(claim.get('max_consecutive_blocks') or 2):
                    exit_reason = kind; say(f'{consecutive_blocks} blocks in a row - ending machine'); break
            else:
                consecutive_blocks = 0
            gmin, gmax = float(claim.get('area_gap_min_s') or 15), float(claim.get('area_gap_max_s') or 75)
            gap = gmin + random.random() * max(0.0, gmax - gmin)
            if kind != 'ok': gap *= 2  # back off longer after a block, like a user giving up for a bit
            ctx['last_gap_s'] = round(gap, 1)
            STOP.wait(gap)
            if STOP.is_set(): exit_reason = 'stopped'; break
    finally:
        if SHARED:
            say(f'=== worker done: {exit_reason} ===')
            print(f'FB_EXIT_REASON={exit_reason}', flush=True)
            return
        try:
            rpc('noon_ranking_machine_end', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE, 'p_reason': f'fallback: {exit_reason}'})
        except Exception as e:
            say(f'machine_end failed: {str(e)[:120]}')
        say(f'=== machine done: {exit_reason} ===')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        say(f'FATAL: {e}'); sys.exit(1)
