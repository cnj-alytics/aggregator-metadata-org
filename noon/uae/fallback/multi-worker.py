#!/usr/bin/env python3
"""noon/uae/fallback/multi-worker.py — PRODUCTION multi-IP coordinator.

Runs multiple fallback-worker.py instances in parallel on one GitHub machine,
each with its own Decodo datacenter proxy port = its own IP.

Architecture: 42 GitHub machines × 6 workers each = 252 concurrent IPs
covering all 504 areas (~2 areas per IP) in ~20 minutes.

How it maps onto Supabase (fixed 2026-11-07 after run 774, where every DC machine
reported 'machine_not_found' and was marked lost):
  * MACHINE_NO is the run-wide noon_ranking_run_machine.machine_no (e.g. 208), NOT a
    1..42 fleet index. The DC slot (which decides the port range) is this machine's
    position among the run's fleet='dc' rows, wrapped at fb_dc_machines.
  * All workers share that ONE run_machine row. This coordinator calls
    noon_ranking_machine_start / noon_ranking_machine_end exactly once; the workers
    run with FB_SHARED_MACHINE=1 and skip both.
  * Page pacing is per worker (= per IP), at settings.fb_ip_pages_per_min.

Env (set by workflow):
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RUN_ID, GITHUB_RUN_ID,
  NOON_PROXY_URL_DC  — base proxy URL without port (http://user:pass@dc.decodo.com)
  MACHINE_NO         — run_machine.machine_no of this GitHub machine
  WORKER_COUNT       — number of parallel workers (default 6)
  WALL_LIMIT_SEC     — overall time limit (default 3600)
"""
import json, os, subprocess, sys, threading, time
import urllib.request

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
RUN_ID = int(os.environ.get('RUN_ID') or 0)
MACHINE_NO = int(os.environ.get('MACHINE_NO') or 0)
GH_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
WORKER_COUNT = int(os.environ.get('WORKER_COUNT') or 6)
WALL_LIMIT_S = int(os.environ.get('WALL_LIMIT_SEC') or 3600)
DC_PROXY_BASE = os.environ.get('NOON_PROXY_URL_DC', '').rstrip('/')  # no port
DC_PORT_BASE = 10001  # default; settings.fb_dc_port_base wins (read in dc_slot)

START = time.time()

def say(m):
    print(f'[multi-fb r{RUN_ID} m{MACHINE_NO}] {m}', flush=True)

def _req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f'{SUPABASE_URL}{path}', data=data, method=method, headers={
        'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'})
    last = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                txt = r.read().decode() or 'null'
                return json.loads(txt)
        except Exception as e:
            last = e
            say(f'{method} {path.split("?")[0]} attempt {attempt + 1} failed: {str(e)[:120]}')
            time.sleep(2 + attempt * 3)
    raise last

def rpc(name, params):
    return _req('POST', f'/rest/v1/rpc/{name}', params)

def dc_slot():
    """DC slot (1..fb_dc_machines) of this machine. Supabase stores it in run_machine.proxy_port (2026-11-08),
    so a replacement reuses the slot (= the same 8 IPs) of the machine it replaces. Older rows without it fall
    back to this machine's position among the run's DC machines."""
    rows = _req('GET', f'/rest/v1/noon_ranking_run_machine?run_id=eq.{RUN_ID}&fleet=eq.dc'
                       f'&select=machine_no,proxy_port&order=machine_no.asc') or []
    nos = [r['machine_no'] for r in rows]
    if MACHINE_NO not in nos:
        return None, len(nos)
    own = rows[nos.index(MACHINE_NO)].get('proxy_port')
    if own:
        return int(own), len(nos)
    st = _settings()
    fleet_size = int(st.get('fb_dc_machines') or len(nos) or 1)
    return (nos.index(MACHINE_NO) % fleet_size) + 1, len(nos)

def _settings():
    try:
        return (_req('GET', '/rest/v1/noon_ranking_settings?id=eq.1&select=*') or [{}])[0]
    except Exception:
        return {}

def ip_pages_per_min():
    try:
        st = (_req('GET', '/rest/v1/noon_ranking_settings?id=eq.1&select=fb_ip_pages_per_min') or [{}])[0]
        return float(st.get('fb_ip_pages_per_min') or 12)
    except Exception:
        return 12.0

def run_worker(worker_idx, port, start_json, ppm, reasons):
    env = {
        **os.environ,
        'RUN_ID': str(RUN_ID),
        'MACHINE_NO': str(MACHINE_NO),          # shared run_machine row
        'FLEET': 'dc',
        'NOON_PROXY_URL': f'{DC_PROXY_BASE}:{port}',
        'PROXY_PORT': str(port),
        'GITHUB_RUN_ID': GH_RUN_ID,
        'FB_SHARED_MACHINE': '1',
        'FB_START_JSON': start_json,
        'FB_IP_PAGES_PER_MIN': str(ppm),
        'WALL_LIMIT_SEC': str(max(60, WALL_LIMIT_S - int(time.time() - START) - 60)),
    }
    tag = f'[w{worker_idx} :{port}]'
    say(f'{tag} starting')
    try:
        worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fallback-worker.py')
        proc = subprocess.Popen(['python', '-u', worker_path], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            line = line.rstrip('\n')
            if line.startswith('FB_EXIT_REASON='):
                reasons[worker_idx] = line.split('=', 1)[1]
            elif line:
                print(f'{tag} {line}', flush=True)
        proc.wait()
        say(f'{tag} exited with code {proc.returncode}')
        return proc.returncode
    except Exception as e:
        say(f'{tag} FAILED: {e}')
        return 1


def main():
    if not (SUPABASE_URL and SUPABASE_KEY and RUN_ID and MACHINE_NO and DC_PROXY_BASE):
        say('missing env'); sys.exit(1)

    slot, n_dc = dc_slot()
    if slot is None:
        say(f'machine {MACHINE_NO} is not a DC machine of run {RUN_ID} ({n_dc} DC rows) - exiting'); sys.exit(1)
    port_base = int(_settings().get('fb_dc_port_base') or DC_PORT_BASE)
    port_start = port_base + (slot - 1) * WORKER_COUNT
    ports = list(range(port_start, port_start + WORKER_COUNT))
    say(f'DC slot {slot}/{n_dc} -> {WORKER_COUNT} workers on ports {ports[0]}-{ports[-1]}')
    say(f'proxy host: {DC_PROXY_BASE.split("@")[-1]}')

    start = rpc('noon_ranking_machine_start', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE_NO, 'p_github_run_id': GH_RUN_ID})
    if not start or start.get('stop') or start.get('error'):
        say(f'told to stop at check-in: {json.dumps(start)[:200]}'); return
    wait = max(0, min(int(start.get('launch_wait_s') or 0), 900))
    if wait:
        say(f'launch wait {wait}s'); time.sleep(wait)
    start['launch_wait_s'] = 0  # already waited here; workers only stagger
    start_json = json.dumps(start)
    ppm = ip_pages_per_min()
    say(f'checked in | per-IP budget {ppm} pages/min')

    results = [None] * WORKER_COUNT
    reasons = [None] * WORKER_COUNT

    def worker_thread(idx):
        time.sleep(idx * 3)  # stagger so workers don't all mint at once
        results[idx] = run_worker(idx, ports[idx], start_json, ppm, reasons)

    threads = [threading.Thread(target=worker_thread, args=(i,), daemon=True) for i in range(WORKER_COUNT)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=max(1, WALL_LIMIT_S - (time.time() - START)))

    ok = sum(1 for r in results if r == 0)
    summary = ','.join(f'{ports[i]}:{reasons[i] or ("exit " + str(results[i]))}' for i in range(WORKER_COUNT))
    say(f'=== DONE: {ok}/{WORKER_COUNT} workers exited cleanly | {summary} ===')
    try:
        rpc('noon_ranking_machine_end', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE_NO,
                                         'p_reason': f'fallback dc x{WORKER_COUNT}: {summary}'[:500]})
    except Exception as e:
        say(f'machine_end failed: {str(e)[:120]}')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        say(f'FATAL: {e}')
        try:
            rpc('noon_ranking_machine_end', {'p_run_id': RUN_ID, 'p_machine_no': MACHINE_NO, 'p_reason': f'fallback dc fatal: {str(e)[:200]}'})
        except Exception:
            pass
        sys.exit(1)
