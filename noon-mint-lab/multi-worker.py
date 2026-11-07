#!/usr/bin/env python3
"""noon-mint-lab/multi-worker.py — Run multiple fallback-lab workers in parallel on one GitHub machine.

Each worker gets its own Decodo datacenter proxy port = its own IP.
One GitHub machine manages N parallel workers, each scraping independently.

Env (set by workflow):
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TEST_ID, MACHINE_NO, GITHUB_RUN_ID,
  NOON_PROXY_URL_DC  — base proxy URL without port (http://user:pass@dc.decodo.com)
  WORKER_COUNT       — number of parallel workers (default 6)
  PORT_OFFSET        — starting port = 10001 + (MACHINE_NO - 1) * WORKER_COUNT
  WALL_LIMIT_SEC     — overall time limit (default 3600)
"""
import json, os, subprocess, sys, threading, time
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
TEST_ID = int(os.environ.get('TEST_ID') or 0)
MACHINE_NO = int(os.environ.get('MACHINE_NO') or 0)
GH_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
WORKER_COUNT = int(os.environ.get('WORKER_COUNT') or 6)
WALL_LIMIT_S = int(os.environ.get('WALL_LIMIT_SEC') or 3600)
DC_PROXY_BASE = os.environ.get('NOON_PROXY_URL_DC', '')  # http://user:pass@dc.decodo.com (no port)

START = time.time()

def say(m):
    print(f'[multi t{TEST_ID} m{MACHINE_NO}] {m}', flush=True)

def run_worker(worker_idx, port, sub_machine_no):
    """Run one fallback-lab.py worker as a subprocess with its own proxy port."""
    proxy_url = f'{DC_PROXY_BASE}:{port}'
    env = {
        **os.environ,
        'TEST_ID': str(TEST_ID),
        'MACHINE_NO': str(sub_machine_no),
        'FLEET': 'dc_free',
        'NOON_PROXY_URL': proxy_url,
        'PROXY_PORT': str(port),
        'GITHUB_RUN_ID': GH_RUN_ID,
        'WALL_LIMIT_SEC': str(max(60, WALL_LIMIT_S - int(time.time() - START) - 60)),
        'MAX_AREAS': '2',  # 2 areas per IP
    }

    tag = f'[w{worker_idx} port:{port} sub_m{sub_machine_no}]'
    say(f'{tag} starting with proxy :{port}')

    try:
        proc = subprocess.Popen(
            ['python', '-u', 'fallback-lab.py'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        # Stream output with worker prefix
        for line in proc.stdout:
            line = line.rstrip('\n')
            if line:
                print(f'{tag} {line}', flush=True)

        proc.wait()
        say(f'{tag} exited with code {proc.returncode}')
        return proc.returncode
    except Exception as e:
        say(f'{tag} FAILED: {e}')
        return 1


def main():
    if not (SUPABASE_URL and SUPABASE_KEY and TEST_ID and MACHINE_NO and DC_PROXY_BASE):
        say('missing env'); sys.exit(1)

    # Calculate port range for this machine
    # Machine 1 gets ports 10001-10006, Machine 2 gets 10007-10012, etc.
    port_start = 10001 + (MACHINE_NO - 1) * WORKER_COUNT
    ports = list(range(port_start, port_start + WORKER_COUNT))

    # Sub-machine numbers: machine 1 workers get sub_m 1-6, machine 2 gets 7-12, etc.
    sub_machine_start = (MACHINE_NO - 1) * WORKER_COUNT + 1

    say(f'starting {WORKER_COUNT} workers on ports {ports[0]}-{ports[-1]}')
    say(f'proxy base: {DC_PROXY_BASE.split("@")[1] if "@" in DC_PROXY_BASE else DC_PROXY_BASE}')
    say(f'sub-machines: {sub_machine_start}-{sub_machine_start + WORKER_COUNT - 1}')

    # Launch all workers in parallel
    threads = []
    results = [None] * WORKER_COUNT

    def worker_thread(idx, port, sub_m):
        # Stagger starts by 2-5 seconds to avoid all minting at once
        time.sleep(idx * 3)
        results[idx] = run_worker(idx, port, sub_m)

    for i in range(WORKER_COUNT):
        t = threading.Thread(
            target=worker_thread,
            args=(i, ports[i], sub_machine_start + i),
            daemon=True,
        )
        threads.append(t)
        t.start()

    # Wait for all to finish
    for t in threads:
        t.join(timeout=WALL_LIMIT_S)

    # Summary
    ok = sum(1 for r in results if r == 0)
    say(f'=== DONE: {ok}/{WORKER_COUNT} workers succeeded ===')
    for i, r in enumerate(results):
        say(f'  worker {i} (port {ports[i]}): {"ok" if r == 0 else f"exit {r}"}')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        say(f'FATAL: {e}')
        sys.exit(1)
