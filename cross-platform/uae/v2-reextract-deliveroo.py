#!/usr/bin/env python3
"""cross-platform/uae/v2-reextract-deliveroo.py

Re-reads the archived Deliveroo restaurant pages (Iceberg v2_raw.detail_pages) and rewrites the
dish list in v2.detail_extract with dishes only.

Why: a Deliveroo menu page lists every add-on option (e.g. "Oat Milk", "No Bell Pepper") in the same
items array as the dishes. A dish is an item whose categoryId is one of the menu's categories - the
same rule the live menu tool (dash_menu_live) uses. The first extraction kept all items, so Deliveroo
menus looked 3-4x larger than the same restaurant on the other apps.

Matching is by raw file path, so each extract row is rewritten from exactly the page it came from.
The previous lists are kept in v2.detail_extract_deliveroo_before_dishfix (created once, never overwritten).

Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_DB_URL,
     SUPABASE_S3_ACCESS_KEY_ID, SUPABASE_S3_SECRET_ACCESS_KEY, SUPABASE_S3_REGION, DRY_RUN
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"


def log(m):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {m}", flush=True)


def menu_names(names):
    # identical to menuNames() in v2-detail-scrape.js
    out = []
    seen = set()
    for n in names:
        if not isinstance(n, str):
            continue
        t = re.sub(r"[^a-z0-9]+", " ", n.lower()).strip()
        if len(t) >= 3 and t not in seen:
            seen.add(t)
            out.append(t)
        if len(out) >= 800:
            break
    return out


def dishes(body_text):
    try:
        b = json.loads(body_text)
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(b, dict):
        return None
    root = b
    for k in ("menuPage", "menu", "metas", "root"):
        root = root.get(k) if isinstance(root, dict) else None
    if not isinstance(root, dict):
        return None
    cats = {c.get("id") for c in (root.get("categories") or []) if isinstance(c, dict)}
    seen, names = set(), []
    for it in root.get("items") or []:
        if not isinstance(it, dict) or it.get("categoryId") not in cats or it.get("id") in seen:
            continue
        seen.add(it.get("id"))
        names.append(it.get("name"))
    return menu_names(names)


def load_table():
    from pyiceberg.catalog import load_catalog
    ref = urlparse(SUPABASE_URL).hostname.split(".")[0]
    catalog = load_catalog(
        "supabase-analytics", type="rest", warehouse="v2-archive",
        uri=f"https://{ref}.supabase.co/storage/v1/iceberg", token=KEY,
        **{
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": f"https://{ref}.supabase.co/storage/v1/s3",
            "s3.access-key-id": os.environ["SUPABASE_S3_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["SUPABASE_S3_SECRET_ACCESS_KEY"],
            "s3.region": os.environ.get("SUPABASE_S3_REGION", "ap-southeast-2"),
            "s3.force-virtual-addressing": False,
        },
    )
    return catalog.load_table(("v2_raw", "detail_pages"))


def main():
    from pyiceberg.expressions import EqualTo

    conn = psycopg.connect(os.environ["SUPABASE_DB_URL"], autocommit=True)
    cur = conn.cursor()
    cur.execute("select raw_path from v2.detail_extract where platform = 'deliveroo' and raw_path is not null")
    wanted = {p for (p,) in cur.fetchall()}
    log(f"deliveroo extract rows with a raw page: {len(wanted)}")

    import pyarrow.parquet as pq

    table = load_table()
    # Stream each data file in small batches: a file holds up to 1,500 full pages, far too big to load at once.
    tasks = list(table.scan(row_filter=EqualTo("platform", "deliveroo")).plan_files())
    log(f"archive files with Deliveroo pages: {len(tasks)}")
    found, empty, rows = 0, 0, []
    for n, task in enumerate(tasks, 1):
        with table.io.new_input(task.file.file_path).open() as f:
            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(batch_size=20, columns=["platform", "path", "listing_id", "body"]):
                plats = batch.column("platform").to_pylist()
                paths = batch.column("path").to_pylist()
                lids = batch.column("listing_id").to_pylist()
                bodies = batch.column("body")
                for i, p in enumerate(paths):
                    if plats[i] != "deliveroo" or p not in wanted:
                        continue
                    d = dishes(bodies[i].as_py())
                    found += 1
                    if d is None:
                        empty += 1
                        continue
                    rows.append((p, lids[i], d))
                del batch, bodies
        log(f"file {n}/{len(tasks)}: {found} pages matched, {len(rows)} with a menu, {empty} without")
    log(f"done reading: {len(rows)} menus to rewrite, {len(wanted) - found} extract rows whose page was not found")
    if DRY_RUN or not rows:
        sample = rows[:3]
        for p, l, d in sample:
            log(f"sample {l}: {len(d)} dishes, e.g. {d[:5]}")
        return

    cur.execute("""create table if not exists v2.detail_extract_deliveroo_before_dishfix as
                   select platform, listing_id, raw_path, menu_items, menu_count, now() saved_at from v2.detail_extract where platform = 'deliveroo'""")
    cur.execute("comment on table v2.detail_extract_deliveroo_before_dishfix is "
                "'Deliveroo dish lists as first extracted (they included add-on options), kept before the dishes-only re-extraction.'")
    cur.execute("create temp table _fix (raw_path text primary key, listing_id text, menu_items text[])")
    with cur.copy("copy _fix (raw_path, listing_id, menu_items) from stdin") as cp:
        for p, l, d in rows:
            cp.write_row((p, l, d))
    cur.execute("""update v2.detail_extract e set menu_items = f.menu_items, menu_count = cardinality(f.menu_items)
                   from _fix f where e.platform = 'deliveroo' and e.raw_path = f.raw_path
                     and e.menu_items is distinct from f.menu_items""")
    log(f"rewritten: {cur.rowcount} extract rows")


if __name__ == "__main__":
    main()
