#!/usr/bin/env python3
"""cross-platform/uae/v2-archive.py

v2 phase 5: move raw pages from Supabase Storage (v2-staging) into the v2 analytics
bucket (Iceberg, bucket v2-archive, namespace v2_raw, tables listing_pages / detail_pages).

Per batch:
  1. claim rows in v2.raw_file (archived_at is null) by stamping archive_run = batch id
  2. download each gzipped page from staging, decompress
  3. overwrite the Iceberg rows for that batch id (re-running a batch is safe)
  4. read the batch back from Iceberg; refuse to continue if the count differs
  5. stamp archived_at, then remove the staged objects

Only data that has been verified in Iceberg is removed from staging. Postgres keeps
the small v2.raw_file index (path, sizes, status) so every page stays findable.

Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_DB_URL,
     SUPABASE_S3_ACCESS_KEY_ID, SUPABASE_S3_SECRET_ACCESS_KEY, SUPABASE_S3_REGION,
     PHASE (detail|listing), MAX_BATCHES, DRY_RUN
"""
import gzip
import json
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import psycopg
import pyarrow as pa

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
PHASE = os.environ.get("PHASE", "detail")
MAX_BATCHES = int(os.environ.get("MAX_BATCHES", "1000"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
BATCH_ROWS = 1500
BATCH_BYTES = 400_000_000  # uncompressed bytes per batch (memory guard)
NAMESPACE = "v2_raw"
TABLE = f"{PHASE}_pages"
if PHASE not in ("detail", "listing"):
    sys.exit("PHASE must be detail or listing")

ARROW_SCHEMA = pa.schema([
    pa.field("archive_batch", pa.string(), nullable=False),
    pa.field("raw_id", pa.int64(), nullable=False),
    pa.field("phase", pa.string()),
    pa.field("run_id", pa.int64()),
    pa.field("job_no", pa.int32()),
    pa.field("platform", pa.string()),
    pa.field("listing_id", pa.string()),
    pa.field("seq", pa.int32()),
    pa.field("kind", pa.string()),
    pa.field("http_status", pa.int32()),
    pa.field("bytes", pa.int64()),
    pa.field("path", pa.string()),
    pa.field("request", pa.string()),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("body", pa.large_string()),
])


def log(m):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {m}", flush=True)


def storage(method, path, body=None):
    url = f"{SUPABASE_URL}/storage/v1/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = Request(url, data=data, method=method, headers={
        "apikey": KEY, "Authorization": f"Bearer {KEY}",
        **({"Content-Type": "application/json"} if data else {})})
    for attempt in range(1, 5):
        try:
            with urlopen(req, timeout=120) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            if attempt == 4:
                raise
            log(f"storage {method} {path[:80]} attempt {attempt}: {e}")
            time.sleep(3 * attempt)


def load_table(bucket):
    from pyiceberg.catalog import load_catalog
    from pyiceberg.transforms import IdentityTransform

    ref = urlparse(SUPABASE_URL).hostname.split(".")[0]
    catalog = load_catalog(
        "supabase-analytics", type="rest", warehouse=bucket,
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
    catalog.create_namespace_if_not_exists(NAMESPACE)
    try:
        return catalog.load_table((NAMESPACE, TABLE))
    except Exception:  # noqa: BLE001
        pass
    table = catalog.create_table((NAMESPACE, TABLE), schema=ARROW_SCHEMA, properties={
        "write.parquet.compression-codec": "zstd", "write.parquet.row-group-limit": "20000"})
    with table.update_spec() as spec:
        spec.add_field("platform", IdentityTransform(), "platform")
    return catalog.load_table((NAMESPACE, TABLE))


def main():
    from pyiceberg.expressions import EqualTo

    conn = psycopg.connect(os.environ["SUPABASE_DB_URL"], autocommit=True)
    cur = conn.cursor()
    cur.execute("select value #>> '{}' from v2.settings where key = 'staging_bucket'")
    staging = cur.fetchone()[0]
    cur.execute("select value #>> '{}' from v2.settings where key = 'archive_bucket'")
    archive = cur.fetchone()[0]
    cur.execute("select count(*), coalesce(sum(bytes),0) from v2.raw_file where phase = %s and archived_at is null", (PHASE,))
    n_left, b_left = cur.fetchone()
    log(f"phase={PHASE} staging={staging} archive={archive} waiting={n_left} files / {b_left/1e6:.0f} MB")
    if DRY_RUN or n_left == 0:
        return
    table = load_table(archive)

    total = 0
    for b in range(MAX_BATCHES):
        # resume a batch that was claimed but never finished, else claim a new one
        cur.execute("""select archive_run from v2.raw_file where phase = %s and archived_at is null and archive_run is not null
                       limit 1""", (PHASE,))
        row = cur.fetchone()
        if row:
            batch = row[0]
            log(f"resuming batch {batch}")
        else:
            batch = f"{PHASE}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
            cur.execute("""with c as (select id, sum(bytes) over (order by id) cb from v2.raw_file
                                       where phase = %s and archived_at is null and archive_run is null order by id limit %s)
                           update v2.raw_file f set archive_run = %s from c where f.id = c.id and c.cb <= %s""",
                        (PHASE, BATCH_ROWS, batch, BATCH_BYTES))
            if cur.rowcount == 0:  # a single file bigger than the byte cap: take it alone
                cur.execute("""update v2.raw_file set archive_run = %s where id = (select min(id) from v2.raw_file
                               where phase = %s and archived_at is null and archive_run is null)""", (batch, PHASE))
            if cur.rowcount == 0:
                break
        cur.execute("""select id, phase, run_id, job_no, platform, listing_id, seq, kind, http_status, bytes, path,
                              request::text, fetched_at from v2.raw_file where archive_run = %s order by id""", (batch,))
        rows = cur.fetchall()

        def fetch(r):
            raw = storage("GET", f"object/authenticated/{staging}/{r[10]}")
            try:
                return gzip.decompress(raw).decode("utf-8", "replace")
            except OSError:
                return raw.decode("utf-8", "replace")

        with ThreadPoolExecutor(24) as ex:
            bodies = list(ex.map(fetch, rows))

        cols = list(zip(*rows))
        data = pa.table({
            "archive_batch": pa.array([batch] * len(rows), pa.string()),
            "raw_id": pa.array(cols[0], pa.int64()),
            "phase": pa.array(cols[1], pa.string()),
            "run_id": pa.array(cols[2], pa.int64()),
            "job_no": pa.array(cols[3], pa.int32()),
            "platform": pa.array(cols[4], pa.string()),
            "listing_id": pa.array(cols[5], pa.string()),
            "seq": pa.array(cols[6], pa.int32()),
            "kind": pa.array(cols[7], pa.string()),
            "http_status": pa.array(cols[8], pa.int32()),
            "bytes": pa.array(cols[9], pa.int64()),
            "path": pa.array(cols[10], pa.string()),
            "request": pa.array(cols[11], pa.string()),
            "fetched_at": pa.array(cols[12], pa.timestamp("us", tz="UTC")),
            "body": pa.array(bodies, pa.large_string()),
        }, schema=ARROW_SCHEMA)

        flt = EqualTo("archive_batch", batch)
        for attempt in range(1, 4):
            try:
                table.overwrite(data, overwrite_filter=flt)
                break
            except Exception as e:  # noqa: BLE001  (retrying the same overwrite is safe)
                if attempt == 3:
                    raise
                log(f"overwrite attempt {attempt} failed: {e}")
                time.sleep(10 * attempt)
                table.refresh()
        table.refresh()
        got = table.scan(row_filter=flt, selected_fields=("raw_id",)).to_arrow().num_rows
        if got != len(rows):
            sys.exit(f"batch {batch}: wrote {len(rows)} but Iceberg returned {got} - stopping, staging kept")

        ids = list(cols[0])
        cur.execute("update v2.raw_file set archived_at = now() where archive_run = %s and id = any(%s)", (batch, ids))
        paths = list(cols[10])
        for i in range(0, len(paths), 500):
            storage("DELETE", f"object/{staging}", {"prefixes": paths[i:i + 500]})
        total += len(rows)
        log(f"batch {batch}: {len(rows)} pages, {sum(cols[9])/1e6:.0f} MB -> Iceberg verified, staging cleared (total {total})")

    log(f"done: {total} pages archived")


if __name__ == "__main__":
    main()
