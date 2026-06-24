#!/usr/bin/env python3
"""
Stage-once + replay-from-Delta ingester for Databricks Liquid-Clustered tables.

Two-phase pattern:
  1. ONE-TIME staging:
     a. CREATE the staging table with the same schema as the target
        (no CLUSTER BY, no PARTITION BY — just a plain Delta table).
     b. INSERT from S3 Parquet into staging, with explicit CASTs to match
        the target's narrow types (SMALLINT etc.), randomized via
        DISTRIBUTE BY rand() so the stage's physical files are scrambled.
     Single S3 read + single global shuffle, paid once.
  2. LOOP: INSERT from the staging table into the LC target, sub-chunked
     by hash(WatchID) modulo K. No CASTs, no shuffle in the loop — the
     stage has the right schema and is already random.

Setup:
  pip install databricks-sql-connector
  export DATABRICKS_SERVER_HOSTNAME=...
  export DATABRICKS_HTTP_PATH=...
  export DATABRICKS_TOKEN=...

Example — stage only:
  python3 databricks_stage_and_ingest.py \\
      --target 'workspace.clickbench.`100b_clustered`' \\
      --stage-only

Example — smoke test:
  python3 databricks_stage_and_ingest.py \\
      --target 'workspace.clickbench.`100b_clustered`' \\
      --skip-stage --total-rows 10000000 --commit-rows 10000000 --parallel 1

Example — full 100B:
  python3 databricks_stage_and_ingest.py \\
      --target 'workspace.clickbench.`100b_clustered`' \\
      --skip-stage --parallel 16
"""

import argparse
import multiprocessing as mp
import os
import signal
import sys
import time
from datetime import datetime, timezone
from databricks import sql


# Graceful Ctrl+C: workers check this flag between INSERTs and exit cleanly.
_INTERRUPTED = False

def _on_sigint(signum, frame):
    global _INTERRUPTED
    _INTERRUPTED = True
    print(
        "\n[!] Interrupt received. Workers will finish current INSERT then "
        "stop.\n",
        file=sys.stderr,
    )

signal.signal(signal.SIGINT, _on_sigint)


DEFAULT_STAGE = "workspace.clickbench.hits_stage_100m"
# NOTE: do NOT use s3://hits-parquet-100m-sorted-zstd/*.parquet — that
# bucket has zeroed-out numeric columns (WatchID, UserID, etc. all = 0).
# The real ClickBench public dataset lives in the clickhouse-public-datasets
# bucket (datasets.clickhouse.com is just a CloudFront alias for it).
DEFAULT_SOURCE = (
    "s3://clickhouse-public-datasets/hits_compatible/athena_partitioned/"
    "hits_*.parquet"
)
SOURCE_ROW_COUNT = 100_000_000


# ---- explicit stage DDL (matches the target's schema, no CLUSTER BY) -------

STAGE_DDL_TEMPLATE = """
CREATE TABLE {stage}
(
    WatchID BIGINT NOT NULL,
    JavaEnable SMALLINT NOT NULL,
    Title STRING NOT NULL,
    GoodEvent SMALLINT NOT NULL,
    EventTime TIMESTAMP NOT NULL,
    EventDate DATE NOT NULL,
    CounterID INT NOT NULL,
    ClientIP INT NOT NULL,
    RegionID INT NOT NULL,
    UserID BIGINT NOT NULL,
    CounterClass SMALLINT NOT NULL,
    OS SMALLINT NOT NULL,
    UserAgent SMALLINT NOT NULL,
    URL STRING NOT NULL,
    Referer STRING NOT NULL,
    IsRefresh SMALLINT NOT NULL,
    RefererCategoryID SMALLINT NOT NULL,
    RefererRegionID INT NOT NULL,
    URLCategoryID SMALLINT NOT NULL,
    URLRegionID INT NOT NULL,
    ResolutionWidth SMALLINT NOT NULL,
    ResolutionHeight SMALLINT NOT NULL,
    ResolutionDepth SMALLINT NOT NULL,
    FlashMajor SMALLINT NOT NULL,
    FlashMinor SMALLINT NOT NULL,
    FlashMinor2 STRING NOT NULL,
    NetMajor SMALLINT NOT NULL,
    NetMinor SMALLINT NOT NULL,
    UserAgentMajor SMALLINT NOT NULL,
    UserAgentMinor STRING NOT NULL,
    CookieEnable SMALLINT NOT NULL,
    JavascriptEnable SMALLINT NOT NULL,
    IsMobile SMALLINT NOT NULL,
    MobilePhone SMALLINT NOT NULL,
    MobilePhoneModel STRING NOT NULL,
    Params STRING NOT NULL,
    IPNetworkID INT NOT NULL,
    TraficSourceID SMALLINT NOT NULL,
    SearchEngineID SMALLINT NOT NULL,
    SearchPhrase STRING NOT NULL,
    AdvEngineID SMALLINT NOT NULL,
    IsArtifical SMALLINT NOT NULL,
    WindowClientWidth SMALLINT NOT NULL,
    WindowClientHeight SMALLINT NOT NULL,
    ClientTimeZone SMALLINT NOT NULL,
    ClientEventTime TIMESTAMP NOT NULL,
    SilverlightVersion1 SMALLINT NOT NULL,
    SilverlightVersion2 SMALLINT NOT NULL,
    SilverlightVersion3 INT NOT NULL,
    SilverlightVersion4 SMALLINT NOT NULL,
    PageCharset STRING NOT NULL,
    CodeVersion INT NOT NULL,
    IsLink SMALLINT NOT NULL,
    IsDownload SMALLINT NOT NULL,
    IsNotBounce SMALLINT NOT NULL,
    FUniqID BIGINT NOT NULL,
    OriginalURL STRING NOT NULL,
    HID INT NOT NULL,
    IsOldCounter SMALLINT NOT NULL,
    IsEvent SMALLINT NOT NULL,
    IsParameter SMALLINT NOT NULL,
    DontCountHits SMALLINT NOT NULL,
    WithHash SMALLINT NOT NULL,
    HitColor STRING NOT NULL,
    LocalEventTime TIMESTAMP NOT NULL,
    Age SMALLINT NOT NULL,
    Sex SMALLINT NOT NULL,
    Income SMALLINT NOT NULL,
    Interests SMALLINT NOT NULL,
    Robotness SMALLINT NOT NULL,
    RemoteIP INT NOT NULL,
    WindowName INT NOT NULL,
    OpenerName INT NOT NULL,
    HistoryLength SMALLINT NOT NULL,
    BrowserLanguage STRING NOT NULL,
    BrowserCountry STRING NOT NULL,
    SocialNetwork STRING NOT NULL,
    SocialAction STRING NOT NULL,
    HTTPError SMALLINT NOT NULL,
    SendTiming INT NOT NULL,
    DNSTiming INT NOT NULL,
    ConnectTiming INT NOT NULL,
    ResponseStartTiming INT NOT NULL,
    ResponseEndTiming INT NOT NULL,
    FetchTiming INT NOT NULL,
    SocialSourceNetworkID SMALLINT NOT NULL,
    SocialSourcePage STRING NOT NULL,
    ParamPrice BIGINT NOT NULL,
    ParamOrderID STRING NOT NULL,
    ParamCurrency STRING NOT NULL,
    ParamCurrencyID SMALLINT NOT NULL,
    OpenstatServiceName STRING NOT NULL,
    OpenstatCampaignID STRING NOT NULL,
    OpenstatAdID STRING NOT NULL,
    OpenstatSourceID STRING NOT NULL,
    UTMSource STRING NOT NULL,
    UTMMedium STRING NOT NULL,
    UTMCampaign STRING NOT NULL,
    UTMContent STRING NOT NULL,
    UTMTerm STRING NOT NULL,
    FromTag STRING NOT NULL,
    HasGCLID SMALLINT NOT NULL,
    RefererHash BIGINT NOT NULL,
    URLHash BIGINT NOT NULL,
    CLID INT NOT NULL
)
USING delta
"""

STAGE_INSERT_TEMPLATE = """
INSERT INTO {stage}
SELECT
    CAST(WatchID AS BIGINT)              AS WatchID,
    CAST(JavaEnable AS SMALLINT)         AS JavaEnable,
    Title,
    CAST(GoodEvent AS SMALLINT)          AS GoodEvent,
    -- EventTime is stored as BIGINT Unix seconds in the public bucket.
    TIMESTAMP_SECONDS(EventTime)         AS EventTime,
    -- EventDate is stored as INT days-since-epoch in the public bucket.
    DATE_FROM_UNIX_DATE(EventDate)       AS EventDate,
    CAST(CounterID AS INT)               AS CounterID,
    CAST(ClientIP AS INT)                AS ClientIP,
    CAST(RegionID AS INT)                AS RegionID,
    CAST(UserID AS BIGINT)               AS UserID,
    CAST(CounterClass AS SMALLINT)       AS CounterClass,
    CAST(OS AS SMALLINT)                 AS OS,
    CAST(UserAgent AS SMALLINT)          AS UserAgent,
    URL, Referer,
    CAST(IsRefresh AS SMALLINT)          AS IsRefresh,
    CAST(RefererCategoryID AS SMALLINT)  AS RefererCategoryID,
    CAST(RefererRegionID AS INT)         AS RefererRegionID,
    CAST(URLCategoryID AS SMALLINT)      AS URLCategoryID,
    CAST(URLRegionID AS INT)             AS URLRegionID,
    CAST(ResolutionWidth AS SMALLINT)    AS ResolutionWidth,
    CAST(ResolutionHeight AS SMALLINT)   AS ResolutionHeight,
    CAST(ResolutionDepth AS SMALLINT)    AS ResolutionDepth,
    CAST(FlashMajor AS SMALLINT)         AS FlashMajor,
    CAST(FlashMinor AS SMALLINT)         AS FlashMinor,
    FlashMinor2,
    CAST(NetMajor AS SMALLINT)           AS NetMajor,
    CAST(NetMinor AS SMALLINT)           AS NetMinor,
    CAST(UserAgentMajor AS SMALLINT)     AS UserAgentMajor,
    UserAgentMinor,
    CAST(CookieEnable AS SMALLINT)       AS CookieEnable,
    CAST(JavascriptEnable AS SMALLINT)   AS JavascriptEnable,
    CAST(IsMobile AS SMALLINT)           AS IsMobile,
    CAST(MobilePhone AS SMALLINT)        AS MobilePhone,
    MobilePhoneModel, Params,
    CAST(IPNetworkID AS INT)             AS IPNetworkID,
    CAST(TraficSourceID AS SMALLINT)     AS TraficSourceID,
    CAST(SearchEngineID AS SMALLINT)     AS SearchEngineID,
    SearchPhrase,
    CAST(AdvEngineID AS SMALLINT)        AS AdvEngineID,
    CAST(IsArtifical AS SMALLINT)        AS IsArtifical,
    CAST(WindowClientWidth AS SMALLINT)  AS WindowClientWidth,
    CAST(WindowClientHeight AS SMALLINT) AS WindowClientHeight,
    CAST(ClientTimeZone AS SMALLINT)     AS ClientTimeZone,
    TIMESTAMP_SECONDS(ClientEventTime)   AS ClientEventTime,
    CAST(SilverlightVersion1 AS SMALLINT) AS SilverlightVersion1,
    CAST(SilverlightVersion2 AS SMALLINT) AS SilverlightVersion2,
    CAST(SilverlightVersion3 AS INT)     AS SilverlightVersion3,
    CAST(SilverlightVersion4 AS SMALLINT) AS SilverlightVersion4,
    PageCharset,
    CAST(CodeVersion AS INT)             AS CodeVersion,
    CAST(IsLink AS SMALLINT)             AS IsLink,
    CAST(IsDownload AS SMALLINT)         AS IsDownload,
    CAST(IsNotBounce AS SMALLINT)        AS IsNotBounce,
    CAST(FUniqID AS BIGINT)              AS FUniqID,
    OriginalURL,
    CAST(HID AS INT)                     AS HID,
    CAST(IsOldCounter AS SMALLINT)       AS IsOldCounter,
    CAST(IsEvent AS SMALLINT)            AS IsEvent,
    CAST(IsParameter AS SMALLINT)        AS IsParameter,
    CAST(DontCountHits AS SMALLINT)      AS DontCountHits,
    CAST(WithHash AS SMALLINT)           AS WithHash,
    HitColor,
    TIMESTAMP_SECONDS(LocalEventTime)    AS LocalEventTime,
    CAST(Age AS SMALLINT)                AS Age,
    CAST(Sex AS SMALLINT)                AS Sex,
    CAST(Income AS SMALLINT)             AS Income,
    CAST(Interests AS SMALLINT)          AS Interests,
    CAST(Robotness AS SMALLINT)          AS Robotness,
    CAST(RemoteIP AS INT)                AS RemoteIP,
    CAST(WindowName AS INT)              AS WindowName,
    CAST(OpenerName AS INT)              AS OpenerName,
    CAST(HistoryLength AS SMALLINT)      AS HistoryLength,
    BrowserLanguage, BrowserCountry, SocialNetwork, SocialAction,
    CAST(HTTPError AS SMALLINT)          AS HTTPError,
    CAST(SendTiming AS INT)              AS SendTiming,
    CAST(DNSTiming AS INT)               AS DNSTiming,
    CAST(ConnectTiming AS INT)           AS ConnectTiming,
    CAST(ResponseStartTiming AS INT)     AS ResponseStartTiming,
    CAST(ResponseEndTiming AS INT)       AS ResponseEndTiming,
    CAST(FetchTiming AS INT)             AS FetchTiming,
    CAST(SocialSourceNetworkID AS SMALLINT) AS SocialSourceNetworkID,
    SocialSourcePage,
    CAST(ParamPrice AS BIGINT)           AS ParamPrice,
    ParamOrderID, ParamCurrency,
    CAST(ParamCurrencyID AS SMALLINT)    AS ParamCurrencyID,
    OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag,
    CAST(HasGCLID AS SMALLINT)           AS HasGCLID,
    CAST(RefererHash AS BIGINT)          AS RefererHash,
    CAST(URLHash AS BIGINT)              AS URLHash,
    CAST(CLID AS INT)                    AS CLID
FROM read_files('{source}', format => 'parquet')
DISTRIBUTE BY rand()
"""


# ---- helpers ----------------------------------------------------------------

def require_env(name):
    v = os.getenv(name)
    if not v:
        sys.exit(f"ERROR: set {name}")
    return v


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def connect(host, http_path, token):
    return sql.connect(server_hostname=host, http_path=http_path,
                       access_token=token)


def discover_target_columns(host, http_path, token, target):
    with connect(host, http_path, token) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {target}")
            rows = cur.fetchall()
    columns = []
    for row in rows:
        col_name = row[0]
        if not col_name or col_name.startswith('#'):
            break
        columns.append(col_name)
    return columns


def stage_exists(cur, stage):
    try:
        cur.execute(f"DESCRIBE TABLE {stage}")
        cur.fetchall()
        return True
    except Exception:
        return False


def ensure_stage(host, http_path, token, stage, source):
    """Create + populate staging if it doesn't already exist.

    Schema is defined explicitly via STAGE_DDL_TEMPLATE (matches target's
    types). Load uses explicit CASTs via STAGE_INSERT_TEMPLATE plus
    DISTRIBUTE BY rand() so the resulting files are randomized.
    """
    with connect(host, http_path, token) as conn:
        with conn.cursor() as cur:
            if stage_exists(cur, stage):
                cur.execute(f"SELECT COUNT(*) FROM {stage}")
                n = cur.fetchall()[0][0]
                print(f"→ Staging table {stage} already exists ({n:,} rows)")
                return

            print(f"→ Creating staging table {stage} with explicit schema")
            cur.execute(STAGE_DDL_TEMPLATE.format(stage=stage))

            print(f"→ Populating from '{source}' (CAST + DISTRIBUTE BY rand)")
            t0 = time.monotonic()
            cur.execute(STAGE_INSERT_TEMPLATE.format(stage=stage, source=source))
            cur.fetchall()
            elapsed = time.monotonic() - t0

            cur.execute(f"SELECT COUNT(*) FROM {stage}")
            n = cur.fetchall()[0][0]
            print(f"→ Staging populated: {n:,} rows in {elapsed:.1f}s")


# ---- worker -----------------------------------------------------------------

def worker(
    worker_id, assignments,
    target, stage, columns, K, rows_per_commit,
    host, http_path, token,
    per_worker_rps,
    error_queue,
    global_total_rows, global_start_mono,
):
    cols_csv = ",".join(columns)

    try:
        conn = connect(host, http_path, token)
        cur = conn.cursor()
    except Exception as exc:
        error_queue.put(f"[w{worker_id}] connect failed: {exc}")
        return

    rows_sent = 0
    chunk_num = 0
    w_start = time.monotonic()

    try:
        for (iter_idx, bucket_idx) in assignments:
            if _INTERRUPTED:
                break
            # Stage already has the right schema and is already randomized,
            # so this INSERT is just SELECT * with a hash-bucket filter.
            insert_sql = (
                f"INSERT INTO {target} ({cols_csv})\n"
                f"SELECT {cols_csv} FROM {stage}\n"
                f"WHERE pmod(abs(hash(WatchID)), {K}) = {bucket_idx}"
            )

            try:
                t0 = time.monotonic()
                cur.execute(insert_sql)
                result = cur.fetchall()
                insert_s = time.monotonic() - t0
            except Exception as exc:
                error_queue.put(
                    f"[w{worker_id}] INSERT iter={iter_idx} bucket={bucket_idx}: {exc}"
                )
                continue

            rows_in_batch = 0
            if result:
                desc = [d[0] for d in cur.description] if cur.description else []
                for name in ("num_inserted_rows", "num_affected_rows",
                             "numInsertedRows", "numAffectedRows"):
                    if name in desc:
                        rows_in_batch = result[0][desc.index(name)]
                        break
            if not rows_in_batch:
                rows_in_batch = rows_per_commit

            chunk_num += 1
            rows_sent += rows_in_batch
            elapsed = time.monotonic() - w_start
            rps = rows_sent / elapsed if elapsed > 0 else 0
            batch_rps = rows_in_batch / insert_s if insert_s > 0 else 0

            # Update shared counter and snapshot for aggregate rate.
            with global_total_rows.get_lock():
                global_total_rows.value += rows_in_batch
                agg_total = global_total_rows.value
            agg_elapsed = time.monotonic() - global_start_mono.value
            agg_rps = agg_total / agg_elapsed if agg_elapsed > 0 else 0

            print(
                f"[w{worker_id:>3d} c{chunk_num:>6d}] "
                f"iter={iter_idx:>5d} bkt={bucket_idx:>3d}  "
                f"rows={rows_in_batch:>10,} in {insert_s:>5.1f}s  "
                f"batch_rps={batch_rps:>9,.0f}  "
                f"w_avg_rps={rps:>9,.0f}  "
                f"agg_total={agg_total:>14,}  "
                f"agg_rps={agg_rps:>10,.0f}",
                flush=True,
            )

            if per_worker_rps > 0:
                expected = rows_sent / per_worker_rps
                if elapsed < expected:
                    time.sleep(expected - elapsed)

    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

    print(f"[w{worker_id:>3d}] done: {rows_sent:,} rows in "
          f"{time.monotonic() - w_start:.1f}s", flush=True)


# ---- main -------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--target",       required=True)
    p.add_argument("--stage",        default=DEFAULT_STAGE)
    p.add_argument("--source",       default=DEFAULT_SOURCE)
    p.add_argument("--total-rows",   type=int, default=100_000_000_000)
    p.add_argument("--commit-rows",  type=int, default=10_000_000)
    p.add_argument("--parallel",     type=int, default=16)
    p.add_argument("--target-rps",   type=int, default=0)
    p.add_argument("--stage-only",   action="store_true")
    p.add_argument("--skip-stage",   action="store_true")
    args = p.parse_args()

    host      = require_env("DATABRICKS_SERVER_HOSTNAME")
    http_path = require_env("DATABRICKS_HTTP_PATH")
    token     = require_env("DATABRICKS_TOKEN")

    if not args.skip_stage:
        ensure_stage(host, http_path, token, args.stage, args.source)
        if args.stage_only:
            print("\n--stage-only set: exiting after staging.")
            return

    print(f"\nStart time      : {now_utc()}")
    print(f"Staging table   : {args.stage}")
    print(f"Target          : {args.target}")
    print(f"Total rows      : {args.total_rows:,}")
    print(f"Commit rows     : {args.commit_rows:,}")

    # Plan: each commit writes ~commit_rows rows. We slice the stage into
    # K hash buckets (each ~SOURCE_ROW_COUNT/K rows). After K commits we
    # exhaust one "replay" through the stage; further replays are needed
    # iff total_rows demands more than K commits.
    total_commits = max(1, args.total_rows // args.commit_rows)
    K = max(1, SOURCE_ROW_COUNT // args.commit_rows)
    iterations_needed = (total_commits + K - 1) // K  # ceil

    print(f"Sub-chunks (K)  : {K}")
    print(f"Replays of stage: {iterations_needed:,}")
    print(f"Total commits   : {total_commits:,}")
    print(f"Parallel workers: {args.parallel}")
    if args.target_rps:
        per_w = args.target_rps // args.parallel
        print(f"Throttle        : {args.target_rps:,} rps total "
              f"(~{per_w:,}/worker)")
    else:
        print("Throttle        : off")

    print("→ Discovering target columns")
    columns = discover_target_columns(host, http_path, token, args.target)
    print(f"  {len(columns)} columns")

    # Build the full (iter, bucket) grid, then cap to exactly the number
    # of commits requested. This honors --total-rows precisely.
    plan = [(i, b) for i in range(iterations_needed) for b in range(K)]
    plan = plan[:total_commits]

    worker_lists = [[] for _ in range(args.parallel)]
    for idx, item in enumerate(plan):
        worker_lists[idx % args.parallel].append(item)

    per_worker_rps = (args.target_rps // args.parallel
                      if args.target_rps else 0)

    print()
    error_queue = mp.Queue()
    procs = []
    t0 = time.monotonic()

    # Shared aggregate counters across workers.
    global_total_rows = mp.Value('q', 0)
    global_start_mono = mp.Value('d', t0)

    for w in range(args.parallel):
        proc = mp.Process(
            target=worker,
            args=(
                w + 1, worker_lists[w],
                args.target, args.stage, columns, K, args.commit_rows,
                host, http_path, token,
                per_worker_rps,
                error_queue,
                global_total_rows, global_start_mono,
            ),
        )
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()

    elapsed = time.monotonic() - t0
    errors = []
    while not error_queue.empty():
        errors.append(error_queue.get())

    print()
    print("=" * 70)
    print(f"End time        : {now_utc()}")
    print(f"Duration        : {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"Total commits   : {total_commits:,}")
    print(f"Errors          : {len(errors)}")
    for e in errors[:20]:
        print(f"  {e}", file=sys.stderr)
    if len(errors) > 20:
        print(f"  ... {len(errors) - 20} more", file=sys.stderr)
    print("=" * 70)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
