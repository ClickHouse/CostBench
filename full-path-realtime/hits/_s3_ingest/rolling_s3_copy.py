#!/usr/bin/env python3
"""
rolling_s3_copy.py

Continuously copies the ClickHouse "hits_100B" benchmark Parquet files from the
public source bucket into a destination bucket/prefix you control, pacing the
copies so the *effective ingest rate* (rows/sec, i.e. "eps") matches a target
you set.

This does a server-side S3-to-S3 copy (via boto3's managed multipart copy) --
data never transits through this machine, so it's fast and doesn't burn your
instance's bandwidth or local disk.

Why pacing by file works:
    Each source file holds ~50,000,000 rows. If you want the destination
    prefix to fill up at a rate of `--eps` rows/sec, you copy one file every

        interval = rows_per_file / eps   seconds

    e.g. eps=1,000,000 (the dataset's native rate) -> ~50s between files,
    which reproduces the original real-time cadence. eps=100,000 -> ~500s
    (8m20s) between files, i.e. 10x slower than native.

Requirements:
    pip install boto3 --break-system-packages   # or use a venv
    AWS credentials configured (aws configure) with:
      - GetObject/ListBucket on the source bucket
      - PutObject on your destination bucket/prefix

Examples:
    # Dry run to sanity check timing without copying anything:
    ./rolling_s3_copy.py --dest-bucket public-pme --dest-prefix hits_100B_feed/ \\
        --eps 1000000 --dry-run

    # Real run at native rate (1,000,000 eps), logging to a file too:
    ./rolling_s3_copy.py --dest-bucket public-pme --dest-prefix hits_100B_feed/ \\
        --eps 1000000 --log-file ./rolling_copy.log

    # 10x slower than native, resuming from file 500 after an interruption:
    ./rolling_s3_copy.py --dest-bucket public-pme --dest-prefix hits_100B_feed/ \\
        --eps 100000 --start-index 500

    # Loop indefinitely, keeping every file's key lexicographically increasing
    # across passes (needed for ClickPipes' default lexicographic
    # continuous-ingestion mode):
    ./rolling_s3_copy.py --dest-bucket public-pme --dest-prefix hits_100B_feed/ \\
        --eps 1000000 --loop
"""

import argparse
import logging
import sys
import time

import boto3
from boto3.s3.transfer import TransferConfig

DEFAULT_SOURCE_BUCKET = "public-pme"
DEFAULT_SOURCE_PREFIX = "hits_100B/2000x50m_span_27h46m40s_rps_1m/"
DEFAULT_ROWS_PER_FILE = 50_000_000  # README median is 49,999,863; close enough
DEFAULT_NUM_FILES = 2000
SUMMARY_EVERY = 20  # print a running-average summary every N files

log = logging.getLogger("rolling_s3_copy")


def setup_logging(log_file=None):
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    log.addHandler(stream_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(fmt)
        log.addHandler(file_handler)


def human_bytes(n):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


def parse_args():
    p = argparse.ArgumentParser(
        description="Pace a rolling S3-to-S3 copy of the hits_100B dataset to simulate a target ingest rate."
    )
    p.add_argument("--source-bucket", default=DEFAULT_SOURCE_BUCKET)
    p.add_argument("--source-prefix", default=DEFAULT_SOURCE_PREFIX)
    p.add_argument("--dest-bucket", required=True)
    p.add_argument("--dest-prefix", default="", help="Optional key prefix in the destination bucket")
    p.add_argument("--num-files", type=int, default=DEFAULT_NUM_FILES)
    p.add_argument("--rows-per-file", type=int, default=DEFAULT_ROWS_PER_FILE)
    p.add_argument("--eps", type=float, required=True, help="Target ingest rate in rows/sec")
    p.add_argument("--start-index", type=int, default=0, help="File index to start/resume from (0-1999)")
    p.add_argument("--region", default="eu-west-3", help="Region for the S3 client")
    p.add_argument("--loop", action="store_true", help="After the last file, wrap around and keep going")
    p.add_argument("--dry-run", action="store_true", help="Print the plan without copying anything")
    p.add_argument("--log-file", default=None, help="Also write logs to this file (in addition to stdout)")
    return p.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_file)

    if args.eps <= 0:
        sys.exit("--eps must be positive")

    interval = args.rows_per_file / args.eps
    total_files = args.num_files - args.start_index
    log.info(
        "Target eps: %s rows/sec | rows/file: %s | interval: %.2fs | files: %d (index %d-%d)%s | "
        "single-pass wall time: ~%.2fh",
        f"{args.eps:,.0f}",
        f"{args.rows_per_file:,}",
        interval,
        total_files,
        args.start_index,
        args.num_files - 1,
        ", looping indefinitely after that" if args.loop else "",
        total_files * interval / 3600,
    )

    if args.dry_run:
        log.info("Dry run only -- no objects will be copied.")
        return

    s3 = boto3.client("s3", region_name=args.region)
    transfer_config = TransferConfig(multipart_threshold=1024 * 1024 * 1024, max_concurrency=10)

    # Running totals for periodic throughput summaries.
    run_start = time.time()
    files_done = 0
    bytes_done = 0
    rows_done = 0

    generation = 0
    while True:
        for idx in range(args.start_index, args.num_files):
            src_key = f"{args.source_prefix}hits_p{idx:05d}.parquet"

            if args.loop and generation > 0:
                # Keep keys lexicographically increasing across passes so
                # ClickPipes' default ordered-ingestion mode keeps working.
                dest_key = f"{args.dest_prefix}g{generation:04d}_hits_p{idx:05d}.parquet"
            else:
                dest_key = f"{args.dest_prefix}hits_p{idx:05d}.parquet"

            try:
                size_bytes = s3.head_object(Bucket=args.source_bucket, Key=src_key)["ContentLength"]
            except Exception as exc:
                log.warning("Could not stat %s before copying (%s) -- continuing anyway", src_key, exc)
                size_bytes = None

            log.info(
                "[gen %d %d/%d] copying %s -> s3://%s/%s%s",
                generation,
                idx + 1,
                args.num_files,
                src_key,
                args.dest_bucket,
                dest_key,
                f" ({human_bytes(size_bytes)})" if size_bytes else "",
            )

            t0 = time.time()
            s3.copy(
                {"Bucket": args.source_bucket, "Key": src_key},
                args.dest_bucket,
                dest_key,
                Config=transfer_config,
            )
            elapsed = time.time() - t0

            throughput = f"{human_bytes(size_bytes / elapsed)}/s" if size_bytes and elapsed > 0 else "n/a"
            log.info(
                "[gen %d %d/%d] done in %.1fs (throughput: %s)",
                generation,
                idx + 1,
                args.num_files,
                elapsed,
                throughput,
            )

            files_done += 1
            if size_bytes:
                bytes_done += size_bytes
            rows_done += args.rows_per_file

            if files_done % SUMMARY_EVERY == 0:
                wall_elapsed = time.time() - run_start
                avg_throughput = bytes_done / wall_elapsed if wall_elapsed > 0 else 0
                avg_eps = rows_done / wall_elapsed if wall_elapsed > 0 else 0
                log.info(
                    "-- summary: %d files copied, %s total, running avg %s/s, %.0f rows/sec (target %.0f) --",
                    files_done,
                    human_bytes(bytes_done),
                    human_bytes(avg_throughput),
                    avg_eps,
                    args.eps,
                )

            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                log.warning(
                    "copy took %.1fs, longer than the %.1fs target interval -- falling behind eps target",
                    elapsed,
                    interval,
                )

        if not args.loop:
            break
        generation += 1
        args.start_index = 0  # subsequent passes always start from file 0


if __name__ == "__main__":
    main()
