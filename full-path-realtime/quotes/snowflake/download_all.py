#!/usr/bin/env python3
"""Parallel, resume-friendly download of all quotes_YYYY-MM-DD.parquet files
from S3 (us-east-2) to /data/quotes on the Paris box. Skips files already present
with matching size, so it's safe to re-run (e.g. after a creds refresh)."""
import boto3, os, time
from concurrent.futures import ThreadPoolExecutor, as_completed

BUCKET = "pme-internal"
PREFIX = "stockhouse/"
DEST = "/data/quotes"
WORKERS = 8

os.makedirs(DEST, exist_ok=True)
s3 = boto3.client("s3", region_name="us-east-2")

files = []
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX + "quotes_"):
    for o in pg.get("Contents", []):
        name = o["Key"].split("/")[-1]
        if name.endswith(".parquet") and name != "quotes_0.parquet":
            files.append((o["Key"], name, o["Size"]))
files.sort()
total_gb = sum(s for _, _, s in files) / 1e9
print(f"{len(files)} files, {total_gb:.1f} GB total -> {DEST}", flush=True)

def dl(item):
    key, name, size = item
    dst = os.path.join(DEST, name)
    if os.path.exists(dst) and os.path.getsize(dst) == size:
        return (name, size, True)
    s3.download_file(BUCKET, key, dst)
    return (name, size, False)

done = skipped = 0
got_gb = 0.0
t0 = time.time()
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    for fut in as_completed([ex.submit(dl, f) for f in files]):
        name, size, was_skip = fut.result()
        done += 1
        if was_skip:
            skipped += 1
        else:
            got_gb += size / 1e9
        el = time.time() - t0
        rate = got_gb / el if el > 0 else 0
        print(f"[{done}/{len(files)}] {name} {'(have)' if was_skip else f'{size/1e9:.2f}GB'} "
              f"| downloaded {got_gb:.1f}GB in {el:.0f}s ({rate:.0f}... )" if False else
              f"[{done}/{len(files)}] {name} {'(have)' if was_skip else f'{size/1e9:.2f}GB'} "
              f"| cum {got_gb:.1f}GB @ {rate:.2f}GB/s", flush=True)

print(f"DONE: {done} files ({skipped} already present), {got_gb:.1f}GB downloaded in {time.time()-t0:.0f}s", flush=True)
