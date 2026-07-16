# Rolling ingest feed for the `hits_100B` dataset

This sets up a controllable, continuous feed of the `hits_100B` Parquet files
into an S3 bucket you own, at a target ingest rate (rows/sec). It's the shared
source-side piece for full-path real-time ingest tests — any system with an
S3-triggered continuous-ingestion mechanism can point at the resulting bucket:

- **ClickHouse Cloud** — an S3 ClickPipe in continuous ingestion mode (see
  [`clickhouse-cloud/`](clickhouse-cloud/))
- **Snowflake** — Snowpipe (auto-ingest) watching the same bucket (see
  [`snowflake/`](snowflake/))

The feeder script (`rolling_s3_copy.py`) does a server-side S3-to-S3 copy —
data never transits through the machine running the script, so it's fast and
doesn't burn local disk or bandwidth.

---

## 1. Prerequisites

- An AWS account/role with:
  - `s3:GetObject` / `s3:ListBucket` on the source bucket (`public-pme` — already
    public-read, so this works with no extra setup)
  - `s3:PutObject` on your destination bucket
- AWS credentials configured on the machine running the script (`aws configure`,
  or an instance role if running on EC2)
- Python 3.8+

## 2. Set up the ingester environment

```bash
# 1. Make sure venv support is installed
sudo apt-get update
sudo apt-get install -y python3-venv

# 2. Create a venv for the ingester
python3 -m venv ~/.venvs/ch-ingest

# 3. Activate it
source ~/.venvs/ch-ingest/bin/activate

# 4. Install dependencies
pip install --upgrade pip
pip install boto3 clickhouse-connect pyarrow
pip install thriftpy2
```

`boto3` is required by `rolling_s3_copy.py` itself (server-side S3 copy).
`clickhouse-connect`, `pyarrow`, and `thriftpy2` are for validation/analysis
scripts elsewhere in this benchmark (e.g. querying ClickHouse Cloud or reading
Parquet metadata directly) — install them here too so the same venv covers
both feeding and checking the ingest.

Remember to `source ~/.venvs/ch-ingest/bin/activate` again in any new shell
before running these scripts.

## 3. Create the destination bucket

Same region as the source (`eu-west-3`) avoids cross-region transfer fees and
is faster:

```bash
aws s3 mb s3://my-benchmark-bucket --region eu-west-3
```

Grant read access on this bucket to whichever system will be watching it
(ClickPipes IAM role, Snowflake's storage integration role, etc.) — see the
relevant system folder for the exact policy.

## 4. Run the feeder

`rolling_s3_copy.py` lives alongside this doc. It paces file copies so the
destination bucket fills at a target rows/sec rate:

```
interval_between_files = rows_per_file (~50,000,000) / eps
```

Sanity-check the timing first, without copying anything:

```bash
./rolling_s3_copy.py --dest-bucket my-benchmark-bucket --eps 1000000 --dry-run
```

Run for real, at the dataset's native rate (1,000,000 rows/sec):

```bash
./rolling_s3_copy.py --dest-bucket my-benchmark-bucket --eps 1000000
```

Run slower, e.g. 100,000 rows/sec:

```bash
./rolling_s3_copy.py --dest-bucket my-benchmark-bucket --eps 100000
```

Resume after an interruption (pick up at file index 500):

```bash
./rolling_s3_copy.py --dest-bucket my-benchmark-bucket --eps 1000000 --start-index 500
```

Loop indefinitely once all 2,000 files are copied (keeps keys lexicographically
increasing across passes, which matters if the consumer uses ordered
continuous-ingestion mode):

```bash
./rolling_s3_copy.py --dest-bucket my-benchmark-bucket --eps 1000000 --loop
```

Run `./rolling_s3_copy.py --help` for the full flag list.

## 5. Point a consumer at the bucket

Once the feeder is running, configure the system under test to continuously
ingest from `my-benchmark-bucket`:

- ClickHouse Cloud: see [`clickhouse-cloud/`](clickhouse-cloud/) for the
  S3 ClickPipe setup (continuous ingestion mode).
- Snowflake: see [`snowflake/`](snowflake/) for the Snowpipe auto-ingest setup
  (S3 event notifications → SQS → Snowpipe).

## 6. Notes

- The feeder does a managed multipart server-side copy per file (~8.78 GB
  median), so per-file copy time is typically well under the interval needed
  for realistic eps targets. If you see "falling behind eps target" warnings,
  either raise `--eps` expectations or check for throttling on the destination
  bucket.
- No local disk space is required by the feeder itself — only the destination
  S3 bucket needs to be able to hold whatever portion of the ~17.5 TB dataset
  you plan to feed through.
