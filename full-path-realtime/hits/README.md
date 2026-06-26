# ClickHouse Hits 100B Parquet Datasets

This document describes two public S3 Parquet datasets exported from the same randomized 100 billion row ClickHouse `hits` source layout.

Both datasets contain the same rows and the same original `hits` schema, with synthetic `EventTime` and `EventDate` values generated during export. The only intended difference between the two datasets is the synthetic timeline density:

- **Compact dataset:** 1,000,000 rows/second, total span **27h46m40s**
- **Long dataset:** 10,000 rows/second, total span **115d17h46m40s**

Both datasets were exported as 2,000 Parquet files, each around 50 million rows and below the 10 GB ClickPipes maximum file-size limit.

---


## S3 bucket and region

```text
Bucket:  public-pme
Region:  eu-west-3
Access:  public read
Format:  Parquet
Compression: ZSTD
Files:   2,000 per dataset
```

S3 URL style used by ClickHouse:

```text
https://s3.eu-west-3.amazonaws.com/public-pme/...
```

AWS CLI bucket path:

```text
s3://public-pme/...
```

---

## Dataset 1: Compact 27h46m40s timeline

### Location

```text
https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_27h46m40s_rps_1m/
```

File pattern:

```text
hits_p00000.parquet
hits_p00001.parquet
...
hits_p01999.parquet
```

ClickHouse read pattern:

```sql
FROM s3(
    'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_27h46m40s_rps_1m/hits_p*.parquet',
    'Parquet'
)
```

### Shape and size

Validated with ClickHouse over public S3:

```text
Files:                         2,000
Total rows:                    100,000,000,000
Min rows/file:                 49,977,890
Median rows/file:              49,999,863
Max rows/file:                 50,025,666

Total compressed Parquet size: 17,558.03 GB
Min file size:                 8.75 GB
Median file size:              8.78 GB
Max file size:                 8.80 GB
ClickPipes file-size target:   below 10 GB per file
```

All files are below the 10 GB ClickPipes maximum file-size limit. The largest observed file is **8.80 GB**, leaving about **1.20 GB** of headroom.

### Disk space needed to download

To download the full compact dataset, plan for at least:

```text
Minimum object size on disk:   ~17,558 GB
Practical free space target:   at least 19 TB
Safer working space target:    20 TB+
```

The practical target includes filesystem overhead, partial downloads, retries, temporary files, and room for checksums or manifests.

### Synthetic timeline

```text
Start time:       2024-01-01 00:00:00
Rows per second:  1,000,000
Total span:       100,000 seconds
Total span:       27h46m40s
```

Expected global time range:

```text
2024-01-01 00:00:00.000000
to approximately
2024-01-02 03:46:39.999999
```

Each file covers roughly 50 seconds of synthetic time.

Validated examples:

```text
hits_p00000.parquet:
  rows: 50,000,328
  EventTime: 2024-01-01 00:00:00.000005 .. 2024-01-01 00:00:49.999999
  EventDate: 2024-01-01
  bad EventDate rows: 0

hits_p01998.parquet:
  rows: 49,998,030
  EventTime: 2024-01-02 03:45:00.000000 .. 2024-01-02 03:45:49.999997
  EventDate: 2024-01-02
  bad EventDate rows: 0

hits_p01999.parquet:
  rows: 49,994,856
  EventTime: 2024-01-02 03:45:50.000001 .. 2024-01-02 03:46:39.999999
  EventDate: 2024-01-02
  bad EventDate rows: 0
```

Tiny microsecond gaps at file boundaries are expected because each file contains a randomized subset around 50 million rows, not exactly every possible synthetic row offset.

---

## Dataset 2: Long 115d17h46m40s timeline

### Location

```text
https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_115d17h46m40s_rps_10k/
```

File pattern:

```text
hits_p00000.parquet
hits_p00001.parquet
...
hits_p01999.parquet
```

ClickHouse read pattern:

```sql
FROM s3(
    'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_115d17h46m40s_rps_10k/hits_p*.parquet',
    'Parquet'
)
```

### Shape and size

Validated with ClickHouse over public S3:

```text
Files:                         2,000
Total rows:                    100,000,000,000
Min rows/file:                 49,977,890
Median rows/file:              49,999,863
Max rows/file:                 50,025,666
```

The row distribution matches the compact dataset because both datasets were exported from the same randomized source layout. Only the synthetic `EventTime` / `EventDate` mapping differs.

The long dataset should be treated as approximately the same compressed size class as the compact dataset:

```text
Expected compressed Parquet size: ~17.6 TB
Expected file size range:         ~8.75 GB .. ~8.80 GB
ClickPipes file-size target:      below 10 GB per file
```

For exact long-dataset file-size numbers, run the `ParquetMetadata` query in the validation section below.

### Disk space needed to download

To download the full long dataset, plan for at least:

```text
Expected object size on disk:   ~17,600 GB
Practical free space target:    at least 19 TB
Safer working space target:     20 TB+
```

The practical target includes filesystem overhead, partial downloads, retries, temporary files, and room for checksums or manifests.

### Synthetic timeline

```text
Start time:       2024-01-01 00:00:00
Rows per second:  10,000
Total span:       10,000,000 seconds
Total span:       115d17h46m40s
```

Expected global time range:

```text
2024-01-01 00:00:00.000000
to approximately
2024-04-25 17:46:39.999999
```

Each file covers roughly 5,000 seconds, or 1h23m20s, of synthetic time.

---

## Validation queries

### Count files and rows

Compact:

```sql
WITH per_file AS
(
    SELECT
        _file,
        count() AS rows
    FROM s3(
        'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_27h46m40s_rps_1m/hits_p*.parquet',
        'Parquet'
    )
    GROUP BY _file
)
SELECT
    count() AS files,
    min(rows) AS min_rows,
    quantileExact(0.5)(rows) AS p50_rows,
    max(rows) AS max_rows,
    sum(rows) AS total_rows,
    formatReadableQuantity(total_rows) AS readable_rows
FROM per_file;
```

Long:

```sql
WITH per_file AS
(
    SELECT
        _file,
        count() AS rows
    FROM s3(
        'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_115d17h46m40s_rps_10k/hits_p*.parquet',
        'Parquet'
    )
    GROUP BY _file
)
SELECT
    count() AS files,
    min(rows) AS min_rows,
    quantileExact(0.5)(rows) AS p50_rows,
    max(rows) AS max_rows,
    sum(rows) AS total_rows,
    formatReadableQuantity(total_rows) AS readable_rows
FROM per_file;
```

Expected for both datasets:

```text
files = 2000
total_rows = 100,000,000,000
```

### File sizes from Parquet metadata

This reads Parquet metadata, not the full dataset.

Compact:

```sql
WITH files AS
(
    SELECT
        _file,
        sum(toUInt64(total_compressed_size)) AS file_compressed_bytes,
        any(toUInt64(num_rows)) AS rows
    FROM s3(
        'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_27h46m40s_rps_1m/hits_p*.parquet',
        'ParquetMetadata'
    )
    GROUP BY _file
)
SELECT
    count() AS files,
    round(sum(file_compressed_bytes) / 1000000000., 2) AS total_compressed_GB,
    round(min(file_compressed_bytes) / 1000000000., 2) AS min_file_GB,
    round(quantileExact(0.5)(file_compressed_bytes) / 1000000000., 2) AS p50_file_GB,
    round(max(file_compressed_bytes) / 1000000000., 2) AS max_file_GB,
    sum(rows) AS total_rows,
    min(rows) AS min_rows,
    quantileExact(0.5)(rows) AS p50_rows,
    max(rows) AS max_rows
FROM files;
```

Long:

```sql
WITH files AS
(
    SELECT
        _file,
        sum(toUInt64(total_compressed_size)) AS file_compressed_bytes,
        any(toUInt64(num_rows)) AS rows
    FROM s3(
        'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_115d17h46m40s_rps_10k/hits_p*.parquet',
        'ParquetMetadata'
    )
    GROUP BY _file
)
SELECT
    count() AS files,
    round(sum(file_compressed_bytes) / 1000000000., 2) AS total_compressed_GB,
    round(min(file_compressed_bytes) / 1000000000., 2) AS min_file_GB,
    round(quantileExact(0.5)(file_compressed_bytes) / 1000000000., 2) AS p50_file_GB,
    round(max(file_compressed_bytes) / 1000000000., 2) AS max_file_GB,
    sum(rows) AS total_rows,
    min(rows) AS min_rows,
    quantileExact(0.5)(rows) AS p50_rows,
    max(rows) AS max_rows
FROM files;
```

### Time-span validation

Compact:

```sql
SELECT
    countDistinct(_path) AS files,
    count() AS rows,
    min(EventTime) AS min_time,
    max(EventTime) AS max_time,
    min(EventDate) AS min_date,
    max(EventDate) AS max_date,
    countIf(EventDate != toDate(EventTime)) AS bad_event_date_rows
FROM s3(
    'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_27h46m40s_rps_1m/hits_p*.parquet',
    'Parquet'
);
```

Long:

```sql
SELECT
    countDistinct(_path) AS files,
    count() AS rows,
    min(EventTime) AS min_time,
    max(EventTime) AS max_time,
    min(EventDate) AS min_date,
    max(EventDate) AS max_date,
    countIf(EventDate != toDate(EventTime)) AS bad_event_date_rows
FROM s3(
    'https://s3.eu-west-3.amazonaws.com/public-pme/hits_100B/2000x50m_span_115d17h46m40s_rps_10k/hits_p*.parquet',
    'Parquet'
);
```

---

## Download examples

Compact dataset:

```bash
aws s3 sync \
  s3://public-pme/hits_100B/2000x50m_span_27h46m40s_rps_1m/ \
  ./hits_100B_2000x50m_span_27h46m40s_rps_1m/ \
  --region eu-west-3 \
  --no-sign-request
```

Long dataset:

```bash
aws s3 sync \
  s3://public-pme/hits_100B/2000x50m_span_115d17h46m40s_rps_10k/ \
  ./hits_100B_2000x50m_span_115d17h46m40s_rps_10k/ \
  --region eu-west-3 \
  --no-sign-request
```

Before downloading either full dataset, verify available disk space:

```bash
df -h .
```

Recommended free space:

```text
One dataset:   at least 19 TB free, preferably 20 TB+
Both datasets: at least 38 TB free, preferably 40 TB+
```

---

## Summary

| Dataset | S3 prefix | Files | Rows | Timeline | Rows/sec | Approx compressed size | Max file size |
|---|---|---:|---:|---:|---:|---:|---:|
| Compact | `hits_100B/2000x50m_span_27h46m40s_rps_1m/` | 2,000 | 100B | 27h46m40s | 1,000,000 | 17,558.03 GB | 8.80 GB |
| Long | `hits_100B/2000x50m_span_115d17h46m40s_rps_10k/` | 2,000 | 100B | 115d17h46m40s | 10,000 | ~17,600 GB | ~8.8 GB |

Both datasets are public-read Parquet datasets in S3 `eu-west-3`, designed to remain below the 10 GB ClickPipes per-file limit while preserving a full 100 billion row scale.
