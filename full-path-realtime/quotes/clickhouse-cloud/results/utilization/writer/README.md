# ClickHouse Cloud writer utilization

This folder contains utilization screenshots for the ClickHouse Cloud write and ordering service used in the full-path real-time analytics benchmark.

The service was responsible for the write side of the benchmark: ingesting the continuous stock-quotes stream, writing raw data directly into a sorted `MergeTree` table, maintaining pre-aggregations through incremental materialized views into an `AggregatingMergeTree` table, and keeping both tables query-ready while the read workload ran on a separate service.

The charts below come from the ClickHouse Cloud advanced dashboard and show the writer service during the benchmark run.

## Setup

The write service used:

- **2 ClickHouse Cloud nodes**
- **2 CPU cores per node**
- **8 GiB RAM per node**
- continuous ingest at roughly **1 million rows/sec**
- raw data written directly to a sorted `MergeTree` table
- pre-aggregated data maintained through incremental materialized views into an `AggregatingMergeTree` table

Together, these charts validate that the writer service sustained the target ingest rate, kept background merges active, controlled part growth, and used CPU and memory without saturating.

## 1. Inserted rows/sec

This chart validates that the writer service sustained the target ingest rate throughout the run.

Rows/sec stayed close to **1 million rows per second**. The important point is that the service continuously kept up with the configured real-time ingest rate instead of falling behind.

![Inserted Rows/sec](Screenshot%202026-06-12%20at%2011.01.56.png)

## 2. Inserted bytes/sec

Rows/sec alone can be misleading: many tiny rows are not the same as wider analytical rows. This chart shows the actual byte throughput handled by the writer service.

The workload sustained roughly **70–80 MB/sec** of inserted data.

![Inserted Bytes/sec](Screenshot%202026-06-12%20at%2011.02.06.png)

## 3. Merged rows/sec

ClickHouse keeps tables query-ready through continuous background merges. These merges preserve the physical layout required for efficient reads.

The merged rows/sec chart shows that background merges were active throughout the run, often processing several million rows per second. That is expected: the write service is not just accepting new rows, but continuously reshaping them into the sorted, query-ready layout used by the drill-down workload.

![Merged Rows/sec](Screenshot%202026-06-12%20at%2011.02.38.png)

## 4. Max parts per partition

Part count is an important signal for whether background merges are keeping up. If inserts create parts faster than merges can reduce their number, the number of active parts grows without bound and eventually hurts query performance.

In this run, the maximum part count remained under control, staying below roughly **100 parts**. That shows that the writer service was able to ingest, sort, pre-aggregate, and merge continuously without accumulating an unhealthy backlog.

![Max Parts For Partition](Screenshot%202026-06-12%20at%2011.02.53.png)

## 5. CPU usage

This chart shows CPU usage on the writer service during the run.

CPU was well utilized but did not saturate. The service had enough work to do — ingest, sorting, materialized-view updates, and merges — but still had headroom instead of running pinned at the limit.

![CPU Usage](Screenshot%202026-06-12%20at%2011.01.30.png)

## 6. Memory usage

This chart shows tracked memory usage on the writer service.

Memory stayed stable during the run, with normal variation but no sustained upward trend. That indicates the workload was bounded: the service was not accumulating unmerged data or refresh state in memory, and the write path remained stable over time.

![Memory Usage](Screenshot%202026-06-12%20at%2011.01.46.png)

## Summary

Together, these charts validate the ClickHouse write-side setup used in the benchmark.

The writer service sustained the target ingest rate, handled the corresponding byte throughput, kept background merges active, maintained part counts under control, and used CPU and memory without saturating. In other words, ClickHouse kept the raw `MergeTree` table and the pre-aggregated `AggregatingMergeTree` table fresh and query-ready continuously while new stock-quotes data kept arriving.
