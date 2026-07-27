# ClickHouse Cloud reader utilization

This folder contains utilization screenshots for the ClickHouse Cloud read service used in the full-path real-time analytics benchmark.

The read service was responsible for serving the continuous query workload while fresh data kept arriving in the writer service. It ran separately from ingest, so the read path and write path were isolated from each other.

The service was provisioned with **16 CPU cores**, but the benchmark used only a small fraction of that capacity. Even while querying a table that grew toward 100 billion rows, ClickHouse served both dashboard queries and raw-data drill-downs with very low CPU usage.

## Setup

The read service used:

- **1 ClickHouse Cloud node**
- **16 CPU cores**
- **64 GiB RAM**
- dashboard queries against the pre-aggregated `AggregatingMergeTree` table
- raw-data drill-down queries against the sorted `MergeTree` table
- the same continuous query schedule used for Snowflake

The charts below come from the ClickHouse Cloud advanced dashboard and show the behavior of the read service during the benchmark run.

## 1. CPU usage

This chart shows how much CPU the read service used while serving the continuous workload.

Although the service had **16 CPU cores** provisioned, CPU usage stayed far below that limit, usually well under a single core. That is the key point: ClickHouse served the workload with substantial headroom, even while the raw table kept growing and queries continued to run throughout the benchmark.

![CPU Usage](Screenshot%202026-06-12%20at%2011.03.45.png)

## 2. Queries/second

This chart shows the continuous query activity on the read service.

The workload runs steadily throughout the benchmark. The dashboard and drill-down queries are sent on a fixed schedule while ingest continues in the background. The important point is that query execution remains regular and stable; the read service is not backing up or falling behind.

![Queries/second](Screenshot%202026-06-12%20at%2011.03.37.png)

## 3. Selected bytes/second

This chart shows the amount of data selected by the read workload over time.

As the benchmark progresses, the raw table grows larger, and the drill-down queries touch more data. The periodic spikes reflect those scheduled reads. Even with increasing selected bytes, CPU usage remains low, showing that ClickHouse can process the read workload efficiently without needing much of the provisioned read capacity.

![Selected Bytes/second](Screenshot%202026-06-12%20at%2011.20.09.png)

## 4. Selected rows/second

This chart shows the number of rows selected by the read workload.

The same pattern appears here: scheduled query bursts become larger as the dataset grows, especially for raw-data drill-downs. ClickHouse still keeps query execution efficient because the raw `MergeTree` table is sorted for the drill-down filters and the dashboard workload reads from pre-aggregated data.

![Selected Rows/second](Screenshot%202026-06-12%20at%2011.21.01.png)

## 5. Memory usage

This chart shows tracked memory usage on the read service.

Memory rises as the workload warms up and then stabilizes around the available working set. The important signal is that memory usage does not grow without bound as the table grows. The read service reaches a steady operating range and continues serving queries from there.

![Memory Usage](Screenshot%202026-06-12%20at%2011.20.48.png)

## 6. In-memory caches

This chart shows the in-memory cache footprint on the read service.

The cache warms up during the first part of the run and then stays broadly stable. That is expected for a long-running analytical service: frequently accessed data and metadata remain hot, helping repeated dashboard and drill-down queries stay fast.

![In-Memory Caches](Screenshot%202026-06-12%20at%2011.20.54.png)

## Summary

Together, these charts validate the ClickHouse read-side setup used in the benchmark.

The read service had 16 CPU cores available, but the continuous workload used only a small fraction of that capacity. Queries ran steadily, selected bytes and rows increased as the table grew, memory and caches stabilized, and CPU stayed low.

In other words, ClickHouse did not need much read compute to serve fast queries over continuously growing 100-billion-row data. The raw `MergeTree` table was already shaped for efficient drill-downs, and the `AggregatingMergeTree` table was already shaped for fast dashboards.
