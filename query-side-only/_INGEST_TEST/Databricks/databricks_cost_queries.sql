-- =====================================================================
-- Cost & history verification queries for the smoke test
--
-- Run these AFTER the smoke-test streaming script completes.
-- Goal: confirm the queries return data (or empty-but-valid results)
-- so we know they'll work for the real 100B run.
--
-- Note on billing data latency: system.billing.usage typically lags
-- real consumption by 5-15 minutes. If queries 3 & 4 return nothing
-- immediately after the smoke test, wait a bit and re-run.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Row count in the target table (sanity check)
-- ---------------------------------------------------------------------

SELECT COUNT(*) AS rows_in_target
FROM workspace.clickbench.`100b_clustered`;


-- ---------------------------------------------------------------------
-- 2. Delta transaction history on the target
--    Each streaming micro-batch produces one entry, with metrics.
--    Look at numFilesAdded / numOutputBytes / numOutputRows per batch.
-- ---------------------------------------------------------------------

DESCRIBE HISTORY workspace.clickbench.`100b_clustered`;


-- ---------------------------------------------------------------------
-- 3. Warehouse / cluster DBU consumption during the smoke-test window
--    Adjust the time window to match when you actually ran the test.
--    The 'usage_metadata' struct contains cluster_id / warehouse_id —
--    filter by yours if you want to isolate the streaming job's compute
--    from other activity.
-- ---------------------------------------------------------------------

SELECT
    usage_start_time,
    usage_end_time,
    sku_name,
    billing_origin_product,
    usage_quantity,
    usage_unit,
    usage_metadata
FROM system.billing.usage
WHERE usage_start_time >= current_timestamp() - INTERVAL 2 HOURS
ORDER BY usage_start_time DESC
LIMIT 100;


-- ---------------------------------------------------------------------
-- 4. Predictive Optimization activity on the target table
--    Expected for the smoke test: empty (PO doesn't fire in 60 seconds).
--    What we're confirming here is that the QUERY works against the
--    system table, so we'll be able to read PO cost during the real run.
-- ---------------------------------------------------------------------

SELECT
    catalog_name,
    schema_name,
    table_name,
    operation_type,
    start_time,
    usage_quantity,
    usage_unit,
    operation_metrics
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = 'workspace'
  AND schema_name = 'clickbench'
  AND table_name  = '100b_clustered'
ORDER BY start_time DESC;


-- ---------------------------------------------------------------------
-- 5. Same PO query, scoped to the whole workspace (broader view)
--    Useful to confirm PO is even running on this account / schema.
--    If this returns rows for OTHER tables but nothing for 100b_clustered,
--    PO is alive but hasn't picked up our table yet (it likely will once
--    enough unclustered files accumulate during the real run).
-- ---------------------------------------------------------------------

SELECT
    catalog_name,
    schema_name,
    table_name,
    operation_type,
    start_time,
    usage_quantity,
    usage_unit
FROM system.storage.predictive_optimization_operations_history
WHERE start_time >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY start_time DESC
LIMIT 50;


-- ---------------------------------------------------------------------
-- 6. PO-attributable DBUs across the workspace (last 24h)
--    Cross-check against query #5: each operation in PO history should
--    have a corresponding billing row tagged PREDICTIVE_OPTIMIZATION.
-- ---------------------------------------------------------------------

SELECT
    usage_start_time,
    sku_name,
    usage_quantity,
    usage_unit
FROM system.billing.usage
WHERE billing_origin_product = 'PREDICTIVE_OPTIMIZATION'
  AND usage_start_time >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY usage_start_time DESC
LIMIT 50;


-- ---------------------------------------------------------------------
-- 7. Is PO enabled on this catalog / schema / table?
--    Predictive Optimization is now enabled by default on most accounts
--    by April 2026, but worth confirming for your workspace.
-- ---------------------------------------------------------------------

SHOW TBLPROPERTIES workspace.clickbench.`100b_clustered`;
-- Look for delta.feature.clustering = supported (already confirmed)
-- PO opt-out at the table level shows as delta.predictiveOptimization = disabled


-- =====================================================================
-- Smoke-test pass criteria
-- =====================================================================
-- (1) Achieved rows/sec from the Python notebook >= ~800k/s
--     (close enough to 1M to scale up; if much lower, resize the cluster
--     before the real run)
-- (2) Query #1 returns the row count matching the notebook output
-- (3) Query #2 returns one row per streaming micro-batch with non-zero
--     numFilesAdded
-- (4) Query #3 returns at least one row with billing_origin_product
--     showing the streaming compute usage (may take 5-15 min)
-- (5) Query #4 returns successfully (empty result is OK for smoke test)
-- (6) Query #5 returns rows from somewhere in your workspace
--     (confirms PO is operational on the account)
--
-- If all six are green, plumbing is verified. Then:
--   TRUNCATE TABLE workspace.clickbench.`100b_clustered`;
--   dbutils.fs.rm("dbfs:/tmp/100b_clustered_smoketest_checkpoint",
--                 recurse=True)
-- and start the real run with a longer duration, larger row cap, and
-- the .repartition(rand()) shuffle we discussed earlier.
