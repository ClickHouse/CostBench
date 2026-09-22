-- Detailed, non-aggregated exports used by the compact result package.
-- Replace every <...> placeholder before running on a SQL warehouse.
-- Public result packaging pseudonymizes provider record/resource IDs.

-- ---------------------------------------------------------------------------
-- 1. MV refresh billing detail
-- Output: freshness/mv_refresh_allocation.csv
-- ---------------------------------------------------------------------------

WITH
params AS (
    SELECT
        CAST('<run-start-utc>' AS TIMESTAMP) AS run_start,
        CAST('<producer-finished-utc>' AS TIMESTAMP) AS run_end
),

mv_pipeline_ids AS (
    SELECT DISTINCT usage_metadata.dlt_pipeline_id AS pipeline_id
    FROM system.billing.usage
    CROSS JOIN params
    WHERE usage_metadata.uc_table_catalog = '<catalog>'
      AND usage_metadata.uc_table_schema = '<schema>'
      AND usage_metadata.uc_table_name =
          '<catalog>.<schema>.<materialized-view>'
      AND usage_metadata.dlt_pipeline_id IS NOT NULL
      AND usage_start_time < run_end
      AND usage_end_time > run_start
),

usage_lines AS (
    SELECT
        u.*,
        greatest(u.usage_start_time, p.run_start) AS allocated_start,
        least(u.usage_end_time, p.run_end) AS allocated_end
    FROM system.billing.usage AS u
    CROSS JOIN params AS p
    WHERE u.usage_metadata.dlt_pipeline_id IN (
        SELECT pipeline_id FROM mv_pipeline_ids
    )
      AND u.usage_unit = 'DBU'
      AND u.usage_start_time < p.run_end
      AND u.usage_end_time > p.run_start
),

allocated AS (
    SELECT
        *,
        usage_quantity
        * CAST(
            timestampdiff(
                MICROSECOND,
                allocated_start,
                allocated_end
            ) AS DECIMAL(38, 18)
        )
        / nullif(
            CAST(
                timestampdiff(
                    MICROSECOND,
                    usage_start_time,
                    usage_end_time
                ) AS DECIMAL(38, 18)
            ),
            0
        ) AS allocated_dbu
    FROM usage_lines
    WHERE allocated_end > allocated_start
)

SELECT
    u.usage_start_time,
    u.usage_end_time,
    u.allocated_start,
    u.allocated_end,
    u.record_id,
    u.record_type,
    u.usage_metadata.dlt_pipeline_id AS pipeline_id,
    u.usage_metadata.uc_table_catalog AS catalog_name,
    u.usage_metadata.uc_table_schema AS schema_name,
    u.usage_metadata.uc_table_name AS table_name,
    u.billing_origin_product,
    u.sku_name,
    u.usage_quantity AS source_dbu,
    u.allocated_dbu,
    p.currency_code,
    coalesce(
        p.pricing.effective_list.default,
        p.pricing.default
    ) AS price_per_dbu,
    u.allocated_dbu * coalesce(
        p.pricing.effective_list.default,
        p.pricing.default
    ) AS cost,
    u.custom_tags,
    u.ingestion_date
FROM allocated AS u
JOIN system.billing.list_prices AS p
  ON lower(p.sku_name) = lower(u.sku_name)
 AND lower(p.cloud) = lower(u.cloud)
 AND lower(p.usage_unit) = lower(u.usage_unit)
 AND p.price_start_time <= u.usage_start_time
 AND (
      p.price_end_time IS NULL
      OR u.usage_end_time <= p.price_end_time
 )
ORDER BY u.usage_start_time, u.record_id;


-- ---------------------------------------------------------------------------
-- 2. Predictive Optimization operation detail
-- Output: ingest/predictive_optimization_allocation.csv
-- `estimated_dbu` is provider-estimated, not authoritative billed DBU.
-- ---------------------------------------------------------------------------

SELECT
    metastore_name,
    catalog_name,
    schema_name,
    table_name,
    operation_id,
    operation_type,
    operation_status,
    start_time,
    end_time,
    usage_unit,
    usage_quantity AS estimated_dbu,
    operation_metrics
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = '<catalog>'
  AND schema_name = '<schema>'
  AND table_name IN ('<raw-table>', '<materialized-view>')
  AND start_time < CAST('<producer-finished-utc>' AS TIMESTAMP)
  AND coalesce(
        end_time,
        CAST('<producer-finished-utc>' AS TIMESTAMP)
      ) > CAST('<run-start-utc>' AS TIMESTAMP)
ORDER BY start_time, operation_id;


-- ---------------------------------------------------------------------------
-- 3. Zerobus DBU detail
-- Output: ingest/zerobus_ingest_allocation.csv
-- Find the UC table ID in DESCRIBE DETAIL properties['io.unitycatalog.tableId'].
-- ---------------------------------------------------------------------------

WITH
params AS (
    SELECT
        CAST('<run-start-utc>' AS TIMESTAMP) AS run_start,
        CAST('<producer-finished-utc>' AS TIMESTAMP) AS run_end
),

usage_lines AS (
    SELECT
        u.*,
        greatest(u.usage_start_time, p.run_start) AS allocated_start,
        least(u.usage_end_time, p.run_end) AS allocated_end
    FROM system.billing.usage AS u
    CROSS JOIN params AS p
    WHERE u.product_features.lakeflow_connect.zerobus_request_type = 'GRPC'
      AND u.usage_metadata.table_id = '<unity-catalog-table-id>'
      AND u.usage_unit = 'DBU'
      AND u.usage_start_time < p.run_end
      AND u.usage_end_time > p.run_start
)

SELECT
    usage_start_time,
    usage_end_time,
    allocated_start,
    allocated_end,
    record_id,
    record_type,
    usage_metadata.table_id AS table_id,
    product_features.lakeflow_connect.zerobus_request_type
        AS zerobus_request_type,
    billing_origin_product,
    sku_name,
    usage_quantity AS source_dbu,
    usage_quantity
    * CAST(
        timestampdiff(
            MICROSECOND,
            allocated_start,
            allocated_end
        ) AS DECIMAL(38, 18)
    )
    / nullif(
        CAST(
            timestampdiff(
                MICROSECOND,
                usage_start_time,
                usage_end_time
            ) AS DECIMAL(38, 18)
        ),
        0
    ) AS allocated_dbu,
    custom_tags,
    ingestion_date
FROM usage_lines
WHERE allocated_end > allocated_start
ORDER BY usage_start_time, record_id;
