WITH
sel AS
(
    SELECT
        '01' AS id,
        'ClickHouse (40×236GiB)' AS bar_label,
        'ClickHouse%' AS system_pat,
        'Enterprise' AS tier,
        'default' AS compute_model,
        'aws' AS provider,
        'us-east-1' AS region,
        '236GiB' AS machine,
        '40' AS cluster_size,
        'no' AS tuned,
        toDate('2026-02-01') AS min_date,
        toDate('2026-05-28') AS max_date
    UNION ALL

    SELECT
        '19' AS id,
        'Snowflake (unclustered, Gen2 4X-L)' AS bar_label,
        'Snowflake%' AS system_pat,
        'enterprise' AS tier,
        NULL AS compute_model,
        'aws' AS provider,
        'us-east-1' AS region,
        'Gen2 4X-Large' AS machine,
        '172.8' AS cluster_size,
        'no' AS tuned,
        toDate('2026-02-01') AS min_date,
        toDate('2026-05-28') AS max_date
    UNION ALL

    SELECT
        '20' AS id,
        'Snowflake (clustered, Gen2 4X-L, amortized)' AS bar_label,
        'Snowflake%' AS system_pat,
        'enterprise' AS tier,
        NULL AS compute_model,
        'aws' AS provider,
        'us-east-1' AS region,
        'Gen2 4X-Large' AS machine,
        '172.8' AS cluster_size,
        'clustered' AS tuned,
        toDate('2026-02-01') AS min_date,
        toDate('2026-05-28') AS max_date

    UNION ALL

    SELECT
        '21' AS id,
        'Snowflake (clustered, Gen2 4X-L, full)' AS bar_label,
        'Snowflake%' AS system_pat,
        'enterprise' AS tier,
        NULL AS compute_model,
        'aws' AS provider,
        'us-east-1' AS region,
        'Gen2 4X-Large' AS machine,
        '172.8' AS cluster_size,
        'clustered' AS tuned,
        toDate('2026-02-01') AS min_date,
        toDate('2026-05-28') AS max_date
),

rows AS
(
  SELECT
      s.id,
      s.bar_label,
      replaceRegexpOne(
          replaceRegexpOne(
              replaceRegexpOne(c.system, '^Redshift.*$', 'Redshift'),
              '^ClickHouse.*$', 'ClickHouse'
          ),
          '^Databricks.*$', 'Databricks'
      ) AS sys,
      c.tier AS tier,
      c.compute_model AS cmodel,
      c.provider AS prov,
      c.region AS reg,
      c.machine AS mach,
      c.cluster_size AS csize,
      c.tuned AS tuned,
      c.date AS dt,
      c.data_size AS data_sz,
      c.storage_cost AS stor_cost,
      c.compute_costs AS comp_arr,
      c.result AS res_arr
  FROM sel s
  INNER JOIN
  (
      SELECT
          system,
          date,
          tier,
          compute_model,
          provider,
          region,
          machine,
          cluster_size,
          tuned,
          data_size,
          storage_cost,
          compute_costs,
          result
      FROM bench2cost_100B.costs
  ) c
      ON  lowerUTF8(c.system) LIKE lowerUTF8(s.system_pat)
      AND toDate(c.date) >= s.min_date
      AND toDate(c.date) <= s.max_date
      AND ifNull(c.tier, '') = s.tier
      AND ifNull(c.compute_model, 'default') = ifNull(s.compute_model, 'default')
      AND lowerUTF8(ifNull(c.provider, '')) = lowerUTF8(s.provider)
      AND replaceAll(lowerUTF8(ifNull(c.region, '')), '-', '') =
          replaceAll(lowerUTF8(s.region), '-', '')
      AND c.machine LIKE concat('%', s.machine)
      AND ifNull(nullIf(c.cluster_size, 'null'), 'serverless') =
          ifNull(s.cluster_size, 'serverless')
      AND ifNull(c.tuned, 'no') = ifNull(s.tuned, 'no')
),

per_idx AS
(
    SELECT
        id, bar_label, sys, tier, cmodel, prov, reg, mach, csize, tuned, dt, data_sz, stor_cost,
        idx, tup,
        (isNull(tup.1) AND isNull(tup.2) AND isNull(tup.3)) AS all_null,
        arrayMin(arrayFilter(x -> isNotNull(x), [tup.1, tup.2, tup.3])) AS hot_cost,
        arrayElement(res_arr, idx) AS rt,
        arrayMin(arrayFilter(x -> isNotNull(x), [rt.1, rt.2, rt.3])) AS hot_rt
    FROM rows
    ARRAY JOIN arrayEnumerate(comp_arr) AS idx, comp_arr AS tup
),

mask AS
(
    SELECT idx, min(toUInt8(NOT all_null)) AS keep_idx
    FROM per_idx
    GROUP BY idx
)

SELECT
    id,
    sys AS system,
    tier,
    cmodel AS compute_model,
    tuned,
    bar_label,
    prov AS provider,
    reg AS region,
    mach AS machine,
    csize AS cluster,
    round(sumIf(hot_rt,   keep_idx = 1 AND isNotNull(hot_rt)),   3) AS rt_hot,
    round(sumIf(hot_cost, keep_idx = 1 AND isNotNull(hot_cost)), 5) AS cost_hot,
    sumIf(1, keep_idx = 1) AS nq
FROM per_idx
INNER JOIN mask USING (idx)
GROUP BY id, sys, tier, cmodel, tuned, bar_label, prov, reg, mach, csize
ORDER BY id
-- FORMAT JSONEachRow
;