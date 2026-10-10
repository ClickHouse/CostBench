SELECT
    toString(normalized_query_hash) AS query_hash,
    count() AS executions,
    countIf(mapContains(ProfileEvents, 'UserTimeMicroseconds')) AS with_user_cpu,
    countIf(mapContains(ProfileEvents, 'SystemTimeMicroseconds')) AS with_system_cpu,
    round(avg(ProfileEvents['UserTimeMicroseconds']
            + ProfileEvents['SystemTimeMicroseconds']) / 1000, 3) AS cpu_ms_per_query,
    round(avg(query_duration_ms), 3) AS duration_ms,
    round(avg(read_rows)) AS rows_read_per_query,
    any(query) AS example_query
FROM clusterAllReplicas(default, merge('system', '^query_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
    AND query_start_time >= toDateTime('2026-08-08 06:50:00', 'UTC')
    AND query_start_time < toDateTime('2026-08-09 20:00:00', 'UTC')
    AND type = 'QueryFinish'
    AND is_initial_query = 1
    AND query_kind = 'Select'
    AND current_database = 'quotes_async_t4'
GROUP BY normalized_query_hash
ORDER BY executions DESC
LIMIT 20
SETTINGS skip_unavailable_shards = 1,
         max_execution_time = 30,
         max_rows_to_read = 10000000;
