SELECT
    multiIf(
        normalized_query_hash = 4268017077458496684, 'A1',
        normalized_query_hash = 13116456435110875434, 'A2',
        normalized_query_hash = 16441976537628221706, 'A3',
        normalized_query_hash = 16076059573740575684, 'A4',
        normalized_query_hash = 14900422428945064879, 'D1', 'D2'
    ) AS benchmark_query,
    toString(toTimeZone(query_start_time_microseconds, 'UTC')) AS started_at_utc,
    query_id,
    query_duration_ms,
    read_rows,
    read_bytes,
    peak_threads_usage,
    length(thread_ids) AS total_distinct_threads,
    ProfileEvents['UserTimeMicroseconds'] AS user_cpu_us,
    ProfileEvents['SystemTimeMicroseconds'] AS system_cpu_us,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS os_cpu_us,
    ProfileEvents['RealTimeMicroseconds'] AS thread_wall_us,
    ProfileEvents['SelectedRows'] AS selected_rows,
    ProfileEvents['SelectedBytes'] AS selected_bytes,
    ProfileEvents['QueryTimeMicroseconds'] AS query_time_us,
    ProfileEvents['SelectQueryTimeMicroseconds'] AS select_query_time_us,
    ProfileEvents['OSCPUWaitMicroseconds'] AS cpu_wait_us,
    ProfileEvents['OSIOWaitMicroseconds'] AS io_wait_us,
    mapContains(ProfileEvents, 'QueryTimeMicroseconds') AS has_query_time,
    mapContains(ProfileEvents, 'SelectQueryTimeMicroseconds') AS has_select_query_time,
    mapContains(Settings, 'max_threads') AS has_max_threads_setting,
    Settings['max_threads'] AS max_threads_setting
FROM clusterAllReplicas(default, merge('system', '^query_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-10'
    AND query_start_time >= toDateTime('2026-08-08 06:50:00', 'UTC')
    AND query_start_time < toDateTime('2026-08-10 06:30:00', 'UTC')
    AND type = 'QueryFinish'
    AND is_initial_query = 1
    AND query_kind = 'Select'
    AND current_database = 'quotes_async_t4'
    AND normalized_query_hash IN (4268017077458496684, 13116456435110875434, 16441976537628221706, 16076059573740575684, 14900422428945064879, 4120463799277501782)
ORDER BY query_start_time_microseconds, benchmark_query
LIMIT 3000
SETTINGS skip_unavailable_shards = 1, max_execution_time = 30, max_rows_to_read = 10000000, timeout_before_checking_execution_speed = 0;
