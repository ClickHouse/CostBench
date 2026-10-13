SELECT query_kind,normalized_query_hash,count() AS queries,
sum(query_duration_ms) AS total_elapsed_ms,max(query_duration_ms) AS max_elapsed_ms,
sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1e6 AS total_cpu_s,
sum(read_bytes) AS total_read_bytes,max(memory_usage) AS max_peak_memory_bytes
FROM clusterAllReplicas(default,merge('system','^query_log'))
WHERE event_date BETWEEN toDate('2026-10-11') AND toDate('2026-10-11') AND query_start_time_microseconds<toDateTime64('2026-10-11 09:49:39.578368',6,'UTC') AND event_time_microseconds>=toDateTime64('2026-10-11 09:39:43.143738',6,'UTC') AND is_initial_query=1 AND query_id NOT LIKE 'reader-scaling-%'
AND type IN ('QueryFinish','ExceptionWhileProcessing','ExceptionBeforeStart')
GROUP BY query_kind,normalized_query_hash ORDER BY total_cpu_s DESC LIMIT 1000 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000000,max_bytes_to_read=100000000000,skip_unavailable_shards=0 FORMAT CSVWithNames
