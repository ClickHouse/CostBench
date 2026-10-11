SELECT toString(query_start_time_microseconds) AS started_at_utc,toString(normalized_query_hash) AS query_hash,memory_usage AS peak_memory_bytes,query_duration_ms,read_rows,read_bytes
FROM clusterAllReplicas(default,merge('system','^query_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
AND query_start_time>=toDateTime('2026-08-08 06:54:00','UTC') AND query_start_time<toDateTime('2026-08-09 20:00:00','UTC')
AND type='QueryFinish' AND is_initial_query=1 AND query_kind='Select' AND has(databases,'quotes_async_t4')
AND normalized_query_hash IN (4268017077458496684,13116456435110875434,16441976537628221706,16076059573740575684,14900422428945064879,4120463799277501782)
ORDER BY query_start_time_microseconds LIMIT 1200 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=0 FORMAT CSVWithNames
