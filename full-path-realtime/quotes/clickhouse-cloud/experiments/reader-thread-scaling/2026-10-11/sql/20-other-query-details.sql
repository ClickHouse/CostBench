SELECT query_kind,normalized_query_hash,count() AS queries,any(tables) AS tables,
substring(any(normalizeQuery(query)),1,350) AS normalized_query,
sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1e6 AS total_cpu_s
FROM clusterAllReplicas(default,merge('system','^query_log'))
WHERE event_date='2026-10-11' AND query_start_time_microseconds<toDateTime64('2026-10-11 09:49:39.578368',6,'UTC')
AND event_time_microseconds>=toDateTime64('2026-10-11 09:39:43.143738',6,'UTC') AND is_initial_query=1
AND query_id NOT LIKE 'reader-scaling-%' AND type='QueryFinish'
GROUP BY query_kind,normalized_query_hash ORDER BY total_cpu_s DESC LIMIT 20 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000000,max_bytes_to_read=100000000000,skip_unavailable_shards=0 FORMAT CSVWithNames
