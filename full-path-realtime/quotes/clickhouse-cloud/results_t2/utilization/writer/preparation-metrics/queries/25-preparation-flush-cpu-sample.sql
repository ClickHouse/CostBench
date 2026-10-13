SELECT toJSONString(tuple(count(),uniqExact(hostname),sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']),sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']),sum(ProfileEvents['RealTimeMicroseconds']),sum(query_duration_ms),sum(written_rows),sum(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds']),quantilesExact(0.5,0.95,0.99)(query_duration_ms))) AS payload
FROM clusterAllReplicas(default,merge('system','^query_log'))
WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC')
AND type='QueryFinish' AND query_kind='AsyncInsertFlush'
AND query_id GLOBAL IN (SELECT initial_query_id FROM clusterAllReplicas(default,merge('system','^query_views_log')) WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC') AND view_name='quotes_async_t4.quotes_daily_mv' AND status='QueryFinish')
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
