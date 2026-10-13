SELECT toJSONString(groupArray(tuple(t,executions,rows_in,rows_out,view_ms,cpu_us,sort_us,merge_us,real_us,peak_memory))) AS payload
FROM (
 SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 5 MINUTE)) AS t,count() AS executions,sum(read_rows) AS rows_in,sum(written_rows) AS rows_out,
 sum(view_duration_ms) AS view_ms,sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,
 sum(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds']) AS sort_us,
 sum(ProfileEvents['MergeTreeDataWriterMergingBlocksMicroseconds']) AS merge_us,sum(ProfileEvents['RealTimeMicroseconds']) AS real_us,max(peak_memory_usage) AS peak_memory
 FROM clusterAllReplicas(default,merge('system','^query_views_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
 AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
 AND view_name='quotes_async_t4.quotes_daily_mv' AND status='QueryFinish'
 GROUP BY t ORDER BY t LIMIT 1000
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
