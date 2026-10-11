SELECT toJSONString(groupArray(tuple(t,host,executions,cpu_us,os_cpu_us,elapsed_ms,read_rows,written_rows,sorting_us))) AS payload
FROM (
 SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 30 MINUTE)) AS t,hostname AS host,count() AS executions,
 sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS os_cpu_us,
 sum(query_duration_ms) AS elapsed_ms,sum(read_rows) AS read_rows,sum(written_rows) AS written_rows,sum(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds']) AS sorting_us
 FROM clusterAllReplicas(default,merge('system','^query_log'))
 WHERE event_date BETWEEN toDate('2026-08-08 12:55:56') AND toDate('2026-08-08 14:55:56')
 AND event_time>=toDateTime('2026-08-08 12:55:56','UTC') AND event_time<toDateTime('2026-08-08 14:55:56','UTC') AND type='QueryFinish' AND query_kind='AsyncInsertFlush'
 AND query_id GLOBAL IN (
 SELECT initial_query_id FROM clusterAllReplicas(default,merge('system','^query_views_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
 AND view_name='quotes_async_t4.quotes_daily_mv' AND status='QueryFinish'
 )
 GROUP BY t,host ORDER BY t,host LIMIT 1000
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;
