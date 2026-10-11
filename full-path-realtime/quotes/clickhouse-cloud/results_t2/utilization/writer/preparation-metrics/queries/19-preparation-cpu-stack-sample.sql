SELECT toJSONString(groupArray(tuple(samples,hosts,thread_names,stack))) AS payload
FROM (
 SELECT count() AS samples,uniqExact(hostname) AS hosts,groupUniqArray(thread_name) AS thread_names,symbols AS stack
 FROM clusterAllReplicas(default,merge('system','^trace_log'))
 WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC') AND trace_type='CPU'
 AND query_id GLOBAL IN (
  SELECT initial_query_id FROM clusterAllReplicas(default,merge('system','^query_views_log'))
  WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC')
  AND view_name='quotes_async_t4.quotes_daily_mv' AND status='QueryFinish'
 )
 GROUP BY stack ORDER BY samples DESC LIMIT 10000
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
