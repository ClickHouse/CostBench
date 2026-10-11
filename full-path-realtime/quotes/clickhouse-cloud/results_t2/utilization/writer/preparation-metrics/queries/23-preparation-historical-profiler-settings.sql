SELECT toJSONString(groupArray(tuple(toString(type),query_kind,records,cpu_periods,real_periods,processor_settings,example_query))) AS payload
FROM (
 SELECT type,query_kind,count() AS records,groupUniqArray(Settings['query_profiler_cpu_time_period_ns']) AS cpu_periods,groupUniqArray(Settings['query_profiler_real_time_period_ns']) AS real_periods,
 groupUniqArray(Settings['log_processors_profiles']) AS processor_settings,any(query) AS example_query
 FROM clusterAllReplicas(default,merge('system','^query_log'))
 WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC')
 AND query_id GLOBAL IN (SELECT initial_query_id FROM clusterAllReplicas(default,merge('system','^query_views_log')) WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC') AND view_name='quotes_async_t4.quotes_daily_mv' AND status='QueryFinish')
 GROUP BY type,query_kind ORDER BY type LIMIT 10
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
