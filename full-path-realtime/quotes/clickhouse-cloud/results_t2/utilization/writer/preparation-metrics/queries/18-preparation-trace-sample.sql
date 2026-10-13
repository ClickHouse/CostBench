SELECT toJSONString(groupArray(tuple(toString(trace_type),thread_name,records,hosts,symbolized,queries,versions,example_query_id,example_symbols))) AS payload
FROM (
 SELECT trace_type,thread_name,count() AS records,uniqExact(hostname) AS hosts,countIf(length(symbols)>0) AS symbolized,
 uniqExact(query_id) AS queries,groupUniqArray(5)(revision) AS versions,any(query_id) AS example_query_id,
 anyIf(symbols,trace_type='CPU') AS example_symbols
 FROM clusterAllReplicas(default,merge('system','^trace_log'))
 WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC')
 GROUP BY trace_type,thread_name ORDER BY records DESC LIMIT 100
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
