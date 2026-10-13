SELECT toJSONString(groupArray(tuple(source,records,hosts,earliest,latest))) AS payload
FROM (
 SELECT 'query_views_log' source,count() records,uniqExact(hostname) hosts,min(event_time) earliest,max(event_time) latest
 FROM clusterAllReplicas(default,merge('system','^query_views_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time >= toDateTime('2026-08-08 06:55:56','UTC') AND event_time < toDateTime('2026-08-09 19:40:58','UTC')
 UNION ALL
 SELECT 'processors_profile_log',count(),uniqExact(hostname),min(event_time),max(event_time)
 FROM clusterAllReplicas(default,merge('system','^processors_profile_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time >= toDateTime('2026-08-08 06:55:56','UTC') AND event_time < toDateTime('2026-08-09 19:40:58','UTC')
 UNION ALL
 SELECT 'query_log',count(),uniqExact(hostname),min(event_time),max(event_time)
 FROM clusterAllReplicas(default,merge('system','^query_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time >= toDateTime('2026-08-08 06:55:56','UTC') AND event_time < toDateTime('2026-08-09 19:40:58','UTC')
 UNION ALL
 SELECT 'query_thread_log',count(),uniqExact(hostname),min(event_time),max(event_time)
 FROM clusterAllReplicas(default,merge('system','^query_thread_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time >= toDateTime('2026-08-08 06:55:56','UTC') AND event_time < toDateTime('2026-08-09 19:40:58','UTC')
) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=10000000,skip_unavailable_shards=1;
