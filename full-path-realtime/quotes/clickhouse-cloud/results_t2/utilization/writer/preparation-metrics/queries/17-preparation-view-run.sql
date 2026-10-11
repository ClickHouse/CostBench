SELECT toJSONString(groupArray(tuple(view_name,view_target,toString(status),executions,hosts,read_rows,written_rows,view_ms,cpu_us,os_cpu_us,sort_us,with_cpu,query_ids,example_query,profile_keys))) AS payload
FROM (
 SELECT view_name,view_target,status,count() AS executions,uniqExact(hostname) AS hosts,
 sum(read_rows) AS read_rows,sum(written_rows) AS written_rows,sum(view_duration_ms) AS view_ms,
 sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,
 sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS os_cpu_us,
 sum(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds']) AS sort_us,
 countIf(mapContains(ProfileEvents,'UserTimeMicroseconds')) AS with_cpu,
 groupUniqArray(5)(initial_query_id) AS query_ids,any(view_query) AS example_query,
 arraySort(arrayDistinct(arrayFlatten(groupArray(mapKeys(ProfileEvents))))) AS profile_keys
 FROM clusterAllReplicas(default,merge('system','^query_views_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
 AND event_time>=toDateTime('2026-08-08 06:55:56','UTC')
 AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
 GROUP BY view_name,view_target,status
 ORDER BY executions DESC LIMIT 20
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=2000000,skip_unavailable_shards=1;
