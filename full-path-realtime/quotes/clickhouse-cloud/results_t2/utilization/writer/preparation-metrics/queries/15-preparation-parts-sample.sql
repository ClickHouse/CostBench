SELECT toJSONString(groupArray(tuple(table,toString(event_type),records,parts,rows,cpu_us,sort_us,merge_us,duration_ms,profile_keys))) AS payload
FROM (
 SELECT table,event_type,count() AS records,uniqExact(tuple(table_uuid,part_name)) AS parts,
 sum(rows) AS rows,sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,
 sum(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds']) AS sort_us,
 sum(ProfileEvents['MergeTreeDataWriterMergingBlocksMicroseconds']) AS merge_us,sum(duration_ms) AS duration_ms,
 arraySort(arrayDistinct(arrayFlatten(groupArray(mapKeys(ProfileEvents))))) AS profile_keys
 FROM clusterAllReplicas(default,merge('system','^part_log'))
 WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC')
 AND database='quotes_async_t4' AND error=0
 GROUP BY table,event_type ORDER BY table,event_type LIMIT 20
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
