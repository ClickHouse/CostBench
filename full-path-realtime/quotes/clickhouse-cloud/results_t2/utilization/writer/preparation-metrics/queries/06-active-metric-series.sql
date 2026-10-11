SELECT toJSONString(groupArray(tuple(t,samples,async_rows,async_bytes,inserted_rows,inserted_bytes,cpu_us,user_cpu_us,system_cpu_us,merge_rows,merge_bytes,delayed,rejected))) AS payload
FROM (
 SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 5 MINUTE)) t,count() samples,
 sum(ProfileEvent_AsyncInsertRows) async_rows,sum(ProfileEvent_AsyncInsertBytes) async_bytes,
 sum(ProfileEvent_InsertedRows) inserted_rows,sum(ProfileEvent_InsertedBytes) inserted_bytes,
 sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) cpu_us,sum(ProfileEvent_UserTimeMicroseconds) user_cpu_us,sum(ProfileEvent_SystemTimeMicroseconds) system_cpu_us,
 sum(ProfileEvent_MergedRows) merge_rows,sum(ProfileEvent_MergedUncompressedBytes) merge_bytes,
 sum(ProfileEvent_DelayedInserts) delayed,sum(ProfileEvent_RejectedInserts) rejected
 FROM clusterAllReplicas(default,merge('system','^metric_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
 AND event_time >= toDateTime('2026-08-08 06:55:56','UTC')
 AND event_time < toDateTime('2026-08-09 19:40:58','UTC')
 GROUP BY t ORDER BY t LIMIT 500
) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;