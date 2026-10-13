SELECT toJSONString(groupArray(tuple(hostname, first_sample, last_sample, samples, inserted_rows, inserted_bytes, async_rows, async_bytes, cpu_us, user_cpu_us, system_cpu_us, merge_bytes, delayed_inserts, rejected_inserts))) AS payload
FROM (SELECT hostname, toString(min(event_time)) first_sample, toString(max(event_time)) last_sample, count() samples,
sum(ProfileEvent_InsertedRows) inserted_rows, sum(ProfileEvent_InsertedBytes) inserted_bytes,
sum(ProfileEvent_AsyncInsertRows) async_rows, sum(ProfileEvent_AsyncInsertBytes) async_bytes,
sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) cpu_us, sum(ProfileEvent_UserTimeMicroseconds) user_cpu_us, sum(ProfileEvent_SystemTimeMicroseconds) system_cpu_us,
sum(ProfileEvent_MergedUncompressedBytes) merge_bytes, sum(ProfileEvent_DelayedInserts) delayed_inserts, sum(ProfileEvent_RejectedInserts) rejected_inserts
FROM clusterAllReplicas(default, merge('system','^metric_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time >= toDateTime('2026-08-08 06:50:00','UTC') AND event_time < toDateTime('2026-08-09 20:00:00','UTC')
GROUP BY hostname ORDER BY hostname LIMIT 4)
LIMIT 1 SETTINGS max_threads=2, max_execution_time=30, timeout_before_checking_execution_speed=0, max_rows_to_read=1000000, skip_unavailable_shards=1;