SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 5 MINUTE)) AS t,hostname,count() AS seconds,sum(second_samples) AS samples,sum(cpu_us) AS cpu_us,sum(memory) AS memory_byte_seconds,min(memory) AS min_memory,max(memory) AS max_memory,max(second_samples) AS max_samples_per_second,toString(min(event_time)) AS first,toString(max(event_time)) AS last FROM (
SELECT event_time,hostname,count() AS second_samples,sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS cpu_us,avg(CurrentMetric_MemoryTracking) AS memory
FROM clusterAllReplicas(default,merge('system','^metric_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
GROUP BY event_time,hostname)
GROUP BY t,hostname ORDER BY t,hostname LIMIT 1000 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=0 FORMAT CSVWithNames
