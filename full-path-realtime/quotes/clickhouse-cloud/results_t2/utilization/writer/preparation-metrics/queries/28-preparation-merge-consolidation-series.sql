SELECT toJSONString(groupArray(tuple(t,table,merges,input_parts,rows,read_rows,read_bytes,output_bytes,output_uncompressed_bytes,cpu_us,duration_ms,peak_memory))) AS payload
FROM (
 SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 5 MINUTE)) AS t,table,count() AS merges,sum(length(merged_from)) AS input_parts,sum(rows) AS rows,sum(read_rows) AS read_rows,sum(read_bytes) AS read_bytes,
 sum(size_in_bytes) AS output_bytes,sum(bytes_uncompressed) AS output_uncompressed_bytes,sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,sum(duration_ms) AS duration_ms,max(peak_memory_usage) AS peak_memory
 FROM clusterAllReplicas(default,merge('system','^part_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
 AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
 AND database='quotes_async_t4' AND table IN ('quotes','quotes_daily') AND event_type='MergeParts' AND error=0
 GROUP BY t,table ORDER BY t,table LIMIT 1000
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;
