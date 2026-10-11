SELECT toJSONString(groupArray(tuple(t,max_parts,avg_parts,min_parts,samples))) AS payload FROM (
 SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 5 MINUTE)) t,
 max(value) max_parts,round(avg(value),4) avg_parts,min(value) min_parts,count() samples
 FROM clusterAllReplicas(default,merge('system','^asynchronous_metric_log'))
 WHERE metric='MaxPartCountForPartition' AND event_date BETWEEN '2026-08-08' AND '2026-08-09'
 AND event_time >= toDateTime('2026-08-08 06:55:56','UTC')
 AND event_time < toDateTime('2026-08-09 19:40:58','UTC')
 GROUP BY t ORDER BY t LIMIT 500
) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;