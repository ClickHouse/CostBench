SELECT toJSONString(groupArray(tuple(table,event_type,records,input_parts,rows,rows_read,first_event,last_event))) AS payload FROM (
 SELECT table,event_type,count() AS records,sum(length(merged_from)) AS input_parts,sum(rows) AS rows,sum(read_rows) AS rows_read,min(event_time) AS first_event,max(event_time) AS last_event
 FROM clusterAllReplicas(default,merge('system','^part_log'))
 WHERE event_date BETWEEN '2026-08-09' AND '2026-08-12' AND event_time>=toDateTime('2026-08-09 19:40:58','UTC') AND event_time<toDateTime('2026-08-12 00:00:00','UTC')
 AND table_uuid IN (toUUID('7d9eac64-dbb9-4c43-8275-b70fd2b7c3ee'),toUUID('51ef6752-cdc7-4d90-bd20-b295dfbd561e')) AND event_type IN ('NewPart','MergeParts') AND error=0
 GROUP BY table,event_type ORDER BY table,event_type LIMIT 10
 ) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;
