WITH events AS (SELECT event_time,event_type,table_uuid,table,part_name,rows,bytes_uncompressed,size_in_bytes,merged_from FROM clusterAllReplicas(default,merge('system','^part_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
 AND table_uuid IN (toUUID('7d9eac64-dbb9-4c43-8275-b70fd2b7c3ee'),toUUID('51ef6752-cdc7-4d90-bd20-b295dfbd561e')) AND event_type IN ('NewPart','MergeParts') AND error=0) SELECT toJSONString(groupArray(tuple(t,table,merges,input_rows,output_rows,missing_input_parts))) AS payload FROM (
 SELECT t,table,count() AS merges,sum(input_rows) AS input_rows,sum(output_rows) AS output_rows,sum(missing_inputs) AS missing_input_parts FROM (
 SELECT m.t,m.table_uuid,m.table,m.part_name,m.rows AS output_rows,sum(c.rows) AS input_rows,countIf(c.found=0) AS missing_inputs
 FROM (SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 5 MINUTE)) AS t,table_uuid,table,part_name,rows,arrayJoin(merged_from) AS input_part FROM events WHERE event_type='MergeParts') AS m
 ANY LEFT JOIN (SELECT table_uuid,part_name,rows,toUInt8(1) AS found FROM events) AS c ON m.table_uuid=c.table_uuid AND m.input_part=c.part_name
 GROUP BY m.t,m.table_uuid,m.table,m.part_name,m.rows
 ) GROUP BY t,table ORDER BY t,table LIMIT 1000
 ) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,max_memory_usage=1500000000,skip_unavailable_shards=1;
