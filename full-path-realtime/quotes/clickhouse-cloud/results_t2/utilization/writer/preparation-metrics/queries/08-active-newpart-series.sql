SELECT toJSONString(groupArray(tuple(t,table,parts,rows,bytes,compressed_bytes))) AS payload
FROM (
 SELECT toUnixTimestamp(toStartOfInterval(ts,INTERVAL 5 MINUTE)) t,table,count() parts,sum(part_rows) rows,sum(part_bytes) bytes,sum(compressed) compressed_bytes
 FROM (
  SELECT table,table_uuid,part_name,min(event_time) ts,max(rows) part_rows,max(bytes_uncompressed) part_bytes,max(size_in_bytes) compressed
  FROM clusterAllReplicas(default,merge('system','^part_log'))
  WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
  AND event_time >= toDateTime('2026-08-08 06:55:56','UTC')
  AND event_time < toDateTime('2026-08-09 19:40:58','UTC')
  AND database='quotes_async_t4' AND table IN ('quotes','quotes_daily') AND event_type='NewPart' AND error=0
  GROUP BY table,table_uuid,part_name
 ) GROUP BY t,table ORDER BY t,table LIMIT 1000
) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;