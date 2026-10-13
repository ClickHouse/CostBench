SELECT toJSONString(groupArray(tuple(table, event_type, records, unique_parts, rows, bytes, read_bytes, first_record, last_record))) AS payload
FROM (
 SELECT table, toString(event_type) event_type, count() records, uniqExact(part_name) unique_parts,
 sum(rows) rows, sum(bytes_uncompressed) bytes, sum(read_bytes) read_bytes,
 toString(min(event_time)) first_record, toString(max(event_time)) last_record
 FROM clusterAllReplicas(default, merge('system','^part_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
 AND event_time >= toDateTime('2026-08-08 06:50:00','UTC')
 AND event_time < toDateTime('2026-08-09 20:00:00','UTC')
 AND database='quotes_async_t4'
 GROUP BY table,event_type ORDER BY table,event_type LIMIT 30
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;