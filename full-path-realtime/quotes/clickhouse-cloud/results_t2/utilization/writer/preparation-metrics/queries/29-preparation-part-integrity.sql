SELECT toJSONString(groupArray(tuple(table,event_type,records,unique_parts,hosts))) AS payload FROM (SELECT table,event_type,count() AS records,uniqExact(tuple(table_uuid,part_name)) AS unique_parts,uniqExact(hostname) AS hosts FROM clusterAllReplicas(default,merge('system','^part_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
AND database='quotes_async_t4' AND table IN ('quotes','quotes_daily') AND event_type IN ('NewPart','MergeParts') AND error=0 GROUP BY table,event_type ORDER BY table,event_type LIMIT 10) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;
