SELECT toJSONString(groupArray(tuple(table,uuid,records,rows))) AS payload FROM (
 SELECT table,toString(table_uuid) AS uuid,count() AS records,sum(rows) AS rows FROM clusterAllReplicas(default,merge('system','^part_log'))
 WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:50:00','UTC') AND event_time<toDateTime('2026-08-08 06:55:56','UTC') AND database='quotes_async_t4' AND table IN ('quotes','quotes_daily') AND event_type='NewPart' AND error=0
 GROUP BY table,table_uuid ORDER BY table,uuid LIMIT 10
 ) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;
