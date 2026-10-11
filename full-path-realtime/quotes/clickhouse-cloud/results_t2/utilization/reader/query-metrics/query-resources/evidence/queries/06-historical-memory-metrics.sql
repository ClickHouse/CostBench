SELECT metric,min(value) AS minimum,max(value) AS maximum FROM clusterAllReplicas(default,merge('system','^asynchronous_metric_log'))
WHERE event_date='2026-08-09' AND event_time>=toDateTime('2026-08-09 19:40:00','UTC') AND event_time<toDateTime('2026-08-09 19:40:01','UTC') AND positionCaseInsensitive(metric,'memory')>0
GROUP BY metric ORDER BY metric LIMIT 100 SETTINGS max_threads=2,max_execution_time=20,timeout_before_checking_execution_speed=0,max_rows_to_read=100000,skip_unavailable_shards=0 FORMAT CSVWithNames
