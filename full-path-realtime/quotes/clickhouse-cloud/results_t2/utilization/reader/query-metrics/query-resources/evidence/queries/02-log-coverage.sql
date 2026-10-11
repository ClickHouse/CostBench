SELECT 'schema' AS kind, name AS detail, sorting_key AS extra, 0 AS samples, '' AS first, '' AS last FROM system.tables WHERE database='system' AND (match(name,'^metric_log') OR match(name,'^asynchronous_metric_log'))
UNION ALL
SELECT 'coverage',hostname,'',count(),toString(min(event_time)),toString(max(event_time)) FROM clusterAllReplicas(default,merge('system','^metric_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC') GROUP BY hostname
LIMIT 100 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=0 FORMAT CSVWithNames
