SELECT hostName() AS hostname,metric,count() AS samples,min(value) AS min_value,max(value) AS max_value
FROM clusterAllReplicas(default,merge('system','^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate('2026-10-11') AND toDate('2026-10-11')
AND event_time BETWEEN toDateTime('2026-10-11 10:13:31','UTC') AND toDateTime('2026-10-11 10:20:34','UTC')
AND metric IN ('CGroupMaxCPU','CGroupMemoryTotal') GROUP BY hostname,metric ORDER BY hostname,metric LIMIT 100 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000000,max_bytes_to_read=100000000000,skip_unavailable_shards=0 FORMAT CSVWithNames
