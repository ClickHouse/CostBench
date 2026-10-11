SELECT table, name, type, comment
FROM system.columns
WHERE database = 'system' AND (match(table, '^metric_log') OR match(table, '^asynchronous_metric_log'))
AND (name IN ('hostname', 'event_time', 'event_date', 'metric', 'value', 'CurrentMetric_MemoryTracking') OR match(name, '^ProfileEvent_(OSCPUVirtualTimeMicroseconds|UserTimeMicroseconds|SystemTimeMicroseconds)$'))
ORDER BY table, name LIMIT 100
SETTINGS max_threads=2, max_execution_time=15, timeout_before_checking_execution_speed=0, max_rows_to_read=100000
FORMAT CSVWithNames
