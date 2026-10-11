SELECT toJSONString(groupArray(tuple(table, name, type, comment))) AS payload
FROM system.columns
WHERE database = 'system' AND table IN ('metric_log','part_log','asynchronous_insert_log','query_views_log','query_log','asynchronous_metric_log')
AND (table != 'metric_log' OR match(name,'^(hostname|event_|ProfileEvent_(Inserted|AsyncInsert|Merge|OSCPU|UserTime|SystemTime|Sorting|DelayedInsert|RejectedInsert))'))
LIMIT 1 SETTINGS max_threads=2, max_execution_time=15, timeout_before_checking_execution_speed=0, max_rows_to_read=100000;