SELECT name,type,comment FROM system.columns WHERE database='system' AND table='query_log'
AND (positionCaseInsensitive(name,'memory')>0 OR name IN ('query_start_time_microseconds','event_time_microseconds','normalized_query_hash','databases','type','is_initial_query','query_kind','ProfileEvents'))
ORDER BY name LIMIT 50 SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=100000 FORMAT CSVWithNames
