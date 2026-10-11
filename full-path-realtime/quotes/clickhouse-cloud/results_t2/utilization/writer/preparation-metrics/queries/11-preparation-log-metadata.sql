SELECT toJSONString(groupArray(tuple(name,total_rows,sorting_key,partition_key))) AS payload
FROM system.tables
WHERE database='system' AND match(name,'^(query_views_log|processors_profile_log|query_thread_log|query_log|part_log|metric_log)')
LIMIT 1 SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=100000;
