SELECT toJSONString(groupArray(tuple(table,name,type,comment))) AS payload
FROM system.columns
WHERE database='system'
AND (match(table,'^(query_views_log|processors_profile_log|query_thread_log|asynchronous_insert_log|query_log|part_log)') OR table='metric_log')
AND (table!='metric_log' OR match(name,'^ProfileEvent_.*(Sort|Aggregat|Materialized|Insert|Merge|Compress|CPU)'))
LIMIT 1
SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=100000;
