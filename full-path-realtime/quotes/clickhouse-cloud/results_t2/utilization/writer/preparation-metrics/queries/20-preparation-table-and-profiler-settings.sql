SELECT toJSONString(tuple(
 (SELECT groupArray(tuple(name,toString(uuid))) FROM system.tables WHERE database='quotes_async_t4'),
 (SELECT groupArray(tuple(name,value,changed)) FROM system.settings WHERE name IN ('query_profiler_cpu_time_period_ns','query_profiler_real_time_period_ns','log_query_views','log_processors_profiles','max_threads'))
)) AS payload
LIMIT 1 SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=100000;
