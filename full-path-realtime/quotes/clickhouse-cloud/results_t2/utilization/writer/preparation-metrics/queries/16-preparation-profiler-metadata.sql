SELECT toJSONString(tuple(
 (SELECT groupArray(tuple(name,total_rows,sorting_key,partition_key)) FROM system.tables WHERE database='system' AND match(name,'^trace_log')),
 (SELECT groupArray(tuple(table,name,type,comment)) FROM system.columns WHERE database='system' AND match(table,'^trace_log'))
)) AS payload
LIMIT 1 SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=100000;
