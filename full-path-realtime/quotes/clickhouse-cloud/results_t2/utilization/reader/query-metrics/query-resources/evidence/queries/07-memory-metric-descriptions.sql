SELECT metric,value,description FROM system.asynchronous_metrics WHERE positionCaseInsensitive(metric,'memory')>0
ORDER BY metric LIMIT 100 SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=100000 FORMAT CSVWithNames
