SELECT name, type, comment FROM system.columns WHERE database='system' AND table='asynchronous_metrics' LIMIT 20
SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=100000 FORMAT CSVWithNames
