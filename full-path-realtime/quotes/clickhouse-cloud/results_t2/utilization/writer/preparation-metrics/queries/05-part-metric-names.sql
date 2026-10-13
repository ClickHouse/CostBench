SELECT toJSONString(groupArray(tuple(metric,value))) AS payload FROM (
 SELECT metric,max(value) value FROM clusterAllReplicas(default,system.asynchronous_metrics)
 WHERE metric ILIKE '%Part%' GROUP BY metric ORDER BY metric LIMIT 100
) LIMIT 1 SETTINGS max_threads=2,max_execution_time=15,timeout_before_checking_execution_speed=0,max_rows_to_read=10000,skip_unavailable_shards=1;