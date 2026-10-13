SELECT toJSONString(groupArray(tuple(name,records,queries,elapsed_us,input_wait_us,output_wait_us,input_rows,output_rows,example_query_id))) AS payload
FROM (
 SELECT name,count() AS records,uniqExact(query_id) AS queries,sum(elapsed_us) AS elapsed_us,
 sum(input_wait_elapsed_us) AS input_wait_us,sum(output_wait_elapsed_us) AS output_wait_us,
 sum(input_rows) AS input_rows,sum(output_rows) AS output_rows,any(query_id) AS example_query_id
 FROM clusterAllReplicas(default,merge('system','^processors_profile_log'))
 WHERE event_date='2026-08-08' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-08 07:05:56','UTC')
 GROUP BY name ORDER BY elapsed_us DESC LIMIT 100
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
