SELECT hostname, flush_time, count()
FROM clusterAllReplicas(default, system.asynchronous_insert_log)
WHERE database = 'hits_100b_test15' AND table = 'hits'
GROUP BY hostname, flush_time
ORDER BY hostname, flush_time DESC