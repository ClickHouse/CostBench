select *
from clusterAllReplicas(default, system.query_log)
WHERE has(tables, 'quotes_async_01.quotes')
AND type = 'QueryFinish'
-- AND type = 'QueryStart'
AND query_kind = 'AsyncInsertFlush'
-- AND query_kind = 'Insert'
ORDER BY event_time DESC
-- limit 10;







