SELECT toJSONString(groupArray(tuple(table,parts,rows,uncompressed_bytes,compressed_bytes,on_disk_bytes,last_modified))) AS payload FROM (
 SELECT table,count() AS parts,sum(rows) AS rows,sum(uncompressed_bytes) AS uncompressed_bytes,sum(compressed_bytes) AS compressed_bytes,sum(on_disk_bytes) AS on_disk_bytes,max(last_modified) AS last_modified FROM (
 SELECT table,name,max(rows) AS rows,max(data_uncompressed_bytes) AS uncompressed_bytes,max(data_compressed_bytes) AS compressed_bytes,max(bytes_on_disk) AS on_disk_bytes,max(modification_time) AS last_modified
 FROM clusterAllReplicas(default,system.parts) WHERE database='quotes_async_t4' AND table IN ('quotes','quotes_daily') AND active GROUP BY table,name LIMIT 10000
 ) GROUP BY table ORDER BY table LIMIT 10
 ) LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;
