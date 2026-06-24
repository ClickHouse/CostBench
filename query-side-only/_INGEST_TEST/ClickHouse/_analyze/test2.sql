WITH
    'hits_100b_test10' AS db_name,
    'hits' AS table_name,
    (
        SELECT uuid
        FROM system.tables
        WHERE (database = db_name) AND (name = table_name)
    ) AS table_id
SELECT
   hostname,
    event_time,
   duration_ms,
   part_type,
   formatReadableQuantity(rows) as rows,
   formatReadableSize(bytes_uncompressed) as bytes_uncompressed,
   formatReadableSize(size_in_bytes) as bytes_compressed

FROM clusterAllReplicas(default, system.part_log)
WHERE table_uuid = table_id
AND event_type = 'NewPart'
ORDER BY event_time DESC, hostname DESC
LIMIT 100
