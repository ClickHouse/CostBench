


WITH
    'quotes_async_01' AS db_name,
    'quotes' AS table_name,
    (
        SELECT uuid
        FROM system.tables
        WHERE (database = db_name) AND (name = table_name)
    ) AS table_id,
    (
        SELECT groupArray(event_time)
        FROM (
            SELECT event_time
            FROM clusterAllReplicas(default, system.part_log)
            WHERE table_uuid = table_id
            AND event_type = 'NewPart'
            ORDER BY event_time
        )
    ) AS times
SELECT avgArray(arrayMap((a, b) -> dateDiff('second', a, b),
    arraySlice(times, 1, length(times) - 1),
    arraySlice(times, 2)))  AS avg_seconds_between_parts