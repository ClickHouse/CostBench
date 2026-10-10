SELECT name, type, comment
FROM system.columns
WHERE database = 'system'
    AND table = 'query_log'
    AND name IN ('event_date', 'event_time', 'query_start_time',
                'query_start_time_microseconds', 'query_duration_ms',
                'read_rows', 'read_bytes', 'result_rows', 'query',
                'normalized_query_hash', 'is_initial_query', 'current_database',
                'ProfileEvents', 'query_id', 'initial_query_id', 'hostname',
                'type', 'query_kind', 'tables')
ORDER BY position
LIMIT 30
SETTINGS max_execution_time = 10, max_rows_to_read = 100000;
