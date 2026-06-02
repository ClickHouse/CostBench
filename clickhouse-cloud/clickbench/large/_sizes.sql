SELECT
    database,
    table,
    formatReadableQuantity(sum(rows))                 AS rows,
    formatReadableQuantity(count())                   AS parts,
    formatReadableSize(sum(data_uncompressed_bytes))  AS data_size_uncompressed,
    formatReadableSize(sum(data_compressed_bytes))    AS data_size_compressed,
    formatReadableSize(sum(bytes_on_disk))            AS total_size_on_disk,
    formatReadableSize(avg(bytes_on_disk))            AS part_size_on_disk_avg,
    formatReadableSize(min(bytes_on_disk))            AS part_size_on_disk_min,
    formatReadableSize(max(bytes_on_disk))            AS part_size_on_disk_max
FROM system.parts
WHERE active and (`table` = 'hits')
GROUP BY database, table
ORDER BY  database, table ASC