WITH
 '7d9eac64-dbb9-4c43-8275-b70fd2b7c3ee::' AS raw_merge_prefix,
 '51ef6752-cdc7-4d90-bd20-b295dfbd561e::' AS agg_merge_prefix
SELECT toJSONString(groupArray(tuple(t,scope,stage,hostname,samples,symbolized))) AS payload
FROM (
 SELECT toUnixTimestamp(toStartOfInterval(event_time,INTERVAL 30 MINUTE)) AS t,
 if(startsWith(query_id,raw_merge_prefix) OR startsWith(query_id,agg_merge_prefix),'Background merges','Ingestion') AS scope,
 multiIf(scope='Background merges','Background merges',
 arrayExists(s->position(s,'getBlockSortPermutation')>0 OR position(s,'stableGetPermutation')>0 OR position(s,'updatePermutation')>0 OR position(s,'getPermutation')>0 OR position(s,'isAlreadySorted')>0,symbols),'Sorting',
 arrayExists(s->(position(s,'CompressionCodec')>0 AND (position(s,'compress')>0 OR position(s,'doCompress')>0)) OR (position(s,'LZ4')>0 AND position(s,'compress')>0) OR (position(s,'ZSTD')>0 AND position(s,'compress')>0),symbols),'Compression',
 arrayExists(s->position(s,'CSVFormatReader')>0 OR position(s,'IRowInputFormat')>0 OR position(s,'StreamingFormatExecutor')>0,symbols),'Parsing',
 arrayExists(s->position(s,'Aggregator::')>0 OR position(s,'AggregatingTransform')>0 OR position(s,'ConvertingAggregated')>0 OR position(s,'MergingAggregated')>0 OR position(s,'AggregateFunction')>0,symbols)
 OR (arrayExists(s->position(s,'ExecutingInnerQueryFromView')>0,symbols) AND NOT arrayExists(s->position(s,'MergeTreeDataWriter')>0 OR position(s,'MergeTreeDataPartWriter')>0 OR position(s,'SharedMergeTreeSink')>0,symbols)),'Pre-aggregation',
 'Other ingestion') AS stage,
 hostname,count() AS samples,countIf(length(symbols)>0) AS symbolized
 FROM clusterAllReplicas(default,merge('system','^trace_log'))
 WHERE event_date BETWEEN toDate('2026-08-08 18:55:56') AND toDate('2026-08-08 22:55:56')
 AND event_time>=toDateTime('2026-08-08 18:55:56','UTC') AND event_time<toDateTime('2026-08-08 22:55:56','UTC') AND trace_type='CPU'
 AND (startsWith(query_id,raw_merge_prefix) OR startsWith(query_id,agg_merge_prefix) OR query_id GLOBAL IN (
 SELECT initial_query_id FROM clusterAllReplicas(default,merge('system','^query_views_log'))
 WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09' AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
 AND view_name='quotes_async_t4.quotes_daily_mv' AND status='QueryFinish'
 ))
 GROUP BY t,scope,stage,hostname ORDER BY t,scope,stage,hostname LIMIT 1000
)
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=5000000,skip_unavailable_shards=1;
