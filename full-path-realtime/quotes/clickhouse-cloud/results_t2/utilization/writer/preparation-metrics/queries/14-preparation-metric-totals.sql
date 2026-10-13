SELECT toJSONString(tuple(count(),uniqExact(hostName()),min(event_time),max(event_time),
 sum(ProfileEvent_MergeTreeDataWriterSortingBlocksMicroseconds),sum(ProfileEvent_MergeTreeDataWriterMergingBlocksMicroseconds),
 sum(ProfileEvent_MergeTreeDataWriterRows),sum(ProfileEvent_MergeTreeDataWriterBlocks),sum(ProfileEvent_MergeTreeDataWriterBlocksAlreadySorted),
 sum(ProfileEvent_OSCPUVirtualTimeMicroseconds),sum(ProfileEvent_AggregatingSortedMilliseconds),
 sum(ProfileEvent_ExternalSortCompressedBytes),sum(ProfileEvent_ExternalAggregationCompressedBytes))) AS payload
FROM clusterAllReplicas(default,merge('system','^metric_log'))
WHERE event_date BETWEEN '2026-08-08' AND '2026-08-09'
AND event_time>=toDateTime('2026-08-08 06:55:56','UTC') AND event_time<toDateTime('2026-08-09 19:40:58','UTC')
LIMIT 1 SETTINGS max_threads=2,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000,skip_unavailable_shards=1;
