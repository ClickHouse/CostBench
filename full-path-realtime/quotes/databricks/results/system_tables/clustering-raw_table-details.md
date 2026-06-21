```sql
SELECT
    operation_type,
    start_time,
    end_time,
    operation_status,
    usage_unit,
    usage_quantity,
    operation_metrics
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = 'workspace'
  AND schema_name = 'benchmarking'
  AND table_name = 'quotes'
ORDER BY start_time DESC;

```

| operation_type | start_time | end_time | operation_status | usage_unit | usage_quantity | operation_metrics |
|---|---|---|---|---|---:|---|
| CLUSTERING | 2026-06-11T08:36:16.159+00:00 | 2026-06-11T08:51:19.067+00:00 | SUCCESSFUL | ESTIMATED_DBU | null | `{"number_of_removed_files":"4475","number_of_clustered_files":"796","amount_of_data_removed_bytes":"43045052379"...}` |
| CLUSTERING | 2026-06-11T06:36:10.404+00:00 | 2026-06-11T06:54:22.542+00:00 | SUCCESSFUL | ESTIMATED_DBU | 8.8025922933333300 | `{"number_of_removed_files":"6045","number_of_clustered_files":"1165","amount_of_data_removed_bytes":"5783578688"...}` |
| CLUSTERING | 2026-06-11T04:36:18.251+00:00 | 2026-06-11T04:53:49.594+00:00 | SUCCESSFUL | ESTIMATED_DBU | 7.8127879983333350 | `{"number_of_removed_files":"5791","number_of_clustered_files":"1113","amount_of_data_removed_bytes":"5515187552"...}` |
| CLUSTERING | 2026-06-11T02:36:31.197+00:00 | 2026-06-11T02:54:26.522+00:00 | SUCCESSFUL | ESTIMATED_DBU | 7.7109413766666700 | `{"number_of_removed_files":"5869","number_of_clustered_files":"1047","amount_of_data_removed_bytes":"5231746963"...}` |
| CLUSTERING | 2026-06-11T00:37:30.873+00:00 | 2026-06-11T00:54:51.187+00:00 | SUCCESSFUL | ESTIMATED_DBU | 7.6366948316666650 | `{"number_of_removed_files":"5791","number_of_clustered_files":"910","amount_of_data_removed_bytes":"47290652537"...}` |
| CLUSTERING | 2026-06-10T22:37:16.360+00:00 | 2026-06-10T22:54:47.357+00:00 | SUCCESSFUL | ESTIMATED_DBU | 7.4947335533333300 | `{"number_of_removed_files":"5983","number_of_clustered_files":"912","amount_of_data_removed_bytes":"47299798831"...}` |
| CLUSTERING | 2026-06-10T20:36:28.192+00:00 | 2026-06-10T20:52:16.861+00:00 | SUCCESSFUL | ESTIMATED_DBU | 7.7213949649999900 | `{"number_of_removed_files":"5628","number_of_clustered_files":"1065","amount_of_data_removed_bytes":"4960544820"...}` |
| CLUSTERING | 2026-06-10T18:36:35.346+00:00 | 2026-06-10T18:54:01.133+00:00 | SUCCESSFUL | ESTIMATED_DBU | 7.7460147816666800 | `{"number_of_removed_files":"5375","number_of_clustered_files":"952","amount_of_data_removed_bytes":"48527983519"...}` |
| CLUSTERING | 2026-06-10T16:36:28.088+00:00 | 2026-06-10T16:52:19.925+00:00 | SUCCESSFUL | ESTIMATED_DBU | 6.3775379516666700 | `{"number_of_removed_files":"5046","number_of_clustered_files":"855","amount_of_data_removed_bytes":"41525828181"...}` |
| CLUSTERING | 2026-06-10T14:37:39.353+00:00 | 2026-06-10T14:54:30.154+00:00 | SUCCESSFUL | ESTIMATED_DBU | 7.1323797550000000 | `{"number_of_removed_files":"5320","number_of_clustered_files":"858","amount_of_data_removed_bytes":"45224282017"...}` |
| CLUSTERING | 2026-06-10T12:36:17.799+00:00 | 2026-06-10T12:52:45.666+00:00 | SUCCESSFUL | ESTIMATED_DBU | 6.7289911866666700 | `{"number_of_removed_files":"5261","number_of_clustered_files":"991","amount_of_data_removed_bytes":"52454134873"...}` |
| CLUSTERING | 2026-06-10T10:36:43.948+00:00 | 2026-06-10T10:56:28.825+00:00 | SUCCESSFUL | ESTIMATED_DBU | 6.9715144350000050 | `{"number_of_removed_files":"4318","number_of_clustered_files":"1032","amount_of_data_removed_bytes":"5360044501"...}` |
| CLUSTERING | 2026-06-10T09:07:20.444+00:00 | 2026-06-10T09:27:52.252+00:00 | SUCCESSFUL | ESTIMATED_DBU | 8.3562369216666700 | `{"number_of_removed_files":"5034","number_of_clustered_files":"1562","amount_of_data_removed_bytes":"7833679511"...}` |
| CLUSTERING | 2026-06-10T07:36:45.529+00:00 | 2026-06-10T07:50:37.357+00:00 | SUCCESSFUL | ESTIMATED_DBU | 3.8683536672037350 | `{"number_of_removed_files":"2691","number_of_clustered_files":"535","amount_of_data_removed_bytes":"28822195939"...}` |
| CLUSTERING | 2026-06-10T06:36:42.238+00:00 | 2026-06-10T06:53:42.253+00:00 | SUCCESSFUL | ESTIMATED_DBU | 4.9791865533333300 | `{"number_of_removed_files":"3108","number_of_clustered_files":"798","amount_of_data_removed_bytes":"41641234608"...}` |
| CLUSTERING | 2026-06-10T05:36:20.873+00:00 | 2026-06-10T05:51:46.226+00:00 | SUCCESSFUL | ESTIMATED_DBU | 4.4955925000000000 | `{"number_of_removed_files":"3224","number_of_clustered_files":"791","amount_of_data_removed_bytes":"41894151639"...}` |
| CLUSTERING | 2026-06-10T04:36:53.477+00:00 | 2026-06-10T04:48:48.598+00:00 | SUCCESSFUL | ESTIMATED_DBU | 3.1171291883333300 | `{"number_of_removed_files":"2607","number_of_clustered_files":"381","amount_of_data_removed_bytes":"20837623813"...}` |
| CLUSTERING | 2026-06-10T03:38:10.697+00:00 | 2026-06-10T03:51:57.650+00:00 | SUCCESSFUL | ESTIMATED_DBU | 3.6333679133333300 | `{"number_of_removed_files":"2878","number_of_clustered_files":"389","amount_of_data_removed_bytes":"21322625050"...}` |
| CLUSTERING | 2026-06-10T02:36:57.544+00:00 | 2026-06-10T02:48:37.225+00:00 | SUCCESSFUL | ESTIMATED_DBU | 3.1004884916666700 | `{"number_of_removed_files":"2490","number_of_clustered_files":"335","amount_of_data_removed_bytes":"18122154070"...}` |
| CLUSTERING | 2026-06-10T01:37:02.591+00:00 | 2026-06-10T01:54:03.714+00:00 | SUCCESSFUL | ESTIMATED_DBU | 5.7019643633333400 | `{"number_of_removed_files":"5290","number_of_clustered_files":"582","amount_of_data_removed_bytes":"36841669289"...}` |
| CLUSTERING | 2026-06-09T23:36:08.524+00:00 | 2026-06-09T23:59:28.439+00:00 | SUCCESSFUL | ESTIMATED_DBU | 12.6365858316666600 | `{"number_of_removed_files":"7979","number_of_clustered_files":"2231","amount_of_data_removed_bytes":"1347665999"...}` |
| CLUSTERING | 2026-06-09T21:07:26.301+00:00 | 2026-06-09T21:22:04.385+00:00 | SUCCESSFUL | ESTIMATED_DBU | 3.9552933233333360 | `{"number_of_removed_files":"3797","number_of_clustered_files":"456","amount_of_data_removed_bytes":"25783514662"...}` |
| ANALYZE | 2026-06-09T21:02:16.864+00:00 | 2026-06-09T21:04:07.219+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.117050963917269250 | `{"amount_of_scanned_bytes":"19683458933","number_of_scanned_files":"2918","staleness_percentage_reduced":"100"}` |
| COMPACTION | 2026-06-09T19:47:17.650+00:00 | 2026-06-09T19:50:41.795+00:00 | FAILED: INTERNAL_ERROR | ESTIMATED_DBU | 0.036041775000000000 | null |
| ANALYZE | 2026-06-09T16:24:27.804+00:00 | 2026-06-09T16:26:45.458+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.517892699857925400 | `{"amount_of_scanned_bytes":"28963648562","number_of_scanned_files":"1975","staleness_percentage_reduced":"100"}` |
| COMPACTION | 2026-06-09T15:25:25.595+00:00 | 2026-06-09T15:30:42.621+00:00 | SUCCESSFUL | ESTIMATED_DBU | 1.0932814033333330 | `{"number_of_output_files":"293","number_of_compacted_files":"3128","amount_of_output_data_bytes":"18712016822"...}` |
| COMPACTION | 2026-06-09T14:16:01.091+00:00 | 2026-06-09T14:19:02.815+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.212778403286580600 | `{"number_of_output_files":"152","number_of_compacted_files":"1634","amount_of_output_data_bytes":"9782744387"...}` |
| ANALYZE | 2026-06-09T13:13:00.354+00:00 | 2026-06-09T13:13:53.696+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.355170711647935230 | `{"amount_of_scanned_bytes":"7561083443","number_of_scanned_files":"1266","staleness_percentage_reduced":"100"}` |
| ANALYZE | 2026-06-07T11:19:53.217+00:00 | 2026-06-07T11:22:15.484+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.568497605000000000 | `{"amount_of_scanned_bytes":"29224380764","number_of_scanned_files":"458","staleness_percentage_reduced":"10"}` |
| COMPACTION | 2026-06-05T15:54:58.451+00:00 | 2026-06-05T16:00:05.281+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.9149732083333330 | `{"number_of_output_files":"271","number_of_compacted_files":"2860","amount_of_output_data_bytes":"17195008477"...}` |
| COMPACTION | 2026-06-05T11:13:38.216+00:00 | 2026-06-05T11:17:41.897+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.7830155950000010 | `{"number_of_output_files":"187","number_of_compacted_files":"2037","amount_of_output_data_bytes":"12029372287"...}` |
| COMPACTION | 2026-06-04T19:08:05.973+00:00 | 2026-06-04T19:13:16.717+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.8392921316666660 | `{"number_of_output_files":"270","number_of_compacted_files":"2853","amount_of_output_data_bytes":"17147721257"...}` |
| COMPACTION | 2026-06-04T14:43:35.328+00:00 | 2026-06-04T14:49:54.353+00:00 | SUCCESSFUL | ESTIMATED_DBU | 1.2072124233333350 | `{"number_of_output_files":"319","number_of_compacted_files":"8853","amount_of_output_data_bytes":"20802884565"...}` |
| ANALYZE | 2026-06-04T10:55:48.481+00:00 | 2026-06-04T10:57:17.233+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.441252025000000000 | `{"amount_of_scanned_bytes":"8967591798","number_of_scanned_files":"6896","staleness_percentage_reduced":"100"}` |