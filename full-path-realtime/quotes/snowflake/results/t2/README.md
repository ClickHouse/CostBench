# Snowflake T2 accepted evidence

The accepted Snowflake T2 comparison is Run 14, captured on 2026-08-11. The
headline evidence consists of 209 dashboard iterations and 35 drill-down
iterations. Supporting dashboard variants are retained for path-level
analysis, but they do not replace the accepted headline dashboard.

## Metadata normalization

The four accepted JSONL files were normalized after capture so their
`machine` and `cluster_size` fields name the warehouse rate used by the cost
model. This is an evidence-label correction only: iteration counts, row
progress, timestamps, query results, compilation times, execution times, and
all other fields are byte-for-byte equivalent after removing those two
metadata fields.

The machine-readable
[`run14_metadata_corrections.json`](run14_metadata_corrections.json) records
the before/after values, record counts, and SHA-256 digests. The cost model
uses Interactive Small at 1.2 credits/hour for the accepted interactive
files, with the disclosed Gen2 Small fallback proxy for query jobs whose
elapsed time is strictly greater than five seconds.

## Generated evidence

- `../../costs/out/t2/run14/` contains the accepted normalized Snowflake cost
  summaries.
- `../../../clickhouse-cloud/results_t2/matched/snowflake_run14/` contains the
  row-progress-matched ClickHouse query windows and matching reports.
- `charts/run14/` contains the canonical reproducible Snowflake comparison
  charts and their CSV/JSON provenance sidecars.

Regenerate matching from `full-path-realtime/utils/_commands.txt`, costs from
`../../costs/_commands.txt`, and charts from
`../../visualizations/_commands.txt`.
