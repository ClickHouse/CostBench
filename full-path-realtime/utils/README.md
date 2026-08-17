# Full-path benchmark utilities

## Match query observations by ingest progress

`match_progress.py` selects observations from a candidate run that best match
the normalized ingest progress of a reference run. Matching is monotonic,
without replacement, and minimizes total absolute progress difference.

The default is the complete active-ingestion window. For each input, the tool:

1. infers `final_rows` as the maximum observed `raw_rows` unless supplied;
2. includes records from the beginning through the first observation at that
   final row count;
3. includes that first final-row observation as the active endpoint;
4. excludes every later repeated-final-row observation;
5. uses all reference active observations and selects the same number from the
   candidate.

Therefore, do **not** pass `--count` for a complete active-ingestion cost
comparison. Use `--count` only when deliberately comparing a shorter prefix.

The adjacent `.match.json` report records the automatically derived active
counts and endpoint iterations, every selected pair, and the mean, median, and
maximum progress gap.

Example:

```bash
python3 utils/match_progress.py \
  --reference quotes/bigquery/results/bq-full-t2-20260810_152224/mv/dashboard_20260810T152439Z.jsonl \
  --candidate quotes/clickhouse-cloud/results_t2/mv/dashboard_20260808T065559Z.jsonl \
  --output quotes/clickhouse-cloud/results_t2/matched/bigquery/dashboard_active_matched_to_bigquery.jsonl
```

Read the derived count for downstream cost summarizers with:

```bash
jq -r '.matched_observations' \
  quotes/clickhouse-cloud/results_t2/matched/bigquery/dashboard_active_matched_to_bigquery.jsonl.match.json
```

Run all current Snowflake and BigQuery dashboard/drill-down matches with:

```bash
bash utils/_commands.txt
```

The command file does not pass `--count`; it prints each automatically derived
active-ingestion count and its match-quality statistics after creating the four
matched ClickHouse files. It also creates an adjacent empty marker whose name
contains the derived count, for example
`dashboard_active_matched_to_bigquery_iterations_190.txt`.
