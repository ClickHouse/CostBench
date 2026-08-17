# Snowflake T2 comparison visualizations

These generators are the Snowflake counterparts to the current BigQuery T2
visualizations. They deliberately do not reuse the deprecated Snowflake chart
generators or overwrite their outputs.

The commands in `_commands.txt` write publication artifacts to
`results/t2/charts/run14`. Every chart is rendered as PNG and SVG with a JSON
provenance summary. The lag and latency charts also emit the exact plotted data
as CSV.

## Evidence rules

- Latency charts use the original fixed-rate runner JSONL files and plot each
  system at its own observed base-table row count through 100B rows. They do not
  match records by iteration.
- The aggregate-query chart applies the deprecated renderer's Snowflake-only,
  per-query Tukey upper-fence rule (`Q3 + 1.5 * IQR`). Excluded observations
  remain explicitly marked in the chart CSV and are listed in the provenance
  summary. Its displayed line is the centered seven-observation rolling median,
  matching the deprecated renderer's trend treatment. The drill-down chart does
  not exclude outliers; it uses the deprecated renderer's centered
  five-observation rolling median for display. Raw measured latencies remain
  unchanged in the evidence CSV.
- Both outlier-filtering renderers support the optional `--annotate-outliers`
  overlay. A panel reports Snowflake's excluded observation count and
  percentage, the per-query Tukey upper fence, and the maximum excluded
  end-to-end latency only when at least one observation was excluded. When no
  exclusions occur, the chart shows neither an overlay nor an outlier legend
  qualifier. The overlay requires `--drop-outliers`.
- Snowflake `result` is the end-to-end latency shown in the chart.
  `compilation_time` and `execution_time` are preserved as supporting telemetry.
- The Snowflake-only dashboard experiment compares three conditions with a
  dedicated renderer: Interactive MV + Interactive Small (blue solid),
  Interactive MV + Gen2 Small (orange dashed), and raw Interactive Table +
  Interactive Small (purple dash-dot). Each condition is plotted at its own
  observed `raw_rows`; it is not joined by iteration. The MV and raw SQL files
  implement the same four dashboard questions but perform materially different
  query-time work. A per-condition, per-query Tukey rule keeps rare spikes from
  flattening the displayed trends while retaining every measurement in CSV and
  JSON provenance. Only conditions and panels with actual exclusions receive
  visible filtering disclosure.
- A separate one-to-one dashboard chart compares ClickHouse's maintained
  materialized-view target with Snowflake computing the same four dashboard
  questions directly from the raw Interactive Table on Interactive Small. Its
  legend names both physical paths explicitly. This is an intentionally
  asymmetric data-path experiment, not the architecture-equivalent headline
  comparison, and it contains no accumulated-cost strip.
- Its companion Snowflake-only phase chart uses the same raw Interactive Table
  runner, 100B row cap, Tukey rule, and seven-observation display median. It
  stacks provider-reported compilation in orange beneath execution on
  Interactive Small in Snowflake blue. ClickHouse is intentionally absent;
  end-to-end `result` and `result - compilation_time - execution_time` remain
  available in CSV and summary provenance.
- The query-time breakdown chart stacks Snowflake's provider-reported
  `compilation_time` and `execution_time`. It applies the same per-query Tukey
  exclusion to the whole observation based on end-to-end `result`, then applies
  the same centered seven-observation rolling median to cumulative phase
  boundaries. This keeps the displayed stack additive. Every raw measurement,
  exclusion, and the residual `result - compilation_time - execution_time`
  remains in the CSV and provenance summary.
  Compilation uses the architecture diagram's orange serverless-compilation
  color; execution uses Snowflake blue to represent warehouse execution.
- The same phase renderer is used for the two Snowflake drill-down queries. Its
  phase chart uses the drill-down latency chart's centered five-observation
  rolling median and does not exclude any observations.
- The aggregate and drill-down latency renderers add a slim, workload-specific
  strip below their panels. Each strip summarizes the matched active-ingestion
  accumulated runtime and query cost for only the workload plotted above it.
  The strip validates equal iteration, queries-per-iteration, and execution
  counts before rendering. This replaces the former standalone combined
  runtime-and-cost chart in the presentation flow.
- Lag uses Snowflake's `behind_by`, interpolates base-table volume from the
  dashboard timeline, and plots a centered 61-sample rolling mean over the
  measured one-minute polls. The publication command then applies a gentle
  centered 11-sample display-only mean and shape-preserving PCHIP interpolation
  so the dense one-minute series renders as a fluid curve. Legend statistics
  still come from the 61-sample evidence trend. Raw evidence remains unchanged;
  the source-sample CSV and exact rendered-curve CSV preserve both layers.
- Accumulated runtime and cost strips use the already generated,
  row-progress-matched ClickHouse cost summaries and the 209/35-observation
  Snowflake summaries. The renderers never slice these summaries again.
- The complete-ingest Snowflake fresh-data path is **Snowpipe Streaming plus
  serverless MV refresh**. No ingest warehouse is used or priced.
- Enterprise pricing is used for both systems. Gen2 and Interactive warehouse
  pricing files are used only to validate dashboard and drill-down query cost.
