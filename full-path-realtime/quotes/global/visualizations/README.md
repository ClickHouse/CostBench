# Global real-time benchmark visualizations

This directory is the provider-neutral synthesis layer for the quotes benchmark.
It does not replace provider-specific renderers or matched cost artifacts.

## Contracts

- Query latency charts reread original runner JSONL. Each provider is plotted at
  its own observed `raw_rows`; iteration numbers are never joined across systems.
- The 100B row cap is inclusive. Aggregate lines use a provider-local centered
  seven-observation median; drill-down lines use five observations.
- The Snowflake aggregate series retains the accepted pairwise Tukey upper-fence
  policy. No outlier policy is invented for ClickHouse, BigQuery, or drill-downs.
- MV lag means persisted materialized-view refresh lag. ClickHouse is zero by
  ingest-time incremental-MV design. BigQuery's default direct-query delta
  reconciliation is query-answer behavior and is not this persisted-lag metric.
- Complete-ingest fresh-data-path costs are read unchanged from the accepted
  pairwise fresh-path summaries. The renderer verifies that both summaries use
  the same ClickHouse total and source hash, and keeps BigQuery Capacity and
  On-demand MV-refresh pricing as alternatives rather than adding them.
- Full-path cost-performance values are read unchanged from the accepted
  ClickHouse-vs-Snowflake and ClickHouse-vs-BigQuery summary JSON files. Each
  provider is normalized to ClickHouse within its own pairwise matched window;
  this chart does not introduce a cross-provider iteration match.
- The absolute cost-versus-runtime map reads `total_cost` and `runtime_sec` from
  those same summaries. It intentionally accepts their different pairwise
  query windows, and uses inverted logarithmic axes so faster is right and
  lower cost is up.

Run the commands in `_commands.txt`. Charts, SVGs, source CSVs, and provenance
summaries are written to `quotes/global/results/charts/`.

The command file defines `CHART_BACKGROUND_COLOR` once (default `#161614`) and
always produces both standard and true slide-wide variants. Wide variants use
larger presentation type and expand the plot into the available 16:8.5 canvas.
The global latency charts intentionally omit the pairwise runtime/cost evidence
strip because each provider retains its own accepted comparison window. The
absolute cost-versus-runtime quadrant also has a wide layout so its inverted
logarithmic axes, markers, and labels remain legible on a webinar slide.
