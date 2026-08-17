# Redshift Serverless T2 visualizations

Run `bash _commands.txt` after `utils/_commands.txt`, the Redshift additions in
`quotes/clickhouse-cloud/costs/_commands.txt`, and `quotes/redshift-serverless/costs/_commands.txt`.

The pairwise latency charts plot each system at its own observed base-table row
count. Accumulated runtime and query cost use progress-matched ClickHouse
subsets: 190 dashboard observations and 33 drill-down observations. SUPER and
typed drill-down are separate counterfactual read paths. They reuse one shared
writer+MSK complete-ingest cost; that cost is never split or duplicated.

Every wide output is 5156×2900 with a transparent 560-pixel header-safe area
and the common subtle chart stage. The charts show total query price. The
hourly allocation details remain in the generated JSON summaries.
