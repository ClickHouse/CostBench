# Extraction query index

Every query below is retained verbatim. Source CSVs contain the published, sanitized result values. The [manifest](../source-manifest.json) specifies result schemas, hashes and any omitted diagnostic columns. Queries without retained results do not feed a visual.

## Measurement and supporting exports

| SQL | Result CSV(s) | Source key |
| --- | --- | --- |
| [00-fields.sql](00-fields.sql) | [fields.csv](../results/fields.csv) | `fields` |
| [01-coverage.sql](01-coverage.sql) | [coverage.csv](../results/coverage.csv) | `coverage` |
| [02-part-summary.sql](02-part-summary.sql) | [part-summary.csv](../results/part-summary.csv) | `part-summary` |
| [03-metric-series.sql](03-metric-series.sql) | [metric-series.csv](../results/metric-series.csv) | `metric-series` |
| [04-newpart-series.sql](04-newpart-series.sql) | [newpart-series.csv](../results/newpart-series.csv) | `newpart-series` |
| [05-part-metric-names.sql](05-part-metric-names.sql) | [part-metric-names.csv](../results/part-metric-names.csv) | `part-metric-names` |
| [06-active-metric-series.sql](06-active-metric-series.sql) | [active-metric-series.csv](../results/active-metric-series.csv) | `active-metric-series` |
| [07-active-parts-series.sql](07-active-parts-series.sql) | [active-parts-series.csv](../results/active-parts-series.csv) | `active-parts-series` |
| [08-active-newpart-series.sql](08-active-newpart-series.sql) | [active-newpart-series.csv](../results/active-newpart-series.csv) | `active-newpart-series` |
| [09-preparation-fields.sql](09-preparation-fields.sql) | [preparation-fields.csv](../results/preparation-fields.csv) | `preparation-fields` |
| [10a-preparation-log-sample-coverage.sql](10a-preparation-log-sample-coverage.sql) | [sample-log-coverage.csv](../results/sample-log-coverage.csv) | `sample-log-coverage` |
| [11-preparation-log-metadata.sql](11-preparation-log-metadata.sql) | [log-metadata.csv](../results/log-metadata.csv) | `log-metadata` |
| [12-preparation-view-sample.sql](12-preparation-view-sample.sql) | [view-sample.csv](../results/view-sample.csv) | `view-sample` |
| [13-preparation-processors-sample.sql](13-preparation-processors-sample.sql) | [processors-sample.csv](../results/processors-sample.csv) | `processors-sample` |
| [14-preparation-metric-totals.sql](14-preparation-metric-totals.sql) | [metric-totals.csv](../results/metric-totals.csv) | `metric-totals` |
| [15-preparation-parts-sample.sql](15-preparation-parts-sample.sql) | [parts-sample.csv](../results/parts-sample.csv) | `parts-sample` |
| [16-preparation-profiler-metadata.sql](16-preparation-profiler-metadata.sql) | [profiler-metadata-tables.csv](../results/profiler-metadata-tables.csv), [profiler-metadata-columns.csv](../results/profiler-metadata-columns.csv) | `profiler-metadata` |
| [17-preparation-view-run.sql](17-preparation-view-run.sql) | [view-run.csv](../results/view-run.csv) | `view-run` |
| [18-preparation-trace-sample.sql](18-preparation-trace-sample.sql) | [trace-sample.csv](../results/trace-sample.csv) | `trace-sample` |
| [19-preparation-cpu-stack-sample.sql](19-preparation-cpu-stack-sample.sql) | [cpu-stacks-sample.csv](../results/cpu-stacks-sample.csv) | `cpu-stacks-sample` |
| [20-preparation-table-and-profiler-settings.sql](20-preparation-table-and-profiler-settings.sql) | [table-and-settings-tables.csv](../results/table-and-settings-tables.csv), [table-and-settings-settings.csv](../results/table-and-settings-settings.csv) | `table-and-settings` |
| [21-preparation-view-series.sql](21-preparation-view-series.sql) | [view-series.csv](../results/view-series.csv) | `view-series` |
| [22-preparation-newpart-timing-series.sql](22-preparation-newpart-timing-series.sql) | [newpart-timing-series.csv](../results/newpart-timing-series.csv) | `newpart-timing-series` |
| [23-preparation-historical-profiler-settings.sql](23-preparation-historical-profiler-settings.sql) | [historical-settings.csv](../results/historical-settings.csv) | `historical-settings` |
| [25-preparation-flush-cpu-sample.sql](25-preparation-flush-cpu-sample.sql) | [flush-cpu-sample.csv](../results/flush-cpu-sample.csv) | `flush-cpu-sample` |
| [26-preparation-merge-series.sql](26-preparation-merge-series.sql) | [merge-series.csv](../results/merge-series.csv) | `merge-series` |
| [28-preparation-merge-consolidation-series.sql](28-preparation-merge-consolidation-series.sql) | [merge-consolidation-series.csv](../results/merge-consolidation-series.csv) | `merge-consolidation-series` |
| [29-preparation-part-integrity.sql](29-preparation-part-integrity.sql) | [part-integrity.csv](../results/part-integrity.csv) | `part-integrity` |
| [30-preparation-before-run-parts.sql](30-preparation-before-run-parts.sql) | [before-run-parts.csv](../results/before-run-parts.csv) | `before-run-parts` |
| [31-preparation-current-part-snapshot.sql](31-preparation-current-part-snapshot.sql) | [current-part-snapshot.csv](../results/current-part-snapshot.csv) | `current-part-snapshot` |
| [32-preparation-summary-coverage-snapshot.sql](32-preparation-summary-coverage-snapshot.sql) | [summary-coverage-snapshot.csv](../results/summary-coverage-snapshot.csv) | `summary-coverage-snapshot` |
| [33-preparation-after-run-merges.sql](33-preparation-after-run-merges.sql) | [after-run-merges.csv](../results/after-run-merges.csv) | `after-run-merges` |
| [34-preparation-before-run-table-identities.sql](34-preparation-before-run-table-identities.sql) | [before-run-table-identities.csv](../results/before-run-table-identities.csv) | `before-run-table-identities` |
| [37-preparation-part-operation-types.sql](37-preparation-part-operation-types.sql) | [part-operation-types.csv](../results/part-operation-types.csv) | `part-operation-types` |
| [38-memory-discovery.sql](38-memory-discovery.sql) | [memory-discovery-tables.csv](../results/memory-discovery-tables.csv), [memory-discovery-columns.csv](../results/memory-discovery-columns.csv), [memory-discovery-metrics.csv](../results/memory-discovery-metrics.csv) | `memory-discovery` |
| [39-memory-coverage-sample.sql](39-memory-coverage-sample.sql) | [memory-coverage-sample.csv](../results/memory-coverage-sample.csv) | `memory-coverage-sample` |
| [40-memory-dashboard-definition.sql](40-memory-dashboard-definition.sql) | [memory-dashboard-definition-columns.csv](../results/memory-dashboard-definition-columns.csv), [memory-dashboard-definition-metrics.csv](../results/memory-dashboard-definition-metrics.csv) | `memory-dashboard-definition` |
| [41-memory-dashboard-series.sql](41-memory-dashboard-series.sql) | [memory-dashboard-series.csv](../results/memory-dashboard-series.csv) | `memory-dashboard-series` |
| [42-memory-node-series.sql](42-memory-node-series.sql) | [memory-node-series.csv](../results/memory-node-series.csv) | `memory-node-series` |
| [43-memory-capacity-check.sql](43-memory-capacity-check.sql) | [memory-capacity-check.csv](../results/memory-capacity-check.csv) | `memory-capacity-check` |

## CPU profiler chunks

| SQL | Result CSV |
| --- | --- |
| [24-00-preparation-cpu-stage-series.sql](24-00-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-00.csv](../results/cpu-stage-chunk-00.csv) |
| [24-01-preparation-cpu-stage-series.sql](24-01-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-01.csv](../results/cpu-stage-chunk-01.csv) |
| [24-02-preparation-cpu-stage-series.sql](24-02-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-02.csv](../results/cpu-stage-chunk-02.csv) |
| [24-03-preparation-cpu-stage-series.sql](24-03-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-03.csv](../results/cpu-stage-chunk-03.csv) |
| [24-04-preparation-cpu-stage-series.sql](24-04-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-04.csv](../results/cpu-stage-chunk-04.csv) |
| [24-05-preparation-cpu-stage-series.sql](24-05-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-05.csv](../results/cpu-stage-chunk-05.csv) |
| [24-06-preparation-cpu-stage-series.sql](24-06-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-06.csv](../results/cpu-stage-chunk-06.csv) |
| [24-07-preparation-cpu-stage-series.sql](24-07-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-07.csv](../results/cpu-stage-chunk-07.csv) |
| [24-08-preparation-cpu-stage-series.sql](24-08-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-08.csv](../results/cpu-stage-chunk-08.csv) |
| [24-09-preparation-cpu-stage-series.sql](24-09-preparation-cpu-stage-series.sql) | [cpu-stage-chunk-09.csv](../results/cpu-stage-chunk-09.csv) |
| [24c-00-preparation-cpu-stage-series.sql](24c-00-preparation-cpu-stage-series.sql) | [cpu-stage-audit-00.csv](../results/cpu-stage-audit-00.csv) |
| [24c-01-preparation-cpu-stage-series.sql](24c-01-preparation-cpu-stage-series.sql) | [cpu-stage-audit-01.csv](../results/cpu-stage-audit-01.csv) |
| [24c-02-preparation-cpu-stage-series.sql](24c-02-preparation-cpu-stage-series.sql) | [cpu-stage-audit-02.csv](../results/cpu-stage-audit-02.csv) |
| [24c-03-preparation-cpu-stage-series.sql](24c-03-preparation-cpu-stage-series.sql) | [cpu-stage-audit-03.csv](../results/cpu-stage-audit-03.csv) |
| [24c-04-preparation-cpu-stage-series.sql](24c-04-preparation-cpu-stage-series.sql) | [cpu-stage-audit-04.csv](../results/cpu-stage-audit-04.csv) |
| [24c-05-preparation-cpu-stage-series.sql](24c-05-preparation-cpu-stage-series.sql) | [cpu-stage-audit-05.csv](../results/cpu-stage-audit-05.csv) |
| [24c-06-preparation-cpu-stage-series.sql](24c-06-preparation-cpu-stage-series.sql) | [cpu-stage-audit-06.csv](../results/cpu-stage-audit-06.csv) |
| [24c-07-preparation-cpu-stage-series.sql](24c-07-preparation-cpu-stage-series.sql) | [cpu-stage-audit-07.csv](../results/cpu-stage-audit-07.csv) |
| [24c-08-preparation-cpu-stage-series.sql](24c-08-preparation-cpu-stage-series.sql) | [cpu-stage-audit-08.csv](../results/cpu-stage-audit-08.csv) |
| [24c-09-preparation-cpu-stage-series.sql](24c-09-preparation-cpu-stage-series.sql) | [cpu-stage-audit-09.csv](../results/cpu-stage-audit-09.csv) |

## Insert CPU chunks

| SQL | Result CSV |
| --- | --- |
| [27b-00-preparation-flush-cpu-series.sql](27b-00-preparation-flush-cpu-series.sql) | [flush-cpu-hour-00.csv](../results/flush-cpu-hour-00.csv) |
| [27b-01-preparation-flush-cpu-series.sql](27b-01-preparation-flush-cpu-series.sql) | [flush-cpu-hour-01.csv](../results/flush-cpu-hour-01.csv) |
| [27b-02-preparation-flush-cpu-series.sql](27b-02-preparation-flush-cpu-series.sql) | [flush-cpu-hour-02.csv](../results/flush-cpu-hour-02.csv) |
| [27b-03-preparation-flush-cpu-series.sql](27b-03-preparation-flush-cpu-series.sql) | [flush-cpu-hour-03.csv](../results/flush-cpu-hour-03.csv) |
| [27b-04-preparation-flush-cpu-series.sql](27b-04-preparation-flush-cpu-series.sql) | [flush-cpu-hour-04.csv](../results/flush-cpu-hour-04.csv) |
| [27b-05-preparation-flush-cpu-series.sql](27b-05-preparation-flush-cpu-series.sql) | [flush-cpu-hour-05.csv](../results/flush-cpu-hour-05.csv) |
| [27b-06-preparation-flush-cpu-series.sql](27b-06-preparation-flush-cpu-series.sql) | [flush-cpu-hour-06.csv](../results/flush-cpu-hour-06.csv) |
| [27b-07-preparation-flush-cpu-series.sql](27b-07-preparation-flush-cpu-series.sql) | [flush-cpu-hour-07.csv](../results/flush-cpu-hour-07.csv) |
| [27b-08-preparation-flush-cpu-series.sql](27b-08-preparation-flush-cpu-series.sql) | [flush-cpu-hour-08.csv](../results/flush-cpu-hour-08.csv) |
| [27b-09-preparation-flush-cpu-series.sql](27b-09-preparation-flush-cpu-series.sql) | [flush-cpu-hour-09.csv](../results/flush-cpu-hour-09.csv) |
| [27b-10-preparation-flush-cpu-series.sql](27b-10-preparation-flush-cpu-series.sql) | [flush-cpu-hour-10.csv](../results/flush-cpu-hour-10.csv) |
| [27b-11-preparation-flush-cpu-series.sql](27b-11-preparation-flush-cpu-series.sql) | [flush-cpu-hour-11.csv](../results/flush-cpu-hour-11.csv) |
| [27b-12-preparation-flush-cpu-series.sql](27b-12-preparation-flush-cpu-series.sql) | [flush-cpu-hour-12.csv](../results/flush-cpu-hour-12.csv) |
| [27b-13-preparation-flush-cpu-series.sql](27b-13-preparation-flush-cpu-series.sql) | [flush-cpu-hour-13.csv](../results/flush-cpu-hour-13.csv) |
| [27b-14-preparation-flush-cpu-series.sql](27b-14-preparation-flush-cpu-series.sql) | [flush-cpu-hour-14.csv](../results/flush-cpu-hour-14.csv) |
| [27b-15-preparation-flush-cpu-series.sql](27b-15-preparation-flush-cpu-series.sql) | [flush-cpu-hour-15.csv](../results/flush-cpu-hour-15.csv) |
| [27b-16-preparation-flush-cpu-series.sql](27b-16-preparation-flush-cpu-series.sql) | [flush-cpu-hour-16.csv](../results/flush-cpu-hour-16.csv) |
| [27b-17-preparation-flush-cpu-series.sql](27b-17-preparation-flush-cpu-series.sql) | [flush-cpu-hour-17.csv](../results/flush-cpu-hour-17.csv) |
| [27b-18-preparation-flush-cpu-series.sql](27b-18-preparation-flush-cpu-series.sql) | [flush-cpu-hour-18.csv](../results/flush-cpu-hour-18.csv) |
| [27b-19-preparation-flush-cpu-series.sql](27b-19-preparation-flush-cpu-series.sql) | [flush-cpu-hour-19.csv](../results/flush-cpu-hour-19.csv) |
| [27b-20-preparation-flush-cpu-series.sql](27b-20-preparation-flush-cpu-series.sql) | [flush-cpu-hour-20.csv](../results/flush-cpu-hour-20.csv) |
| [27b-21-preparation-flush-cpu-series.sql](27b-21-preparation-flush-cpu-series.sql) | [flush-cpu-hour-21.csv](../results/flush-cpu-hour-21.csv) |
| [27b-22-preparation-flush-cpu-series.sql](27b-22-preparation-flush-cpu-series.sql) | [flush-cpu-hour-22.csv](../results/flush-cpu-hour-22.csv) |
| [27b-23-preparation-flush-cpu-series.sql](27b-23-preparation-flush-cpu-series.sql) | [flush-cpu-hour-23.csv](../results/flush-cpu-hour-23.csv) |
| [27b-24-preparation-flush-cpu-series.sql](27b-24-preparation-flush-cpu-series.sql) | [flush-cpu-hour-24.csv](../results/flush-cpu-hour-24.csv) |
| [27b-25-preparation-flush-cpu-series.sql](27b-25-preparation-flush-cpu-series.sql) | [flush-cpu-hour-25.csv](../results/flush-cpu-hour-25.csv) |
| [27b-26-preparation-flush-cpu-series.sql](27b-26-preparation-flush-cpu-series.sql) | [flush-cpu-hour-26.csv](../results/flush-cpu-hour-26.csv) |
| [27b-27-preparation-flush-cpu-series.sql](27b-27-preparation-flush-cpu-series.sql) | [flush-cpu-hour-27.csv](../results/flush-cpu-hour-27.csv) |
| [27b-28-preparation-flush-cpu-series.sql](27b-28-preparation-flush-cpu-series.sql) | [flush-cpu-hour-28.csv](../results/flush-cpu-hour-28.csv) |
| [27b-29-preparation-flush-cpu-series.sql](27b-29-preparation-flush-cpu-series.sql) | [flush-cpu-hour-29.csv](../results/flush-cpu-hour-29.csv) |
| [27b-30-preparation-flush-cpu-series.sql](27b-30-preparation-flush-cpu-series.sql) | [flush-cpu-hour-30.csv](../results/flush-cpu-hour-30.csv) |
| [27b-31-preparation-flush-cpu-series.sql](27b-31-preparation-flush-cpu-series.sql) | [flush-cpu-hour-31.csv](../results/flush-cpu-hour-31.csv) |
| [27b-32-preparation-flush-cpu-series.sql](27b-32-preparation-flush-cpu-series.sql) | [flush-cpu-hour-32.csv](../results/flush-cpu-hour-32.csv) |
| [27b-33-preparation-flush-cpu-series.sql](27b-33-preparation-flush-cpu-series.sql) | [flush-cpu-hour-33.csv](../results/flush-cpu-hour-33.csv) |
| [27b-34-preparation-flush-cpu-series.sql](27b-34-preparation-flush-cpu-series.sql) | [flush-cpu-hour-34.csv](../results/flush-cpu-hour-34.csv) |
| [27b-35-preparation-flush-cpu-series.sql](27b-35-preparation-flush-cpu-series.sql) | [flush-cpu-hour-35.csv](../results/flush-cpu-hour-35.csv) |
| [27b-36-preparation-flush-cpu-series.sql](27b-36-preparation-flush-cpu-series.sql) | [flush-cpu-hour-36.csv](../results/flush-cpu-hour-36.csv) |

## Queries without retained results

These are superseded or unsuccessful diagnostics. Their extraction status is recorded explicitly; no result CSV is invented for them.

| SQL | Status / reason |
| --- | --- |
| [10-preparation-log-coverage.sql](10-preparation-log-coverage.sql) | Superseded diagnostic extraction; no accepted result is retained. |
| [27-00-preparation-flush-cpu-series.sql](27-00-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-01-preparation-flush-cpu-series.sql](27-01-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-02-preparation-flush-cpu-series.sql](27-02-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-03-preparation-flush-cpu-series.sql](27-03-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-04-preparation-flush-cpu-series.sql](27-04-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-05-preparation-flush-cpu-series.sql](27-05-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-06-preparation-flush-cpu-series.sql](27-06-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-07-preparation-flush-cpu-series.sql](27-07-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-08-preparation-flush-cpu-series.sql](27-08-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-09-preparation-flush-cpu-series.sql](27-09-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-10-preparation-flush-cpu-series.sql](27-10-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-11-preparation-flush-cpu-series.sql](27-11-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-12-preparation-flush-cpu-series.sql](27-12-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-13-preparation-flush-cpu-series.sql](27-13-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-14-preparation-flush-cpu-series.sql](27-14-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-15-preparation-flush-cpu-series.sql](27-15-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-16-preparation-flush-cpu-series.sql](27-16-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-17-preparation-flush-cpu-series.sql](27-17-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [27-18-preparation-flush-cpu-series.sql](27-18-preparation-flush-cpu-series.sql) | Two-hour query-log chunks were replaced by the retained one-hour 27b chunks; not used. |
| [35-preparation-cutoff-parts-from-lineage.sql](35-preparation-cutoff-parts-from-lineage.sql) | Full part-name lineage diagnostic exceeded the unchanged scan guard; not used in any figure. |
| [35b-preparation-cutoff-summary-lineage.sql](35b-preparation-cutoff-summary-lineage.sql) | Full part-name lineage diagnostic exceeded the unchanged scan guard; not used in any figure. |
| [36-preparation-merge-row-flow.sql](36-preparation-merge-row-flow.sql) | Superseded diagnostic extraction; no accepted result is retained. |
| [42-memory-node-series-alias.sql](diagnostics/42-memory-node-series-alias.sql) | Alias-resolution error; replaced by 42-memory-node-series.sql. No result. |
