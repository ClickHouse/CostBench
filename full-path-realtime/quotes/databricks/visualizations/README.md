# Databricks publication charts

`plot_publication.py` renders pairwise fresh-path cost, full-path
cost-performance, and matched query-latency PNGs from a publisher-produced
`publication_manifest.json`.

The renderer verifies the manifest's exact accepted status and every declared
source and artifact SHA-256 before importing Matplotlib or drawing a chart:

```bash
uv run plot_publication.py \
  --publication-manifest ../out/publication_manifest.json \
  --output-dir ../out/charts
```

The renderer creates no benchmark data. Run `../publish_results.py` only after
the validator and cost summarizer have accepted complete real-run evidence.

## Preliminary MV freshness characterization

`plot_freshness_characterization.py` reconstructs the qualification MV's
source-row watermark from provider metadata. It pairs each completed pipeline
`update_id` with
`planning_information.source_table_information.num_rows`, then aligns that
watermark with the monotonic provider-acknowledged producer timeline.

```bash
uv run plot_freshness_characterization.py \
  --ingest-metrics ../results/qualification/RUN/ingest/ingest_metrics.jsonl \
  --ingest-summary ../results/qualification/RUN/ingest/ingest_summary.json \
  --freshness ../results/qualification/RUN/freshness-recovered/freshness.jsonl \
  --output-dir ../results/qualification/RUN/charts
```

It emits PNG and SVG plots, the plotted CSV, and a JSON methodology/provenance
summary with source SHA-256 hashes. The plot reports rows behind and a
metadata-derived time bound; it does not claim event-time lag or query raw/MV
contents. Preliminary characterization artifacts are not accepted publication
results.
