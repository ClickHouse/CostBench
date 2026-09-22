# Databricks packaged-result field glossary

This glossary covers the compact JSON and JSONL files in a packaged result. Sections are keyed by package-relative filename pattern. `<timestamp>` and `<nn>` are placeholders.

## Conventions

- `a.b` means member `b` inside object `a`; `items[]` means each array element; `{key}` means a dynamic object key.
- `gates[id=name]` means the element of `gates[]` whose `id` is `name`.
- Each non-empty JSONL line is one independent JSON object.
- Timestamps are UTC ISO-8601 strings unless explicitly described as epoch milliseconds or nanoseconds.
- `_sec` and `_seconds` are seconds; `_ms` is milliseconds; `_ns` is nanoseconds.
- `_bytes` is bytes; `_gib` is GiB; `_percent` is percentage points; `_per_second` is a rate per second.
- Row, file, batch, task, operation, iteration, and error counts are integers. Query and host durations are JSON numbers.
- Money amounts and provider usage quantities in validation evidence are decimal strings.
- SHA-256 fields are hexadecimal hashes of the named bytes or canonical payload.
- `null` means unavailable, not applicable, or not yet observed; it never means zero.
- An empty array or object means the collection was present with no entries. An absent optional member was not emitted by that producing branch.

## Reusable objects

### `collection_window`

- `since`, `until`: inclusive start and exclusive end.
- `duration_seconds`: window duration.

### `money_total`

Used only by cost-related evidence embedded in the validation report.

- `currency`, `amount`: singular currency code and total; both may be `null` when the total is not singular.
- `amount_by_currency.{currency}`: exact total for each represented currency.

### `error_entry`

- `source` or `dataset`: component or dataset associated with the error.
- `required`: optional required-source flag.
- `error`: redacted error text.

### `latest_successful_refresh`

The whole object may be `null`.

- `finished_at`, `planned_at`: completion time and optional matching planning time.
- `update_id`: optional provider update identifier.
- `output_rows`: provider-reported refresh output rows.
- `maintenance_type`, `maintenance_class`: provider maintenance type and normalized category.
- `source_snapshot.table_name`: source table represented by the refresh.
- `source_snapshot.num_rows`, `source_snapshot.num_files`, `source_snapshot.full_size_bytes`: complete snapshot totals.
- `source_snapshot.changed_rows`, `source_snapshot.changed_files`, `source_snapshot.change_size_bytes`: refresh-change totals.
- `source_snapshot.delta_version`: source Delta version when exposed.

### `transport_recovery`

- `event_count`, `completed_count`, `failed_count`: attempt totals.
- `total_duration_sec`, `max_duration_sec`: summed and maximum attempt duration.
- `last_event`: latest event, or `null`.
- `last_event.at`, `last_event.stream`, `last_event.generation`: event time and stream identity.
- `last_event.operation`, `last_event.status`, `last_event.duration_sec`: operation, outcome, and duration.
- `last_event.pending_batches`, `last_event.pending_rows`: data awaiting durability.
- `last_event.error`: redacted failure text, if any.

### `stream_status`

Used at `stream_status.{worker}`.

- `worker`, `stream`, `state`: one-based worker number, stable stream label, and lifecycle state.
- `automatic_recovery`, `manual_stream_rotation_seconds`: recovery switch and proactive rotation interval.
- `generation`, `rotation_count`: current generation and completed proactive rotations.
- `opened_at`, `last_rotation_at`, `closed_at`: lifecycle times.
- `last_rotation_unacked_batches`: unacknowledged batches at the last rotation.
- `close_succeeded`: final SDK close result.
- `inspected_at`, `unacked_inspection_succeeded`: final inspection time and result.
- `unacked_batches`, `unacked_rows`: final unacknowledged totals.
- `artifacts[]`: preserved unacknowledged Arrow batches.
- `artifacts[].index`, `artifacts[].path`, `artifacts[].rows`, `artifacts[].bytes`, `artifacts[].sha256`: artifact ordinal, producer path, row count, size, and byte hash.
- `inspection_error`: optional redacted inspection failure.
- `summary_path`: producer path of the worker summary.

### `warehouse_snapshot`

Used at `query_warehouse` in compact query records.

- `captured_at`, `warehouse_id`, `warehouse_mode`, `http_path`: capture time, serving identity, mode, and connector path.
- `connector_version`, `use_kernel`: SQL connector version and Statement Execution kernel switch.
- `session_configuration.use_cached_result`, `session_configuration.ansi_mode`, `session_configuration.timezone`: session settings.
- `api_metadata.id`, `api_metadata.name`, `api_metadata.size`, `api_metadata.cluster_size`: provider identity and size labels.
- `api_metadata.min_num_clusters`, `api_metadata.max_num_clusters`: configured scaling bounds.
- `api_metadata.num_clusters`, `api_metadata.num_active_sessions`: observed cluster and session counts.
- `api_metadata.auto_stop_mins`, `api_metadata.auto_resume`: idle-stop and resume settings.
- `api_metadata.tags.{tag}`: provider warehouse tags.
- `api_metadata.spot_instance_policy`, `api_metadata.enable_photon`, `api_metadata.enable_serverless_compute`: compute settings.
- `api_metadata.warehouse_type`, `api_metadata.state`, `api_metadata.health.status`: provider type, observed state, and health.

### `validation_gate`

- `id`: stable gate identifier.
- `passed`: gate decision.
- `evidence`: gate-specific bounded evidence.
- `reasons[]`: failure reasons.

## `run_context.json`

- `schema_version`, `run_id`: schema revision and immutable run identifier.
- `run_root`: producer-side result path.
- `status`, `publishable`: final lifecycle and publication decisions.
- `since`, `until`, `producer_finished_at`: measurement bounds and producer completion time.
- `catalog`, `schema`, `raw_table`, `mv_table`: target namespace and tables.
- `query_warehouse_id`, `control_warehouse_id`: serving and control warehouse identifiers.
- `query_warehouse_mode`, `query_size`: configured serving mode and size.
- `expected_rows`, `target_eps`: required source rows and target rows per second.
- `dashboard_interval_seconds`, `drilldown_interval_seconds`: workload schedule intervals.
- `post_ingest_query_seconds`: configured post-ingest query duration.
- `result_profile`: result packaging profile.
- `ingest_metrics_interval_seconds`, `freshness_interval_seconds`: telemetry persistence intervals.
- `automatic_stream_recovery`: SDK recovery switch.
- `recovery_timeout_ms`, `recovery_backoff_ms`, `recovery_retries`: recovery timing and retry limit.
- `manual_stream_rotation_seconds`: proactive stream-rotation interval.
- `reconciliation.client_durable_rows`, `reconciliation.provider_committed_rows`: producer and settled provider row totals.
- `reconciliation.duplicate_surplus_rows`, `reconciliation.exact`: row surplus and equality result.
- `settled_validation_process_status`: settled validator process status.
- `settled_cost_collected_at`, `settled_cost_file`: collection time and basename of the external settled cost input.
- `settled_validation_file`: basename of the packaged settled validation report.

## `mv/dashboard_<timestamp>.jsonl` and `raw/drilldown_<timestamp>.jsonl`

Both patterns use compact query schema version 3. Fields shaped as `[query][trial]` align to query order in the corresponding SQL workload.

- `schema_version`, `run_id`: compact schema revision and correlated run identifier.
- `runner`, `workload`, `iteration`: runner label, workload label, and one-based iteration.
- `scheduled_start_at`, `start_lag_sec`, `scheduled_interval_sec`: scheduled start, start lag, and schedule interval.
- `iteration_started_at`, `iteration_finished_at`, `iteration_elapsed_sec`: observed bounds and total duration.
- `raw_rows`, `raw_rows_source`: producer logical-row watermark and its source expression.
- `system`, `version`, `machine`, `cluster_size`: platform and compute display labels.
- `comment`, `tags[]`: configuration description and grouping labels.
- `result[query][trial]`: canonical provider-side duration in seconds.
- `compilation_time[query][trial]`, `execution_time[query][trial]`, `queue_time[query][trial]`, `result_fetch_time[query][trial]`: provider phase durations in seconds.
- `client_wall_time[query][trial]`: client end-to-end duration in seconds.
- `statement_ids[query][trial]`: provider statement identifiers.
- `cache_hit[query][trial]`, `read_io_cache_percent[query][trial]`: result-cache indicator and IO-cache percentage.
- `query_errors[query][]`: redacted errors for each aligned query.
- `warehouse_mode`, `control_warehouse_id`: serving mode and control warehouse identifier.
- `query_warehouse`: shared `warehouse_snapshot`.
- `producer_progress_path`: producer journal path.
- `producer_progress_loaded_at`, `producer_progress_file_modified_at`, `producer_progress_updated_at`: read, file-modification, and journal-update times.
- `producer_progress_run_id`, `producer_progress_finished`, `producer_progress_error`: journal run identity, completion flag, and read or validation error.
- `producer_progress_evidence.logical_raw_rows`, `producer_progress_evidence.provider_committed_rows`: logical and durable row watermarks.
- `producer_progress_evidence.baseline_table_rows`, `producer_progress_evidence.baseline_table_rows_unknown`: starting rows and baseline-proof flag.
- `producer_progress_evidence.running`, `producer_progress_evidence.finished`: producer lifecycle flags.
- `freshness_monitor_path`: compact freshness-progress path.
- `freshness_monitor_loaded_at`, `freshness_monitor_file_modified_at`, `freshness_monitor_updated_at`: read, file-modification, and snapshot-update times.
- `mv_refresh_finished_at`, `mv_source_rows`, `mv_rows_behind`: latest refresh time, represented source rows, and unrepresented committed rows.
- `freshness_monitor_error`: freshness snapshot read or validation error.

Full query-audit sidecars are excluded runtime state, not packaged result artifacts.

## `freshness/freshness_progress.json` and `freshness/mv_freshness_<timestamp>.jsonl`

The progress file is the latest snapshot; each JSONL line has the same shape.

- `schema_version`, `run_id`, `iteration`: schema revision, correlated run identifier, and one-based monitor iteration.
- `updated_at`, `observed_at`: snapshot write time and scheduled sample time.
- `client_durable_rows`, `provider_committed_rows`, `provider_committed_bytes`: producer and provider watermarks.
- `provider_error_count`: cumulative provider ingest errors.
- `latest_commit_version`, `latest_commit_time`: latest provider commit watermark.
- `raw_delta_version`, `raw_delta_timestamp`: latest raw-table Delta history watermark.
- `mv_rows`: reserved materialized-view row-count field.
- `latest_successful_refresh`: shared `latest_successful_refresh` object.
- `mv_source_rows`, `mv_rows_behind`: rows represented by the latest refresh and committed rows not represented.
- `raw_commit_age_sec`, `producer_progress_age_sec`, `mv_refresh_age_sec`: observational ages relative to the sample time.
- `age_semantics`: interpretation of the observational age fields.
- `statement_ids.zerobus_ingest`, `statement_ids.raw_history`, `statement_ids.mv_events`: provider evidence statements.
- `errors[]`: shared `error_entry` objects.

## `ingest/ingest_metrics.jsonl`

Each line is one compact producer metrics snapshot.

- `schema_version`, `run_id`, `observed_at`: schema revision, correlated run identifier, and observation time.
- `elapsed_sec`, `source_rows`: cumulative producer time and selected source rows.
- `provider_committed_rows`, `submitted_rows`, `pending_rows`: durable, submitted, and outstanding rows.
- `completed_tasks`: fully durable source tasks.
- `target_rows_per_sec`, `average_committed_rows_per_sec`, `session_committed_rows_per_sec`: configured, cumulative, and session rates.
- `terminal_error`, `ambiguous`, `error_count`: terminal error, durability-uncertainty flag, and error count.
- `streams.count`, `streams.states.{state}`: total streams and count by lifecycle state.
- `streams.rotation_count_total`, `streams.rotation_count_min`, `streams.rotation_count_max`: stream-rotation aggregates.
- `transport_recovery`: shared `transport_recovery` object.
- `producer.process_rss_bytes`, `producer.process_peak_rss_bytes`, `producer.system_available_bytes`, `producer.arrow_allocated_bytes`: memory gauges.
- `producer.cpu_cores`, `producer.host_cpu_utilization_percent`: process and host CPU gauges.
- `producer.network_receive_bytes_per_second`, `producer.network_transmit_bytes_per_second`: host network rates.
- `rate_limiter.scheduled_rows`, `rate_limiter.next_send_delay_sec`: limiter reservations and current delay.

## `ingest/ingest_progress.json`

- `schema_version`, `run_id`: producer-state schema revision and correlated run identifier.
- `started_at`, `session_started_at`, `updated_at`, `finished_at`: first-run start, session start, latest checkpoint, and completion times.
- `running`, `finished`, `stopped_early`: lifecycle flags.
- `clean_checkpoint`, `safe_to_resume`, `ambiguous`: checkpoint, resume-safety, and durability-uncertainty flags.
- `terminal_error`, `errors[]`, `resume_count`, `elapsed_sec`: terminal error, redacted error list, resume count, and cumulative time.
- `baseline_table_rows`, `baseline_table_rows_unknown`: proven starting rows and proof-availability flag.
- `provider_committed_rows`, `logical_raw_rows`: durable producer watermark and baseline-plus-durable rows.
- `submitted_rows`, `submitted_batches`, `durable_batches`, `durable_uncompressed_arrow_buffer_bytes`: submission and durability totals.
- `pending_rows`, `pending_batches`: outstanding submission totals.
- `assigned_tasks`, `completed_tasks`, `completed_task_ranges[]`: task totals and inclusive completed-ordinal ranges.
- `partial_task_rows.{task_ordinal}`: durable rows in each incomplete task.
- `pending_batch_evidence[]`: outstanding submitted batches.
- `pending_batch_evidence[].task_ordinal`, `pending_batch_evidence[].file_ordinal`, `pending_batch_evidence[].file`, `pending_batch_evidence[].row_group`: source coordinate.
- `pending_batch_evidence[].batch_ordinal`, `pending_batch_evidence[].row_offset`, `pending_batch_evidence[].row_count`: batch coordinate and size.
- `pending_batch_evidence[].uncompressed_arrow_buffer_bytes`, `pending_batch_evidence[].logical_offset`, `pending_batch_evidence[].submitted_at`: buffer estimate, acknowledgment offset, and submit time.
- `pending_batch_evidence[].worker`, `pending_batch_evidence[].stream`: submitting worker identity.
- `config.allow_partial`, `config.include_quotes_0`, `config.source_hashes_deferred`: source-selection switches.
- `config.automatic_recovery`, `config.manual_stream_rotation_seconds`: stream recovery settings.
- `config.recovery_backoff_ms`, `config.recovery_timeout_ms`, `config.recovery_retries`, `config.recovery_log_threshold_seconds`: recovery policy.
- `config.batch_size`, `config.checkpoint_batches`, `config.checkpoint_method`, `config.checkpoint_seconds`: batching and checkpoint policy.
- `config.compact_evidence`, `config.compact_metrics`, `config.compact_progress`: compact-output switches.
- `config.evidence_paths.output_dir`, `config.evidence_paths.progress`, `config.evidence_paths.ledger`: output, journal, and event-ledger paths.
- `config.evidence_paths.metrics`, `config.evidence_paths.manifest`, `config.evidence_paths.summary`: metrics, source-manifest, and summary paths.
- `config.expected_rows`, `config.target_rows_per_sec`, `config.workers`: row target, rate target, and worker count.
- `config.ipc_compression`, `config.memory_trim_interval`, `config.min_system_available_gib`: IPC and memory settings.
- `config.max_files`, `config.max_row_groups`, `config.max_rows`: optional source caps.
- `config.pattern`, `config.queue_capacity`: source filename pattern and task-queue capacity.
- `config.sdk_distribution`, `config.sdk_version`: producer SDK identity.
- `source.manifest_sha256`, `source.total_rows`, `source.file_count`, `source.task_count`: source fingerprint and totals.
- `target.catalog`, `target.schema`, `target.table`, `target.full_name`: target identity.
- `target.host`, `target.zerobus_endpoint`: workspace and ingest endpoints.
- `eps.target_rows_per_sec`, `eps.average_provider_committed_rows_per_sec`, `eps.session_provider_committed_rows_per_sec`: configured and observed rates.
- `rate_limiter.scheduled_rows`, `rate_limiter.next_send_delay_sec`, `rate_limiter.no_catch_up`, `rate_limiter.target_rows_per_sec`: limiter state.
- `memory.arrow_allocated_bytes`, `memory.arrow_peak_bytes`, `memory.process_rss_bytes`, `memory.process_peak_rss_bytes`: Arrow and process memory.
- `memory.system_available_bytes`, `memory.system_total_bytes`: host memory.
- `memory.trim_attempts`, `memory.trim_successes`, `memory.trim_unsupported`, `memory.gc_collected_objects`: maintenance counters.
- `memory.trim_reclaimed_rss_bytes`, `memory.last_trim_at`, `memory.last_trim_duration_sec`: observed reclamation and latest trim.
- `host.logical_cpu_count`, `host.sample_interval_seconds`: host CPU count and sampling interval.
- `host.producer_process_cpu_cores`, `host.producer_process_cpu_percent_of_host`, `host.host_cpu_utilization_percent`: process and host CPU gauges.
- `host.network_receive_bytes_per_second`, `host.network_transmit_bytes_per_second`: host network rates.
- `stream_status.{worker}`: shared `stream_status` object.
- `transport_recovery`: shared `transport_recovery` object.

## `ingest/ingest_summary.json`

This pattern contains every field defined for `ingest/ingest_progress.json` plus:

- `manifest_path`, `progress_path`, `evidence_ledger_path`, `metrics_path`: producer source-manifest, journal, event-ledger, and metrics paths.
- `dependencies.{distribution}`: dependency version by package distribution.
- `table_protection.attempted`, `table_protection.available`, `table_protection.method`: starting-state check execution, availability, and proof method.
- `table_protection.allow_nonempty_table`, `table_protection.fresh`: configured exception and derived fresh-target result.
- `table_protection.row_count`, `table_protection.num_files`, `table_protection.size_in_bytes`: starting metadata totals.
- `table_protection.history_versions[]`, `table_protection.statement_ids[]`, `table_protection.warehouse_id`: history and control-query evidence.
- `result.source_files`, `result.source_tasks`, `result.source_rows`: selected source totals.
- `result.provider_committed_rows`, `result.submitted_rows`, `result.completed_tasks`: final producer totals.
- `result.finished`, `result.clean_checkpoint`, `result.safe_to_resume`: final state flags.

## `ingest/source_hashes_post_run.json`

- `schema_version`, `status`, `generated_at`: schema revision, report status, and completion time.
- `source_manifest_path`, `source_manifest_sha256`: producer manifest path and exact file hash.
- `file_count`, `total_size_bytes`: hash-record count and summed selected-file size.
- `files[]`: selected source-file hash records.
- `files[].file_ordinal`, `files[].path`: canonical source ordinal and source-root-relative path.
- `files[].size_bytes`, `files[].mtime_ns`, `files[].sha256`: filesystem size, epoch-nanosecond modification time, and content hash.

## `ingest/streams/worker-<nn>-arrow.json`

- `worker`, `stream`: one-based worker number and stable stream label.
- `close_succeeded`, `inspected_at`, `unacked_inspection_succeeded`: final close and inspection evidence.
- `unacked_batches`, `unacked_rows`: final unacknowledged totals.
- `artifacts[]`: preserved unacknowledged Arrow batches using the artifact fields under `stream_status`.
- `inspection_error`: optional redacted inspection failure.

## `evidence/provider_reconciliation.json`

- `schema_version`, `run_id`, `collected_at`: schema revision, correlated run identifier, and collection time.
- `collection_window`: shared `collection_window`.
- `provider_committed_records`, `provider_committed_bytes`, `provider_errors`: settled provider totals.
- `zerobus_stream_error_count`: errors in stream evidence.
- `zerobus_ingest_bounds.min_commit_version`, `zerobus_ingest_bounds.max_commit_version`: commit-version bounds.
- `zerobus_ingest_bounds.min_commit_time`, `zerobus_ingest_bounds.max_commit_time`: commit-time bounds.
- `table_num_rows.raw`, `table_num_rows.materialized_view`: provider metadata row counts when exposed.
- `required_dataset_completeness.required[]`, `required_dataset_completeness.succeeded[]`, `required_dataset_completeness.failed[]`: required dataset outcomes.
- `required_dataset_completeness.complete`: required-source completeness.
- `optional_dataset_errors[]`, `complete`: optional failures and overall compact-evidence completeness.

## `evidence/mv_refresh_summary.json`

- `schema_version`, `run_id`, `collected_at`: schema revision, correlated run identifier, and collection time.
- `collection_window`: shared `collection_window`.
- `maintenance_type_counts.{maintenance_type}`: refresh count by provider maintenance type.
- `maintenance_class_counts.{class}`: refresh count by normalized class.
- `full_refresh_count`: refreshes classified as full.

## `evidence/clustering_status.json`

- `schema_version`, `run_id`, `collected_at`: schema revision, correlated run identifier, and collection time.
- `collection_window`: shared `collection_window`.
- `tables.{role}.name`, `tables.{role}.catalog`, `tables.{role}.schema`: normalized table identity.
- `tables.{role}.type`, `tables.{role}.format`, `tables.{role}.location`: normalized provider metadata.
- `tables.{role}.clustering_columns[]`, `tables.{role}.cluster_by_auto`, `tables.{role}.partition_columns[]`: clustering and partition metadata.
- `tables.{role}.num_files`, `tables.{role}.size_in_bytes`, `tables.{role}.table_features[]`: table totals and features.
- `tables.{role}.properties.{property}`: filtered table-property map.
- `tables.{role}.enable_predictive_optimization`, `tables.{role}.effective_predictive_optimization_flag`, `tables.{role}.predictive_optimization_effectively_enabled`: configured, effective, and normalized values.
- `predictive_optimization.preflight_status`: inherited preflight status.
- `predictive_optimization.summary.operation_count`, `predictive_optimization.summary.operation_counts_by_type.{type}`: operation totals.
- `predictive_optimization.summary.usage_by_unit.{unit}`: usage quantity by provider unit.
- `predictive_optimization.clustering_operation_count`: clustering-class operation count.
- `predictive_optimization.operations[]`: compact provider operations.
- `operations[].table_name`, `operations[].operation_id`, `operations[].operation_type`, `operations[].operation_status`: operation identity and status.
- `operations[].start_time`, `operations[].end_time`, `operations[].usage_unit`, `operations[].usage_quantity`: operation bounds and usage.
- `operations[].operation_metrics.{metric}`: decoded provider metric map.

## `validation/preflight.json`

- `generated_at`, `mode`: report time and preflight mode.
- `config.host`, `config.cloud`, `config.region`, `config.workspace_id`: requested workspace.
- `config.catalog`, `config.schema`, `config.raw_table`, `config.mv_table`: requested target.
- `config.rt_warehouse_id`, `config.control_warehouse_id`, `config.zerobus_endpoint`: requested warehouses and ingest endpoint.
- `offline`, `online`: local and workspace check groups.
- `summary.status`, `summary.error_count`, `summary.warning_count`: aggregate status and issue counts.
- `offline.status`, `online.status`, `offline.errors[]`, `online.errors[]`, `offline.warnings[]`, `online.warnings[]`: group results.
- `offline.config_validation`, `online.config_validation`: objects with `errors[]` and `warnings[]`.
- `offline.credentials`, `online.credentials`: credential-presence objects.
- `credentials.pat_configured`, `credentials.oauth_client_id_configured`, `credentials.oauth_client_secret_configured`, `credentials.oauth_client_credentials_complete`: redacted credential indicators.
- `offline.packages.{module}`, `online.packages.{module}`: dependency probes.
- `packages.{module}.available`, `version`, `probe_error`, `distribution`, `required_for`, `online_required`, `environment`, `interpreter`: probe fields.
- `offline.query_contract.expected.{workload}`, `offline.query_contract.actual.{workload}`, `offline.query_contract.files.{workload}`: SQL counts and paths.
- `offline.ddl_contract.file`, `offline.ddl_contract.create_materialized_view_found`: DDL path and CREATE MV detection.
- `online.authentication.mode`, `online.authentication.status`: authentication configuration.
- `online.query_warehouse_mode`: requested serving mode.
- `online.lakehouse_rt_type.status`, `warehouse_api_signal`, `statement_execution_succeeded`, `reason`: serving-type check.
- `online.warehouses.{role}.status`: warehouse lookup status.
- `online.warehouses.{role}.metadata.id`, `name`, `state`, `cluster_size`: warehouse identity and observed state.
- `online.warehouses.{role}.metadata.min_num_clusters`, `max_num_clusters`, `auto_stop_mins`, `enable_serverless_compute`, `warehouse_type`, `creator_name`: warehouse settings.
- `online.warehouses.workspace_configuration.enabled_warehouse_types`, `enable_serverless_compute`: optional workspace settings.
- `online.statement_execution_on_rt.status`, `statement_id`, `rows[]`: serving statement result and provider row maps.
- `online.statement_execution_on_rt.rows[].preflight_ok`, `current_catalog`, `current_schema`: serving probe result and namespace.
- `online.system_table_permission.status`, `table`, `statement_id`, `reason`: system-table permission check.
- `online.query_history_api.status`, `statement_id`, `warehouse_id`, `status_value`, `reason`: Query History check.
- `online.materialized_view_create_compatibility.method`, `warehouse_id`, `status`, `statement_id`, `incrementalizable`: MV compatibility result.
- `online.materialized_view_create_compatibility.rows[]`, `error`: provider EXPLAIN row maps and optional error.
- `online.materialized_view_create_compatibility.rows[].plan`: provider compatibility plan text.
- `online.query_compatibility.method`, `online.query_compatibility.queries[]`: query compilation method and checks.
- `queries[].workload`, `query_number`, `status`, `statement_id`, `error`: per-query compilation fields.
- `online.tables.{role}.status`, `online.tables.{role}.errors[]`: table lookup result.
- `online.tables.{role}.metadata.full_name`, `name`, `catalog_name`, `schema_name`: table identity.
- `online.tables.{role}.metadata.table_type`, `data_source_format`, `storage_location`: table type, format, and location.
- `online.tables.{role}.metadata.created_at`, `updated_at`: epoch-millisecond metadata times.
- `online.predictive_optimization.status`: combined table result.
- `online.predictive_optimization.{role}.status`, `source`, `configured_value`, `effective_value`, `effectively_enabled`: per-table result.
- `online.predictive_optimization.{role}.inherited_from_type`, `inherited_from_name`: inheritance source.
- `online.metastore.metastore_id`, `cloud`, `region`, `storage_root_configured`, `status`: metastore metadata and optional failure status.
- `online.catalog_storage.name`, `storage_root`, `status`: catalog storage metadata and optional failure status.
- `online.tables.raw_storage_assumption.status`, `raw_storage_location_configured`, `catalog_managed_storage_configured`, `metastore_default_storage_configured`: storage-assumption result.
- `online.tables.raw_storage_assumption.uses_catalog_managed_storage`, `uses_metastore_default_storage`: normalized location matches.
- `online.target_contract.status`, `method`, `errors[]`: target contract result.
- `online.target_contract.statements.{check}.statement_id`, `row_count`: control statement evidence.
- `online.target_contract.observed.raw.format`, `num_files`, `size_in_bytes`, `clustering_columns[]`: raw metadata.
- `online.target_contract.observed.raw.history_versions[]`, `history_operations[]`, `source_features.{property}`: raw history and required features.
- `online.target_contract.observed.materialized_view.owner`, `clustering_columns[]`, `predictive_optimization`, `refresh_policy`, `refresh_schedule`: MV contract evidence.
- `online.target_contract.checks.{check}`: normalized contract-check result.
- `online.zerobus_endpoint.url`, `status`: endpoint and probe result.
- `online.zerobus_endpoint.tls.hostname`, `port`, `tls_version`, `latency_ms`: TLS evidence.
- `online.zerobus_service_principal.status`, `scoped_oauth_token_acquired`: scoped-token check.
- `online.zerobus_service_principal.required_privileges[]`: required grants.
- `required_privileges[].type`, `object_type`, `object_full_path`, `privileges[]`: grant target and privileges.

Status-bearing preflight objects may also include `error`, `errors[]`, `reason`, or `skipped`.

## `validation/query_context.json`

- `schema_version`, `status`: schema revision and orchestration status.
- `query_run_id`, `ingest_run_id`: correlated query and ingest identifiers.
- `started_at`, `finished_at`, `post_ingest_started_at`: orchestration and post-ingest bounds.
- `starting_provider_committed_rows`, `final_provider_committed_rows`: producer watermarks at query-window boundaries.
- `query_warehouse_id`, `warehouse_mode`, `query_size`: serving configuration.
- `catalog`, `schema`, `raw_table`, `mv_table`: target identity.
- `dashboard_interval_seconds`, `drilldown_interval_seconds`, `post_ingest_query_seconds`: workload timing.
- `query_window_scope`, `partial_ingest_window`: query scope and partial-window flag.
- `dashboard_iterations`, `drilldown_iterations`: completed iteration counts.
- `dashboard_process_status`, `drilldown_process_status`: child process statuses.

## `validation/validation_report_settled_<timestamp>.json`

- `schema_version`, `accepted`: schema revision and conjunction of all gate decisions.
- `window.since`, `window.until`, `window.duration_seconds`, `window.error`: normalized window and optional error.
- `configuration.expected_rows`: required source rows.
- `configuration.eps_bounds.minimum`, `configuration.eps_bounds.maximum`, `configuration.minimum_interval_pass_fraction`: ingest-rate criteria.
- `configuration.maximum_raw_observational_age_seconds`, `configuration.maximum_mv_observational_age_seconds`: freshness-age bounds.
- `configuration.freshness_startup_grace_seconds`, `configuration.cadence_tolerance_seconds`, `configuration.freshness_age_semantics`: timing criteria.
- `gates[]`: ordered shared `validation_gate` objects.
- `query_samples.dashboard`, `query_samples.drilldown`: workload sample reports also carried by their gates.
- `freshness_samples`: freshness sample report also carried by its gate.

### `gates[id=input_availability]`

- `evidence.paths.source_manifest`, `ingest_progress`, `ingest_metrics`, `dashboard`, `drilldown`, `freshness`, `evidence_summary`, `cost_summary`: validated input paths.
- `evidence.errors_by_input.{input}[]`: parse or read errors by input.

### `gates[id=source_manifest]`

- `evidence.expected_rows`, `evidence.actual_rows`, `evidence.excluded_files[]`: required rows, selected rows, and exclusions.
- `evidence.checks.total_rows_exact`, `quotes_0_flag_false`, `quotes_0_not_selected`, `manifest_sha256_present`, `tasks_present`: source checks.
- `evidence.checks.file_count_consistent`, `task_count_consistent`, `canonical_task_order`, `canonical_file_order`: count and ordering checks.
- `evidence.checks.task_rows_sum_exact`, `file_rows_sum_exact`: row-sum checks.

### `gates[id=producer_completion]`

- `evidence.provider_committed_rows`, `evidence.completed_tasks`, `evidence.expected_tasks`, `evidence.stream_count`: final producer totals.
- `evidence.checks.provider_committed_exact`, `logical_raw_exact`, `submitted_exact`: row checks.
- `evidence.checks.finished`, `not_running`, `clean_checkpoint`, `not_ambiguous`: lifecycle checks.
- `evidence.checks.no_terminal_error`, `no_errors`, `no_pending_batches`, `no_pending_rows`, `no_partial_tasks`: unresolved-state checks.
- `evidence.checks.fresh_table_baseline`, `manifest_hash_matches`, `source_total_matches`: source-identity checks.
- `evidence.checks.all_tasks_completed`, `all_streams_clean`: completion checks.

### `gates[id=ingest_rate]`

- `evidence.bounds.minimum`, `evidence.bounds.maximum`, `evidence.average_provider_committed_rows_per_sec`, `evidence.average_passed`: bounds and cumulative result.
- `evidence.intervals[]`: per-metrics-record rate decisions.
- `intervals[].index`, `ignored`, `reason`, `eps`, `source`, `passed`: record index, exclusion, rate, derivation, and decision.
- `evidence.interval_pass_count`, `evidence.interval_count`, `evidence.interval_pass_fraction`, `evidence.minimum_pass_fraction`: aggregate interval result.

### `gates[id=dashboard_queries]` and `gates[id=drilldown_queries]`

The same object is repeated at `query_samples.{workload}`.

- `accepted_indices[]`, `accepted_count`, `rejected[]`, `rejected_count`: accepted and rejected record summaries.
- `rejected[].index`, `rejected[].reasons[]`, `active_invalid[].index`, `active_invalid[].reasons[]`: record-level failures.
- `self_overlap_failures[]`: pairs of overlapping record indices.
- `cadence_failures[].previous_index`, `cadence_failures[].index`, `cadence_failures[].scheduled_delta_seconds`: schedule failures.
- `raw_rows_monotonic_failures[]`: pairs of decreasing row-watermark indices.
- `expected_query_count`, `expected_interval_seconds`, `cadence_tolerance_seconds`: workload shape and timing.

### `gates[id=query_raw_rows_monotonic]`

- `evidence.sample_count`: combined query sample count.
- `evidence.failures[].previous`, `current`, `previous_raw_rows`, `current_raw_rows`: cross-workload row-watermark decrease.

### `gates[id=freshness]`

The same object is repeated at `freshness_samples`.

- `accepted_indices[]`, `accepted_count`, `rejected[]`, `rejected_count`: accepted and rejected record summaries.
- `rejected[].index`, `rejected[].reasons[]`, `invalid_eligible[].index`, `invalid_eligible[].reasons[]`: record-level failures.
- `startup_grace_seconds`, `active_measurement_until`: measurement bounds.
- `maximum_raw_observational_age_seconds`, `maximum_mv_observational_age_seconds`, `age_interpretation`: age criteria.

### `gates[id=row_reconciliation]`

- `evidence.expected_rows`: required row count.
- `evidence.counters.source_manifest_total_rows`, `producer_provider_committed_rows`, `system_zerobus_provider_committed_records`: reconciled row counters.
- `evidence.optional_describe_detail_raw_numRows`: optional table metadata count.
- `evidence.provider_errors`, `evidence.zerobus_stream_error_count`: provider error totals.

### `gates[id=evidence_completeness]`

- `evidence.complete`: overall evidence-summary completeness.
- `evidence.required_dataset_completeness.required[]`, `succeeded[]`, `failed[]`, `complete`: required dataset outcomes.
- `evidence.optional_dataset_errors[]`: failed optional datasets.

### `gates[id=mv_incrementality]`

- `evidence.maintenance_types[]`, `evidence.maintenance_classes[]`, `evidence.full_refresh_count`: maintenance evidence.

### `gates[id=warehouse_configuration]`

- `evidence.expected_warehouse_ids[]`, `evidence.snapshot_warehouse_ids[]`, `evidence.changed_or_unproven_warehouse_ids[]`: warehouse identity and stability evidence.
- `evidence.accepted_runner_config_count`, `evidence.accepted_runner_config_sha256[]`, `evidence.missing_runner_configs[]`: runner snapshot evidence.

### `gates[id=cost_completeness]`

This gate summarizes an external cost input; no cost artifact is packaged.

- `evidence.complete`: external cost-summary completeness.
- `evidence.completeness.sources_complete`, `pricing_complete`, `classification_complete`, `single_primary_currency`, `canonical_primary_total_present`, `complete`: completeness checks.
- `evidence.completeness.ambiguous_price_usage_row_indices[]`, `unpriced_usage_row_indices[]`, `unknown_usage_row_indices[]`, `invalid_quantity_usage_row_indices[]`: exception rows.
- `evidence.canonical_undiscounted_primary_total`, `canonical_undiscounted_fresh_path_total`, `canonical_undiscounted_query_serving_total`, `unknown_total`: shared `money_total` objects.
- `evidence.unpriced_total.list_cost`, `usage_quantity_by_unit.{unit}`, `usage_row_indices[]`: unpriced-usage summary.
- `evidence.beta_scenario`: separately labeled noncanonical pricing scenario.
- `beta_scenario.discount_rate`, `applies_only_to`, `canonical_total_unchanged`: scenario parameters and canonical-preservation flag.
- `beta_scenario.rt_serving_undiscounted`, `rt_serving_discounted`, `scenario_primary_total`, `canonical_undiscounted_primary_total`: scenario `money_total` objects.

### `gates[id=predictive_optimization]`

- `evidence.operation_count`, `evidence.failed_operation_count`, `evidence.failed_operation_row_indices[]`: provider operation and failure totals.

## `validation/manifest.json`

- `schema_version`, `generated_at`, `status`, `run_id`: schema revision, generation time, package stage, and run identifier.
- `artifact_count`, `result_size_bytes`: listed-file count and summed size.
- `artifacts[]`: package file records; the manifest does not list itself.
- `artifacts[].path`, `artifacts[].size_bytes`, `artifacts[].sha256`: package-relative path, exact size, and byte hash.
- `source.total_rows`, `source.selected_file_count`, `source.selected_row_group_count`: selected source totals.
- `source.quotes_0_parquet_included`, `source.excluded_files[]`: source-file inclusion evidence.
- `source.manifest_sha256`: canonical source-selection fingerprint.
- `source.hash_validation_status`, `source.hash_validation_file_count`: post-run source-hash result.
- `runtime_state.included_in_results`: runtime-state inclusion flag.
- `runtime_state.files_excluded[]`: excluded runtime-state classes, including source manifests, ingest ledgers, full query-audit sidecars, raw provider exports, and process logs.
- `sanitization.profile`: publication sanitization profile.
- `sanitization.metrics_changed`: whether sanitization altered benchmark
  measurements; public packages require `false`.
- `sanitization.redacted_classes[]`: classes of deployment-specific metadata
  replaced by placeholders.
