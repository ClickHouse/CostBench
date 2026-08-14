-- =============================================================================
-- Live datashare: WRITER namespace (MSK ingest + MV maintenance) -> READER namespace (queries).
-- This is what makes the T2 Redshift run compute-isolated, matching Snowflake T2 (separate
-- interactive read warehouse) and ClickHouse (separate read service). No copy, no ETL: the reader
-- queries the producer's data live, on its OWN RPUs, so read latency and read cost are attributable.
--
-- Docs: serverless data sharing  https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-datasharing.html
--       sharing views/MVs        https://docs.aws.amazon.com/redshift/latest/dg/datashare-views.html
--
-- Namespace GUIDs come from terraform:  terraform output writer_namespace_id / reader_namespace_id
-- (or SQL: SELECT current_namespace;  on each side). Substitute below before running.
--
-- RUN ORDER:  part 1 on the WRITER endpoint, then part 2 on the READER endpoint.
-- =============================================================================

-- =====================  PART 1 — on the WRITER (producer)  ===================
-- Re-runnable: drop first so re-creating after a schema change is clean.
-- NOTE: Redshift does NOT support `IF EXISTS` on DROP DATASHARE / DROP DATABASE (syntax error) —
-- run the DROP on its own and ignore "does not exist" the first time.
DROP DATASHARE quotes_share;
CREATE DATASHARE quotes_share;

-- The schema must be added before its objects.
ALTER DATASHARE quotes_share ADD SCHEMA public;

-- Share ALL THREE query targets — the T2 read workload measures all of them from the reader:
--   quotes_streamed  live SUPER payload   -> queries_drilldown_super.sql
--   quotes_typed     typed columns        -> queries_drilldown_typed.sql
--   quotes_daily     (sym, day) rollup    -> queries_dashboard.sql
-- The SUPER landing MV is shared deliberately here (unlike a production setup where readers would
-- only see the typed layer): the benchmark's whole point is comparing SUPER vs typed read cost.
-- VERIFIED 2026-08-12: streaming ingestion MVs and MV-on-MV children CAN both be added to a
-- datashare (SVV_DATASHARE_OBJECTS reports them as object_type 'materialized view').
ALTER DATASHARE quotes_share ADD TABLE public.quotes_streamed;  -- streaming MV, SUPER payload
ALTER DATASHARE quotes_share ADD TABLE public.quotes_typed;     -- typed rows (sym, bx, bp, ... z)
ALTER DATASHARE quotes_share ADD TABLE public.quotes_daily;     -- (sym, day) rollup

-- Grant to the consumer namespace GUID (terraform output reader_namespace_id).
GRANT USAGE ON DATASHARE quotes_share TO NAMESPACE '<READER_NAMESPACE_ID>';

-- checks
SHOW DATASHARES;
SELECT * FROM SVV_DATASHARE_OBJECTS WHERE share_name = 'quotes_share';


-- =====================  PART 2 — on the READER (consumer)  ===================
-- Mount the share as a local database. Queries then run as quotes_shared.public.<obj>, on the
-- reader's own RPUs, while the writer keeps ingesting.
-- PREREQUISITE: the reader workgroup must NOT be publicly accessible, or every query on a shared
-- object fails with "Publicly accessible consumer cannot access object in the database."
-- (see infra/redshift_reader.tf: publicly_accessible = false).
DROP DATABASE quotes_shared;   -- no IF EXISTS support; ignore "does not exist" on first run
CREATE DATABASE quotes_shared FROM DATASHARE quotes_share OF NAMESPACE '<WRITER_NAMESPACE_ID>';

-- Convenience: let the read queries use unqualified names (quotes / quotes_daily) exactly as they
-- do on the writer, so ONE set of query files works against both endpoints.
CREATE EXTERNAL SCHEMA IF NOT EXISTS shared_public
FROM REDSHIFT DATABASE 'quotes_shared' SCHEMA 'public';

-- checks — the reader should see live row counts that climb while the writer ingests/refreshes.
-- Validate the STREAMING MV is queryable through the share BEFORE the long run: it is the one
-- object whose shareability isn't spelled out in the docs (we verified it works 2026-08-12).
SELECT COUNT(*) FROM shared_public.quotes_streamed;
SELECT COUNT(*) FROM shared_public.quotes_typed;
SELECT COUNT(*) FROM shared_public.quotes_daily;

-- Point the runners at the shared objects, e.g.
--   RS_HOST=<reader endpoint> RS_MV_TABLE=shared_public.quotes_daily \
--     python runner_redshift.py --role dashboard ...
-- (the query files use unqualified names, so set search_path or run them against shared_public).
