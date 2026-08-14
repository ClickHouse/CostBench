// Reader (consumer) Redshift Serverless namespace + workgroup — COMPUTE ISOLATION for T2.
//
// WHY: the writer workgroup (redshift.tf) consumes MSK, auto-refreshes the streaming MV and
// maintains the rollup. Running dashboard/drilldown reads on that SAME workgroup mixes ingest and
// query compute, which breaks the isolation the other vendors have (Snowflake T2 reads on a separate
// interactive warehouse; ClickHouse reads on its own service). So reads get their own compute here.
//
// A Serverless NAMESPACE can be associated with only ONE workgroup, so isolating compute means a
// second namespace as well — the two are linked by a LIVE DATASHARE (no copy, no ETL):
//
//   writer ns/wg  --(datashare: quotes + quotes_daily)-->  reader ns/wg  (dashboard + drilldown)
//
// Docs: serverless workgroup/namespace  https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-workgroup-namespace.html
//       serverless data sharing         https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-datasharing.html
//       sharing materialized views      https://docs.aws.amazon.com/redshift/latest/dg/datashare-views.html
//       (MVs CAN be added to a datashare; consumers get incremental refresh of shared MVs)
//
// The datashare itself is SQL, not terraform — see sql/setup_datashare.sql (it needs the namespace
// GUIDs, exposed as the outputs writer_namespace_id / reader_namespace_id).

resource "aws_redshiftserverless_namespace" "reader" {
  namespace_name      = "${var.name_prefix}-reader-ns"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
}

resource "aws_redshiftserverless_workgroup" "reader" {
  namespace_name = aws_redshiftserverless_namespace.reader.namespace_name
  workgroup_name = "${var.name_prefix}-reader-wg"

  // Characterization rule (per review): pin max == base so the reader's capacity is CONTROLLED and
  // the measured latency belongs to a known RPU size, instead of silently autoscaling mid-run.
  base_capacity = var.redshift_reader_base_rpu
  max_capacity  = var.redshift_reader_max_rpu

  // MUST be false: a publicly-accessible consumer cannot read datashare objects. Verified live
  // 2026-08-12 — with publicly_accessible = true every query on the shared objects failed with
  // "Publicly accessible consumer cannot access object in the database." Private is fine for us:
  // the runners execute on the producer EC2, which is in this same VPC, so it reaches the reader
  // over the private endpoint.
  publicly_accessible = false
  subnet_ids          = local.redshift_subnets
  security_group_ids  = [aws_security_group.redshift.id]

  // No enhanced_vpc_routing: the reader never talks to MSK — it only serves queries over the
  // datashare. (The writer needs it to reach the in-VPC MSK brokers.)
}
