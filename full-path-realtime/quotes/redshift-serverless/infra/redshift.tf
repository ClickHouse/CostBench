// Redshift Serverless namespace + workgroup, in the default VPC. No IAM role needed for the Kafka
// external schema when AUTHENTICATION none is used (plaintext, in-VPC — fine for a benchmark).

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = "${var.name_prefix}-ns"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = "${var.name_prefix}-wg"
  base_capacity  = var.redshift_base_rpu
  max_capacity   = var.redshift_max_rpu

  // 2026-08-14: was `true`. Set to false so there is NO internet-facing endpoint. The data is
  // unaffected — public access is a workgroup/network property, while the MVs and their 113B rows
  // live in the namespace. The reader workgroup has always run private and queries the same data
  // fine, so this is proven. Consequence: the writer is reachable only from inside the VPC (the
  // producer box) or via the AWS-hosted Query Editor v2 — not from a laptop over the internet.
  publicly_accessible = false

  subnet_ids         = local.redshift_subnets
  security_group_ids = [aws_security_group.redshift.id]

  # REQUIRED so Redshift routes through the VPC to reach the in-VPC broker/MSK private endpoints.
  # (Enabled out-of-band via CLI on 2026-08-06; declared here so `apply` doesn't revert it to false.)
  enhanced_vpc_routing = true

  # Guardrail: abort if we resolved the wrong VPC (wrong region/account) — Redshift must be in the
  # box's VPC to reach the broker's private IP.
  lifecycle {
    precondition {
      condition     = var.expected_vpc_id == "" || data.aws_vpc.default.id == var.expected_vpc_id
      error_message = "Resolved default VPC ${data.aws_vpc.default.id} != expected ${var.expected_vpc_id}. Wrong region (${var.region}) or account? Redshift must be in the box's VPC."
    }
  }
}
