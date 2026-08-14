// redshift_sg -> attached to the Serverless workgroups.
// (The self-managed-broker SG was removed 2026-08-06: we pivoted to Amazon MSK — Redshift rejected
//  the self-signed TLS cert of the single-broker Kafka. The MSK SG lives in msk.tf.)
//
// SECURITY FIX 2026-08-14: this rule used to be
//     cidr_blocks = var.my_ip_cidr != "" ? [var.my_ip_cidr] : ["0.0.0.0/0"]
// i.e. it fell back to THE WHOLE INTERNET whenever my_ip_cidr was left at its "" default — which it
// was for this entire benchmark. Combined with publicly_accessible = true on the writer, port 5439
// was reachable from anywhere, and the endpoint/username appear in the committed runbook and logs.
// The default is now the VPC CIDR (which is all the benchmark actually needs: the runners execute on
// the in-VPC producer box). Laptop access is opt-in via my_ip_cidr and is ADDITIVE, never a fallback.

resource "aws_security_group" "redshift" {
  name_prefix = "${var.name_prefix}-rs-"
  vpc_id      = data.aws_vpc.default.id
  description = "Redshift Serverless workgroups - in-VPC clients only by default"

  // Always: in-VPC clients (the producer box runs the refresh controller + read runners).
  ingress {
    description = "Redshift SQL from inside the VPC"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }

  // Opt-in only: set my_ip_cidr (e.g. "203.0.113.5/32") to reach the workgroup from your machine.
  // Note this ALSO requires the workgroup to be publicly_accessible; the writer is now private, so
  // this rule is for a deliberate, temporary exception.
  dynamic "ingress" {
    for_each = var.my_ip_cidr != "" ? [var.my_ip_cidr] : []
    content {
      description = "Redshift SQL from an explicitly allowed address"
      from_port   = 5439
      to_port     = 5439
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle { create_before_destroy = true }
}
