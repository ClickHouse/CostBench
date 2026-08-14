// redshift_sg -> attached to the Serverless workgroup (Kafka CLIENT). Inbound 5439 from your IP.
// (The self-managed-broker SG was removed 2026-08-06: we pivoted to Amazon MSK — Redshift rejected
//  the self-signed TLS cert of the single-broker Kafka. The MSK SG lives in msk.tf.)

resource "aws_security_group" "redshift" {
  name_prefix = "${var.name_prefix}-rs-"
  vpc_id      = data.aws_vpc.default.id
  description = "Redshift Serverless workgroup (Kafka client)"

  ingress {
    description = "Redshift SQL from your machine"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = var.my_ip_cidr != "" ? [var.my_ip_cidr] : ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle { create_before_destroy = true }
}
