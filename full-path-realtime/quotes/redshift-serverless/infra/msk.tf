// Amazon MSK (provisioned) — the TLS stream source Redshift streaming ingestion can trust.
// WHY: the self-managed single-broker Kafka only offered a self-signed cert, which Redshift rejects
// at the TLS handshake ("SSL handshake failed"). MSK presents Amazon-issued broker certs that
// Redshift trusts, so `AUTHENTICATION none` (one-way TLS) works. Same VPC/subnets as the workgroup
// and the producer box, so everything routes in-VPC.
//
// Data path (mirrors the self-managed design, minus the cert problem):
//   producer box -> MSK PLAINTEXT :9092   (fast, no TLS overhead on the write side)
//   Redshift     -> MSK TLS      :9094    (AUTHENTICATION none; Amazon-trusted cert)

resource "aws_security_group" "msk" {
  name_prefix = "${var.name_prefix}-msk-"
  vpc_id      = data.aws_vpc.default.id
  description = "MSK cluster - in-VPC Kafka clients (producer + Redshift)"

  ingress {
    description = "Kafka plaintext (producer) from VPC"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }
  ingress {
    description = "Kafka TLS (Redshift + producer) from VPC"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }
  ingress {
    description = "broker-to-broker"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle { create_before_destroy = true }
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name_prefix}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.msk_broker_count // must be a multiple of the AZ/subnet count (3)

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = local.redshift_subnets // 3 subnets / 3 AZs — same as the workgroup + box
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = var.msk_volume_size
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT" // expose BOTH: TLS :9094 (Redshift) + plaintext :9092 (producer)
      in_cluster    = true
    }
  }

  client_authentication {
    unauthenticated = true // no client auth; Redshift connects with AUTHENTICATION none over trusted TLS
  }
}
