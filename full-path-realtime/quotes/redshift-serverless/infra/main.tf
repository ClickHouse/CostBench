// Real-time quotes benchmark — Redshift Serverless, ingesting from a SELF-MANAGED single-broker
// Kafka running on a producer EC2 you provision (closest analog to the Snowflake/ClickHouse boxes).
// FIRST CUT — review with `terraform plan` before `apply`; not yet applied/tested.
// Terraform provisions ONLY: Redshift Serverless + security groups. You launch the Kafka/producer
// EC2 (see ../broker + RUNBOOK.md) and attach the broker security group this outputs.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project = "costbench-quotes-realtime"
      vendor  = "redshift-serverless"
      owner   = var.owner
    }
  }
}

// Default VPC — the Redshift workgroup and your broker EC2 must share it so Redshift can reach the
// broker's private IP.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  // Redshift Serverless needs >=3 subnets across >=3 AZs.
  redshift_subnets = slice(data.aws_subnets.default.ids, 0, 3)
}
