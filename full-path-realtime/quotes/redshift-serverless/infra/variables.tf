variable "region" {
  type        = string
  description = "AWS region — MUST match the box (eu-west-2) so Redshift is in the box's VPC"
  default     = "eu-west-2"
}

# Safety: apply fails if the resolved default VPC isn't this one (i.e. wrong region / wrong account).
# Set to "" to skip the check (e.g. running in a different sandbox).
variable "expected_vpc_id" {
  type    = string
  default = "vpc-09739962dc6ea0853" # the quotes box's default VPC in eu-west-2
}

variable "owner" {
  type    = string
  default = "costbench"
}

variable "name_prefix" {
  type    = string
  default = "cb-quotes-rt"
}

// ---- Redshift Serverless — WRITER (streaming ingest + both child MV refreshes) ----
// 2026-08-10: 8 -> 128. At 128 RPU the MINIMAL streaming MV (JSON_PARSE only, 1 parse/record)
//   sustained 1M EPS with offset lag 0 and ~6-9 s freshness.
// 2026-08-12: tried 128 -> 256, then REVERTED to 128. Promoting `sym` and `t` to typed columns so
//   quotes_streamed can carry SORTKEY (sym, t) forces INLINE extraction (a streaming MV rejects a
//   parse-once subquery), i.e. 3 parses per record. Measured against a 1.00M/s producer:
//       unsorted landing MV, 128 RPU : ~1.06M rows/s consumed, lag flat 8-10 s      OK
//       sorted (3 parses),  128 RPU : ~630K rows/s, latency +25.5 s/min (monotonic) FAIL
//       sorted (3 parses),  256 RPU : ~620K rows/s, latency +27   s/min (monotonic) FAIL
//   Doubling RPU bought ~0% ingest throughput => the wall is NOT compute. Redshift runs one stream
//   consumer PER TOPIC PARTITION, and the topic had only 6 (vs Snowflake's 8 Snowpipe channels), so
//   consumer parallelism was the suspected cap. Next test raises the topic to 24 partitions at 128 RPU
//   (single variable changed). Keep 128 until a measurement justifies more.
variable "redshift_base_rpu" {
  type        = number
  description = "writer base RPU (min 8). 128 sustains 1M EPS on the minimal landing MV; 256 gave no ingest gain."
  default     = 128
}

// Kept == base during characterization so a measured throughput/latency maps to a KNOWN capacity
// instead of silently autoscaling mid-run (same rule as the reader).
variable "redshift_max_rpu" {
  type        = number
  description = "writer max RPU. Keep == base during characterization (controlled capacity)."
  default     = 256
}

// ---- Reader (consumer) workgroup — compute isolation for the read queries (redshift_reader.tf) ----
// Sized independently of the writer: it only serves dashboard/drilldown over the datashare, no ingest.
// Sizing rule: start small and step up (32 -> 64 ...) until the reads stop being capacity-bound;
// keep max == base during characterization so a measured latency maps to a KNOWN RPU size.
// NOTE: Redshift Serverless bills RPU-seconds per workgroup, so the reader is its own cost line.
variable "redshift_reader_base_rpu" {
  type        = number
  description = "base RPU for the read-only workgroup (min 8; step 8). Start 32."
  default     = 32
}

variable "redshift_reader_max_rpu" {
  type        = number
  description = "max RPU for the reader. Keep == base during characterization (controlled capacity)."
  default     = 32
}

variable "redshift_db_name" {
  type    = string
  default = "quotes"
}

variable "redshift_admin_user" {
  type    = string
  default = "cbadmin"
}

variable "redshift_admin_password" {
  type        = string
  sensitive   = true
  description = "set via TF_VAR_redshift_admin_password; never commit"
}

// ---- access ----
variable "my_ip_cidr" {
  type        = string
  description = "your IP/32 — allowed to reach Redshift SQL (5439) and SSH the broker (22)"
  default     = ""
}

variable "kafka_port" {
  type    = number
  default = 9092
}

# ---- Amazon MSK (streaming source Redshift can trust over TLS) ----
variable "kafka_version" {
  type    = string
  default = "3.6.0"
}
variable "msk_instance_type" {
  type = string
  # Graviton m7g beats m5 on price/perf (per AWS MSK Sizing sheet + user guidance, 2026-08-11):
  # kafka.m7g.xlarge Standard = $0.408/broker/hr (us-east-1), 24 MB/s ingest entitlement, 4 vCPU.
  # 3 brokers comfortably cover our real ~30 MB/s lz4-compressed 1M-EPS load (the sheet's 9-broker
  # figure was for 100 MB/s uncompressed). Inter-broker (cross-AZ) replication is FREE on MSK
  # provisioned, so broker-hours dominate cost — NOT the sheet's phantom cross-AZ transfer line.
  # m5.xlarge -> m7g.xlarge is same-vCPU: a supported in-place UpdateBrokerType (bootstrap unchanged).
  default = "kafka.m7g.xlarge"
}
variable "msk_broker_count" {
  type    = number
  default = 3 # must be a multiple of the 3 client subnets/AZs
}
variable "msk_volume_size" {
  type = number
  # GB per broker. 100 was too small: at 1M EPS x ~138 B/row x RF3 the disks filled at ~720M rows
  # and the brokers wedged (2026-08-10). The REAL fix is a bounded TOPIC retention (retention.ms /
  # retention.bytes — set on topic creation, not here; Redshift consumes live so we needn't retain).
  # Keep headroom too: 30-min retention at 1M EPS RF3 ~= 250 GB/broker peak.
  default = 500
}
