output "redshift_workgroup_endpoint" {
  description = "WRITER endpoint — MSK ingest + MV maintenance (host for a SQL client)"
  value       = try(aws_redshiftserverless_workgroup.this.endpoint[0].address, null)
}

// ---- reader (consumer) side: compute-isolated read workgroup + the GUIDs the datashare needs ----
output "redshift_reader_endpoint" {
  description = "READER endpoint — point the dashboard/drilldown runners here"
  value       = try(aws_redshiftserverless_workgroup.reader.endpoint[0].address, null)
}

output "writer_namespace_id" {
  description = "producer namespace GUID — used in: CREATE DATABASE ... FROM DATASHARE ... OF NAMESPACE '<this>'"
  value       = try(aws_redshiftserverless_namespace.this.namespace_id, null)
}

output "reader_namespace_id" {
  description = "consumer namespace GUID — used in: GRANT USAGE ON DATASHARE ... TO NAMESPACE '<this>'"
  value       = try(aws_redshiftserverless_namespace.reader.namespace_id, null)
}

output "redshift_db_name" {
  value = var.redshift_db_name
}

output "msk_bootstrap_plaintext" {
  description = "producer --bootstrap target (plaintext :9092)"
  value       = try(aws_msk_cluster.this.bootstrap_brokers, null)
}

output "msk_bootstrap_tls" {
  description = "Redshift external schema URI (TLS :9094, AUTHENTICATION none)"
  value       = try(aws_msk_cluster.this.bootstrap_brokers_tls, null)
}

output "msk_security_group_id" {
  value = aws_security_group.msk.id
}

output "redshift_security_group_id" {
  value = aws_security_group.redshift.id
}

output "default_vpc_id" {
  description = "launch your broker EC2 in THIS VPC so Redshift can reach its private IP"
  value       = data.aws_vpc.default.id
}

output "kafka_port" {
  value = var.kafka_port
}

output "region" {
  description = "region resources are created in — confirm this is eu-west-2 (the box's region)"
  value       = var.region
}
