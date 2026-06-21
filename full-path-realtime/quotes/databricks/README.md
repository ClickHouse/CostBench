# Stockhouse — Databricks Ingestion

End-to-end steps to spin up an EC2 box, download the Stockhouse dataset, ingest it into Databricks, and run benchmark queries.

## 1. Spin up an EC2 instance

```bash
# Create key pair
aws ec2 create-key-pair \
  --region eu-west-1 \
  --key-name benchmarking-eu-west-1 \
  --query 'KeyMaterial' \
  --output text > ~/.ssh/benchmarking-eu-west-1.pem
chmod 600 ~/.ssh/benchmarking-eu-west-1.pem

# Get default VPC and subnet
VPC_ID=$(aws ec2 describe-vpcs --region eu-west-1 \
  --filters "Name=isDefault,Values=true" \
  --query 'Vpcs[0].VpcId' --output text)
SUBNET_ID=$(aws ec2 describe-subnets --region eu-west-1 \
  --filters "Name=vpcId,Values=$VPC_ID" \
  --query 'Subnets[0].SubnetId' --output text)

# Security group (SSH from your IP only)
MY_IP=$(curl -s https://checkip.amazonaws.com)
SG_ID=$(aws ec2 create-security-group \
  --region eu-west-1 \
  --group-name benchmarking-sg \
  --description "Benchmarking instance" \
  --vpc-id $VPC_ID \
  --query 'GroupId' --output text)
aws ec2 authorize-security-group-ingress \
  --region eu-west-1 \
  --group-id $SG_ID \
  --protocol tcp --port 22 --cidr $MY_IP/32

# Launch m5.16xlarge (64 vCPU, 256 GB RAM) with 2 TB gp3
aws ec2 run-instances \
  --region eu-west-1 \
  --image-id ami-0daff188b5216c5f0 \
  --instance-type m5.16xlarge \
  --key-name benchmarking-eu-west-1 \
  --security-group-ids $SG_ID \
  --subnet-id $SUBNET_ID \
  --associate-public-ip-address \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":2000,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=benchmarking-databricks}]'
```

Get the public IP once running:

```bash
aws ec2 describe-instances \
  --region eu-west-1 \
  --instance-ids <instance-id> \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text
```

SSH in:

```bash
ssh -i ~/.ssh/benchmarking-eu-west-1.pem ubuntu@<public-ip>
```

**Cost:** ~$3.07/hr — terminate when done.

## 2. Set up Python environment on EC2

```bash
sudo apt-get update -q
sudo apt-get install -y python3.12-venv unzip -q
python3 -m venv ~/venv
~/venv/bin/pip install databricks-sdk pyarrow
```

Install AWS CLI:

```bash
curl -s 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o awscliv2.zip
unzip -q awscliv2.zip
sudo ./aws/install
```

## 3. Download the dataset

Copy `download_stockhouse.sh` to the instance and run it with your AWS credentials exported:

```bash
scp -i ~/.ssh/benchmarking-eu-west-1.pem download_stockhouse.sh ubuntu@<public-ip>:~/

# On the EC2 instance:
chmod u+x ~/download_stockhouse.sh
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."

bash ~/download_stockhouse.sh        # all files
bash ~/download_stockhouse.sh 10     # first 10 files only
```

Data is saved to `~/data/stockhouse/`. Files are named `quotes_YYYY-MM-DD.parquet` (daily files) plus `quotes_0.parquet` (10B row historical file, excluded by the script).

### Dataset summary

| File | Rows | Size |
|---|---|---|
| quotes_0.parquet | 10,000,000,000 | 45 GB |
| quotes_YYYY-MM-DD.parquet (10 files) | ~5,030,000,000 | ~7 GB total |

Note: some daily files may be empty (0 rows) for days with no data.

## 4. Create Databricks schema and table

Run `create.sql` in the Databricks SQL editor, or via the API:

```bash
export DATABRICKS_TOKEN="dapi..."

# Create schema
curl -s -X POST \
  "https://dbc-37858cc0-7910.cloud.databricks.com/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"warehouse_id":"47486f539b196632","statement":"CREATE SCHEMA IF NOT EXISTS workspace.benchmarking","wait_timeout":"50s"}'

# Create table
curl -s -X POST \
  "https://dbc-37858cc0-7910.cloud.databricks.com/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"warehouse_id":"47486f539b196632","statement":"<contents of create.sql>","wait_timeout":"50s"}'
```

## 5. Copy ingestion script to EC2

```bash
scp -i ~/.ssh/benchmarking-eu-west-1.pem \
  ingest.py ubuntu@<public-ip>:~/ingest.py
```

## 6. Run ingestion

SSH in and run inside a `screen` session:

```bash
screen -S ingest

export DATABRICKS_HOST="https://dbc-37858cc0-7910.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/47486f539b196632"
export DATABRICKS_TOKEN="dapi..."

~/venv/bin/python3 ~/ingest.py \
    --dir ~/data/stockhouse \
    --parallel 32 \
    --row-groups-per-insert 4 2>&1 | tee ingest.log
```

Detach with `Ctrl-A D`. Reattach with `screen -r ingest`.

### Key parameters

| Flag | Default | Description |
|---|---|---|
| `--parallel` | required | Number of worker processes |
| `--row-groups-per-insert` | 1 | Batch N row groups per INSERT — reduces Delta transaction log pressure |
| `--target-rps` | 0 (unlimited) | Max rows/s across all workers |
| `--max-files` | all | Limit to first N files |
| `--live-eps-interval` | 30s | Seconds between live row-count samples |

### Observed throughput

- ~32 workers → ~600-900k rows/s on a Small warehouse (3 clusters)
- Bottleneck is Delta Lake transaction log contention under concurrent writes
- Read times drop from ~1.2s (single 45GB file) to ~110ms (daily files) due to reduced file contention

### Ingestion results (dated files, 10 files)

| Metric | Value |
|---|---|
| Duration | 89.5 min |
| Rows ingested | 5,029,995,919 |
| Average throughput | ~937k rows/s |
| Errors | 0 |

## 7. Run benchmark queries

Copy the query runner and queries directory to EC2:

```bash
scp -i ~/.ssh/benchmarking-eu-west-1.pem run_queries.py ubuntu@<public-ip>:~/
scp -i ~/.ssh/benchmarking-eu-west-1.pem queries/*.sql ubuntu@<public-ip>:~/queries/
```

Run on an interval, recording results to CSV:

```bash
~/venv/bin/python3 ~/run_queries.py \
    --queries-dir ~/queries \
    --interval 60 \
    --output ~/results.csv
```

### Queries

| File | Description |
|---|---|
| `spread_by_symbol.sql` | Avg/min/max bid-ask spread per symbol |
| `top_symbols_by_volume.sql` | Quote count per symbol |
| `bid_ask_imbalance.sql` | Bid vs ask size imbalance per symbol |
| `quote_activity_over_time.sql` | Quote count bucketed by hour |

### Benchmark results (Small warehouse, 1 cluster)

Query times during and after ingestion at 6.59B total rows:

| Row Count | bid_ask_imbalance | quote_activity_over_time | spread_by_symbol | top_symbols_by_volume |
|---|---|---|---|---|
| 1.78B | 3.9s | 3.0s | 4.4s | 2.9s |
| 2.50B | 4.3s | 3.1s | 4.5s | 3.1s |
| 3.24B | 5.4s | 3.2s | 6.0s | 4.8s |
| 4.02B | 6.7s | 3.1s | 7.2s | 4.7s |
| 4.84B | 7.6s | 3.4s | 8.2s | 5.3s |
| 5.69B | 8.7s | 4.6s | 8.9s | 5.8s |
| 6.59B (during ingest) | 9.6s | 5.2s | 9.5s | 6.1s |
| 6.59B (ingest complete) | **0.5s** | **0.5s** | **0.5s** | **0.5s** |

Queries degrade gradually during ingest as the cache is invalidated by concurrent writes, then drop to ~0.5s once ingest completes and the cache stabilises.

## 8. Terminate the instance

```bash
aws ec2 terminate-instances --region eu-west-1 --instance-ids <instance-id>
```
