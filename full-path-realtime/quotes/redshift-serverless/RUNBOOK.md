# RUNBOOK — Redshift Serverless real-time quotes pipeline (reach 1M EPS)

Architecture: **producer EC2 (the existing quotes box) → Amazon MSK (TLS-capable Kafka) → Redshift
Serverless** streaming materialized view. Producer writes to MSK over **plaintext :9092**; Redshift
consumes over MSK's **Amazon-trusted TLS :9094** (`AUTHENTICATION none`). Everything is in the box's
default VPC. Use a **dedicated benchmark account**, not prod.

## Status — where we are (updated 2026-08-12)

**Goal: achieve and *sustain* 1M EPS end-to-end** (producer → MSK → Redshift streaming MV) and serve
the read workload from isolated compute.

### Current state — the pipeline works end to end
- ✅ **1M EPS sustained**: producer 1,000,465 rows/s avg; MSK ingress **~999K EPS / ~28.5 MB/s
  compressed**; `quotes_streamed` consumes ~989K rows/s with **offset lag 0** and ~6–9 s freshness
  at the **128 RPU** base. Peak validated to ~2.6B rows.
- ✅ **MSK**: `cb-quotes-rt-msk` ACTIVE, **3× `kafka.m7g.xlarge`** (in-place `update-broker-type`
  from m5.xlarge on 2026-08-12), 3 AZ, RF=3, 500 GB/broker. Topic `quotes`: 6 partitions,
  **`retention.ms=1800000` + `retention.bytes=32 GB/partition`** (the disk-full fix).
- ✅ **Three-object fork deployed** — `quotes_streamed` (AUTO REFRESH) → `quotes_typed` +
  `quotes_daily`, both refreshed manually with `RESTRICT`. **Both verified incremental**; zero
  `Manual` refreshes of the streaming MV; `SYS_STREAM_SCAN_ERRORS` = 0.
- ✅ **Read suites validated** on 571.9M quiesced rows: SUPER vs typed drilldown return **identical
  results**, typed **1.7–2.3× faster**. Dashboard queries ~0.04–0.12 s on the rollup.
- ✅ **Compute isolation PROVEN (2026-08-12)** — reader workgroup `cb-quotes-rt-reader-wg` (namespace
  `cb-quotes-rt-reader-ns`, **32 RPU, `max = base`**, `publicly_accessible = false`) reads all three
  objects through the live datashare `quotes_share`, with counts matching the writer exactly
  (571,895,549 / 571,895,549 / 9,825). Streaming MVs **are** shareable. Typed-vs-SUPER gap reproduces
  through the share (~2.2× on the same filter).
  **Gotcha:** the reader MUST NOT be publicly accessible — with `publicly_accessible = true` every
  query on a shared object fails with *"Publicly accessible consumer cannot access object in the
  database"*. Hence the runners execute from the in-VPC producer box.
- ⚠️ **Producer currently stopped** (quiesced for the equivalence check). Restart it for the run.

**Historical: pivot to MSK (2026-08-06).** The original design streamed from a **self-managed
single-broker Kafka** on the box. Redshift streaming ingestion requires **TLS with a CA-trusted
server cert** and rejected both our plaintext listener (`Broker transport failure`) *and* a
self-signed TLS listener (`SSL handshake failed`). Hence Amazon MSK, whose broker certs Redshift
trusts. (Full detail: memory `redshift-streaming-requires-trusted-tls`.)

- ✅ **Networking proven end-to-end.** Redshift's ENI reached the in-VPC broker once these held:
  workgroup **enhanced VPC routing = true**; workgroup + box in the **same VPC** `vpc-09739962…`
  (box in `subnet-036503b17dc39260f`, eu-west-2b — one of the workgroup subnets); redshift SG
  `sg-06cc5712e19c8f7df` egress all; broker port allowed from the redshift SG + VPC CIDR. Only the
  cert failed → MSK.
- ✅ **Redshift Serverless live** — workgroup `cb-quotes-rt-wg`, **AVAILABLE**, EVR on. **base 128 RPU
  / max 256 RPU** (raised from 8 on 2026-08-10, via console; declared in `infra/{variables,redshift}.tf`
  as `redshift_base_rpu=128` + `redshift_max_rpu=256` / `max_capacity` so `apply` won't revert). Endpoint
  `cb-quotes-rt-wg.244449518788.eu-west-2.redshift-serverless.amazonaws.com:5439`, db `quotes`,
  account `244449518788`, region `eu-west-2`.
- ✅ **Phase A proven — producer → Kafka ~1.45M EPS** (dry-run Jul 23, 16 producers, 247.9M rows/170s;
  log `/home/ubuntu/producer/prodA.log`). Bottleneck #1 cleared; the producer path carries to MSK.
- ✅ **Box cleaned up (2026-08-06)** — self-managed broker stopped, SSL config reverted, keystore
  removed; **broker SG detached** (box now has ONLY `sg-08e2cad316c9ef1a4` launch-wizard-4, SSH intact).
  Box is **producer-only** `i-0207b5d065bc7edce` (`172.31.41.124`), dataset `/data/quotes`, Kafka 3.6.0
  + `/opt/producer-venv` still installed. (The box EC2 is still *running* — stop it to save cost if idle.)
- ✅ **MSK applied** (was half-applied on 2026-08-06). `infra/msk.tf` = provisioned cluster
  (3× `kafka.m7g.xlarge` as of 2026-08-12, `TLS_PLAINTEXT`, unauthenticated) + MSK SG. The first
  `terraform apply` **destroyed the obsolete broker SG** (intended) and re-set the namespace password
  to the same value (harmless — TF can't read it back so it re-applies each run), but **failed
  creating the MSK SG** because of an em-dash in its description (AWS SG descriptions are ASCII-only).
  Fixed to ASCII; cluster + SG now exist and are ACTIVE.
- ℹ️ **External schema `kafka`** now points at the MSK **TLS** bootstrap. Note `setup_streaming.sql`
  uses `CREATE ... IF NOT EXISTS`, so if the URI ever needs changing, `DROP SCHEMA kafka;` first —
  otherwise the stale URI survives.
- ✅ **Phase B done** — topic created, producer on the MSK plaintext bootstrap, fork created, keep-up
  measured at 1M EPS (see Current state above).
- ✅ **Read-runners + trackers built** (`runner_redshift.py` roles, `monitor_lag.py` controller,
  `get_metrics.py`).

### Resume from here — to start the full run
Everything is built, validated and the reader path is proven. Remaining steps, in order:

1. **Reset for a clean timed run**: recreate the `quotes` topic (bounded retention baked in), then
   run `sql/setup_streaming.sql` so **all three MVs exist before the stream starts** (otherwise the
   initial build lands in the steady-state refresh numbers).
2. **Re-establish the datashare** — recreating the MVs invalidates it, so re-run
   `sql/setup_datashare.sql` (part 1 writer, part 2 reader) and confirm the reader sees all three
   objects before starting the clock.
3. **Start, in this order:** refresh controller → read-runners → producer.
   ```bash
   # writer: freshness sampling + both child refreshes
   python monitor_lag.py --typed-delay 2 --daily-interval 60 --lag-interval 60
   # reader (from inside the VPC): three roles, one process each
   python runner_redshift.py --role dashboard       --interval 600 redshift "Redshift Serverless" 128-256RPU "T2 quotes" MSK-3x-m7g
   python runner_redshift.py --role drilldown_super --interval 3600 redshift "Redshift Serverless" 128-256RPU "T2 quotes" MSK-3x-m7g
   python runner_redshift.py --role drilldown_typed --interval 3600 redshift "Redshift Serverless" 128-256RPU "T2 quotes" MSK-3x-m7g
   # producer
   python produce_quotes.py --bootstrap <MSK plaintext> --topic quotes --dir /data/quotes \
       --parallel 16 --target-rps 1000000
   ```
4. **Watch:** offset lag must stay bounded, both children must keep saying *incrementally*,
   `SYS_STREAM_SCAN_ERRORS` must stay empty. If refresh load threatens keep-up, raise
   `--typed-delay` (or `--serialize-refresh`) before changing the architecture — and record the
   cadence change in the run metadata.
5. **After the run:** `get_metrics.py` **once per workgroup** (writer + reader) for the two RPU
   lines, storage, and the per-MV refresh summary; add MSK broker-hours.

### Tooling (built 2026-08-10) + methodology vs the prior query-side-only benchmark
Scripts at `redshift-serverless/` (deployed to the box `/home/ubuntu/redshift/`, venv w/ `redshift_connector`):
- `runner_redshift.py` — read-runner with three **roles** (`--role dashboard | drilldown_super |
  drilldown_typed`), one process each; fixed-rate; server-side timings from `SYS_QUERY_HISTORY`;
  shared JSONL schema; **result cache OFF** per session. `--counts-mode rollup` (default) derives
  the volume axis from the small rollup so telemetry doesn't scan the billion-row MV on the
  measured workgroup.
- `monitor_lag.py` — the **refresh controller + freshness monitor**, one process on the *writer*:
  live poll of `SYS_STREAM_SCAN_STATES` (the `behind_by` analogue) → `lag_*.jsonl`, plus independent
  single-flight refresh loops for `quotes_typed` (continuous, `--typed-delay`) and `quotes_daily`
  (fixed-rate, `--daily-interval`), each journalled to `refresh_*.jsonl` with the server's
  Incremental-vs-Full verdict. Uses `RESTRICT`, never `CASCADE`. **Must run DURING the run.**
- `get_metrics.py` — **RPU cost** (`SYS_SERVERLESS_USAGE`) + read-query timings (`SYS_QUERY_HISTORY`,
  historized → pull after the run) + **storage** (`SVV_TABLE_INFO`, snapshot before dropping the schema)
  + **MSK cost** (computed from cluster spec × `--msk-hours`; not in any SQL view). None need live
  sampling — only `monitor_lag.py`'s freshness poll does. Verify region prices (`--price`,
  `--storage-price`, `--msk-*`).
- `sql/queries_dashboard.sql`, `sql/queries_drilldown_super.sql`, `sql/queries_drilldown_typed.sql` —
  the two drilldown suites are logically identical and differ only in what they read
  (`quotes_streamed` SUPER navigation vs `quotes_typed` columns), which is what makes the
  semi-structured-vs-typed comparison meaningful. Validated live 2026-08-12.

Checked against `query-side-only/redshift-serverless` (ClickBench-style; `create.sql`/`queries.sql`/`run.sh`/`get_metrics.sh`):
- ✅ result cache off, ✅ server-side timings, ✅ **cost via `SYS_SERVERLESS_USAGE`** (was the main gap — now `get_metrics.py`).
- Δ they run each query **3×** (cold+warm, take best); our streaming runner runs each **once per iteration** as
  data grows (time-series design, matches the Snowflake runner) — intentional, not a gap.
- ✅ **storage cost** (`SVV_TABLE_INFO`) and **MSK cluster cost** (spec × uptime) now in `get_metrics.py`.
  MSK stays a separate reported line ($/hr while up) that the push-SDK vendors (Snowflake/ClickHouse)
  don't carry — surface it, don't hide it.

> Sections 1–6 below are the original self-managed-broker walkthrough, kept for reference. The
> **Status / Resume above is authoritative** for the current MSK-based flow.

## 0. Prerequisites
- AWS creds for the sandbox account active (`aws sts get-caller-identity`).
- `terraform >= 1.6`.
- Quotes parquet reachable (S3 in this region, or download onto the EC2 later). ≈684 GB.

## 1. Provision Redshift + security groups (Terraform)
```bash
cd infra
cp terraform.tfvars.example terraform.tfvars      # set region, my_ip_cidr
export TF_VAR_redshift_admin_password='<strong-password>'
terraform init && terraform plan                  # REVIEW
terraform apply
terraform output   # redshift_workgroup_endpoint, broker_security_group_id, default_vpc_id, kafka_port
```

## 2. Broker/producer EC2 — REUSE the existing quotes box
We reuse the existing quotes box (Ubuntu `m6i.8xlarge`, `172.31.41.124`, VPC `vpc-09739962…`,
eu-west-2) — dataset is already at `/data/quotes` (644 GB, 1.2 TB free). Just make it reachable
from Redshift: attach the Terraform `broker_security_group_id` to it, e.g.
```bash
aws ec2 modify-instance-attribute --region eu-west-2 --instance-id <box-instance-id> \
  --groups <box-existing-sg-ids> <broker_security_group_id>
```
(Or add an inbound rule to `launch-wizard-4`: TCP 9092 from the Redshift SG / VPC CIDR 172.31.0.0/16.)
No new instance or data download needed. (To provision a *fresh* box instead, launch a large
instance in this VPC with the broker SG and a ~1.5 TB volume, then sync the dataset.)

## 3. On the EC2: Kafka + producer (dataset already at /data/quotes)
```bash
# copy broker/ over (scp with the box key), then:
sudo bash setup_kafka.sh 9092 quotes 6      # installs Kafka (apt), starts the broker, creates the topic,
                                            # builds /opt/producer-venv. Prints PRIVATE IP + broker address.
```

## 4. Phase A — producer → local Kafka at 1M EPS  (bottleneck #1: single broker + instance)
```bash
/opt/producer-venv/bin/python produce_quotes.py --bootstrap <PRIV_IP>:9092 --topic quotes \
    --dir /data/quotes --parallel 16 --target-rps 1000000     # 32-vCPU box -> try --parallel 16
```
Watch the producer EPS line, and the broker's accepted rate:
```bash
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list <PRIV_IP>:9092 --topic quotes | \
  awk -F: '{s+=$3} END{print s" total msgs"}'   # sample twice, diff / elapsed = EPS into Kafka
```
If it caps below 1M EPS: raise producer `--parallel`, bigger instance (`c6in.8xlarge`), more `--partitions`
(recreate topic). A single broker's ceiling is the instance's disk+network — scale the instance first.

## 5. Phase B — Kafka → Redshift streaming MV  (bottleneck #2, the interesting one)
In Redshift Query Editor v2 (or psql to `<redshift_workgroup_endpoint>:5439`, db `quotes`):
1. Edit `sql/setup_streaming.sql`, set the MSK **TLS** bootstrap in the external-schema `URI`.
2. Run it — external schema → `quotes_streamed` (streaming MV) → `quotes_typed` + `quotes_daily`
   (the fork; both directly on `quotes_streamed`).
3. Measure keep-up:
```sql
SELECT COUNT(*) FROM quotes_streamed;                   -- repeat -> rows/sec ingested
SELECT * FROM SYS_STREAM_SCAN_STATES ORDER BY record_time DESC LIMIT 20;  -- offset lag per partition
SELECT * FROM SYS_STREAM_SCAN_ERRORS ORDER BY record_time DESC LIMIT 20;  -- must stay empty
-- ACCEPTANCE: both children must say "incrementally", and quotes_streamed must show only Auto rows
SELECT TRIM(mv_name), TRIM(refresh_type), TRIM(status), duration/1e6 AS secs
  FROM SYS_MV_REFRESH_HISTORY ORDER BY start_time DESC LIMIT 20;
```
If offset lag grows: raise `redshift_base_rpu` (tfvar) and re-apply; keep `quotes_streamed` minimal
(it is — SUPER only, no per-field casts). Measured: 128 RPU sustains 1M EPS with lag 0.

## Scaling levers (start-small → 1M EPS)
1. producer `--parallel` / EC2 instance type. 2. topic `--partitions`. 3. `redshift_base_rpu`.
Change → re-run / `terraform apply` → re-measure.

## Teardown
```bash
cd infra && terraform destroy          # Redshift + SGs
aws ec2 terminate-instances --instance-ids <your-broker-instance-id>
```
Terminate the EC2 you launched (Terraform doesn't own it). Redshift Serverless base capacity + the
EC2 + EBS bill while up — tear down between sessions.
