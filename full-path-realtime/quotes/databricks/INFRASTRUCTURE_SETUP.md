# Databricks AWS infrastructure setup

This guide records the successful AWS and Unity Catalog setup used to prepare
the Databricks Lakehouse//RT benchmark on September 14, 2026. Deployment-specific identifiers are represented by placeholders so the guide can be
shared safely.

Creating this infrastructure does not qualify a benchmark run. Lakehouse//RT
must become available, all online preflight checks must pass, and
`qualification_report.json` must contain exact JSON boolean `"qualified":
true` before creating a full-run schema.

## Deployment record

- Databricks workspace region: `eu-west-1`
- Databricks workspace host:
  `https://<databricks-workspace-host>`
- Databricks workspace ID: `<databricks-workspace-id>`
- Zerobus endpoint:
  `https://<workspace-id>.zerobus.<region>.cloud.databricks.com`
- Unmeasured control warehouse: `<control-warehouse-name>`, serverless `2X-Small`, ID
  `<control-warehouse-id>`, one cluster minimum/maximum, ten-minute auto-stop,
  Current channel version `2026.32`
- Producer: EC2 `m6i.8xlarge` in `eu-west-1c`, Ubuntu 26.04, 32 vCPU,
  123 GiB RAM
- Producer data volume: 7.8 TiB ext4 mounted at `/data`
- Licensed source: `s3://<licensed-source-bucket>/<source-prefix>/` in `us-east-2`
- Local source directory: `/data/quotes`
- Source validation: complete; 232 Parquet files, 865,579 row groups, and
  exactly 113,219,565,734 rows
- Unity Catalog managed-storage bucket: `<unity-catalog-storage-bucket>` in
  `eu-west-1`
- Automatically created external location: `<external-location-name>`
- External-location URL: `s3://<unity-catalog-storage-bucket>/`
- Automatically created storage credential:
  `<storage-credential-name>`
- External location is writable, has file events enabled, and was created by
  Databricks automatic setup
- Benchmark catalog: `costbench`
- Catalog managed location: `s3://<unity-catalog-storage-bucket>/costbench`
- Reusable qualification schema: `costbench.rt_qualification`; it inherits
  the catalog managed location
- Permanent raw qualification target:
  `costbench.rt_qualification.quotes`, created as an empty Unity Catalog
  managed Delta table
- Canonical `quotes_daily` definition: checked with
  `EXPLAIN CREATE MATERIALIZED VIEW`; Databricks reported it eligible for
  incremental refresh with no issues
- Materialized view: `costbench.rt_qualification.quotes_daily`, created with
  strict incremental refresh and a 60-second update trigger; ownership
  transferred to `<benchmark-service-principal>`
- Shared benchmark identity: `<benchmark-service-principal>`, Application ID
  `<service-principal-application-id>`, assigned to the workspace with
  Workspace and Databricks SQL access and granted the documented Unity Catalog
  privileges on the raw table
- Zerobus OAuth: client-credentials token issuance verified successfully from
  the EC2 producer; Databricks returned a one-hour bearer token
- System-table evidence access: granted to `<benchmark-service-principal>` for the required
  query, Zerobus, compute, Predictive Optimization, usage, and price tables
- Standard workspace OAuth and SQL Statement Execution: verified through
  `<control-warehouse-name>`; the service principal successfully queried
  `system.lakeflow.zerobus_stream`
- Workspace preview enabled September 15, 2026:
  `System-Managed Job for Materialized Views & Streaming Tables`

The `<external-location-name>` name is only an object label. It does not control or report
the bucket region.

## 1. Prepare the same-region producer

Use a dedicated `m6i.8xlarge`-equivalent host in the Databricks workspace
region. Confirm that the attached data disk is empty before formatting it:

```sh
sudo file -s /dev/nvme1n1
sudo wipefs -n /dev/nvme1n1
```

Only if the output proves that the intended device is blank, create and mount
the filesystem:

```sh
sudo mkfs.ext4 -F -L costbench-data /dev/nvme1n1
sudo mkdir -p /data
UUID="$(sudo blkid -s UUID -o value /dev/nvme1n1)"
printf 'UUID=%s /data ext4 defaults,nofail 0 2\n' "$UUID" |
  sudo tee -a /etc/fstab
sudo mount /data
sudo chown ubuntu:ubuntu /data
mkdir -p /data/quotes /data/databricks-runs /data/databricks-qualification
```

Install `uv`, Python 3.12, and AWS CLI v2. Install the two Python dependency
sets into separate environments: the SQL kernel requires PyArrow 23.x while
Zerobus 1.8 requires PyArrow earlier than 22.

The prepared host uses:

```text
/home/ubuntu/costbench/full-path-realtime/quotes/databricks
├── .venv-runner
└── .venv-zerobus
```

The non-secret host environment is `/home/ubuntu/databricks-host.env`.
Credentials must not be written to this file.

## 2. Stage the licensed source

The source bucket is in `us-east-2`, but the producer and Databricks workspace
are in `eu-west-1`. Downloading the source is an unmeasured preparation step;
record its cross-region transfer cost separately and do not include it in the
measured ingestion window.

Enter temporary AWS credentials without putting their values in shell history:

```sh
tmux new -s source-stage
source /home/ubuntu/databricks-host.env

read -rsp 'AWS access key ID: ' AWS_ACCESS_KEY_ID; echo
read -rsp 'AWS secret access key: ' AWS_SECRET_ACCESS_KEY; echo
read -rsp 'AWS session token: ' AWS_SESSION_TOKEN; echo
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

export S3_SOURCE_URI='s3://<licensed-source-bucket>/<source-prefix>'
"$DBX_BENCH_DIR/stage_source_from_s3.sh"
```

The staging script is resumable, excludes `quotes_0.parquet`, and fails unless
the top-level Parquet files contain exactly 113,219,565,734 rows with the
required columns. If temporary credentials expire, enter a fresh set and run
the same command again.

After completion, clear the credential variables:

```sh
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
```

The successful staging run wrote
`/data/databricks-qualification/source_inventory.json` with status
`complete`, 232 files, 865,579 row groups, and exactly 113,219,565,734 rows.
The local directory occupies approximately 637 GiB on the producer.

## 3. Create private same-region managed storage

Unity Catalog managed storage is separate from the licensed source bucket.
Create a dedicated bucket in the Databricks workspace region. Bucket names are
globally unique; another deployment should include its AWS account ID if the
name below is unavailable.

The successful deployment used:

```sh
BUCKET='<unity-catalog-storage-bucket>'

aws s3api create-bucket \
  --bucket "$BUCKET" \
  --region eu-west-1 \
  --create-bucket-configuration LocationConstraint=eu-west-1
```

Explicitly block every form of public access:

```sh
aws s3api put-public-access-block \
  --bucket "$BUCKET" \
  --public-access-block-configuration \
  'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'
```

Verify the bucket location and public-access controls:

```sh
aws s3api get-bucket-location --bucket "$BUCKET"
aws s3api get-public-access-block --bucket "$BUCKET"
```

Do not make this bucket public. Databricks accesses it through a Unity Catalog
storage credential backed by an AWS IAM role.

## 4. Create the external location with automatic setup

The successful setup used Databricks automatic external-location setup rather
than the abandoned manual IAM-role flow:

1. In Databricks, open **Catalog > Connect > External locations**.
2. Select **Create external location**, then **Set up automatically**.
3. Enter external-location name `<external-location-name>`.
4. Enter S3 URL `s3://<unity-catalog-storage-bucket>/`.
5. Leave **Limit to read-only use** disabled.
6. Select **Log into AWS and create external location**.
7. Review the requested resources and select **Initiate external location
   creation**.
8. In AWS, select **Allow access** for the temporary IAM delegation request.
9. Return to Databricks and wait for the location to move from **Pending
   locations** to **Existing locations**.
10. Open the external location and select **Test connection**.

Automatic setup uses AWS IAM temporary delegation, not CloudFormation. The
request is visible under **AWS IAM > Temporary delegation requests**, and the
resulting operations are recorded in CloudTrail. Databricks documents that
provisioning can take up to eight minutes.

The completed setup automatically created this storage credential:

```text
<storage-credential-name>
```

It also enabled file events and configured writable access. No AWS access key
or secret key was copied into Databricks.

## 5. Create the catalog and qualification schema

Run the following on a standard Databricks SQL warehouse:

```sql
CREATE CATALOG costbench
MANAGED LOCATION 's3://<unity-catalog-storage-bucket>/costbench';

ALTER CATALOG costbench ENABLE PREDICTIVE OPTIMIZATION;

CREATE SCHEMA costbench.rt_qualification;
```

The schema intentionally inherits the catalog's explicit managed location.
This is not Unity Catalog metastore default storage. Tables created without a
table-level `LOCATION` remain Unity Catalog managed tables under the catalog's
storage boundary.

Verify every resulting object:

```sql
DESCRIBE EXTERNAL LOCATION `<external-location-name>`;
DESCRIBE CATALOG EXTENDED costbench;
DESCRIBE SCHEMA EXTENDED costbench.rt_qualification;
```

The catalog description must report
`s3://<unity-catalog-storage-bucket>/costbench`, and the external location must remain
writable. The online benchmark preflight independently verifies the resolved
raw-table storage location and rejects metastore default storage.

## 6. Create the raw qualification target

On September 14, 2026, the empty raw qualification target was created directly
from the Databricks SQL editor. No Python setup tool was used for this step.
Because the schema was empty, no `DROP` statement or `IF NOT EXISTS` clause was
used:

```sql
CREATE TABLE costbench.rt_qualification.quotes (
    sym STRING,
    bx SMALLINT,
    bp DOUBLE,
    bs BIGINT,
    ax SMALLINT,
    ap DOUBLE,
    `as` BIGINT,
    c SMALLINT,
    i ARRAY<SMALLINT>,
    t BIGINT,
    q BIGINT,
    z SMALLINT
)
USING DELTA
CLUSTER BY (sym, t)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);
```

The table has no table-level `LOCATION`, so it resolves under the explicit
managed location inherited from the `costbench` catalog. The three Delta
features are enabled before ingestion because Databricks recommends them on
materialized-view source tables; row tracking is required by some incremental
refresh plans.

Verify the table before registering it as a Zerobus target:

```sql
DESCRIBE DETAIL costbench.rt_qualification.quotes;
SHOW TBLPROPERTIES costbench.rt_qualification.quotes;
```

Do not drop and recreate this name after Zerobus has targeted it. Current
Zerobus documentation states that recreating a target table is unsupported.
Qualification cases must therefore use fresh target objects rather than
resetting this target with `DROP TABLE`.

## 7. Prove materialized-view incrementalization

Before creating the materialized view, the canonical DDL was prefixed with
`EXPLAIN` and run in the Databricks SQL editor. The result was:

```text
== Incremental Update Eligibility ==
The Materialized View can be incrementally refreshed.

== Detailed Incrementalization Info ==
No issues detected.
```

The physical plan also preserved all required properties:

- target `costbench.rt_qualification.quotes_daily`;
- `IncrementalStrict` refresh policy;
- 60-second update trigger;
- `CLUSTER BY (sym, day)`; and
- the complete canonical aggregate, including `sum(ap - bp)` over the two
  `DOUBLE` source columns.

This provider-generated result resolves the potential concern about
floating-point `SUM` eligibility for this exact definition. Preserve the full
SQL editor output with the qualification artifacts.

## 8. Create the materialized view

After the incrementalization check passed, the same canonical statement was
run without `EXPLAIN`:

```sql
CREATE MATERIALIZED VIEW costbench.rt_qualification.quotes_daily
USING DELTA
CLUSTER BY (sym, day)
REFRESH POLICY INCREMENTAL STRICT
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
SELECT
    sym,
    date_add(
        DATE '1970-01-01',
        CAST(
            floor(
                CAST(t AS DECIMAL(20, 0))
                / CAST(86400000 AS DECIMAL(20, 0))
            ) AS INT
        )
    ) AS day,
    count(*) AS n_quotes,
    min(bp) AS bp_min,
    max(bp) AS bp_max,
    min(ap) AS ap_min,
    max(ap) AS ap_max,
    sum(bs) AS bs_sum,
    sum(`as`) AS as_sum,
    sum(ap - bp) AS spread_sum
FROM costbench.rt_qualification.quotes
GROUP BY sym, day;
```

The resulting namespace contains the empty raw table and its materialized
view:

```sql
SHOW TABLES IN costbench.rt_qualification;
DESCRIBE TABLE EXTENDED costbench.rt_qualification.quotes_daily;
```

Databricks owns the serverless pipeline that refreshes this standalone
materialized view. Lakehouse//RT will serve read queries but will not run the
refresh.

### Enable scheduled-refresh performance controls

After the first sustained-ingestion characterization exposed long
`WAITING_FOR_RESOURCES` phases, enable this workspace preview under
**Workspace Settings > Previews**:

```text
System-Managed Job for Materialized Views & Streaming Tables
```

This Beta preview was enabled on September 15, 2026. It exposes the
system-managed-job features described by Databricks. Enabling the preview does
not change an existing schedule by itself.

For the existing `TRIGGER ON UPDATE` MV, Catalog Explorer continued to show
only `Trigger on update (at most every 1 minute)`, and its **Edit** link opened
documentation rather than a schedule editor. No Performance optimized toggle
was available. Therefore, this deployment must not claim that the setting was
enabled or that the preview provides that control for trigger-on-update MVs.

Databricks' detailed UI instruction specifically refers to pipelines scheduled
with a `SCHEDULE` clause. A fresh-target experiment may test whether the
control appears while the creating user still owns the MV, before ownership is
transferred to the service principal. If it remains unavailable, testing a
performance-optimized `SCHEDULE`/SQL-job refresh or a continuous Lakeflow
pipeline is a separate architecture variant, not a tuning change to the
current trigger-on-update path.

Performance mode is scheduler/compute metadata and cannot currently be set
with the materialized-view SQL definition. Standalone serverless MV refresh
does not expose an assignable warehouse or cluster size.

## 9. Create the Zerobus service principal

Create a dedicated service principal named `<benchmark-service-principal>` under
**Settings > Identity and Access > Service principals**. Use these workspace
entitlements:

- Consumer access: off
- Databricks SQL access: on
- Workspace access: on
- Admin access: off

Copy its Application ID and generate a client secret. Store the secret outside
the repository and never include it in SQL, shell history, environment files,
or benchmark evidence.

Grant only the permissions documented for a Zerobus target:

```sql
GRANT USE CATALOG ON CATALOG costbench
TO `<APPLICATION_ID>`;

GRANT USE SCHEMA ON SCHEMA costbench.rt_qualification
TO `<APPLICATION_ID>`;

GRANT SELECT, MODIFY
ON TABLE costbench.rt_qualification.quotes
TO `<APPLICATION_ID>`;

SHOW GRANTS ON TABLE costbench.rt_qualification.quotes;
```

`SELECT` permits target schema discovery and `MODIFY` permits direct
ingestion. This deployment intentionally reuses the principal for control and
query operations. Give it `CAN MONITOR` on the control warehouse and, once
available, the Lakehouse//RT warehouse. `CAN MONITOR` permits both query
execution and warehouse/query monitoring without warehouse administration.

Make the service principal the owner of
`costbench.rt_qualification.quotes_daily`. The owner identity runs future
refreshes and is required to query the standalone materialized view's event log
directly. Before transferring ownership, retain `MANAGE` and `SELECT` for the
administrator who created the view if continued interactive access is needed.

The transfer was completed through Catalog Explorer after granting the benchmark administrator the account-level **Use** role on `<benchmark-service-principal>`. the benchmark administrator retained
explicit `MANAGE` and `SELECT` privileges on the materialized view.

Grant the shared identity bounded system-table access:

```sql
GRANT USE CATALOG ON CATALOG system
TO `<service-principal-application-id>`;

GRANT USE SCHEMA ON SCHEMA system.query
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE system.query.history
TO `<service-principal-application-id>`;

GRANT USE SCHEMA ON SCHEMA system.lakeflow
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE system.lakeflow.zerobus_stream
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE system.lakeflow.zerobus_ingest
TO `<service-principal-application-id>`;

GRANT USE SCHEMA ON SCHEMA system.compute
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE system.compute.warehouses
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE system.compute.warehouse_events
TO `<service-principal-application-id>`;

GRANT USE SCHEMA ON SCHEMA system.storage
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE
    system.storage.predictive_optimization_operations_history
TO `<service-principal-application-id>`;

GRANT USE SCHEMA ON SCHEMA system.billing
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE system.billing.usage
TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE system.billing.list_prices
TO `<service-principal-application-id>`;
```

The tables under `system.query` and `system.billing` contain account-wide
evidence. Table-level `SELECT` grants deliberately avoid granting access to
every table in those system schemas. This account-wide read access is still
the security trade-off accepted by reusing one principal instead of separating
ingestion and control identities.

OAuth was verified from the producer without writing any rows. The request
used the workspace-specific Zerobus resource and explicit authorization
details for the three grants above. Databricks returned:

```json
{
  "token_type": "Bearer",
  "expires_in": 3600,
  "has_access_token": true
}
```

Only this redacted summary is retained. The client secret and bearer token were
unset immediately and are not benchmark artifacts.

## 10. Remaining Databricks preparation

Network connectivity was verified from the EC2 producer after recording the
workspace coordinates. The Zerobus hostname resolved successfully, its HTTPS
endpoint returned the expected unauthenticated `401`, and the workspace host
returned its normal login redirect. This proves DNS, routing, and TLS access;
OAuth token issuance still needs to be verified with the client secret.

These steps can be completed before Lakehouse//RT becomes available:

The dedicated serverless `2X-Small` control warehouse `<control-warehouse-name>`
(`<control-warehouse-id>`) is created. The shared benchmark principal has
`CAN MONITOR`, which includes query execution and monitoring but not warehouse
administration. It has a fixed one-cluster minimum and maximum, a ten-minute
auto-stop, and Current channel version `2026.32`.

Remaining:

The initial grant attempt failed with `PERMISSION_DENIED: User does not have
MANAGE on Catalog 'system'` because workspace-admin privileges do not control
the system catalog. A metastore administrator subsequently granted the bounded
table access above to `<benchmark-service-principal>`.

1. If the benchmark administrator was assigned temporarily as Metastore Admin, confirm that the
   temporary role has been removed after the explicit grants were applied.
2. Verify the remaining required system tables and preserve only redacted
   statement IDs and statuses.

Standard workspace OAuth and Statement Execution were verified with the shared
principal through `<control-warehouse-name>`. Statement
`01f1b10c-83ff-1ae8-962c-b397351c5eb1` completed successfully for a
zero-row access probe against `system.lakeflow.zerobus_stream`; no token or
secret was retained.

Do not create `rt_full_<run_id>` yet. Create a fresh full-run schema only after
Lakehouse//RT is available and the complete qualification matrix passes.
