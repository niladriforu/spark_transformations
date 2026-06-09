# Spark Transformations — CDC Pipeline

Databricks Delta Live Tables (DLT) pipeline for employee CSV ingestion (raw → silver → curated) with a Unity Catalog metric view.

## Deployment (GitHub Actions)

The workflow **CDC_AUTOMATED_WORKFLOW** (`.github/workflows/cdc_automated_workflow.yml`) deploys on push to `main` or via manual dispatch.

### 1. Databricks prerequisites (one-time)

- Unity Catalog enabled workspace
- Catalog/schema: `workspace.default` (or update `databricks.yml` variables)
- UC Volume with source data: `/Volumes/workspace/default/niladri_created_volume/data/`
- SQL warehouse running (for metric view deployment)
- Upload sample CSVs from `data/` to the volume path above

### 2. GitHub secrets

Go to **GitHub → your repo → Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Description | Where to find it |
|--------|-------------|------------------|
| `DATABRICKS_HOST` | Workspace URL | Browser address bar, e.g. `https://dbc-xxxxx.cloud.databricks.com` (no trailing slash) |
| `DATABRICKS_TOKEN` | Personal access token | Databricks → User Settings → Developer → Access tokens → Generate new token |
| `SQL_WAREHOUSE_ID` | SQL warehouse ID | SQL → SQL Warehouses → open warehouse → ID in URL (`/sql/warehouses/<id>`) |

The token needs permissions to create/update DLT pipelines, run pipelines, and execute SQL (CREATE VIEW).

### 3. What the workflow deploys

1. **Validate** — `databricks bundle validate`
2. **Deploy** — syncs DLT pipeline code and creates/updates `cdc-automated-workflow-<target>`
3. **Run pipeline** — starts `cdc-automated-workflow-<target>`, then `cdc-propagate-workflow-<target>`
4. **Metric view** — creates `employee_metrics_<env>` on the SQL warehouse

### Environment-based naming

Table and metric view names include the bundle target as a suffix (`dev` or `prod`):

| Object | dev example | prod example |
|--------|-------------|--------------|
| Raw table | `raw_events_dev` | `raw_events_prod` |
| Silver table | `silver_events_dev` | `silver_events_prod` |
| Bad records | `silver_bad_events_dev` | `silver_bad_events_prod` |
| Curated table | `silver_curated_events_dev` | `silver_curated_events_prod` |
| Gold MV | `gold_employee_summary_deduped_dev` | `gold_employee_summary_deduped_prod` |
| Gold CDC (SCD2) | `gold_employee_summary_cdc_dev` | `gold_employee_summary_cdc_prod` |
| Metric view | `employee_metrics_dev` | `employee_metrics_prod` |

**Propagate pipeline** (`cdc-propagate-workflow-<target>`) reads shared `raw_events_<env>` and writes parallel tables with `_propagate_` in the name:

| Object | dev example | prod example |
|--------|-------------|--------------|
| Silver | `silver_events_propagate_dev` | `silver_events_propagate_prod` |
| Silver bad | `silver_bad_events_propagate_dev` | `silver_bad_events_propagate_prod` |
| Curated (DLT DQ) | `silver_curated_events_propagate_dev` | `silver_curated_events_propagate_prod` |
| Curated bad | `silver_curated_bad_events_propagate_dev` | `silver_curated_bad_events_propagate_prod` |
| Quarantine | `silver_quarantine_propagate_dev` | `silver_quarantine_propagate_prod` |
| Curated (manual DQ) | `silver_curated_events_manual_dq_propagate_dev` | `silver_curated_events_manual_dq_propagate_prod` |
| Curated bad (manual DQ) | `silver_curated_bad_events_manual_dq_propagate_dev` | `silver_curated_bad_events_manual_dq_propagate_prod` |
| Gold MV | `gold_employee_summary_deduped_propagate_dev` | `gold_employee_summary_deduped_propagate_prod` |
| Gold CDC (SCD2) | `gold_employee_summary_cdc_propagate_dev` | `gold_employee_summary_cdc_propagate_prod` |

The suffix is set via `pipeline.environment` in `resources/pipeline.yml`, driven by the `environment` variable in `databricks.yml`.

### 4. Local deployment (optional)

Requires Databricks CLI **v0.297.2+** (or other patched version — see troubleshooting below).

```bash
# Upgrade CLI if you hit the Terraform GPG key error
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.297.2/install.sh | sh

export DATABRICKS_HOST="https://dbc-xxxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export ENV=dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run employee_cdc_pipeline -t dev
databricks bundle run employee_cdc_propagate_pipeline -t dev
sed "s/__ENV__/${ENV}/g" src/metric_views/employee_metrics.sql > /tmp/employee_metrics_${ENV}.sql
databricks api post /api/2.0/sql/statements --json "$(jq -n \
  --arg warehouse_id "<warehouse-id>" \
  --rawfile statement "/tmp/employee_metrics_${ENV}.sql" \
  '{warehouse_id: $warehouse_id, statement: $statement, wait_timeout: "50s"}')"
```

### Troubleshooting: Terraform GPG key expired

If deploy fails with:

```text
error downloading Terraform: unable to verify checksums signature: openpgp: key expired
```

Upgrade the Databricks CLI to a patched version ([databricks/cli#5022](https://github.com/databricks/cli/issues/5022)):

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.297.2/install.sh | sh
databricks --version   # should show v0.297.2 or newer patched release
```

Then retry `databricks bundle deploy -t dev`.

### Pipeline lineage

```
UC Volume (CSV) → raw_events_<env> → silver_events_<env> / silver_bad_events_<env> → silver_curated_events_<env> → employee_metrics_<env>
```

---

# Steps for good performance tuning techniques for your GOLD table 
## **1. delta.enableLiquidClustering: "true"**

**Default:** Not enabled (must use CLUSTER BY clause)  
**Purpose:** Enables liquid clustering for automatic data organization and optimization. Liquid clustering replaces traditional partitioning and Z-ordering with dynamic clustering keys that adapt to query patterns. It reduces write conflicts, improves data skipping, and works seamlessly with predictive optimization for automatic maintenance.

## **2. delta.autoOptimize.optimizeWrite: "true"**

**Default:** (none) - Not enabled by default for all operations  
**Purpose:** Automatically optimizes file layout during writes by using 128 MB as target file size. Reduces small files written to partitions by shuffling data before writing. Eliminates need for manual coalesce(n) or repartition(n) calls. Default enabled for MERGE, UPDATE, DELETE operations in DBR 9.1+.

## **3. delta.autoOptimize.autoCompact: "true"**

**Default:** (none) - Not enabled by default  
**Purpose:** Automatically compacts small files within table partitions synchronously after writes complete. Runs on the same cluster performing the write. Uses 128 MB target file size (or auto for dynamic sizing). Works independently from predictive optimization.

## **4. delta.enablePredictiveOptimization: "true"**

**Default:** Enabled by default for accounts created after **November 11, 2024**. Older accounts are being gradually enabled through August 2026.  
**Purpose:** Enables automatic serverless maintenance operations (OPTIMIZE, VACUUM, automatic liquid clustering, statistics collection). Databricks intelligently identifies tables needing maintenance and schedules operations. Eliminates manual performance tuning burden.

## **5. delta.targetFileSize: "256mb"**

**Default:** (none) - Uses **autotune** behavior:

- **< 2.56 TB tables:** 256 MB
- **2.56 - 10 TB tables:** 256 MB → 1 GB (linear growth)
- **> 10 TB tables:** 1 GB

**Purpose:** Sets explicit target file size for OPTIMIZE, liquid clustering, auto compaction, and optimized writes. Your 256 MB setting locks in this size instead of allowing autotune to scale up for larger tables.


## **8. delta.enableChangeDataFeed: "true"**

**Default:** **false**  
**Purpose:** Enables Change Data Feed (CDF) to track row-level changes (inserts, updates, deletes) between table versions. Essential for CDC pipelines, audit trails, and incremental ETL. Adds metadata columns (_change_type, *commit*version, *commit*timestamp). Only captures changes after enablement.

## **9. delta.enableDeletionVectors: "true"**

**Default:** **Varies by workspace** - New workspaces may have auto-enable setting; older workspaces typically default to disabled  
**Purpose:** Accelerates DELETE, UPDATE, MERGE operations by marking rows as deleted without rewriting Parquet files. Dramatically improves write performance on large tables. Required for row-level concurrency (DBR 14.2+). Works with Photon's predictive I/O.

## **10. delta.checkpointInterval: "50"**

**Default:** **10** (transactions)  
**Purpose:** Number of transactions between Delta log checkpoints. Checkpoints speed up query planning by snapshotting transaction log state. Default 10 is optimal for most cases. Setting 50 means less frequent checkpoints, which can delay metadata reads but reduces checkpoint write overhead for high-frequency write workloads.

## **11. delta.columnMapping.mode: "name"**

**Default:** **"none"**  
**Purpose:** Enables metadata-only column renames/drops without rewriting data files. Allows special characters (spaces, ;{}()) in column names. **"name" mode** maps physical to logical column names. **"id" mode** (recommended) uses unique IDs for better compatibility. Required for UniForm (Iceberg compatibility). Upgrades table protocol to reader v2/writer v5.