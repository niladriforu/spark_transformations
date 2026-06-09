# Raw Table Change Scenarios

This document describes how the employee CDC pipeline behaves in two situations:

1. **Normal flow** — new CSV files land in the volume; nobody modifies `raw_events` directly.
2. **Manual changes to raw** — someone runs `DELETE` or `UPDATE` on `raw_events` (for example, to remove bad ingested rows or fix a value).

It also shows the code changes required for each downstream behavior you may want.

---

## Pipeline overview

```
Volume (CSV)  →  raw_events  →  silver_events  →  silver_curated_events  →  gold
                     ↑                  ↑                      ↑
              read_and_write_    read_and_write_      write_to_gold_table_cdc.py
              to_raw.py          to_silver_schema_    (AUTO CDC, SCD Type 2)
                                   enforced.py
                                   read_and_write_to_
                                   silver_curated_dlt_dq.py
```

All Delta tables in this pipeline have Change Data Feed (CDF) enabled:

```python
"delta.enableChangeDataFeed": "true"
```

That property **records** inserts, updates, and deletes on the table. It does **not** by itself tell downstream streaming tables how to react. Downstream behavior depends entirely on how each layer reads its source.

---

## Scenario 1: Normal flow (do not touch raw)

This is the intended day-to-day path.

### What happens

1. Auto Loader ingests new CSV files from the UC volume into `raw_events` (append-only).
2. `silver_events` streams new rows from `raw_events`, applies casting and quality checks, and keeps good rows.
3. `silver_bad_events` captures rows that fail quality checks.
4. `silver_curated_events` applies DLT expectations and forwards good rows.
5. Gold layers consume curated data:
   - `gold_employee_summary_cdc` — AUTO CDC flow with SCD Type 2 history.
   - `gold_employee_summary_deduped` — materialized view that refreshes from curated.

### Current code (works for this scenario)

**Raw ingestion** — `src/transformations/read_and_write_to_raw.py`

```python
@dp.table(name=table("raw_events"), ...)
def raw_events():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        ...
        .load("/Volumes/workspace/default/niladri_created_volume/data/")
    )
    return df
```

**Silver** — `src/transformations/read_and_write_to_silver_schema_enforced.py`

```python
@dp.table(name=table("silver_events"), ...)
def silver_events():
    df_raw = spark.readStream.table(qualified_table("raw_events"))
    df_with_quality = add_quality_checks(df_raw)
    return df_with_quality.filter(~F.col("_is_bad")).drop("_is_bad", "_rescue_data")
```

**Curated** — `src/transformations/read_and_write_to_silver_curated_dlt_dq.py`

```python
@dp.table(name=table("silver_curated_events"), ...)
def silver_curated_events():
    return spark.readStream.table(qualified_table("silver_events"))
```

**Gold CDC** — `src/transformations/write_to_gold_table_cdc.py`

```python
@dp.temporary_view(name=table("gold_employee_source_cdc"))
def gold_employee_source_cdc():
    df = spark.readStream.table(qualified_table("silver_curated_events"))
    return df.withColumns({...})

dp.create_auto_cdc_flow(
    target=table("gold_employee_summary_cdc"),
    source=table("gold_employee_source_cdc"),
    keys=["empid"],
    sequence_by=struct("upd_tmst", "create_tmst"),
    stored_as_scd_type=2,
)
```

### Result

| Layer   | Behavior on new CSV file |
|---------|--------------------------|
| Raw     | New rows appended        |
| Silver  | New good rows appended   |
| Curated | New good rows appended   |
| Gold    | New/updated SCD2 versions created |

No code changes are needed for this scenario.

---

## Scenario 2: Manual changes to raw

If you run SQL against `raw_events` after ingestion, behavior depends on the operation and on your downstream configuration.

Example manual operations:

```sql
-- Delete a bad row
DELETE FROM workspace.default.raw_events_dev
WHERE empid = 'E12345';

-- Fix a value manually
UPDATE workspace.default.raw_events_dev
SET salary = 55000, upd_tmst = '2024-06-01'
WHERE empid = 'E12345';
```

Because CDF is enabled, both operations are recorded in the change feed with `_change_type` values such as `delete`, `update_postimage`, and `update_preimage`.

### Default behavior today (append-only streaming)

Silver and curated use plain `spark.readStream.table()` with no change-handling options. DLT streaming tables expect **append-only** upstream sources.

| Manual operation on raw | What happens to the pipeline |
|-------------------------|------------------------------|
| `DELETE`                | Silver stream **fails** with an error like *"Detected a data update … This is currently not supported"* |
| `UPDATE`                | Same — stream **fails** |
| Downstream silver/gold  | **Not updated** (pipeline is broken until fixed or full-refreshed) |

Gold AUTO CDC does not help here on its own. Deletes and updates never reach silver or curated because the raw → silver step fails first.

> **Important:** Enabling `delta.enableChangeDataFeed` does not switch streaming reads into CDC mode. You must explicitly choose one of the patterns below.

---

## Choosing a strategy when you touch raw

There are two common goals. Pick one (they are mutually exclusive on the same source read).

| Goal | Strategy | Downstream effect |
|------|----------|-------------------|
| Ignore manual raw changes; keep ingesting new files only | `skipChangeCommits` | Silver/gold keep old rows; raw and silver can diverge |
| Propagate deletes/updates through silver and gold | AUTO CDC + `readChangeFeed` + `apply_as_deletes` | Changes cascade by business key (`empid`) |

---

## Option A: Ignore manual changes (inserts / new files only)

Use this when you delete or update raw for operational cleanup but **do not** want silver or gold to change.

### Important: `skipChangeCommits` is a Structured Streaming option

`skipChangeCommits` **only** works on **`spark.readStream`** — set it with `.option()` **before** `.table()`:

```python
# Works — Structured Streaming read
spark.readStream \
    .option("skipChangeCommits", "true") \
    .table(qualified_table("raw_events"))
```

It does **not** apply to:

| Read type | Example | `skipChangeCommits`? |
|-----------|---------|----------------------|
| Structured Streaming | `spark.readStream.option(...).table(...)` | **Yes** |
| Batch read | `spark.read.table(...)` | **No** |
| Materialized view source | `@dp.materialized_view` + `spark.read.table(...)` | **No** |
| Legacy DLT helper | `dlt.read_stream("table_name")` | **No** — use `spark.readStream` instead |

`create_auto_cdc_flow` has no `skipChangeCommits` parameter. If gold uses AUTO CDC, put the option on the **source view's** `readStream` (see `write_to_gold_table_cdc.py`), not on the flow itself.

**Do not combine** `skipChangeCommits` with `readChangeFeed` + `apply_as_deletes` on the same read. They are opposite strategies (ignore upstream changes vs propagate them). Use one or the other per source read.

### Where to enable it in this pipeline

| File | Function | Reads from | Enable? |
|------|----------|------------|---------|
| `read_and_write_to_silver_schema_enforced.py` | `silver_events`, `silver_bad_events` | `raw_events` | **Yes** — required if raw may be edited |
| `read_and_write_to_silver_curated_dlt_dq.py` | `silver_curated_events`, `silver_curated_bad_events` | `silver_events` | **Yes** — if silver may be edited |
| `write_to_gold_table_cdc.py` | `gold_employee_source_cdc` | `silver_curated_events` | **Yes** — if curated may be edited |
| `write_to_gold_table.py` | `gold_employee_summary_deduped` | `silver_curated_events` | **No** — see materialized view note below |

### Code change — silver layer

In `read_and_write_to_silver_schema_enforced.py`, add `skipChangeCommits` to both `silver_events` and `silver_bad_events`:

```python
def silver_events():
    df_raw = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(qualified_table("raw_events"))
    )
    df_with_quality = add_quality_checks(df_raw)
    return df_with_quality.filter(~F.col("_is_bad")).drop("_is_bad", "_rescue_data")


def silver_bad_events():
    df_raw = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(qualified_table("raw_events"))
    )
    df_with_quality = add_quality_checks(df_raw)
    return df_with_quality.filter(F.col("_is_bad"))
```

Apply the same option anywhere else you stream from a table that might be manually edited (curated reading from silver, gold CDC reading from curated).

### Code change — curated layer

In `read_and_write_to_silver_curated_dlt_dq.py`:

```python
def silver_curated_events():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(qualified_table("silver_events"))
    )
```

Apply the same pattern to `silver_curated_bad_events` and to `silver_quarantine` in `read_and_write_to_silver_curated_alternate_dq.py` if those paths are active in your pipeline.

### Code change — gold CDC layer

In `write_to_gold_table_cdc.py`, add the option on the source view's streaming read:

```python
@dp.temporary_view(name=table("gold_employee_source_cdc"))
def gold_employee_source_cdc():
    df = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(qualified_table("silver_curated_events"))
    )
    return df.withColumns({...})
```

### Gold materialized view — cannot use `skipChangeCommits`

`write_to_gold_table.py` defines a **materialized view** with a batch read:

```python
@dp.materialized_view(name=table("gold_employee_summary_deduped"), refresh_policy="auto", ...)
def gold_employee_summary_deduped():
    df = spark.read.table(qualified_table("silver_curated_events"))  # batch, not streaming
    ...
```

There is no streaming checkpoint here, so `skipChangeCommits` cannot be set and is not needed.

Instead, on each refresh the MV **re-reads the full current state** of `silver_curated_events` and recomputes the result.

| Scenario | Gold CDC (`write_to_gold_table_cdc.py`) | Gold MV (`write_to_gold_table.py`) |
|----------|----------------------------------------|----------------------------------|
| Delete on raw (with `skipChangeCommits` on silver/curated) | Row **remains** in gold CDC | Row **remains** — curated unchanged, so MV unchanged |
| Delete on `silver_curated_events` directly | Row **remains** — `skipChangeCommits` ignores the delete | Row **removed** on next MV refresh — MV mirrors curated's current contents |

So the two gold outputs can **diverge** if someone edits curated directly: the CDC table keeps old rows; the materialized view reflects the deletion after refresh. If you need identical behavior, use streaming + `skipChangeCommits` for both, or switch both to Option B (AUTO CDC with delete propagation).

### Result

| Manual operation on raw | Silver | Gold |
|-------------------------|--------|------|
| `DELETE`                | Row **remains** in silver | Row **remains** in gold |
| `UPDATE`                | Old value **remains** | Old value **remains** |
| New CSV file            | Processed normally | Processed normally |

### Trade-off

Raw and silver can drift apart after cleanup. To realign, run a **full refresh** on affected DLT tables.

---

## Option B: Propagate changes to silver and gold

Use this when deleting or updating raw should also remove or update rows in silver, curated, and gold.

This requires replacing append-only `@dp.table` streaming with **AUTO CDC flows** at each hop, reading the upstream table's CDF.

### Prerequisites

- A **stable business key** on every row (this pipeline uses `empid`).
- CDF enabled on every table in the chain (already configured).
- A one-time **full refresh** after refactoring from append-only to AUTO CDC.

### Step 1 — Raw → Silver

Replace the `silver_events` `@dp.table` definition in `read_and_write_to_silver_schema_enforced.py`:

```python
from pyspark.sql.functions import expr, struct

CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]

@dp.temporary_view(name=table("silver_events_source"))
def silver_events_source():
    df = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(qualified_table("raw_events"))
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )
    df_with_quality = add_quality_checks(df)
    # Always pass deletes through; filter bad rows only on inserts/updates
    return df_with_quality.filter(
        (F.col("_change_type") == "delete") | ~F.col("_is_bad")
    ).drop("_is_bad", "_rescue_data")


dp.create_streaming_table(
    name=table("silver_events"),
    comment="Silver events with CDC from raw",
    cluster_by=["dept_id", "joining_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.targetFileSize": "1gb",
        "delta.enableChangeDataFeed": "true",
        "delta.checkpointInterval": "50",
        "delta.dataSkippingNumIndexedCols": "32",
    },
)

dp.create_auto_cdc_flow(
    target=table("silver_events"),
    source=table("silver_events_source"),
    keys=["empid"],
    sequence_by=struct(F.col("_commit_version"), F.col("_commit_timestamp")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS,
    stored_as_scd_type=1,
)
```

Repeat a similar pattern for `silver_bad_events` if quarantined rows should also be removed when raw is cleaned up.

### Step 2 — Silver → Curated

Replace the append-only curated table in `read_and_write_to_silver_curated_dlt_dq.py`:

```python
from pyspark.sql.functions import expr, struct

CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]

@dp.temporary_view(name=table("silver_curated_source"))
def silver_curated_source():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(qualified_table("silver_events"))
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )

dp.create_streaming_table(
    name=table("silver_curated_events"),
    comment="Curated silver events with CDC from silver",
    cluster_by=["dept_id", "joining_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.targetFileSize": "1gb",
        "delta.enableChangeDataFeed": "true",
        "delta.checkpointInterval": "100",
        "delta.dataSkippingNumIndexedCols": "32",
    },
)

dp.create_auto_cdc_flow(
    target=table("silver_curated_events"),
    source=table("silver_curated_source"),
    keys=["empid"],
    sequence_by=struct(F.col("_commit_version"), F.col("_commit_timestamp")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS,
    stored_as_scd_type=1,
)
```

> **Note:** `@dp.expect_or_drop` decorators do not apply to AUTO CDC target tables. Move quality rules into the source view (as in Step 1) or keep a separate quarantine flow.

### Step 3 — Curated → Gold

Update `write_to_gold_table_cdc.py` to read CDF from curated and handle deletes:

```python
from pyspark.sql.functions import expr, struct

CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]

@dp.temporary_view(name=table("gold_employee_source_cdc"))
def gold_employee_source_cdc():
    df = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(qualified_table("silver_curated_events"))
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )
    return df.withColumns({
        "age": F.floor(F.months_between(F.current_date(), F.col("dob")) / 12),
        "tenure_years": F.floor(F.months_between(F.current_date(), F.col("joining_date")) / 12),
        "salary_band": F.when(F.col("salary") < 30000, "Junior")
                        .when(F.col("salary") < 60000, "Mid")
                        .when(F.col("salary") < 100000, "Senior")
                        .otherwise("Executive"),
    })

dp.create_streaming_table(
    name=table("gold_employee_summary_cdc"),
    ...
)

dp.create_auto_cdc_flow(
    target=table("gold_employee_summary_cdc"),
    source=table("gold_employee_source_cdc"),
    keys=["empid"],
    sequence_by=struct("upd_tmst", "create_tmst", F.col("_commit_version")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS,
    stored_as_scd_type=2,
)
```

With **SCD Type 2**, a delete does not physically remove history. It closes the active version (`__END_AT` is set). The row disappears from the current view but remains in history.

### Result — Option B

| Manual operation on raw | Silver (SCD1) | Gold (SCD2) |
|-------------------------|---------------|-------------|
| `DELETE`                | Row removed   | Active version closed; history retained |
| `UPDATE`                | Row updated   | New SCD2 version opened |
| New CSV file            | New row inserted | New SCD2 version created |

---

## Side-by-side comparison

| | Normal flow (no raw edits) | Raw `DELETE` / `UPDATE` — current code | Option A: `skipChangeCommits` | Option B: AUTO CDC |
|---|---|---|---|---|
| New CSV ingestion | Works | Works | Works | Works |
| Pipeline on raw delete | N/A | **Fails** | Continues | Continues |
| Silver reflects raw delete | N/A | No (pipeline broken) | No | Yes |
| Gold reflects raw delete | N/A | No (pipeline broken) | No | Yes (SCD2 close) |
| Complexity | Low | N/A | Low | Medium |
| Full refresh needed | No | Yes (to recover) | Only to realign drift | Once after refactor |

---

## Operational notes

### Full refresh after changing patterns

Switching between append-only streaming and AUTO CDC (or adding `skipChangeCommits` after a failure) usually requires a full refresh of affected DLT tables. In the Databricks UI: open the pipeline → select the table → **Full refresh**.

### Bad-record quarantine

Good rows flow to `silver_events`; bad rows flow to `silver_bad_events`. If you delete a bad row from raw under Option B, add an AUTO CDC flow for `silver_bad_events` as well, or orphaned quarantine rows will remain.

### Out-of-order deletes (SCD Type 2)

AUTO CDC with SCD2 uses delete tombstones for ordering. If events can arrive late, set the table property:

```python
"pipelines.cdc.tombstoneGCThresholdInSeconds": "<seconds>"
```

Choose a value larger than your maximum expected delay between raw change and pipeline run.

### Materialized view gold

`gold_employee_summary_deduped` in `write_to_gold_table.py` uses `refresh_policy="auto"` with `spark.read.table()` (batch). **`skipChangeCommits` does not apply** — see the [Gold materialized view](#gold-materialized-view--cannot-use-skipchangecommits) section under Option A.

After curated data changes, the view recomputes on refresh. Refresh latency is higher than the streaming gold CDC table, and behavior differs from gold CDC when curated is edited directly.

---

## Files to modify (summary)

| File | Option A (ignore) | Option B (propagate) |
|------|-------------------|----------------------|
| `read_and_write_to_silver_schema_enforced.py` | Add `skipChangeCommits` on `readStream` | Replace with AUTO CDC source + flow |
| `read_and_write_to_silver_curated_dlt_dq.py` | Add `skipChangeCommits` on `readStream` | Replace with AUTO CDC source + flow |
| `write_to_gold_table_cdc.py` | Add `skipChangeCommits` on source view `readStream` | Add `readChangeFeed`, `apply_as_deletes` |
| `write_to_gold_table.py` | No change (`spark.read.table` — not eligible) | No change (MV refreshes from curated state) |
| `read_and_write_to_raw.py` | No change | No change |

---

## References

- [Handle changes to source Delta tables](https://docs.databricks.com/aws/en/structured-streaming/delta-lake) — `skipChangeCommits`, CDF streaming
- [Load data in pipelines](https://docs.databricks.com/aws/en/ldp/load) — ignoring upstream changes in LDP
- [AUTO CDC in pipelines](https://docs.databricks.com/aws/en/ldp/cdc) — `create_auto_cdc_flow`, SCD Type 1/2
- [create_auto_cdc_flow API](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes) — `apply_as_deletes` parameter
