from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, concat_ws, expr, length, lit, struct, when

from pipeline_config import propagate_qualified_table, propagate_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]

CURATED_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.targetFileSize": "1gb",
    "delta.enableChangeDataFeed": "true",
    "delta.checkpointInterval": "100",
    "delta.dataSkippingNumIndexedCols": "32",
}

BAD_RULES = {
    "valid_dob": "dob IS NULL",
    "valid_empid": "empid IS NULL",
    "valid_salary": "salary <= 0 OR salary IS NULL",
    "valid_dept_format": "dept_id NOT LIKE 'D%'",
}


def read_propagate_silver_cdf_for_quarantine():
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table(propagate_qualified_table("silver_events"))
        .filter(col("_change_type").isin("insert", "update_postimage", "delete"))
    )


def read_propagate_quarantine_cdf():
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table(propagate_qualified_table("silver_quarantine"))
        .filter(col("_change_type").isin("insert", "update_postimage", "delete"))
    )


def with_quarantine_columns(df):
    reason_parts = [
        when(expr(rule_condition), lit(rule_name)).otherwise(lit(None))
        for rule_name, rule_condition in BAD_RULES.items()
    ]
    df = df.withColumn("quarantine_reason", concat_ws("; ", array(*reason_parts)))
    return df.withColumn("is_quarantined", length(col("quarantine_reason")) > 0)


@dp.temporary_view(name=propagate_table("silver_quarantine_source"))
def silver_quarantine_source_propagate():
    return with_quarantine_columns(read_propagate_silver_cdf_for_quarantine())


@dp.temporary_view(name=propagate_table("silver_curated_events_manual_dq_source"))
def silver_curated_events_manual_dq_source_propagate():
    return read_propagate_quarantine_cdf().filter(
        (col("_change_type") == "delete") | (length(col("quarantine_reason")) == 0)
    )


@dp.temporary_view(name=propagate_table("silver_curated_bad_events_manual_dq_source"))
def silver_curated_bad_events_manual_dq_source_propagate():
    return read_propagate_quarantine_cdf().filter(
        (col("_change_type") == "delete") | (length(col("quarantine_reason")) > 0)
    )


dp.create_streaming_table(
    name=propagate_table("silver_quarantine"),
    comment="Quarantine staging with AUTO CDC from propagate silver (delete propagation)",
    partition_cols=["is_quarantined"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    target=propagate_table("silver_quarantine"),
    source=propagate_table("silver_quarantine_source"),
    keys=["empid"],
    sequence_by=struct(col("_commit_version"), col("_commit_timestamp")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS,
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name=propagate_table("silver_curated_events_manual_dq"),
    comment="Manual DQ curated events with AUTO CDC (delete propagation)",
    cluster_by=["dept_id", "joining_date"],
    table_properties=CURATED_TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target=propagate_table("silver_curated_events_manual_dq"),
    source=propagate_table("silver_curated_events_manual_dq_source"),
    keys=["empid"],
    sequence_by=struct(col("_commit_version"), col("_commit_timestamp")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS + ["quarantine_reason", "is_quarantined"],
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name=propagate_table("silver_curated_bad_events_manual_dq"),
    comment="Manual DQ bad events with AUTO CDC (delete propagation)",
)

dp.create_auto_cdc_flow(
    target=propagate_table("silver_curated_bad_events_manual_dq"),
    source=propagate_table("silver_curated_bad_events_manual_dq_source"),
    keys=["empid"],
    sequence_by=struct(col("_commit_version"), col("_commit_timestamp")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS + ["quarantine_reason", "is_quarantined"],
    stored_as_scd_type=1,
)
