from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, struct

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


def read_propagate_silver_cdf():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(propagate_qualified_table("silver_events"))
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )


def read_propagate_silver_bad_cdf():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(propagate_qualified_table("silver_bad_events"))
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )


def passes_curated_quality():
    return (
        (F.col("_change_type") == "delete")
        | (
            F.col("dob").isNotNull()
            & F.col("empid").isNotNull()
            & (F.col("salary") > 0)
        )
    )


@dp.temporary_view(name=propagate_table("silver_curated_events_source"))
def silver_curated_events_source_propagate():
    return read_propagate_silver_cdf().filter(passes_curated_quality())


@dp.temporary_view(name=propagate_table("silver_curated_bad_events_source"))
def silver_curated_bad_events_source_propagate():
    df = read_propagate_silver_bad_cdf()
    return df.withColumn(
        "failure_reason",
        F.when(F.col("_change_type") == "delete", "DELETED")
         .when(F.col("dob").isNull(), "NULL_DOB")
         .when(F.col("empid").isNull(), "NULL_EMPID")
         .when(F.col("salary") <= 0, "INVALID_SALARY")
         .otherwise("MULTIPLE_FAILURES"),
    )


dp.create_streaming_table(
    name=propagate_table("silver_curated_events"),
    comment="Curated silver events with AUTO CDC (delete propagation)",
    cluster_by=["dept_id", "joining_date"],
    table_properties=CURATED_TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target=propagate_table("silver_curated_events"),
    source=propagate_table("silver_curated_events_source"),
    keys=["empid"],
    sequence_by=struct(F.col("_commit_version"), F.col("_commit_timestamp")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS,
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name=propagate_table("silver_curated_bad_events"),
    comment="Curated bad events with AUTO CDC (delete propagation)",
)

dp.create_auto_cdc_flow(
    target=propagate_table("silver_curated_bad_events"),
    source=propagate_table("silver_curated_bad_events_source"),
    keys=["empid"],
    sequence_by=struct(F.col("_commit_version"), F.col("_commit_timestamp")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS,
    stored_as_scd_type=1,
)
