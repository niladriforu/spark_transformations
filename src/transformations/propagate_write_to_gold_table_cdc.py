from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, struct

from pipeline_config import propagate_qualified_table, propagate_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


@dp.temporary_view(name=propagate_table("gold_employee_source_cdc"))
def gold_employee_source_cdc_propagate():
    df = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(propagate_qualified_table("silver_curated_events"))
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
    name=propagate_table("gold_employee_summary_cdc"),
    comment="Gold CDC with delete propagation from propagate curated silver",
    cluster_by=["empid", "dept_id", "joining_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.targetFileSize": "256mb",
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.checkpointInterval": "50",
        "delta.columnMapping.mode": "name",
    },
)

dp.create_auto_cdc_flow(
    target=propagate_table("gold_employee_summary_cdc"),
    source=propagate_table("gold_employee_source_cdc"),
    keys=["empid"],
    sequence_by=struct("upd_tmst", "create_tmst", F.col("_commit_version")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=CDF_COLUMNS,
    stored_as_scd_type=2,
)
