from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from pipeline_config import propagate_qualified_table, propagate_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name=propagate_table("gold_employee_summary_deduped"),
    comment="Gold MV with delete propagation from propagate curated silver",
    cluster_by=["empid", "dept_id", "joining_date"],
    refresh_policy="auto",
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
def gold_employee_summary_deduped_propagate():
    df = spark.read.table(propagate_qualified_table("silver_curated_events"))

    latest_by_empid = Window.partitionBy("empid").orderBy(
        F.col("upd_tmst").desc_nulls_last(),
        F.col("create_tmst").desc_nulls_last(),
    )

    df_latest = (
        df.withColumn("_row_num", F.row_number().over(latest_by_empid))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    return df_latest.withColumns(
        {
            "age": F.floor(F.months_between(F.current_date(), F.col("dob")) / 12),
            "tenure_years": F.floor(F.months_between(F.current_date(), F.col("joining_date")) / 12),
            "salary_band": F.when(F.col("salary") < 30000, "Junior")
            .when(F.col("salary") < 60000, "Mid")
            .when(F.col("salary") < 100000, "Senior")
            .otherwise("Executive"),
        }
    )
