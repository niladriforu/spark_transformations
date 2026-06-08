from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_config import qualified_table, table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


@dp.materialized_view(
  name=table("gold_employee_summary"),
  comment="Gold layer: Materialized view with business logic and performance optimizations",
  cluster_by=["empid", "dept_id", "joining_date"],
  refresh_policy="auto",
  table_properties={
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enablePredictiveOptimization": "true",
    "delta.targetFileSize": "256mb",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.checkpointInterval": "50",
    "delta.columnMapping.mode": "name"
  }
)
def gold_employee_summary():
    """Materialized view with computed columns for analytics"""
    df = spark.read.table(qualified_table("silver_curated_events"))

    return df.withColumns({
        "age": F.floor(F.months_between(F.current_date(), F.col("dob")) / 12),
        "tenure_years": F.floor(F.months_between(F.current_date(), F.col("joining_date")) / 12),
        "salary_band": F.when(F.col("salary") < 30000, "Junior")
                        .when(F.col("salary") < 60000, "Mid")
                        .when(F.col("salary") < 100000, "Senior")
                        .otherwise("Executive")
    })
