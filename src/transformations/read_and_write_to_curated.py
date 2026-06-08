import dlt
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pipeline_config import table, qualified_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

@dlt.table(
  name=table("silver_curated_events"),
  comment="Curated silver events with multiple quality checks",
  cluster_by=["dept_id", "joining_date"],
  table_properties={
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.targetFileSize": "1gb",
    "delta.enableChangeDataFeed": "true",
    "delta.checkpointInterval": "100",
    "delta.dataSkippingNumIndexedCols": "32"
  }
)
@dlt.expect_or_drop("valid_dob", "dob IS NOT NULL")
@dlt.expect_or_drop("valid_empid", "empid IS NOT NULL")
@dlt.expect_or_drop("valid_salary", "salary > 0")
@dlt.expect("valid_dept_format", "dept_id LIKE 'D%'")  # Logs only, doesn't drop
def silver_curated_events():
    """
    Multiple quality checks:
    - Drops records with null dob, empid, or invalid salary
    - Logs (but keeps) records with invalid dept_id format
    """
    df = dlt.read_stream(qualified_table("silver_events"))
    return df


@dlt.table(name=table("silver_curated_bad_events"))
def silver_curated_bad_events():
    """Quarantined records that failed quality checks"""
    df = dlt.read_stream(qualified_table("silver_events"))
    
    # Capture all records that would be dropped by expectations
    df_bad = df.filter(
        (F.col("dob").isNull()) |
        (F.col("empid").isNull()) |
        (F.col("salary") <= 0)
    )
    
    # Add column to explain what failed
    df_bad = df_bad.withColumn(
        "failure_reason",
        F.when(F.col("dob").isNull(), "NULL_DOB")
         .when(F.col("empid").isNull(), "NULL_EMPID")
         .when(F.col("salary") <= 0, "INVALID_SALARY")
         .otherwise("MULTIPLE_FAILURES")
    )
    
    return df_bad