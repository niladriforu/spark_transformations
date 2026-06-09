from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pipeline_config import qualified_table, table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


bad_rules = {
    "valid_dob": "dob IS  NULL",
    "valid_empid": "empid IS  NULL",
    "valid_salary": "salary <= 0 OR salary IS NULL",
    "valid_dept_format": "dept_id NOT LIKE 'D%'",
}

@dp.table(
    temporary=True,
    partition_cols=["is_quarantined"],
)
def silver_quarantine():
    df = spark.readStream.table(qualified_table("silver_events"))
    
    # Build reason_parts
    reason_parts = []
    for rule_name, rule_condition in bad_rules.items():
        reason_parts.append(
            when(expr(rule_condition), lit(rule_name)).otherwise(lit(None))
        )
    
    # First: Compute quarantine_reason
    df = df.withColumn(
        "quarantine_reason",
        concat_ws("; ", array(*reason_parts))
    )
    
    # Second: Derive is_quarantined from quarantine_reason
    # If reason is non-empty, record is quarantined
    df = df.withColumn(
        "is_quarantined",
        length(col("quarantine_reason")) > 0
    )
    
    return df

@dp.table(
  name=table("silver_curated_events_manual_dq"),
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
def silver_curated_events_manual_dq():
    """
    Multiple quality checks:
    - Drops records with null dob, empid, or invalid salary
    - Logs (but keeps) records with invalid dept_id format
    """
    return spark.readStream.table("silver_quarantine").filter(length(col("quarantine_reason")) == 0)


@dp.table(name=table("silver_curated_bad_events_manual_dq"))
def silver_curated_bad_events_manual_dq():
    """Quarantined records that failed quality checks"""
    return spark.readStream.table("silver_quarantine").filter(length(col("quarantine_reason")) > 0)

