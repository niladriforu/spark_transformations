from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

from pipeline_config import qualified_table, table
from schemas_silver import employee_schema_silver

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def add_quality_checks(df_raw):
    """Helper function to add casting and quality check columns"""
    cast_cols = []
    rescue_cols = []

    # Define date format mapping (adjust formats based on your actual data)
    date_formats = {
        "dob": "M/d/yyyy",  # e.g., "1990-05-15"
        "joining_date": "M/d/yyyy",  # e.g., "2020-01-10"
        "create_tmst": "M/d/yyyy",  # e.g., "2024-01-01"
        "upd_tmst": "M/d/yyyy",  # e.g., "2024-01-01"
    }

    for field in employee_schema_silver.fields:
        source_col = F.col(field.name)
        # Use to_date() with format for date fields, otherwise cast normally
        if field.name in date_formats and str(field.dataType) == "DateType()":
            casted = F.to_date(source_col, date_formats[field.name]).alias(field.name)
        else:
            casted = source_col.cast(field.dataType).alias(field.name)
        cast_cols.append(casted)

        # Calculate cast result (same logic as above, without alias)
        if field.name in date_formats and str(field.dataType) == "DateType()":
            cast_result = F.to_date(source_col, date_formats[field.name])
        else:
            cast_result = source_col.cast(field.dataType)

        if not field.nullable:
            failed = source_col.isNull() | cast_result.isNull()
        else:
            failed = source_col.isNotNull() & cast_result.isNull()

        rescue_cols.append(F.when(failed, source_col).otherwise(F.lit(None)).alias(field.name))

    df_casted = df_raw.select(*cast_cols, F.struct(*rescue_cols).alias("_rescue_data"))

    is_bad = F.array_max(
        F.array(
            [
                F.col(f"_rescue_data.{field.name}").isNotNull()
                for field in employee_schema_silver.fields
            ]
        )
    )

    return df_casted.withColumn("_is_bad", is_bad)


@dp.table(
    name=table("silver_events"),
    comment="Raw events from Volume",
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
def silver_events():
    df_raw = spark.readStream.option("skipChangeCommits", "true").table(
        qualified_table("raw_events")
    )
    df_with_quality = add_quality_checks(df_raw)
    # Return only good records
    return df_with_quality.filter(~F.col("_is_bad")).drop("_is_bad", "_rescue_data")


@dp.table(
    name=table("silver_bad_events"),
    comment="silver bad events ",
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
def silver_bad_events():
    df_raw = spark.readStream.option("skipChangeCommits", "true").table(
        qualified_table("raw_events")
    )
    df_with_quality = add_quality_checks(df_raw)
    # Return only good records
    return df_with_quality.filter(F.col("_is_bad"))
