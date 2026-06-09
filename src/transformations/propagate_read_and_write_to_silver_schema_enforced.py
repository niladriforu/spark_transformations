from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import expr, struct

from pipeline_config import propagate_table, shared_qualified_table
from schemas_silver import employee_schema_silver

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

SILVER_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.targetFileSize": "1gb",
    "delta.enableChangeDataFeed": "true",
    "delta.checkpointInterval": "50",
    "delta.dataSkippingNumIndexedCols": "32",
}


def enforce_schema(df_raw):
    cast_cols = []
    rescue_cols = []
    date_formats = {
        "dob": "M/d/yyyy",
        "joining_date": "M/d/yyyy",
        "create_tmst": "M/d/yyyy",
        "upd_tmst": "M/d/yyyy",
    }

    for field in employee_schema_silver.fields:
        source_col = F.col(field.name)
        if field.name in date_formats and str(field.dataType) == "DateType()":
            casted = F.to_date(source_col, date_formats[field.name]).alias(field.name)
        else:
            casted = source_col.cast(field.dataType).alias(field.name)
        cast_cols.append(casted)

        if field.name in date_formats and str(field.dataType) == "DateType()":
            cast_result = F.to_date(source_col, date_formats[field.name])
        else:
            cast_result = source_col.cast(field.dataType)

        if not field.nullable:
            failed = source_col.isNull() | cast_result.isNull()
        else:
            failed = source_col.isNotNull() & cast_result.isNull()

        rescue_cols.append(F.when(failed, source_col).otherwise(F.lit(None)).alias(field.name))

    df_casted = df_raw.select(
        F.col("_change_type"),
        *cast_cols,
        F.struct(*rescue_cols).alias("_rescue_data"),
    )

    is_bad = F.array_max(
        F.array(
            [
                F.col(f"_rescue_data.{field.name}").isNotNull()
                for field in employee_schema_silver.fields
            ]
        )
    )

    return df_casted.withColumn("_is_bad", is_bad)


def read_raw_cdf():
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table(shared_qualified_table("raw_events"))
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )


@dp.temporary_view(name=propagate_table("silver_events_source"))
def silver_events_source_propagate():
    df_with_quality = enforce_schema(read_raw_cdf())
    return df_with_quality.filter((F.col("_change_type") == "delete") | ~F.col("_is_bad")).drop(
        "_is_bad", "_rescue_data"
    )


@dp.temporary_view(name=propagate_table("silver_bad_events_source"))
def silver_bad_events_source_propagate():
    df_with_quality = enforce_schema(read_raw_cdf())
    return df_with_quality.filter((F.col("_change_type") == "delete") | F.col("_is_bad")).drop(
        "_is_bad", "_rescue_data"
    )


dp.create_streaming_table(
    name=propagate_table("silver_events"),
    comment="Silver events with AUTO CDC from shared raw_events (delete propagation)",
    cluster_by=["dept_id", "joining_date"],
    table_properties=SILVER_TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target=propagate_table("silver_events"),
    source=propagate_table("silver_events_source"),
    keys=["empid"],
    sequence_by=struct(F.col("upd_tmst"), F.col("create_tmst")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name=propagate_table("silver_bad_events"),
    comment="Silver bad events with AUTO CDC from shared raw_events (delete propagation)",
    cluster_by=["dept_id", "joining_date"],
    table_properties={
        **SILVER_TABLE_PROPERTIES,
        "delta.checkpointInterval": "100",
    },
)

dp.create_auto_cdc_flow(
    target=propagate_table("silver_bad_events"),
    source=propagate_table("silver_bad_events_source"),
    keys=["empid"],
    sequence_by=struct(F.col("upd_tmst"), F.col("create_tmst")),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
    stored_as_scd_type=1,
)
