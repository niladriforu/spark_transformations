import dlt
from pipeline_config import table, qualified_table
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name=table("silver_curated_events"),
    comment="Curated employee events for analytics and metric views",
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
def silver_curated_events():
    df = dlt.read_stream(qualified_table("silver_events"))
    window = Window.partitionBy("empid").orderBy(F.col("upd_tmst").desc_nulls_last())

    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
