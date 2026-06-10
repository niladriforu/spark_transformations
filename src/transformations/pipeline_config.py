from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def environment():
    return spark.conf.get("pipeline.environment", "dev")


def catalog():
    return spark.conf.get("pipeline.catalog", "main")


def schema():
    return spark.conf.get("pipeline.schema", "dbdemos_sdp_cdc")


def table_variant():
    return spark.conf.get("pipeline.table_variant", "")


def table(base_name):
    env = environment()
    variant = table_variant()
    if variant:
        return f"{base_name}_{variant}_{env}"
    return f"{base_name}_{env}"


def qualified_table(base_name):
    return f"{catalog()}.{schema()}.{table(base_name)}"


def propagate_table(base_name):
    """Propagate-pipeline tables; distinct from primary pipeline table names."""
    return f"{base_name}_propagate_{environment()}"


def propagate_qualified_table(base_name):
    return f"{catalog()}.{schema()}.{propagate_table(base_name)}"


def shared_qualified_table(base_name):
    """Primary-pipeline tables without a variant suffix (e.g. shared raw_events)."""
    return f"{catalog()}.{schema()}.{base_name}_{environment()}"
