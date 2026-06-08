from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def environment():
    return spark.conf.get("pipeline.environment", "dev")


def catalog():
    return spark.conf.get("pipeline.catalog", "workspace")


def schema():
    return spark.conf.get("pipeline.schema", "default")


def table(base_name):
    return f"{base_name}_{environment()}"


def qualified_table(base_name):
    return f"{catalog()}.{schema()}.{table(base_name)}"
