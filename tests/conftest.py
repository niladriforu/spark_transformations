"""Shared pytest fixtures for transformation unit tests."""

from __future__ import annotations

import importlib
import sys
import types
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

TRANSFORMATION_MODULES = [
    "pipeline_config",
    "schemas",
    "schemas_silver",
    "read_and_write_to_raw",
    "read_and_write_to_silver_schema_enforced",
    "read_and_write_to_silver_curated_dlt_dq",
    "read_and_write_to_silver_curated_alternate_dq",
    "write_to_gold_table",
    "write_to_gold_table_cdc",
    "propagate_read_and_write_to_silver_schema_enforced",
    "propagate_read_and_write_to_silver_curated_dlt_dq",
    "propagate_read_and_write_to_silver_curated_alternate_dq",
    "propagate_write_to_gold_table",
    "propagate_write_to_gold_table_cdc",
]

SILVER_ROW = Row(
    empid="E001",
    empname="Alice",
    dob=date(1990, 1, 15),
    salary=Decimal("75000.00"),
    joining_date=date(2020, 3, 1),
    dept_id="D01",
    create_tmst=date(2024, 1, 1),
    upd_tmst=date(2024, 6, 1),
)

SILVER_ROW_BAD = Row(
    empid="E002",
    empname="Bob",
    dob=None,
    salary=Decimal("0.00"),
    joining_date=date(2021, 5, 1),
    dept_id="X99",
    create_tmst=date(2024, 2, 1),
    upd_tmst=date(2024, 7, 1),
)

CDF_ROW = Row(
    _change_type="insert",
    empid="E001",
    empname="Alice",
    dob=date(1990, 1, 15),
    salary=Decimal("75000.00"),
    joining_date=date(2020, 3, 1),
    dept_id="D01",
    create_tmst=date(2024, 1, 1),
    upd_tmst=date(2024, 6, 1),
)

CDF_DELETE_ROW = Row(
    _change_type="delete",
    empid="E003",
    empname="Carol",
    dob=date(1988, 2, 2),
    salary=Decimal("90000.00"),
    joining_date=date(2019, 4, 4),
    dept_id="D02",
    create_tmst=date(2023, 3, 3),
    upd_tmst=date(2024, 5, 5),
)

RAW_STRING_ROW = Row(
    empid="E001",
    empname="Alice",
    dob="1/15/1990",
    salary=Decimal("75000.00"),
    joining_date="3/1/2020",
    dept_id="D01",
    create_tmst="1/1/2024",
    upd_tmst="6/1/2024",
    _corrupt_record=None,
)

SILVER_SCHEMA = StructType(
    [
        StructField("empid", StringType(), True),
        StructField("empname", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("salary", DecimalType(10, 2), True),
        StructField("joining_date", DateType(), True),
        StructField("dept_id", StringType(), True),
        StructField("create_tmst", DateType(), True),
        StructField("upd_tmst", DateType(), True),
    ]
)

CDF_SILVER_SCHEMA = StructType(
    [StructField("_change_type", StringType(), True), *SILVER_SCHEMA.fields]
)

QUARANTINE_SCHEMA = StructType(
    [
        *CDF_SILVER_SCHEMA.fields,
        StructField("quarantine_reason", StringType(), True),
        StructField("is_quarantined", StringType(), True),
    ]
)


def _passthrough_decorator(*args, **kwargs):
    if len(args) == 1 and callable(args[0]):
        return args[0]

    def decorator(fn):
        return fn

    return decorator


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark-transformations-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.conf.set("pipeline.environment", "dev")
    session.conf.set("pipeline.catalog", "workspace")
    session.conf.set("pipeline.schema", "default")
    session.conf.set("pipeline.reprocess_days", "0")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def mock_pipelines():
    dp = types.ModuleType("pyspark.pipelines")
    dp.table = _passthrough_decorator
    dp.temporary_view = _passthrough_decorator
    dp.materialized_view = _passthrough_decorator
    dp.expect = _passthrough_decorator
    dp.expect_or_drop = _passthrough_decorator
    dp.create_streaming_table = MagicMock()
    dp.create_auto_cdc_flow = MagicMock()
    sys.modules["pyspark.pipelines"] = dp
    return dp


@pytest.fixture(scope="session")
def modules(spark, mock_pipelines):
    loaded = {}
    for name in TRANSFORMATION_MODULES:
        loaded[name] = importlib.import_module(name)
    return loaded


def silver_df(spark, *rows):
    return spark.createDataFrame(list(rows), SILVER_SCHEMA)


def cdf_df(spark, *rows):
    return spark.createDataFrame(list(rows), CDF_SILVER_SCHEMA)


def raw_string_df(spark, *rows):
    from schemas import employee_schema_raw

    return spark.createDataFrame(list(rows), employee_schema_raw)


def patch_read_stream(module, result_df, *, filter_after_table=False):
    mock_spark = MagicMock()
    mock_spark.conf = module.spark.conf
    chain = MagicMock()
    chain.format.return_value = chain
    chain.option.return_value = chain
    chain.schema.return_value = chain
    if filter_after_table:
        chain.table.return_value = chain
        chain.filter.return_value = result_df
    else:
        chain.table.return_value = result_df
    chain.load.return_value = result_df
    mock_spark.readStream = chain
    module.spark = mock_spark
    return chain


def patch_read_table(module, result_df):
    mock_spark = MagicMock()
    mock_spark.conf = module.spark.conf
    read = MagicMock()
    read.table.return_value = result_df
    mock_spark.read = read
    mock_spark.readStream = MagicMock()
    module.spark = mock_spark
    return read


def patch_quarantine_stream(module, result_df):
    mock_spark = MagicMock()
    mock_spark.conf = module.spark.conf
    table_chain = MagicMock()
    table_chain.filter.return_value = result_df
    stream = MagicMock()
    stream.table.return_value = table_chain
    mock_spark.readStream = stream
    module.spark = mock_spark
    return stream
