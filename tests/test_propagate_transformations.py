from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType

from conftest import (
    CDF_DELETE_ROW,
    CDF_ROW,
    RAW_STRING_ROW,
    SILVER_ROW,
    SILVER_ROW_BAD,
    cdf_df,
    patch_read_stream,
    patch_read_table,
    silver_df,
)


def cdf_row(**overrides):
    data = CDF_ROW.asDict()
    data.update(overrides)
    return Row(**data)


def _mock_cdf_read(module, result_df):
    patch_read_stream(module, result_df, filter_after_table=True)


def test_enforce_schema(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_schema_enforced"]
    from pyspark.sql.types import StringType, StructField, StructType

    from schemas import employee_schema_raw

    schema = StructType(
        [StructField("_change_type", StringType(), True), *employee_schema_raw.fields]
    )
    cdf_input = spark.createDataFrame(
        [{"_change_type": "insert", **RAW_STRING_ROW.asDict()}],
        schema,
    )
    result = propagate.enforce_schema(cdf_input)
    assert "_is_bad" in result.columns
    assert result.filter("_is_bad = false").count() == 1


def test_enforce_schema_bad_row(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_schema_enforced"]
    from pyspark.sql.types import StringType, StructField, StructType

    from schemas import employee_schema_raw

    schema = StructType(
        [StructField("_change_type", StringType(), True), *employee_schema_raw.fields]
    )
    bad = RAW_STRING_ROW.asDict()
    bad["_change_type"] = "insert"
    bad["dob"] = "bad-date"
    cdf_input = spark.createDataFrame([bad], schema)
    result = propagate.enforce_schema(cdf_input)
    assert result.filter("_is_bad = true").count() == 1


def test_enforce_schema_non_nullable_field_branch(modules, spark, monkeypatch):
    propagate = modules["propagate_read_and_write_to_silver_schema_enforced"]
    required_schema = StructType([StructField("empid", StringType(), False)])
    monkeypatch.setattr(propagate, "employee_schema_silver", required_schema)
    schema = StructType(
        [
            StructField("_change_type", StringType(), True),
            StructField("empid", StringType(), True),
        ]
    )
    cdf_input = spark.createDataFrame(
        [{"_change_type": "insert", "empid": "E001"}, {"_change_type": "insert", "empid": None}],
        schema,
    )
    result = propagate.enforce_schema(cdf_input)
    assert result.count() == 2


def test_read_raw_cdf(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_schema_enforced"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW))
    result = propagate.read_raw_cdf()
    assert result.count() == 1


def test_silver_events_source_propagate(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_schema_enforced"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW, CDF_DELETE_ROW))
    result = propagate.silver_events_source_propagate()
    assert "_change_type" in result.columns
    assert result.count() >= 1


def test_silver_bad_events_source_propagate(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_schema_enforced"]
    bad = RAW_STRING_ROW.asDict()
    bad["_change_type"] = "insert"
    bad["dob"] = "invalid"
    from pyspark.sql.types import StringType, StructField, StructType

    from schemas import employee_schema_raw

    schema = StructType(
        [StructField("_change_type", StringType(), True), *employee_schema_raw.fields]
    )
    _mock_cdf_read(propagate, spark.createDataFrame([bad], schema))
    result = propagate.silver_bad_events_source_propagate()
    assert result.count() >= 1


def test_passes_curated_quality(modules):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    expr = propagate.passes_curated_quality()
    assert expr is not None


def test_read_propagate_silver_cdf(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW))
    assert propagate.read_propagate_silver_cdf().count() == 1


def test_read_propagate_silver_bad_cdf(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW))
    assert propagate.read_propagate_silver_bad_cdf().count() == 1


def test_silver_curated_events_source_propagate(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW, CDF_DELETE_ROW))
    result = propagate.silver_curated_events_source_propagate()
    assert result.count() >= 1


def test_silver_curated_bad_events_source_propagate(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    row = SILVER_ROW_BAD.asDict()
    row["_change_type"] = "insert"
    _mock_cdf_read(propagate, cdf_df(spark, cdf_row(**row)))
    result = propagate.silver_curated_bad_events_source_propagate()
    assert result.collect()[0].failure_reason == "NULL_DOB"


def test_silver_curated_bad_events_source_delete(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_DELETE_ROW))
    result = propagate.silver_curated_bad_events_source_propagate()
    assert result.collect()[0].failure_reason == "DELETED"


def test_silver_curated_bad_events_source_null_empid(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    row = SILVER_ROW.asDict()
    row["_change_type"] = "insert"
    row["empid"] = None
    _mock_cdf_read(propagate, cdf_df(spark, cdf_row(**row)))
    assert (
        propagate.silver_curated_bad_events_source_propagate().collect()[0].failure_reason
        == "NULL_EMPID"
    )


def test_silver_curated_bad_events_source_invalid_salary(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    row = SILVER_ROW.asDict()
    row["_change_type"] = "insert"
    row["salary"] = Decimal("0.00")
    _mock_cdf_read(propagate, cdf_df(spark, cdf_row(**row)))
    assert (
        propagate.silver_curated_bad_events_source_propagate().collect()[0].failure_reason
        == "INVALID_SALARY"
    )


def test_silver_curated_bad_events_source_multiple_failures(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_dlt_dq"]
    row = SILVER_ROW.asDict()
    row["_change_type"] = "insert"
    row["dob"] = None
    row["empid"] = None
    row["salary"] = Decimal("-1.00")
    _mock_cdf_read(propagate, cdf_df(spark, cdf_row(**row)))
    assert (
        propagate.silver_curated_bad_events_source_propagate().collect()[0].failure_reason
        == "NULL_DOB"
    )


def _quarantine_cdf(spark, *rows):
    return (
        cdf_df(spark, *rows)
        .withColumn("quarantine_reason", lit(""))
        .withColumn("is_quarantined", lit(False))
    )


def _bad_quarantine_cdf(spark, row):
    return (
        cdf_df(spark, row)
        .withColumn("quarantine_reason", lit("valid_dob"))
        .withColumn("is_quarantined", lit(True))
    )


def test_read_propagate_silver_cdf_for_quarantine(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_alternate_dq"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW))
    assert propagate.read_propagate_silver_cdf_for_quarantine().count() == 1


def test_read_propagate_quarantine_cdf(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_alternate_dq"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW))
    assert propagate.read_propagate_quarantine_cdf().count() == 1


def test_with_quarantine_columns(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_alternate_dq"]
    row_bad = SILVER_ROW_BAD.asDict()
    row_bad["_change_type"] = "insert"
    result = propagate.with_quarantine_columns(cdf_df(spark, CDF_ROW, cdf_row(**row_bad)))
    rows = result.collect()
    assert any(row.is_quarantined for row in rows)


def test_silver_quarantine_source_propagate(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_alternate_dq"]
    _mock_cdf_read(propagate, cdf_df(spark, CDF_ROW))
    result = propagate.silver_quarantine_source_propagate()
    assert "quarantine_reason" in result.columns


def test_silver_curated_events_manual_dq_source_propagate(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_alternate_dq"]
    _mock_cdf_read(propagate, _quarantine_cdf(spark, CDF_ROW, CDF_DELETE_ROW))
    result = propagate.silver_curated_events_manual_dq_source_propagate()
    assert result.count() >= 1


def test_silver_curated_bad_events_manual_dq_source_propagate(modules, spark):
    propagate = modules["propagate_read_and_write_to_silver_curated_alternate_dq"]
    row = SILVER_ROW_BAD.asDict()
    row["_change_type"] = "insert"
    _mock_cdf_read(propagate, _bad_quarantine_cdf(spark, cdf_row(**row)))
    result = propagate.silver_curated_bad_events_manual_dq_source_propagate()
    assert result.count() >= 1


def test_gold_employee_summary_deduped_propagate(modules, spark):
    gold = modules["propagate_write_to_gold_table"]
    patch_read_table(gold, silver_df(spark, SILVER_ROW))
    result = gold.gold_employee_summary_deduped_propagate()
    assert result.collect()[0].salary_band == "Senior"


def test_gold_employee_source_cdc_propagate(modules, spark):
    gold_cdc = modules["propagate_write_to_gold_table_cdc"]
    _mock_cdf_read(gold_cdc, cdf_df(spark, CDF_ROW))
    result = gold_cdc.gold_employee_source_cdc_propagate()
    assert result.collect()[0].age is not None


def test_gold_propagate_salary_bands(modules, spark):
    gold = modules["propagate_write_to_gold_table"]

    def assert_band(salary, expected):
        row = SILVER_ROW.asDict()
        row["salary"] = Decimal(str(salary))
        patch_read_table(gold, spark.createDataFrame([row], silver_df(spark).schema))
        result = gold.gold_employee_summary_deduped_propagate().collect()[0]
        assert result.salary_band == expected

    assert_band(25000, "Junior")
    assert_band(45000, "Mid")
    assert_band(80000, "Senior")
    assert_band(150000, "Executive")
