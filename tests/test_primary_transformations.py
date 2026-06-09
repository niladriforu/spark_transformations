from datetime import date
from decimal import Decimal

from pyspark.sql.types import StringType, StructField, StructType

from conftest import (
    CDF_DELETE_ROW,
    CDF_ROW,
    RAW_STRING_ROW,
    SILVER_ROW,
    SILVER_ROW_BAD,
    patch_quarantine_stream,
    patch_read_stream,
    patch_read_table,
    raw_string_df,
    silver_df,
)


def test_raw_events_default_reprocess_date(modules, spark):
    raw = modules["read_and_write_to_raw"]
    spark.conf.set("pipeline.reprocess_days", "0")
    patch_read_stream(raw, raw_string_df(spark, RAW_STRING_ROW))
    result = raw.raw_events()
    assert result.count() == 1


def test_raw_events_custom_reprocess_days(modules, spark):
    raw = modules["read_and_write_to_raw"]
    spark.conf.set("pipeline.reprocess_days", "7")
    patch_read_stream(raw, raw_string_df(spark, RAW_STRING_ROW))
    result = raw.raw_events()
    assert result.count() == 1
    spark.conf.set("pipeline.reprocess_days", "0")


def test_add_quality_checks_good_and_bad_rows(modules, spark):
    silver = modules["read_and_write_to_silver_schema_enforced"]
    bad_raw = RAW_STRING_ROW.asDict()
    bad_raw["empid"] = "E002"
    bad_raw["dob"] = "not-a-date"
    bad_row = type(RAW_STRING_ROW)(**bad_raw)

    df = raw_string_df(spark, RAW_STRING_ROW, bad_row)
    checked = silver.add_quality_checks(df)
    rows = checked.select("empid", "_is_bad").collect()
    by_id = {row.empid: row._is_bad for row in rows}
    assert by_id["E001"] is False
    assert by_id["E002"] is True


def test_add_quality_checks_non_nullable_field_branch(modules, spark, monkeypatch):
    silver = modules["read_and_write_to_silver_schema_enforced"]
    required_schema = StructType([StructField("empid", StringType(), False)])
    monkeypatch.setattr(silver, "employee_schema_silver", required_schema)
    df = spark.createDataFrame([("E001",), (None,)], "empid string")
    result = silver.add_quality_checks(df)
    assert "_is_bad" in result.columns
    assert result.count() == 2


def test_silver_events_filters_good_rows(modules, spark):
    silver = modules["read_and_write_to_silver_schema_enforced"]
    patch_read_stream(silver, raw_string_df(spark, RAW_STRING_ROW))
    result = silver.silver_events()
    assert "_is_bad" not in result.columns
    assert result.count() == 1


def test_silver_bad_events_keeps_bad_rows(modules, spark):
    silver = modules["read_and_write_to_silver_schema_enforced"]
    bad_raw = RAW_STRING_ROW.asDict()
    bad_raw["dob"] = "invalid"
    bad_row = type(RAW_STRING_ROW)(**bad_raw)
    patch_read_stream(silver, raw_string_df(spark, bad_row))
    result = silver.silver_bad_events()
    assert result.count() == 1


def test_silver_curated_events(modules, spark):
    curated = modules["read_and_write_to_silver_curated_dlt_dq"]
    patch_read_stream(curated, silver_df(spark, SILVER_ROW))
    result = curated.silver_curated_events()
    assert result.count() == 1


def test_silver_curated_bad_events_null_dob(modules, spark):
    curated = modules["read_and_write_to_silver_curated_dlt_dq"]
    patch_read_stream(curated, silver_df(spark, SILVER_ROW_BAD))
    result = curated.silver_curated_bad_events()
    assert result.collect()[0].failure_reason == "NULL_DOB"


def test_silver_curated_bad_events_null_empid(modules, spark):
    curated = modules["read_and_write_to_silver_curated_dlt_dq"]
    row = SILVER_ROW_BAD.asDict()
    row["dob"] = date(1990, 1, 1)
    row["empid"] = None
    patch_read_stream(curated, spark.createDataFrame([row], silver_df(spark).schema))
    result = curated.silver_curated_bad_events()
    assert result.collect()[0].failure_reason == "NULL_EMPID"


def test_silver_curated_bad_events_invalid_salary(modules, spark):
    curated = modules["read_and_write_to_silver_curated_dlt_dq"]
    row = SILVER_ROW.asDict()
    row["salary"] = Decimal("0.00")
    patch_read_stream(curated, spark.createDataFrame([row], silver_df(spark).schema))
    result = curated.silver_curated_bad_events()
    assert result.collect()[0].failure_reason == "INVALID_SALARY"


def test_silver_curated_bad_events_multiple_failures(modules, spark):
    curated = modules["read_and_write_to_silver_curated_dlt_dq"]
    row = SILVER_ROW.asDict()
    row["dob"] = None
    row["empid"] = None
    row["salary"] = Decimal("-1.00")
    patch_read_stream(curated, spark.createDataFrame([row], silver_df(spark).schema))
    result = curated.silver_curated_bad_events()
    assert result.collect()[0].failure_reason == "NULL_DOB"


def test_silver_quarantine(modules, spark):
    alternate = modules["read_and_write_to_silver_curated_alternate_dq"]
    patch_read_stream(alternate, silver_df(spark, SILVER_ROW, SILVER_ROW_BAD))
    result = alternate.silver_quarantine()
    rows = result.collect()
    assert len(rows) == 2
    assert any(row.is_quarantined for row in rows)


def _mock_quarantine_read(module, result_df):
    patch_quarantine_stream(module, result_df)


def test_silver_curated_events_manual_dq(modules, spark):
    alternate = modules["read_and_write_to_silver_curated_alternate_dq"]
    _mock_quarantine_read(alternate, silver_df(spark, SILVER_ROW))
    result = alternate.silver_curated_events_manual_dq()
    assert result.count() == 1


def test_silver_curated_bad_events_manual_dq(modules, spark):
    alternate = modules["read_and_write_to_silver_curated_alternate_dq"]
    _mock_quarantine_read(alternate, silver_df(spark, SILVER_ROW_BAD))
    result = alternate.silver_curated_bad_events_manual_dq()
    assert result.count() == 1


def test_gold_employee_summary_deduped(modules, spark):
    gold = modules["write_to_gold_table"]
    older = SILVER_ROW.asDict()
    older["upd_tmst"] = date(2024, 1, 1)
    newer = SILVER_ROW.asDict()
    newer["upd_tmst"] = date(2024, 8, 1)
    patch_read_table(
        gold,
        spark.createDataFrame([older, newer], silver_df(spark).schema),
    )
    result = gold.gold_employee_summary_deduped()
    row = result.collect()[0]
    assert row.salary_band == "Senior"
    assert row.age is not None


def test_gold_employee_source_cdc(modules, spark):
    gold_cdc = modules["write_to_gold_table_cdc"]
    patch_read_stream(gold_cdc, silver_df(spark, SILVER_ROW))
    result = gold_cdc.gold_employee_source_cdc()
    row = result.collect()[0]
    assert row.tenure_years is not None


def test_gold_salary_bands(modules, spark):
    gold = modules["write_to_gold_table"]

    def assert_band(salary, expected):
        row = SILVER_ROW.asDict()
        row["salary"] = Decimal(str(salary))
        patch_read_table(gold, spark.createDataFrame([row], silver_df(spark).schema))
        result = gold.gold_employee_summary_deduped().collect()[0]
        assert result.salary_band == expected

    assert_band(25000, "Junior")
    assert_band(45000, "Mid")
    assert_band(80000, "Senior")
    assert_band(150000, "Executive")
