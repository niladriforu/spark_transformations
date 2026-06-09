import schemas
import schemas_silver


def test_employee_schema_raw_fields():
    assert len(schemas.employee_schema_raw.fields) == 9
    assert schemas.employee_schema_raw["empid"].dataType.simpleString() == "string"


def test_employee_schema_silver_fields():
    assert len(schemas_silver.employee_schema_silver.fields) == 8
    assert schemas_silver.employee_schema_silver["dob"].dataType.simpleString() == "date"
