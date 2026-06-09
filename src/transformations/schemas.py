from pyspark.sql.types import DecimalType, StringType, StructField, StructType

# Schema with all date/timestamp columns as STRING for initial read
employee_schema_raw = StructType(
    [
        StructField("empid", StringType(), True),
        StructField("empname", StringType(), True),
        StructField("dob", StringType(), True),  # Read as STRING first
        StructField("salary", DecimalType(10, 2), True),
        StructField("joining_date", StringType(), True),  # Read as STRING first
        StructField("dept_id", StringType(), True),
        StructField("create_tmst", StringType(), True),  # Read as STRING first
        StructField("upd_tmst", StringType(), True),  # Read as STRING first
        StructField("_corrupt_record", StringType(), True),  # ← add this
    ]
)
