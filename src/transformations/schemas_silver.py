from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

# Schema with all date/timestamp columns as STRING for initial read
employee_schema_silver = StructType(
    [
        StructField("empid", StringType(), True),
        StructField("empname", StringType(), True),
        StructField("dob", DateType(), True),  # Read as STRING first
        StructField("salary", DecimalType(10, 2), True),
        StructField("joining_date", DateType(), True),  # Read as STRING first
        StructField("dept_id", StringType(), True),
        StructField("create_tmst", DateType(), True),  # Read as STRING first
        StructField("upd_tmst", DateType(), True),  # Read as STRING first
    ]
)
