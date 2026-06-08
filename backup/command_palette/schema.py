StructType([
    StructField("empid", IntegerType(), True),
    StructField("empname", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("salary", DecimalType(10, 2), True),
    StructField("joining_date", DateType(), True),
    StructField("dept_id", IntegerType(), True),
    StructField("create_tmst", TimestampType(), True),
    StructField("upd_tmst", TimestampType(), True)
])


# StructType([
#     StructField("empid", IntegerType(), True),
#     StructField("empname", StringType(), True),
#     StructField("address", StructType([
#         StructField("street", StringType(), True),
#         StructField("city", StringType(), True),
#         StructField("zipcode", StringType(), True)
#     ]), True),
#     StructField("department", StructType([
#         StructField("dept_id", IntegerType(), True),
#         StructField("dept_name", StringType(), True),
#         StructField("manager", StructType([
#             StructField("manager_id", IntegerType(), True),
#             StructField("manager_name", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("skills", ArrayType(StringType()), True),
#     StructField("certifications", ArrayType(StructType([
#         StructField("cert_name", StringType(), True),
#         StructField("cert_date", DateType(), True)
#     ])), True)
# ])