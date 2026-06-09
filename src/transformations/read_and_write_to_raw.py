from datetime import datetime, timedelta

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from pipeline_config import table
from schemas import employee_schema_raw

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# if you want to pass AWS credentials, uncomment the following lines
# spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="aws", key="access_key"))
# spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="aws", key="secret_key"))

# triggered modes are continuous and triggered. if Not continuous is set in the pipeline, it will be trigger(available=True)
# For files to be lexically ordered, new files that are uploaded need to have a prefix that is lexicographically greater than existing files. Some examples of lexical ordered directories are shown below.

# Lexical ordering improves file discovery efficiency, but Auto Loader does not guarantee the order in which files are discovered or processed. Design your pipelines to handle out-of-order file arrivals. For guidance, see Handle out-of-order data.
# When files are uploaded with date partitioning, some things to keep in mind are:
# Months, days, hours, minutes need to be left padded with zeros to ensure lexical ordering (should be uploaded as hour=03, instead of hour=3 or 2021/05/03 instead of 2021/5/3).


@dp.table(
    name=table("raw_events"),
    comment="Raw events from Volume",
    cluster_by=["dept_id", "joining_date"],
    table_properties={
        # "delta.mergeSchema": "true",                 cannot specify them as DTL handles automatically.
        # "delta.overwriteSchema": "true",             cannot specify them as DTL handles automatically.
        "delta.autoOptimize.optimizeWrite": "true",  # this anyways optimizes the file size during write
        "delta.autoOptimize.autoCompact": "true",  # merge small files automatically
        "delta.targetFileSize": "1gb",  # this is where you specify size of files while writing the data
        "delta.enableChangeDataFeed": "true",
        "delta.checkpointInterval": "50",
        "delta.dataSkippingNumIndexedCols": "32",
    },
)
def raw_events():
    reprocess_days = spark.conf.get("pipeline.reprocess_days", "0")
    if int(reprocess_days) > 0:
        reprocess_date = (datetime.now() - timedelta(days=int(reprocess_days))).strftime(
            "%Y-%m-%dT00:00:00"
        )
    else:
        reprocess_date = "2024-01-01T00:00:00"

    # Read with strings for date/timestamp columns
    df = (
        spark.readStream.format("cloudFiles")
        ## ── FORMAT ──────────────────────────────────────────
        .option("cloudFiles.format", "csv")  # json | csv | parquet | avro | orc | text | binaryFile
        .option(
            "cloudFiles.includeExistingFiles", "true"
        )  # true → backfill; false → new files only
        .option(
            "cloudFiles.useNotifications", "false"
        )  # false=dir-listing (simpler); true=event-based (scalable)
        .option("pathGlobFilter", "*.csv")  # glob to include only matching filenames
        .option("recursiveFileLookup", "true")  # recurse into sub-directories
        .option("modifiedAfter", reprocess_date)  # ISO datetime lower bound
        ## ── SCHEMA ───────────────────────────────────────────
        .option(
            "cloudFiles.schemaEvolutionMode", "rescue"
        )  # addNewColumns | rescue | failOnNewColumns | none
        .option(
            "cloudFiles.rescuedDataColumn", "_rescued_data"
        )  # column that captures mismatched / extra fields
        .schema(employee_schema_raw)  # Using string schema for dates/timestamps
        ## ── PERFORMANCE ──────────────────────────────────────
        .option("cloudFiles.maxFilesPerTrigger", "2")  # max files per micro-batch
        .option("cloudFiles.maxBytesPerTrigger", "10g")  # size cap per micro-batch (k/m/g/t)
        ## ── TRIGGER / CHECKPOINT ─────────────────────────────
        # .option("checkpointLocation",              "/Volumes/workspace/default/niladri_created_volume/checkpoints/stream1")
        # required: tracks progress & processed files. you CANNOT use it in DLT. DLT maintains own checkpointing which you cant edit.
        ## ── FORMAT-SPECIFIC (JSON / CSV) ─────────────────────
        .option("multiLine", "false")  # true → one JSON object spans multiple lines
        .option("header", "true")  # CSV: first row is header
        .option("delimiter", ",")  # CSV: field separator character
        .option("quote", '"')  # CSV: quote character
        .option("escape", "\\")  # CSV: escape character
        .option("encoding", "UTF-8")  # character encoding
        # NOTE: dateFormat and timestampFormat removed - parsing dates manually below
        .option("mode", "PERMISSIVE")  # PERMISSIVE | DROPMALFORMED | FAILFAST
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )  # PERMISSIVE mode: stores bad rows here
        .load("/Volumes/workspace/default/niladri_created_volume/data/")
    )

    # Return raw data as strings - parsing happens in silver layer
    return df
