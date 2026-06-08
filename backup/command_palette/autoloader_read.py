# Full Autoloader read with all probable options
# Distinctions : 
# "mode" deals with unparseable / malformed rows — things like:
# A JSON row with a syntax error ({"id": 123, "name":})
# A CSV row where a field that should be an integer contains "abc" and can't be cast
# A completely corrupted/truncated record
# "cloudFiles.rescuedDataColumn with schemaEvolutionMode=rescue" deals with structurally valid rows that have extra or unexpected columns — the row parsed fine, but it has a field your schema doesn't know about yet. That extra field gets sidelined into _rescued_data as JSON rather than being dropped.
# So they're complementary, not overlapping:

df = (spark.readStream
  .format("cloudFiles")
  ## ── FORMAT ──────────────────────────────────────────
  .option("cloudFiles.format",              "json")           # json | csv | parquet | avro | orc | text | binaryFile
  .option("cloudFiles.includeExistingFiles",  "true")          # true → backfill; false → new files only
  .option("cloudFiles.useNotifications",      "false")         # false=dir-listing (simpler); true=event-based (scalable)
  .option("pathGlobFilter",                  "*.json")        # glob to include only matching filenames
  .option("recursiveFileLookup",             "true")          # recurse into sub-directories
  .option("modifiedAfter",                   "2024-01-01T00:00:00") # ISO datetime lower bound
  .option("modifiedBefore",                  "2025-01-01T00:00:00") # ISO datetime upper bound

  ## ── SCHEMA ───────────────────────────────────────────
  .option("cloudFiles.schemaLocation",        "/checkpoints/schema") # inferred schema stored here (required for inference)
  .option("cloudFiles.inferColumnTypes",      "true")          # true → infer precise types; false → all string
  .option("cloudFiles.schemaHints",           "id BIGINT, ts TIMESTAMP") # override inferred types for specific cols
  .option("cloudFiles.schemaEvolutionMode",   "addNewColumns") # addNewColumns | rescue | failOnNewColumns | none
  .option("cloudFiles.rescuedDataColumn",    "_rescued_data") # column that captures mismatched / extra fields

  ## ── PERFORMANCE ──────────────────────────────────────
  .option("cloudFiles.maxFilesPerTrigger",    "1000")          # max files per micro-batch
  .option("cloudFiles.maxBytesPerTrigger",    "10g")           # size cap per micro-batch (k/m/g/t)
  .option("cloudFiles.concurrentFiles",       "8")             # parallel file readers per batch

  ## ── TRIGGER / CHECKPOINT ─────────────────────────────
  .option("checkpointLocation",              "/checkpoints/stream1") # required: tracks progress & processed files

  ## ── CLOUD / INFRA ────────────────────────────────────
  .option("cloudFiles.region",               "us-east-1")    # AWS region (or Azure equivalent)
  .option("cloudFiles.resourceGroup",         "my-rg")         # Azure resource group (notifications mode)
  .option("cloudFiles.subscriptionId",        "")        # Azure subscription ID (notifications mode)
  .option("cloudFiles.tenantId",              "")        # Azure tenant ID (notifications mode)
  .option("cloudFiles.connectionString",      "")    # Azure Storage connection string
  .option("cloudFiles.clientId",              "")        # Service principal client ID
  .option("cloudFiles.clientSecret",          "")      # Service principal secret (use secrets())
  .option("cloudFiles.queueUrl",              "https://...")   # SQS/Event Grid URL (notifications mode only)

  ## ── FORMAT-SPECIFIC (JSON / CSV) ─────────────────────
  .option("multiLine",                       "false")         # true → one JSON object spans multiple lines
  .option("header",                          "true")          # CSV: first row is header
  .option("delimiter",                       ",")             # CSV: field separator character
  .option("quote",                           "\"")            # CSV: quote character
  .option("escape",                          "\\")            # CSV: escape character
  .option("encoding",                        "UTF-8")         # character encoding
  .option("timestampFormat",                 "yyyy-MM-dd HH:mm:ss") # custom timestamp parse pattern
  .option("dateFormat",                      "yyyy-MM-dd")    # custom date parse pattern
  .option("mode",                            "PERMISSIVE")    # PERMISSIVE | DROPMALFORMED | FAILFAST
  .option("columnNameOfCorruptRecord",       "_corrupt_record") # PERMISSIVE mode: stores bad rows here

  .load("/mnt/raw/events/")
)