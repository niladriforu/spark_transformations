# writeStream — all probable options
(df.writeStream
  ## ── CORE ────────────────────────────────────────────────
  .format("delta")                              # delta | parquet | json | csv | orc | avro | kafka | console | memory
  .outputMode("append")                         # append | complete | update
  .option("checkpointLocation", "/ckpt/stream1") # required: tracks offsets & processed files
  .option("queryName",          "my_stream")    # name shown in Spark UI & query progress logs

  ## ── OUTPUT DESTINATION ──────────────────────────────────
  # Option A — write to a registered catalog table (preferred)
  .toTable("catalog.schema.my_table")

  # Option B — write Delta files to a path (no catalog entry)
  # .start("/mnt/delta/my_table")

  ## ── TRIGGER ─────────────────────────────────────────────
  .trigger(processingTime="30 seconds")         # fixed interval micro-batch
  # .trigger(availableNow=True)                  # process all backlog, then stop (Spark 3.3+)
  # .trigger(once=True)                          # legacy one-shot (use availableNow instead)
  # .trigger(continuous="1 second")              # experimental low-latency continuous mode

  ## ── DELTA-SPECIFIC ──────────────────────────────────────
  .option("mergeSchema",            "true")      # auto-evolve Delta schema on new columns
  .option("overwriteSchema",        "false")     # true → replace schema entirely (destructive)
  .option("txnAppId",               "loader1")   # idempotent write app ID (exactly-once)
  .option("txnVersion",             "1")         # idempotent write version (pair with txnAppId)
  .option("delta.dataSkippingNumIndexedCols", "4") # cols indexed for data skipping

  ## ── PARTITIONING ────────────────────────────────────────
  .partitionBy("date", "region")               # partition output files by these columns

  ## ── FOREACHBATCH (custom sink / upsert) ─────────────────
  # .foreachBatch(upsert_fn)                     # run custom function per micro-batch
  # .foreach(row_writer)                         # run custom function per row (rare)
)
