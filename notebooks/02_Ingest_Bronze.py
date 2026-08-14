# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Layer: Auto Loader Ingestion
# MAGIC
# MAGIC **Purpose:** Ingests the raw ERP exports from the **UC volume landing zone**
# MAGIC (`/Volumes/SalesRevenueCustomerAnalytics/sales_data/raw_data`) into the `SalesRevenueCustomerAnalytics.bronze.*` Delta tables
# MAGIC using **Auto Loader** (`cloudFiles`):
# MAGIC - **Incremental + exactly-once**: checkpoints under the volume track every
# MAGIC   file — new files (e.g. notebook 10's "next-day" exports) are picked up
# MAGIC   automatically, nothing is re-read
# MAGIC - **Multi-format**: CSV (most ERP exports) and JSON (`erp/region`)
# MAGIC - **Schema evolution**: `addNewColumns` + late-data rescue
# MAGIC - **Audit columns**: ingestion timestamp, source file, batch ID, source
# MAGIC   system, and a record hash for downstream dedup
# MAGIC - **Dedup on ingest**: by `_record_hash` (content hash) — re-ingesting the
# MAGIC   same content (e.g. a re-run of `01`) adds nothing; duplicate rows a
# MAGIC   source shipped twice collapse to one
# MAGIC - **Delta history**: every batch is a new table version (time travel)
# MAGIC
# MAGIC **Run `00` then `01` first.**
# MAGIC
# MAGIC ⚠️ **Serverless note:** if `cloudFiles` is unavailable on your compute,
# MAGIC notebook falls back to a plain `spark.read.csv / json` batch load
# MAGIC (audit columns + dedup preserved) — the rest of the pipeline is unchanged.

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader configuration
# MAGIC Every dataset maps a raw folder (written by `01`) to a bronze table.
# MAGIC The `transactions` dataset is partitioned by `dt` (the daily-batch marker
# MAGIC extracted from the `dt=YYYY-MM-DD/` folder names).

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql import functions as F

AUTO_LOADER = {
    "customer":       {"path": RAW_CUSTOMER,       "table": f"{BRONZE}.erp_customer",      "format": "csv"},
    "product":        {"path": RAW_PRODUCT,        "table": f"{BRONZE}.erp_product",       "format": "csv"},
    "region":         {"path": f"{RAW_BASE}/erp/region",       "table": f"{BRONZE}.erp_region",        "format": "json"},
    "sales_rep":      {"path": f"{RAW_BASE}/erp/sales_rep",    "table": f"{BRONZE}.erp_sales_rep",     "format": "csv"},
    "currency":       {"path": f"{RAW_BASE}/erp/currency",     "table": f"{BRONZE}.erp_currency",      "format": "csv"},
    "order":          {"path": f"{RAW_BASE}/orders",           "table": f"{BRONZE}.sales_order",       "format": "csv"},
    "transaction":    {"path": RAW_TRANSACT,       "table": f"{BRONZE}.sales_transaction", "format": "csv", "partitioned": True},
}

# Audit columns added to every row on ingest (the Bronze "receipt")
BATCH_ID = f"BATCH_{datetime.now():%Y%m%d_%H%M%S}"
AUDIT_COLS = {
    "_ingestion_timestamp": F.current_timestamp(),
    "_source_system":       F.lit("ERP"),
    "_batch_id":            F.lit(BATCH_ID),
}

# Column sets used to build the record hash (same columns the generator wrote)
CUST_COLS = ["customer_id", "customer_name", "customer_type", "customer_segment", "country",
             "state", "city", "region", "industry", "sales_rep_id", "customer_status",
             "created_date", "updated_date"]
PROD_COLS = ["product_id", "product_name", "product_category", "product_subcategory", "brand",
             "unit_price", "cost", "product_status", "effective_date", "updated_date"]
ORD_COLS  = ["order_id", "customer_id", "order_date", "order_status", "total_amount", "currency",
             "region_id", "sales_rep_id", "source_system"]
TXN_COLS  = ["transaction_id", "order_id", "transaction_date", "customer_id", "product_id",
             "quantity", "unit_price", "discount", "tax", "gross_amount", "net_amount",
             "currency", "region_id", "sales_rep_id", "transaction_status", "source_system"]
REP_COLS  = ["sales_rep_id", "sales_rep_name", "region_id", "sales_rep_email", "hire_date", "status"]
CUR_COLS  = ["currency_code", "currency_name", "exchange_rate_to_usd", "effective_date"]
REG_COLS  = ["region_id", "region_name", "country", "state", "territory"]

DATA_COLS = {
    "customer":    CUST_COLS,
    "product":     PROD_COLS,
    "region":      REG_COLS,
    "sales_rep":   REP_COLS,
    "currency":    CUR_COLS,
    "order":       ORD_COLS,
    "transaction": TXN_COLS,
}

# Schema evolution is enabled per-write via `.option("mergeSchema", "true")`
# below — serverless does not allow setting
# `spark.databricks.delta.schema.autoMerge.enabled` as a session config.

# Fail-fast: every source folder must already contain files (written by 01).
# Auto Loader cannot infer a CSV schema from an empty folder — without this
# guard that surfaces as a cryptic CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE / 
# UNABLE_TO_INFER_SCHEMA deep inside the stream or the batch fallback.
EMPTY_DIRS = []
for name, cfg in AUTO_LOADER.items():
    try:
        entries = [e for e in dbutils.fs.ls(cfg["path"]) if e.isDir or e.size > 0]
    except Exception:
        entries = []
    if not entries:
        EMPTY_DIRS.append(name)
if EMPTY_DIRS:
    raise RuntimeError(
        "Source folders are empty: " + ", ".join(EMPTY_DIRS) +
        " — run notebook 01 (data generator) against the raw volume first."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest with Auto Loader (`cloudFiles`)
# MAGIC One incremental stream per dataset. Each stream:
# MAGIC 1. reads files with `cloudFiles` (checkpoint under `_checkpoints/`),
# MAGIC 2. stamps audit columns + `_source_file` (from the file path),
# MAGIC 3. adds `dt` for transactions (parsed from `dt=YYYY-MM-DD/` folders),
# MAGIC 4. computes `_record_hash` (dedup key),
# MAGIC 5. drops `_rescued_data` (late-schema-change column),
# MAGIC 6. dedupes on `_record_hash` (content hash) and writes to the Delta table.

# COMMAND ----------

def ingest_stream(dataset, cfg):
    """Run one Auto Loader stream; returns (rows_written, error_or_None)."""
    checkpoint = f"{CHECKPOINT_BASE}/{dataset}"
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", cfg["format"])
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumnName", "_rescued_data")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.checkpointLocation", checkpoint)
        # Spark Connect (serverless) lowercases option keys, which breaks
        # cloudFiles' case-sensitive option validation — skip the check.
        .option("cloudFiles.validateOptions", "false")
    )
    if cfg["format"] == "csv":
        reader = reader.option("header", "true")

    df = reader.load(cfg["path"])

    if cfg.get("partitioned"):
        df = df.withColumn("dt", F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"dt=(\d{4}-\d{2}-\d{2})", 1)))

    data_cols = DATA_COLS[dataset]
    df = (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumns(AUDIT_COLS)
          .withColumn("_record_hash",
                      F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in data_cols]), 256))
          .drop("_rescued_data")
          .dropDuplicates(["_record_hash"])
    )

    q = (
        df.writeStream
          .option("checkpointLocation", checkpoint)
          .option("mergeSchema", "true")
    )
    if cfg.get("partitioned"):
        q = q.partitionBy("dt")
    q = q.trigger(availableNow=True).toTable(cfg["table"])
    return q

streams = {}
for dataset, cfg in AUTO_LOADER.items():
    try:
        streams[dataset] = ingest_stream(dataset, cfg)
        print(f"  stream started: {dataset:12s} -> {cfg['table']}  (checkpoint: {CHECKPOINT_BASE}/{dataset})")
    except Exception as e:
        print(f"  ! cloudFiles failed for {dataset}: {type(e).__name__}: {e}")
        streams[dataset] = None

for name, q in streams.items():
    if q is None:
        continue
    q.awaitTermination()

failed = [n for n, q in streams.items() if q is not None and q.lastProgress is None and not q.isActive]
for n in failed:
    print(f"  ! stream failed: {n}")

print(f"\nBatch ID: {BATCH_ID}")
print("Auto Loader ingestion complete." if not failed else "Auto Loader ingestion had failures — check the batch fallback below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Batch fallback (if `cloudFiles` is unavailable on this compute)
# MAGIC Read the same volume folders with plain `spark.read.csv / json` and write
# MAGIC with the identical audit pipeline — same schema, same dedup, same result.

# COMMAND ----------

FALLBACK_MODE = any(q is None for q in streams.values())

def batch_load(dataset, cfg):
    """Plain spark.read of a raw folder with the same audit+dedup pipeline."""
    read_opts = {}
    if cfg["format"] == "csv":
        read_opts = {"header": "true", "inferSchema": "true"}
        df = spark.read.csv(cfg["path"], **read_opts)
    else:
        df = spark.read.json(cfg["path"])

    if cfg.get("partitioned"):
        df = df.withColumn("dt", F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"dt=(\d{4}-\d{2}-\d{2})", 1)))

    data_cols = DATA_COLS[dataset]
    df = (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumns(AUDIT_COLS)
          .withColumn("_record_hash",
                      F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in data_cols]), 256))
          .dropDuplicates(["_record_hash"])
    )
    return df

if FALLBACK_MODE:
    print("FALLBACK: streaming unavailable — running batch ingestion.")
    for dataset, cfg in AUTO_LOADER.items():
        n = batch_load(dataset, cfg).write.mode("overwrite").option("mergeSchema", "true").saveAsTable(cfg["table"])
        print(f"  loaded {dataset:12s} -> {cfg['table']}")
else:
    print("No fallback needed — Auto Loader streams succeeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingestion checkpoints
# MAGIC Proves every bronze table is populated and stamped with the audit columns.

# COMMAND ----------

print("Bronze tables after ingestion:")
for name, tbl in sorted(AUTO_LOADER.items(), key=lambda x: x[1]["table"]):
    df = spark.read.table(tbl["table"])
    audit = [c for c in df.columns if c.startswith("_")]
    print(f"  {tbl['table']:42s} {df.count():>10,} rows  |  {len(df.columns)} cols  |  audit: {','.join(audit) or 'MISSING'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta history (time travel + auditability)
# MAGIC Every ingest run is a new table version. `DESCRIBE HISTORY` proves Bronze
# MAGIC retains full lineage — and enables `VERSION AS OF` time travel queries.

# COMMAND ----------


display(spark.sql(f"""DESCRIBE HISTORY {BRONZE}.sales_transaction;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Health check — a quick data-quality snapshot at the source
# MAGIC (This is *observation*, not cleanup — cleansing happens in Silver.)

# COMMAND ----------


display(spark.sql(f"""SELECT
  COUNT(*)                                                    AS total_rows,
  COUNT(DISTINCT transaction_id)                              AS distinct_txns,
  COUNT(*) - COUNT(DISTINCT transaction_id)                   AS duplicate_txns,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)        AS null_customer,
  SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END)               AS negative_qty,
  SUM(CASE WHEN transaction_date > CURRENT_DATE THEN 1 ELSE 0 END) AS future_dates,
  SUM(CASE WHEN currency = 'XYZ' THEN 1 ELSE 0 END)           AS invalid_currency,
  SUM(CASE WHEN transaction_status = 'Shipped' THEN 1 ELSE 0 END) AS invalid_status
FROM {BRONZE}.sales_transaction;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - Bronze is a **forensic near-raw copy**: every row carries *when*, *from
# MAGIC   where*, *in which batch*, and a hash for later dedup — stamped by Auto
# MAGIC   Loader exactly-once semantics
# MAGIC - Auto Loader checkpoints make ingestion **incremental by design**: new
# MAGIC   files (notebook 10's "next-day" demo) are discovered automatically
# MAGIC - Delta history gives **time travel** out of the box
# MAGIC - **Next:** `03_Silver_Cleansing` — dedup, DQ validation, quarantine, standardization.
