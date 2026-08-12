# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Layer: Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest raw files from the landing zone into Bronze Delta tables
# MAGIC using **Auto Loader** (incremental file discovery) with a **batch fallback**.
# MAGIC
# MAGIC **Medallion principle — Bronze is a "near-raw" copy:**
# MAGIC - Data is preserved **as received** (no business transformation)
# MAGIC - **Audit columns** are added: ingestion timestamp, source file, batch ID,
# MAGIC   source system, and a record hash (for downstream dedup)
# MAGIC - **Schema evolution** is enabled (`addNewColumns`) — new source columns
# MAGIC   don't break the pipeline; they get appended automatically
# MAGIC - Full history is retained in Delta Lake (time travel / `DESCRIBE HISTORY`)
# MAGIC
# MAGIC **Auto Loader vs plain `spark.read`:**
# MAGIC - Auto Loader discovers **only new files** across runs (incremental by design)
# MAGIC - It keeps a **checkpoint** (its own state) + **schema inference location**
# MAGIC - `trigger(availableNow=True)` = process everything discovered, then stop
# MAGIC   (batch-style, perfect for daily jobs — no always-on streaming cost)
# MAGIC
# MAGIC **Fallback:** if Auto Loader is unavailable in the workspace, the notebook
# MAGIC automatically falls back to plain batch `spark.read` (set
# MAGIC `USE_AUTO_LOADER = False` to force it).

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingestion config

# COMMAND ----------

USE_AUTO_LOADER = True   # set False to force the plain-batch fallback

# batch identifier for this load run — stamped on every row for auditability
from datetime import datetime
BATCH_ID = f"BATCH_{datetime.now():%Y%m%d_%H%M%S}"
SOURCE_SYSTEM = "ERP"

# (source folder, target table, checkpoint) for each dataset
INGESTIONS = [
    (RAW_CUSTOMER, TABLES["bronze_customer"],  f"{CHECKPOINT_BASE}/customer"),
    (RAW_PRODUCT,  TABLES["bronze_product"],   f"{CHECKPOINT_BASE}/product"),
    (f"{RAW_BASE}/orders", TABLES["bronze_order"], f"{CHECKPOINT_BASE}/order"),
    (RAW_TRANSACT, TABLES["bronze_transaction"], f"{CHECKPOINT_BASE}/transaction"),
    (f"{RAW_BASE}/erp/region",    "bronze.erp_region",    f"{CHECKPOINT_BASE}/region"),
    (f"{RAW_BASE}/erp/sales_rep", "bronze.erp_sales_rep", f"{CHECKPOINT_BASE}/sales_rep"),
    (f"{RAW_BASE}/erp/currency",  "bronze.erp_currency",  f"{CHECKPOINT_BASE}/currency"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generic ingest function
# MAGIC One reusable function ingests every dataset — the same pattern you would
# MAGIC deploy for N source systems with zero extra code.

# COMMAND ----------

from pyspark.sql import functions as F

def ingest_to_bronze(source_path, target_table, checkpoint_path, use_autoloader=USE_AUTO_LOADER):
    """Incremental file ingestion into a bronze Delta table with audit columns."""
    data_cols = None
    if use_autoloader:
        try:
            stream = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("cloudFiles.schemaLocation", checkpoint_path)          # schema evolution state
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")     # accept new source columns
                .load(source_path)
            )
            data_cols = [c for c in stream.columns if not c.startswith("_")]
        except Exception as e:
            print(f"  [!] Auto Loader unavailable ({e}) — falling back to batch read")
            use_autoloader = False

    if use_autoloader:
        audit = (
            stream
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file",        F.input_file_name())
            .withColumn("_source_system",      F.lit(SOURCE_SYSTEM))
            .withColumn("_batch_id",           F.lit(BATCH_ID))
            .withColumn("_record_hash",        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in data_cols]), 256))
        )
        query = (
            audit.writeStream
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)                 # batch-like run, stops when done
            .toTable(target_table)
        )
        query.awaitTermination()
        mode = "AUTO LOADER (streaming)"
    else:
        raw = spark.read.csv(source_path, header=True, inferSchema=True)
        audit = (
            raw
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file",        F.input_file_name())
            .withColumn("_source_system",      F.lit(SOURCE_SYSTEM))
            .withColumn("_batch_id",           F.lit(BATCH_ID))
            .withColumn("_record_hash",        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in raw.columns if not c.startswith("_")]), 256))
        )
        audit.write.mode("append").saveAsTable(target_table)
        mode = "BATCH (spark.read)"

    count = spark.read.table(target_table).count()
    print(f"  [{mode}] {source_path} -> {target_table}  (total rows now: {count:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run ingestion for all datasets

# COMMAND ----------

for src, tgt, ckpt in INGESTIONS:
    print(f"> Ingesting {src}")
    ingest_to_bronze(src, tgt, ckpt)

print("\nBatch ID:", BATCH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify — raw data + audit columns, as received

# COMMAND ----------

from pyspark.sql import functions as F

for name, tbl in [("customer", TABLES["bronze_customer"]),
                  ("product",  TABLES["bronze_product"]),
                  ("transaction", TABLES["bronze_transaction"]),
                  ("order",    TABLES["bronze_order"])]:
    df = spark.read.table(tbl)
    print(f"--- bronze.erp_{name}: {df.count():,} rows, {len(df.columns)} cols")
    df.orderBy(F.col("_ingestion_timestamp").desc()).show(3, truncate=False, vertical=True)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta history (time travel + auditability)
# MAGIC Every ingest run is a new version. `DESCRIBE HISTORY` proves Bronze retains
# MAGIC full lineage — and enables `VERSION AS OF` time travel queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze.sales_transaction;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Health check — a quick data-quality snapshot at the source
# MAGIC (This is *observation*, not cleanup — cleansing happens in Silver.)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                    AS total_rows,
# MAGIC   COUNT(DISTINCT transaction_id)                              AS distinct_txns,
# MAGIC   COUNT(*) - COUNT(DISTINCT transaction_id)                   AS duplicate_txns,
# MAGIC   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)        AS null_customer,
# MAGIC   SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END)               AS negative_qty,
# MAGIC   SUM(CASE WHEN transaction_date > CURRENT_DATE THEN 1 ELSE 0 END) AS future_dates,
# MAGIC   SUM(CASE WHEN currency = 'XYZ' THEN 1 ELSE 0 END)           AS invalid_currency,
# MAGIC   SUM(CASE WHEN transaction_status = 'Shipped' THEN 1 ELSE 0 END) AS invalid_status
# MAGIC FROM bronze.sales_transaction;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - Auto Loader = zero-code incremental discovery via checkpoints (no timestamps needed)
# MAGIC - Audit columns make Bronze **forensic**: every row knows *when*, *from where*,
# MAGIC   *in which batch*, and a hash for later dedup
# MAGIC - `addNewColumns` schema evolution = sources can change without breaking the pipeline
# MAGIC - Delta history gives **time travel** out of the box
# MAGIC
# MAGIC **Next:** `03_Silver_Cleansing` — dedup, DQ validation, quarantine, standardization.
