# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Workspace Setup
# MAGIC
# MAGIC **Project:** Sales Revenue & Customer Analytics (Databricks Lakehouse)
# MAGIC
# MAGIC **Purpose:** One-time environment setup for the entire platform:
# MAGIC - Creates the Medallion schemas: `bronze` → `silver` → `gold`
# MAGIC - Creates the **Unity Catalog volume** that hosts the raw-data landing
# MAGIC   zone (serverless-compatible: the public DBFS root `/FileStore` is
# MAGIC   disabled on serverless, so raw files live in a UC volume instead)
# MAGIC - Centralizes configuration used by every downstream notebook
# MAGIC
# MAGIC **Note on Unity Catalog:** everything lives under the single
# MAGIC `{CATALOG}` catalog — medallion schemas `bronze`,
# MAGIC `silver`, `gold` plus the `{VOLUME_SCHEMA}` schema hosting the raw-data UC
# MAGIC volume. Every reference in every notebook is **fully qualified**
# MAGIC (`catalog.schema.table` / `/Volumes/catalog/schema/...`), so the pipeline
# MAGIC works regardless of the session's default catalog.
# MAGIC
# MAGIC **Run order:** this notebook first, then every notebook in numeric order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Central configuration
# MAGIC All paths, table names and business constants live here. In production this
# MAGIC would be a `config/` module or environment-scoped variable group deployed via
# MAGIC Databricks Asset Bundles.

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import date

# Catalog/schema names — one catalog, fully qualified in every notebook
CATALOG = "SalesRevenueCustomerAnalytics"
BRONZE  = f"{CATALOG}.bronze"
SILVER  = f"{CATALOG}.silver"
GOLD    = f"{CATALOG}.gold"

# Raw-data landing zone — a Unity Catalog volume (serverless-compatible)
# The public DBFS root (`/FileStore`) is disabled on serverless compute, so the
# ERP export files live in a UC volume instead. Layout mirrors a daily batch:
#   /Volumes/{CATALOG}/{VOLUME_SCHEMA}/raw_data/erp/customer/
#   /Volumes/{CATALOG}/{VOLUME_SCHEMA}/raw_data/erp/product/
#   /Volumes/{CATALOG}/{VOLUME_SCHEMA}/raw_data/transactions/dt=YYYY-MM-DD/
VOLUME_SCHEMA = "sales_data"        # UC schema hosting the raw-data volume
VOLUME_RAW    = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/raw_data"
RAW_BASE     = VOLUME_RAW
RAW_CUSTOMER = f"{RAW_BASE}/erp/customer"
RAW_PRODUCT  = f"{RAW_BASE}/erp/product"
RAW_TRANSACT = f"{RAW_BASE}/transactions"

# Auto Loader checkpoint + schema-evolution location (inside the volume)
CHECKPOINT_BASE = f"{RAW_BASE}/_checkpoints"

# Tables
TABLES = {
    "bronze_customer":   f"{BRONZE}.erp_customer",
    "bronze_product":    f"{BRONZE}.erp_product",
    "bronze_transaction":f"{BRONZE}.sales_transaction",
    "bronze_order":      f"{BRONZE}.sales_order",
    "silver_customer":   f"{SILVER}.customer",
    "silver_product":    f"{SILVER}.product",
    "silver_transaction":f"{SILVER}.sales_transaction",
    "silver_order":      f"{SILVER}.sales_order",
    "silver_quarantine": f"{SILVER}.sales_quarantine",
    "gold_dim_customer": f"{GOLD}.dim_customer",
    "gold_dim_product":  f"{GOLD}.dim_product",
    "gold_dim_date":     f"{GOLD}.dim_date",
    "gold_dim_region":   f"{GOLD}.dim_region",
    "gold_dim_sales_rep":f"{GOLD}.dim_sales_rep",
    "gold_fact_sales":   f"{GOLD}.fact_sales",
    "gold_dq_audit":     f"{GOLD}.data_quality_audit",
}

# Business constants (configurable by design — see doc §18 segmentation thresholds)
SEGMENT_THRESHOLDS = {"high": 100000, "low": 25000}
BUDGET_DAILY       = 60000  # daily revenue budget/target for actual-vs-budget analysis

print("Config loaded:")
for k, v in TABLES.items():
    print(f"  {k:20s} -> {v}")
print(f"  Raw data base        -> {RAW_BASE}  (UC volume, serverless-compatible)")
print(f"  Checkpoint base      -> {CHECKPOINT_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Medallion schemas
# MAGIC `CREATE DATABASE IF NOT EXISTS` is idempotent — safe to run on every
# MAGIC deploy, matching production CI/CD semantics.

# COMMAND ----------


spark.sql(f"""CREATE DATABASE IF NOT EXISTS {BRONZE} COMMENT 'Raw, near-source data as received from ERP/transaction systems';""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {SILVER} COMMENT 'Cleansed, standardized, conformed data';""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {GOLD}   COMMENT 'Business-ready dimensional model for dashboards';""")
# COMMAND ----------


display(spark.sql(f"""SHOW DATABASES;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Raw-data landing zone in a Unity Catalog volume
# MAGIC Serverless compute disables the public DBFS root (`/FileStore`), so the
# MAGIC ERP export files live in a **UC volume** — `dbutils.fs`
# MAGIC (`mkdirs`/`ls`/`rm`), `spark.read` and **Auto Loader** all support
# MAGIC `/Volumes/...` paths on serverless.
# MAGIC
# MAGIC - The `raw_data` volume is created *if not exists* (idempotent) under
# MAGIC   the `{CATALOG}` catalog
# MAGIC   → `{CATALOG}.{VOLUME_SCHEMA}.raw_data`
# MAGIC - `01` writes the synthetic ERP exports there (CSV + one JSON folder)
# MAGIC - `02` ingests them with Auto Loader (checkpoints under the volume)
# MAGIC - `10` appends "next-day" files for the incremental demo
# MAGIC
# MAGIC ⚠️ Set `RESET_LANDING_ZONE = True` **only** if you want to wipe and
# MAGIC regenerate everything from scratch — it deletes raw data + checkpoints.

# COMMAND ----------


spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA} COMMENT 'Raw-data landing zone for the sales platform';""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.raw_data COMMENT 'ERP export files (serverless-compatible replacement for DBFS /FileStore)';""")
# COMMAND ----------

RESET_LANDING_ZONE = False

if RESET_LANDING_ZONE:
    dbutils.fs.rm(RAW_BASE, recurse=True)
for path in [RAW_CUSTOMER, RAW_PRODUCT, RAW_TRANSACT, f"{RAW_BASE}/erp/customer_updates",
             f"{RAW_BASE}/erp/region", f"{RAW_BASE}/erp/sales_rep", f"{RAW_BASE}/erp/currency",
             f"{RAW_BASE}/orders", CHECKPOINT_BASE]:
    dbutils.fs.mkdirs(path)
print("Landing zone ready (UC volume):")
dbutils.fs.ls(RAW_BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Environment summary
# MAGIC Verify the runtime — serverless compute runs a recent DBR with Delta
# MAGIC Lake built in (no `%pip install delta` needed).

# COMMAND ----------

from datetime import date

def spark_conf(key, default):
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default

spark_version = spark.version
dbr_version = spark_conf("spark.databricks.clusterUsageTags.sparkVersion", "n/a (serverless)")
runtime = spark_conf("spark.databricks.clusterUsageTags.clusterType", "serverless (Spark Connect)")
print(f"Spark version : {spark_version}")
print(f"DBR version   : {dbr_version}")
print(f"Runtime       : {runtime}")
print(f"Delta tables  : enabled (built into DBR)")
print(f"Current date  : {date.today().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cleanup utilities — MANUAL ONLY
# MAGIC
# MAGIC ⚠️ **Nothing in this section runs automatically.** Every cell is guarded by
# MAGIC a flag that defaults to `False`. To use one: set the flag to `True`, run
# MAGIC the cell, then set it back to `False` — exactly like the
# MAGIC `RESET_LANDING_ZONE` flag in section 3.
# MAGIC
# MAGIC **When to use:**
# MAGIC - **5a — Purge the raw landing zone:** wipe the volume's raw data files
# MAGIC   (including Auto Loader checkpoints) before a from-scratch regeneration.
# MAGIC   Run it, then run notebook `01` to regenerate the exports.
# MAGIC - **5b — Drop the medallion schemas:** remove `bronze` / `silver` / `gold`
# MAGIC   and **all their tables and data** (e.g. a clean teardown). Run it, then
# MAGIC   re-run this notebook (it recreates the schemas idempotently) and the
# MAGIC   pipeline again if needed.
# MAGIC - **5c — Drop the raw-data volume:** remove the `raw_data` UC volume itself
# MAGIC   (all raw exports + Auto Loader checkpoints). Run it, then re-run this
# MAGIC   notebook (it recreates the empty volume) and notebook `01` to regenerate
# MAGIC   the exports.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Purge the raw-data landing zone (volume)
# MAGIC Deletes **all files and folders** under `RAW_BASE` — the ERP exports and
# MAGIC every Auto Loader checkpoint (`_checkpoints/...`) — then recreates the
# MAGIC base folder structure so notebook 01 can write into it again.

# COMMAND ----------

PURGE_RAW_DATA = False   # <-- set to True, run, then set back to False

if PURGE_RAW_DATA:
    print("== PURGE: raw landing zone BEFORE ==")
    try:
        for f in dbutils.fs.ls(RAW_BASE):
            print(f"  {f.path}")
    except Exception:
        print("  (raw base does not exist yet)")
    dbutils.fs.rm(RAW_BASE, recurse=True)
    print(f"Deleted: {RAW_BASE} (files + folders + checkpoints)")
    for path in [RAW_CUSTOMER, RAW_PRODUCT, RAW_TRANSACT, f"{RAW_BASE}/erp/customer_updates",
                 f"{RAW_BASE}/erp/region", f"{RAW_BASE}/erp/sales_rep", f"{RAW_BASE}/erp/currency",
                 f"{RAW_BASE}/orders", CHECKPOINT_BASE]:
        dbutils.fs.mkdirs(path)
    print("== PURGE: raw landing zone AFTER ==")
    print(f"  {RAW_BASE} -> recreated (empty) base folders, ready for notebook 01")
else:
    print("PURGE_RAW_DATA is False — nothing deleted. Set it to True in this cell to wipe the raw landing zone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Drop the medallion schemas (CASCADE)
# MAGIC Drops `bronze`, `silver` and `gold` **including every table and all their
# MAGIC data** (`DROP SCHEMA ... CASCADE`). The `{VOLUME_SCHEMA}` schema and the
# MAGIC `raw_data` volume are left untouched — raw exports survive a schema reset.
# MAGIC Run this notebook again afterwards to recreate the schemas.

# COMMAND ----------

DROP_MEDALLION_SCHEMAS = False   # <-- set to True, run, then set back to False

if DROP_MEDALLION_SCHEMAS:
    for schema in ["bronze", "silver", "gold"]:
        full = f"{CATALOG}.{schema}"
        spark.sql(f"DROP SCHEMA IF EXISTS {full} CASCADE")
        print(f"Dropped: {full} (schema + all tables + data)")
    print("Medallion schemas removed. Re-run this notebook to recreate them, then notebooks 01-10.")
else:
    print("DROP_MEDALLION_SCHEMAS is False — nothing dropped. Set it to True in this cell to remove bronze/silver/gold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Drop the raw-data volume + schema (CASCADE)
# MAGIC Drops the `raw_data` **volume itself** — all raw export files and every
# MAGIC Auto Loader checkpoint go with it (dropping a volume deletes its data) — and
# MAGIC then drops the hosting `{VOLUME_SCHEMA}` schema (CASCADE), so the entire
# MAGIC raw-data landing zone is removed. Re-run this notebook to recreate the (empty)
# MAGIC schema and volume with `CREATE SCHEMA` / `CREATE VOLUME IF NOT EXISTS`.

# COMMAND ----------

DROP_RAW_VOLUME = False   # <-- set to True, run, then set back to False

if DROP_RAW_VOLUME:
    vol = f"{CATALOG}.{VOLUME_SCHEMA}.raw_data"
    spark.sql(f"DROP VOLUME IF EXISTS {vol}")
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{VOLUME_SCHEMA} CASCADE")
    print(f"Dropped: {vol} (volume + all raw files + checkpoints)")
    print(f"Dropped: {CATALOG}.{VOLUME_SCHEMA} (schema hosting the volume)")
    print("Re-run this notebook to recreate the empty schema/volume, then notebook 01 to regenerate exports.")
else:
    print("DROP_RAW_VOLUME is False — nothing dropped. Set it to True in this cell to remove the raw_data volume and sales_data schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** run `01_Generate_Synthetic_ERP_Data` to create realistic source data
# MAGIC (with deliberate data-quality issues so the Silver layer has something to fix).
