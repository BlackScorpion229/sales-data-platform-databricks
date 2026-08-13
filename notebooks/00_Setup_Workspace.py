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
# MAGIC **Note on Unity Catalog:** Community Edition now provides a managed
# MAGIC `workspace` catalog, but this project keeps the documented `hive_metastore`
# MAGIC schema design — the medallion pattern is identical either way. The **raw
# MAGIC file layer** uses a UC volume under the `workspace` catalog
# MAGIC (`workspace.sales_data.raw_data`), which both dbutils and Auto Loader
# MAGIC support on serverless.
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

# Catalog/schema names (Community Edition: hive_metastore)
CATALOG = "hive_metastore"
BRONZE  = f"{CATALOG}.bronze"
SILVER  = f"{CATALOG}.silver"
GOLD    = f"{CATALOG}.gold"

# Raw-data landing zone — a Unity Catalog volume (serverless-compatible)
# The public DBFS root (`/FileStore`) is disabled on serverless compute, so the
# ERP export files live in a UC volume instead. Layout mirrors a daily batch:
#   /Volumes/workspace/sales_data/raw_data/erp/customer/
#   /Volumes/workspace/sales_data/raw_data/erp/product/
#   /Volumes/workspace/sales_data/raw_data/transactions/dt=YYYY-MM-DD/
VOLUME_RAW   = "/Volumes/workspace/sales_data/raw_data"
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

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Raw, near-source data as received from ERP/transaction systems';
# MAGIC CREATE DATABASE IF NOT EXISTS silver COMMENT 'Cleansed, standardized, conformed data';
# MAGIC CREATE DATABASE IF NOT EXISTS gold   COMMENT 'Business-ready dimensional model for dashboards';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Raw-data landing zone in a Unity Catalog volume
# MAGIC Serverless compute disables the public DBFS root (`/FileStore`), so the
# MAGIC ERP export files live in a **UC volume** — `dbutils.fs`
# MAGIC (`mkdirs`/`ls`/`rm`), `spark.read` and **Auto Loader** all support
# MAGIC `/Volumes/...` paths on serverless.
# MAGIC
# MAGIC - The `raw_data` volume is created *if not exists* (idempotent) under
# MAGIC   the `workspace` catalog → `workspace.sales_data.raw_data`
# MAGIC - `01` writes the synthetic ERP exports there (CSV + one JSON folder)
# MAGIC - `02` ingests them with Auto Loader (checkpoints under the volume)
# MAGIC - `10` appends "next-day" files for the incremental demo
# MAGIC
# MAGIC ⚠️ Set `RESET_LANDING_ZONE = True` **only** if you want to wipe and
# MAGIC regenerate everything from scratch — it deletes raw data + checkpoints.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.sales_data COMMENT 'Raw-data landing zone for the sales platform';
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.sales_data.raw_data COMMENT 'ERP export files (serverless-compatible replacement for DBFS /FileStore)';

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

from pyspark import SparkContext

sc = SparkContext.getOrCreate()
spark_version = sc.version
dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "n/a")
print(f"Spark version : {spark_version}")
print(f"DBR version   : {dbr_version}")
print(f"Delta tables  : enabled (built into DBR)")
print(f"Current date  : {date.today().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** run `01_Generate_Synthetic_ERP_Data` to create realistic source data
# MAGIC (with deliberate data-quality issues so the Silver layer has something to fix).
