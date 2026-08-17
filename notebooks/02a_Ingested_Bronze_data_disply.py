# Databricks notebook source
# MAGIC %md
# MAGIC # 02a — Display Bronze Layer Tables
# MAGIC
# MAGIC **Purpose:** Read every Bronze table as a DataFrame (no transformation)
# MAGIC and display the first 10 rows of each, plus row/column counts.
# MAGIC
# MAGIC **Run order:** `00` → `01` → `02` first, then this notebook.

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

BRONZE_TABLES = {
    "erp_customer":          f"{BRONZE}.erp_customer",
    "erp_customer_updates":  f"{BRONZE}.erp_customer_updates",
    "erp_product":           f"{BRONZE}.erp_product",
    "erp_region":            f"{BRONZE}.erp_region",
    "erp_sales_rep":         f"{BRONZE}.erp_sales_rep",
    "erp_currency":          f"{BRONZE}.erp_currency",
    "sales_order":           f"{BRONZE}.sales_order",
    "sales_transaction":     f"{BRONZE}.sales_transaction",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read each table and show the first 10 rows

# COMMAND ----------

for name, tbl in BRONZE_TABLES.items():
    print(f"=== {tbl} ===")
    try:
        df = spark.read.table(tbl)
        n = df.count()
        print(f"  rows: {n:,}  |  columns: {len(df.columns)}")
        display(df.limit(10))
    except Exception as e:
        print(f"  ! could not read table: {type(e).__name__}: {e}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Summary — all Bronze tables and their row counts

# COMMAND ----------

results = []
for name, tbl in BRONZE_TABLES.items():
    try:
        df = spark.read.table(tbl)
        results.append((name, tbl, df.count(), len(df.columns)))
    except Exception as e:
        results.append((name, tbl, None, None))

print(f"{'dataset':<22s} {'table':<50s} {'rows':>12s} {'cols':>5s}")
print("-" * 95)
for name, tbl, n, c in results:
    print(f"{name:<22s} {tbl:<50s} {str(n) if n is not None else 'N/A':>12s} {str(c) if c is not None else 'N/A':>5s}")