# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Data Quality & Audit Framework
# MAGIC
# MAGIC **Purpose:** Every pipeline run records structured DQ metrics into
# MAGIC `SalesRevenueCustomerAnalytics.gold.data_quality_audit` (doc §24 / §34):
# MAGIC
# MAGIC | Metric | Meaning |
# MAGIC |--------|---------|
# MAGIC | `records_received` | rows read from source (Bronze) |
# MAGIC | `records_processed` | rows written to target (Silver) |
# MAGIC | `records_rejected` | rows quarantined / failed checks |
# MAGIC | `duplicate_records` | duplicates removed |
# MAGIC | `null_records` | rows failing completeness rules |
# MAGIC | `invalid_records` | rows failing validity/domain/RI rules |
# MAGIC | `processing_time` | wall-clock duration |
# MAGIC
# MAGIC Plus a **reconciliation check** (doc §40): compare Silver net revenue against
# MAGIC the raw source within an agreed tolerance — the "did we lose or invent money
# MAGIC in the pipeline?" question.

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Audit table (created once, appended every run)

# COMMAND ----------


spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.data_quality_audit (
  run_id             STRING,
  pipeline_name      STRING,
  run_date           DATE,
  start_time         TIMESTAMP,
  end_time           TIMESTAMP,
  status             STRING,
  records_received   BIGINT,
  records_processed  BIGINT,
  records_rejected   BIGINT,
  duplicate_records  BIGINT,
  null_records       BIGINT,
  invalid_records    BIGINT,
  processing_time_sec DOUBLE,
  message            STRING
)
USING DELTA
COMMENT 'Data quality metrics per pipeline execution (doc §24)';""")
# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DQ metric computation — Bronze → Silver (transaction pipeline)
# MAGIC Recomputes the numbers for the most recent batch. In a production deploy
# MAGIC this runs *inside* the pipeline (same transaction); here it demonstrates
# MAGIC exactly what each metric measures.

# COMMAND ----------

from pyspark.sql import functions as F

from datetime import datetime
import time
t0 = time.time()

bronze_txn = spark.read.table(TABLES["bronze_transaction"])
silver_txn = spark.read.table(TABLES["silver_transaction"])
quarantine = spark.read.table(TABLES["silver_quarantine"])

n_received  = bronze_txn.count()
n_processed = silver_txn.count()
n_rejected  = quarantine.count()

# duplicates: bronze rows whose transaction_id appears more than once in bronze
n_dupes = bronze_txn.groupBy("transaction_id").count().filter(F.col("count") > 1).count()

# nulls / invalids = rows failing completeness vs validity/domain/RI rules
null_rows    = bronze_txn.filter(
    F.col("transaction_id").isNull() | F.col("customer_id").isNull() |
    F.col("product_id").isNull() | F.col("transaction_date").isNull() |
    F.col("net_amount").isNull()
).count()
invalid_rows = n_rejected - null_rows

# how much revenue was removed (i.e. is *not* in Silver) — should equal rejected
rev_rejected = (
    quarantine
    .selectExpr("COALESCE(CAST(get_json_object(record_data, '$.net_amount') AS DOUBLE), 0) AS n")
    .agg(F.sum("n").alias("v"))
    .collect()[0]["v"] or 0.0
)

elapsed = round(time.time() - t0, 2)

metrics = {
    "run_id": f"RUN_{datetime.now():%Y%m%d_%H%M%S}",
    "pipeline_name": "bronze_to_silver_transactions",
    "run_date": datetime.now().date(),
    "start_time": datetime.now(),
    "end_time": datetime.now(),
    "status": "SUCCESS",
    "records_received": n_received,
    "records_processed": n_processed,
    "records_rejected": n_rejected,
    "duplicate_records": n_dupes,
    "null_records": null_rows,
    "invalid_records": invalid_rows,
    "processing_time_sec": elapsed,
    "message": f"Rejected revenue (net): {round(rev_rejected or 0, 2)}",
}
print(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Persist the audit record

# COMMAND ----------

audit_df = spark.createDataFrame([metrics])
audit_df.write.mode("append").saveAsTable(TABLES["gold_dq_audit"])
print("Audit record written")

display(spark.sql(f"""SELECT * FROM {GOLD}.data_quality_audit ORDER BY run_date DESC;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reconciliation: source vs Silver (doc §40)
# MAGIC Compares Bronze (raw) revenue against Silver revenue. The difference must
# MAGIC equal exactly what was quarantined — anything else means data was lost.

# COMMAND ----------

from pyspark.sql import functions as F

tol_usd = 0.01  # tolerance in USD (business-defined in production)

bronze_net = bronze_txn.selectExpr("CAST(net_amount AS DOUBLE) AS n").selectExpr("COALESCE(SUM(n),0) AS v").collect()[0]["v"]
silver_net = silver_txn.agg(F.sum("net_sales_usd")).collect()[0][0] or 0.0
quarantined_net = (spark.table(f"{SILVER}.sales_quarantine")
                   .selectExpr("COALESCE(CAST(get_json_object(record_data, '$.net_amount') AS DOUBLE), 0) AS n")
                   .selectExpr("COALESCE(SUM(n),0) AS v").collect()[0]["v"])

difference = abs(bronze_net - (silver_net + quarantined_net))
status = "RECONCILED" if difference <= tol_usd else "MISMATCH"

print(f"Bronze net revenue  : {bronze_net:>16,.2f}")
print(f"Silver net revenue  : {silver_net:>16,.2f}")
print(f"Quarantined revenue : {quarantined_net:>16,.2f}")
print(f"Bronze - (Silver + Quarantine) = {difference:>16,.2f}  →  {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - DQ metrics are **structured, persisted, and queryable** — not console noise
# MAGIC - **Reconciliation proves data integrity**: source == gold + rejected within tolerance
# MAGIC - The same metrics feed **alerts** (failures, volume changes) and **SLA dashboards**
# MAGIC
# MAGIC **Next:** `07_Aggregates` — pre-aggregated tables for fast dashboards.
