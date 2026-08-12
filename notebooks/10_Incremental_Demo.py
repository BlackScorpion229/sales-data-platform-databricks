# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Incremental Load & SCD2 Demo (the "next day" simulation)
# MAGIC
# MAGIC **Purpose:** Simulate the next business day after the initial load and prove
# MAGIC the platform's three hardest requirements (doc §26, §36, §38):
# MAGIC
# MAGIC 1. **Auto Loader processes ONLY the new files** — the ~270K existing
# MAGIC    transaction files are never re-read (checkpoint-based incremental discovery)
# MAGIC 2. **MERGE upserts are idempotent** — re-running a pipeline changes nothing
# MAGIC 3. **SCD Type 2** — changed customers get a closed + opened version, and
# MAGIC    historical revenue stays attributed to their *then-current* attributes
# MAGIC
# MAGIC **Run me after notebooks 00–09.**

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Baseline snapshot (before the new batch)

# COMMAND ----------

from pyspark.sql import functions as F

def table_count(tbl):
    return spark.read.table(tbl).count() if spark.catalog.tableExists(tbl) else 0

baseline = {
    "bronze_transactions": table_count(TABLES["bronze_transaction"]),
    "silver_transactions": table_count(TABLES["silver_transaction"]),
    "quarantine":          table_count(TABLES["silver_quarantine"]),
    "dim_customer_versions": table_count(TABLES["gold_dim_customer"]),
    "fact_sales":          table_count(TABLES["gold_fact_sales"]),
}
print("Baseline:")
for k, v in baseline.items():
    print(f"  {k:24s} {v:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulate the ERP pushing a new daily batch
# MAGIC New transaction files for 4 new days with fresh DQ issues — both classes:
# MAGIC **unrecoverable** (null customer, `On Hold` status, `XYZ` currency,
# MAGIC duplicates → quarantine/dedup) and **recoverable** (`shipped` → normalized
# MAGIC to `Completed`). Plus the customer-master update file (region moves /
# MAGIC status changes / new customers → SCD2).

# COMMAND ----------

import random
from datetime import date, timedelta

rng = random.Random(20260812)
new_start = date(2026, 8, 12)

customers = spark.read.table(TABLES["silver_customer"]).collect()
products  = spark.read.table(TABLES["silver_product"]).select("product_id", "unit_price", "product_category").collect()
fx        = {r["currency_code"]: r["exchange_rate_to_usd"] for r in spark.read.table(f"{SILVER}.exchange_rate").collect()}

cur_w = ["USD"] * 92 + ["EUR"] * 3 + ["GBP"] * 2 + ["CAD"] * 2 + ["INR"] * 1
cust_w = [rng.expovariate(1 / (1 + (int(c["customer_id"][1:]) % 100) * 0.6)) for c in customers]

rows = []
order_id = 600000
for i in range(4):  # 4 new days
    day = new_start + timedelta(days=i)
    for _ in range(rng.randint(80, 120)):
        cust = rng.choices(customers, weights=cust_w)[0]
        order_id += 1
        cur = rng.choice(cur_w)
        for _ in range(rng.randint(1, 5)):
            p = rng.choice(products)
            qty = rng.randint(1, 20)
            up = round(p["unit_price"] * rng.uniform(0.85, 1.05), 2)
            gross = round(qty * up, 2)
            disc = round(gross * rng.choice([0, 0, 0, 0.05, 0.1, 0.15]), 2)
            tax = round((gross - disc) * 0.08, 2)
            rows.append({
                "transaction_id": f"T{order_id}-{i}-{rng.randint(0, 9999)}",
                "order_id": str(order_id),
                "transaction_date": day.isoformat(),
                "customer_id": cust["customer_id"],
                "product_id": p["product_id"],
                "quantity": qty, "unit_price": up, "discount": disc, "tax": tax,
                "gross_amount": gross, "net_amount": round(gross - disc + tax, 2),
                "currency": cur, "region_id": cust["region"], "sales_rep_id": cust["sales_rep_id"],
                "transaction_status": "Completed", "source_system": "ERP-SALES",
            })

# fresh DQ issues in this batch (mix of recoverable + unrecoverable)
for idx in rng.sample(range(len(rows)), 5):      rows[idx]["customer_id"] = None          # unrecoverable -> quarantine
for idx in rng.sample(range(len(rows)), 4):      rows[idx]["transaction_status"] = "On Hold"  # unrecoverable -> quarantine
for idx in rng.sample(range(len(rows)), 6):      rows[idx]["transaction_status"] = "shipped"  # recoverable -> normalized
for idx in rng.sample(range(len(rows)), 3):      rows[idx]["currency"] = "XYZ"          # unrecoverable -> quarantine
for idx in rng.sample(range(len(rows)), 4):      rows.append(dict(rows[idx]))           # duplicates -> dedup

TXN_COLS = ["transaction_id", "order_id", "transaction_date", "customer_id", "product_id",
            "quantity", "unit_price", "discount", "tax", "gross_amount", "net_amount",
            "currency", "region_id", "sales_rep_id", "transaction_status", "source_system"]

spark.createDataFrame(rows, schema=TXN_COLS) \
    .withColumn("dt", F.col("transaction_date")) \
    .coalesce(1) \
    .write.mode("append").partitionBy("dt").option("header", True).csv(RAW_TRANSACT)

print(f"New batch written: {len(rows):,} transaction rows across 4 new daily files")
print(f"New customers / changes: /FileStore/raw_data/erp/customer_updates/ (from notebook 01)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Re-run Bronze ingestion — Auto Loader picks up **only the new files**
# MAGIC
# MAGIC `%run` executes the notebook code inline: the checkpoint remembers everything
# MAGIC already seen, so the 270K+ existing files are *not* reprocessed — only the 4
# MAGIC new files (and the customer-update file) enter Bronze.

# COMMAND ----------

# ingest the new transaction files + the customer update file
for src, tgt, ckpt in [
    (RAW_TRANSACT, TABLES["bronze_transaction"], f"{CHECKPOINT_BASE}/transaction"),
    (f"{RAW_BASE}/erp/customer_updates", TABLES["bronze_customer"], f"{CHECKPOINT_BASE}/customer_updates"),
]:
    from datetime import datetime
    BATCH_ID = f"BATCH_{datetime.now():%Y%m%d_%H%M%S}"
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", ckpt)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(src)
    )
    audit = (
        stream
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file",         F.input_file_name())
        .withColumn("_source_system",       F.lit("ERP"))
        .withColumn("_batch_id",            F.lit(BATCH_ID))
        .withColumn("_record_hash",         F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in stream.columns]), 256))
    )
    q = (audit.writeStream.option("checkpointLocation", ckpt)
         .trigger(availableNow=True).toTable(tgt))
    q.awaitTermination()
    print(f"  bronze <- {src}: total now {table_count(tgt):,} rows")

print(f"\nBronze transactions grew by: {table_count(TABLES['bronze_transaction']) - baseline['bronze_transactions']:,} (≈ new files only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Re-run the transformation chain (Silver → Gold)
# MAGIC All pipelines are idempotent upserts — re-running them is exactly how
# MAGIC incremental processing works (doc §26: Bronze → Silver MERGE → Gold MERGE).

# COMMAND ----------

# MAGIC %run ./03_Silver_Cleansing

# COMMAND ----------

# MAGIC %run ./04_Gold_Dimensions

# COMMAND ----------

# MAGIC %run ./05_Gold_Fact_Sales

# COMMAND ----------

# MAGIC %run ./06_Data_Quality_Audit

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prove it worked
# MAGIC ### 5a. Counts after the incremental run

# COMMAND ----------

after = {
    "bronze_transactions": table_count(TABLES["bronze_transaction"]),
    "silver_transactions": table_count(TABLES["silver_transaction"]),
    "quarantine":          table_count(TABLES["silver_quarantine"]),
    "dim_customer_versions": table_count(TABLES["gold_dim_customer"]),
    "fact_sales":          table_count(TABLES["gold_fact_sales"]),
}
print(f"{'metric':24s} {'before':>10s} {'after':>10s} {'delta':>10s}")
for k in after:
    d = after[k] - baseline[k]
    print(f"{k:24s} {baseline[k]:>10,} {after[k]:>10,} {d:>+10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. SCD2 in action — customers now have multiple versions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer_name, region, state, customer_status,
# MAGIC        effective_start_date, effective_end_date, is_current
# MAGIC FROM gold.dim_customer
# MAGIC WHERE customer_id IN (
# MAGIC   SELECT customer_id FROM gold.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
# MAGIC )
# MAGIC ORDER BY customer_id, effective_start_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. The payoff: historical revenue follows the *then-current* attributes
# MAGIC Pick a customer who moved regions — their pre-move revenue must be attributed
# MAGIC to the OLD region, post-move revenue to the NEW region (doc §38).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH moved AS (
# MAGIC   SELECT customer_id, region AS new_region, effective_start_date AS moved_on
# MAGIC   FROM gold.dim_customer
# MAGIC   WHERE is_current = true AND effective_start_date > '2026-06-01'
# MAGIC     AND customer_id IN (SELECT customer_id FROM gold.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1)
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT
# MAGIC   m.customer_id,
# MAGIC   c.customer_name,
# MAGIC   d.year_month,
# MAGIC   r.region_name,
# MAGIC   CASE WHEN d.calendar_date < m.moved_on THEN 'BEFORE move' ELSE 'AFTER move' END AS period,
# MAGIC   ROUND(SUM(f.net_sales), 2) AS revenue
# MAGIC FROM moved m
# MAGIC JOIN gold.fact_sales f ON f.customer_key IN (
# MAGIC   SELECT customer_key FROM gold.dim_customer WHERE customer_id = m.customer_id)
# MAGIC JOIN gold.dim_customer c ON c.customer_key = f.customer_key
# MAGIC JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC JOIN gold.dim_region r ON f.region_key = r.region_key
# MAGIC GROUP BY m.customer_id, c.customer_name, d.year_month, r.region_name, m.moved_on, d.calendar_date
# MAGIC ORDER BY d.calendar_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d. Idempotency proof — re-run Silver again: **zero change**

# COMMAND ----------

silver_before = table_count(TABLES["silver_transaction"])

# COMMAND ----------

# MAGIC %run ./03_Silver_Cleansing

# COMMAND ----------

silver_after = table_count(TABLES["silver_transaction"])
print(f"silver.sales_transaction: {silver_before:,} -> {silver_after:,}"
      + ("  ✓ IDEMPOTENT (no duplicates)" if silver_before == silver_after else "  ✗ CHANGED!"))

# MAGIC %sql
# MAGIC SELECT * FROM gold.data_quality_audit ORDER BY run_date DESC, end_time DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - **Incremental ingestion** proven with numbers (only new files processed)
# MAGIC - **SCD2 versioning** + as-of attribution demonstrated on real changed customers
# MAGIC - **Idempotency** proven by re-running the pipeline (zero duplicates)
# MAGIC - **Audit trail** shows every batch with its DQ metrics
