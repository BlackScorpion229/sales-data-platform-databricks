# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Incremental Load & SCD2 Demo (the "next day" simulation)
# MAGIC
# MAGIC **Purpose:** Simulate the next business day after the initial load and prove
# MAGIC the platform's three hardest requirements (doc §26, §36, §38):
# MAGIC
# MAGIC 1. **Only the new rows enter Bronze** — the ~270K existing transactions
# MAGIC    are never re-read or rewritten: new daily files land in the raw
# MAGIC    **UC volume**, and Auto Loader's checkpoint discovers **only those
# MAGIC    files** (incremental by design)
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
# MAGIC ## 2. Simulate the ERP pushing a new daily batch (raw volume)
# MAGIC New transaction rows for 4 new days are written as **new files** into
# MAGIC `transactions/dt=YYYY-MM-DD/` with fresh DQ issues — both classes:
# MAGIC **unrecoverable** (null customer, `On Hold` status, `XYZ` currency,
# MAGIC duplicates → quarantine/dedup) and **recoverable** (`shipped` → normalized
# MAGIC to `Completed`). The customer-master updates (region moves / status changes
# MAGIC / new customers → SCD2) were staged by notebook 01 in
# MAGIC `erp/customer_updates/` and are ingested in step 3.

# COMMAND ----------

import random
from datetime import date, datetime, timedelta

rng = random.Random(20260812)
new_start = date(2026, 8, 12)

customers = spark.read.table(TABLES["silver_customer"]).collect()
products  = [p for p in spark.read.table(TABLES["silver_product"])
               .select("product_id", "unit_price", "product_category").collect()
               if p["unit_price"] is not None]
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

from pyspark.sql import functions as F

# select(*) after createDataFrame: dicts are inferred with alphabetically-sorted
# keys, so a positional schema would map values to the wrong columns
new_txns = spark.createDataFrame(rows).select(*TXN_COLS)
new_txns.coalesce(1).withColumn("dt", F.col("transaction_date")) \
    .write.mode("append").partitionBy("dt").option("header", True).csv(RAW_TRANSACT)

print(f"New batch written to raw volume: {len(rows):,} transaction rows -> {len({r['transaction_date'] for r in rows})} new daily files under {RAW_TRANSACT}")
print(f"Customer-update file staged by notebook 01: {RAW_BASE}/erp/customer_updates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Re-run Auto Loader — only the new files are discovered
# MAGIC
# MAGIC Re-running the exact Auto Loader pipeline from notebook 02 against the same
# MAGIC volume **with the same checkpoint** processes *only* the files that arrived
# MAGIC since the initial load: the 4 new daily transaction files. The ~730
# MAGIC existing daily files are skipped by the checkpoint — no re-read, no
# MAGIC re-parse. The customer-update file (held back since notebook 01) is
# MAGIC ingested with its own fresh checkpoint.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

BATCH_ID = f"BATCH_{datetime.now():%Y%m%d_%H%M%S}"
AUDIT_COLS = {
    "_ingestion_timestamp": F.current_timestamp(),
    "_source_system":       F.lit("ERP"),
    "_batch_id":            F.lit(BATCH_ID),
}
TXN_DATA_COLS = TXN_COLS
CUST_DATA_COLS = ["customer_id", "customer_name", "customer_type", "customer_segment", "country",
                  "state", "city", "region", "industry", "sales_rep_id", "customer_status",
                  "created_date", "updated_date"]

def incremental_ingest(dataset, path, table, fmt, data_cols, partitioned=False, checkpoint=None):
    """Auto Loader stream from the volume with the same audit/dedup contract as 02."""
    if checkpoint is None:
        checkpoint = f"{CHECKPOINT_BASE}/{dataset}"
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumnName", "_rescued_data")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.checkpointLocation", checkpoint)
        # Spark Connect (serverless) lowercases option keys, which breaks
        # cloudFiles' case-sensitive option validation — skip the check.
        .option("cloudFiles.validateOptions", "false")
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    df = reader.load(path)
    if partitioned:
        df = df.withColumn("dt", F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"dt=(\d{4}-\d{2}-\d{2})", 1)))
    df = (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumns(AUDIT_COLS)
          .withColumn("_record_hash",
                      F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in data_cols]), 256))
          .drop("_rescued_data")
          .dropDuplicates(["_record_hash"])
    )
    q = df.writeStream.option("checkpointLocation", checkpoint)
    if partitioned:
        q = q.partitionBy("dt")
    q = q.trigger(availableNow=True).toTable(table)
    return q

# 1) transactions: SAME checkpoint as notebook 02 -> only the 4 new daily files
q_txn = incremental_ingest(
    "transaction", RAW_TRANSACT, TABLES["bronze_transaction"],
    "csv", TXN_DATA_COLS, partitioned=True, checkpoint=f"{CHECKPOINT_BASE}/transaction")

# 2) customer updates: fresh checkpoint (held back from 02's load)
q_cust = incremental_ingest(
    "customer_updates", f"{RAW_BASE}/erp/customer_updates", f"{BRONZE}.erp_customer_updates",
    "csv", CUST_DATA_COLS, checkpoint=f"{CHECKPOINT_BASE}/customer_updates")

for q in [q_txn, q_cust]:
    q.awaitTermination()

files_txn = q_txn.lastProgress["numInputFiles"] if q_txn.lastProgress else 0
print(f"Auto Loader incremental run:")
print(f"  transactions    : {files_txn} new file(s) discovered (checkpoint: {CHECKPOINT_BASE}/transaction)")
print(f"  customer_updates: {q_cust.lastProgress['numInputFiles'] if q_cust.lastProgress else 0} file(s) discovered")
print(f"Bronze transactions grew by: {table_count(TABLES['bronze_transaction']) - baseline['bronze_transactions']:,} (only the new rows)")

# 3) fold the updates into bronze.erp_customer (SCD2 source for Silver/Gold)
updates = (
    spark.read.table(f"{BRONZE}.erp_customer_updates")
    .select(*CUST_DATA_COLS)
    .withColumns(AUDIT_COLS)
    .withColumn("_record_hash",
                F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in CUST_DATA_COLS]), 256))
    .dropDuplicates(["_record_hash"])
)
updates.write.mode("append").saveAsTable(TABLES["bronze_customer"])
print(f"SalesRevenueCustomerAnalytics.bronze.erp_customer += {updates.count():,} update rows (SCD2 source)")

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


display(spark.sql(f"""SELECT customer_id, customer_name, region, state, customer_status,
       effective_start_date, effective_end_date, is_current
FROM {GOLD}.dim_customer
WHERE customer_id IN (
  SELECT customer_id FROM {GOLD}.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
)
ORDER BY customer_id, effective_start_date;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. The payoff: historical revenue follows the *then-current* attributes
# MAGIC Pick a customer who moved regions — their pre-move revenue must be attributed
# MAGIC to the OLD region, post-move revenue to the NEW region (doc §38).

# COMMAND ----------


display(spark.sql(f"""WITH moved AS (
  SELECT customer_id, region AS new_region, effective_start_date AS moved_on
  FROM {GOLD}.dim_customer
  WHERE is_current = true AND effective_start_date > '2026-06-01'
    AND customer_id IN (SELECT customer_id FROM {GOLD}.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1)
  LIMIT 1
)
SELECT
  m.customer_id,
  c.customer_name,
  d.year_month,
  r.region_name,
  CASE WHEN d.calendar_date < m.moved_on THEN 'BEFORE move' ELSE 'AFTER move' END AS period,
  ROUND(SUM(f.net_sales), 2) AS revenue
FROM moved m
JOIN {GOLD}.fact_sales f ON f.customer_key IN (
  SELECT customer_key FROM {GOLD}.dim_customer WHERE customer_id = m.customer_id)
JOIN {GOLD}.dim_customer c ON c.customer_key = f.customer_key
JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
JOIN {GOLD}.dim_region r ON f.region_key = r.region_key
GROUP BY m.customer_id, c.customer_name, d.year_month, r.region_name, m.moved_on, d.calendar_date
ORDER BY d.calendar_date;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d. Idempotency proof — re-run Silver again: **zero change**

# COMMAND ----------

silver_before = table_count(TABLES["silver_transaction"])

# COMMAND ----------

# MAGIC %run ./03_Silver_Cleansing

# COMMAND ----------

silver_after = table_count(TABLES["silver_transaction"])
print(f"SalesRevenueCustomerAnalytics.silver.sales_transaction: {silver_before:,} -> {silver_after:,}"
      + ("  ✓ IDEMPOTENT (no duplicates)" if silver_before == silver_after else "  ✗ CHANGED!"))

display(spark.sql(f"""SELECT * FROM {GOLD}.data_quality_audit ORDER BY run_date DESC, end_time DESC LIMIT 5;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - **Incremental processing** proven with numbers (only the new rows enter
# MAGIC   Bronze — the ~270K existing rows are untouched)
# MAGIC - **SCD2 versioning** + as-of attribution demonstrated on real changed customers
# MAGIC - **Idempotency** proven by re-running the pipeline (zero duplicates)
# MAGIC - **Audit trail** shows every batch with its DQ metrics
