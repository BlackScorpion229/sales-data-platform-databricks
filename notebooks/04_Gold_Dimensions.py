# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold Layer: Dimension Tables (SCD Type 2)
# MAGIC
# MAGIC **Purpose:** Build the conformed dimension tables from doc §15:
# MAGIC `dim_customer`, `dim_product`, `dim_date`, `dim_region`, `dim_sales_rep`.
# MAGIC
# MAGIC **Slowly Changing Dimension Type 2 (doc §38)** — the star of this notebook:
# MAGIC - Every attribute change **keeps the old version** (closed) and **opens a new
# MAGIC   version** (current) with `effective_start_date` / `effective_end_date` /
# MAGIC   `is_current` flags
# MAGIC - Facts are joined **as-of the transaction date**, so historical revenue is
# MAGIC   always attributed to the customer's attributes *at that time*
# MAGIC - The SCD2 engine is **generic** — one function serves `dim_customer` and
# MAGIC   `dim_product`
# MAGIC - **Idempotent**: re-running with unchanged source data is a no-op
# MAGIC
# MAGIC **Surrogate keys** are deterministic (derived from natural IDs) — stable across
# MAGIC environments and reloads, no lookup joins needed.

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `dim_date` — static calendar (2024-01-01 → 2027-12-31)
# MAGIC Includes **fiscal calendar** fields (fiscal year starts in April) per doc §15.3.

# COMMAND ----------

from datetime import date as d, timedelta

start_date = d(2024, 1, 1)
end_date   = d(2027, 12, 31)

rows = []
cur = start_date
while cur <= end_date:
    rows.append({
        "date_key":       int(cur.strftime("%Y%m%d")),
        "calendar_date":  cur.isoformat(),
        "day":            cur.day,
        "day_name":       cur.strftime("%A"),
        "day_of_week":    cur.isoweekday(),
        "week":           cur.isocalendar().week,
        "month":          cur.month,
        "month_name":     cur.strftime("%B"),
        "quarter":        (cur.month - 1) // 3 + 1,
        "year":           cur.year,
        "is_weekend":     cur.weekday() >= 5,
        "fiscal_month":   (cur.month + 8) % 12 + 1,
        "fiscal_quarter": ((cur.month + 8) % 12) // 3 + 1,
        "fiscal_year":    cur.year - 1 if cur.month < 4 else cur.year,
    })
    cur += timedelta(days=1)

dim_date = spark.createDataFrame(rows)
dim_date.write.mode("overwrite").saveAsTable(TABLES["gold_dim_date"])
print(f"SalesRevenueCustomerAnalytics.gold.dim_date: {len(rows):,} days ({start_date} → {end_date})")
spark.sql(f"DESCRIBE {TABLES['gold_dim_date']}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conformed dimensions (static) — `dim_region`, `dim_sales_rep`

# COMMAND ----------

from pyspark.sql import functions as F

region = (
    spark.read.table(f"{BRONZE}.erp_region")
    .withColumn("region_key", F.col("region_id").substr(2, 2).cast("int"))
    .select("region_key", "region_id", "region_name", "country", "state", "territory")
    .orderBy("region_key")
)
region.write.mode("overwrite").saveAsTable(TABLES["gold_dim_region"])
print(f"SalesRevenueCustomerAnalytics.gold.dim_region: {region.count()} rows")

sales_rep = (
    spark.read.table(f"{BRONZE}.erp_sales_rep")
    .withColumn("sales_rep_key", F.col("sales_rep_id").substr(3, 3).cast("int"))
    .select("sales_rep_key", "sales_rep_id", "sales_rep_name", "region_id", "sales_rep_email", "hire_date", "status")
    .orderBy("sales_rep_key")
)
sales_rep.write.mode("overwrite").saveAsTable(TABLES["gold_dim_sales_rep"])
print(f"SalesRevenueCustomerAnalytics.gold.dim_sales_rep: {sales_rep.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generic SCD Type 2 engine
# MAGIC
# MAGIC **How it works** (standard, proven approach):
# MAGIC 1. Load **current** versions from the target dim and hash their attribute set
# MAGIC 2. Load the **latest source state** (Silver) and hash the same attributes
# MAGIC 3. `hash_source != hash_target` (or customer is new)  →  rows to insert
# MAGIC 4. Rows that changed → the existing current version gets **closed** at the
# MAGIC    source's `updated_date` (the moment the change took effect)
# MAGIC 5. Apply: `MERGE` closes (update flags) + append new versions
# MAGIC
# MAGIC **Idempotency:** after a successful run, source hash == target hash for every
# MAGIC current row → next run computes zero changes → no-op.

# COMMAND ----------

def scd2_upsert(target_table, source_df, key_col, attr_cols, start_col, version_col="updated_date"):
    """Type-2 upsert of a master dimension.
    - source_df   : latest state (from Silver)
    - attr_cols   : business attributes tracked for change detection
    - start_col   : column used as effective_start_date for new versions
    - version_col : source column giving the moment an attribute changed
    """
    def hash_cols(df):
        return df.withColumn(
            "_attr_hash",
            F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in attr_cols]), 256),
        )

    source = hash_cols(source_df)
    target_exists = spark.catalog.tableExists(target_table)

    if not target_exists:
        # initial load — every row is a new current version
        dim = (
            source.drop("_attr_hash")
            .withColumn("effective_start_date", F.col(start_col))
            .withColumn("effective_end_date",   F.lit(None).cast("date"))
            .withColumn("is_current",           F.lit(True))
        )
        dim.write.saveAsTable(target_table)
        print(f"  [{target_table}] initial load: {dim.count():,} rows (all current)")
        return

    tgt = spark.read.table(target_table)
    if "_attr_hash" in tgt.columns:
        tgt = tgt.drop("_attr_hash")   # defensive: hash must never persist in the dim
    current = hash_cols(tgt.filter("is_current = true"))

    # prefix every source column so joins never hit ambiguous names
    non_key = [c for c in source.columns if c not in (key_col, "_attr_hash")]
    src = source.select(
        *[F.col(c).alias(f"s_{c}") for c in non_key],
        F.col(key_col),
        F.col("_attr_hash").alias("src_hash"),
    )
    cur = current.select(
        *[F.col(c).alias(f"t_{c}") for c in current.columns if c not in (key_col, "_attr_hash")],
        F.col(key_col),
        F.col("_attr_hash").alias("tgt_hash"),
    )

    # changes = masters whose current version hash differs from latest source hash
    changes = (
        cur.join(src, on=key_col, how="inner")
        .filter(F.col("tgt_hash") != F.col("src_hash"))
    )

    # 1) close the old current version at the change date
    closes = changes.select(
        F.col(key_col).alias("_key_to_close"),
        F.coalesce(F.col(f"s_{version_col}"), F.current_date()).alias("_close_date"),
    )

    # 2) open the new version (for both changed and brand-new masters)
    new_src = src.withColumnRenamed("src_hash", "_attr_hash")
    for c in list(new_src.columns):
        if c.startswith("s_"):
            new_src = new_src.withColumnRenamed(c, c[2:])

    changed_ids = changes.select(key_col)

    new_versions = (
        new_src.join(cur.select(key_col), on=key_col, how="left_anti")          # brand-new
        .union(new_src.join(changed_ids, on=key_col, how="inner"))              # changed
        .withColumn("effective_start_date", F.coalesce(F.col(version_col), F.col(start_col), F.current_date()))
        .withColumn("effective_end_date",   F.lit(None).cast("date"))
        .withColumn("is_current",           F.lit(True))
        .drop("_attr_hash")
    )

    # 3) apply: update closes first (they share the key), then append new versions
    closes.createOrReplaceTempView("closes_v")
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING closes_v c
        ON t.{key_col} = c._key_to_close AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            effective_end_date = c._close_date,
            is_current         = false
    """)

    new_versions.write.option("mergeSchema", "true").mode("append").saveAsTable(target_table)

    tgt_after = spark.read.table(target_table)
    n_cur = tgt_after.filter("is_current = true").count()
    print(f"  [{target_table}] closed {closes.count():,} | opened {new_versions.count():,} "
          f"| total {tgt_after.count():,} ({n_cur:,} current)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build `dim_customer` (SCD2)
# MAGIC Tracks: region, state, city, customer_status, customer_segment, customer_type,
# MAGIC industry, sales_rep_id — anything that affects reporting.

# COMMAND ----------

CUST_ATTRS = ["customer_name", "customer_type", "customer_segment", "country", "state",
              "city", "region", "industry", "sales_rep_id", "customer_status"]

silver_customer = (
    spark.read.table(TABLES["silver_customer"])
    .withColumnRenamed("customer_id", "customer_id")
    .withColumn("customer_key", F.col("customer_id").substr(2, 5).cast("int"))
)

scd2_upsert(
    target_table = TABLES["gold_dim_customer"],
    source_df    = silver_customer,
    key_col      = "customer_id",
    attr_cols    = CUST_ATTRS,
    start_col    = "created_date",
    version_col  = "updated_date",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build `dim_product` (SCD2)
# MAGIC Tracks: category, subcategory, brand, price, cost, status.

# COMMAND ----------

PROD_ATTRS = ["product_name", "product_category", "product_subcategory", "brand",
              "unit_price", "cost", "product_status"]

silver_product = (
    spark.read.table(TABLES["silver_product"])
    .withColumnRenamed("product_id", "product_id")
    .withColumn("product_key", F.col("product_id").substr(2, 4).cast("int"))
)

scd2_upsert(
    target_table = TABLES["gold_dim_product"],
    source_df    = silver_product,
    key_col      = "product_id",
    attr_cols    = PROD_ATTRS,
    start_col    = "effective_date",
    version_col  = "updated_date",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspect the result
# MAGIC All customers are current (one version) at this stage. After the incremental
# MAGIC demo (notebook 10) you'll see **two versions per changed customer**.

# COMMAND ----------


display(spark.sql(f"""SELECT
  COUNT(*)                              AS total_versions,
  SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
  COUNT(DISTINCT customer_id)           AS distinct_customers,
  ROUND(SUM(CASE WHEN is_current THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS avg_versions_per_customer
FROM {GOLD}.dim_customer;"""))
# COMMAND ----------


display(spark.sql(f"""SELECT customer_key, customer_id, customer_name, country, state, region,
       customer_status, effective_start_date, effective_end_date, is_current
FROM {GOLD}.dim_customer
ORDER BY customer_id, effective_start_date
LIMIT 15;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - Generic **SCD2 engine** (reusable for any master data) — change detection by
# MAGIC   attribute hashing, closure at the source `updated_date`
# MAGIC - **Deterministic surrogate keys** (stable across runs/environments)
# MAGIC - Idempotent by design — re-runs are no-ops, which you can prove in notebook 10
# MAGIC
# MAGIC **Next:** `05_Gold_Fact_Sales` — the fact table with as-of SCD2 joins.
