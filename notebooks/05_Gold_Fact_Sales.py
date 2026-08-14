# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold Layer: `fact_sales`
# MAGIC
# MAGIC **Purpose:** Build the central fact table (doc §14) by joining cleansed
# MAGIC Silver transactions to the Gold dimension model — using **as-of SCD2 joins**:
# MAGIC
# MAGIC ```
# MAGIC     SalesRevenueCustomerAnalytics.silver.sales_transaction
# MAGIC        ├── dim_date       (ON date_key)
# MAGIC        ├── dim_customer   (ON customer_id AND transaction_date BETWEEN effective_start/end)
# MAGIC        ├── dim_product    (ON product_id   AND transaction_date BETWEEN effective_start/end)
# MAGIC        ├── dim_region     (ON region_id)
# MAGIC        └── dim_sales_rep  (ON sales_rep_id)
# MAGIC ```
# MAGIC
# MAGIC Because the customer/product dims keep **versions**, the join picks the
# MAGIC version that was valid *on the transaction date* — so a customer's historical
# MAGIC revenue is attributed to the region they were in **at that time** (doc §38).
# MAGIC
# MAGIC **Also:** profit calculation (net sales − cost of goods sold), currency
# MAGIC exposure retained for audit, and an idempotent `MERGE` + `OPTIMIZE` with
# MAGIC Z-ORDER for dashboard performance (doc §27).

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. As-of SCD2 joins (SQL for clarity)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW fact_prep AS
SELECT
    t.transaction_id                                        AS sales_key,
    t.transaction_id,
    t.order_id,
    d.date_key                                             AS date_key,
    c.customer_key                                         AS customer_key,
    p.product_key                                          AS product_key,
    r.region_key                                           AS region_key,
    sr.sales_rep_key                                       AS sales_rep_key,
    t.quantity,
    t.gross_amount_usd,
    t.discount_usd,
    t.tax_usd,
    t.net_sales_usd                                        AS net_sales,
    ROUND(t.quantity * p.cost, 2)                          AS cost_amount,
    ROUND(t.net_sales_usd - t.quantity * p.cost, 2)        AS profit_amount,
    t.currency,
    t.fx_rate,
    t.transaction_status,
    t.transaction_date,
    t._ingestion_timestamp                                 AS created_timestamp,
    t._ingestion_timestamp                                 AS updated_timestamp
FROM {SILVER}.sales_transaction t
JOIN {GOLD}.dim_date      d ON d.date_key    = CAST(DATE_FORMAT(t.transaction_date, 'yyyyMMdd') AS INT)
JOIN {GOLD}.dim_customer  c ON c.customer_id = t.customer_id
    AND t.transaction_date >= c.effective_start_date
    AND (c.effective_end_date IS NULL OR t.transaction_date < c.effective_end_date)
JOIN {GOLD}.dim_product   p ON p.product_id  = t.product_id
    AND t.transaction_date >= p.effective_start_date
    AND (p.effective_end_date IS NULL OR t.transaction_date < p.effective_end_date)
JOIN {GOLD}.dim_region    r ON r.region_id   = t.region_id
JOIN {GOLD}.dim_sales_rep sr ON sr.sales_rep_id = t.sales_rep_id
""")

fact_prep = spark.table("fact_prep")
print(f"fact_prep rows: {fact_prep.count():,}")
fact_prep.select("sales_key", "date_key", "customer_key", "product_key", "region_key",
                 "net_sales", "cost_amount", "profit_amount", "transaction_status").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Idempotent upsert into `SalesRevenueCustomerAnalytics.gold.fact_sales`
# MAGIC MERGE on `sales_key` (the unique transaction id) — re-runs and incremental
# MAGIC loads simply refresh changed rows.

# COMMAND ----------

from pyspark.sql import functions as F

if spark.catalog.tableExists(TABLES["gold_fact_sales"]):
    fact_prep.createOrReplaceTempView("u")
    spark.sql(f"""
        MERGE INTO {TABLES["gold_fact_sales"]} t
        USING u
        ON t.sales_key = u.sales_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("fact_sales: MERGE upsert applied")
else:
    fact_prep.write.saveAsTable(TABLES["gold_fact_sales"])
    print("fact_sales: table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Optimize for dashboard queries
# MAGIC Z-ORDER on the two most-used join/filter columns (date + customer) →
# MAGIC massive pruning on date-range dashboard queries (doc §27).

# COMMAND ----------

spark.sql(f"OPTIMIZE {TABLES['gold_fact_sales']} ZORDER BY (date_key, customer_key)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify — does the model reconcile?
# MAGIC A quick sanity check of the star schema: no orphan keys, sane profit margins,
# MAGIC and revenue trends over time.

# COMMAND ----------


display(spark.sql(f"""SELECT
  COUNT(*)                         AS rows,
  COUNT(DISTINCT sales_key)        AS distinct_sales,
  COUNT(DISTINCT customer_key)     AS customers,
  COUNT(DISTINCT product_key)      AS products,
  COUNT(DISTINCT region_key)       AS regions,
  ROUND(SUM(net_sales), 2)         AS net_revenue_usd,
  ROUND(SUM(profit_amount), 2)     AS profit_usd,
  ROUND(SUM(profit_amount) / SUM(net_sales) * 100, 2) AS margin_pct
FROM {GOLD}.fact_sales;"""))
# COMMAND ----------


display(spark.sql(f"""SELECT d.year, d.month,
       ROUND(SUM(f.net_sales), 2)          AS revenue_usd,
       ROUND(SUM(f.profit_amount), 2)      AS profit_usd,
       COUNT(DISTINCT f.order_id)          AS orders,
       COUNT(DISTINCT f.customer_key)      AS customers
FROM {GOLD}.fact_sales f
JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - **As-of SCD2 joins** — historical attribution done right
# MAGIC - **Profit model** — cost from the product version valid at sale time
# MAGIC - **MERGE upsert** — fact table is idempotent under re-runs and increments
# MAGIC - **OPTIMIZE ZORDER** — a concrete performance-engineering step
# MAGIC
# MAGIC **Next:** `06_Data_Quality_Audit` — the pipeline-wide DQ/audit framework.
