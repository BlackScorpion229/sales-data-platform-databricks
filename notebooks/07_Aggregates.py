# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Gold Aggregates (dashboard-ready marts)
# MAGIC
# MAGIC **Purpose:** Pre-aggregate the most-queried analytics so dashboards stay fast
# MAGIC (doc §7 "Aggregations"). These are **analytical marts**, not raw gold:
# MAGIC
# MAGIC | Table | Grain | Used by |
# MAGIC |-------|-------|---------|
# MAGIC | `agg_revenue_daily` | date × region × category × status | Revenue trends, region/category drill-downs |
# MAGIC | `agg_customer_monthly` | year-month × customer | Customer acquisition/retention, top customers |
# MAGIC | `customer_segmentation` | customer (lifetime) | Segmentation KPIs (doc §18) |
# MAGIC | `agg_product` | product (lifetime) | Product performance (doc §19) |
# MAGIC | `budget_monthly` | year-month | Actual vs budget (doc §2) |

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

from pyspark.sql import functions as F

fact = spark.read.table(TABLES["gold_fact_sales"])
date_dim = spark.read.table(TABLES["gold_dim_date"])
cust_dim = spark.read.table(TABLES["gold_dim_customer"]).filter("is_current = true")
prod_dim = spark.read.table(TABLES["gold_dim_product"]).filter("is_current = true")
reg_dim  = spark.read.table(TABLES["gold_dim_region"])
rep_dim  = spark.read.table(TABLES["gold_dim_sales_rep"])

f = (fact
     .join(date_dim.select("date_key", "year", "month", "year_month", "calendar_date"), "date_key")
     .join(reg_dim.select("region_key", "region_name", "territory"), "region_key")
     .join(prod_dim.select("product_key", "product_category", "product_subcategory"), "product_key")
     .join(cust_dim.select("customer_key", "customer_id", "customer_segment", "customer_type", "country"), "customer_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `agg_revenue_daily` — date × region × category × status

# COMMAND ----------

agg_daily = (
    f.groupBy(
        F.col("date_key"),
        F.col("calendar_date"),
        F.col("region_key"),
        F.col("region_name"),
        F.col("product_category"),
        F.col("transaction_status"),
    ).agg(
        F.sum("net_sales").alias("revenue_usd"),
        F.sum("profit_amount").alias("profit_usd"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("line_items"),
    )
)
agg_daily.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_revenue_daily")
print(f"gold.agg_revenue_daily: {agg_daily.count():,} rows")
agg_daily.orderBy(F.col("calendar_date").desc()).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `agg_customer_monthly` — customer × month
# MAGIC `is_new_customer` = first-ever purchase month (enables new vs existing split).

# COMMAND ----------

first_purchase = f.groupBy("customer_key").agg(F.min("calendar_date").alias("first_purchase_date"))

agg_customer_monthly = (
    f.groupBy(
        F.col("customer_key"),
        F.col("year_month"),
        F.col("customer_segment"),
        F.col("customer_type"),
        F.col("country"),
        F.col("region_key"),
        F.col("region_name"),
    ).agg(
        F.sum("net_sales").alias("revenue_usd"),
        F.sum("profit_amount").alias("profit_usd"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units_sold"),
        F.count("*").alias("line_items"),
    )
    .join(first_purchase, "customer_key")
    .withColumn("is_new_customer",
                F.col("year_month") == F.date_format("first_purchase_date", "yyyy-MM"))
)
agg_customer_monthly.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_customer_monthly")
print(f"gold.agg_customer_monthly: {agg_customer_monthly.count():,} rows")
agg_customer_monthly.orderBy(F.col("year_month").desc(), F.col("revenue_usd").desc()).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `customer_segmentation` — configurable thresholds (doc §18)
# MAGIC Thresholds come from `SEGMENT_THRESHOLDS` in the config — change once,
# MAGIC re-run, and every report re-buckets automatically.

# COMMAND ----------

lifetime = (
    f.groupBy("customer_key", "customer_id")
    .agg(F.sum("net_sales").alias("lifetime_revenue"))
    .join(cust_dim.select("customer_key", "customer_name"), "customer_key")
)

HI, LO = SEGMENT_THRESHOLDS["high"], SEGMENT_THRESHOLDS["low"]

segmentation = lifetime.withColumn(
    "customer_segment_value",
    F.when(F.col("lifetime_revenue") > HI, "High Value")
     .when(F.col("lifetime_revenue") > LO, "Medium Value")
     .otherwise("Low Value"),
)
segmentation.write.mode("overwrite").saveAsTable(f"{GOLD}.customer_segmentation")
print(f"gold.customer_segmentation: {segmentation.count():,} rows")
segmentation.orderBy(F.col("lifetime_revenue").desc()).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `agg_product` — lifetime product performance (doc §19)

# COMMAND ----------

agg_product = (
    f.groupBy("product_key", "product_category", "product_subcategory")
    .agg(
        F.sum("net_sales").alias("revenue_usd"),
        F.sum("profit_amount").alias("profit_usd"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("gross_amount_usd") / F.sum("quantity"), 2).alias("avg_selling_price"),
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("line_items"),
    )
    .withColumn("revenue_contribution_pct",
               F.round(F.col("revenue_usd") / F.sum("revenue_usd").over(Window.orderBy(F.lit(1))) * 100, 2))
)
agg_product.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_product")
print(f"gold.agg_product: {agg_product.count():,} rows")
agg_product.orderBy(F.col("revenue_usd").desc()).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `budget_monthly` — configurable target (doc §2 "actual vs target")
# MAGIC Daily budget constant × days in month → simple, adjustable monthly targets.

# COMMAND ----------

from pyspark.sql.window import Window

budget = (
    date_dim.filter(F.col("calendar_date") <= F.current_date())
    .groupBy("year_month")
    .agg(F.count("*").alias("days_in_month"))
    .withColumn("budget_usd", F.round(F.lit(BUDGET_DAILY) * F.col("days_in_month"), 2))
)
budget.write.mode("overwrite").saveAsTable(f"{GOLD}.budget_monthly")
budget.orderBy("year_month").show(6, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - **Mart design**: aggregate at the grain your dashboards actually query
# MAGIC - **Config-driven segmentation** (thresholds are parameters, not constants)
# MAGIC - Contribution % via window functions (no self-joins)
# MAGIC - Budget table makes actual-vs-target a simple join
# MAGIC
# MAGIC **Next:** `08_Analysis_Revenue` + `09_Analysis_Customers` — the dashboards.
