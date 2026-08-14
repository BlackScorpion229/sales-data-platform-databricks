# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Revenue Analytics (Executive Sales Dashboard)
# MAGIC
# MAGIC **Purpose:** The doc's §20 "Executive Sales Dashboard" as notebook
# MAGIC visualizations. In a full workspace this would be a Databricks SQL /
# MAGIC Power BI dashboard; the queries below are the exact semantic definitions.
# MAGIC
# MAGIC **Sections:** KPI cards → revenue trend → region/category/segment breakdowns
# MAGIC → top customers/products → actual vs budget.

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. KPI cards (doc §20)

# COMMAND ----------


display(spark.sql(f"""-- Revenue, orders, customers, AOV, profit — for the last complete month
WITH last_month AS (
  SELECT MAX(d.year_month) AS ym
  FROM {GOLD}.fact_sales f JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
  WHERE d.calendar_date <= CURRENT_DATE
),
m AS (
  SELECT d.year_month,
         SUM(f.net_sales)      AS revenue,
         SUM(f.profit_amount)  AS profit,
         COUNT(DISTINCT f.order_id) AS orders,
         COUNT(DISTINCT f.customer_key) AS customers
  FROM {GOLD}.fact_sales f
  JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
  WHERE d.year_month = (SELECT ym FROM last_month)
  GROUP BY d.year_month
),
prev AS (
  SELECT SUM(f.net_sales) AS revenue
  FROM {GOLD}.fact_sales f
  JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
  WHERE d.year_month = date_format(add_months(to_date((SELECT ym || '-01' FROM last_month)), -1), 'yyyy-MM')
)
SELECT
  ROUND(m.revenue, 0)                                   AS total_revenue,
  ROUND((m.revenue - p.revenue) / p.revenue * 100, 2)   AS revenue_growth_pct,
  m.orders                                              AS total_orders,
  m.customers                                           AS total_customers,
  ROUND(m.revenue / m.orders, 2)                        AS avg_order_value,
  ROUND(m.profit, 0)                                    AS profit
FROM m CROSS JOIN prev p;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Revenue trend (daily → monthly view)

# COMMAND ----------


display(spark.sql(f"""SELECT d.year_month AS month,
       ROUND(SUM(f.net_sales), 0)     AS revenue,
       ROUND(SUM(f.profit_amount), 0) AS profit,
       COUNT(DISTINCT f.order_id)     AS orders
FROM {GOLD}.fact_sales f
JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
GROUP BY d.year_month
ORDER BY d.year_month;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Revenue by region (doc §20)

# COMMAND ----------


display(spark.sql(f"""SELECT r.region_name,
       ROUND(SUM(f.net_sales), 0) AS revenue,
       ROUND(SUM(f.net_sales) * 100.0 / SUM(SUM(f.net_sales)) OVER (), 2) AS share_pct
FROM {GOLD}.fact_sales f
JOIN {GOLD}.dim_region r ON f.region_key = r.region_key
GROUP BY r.region_name
ORDER BY revenue DESC;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Revenue by product category + segment (doc §20)

# COMMAND ----------


display(spark.sql(f"""SELECT p.product_category,
       ROUND(SUM(f.net_sales), 0) AS revenue,
       SUM(f.quantity)            AS units
FROM {GOLD}.fact_sales f
JOIN {GOLD}.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_category
ORDER BY revenue DESC;"""))
# COMMAND ----------


display(spark.sql(f"""SELECT c.customer_segment,
       ROUND(SUM(f.net_sales), 0) AS revenue,
       COUNT(DISTINCT f.customer_key) AS customers
FROM {GOLD}.fact_sales f
JOIN {GOLD}.dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.customer_segment
ORDER BY revenue DESC;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top 10 customers / products (doc §20)

# COMMAND ----------


display(spark.sql(f"""SELECT c.customer_name,
       ROUND(SUM(f.net_sales), 2) AS revenue,
       COUNT(DISTINCT f.order_id) AS orders
FROM {GOLD}.fact_sales f
JOIN {GOLD}.dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.customer_name
ORDER BY revenue DESC
LIMIT 10;"""))
# COMMAND ----------


display(spark.sql(f"""SELECT p.product_name,
       ROUND(SUM(f.net_sales), 2) AS revenue,
       SUM(f.quantity)            AS units
FROM {GOLD}.fact_sales f
JOIN {GOLD}.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Actual vs budget (doc §2)
# MAGIC Monthly target comparison straight from the budget mart.

# COMMAND ----------


display(spark.sql(f"""SELECT b.year_month,
       b.budget_usd,
       ROUND(COALESCE(a.revenue, 0), 0)                 AS actual,
       ROUND(COALESCE(a.revenue, 0) - b.budget_usd, 0)  AS variance,
       ROUND((COALESCE(a.revenue, 0) - b.budget_usd) / b.budget_usd * 100, 2) AS variance_pct
FROM {GOLD}.budget_monthly b
LEFT JOIN (
  SELECT d.year_month, SUM(f.net_sales) AS revenue
  FROM {GOLD}.fact_sales f JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
  GROUP BY d.year_month
) a ON b.year_month = a.year_month
ORDER BY b.year_month;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways:**
# MAGIC - Window functions for **share %** without self-joins
# MAGIC - **MoM growth** via `add_months` on the last complete month
# MAGIC - The same SQL ports 1:1 into Databricks SQL / Power BI semantic models
