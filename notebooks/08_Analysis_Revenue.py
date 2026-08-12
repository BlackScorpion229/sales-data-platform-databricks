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

# MAGIC %sql
# MAGIC -- Revenue, orders, customers, AOV, profit — for the last complete month
# MAGIC WITH last_month AS (
# MAGIC   SELECT MAX(d.year_month) AS ym
# MAGIC   FROM gold.fact_sales f JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC   WHERE d.calendar_date <= CURRENT_DATE
# MAGIC ),
# MAGIC m AS (
# MAGIC   SELECT d.year_month,
# MAGIC          SUM(f.net_sales)      AS revenue,
# MAGIC          SUM(f.profit_amount)  AS profit,
# MAGIC          COUNT(DISTINCT f.order_id) AS orders,
# MAGIC          COUNT(DISTINCT f.customer_key) AS customers
# MAGIC   FROM gold.fact_sales f
# MAGIC   JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC   WHERE d.year_month = (SELECT ym FROM last_month)
# MAGIC   GROUP BY d.year_month
# MAGIC ),
# MAGIC prev AS (
# MAGIC   SELECT SUM(f.net_sales) AS revenue
# MAGIC   FROM gold.fact_sales f
# MAGIC   JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC   WHERE d.year_month = date_format(add_months(to_date((SELECT ym || '-01' FROM last_month)), -1), 'yyyy-MM')
# MAGIC )
# MAGIC SELECT
# MAGIC   ROUND(m.revenue, 0)                                   AS total_revenue,
# MAGIC   ROUND((m.revenue - p.revenue) / p.revenue * 100, 2)   AS revenue_growth_pct,
# MAGIC   m.orders                                              AS total_orders,
# MAGIC   m.customers                                           AS total_customers,
# MAGIC   ROUND(m.revenue / m.orders, 2)                        AS avg_order_value,
# MAGIC   ROUND(m.profit, 0)                                    AS profit
# MAGIC FROM m CROSS JOIN prev p;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Revenue trend (daily → monthly view)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.year_month AS month,
# MAGIC        ROUND(SUM(f.net_sales), 0)     AS revenue,
# MAGIC        ROUND(SUM(f.profit_amount), 0) AS profit,
# MAGIC        COUNT(DISTINCT f.order_id)     AS orders
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.year_month
# MAGIC ORDER BY d.year_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Revenue by region (doc §20)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.region_name,
# MAGIC        ROUND(SUM(f.net_sales), 0) AS revenue,
# MAGIC        ROUND(SUM(f.net_sales) * 100.0 / SUM(SUM(f.net_sales)) OVER (), 2) AS share_pct
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_region r ON f.region_key = r.region_key
# MAGIC GROUP BY r.region_name
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Revenue by product category + segment (doc §20)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.product_category,
# MAGIC        ROUND(SUM(f.net_sales), 0) AS revenue,
# MAGIC        SUM(f.quantity)            AS units
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY p.product_category
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.customer_segment,
# MAGIC        ROUND(SUM(f.net_sales), 0) AS revenue,
# MAGIC        COUNT(DISTINCT f.customer_key) AS customers
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC WHERE c.is_current = true
# MAGIC GROUP BY c.customer_segment
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top 10 customers / products (doc §20)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.customer_name,
# MAGIC        ROUND(SUM(f.net_sales), 2) AS revenue,
# MAGIC        COUNT(DISTINCT f.order_id) AS orders
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC WHERE c.is_current = true
# MAGIC GROUP BY c.customer_name
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.product_name,
# MAGIC        ROUND(SUM(f.net_sales), 2) AS revenue,
# MAGIC        SUM(f.quantity)            AS units
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY p.product_name
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Actual vs budget (doc §2)
# MAGIC Monthly target comparison straight from the budget mart.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT b.year_month,
# MAGIC        b.budget_usd,
# MAGIC        ROUND(COALESCE(a.revenue, 0), 0)                 AS actual,
# MAGIC        ROUND(COALESCE(a.revenue, 0) - b.budget_usd, 0)  AS variance,
# MAGIC        ROUND((COALESCE(a.revenue, 0) - b.budget_usd) / b.budget_usd * 100, 2) AS variance_pct
# MAGIC FROM gold.budget_monthly b
# MAGIC LEFT JOIN (
# MAGIC   SELECT d.year_month, SUM(f.net_sales) AS revenue
# MAGIC   FROM gold.fact_sales f JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC   GROUP BY d.year_month
# MAGIC ) a ON b.year_month = a.year_month
# MAGIC ORDER BY b.year_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways:**
# MAGIC - Window functions for **share %** without self-joins
# MAGIC - **MoM growth** via `add_months` on the last complete month
# MAGIC - The same SQL ports 1:1 into Databricks SQL / Power BI semantic models
