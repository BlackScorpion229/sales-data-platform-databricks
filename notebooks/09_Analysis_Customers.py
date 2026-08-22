# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Customer Analytics (doc §21)
# MAGIC
# MAGIC **Purpose:** Customer KPIs + acquisition/retention/segmentation analysis:
# MAGIC new vs existing customers, retention rate, purchase frequency, segmentation
# MAGIC mix and revenue concentration (the "are we healthy?" questions).

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customer KPIs

# COMMAND ----------


display(spark.sql(f"""SELECT
  COUNT(*)                                   AS total_customers,
  SUM(CASE WHEN c.customer_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_customers,
  COUNT(DISTINCT CASE WHEN is_new_customer THEN customer_key END) AS new_customers_last_month,
  COUNT(DISTINCT CASE WHEN NOT is_new_customer AND revenue_usd > 0 THEN customer_key END) AS returning_customers,
  ROUND(SUM(revenue_usd) /
        NULLIF(COUNT(DISTINCT CASE WHEN revenue_usd > 0 THEN customer_key END), 0), 2) AS avg_revenue_per_customer
FROM {GOLD}.agg_customer_monthly a
JOIN {GOLD}.dim_customer c ON c.customer_key = a.customer_key AND c.is_current = true
WHERE a.year_month = (SELECT MAX(year_month) FROM {GOLD}.agg_customer_monthly
                      WHERE year_month < date_format(CURRENT_DATE, 'yyyy-MM'));"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer acquisition trend (new customers per month)

# COMMAND ----------


display(spark.sql(f"""SELECT year_month,
       COUNT(DISTINCT CASE WHEN is_new_customer THEN customer_key END) AS new_customers,
       COUNT(DISTINCT customer_key)                                     AS total_active_customers
FROM {GOLD}.agg_customer_monthly
GROUP BY year_month
ORDER BY year_month;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. New vs existing revenue split + revenue trend

# COMMAND ----------


display(spark.sql(f"""SELECT year_month,
       CASE WHEN is_new_customer THEN 'New' ELSE 'Existing' END AS cohort,
       ROUND(SUM(revenue_usd), 0) AS revenue
FROM {GOLD}.agg_customer_monthly
GROUP BY year_month, cohort
ORDER BY year_month, cohort;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Retention — % of last month's buyers who bought again this month
# MAGIC Classic repeat-purchase retention: rolling month-over-month behavior.

# COMMAND ----------


display(spark.sql(f"""WITH buyers AS (
  SELECT DISTINCT customer_key, year_month
  FROM {GOLD}.agg_customer_monthly
  WHERE revenue_usd > 0
),
joined AS (
  SELECT b.year_month,
         COUNT(DISTINCT b.customer_key)                                    AS buyers,
         COUNT(DISTINCT CASE WHEN p.customer_key IS NOT NULL THEN b.customer_key END) AS retained
  FROM buyers b
  LEFT JOIN buyers p
    ON b.customer_key = p.customer_key
   AND p.year_month = date_format(add_months(to_date(b.year_month || '-01'), -1), 'yyyy-MM')
  GROUP BY b.year_month
)
SELECT year_month,
       buyers,
       retained,
       ROUND(retained * 100.0 / buyers, 2) AS retention_pct
FROM joined
ORDER BY year_month;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Purchase frequency (orders per customer)

# COMMAND ----------


display(spark.sql(f"""WITH freq AS (
  SELECT customer_key, COUNT(DISTINCT order_id) AS orders
  FROM {GOLD}.fact_sales
  GROUP BY customer_key
)
SELECT CASE
         WHEN orders = 1 THEN '1 (one-off)'
         WHEN orders <= 3 THEN '2-3'
         WHEN orders <= 10 THEN '4-10'
         ELSE '10+' END AS frequency_band,
       COUNT(*) AS customers,
       MIN(orders) AS min_orders
FROM freq
GROUP BY frequency_band
ORDER BY min_orders;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Segmentation mix + revenue concentration (doc §18, §2)

# COMMAND ----------


display(spark.sql(f"""SELECT customer_segment_value,
       COUNT(*)                          AS customers,
       ROUND(SUM(lifetime_revenue), 0)  AS lifetime_revenue,
       ROUND(SUM(lifetime_revenue) * 100.0 / SUM(SUM(lifetime_revenue)) OVER (), 2) AS share_pct
FROM {GOLD}.customer_segmentation
GROUP BY customer_segment_value
ORDER BY lifetime_revenue DESC;"""))
# COMMAND ----------


display(spark.sql(f"""-- Pareto check: what % of revenue comes from the top 20% of customers?
WITH ranked AS (
  SELECT customer_key,
         lifetime_revenue,
         ROW_NUMBER() OVER (ORDER BY lifetime_revenue DESC) AS rn,
         COUNT(*) OVER () AS n_customers
  FROM {GOLD}.customer_segmentation
)
SELECT ROUND(SUM(lifetime_revenue) * 100.0 /
              (SELECT SUM(lifetime_revenue) FROM {GOLD}.customer_segmentation), 2) AS top20_revenue_share_pct
FROM ranked
WHERE rn <= n_customers * 0.2;"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. At-risk customers — no purchase in the last 180 days (doc §2)

# COMMAND ----------


display(spark.sql(f"""WITH last_purchase AS (
  SELECT customer_key, MAX(d.calendar_date) AS last_order_date
  FROM {GOLD}.fact_sales f
  JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
  GROUP BY customer_key
)
SELECT
  COUNT(*) AS at_risk_customers,
  ROUND(SUM(c.lifetime_revenue), 0) AS revenue_at_risk
FROM last_purchase lp
JOIN {GOLD}.customer_segmentation c USING (customer_key)
WHERE lp.last_order_date <= date_add(CURRENT_DATE, -180);"""))
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways:**
# MAGIC - Self-join on month buckets = **retention rate** without complex math
# MAGIC - **Pareto analysis** with window functions — the "top-20% concentration" story
# MAGIC - At-risk detection — directly actionable for sales teams
