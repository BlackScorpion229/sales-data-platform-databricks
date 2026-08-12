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

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                   AS total_customers,
# MAGIC   SUM(CASE WHEN customer_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN is_new_customer THEN customer_key END) AS new_customers_last_month,
# MAGIC   COUNT(DISTINCT CASE WHEN NOT is_new_customer AND revenue_usd > 0 THEN customer_key END) AS returning_customers,
# MAGIC   ROUND(SUM(revenue_usd) /
# MAGIC         NULLIF(COUNT(DISTINCT CASE WHEN revenue_usd > 0 THEN customer_key END), 0), 2) AS avg_revenue_per_customer
# MAGIC FROM gold.agg_customer_monthly
# MAGIC WHERE year_month = (SELECT MAX(year_month) FROM gold.agg_customer_monthly
# MAGIC                     WHERE year_month < date_format(CURRENT_DATE, 'yyyy-MM'));

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer acquisition trend (new customers per month)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year_month,
# MAGIC        COUNT(DISTINCT CASE WHEN is_new_customer THEN customer_key END) AS new_customers,
# MAGIC        COUNT(DISTINCT customer_key)                                     AS total_active_customers
# MAGIC FROM gold.agg_customer_monthly
# MAGIC GROUP BY year_month
# MAGIC ORDER BY year_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. New vs existing revenue split + revenue trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year_month,
# MAGIC        CASE WHEN is_new_customer THEN 'New' ELSE 'Existing' END AS cohort,
# MAGIC        ROUND(SUM(revenue_usd), 0) AS revenue
# MAGIC FROM gold.agg_customer_monthly
# MAGIC GROUP BY year_month, cohort
# MAGIC ORDER BY year_month, cohort;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Retention — % of last month's buyers who bought again this month
# MAGIC Classic repeat-purchase retention: rolling month-over-month behavior.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH buyers AS (
# MAGIC   SELECT DISTINCT customer_key, year_month
# MAGIC   FROM gold.agg_customer_monthly
# MAGIC   WHERE revenue_usd > 0
# MAGIC ),
# MAGIC joined AS (
# MAGIC   SELECT b.year_month,
# MAGIC          COUNT(DISTINCT b.customer_key)                                    AS buyers,
# MAGIC          COUNT(DISTINCT CASE WHEN p.customer_key IS NOT NULL THEN b.customer_key END) AS retained
# MAGIC   FROM buyers b
# MAGIC   LEFT JOIN buyers p
# MAGIC     ON b.customer_key = p.customer_key
# MAGIC    AND p.year_month = date_format(add_months(to_date(b.year_month || '-01'), -1), 'yyyy-MM')
# MAGIC   GROUP BY b.year_month
# MAGIC )
# MAGIC SELECT year_month,
# MAGIC        buyers,
# MAGIC        retained,
# MAGIC        ROUND(retained * 100.0 / buyers, 2) AS retention_pct
# MAGIC FROM joined
# MAGIC ORDER BY year_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Purchase frequency (orders per customer)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH freq AS (
# MAGIC   SELECT customer_key, COUNT(DISTINCT order_id) AS orders
# MAGIC   FROM gold.fact_sales
# MAGIC   GROUP BY customer_key
# MAGIC )
# MAGIC SELECT CASE
# MAGIC          WHEN orders = 1 THEN '1 (one-off)'
# MAGIC          WHEN orders <= 3 THEN '2-3'
# MAGIC          WHEN orders <= 10 THEN '4-10'
# MAGIC          ELSE '10+' END AS frequency_band,
# MAGIC        COUNT(*) AS customers,
# MAGIC        MIN(orders) AS min_orders
# MAGIC FROM freq
# MAGIC GROUP BY frequency_band
# MAGIC ORDER BY min_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Segmentation mix + revenue concentration (doc §18, §2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_segment_value,
# MAGIC        COUNT(*)                          AS customers,
# MAGIC        ROUND(SUM(lifetime_revenue), 0)  AS lifetime_revenue,
# MAGIC        ROUND(SUM(lifetime_revenue) * 100.0 / SUM(SUM(lifetime_revenue)) OVER (), 2) AS share_pct
# MAGIC FROM gold.customer_segmentation
# MAGIC GROUP BY customer_segment_value
# MAGIC ORDER BY lifetime_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pareto check: what % of revenue comes from the top 20% of customers?
# MAGIC WITH ranked AS (
# MAGIC   SELECT customer_key,
# MAGIC          lifetime_revenue,
# MAGIC          ROW_NUMBER() OVER (ORDER BY lifetime_revenue DESC) AS rn,
# MAGIC          COUNT(*) OVER () AS n_customers
# MAGIC   FROM gold.customer_segmentation
# MAGIC )
# MAGIC SELECT ROUND(SUM(lifetime_revenue) * 100.0 /
# MAGIC               (SELECT SUM(lifetime_revenue) FROM gold.customer_segmentation), 2) AS top20_revenue_share_pct
# MAGIC FROM ranked
# MAGIC WHERE rn <= n_customers * 0.2;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. At-risk customers — no purchase in the last 180 days (doc §2)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH last_purchase AS (
# MAGIC   SELECT customer_key, MAX(d.calendar_date) AS last_order_date
# MAGIC   FROM gold.fact_sales f
# MAGIC   JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC   GROUP BY customer_key
# MAGIC )
# MAGIC SELECT
# MAGIC   COUNT(*) AS at_risk_customers,
# MAGIC   ROUND(SUM(c.lifetime_revenue), 0) AS revenue_at_risk
# MAGIC FROM last_purchase lp
# MAGIC JOIN gold.customer_segmentation c USING (customer_key)
# MAGIC WHERE lp.last_order_date <= date_add(CURRENT_DATE, -180);

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways:**
# MAGIC - Self-join on month buckets = **retention rate** without complex math
# MAGIC - **Pareto analysis** with window functions — the "top-20% concentration" story
# MAGIC - At-risk detection — directly actionable for sales teams
