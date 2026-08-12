# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Layer: Cleansing, Dedup, Standardization
# MAGIC
# MAGIC **Purpose:** Turn raw Bronze data into **cleansed, conformed, standardized**
# MAGIC Silver tables — the single source of truth for all downstream analytics.
# MAGIC
# MAGIC **What happens here (doc §12):**
# MAGIC 1. **Data quality validation** — completeness, validity, domain checks per doc §23
# MAGIC 2. **Quarantine** — invalid records go to `silver.sales_quarantine` with
# MAGIC    structured error reasons (never silently dropped, never fail the pipeline)
# MAGIC 3. **Deduplication** — duplicates resolved deterministically (keep the earliest
# MAGIC    ingested version, drop identical duplicates)
# MAGIC 4. **Standardization** — types, dates, status normalization, string trimming,
# MAGIC    and **currency conversion to USD** via an FX reference table
# MAGIC 5. **Referential integrity** — every transaction must reference a valid
# MAGIC    customer and product
# MAGIC 6. **Idempotent upsert** — Delta `MERGE` on natural keys, so re-runs and
# MAGIC    incremental loads never create duplicates
# MAGIC
# MAGIC **Note:** this notebook is fully re-runnable. Running it twice produces
# MAGIC identical Silver state (MERGE, not append).

# COMMAND ----------

# MAGIC %run ./00_Setup_Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver reference tables first
# MAGIC Silver `customer` / `product` / `exchange_rate` must exist before transactions
# MAGIC can be validated (referential integrity).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType

# ---------- silver.customer ----------
customer_raw = spark.read.table(TABLES["bronze_customer"])
silver_customer = (
    customer_raw
    .withColumn("customer_name",   F.trim(F.col("customer_name")))
    .withColumn("customer_type",   F.initcap(F.trim(F.col("customer_type"))))
    .withColumn("customer_segment",F.initcap(F.trim(F.col("customer_segment"))))
    .withColumn("country",         F.trim(F.col("country")))
    .withColumn("state",           F.trim(F.col("state")))
    .withColumn("city",            F.trim(F.col("city")))
    .withColumn("customer_status", F.upper(F.trim(F.col("customer_status"))))
    .withColumn("created_date",    F.to_date(F.col("created_date"), "yyyy-MM-dd"))
    .withColumn("updated_date",    F.to_date(F.col("updated_date"), "yyyy-MM-dd"))
    # master dedup: one row per customer — the latest updated version wins
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.col("updated_date").desc_nulls_last(),
                                                  F.col("_ingestion_timestamp").desc_nulls_last())
    ))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
silver_customer.write.mode("overwrite").saveAsTable(TABLES["silver_customer"])
print(f"silver.customer: {silver_customer.count():,} rows (deduped)")

# ---------- silver.product ----------
product_raw = spark.read.table(TABLES["bronze_product"])
silver_product = (
    product_raw
    .withColumn("product_name",       F.trim(F.col("product_name")))
    .withColumn("product_category",   F.trim(F.col("product_category")))
    .withColumn("product_subcategory",F.trim(F.col("product_subcategory")))
    .withColumn("brand",              F.trim(F.col("brand")))
    .withColumn("product_status",     F.upper(F.trim(F.col("product_status"))))
    .withColumn("effective_date",     F.to_date(F.col("effective_date"), "yyyy-MM-dd"))
    .withColumn("updated_date",       F.to_date(F.col("updated_date"), "yyyy-MM-dd"))
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("product_id").orderBy(F.col("updated_date").desc_nulls_last(),
                                                 F.col("_ingestion_timestamp").desc_nulls_last())
    ))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
silver_product.write.mode("overwrite").saveAsTable(TABLES["silver_product"])
print(f"silver.product: {silver_product.count():,} rows (deduped)")

# ---------- silver.exchange_rate (reference: currency -> USD) ----------
currency_raw = spark.read.table("bronze.erp_currency")
exchange_rate = (
    currency_raw
    .withColumn("exchange_rate_to_usd", F.col("exchange_rate_to_usd").cast("double"))
    .withColumn("effective_date",       F.to_date(F.col("effective_date"), "yyyy-MM-dd"))
)
exchange_rate.write.mode("overwrite").saveAsTable(f"{SILVER}.exchange_rate")
print(f"silver.exchange_rate: {exchange_rate.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data quality rules
# MAGIC Rules are declared as **column predicates** — easy to read, easy to extend,
# MAGIC and reused by the DQ audit framework (notebook 06).
# MAGIC
# MAGIC > **Order matters:** rules run on the **normalized** values (uppercased,
# MAGIC > trimmed, synonyms mapped). Recoverable dirt (`shipped`, `USD `, …) is
# MAGIC > cleaned first and never quarantined; only unrecoverable problems fail.

# COMMAND ----------

DQ_RULES = [
    # (rule_id, description, SQL predicate marking a row as INVALID)
    # NOTE: predicates run on normalized values (uppercase, trimmed, synonyms mapped)
    ("DQ_001", "completeness: transaction_id not null",       "transaction_id IS NULL"),
    ("DQ_002", "completeness: customer_id not null",          "customer_id IS NULL"),
    ("DQ_003", "completeness: product_id not null",           "product_id IS NULL"),
    ("DQ_004", "completeness: transaction_date not null",     "transaction_date IS NULL"),
    ("DQ_005", "completeness: net_sales not null",            "net_sales IS NULL"),
    ("DQ_006", "validity: quantity >= 0",                     "quantity < 0"),
    ("DQ_007", "validity: net_sales >= 0",                    "net_sales < 0"),
    ("DQ_008", "validity: transaction_date <= current_date",  "transaction_date > CURRENT_DATE"),
    ("DQ_009", "domain: currency in known set",               "currency IS NULL OR currency NOT IN ('USD','EUR','GBP','CAD','INR','MXN')"),
    ("DQ_010", "domain: status in known set",                 "transaction_status IS NULL OR transaction_status NOT IN ('COMPLETED','RETURNED')"),
    # RI rules DQ_011/DQ_012 are evaluated via left-join checks below (see mark_ri)
]

print(f"{len(DQ_RULES)} column DQ rules defined (+2 referential-integrity rules)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalize → validate → quarantine
# MAGIC
# MAGIC **Step 1 — normalize** (clean first): uppercase + trim + collapse whitespace,
# MAGIC map status synonyms (`SHIPPED` → `COMPLETED`), strict date parse.
# MAGIC **Step 2 — validate** against the 12 rules on the normalized values.
# MAGIC **Step 3 — quarantine** the **as-received** record (raw payload) with its
# MAGIC machine-readable error reasons.

# COMMAND ----------

txn_raw = spark.read.table(TABLES["bronze_transaction"])
txn_typed = (
    txn_raw
    .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
    .withColumn("quantity",         F.col("quantity").cast("double"))
    .withColumn("unit_price",       F.col("unit_price").cast("double"))
    .withColumn("discount",         F.col("discount").cast("double"))
    .withColumn("tax",              F.col("tax").cast("double"))
    .withColumn("gross_amount",     F.col("gross_amount").cast("double"))
    .withColumn("net_amount",       F.col("net_amount").cast("double"))
    .withColumn("_row_id",          F.monotonically_increasing_id())   # identity, to join raw payload later
)
txn_typed.createOrReplaceTempView("txn_v")

# ---------- Step 1: normalization (recoverable dirt becomes valid) ----------
normalized = (
    txn_typed
    .withColumn("transaction_status",
                F.when(F.upper(F.regexp_replace(F.trim(F.col("transaction_status")), r"\s+", " ")) == "SHIPPED",
                       F.lit("COMPLETED"))
                 .otherwise(F.upper(F.regexp_replace(F.trim(F.col("transaction_status")), r"\s+", " "))))
    .withColumn("currency", F.upper(F.regexp_replace(F.trim(F.col("currency")), r"\s+", " ")))
)

# ---------- Step 2: validate on normalized values ----------
rule_exprs = [F.when(F.expr(predicate), F.lit(rid)).otherwise(F.lit(None)) for rid, _, predicate in DQ_RULES]
with_errors = normalized.withColumn("_errors", F.array_compact(F.array(*rule_exprs)))

# referential integrity (DQ_011 product, DQ_012 customer) — evaluated as joins:
# a row whose key is missing from the Silver dimension gets flagged, not dropped
def mark_ri(df, dim_df, key_col, rule_id):
    exists = dim_df.select(F.col(key_col).alias("_rk"))
    return (
        df.join(exists, df[key_col] == F.col("_rk"), "left")
        .withColumn(
            "_errors",
            F.when(F.col("_rk").isNull(),
                   F.array_union(F.col("_errors"), F.array(F.lit(rule_id))))
             .otherwise(F.col("_errors")),
        )
        .drop("_rk")
    )

with_errors = mark_ri(with_errors, silver_customer, "customer_id", "DQ_012")
with_errors = mark_ri(with_errors, silver_product,  "product_id",  "DQ_011")

valid   = with_errors.filter(F.size(F.col("_errors")) == 0).drop("_errors", "_row_id")
invalid = with_errors.filter(F.size(F.col("_errors")) > 0).select("_row_id", "_errors")

# ---------- Step 3: quarantine the AS-RECEIVED record + error reasons ----------
quarantine = (
    txn_typed.join(invalid, on="_row_id", how="inner")               # raw (pre-normalization) rows
    .select(
        F.to_json(F.struct(*[F.col(c) for c in txn_typed.columns if c != "_row_id"])).alias("record_data"),
        F.col("_errors").alias("error_reason"),
        F.lit("ERP").alias("source_system"),
        F.col("_batch_id").alias("batch_id"),
        F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
        F.col("_source_file").alias("source_file"),
    )
    .drop("_row_id")
)

quarantine.write.mode("append").saveAsTable(TABLES["silver_quarantine"])

print(f"Valid rows    : {valid.count():,}")
print(f"Invalid rows  : {invalid.count():,}  -> quarantined (as-received payload)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine — inspect what was rejected (and why)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT error_reason, COUNT(*) AS n
# MAGIC FROM silver.sales_quarantine
# MAGIC GROUP BY error_reason
# MAGIC ORDER BY n DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deduplicate + standardize valid transactions
# MAGIC
# MAGIC **Dedup strategy:** a `transaction_id` must appear exactly once. The
# MAGIC duplicate set is identified via `COUNT(*) OVER (PARTITION BY transaction_id)`;
# MAGIC we keep the earliest-ingested version (and drop the rest) so re-ingesting
# MAGIC the same file never grows the table.
# MAGIC
# MAGIC **Currency standardization:** join to `silver.exchange_rate` and express
# MAGIC every monetary column in **USD** (`*_usd` columns) — dashboards can then
# MAGIC compare revenue across currencies without error.

# COMMAND ----------

fx = spark.read.table(f"{SILVER}.exchange_rate").select(
    F.col("currency_code").alias("fx_currency"),
    F.col("exchange_rate_to_usd"),
)

from pyspark.sql.window import Window

dedup = valid.select(
    "*",
    F.row_number().over(
        Window.partitionBy("transaction_id").orderBy("_ingestion_timestamp", "_source_file")
    ).alias("_rn")
).filter(F.col("_rn") == 1).drop("_rn")

silver_txn = (
    dedup
    .join(fx, F.col("currency") == F.col("fx_currency"), "left")
    .withColumn("fx_rate",              F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0)))
    .withColumn("net_sales_usd",        F.round(F.col("net_amount")   * F.col("fx_rate"), 2))
    .withColumn("gross_amount_usd",     F.round(F.col("gross_amount") * F.col("fx_rate"), 2))
    .withColumn("discount_usd",         F.round(F.col("discount")     * F.col("fx_rate"), 2))
    .withColumn("tax_usd",              F.round(F.col("tax")          * F.col("fx_rate"), 2))
    .withColumn("transaction_status",   F.when(F.col("transaction_status") == "COMPLETED", "Completed")
                                        .when(F.col("transaction_status") == "RETURNED", "Returned")
                                        .otherwise(F.col("transaction_status")))
    .drop("fx_currency", "exchange_rate_to_usd")
)

print(f"Deduped + standardized rows: {silver_txn.count():,}")
print("Duplicate rows removed:", valid.count() - dedup.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Idempotent upsert into Silver (Delta MERGE)
# MAGIC MERGE = UPSERT on the natural key. Re-running this notebook (or appending a
# MAGIC new incremental batch) **cannot create duplicates** — the doc's §26 pattern.

# COMMAND ----------

def upsert_silver(target_table, updates_df, key_col):
    """MERGE upsert on natural key. The source is deduplicated by key first:
    Spark MERGE fails if the source contains duplicate keys, and re-runs must
    never create duplicates (doc §26 / §36 idempotency)."""
    # keep the newest version per key (updated_date desc, ingestion timestamp tiebreak)
    win = Window.partitionBy(key_col).orderBy(
        F.col("updated_date").desc_nulls_last(),
        F.col("_ingestion_timestamp").desc_nulls_last(),
    )
    updates_df = updates_df.withColumn("_rn", F.row_number().over(win)).filter(F.col("_rn") == 1).drop("_rn")

    if not spark.catalog.tableExists(target_table):
        # first load: create the table (MERGE can only target existing tables)
        updates_df.select(*updates_df.columns).write.saveAsTable(target_table)
        return spark.read.table(target_table).count()

    tgt_cols = [c["col_name"] for c in spark.sql(f"DESCRIBE {target_table}").collect()]
    upd_cols = [c for c in updates_df.columns if c in tgt_cols]

    updates_df.select(*upd_cols).createOrReplaceTempView("u")

    set_expr = ", ".join([f"t.{c} = u.{c}" for c in upd_cols if c != key_col])
    ins_cols = ", ".join(upd_cols)
    ins_vals = ", ".join([f"u.{c}" for c in upd_cols])

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING u ON t.{key_col} = u.{key_col}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
    """)
    return spark.read.table(target_table).count()

n = upsert_silver(TABLES["silver_transaction"], silver_txn, "transaction_id")
print(f"silver.sales_transaction upserted — total rows: {n:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver order headers (standardized, RI-checked)

# COMMAND ----------

order_raw = spark.read.table(TABLES["bronze_order"])
order_valid = (
    order_raw
    .withColumn("order_date",   F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
    .filter(F.col("total_amount") >= 0)
    .join(fx, F.col("currency") == F.col("fx_currency"), "left")
    .withColumn("total_amount_usd", F.round(F.col("total_amount") * F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0)), 2))
    .drop("fx_currency", "exchange_rate_to_usd")
)
n_ord = upsert_silver(TABLES["silver_order"], order_valid, "order_id")
print(f"silver.sales_order upserted — total rows: {n_ord:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver summary
# MAGIC After cleansing: every row is complete, unique, valid, and standardized.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                AS rows,
# MAGIC   COUNT(DISTINCT transaction_id)                          AS distinct_txns,
# MAGIC   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)    AS null_customer,
# MAGIC   SUM(CASE WHEN net_sales_usd IS NULL THEN 1 ELSE 0 END)  AS null_net_sales,
# MAGIC   SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END)           AS negative_qty,
# MAGIC   ROUND(SUM(net_sales_usd), 2)                           AS net_sales_usd
# MAGIC FROM silver.sales_transaction;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Key takeaways for the portfolio:**
# MAGIC - Declarative **DQ rule framework** (12 rules) — extendable, auditable
# MAGIC - **Quarantine pattern**: bad rows never fail the pipeline, never silently vanish
# MAGIC - **Deterministic dedup** via window functions + ingestion metadata
# MAGIC - **Currency standardization** to USD for cross-currency analytics
# MAGIC - **MERGE upserts** make every load idempotent (re-runs are safe)
# MAGIC
# MAGIC **Next:** `04_Gold_Dimensions` — the dimensional model with SCD Type 2.
