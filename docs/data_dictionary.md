# Data Dictionary & Source-to-Target Mapping

Medallion mapping (doc §7–§15): every Bronze table maps 1:1 to a source file;
Silver standardizes; Gold conforms to the star schema.

## Source files (simulated ERP exports) → Bronze

| Source file | Bronze table | Notes |
|-------------|--------------|-------|
| `erp/customer/` | `bronze.erp_customer` | +`erp/customer_updates/` ingested incrementally |
| `erp/product/` | `bronze.erp_product` | |
| `orders/` | `bronze.sales_order` | order headers |
| `transactions/dt=YYYY-MM-DD/` | `bronze.sales_transaction` | one file per day |
| `erp/region/` | `bronze.erp_region` | |
| `erp/sales_rep/` | `bronze.erp_sales_rep` | |
| `erp/currency/` | `bronze.erp_currency` | FX reference |

**Bronze audit columns** (added on every table):
`_ingestion_timestamp` · `_source_file` · `_source_system` · `_batch_id` · `_record_hash` (sha256 of all payload columns).

## Bronze → Silver

| Silver table | Transformation summary |
|--------------|------------------------|
| `silver.customer` | trim, upper status, date casts, **dedup by customer_id** (latest `updated_date` wins) |
| `silver.product` | trim, upper status, date casts, dedup by product_id |
| `silver.exchange_rate` | typed FX rates (currency → USD) |
| `silver.sales_transaction` | DQ validation → quarantine; dedup by transaction_id (earliest ingested wins); status normalization (`Shipped`→`Completed`); **`*_usd` columns** via FX join; MERGE upsert |
| `silver.sales_order` | date cast, status upper, `total_amount >= 0`, `total_amount_usd`; MERGE upsert |
| `silver.sales_quarantine` | `record_data` (JSON), `error_reason` (array), `source_system`, `batch_id`, `ingestion_timestamp`, `source_file` |

**DQ rule registry (doc §23)** — rules run on **normalized** values
(uppercase, trimmed, synonyms mapped): recoverable dirt is cleaned first and
never rejected; unrecoverable problems fail:

| ID | Rule |
|----|------|
| DQ_001–005 | completeness: transaction_id, customer_id, product_id, transaction_date, net_sales not null |
| DQ_006–008 | validity: quantity ≥ 0, net_sales ≥ 0, transaction_date ≤ current_date |
| DQ_009 | domain: currency ∈ {USD, EUR, GBP, CAD, INR, MXN} (NULL also fails) |
| DQ_010 | domain: status ∈ {COMPLETED, RETURNED} after normalization (NULL also fails) |
| DQ_011–012 | referential integrity: product / customer exists in Silver dims |

**Standardization before validation (Silver):**
- status: trim → collapse whitespace → uppercase → synonyms (`SHIPPED`→`COMPLETED`) → title-case canonical (`Completed`/`Returned`)
- currency: trim → uppercase (lowercase `usd`/`gbp` become valid)
- customer_type / customer_segment: `initcap` canonicalization
- dates: strict `to_date(…, 'yyyy-MM-dd')` — malformed formats → NULL → DQ_004

**Quarantine** stores the **as-received** (pre-normalization) record as JSON
plus the failing rule ids, source system, batch id and ingestion timestamp.

## Gold dimensional model (doc §14–§15)

| Table | Grain / key | Notes |
|-------|-------------|-------|
| `gold.dim_date` | `date_key` (yyyyMMdd) | 2024-01-01 → 2027-12-31; calendar + fiscal (April start), `is_weekend` |
| `gold.dim_customer` | `customer_key` (int, derived from id) | **SCD2**: `effective_start_date`, `effective_end_date`, `is_current` |
| `gold.dim_product` | `product_key` | **SCD2** same pattern |
| `gold.dim_region` | `region_key` | region, country, state, territory |
| `gold.dim_sales_rep` | `sales_rep_key` | rep, region link, status |
| `gold.fact_sales` | `sales_key` = transaction_id | as-of SCD2 joins; `net_sales`, `profit_amount` (= net − qty×cost); ZORDER(date_key, customer_key) |

**As-of SCD2 join** (doc §38): `transaction_date >= effective_start_date AND (effective_end_date IS NULL OR transaction_date < effective_end_date)`

## Gold analytical marts (doc §7 "Aggregations")

| Table | Grain | Columns of note |
|-------|-------|-----------------|
| `gold.agg_revenue_daily` | date × region × category × status | revenue/profit USD, units, orders, line_items |
| `gold.agg_customer_monthly` | customer × year-month | `is_new_customer` (first-purchase month) |
| `gold.customer_segmentation` | customer | lifetime revenue + `High/Medium/Low Value` (thresholds configurable) |
| `gold.agg_product` | product category × subcategory | revenue, units, avg selling price, contribution % |
| `gold.budget_monthly` | year-month | daily budget constant × days |
| `gold.data_quality_audit` | per pipeline run | received/processed/rejected/dupes/nulls/invalid/duration/status |

## Key business metrics (doc §16–§19) — definitions

| Metric | Definition |
|--------|------------|
| Total Revenue | `SUM(net_sales)` (USD) |
| Revenue Growth % | `(current − previous) / previous` (MoM, on last complete month) |
| Avg Order Value | `SUM(net_sales) / COUNT(DISTINCT order_id)` |
| Revenue per Customer | `SUM(net_sales) / COUNT(DISTINCT customer_key)` |
| Profit / Margin | `profit_amount`; margin = profit / net sales |
| Retention | % of prior-month buyers who purchased again this month |
| Segmentation | revenue thresholds: High > 100K; Medium 25–100K; Low < 25K (`SEGMENT_THRESHOLDS`) |
| At-risk customers | no purchase in last 180 days |
| Reconciliation | `bronze net ≈ silver net + quarantined net` within tolerance (0.01 USD) |
