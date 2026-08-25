# Sales Revenue & Customer Analytics — Databricks Lakehouse

A production-style **Data Engineering portfolio project** implementing the
"Sales Revenue & Customer Analytics" solution on Databricks (Community
Edition–compatible), following the **Medallion Architecture**:

```
ERP + Transaction Systems ──► Bronze ──► Silver ──► Gold ──► BI Dashboards
 (synthetic CSV/JSON)         raw      cleansed   star       revenue +
                              (UC       + DQ +     schema     customer
                             volume)    FX USD    + SCD2      analytics
```

Everything runs under one Unity Catalog `SalesRevenueCustomerAnalyticsKNK` with
schemas `bronze` / `silver` / `gold` and a `sales_data` UC volume that hosts the
raw-file landing zone (serverless-compatible).

---

## What this project demonstrates

| Capability | Where | Concept |
|------------|-------|---------|
| Auto Loader incremental ingestion + schema evolution + audit columns | `02_Ingest_Bronze` | Auto Loader, checkpoints |
| Declarative data-quality framework (12 rules) + quarantine | `03_Silver_Cleansing` | DQ dimensions, MERGE |
| Deterministic dedup + currency standardization to USD | `03_Silver_Cleansing` | Standardization |
| Generic **SCD Type 2** engine + as-of historical attribution | `04_Gold_Dimensions` | SCD2, dimensional modeling |
| Star schema: `fact_sales` + 5 conformed dimensions | `04`/`05` | Facts & dimensions |
| Idempotent Delta `MERGE` pipelines (re-runnable) | `03`/`05` | Idempotency |
| DQ audit table + source↔gold **reconciliation** | `06_Data_Quality_Audit` | Reconciliation |
| Dashboard-ready aggregates (daily/monthly/customer/product/budget) | `07_Aggregates` | Marts |
| Executive revenue analytics (KPIs, MoM growth, actual vs budget) | `08_Analysis_Revenue` | BI metrics |
| Customer analytics (acquisition, retention, Pareto, at-risk) | `09_Analysis_Customers` | Segmentation |
| **Incremental load + SCD2 demo** (the "next day" simulation) | `10_Incremental_Demo` | Incremental proof |

---

## Project structure

```
sales-data-platform/
├── notebooks/                    # Databricks notebooks (import as source)
│   ├── 00_Setup_Workspace.py     # UC catalog, schemas, volume, config
│   ├── 01_Generate_Synthetic_ERP_Data.py   # simulated ERP source system
│   ├── 02_Ingest_Bronze.py       # Auto Loader → bronze.* (+audit columns)
│   ├── 02a_Ingested_Bronze_data_disply.py  # Bronze inspection utility
│   ├── 03_Silver_Cleansing.py    # DQ rules, quarantine, dedup, FX→USD
│   ├── 04_Gold_Dimensions.py     # SCD2 dims: customer, product, date, region, rep
│   ├── 05_Gold_Fact_Sales.py     # fact_sales (as-of SCD2 joins, ZORDER)
│   ├── 06_Data_Quality_Audit.py  # DQ metrics + reconciliation
│   ├── 07_Aggregates.py          # gold.agg_* marts + budget table
│   ├── 08_Analysis_Revenue.py    # executive dashboard queries
│   ├── 09_Analysis_Customers.py  # customer analytics queries
│   └── 10_Incremental_Demo.py    # next-day batch: incremental + SCD2 proof
├── src/sales_platform/           # production package (logic mirrored from notebooks)
│   ├── config.py                 # table names, paths, thresholds
│   ├── dq_rules.py               # declarative DQ registry + helpers
│   └── scd2.py                   # generic SCD2 engine
├── tests/                        # pytest suite — runs locally, no Spark needed
│   ├── test_config.py
│   └── test_dq_rules.py
├── scripts/
│   └── generate_raw_data_local.py  # local mirror of notebook 01 (no Spark)
├── resources/jobs/
│   └── sales_platform_workflow.yml   # Asset Bundle job definition (12 tasks)
├── docs/
│   └── data_dictionary.md        # source-to-target mapping
├── databricks.yml                # Asset Bundle: dev/qa/prod targets
├── pyproject.toml
├── README.md
└── GUIDE.md
```

> **Full step-by-step run instructions and the Databricks / data-engineering
> concepts used (Medallion, Unity Catalog, Auto Loader, Delta, SCD2, DQ
> dimensions, reconciliation) are in `GUIDE.md`.** Read it before running.

---

## Quick start (Databricks)

1. Create a Community Edition cluster (DBR 12.2 LTS+, Single Node).
2. Import all `notebooks/*.py` into **one folder** in your Workspace.
3. Attach the cluster, then **Run All** in order: `00` → `01` → `02` → (optional `02a`) → `03` → `04` → `05` → `06` → `07` → `08` → `09`, then `10` last.
4. Use `10_Incremental_Demo` as the interview/demo showpiece (incremental + SCD2 + idempotency).

---

## Local (no Databricks)

```bash
# regenerate the deterministic dataset as plain CSV/JSON
python scripts/generate_raw_data_local.py

# unit-test the DQ registry / config / SCD2 logic (no Spark required)
pip install pytest
python -m pytest tests -q     # 15 tests
```

---

## Production deployment (Databricks Asset Bundles)

```bash
databricks bundle deploy --target dev     # dev → your user folder
databricks bundle deploy --target prod    # prod → /Shared
databricks bundle run sales_platform_workflow
```

Notebooks and the 12-task workflow (Auto Loader → Silver → Gold → analytics)
deploy together; `databricks.yml` defines `dev`/`qa`/`prod` targets. The
`src/sales_platform` package keeps the same logic testable outside Spark.

---

## Data at a glance (deterministic, seeded — deliberately NOT clean)

- 1,500 customers · 360 products · 6 categories · 12 regions · 60 sales reps
- 24 months of daily orders (2024-09-01 → 2026-08-11): **~78K orders / ~260K line items**, one CSV per day
- 8 currencies (FX-standardized to USD in Silver)
- **~2.3% of transaction rows fail ≥1 DQ rule** → quarantined:
  missing/orphan `customer_id`, unknown `product_id`, invalid status/currency,
  malformed dates, negative quantities, future dates
- **~1% exact duplicates** → deterministic dedup in Silver
- **~3.7% recoverable dirt** (lowercase/whitespace statuses & currencies, `shipped`→`Completed`) → **normalized, not rejected**
- Master data is dirty too: duplicate customer/product records, NULL city/price, case variants — cleansed in Silver before SCD2
- ~30 customers change attributes + 10 new customers in the update batch → SCD2

---

## Community Edition / serverless notes

- Serverless runs Unity Catalog → catalog `SalesRevenueCustomerAnalyticsKNK` with schemas `bronze`/`silver`/`gold` + volume `sales_data`; every reference is fully qualified (`catalog.schema.table`, `/Volumes/catalog/schema/...`).
- No SQL Warehouses/dashboards → notebook `display()` charts; the SQL queries port 1:1 to Databricks SQL / Power BI.
- No scheduled jobs → run notebooks manually; `resources/jobs/` YAML shows the production workflow wiring.
- Auto Loader runs in directory-listing mode with a batch fallback (`USE_AUTO_LOADER`).
- Serverless/Spark Connect gotchas handled in code: no session conf for Delta `autoMerge` (use write option `mergeSchema`), `input_file_name()` banned in UC (use `_metadata.file_path`), cloudFiles option keys lowercased (`cloudFiles.validateOptions=false`), no `StreamingQuery.awaitAnyTermination()` (use `awaitTermination()`), and partition directory values must be URL-safe.

---

## Requirements traceability

Implements solution requirements doc v1.0 sections: 5 (architecture), 7–9 (source schemas), 10 (ingestion), 11–13 (layers), 14–15 (gold model), 16–19 (metrics/segmentation), 23–24 (DQ), 26 (incremental), 27 (performance), 38 (SCD), 40 (reconciliation), 41 (deliverables).
