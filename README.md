# Sales Revenue & Customer Analytics — Databricks Lakehouse

A production-style **Data Engineering portfolio project** implementing the
"Sales Revenue & Customer Analytics" solution requirements on Databricks
(Community Edition–compatible), following the **Medallion Architecture**:

```
ERP + Transaction Systems ──► Bronze ──► Silver ──► Gold ──► BI Dashboards
```

## What this project demonstrates

| Capability | Where |
|------------|-------|
| Auto Loader incremental ingestion + schema evolution | `02_Ingest_Bronze` |
| Declarative data-quality framework (12 rules) + quarantine | `03_Silver_Cleansing` |
| Deterministic dedup + currency standardization to USD | `03_Silver_Cleansing` |
| Generic **SCD Type 2** engine + as-of historical attribution | `04_Gold_Dimensions` |
| Star schema: `fact_sales` + 5 conformed dimensions | `04/05` |
| Idempotent Delta `MERGE` pipelines (re-runnable) | `03/05` |
| DQ audit table + source↔gold **reconciliation** | `06_Data_Quality_Audit` |
| Dashboard-ready aggregates (daily/monthly/customer/product/budget) | `07_Aggregates` |
| Executive revenue analytics (KPIs, MoM growth, actual vs budget) | `08_Analysis_Revenue` |
| Customer analytics (acquisition, retention, Pareto, at-risk) | `09_Analysis_Customers` |
| **Incremental load + SCD2 demo** (the "next day" simulation) | `10_Incremental_Demo` |

## Project structure

```
sales-data-platform/
├── notebooks/                    # Databricks notebooks (import as source)
│   ├── 00_Setup_Workspace.py     # schemas, config, landing zone
│   ├── 01_Generate_Synthetic_ERP_Data.py   # simulated ERP source system
│   ├── 02_Ingest_Bronze.py       # Auto Loader → bronze.* (+audit columns)
│   ├── 03_Silver_Cleansing.py    # DQ rules, quarantine, dedup, standardization
│   ├── 04_Gold_Dimensions.py     # SCD2 dims: customer, product, date, region, rep
│   ├── 05_Gold_Fact_Sales.py     # fact_sales (as-of SCD2 joins, ZORDER)
│   ├── 06_Data_Quality_Audit.py  # DQ metrics + reconciliation
│   ├── 07_Aggregates.py          # gold.agg_* marts + budget table
│   ├── 08_Analysis_Revenue.py    # executive dashboard queries
│   ├── 09_Analysis_Customers.py  # customer analytics queries
│   └── 10_Incremental_Demo.py    # next-day batch: incremental + SCD2 proof
├── src/sales_platform/           # production package (single source of truth)
│   ├── config.py                 # table names, paths, thresholds (mirrors 00)
│   ├── dq_rules.py               # declarative DQ registry + helpers (mirrors 03)
│   └── scd2.py                   # generic SCD2 engine (mirrors 04)
├── tests/                        # pytest suite — runs locally, no Spark needed
│   ├── test_config.py
│   └── test_dq_rules.py
├── resources/jobs/
│   └── sales_platform_workflow.yml   # Asset Bundle job definition (10 tasks)
├── databricks.yml                # Asset Bundle: dev/qa/prod targets
├── pyproject.toml
├── docs/
│   └── data_dictionary.md        # source-to-target mapping
└── README.md
```

## Production deployment (Databricks Asset Bundles)

```bash
databricks bundle deploy --target dev     # dev → your user folder
databricks bundle deploy --target prod    # prod → /Shared
databricks bundle run sales_platform_workflow
```

Notebooks and the 10-task workflow (Auto Loader → Silver → Gold → analytics)
deploy together; `databricks.yml` defines `dev`/`qa`/`prod` targets per doc §30.
The `src/sales_platform` package keeps the same logic testable outside Spark.

## Local tests (CI-friendly, no Databricks needed)

```bash
pip install pytest
python -m pytest tests -q     # 15 tests: config, DQ registry, thresholds
```

## Run order (one-time setup)

1. `00_Setup_Workspace` — creates `bronze`/`silver`/`gold` schemas + DBFS landing zone
2. `01_Generate_Synthetic_ERP_Data` — writes realistic ERP exports to
   `/FileStore/raw_data` (CSV files plus `erp/region` as JSON — deliberate
   multi-format landing zone, with DQ issues)
3. `02_Ingest_Bronze` → 4. `03_Silver_Cleansing` → 5. `04_Gold_Dimensions`
4. `05_Gold_Fact_Sales` → 6. `06_Data_Quality_Audit` → 7. `07_Aggregates`
5. `08_Analysis_Revenue` + `09_Analysis_Customers` — visualizations
6. `10_Incremental_Demo` — the incremental/SCD2 showpiece

> Full step-by-step instructions (cluster setup, notebook import, screenshots
> guide): see `GUIDE.md`

## Data at a glance (deterministic, seeded — deliberately NOT clean)

- 1,500 customers · 360 products · 6 categories (24 months of daily orders:
  ~78K orders / ~260K line items, one CSV per day)
- 8 currencies (FX-standardized to USD in Silver)
- **~2.3% of transaction rows fail ≥1 DQ rule** → quarantined with error reasons:
  missing/orphan `customer_id`, unknown `product_id`, invalid status/currency,
  malformed dates, negative quantities, future dates
- **~1% exact duplicates** → deterministic dedup in Silver
- **~3.7% recoverable dirt** (lowercase/whitespace statuses & currencies,
  `shipped`→`Completed`) → **normalized, not rejected**
- Master data is dirty too: duplicate customer/product records, NULL city/price,
  case variants — cleansed in Silver before SCD2
- ~30 customers change attributes + 10 new customers in the update batch → SCD2

## Community Edition notes

- Serverless CE runs Unity Catalog → catalog `SalesRevenueCustomerAnalytics` with
  schemas `bronze`/`silver`/`gold` + volume `sales_data`; every reference is fully
  qualified (`catalog.schema.table`, `/Volumes/catalog/schema/...`)
  (legacy shared CE: `hive_metastore` schemas instead)
- No SQL Warehouses/dashboards → notebook `display()` charts; the SQL queries
  port 1:1 to Databricks SQL / Power BI
- No scheduled jobs → run notebooks manually; `resources/jobs/` YAML shows the
  production workflow wiring
- Auto Loader used in directory-listing mode with a batch fallback
- Serverless/Spark Connect gotchas: no session conf for delta `autoMerge` (use
  write option `mergeSchema`), `input_file_name()` banned in UC (use
  `_metadata.file_path`), cloudFiles option keys get lowercased
  (`cloudFiles.validateOptions=false`), no `StreamingQuery.awaitAnyTermination()`
  (use `awaitTermination()`), and partition directory values must be URL-safe
  (time-suffixed `dt=` dirs break partition pruning with a `CAST_INVALID_INPUT`)

## Requirements traceability

Implements solution requirements doc v1.0 sections: 5 (architecture), 7–9
(source schemas), 10 (ingestion), 11–13 (layers), 14–15 (gold model), 16–19
(metrics/segmentation), 23–24 (DQ), 26 (incremental), 27 (performance),
38 (SCD), 40 (reconciliation), 41 (deliverables).
