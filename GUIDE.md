# GUIDE — Sales Revenue & Customer Analytics (Databricks Lakehouse)

A **production-style Data Engineering portfolio project** that builds an end-to-end
lakehouse for sales revenue and customer analytics, following the **Medallion
Architecture** on Databricks. It is designed to be:

- **Demo / lecture friendly** — every pipeline is explained with the underlying
  Databricks & data-engineering concept inline.
- **Run end-to-end on free Databricks Community Edition** (serverless, Unity Catalog).
- **Reproducible & testable** — deterministic synthetic data + a local Python port
  and a `pytest` suite that needs no Spark.

```
ERP + Transaction Systems ──► Bronze ──► Silver ──► Gold ──► BI Dashboards
 (synthetic CSV/JSON)         raw      cleansed   star       revenue +
                              (UC       + DQ +     schema     customer
                             volume)    FX USD    + SCD2      analytics
```

---

## 0. What this project demonstrates (capability → notebook)

| Capability | Where | Concept |
|------------|-------|---------|
| Unity Catalog setup, Medallion schemas, UC volume landing zone | `00_Setup_Workspace` | UC, Volumes, idempotent DDL |
| Synthetic ERP source with **deliberate** DQ dirt | `01_Generate_Synthetic_ERP_Data` | Source simulation, data profiling |
| Incremental ingestion + schema evolution + audit columns | `02_Ingest_Bronze` (+`02a` display) | **Auto Loader**, checkpoints |
| Declarative DQ framework (12 rules) + quarantine + dedup + FX to USD | `03_Silver_Cleansing` | DQ dimensions, MERGE, standardization |
| Generic **SCD Type 2** engine + conformed dimensions | `04_Gold_Dimensions` | SCD2, dimensional modeling |
| Star-schema `fact_sales` + as-of SCD2 joins + `OPTIMIZE`/`ZORDER` | `05_Gold_Fact_Sales` | Fact tables, as-of joins, ZORDER |
| DQ audit table + **source↔gold reconciliation** | `06_Data_Quality_Audit` | Reconciliation, audit trail |
| Dashboard-ready aggregates (daily/monthly/product/budget) | `07_Aggregates` | Aggregation marts |
| Executive revenue analytics (KPIs, MoM growth, actual vs budget) | `08_Analysis_Revenue` | BI metrics |
| Customer analytics (acquisition, retention, Pareto, at-risk) | `09_Analysis_Customers` | Segmentation, RFM-style |
| **Incremental load + SCD2 showpiece** (the "next day" simulation) | `10_Incremental_Demo` | Incremental + idempotency proof |

---

## 1. Databricks & Data-Engineering Concepts (for lecturing)

> This section is the talking track. Each concept is tied to a notebook so you
> can demonstrate it live.

### 1.1 Medallion Architecture
Raw → cleansed → business-ready, in three schemas:
- **Bronze** (`bronze.*`): data *as received* from source systems, plus ingestion
  metadata (`_ingestion_timestamp`, `_source_file`, `_batch_id`, `_record_hash`).
- **Silver** (`silver.*`): cleansed, standardized, deduplicated, conformed, with
  FX converted to USD and invalid rows quarantined.
- **Gold** (`gold.*`): dimensional model (star schema) + aggregates for BI.

*Why:* separates "land the data" from "fix the data" from "serve the data", so a
bad upstream change is isolated in Bronze and never silently corrupts dashboards.

### 1.2 Unity Catalog (UC) & Volumes
Everything lives under **one catalog** `SalesRevenueCustomerAnalyticsKNK`:
schemas `bronze`/`silver`/`gold` plus a `sales_data` schema hosting the
**`raw_data` UC volume** (the raw-file landing zone). All references are fully
qualified (`catalog.schema.table`, `/Volumes/catalog/schema/...`).

*Why UC:* central governance, fine-grained grants (BI users see only `gold`),
and lineage. *Why a Volume:* on serverless compute the public DBFS root
(`/FileStore`) is disabled, so raw files live in a governed volume instead.

### 1.3 Delta Lake
All tables are **Delta** tables. We use:
- **ACID `MERGE`** for idempotent upserts (Silver/Gold).
- **`OPTIMIZE ... ZORDER BY`** on `fact_sales` for query performance (`05`).
- **`DESCRIBE HISTORY`** / **time travel** on Bronze to show Delta's transaction log.

*Why:* Delta gives reliability (ACID), performance (ZORDER, file compaction) and
auditability (history) on top of object storage.

### 1.4 Auto Loader (incremental ingestion)
`02` and `10` use `cloudFiles` (Auto Loader) from the UC volume. A **checkpoint**
remembers which files were already ingested, so re-runs process **only new files**
— the heart of incremental ETL (concept: *exactly-once* via checkpoint + idempotent sink).

### 1.5 Data Quality framework (declarative)
12 rules grouped by DQ dimension (see data dictionary & `src/sales_platform/dq_rules.py`):
- **Completeness** DQ_001–005: required fields not null.
- **Validity** DQ_006–008: `quantity ≥ 0`, `net_amount ≥ 0`, date ≤ today.
- **Domain** DQ_009–010: currency ∈ known set, status ∈ {Completed, Returned}.
- **Referential integrity** DQ_011–012: product/customer exist in Silver dims.

Failing rows go to **`silver.sales_quarantine`** with the original JSON + error
reasons. **Recoverable dirt** (lowercase `usd`, `shipped`→`Completed`) is
*normalized, not rejected*; only **unrecoverable** problems are quarantined.

*Why:* quarantine (vs. dropping) keeps an auditable trail and lets you replay fixes.

### 1.6 Dimensional modeling & SCD Type 2
Gold uses a **star schema**: `fact_sales` surrounded by conformed dimensions
(`dim_customer`, `dim_product`, `dim_date`, `dim_region`, `dim_sales_rep`).

`dim_customer`/`dim_product` are **SCD Type 2**: when a tracked attribute changes
(region move, status flip), the old row is *closed* (`effective_end_date` set,
`is_current=false`) and a *new version* is opened. This preserves **history**.
`05` joins facts to dimensions **as-of** the transaction date, so revenue is
attributed to the customer's *then-current* attributes (demonstrated in `10_5c`).

### 1.7 Idempotency & Reconciliation
Every transform is a re-runnable `MERGE`/dedup — re-running changes nothing
(proven in `10_5d`). `06` reconciles **Bronze net ≈ Silver net + quarantined net**
within $0.01, proving no revenue was lost in cleansing.

---

## 2. Prerequisites

- A free **Databricks Community Edition** account: <https://databricks.com/product/community-edition>
- ~30–45 min of compute for the full run (CE's free serverless cluster is small).
- *(Optional, local)* Python 3.10+ to run `scripts/generate_raw_data_local.py` and `pytest`.

---

## 3. Step-by-step: run on Databricks (the demo path)

### Step 1 — Create the cluster
1. Sign in → sidebar **Compute** → **Create Compute**.
2. Name: `sales-analytics` · Policy: **Unrestricted** · Single Node.
3. Databricks Runtime: **12.2 LTS or newer**.
4. **Create** → wait for state **RUNNING**.

### Step 2 — Import the notebooks
1. Sidebar → **Workspace** → **+ New → Import** (or drag-and-drop the files).
2. Select **all** `.py` files from `notebooks/` (multi-select).
3. Recommended layout (all 10+ notebooks + `02a` in **one folder**):

```
/Workspace
└── sales-data-platform/
    ├── 00_Setup_Workspace
    ├── 01_Generate_Synthetic_ERP_Data
    ├── 02_Ingest_Bronze
    ├── 02a_Ingested_Bronze_data_disply
    ├── 03_Silver_Cleansing
    ├── 04_Gold_Dimensions
    ├── 05_Gold_Fact_Sales
    ├── 06_Data_Quality_Audit
    ├── 07_Aggregates
    ├── 08_Analysis_Revenue
    ├── 09_Analysis_Customers
    └── 10_Incremental_Demo
```

> ⚠️ Notebooks use relative `%run ./00_Setup_Workspace` — **all must live in the same folder**.

### Step 3 — Attach cluster & run in numeric order
Open a notebook → select the cluster → **Run All**. Run `00` → `01` → `02` → `02a` (optional) → `03` → `04` → `05` → `06` → `07` → `08` → `09`, then `10` last.

| # | Notebook | What you should see |
|---|----------|---------------------|
| 00 | Setup | Catalog + 3 schemas + UC volume landing zone created |
| 01 | Generate data | ~78K orders / ~260K txns, **~2.3% invalid + ~1% dupes + recoverable dirt injected** |
| 02 | Ingest Bronze | 4+ bronze tables + audit columns; try `DESCRIBE HISTORY bronze.sales_transaction` |
| 02a | Bronze display | quick `display()` of raw bronze rows (optional) |
| 03 | Silver cleansing | invalid → quarantine, dedup counts, FX→USD columns |
| 04 | Gold dimensions | 5 dims; customers `is_current=true` only (1 version) |
| 05 | Fact sales | `fact_sales` + `OPTIMIZE ZORDER` output |
| 06 | DQ audit | audit row + **RECONCILED** message |
| 07 | Aggregates | 5+ mart tables |
| 08 | Revenue analysis | charts: trend, region, category, top 10, budget |
| 09 | Customer analysis | charts: acquisition, retention, Pareto, at-risk |
| 10 | Incremental demo | new-batch delta, **SCD2 versions**, **IDEMPOTENT ✓** |

### Step 4 — The demo script (for interviews / lecturing)
Run `10_Incremental_Demo` last and narrate the concepts from §1:
1. **Baseline snapshot** — counts before the new batch.
2. **New batch** — 4 new daily files with fresh DQ issues + customer updates.
3. **Auto Loader** — Bronze grows by *only* the new rows (checkpoint = incremental).
4. **SCD2** — customers now have 2 versions; `10_5c` shows historical revenue
   attributed to the *old* region (as-of join).
5. **Idempotency** — re-running Silver changes nothing (`✓ IDEMPOTENT`).
6. **Audit table** — one new row per run with DQ metrics; `06` shows reconciliation.

---

## 4. Local (no Databricks) workflow

### 4.1 Generate the dataset locally
Faithful mirror of notebook `01` (same seed 42, same "today" 2026-08-11) writing
plain CSV/JSON into `data/sales_data/raw_data/`:
```bash
python scripts/generate_raw_data_local.py
```

### 4.2 Run the unit tests (CI-friendly, no Spark)
The `src/sales_platform` package keeps the DQ registry, config and SCD2 engine
testable outside Spark:
```bash
pip install pytest
python -m pytest tests -q     # 15 tests: config, DQ registry, thresholds
```
This proves the **business logic** is correct independent of the Databricks runtime.

### 4.3 Production deployment (Databricks Asset Bundles)
```bash
databricks bundle deploy --target dev      # dev → your user folder
databricks bundle deploy --target prod     # prod → /Shared
databricks bundle run sales_platform_workflow
```
Notebooks + a 12-task workflow (Auto Loader → Silver → Gold → analytics) deploy
together; `databricks.yml` defines `dev`/`qa`/`prod` targets.

---

## 5. Data at a glance (deterministic, seeded — deliberately NOT clean)

- 1,500 customers · 360 products · 6 categories · 12 regions · 60 sales reps
- 24 months of daily orders (2024-09-01 → 2026-08-11, ~710 daily files):
  **~78K orders / ~260K line items** (one CSV per day).
- 8 currencies (FX-standardized to USD in Silver).
- **~2.3% of transaction rows fail ≥1 DQ rule** → quarantined:
  missing/orphan `customer_id`, unknown `product_id`, invalid status/currency,
  malformed dates, negative quantities, future dates.
- **~1% exact duplicates** → deterministic dedup in Silver.
- **~3.7% recoverable dirt** (lowercase/whitespace statuses & currencies,
  `shipped`→`Completed`) → **normalized, not rejected**.
- Master data is dirty too: duplicate customer/product records, NULL city/price,
  case variants — cleansed in Silver before SCD2.
- ~30 customers change attributes + 10 new customers in the update batch → SCD2.

---

## 6. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `table or view not found: bronze.sales_transaction` | Run notebooks in order; `02` before `03` |
| Auto Loader error in `02` | Set `USE_AUTO_LOADER = False` in cell 1 → batch fallback |
| `%run ./00_Setup_Workspace not found` | All notebooks must be in the **same folder** |
| Cluster "detached" | Compute → restart cluster → re-attach |
| Out of memory on data generation | Runs on driver; reduce rows via `range(1500)` / `range(360)` in `01` |
| CSV partition column `dt` looks weird | Expected — `dt` is a directory, not a data column |
| Want a clean slate | `00` §5 flags: `PURGE_RAW_DATA` / `DROP_MEDALLION_SCHEMAS` / `DROP_RAW_VOLUME` (each defaults `False`) |

---

## 7. Leveling up (optional, paid workspace)

- **Unity Catalog grants**: grant BI users on `gold` only.
- **Databricks SQL dashboards**: paste `08`/`09` queries — they work 1:1.
- **Workflows**: `resources/jobs/sales_platform_workflow.yml` schedules the 12-step job.
- **Power BI**: DirectQuery to the gold tables.
- **Streaming**: `02`/`10` already use `availableNow` Auto Loader triggers — flip to
  continuous for true streaming.

---

## 8. Project structure (current)

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
├── src/sales_platform/           # production package (single source of truth for logic)
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
│   ├── data_dictionary.md        # source-to-target mapping
│   ├── data.json
│   └── KNK.md
├── data/sales_data/raw_data/     # git-ignored local/volume raw snapshot
├── databricks.yml                # Asset Bundle: dev/qa/prod targets
├── pyproject.toml
├── README.md
└── GUIDE.md
```

> For the full source-to-target mapping (every Bronze→Silver→Gold table, all 12 DQ
> rules, and metric definitions) see `docs/data_dictionary.md`.
