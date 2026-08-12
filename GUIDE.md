# GUIDE — Running the project on Databricks Community Edition

## Prerequisites

- A free **Databricks Community Edition** account: <https://databricks.com/product/community-edition>
- ~20–30 min of compute for the full run (the CE free cluster is small)

## Step 0 — Create the cluster

1. Sign in → sidebar **Compute** → **Create Compute**
2. Name: `sales-analytics` · Policy: **Unrestricted** · Single Node
3. Databricks Runtime: **12.2 LTS or newer** (any recent LTS works)
4. **Create** → wait for state **RUNNING** (first start takes a few minutes)

> Community Edition grants one free cluster. Keep it running while you run the
> notebooks, or accept the auto-termination and restart when needed.

## Step 1 — Import the notebooks

1. Sidebar → **Workspace** → **+ New → Import** (or drag-and-drop the files)
2. Select **all** `.py` files from `notebooks/` (multi-select)
3. Choose "Import as: **Source**" if prompted — each becomes a notebook
4. Recommended layout (creates the same folder structure as this repo):

```
/Workspace
└── sales-data-platform/
    ├── 00_Setup_Workspace
    ├── 01_Generate_Synthetic_ERP_Data
    ├── ...
    └── 10_Incremental_Demo
```

> ⚠️ **Important:** notebooks use relative `%run ./00_Setup_Workspace` — all
> 10 notebooks must live in the **same folder**.

## Step 2 — Attach cluster + run in order

Open a notebook → select the cluster from the dropdown → **Run All**.

| # | Notebook | Runtime (est.) | What you should see |
|---|----------|----------------|---------------------|
| 00 | Setup | < 1 min | 3 schemas created, landing zone ready |
| 01 | Generate data | 2–4 min | ~78K orders, ~260K txns — **~2.3% invalid + ~1% dupes + recoverable dirt injected** |
| 02 | Ingest Bronze | 1–3 min | 4 bronze tables + audit columns, `DESCRIBE HISTORY` |
| 03 | Silver cleansing | 2–4 min | invalid rows → quarantine, dedup counts |
| 04 | Gold dimensions | 1–2 min | 5 dims; all customers current (1 version) |
| 05 | Fact sales | 1–3 min | fact_sales + OPTIMIZE ZORDER output |
| 06 | DQ audit | < 1 min | audit row + **RECONCILED** message |
| 07 | Aggregates | 1–2 min | 5 mart tables |
| 08 | Revenue analysis | < 1 min | charts: trend, region, category, top 10, budget |
| 09 | Customer analysis | < 1 min | charts: acquisition, retention, Pareto, at-risk |
| 10 | Incremental demo | 3–5 min | counts delta, **SCD2 versions**, **IDEMPOTENT ✓** |

### Time-saving tips

- **Skip cells you've already run** — every notebook is safe to re-run
  (idempotent by design), but re-running 01 overwrites raw data, so:
  - `01` → run **once**, unless you want to reset the whole dataset
  - `02` → safe to re-run (Auto Loader skips known files via checkpoint)
  - `03/04/05/06` → safe and *recommended* to re-run after `10`
- The **execution time labels** are rough; CE's small cluster is the bottleneck.
- If a cell errors with a confusing message, run `%restart_python` then **Run All** again.

## Step 3 — The demo script (for interviews)

Run `10_Incremental_Demo` last and narrate:

1. **Baseline snapshot** — counts before the new batch
2. **New batch** — 4 new daily files with fresh DQ issues + customer updates
3. **Auto Loader** — bronze grows by only the new rows (checkpoint = incremental)
4. **SCD2** — customers now have 2 versions; 5c shows historical revenue
   attributed to the *old* region
5. **Idempotency** — re-running Silver changes nothing (`✓ IDEMPOTENT`)
6. **Audit table** — one new row per pipeline run with DQ metrics

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `table or view not found: bronze.sales_transaction` | Run notebooks in order; 02 must complete before 03 |
| Auto Loader error in `02` | Set `USE_AUTO_LOADER = False` in cell 1 → batch fallback |
| `%run ./00_Setup_Workspace not found` | All notebooks must be in the **same folder** |
| Cluster "detached" | Compute → restart cluster → re-attach |
| Out of memory on data generation | It's fine — generation runs on the driver; reduce rows by editing `range(1500)` / `range(360)` / order counts in `01` |
| CSV partition column `dt` appears weird | Expected — `dt` is a directory, not a data column |

## Leveling up (optional, paid workspace)

- **Unity Catalog**: rename schemas → `sales_catalog.bronze/silver/gold` (SQL:
  `CREATE CATALOG sales_catalog`), grants for BI users on `gold` only
- **Databricks SQL dashboards**: paste the queries from `08`/`09` into SQL
  alerts/dashboards — they work unchanged
- **Workflows**: deploy `resources/jobs/sales_platform_workflow.yml` (via
  Databricks Asset Bundles or the UI) to schedule the 12-step job
- **Power BI**: connect DirectQuery to the gold tables
