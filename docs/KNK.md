# KNK — Conversation Log

**Project:** Sales Revenue & Customer Analytics — Databricks Data Engineering Portfolio Project
**Format:** Full Q&A conversation (code excluded)
**Date:** August 2026

---

## 1. Opening

**User:** hi

**Assistant:** Greeting — offered help with whatever the user wanted to work on.

---

## 2. Document analysis

**User:** analyse this document "Sales Revenue & Customer Analytics"

**Assistant:** Extracted the text from the `.docx` requirements document and analyzed it:

- It is a **Databricks Data Engineering Solution Requirements document** (v1.0, August 2026) for a Sales Revenue & Customer Analytics platform.
- **Core design:** ERP + Transaction Systems → Bronze → Silver → Gold → BI dashboards, built on the Lakehouse + Medallion Architecture with Delta Lake and Unity Catalog.
- **Coverage:** source schemas (customer, product, transaction), initial + incremental ingestion, Bronze audit columns, Silver cleansing rules, a Gold star schema (fact_sales + dim_customer/product/date/region), business metrics, customer segmentation, three dashboards, a data-quality framework with a quarantine pattern, SCD Type 2 dimensions, reconciliation vs ERP, monitoring/alerting, SLAs, three environments with CI/CD via Databricks Asset Bundles, seven implementation phases, and success metrics.
- **Key observations:** strong on architecture and requirements but thin on concrete technical details (no volumes, no catalog naming, no column-level types); some ambiguities (SCD2 on product, unquantified reconciliation tolerance, no explicit dedup winner); a clear future path to ML (forecasting, churn, CLV).

---

## 3. The project request

**User:** I want to do this project to showcase my databricks skills, so do step by step help to me.

**Assistant:** Asked four scoping questions:

1. **Databricks environment** → answer: **Community Edition**
2. **Implementation style** → answer: **Notebooks**
3. **Source data** → answer: **None — generate synthetic ERP data**
4. **Skill level** → answer: **Advanced**

**Assistant:** Mapped the requirements onto Community Edition constraints:

- No Unity Catalog → `hive_metastore` schemas `bronze`/`silver`/`gold` instead
- No SQL Warehouses/Dashboards → notebook `display()` charts + documented Power BI path
- No scheduled Jobs/Workflows → manual notebook runs + a Workflow YAML for later
- Auto Loader works on CE (directory-listing mode; verified via docs) with a batch fallback included anyway

---

## 4. Roadmap

**Assistant:** Laid out an 8-step roadmap:

| Step | Deliverable | Skills showcased |
|------|-------------|------------------|
| 1 | Workspace setup + project structure | Schemas, DBFS paths, config-as-code |
| 2 | Synthetic ERP data generator | Spark generation, realistic distributions, injected DQ issues |
| 3 | Bronze layer | Auto Loader, audit columns, schema evolution |
| 4 | Silver layer | Cleansing, dedup, quarantine |
| 5 | Gold dimensions | SCD Type 2, calendar, surrogate keys |
| 6 | Fact + aggregates | Incremental MERGE, metrics, DQ audit |
| 7 | Analytics notebooks | Revenue/customer visualizations |
| 8 | Incremental demo + docs | Idempotency proof, README, dictionary |

Then built the entire project folder `sales-data-platform/` with 11 Databricks notebooks (00–10), each covering a medallion stage, plus supporting documentation.

---

## 5. Building the platform

**Assistant:** Created and verified, step by step:

- **Notebook 00** — workspace setup: creates the three schemas, central config (table names, paths, thresholds), landing zone.
- **Notebook 01** — synthetic ERP data generator: ~1,500 customers (Pareto-weighted revenue), ~360 products, ~78K orders, ~260K transaction line items over 24 months, one CSV file per day, plus region/sales-rep/currency reference files and a customer-updates file for SCD2 demos.
- **Notebook 02** — Bronze ingestion via Auto Loader with audit columns (ingestion timestamp, source file, source system, batch id, record hash), schema evolution, Delta history/time travel.
- **Notebook 03** — Silver cleansing: 12 declarative DQ rules, quarantine table, deterministic dedup, currency standardization to USD, idempotent MERGE upserts.
- **Notebook 04** — Gold dimensions: generic SCD Type 2 engine, calendar dimension with fiscal fields, deterministic surrogate keys.
- **Notebook 05** — fact_sales with as-of SCD2 joins, profit calculation, OPTIMIZE + ZORDER.
- **Notebook 06** — DQ audit framework + reconciliation (bronze ≈ silver + quarantined).
- **Notebook 07** — dashboard-ready aggregates and a budget table.
- **Notebooks 08/09** — revenue and customer analytics queries (KPIs, trends, retention, Pareto, at-risk).
- **Notebook 10** — incremental demo: simulates the next business day and proves incremental ingestion, SCD2 versioning, and idempotency.

Along the way several real bugs were found and fixed during review: MERGE targets that don't exist yet on first run, ambiguous column names in the SCD2 join, source-key duplication before MERGE, a stray invalid line, and rule-registry unpacking errors — all verified via a headless generator test and a syntax checker across all files.

---

## 6. Git + production-grade upgrade

**User:** yes

**Assistant:** (responding to the offer of git init and/or production conversion) Did both:

- **Git**: initialized the repo, added `.gitignore`, made the initial commit.
- **Databricks Asset Bundles**: added `databricks.yml` with dev/QA/prod targets and converted the workflow YAML to a DAB-native 10-task job definition.
- **Python package**: `src/sales_platform/` with shared modules (config, DQ rule registry, generic SCD2 engine) mirroring the notebook logic as a single source of truth.
- **Tests**: `tests/` with 15 pytest tests that run locally without Spark — all passing. The test suite even caught a real bug (wrong tuple shapes in the rule registry) during the session.
- Updated README with the new structure and deployment commands; committed.

---

## 7. Raw data must not be clean

**User:** first please be careful and check once again — the raw data generated is not a clean data — as I have to perform the cleaning of data in silver stage the raw data should not be a clean ready-to-use data set.

**Assistant:** Audited the generator against that requirement and found genuine gaps:

- **"Shipped" rows were being quarantined**, which meant Silver's "Shipped → Completed" normalization code was dead — the pipeline treated recoverable formatting dirt as fatal.
- **No formatting dirt at all** — statuses and currencies were perfectly cased, so standardization had nothing to do.
- **No orphan customer IDs, no duplicates in the customer/product masters**, and overall dirt rates were too subtle to demo.

**Fixed (generator + pipeline):**

- **Unrecoverable dirt (~2.3% of rows)** → quarantined: NULL/orphan `customer_id`, unknown `product_id`, invalid statuses (`On Hold`, blank), malformed dates (`2024-09-01 08:15:00`), invalid currency (`XYZ`), negative quantities, future dates.
- **Recoverable dirt (~3.7% of rows)** → normalized in Silver, never rejected: lowercase/whitespace status variants (`shipped`, `completed `), lowercase currencies (`usd`, `gbp`), status synonyms.
- **Deduplication (~1%)**: exact duplicate rows.
- **Master-data dirt**: duplicate customer/product records, NULL city/price, case variants, ~1,566 messy order statuses.

**Pipeline redesign:** *normalize first, then validate* — rules now run on normalized values (uppercase, trimmed, synonyms mapped), DQ_009/DQ_010 also catch blanks, and the **quarantine table stores the as-received record** (joined back by row identity) with per-rule error reasons.

**Verification:** a headless simulation of the entire raw-data → DQ pipeline proved every rule fires with exactly the intended counts (e.g., DQ_004 malformed dates: 1,543; DQ_012 orphan customers: 1,803; DQ_009 invalid currency: 1,030) — 5,933 rows (2.28%) flagged overall, plus master-data dedup cases. Docs were updated, all tests still pass (15/15), and the changes were committed.

---

## 8. This file

**User:** ok good, now can you create a "knk.md" file with this full conversation (without the code) and should include my questions and your responses. make it formatted as markdown file.

**Assistant:** Created `knk.md` — this document.
