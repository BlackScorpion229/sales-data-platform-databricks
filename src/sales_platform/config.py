"""Sales Revenue & Customer Analytics — shared configuration.

Mirrors notebook 00 (single source of truth for the notebook flow).
Pure Python: importable locally (tests) and on Databricks clusters.
"""

from __future__ import annotations

CATALOG = "hive_metastore"  # Unity Catalog would be: "sales_catalog"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

RAW_BASE = "/FileStore/raw_data"
RAW_CUSTOMER = f"{RAW_BASE}/erp/customer"
RAW_PRODUCT = f"{RAW_BASE}/erp/product"
RAW_TRANSACT = f"{RAW_BASE}/transactions"
RAW_ORDERS = f"{RAW_BASE}/orders"
RAW_CUSTOMER_UPDATES = f"{RAW_BASE}/erp/customer_updates"
CHECKPOINT_BASE = f"{RAW_BASE}/_checkpoints"

TABLES = {
    "bronze_customer": f"{BRONZE}.erp_customer",
    "bronze_product": f"{BRONZE}.erp_product",
    "bronze_transaction": f"{BRONZE}.sales_transaction",
    "bronze_order": f"{BRONZE}.sales_order",
    "bronze_region": f"{BRONZE}.erp_region",
    "bronze_sales_rep": f"{BRONZE}.erp_sales_rep",
    "bronze_currency": f"{BRONZE}.erp_currency",
    "silver_customer": f"{SILVER}.customer",
    "silver_product": f"{SILVER}.product",
    "silver_transaction": f"{SILVER}.sales_transaction",
    "silver_order": f"{SILVER}.sales_order",
    "silver_exchange_rate": f"{SILVER}.exchange_rate",
    "silver_quarantine": f"{SILVER}.sales_quarantine",
    "gold_dim_customer": f"{GOLD}.dim_customer",
    "gold_dim_product": f"{GOLD}.dim_product",
    "gold_dim_date": f"{GOLD}.dim_date",
    "gold_dim_region": f"{GOLD}.dim_region",
    "gold_dim_sales_rep": f"{GOLD}.dim_sales_rep",
    "gold_fact_sales": f"{GOLD}.fact_sales",
    "gold_dq_audit": f"{GOLD}.data_quality_audit",
    "gold_agg_revenue_daily": f"{GOLD}.agg_revenue_daily",
    "gold_agg_customer_monthly": f"{GOLD}.agg_customer_monthly",
    "gold_customer_segmentation": f"{GOLD}.customer_segmentation",
    "gold_agg_product": f"{GOLD}.agg_product",
    "gold_budget_monthly": f"{GOLD}.budget_monthly",
}

# Business constants (doc §18) — keep configurable, never hard-coded in SQL
SEGMENT_THRESHOLDS = {"high": 100_000, "low": 25_000}
BUDGET_DAILY = 60_000
FX_DIM_DATE_START = "2024-01-01"
FX_DIM_DATE_END = "2027-12-31"
RECONCILIATION_TOLERANCE_USD = 0.01

# DQ severity buckets used by the audit framework (doc §23/§24)
DQ_BUCKETS = {
    "completeness": ("DQ_001", "DQ_002", "DQ_003", "DQ_004", "DQ_005"),
    "validity": ("DQ_006", "DQ_007", "DQ_008"),
    "domain": ("DQ_009", "DQ_010"),
    "referential_integrity": ("DQ_011", "DQ_012"),
}


def validate() -> None:
    """Cheap sanity checks — run at import time in tests and CI."""
    assert all(v.startswith(f"{CATALOG}.") for v in TABLES.values())
    assert SEGMENT_THRESHOLDS["low"] < SEGMENT_THRESHOLDS["high"]
    assert set(DQ_BUCKETS) == {"completeness", "validity", "domain", "referential_integrity"}
