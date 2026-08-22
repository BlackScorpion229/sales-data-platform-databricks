"""Declarative data-quality rule registry (doc §23).

The registry is pure data + pure functions, so it can be unit-tested locally
without Spark. The `apply_rules` helper (lazy pyspark import) mirrors the
validation cell in notebook 03.

Rule IDs:  DQ_001..005 completeness · DQ_006..008 validity ·
           DQ_009..010 domain · DQ_011..012 referential integrity
"""

from __future__ import annotations

from typing import Callable

# (rule_id, description, SQL predicate that marks a row INVALID)
# NOTE: predicates run on NORMALIZED values (uppercase, trimmed, synonyms mapped)
COLUMN_RULES = [
    ("DQ_001", "completeness: transaction_id not null", "transaction_id IS NULL"),
    ("DQ_002", "completeness: customer_id not null", "customer_id IS NULL"),
    ("DQ_003", "completeness: product_id not null", "product_id IS NULL"),
    ("DQ_004", "completeness: transaction_date not null", "transaction_date IS NULL"),
    ("DQ_005", "completeness: net_amount not null", "net_amount IS NULL"),
    ("DQ_006", "validity: quantity >= 0", "quantity < 0"),
    ("DQ_007", "validity: net_amount >= 0", "net_amount < 0"),
    ("DQ_008", "validity: transaction_date <= current_date", "transaction_date > CURRENT_DATE"),
    ("DQ_009", "domain: currency in known set",
     "currency IS NULL OR currency NOT IN ('USD','EUR','GBP','CAD','INR','MXN')"),
    ("DQ_010", "domain: status in known set",
     "transaction_status IS NULL OR transaction_status NOT IN ('COMPLETED','RETURNED')"),
]

# RI rules are evaluated as join checks against Silver dims, not SQL predicates
REFERENTIAL_RULES = [
    ("DQ_011", "referential integrity: product exists in silver.product"),
    ("DQ_012", "referential integrity: customer exists in silver.customer"),
]

ALL_RULES = COLUMN_RULES + REFERENTIAL_RULES

VALID_CURRENCIES = ("USD", "EUR", "GBP", "CAD", "INR", "MXN")
VALID_STATUSES = ("Completed", "Returned")

# recoverable dirt -> mapped to a canonical status BEFORE rules are evaluated
STATUS_SYNONYMS = {"SHIPPED": "COMPLETED"}


def rules_by_id(rule_ids: set[str]) -> list[tuple[str, str, str]]:
    """Return the (id, description, predicate) tuples for the requested ids."""
    index = {rid: (rid, desc, pred) for rid, desc, pred in COLUMN_RULES}
    index.update({rid: (rid, desc, "") for rid, desc in REFERENTIAL_RULES})
    return [index[rid] for rid in sorted(rule_ids)]


def apply_rules(df, rules: list[tuple[str, str, str]] | None = None):
    """Attach an `_errors` array column to a Spark DataFrame (mirrors notebook 03).

    Imported lazily so the module works in local pytest (no Spark required).
    """
    from pyspark.sql import functions as F  # noqa: PLC0415

    rules = rules or COLUMN_RULES
    rule_exprs = [F.when(F.expr(predicate), F.lit(rid)).otherwise(F.lit(None))
                  for rid, _, predicate in rules]
    return df.withColumn("_errors", F.array_compact(F.array(*rule_exprs)))


def mark_referential_ri(df, dim_df, key_col: str, rule_id: str):
    """Flag rows whose key is missing from a Silver dimension (DQ_011/012)."""
    from pyspark.sql import functions as F  # noqa: PLC0415

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


def validate() -> None:
    """Sanity checks for the registry (invoked from tests/CI)."""
    ids = [rid for rid, *_ in ALL_RULES]
    assert len(ids) == len(set(ids)), "rule ids must be unique"
    assert all(rid.startswith("DQ_") for rid in ids)
    assert all(rid in dq_bucket_index() for rid in ids)
    assert all(len(r) == 3 for r in COLUMN_RULES), "column rules need (id, desc, predicate)"
    assert all(len(r) == 2 for r in REFERENTIAL_RULES), "RI rules need (id, desc)"
    assert VALID_CURRENCIES and VALID_STATUSES
    # domain predicates must catch NULLs (blank source values round-trip to NULL)
    dq_009 = [p for rid, _, p in COLUMN_RULES if rid == "DQ_009"][0]
    dq_010 = [p for rid, _, p in COLUMN_RULES if rid == "DQ_010"][0]
    assert "IS NULL" in dq_009 and "IS NULL" in dq_010
    # every synonym must map to a status that passes DQ_010
    for synonym, canonical in STATUS_SYNONYMS.items():
        assert canonical in dq_010, f"{synonym} maps to {canonical} which fails DQ_010"
        assert synonym != canonical


def dq_bucket_index() -> dict[str, str]:
    """Map every rule id to its bucket (completeness/validity/domain/ri)."""
    from . import config

    index = {}
    for bucket, ids in config.DQ_BUCKETS.items():
        for rid in ids:
            index[rid] = bucket
    return index
