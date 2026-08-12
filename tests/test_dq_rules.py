"""Unit tests for the DQ rule registry (no Spark required)."""

import pytest

from sales_platform import config, dq_rules


def test_all_rules_are_registered():
    ids = [rid for rid, *_ in dq_rules.ALL_RULES]
    assert len(ids) == 12
    assert len(set(ids)) == len(ids)


def test_column_rules_have_sql_predicates():
    for rid, desc, predicate in dq_rules.COLUMN_RULES:
        assert rid.startswith("DQ_")
        assert desc
        assert predicate, f"{rid} must have a predicate"


def test_referential_rules_are_join_based():
    for rid, desc in dq_rules.REFERENTIAL_RULES:
        assert rid in ("DQ_011", "DQ_012")


def test_bucket_mapping_consistent():
    index = dq_rules.dq_bucket_index()
    assert index["DQ_001"] == "completeness"
    assert index["DQ_006"] == "validity"
    assert index["DQ_009"] == "domain"
    assert index["DQ_011"] == "referential_integrity"


def test_valid_domains():
    assert "USD" in dq_rules.VALID_CURRENCIES
    assert "XYZ" not in dq_rules.VALID_CURRENCIES
    assert "Shipped" not in dq_rules.VALID_STATUSES


def test_rules_by_id_lookup():
    found = dq_rules.rules_by_id({"DQ_002", "DQ_012"})
    assert len(found) == 2
    assert {r[0] for r in found} == {"DQ_002", "DQ_012"}


def test_validate():
    dq_rules.validate()


def test_currencies_match_config_bucket():
    # every currency check in the rules must use the documented currency set
    dq_009 = [p for rid, _, p in dq_rules.COLUMN_RULES if rid == "DQ_009"][0]
    for c in dq_rules.VALID_CURRENCIES:
        assert c in dq_009
