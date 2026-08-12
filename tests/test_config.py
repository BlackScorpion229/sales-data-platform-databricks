"""Unit tests for shared configuration (no Spark required)."""

import pytest

from sales_platform import config


def test_table_names_are_qualified():
    for name, table in config.TABLES.items():
        assert table.startswith(f"{config.CATALOG}."), f"{name} not fully qualified"
        assert not table.endswith(".")


def test_medallion_schema_layout():
    bronze = [t for t in config.TABLES.values() if t.startswith(f"{config.BRONZE}.")]
    silver = [t for t in config.TABLES.values() if t.startswith(f"{config.SILVER}.")]
    gold = [t for t in config.TABLES.values() if t.startswith(f"{config.GOLD}.")]
    assert bronze and silver and gold
    assert len(bronze) + len(silver) + len(gold) == len(config.TABLES)


def test_segment_thresholds_sane():
    assert 0 < config.SEGMENT_THRESHOLDS["low"] < config.SEGMENT_THRESHOLDS["high"]


def test_reference_values():
    assert config.BUDGET_DAILY > 0
    assert config.RECONCILIATION_TOLERANCE_USD >= 0
    assert config.FX_DIM_DATE_START < config.FX_DIM_DATE_END


def test_dq_buckets_cover_all_rules():
    from sales_platform import dq_rules

    all_ids = {rid for rid, *_ in dq_rules.ALL_RULES}
    bucket_ids = set(dq_rules.dq_bucket_index())
    assert all_ids == bucket_ids


def test_validate():
    config.validate()


def test_raw_paths_under_base():
    assert config.RAW_CUSTOMER.startswith(config.RAW_BASE)
    assert config.RAW_PRODUCT.startswith(config.RAW_BASE)
    assert config.RAW_TRANSACT.startswith(config.RAW_BASE)
    assert config.CHECKPOINT_BASE.startswith(config.RAW_BASE)
