from __future__ import annotations

import importlib

import pytest
from pyspark.sql import functions as F

from tests.conftest import first_existing_attr


contract_module = importlib.import_module("src.contracts.input_contract")


def _validate_input(df):
    fn = first_existing_attr(
        contract_module,
        [
            "validate_raw_events",
            "validate_input_events",
            "validate_input_contract",
            "validate_events",
            "validate_contract",
        ],
    )
    result = fn(df)
    if isinstance(result, tuple):
        return result  # (valid_df, rejected_df)
    return result, None


def test_valid_input_contract_accepts_good_rows(spark, valid_events):
    df = spark.createDataFrame(valid_events)
    valid_df, rejected_df = _validate_input(df)

    assert valid_df.count() == 3
    if rejected_df is not None:
        assert rejected_df.count() == 0


def test_invalid_event_type_is_rejected(spark, invalid_events):
    df = spark.createDataFrame([invalid_events[0]])
    valid_df, rejected_df = _validate_input(df)

    assert valid_df.count() == 0
    assert rejected_df.count() == 1


def test_negative_amount_is_rejected(spark, invalid_events):
    df = spark.createDataFrame([invalid_events[1]])
    valid_df, rejected_df = _validate_input(df)

    assert valid_df.count() == 0
    assert rejected_df.count() == 1


def test_non_gbp_currency_is_rejected(spark, invalid_events):
    df = spark.createDataFrame([invalid_events[2]])
    valid_df, rejected_df = _validate_input(df)

    assert valid_df.count() == 0
    assert rejected_df.count() == 1


def test_missing_required_column_fails_contract(spark, valid_events):
    df = spark.createDataFrame(valid_events).drop("channel")
    with pytest.raises(Exception):
        _validate_input(df)


def test_duplicate_event_ids_can_be_detected_before_dedup(spark, valid_events):
    rows = valid_events + [dict(valid_events[0])]
    df = spark.createDataFrame(rows)
    dupes = (
        df.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    assert dupes == 1