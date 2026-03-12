from __future__ import annotations

import importlib

from tests.conftest import first_existing_attr


input_contract = importlib.import_module("src.contracts.input_contract")
aggregations = importlib.import_module("src.streaming.aggregations")


def _validate_input(df):
    fn = first_existing_attr(
        input_contract,
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
        return result
    return result, None


def test_smoke_contract_to_metrics_pipeline(spark, valid_events):
    raw_df = spark.createDataFrame(valid_events)

    validated_df, rejected_df = _validate_input(raw_df)

    net_flow_fn = first_existing_attr(aggregations, ["compute_net_flow"])
    user_metrics_fn = first_existing_attr(aggregations, ["compute_user_metrics"])
    channel_dist_fn = first_existing_attr(aggregations, ["compute_channel_distribution"])

    net_flow_df = net_flow_fn(validated_df)
    user_metrics_df = user_metrics_fn(validated_df)
    channel_dist_df = channel_dist_fn(validated_df)

    assert validated_df.count() == 3
    if rejected_df is not None:
        assert rejected_df.count() == 0

    assert net_flow_df.count() == 1
    assert user_metrics_df.count() == 2
    assert channel_dist_df.count() == 3