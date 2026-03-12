from __future__ import annotations

import importlib

from tests.conftest import first_existing_attr


agg_module = importlib.import_module("src.streaming.aggregations")


def test_compute_net_flow(spark, valid_events):
    df = spark.createDataFrame(valid_events)
    fn = first_existing_attr(agg_module, ["compute_net_flow"])
    out = fn(df)

    row = out.collect()[0].asDict()
    assert row["total_deposits"] == 150.0
    assert row["total_withdrawals"] == 30.0
    assert row["deposit_count"] == 2
    assert row["withdrawal_count"] == 1

    if "net_flow" in row:
        assert row["net_flow"] == 120.0


def test_compute_user_metrics(spark, valid_events):
    df = spark.createDataFrame(valid_events)
    fn = first_existing_attr(agg_module, ["compute_user_metrics"])
    out = fn(df)

    rows = {r["user_id"]: r.asDict() for r in out.collect()}
    assert set(rows.keys()) == {"u1", "u2"}
    assert rows["u1"]["total_deposits"] == 100.0
    assert rows["u1"]["total_withdrawals"] == 30.0
    assert rows["u2"]["total_deposits"] == 50.0
    assert rows["u2"]["total_withdrawals"] == 0.0


def test_compute_channel_distribution(spark, valid_events):
    df = spark.createDataFrame(valid_events)
    fn = first_existing_attr(agg_module, ["compute_channel_distribution"])
    out = fn(df)

    rows = {(r["channel"], r["event_type"]): r.asDict() for r in out.collect()}
    assert rows[("web", "deposit_completed")]["event_count"] == 1
    assert rows[("mobile", "withdrawal_completed")]["event_count"] == 1
    assert rows[("api", "deposit_completed")]["event_count"] == 1