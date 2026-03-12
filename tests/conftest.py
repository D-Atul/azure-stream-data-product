from __future__ import annotations

from typing import Iterable

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("azure-stream-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture()
def valid_events():
    return [
        {
            "event_id": "evt_001",
            "user_id": "u1",
            "event_time": "2026-01-01T10:00:00Z",
            "event_type": "deposit_completed",
            "amount": 100.0,
            "currency": "GBP",
            "channel": "web",
            "ingest_time": "2026-01-01T10:00:05Z",
        },
        {
            "event_id": "evt_002",
            "user_id": "u1",
            "event_time": "2026-01-01T10:01:00Z",
            "event_type": "withdrawal_completed",
            "amount": 30.0,
            "currency": "GBP",
            "channel": "mobile",
            "ingest_time": "2026-01-01T10:01:05Z",
        },
        {
            "event_id": "evt_003",
            "user_id": "u2",
            "event_time": "2026-01-01T10:02:00Z",
            "event_type": "deposit_completed",
            "amount": 50.0,
            "currency": "GBP",
            "channel": "api",
            "ingest_time": "2026-01-01T10:02:05Z",
        },
    ]


@pytest.fixture()
def invalid_events():
    return [
        {
            "event_id": "evt_bad_1",
            "user_id": "u3",
            "event_time": "2026-01-01T10:03:00Z",
            "event_type": "bad_type",
            "amount": 10.0,
            "currency": "GBP",
            "channel": "web",
            "ingest_time": "2026-01-01T10:03:05Z",
        },
        {
            "event_id": "evt_bad_2",
            "user_id": "u4",
            "event_time": "2026-01-01T10:04:00Z",
            "event_type": "deposit_completed",
            "amount": -5.0,
            "currency": "GBP",
            "channel": "mobile",
            "ingest_time": "2026-01-01T10:04:05Z",
        },
        {
            "event_id": "evt_bad_3",
            "user_id": "u5",
            "event_time": "2026-01-01T10:05:00Z",
            "event_type": "deposit_completed",
            "amount": 20.0,
            "currency": "USD",
            "channel": "web",
            "ingest_time": "2026-01-01T10:05:05Z",
        },
    ]


def first_existing_attr(module, names: Iterable[str]):
    for name in names:
        if hasattr(module, name):
            return getattr(module, name)
    raise AttributeError(f"None of these attributes exist in {module.__name__}: {list(names)}")