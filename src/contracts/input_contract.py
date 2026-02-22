from __future__ import annotations

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


class ContractViolation(Exception):
    """Raised when stream schema is missing required columns."""


VALID_EVENT_TYPES = {"deposit_completed", "withdrawal_completed"}
VALID_CHANNELS = {"web", "mobile", "api"}
VALID_CURRENCIES = {"GBP"}


def raw_events_schema() -> StructType:
    return StructType([
        StructField("event_id",    StringType(),    True),
        StructField("user_id",     StringType(),    True),
        StructField("event_time",  TimestampType(), True),
        StructField("ingest_time", TimestampType(), True),
        StructField("event_type",  StringType(),    True),
        StructField("amount",      DoubleType(),    True),
        StructField("currency",    StringType(),    True),
        StructField("channel",     StringType(),    True),
    ])


def validate_raw_events(df: SparkDF) -> tuple[SparkDF, SparkDF]:
    """
    Validate parsed events against input contract.
    Returns (df_valid, df_rejected).
    Rejected rows carry a stable rule ID as rejection_reason.

    Rule IDs:
        R010 — null event_id
        R020 — null user_id
        R030 — null event_time
        R040 — null ingest_time
        R050 — null event_type
        R060 — null amount
        R070 — null currency
        R080 — null channel
        R090 — invalid event_type
        R100 — invalid channel
        R110 — invalid currency
        R120 — non-positive amount
    """
    expected = [f.name for f in raw_events_schema().fields]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ContractViolation(f"Input schema missing required columns: {missing}")

    df = df.select(*expected)

    rejection_reason = (
        F.when(F.col("event_id").isNull(),                              "R010_null_event_id")
        .when(F.col("user_id").isNull(),                                "R020_null_user_id")
        .when(F.col("event_time").isNull(),                             "R030_null_event_time")
        .when(F.col("ingest_time").isNull(),                            "R040_null_ingest_time")
        .when(F.col("event_type").isNull(),                             "R050_null_event_type")
        .when(F.col("amount").isNull(),                                 "R060_null_amount")
        .when(F.col("currency").isNull(),                               "R070_null_currency")
        .when(F.col("channel").isNull(),                                "R080_null_channel")
        .when(~F.col("event_type").isin(VALID_EVENT_TYPES),             "R090_invalid_event_type")
        .when(~F.col("channel").isin(VALID_CHANNELS),                   "R100_invalid_channel")
        .when(~F.col("currency").isin(VALID_CURRENCIES),                "R110_invalid_currency")
        .when(F.col("amount") <= 0,                                     "R120_non_positive_amount")
    )

    df = df.withColumn("_rejection_reason", rejection_reason)

    df_valid = (
        df.filter(F.col("_rejection_reason").isNull())
        .drop("_rejection_reason")
    )

    df_rejected = (
        df.filter(F.col("_rejection_reason").isNotNull())
        .withColumnRenamed("_rejection_reason", "rejection_reason")
    )

    return df_valid, df_rejected