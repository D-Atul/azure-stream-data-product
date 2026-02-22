from __future__ import annotations

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F


def compute_net_flow(df: SparkDF) -> SparkDF:
    """
    M1: Global aggregate from current micro-batch.
    Returns exactly: total_deposits, total_withdrawals,
                     deposit_count, withdrawal_count
    updated_at added by merge layer.
    """
    return df.agg(
        F.sum(
            F.when(F.col("event_type") == "deposit_completed", F.col("amount"))
            .otherwise(0)
        ).alias("total_deposits"),
        F.sum(
            F.when(F.col("event_type") == "withdrawal_completed", F.col("amount"))
            .otherwise(0)
        ).alias("total_withdrawals"),
        F.count(
            F.when(F.col("event_type") == "deposit_completed", F.lit(1))
        ).alias("deposit_count"),
        F.count(
            F.when(F.col("event_type") == "withdrawal_completed", F.lit(1))
        ).alias("withdrawal_count"),
    )


def compute_user_metrics(df: SparkDF) -> SparkDF:
    """
    M2: Per-user aggregates from current micro-batch.
    Returns exactly: user_id, total_deposits, total_withdrawals,
                     deposit_count, withdrawal_count,
                     first_seen, last_seen, updated_at
    """
    return df.groupBy("user_id").agg(
        F.sum(
            F.when(F.col("event_type") == "deposit_completed", F.col("amount"))
            .otherwise(0)
        ).alias("total_deposits"),
        F.sum(
            F.when(F.col("event_type") == "withdrawal_completed", F.col("amount"))
            .otherwise(0)
        ).alias("total_withdrawals"),
        F.count(
            F.when(F.col("event_type") == "deposit_completed", F.lit(1))
        ).alias("deposit_count"),
        F.count(
            F.when(F.col("event_type") == "withdrawal_completed", F.lit(1))
        ).alias("withdrawal_count"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.current_timestamp().alias("updated_at"),
    )


def compute_channel_distribution(df: SparkDF) -> SparkDF:
    """
    M3: Per channel-event_type aggregates from current micro-batch.
    Returns exactly: channel, event_type, event_count,
                     total_amount, updated_at
    """
    return df.groupBy("channel", "event_type").agg(
        F.count(F.lit(1)).alias("event_count"),
        F.sum("amount").alias("total_amount"),
        F.current_timestamp().alias("updated_at"),
    )