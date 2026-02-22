from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.contracts.input_contract import raw_events_schema, validate_raw_events
from src.streaming.aggregations import (
    compute_net_flow,
    compute_user_metrics,
    compute_channel_distribution,
)

logger = logging.getLogger(__name__)


# ----------------------------
# Helpers
# ----------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _has_rows(df) -> bool:
    # Cheaper than rdd.isEmpty(); stops after 1 row
    return bool(df.take(1))


# ----------------------------
# Output writers / mergers
# ----------------------------
def _merge_net_flow(spark: SparkSession, batch_df, output_path: str) -> None:
    """
    M1: single-row cumulative totals (ACID-safe via Delta MERGE on a constant key).
    Safe for:
      - empty batches (skips)
      - null sums (coerces to 0)
      - first run (table created then merged)
    """
    if not _has_rows(batch_df):
        return

    path = f"{output_path}/metrics/net_flow"

    row = compute_net_flow(batch_df).first()
    if row is None:
        return

    # Guard against null aggregates
    batch_total_deposits = float(row["total_deposits"] or 0.0)
    batch_total_withdrawals = float(row["total_withdrawals"] or 0.0)
    batch_deposit_count = int(row["deposit_count"] or 0)
    batch_withdrawal_count = int(row["withdrawal_count"] or 0)

    # Single-row state with constant key
    batch_state = spark.createDataFrame([{
        "id": 1,
        "batch_total_deposits": batch_total_deposits,
        "batch_total_withdrawals": batch_total_withdrawals,
        "batch_deposit_count": batch_deposit_count,
        "batch_withdrawal_count": batch_withdrawal_count,
        "updated_at": _utc_now(),
    }]).coalesce(1)

    try:
        from delta.tables import DeltaTable
        target = DeltaTable.forPath(spark, path)

        # Atomic upsert/update of the single row (id=1)
        (target.alias("t")
            .merge(batch_state.alias("b"), "t.id = b.id")
            .whenMatchedUpdate(set={
                "total_deposits":              "t.total_deposits + b.batch_total_deposits",
                "total_withdrawals":           "t.total_withdrawals + b.batch_total_withdrawals",
                "net_flow":                    "(t.total_deposits + b.batch_total_deposits) - (t.total_withdrawals + b.batch_total_withdrawals)",
                "deposit_count":               "t.deposit_count + b.batch_deposit_count",
                "withdrawal_count":            "t.withdrawal_count + b.batch_withdrawal_count",
                "avg_deposit":                 "(t.total_deposits + b.batch_total_deposits) / greatest((t.deposit_count + b.batch_deposit_count), 1)",
                "avg_withdrawal":              "(t.total_withdrawals + b.batch_total_withdrawals) / greatest((t.withdrawal_count + b.batch_withdrawal_count), 1)",
                "deposit_to_withdrawal_ratio": "(t.total_deposits + b.batch_total_deposits) / greatest((t.total_withdrawals + b.batch_total_withdrawals), 0.01)",
                "updated_at":                  "b.updated_at",
            })
            .whenNotMatchedInsert(values={
                "id":                          "b.id",
                "total_deposits":              "b.batch_total_deposits",
                "total_withdrawals":           "b.batch_total_withdrawals",
                "net_flow":                    "b.batch_total_deposits - b.batch_total_withdrawals",
                "deposit_count":               "b.batch_deposit_count",
                "withdrawal_count":            "b.batch_withdrawal_count",
                "avg_deposit":                 "b.batch_total_deposits / greatest(b.batch_deposit_count, 1)",
                "avg_withdrawal":              "b.batch_total_withdrawals / greatest(b.batch_withdrawal_count, 1)",
                "deposit_to_withdrawal_ratio": "b.batch_total_deposits / greatest(b.batch_total_withdrawals, 0.01)",
                "updated_at":                  "b.updated_at",
            })
            .execute()
        )

    except AnalysisException:
        # First run: create a correctly-shaped Delta table, then MERGE will work next batch.
        logger.info("net_flow table not found at %s — creating on first batch.", path)

        created = spark.createDataFrame([{
            "id": 1,
            "total_deposits": batch_total_deposits,
            "total_withdrawals": batch_total_withdrawals,
            "net_flow": batch_total_deposits - batch_total_withdrawals,
            "deposit_count": batch_deposit_count,
            "withdrawal_count": batch_withdrawal_count,
            "avg_deposit": batch_total_deposits / max(batch_deposit_count, 1),
            "avg_withdrawal": batch_total_withdrawals / max(batch_withdrawal_count, 1),
            "deposit_to_withdrawal_ratio": batch_total_deposits / max(batch_total_withdrawals, 0.01),
            "updated_at": _utc_now(),
        }]).coalesce(1)

        created.write.format("delta").mode("overwrite").save(path)


def _merge_user_metrics(spark: SparkSession, batch_df, output_path: str, max_partitions: int = 4) -> None:
    """
    M2: upsert per-user cumulative aggregates (Delta MERGE).
    """
    if not _has_rows(batch_df):
        return

    path = f"{output_path}/metrics/user_metrics"
    batch_agg = compute_user_metrics(batch_df).coalesce(max_partitions)

    try:
        from delta.tables import DeltaTable
        (DeltaTable.forPath(spark, path)
            .alias("existing")
            .merge(batch_agg.alias("batch"), "existing.user_id = batch.user_id")
            .whenMatchedUpdate(set={
                "total_deposits":    "existing.total_deposits + batch.total_deposits",
                "total_withdrawals": "existing.total_withdrawals + batch.total_withdrawals",
                "deposit_count":     "existing.deposit_count + batch.deposit_count",
                "withdrawal_count":  "existing.withdrawal_count + batch.withdrawal_count",
                "last_seen":         "batch.last_seen",
                "updated_at":        "batch.updated_at",
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException:
        logger.info("user_metrics table not found at %s — creating on first batch.", path)
        batch_agg.write.format("delta").mode("overwrite").save(path)


def _merge_channel_distribution(spark: SparkSession, batch_df, output_path: str, max_partitions: int = 4) -> None:
    """
    M3: upsert per-channel + event_type cumulative aggregates (Delta MERGE).
    """
    if not _has_rows(batch_df):
        return

    path = f"{output_path}/metrics/channel_distribution"
    batch_agg = compute_channel_distribution(batch_df).coalesce(max_partitions)

    try:
        from delta.tables import DeltaTable
        (DeltaTable.forPath(spark, path)
            .alias("existing")
            .merge(
                batch_agg.alias("batch"),
                "existing.channel = batch.channel AND existing.event_type = batch.event_type",
            )
            .whenMatchedUpdate(set={
                "event_count":  "existing.event_count + batch.event_count",
                "total_amount": "existing.total_amount + batch.total_amount",
                "updated_at":   "batch.updated_at",
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException:
        logger.info("channel_distribution table not found at %s — creating on first batch.", path)
        batch_agg.write.format("delta").mode("overwrite").save(path)


def _write_curated(batch_df, output_path: str, max_partitions: int = 4) -> None:
    """
    Optional: append valid deduped events as a Delta table.
    """
    if not _has_rows(batch_df):
        return

    (batch_df
        .coalesce(max_partitions)
        .write.format("delta")
        .mode("append")
        .save(f"{output_path}/curated/transaction_events")
    )


# ----------------------------
# Stream entry point 
# ----------------------------
def run_stream(
    spark: SparkSession,
    connection_string: str,
    event_hub_name: str,
    output_path: str,
    checkpoint_path: str,
    trigger_seconds: int = 30,
    max_partitions: int = 4,
    write_curated: bool = False,
    event_time_col: str = "event_time",
    watermark: str = "1 day",
) -> None:
    """
    Synapse streaming entry point (metrics-only).
    Option A: watermark-based cross-batch dedup (Spark state store).
    """

    spark.conf.set("spark.sql.shuffle.partitions", str(max_partitions))
    spark.conf.set("spark.default.parallelism", str(max_partitions))

    plain = f"{connection_string};EntityPath={event_hub_name}"
    encrypted = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(plain)
    eh_conf = {"eventhubs.connectionString": encrypted}

    # Read stream + safe parse
    raw_stream = (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
        .select(
            F.from_json(F.expr("try_cast(body as string)"), raw_events_schema()).alias("data")
        )
    )

    parsed_stream = raw_stream.filter(F.col("data").isNotNull()).select("data.*")

    # Contract validation as transformations
    valid_stream, _rejected_stream_unused = validate_raw_events(parsed_stream)

    # Ensure event-time column is timestamp for watermarking
    valid_stream = valid_stream.withColumn(event_time_col, F.col(event_time_col).cast("timestamp"))

    deduped_stream = (
        valid_stream
        .withWatermark(event_time_col, watermark)
        .dropDuplicates(["event_id"])
    )

    def process_batch(batch_df, batch_id):
        if not _has_rows(batch_df):
            return

        if write_curated:
            _write_curated(batch_df, output_path, max_partitions=max_partitions)

        _merge_net_flow(spark, batch_df, output_path)
        _merge_user_metrics(spark, batch_df, output_path, max_partitions=max_partitions)
        _merge_channel_distribution(spark, batch_df, output_path, max_partitions=max_partitions)

        logger.info("Batch %s complete", batch_id)

    (
        deduped_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
        .awaitTermination()
    )