from __future__ import annotations

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
    IntegerType,
)


# ─── Curated Events ───────────────────────────────────────────────────────────

def curated_events_schema() -> StructType:
    """
    Validated events written to curated layer.
    Append-only. One row per valid event.
    """
    return StructType([
        StructField("event_id",    StringType(),    False),
        StructField("user_id",     StringType(),    False),
        StructField("event_time",  TimestampType(), False),
        StructField("ingest_time", TimestampType(), False),
        StructField("event_type",  StringType(),    False),
        StructField("amount",      DoubleType(),    False),
        StructField("currency",    StringType(),    False),
        StructField("channel",     StringType(),    False),
    ])


# ─── Rejected Events ──────────────────────────────────────────────────────────

def rejected_events_schema() -> StructType:
    """
    Contract-violating events written to monitoring layer.
    Append-only. Includes raw payload and stable rule ID.
    """
    return StructType([
        StructField("event_id",         StringType(),    True),
        StructField("raw_payload",       StringType(),    False),
        StructField("rejection_reason",  StringType(),    False),
        StructField("rejected_at",       TimestampType(), False),
    ])


# ─── Stream Health ────────────────────────────────────────────────────────────

def stream_health_schema() -> StructType:
    """
    One row per micro-batch written to monitoring layer.
    Append-only. Continuous evidence of stream health.
    """
    return StructType([
        StructField("batch_id",            LongType(),      False),
        StructField("batch_start",         TimestampType(), False),
        StructField("batch_end",           TimestampType(), False),
        StructField("events_received",     LongType(),      False),
        StructField("events_valid",        LongType(),      False),
        StructField("events_rejected",     LongType(),      False),
        StructField("duplicate_events",    LongType(),      False),
        StructField("processing_time_ms",  LongType(),      False),
        StructField("lag_seconds",         LongType(),      False),
        StructField("status",              StringType(),    False),
    ])


# ─── Run Manifest ─────────────────────────────────────────────────────────────

def run_manifest_schema() -> StructType:
    """
    Single-row evidence artifact overwritten each micro-batch.
    Shows cumulative stream totals since stream start.
    """
    return StructType([
        StructField("stream_start",             StringType(), False),
        StructField("last_updated",             StringType(), False),
        StructField("batches_processed",        LongType(),   False),
        StructField("total_events_received",    LongType(),   False),
        StructField("total_events_valid",       LongType(),   False),
        StructField("total_events_rejected",    LongType(),   False),
        StructField("total_duplicate_events",   LongType(),   False),
        StructField("output_path",              StringType(), False),
        StructField("event_hub_name",           StringType(), False),
        StructField("dedup_scope",              StringType(), False),
    ])


# ─── Metric Schemas ───────────────────────────────────────────────────────────

def net_flow_schema() -> StructType:
    """
    M1: Global aggregate. Single row, overwritten each micro-batch.
    """
    return StructType([
        StructField("total_deposits",              DoubleType(), False),
        StructField("total_withdrawals",           DoubleType(), False),
        StructField("net_flow",                    DoubleType(), False),
        StructField("deposit_count",               LongType(),   False),
        StructField("withdrawal_count",            LongType(),   False),
        StructField("avg_deposit",                 DoubleType(), False),
        StructField("avg_withdrawal",              DoubleType(), False),
        StructField("deposit_to_withdrawal_ratio", DoubleType(), False),
        StructField("updated_at",                  TimestampType(), False),
    ])


def user_metrics_schema() -> StructType:
    """
    M2: Per-user running totals. One row per user, upserted each micro-batch.
    """
    return StructType([
        StructField("user_id",           StringType(),    False),
        StructField("total_deposits",    DoubleType(),    False),
        StructField("total_withdrawals", DoubleType(),    False),
        StructField("deposit_count",     LongType(),      False),
        StructField("withdrawal_count",  LongType(),      False),
        StructField("first_seen",        TimestampType(), False),
        StructField("last_seen",         TimestampType(), False),
        StructField("updated_at",        TimestampType(), False),
    ])


def channel_distribution_schema() -> StructType:
    """
    M3: Per channel-event_type running counts.
    Six rows total (3 channels x 2 event types), upserted each micro-batch.
    """
    return StructType([
        StructField("channel",      StringType(),    False),
        StructField("event_type",   StringType(),    False),
        StructField("event_count",  LongType(),      False),
        StructField("total_amount", DoubleType(),    False),
        StructField("updated_at",   TimestampType(), False),
    ])