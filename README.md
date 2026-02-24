# Azure Event Stream — Real-Time Financial Metrics Pipeline

A production-grade streaming pipeline built on Azure Event Hubs, Spark Structured Streaming, and Delta Lake. Ingests synthetic financial transaction events in real time, validates them against a strict data contract, deduplicates across batches, and publishes three incremental metrics to Delta tables every 30 seconds.

Built as a portfolio project to demonstrate real streaming engineering judgment — not tutorial work.

---

## What This Is

A end-to-end streaming data product that does exactly three things well:

- Ingests transaction events from Azure Event Hubs continuously
- Enforces a data contract on every event before it touches any metric
- Publishes three ACID-safe, incrementally updated metrics to Delta Lake

The scope is intentionally narrow. The depth is intentionally real.

---

## Architecture

```
┌─────────────────┐     JSON events      ┌──────────────────┐
│  Event Generator │ ──────────────────► │  Azure Event Hubs │
│  ~100 events/sec │                     │                  │
└─────────────────┘                      └────────┬─────────┘
                                                  │
                                                  ▼
                                    ┌─────────────────────────┐
                                    │   Spark Structured       │
                                    │   Streaming              │
                                    │                          │
                                    │  1. Parse JSON body      │
                                    │  2. Contract validation  │
                                    │  3. Watermark dedup      │
                                    │  4. foreachBatch writes  │
                                    └────────────┬────────────┘
                                                 │
                          ┌──────────────────────┼──────────────────────┐
                          ▼                      ▼                      ▼
               ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
               │  M1: Net Flow   │   │ M2: User Metrics│   │  M3: Channel    │
               │  Single row     │   │ One row/user    │   │  Distribution   │
               │  ACID MERGE     │   │  Delta MERGE    │   │  Delta MERGE    │
               └─────────────────┘   └─────────────────┘   └─────────────────┘
                                                 │
                                    ┌────────────▼────────────┐
                                    │  Checkpoint (ADLS)       │
                                    │  Event Hub offsets       │
                                    │  Watermark + dedup state │
                                    └─────────────────────────┘
```

---
## Quick Proof of Correctness

This system guarantees correctness through deterministic processing, contract enforcement, and idempotent state updates.

**Core Proof Points:**

- **Contract-first validation:** All incoming events are validated against schema and domain rules (R010–R120). Invalid events are rejected before processing.
- **Deterministic streaming:** Micro-batch execution with checkpointing ensures consistent state recovery and replay safety.
- **Duplicate protection:** Watermark-based deduplication prevents double counting from late or repeated events.
- **Idempotent metrics:** Delta Lake MERGE operations ensure aggregates update safely without overwrite risks.
- **Auditability:** Run logs and evidence notebooks allow independent verification of processing outcomes.

This project includes a dedicated **Evidence Notebook** demonstrating validation results, metric consistency checks, and run-level audit proof.

---

## What Is Implemented

### Contract Validation

Every event is validated before it reaches any metric. Invalid events are silently filtered — they never touch the output tables.

Validation rules enforced:

| Rule | Check |
|------|-------|
| R010 – R080 | Null check on all 8 required fields |
| R090 | `event_type` must be `deposit_completed` or `withdrawal_completed` |
| R100 | `channel` must be `web`, `mobile`, or `api` |
| R110 | `currency` must be `GBP` |
| R120 | `amount` must be greater than zero |

Stable rule IDs mean failures are traceable and consistent across versions.

---

### Watermark-Based Deduplication

Event Hubs delivers at-least-once. The same `event_id` can arrive in a later batch. A simple `dropDuplicates()` within a single batch does not solve this.

This pipeline uses Spark's native watermark state store:

```python
valid_stream
    .withWatermark("event_time", "1 day")
    .dropDuplicates(["event_id"])
```

Spark tracks seen `event_id` values across batches within the watermark horizon and drops anything already processed. The state is bounded — after the horizon passes, old IDs are evicted automatically. No custom dedup table to maintain or clean up.

---

### Published Metrics

#### M1 — Net Flow `metrics/net_flow/`

Global cumulative deposit and withdrawal totals. Single row, updated every batch via **Delta MERGE on a constant key**. This means the update is fully atomic — no read-then-overwrite, no race condition under retry.

```
total_deposits | total_withdrawals | net_flow | deposit_count
withdrawal_count | avg_deposit | avg_withdrawal
deposit_to_withdrawal_ratio | updated_at
```

#### M2 — User Metrics `metrics/user_metrics/`

Per-user running totals. One row per `user_id`, upserted via Delta MERGE. New users are inserted; existing users have their totals accumulated. `first_seen` is preserved on match and never overwritten.

```
user_id | total_deposits | total_withdrawals
deposit_count | withdrawal_count | first_seen | last_seen | updated_at
```

#### M3 — Channel Distribution `metrics/channel_distribution/`

Running event counts and amounts grouped by `(channel, event_type)`. Six possible rows maximum (3 channels × 2 event types), upserted via Delta MERGE.

```
channel | event_type | event_count | total_amount | updated_at
```

---

### Checkpoint Recovery

All stream state is persisted to ADLS:

- Event Hub offsets — so no events are re-read on restart
- Watermark and dedup state — so cross-batch deduplication survives failures
- Batch metadata

On failure, the stream restarts from the last committed checkpoint. At most one micro-batch is replayed. Delta MERGE idempotency ensures replayed batches do not corrupt metric totals.

---

## Project Structure

```
src/
├── contracts/
│   ├── input_contract.py     # Schema definition + validation rules (R010–R120)
│   └── output_contract.py    # Delta table schemas for all 3 metrics
└── streaming/
    ├── stream_processor.py   # Entry point, dedup, foreachBatch writes
    └── aggregations.py       # compute_net_flow, compute_user_metrics, compute_channel_distribution
```

---

## Design Decisions

### Why micro-batching and not continuous streaming

Micro-batching (30-second trigger) gives atomic Delta commits per batch. Each batch either fully succeeds or fully fails — no partial metric updates. Continuous streaming makes this harder to reason about and harder to debug. For a financial metric pipeline where correctness matters more than sub-second latency, micro-batching is the right trade-off.

### Why watermark dedup and not a custom seen-events table

An alternative approach is to maintain a Delta table of every processed `event_id` and do a left-anti join each batch. This works but creates an ever-growing table with no natural cleanup mechanism. Watermark-based dedup uses Spark's built-in state store, bounds the state automatically by time horizon, and requires no infrastructure beyond the checkpoint. Less to build, less to maintain, same guarantee.

### Why Delta MERGE for all three metrics

All three metrics use Delta MERGE instead of overwrite. This is deliberate. Overwrite is simple but not ACID-safe under concurrent retries — two overlapping batch retries can both read the same old value and produce a wrong cumulative total. MERGE is atomic at the row level. The state is always consistent regardless of how many times a batch retries.

### Why three metrics and not more

M1 demonstrates a single-row ACID merge. M2 demonstrates per-entity upserts. M3 demonstrates multi-grain aggregation. Together they cover the core patterns of stateful streaming. Adding more metrics without a new requirement would be scope creep, not depth.

---

## Generator Characteristics

The synthetic event generator produces:

- ~100 events per second
- ~500 active users
- Channels: `web`, `mobile`, `api`
- Currency: `GBP`
- Amount range: £5 – £500
- Event types: `deposit_completed`, `withdrawal_completed`

This is controlled simulation data used to validate the streaming patterns — not real transaction data.

---

## How to Run (Azure Synapse)

### Prerequisites

- Azure Event Hubs namespace with a hub named `transactions`
- Azure Data Lake Storage Gen2 container named `data`
- Synapse Spark pool attached to the workspace
- Event Hub connection string stored in Azure Key Vault (never hardcoded)

### Steps

**1. Set up the event generator (run locally)**

Install dependencies:

```bash
pip install azure-eventhub python-dotenv
```

Create a `.env` file in the generator directory:

```
EVENT_HUB_CONNECTION_STRING=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...
EVENT_HUB_NAME=transactions
```

Run the generator:

```bash
python generator.py
```

Or override defaults via arguments:

```bash
python generator.py --eps 100 --num-users 500
```

The generator will emit ~100 events/second and print progress every 100 events:

```
Starting event generation at 100 events/sec...
Events sent: 100 | Actual rate: 99.8 eps
Events sent: 200 | Actual rate: 100.1 eps
```

Stop it at any time with `Ctrl+C`. It will print a final summary before exiting.

> **Note:** Start the generator before running the Synapse stream so events are already queuing in Event Hubs when the first micro-batch fires.

---

**2. Upload the code package to ADLS**

```
abfss://data@<storage_account>.dfs.core.windows.net/code/azure_stream_code.zip
```

**2. Open a Synapse notebook attached to your Spark pool**

**3. In the first cell, install the package and start the stream**

```python
# Download zip from ADLS and attach to Spark context
spark.sparkContext.addPyFile(
    "abfss://data@<storage_account>.dfs.core.windows.net/code/azure_stream_code.zip"
)

# Retrieve connection string from Key Vault (never pass plaintext)
connection_string = mssparkutils.credentials.getSecret("<keyvault_name>", "<secret_name>")

from src.streaming.stream_processor import run_stream

run_stream(
    spark=spark,
    connection_string=connection_string,
    event_hub_name="transactions",
    output_path="abfss://data@<storage_account>.dfs.core.windows.net/stream",
    checkpoint_path="abfss://data@<storage_account>.dfs.core.windows.net/checkpoints",
    trigger_seconds=30,
    max_partitions=4,
    write_curated=False,
)
```

**4. Let the stream run for at least one micro-batch (30 seconds)**

### Output Locations

```
abfss://data@<storage_account>.dfs.core.windows.net/
├── stream/
│   └── metrics/
│       ├── net_flow/
│       ├── user_metrics/
│       └── channel_distribution/
├── checkpoints/
└── stream/curated/               # only if write_curated=True
```

### Stopping the Stream

Interrupt the notebook execution. The checkpoint is already committed — restarting the notebook resumes safely from the last completed batch with no data loss or duplication.

### Validating Outputs

Use the included Evidence Notebook to verify:

- Metric invariants (e.g. `net_flow = total_deposits - total_withdrawals`)
- Cross-table consistency across M1, M2, and M3
- Checkpoint integrity and batch progression

---

## What This Project Demonstrates

- Azure Event Hubs ingestion with Spark Structured Streaming
- Data contract enforcement in a streaming context
- Watermark-based cross-batch deduplication using Spark state store
- Delta Lake ACID merges for stateful incremental metrics
- Checkpoint-based failure recovery
- Clean separation between contract, aggregation, and processing logic
- Engineering trade-off reasoning documented alongside the code
