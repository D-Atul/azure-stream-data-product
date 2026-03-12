# Azure Streaming Data Product — Framework

## Purpose
Design and deliver a cloud streaming data product that ingests transaction events from Azure Event Hubs, validates them under explicit contracts, computes governed near-real-time metrics, and publishes reliable outputs to Delta Lake with checkpoint-backed recovery.

This framework defines the intended architectural boundaries, operating rules, and quality guarantees before implementation.

---

## Non-Goals
- No predictive modelling or anomaly scoring
- No machine learning inference
- No causal claims
- No silent data fixing or imputation
- No enrichment from external business systems
- No prescriptive fraud decisions or automated account actions

---

## PHASE A — PRODUCT DEFINITION

### 1) Product Charter
- **Product:** Azure Streaming Transaction Monitoring Data Product
- **Owner:** Data Engineering (Portfolio Project)
- **Consumers:** Analysts, monitoring teams, downstream reporting pipelines

**Supported use cases:**
- Near-real-time monitoring of transaction activity
- Governed metric production over streaming inputs
- Demonstration of stateful stream processing under explicit contracts
- Audit-ready verification of streaming outputs

**Outputs:**
- Curated Delta outputs
- Streaming metric tables
- Checkpoint-backed recoverable state
- Evidence artifacts for verification

**Freshness:**
- Near-real-time, micro-batch streaming execution

---

### 2) Source Trust & Lineage
- **Primary source:** Azure Event Hubs
- **Upstream producer:** Controlled synthetic event generator for portfolio execution

**Trust posture:**
- Inputs are trusted only after contract validation
- Streaming outputs are conditional on schema, ordering, and deduplication rules
- No external business truth source is assumed

---

### 3) Data Model & Grain

**Primary entity:**
- **Transactions:** one row per event emitted into the stream

**Supporting outputs:**
- Curated accepted events
- Governed aggregate metric tables

**Grain:**
- Input stream: one row per transaction event
- Published metrics: table-specific aggregate grain

**Identifiers:**
- Event-level transaction identifier used for deduplication and output governance
- User-level identifiers may be used for grouped metrics where defined

> **Clarification (execution scope):**  
> The product operates on a **single streaming transaction entity**.  
> All published metrics must be derivable from the validated event stream and declared transformations.

---

## PHASE B — CONTRACTS & GUARANTEES

### 4) Data Contracts
Contracts define:
- Required columns
- Data types and nullability
- Allowed enum/value domains
- Semantic interpretation of key fields
- Rules for accepting or rejecting incoming events

**Principles:**
- Contract validation is fail-fast for invalid records at the processing boundary
- Rules are explicit and versionable
- No silent coercion beyond declared transformation logic
- Contract outcomes must be inspectable

---

### 5) Quality Gates

**Structural:**
- Incoming events must conform to the declared schema
- Required identifiers and timestamps must be present
- Event types and currency/value domains must pass allowed-value checks

**Streaming reliability:**
- Duplicate handling must be explicit
- Watermarking / late-arrival policy must be declared
- Restart recovery must rely on checkpointed state, not manual repair

**Completeness:**
- The pipeline may continue operating under valid sparse event patterns where metric logic allows
- Invalid events are rejected rather than silently repaired

---

## PHASE C — METRIC SCOPE (DEFINED PRE-IMPLEMENTATION)

### 6) Eligible Metric Categories
Subject to validation, the stream may support:
- Transaction volume metrics
- Amount-based aggregate metrics
- User- or channel-level grouped metrics
- Net flow or directional money movement summaries
- Distribution metrics over declared dimensions
- Audit and operational monitoring summaries

**Explicitly excluded:**
- Predictive fraud scoring
- Behavioural modelling
- Causal interpretation
- Prescriptive interventions

> **Constraint:**  
> Metrics must be derived only from the validated streaming event contract and declared aggregation logic.

---

### 6.1 Reconciliation and Integrity Rules
At least one end-to-end integrity rule should hold, such as:
- accepted event counts align with published aggregate inputs
- aggregate totals derive only from validated accepted events
- duplicate transactions do not materially inflate published metrics

Violations are treated as hard engineering failures.

---

## PHASE D — STREAMING PIPELINE DESIGN

### 7) Streaming Stages
1. Ingest events from Event Hubs
2. Parse and validate input schema
3. Reject invalid records under declared rules
4. Apply deduplication and event-time controls
5. Compute governed streaming metrics
6. Publish Delta outputs
7. Persist checkpoints for restart recovery
8. Verify outputs through evidence artifacts

**Design principles:**
- Deterministic logic given equivalent input stream and configuration
- Explicit checkpoint-backed restart behaviour
- Governed output publication
- No hidden mutation of accepted event meaning

---

### 8) Transformation Policy

**Allowed:**
- Parsing and schema normalization required for stream ingestion
- Explicit type casting required by contract or engine
- Deduplication using declared event keys and watermark policy
- Aggregate computation for approved metrics
- Writing curated/metric outputs to Delta tables

**Disallowed by default:**
- Silent null filling
- Unexplained record repair
- Ad hoc schema drift acceptance
- Undeclared business rule enrichment
- Hidden backfill logic outside the published processing path

---

## PHASE E — OBSERVABILITY & RELIABILITY

### 9) Run and Processing Evidence
Streaming execution should provide inspectable evidence such as:
- checkpoint locations
- output table locations
- contract/rejection outcomes
- evidence notebook artifacts
- representative verification snapshots

---

### 10) Error Handling & Failure Semantics
- Invalid records are rejected according to declared contract behavior
- Processing failures must not silently publish corrupt outputs
- Restart behavior must be recoverable through checkpointing
- Partial or invalid publishes must be avoided through controlled write paths
- Recovery should occur by rerunning the stream with the same governed logic

---

### 11) Testing
- Contract validation tests for accepted/rejected event behavior
- Aggregation logic tests for governed metrics
- Output schema checks for published Delta tables
- Smoke validation for core processing logic where feasible

---

### 12) Reproducibility & Determinism
- Contract logic is versioned and explicit
- Aggregations are deterministic relative to accepted input events
- Checkpoints govern restart continuity
- Output schemas are fixed and inspectable
- Evidence artifacts support execution verification

---

## Known Limitations
- No external master-data joins
- No production deployment hardening beyond portfolio scope
- No autoscaling or cost-tuning guarantees
- No advanced observability stack beyond project evidence artifacts
- No live operational alert routing

---

## PHASE F — REVIEW & GOVERNANCE

### 13) Evidence Artifacts
- Framework document
- Evidence notebook
- Architecture diagram
- Pipeline flow diagram
- Sample output verification assets
- Published Delta metric outputs
- Checkpoint-backed recovery evidence

---

### 14) Framework Governance
- This document defines the intended architectural contract
- Analytical findings do not alter the framework during execution
- Implementation details may evolve while preserving the declared design intent
- Material design changes should be captured in a future version with a change log

---

## Implementation Notes (Portfolio Execution Context)

This framework was defined to describe the **intended cloud streaming architecture** before implementation.  
During repository execution, several practical engineering additions and packaging decisions were introduced to make the project more reproducible, demonstrable, and recruiter-readable.

These implementation choices do **not change the core architectural intent** of the framework. They explain how the repository evolved from design intent into a portfolio-grade engineering artifact.

### 1. Dedicated Local Event Generator Added

The framework assumes a governed streaming source via Azure Event Hubs.  
In implementation, a dedicated **local event generator layer** was added to make the project testable and demonstrable end-to-end.

This generator exists to:
- produce controlled synthetic transaction events
- make the streaming story reproducible
- support portfolio verification without relying on an external business producer

This is an execution aid, not a change to the streaming architecture.

---

### 2. Framework and Evidence Were Initially Stored as Root Notebooks

The original repo implementation used notebook artifacts directly in the repository root for:
- framework definition
- execution evidence

For a cleaner engineering surface, these are better treated as:
- `framework/stream_framework.*`
- `evidence/evidence_notebook.ipynb`

This does not alter design intent; it is a repository presentation and governance improvement.

---

### 3. Additional Repository Standardization Was Needed

The original implementation emphasized streaming logic first.  
Compared with the intended 987 portfolio standard, the repository required additional structural polish such as:

- dedicated `framework/` and `evidence/` folders
- a clearer `docs/` surface
- explicit screenshot/evidence assets
- stronger root-level consistency for recruiter scanability

These are presentation and maintainability upgrades, not architectural changes.

---

### 4. README and Trust-Signal Surface Expanded Beyond the Original Framework

The framework focuses on architecture and operating rules.  
During implementation, the repository also required a stronger recruiter-facing surface through:

- a structured README
- explicit run instructions
- architecture and pipeline visuals
- sample output/evidence presentation
- clearer explanation of what the project demonstrates

These additions improve accessibility and trust but do not change the system design.

---

### 5. Tests and CI Became Required Portfolio Trust Signals

The original framework defines validation and correctness expectations, but portfolio execution benefits from visible engineering trust signals beyond the runtime design itself.

In implementation, the project may include or should include:
- contract tests
- aggregation/output checks
- smoke tests
- CI validation for stable repo quality

These are engineering assurance layers added around the framework, not changes to the streaming model.

---

### 6. Output Verification Became More Explicit Than Originally Stated

The framework already expects evidence artifacts, but implementation made the need for **visual and inspectable proof** more explicit, such as:

- evidence notebook review
- Delta output inspection
- checkpoint/restart verification
- screenshots of published metrics or storage state

This strengthens auditability and recruiter trust while remaining consistent with the original design.

---

## Summary

This framework remains the **design contract** for the Azure streaming data product.

The implemented repository adds practical engineering layers for:
- reproducibility
- portfolio usability
- evidence visibility
- repository clarity
- recruiter trust

The architecture itself remains the same:

Event source  
→ contract validation  
→ governed stream processing  
→ deduplication and event-time controls  
→ streaming metric computation  
→ Delta publication  
→ checkpoint-backed recovery  
→ evidence-based verification