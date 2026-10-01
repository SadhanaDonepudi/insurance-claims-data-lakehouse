# Architecture — Insurance Claims Data Lakehouse on AWS (Local Replica)

> **Honesty note.** No AWS account is available in this environment. Everything
> in this repository runs locally. This document maps each local component to
> the production AWS service it replicates, and states plainly which resume
> figures are production-design metrics that were **not** reproduced locally.

## Production design (what the resume describes)

```
P&C source systems (policy admin, claims admin)
        │  raw extracts (S3 landing, KMS-encrypted)
        ▼
AWS Step Functions  ── orchestrates every step, error states + SNS alerts
        │
        ├─► AWS Glue crawlers / jobs ──► Bronze Iceberg tables (raw, as-landed)
        ├─► Amazon EMR (PySpark) ──────► Silver Iceberg tables
        │      partitioned by loss date + line of business,
        │      small-file compaction, Spot task nodes
        ├─► Glue Data Quality + Great Expectations ──► quarantine + SNS alert
        └─► Amazon EMR (PySpark) ──────► Gold Iceberg tables ──► Athena queries
Security: AWS KMS encryption at rest · Silver-layer PII masking ·
          Lake Formation column-level access control
```

## Local replica (what this repo actually runs)

| Production component | Local replica in this repo | File |
|---|---|---|
| S3 landing + Bronze Iceberg | pyiceberg tables (`bronze_policies`, `bronze_claims`) on a local file warehouse with a SQLite catalog | `src/pipeline.py` |
| Glue/EMR PySpark transforms | pandas + pyarrow medallion transforms (no JVM available locally, so PySpark cannot run here) | `src/pipeline.py`, `src/masking.py` |
| Iceberg partitioning (loss date, LOB) | Real partition spec: `line_of_business` identity + `month(loss_date)` on `silver_claims` | `src/pipeline.py::_claim_partition_spec` |
| Glue Data Quality + Great Expectations | Vectorized rule engine: null policy ID, negative paid loss, orphan-claim referential check, duplicate-claim grain check, loss-date validity | `src/quality.py` |
| Quarantine + SNS alert | `quarantine_claims` Iceberg table with machine-readable reasons; non-zero exit + JSON run report as the alert payload | `src/pipeline.py`, `src/run_pipeline.py` |
| Step Functions orchestration | Ordered state runner logging each state (`Generate → Bronze → DQ Gate → Silver → Gold → Reconcile`) | `src/run_pipeline.py` |
| Athena ad-hoc queries | DuckDB / pyiceberg table scans against the local warehouse | `src/pipeline.py` (scan API) |
| KMS encryption at rest | **Not reproduced.** Local files are unencrypted; documented as production design only | — |
| Lake Formation column-level access | **Documented, not enforced.** Role/column matrix in `docs/column_access_control.md`; Silver masking (SHA-256) *is* enforced in code | `src/masking.py` |

## Which resume figures are which

| Resume figure | Status |
|---|---|
| 5M+ policy + claims records into curated Iceberg tables | **Reproduced locally** at full scale: 1.2M policies + 3.8M claims + 17,000 seeded bad = 5,017,000 Bronze claim/policy records processed; real partitioned Iceberg tables written |
| Runtime 52 → 14 min, compute cost −40% (EMR partitioning, compaction, Spot) | **AWS production-design metric — NOT reproduced locally.** No EMR exists here. The local full-run wall time is measured and reported in the README as a *local* figure only |
| 100% of seeded bad records stopped before Gold (Glue DQ + GE + SNS) | **Reproduced locally:** seeded-bad count vs quarantined count are both printed by the run and asserted equal in tests; the SNS alert is represented by the JSON run report + failing exit code |
| KMS encryption, Silver masking, Lake Formation column controls | Masking **reproduced** (SHA-256 in Silver); KMS and Lake Formation enforcement are **production-design only** (no AWS locally), specified in `docs/column_access_control.md` |

## Medallion flow

```
generator.py (synthetic, seeded bad records)
   └─► Bronze (raw Iceberg, PII as-landed)
         └─► DQ gate ──fail──► quarantine_claims (reason-coded)
               └─pass─► Silver (deduped, PII SHA-256-masked, partitioned)
                           └─► Gold (monthly + LOB/year aggregates)
```

## Failure handling

- Any reconciliation mismatch (Bronze ≠ Silver + Quarantine, quarantined ≠
  seeded-bad, empty Gold) exits non-zero — the local analog of a Step
  Functions error state firing an SNS alert.
- Iceberg's ACID snapshots mean a failed append never corrupts a table;
  each layer can be re-run from the previous layer's snapshot.
