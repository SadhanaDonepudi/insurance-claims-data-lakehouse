# Insurance Claims Data Lakehouse on AWS — Local Replica

> ## ⚠️ SYNTHETIC DATA
> **Every policy, claim, claimant name, SSN, and email in this project is
> SYNTHETIC** — generated deterministically (`src/generator.py`, fixed
> seed) to mirror the shape of a real property & casualty claims pipeline.
> No real claimant, policyholder, or insurer data is used anywhere.

A masters-level data-engineering project: a Bronze → Silver → Gold
medallion pipeline that transforms raw P&C policy and claims records into
curated, analysis-ready **Apache Iceberg** tables, with a data-quality gate
that quarantines 100% of seeded bad records before they can reach Gold.

**No AWS is available in this environment — everything runs locally.** The
local engine is pandas + pyarrow writing **real Iceberg tables** through
pyiceberg (local SQLite catalog, file warehouse; no JVM, so PySpark cannot
run here). Each local component replicates one production AWS service;
[`docs/architecture.md`](docs/architecture.md) maps them one-to-one.

## Resume claims → what this repo actually does

| # | Resume claim (AIG Data Engineering Analyst resume) | In this repo |
|---|---|---|
| 1 | 5M+ raw P&C policy/claims records → curated Iceberg tables via Bronze/Silver/Gold PySpark on EMR + Glue, Step Functions orchestration | ✅ **Reproduced locally at full scale** — 1.2M policies + 3.8M claims generated; real partitioned Iceberg tables written. Engine is pandas/pyarrow (no JVM locally), orchestration is a Step Functions-style state runner (`src/run_pipeline.py`) |
| 2 | Runtime 52 → 14 min, compute cost −40% (partitioning, compaction, EMR Spot) | ⚠️ **AWS production-design metric — NOT reproduced locally.** No EMR exists here. Local wall time is measured and reported below as a *local* figure only |
| 3 | 100% of seeded bad records (null policy IDs, negative paid losses, orphan claims) stopped before Gold via Glue DQ + Great Expectations + SNS alerts | ✅ **Reproduced locally** — rule engine in `src/quality.py`; seeded-vs-quarantined counts printed by the run, asserted equal in tests; "SNS alert" = JSON run report + non-zero exit |
| 4 | KMS encryption, Silver-layer PII masking, Lake Formation column-level controls | 🔶 **Partially reproduced** — Silver PII masking (SHA-256) is real code (`src/masking.py`). KMS encryption and Lake Formation enforcement are production-design only (no AWS locally); the access matrix is specified in `docs/column_access_control.md` |

## Measured results (actual local run)

| Metric | Measured value (local run) |
|---|---|
| Policies generated | 1,200,000 |
| Clean claims generated | 3,800,000 |
| Seeded bad records | 17,000 (5,000 null policy ID · 5,000 negative paid loss · 5,000 orphan claim · 2,000 duplicate claim ID) |
| Bronze claims (incl. seeded bad) | 3,817,000 |
| Silver claims (DQ-passing, deduped, masked) | 3,800,000 |
| Quarantined claims | 17,000 — **100% of seeded bad records intercepted; 0 reached Gold** |
| Quarantine rate | 0.4454% of Bronze claims |
| Gold tables | `gold_claims_monthly`: 558 rows · `gold_loss_by_lob_year`: 48 rows |
| Total records processed | 5,017,000 (1.2M policies + 3.817M claims) |
| Local wall time (2 CPU, 8 GB RAM) | 183.93 seconds |
| Reconciliation | Bronze = Silver + Quarantine ✔ · quarantined = seeded bad ✔ |

Reproduce: `python -m src.run_pipeline` (writes `data/outputs/run_summary.json`).
The 52→14 min / −40% cost figures from the resume are AWS production-design
metrics and are **not** claimed here — the only runtime reported is the
measured local wall time above.

## Architecture

```
Synthetic extracts (src/generator.py, seeded bad records)
   └─► Bronze Iceberg tables (raw, PII as-landed)
         └─► Data-quality gate (src/quality.py)
               ├─fail─► quarantine_claims (reason-coded, never reaches Gold)
               └─pass─► Silver Iceberg (deduped, SHA-256-masked,
               │        partitioned by line_of_business + month(loss_date))
               └──────► Gold Iceberg (monthly + LOB/year loss aggregates)
```

Production mapping (EMR/Glue/Step Functions/Athena/KMS/Lake Formation/SNS):
see [`docs/architecture.md`](docs/architecture.md) ·
Data dictionary: [`docs/data_dictionary.md`](docs/data_dictionary.md) ·
Access control: [`docs/column_access_control.md`](docs/column_access_control.md)

## Repository layout

```
├── src/        config · generator · quality · masking · pipeline · run_pipeline
├── tests/      pytest suite (generators, every DQ rule, masking, reconciliation)
├── docs/       architecture · data dictionary · column access control
├── sample/     100-row sample extracts (full data regenerates via the pipeline)
└── data/       generated warehouse + catalog (git-ignored, regenerable)
```

## How to run

```bash
pip install -r requirements.txt
python -m src.run_pipeline            # full 5M-record scale
python -m src.run_pipeline --policies 2000 --claims 5000   # quick smoke run
python -m pytest tests/ -q
```

The run writes `data/outputs/run_summary.json` with every measured count
(rows per layer, seeded vs quarantined, quarantine rate, runtime) and exits
non-zero if reconciliation fails — the local stand-in for the production
Step Functions error state + SNS alert.

## Honest limitations

- PySpark/EMR/Glue/Step Functions/Athena never ran — no AWS, no JVM here.
  The pipeline logic (medallion transforms, DQ rules, partitioning,
  masking) is real; the runtime platform is a local replica.
- The 52→14 min and −40% cost figures are production-design metrics from
  the resume's AWS design. They are documented, never faked: the only
  runtime this repo reports is the measured local wall time above.
- KMS encryption and Lake Formation enforcement cannot exist without AWS;
  both are specified (`docs/column_access_control.md`), and the masking
  half of that claim is enforced in code today.
