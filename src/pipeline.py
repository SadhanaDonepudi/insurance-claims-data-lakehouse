"""Bronze -> Silver -> Gold medallion pipeline on real Apache Iceberg tables.

Local engine: pandas + pyarrow writes into pyiceberg tables backed by a
local SQLite catalog and file warehouse (no JVM, no AWS). This replicates
the production design — PySpark on EMR/Glue writing Iceberg, orchestrated
by Step Functions — piece for piece; see docs/architecture.md for the
mapping. Every count produced here is measured from the actual local run
and written to data/outputs/run_summary.json.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import (
    DateType, DoubleType, IntegerType, LongType, NestedField, StringType,
)

from .config import PipelineConfig
from .generator import generate_clean_claims, generate_policies, seed_bad_records
from .masking import apply_silver_masking
from .quality import evaluate_claims, split_valid_quarantine, table_level_checks

NAMESPACE = "lakehouse"


def get_catalog(cfg: PipelineConfig):
    cfg.ensure_dirs()
    return load_catalog(
        "lakehouse-local",
        **{"type": "sql", "uri": cfg.catalog_uri, "warehouse": cfg.warehouse_uri},
    )


def _table(catalog, cfg: PipelineConfig, name: str, schema: Schema,
           partition_spec: PartitionSpec | None = None):
    ident = f"{NAMESPACE}.{name}"
    try:
        return catalog.load_table(ident)
    except Exception:
        kwargs = {"location": f"{cfg.warehouse_uri}/{name}"}
        if partition_spec is not None:
            kwargs["partition_spec"] = partition_spec
        return catalog.create_table(ident, schema=schema, **kwargs)


def _append(table, df: pd.DataFrame, sort_by: list[str] | None = None,
            chunk: int = 500_000) -> None:
    if sort_by:
        df = df.sort_values(sort_by).reset_index(drop=True)
    for start in range(0, len(df), chunk):
        part = df.iloc[start:start + chunk]
        table.append(pa.Table.from_pandas(part, preserve_index=False))


def _dates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[c] = pd.to_datetime(out[c]).dt.date
    return out


# ---------------------------------------------------------------- schemas
def bronze_policies_schema() -> Schema:
    return Schema(
        NestedField(1, "policy_id", StringType(), required=False),
        NestedField(2, "policyholder_name", StringType(), required=False),
        NestedField(3, "policyholder_ssn", StringType(), required=False),
        NestedField(4, "line_of_business", StringType(), required=False),
        NestedField(5, "state", StringType(), required=False),
        NestedField(6, "effective_date", DateType(), required=False),
        NestedField(7, "expiry_date", DateType(), required=False),
        NestedField(8, "annual_premium", DoubleType(), required=False),
        NestedField(9, "coverage_limit", LongType(), required=False),
    )


def bronze_claims_schema() -> Schema:
    return Schema(
        NestedField(1, "claim_id", StringType(), required=False),
        NestedField(2, "policy_id", StringType(), required=False),
        NestedField(3, "loss_date", DateType(), required=False),
        NestedField(4, "report_date", DateType(), required=False),
        NestedField(5, "line_of_business", StringType(), required=False),
        NestedField(6, "claim_status", StringType(), required=False),
        NestedField(7, "paid_loss", DoubleType(), required=False),
        NestedField(8, "reserved_loss", DoubleType(), required=False),
        NestedField(9, "claimant_name", StringType(), required=False),
        NestedField(10, "claimant_ssn", StringType(), required=False),
        NestedField(11, "claimant_email", StringType(), required=False),
        NestedField(12, "cause_of_loss", StringType(), required=False),
    )


def silver_claims_schema() -> Schema:
    return Schema(
        NestedField(1, "claim_id", StringType(), required=False),
        NestedField(2, "policy_id", StringType(), required=False),
        NestedField(3, "loss_date", DateType(), required=False),
        NestedField(4, "report_date", DateType(), required=False),
        NestedField(5, "line_of_business", StringType(), required=False),
        NestedField(6, "claim_status", StringType(), required=False),
        NestedField(7, "paid_loss", DoubleType(), required=False),
        NestedField(8, "reserved_loss", DoubleType(), required=False),
        NestedField(9, "claimant_name_hash", StringType(), required=False),
        NestedField(10, "claimant_ssn_hash", StringType(), required=False),
        NestedField(11, "masked_email", StringType(), required=False),
        NestedField(12, "cause_of_loss", StringType(), required=False),
    )


def silver_policies_schema() -> Schema:
    return Schema(
        NestedField(1, "policy_id", StringType(), required=False),
        NestedField(2, "line_of_business", StringType(), required=False),
        NestedField(3, "state", StringType(), required=False),
        NestedField(4, "effective_date", DateType(), required=False),
        NestedField(5, "expiry_date", DateType(), required=False),
        NestedField(6, "annual_premium", DoubleType(), required=False),
        NestedField(7, "coverage_limit", LongType(), required=False),
        NestedField(8, "policyholder_name_hash", StringType(), required=False),
        NestedField(9, "policyholder_ssn_hash", StringType(), required=False),
    )


def quarantine_schema() -> Schema:
    fields = list(bronze_claims_schema().fields)
    fields.append(NestedField(13, "quarantine_reason", StringType(), required=False))
    return Schema(*fields)


def gold_monthly_schema() -> Schema:
    return Schema(
        NestedField(1, "loss_month", DateType(), required=False),
        NestedField(2, "line_of_business", StringType(), required=False),
        NestedField(3, "claim_count", LongType(), required=False),
        NestedField(4, "total_paid_loss", DoubleType(), required=False),
        NestedField(5, "avg_paid_loss", DoubleType(), required=False),
        NestedField(6, "open_claims", LongType(), required=False),
        NestedField(7, "closed_claims", LongType(), required=False),
    )


def gold_lob_schema() -> Schema:
    return Schema(
        NestedField(1, "loss_year", IntegerType(), required=False),
        NestedField(2, "line_of_business", StringType(), required=False),
        NestedField(3, "claim_count", LongType(), required=False),
        NestedField(4, "total_paid_loss", DoubleType(), required=False),
        NestedField(5, "total_reserved_loss", DoubleType(), required=False),
        NestedField(6, "policy_count", LongType(), required=False),
        NestedField(7, "total_annual_premium", DoubleType(), required=False),
        NestedField(8, "paid_loss_ratio", DoubleType(), required=False),
    )


def _claim_partition_spec(schema: Schema) -> PartitionSpec:
    """Partition Silver/Gold-adjacent claim tables by LOB + month(loss_date),
    mirroring the production partitioning on loss date and line of business."""
    lob_id = schema.find_field("line_of_business").field_id
    date_id = schema.find_field("loss_date").field_id
    return PartitionSpec(
        PartitionField(source_id=lob_id, field_id=1000, transform=IdentityTransform(), name="line_of_business"),
        PartitionField(source_id=date_id, field_id=1001, transform=MonthTransform(), name="loss_month"),
    )


# ---------------------------------------------------------------- pipeline
def run_pipeline(cfg: PipelineConfig) -> dict:
    started = time.perf_counter()
    cfg.ensure_dirs()
    catalog = get_catalog(cfg)
    catalog.create_namespace_if_not_exists(NAMESPACE)

    # ---- Step 1 (production: Glue/EMR ingest): generate synthetic raw extracts
    policies = generate_policies(cfg)
    policy_ids = policies["policy_id"].to_numpy()
    clean_claims = generate_clean_claims(cfg, policy_ids)
    bronze_claims_df, manifest = seed_bad_records(clean_claims, policy_ids, cfg)

    # ---- Step 2: Bronze (raw, as-landed Iceberg tables)
    bronze_policies = _table(catalog, cfg, "bronze_policies", bronze_policies_schema())
    bronze_claims = _table(catalog, cfg, "bronze_claims", bronze_claims_schema())
    _append(bronze_policies, _dates(policies, ["effective_date", "expiry_date"]))
    _append(bronze_claims, _dates(bronze_claims_df, ["loss_date", "report_date"]),
            sort_by=["claim_id"])

    # ---- Step 3: Data-quality gate (production: Glue DQ + Great Expectations)
    evaluated = evaluate_claims(bronze_claims_df, set(policy_ids.tolist()))
    valid, quarantined = split_valid_quarantine(evaluated)
    quarantine_tbl = _table(catalog, cfg, "quarantine_claims", quarantine_schema())
    q_cols = list(bronze_claims_df.columns) + ["quarantine_reason"]
    _append(quarantine_tbl, _dates(quarantined[q_cols], ["loss_date", "report_date"]))

    # ---- Step 4: Silver (cleaned, deduplicated, PII-masked, partitioned)
    silver_claims_df = apply_silver_masking(valid.drop(columns=["dq_passed", "quarantine_reason"]))
    silver_claims_tbl = _table(catalog, cfg, "silver_claims", silver_claims_schema(),
                               partition_spec=_claim_partition_spec(silver_claims_schema()))
    _append(silver_claims_tbl,
            _dates(silver_claims_df, ["loss_date", "report_date"]),
            sort_by=["line_of_business", "loss_date"])

    silver_policies_df = policies.rename(columns={
        "policyholder_name": "policyholder_name_raw",
        "policyholder_ssn": "policyholder_ssn_raw",
    })
    from .masking import sha256_hash
    silver_policies_df["policyholder_name_hash"] = silver_policies_df["policyholder_name_raw"].map(sha256_hash)
    silver_policies_df["policyholder_ssn_hash"] = silver_policies_df["policyholder_ssn_raw"].map(sha256_hash)
    silver_policies_df = silver_policies_df.drop(
        columns=["policyholder_name_raw", "policyholder_ssn_raw"])
    silver_policies_tbl = _table(catalog, cfg, "silver_policies", silver_policies_schema())
    _append(silver_policies_tbl, _dates(silver_policies_df, ["effective_date", "expiry_date"]))

    # ---- Step 5: Gold (curated, analysis-ready aggregates)
    silver = silver_claims_df.copy()
    silver["loss_date_ts"] = pd.to_datetime(silver["loss_date"])
    monthly = (
        silver.assign(loss_month=silver["loss_date_ts"].dt.to_period("M").dt.to_timestamp().dt.date)
        .groupby(["loss_month", "line_of_business"], as_index=False)
        .agg(claim_count=("claim_id", "count"), total_paid_loss=("paid_loss", "sum"),
             avg_paid_loss=("paid_loss", "mean"),
             open_claims=("claim_status", lambda s: int((s == "Open").sum())),
             closed_claims=("claim_status", lambda s: int((s == "Closed").sum())))
    )
    gold_monthly_tbl = _table(catalog, cfg, "gold_claims_monthly", gold_monthly_schema())
    _append(gold_monthly_tbl, monthly, sort_by=["line_of_business", "loss_month"])

    prem = (silver_policies_df.groupby("line_of_business", as_index=False)
            .agg(policy_count=("policy_id", "count"),
                 total_annual_premium=("annual_premium", "sum")))
    lob = (
        silver.assign(loss_year=silver["loss_date_ts"].dt.year)
        .groupby(["loss_year", "line_of_business"], as_index=False)
        .agg(claim_count=("claim_id", "count"), total_paid_loss=("paid_loss", "sum"),
             total_reserved_loss=("reserved_loss", "sum"))
        .merge(prem, on="line_of_business", how="left")
    )
    lob["paid_loss_ratio"] = lob["total_paid_loss"] / lob["total_annual_premium"]
    gold_lob_tbl = _table(catalog, cfg, "gold_loss_by_lob_year", gold_lob_schema())
    _append(gold_lob_tbl, lob, sort_by=["line_of_business", "loss_year"])

    # ---- Step 6: reconciliation + run report (production: SNS alert payload)
    elapsed = time.perf_counter() - started
    gold_counts = {"gold_claims_monthly": int(len(monthly)), "gold_loss_by_lob_year": int(len(lob))}
    checks = table_level_checks(int(len(bronze_claims_df)), int(len(silver_claims_df)),
                                int(len(quarantined)), gold_counts)
    summary = {
        "runtime_seconds": round(elapsed, 2),
        "n_policies": int(len(policies)),
        "generator_manifest": manifest,
        "bronze_claims": int(len(bronze_claims_df)),
        "silver_claims": int(len(silver_claims_df)),
        "quarantined_claims": int(len(quarantined)),
        "quarantine_rate": round(len(quarantined) / max(len(bronze_claims_df), 1), 6),
        "seeded_bad_total": manifest["total_seeded_bad"],
        "quarantined_vs_seeded_equal": int(len(quarantined)) == manifest["total_seeded_bad"],
        "checks": checks,
        "gold_tables": gold_counts,
        "iceberg_tables": ["bronze_policies", "bronze_claims", "silver_policies",
                           "silver_claims", "quarantine_claims",
                           "gold_claims_monthly", "gold_loss_by_lob_year"],
    }
    out_path = cfg.data_dir / "outputs" / "run_summary.json"
    out_path.write_text(json.dumps(summary, indent=2))
    return summary
