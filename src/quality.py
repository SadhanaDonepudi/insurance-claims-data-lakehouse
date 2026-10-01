"""Data-quality gate (Great Expectations-style rules, PySpark-free).

Every rule is a vectorized boolean mask. Records failing any rule are routed
to the quarantine table with a machine-readable reason; nothing failing
reaches Silver/Gold. Table-level checks (row counts, grain) run alongside
the row-level rules.

Local replica mapping: in production these rules run as AWS Glue Data
Quality + Great Expectations checks with SNS alerts on failure (see
docs/architecture.md). Locally the same rule logic runs in pandas and the
"alert" is a structured JSON run report + non-zero exit on gate failure.
"""
from __future__ import annotations

import pandas as pd

RULE_NULL_POLICY_ID = "null_policy_id"
RULE_NEGATIVE_PAID_LOSS = "negative_paid_loss"
RULE_ORPHAN_CLAIM = "orphan_claim"
RULE_DUPLICATE_CLAIM_ID = "duplicate_claim_id"
RULE_INVALID_LOSS_DATE = "invalid_loss_date"


def evaluate_claims(claims: pd.DataFrame, valid_policy_ids: set[str],
                    reference_date: pd.Timestamp | None = None) -> pd.DataFrame:
    """Add `dq_passed` (bool) and `quarantine_reason` (';'-joined rules)."""
    if reference_date is None:
        reference_date = pd.Timestamp("2026-10-01")
    out = claims.copy()
    loss_dates = pd.to_datetime(out["loss_date"], errors="coerce")
    policy_present = out["policy_id"].notna() & (out["policy_id"].astype(str).str.strip() != "")

    checks = {
        RULE_NULL_POLICY_ID: ~policy_present,
        RULE_NEGATIVE_PAID_LOSS: pd.to_numeric(out["paid_loss"], errors="coerce").fillna(0) < 0,
        RULE_ORPHAN_CLAIM: policy_present & ~out["policy_id"].isin(valid_policy_ids),
        RULE_DUPLICATE_CLAIM_ID: out.duplicated(subset=["claim_id"], keep="first"),
        RULE_INVALID_LOSS_DATE: loss_dates.isna() | (loss_dates > reference_date)
        | (loss_dates < pd.Timestamp("2018-01-01")),
    }
    reason = pd.Series("", index=out.index, dtype=object)
    any_fail = pd.Series(False, index=out.index)
    for name, mask in checks.items():
        reason = reason.where(~mask, reason + name + ";")
        any_fail = any_fail | mask
    out["quarantine_reason"] = reason.str.rstrip(";")
    out["dq_passed"] = ~any_fail
    return out


def split_valid_quarantine(evaluated: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    passed = evaluated[evaluated["dq_passed"]].copy()
    failed = evaluated[~evaluated["dq_passed"]].copy()
    return passed, failed


def table_level_checks(bronze_claims: int, silver_claims: int, quarantined: int,
                       gold_tables: dict[str, int]) -> dict:
    """Grain / row-count reconciliation across the medallion."""
    return {
        "bronze_claims": bronze_claims,
        "silver_claims": silver_claims,
        "quarantined_claims": quarantined,
        "reconciles": bronze_claims == silver_claims + quarantined,
        "gold_tables": gold_tables,
        "gold_nonempty": all(v > 0 for v in gold_tables.values()),
    }
