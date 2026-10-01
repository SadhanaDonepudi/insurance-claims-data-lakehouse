"""Synthetic P&C policy + claims generator (100% synthetic data).

Generates the Bronze-layer raw extracts at production scale:
1.2M policies + 3.8M claims = 5.0M records by default, plus exactly
countable seeded bad records so the data-quality gate can prove 100%
interception:

- null policy IDs          (SEEDED_NULL_POLICY_ID)
- negative paid losses     (SEEDED_NEGATIVE_PAID_LOSS)
- orphan claims            (SEEDED_ORPHAN_CLAIM: policy_id not in policies)
- duplicate claim IDs      (SEEDED_DUPLICATE_CLAIM: grain violations)

All names, SSNs, and emails are fabricated. Nothing here is real claimant
data. Generation is chunked and vectorized (numpy) so the full 5M-record
run fits comfortably in local memory.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .config import (
    CAUSES_OF_LOSS,
    LINES_OF_BUSINESS,
    SEEDED_DUPLICATE_CLAIM,
    SEEDED_NEGATIVE_PAID_LOSS,
    SEEDED_NULL_POLICY_ID,
    SEEDED_ORPHAN_CLAIM,
    US_STATES,
    PipelineConfig,
)

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer",
               "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
               "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
               "Charles", "Karen", "Daniel", "Nancy", "Matthew", "Lisa",
               "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
              "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
              "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
              "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
              "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"]


def _names(rng: np.random.Generator, n: int) -> np.ndarray:
    first = rng.choice(FIRST_NAMES, size=n)
    last = rng.choice(LAST_NAMES, size=n)
    return np.char.add(np.char.add(first, " "), last)


def _ssns(rng: np.random.Generator, n: int) -> np.ndarray:
    # Fabricated SSN-shaped strings; area numbers avoid real SSA ranges.
    area = rng.integers(900, 1000, size=n).astype(str)
    group = np.array([f"{v:02d}" for v in rng.integers(10, 100, size=n)])
    serial = np.array([f"{v:04d}" for v in rng.integers(1000, 10000, size=n)])
    return np.char.add(np.char.add(np.char.add(area, "-"), np.char.add(group, "-")), serial)


def _emails(names: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    domains = rng.choice(["example.com", "mail.example.org", "test.example.net"], size=len(names))
    local = np.char.lower(np.char.replace(names.astype(str), " ", "."))
    return np.char.add(np.char.add(local, "@"), domains.astype(str))


def generate_policies(cfg: PipelineConfig) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.seed)
    n = cfg.n_policies
    effective = pd.to_datetime("2019-01-01") + pd.to_timedelta(rng.integers(0, 2555, size=n), unit="D")
    df = pd.DataFrame({
        "policy_id": [f"POL-{i:07d}" for i in range(1, n + 1)],
        "policyholder_name": _names(rng, n),
        "policyholder_ssn": _ssns(rng, n),
        "line_of_business": rng.choice(LINES_OF_BUSINESS, size=n, p=[0.30, 0.25, 0.15, 0.12, 0.10, 0.08]),
        "state": rng.choice(US_STATES, size=n),
        "effective_date": effective.date,
        "expiry_date": (effective + pd.to_timedelta(365, unit="D")).date,
        "annual_premium": np.round(rng.lognormal(mean=7.0, sigma=0.6, size=n), 2),
        "coverage_limit": rng.choice([50_000, 100_000, 250_000, 500_000, 1_000_000], size=n),
    })
    return df


def generate_clean_claims(cfg: PipelineConfig, policy_ids: np.ndarray) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.seed + 1)
    n = cfg.n_claims
    # Loss dates span 2019-01-01 .. 2026-09-30 (inside the DQ validity window).
    loss_dates = pd.to_datetime("2019-01-01") + pd.to_timedelta(rng.integers(0, 2829, size=n), unit="D")
    report_lag = rng.integers(0, 90, size=n)
    paid = np.round(rng.lognormal(mean=8.0, sigma=1.1, size=n), 2)
    claimant_names = _names(rng, n)
    df = pd.DataFrame({
        "claim_id": [f"CLM-{i:08d}" for i in range(1, n + 1)],
        "policy_id": rng.choice(policy_ids, size=n),
        "loss_date": loss_dates.date,
        "report_date": (loss_dates + pd.to_timedelta(report_lag, unit="D")).date,
        "line_of_business": rng.choice(LINES_OF_BUSINESS, size=n, p=[0.30, 0.25, 0.15, 0.12, 0.10, 0.08]),
        "claim_status": rng.choice(["Closed", "Open"], size=n, p=[0.78, 0.22]),
        "paid_loss": paid,
        "reserved_loss": np.round(paid * rng.uniform(0.0, 0.4, size=n), 2),
        "claimant_name": claimant_names,
        "claimant_ssn": _ssns(rng, n),
        "claimant_email": _emails(claimant_names, rng),
        "cause_of_loss": rng.choice(CAUSES_OF_LOSS, size=n),
    })
    return df


def seed_bad_records(clean: pd.DataFrame, policy_ids: np.ndarray,
                     cfg: PipelineConfig) -> tuple[pd.DataFrame, dict]:
    """Append exactly countable bad records; return (combined, manifest)."""
    rng = np.random.default_rng(cfg.seed + 2)
    base = clean.iloc[0]
    next_id = len(clean) + 1
    frames: list[pd.DataFrame] = []
    manifest = {
        "clean_claims": int(len(clean)),
        "seeded_null_policy_id": SEEDED_NULL_POLICY_ID,
        "seeded_negative_paid_loss": SEEDED_NEGATIVE_PAID_LOSS,
        "seeded_orphan_claim": SEEDED_ORPHAN_CLAIM,
        "seeded_duplicate_claim": SEEDED_DUPLICATE_CLAIM,
    }

    def _batch(count: int, mutate) -> pd.DataFrame:
        nonlocal next_id
        rows = clean.sample(n=min(count, len(clean)), random_state=cfg.seed).copy()
        rows["claim_id"] = [f"CLM-{next_id + i:08d}" for i in range(len(rows))]
        next_id += len(rows)
        mutate(rows, rng)
        return rows

    frames.append(_batch(SEEDED_NULL_POLICY_ID,
                         lambda r, g: r.__setitem__("policy_id", None)))
    frames.append(_batch(SEEDED_NEGATIVE_PAID_LOSS,
                         lambda r, g: r.__setitem__("paid_loss", -np.abs(r["paid_loss"].values) - 1.0)))
    frames.append(_batch(SEEDED_ORPHAN_CLAIM,
                         lambda r, g: r.__setitem__(
                             "policy_id", [f"POL-9{i:06d}" for i in range(len(r))])))

    # Duplicates: exact copies of existing clean rows (same claim_id).
    dup = clean.sample(n=min(SEEDED_DUPLICATE_CLAIM, len(clean)),
                       random_state=cfg.seed + 3).copy()
    frames.append(dup)

    combined = pd.concat([clean, *frames], ignore_index=True)
    manifest["total_bronze_claims"] = int(len(combined))
    manifest["total_seeded_bad"] = int(
        SEEDED_NULL_POLICY_ID + SEEDED_NEGATIVE_PAID_LOSS
        + SEEDED_ORPHAN_CLAIM + SEEDED_DUPLICATE_CLAIM)
    return combined, manifest
