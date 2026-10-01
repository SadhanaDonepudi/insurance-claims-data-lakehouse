"""Central configuration for the Insurance Claims Data Lakehouse (local replica).

Every path and scale knob lives here so the pipeline, generator, and tests
stay consistent. Scale defaults reproduce the resume's 5M-record volume
locally; tests override with small numbers.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
CATALOG_DB = DATA_DIR / "catalog.db"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

# Production-scale defaults (local run). 1.2M policies + 3.8M claims = 5.0M records.
DEFAULT_N_POLICIES = 1_200_000
DEFAULT_N_CLAIMS = 3_800_000

# Seeded-bad-record counts (exactly countable so the DQ gate can prove 100%
# interception). These are *added* on top of the clean claims.
SEEDED_NULL_POLICY_ID = 5_000
SEEDED_NEGATIVE_PAID_LOSS = 5_000
SEEDED_ORPHAN_CLAIM = 5_000
SEEDED_DUPLICATE_CLAIM = 2_000

LINES_OF_BUSINESS = [
    "Personal Auto",
    "Homeowners",
    "Commercial Property",
    "Commercial Liability",
    "Workers Compensation",
    "Umbrella",
]

US_STATES = [
    "IL", "TX", "CA", "FL", "NY", "OH", "GA", "NC", "MI", "PA",
    "AZ", "WA", "CO", "MA", "NJ", "VA", "IN", "TN", "MO", "WI",
]

CAUSES_OF_LOSS = [
    "Collision", "Fire", "Water Damage", "Theft", "Wind/Hail",
    "Liability - Bodily Injury", "Liability - Property Damage",
    "Equipment Breakdown", "Slip and Fall", "Other",
]

RANDOM_SEED = 42


@dataclass
class PipelineConfig:
    data_dir: Path = DATA_DIR
    n_policies: int = DEFAULT_N_POLICIES
    n_claims: int = DEFAULT_N_CLAIMS
    seed: int = RANDOM_SEED
    chunk_size: int = 500_000

    @property
    def catalog_uri(self) -> str:
        return f"sqlite:///{self.data_dir / 'catalog.db'}"

    @property
    def warehouse_uri(self) -> str:
        return f"file://{self.data_dir / 'warehouse'}"

    def ensure_dirs(self) -> None:
        (self.data_dir / "warehouse").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "raw").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "outputs").mkdir(parents=True, exist_ok=True)
