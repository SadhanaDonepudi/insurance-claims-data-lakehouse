"""Tests run on a small synthetic scale against a temp Iceberg warehouse."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import PipelineConfig
from src.generator import generate_clean_claims, generate_policies, seed_bad_records
from src.masking import apply_silver_masking, mask_email, sha256_hash
from src.pipeline import run_pipeline
from src.quality import (
    RULE_DUPLICATE_CLAIM_ID, RULE_NEGATIVE_PAID_LOSS, RULE_NULL_POLICY_ID,
    RULE_ORPHAN_CLAIM, evaluate_claims, split_valid_quarantine,
)


@pytest.fixture(scope="module")
def small_cfg(tmp_path_factory):
    cfg = PipelineConfig(data_dir=tmp_path_factory.mktemp("lakehouse"),
                         n_policies=2_000, n_claims=5_000)
    return cfg


@pytest.fixture(scope="module")
def small_frames(small_cfg):
    policies = generate_policies(small_cfg)
    claims = generate_clean_claims(small_cfg, policies["policy_id"].to_numpy())
    combined, manifest = seed_bad_records(claims, policies["policy_id"].to_numpy(), small_cfg)
    return policies, claims, combined, manifest


def test_generator_scale_and_seed_counts(small_frames):
    policies, claims, combined, manifest = small_frames
    assert len(policies) == 2_000
    assert len(claims) == 5_000
    # Seeded bad appended on top: combined grows by exactly the seeded total,
    # except duplicates reuse clean rows (still appended as extra rows).
    assert len(combined) == 5_000 + manifest["total_seeded_bad"]
    assert manifest["total_seeded_bad"] == 17_000 or manifest["total_seeded_bad"] > 0


def test_generator_policy_ids_unique(small_cfg):
    policies = generate_policies(small_cfg)
    assert policies["policy_id"].is_unique
    assert policies["annual_premium"].gt(0).all()


def test_each_dq_rule_fires(small_frames):
    policies, _, combined, _ = small_frames
    evaluated = evaluate_claims(combined, set(policies["policy_id"]))
    reasons = evaluated.loc[~evaluated["dq_passed"], "quarantine_reason"]
    joined = ";".join(reasons)
    assert RULE_NULL_POLICY_ID in joined
    assert RULE_NEGATIVE_PAID_LOSS in joined
    assert RULE_ORPHAN_CLAIM in joined
    assert RULE_DUPLICATE_CLAIM_ID in joined


def test_quarantine_intercepts_all_seeded_bad(small_frames):
    policies, _, combined, manifest = small_frames
    evaluated = evaluate_claims(combined, set(policies["policy_id"]))
    valid, quarantined = split_valid_quarantine(evaluated)
    assert len(quarantined) == manifest["total_seeded_bad"]
    assert len(valid) == manifest["clean_claims"]
    assert len(valid) + len(quarantined) == len(combined)


def test_no_bad_records_in_valid_split(small_frames):
    policies, _, combined, _ = small_frames
    evaluated = evaluate_claims(combined, set(policies["policy_id"]))
    valid, _ = split_valid_quarantine(evaluated)
    assert valid["policy_id"].notna().all()
    assert (valid["paid_loss"] >= 0).all()
    assert valid["policy_id"].isin(set(policies["policy_id"])).all()
    assert valid["claim_id"].is_unique


def test_masking_hashes_and_drops_raw_pii():
    df = pd.DataFrame({
        "claimant_name": ["Jane Synthetic"],
        "claimant_ssn": ["900-12-3456"],
        "claimant_email": ["jane.synthetic@example.com"],
    })
    masked = apply_silver_masking(df)
    assert "claimant_name" not in masked.columns
    assert "claimant_ssn" not in masked.columns
    assert "claimant_email" not in masked.columns
    assert masked.loc[0, "claimant_name_hash"] == sha256_hash("Jane Synthetic")
    assert len(masked.loc[0, "claimant_ssn_hash"]) == 64
    assert masked.loc[0, "masked_email"] == "j***@example.com"
    assert mask_email("bad-email") is None
    assert sha256_hash(None) is None


def test_medallion_counts_reconcile(small_cfg):
    summary = run_pipeline(small_cfg)
    assert summary["checks"]["reconciles"]
    assert summary["bronze_claims"] == summary["silver_claims"] + summary["quarantined_claims"]
    assert summary["quarantined_vs_seeded_equal"]
    assert summary["checks"]["gold_nonempty"]
    assert summary["gold_tables"]["gold_claims_monthly"] > 0
