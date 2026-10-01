"""SHA-256 PII masking applied in the Silver layer.

Bronze retains raw synthetic PII (claimant name / SSN / email) exactly as
landed. Silver never carries raw PII forward:

- claimant_name  -> claimant_name_hash  (SHA-256 hex)
- claimant_ssn   -> claimant_ssn_hash   (SHA-256 hex)
- claimant_email -> masked_email        (first char + ***@domain)

Hashing is one-way and deterministic, so joins/aggregations on the hashed
value still work without exposing the underlying identifier. This mirrors
the production design's Silver-layer masking step (see
docs/column_access_control.md for which roles may see which columns).
"""
from __future__ import annotations

import hashlib

import pandas as pd


def sha256_hash(value: object) -> str | None:
    """Deterministic SHA-256 hex digest; None/NaN passes through as None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def mask_email(email: object) -> str | None:
    """jane.doe@example.com -> j***@example.com. Invalid input -> None."""
    if email is None or (isinstance(email, float) and pd.isna(email)):
        return None
    text = str(email)
    if "@" not in text or len(text) < 3:
        return None
    local, domain = text.split("@", 1)
    if not local or not domain:
        return None
    return f"{local[0]}***@{domain}"


def apply_silver_masking(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with raw PII columns replaced by masked derivatives."""
    out = df.copy()
    if "claimant_name" in out.columns:
        out["claimant_name_hash"] = out["claimant_name"].map(sha256_hash)
        out = out.drop(columns=["claimant_name"])
    if "claimant_ssn" in out.columns:
        out["claimant_ssn_hash"] = out["claimant_ssn"].map(sha256_hash)
        out = out.drop(columns=["claimant_ssn"])
    if "claimant_email" in out.columns:
        out["masked_email"] = out["claimant_email"].map(mask_email)
        out = out.drop(columns=["claimant_email"])
    return out
