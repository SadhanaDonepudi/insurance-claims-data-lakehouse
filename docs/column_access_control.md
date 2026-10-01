# Column-Level Access Control (Production Design: AWS Lake Formation)

In production this matrix is enforced by Lake Formation column-level
permissions on the Silver/Gold Iceberg tables. Locally there is no Lake
Formation, so the matrix is **documented, not enforced** — except PII
masking, which *is* enforced in code: Silver tables contain only SHA-256
hashes and masked emails, never raw claimant PII (see `src/masking.py`).

| Column group | Data Engineer | Claims Analyst | Actuary | External Auditor |
|---|---|---|---|---|
| claim_id, policy_id, dates, LOB, status, losses | ✅ | ✅ | ✅ | ✅ (Gold only) |
| claimant_name_hash / claimant_ssn_hash | ✅ | ❌ | ❌ | ❌ |
| masked_email | ✅ | ✅ | ❌ | ❌ |
| Raw PII (Bronze only: names, SSNs, emails) | ✅ (break-glass, audited) | ❌ | ❌ | ❌ |
| Gold aggregates | ✅ | ✅ | ✅ | ✅ |

Rules:
1. Raw PII never leaves Bronze. Silver/Gold are the only analyst-facing layers.
2. Hash columns exist for join/dedup support, not for re-identification;
   access is restricted to engineering roles.
3. Encryption at rest (AWS KMS) and in transit applies to every layer in
   production; local files are unencrypted (no AWS/KMS available locally).
