# Data Dictionary — Insurance Claims Data Lakehouse

All data is **synthetic**. PII columns contain fabricated names, SSN-shaped
strings, and example-domain emails.

## Bronze (raw, as-landed)

### bronze_policies
| Column | Type | Description |
|---|---|---|
| policy_id | string | Policy key (`POL-#######`) |
| policyholder_name | string | Synthetic policyholder name (raw PII, Bronze only) |
| policyholder_ssn | string | Fabricated SSN-shaped string (raw PII, Bronze only) |
| line_of_business | string | Personal Auto / Homeowners / Commercial Property / Commercial Liability / Workers Compensation / Umbrella |
| state | string | US state code |
| effective_date / expiry_date | date | Policy term |
| annual_premium | double | Annual premium (USD) |
| coverage_limit | long | Coverage limit (USD) |

### bronze_claims
| Column | Type | Description |
|---|---|---|
| claim_id | string | Claim key (`CLM-########`) |
| policy_id | string | FK to bronze_policies (seeded bad rows may be null/orphan) |
| loss_date / report_date | date | Loss occurrence / report dates |
| line_of_business | string | See above |
| claim_status | string | Open / Closed |
| paid_loss | double | Paid loss (USD; seeded bad rows may be negative) |
| reserved_loss | double | Reserved loss (USD) |
| claimant_name / claimant_ssn / claimant_email | string | Synthetic claimant PII (raw, Bronze only) |
| cause_of_loss | string | Collision, Fire, Water Damage, Theft, Wind/Hail, etc. |

## Silver (cleaned, deduplicated, PII-masked)

### silver_claims — partitioned by `line_of_business`, `month(loss_date)`
Same grain as Bronze minus raw PII: `claimant_name_hash`,
`claimant_ssn_hash` (SHA-256 hex), `masked_email` (`j***@domain`).
Only DQ-passing, de-duplicated claims land here.

### silver_policies
Policy grain; `policyholder_name_hash`, `policyholder_ssn_hash` replace raw PII.

### quarantine_claims
Bronze claims schema + `quarantine_reason` (`;`-joined rule names:
`null_policy_id`, `negative_paid_loss`, `orphan_claim`,
`duplicate_claim_id`, `invalid_loss_date`).

## Gold (curated, analysis-ready)

### gold_claims_monthly
`loss_month` (date), `line_of_business`, `claim_count`, `total_paid_loss`,
`avg_paid_loss`, `open_claims`, `closed_claims`.

### gold_loss_by_lob_year
`loss_year`, `line_of_business`, `claim_count`, `total_paid_loss`,
`total_reserved_loss`, `policy_count`, `total_annual_premium`,
`paid_loss_ratio` (= total paid loss / total annual premium for the LOB).
