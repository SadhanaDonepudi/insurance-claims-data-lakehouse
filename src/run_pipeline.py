#!/usr/bin/env python3
"""End-to-end pipeline entry point (local Step Functions analog).

Runs the medallion steps in order, logs each step like a state machine
execution, and exits non-zero if the DQ gate fails to reconcile — the local
stand-in for the production SNS alert + Step Functions error state.

Usage:
    python -m src.run_pipeline [--policies N] [--claims N]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import PipelineConfig
from src.pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policies", type=int, default=None)
    parser.add_argument("--claims", type=int, default=None)
    parser.add_argument("--data-dir", type=str, default=None)
    args = parser.parse_args()

    cfg = PipelineConfig()
    if args.policies:
        cfg.n_policies = args.policies
    if args.claims:
        cfg.n_claims = args.claims
    if args.data_dir:
        cfg.data_dir = Path(args.data_dir)

    states = ["GenerateSyntheticExtracts", "BronzeIngest", "DataQualityGate",
              "SilverTransform", "GoldAggregate", "ReconcileAndReport"]
    for state in states:
        print(f"[state] {state}")

    summary = run_pipeline(cfg)
    print(json.dumps(summary, indent=2))

    ok = (summary["checks"]["reconciles"] and summary["quarantined_vs_seeded_equal"]
          and summary["checks"]["gold_nonempty"])
    if not ok:
        print("DQ GATE FAILED: reconciliation mismatch "
              "(production: Step Functions error state + SNS alert)", file=sys.stderr)
        return 1
    print("DQ GATE PASSED: 100% of seeded bad records quarantined; "
          "Bronze = Silver + Quarantine.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
