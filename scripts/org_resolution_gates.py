"""Score the organization-resolution release gates (SYNC-98, PLAN-notifications-alert-engine.md 6.7.6).

Core logic lives in src/ddp_sync/pipelines/org_resolution_gates.py. Exit status is 0 when the gate
passes and 1 when it does not, so it can sit in a rollout checklist.

Usage:
    .venv/bin/python scripts/org_resolution_gates.py offline RESULTS.jsonl [--min-recall 0.8]
    .venv/bin/python scripts/org_resolution_gates.py shadow ATTEMPTS.jsonl REVIEWS.json

RESULTS.jsonl: one JSON object per line, one per labeled case:
    {"case_id": "...", "kind": "positive|negative|exact", "expected_org_id": 12 | null,
     "answered_org_id": 12 | null, "tier": "exact|alias|model|none", "held_out": false}
ATTEMPTS.jsonl: one object per broker attempt record:
    {"attempt_id": 1, "choice": 12 | null, "input_name": "...", "eligible": true}
REVIEWS.json: {"<attempt_id>": "right" | "wrong"} for each non-null suggestion a person has reviewed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ddp_sync.pipelines.org_resolution_gates import (
    MIN_RECALL,
    Attempt,
    CaseResult,
    evaluate_offline_gate,
    evaluate_shadow_gate,
)


def _read_jsonl(path: str) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Score the organization-resolution gates")
    sub = parser.add_subparsers(dest="gate", required=True)
    offline = sub.add_parser("offline", help="the frozen-fixture gate")
    offline.add_argument("results")
    offline.add_argument("--min-recall", type=float, default=MIN_RECALL)
    shadow = sub.add_parser("shadow", help="the suggest-mode traffic gate")
    shadow.add_argument("attempts")
    shadow.add_argument("reviews")
    args = parser.parse_args()

    if args.gate == "offline":
        report = evaluate_offline_gate(
            [CaseResult(**row) for row in _read_jsonl(args.results)], min_recall=args.min_recall
        )
    else:
        reviews = {int(k): v for k, v in json.loads(Path(args.reviews).read_text()).items()}
        report = evaluate_shadow_gate([Attempt(**row) for row in _read_jsonl(args.attempts)], reviews)
    print(report.render())
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
