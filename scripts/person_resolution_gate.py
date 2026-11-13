"""Score the person resolver's evaluation gate before act mode is switched on for people (SYNC-150).

Core logic: src/ddp_sync/pipelines/person_resolution_gate.py (scoring reused from org_resolution_gates.py, SYNC-98).
Exit status: 0 gate passed, 1 failed, incomplete or provisional.

    .venv/bin/python scripts/person_resolution_gate.py offline [--cases tests/fixtures/person_resolution_cases.json]
        [--min-recall 0.8] [--results-out results.jsonl]
  Asks LegBot (CAMS, the settings in .env: CAMS_API_BASE, CAMS_API_TOKEN, CAMS_ARTIFACTS_DIR) resolve_person about
  every frozen shortlist, the pipeline's own dispatch, and scores the answers. It writes nothing to any broker.
  It makes real model calls (a few cents).

    .venv/bin/python scripts/person_resolution_gate.py shadow ATTEMPTS.jsonl REVIEWS.json
  The suggest-mode gate, on the broker's person-resolution attempts (one JSON object per line:
  {"attempt_id": 1, "choice": "rep:12" | null, "input_name": "...", "eligible": true}) and a person's verdict on each
  non-null suggestion ({"<attempt_id>": "right" | "wrong"}). Thresholds: PERSON_MIN_ELIGIBLE / PERSON_MIN_NON_NULL.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from pathlib import Path

from ddp_sync.pipelines.org_resolution_gates import (
    MIN_RECALL,
    Attempt,
    evaluate_shadow_gate,
)
from ddp_sync.pipelines.person_resolution_gate import (
    CASES_PATH,
    PERSON_MIN_ELIGIBLE,
    PERSON_MIN_NON_NULL,
    load_cases,
    run_offline,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Score the person resolver's evaluation gate")
    sub = parser.add_subparsers(dest="gate", required=True)
    offline = sub.add_parser("offline", help="the frozen-shortlist gate (asks LegBot)")
    offline.add_argument("--cases", default=str(CASES_PATH))
    offline.add_argument("--min-recall", type=float, default=MIN_RECALL)
    offline.add_argument("--results-out", help="Write the per-case results as JSONL")
    shadow = sub.add_parser("shadow", help="the suggest-mode traffic gate")
    shadow.add_argument("attempts")
    shadow.add_argument("reviews")
    args = parser.parse_args()

    if args.gate == "offline":
        run = asyncio.run(run_offline(load_cases(Path(args.cases))))
        if args.results_out:
            Path(args.results_out).write_text("\n".join(json.dumps(asdict(r)) for r in run.results) + "\n")
        print(run.render(min_recall=args.min_recall))
        return 0 if run.passed(min_recall=args.min_recall) else 1

    rows = [json.loads(line) for line in Path(args.attempts).read_text().splitlines() if line.strip()]
    reviews = {int(k): v for k, v in json.loads(Path(args.reviews).read_text()).items()}
    report = evaluate_shadow_gate(
        [Attempt(**row) for row in rows], reviews, min_eligible=PERSON_MIN_ELIGIBLE, min_non_null=PERSON_MIN_NON_NULL,
    )
    print(report.render())
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
