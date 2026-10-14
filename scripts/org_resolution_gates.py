"""Score the organization-resolution release gates (SYNC-98, PLAN-notifications-alert-engine.md 6.7.6).

Core logic lives in src/ddp_sync/pipelines/org_resolution_gates.py. Exit status is 0 when the gate
passes and 1 when it does not, so it can sit in a rollout checklist.

Usage:
    .venv/bin/python scripts/org_resolution_gates.py offline RESULTS.jsonl --loaded LOADED.json [--min-recall 0.8]
        [--report-out report.json]
    .venv/bin/python scripts/org_resolution_gates.py shadow ATTEMPTS.jsonl REVIEWS.json

RESULTS.jsonl: one JSON object per line, one per labeled case (the file the harness writes with --results-out):
    {"case_id": "...", "answered_org_id": 12 | null, "tier": "exact|alias|model|none"}
LOADED.json: the group-aware (version 2) fixture file the broker loader wrote; it carries each case's expected
group and every record's group, so a match to another record of the right organization is correct and a match
to the parent of a chapter is not. An old (version 1) file is refused. A fixture whose verdicts are not
`reviewed` is PROVISIONAL and exits 1.
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
    Groups,
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
    offline.add_argument("--loaded", required=True, help="loaded.json (group-aware fixture) the results belong to")
    offline.add_argument("--min-recall", type=float, default=MIN_RECALL)
    offline.add_argument("--report-out", help="Write the gate result as JSON")
    shadow = sub.add_parser("shadow", help="the suggest-mode traffic gate")
    shadow.add_argument("attempts")
    shadow.add_argument("reviews")
    args = parser.parse_args()

    if args.gate == "offline":
        loaded = json.loads(Path(args.loaded).read_text())
        try:
            groups = Groups.from_loaded(loaded)
        except ValueError as exc:
            parser.error(str(exc))
        cases = {case["case_id"]: case for case in loaded["cases"]}
        rows = _read_jsonl(args.results)
        ids = [row["case_id"] for row in rows]
        unknown, missing = [i for i in ids if i not in cases], [i for i in cases if i not in set(ids)]
        duplicated = sorted({i for i in ids if ids.count(i) > 1})
        if unknown or missing or duplicated:
            # Scoring a subset would hide wrong matches or misses, so the results must be exactly the fixture's cases.
            parser.error(
                f"results do not match the cases in {args.loaded}: {len(unknown)} unknown {unknown[:3]}, "
                f"{len(missing)} missing {missing[:3]}, {len(duplicated)} duplicated {duplicated[:3]}"
            )
        report = evaluate_offline_gate(
            [groups.result(cases[row["case_id"]], answered_org_id=row["answered_org_id"], tier=row.get("tier", "none"))
             for row in rows],
            min_recall=args.min_recall, meta=groups.meta,
        )
        if args.report_out:
            Path(args.report_out).write_text(json.dumps(report.to_dict(), indent=2) + "\n")
    else:
        reviews = {int(k): v for k, v in json.loads(Path(args.reviews).read_text()).items()}
        report = evaluate_shadow_gate([Attempt(**row) for row in _read_jsonl(args.attempts)], reviews)
    print(report.render())
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
