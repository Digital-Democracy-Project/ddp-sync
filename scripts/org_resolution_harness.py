"""Run the organization resolver over the evaluation cases and score the offline gate (SYNC-98).

Core logic: src/ddp_sync/pipelines/org_resolution_harness.py (the resolver is the pipeline's own resolve step,
SYNC-97) and org_resolution_gates.py (the scoring). Exit status: 0 gate passed, 1 gate failed or the run was
incomplete.

It writes resolution attempts to the broker it is given, so that broker must be a THROWAWAY one loaded with the
fixture; pass --broker-is-throwaway to say so.

Procedure (the command to re-run after any change to the prompt, model, shortlist logic, cleaned-name key,
candidate filters or broker checks):
  1. ddp-broker-py, against a fresh migrated scratch database:
       manage.py build_canonical_organizations --output-dir DIR          # BROKER-192 (reads Webflow)
       manage.py load_resolution_eval_fixture --seed DIR/resolution_eval_seed.json --output loaded.json
     and serve that broker (service token in DDP_EVAL_BROKER_TOKEN below).
  2. here:
       DDP_EVAL_BROKER_TOKEN=... .venv/bin/python scripts/org_resolution_harness.py loaded.json \
           --broker-base http://localhost:8090 --broker-is-throwaway [--min-recall 0.8] [--limit N] \
           [--results-out results.jsonl]
  `--results-out` writes one JSON line per case that scripts/org_resolution_gates.py offline can re-score.
  The model and prompt version of every attempt are recorded by the broker on its attempt rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

from ddp_sync.pipelines.org_resolution_gates import MIN_RECALL
from ddp_sync.pipelines.org_resolution_harness import run_harness


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the organization resolver over the evaluation cases")
    parser.add_argument("loaded", help="loaded.json written by load_resolution_eval_fixture")
    parser.add_argument("--broker-base", required=True, help="Base URL of the THROWAWAY broker holding the fixture")
    parser.add_argument("--broker-is-throwaway", action="store_true", help="Required: this broker's data may be written")
    parser.add_argument("--min-recall", type=float, default=MIN_RECALL)
    parser.add_argument("--limit", type=int, default=None, help="Only the first N cases that need the model (smoke run)")
    parser.add_argument("--results-out", help="Write the per-case results as JSONL")
    args = parser.parse_args()

    if not args.broker_is_throwaway:
        parser.error("--broker-is-throwaway is required: the harness writes resolution attempts to that broker")
    token = os.environ.get("DDP_EVAL_BROKER_TOKEN", "")
    if not token:
        parser.error("set DDP_EVAL_BROKER_TOKEN to the throwaway broker's service token")

    loaded = json.loads(Path(args.loaded).read_text())

    def progress(done, case_id, outcome):
        if done % 25 == 0:
            print(f"  {done} asked (last: {case_id} -> {outcome})", file=sys.stderr)

    run = asyncio.run(
        run_harness(
            loaded, broker_api_base=args.broker_base.rstrip("/"), broker_api_token=token,
            limit=args.limit, progress=progress,
        )
    )
    if args.results_out:
        Path(args.results_out).write_text("\n".join(json.dumps(asdict(r)) for r in run.results) + "\n")
    print(run.render(min_recall=args.min_recall))
    return 0 if run.passed(min_recall=args.min_recall) else 1


if __name__ == "__main__":
    sys.exit(main())
