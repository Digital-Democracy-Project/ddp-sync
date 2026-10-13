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
  Before running it reads the broker's public organization count and refuses unless it equals the fixture's
  (the reviewed organizations plus one stub per case that needs the model), so a real broker is not written to
  by mistake.
  `--limit` scores only a subset: the run is reported INCOMPLETE and exits 1 whatever the subset scores.
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

import httpx

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
    broker_base = args.broker_base.rstrip("/")

    # --broker-is-throwaway is a claim; this is the check. The loader writes `organizations` reviewed
    # organizations plus one stub (each holding a verified position, so public) per case that needs the model;
    # the public list of the broker holding the fixture has exactly that count and a real broker does not.
    expected_public = loaded["organizations"] + sum(1 for case in loaded["cases"] if case.get("position_id"))
    try:
        public_count = httpx.get(f"{broker_base}/api/organizations/", params={"page_size": 1}, timeout=30).json()["count"]
    except Exception as exc:  # noqa: BLE001
        parser.error(f"could not read {broker_base}/api/organizations/ to check it holds the fixture: {exc}")
    if public_count != expected_public:
        parser.error(
            f"{broker_base} lists {public_count} organizations but the fixture has {expected_public}: this "
            "does not look like the throwaway broker the fixture was loaded into; refusing to write to it"
        )

    def progress(done, case_id, outcome):
        if done % 25 == 0:
            print(f"  {done} asked (last: {case_id} -> {outcome})", file=sys.stderr)

    run = asyncio.run(
        run_harness(
            loaded, broker_api_base=broker_base, broker_api_token=token,
            limit=args.limit, progress=progress,
        )
    )
    if args.results_out:
        Path(args.results_out).write_text("\n".join(json.dumps(asdict(r)) for r in run.results) + "\n")
    print(run.render(min_recall=args.min_recall))
    return 0 if run.passed(min_recall=args.min_recall) else 1


if __name__ == "__main__":
    sys.exit(main())
