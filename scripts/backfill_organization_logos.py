"""Find and store the logos of organizations already in the broker (SYNC-161), and refresh old ones.

Calls the same `find_and_store_organization_logo` the organization research pipeline's logo step uses, for the organizations
the broker lists as due (`GET /api/organizations/logo-queue/`, BROKER-502). Resumable (the broker's list is the progress) and
rate-limited. Needs the broker's logo routes deployed and LegBot's `judge_logo` (AGENTS-240) live on CAMS; it does not read
LEGBOT_ORGANIZATION_LOGO_ENABLED (that switch is the pipeline step's), so run it only once both are live.

Usage (from the ddp-sync checkout, its .venv):
    .venv/bin/python scripts/backfill_organization_logos.py --dry-run --limit 20
    .venv/bin/python scripts/backfill_organization_logos.py --limit 500
    .venv/bin/python scripts/backfill_organization_logos.py --older-than 180      # the refresh

When LegBot cannot judge at all (memory full, its vision server busy; SYNC-166) it waits 5 minutes and asks about the same
organization again, and stops after 4 hours of waiting in one run (`"stopped": "judge_unavailable"`): run it again later.

Prints the counts the first full run must report on SYNC-161: from Commons, from the website (by kind), from the parent,
none, and how many candidates were turned down or skipped.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from ddp_sync.pipelines.organization_logo import backfill_organization_logos


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=None, help="look at most this many organizations")
    parser.add_argument("--older-than", type=int, default=None, dest="older_than",
                        help="also re-check logos last checked more than this many days ago (the refresh)")
    parser.add_argument("--dry-run", action="store_true", help="look, but write nothing to the broker")
    parser.add_argument("--pause", type=float, default=1.0, help="seconds between organizations (default 1)")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be 1 or more")
    report = asyncio.run(backfill_organization_logos(
        limit=args.limit, older_than_days=args.older_than, dry_run=args.dry_run, pause_seconds=args.pause))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
