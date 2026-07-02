"""Backfill missing party-2 field on Legislators CMS items from OpenStates.

Reads every Legislator from Webflow, identifies those missing party-2,
looks up the party from the OpenStates v3 /people API using the stored
openstatesid, and PATCHes party-2 back to Webflow.

Safe by default: runs in dry-run mode until --write is passed.

Usage:
    .venv/bin/python scripts/backfill_legislator_party.py [--write] [--limit N]

Env vars required:
    WEBFLOW_SCHEDULER_API_KEY   CMS read+write (cms:* scope)
    OPENSTATES_API_KEY          OpenStates v3 API key
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


# ---------- Result tracking ----------

@dataclass
class BackfillResult:
    updated: list[str] = field(default_factory=list)
    skipped_no_openstates_id: list[str] = field(default_factory=list)
    skipped_not_found: list[str] = field(default_factory=list)
    skipped_no_party: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)  # (name, error)


# ---------- Main logic ----------

async def run(*, dry_run: bool, limit: int | None) -> BackfillResult:
    # Import here so dotenv is loaded before ddp_sync reads settings.
    from ddp_sync.config import get_settings
    from ddp_sync.services.openstates_people import (
        OpenStatesPeopleClient,
        OpenStatesError,
    )
    from ddp_sync.services.rate_limiter import RateLimiter, RateLimitConfig
    from ddp_sync.services.webflow_lookup import WebflowLookupService, WebflowError

    settings = get_settings()

    if not settings.openstates_api_key:
        sys.exit("OPENSTATES_API_KEY not set")
    if not settings.webflow_scheduler_api_key:
        sys.exit("WEBFLOW_SCHEDULER_API_KEY not set")

    webflow = WebflowLookupService(settings)
    openstates = OpenStatesPeopleClient(
        api_key=settings.openstates_api_key,
        rate_limiter=RateLimiter(
            RateLimitConfig(requests_per_minute=30, delay_between_requests_ms=0)
        ),
    )

    result = BackfillResult()
    checked = 0

    print(f"{'[DRY RUN] ' if dry_run else ''}Scanning Webflow Legislators for missing party-2…\n")

    async for item in webflow.iter_legislator_items():
        fd = item.get("fieldData") or {}
        name = fd.get("name", "(unnamed)")
        webflow_id = item.get("id", "")
        party = fd.get("party-2", "")

        if party:
            continue  # already has party — skip

        checked += 1
        if limit and checked > limit:
            break

        openstates_id = (fd.get("openstatesid") or "").strip()
        if not openstates_id:
            print(f"  SKIP (no openstatesid)  {name}")
            result.skipped_no_openstates_id.append(name)
            continue

        try:
            person = await openstates.fetch_by_id(openstates_id)
        except OpenStatesError as e:
            print(f"  FAIL (OpenStates error) {name}: {e}")
            result.failed.append((name, str(e)))
            continue

        if person is None:
            print(f"  SKIP (not in OpenStates) {name}  id={openstates_id}")
            result.skipped_not_found.append(name)
            continue

        os_party = (person.party or "").strip()
        if not os_party:
            print(f"  SKIP (no party in OpenStates) {name}")
            result.skipped_no_party.append(name)
            continue

        action = "DRY-RUN" if dry_run else "PATCH"
        print(f"  {action:7s} {name!r:40s} → {os_party!r}")

        if not dry_run:
            try:
                patch = await webflow.update_legislator_fields(
                    webflow_id,
                    {"party-2": os_party},
                )
                if patch.success:
                    result.updated.append(name)
                else:
                    result.failed.append((name, "PATCH returned success=False"))
            except WebflowError as e:
                print(f"    ↳ Webflow error: {e}")
                result.failed.append((name, str(e)))
        else:
            result.updated.append(name)

    return result


def print_summary(result: BackfillResult, *, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'─' * 55}")
    print(f"{prefix}Summary")
    print(f"{'─' * 55}")
    print(f"  {'Would update' if dry_run else 'Updated'}:          {len(result.updated)}")
    print(f"  No openstatesid:     {len(result.skipped_no_openstates_id)}")
    print(f"  Not in OpenStates:   {len(result.skipped_not_found)}")
    print(f"  No party returned:   {len(result.skipped_no_party)}")
    print(f"  Errors:              {len(result.failed)}")

    if result.failed:
        print("\nFailed records:")
        for name, err in result.failed:
            print(f"  {name}: {err}")

    if result.skipped_no_openstates_id:
        print(f"\nNo OpenStates ID ({len(result.skipped_no_openstates_id)} records — manual entry required):")
        for name in result.skipped_no_openstates_id:
            print(f"  {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Commit changes to Webflow. Omit to run in dry-run mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process at most N missing-party records (useful for testing).",
    )
    args = parser.parse_args()

    result = asyncio.run(run(dry_run=not args.write, limit=args.limit))
    print_summary(result, dry_run=not args.write)

    if result.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
