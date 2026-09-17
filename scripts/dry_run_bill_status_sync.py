"""Dry run for the nightly Daily Bill Sync's Flow 1 fetch step.

Replicates BillVersionSyncService.sync_bill_statuses()'s exact bill-selection
and OpenStates-fetch logic (same filters: has an OpenStates URL, current
session, jurisdiction enabled) against the real Webflow bill list -- but
stops right before update_bill_status() would PATCH Webflow. No Webflow
writes, no Pinecone, no OpenAI calls anywhere in this script.

Built to validate the RDS-OpenStates routing (this branch's
feat/rds-openstates-routing-standalone changes) against every real bill that
would actually be processed by tonight's 04:00 UTC run, without any side
effects. Flow 2 (Pinecone re-ingestion) is untested here on purpose -- that
logic is unchanged by this branch and was already verified end-to-end
against a real bill (HR6509) via /sync/unified.

Usage:
    .venv/bin/python scripts/dry_run_bill_status_sync.py [--jurisdiction FL]

Note: this makes real read calls to Webflow and to OpenStates (both the
public API and the RDS replica) for every bill that would sync tonight --
not free of load, just free of writes.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter

import httpx

from ddp_sync.config import get_settings
from ddp_sync.pipelines.bill_sync import BillSyncService


async def fetch_all_webflow_bills(settings) -> list[dict]:
    """Read-only paginated fetch, mirrors scheduler.py's _fetch_webflow_bills()."""
    bills: list[dict] = []
    async with httpx.AsyncClient(timeout=60.0) as client:
        headers = {
            "Authorization": f"Bearer {settings.webflow_votebot_api_key}",
            "accept": "application/json",
        }
        offset = 0
        while True:
            response = await client.get(
                f"https://api.webflow.com/v2/collections/{settings.webflow_bills_collection_id}/items",
                headers=headers,
                params={"limit": 100, "offset": offset},
            )
            if response.status_code != 200:
                break
            data = response.json()
            items = data.get("items", [])
            if not items:
                break
            bills.extend(items)
            offset += 100
            if len(items) < 100:
                break
    return bills


async def run(jurisdiction_filter: str | None) -> None:
    settings = get_settings()
    sync_service = BillSyncService(settings)

    print("Fetching bills from Webflow (read-only)...")
    bills = await fetch_all_webflow_bills(settings)
    print(f"Fetched {len(bills)} bills total.\n")

    # Warm legislative calendar -- mirrors sync_bill_statuses()'s own warm step,
    # needed for is_current_session_async() to resolve correctly for every state.
    jurisdiction_codes = set()
    for bill in bills:
        fields = bill.get("fieldData", {})
        code = sync_service.resolve_jurisdiction_code(
            fields.get("jurisdiction", ""), fields.get("open-states-url-2", "")
        )
        if code:
            jurisdiction_codes.add(code)

    jurisdiction_data = {}
    for code in jurisdiction_codes:
        try:
            info = await sync_service.get_jurisdiction_info(code)
            if info:
                jurisdiction_data[code] = info
        except Exception as e:
            print(f"  WARN: failed to warm calendar for {code}: {e}")

    if jurisdiction_data:
        sync_service.calendar.warm_cache(jurisdiction_data)

    skipped_no_url = 0
    skipped_jurisdiction_filter = 0
    skipped_not_current = 0
    skipped_jurisdiction_disabled = 0
    succeeded = 0
    failed = 0
    errors: list[str] = []
    routing_counts: Counter = Counter()
    per_jurisdiction_routing: dict[str, str] = {}

    for bill in bills:
        fields = bill.get("fieldData", {})
        title = fields.get("name", "Unknown")
        openstates_url = fields.get("open-states-url-2", "")
        session_year = str(fields.get("bill-session", ""))
        session_code = fields.get("session-code", "")
        jurisdiction_id = fields.get("jurisdiction", "")
        jurisdiction_code = sync_service.resolve_jurisdiction_code(jurisdiction_id, openstates_url)

        if not openstates_url:
            skipped_no_url += 1
            continue

        if jurisdiction_filter and jurisdiction_code.upper() != jurisdiction_filter.upper():
            skipped_jurisdiction_filter += 1
            continue

        if not await sync_service.is_current_session_async(session_year, session_code, jurisdiction_code):
            skipped_not_current += 1
            continue

        if not sync_service.should_sync_jurisdiction(jurisdiction_code):
            skipped_jurisdiction_disabled += 1
            continue

        await sync_service._apply_rate_limit()

        try:
            parsed = sync_service.parse_openstates_url(openstates_url)
            if not parsed:
                failed += 1
                errors.append(f"{title}: could not parse OpenStates URL {openstates_url!r}")
                continue

            api_base, _, is_local_replica = sync_service._get_api_base_and_key(parsed.jurisdiction)
            route = "rds" if is_local_replica else "public"
            routing_counts[route] += 1
            per_jurisdiction_routing[parsed.jurisdiction.upper()] = route

            bill_data = await sync_service.fetch_bill_from_openstates(
                parsed.jurisdiction, parsed.session, parsed.bill_id
            )
            if not bill_data:
                failed += 1
                errors.append(
                    f"{title}: fetch_bill_from_openstates returned None "
                    f"({parsed.jurisdiction}/{parsed.session}/{parsed.bill_id}, route={route})"
                )
                continue

            succeeded += 1

        except Exception as e:
            failed += 1
            errors.append(f"{title}: {type(e).__name__}: {e}")

    print("=" * 70)
    print("DRY RUN RESULTS -- Flow 1 fetch step only (NO writes performed anywhere)")
    print("=" * 70)
    print(f"Total bills in Webflow:            {len(bills)}")
    print(f"Skipped (no OpenStates URL):       {skipped_no_url}")
    print(f"Skipped (jurisdiction filter):     {skipped_jurisdiction_filter}")
    print(f"Skipped (not current session):     {skipped_not_current}")
    print(f"Skipped (jurisdiction disabled):   {skipped_jurisdiction_disabled}")
    print(f"Would be checked tonight:          {succeeded + failed}")
    print(f"  Fetch succeeded:                 {succeeded}")
    print(f"  Fetch FAILED:                    {failed}")
    print()
    print(f"Routing: {routing_counts.get('rds', 0)} via RDS replica, {routing_counts.get('public', 0)} via public API")
    print(f"Per-jurisdiction routing: {dict(sorted(per_jurisdiction_routing.items()))}")
    print()
    if errors:
        print(f"ERRORS ({len(errors)}):")
        for e in errors[:30]:
            print(f"  - {e}")
        if len(errors) > 30:
            print(f"  ... and {len(errors) - 30} more")
    else:
        print("No errors. Safe to let the 04:00 UTC run proceed.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jurisdiction", type=str, default=None,
        help="Limit to one jurisdiction (e.g. FL) for a faster/smaller run.",
    )
    args = parser.parse_args()
    asyncio.run(run(args.jurisdiction))


if __name__ == "__main__":
    main()
