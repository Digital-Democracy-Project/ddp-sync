"""Backfill missing intermediate BillVersion rows for fast-moving bills (SYNC-26).

ddp-sync's BillVersion ledger in ddp-broker-py only ever gets a row for
whichever single version a bill happened to be at the first time ddp-sync's
polling job saw it. A fast-moving bill (e.g. a special session bill that goes
Filed -> e1 -> er between polls) can arrive already several stages ahead of
that first sighting -- every earlier version silently never gets a ledger row,
which later breaks bill_changelog's compare_version FK resolution even though
api-v3 itself has every version and diff archived correctly. The standing fix
(BillVersionSyncService._backfill_missing_versions) prevents this going
forward; this script repairs bills already stuck in that state.

Walks api-v3's already-correct versions[] for every bill in a jurisdiction/
session and calls write_bill_version() for each version -- idempotent
(natural-keyed on bill + version_date + version_note), safe to re-run.
Already-present versions are a safe no-op (write_bill_version reports
created=False); this script never needs to know in advance which versions
are already recorded.

Fetches at most 500 bills per run (api-v3 pagination limit passed through to
list_current_session_bill_candidates) -- pass --limit to raise this for a
larger session.

Dry-run mode (the default) only lists what WOULD be attempted -- it cannot
tell which of those are already present without actually writing, since
ddp-broker-py only exposes a "read the latest version" lookup, not a "list
every version already recorded" one. Safe by default regardless: runs in
dry-run mode until --write is passed.

Usage:
    .venv/bin/python scripts/backfill_bill_versions.py --jurisdiction FL --session 2026E [--write] [--limit N]

Env vars required:
    DDP_BROKER_API_BASE / DDP_BROKER_API_TOKEN   ddp-broker-py write access
    LOCAL_OPENSTATES_API_BASE                    api-v3 read access
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

_DEFAULT_LIMIT = 500


@dataclass
class BackfillResult:
    attempted: list[tuple[str, str, str]] = field(default_factory=list)  # (gov_id, date, note) -- dry-run only
    created: list[tuple[str, str, str]] = field(default_factory=list)  # (gov_id, date, note) -- --write only
    already_present: int = 0
    skipped_single_version: list[str] = field(default_factory=list)
    skipped_no_versions: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)  # (gov_id, error)


async def run(*, jurisdiction: str, session: str, dry_run: bool, limit: int | None) -> BackfillResult:
    from ddp_sync.pipelines.bill_version import BillVersionSyncService
    from ddp_sync.services.broker_client import BrokerClientError, write_bill_version
    from ddp_sync.services.local_openstates_client import (
        get_all_versions,
        list_current_session_bill_candidates,
    )

    result = BackfillResult()

    print(
        f"{'[DRY RUN] ' if dry_run else ''}Listing {jurisdiction} {session} bills…\n"
    )
    candidates = await list_current_session_bill_candidates(
        jurisdiction, session_code=session, limit=limit or _DEFAULT_LIMIT
    )

    for candidate in candidates:
        gov_id = candidate["gov_id"]
        bill_openstates_id = candidate["bill_openstates_id"]

        versions = await get_all_versions(bill_openstates_id)
        if not versions:
            print(f"  SKIP (no versions in api-v3)  {gov_id}")
            result.skipped_no_versions.append(gov_id)
            continue
        if len(versions) == 1:
            result.skipped_single_version.append(gov_id)
            continue

        for version in versions:
            version_date = version.get("date", "")
            version_note = version.get("note", "")
            if not version_note:
                continue

            action = "DRY-RUN" if dry_run else "WRITE"
            print(f"  {action:7s} {gov_id!r:12s} {version_note!r}")

            if dry_run:
                result.attempted.append((gov_id, version_date, version_note))
                continue

            # Same "prefer PDF over HTML" link selection the standing fix
            # uses -- reused rather than re-implemented (BillVersionSyncService
            # is this script's own sibling, not a cross-package boundary).
            url_info = BillVersionSyncService._get_best_text_url(version)
            text_url, media_type = url_info if url_info else ("", "")

            try:
                write_result = await write_bill_version(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction,
                    session_code=session,
                    version_date=version_date,
                    version_note=version_note,
                    text_url=text_url,
                    media_type=media_type,
                )
                if write_result.get("created"):
                    result.created.append((gov_id, version_date, version_note))
                else:
                    result.already_present += 1
            except BrokerClientError as e:
                print(f"    ↳ ddp-broker-py error: {e}")
                result.failed.append((gov_id, str(e)))

    return result


def print_summary(result: BackfillResult, *, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'─' * 55}")
    print(f"{prefix}Summary")
    print(f"{'─' * 55}")
    if dry_run:
        print(f"  Would attempt to write:  {len(result.attempted)}")
        print("    (some of these may already be recorded -- dry-run can't")
        print("     tell without actually writing; re-run with --write to")
        print("     see the real created-vs-already-present split)")
    else:
        print(f"  Newly created:       {len(result.created)}")
        print(f"  Already present:     {result.already_present}")
    print(f"  Single-version bills (nothing to do): {len(result.skipped_single_version)}")
    print(f"  No versions in api-v3:                {len(result.skipped_no_versions)}")
    print(f"  Errors:              {len(result.failed)}")

    if result.failed:
        print("\nFailed records:")
        for gov_id, err in result.failed:
            print(f"  {gov_id}: {err}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jurisdiction", required=True, help="Two-letter jurisdiction code, e.g. FL")
    parser.add_argument("--session", required=True, help="Session code, e.g. 2026E")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Commit changes to ddp-broker-py. Omit to run in dry-run mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help=f"Process at most N bills. Default: {_DEFAULT_LIMIT} -- raise this for a larger session.",
    )
    args = parser.parse_args()

    result = asyncio.run(
        run(
            jurisdiction=args.jurisdiction,
            session=args.session,
            dry_run=not args.write,
            limit=args.limit,
        )
    )
    print_summary(result, dry_run=not args.write)

    if result.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
