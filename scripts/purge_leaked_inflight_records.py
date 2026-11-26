"""List the in-flight Fargate records, and delete the ones the test suite leaked (SYNC-197).

Before SYNC-192's conftest guard, test runs on the Mac wrote fake archive records (made-up task ids such as
`arn:task/archive` and `arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc`) into the live Redis Hash `ddp:inflight_fargate_jobs`, and
every ddp-sync restart tried to resume archives that never existed ("openstates_archive: resuming an archive orphaned by a
previous restart", then "resumed_archive_state_lock_held_by_another_run"; `ddp-sync status` showed ten false in-flight archives).

This looks at EVERY record first and prints it with its verdict. It only ever deletes a record that is an archive record, has
the archive run-id shape and has exactly one of those test task ids; a real in-flight archive, a scrape or load record, or
anything unreadable is listed as KEPT and never touched. It deletes nothing without `--apply`, and with `--apply` it re-reads
each record right before deleting it and leaves it alone if it changed.

Usage (from the live ddp-sync checkout, its .venv; it reads the same Redis the service uses):
    .venv/bin/python scripts/purge_leaked_inflight_records.py            # look only
    .venv/bin/python scripts/purge_leaked_inflight_records.py --apply    # delete the LEAKED ones listed

A change to production data: run `--apply` only after reading the dry run. Exit status 1 if Redis could not be read.
"""

from __future__ import annotations

import argparse
import sys

from ddp_sync.pipelines.inflight_fargate_jobs import (
    INFLIGHT_KEY,
    purge_leaked_test_archive_records,
)


def _line(verdict: str, item: dict) -> str:
    if item.get("unparseable"):
        return f"  {verdict:<7} {item['run_id']}  (not readable as a record)"
    return (f"  {verdict:<7} {item['run_id']}  kind={item.get('kind')}  {item.get('jurisdiction')}  "
            f"task={item.get('task_arn')}  started={item.get('archive_started_at')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="delete the LEAKED records (default: look only)")
    args = parser.parse_args()

    report = purge_leaked_test_archive_records(apply=args.apply)
    if report["error"] and not report["records"]:
        print(f"Could not read {INFLIGHT_KEY}: {report['error']}. Nothing was touched.", file=sys.stderr)
        return 1
    print(f"{INFLIGHT_KEY}: {report['records']} records")
    for item in report["leaked"]:
        print(_line("LEAKED", item))
    for item in report["kept"]:
        print(_line("KEPT", item))
    if not args.apply:
        print(f"\nDRY RUN, nothing deleted. {len(report['leaked'])} LEAKED, {len(report['kept'])} KEPT. "
              "Run again with --apply to delete the LEAKED ones.")
        return 0
    print(f"\nDeleted {len(report['deleted'])} of {len(report['leaked'])} LEAKED; "
          f"{len(report['changed'])} changed since they were read and were left alone; {len(report['kept'])} KEPT untouched.")
    if report["error"]:
        print(f"Stopped early: {report['error']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
