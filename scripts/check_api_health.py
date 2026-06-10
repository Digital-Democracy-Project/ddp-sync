"""CLI wrapper for the API health check pipeline.

Core logic lives in src/ddp_sync/pipelines/api_health_check.py.
The APScheduler job calls run_api_health_check_job() directly.

Usage:
    .venv/bin/python scripts/check_api_health.py
    .venv/bin/python scripts/check_api_health.py --dry-run    # skip Zapier

Environment (from .env or Secrets Manager):
    DDP_API_BASE_URL   — default: https://api.digitaldemocracyproject.org
    DDP_API_KEY        — Bearer token (leave blank for Voatz-authenticated endpoints)
    ZAPIER_WEBHOOK_URL — Zapier webhook for failure alerts
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()


def main() -> int:
    parser = argparse.ArgumentParser(description="DDP API health check")
    parser.add_argument("--dry-run", action="store_true", help="Print results but skip Zapier alert")
    args = parser.parse_args()

    from ddp_sync.pipelines.api_health_check import (
        DDP_API_BASE_URL,
        DDP_API_KEY,
        build_checks,
        push_health_alert,
        run_check,
    )

    headers: dict = {}
    if DDP_API_KEY:
        headers["Authorization"] = f"Bearer {DDP_API_KEY}"

    checks = build_checks()
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] Running {len(checks)} check(s) against {DDP_API_BASE_URL}")

    results = [run_check(check, DDP_API_BASE_URL, headers) for check in checks]

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        line = f"  [{status}] {r.name}  ({r.duration_ms}ms)"
        if r.error:
            line += f"\n         error: {r.error}"
        if not r.passed and r.body_preview:
            line += f"\n         body:  {r.body_preview}"
        print(line)

    failures = [r for r in results if not r.passed]

    if not failures:
        print(f"\nAll {len(results)} check(s) passed.")
        return 0

    print(f"\n{len(failures)} check(s) FAILED.")

    if args.dry_run:
        print("[dry-run] Skipping Zapier alert.")
    else:
        from ddp_sync.config import get_settings
        webhook_url = get_settings().zapier_webhook_url or ""
        sent = push_health_alert(webhook_url, results)
        print("Zapier alert sent." if sent else "Zapier alert NOT sent (see stderr).")

    return 1


if __name__ == "__main__":
    sys.exit(main())
