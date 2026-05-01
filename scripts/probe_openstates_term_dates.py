"""Probe alternative OpenStates v3 endpoints / include params for
state-leg term dates.

Phase-2.5 confirmed `current_role` doesn't carry start/end dates and
the default `/people` response doesn't return `roles[]`. This script
tries:
  1. /people with `include=roles` and `include=memberships`
  2. The same with `include=other_identifiers,memberships`
  3. /people/{id} (singular) — sometimes returns more

If any of these surface term dates, Phase 3 can wire them in. If not,
state-leg term dates stay deferred (would need bulk-data ingestion).
"""

from __future__ import annotations

import asyncio
import json
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://v3.openstates.org"
SAMPLE_OCD_ID = None  # populated from initial /people fetch


async def get(token: str, path: str, params: list[tuple]) -> dict:
    headers = {"X-API-KEY": token, "accept": "application/json"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE_URL}{path}", headers=headers, params=params)
        r.raise_for_status()
        return r.json()


async def main() -> None:
    token = os.getenv("OPENSTATES_API_KEY")
    if not token:
        raise SystemExit("OPENSTATES_API_KEY not set")

    print("=== Probe 1: /people?jurisdiction=fl with include=roles ===")
    try:
        data = await get(token, "/people", [
            ("jurisdiction", "fl"),
            ("per_page", "2"),
            ("include", "roles"),
        ])
        sample = (data.get("results") or [{}])[0]
        roles = sample.get("roles")
        print(f"  sample[0].name = {sample.get('name')!r}")
        print(f"  sample[0].roles = {json.dumps(roles, indent=2)[:600]
                                       if roles else 'NOT PRESENT'}")
    except Exception as e:  # noqa: BLE001
        print(f"  ERROR: {e}")

    print("\n=== Probe 2: /people?jurisdiction=fl with include=memberships ===")
    try:
        data = await get(token, "/people", [
            ("jurisdiction", "fl"),
            ("per_page", "2"),
            ("include", "memberships"),
        ])
        sample = (data.get("results") or [{}])[0]
        memberships = sample.get("memberships")
        print(f"  sample[0].name = {sample.get('name')!r}")
        print(f"  sample[0].memberships = {
            json.dumps(memberships, indent=2)[:600]
            if memberships else 'NOT PRESENT'
        }")
    except Exception as e:  # noqa: BLE001
        print(f"  ERROR: {e}")

    # Capture an OCD ID for the singular fetches
    print("\n=== Probe 3: capture an OCD ID ===")
    try:
        data = await get(token, "/people", [
            ("jurisdiction", "fl"),
            ("per_page", "1"),
        ])
        ocd_id = (data.get("results") or [{}])[0].get("id")
        print(f"  using ocd_id = {ocd_id!r}")
    except Exception as e:  # noqa: BLE001
        print(f"  ERROR: {e}")
        return

    if not ocd_id:
        print("  (no ocd_id available, skipping singular probes)")
        return

    print("\n=== Probe 4: /people?id={ocd_id} with all known includes ===")
    try:
        data = await get(token, "/people", [
            ("id", ocd_id),
            ("include", "roles"),
            ("include", "memberships"),
            ("include", "other_names"),
            ("include", "other_identifiers"),
            ("include", "links"),
            ("include", "sources"),
            ("include", "offices"),
        ])
        sample = (data.get("results") or [{}])[0]
        # Show top-level keys to see what we got
        print(f"  top-level keys: {sorted(sample.keys())}")
        # Specifically dump roles + memberships if present
        for k in ("roles", "memberships"):
            v = sample.get(k)
            if v is not None:
                print(f"\n  {k}:")
                print(f"  {json.dumps(v, indent=2)[:600]}")
            else:
                print(f"\n  {k}: NOT PRESENT")
    except Exception as e:  # noqa: BLE001
        print(f"  ERROR: {e}")


if __name__ == "__main__":
    asyncio.run(main())
