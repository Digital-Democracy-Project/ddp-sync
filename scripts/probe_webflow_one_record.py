"""Fetch one record by ID from BOTH the live and draft endpoints to
diagnose the draft-vs-live discrepancy. The bio sync PATCHes to
/items/{id}/live, but the bulk /items endpoint may return the draft
state.

Usage:
    .venv/bin/python scripts/probe_webflow_one_record.py
"""

from __future__ import annotations

import asyncio
import json
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

COLLECTION_ID = "655288ef928edb1283067255"
BASE_URL = "https://api.webflow.com/v2"

# Mike Haridopolos — known patched in the very first round
SAMPLE_FEDERAL_ID = "67ce0b9740d92f599986c03f"

# Brian Hodgers — first state record we sampled earlier
SAMPLE_STATE_ID = "687c5c05d2b7437ada93639c"

FIELDS_TO_SHOW = (
    "name", "slug",
    "office-email", "open-states-url", "openstatesid",
    "official-website", "ballotpedia-slug", "govtrack-id",
    "phone-capitol", "office-address-capitol",
    "legislator-image", "photo-source-url",
)


async def fetch_item(token: str, item_id: str, *, suffix: str = "") -> dict:
    headers = {"Authorization": f"Bearer {token}", "accept-version": "2.0.0"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{BASE_URL}/collections/{COLLECTION_ID}/items/{item_id}{suffix}",
            headers=headers,
        )
        r.raise_for_status()
        return r.json()


def main() -> None:
    token = os.getenv("WEBFLOW_API_READ_ONLY")
    if not token:
        raise SystemExit("WEBFLOW_API_READ_ONLY not set")

    for label, item_id in (("FEDERAL (Mike Haridopolos)", SAMPLE_FEDERAL_ID),
                            ("STATE (Brian Hodgers)", SAMPLE_STATE_ID)):
        print(f"\n=== {label}  id={item_id} ===")
        # Default endpoint (returns draft state per Webflow v2 docs)
        try:
            draft = asyncio.run(fetch_item(token, item_id))
            fd = draft.get("fieldData") or {}
            print("  --- /items/{id} (draft endpoint) ---")
            for f in FIELDS_TO_SHOW:
                print(f"    {f:24s} = {fd.get(f)!r}")
        except Exception as e:  # noqa: BLE001
            print(f"  draft fetch failed: {e}")

        # Explicit /live endpoint
        try:
            live = asyncio.run(fetch_item(token, item_id, suffix="/live"))
            fd = live.get("fieldData") or {}
            print("  --- /items/{id}/live (live endpoint) ---")
            for f in FIELDS_TO_SHOW:
                print(f"    {f:24s} = {fd.get(f)!r}")
        except Exception as e:  # noqa: BLE001
            print(f"  live fetch failed: {e}")


if __name__ == "__main__":
    main()
