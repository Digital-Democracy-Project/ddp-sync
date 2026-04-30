"""Dump full fieldData for one federal + one state record so we can see
the actual field slugs in the live CMS schema (vs what the bio sync
assumes).

Usage:
    .venv/bin/python scripts/probe_webflow_record.py
"""

from __future__ import annotations

import asyncio
import json
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

SITE_ID = "62d84206d8adc10285c94e0a"
COLLECTION_ID = "655288ef928edb1283067255"
BASE_URL = "https://api.webflow.com/v2"

FEDERAL_SEAT_REF_IDS = frozenset({
    "66316e20ae88354aed5df702",
    "66316e0956dc73af879134b4",
})


async def fetch_collection_schema(token: str) -> list[dict]:
    """Fetch the schema (field definitions) for the Legislators collection."""
    headers = {"Authorization": f"Bearer {token}", "accept-version": "2.0.0"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{BASE_URL}/collections/{COLLECTION_ID}", headers=headers,
        )
        r.raise_for_status()
        return r.json().get("fields") or []


async def fetch_first_50(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}", "accept-version": "2.0.0"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{BASE_URL}/collections/{COLLECTION_ID}/items",
            headers=headers, params={"limit": 50},
        )
        r.raise_for_status()
        return r.json().get("items") or []


def main() -> None:
    token = os.getenv("WEBFLOW_API_READ_ONLY")
    if not token:
        raise SystemExit("WEBFLOW_API_READ_ONLY not set")

    schema = asyncio.run(fetch_collection_schema(token))
    print(f"=== Legislators collection schema (n={len(schema)} fields) ===")
    for f in schema:
        slug = f.get("slug")
        name = f.get("displayName")
        ftype = f.get("type")
        validations = f.get("validations") or {}
        ref = validations.get("collectionId")
        ref_str = f" → {ref}" if ref else ""
        print(f"  {slug:35s} ({ftype}) — {name}{ref_str}")

    items = asyncio.run(fetch_first_50(token))
    federal = next(
        (i for i in items
         if any(r in FEDERAL_SEAT_REF_IDS
                for r in (i.get("fieldData", {}).get("seat") or []))),
        None,
    )
    state = next(
        (i for i in items
         if (i.get("fieldData", {}).get("seat") or [])
         and not any(r in FEDERAL_SEAT_REF_IDS
                     for r in i.get("fieldData", {}).get("seat") or [])),
        None,
    )

    if federal:
        print("\n=== FULL fieldData for sample FEDERAL record ===")
        fd = federal.get("fieldData", {})
        print(f"name: {fd.get('name')}")
        print(json.dumps(fd, indent=2, default=str))

    if state:
        print("\n=== FULL fieldData for sample STATE record ===")
        fd = state.get("fieldData", {})
        print(f"name: {fd.get('name')}")
        print(json.dumps(fd, indent=2, default=str))


if __name__ == "__main__":
    main()
