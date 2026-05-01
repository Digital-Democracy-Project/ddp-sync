"""Read-only probe of the live Webflow Legislators CMS.

Fetches all items from the Legislators collection and reports which
fields are populated vs empty across the population. Helps diagnose
"some fields are empty" reports without manual click-through in the
Webflow Designer.

Usage:
    .venv/bin/python scripts/probe_webflow_legislators.py
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

SITE_ID = "62d84206d8adc10285c94e0a"
COLLECTION_ID = "655288ef928edb1283067255"
BASE_URL = "https://api.webflow.com/v2"

# Federal seat ref-IDs (from pipelines/legislator_bio.py)
FEDERAL_SEAT_REF_IDS = frozenset({
    "66316e20ae88354aed5df702",  # us-house
    "66316e0956dc73af879134b4",  # us-senate
})


async def fetch_all_items(token: str) -> list[dict]:
    """Page through the collection until exhausted."""
    headers = {"Authorization": f"Bearer {token}", "accept-version": "2.0.0"}
    items: list[dict] = []
    offset = 0
    limit = 100
    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            r = await client.get(
                f"{BASE_URL}/collections/{COLLECTION_ID}/items",
                headers=headers,
                params={"offset": offset, "limit": limit},
            )
            r.raise_for_status()
            data = r.json()
            page = data.get("items") or []
            if not page:
                break
            items.extend(page)
            total = data.get("pagination", {}).get("total")
            offset += limit
            if total is not None and offset >= total:
                break
            if len(page) < limit:
                break
    return items


def is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    if isinstance(v, (list, dict)) and len(v) == 0:
        return True
    return False


def is_federal(field_data: dict) -> bool:
    seat = field_data.get("seat")
    if isinstance(seat, str):
        seat = [seat]
    if not isinstance(seat, list):
        return False
    return any(r in FEDERAL_SEAT_REF_IDS for r in seat)


# Fields the bio sync writes (federal + state). Slugs match the live
# Legislators CMS schema as of 2026-04-30.
WRITE_TARGET_FIELDS = [
    "open-states-url", "bioguide-id", "wikidata-id", "opensecrets-id",
    "ballotpedia-slug", "govtrack-id",
    "birth-year", "gender",
    "term-start", "term-end", "seniority-rank",
    "phone-capitol", "office-address-capitol",
    "office-email", "contact-form-url",
    "official-website",
    "twitter-handle", "facebook-handle", "instagram-handle", "youtube-handle",
    "photo-source-url",
]


def main() -> None:
    token = os.getenv("WEBFLOW_API_READ_ONLY")
    if not token:
        raise SystemExit("WEBFLOW_API_READ_ONLY not set")

    items = asyncio.run(fetch_all_items(token))
    print(f"Fetched {len(items)} Legislators CMS items\n")

    federal_records = []
    state_records = []
    no_seat_records = []
    for item in items:
        fd = item.get("fieldData") or {}
        if is_federal(fd):
            federal_records.append(item)
        elif fd.get("seat"):
            state_records.append(item)
        else:
            no_seat_records.append(item)

    print(f"  federal (us-house/us-senate seat): {len(federal_records)}")
    print(f"  state (state-house/state-senate seat): {len(state_records)}")
    print(f"  no seat assigned: {len(no_seat_records)}")
    print()

    def tally(records: list[dict], label: str) -> None:
        if not records:
            return
        print(f"=== {label} (n={len(records)}) — field population ===")
        populated = Counter()
        empty = Counter()
        for r in records:
            fd = r.get("fieldData") or {}
            for f in WRITE_TARGET_FIELDS:
                if is_empty(fd.get(f)):
                    empty[f] += 1
                else:
                    populated[f] += 1
        for f in WRITE_TARGET_FIELDS:
            p = populated.get(f, 0)
            e = empty.get(f, 0)
            pct = (p / len(records) * 100) if records else 0
            print(f"  {f:32s} populated={p:4d} ({pct:5.1f}%) empty={e:4d}")
        print()

    tally(federal_records, "FEDERAL")
    tally(state_records, "STATE")

    # Show sample sparse records (most empty fields)
    def sparseness(item: dict) -> int:
        fd = item.get("fieldData") or {}
        return sum(1 for f in WRITE_TARGET_FIELDS if is_empty(fd.get(f)))

    print("=== 5 sparsest STATE records ===")
    sparse = sorted(state_records, key=sparseness, reverse=True)[:5]
    for s in sparse:
        fd = s.get("fieldData") or {}
        empty_fields = [f for f in WRITE_TARGET_FIELDS if is_empty(fd.get(f))]
        populated_fields = [f for f in WRITE_TARGET_FIELDS
                            if not is_empty(fd.get(f))]
        print(f"  {fd.get('name'):30s} "
              f"slug={fd.get('slug')!r}  "
              f"empty={len(empty_fields)}  populated_target_fields="
              f"{populated_fields}")

    print("\n=== 5 sparsest FEDERAL records ===")
    sparse_fed = sorted(federal_records, key=sparseness, reverse=True)[:5]
    for s in sparse_fed:
        fd = s.get("fieldData") or {}
        empty_fields = [f for f in WRITE_TARGET_FIELDS if is_empty(fd.get(f))]
        populated_fields = [f for f in WRITE_TARGET_FIELDS
                            if not is_empty(fd.get(f))]
        print(f"  {fd.get('name'):30s} "
              f"slug={fd.get('slug')!r}  "
              f"empty={len(empty_fields)}  populated_target_fields="
              f"{populated_fields}")


if __name__ == "__main__":
    main()
