"""Probe what fields are actually populated for state legislators in
OpenStates so we can plan Phase-2.5 field extraction.

Usage:
    .venv/bin/python scripts/probe_openstates_state_legs.py

Reads OPENSTATES_API_KEY from .env. Fetches 10 FL state legs (current
members) with full include set, and reports per-field populated/null
counts plus a structural sample of links[] and current_role.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv

load_dotenv()


BASE_URL = "https://v3.openstates.org"
INCLUDES = (
    "other_names", "other_identifiers", "links", "sources", "offices",
)


async def fetch_sample(api_key: str, n: int = 10) -> list[dict]:
    headers = {"X-API-KEY": api_key}
    params = [
        ("jurisdiction", "fl"),
        ("per_page", str(n)),
        ("page", "1"),
    ] + [("include", inc) for inc in INCLUDES]
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{BASE_URL}/people", headers=headers, params=params,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])


def host_of(url: str | None) -> str:
    if not url:
        return "(none)"
    try:
        return urlparse(url).netloc.lower()
    except Exception:  # noqa: BLE001
        return "(parse-error)"


def main() -> None:
    api_key = os.getenv("OPENSTATES_API_KEY")
    if not api_key:
        raise SystemExit("OPENSTATES_API_KEY not set")

    records = asyncio.run(fetch_sample(api_key, n=10))
    print(f"Fetched {len(records)} FL state legislators\n")

    # Per-field populated/null tally
    field_populated = Counter()
    field_null = Counter()
    fields_to_check = [
        "id", "name", "given_name", "family_name", "party", "gender",
        "birth_date", "death_date", "email", "image", "biography",
    ]

    # current_role keys (what's actually inside the dict)
    current_role_keys = Counter()
    current_role_value_samples: dict[str, list] = {}

    # links analysis
    link_hosts = Counter()
    link_notes = Counter()
    links_per_record = []
    homepage_or_official_count = 0

    # other_identifiers schemes
    other_id_schemes = Counter()

    # offices breakdown
    office_classifications = Counter()
    office_field_population: dict[str, Counter] = {}

    # roles (if present in raw)
    has_roles_field = 0
    role_keys: Counter = Counter()

    for r in records:
        # Top-level field tally
        for f in fields_to_check:
            v = r.get(f)
            if v not in (None, "", [], {}):
                field_populated[f] += 1
            else:
                field_null[f] += 1

        # current_role
        cr = r.get("current_role") or {}
        for k in cr.keys():
            current_role_keys[k] += 1
            current_role_value_samples.setdefault(k, [])
            if len(current_role_value_samples[k]) < 3:
                current_role_value_samples[k].append(cr.get(k))

        # links
        links = r.get("links") or []
        links_per_record.append(len(links))
        for link in links:
            link_hosts[host_of(link.get("url"))] += 1
            note = (link.get("note") or "").strip().lower()
            link_notes[note or "(empty)"] += 1
            if note in ("homepage", "official", "official website", "website"):
                homepage_or_official_count += 1

        # other_identifiers
        for oid in r.get("other_identifiers") or []:
            scheme = (oid.get("scheme") or "").lower()
            if scheme:
                other_id_schemes[scheme] += 1

        # offices
        for o in r.get("offices") or []:
            cls = (o.get("classification") or "(none)").lower()
            office_classifications[cls] += 1
            office_field_population.setdefault(cls, Counter())
            for k, v in o.items():
                if v not in (None, "", [], {}):
                    office_field_population[cls][k] += 1

        # roles (term history)
        if "roles" in r:
            has_roles_field += 1
            for role in r.get("roles") or []:
                for k in role.keys():
                    role_keys[k] += 1

    print("=== Top-level field population (out of N records) ===")
    for f in fields_to_check:
        p = field_populated.get(f, 0)
        n = field_null.get(f, 0)
        print(f"  {f}: populated={p}, null/empty={n}")

    print("\n=== current_role keys (count = # records that have this key) ===")
    for k, c in current_role_keys.most_common():
        samples = current_role_value_samples.get(k, [])
        print(f"  {k} (n={c}): samples={samples!r}")

    print("\n=== links: hosts seen across all records ===")
    for h, c in link_hosts.most_common():
        print(f"  {h}: {c}")
    print(f"\n  (records with explicit homepage/official-website note: "
          f"{homepage_or_official_count} of {len(records)})")
    print(f"  links per record: min={min(links_per_record)}, "
          f"max={max(links_per_record)}, "
          f"avg={sum(links_per_record)/len(links_per_record):.1f}")

    print("\n=== link notes (first 20 most common) ===")
    for n, c in link_notes.most_common(20):
        print(f"  {n!r}: {c}")

    print("\n=== other_identifiers schemes ===")
    if not other_id_schemes:
        print("  (none populated for any record)")
    for s, c in other_id_schemes.most_common():
        print(f"  {s}: {c}")

    print("\n=== offices: classification breakdown ===")
    for cls, c in office_classifications.most_common():
        print(f"  {cls}: {c} records")
        per_field = office_field_population.get(cls, Counter())
        for f, fc in per_field.most_common():
            print(f"    {f}: populated in {fc}")

    print(f"\n=== roles[] (term history) ===")
    print(f"  records with `roles` in response: {has_roles_field} of {len(records)}")
    if role_keys:
        for k, c in role_keys.most_common():
            print(f"    role.{k}: seen on {c} role entries")
    else:
        print("  (no roles[] field — would need separate include if API supports)")

    print("\n=== Sample raw record (first one, truncated) ===")
    if records:
        sample = dict(records[0])
        # trim long lists
        for k in ("links", "sources", "offices", "other_names",
                  "other_identifiers"):
            v = sample.get(k)
            if isinstance(v, list) and len(v) > 3:
                sample[k] = v[:3] + [f"... ({len(v)-3} more)"]
        print(json.dumps(sample, indent=2, default=str)[:3000])


if __name__ == "__main__":
    main()
