"""Loads and caches the unitedstates/congress-legislators YAML dataset.

Three public YAML files at https://unitedstates.github.io/congress-legislators/:
- legislators-current.yaml      (~1 MB,  current 535 federal members)
- legislators-historical.yaml   (~8.6 MB, all departed federal members back to 1789)
- legislators-social-media.yaml (~100 KB, twitter/facebook/instagram/youtube handles)

All three are public, no auth needed. Cached on disk under
``~/.cache/ddp-sync/congress-legislators/`` with a configurable TTL (default 24h)
to avoid re-downloading the 8.6 MB historical file on every sync run.

After ``warm_cache()`` runs, all members are indexed by bioguide-id in memory
so ``get_by_bioguide()`` is O(1). This is critical for the bioguide-id fallback
join used when OpenStates drops a departed federal member.

See ``plans/PLAN-legislator-bio-sync.md`` for the broader design.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import AsyncIterator, Iterable

import httpx
import structlog
import yaml

logger = structlog.get_logger()


@dataclass
class CongressLegislator:
    """Normalized record from the unitedstates/congress-legislators dataset.

    Combines the entry from legislators-{current,historical}.yaml with the
    matching entry from legislators-social-media.yaml (if present).
    """

    bioguide_id: str
    name: dict           # {first, last, middle?, official_full?, suffix?, nickname?}
    bio: dict            # {birthday?, gender?, religion?}
    terms: list[dict]    # term records, ordered chronologically
    ids: dict            # cross-source IDs: bioguide, wikidata, opensecrets, ...
    social: dict = field(default_factory=dict)  # twitter, facebook, instagram, youtube
    is_historical: bool = False

    @property
    def latest_term(self) -> dict:
        """Most recent term (last entry). Raises if terms is empty."""
        return self.terms[-1]

    @property
    def first_term(self) -> dict:
        """Earliest term (first entry). Raises if terms is empty."""
        return self.terms[0]

    @property
    def last_term_end(self) -> date | None:
        """Date of the latest term's end, parsed from ISO string. None if missing."""
        end_str = self.latest_term.get("end")
        if not end_str:
            return None
        try:
            return date.fromisoformat(str(end_str))
        except ValueError:
            return None


class CongressLegislatorsSource:
    """Fetcher + on-disk cache + in-memory index for the unitedstates dataset."""

    BASE_URL = "https://unitedstates.github.io/congress-legislators"
    DEFAULT_CACHE_DIR = Path.home() / ".cache" / "ddp-sync" / "congress-legislators"
    DEFAULT_CACHE_TTL_SECONDS = 24 * 60 * 60

    FILE_CURRENT = "legislators-current.yaml"
    FILE_HISTORICAL = "legislators-historical.yaml"
    FILE_SOCIAL = "legislators-social-media.yaml"
    ALL_FILES = (FILE_CURRENT, FILE_HISTORICAL, FILE_SOCIAL)

    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.cache_dir = cache_dir or self.DEFAULT_CACHE_DIR
        self.cache_ttl_seconds = cache_ttl_seconds
        self._http_client = http_client
        self._owns_client = http_client is None

        # Built by warm_cache():
        self._index_by_bioguide: dict[str, CongressLegislator] = {}
        self._current_bioguides: set[str] = set()
        self._historical_bioguides: set[str] = set()
        self._warmed: bool = False

    async def warm_cache(self) -> None:
        """Fetch (or load from disk cache) all three YAML files and build the index.

        Idempotent — calling twice is a no-op.
        """
        if self._warmed:
            return

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        client = self._http_client or httpx.AsyncClient(timeout=120.0)
        try:
            current_data, historical_data, social_data = await self._load_all(client)
        finally:
            if self._owns_client and self._http_client is None:
                await client.aclose()

        # Index social media by bioguide for quick join
        social_by_bioguide: dict[str, dict] = {}
        for entry in social_data or []:
            bg = (entry.get("id") or {}).get("bioguide")
            if bg:
                social_by_bioguide[bg] = entry.get("social") or {}

        for raw in current_data or []:
            self._ingest(raw, social_by_bioguide, is_historical=False)
        for raw in historical_data or []:
            self._ingest(raw, social_by_bioguide, is_historical=True)

        self._warmed = True
        logger.info(
            "congress-legislators dataset loaded",
            current=len(self._current_bioguides),
            historical=len(self._historical_bioguides),
            social_entries=len(social_by_bioguide),
        )

    def _ingest(
        self,
        raw: dict,
        social_by_bioguide: dict[str, dict],
        *,
        is_historical: bool,
    ) -> None:
        ids = raw.get("id") or {}
        bioguide_id = ids.get("bioguide")
        if not bioguide_id:
            return  # cannot index without the key

        record = CongressLegislator(
            bioguide_id=bioguide_id,
            name=raw.get("name") or {},
            bio=raw.get("bio") or {},
            terms=raw.get("terms") or [],
            ids=ids,
            social=social_by_bioguide.get(bioguide_id, {}),
            is_historical=is_historical,
        )
        self._index_by_bioguide[bioguide_id] = record
        if is_historical:
            self._historical_bioguides.add(bioguide_id)
        else:
            self._current_bioguides.add(bioguide_id)

    async def _load_all(
        self, client: httpx.AsyncClient
    ) -> tuple[list, list, list]:
        """Fetch (or load cached) all three YAMLs. Returns (current, historical, social)."""
        files = await self._fetch_or_cache(client, self.ALL_FILES)
        # Order in tuple matches ALL_FILES
        return files[self.FILE_CURRENT], files[self.FILE_HISTORICAL], files[self.FILE_SOCIAL]

    async def _fetch_or_cache(
        self, client: httpx.AsyncClient, filenames: Iterable[str]
    ) -> dict[str, list]:
        """For each filename, return parsed YAML — from disk if fresh, else fetched."""
        results: dict[str, list] = {}
        for filename in filenames:
            results[filename] = await self._fetch_or_cache_one(client, filename)
        return results

    async def _fetch_or_cache_one(
        self, client: httpx.AsyncClient, filename: str
    ) -> list:
        cache_path = self.cache_dir / filename
        if self._cache_is_fresh(cache_path):
            logger.debug("congress-legislators cache hit", file=filename)
            return await asyncio.to_thread(self._read_yaml, cache_path)

        url = f"{self.BASE_URL}/{filename}"
        logger.info("Fetching congress-legislators YAML", url=url)
        resp = await client.get(url)
        resp.raise_for_status()
        cache_path.write_bytes(resp.content)
        return await asyncio.to_thread(self._read_yaml, cache_path)

    def _cache_is_fresh(self, cache_path: Path) -> bool:
        if not cache_path.exists():
            return False
        age = time.time() - cache_path.stat().st_mtime
        return age < self.cache_ttl_seconds

    @staticmethod
    def _read_yaml(path: Path) -> list:
        """Synchronous YAML parse — wrapped via asyncio.to_thread() at call
        sites so the 8.6 MB historical file (~13s parse) does not block
        the event loop. See PLAN-legislator-bio-sync.md round-5 fixes."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return data or []

    # ---------- Lookup API ----------
    #
    # All public lookup methods are async and call warm_cache() — so callers
    # can hold a reference to the source and use any method without tracking
    # warm state. The lookup itself is O(1) once warmed.

    async def get_by_bioguide(self, bioguide_id: str) -> CongressLegislator | None:
        """O(1) lookup by bioguide-id. Auto-warms cache on first call.

        Returns None if not found in either current or historical.
        """
        await self.warm_cache()
        return self._index_by_bioguide.get(bioguide_id)

    async def iter_current(self) -> AsyncIterator[CongressLegislator]:
        """Iterate all current (in-office) federal members."""
        await self.warm_cache()
        for bg in self._current_bioguides:
            yield self._index_by_bioguide[bg]

    async def iter_historical_since(
        self, end_date: date
    ) -> AsyncIterator[CongressLegislator]:
        """Iterate historical members whose latest term ended on/after end_date.

        Used to backfill recently-departed members (e.g. since 2023-01-01).
        """
        await self.warm_cache()
        for bg in self._historical_bioguides:
            record = self._index_by_bioguide[bg]
            last_end = record.last_term_end
            if last_end is not None and last_end >= end_date:
                yield record
