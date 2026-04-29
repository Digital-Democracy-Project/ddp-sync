"""OpenStates `/people` async client for the legislator bio sync.

Scoped, focused replacement for the inline OpenStates calls scattered across
`pipelines/legislator_sync.py` (which talks to /people only for sponsor-name
lookup). This module exposes a typed dataclass and an async iterator for
paginated jurisdiction fetches, plus a single-record fetch by OCD person ID.

Design choices:

- Takes a shared `RateLimiter` instance from `services/rate_limiter.py` so
  bio-sync can coordinate its OpenStates budget with other pipelines using
  the same shared module. No internal limiter is created — caller owns the
  budget.

- Distinguishes 404 (returns None — well-defined "not found") from hard
  failures (raise `OpenStatesError` / `OpenStatesRateLimitError`). The bio-sync
  orchestrator catches the exceptions and surfaces them in
  `BioSyncReport.errors`; None signals the bioguide-id fallback path.

- Honors `Retry-After` for 429 with jitter. Persistent 429 raises
  `OpenStatesRateLimitError` after the configured retry budget. Same pattern
  as the WebflowLookupService.

- Always passes the full `include` set documented as needed by the
  bio-sync schema (other_identifiers, links, sources, offices, other_names).
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import httpx
import structlog

from ddp_sync.services.rate_limiter import RateLimiter

logger = structlog.get_logger()


# ---------- Error types ----------


class OpenStatesError(Exception):
    """Base for non-success OpenStates responses."""

    def __init__(self, message: str, *, response: httpx.Response | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code if response is not None else None
        self.error_detail = response.text[:500] if response is not None else None


class OpenStatesRateLimitError(OpenStatesError):
    """Raised when 429 persists after the configured retry budget."""


# ---------- Result dataclass ----------


@dataclass
class OpenStatesPerson:
    """Normalized person record from OpenStates `/people`.

    Fields mirror the upstream schema; commonly-accessed values are surfaced
    as named attributes for ergonomics. The full upstream dict is preserved
    in ``.raw`` for fallback access to anything not surfaced explicitly.
    """

    openstates_id: str             # ocd-person/...
    name: str                      # display name (full)
    family_name: str | None
    given_name: str | None
    party: str | None
    gender: str | None
    birth_date: str | None         # ISO; may be "" upstream
    death_date: str | None
    email: str | None              # may be a contact-form URL for federal
    image: str | None              # photo URL
    biography: str | None
    jurisdiction_name: str | None  # human-readable (e.g. "Florida", "United States")
    current_role: dict             # {title, org_classification, district, division_id}
    other_identifiers: list[dict]  # [{identifier, scheme}]
    other_names: list[dict]
    links: list[dict]
    sources: list[dict]
    offices: list[dict]            # [{name, fax, voice, address, classification}]
    raw: dict = field(repr=False)

    @classmethod
    def from_api(cls, data: dict) -> "OpenStatesPerson":
        # `jurisdiction` is a dict like {"id": "...", "name": "Florida",
        # "classification": "state"} — extract the name. OpenStates also
        # surfaces a flatter "jurisdiction" string in some endpoints, so
        # accept either shape.
        juris_field = data.get("jurisdiction")
        if isinstance(juris_field, dict):
            juris_name = juris_field.get("name")
        elif isinstance(juris_field, str):
            juris_name = juris_field
        else:
            juris_name = None
        return cls(
            openstates_id=data.get("id", ""),
            name=data.get("name", ""),
            family_name=(data.get("family_name") or None),
            given_name=(data.get("given_name") or None),
            party=(data.get("party") or None),
            gender=(data.get("gender") or None),
            birth_date=(data.get("birth_date") or None),
            death_date=(data.get("death_date") or None),
            email=(data.get("email") or None),
            image=(data.get("image") or None),
            biography=(data.get("biography") or None),
            jurisdiction_name=(juris_name or None),
            current_role=(data.get("current_role") or {}),
            other_identifiers=(data.get("other_identifiers") or []),
            other_names=(data.get("other_names") or []),
            links=(data.get("links") or []),
            sources=(data.get("sources") or []),
            offices=(data.get("offices") or []),
            raw=data,
        )

    def get_other_id(self, scheme: str) -> str | None:
        """Return the identifier for ``scheme`` if present (e.g., bioguide).

        Convenience wrapper over the static :meth:`extract_other_id` helper.
        """
        return OpenStatesPeopleClient.extract_other_id(self.raw, scheme)

    @property
    def chamber(self) -> str | None:
        """Org classification of the legislator's current role.

        Returns the upstream value verbatim (typically "upper"/"lower" for
        state, or "legislature" for federal). None if no current role.
        """
        return self.current_role.get("org_classification")

    @property
    def district(self) -> str | None:
        """District identifier from the current role (string; varies by state)."""
        return self.current_role.get("district")

    @property
    def is_federal(self) -> bool:
        """Whether this is a federal (US Congress) member.

        Probe-confirmed signal: ``jurisdiction`` is exactly "United States" for
        federal members and the state name (e.g., "Florida") for state
        legislators. Federal members' ``current_role.division_id`` contains
        ``/state:XX`` because they represent specific states — so the more
        intuitive division_id check would mis-classify them as state.
        """
        return (self.jurisdiction_name or "").strip().lower() == "united states"


# ---------- Client ----------


class OpenStatesPeopleClient:
    """Thin async client over OpenStates v3 ``/people``.

    Holds no state beyond auth + a shared rate limiter; safe to keep as a
    module-level singleton, or recreate per-pipeline.
    """

    BASE_URL = "https://v3.openstates.org"
    DEFAULT_TIMEOUT_SECONDS = 30.0
    DEFAULT_PER_PAGE = 50
    DEFAULT_MAX_RETRY_ATTEMPTS = 3

    INCLUDE_PARAMS = (
        "other_names",
        "other_identifiers",
        "links",
        "sources",
        "offices",
    )

    def __init__(
        self,
        api_key: str,
        rate_limiter: RateLimiter | None = None,
        *,
        max_retry_attempts: int = DEFAULT_MAX_RETRY_ATTEMPTS,
        per_page: int = DEFAULT_PER_PAGE,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ):
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.rate_limiter = rate_limiter or RateLimiter()
        self.max_retry_attempts = max_retry_attempts
        self.per_page = per_page
        self.timeout_seconds = timeout_seconds

    # ---------- Public API ----------

    async def fetch_by_id(self, openstates_id: str) -> OpenStatesPerson | None:
        """Fetch a single person by OCD person ID.

        Returns the parsed record on 2xx, or ``None`` on 404 (the well-defined
        "not in OpenStates" signal — bio-sync uses this to trigger the
        bioguide-id fallback for departed federal members).

        Raises:
            OpenStatesRateLimitError: 429 persists after the retry budget.
            OpenStatesError: any other non-2xx, non-404 response.
        """
        if not openstates_id:
            raise ValueError("openstates_id is required")
        params: list[tuple[str, Any]] = [("id", openstates_id)] + [
            ("include", p) for p in self.INCLUDE_PARAMS
        ]
        data = await self._get_json("/people", params, allow_404=True)
        if data is None:
            return None
        results = data.get("results") or []
        if not results:
            return None
        return OpenStatesPerson.from_api(results[0])

    async def iter_jurisdiction(
        self,
        jurisdiction: str,
    ) -> AsyncIterator[OpenStatesPerson]:
        """Paginated iterator over all current people in a jurisdiction.

        Args:
            jurisdiction: Two-letter state code ("fl", "wa", ...) or "us"
                for federal Congress members.

        Yields:
            OpenStatesPerson per current member.

        Raises on any non-2xx (including 429-after-retries) — the orchestrator
        decides whether to skip the jurisdiction or abort the run.
        """
        page = 1
        while True:
            params: list[tuple[str, Any]] = [
                ("jurisdiction", jurisdiction),
                ("per_page", self.per_page),
                ("page", page),
            ] + [("include", p) for p in self.INCLUDE_PARAMS]

            data = await self._get_json("/people", params)
            if not data:
                break
            results = data.get("results") or []
            if not results:
                break
            for raw in results:
                yield OpenStatesPerson.from_api(raw)

            pagination = data.get("pagination") or {}
            max_page = pagination.get("max_page", 1)
            if page >= max_page:
                break
            page += 1

    @staticmethod
    def extract_other_id(person: dict, scheme: str) -> str | None:
        """Pull a specific identifier from ``other_identifiers`` by scheme.

        Convenience helper that works on raw API dicts as well as on
        ``OpenStatesPerson.raw``. Used by bio-sync to crosswalk
        OpenStates federal members to their bioguide-id (and from there
        to the unitedstates dataset).
        """
        for entry in (person.get("other_identifiers") or []):
            if entry.get("scheme") == scheme:
                return entry.get("identifier")
        return None

    # ---------- Internals ----------

    async def _get_json(
        self,
        path: str,
        params: list[tuple[str, Any]],
        *,
        allow_404: bool = False,
    ) -> dict | None:
        """GET with rate-limit + 429-retry. Returns the JSON body on 2xx.

        Returns None for 404 only when ``allow_404`` is True. Raises
        OpenStatesRateLimitError on persistent 429; OpenStatesError on
        any other non-2xx or transport failure that exhausts retries.
        """
        url = f"{self.BASE_URL}{path}"
        headers = {
            "x-api-key": self.api_key,
            "accept": "application/json",
        }
        last_resp: httpx.Response | None = None
        last_exc: Exception | None = None

        for attempt in range(self.max_retry_attempts):
            await self.rate_limiter.apply()
            try:
                async with httpx.AsyncClient(
                    timeout=self.timeout_seconds
                ) as client:
                    resp = await client.get(url, headers=headers, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
                logger.warning(
                    "OpenStates transport error — retrying",
                    attempt=attempt + 1,
                    path=path,
                    error=str(e),
                )
                await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
                continue

            last_resp = resp
            if 200 <= resp.status_code < 300:
                return resp.json()
            if resp.status_code == 404 and allow_404:
                return None
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 2 ** attempt))
                jittered = wait + random.uniform(0, 0.5)
                logger.warning(
                    "OpenStates 429 — backing off",
                    attempt=attempt + 1,
                    wait_seconds=round(jittered, 2),
                    path=path,
                )
                await asyncio.sleep(jittered)
                continue
            # Non-retryable non-2xx
            raise OpenStatesError(
                f"OpenStates {resp.status_code} on {path}",
                response=resp,
            )

        # Exhausted retry budget
        if last_resp is not None and last_resp.status_code == 429:
            raise OpenStatesRateLimitError(
                f"OpenStates 429 persisted after {self.max_retry_attempts} retries: {path}",
                response=last_resp,
            )
        if last_exc is not None:
            raise OpenStatesError(
                f"OpenStates transport error persisted after "
                f"{self.max_retry_attempts} retries: {path} — {last_exc}",
            )
        raise OpenStatesError(
            f"OpenStates request failed after {self.max_retry_attempts} attempts: {path}",
            response=last_resp,
        )
