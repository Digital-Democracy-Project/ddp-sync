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

SYNC-8 (2026-08-12) added jurisdiction-scoped local-replica routing to
iter_jurisdiction() only, mirroring BillSyncService._get_api_base_and_key()
(pipelines/bill_sync.py, SYNC-6) and the equivalent helpers added the same day
to OpenStatesSource and LegislatorSyncService: jurisdictions listed in
ddp_openstates_jurisdictions route to local_openstates_api_base/
local_openstates_api_key instead of the public API. fetch_by_id() looks up a
single person by opaque OpenStates ID with no jurisdiction in scope, so it's
unchanged -- always the public API. Deliberately still no `Settings`
dependency here (see "Design choices" above) -- the caller
(legislator_bio.py's BioSyncOrchestrator, scripts/backfill_legislator_party.py)
threads the relevant settings fields through as constructor kwargs instead.
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
    openstates_url: str | None     # https://openstates.org/person/<slug>/ — Phase-2.5
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
            openstates_url=(data.get("openstates_url") or None),
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
    # Safety valve for paginated iteration. No real jurisdiction has more than
    # a few hundred legislators (largest is US Congress at 535); if the API's
    # pagination semantics change and ``max_page`` becomes unreliable, this
    # cap prevents a runaway loop from exhausting the daily quota.
    DEFAULT_MAX_PAGES = 200

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
        max_pages: int = DEFAULT_MAX_PAGES,
        openstates_api_base: str | None = None,
        local_openstates_api_base: str | None = None,
        local_openstates_api_key: str | None = None,
        ddp_openstates_jurisdictions: list[str] | None = None,
    ):
        """
        Args:
            openstates_api_base: Public v3.openstates.org API base. Defaults
                to BASE_URL, preserving today's behavior for callers that
                don't pass it. Kept as a plain string rather than a `Settings`
                dependency -- this client is deliberately thin (see module
                docstring); the caller (e.g. legislator_bio.py's
                BioSyncOrchestrator) threads its own `settings.
                openstates_api_base` through.
            local_openstates_api_base: Local OpenStates replica base
                (settings.local_openstates_api_base). Only used for
                jurisdictions listed in `ddp_openstates_jurisdictions`.
            local_openstates_api_key: API key for the local replica
                (settings.local_openstates_api_key).
            ddp_openstates_jurisdictions: Jurisdictions to route to the local
                replica instead of the public API (SYNC-8, mirrors
                BillSyncService._get_api_base_and_key() from SYNC-6). Empty by
                default -- always public API, same as before this existed.
        """
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.rate_limiter = rate_limiter or RateLimiter()
        self.max_retry_attempts = max_retry_attempts
        self.per_page = per_page
        self.timeout_seconds = timeout_seconds
        self.max_pages = max_pages
        self.openstates_api_base = openstates_api_base or self.BASE_URL
        self.local_openstates_api_base = local_openstates_api_base or ""
        self.local_openstates_api_key = local_openstates_api_key or ""
        self.ddp_openstates_jurisdictions = ddp_openstates_jurisdictions or []

    def _get_api_base_and_key(self, jurisdiction: str) -> tuple[str, str, bool]:
        """
        Choose which OpenStates-compatible backend to hit for this jurisdiction.

        Mirrors BillSyncService._get_api_base_and_key() (pipelines/bill_sync.py,
        SYNC-6) and the equivalent helpers added to OpenStatesSource and
        LegislatorSyncService (SYNC-8): jurisdictions listed in
        self.ddp_openstates_jurisdictions are routed to the local OpenStates
        replica instead of the public v3.openstates.org API.

        Only used by iter_jurisdiction() -- fetch_by_id() looks up a single
        person by opaque OpenStates ID with no jurisdiction available, so it
        always uses the public API base (see its docstring).

        Returns:
            (api_base, api_key, is_local_replica) tuple. is_local_replica is
            True when the local api-v3 instance's apikey_auth scheme applies --
            it authenticates via an `apikey` query param, not the public API's
            `x-api-key` header scheme.
        """
        replica_jurisdictions = {j.upper() for j in self.ddp_openstates_jurisdictions}
        if jurisdiction.upper() in replica_jurisdictions:
            logger.debug(
                "Routing jurisdiction to local OpenStates replica",
                jurisdiction=jurisdiction,
                api_base=self.local_openstates_api_base,
            )
            return self.local_openstates_api_base, self.local_openstates_api_key, True

        return self.openstates_api_base, self.api_key, False

    # ---------- Public API ----------

    async def fetch_by_id(self, openstates_id: str) -> OpenStatesPerson | None:
        """Fetch a single person by OCD person ID.

        Returns the parsed record on 2xx, or ``None`` on 404 (the well-defined
        "not in OpenStates" signal — bio-sync uses this to trigger the
        bioguide-id fallback for departed federal members).

        Single-ID lookup by opaque OpenStates ID -- no jurisdiction is
        available here to route to the local replica (SYNC-8), so this always
        uses the public API base.

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

        A safety-valve cap (``self.max_pages``, default 200) prevents a
        runaway loop if the upstream API's pagination semantics break
        (round-6 defensive fix).
        """
        page = 1
        while page <= self.max_pages:
            params: list[tuple[str, Any]] = [
                ("jurisdiction", jurisdiction),
                ("per_page", self.per_page),
                ("page", page),
            ] + [("include", p) for p in self.INCLUDE_PARAMS]

            data = await self._get_json("/people", params, jurisdiction=jurisdiction)
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
        else:
            logger.warning(
                "iter_jurisdiction hit max_pages safety valve",
                jurisdiction=jurisdiction,
                max_pages=self.max_pages,
                metric="openstates.iter_max_pages_hit",
            )

    @staticmethod
    def extract_other_id(person: dict, scheme: str) -> str | None:
        """Pull a specific identifier from ``other_identifiers`` by scheme.

        Convenience helper that works on raw API dicts as well as on
        ``OpenStatesPerson.raw``. Used by bio-sync to crosswalk
        OpenStates federal members to their bioguide-id (and from there
        to the unitedstates dataset).

        Defensively skips non-dict entries (round-6 fix). OpenStates has
        historically returned bare strings in ``other_identifiers`` during
        at least one upstream incident — guarding against that prevents
        an AttributeError on .get().
        """
        for entry in (person.get("other_identifiers") or []):
            if not isinstance(entry, dict):
                continue
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
        jurisdiction: str | None = None,
    ) -> dict | None:
        """GET with rate-limit + 429-retry. Returns the JSON body on 2xx.

        Returns None for 404 only when ``allow_404`` is True. Raises
        OpenStatesRateLimitError on persistent 429; OpenStatesError on
        any other non-2xx or transport failure that exhausts retries.

        When ``jurisdiction`` is given, routes to the local OpenStates
        replica instead of the public API if that jurisdiction is listed in
        self.ddp_openstates_jurisdictions (SYNC-8) -- see
        _get_api_base_and_key(). Callers with no jurisdiction in scope
        (fetch_by_id) omit it and always get the public API.
        """
        if jurisdiction is not None:
            api_base, api_key, is_local_replica = self._get_api_base_and_key(jurisdiction)
        else:
            api_base, api_key, is_local_replica = self.openstates_api_base, self.api_key, False

        url = f"{api_base}{path}"
        if is_local_replica:
            # Local api-v3's apikey_auth is a query param, not the public
            # API's x-api-key header.
            headers = {"accept": "application/json"}
            if api_key:
                params = list(params) + [("apikey", api_key)]
        else:
            headers = {
                "x-api-key": api_key,
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
                # Phase-3 stability fix: 404s on /people/{id} are sometimes
                # transient and contribute to the orphan-set instability
                # observed across bio-sync runs (different records flagged
                # as orphan each run with no overlap). Retry with backoff
                # before classifying as a real "not in OpenStates"; only
                # the persistent 404 across retries returns None.
                if attempt < self.max_retry_attempts - 1:
                    wait = 2 ** attempt + random.uniform(0, 0.5)
                    logger.warning(
                        "OpenStates 404 — retrying (may be transient)",
                        attempt=attempt + 1,
                        path=path,
                        metric="openstates.transient_404_retry",
                    )
                    await asyncio.sleep(wait)
                    continue
                # Final attempt was 404 — emit a metric so dashboards
                # can distinguish real orphans from transient-recovered
                # flakes via the existing transient_404_retry counts.
                logger.info(
                    "OpenStates 404 persisted across retries",
                    path=path,
                    attempts=self.max_retry_attempts,
                    metric="openstates.persistent_404",
                )
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

        # Exhausted retry budget. Persistent 404 with allow_404 → None
        # (real orphan; not retried again).
        if (
            last_resp is not None
            and last_resp.status_code == 404
            and allow_404
        ):
            return None
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
