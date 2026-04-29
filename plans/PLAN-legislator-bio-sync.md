# PLAN: Legislator Bio + Contact Sync

**Status:** Draft for review
**Created:** 2026-04-29
**Repo:** ddp-sync
**Target:** Phase 1 in ~2 weeks; Phase 2 in 4–6 weeks; Phases 3–4 in backlog

---

## Goal

Enrich the existing Webflow **Legislators** CMS collection with biographical, contact, term, and social-media fields for federal and state legislators. Source data comes from authoritative free + already-paid feeds; published to the live DDP website via Webflow CMS.

## Scope

**In scope (Phase 1–2):**
- All 535 current US Congress members (federal)
- US Congress members departed since 2023-01-01 (~173 members — the realistic backfill set)
- State legislators in the 7 currently active jurisdictions: FL, VA, WA, UT, AZ, MI, MA
- Robust handling of editor-created CMS items (DDP is about to bulk-create hundreds of state-legislator entries for post-session scorecards)

**Out of scope (Phase 1):**
- Narrative biographical prose (deferred to Phase 4)
- State coverage beyond the 7 active jurisdictions (planned for end-of-2026 expansion)
- Photo asset upload to Webflow (Phase 3)
- Ballotpedia data ingestion (deferred indefinitely; we store the slug as a reference link only)

---

## Sources

| Source | Auth | Use |
|---|---|---|
| `unitedstates/congress-legislators` (`legislators-current.yaml`, `legislators-historical.yaml`) | None (public) | Federal current + historical bio, contact, terms, IDs |
| `unitedstates/congress-legislators` (`legislators-social-media.yaml`) | None | Federal social handles |
| OpenStates `/people` v3 | `OPENSTATES_API_KEY` (existing 30k/day tier) | All state legislators + current federal members; provides `bioguide` via `other_identifiers` |
| Congress.gov API | `CONGRESS_API_KEY` (existing) | Reserved for portrait quality + leadership metadata; not strictly required for Phase 1 |

**References:**
- https://github.com/unitedstates/congress-legislators
- https://docs.openstates.org/api-v3/

---

## Existing infrastructure (reused — do not duplicate)

Repo audit (2026-04-29) confirms these components already exist; Phase 1 extends them rather than creating parallel implementations.

| Component | Location | What it provides | What we extend |
|---|---|---|---|
| `WebflowLookupService` | `services/webflow_lookup.py` | Webflow PATCH (despite the "Lookup" name — it's the write service). Used by `pipelines/bill_version.py` Flow 1. Has API-key fallback (`webflow_scheduler_api_key` → `webflow_votebot_api_key`). | Add: 429/Retry-After handling, in-process rate-limiter (shared across pipelines in the same worker), `update_legislator_fields()`, `create_legislator_draft()`, field-existence tolerance for incremental schema rollout. Consider renaming to `WebflowWriteService` in a follow-up. |
| `RateLimitConfig` + `_apply_rate_limit()` | `pipelines/legislator_sync.py` (also duplicated in `bill_sync.py`) | Sleep-based rate limiter for OpenStates calls; loads from `rate_limit:` config block | Reuse via import (not copy). Document the existing 2-pipeline duplication as an opportunistic follow-up refactor — not blocking for bio sync. |
| `push_alert_to_zapier()` | `pipelines/voatz_brevo.py` | Posts run summary to a configured Zapier webhook | Reuse for bio-sync run summaries. Pattern is the established alerting mechanism for this repo; introducing Sentry/CloudWatch would be a separate initiative. |
| `ZAPIER_WEBHOOK_URL` | `config.py` + `.env.example` | Already configured | No change |
| `dry_run` semantics | `sync/types.py::SyncOptions`, `api/routes/sync_unified.py` | Standard pattern across the repo | Bio sync follows the same convention |
| `sync_schedule.yaml` `notifications:` block | `config/sync_schedule.yaml` | Declared but **never wired** (marked "future use" — `on_failure`, `on_large_changes`) | Phase 1 does not wire it; uses the Zapier pattern matching `voatz_brevo`. Can be unified later as a repo-wide initiative. |

**What does NOT exist yet (real gaps to build):**
- Webflow rate-limiter / 429 handling (currently `WebflowLookupService` is raw `httpx.patch` with 15s timeout)
- Cross-process / Redis-backed rate coordination (single-worker is fine for now)
- Sentry / CloudWatch metrics / on-call escalation (out of scope for this plan)
- Durable PATCH-diff archive (only Redis 30-day TTL planned for Phase 1)

---

## Phase 0 probe findings (completed 2026-04-29)

These findings shape the design below.

| Question | Finding |
|---|---|
| Does OpenStates `other_identifiers` include `bioguide` for federal? | ✅ Yes (verified for Rick Scott — full ID set returned: bioguide, fec, govtrack, opensecrets, ballotpedia, wikidata, votesmart, lis) |
| Does OpenStates retain departed federal members? | ❌ No — Karen Bass (left Congress Jan 2023) returns 0 results from `/people?jurisdiction=us&name=Karen Bass`. **Bioguide-id fallback is mandatory for federal historical entries.** |
| State `other_identifiers` coverage | Mostly empty — FL/AZ samples returned `[]`; MA only `legacy_openstates`. Cross-source IDs are federal-only in practice. |
| State PII risk | Clean — capitol/district *offices* only, government emails (`@flhouse.gov` etc.), no home addresses |
| Federal PII risk | Clean — capitol offices, DC phones, contact forms |
| Federal `email` quirk | Often a contact-form URL, not an email (Rick Scott's `email`: `https://www.rickscott.senate.gov/contact/contact`) — sync detects URL shape and routes to `contact-form-url` |
| Photo source variance | Federal: clean predictable URL (`unitedstates.github.io/images/congress/450x550/{bioguide}.jpg`). State: per-state CDNs of varying reliability (FL House CMS, MA legislature, AZ third-party Apptegy). Justifies isolating photo upload as Phase 3. |
| `bio.birthday` availability | 100% for federal current + historical; mixed for state (MA had it, FL/AZ did not). Store year-only on CMS. |

**ID-field coverage on 536 current federal members:**

| ID | Coverage | Notes |
|---|---|---|
| `bioguide` | 100% | Universal join key |
| `wikidata` | 100% | |
| `govtrack` | 100% | |
| `fec` | 99% | List-typed; skip for now |
| `opensecrets` | 97% | |
| `ballotpedia` | 97% | |
| `votesmart` | 97% | Skip — no current use |
| `thomas` | 41% | Skip — sparse |
| `lis` | 18% | Skip — sparse |

---

## Identity & join strategy

Single primary join key — **`openstatesid`** — for all jurisdictions (state + federal current). One sync code path, no per-jurisdiction branching.

`bioguide-id` is **stored** on every federal record but **only joined on as a fallback** for federal CMS entries that no longer resolve in OpenStates (i.e., departed members):

```
                              ┌─ Found in OpenStates?  → use openstatesid
Federal CMS entry lookup  ────┤
                              └─ Not found?            → fall back to bioguide-id,
                                                         join to legislators-historical.yaml
```

State legislators only ever use `openstatesid` (no historical YAML for state).

**Why no separate DDP-internal ID:** The Webflow item ID + slug already serve as DDP's persistent identity. Adding a third key would duplicate identity infrastructure already in place.

**Cross-source IDs (stored, not joined):** `bioguide-id`, `wikidata-id`, `opensecrets-id`, `ballotpedia-slug`, `govtrack-id`. Populated automatically. Used by the website for outbound reference links to those platforms — even though we don't ingest data from those sources yet, we get the IDs for free from the unitedstates dataset.

---

## Field precedence & overwrite policy (Phase 1 prerequisite)

This was marked deferred in an earlier draft; pm-review correctly flagged that it must be decided **before any write**, because the wrong rule can silently erase editor work.

### The cardinal rule: never blank a populated CMS field with an empty upstream value

```python
# Values empirically observed as "no data" markers in OpenStates / unitedstates
# feeds. Phase 0 probe saw None, missing key, and "" for unpopulated state-leg
# birth_date / biography. The remainder are defensive guards against placeholder
# sentinels that may appear later (per pm-review round 2 recommendation).
EMPTY_VALUES = {
    None, "",
    "-", "—",
    "N/A", "n/a", "NA", "na",
    "TBD", "tbd",
    "UNKNOWN", "unknown",
    "null", "NULL",
}

def is_empty(value) -> bool:
    if value in EMPTY_VALUES:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, dict)) and not value:
        return True
    return False

def should_write(field_name: str, cms_value, upstream_value) -> bool:
    if is_empty(upstream_value):
        return False                  # never overwrite with empty
    if cms_value == upstream_value:
        return False                  # no-op, skip
    if field_name in LOCKED_FIELDS:
        return False                  # editor-curated, never sync-overwrite
    return True
```

**Numeric zero is intentionally excluded from `EMPTY_VALUES`.** A `district` of `0` (some at-large jurisdictions) is a valid value, not an empty. Per-field validators can override this behavior if a field's `0` is meaningful-as-empty (none currently identified).

**Discovered sentinels:** Phase 1 dry-run output explicitly logs every value that triggers `is_empty()` so editors can review whether the rule needs to be tightened or loosened. New sentinels get added to `EMPTY_VALUES` as evidence accumulates — not pre-emptively guessed.

Editors can fill a field manually; if upstream loses that data later, the CMS keeps the editor's value. The cost is some "drift" between CMS and upstream, but the alternative — silently blanking editor work — is the larger trust risk.

### Source precedence per field

When **both** unitedstates and OpenStates have a value for the same federal member, this matrix decides:

| Field | Federal precedence | State precedence |
|---|---|---|
| Name (display) | unitedstates `name.official_full` | OpenStates `name` |
| Birth year | unitedstates `bio.birthday` (year) | OpenStates `birth_date` (year) |
| Gender | unitedstates `bio.gender` | OpenStates `gender` |
| Capitol phone / address | unitedstates `terms[-1]` | OpenStates office where `classification=capitol` |
| District phone / address | (n/a) | OpenStates office where `classification=district` |
| Contact form URL | unitedstates `terms[-1].contact_form` | OpenStates `email` if URL-shaped |
| Email | (federal usually blank) | OpenStates `email` if email-shaped |
| Official website | unitedstates `terms[-1].url` | OpenStates `links[].url` where `note=website` |
| Term start / end | unitedstates `terms[0].start` / `terms[-1].end` | OpenStates `current_role` (no full term history) |
| Photo source URL | derived from bioguide-id | OpenStates `image` |
| Twitter / Facebook / Instagram / YouTube | unitedstates social-media YAML | OpenStates `other_identifiers` (rarely populated for state) |
| All cross-source IDs | unitedstates `id` block | OpenStates `other_identifiers` |

Content team sign-off on this matrix is a Phase 1 prerequisite, not deferrable.

### Multi-office address resolution

Sources may return >1 office. Selection rules:

- **Federal:** always use `terms[-1]` (latest term) for office address/phone. If multiple offices exist in one term, prefer the one named "Capitol Office" or fall back to the first entry.
- **State:** prefer `classification=capitol` for `phone-capitol` / `office-address-capitol`; prefer `classification=district` for `phone-district` / `office-address-district`. If a state has only one office (most common), populate the matching slot and leave the other blank.
- **Multiple district offices** (e.g., Rick Scott has 7 FL district offices): pick the first one returned by OpenStates. Phase 1 limitation; Phase 2 may add a `district-offices` repeater field if editors push for it.

### Per-field sync opt-out

- **Repo-level (Phase 1):** `legislator_bio_sync.locked_fields: [official-website, contact-form-url]` in `sync_schedule.yaml` disables those fields for all records. Used when DDP wants editor-only control over a field globally.
- **Per-record (deferred):** if usage shows individual records need protection, Phase 2 can add a `bio-sync-locked` boolean on the Legislators collection.

---

## Architecture

Mirrors the bill-sync **Flow 1 / Flow 2 decoupling** pattern (`pipelines/bill_version.py`, documented in `TROUBLESHOOTING.md` → "Data Flow Decoupling 2026-03-11").

```
                        ┌── unitedstates YAMLs (federal current + historical + social)
fetch_all_sources ──────┤
                        └── OpenStates /people    (state + federal current)
                                      │
                                      ▼
                          build_legislator_payloads
                                      │
                  ┌───────────────────┴──────────────────┐
                  ▼                                      ▼
          Flow A: Webflow CMS                    Flow B: Pinecone
       (NEW — bio/contact/term/social)        (existing — RAG content)
        PATCH-or-create-as-draft                  no behavior change
```

Either flow can fail independently without blocking the other — same guarantee the bill sync provides today.

---

## New code modules

### `src/ddp_sync/services/congress_legislators.py`

YAML fetcher with on-disk cache (24h TTL). Cache lives at `~/.cache/ddp-sync/congress-legislators/{filename}` to avoid re-downloading the 8.6 MB historical file every run.

```python
class CongressLegislatorsSource:
    """Loads and caches the unitedstates/congress-legislators YAML dataset."""

    BASE_URL = "https://unitedstates.github.io/congress-legislators"
    CACHE_TTL_SECONDS = 24 * 60 * 60
    CACHE_DIR = Path.home() / ".cache" / "ddp-sync" / "congress-legislators"
    FILES = (
        "legislators-current.yaml",
        "legislators-historical.yaml",
        "legislators-social-media.yaml",
    )

    async def warm_cache(self) -> None: ...
    async def get_by_bioguide(self, bioguide_id: str) -> CongressLegislator | None: ...
    async def iter_current(self) -> AsyncIterator[CongressLegislator]: ...
    async def iter_historical_since(
        self, end_date: date
    ) -> AsyncIterator[CongressLegislator]:
        """Historical members whose last term ended on/after end_date."""

@dataclass
class CongressLegislator:
    bioguide_id: str
    name: dict       # {first, last, middle?, official_full}
    bio: dict        # {birthday, gender}
    terms: list[dict]
    ids: dict        # {bioguide, wikidata, opensecrets, ballotpedia, govtrack, fec, ...}
    social: dict     # {twitter, facebook, instagram, youtube, ...}
```

### `src/ddp_sync/services/openstates_people.py` — IMPLEMENTED

Async client over OpenStates v3 `/people`. Takes a shared `RateLimiter` so bio-sync can coordinate its OpenStates budget with future pipelines. Distinguishes 404 (returns None — bioguide-id fallback signal) from hard failures (raises `OpenStatesRateLimitError` on persistent 429, `OpenStatesError` on other non-2xx).

```python
class OpenStatesError(Exception): ...
class OpenStatesRateLimitError(OpenStatesError): ...

@dataclass
class OpenStatesPerson:
    openstates_id: str       # ocd-person/...
    name: str
    family_name, given_name, party, gender, birth_date, death_date
    email, image, biography
    jurisdiction_name: str | None    # "United States" for federal, state name otherwise
    current_role: dict
    other_identifiers, other_names, links, sources, offices: list[dict]
    raw: dict                # full upstream record

    def get_other_id(self, scheme: str) -> str | None: ...
    @property
    def chamber(self) -> str | None: ...      # upper / lower
    @property
    def district(self) -> str | None: ...
    @property
    def is_federal(self) -> bool:
        """True iff jurisdiction_name == 'United States'.
        Probe-confirmed: federal members' division_id contains /state:XX
        because they represent specific states — naive division-id check
        would mis-classify them as state."""

class OpenStatesPeopleClient:
    BASE_URL = "https://v3.openstates.org"
    INCLUDE_PARAMS = ("other_names", "other_identifiers", "links", "sources", "offices")

    def __init__(self, api_key, rate_limiter, *, max_retry_attempts=3, per_page=50): ...

    async def fetch_by_id(self, openstates_id) -> OpenStatesPerson | None:
        """Returns parsed record on 2xx, None on 404. Raises on persistent 429
        or other non-2xx."""

    async def iter_jurisdiction(self, jurisdiction) -> AsyncIterator[OpenStatesPerson]:
        """Paginated /people?jurisdiction=X. Always passes the full include set."""

    @staticmethod
    def extract_other_id(person: dict, scheme: str) -> str | None: ...
```

Verified against the live API (4 smoke tests with the dev key): Rick Scott returns with full ID set + `is_federal=True`; nonexistent ID returns None; `extract_other_id` static helper works; `iter_jurisdiction("fl")` yields state legislators paginated.

### `src/ddp_sync/pipelines/legislator_bio.py`

Orchestrator. For each CMS legislator:
1. Look up upstream record (OpenStates first; bioguide-id fallback for federal historical).
2. Build the field payload (merging unitedstates + OpenStates data).
3. Diff against current CMS state — only PATCH **changed** fields.
4. Auto-create gated by config; auto-created entries always land as `isDraft: true`.

```python
@dataclass
class BioSyncOptions:
    target: Literal["all", "webflow", "pinecone"] = "all"
    jurisdiction: str | None = None       # None = all configured
    auto_create: bool = False
    dry_run: bool = False
    limit: int = 0                         # 0 = unlimited
    historical_since: date = date(2023, 1, 1)

@dataclass
class BioSyncReport:
    cms_items_seen: int
    would_patch: list[dict]                # [{webflow_id, name, changed_fields}]
    would_create: list[dict]
    potential_merges: list[dict]           # state→federal transition candidates
    upstream_orphans: list[dict]           # in CMS but not found upstream
    errors: list[str]

class LegislatorBioPipeline:
    async def run(self, options: BioSyncOptions) -> BioSyncReport:
        # 1. Build CMS index (one Webflow read pass)
        cms_index = await self._build_cms_index()

        # 2. Warm source caches
        await self.congress.warm_cache()

        # 3. For each CMS item: resolve upstream → diff → write
        for cms_item in cms_index:
            upstream = await self._resolve_upstream(cms_item)
            if not upstream:
                report.upstream_orphans.append(cms_item.summary())
                continue
            payload = self._build_payload(cms_item, upstream)
            changed = self._diff_fields(cms_item, payload)
            if not changed:
                continue
            if options.dry_run:
                report.would_patch.append({
                    "webflow_id": cms_item.id, "name": cms_item.name,
                    "changed_fields": list(changed),
                })
            else:
                await self.webflow.patch_legislator(cms_item.id, changed)

        # 4. Optional: discover upstream-only legislators (for auto-create)
        if options.auto_create:
            await self._discover_and_create_drafts(cms_index, options, report)

        return report
```

### Webflow PATCH support — extend `WebflowLookupService`

The existing `services/webflow_lookup.py::WebflowLookupService` is **already the Webflow write service** (despite its misleading name). It's used by `pipelines/bill_version.py` for Flow 1 status PATCHes. **No new service class is needed.** We extend it.

What's missing in the existing service: rate-limiter, 429/Retry-After handling, draft-create endpoint, field-existence tolerance. Adding these here means **bill sync inherits all four improvements** automatically.

**Webflow tier:** 120 req/min on the current plan. Process-local in-instance limiter at **≤60 req/min** for the shared write service leaves 60 req/min headroom for other writers (the unified-sync API, ad-hoc trigger calls). Cross-process / multi-worker coordination is out of scope for Phase 1 — single ddp-sync deployment, one APScheduler leader, so the in-process limiter is sufficient.

```python
# services/webflow_lookup.py — proposed extensions

class WebflowLookupService:
    # Existing: __init__, update_bill_fields, update_bill_gov_url

    def __init__(self, settings=None, *, max_requests_per_minute: int = 60):
        ...
        # Shared limiter — protects ALL pipelines using this service in the
        # same Python worker (bill sync, bio sync, future writers).
        self._limiter = TokenBucketLimiter(
            rate=max_requests_per_minute / 60.0,
            capacity=max_requests_per_minute,
        )
        self._legislators_collection_id = self.settings.webflow_legislators_collection_id

    async def _patch_with_backoff(self, url, headers, payload) -> httpx.Response:
        """Shared PATCH wrapper — limiter, 429 retry, Retry-After honoring.
        Raises WebflowRateLimitError if 429 persists after final retry.
        Non-2xx responses other than 429 are returned for caller to inspect."""
        last_resp = None
        for attempt in range(3):
            await self._limiter.acquire()
            last_resp = await self._client.patch(url, headers=headers, json=payload)
            if last_resp.status_code != 429:
                return last_resp
            wait = float(last_resp.headers.get("Retry-After", 2 ** attempt))
            await asyncio.sleep(wait + random.uniform(0, 0.5))   # jitter
        raise WebflowRateLimitError(
            f"Webflow returned 429 after 3 retries: {url}",
            response=last_resp,
        )

    # NEW for bio sync:

    async def update_legislator_fields(
        self,
        webflow_id: str,
        field_data: dict,
        *,
        publish: bool = True,
    ) -> WebflowPatchResult:
        """PATCH a Legislators item. Returns a WebflowPatchResult capturing
        success/failure and dropped fields. Raises WebflowRateLimitError on
        persistent 429; raises WebflowError on other non-2xx — callers MUST
        treat these as run failures and surface in BioSyncReport.errors."""

    async def create_legislator_draft(self, field_data: dict) -> WebflowCreateResult:
        """POST /collections/{id}/items with isDraft=true. Returns the new
        webflow_id on success; raises WebflowError on non-2xx."""
```

### Error contract (round-3 addition)

```python
class WebflowError(Exception):
    """Base for any non-success Webflow response."""
    def __init__(self, message: str, *, response: httpx.Response | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code if response is not None else None

class WebflowRateLimitError(WebflowError):
    """Raised when Webflow returns 429 after the configured retry budget is exhausted."""

@dataclass
class WebflowPatchResult:
    success: bool                  # True iff response was 2xx
    webflow_id: str
    dropped_fields: set[str]       # fields that don't exist in the collection
    status_code: int
    error_detail: str | None = None
```

**Caller contract:**
- `update_legislator_fields()` raises on non-2xx (including persistent 429). Callers in `pipelines/legislator_bio.py` catch `WebflowError`, log via structlog, and append to `BioSyncReport.errors`. The run continues for the next CMS item — one bad PATCH does not abort the whole run.
- `WebflowPatchResult.success=False` only ever appears when the PATCH succeeded HTTP-wise but had partial issues (e.g., `dropped_fields` non-empty for incremental schema rollout). Never a silent error mask.
- `bill_version.py`'s existing `update_bill_fields()` is migrated to the same contract (raises on non-2xx instead of returning False) — this is a behavior change for bill sync; **migration plan** in "Backwards compatibility" below.

### Backwards compatibility (round-4 revision)

`WebflowLookupService.__init__` gains a new `max_requests_per_minute` keyword argument (default 60). All existing call sites use the no-argument form, so this is purely additive.

**On the `update_bill_fields` migration** — round-3 review recommended changing its return contract from `bool` to a raising `WebflowPatchResult`. After implementation, this was reconsidered: the existing callers in `pipelines/bill_version.py:264-285, 373-401` already wrap the call in `try/except + bool check`, which is **semantically equivalent** to the raising contract. Forcing the migration would touch the bill-sync code without a behavior change. Decision:

- `update_bill_fields()` keeps the legacy `bool` return contract — **no caller migration needed.**
- It now routes through the new `_patch_with_backoff()` helper internally, so it inherits the rate-limiter and 429 retry. `WebflowRateLimitError` raised inside the helper is caught by the existing `try/except` and converted to `False` — exactly what the legacy callers expect.
- New methods `update_legislator_fields()` and `create_legislator_draft()` use the raising contract for bio-sync's needs (where one PATCH failing should land in `BioSyncReport.errors` rather than be papered over with a False).

This satisfies the round-3 reviewer's underlying concern (no silent data loss; persistent 429 always raises somewhere) without forcing any change to existing pipelines.

**Renaming `WebflowLookupService` → `WebflowWriteService`** is a pure rename and held as a follow-up commit. Touches every import site (`pipelines/bill_version.py:264, 373` and others); not blocking for Phase 1.

### Single-worker assumption (round-3 addition)

The in-process limiter is sufficient **only if there is exactly one Python process making Webflow writes at a time**. Phase 1 explicitly assumes:
- One ddp-sync deployment (one EC2 host, one systemd unit `ddp-sync.service`)
- APScheduler runs in a single worker (single uvicorn process — confirm with `ps aux | grep uvicorn` after deploy; multi-worker uvicorn would breach this assumption)
- Ad-hoc CLI / `/trigger` endpoint invocations against the same host go through the same `WebflowLookupService` instance (same process), so they share the limiter naturally
- `sync_unified.py`'s leader-election (Redis-based) ensures only one worker runs scheduled jobs

**Guardrail:** at process startup, log a warning if `UVICORN_WORKERS > 1` is detected. Documented in deployment README.

**CLI dry-run + scheduled job overlap:** they share the same Python process and same `WebflowLookupService` singleton, so they share the limiter. Two simultaneous bursts cannot exceed the configured per-process budget.

**When this assumption breaks:** multi-worker, multi-host, or multi-region deployments need a Redis-backed cross-process limiter. This is documented as a Phase 2+ requirement gated on infrastructure changes — not a current need.

### OpenStates client + shared rate-limiter (round-3 revision)

Round-3 review reversed the earlier deferral: `RateLimitConfig` extraction is a **Phase 1 deliverable**, not a follow-up.

**Phase 1 step (atomic with the new modules):**
1. Create `services/rate_limiter.py` exposing `RateLimitConfig` (dataclass, behavior-preserving move) + a new stateful `RateLimiter` class wrapping `apply()` (was `_apply_rate_limit`)
2. Migrate `pipelines/legislator_sync.py` and `pipelines/bill_sync.py` to import from `services/rate_limiter.py` (their inline copies are deleted; method calls update from `self._apply_rate_limit()` to `self.rate_limiter.apply()`)
3. New `services/openstates_people.py` uses the same shared module — three callers, one source of truth
4. CI runs existing legislator-sync + bill-sync tests against the migrated code before merge

This avoids the "sideways import" coupling round-3 flagged (`legislator_bio.py` importing from `legislator_sync.py`) and eliminates the existing duplication in one motion. Bumps `Retry-After` honoring from partial (`fetch_sponsored_bills` only) to all OpenStates calls. OpenStates capped ≤50 req/min for bio sync.

**Concurrency safety (round-4 fix):** `RateLimiter.apply()` holds an `asyncio.Lock` for the whole compute-and-sleep window so `asyncio.gather()`-style concurrency cannot bypass the inter-call gap by reading `_last_request_time` before another task updates it. The limiter exposes an `enforced_sleeps` counter for runtime observability. None of the current call sites use `gather()` over rate-limited operations — they're sequential awaits in for-loops — so this is future-proofing.

**API consistency (round-4 fix):** `CongressLegislatorsSource` exposes only async public methods. `get_by_bioguide()`, `iter_current()`, `iter_historical_since()` all auto-warm the cache on first call so callers don't have to track warm state. The previous sync `get_by_bioguide()` that raised on unwarmed cache has been removed in favor of the async signature.

**`from_yaml` contract (round-4 fix):** `RateLimitConfig.from_yaml()` **never raises** — returns defaults on missing file, malformed YAML, or missing keys. Existing pipelines depend on this (their prior inline `_load_rate_limit_config` did the same). Documented in the docstring.

### State→federal transition detection (strengthened from pm-review)

Initial last-name + state heuristic is too weak (false-positives on common surnames, misses on hyphenation/marriage). Replaced with a multi-signal scoring approach: a candidate is flagged only when **two or more** signals match.

```python
def find_merge_candidate(
    new_member: CongressLegislator,
    cms_index: CMSIndex,
) -> tuple[CMSItem | None, list[str]]:
    """Returns (candidate, matched_signals). Candidate only flagged if score >= 2."""
    state = new_member.terms[-1]["state"]
    new_full = (new_member.name.get("official_full") or
                f"{new_member.name['first']} {new_member.name['last']}").lower()
    new_birth_year = _year_from_iso(new_member.bio.get("birthday"))
    new_first_term_start = new_member.terms[0]["start"]

    best, best_signals = None, []
    for cms_item in cms_index.by_state(state):
        signals = []
        # Signal 1: family name AND first-name initial match (filters "Smith" noise)
        if (cms_item.family_name.lower() == new_member.name["last"].lower()
                and cms_item.given_name[:1].lower() == new_member.name["first"][:1].lower()):
            signals.append("name_match")
        # Signal 2: full-name similarity >= 0.85 (Jaro-Winkler, handles hyphenations)
        if jaro_winkler(cms_item.full_name.lower(), new_full) >= 0.85:
            signals.append("name_similarity")
        # Signal 3: birth year matches (federal always has it; state sometimes)
        if new_birth_year and cms_item.birth_year == new_birth_year:
            signals.append("birth_year_match")
        # Signal 4: bioguide-id explicitly present and matches
        if cms_item.bioguide_id and cms_item.bioguide_id == new_member.ids["bioguide"]:
            signals.append("bioguide_match")    # decisive; treat as score=2
        # Signal 5: term continuity (state-leg ends before/around federal term start)
        if cms_item.term_end and abs((cms_item.term_end - new_first_term_start).days) <= 730:
            signals.append("term_continuity")

        score = 2 if "bioguide_match" in signals else len(signals)
        if score >= 2 and len(signals) > len(best_signals):
            best, best_signals = cms_item, signals

    return best, best_signals
```

Editor sees: *"Potential merge: existing CMS item 'Jane Smith-Garcia (FL state senate)' looks like the same person as new federal member 'Jane Smith (FL US House)'. Matched signals: name_similarity, birth_year_match. Review and merge if so."*

The dry-run report surfaces all candidates; auto-create is skipped for any flagged item. False-positive rate will be measured during the rollout — see Testing & rollout section.

### Federal `email` URL detection

```python
def split_email_field(value: str) -> tuple[str | None, str | None]:
    """Returns (email, contact_form_url). At most one is non-None."""
    if not value:
        return None, None
    if value.startswith(("http://", "https://")):
        return None, value
    return value, None
```

---

## Configuration

Extends `config/sync_schedule.yaml`:

```yaml
legislator_bio_sync:
  enabled: true
  sync_time_utc: "06:00"            # piggyback on existing weekly slot
  frequency: weekly
  sync_day: sunday

  # Federal: pull all 535 current + departed since 2023-01-01
  federal:
    enabled: true
    historical_since: "2023-01-01"
    auto_create: false              # editors review before publishing

  # State: per-jurisdiction toggles
  state:
    enabled: true
    auto_create: false
    jurisdictions:                  # empty = inherit active_jurisdictions
      - FL
      - VA
      - WA
      - UT
      - AZ
      - MI
      - MA

  # Source data refresh
  unitedstates_cache_ttl_hours: 24
```

---

## API endpoints

New trigger endpoint (mirrors the existing `/trigger/bill-version-check` pattern):

```
POST /ddp-sync/v1/trigger/legislator-bio-sync
```

**Query params:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `dry_run` | bool | false | Preview the diff without writing |
| `auto_create` | bool | (config) | Override per-run |
| `jurisdiction` | str | (all configured) | Limit to one state code (e.g. `FL`, `us`) |
| `target` | enum | `all` | `all` / `webflow` / `pinecone` |
| `limit` | int | 0 | Cap items processed (0 = unlimited) |

**Returns:** `BioSyncReport` JSON.

**Use cases:**
- Pre-bulk-create dry run: editors run `?dry_run=true&jurisdiction=FL` before bulk-creating FL state-leg CMS entries to preview what would be PATCHed
- On-demand backfill: `?jurisdiction=us&historical_since=2023-01-01` to pull post-2022 departed members
- Single-state testing during rollout

---

## Webflow CMS schema additions

**See companion document `plans/webflow-legislator-fields.md`** for the editor-facing field-add checklist (21 new fields, all optional).

The sync code will **gracefully no-op on any field not yet present** in Webflow — it catches `field not found` errors during PATCH and logs a warning. This means the field rollout can happen incrementally without breaking sync.

---

## Pre-flight audits (must pass before first live write)

Two audits gate the rollout. Both run as commands inside the new pipeline (`--audit-only` flag) and produce reports for editor review.

### Audit A — Federal join-key coverage

For every CMS legislator with `chamber` in (Senate, House) — i.e., federal — verify that **at least one** of `openstatesid` or `bioguide-id` is populated. If both are missing, the sync cannot resolve the upstream record without a name-match (which has known false-positive risk).

```python
async def audit_federal_join_keys(cms_index) -> AuditReport:
    """Surface federal CMS records that lack both join keys."""
    missing = [item for item in cms_index.federal()
               if not item.openstatesid and not item.bioguide_id]
    return AuditReport(
        total_federal=cms_index.federal_count(),
        missing_both_keys=missing,
    )
```

**Action on findings:** editors manually populate `bioguide-id` (preferred — stable join key) for each flagged record before auto-create is enabled. Single-pass remediation; should be a one-time clean-up.

### Audit B — Bulk-import readiness

Before the first scheduled run after editors complete the bulk-create of state-legislator scorecards, run `--audit-only` to verify:
- Every newly-created CMS legislator has `openstatesid` populated
- No two CMS records share an `openstatesid` (would indicate a duplicate created during bulk-import)

Sequencing rule: **first scheduled sync runs only after Audit B passes.** Before that, only manual dry-run via the trigger endpoint is permitted.

### Audit C — Pre-existing state CMS records lacking `openstatesid`

Audit B catches duplicates introduced *during* the bulk-create. Audit C catches the orthogonal case: state CMS records that **already** existed before the bulk-create and never had `openstatesid` populated. Without this audit, auto-create could silently duplicate a pre-existing record by creating a new draft for the same person.

```python
async def audit_state_join_keys(cms_index) -> AuditReport:
    """Surface state CMS records without openstatesid (pre-existing, not bulk-import)."""
    missing = [item for item in cms_index.state()
               if not item.openstatesid]
    return AuditReport(
        total_state=cms_index.state_count(),
        missing_openstatesid=missing,
    )
```

**Action on findings:** editors populate `openstatesid` (preferred) or mark records as `bio-sync-skip: true` (Phase 2 field) before auto-create is enabled per jurisdiction. **Auto-create cannot be enabled for a jurisdiction until Audit C is clean for that jurisdiction.**

---

## Run-summary alerting

Reuses the existing repo pattern: `pipelines/voatz_brevo.py::push_alert_to_zapier()` posting to `ZAPIER_WEBHOOK_URL`.

After each scheduled run (not dry-run), the bio sync posts a summary:

```python
push_alert_to_zapier(settings.zapier_webhook_url, [{
    "alert_type": "legislator_bio_sync_complete",
    "items_seen": report.cms_items_seen,
    "patched": len(report.would_patch),       # actual patches when not dry-run
    "created": len(report.would_create),
    "potential_merges": len(report.potential_merges),
    "upstream_orphans": len(report.upstream_orphans),
    "errors": len(report.errors),
}])
```

Threshold-based alert escalation (`on_failure`, `on_large_changes`) follows the `notifications:` block in `sync_schedule.yaml`. Note: this block is currently scaffolded but not wired up anywhere in the repo — bio sync is the first consumer. Wiring it up here means future pipelines can adopt the same convention.

**Out of scope for this plan:** Sentry, CloudWatch metrics, on-call escalation. Those are a separate cross-cutting initiative; bio sync should not unilaterally introduce a new monitoring stack.

---

## Testing & rollout

### Unit tests (Phase 1 deliverable)

| Test | Asserts |
|---|---|
| `test_should_write_skips_empty_upstream` | An editor-filled CMS field is preserved when upstream is null/`""` |
| `test_should_write_skips_sentinel_values` | `"-"`, `"N/A"`, `"TBD"`, whitespace-only, `[]`, `{}` all treated as empty |
| `test_should_write_preserves_numeric_zero` | `district=0` (at-large) is NOT treated as empty; gets written |
| `test_should_write_skips_locked_field` | Fields in `LOCKED_FIELDS` are never overwritten |
| `test_graceful_patch_drops_unknown_field` | PATCH against a CMS without the new field logs a warning and continues |
| `test_webflow_429_honors_retry_after` | 429 response with `Retry-After: 5` sleeps 5s + jitter and retries |
| `test_webflow_429_persistent_raises` | After 3 failed retries, raises `WebflowRateLimitError` (does not silently return) |
| `test_webflow_non_2xx_bubbles_to_run_summary` | Persistent 429 / 4xx during a bio-sync run lands in `BioSyncReport.errors`, with `errors_count > 0` in the Zapier summary |
| `test_webflow_limiter_shared_across_pipelines` | Two pipelines using the same `WebflowLookupService` instance share the rate budget |
| `test_audit_c_finds_state_records_lacking_openstatesid` | Fixture CMS index with mixed records → audit returns only state records missing `openstatesid` |
| `test_bill_sync_migrated_signature_compat` | Existing `update_bill_fields()` callers in `bill_version.py` work with the new raising contract (asserts no regression) |
| `test_split_email_field` | URL-shaped `email` routes to `contact-form-url`; bare email routes to `email` |
| `test_extract_other_id` | OpenStates `bioguide` extraction from `other_identifiers` |
| `test_find_merge_candidate_score_threshold` | Single-signal matches don't flag; ≥2 signals do; bioguide-only is decisive |
| `test_office_resolution_capitol_vs_district` | Multi-office records are split correctly into capitol/district fields |
| `test_historical_lookup_uses_in_memory_index` | YAML loaded once, indexed by bioguide-id; lookups are O(1) |
| `test_openstates_429_honors_retry_after` | Retry-After header is respected; exponential backoff on subsequent failures |

### Synthetic merge-detection validation

Pick 2–3 known historical state→federal transitions (e.g., a recent state legislator who ascended to Congress in 2023+) and run `find_merge_candidate` against staged CMS records. Measure:
- True-positive rate: should flag 100%
- False-positive rate on a sample of 100 unrelated state legislators: target <5%

### Rollout sequence

| Step | Gate |
|---|---|
| 1. Schema fields added to Webflow | Field-add checklist signed off by content team |
| 2. Audit A passes (federal join-key coverage) | All federal CMS records have ≥1 join key |
| 3. Audit C passes per jurisdiction (state join-key coverage) | All pre-existing state CMS records have `openstatesid` |
| 4. Phase 1 code deployed with `enabled: false` | Smoke test: trigger endpoint returns 200, scheduler shows job registered |
| 5. Dry-run against 5 federal members | Manually inspect the diff in `would_patch[]`; review `is_empty()` log to confirm rule isn't too aggressive |
| 6. Live PATCH against 1 low-stakes federal record | Editor confirms the CMS shows the expected updates |
| 7. Audit B passes | Bulk-create complete; no duplicate openstatesid |
| 8. Enable scheduler (`enabled: true`); first scheduled run | Watch logs for 429s, field-not-found warnings, orphan counts; first Zapier summary posts |
| 9. Phase 2: extend to 7 state jurisdictions | Per-state dry-run + per-state Audit C before each is enabled |

### Rollback procedure

Webflow does not have a CMS revision history exposed via API, so true rollback isn't free. Mitigations:
- **Drafts can be deleted via API** — auto-created drafts can be bulk-deleted by `created_by=ddp-sync` filter
- **PATCHes:** the dry-run report (stored in Redis with 30-day TTL) preserves the exact diff for every run, so a manual revert is possible per-record. If a mass-revert is needed, an `--undo-last-run` trigger can replay the inverse diff. Phase 1 includes this command but doesn't ship it as scheduled — manual editor action only.

---

## Phasing

**Phase 1 — Federal sync (target: 2 weeks)**

Round-3 review surfaced two scope additions to land **before** any new modules: the rate-limiter extraction and the `WebflowLookupService` error-contract migration. Both touch existing pipelines and need to land atomically with their migrations to avoid split-deploy breakage.

1. **Foundation refactor (atomic, one PR):**
   a. Create `services/rate_limiter.py` with `RateLimitConfig` + new stateful `RateLimiter` class
   b. Migrate `pipelines/legislator_sync.py` and `pipelines/bill_sync.py` to import from there (delete inline copies)
   c. Run existing legislator-sync + bill-sync tests; no behavior change expected
2. **Webflow service extension (atomic, one PR) — IMPLEMENTED:**
   a. ✅ `WebflowError` / `WebflowRateLimitError` / `WebflowPatchResult` / `WebflowCreateResult` defined
   b. ✅ `WebflowLookupService` extended with shared `RateLimiter` (≤60 req/min), `_patch_with_backoff()` / `_post_with_backoff()` (Retry-After honoring + WebflowRateLimitError on persistent 429), `update_legislator_fields()`, `create_legislator_draft()`, field-existence tolerance via cached collection-schema lookup
   c. ✅ `update_bill_fields()` keeps its legacy `bool` contract — routes through `_patch_with_backoff` internally so it inherits rate-limiting and 429 retry without forcing a caller migration. See "Backwards compatibility" section.
   d. Pre-merge smoke: `/trigger/bill-status-sync?dry_run=true` against staging — TODO before deploy
3. **New modules — IMPLEMENTED:**
   a. ✅ `services/congress_legislators.py` — bioguide-indexed in-memory cache; YAML parse via `asyncio.to_thread()` (round-5 fix) so 8.6 MB historical file doesn't block event loop
   b. ✅ `services/openstates_people.py` — async client + `OpenStatesPerson` dataclass; uses shared `RateLimiter`; full `Retry-After` honoring; 404 returns None; persistent 429 raises `OpenStatesRateLimitError`
4. `pipelines/legislator_bio.py` orchestrator using `should_write()` + `is_empty()`
5. Trigger endpoint with `dry_run` and `--audit-only` modes (Audits A and C as separate flags)
6. Audit A (federal join-key coverage) — editor remediation pass
7. Run-summary alerting via existing `push_alert_to_zapier()` pattern
8. Unit test suite (full matrix above) + synthetic merge-detection validation
9. Dry-run against 5 federal members → 1 live PATCH on low-stakes record

**Phase 2 — State coverage (target: 2–4 weeks after Phase 1)**
1. Extend orchestrator to handle 7 active jurisdictions
2. State-leg field mapping (handle empty `other_identifiers`, missing `birth_date`)
3. Auto-create gating per-jurisdiction (default false)
4. Schedule integration into weekly Sunday job
5. Deploy + monitor first scheduled run; tune backoff if 429s appear

**Phase 3 — Photo asset upload (TBD)**
1. Detect upstream image change via `photo-source-url` field
2. Download → upload to Webflow Assets API
3. Patch the `image` reference field on the legislator
4. Per-source error handling (state photo CDNs are unreliable; AZ uses third-party Apptegy)

**Phase 4 — Narrative bios (TBD)**
Re-evaluate if Phases 1–3 leave real gaps. Options at that time: Ballotpedia API (paid), curated wiki content, Wikipedia summary extraction, manual editor input.

---

## Risks & constraints

1. **OpenStates 429 rate limiting** — Already a known issue (`TROUBLESHOOTING.md`). Bio sync adds ~535 federal calls + ~7 jurisdiction iterations + ~750 state member calls per week. At 500ms delay this is ~11 minutes total, well under the 30k/day budget. If 429s appear, bump `delay_between_bills_ms` to 600–700.

2. **Webflow API rate limits** — Plan tier is **120 req/min**. The shared in-process limiter on `WebflowLookupService` is set to ≤60 req/min for bio-sync (and bill-sync inherits the same limiter when running in the same Python worker), leaving 60 req/min headroom for unified-sync API calls and ad-hoc triggers. PATCH-only-changed-fields keeps payloads small. With ~1300 total legislators worst case, ~22 minutes for full pass at the rate cap. **Cross-process / multi-worker rate coordination is not implemented — single ddp-sync deployment with one APScheduler leader makes process-local sufficient.** If that assumption changes (multi-worker, multi-region), Redis-backed coordination would be needed.

3. **State photo source instability** (deferred to Phase 3) — AZ photos come from third-party Apptegy CDN; not under state control. Re-upload pipeline will need retries + skip-on-fail semantics.

4. **Editor merges for state→federal transitions** — Detection prevents accidental duplicates but requires manual editor action. Frequency: a handful per election cycle. Acceptable.

5. **`bio.birthday` privacy optics** — Even though publicly available via Bioguide, full DOB feels personal. Store **year-only** on CMS; ignore the day/month from the source.

6. **Webflow field count cap** — Plans typically cap collections at 60 fields. Adding 21 brings the Legislators collection to ~33 (existing 12 + new 21). Should be fine; verify before rollout.

7. **Concurrency with imminent bulk-editor import** — Editors are about to bulk-create hundreds of state-legislator CMS records. Sequencing rule: **first scheduled run is gated on Audit B** (every new record has `openstatesid`, no duplicates). Until Audit B passes, only manual dry-runs via the trigger endpoint are permitted. See "Pre-flight audits" and "Rollout sequence" sections.

8. **Historical YAML hot-loop risk** — `legislators-historical.yaml` is 8.6 MB. Naive per-record scan is O(n²) on the lookup. Mitigation: `CongressLegislatorsSource.warm_cache()` builds an in-memory `dict[bioguide_id, record]` once per run; all lookups are O(1) thereafter.

---

## Open questions deferred

1. **What happens when a state-leg's `openstatesid` goes stale?** — When a state legislator leaves office, there's no historical YAML for state. Plan: tag as `upstream_orphan` and surface in the report; editors decide whether to mark the CMS item as past-tenure or remove. Phase 2 may add a `term-end-detected` heuristic (e.g., openstatesid 404s for 3+ consecutive weekly runs → auto-set `term-end`).

2. **Schedule cadence** — Weekly may be conservative for bio data that changes slowly. Could shift to bi-weekly later to reduce API spend if needed.

3. **Should we store `religion`?** — In unitedstates dataset but 0% populated for current members. Skip until populated.

4. **Per-record `bio-sync-locked` boolean field** — Phase 1 ships only the repo-level `locked_fields` config opt-out. If usage shows individual records need protection, Phase 2 can add a per-record CMS boolean. Defer until evidence is in.

---

## References

- `README.md` — Architecture section, Pub/sub events, API path conventions
- `TROUBLESHOOTING.md` — "Data Flow Decoupling 2026-03-11" describes the Flow 1 / Flow 2 pattern this design mirrors
- `src/ddp_sync/pipelines/bill_version.py` — reference implementation of decoupled write paths
- `src/ddp_sync/sync/handlers/legislator.py` — existing batch-sync handler (will be extended in Phase 2)
- `src/ddp_sync/pipelines/legislator_sync.py` — existing OpenStates client patterns (rate limiting, retry)
- `src/ddp_sync/ingestion/sources/webflow.py` — existing Webflow read patterns; PATCH support is the main gap to fill
- `src/ddp_sync/services/legislative_calendar.py` — pattern for OpenStates async API client structure
- `config/sync_schedule.yaml` — schedule config to extend
