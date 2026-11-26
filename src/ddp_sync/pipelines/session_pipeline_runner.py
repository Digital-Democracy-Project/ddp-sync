"""Session-targeted batch runner -- ddp-infra's PLAN-bill-document-provenance.md,
"Step 1, scoped version" (approved 2026-08-01 after 3 rounds of /pm-review).

Not the full 100k-bill backlog job "Processing pipeline shape" describes
elsewhere in that plan -- that job still needs real concurrency control and
weeks of runtime for the full historical corpus, neither of which this
module takes on. This is a much smaller, immediately buildable gap: there
was no way to point this pipeline at "every bill in session X" and have it
fill in whatever's missing -- every one of the 8 BillArtifact types plus
Organization Position Research was a single-bill, single-type manual call.
Motivated directly by trying to do exactly that against FL's real 2026F
session (2026-08-01) and finding the answer was no.

Bounded-concurrency, not sequential -- changed 2026-08-18 after a real live
run confirmed the pipeline processed every bill strictly one at a time,
regardless of real spare capacity. ddp-agents' AGENTS-33/34 shipped a
demand-based, memory-gated MLX-LM/MLX-VLM instance pool specifically to let
CAMS scale up under genuine concurrent load -- but a caller that only ever
has one request in flight can never trigger that pool's scale-up at all, no
matter how much memory is free. `run_legbot_pipeline` runs up to
`SESSION_PIPELINE_CONCURRENCY` (default 1 as of AGENTS-37 -- see
`SyncSettings.session_pipeline_concurrency`'s own docstring for why the
default dropped from this feature's original 4 -- env-configurable higher
once real concurrent MLX-LM throughput is validated) bills' own
`_process_bill` calls at once (`asyncio.Semaphore`-bounded, not unbounded
`asyncio.gather` -- see that setting's own docstring for why an actual cap
still matters even with the pool's own backpressure). Fills only absent
rows -- no freshness/version-currency check against the *upstream* source
this batch's own bill list was itself resolved from (OPEN-193/Fargate,
once that's live). OPEN-275 (services/replica_freshness.py) adds a distinct,
narrower check: whether the Mac's local Postgres replica has caught up to
RDS for the ONE bill about to dispatch, gated behind
`replica_freshness_check_enabled` (default off until that infrastructure
exists).

Called through scraper_triggered_legbot.py (the archive-completion hook and
``POST /trigger/bill-artifact-generation``, api/routes/triggers.py). The weekly ``session_pipeline_batch`` scheduled job that used to call it
directly was removed (SYNC-135).

SYNC-31: ``include_concept_statements`` folds ``ConceptStatementSet``
generation (previously the standalone, since-retired ``concept_statement_
dispatch.py`` weekly job -- see SYNC-32) into this same per-bill batch,
reusing its candidate enumeration and ``ensure_bill_exists()`` call. Kept as
its own boolean option, the same shape as ``include_org_research``, rather
than folded into ``artifact_types``/``ALL_ARTIFACT_TYPES`` -- a
``ConceptStatementSet`` row has no ``BillVersion`` at all and a coarser,
published-only dedup rule (no "failed" status exists for it), neither of
which the generic per-artifact-type coverage loop below (keyed on
``get_bill_artifacts``, which never returns this type) could express
correctly.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import resource
import sys
import time
import uuid

import structlog

from ddp_sync.config import get_settings
from ddp_sync.pipelines.bill_artifact_generation import (
    generate_and_store_bill_artifact,
    generate_and_store_bill_changelog,
)
from ddp_sync.pipelines.bill_organization_position_research import (
    generate_and_store_bill_organization_positions,
)
from ddp_sync.pipelines.concept_statement_dispatch import (
    dispatch_and_store_concept_statements,
)
from ddp_sync.pipelines.policy_statement_dispatch import dispatch_and_store_policy_statements
from ddp_sync.services import activity
from ddp_sync.services.jev_client import JevError
from ddp_sync.services.legbot_client import LegBotDispatchError
from ddp_sync.slack_alerts import post_alert
from ddp_sync.services.broker_client import (
    BrokerClientError,
    artifact_counts_as_present,
    ensure_bill_exists,
    get_bill_artifact_coverage_all_versions,
    get_bill_artifacts,
    get_bill_organization_positions_status,
    get_concept_statement_statuses,
    list_polled_bill_ids,
)
from ddp_sync.services.jurisdiction_enrollment import default_root, enrolled
from ddp_sync.services.local_openstates_client import (
    get_archived_bill_text,
    get_current_version_identity,
    list_current_session_bill_candidates,
)
from ddp_sync.services.replica_freshness import check_bill_version_freshness


def _legbot_allowed_codes(settings) -> set[str]:
    """The jurisdictions LegBot may be sent bills for from the replica: each state's `enrollment.legbot_replica` in the
    shared list (ddp-open-states jurisdictions.yaml), the ONE switch a human flips (SYNC-142). SYNC-157 removed the old
    per-host env list and its fallback: when the shared list cannot be read, nothing is dispatched and an alert says so
    (once per process). An EMPTY answer is a real answer and also dispatches nothing."""
    listed = enrolled("legbot_replica", None, default_root())
    if listed is None:
        _alert_legbot_list_unreadable()
        return set()
    return {str(c).upper() for c in listed}


_legbot_list_alerted = False


def _alert_legbot_list_unreadable() -> None:
    global _legbot_list_alerted
    if _legbot_list_alerted:
        return
    _legbot_list_alerted = True
    logger.error("legbot_replica_list_unreadable_dispatching_nothing", root=default_root())
    try:
        from ddp_sync.slack_alerts import post_alert

        post_alert(
            ":warning: ddp-sync (SYNC-157): the jurisdiction manifest cannot say which states LegBot may read from the "
            f"replica (`enrollment.legbot_replica`, under {default_root()}), so no bill is being sent to LegBot. "
            "Pull or fix the ddp-open-states checkout on this host.",
            source="legbot_replica_gate",
            dedupe_key="legbot_replica_gate:unreadable",
        )
    except Exception as exc:  # noqa: BLE001 - the refusal stands even if the alert fails
        logger.warning("legbot_replica_alert_failed", error=str(exc))


logger = structlog.get_logger()

# SYNC-72: process-wide, not per-call -- see get_legbot_line()'s own docstring for why a gate created fresh inside
# run_legbot_pipeline on every call can't bound anything once two calls are in flight at the same time (e.g. two
# jurisdictions' runs overlapping). SYNC-185: a fair, prioritised line, no longer a first-come asyncio.Semaphore.
_legbot_line: LegBotLine | None = None

# SYNC-79: a plain module-level int, incremented/decremented around the
# line's own acquire/release in _process_bill_bounded below -- not an
# asyncio.Lock-guarded counter, because every mutation here happens with no
# `await` between the check and the update, so nothing can interleave on
# asyncio's single-threaded event loop. Exists to answer, from real logs, a
# question the line alone can't: whether 2 bills' worth of work were ever
# ACTUALLY in flight at once.
_session_pipeline_active_holders = 0

PRIORITY_POLLED = 0  # SYNC-185: a bill that has a voter poll (the broker's has_poll rule)
PRIORITY_OTHER = 1


class LegBotLine:
    """SYNC-185: the Mac's LegBot slots, handed out bill by bill, fairly across runs.

    Same capacity as the semaphore it replaces (session_pipeline_concurrency). When a slot frees it goes to the
    next waiting bill of the highest priority (a polled bill before any other), and within a priority to the RUN
    served longest ago (a run never served yet first; ties in arrival order), each run's bills in their own order.
    A slot taken at once because nobody was waiting counts as that run's turn too. So a run that
    arrives behind a 2,600-bill run starts its first bill when the next slot frees, not after the other run's last
    bill (Ramon, 2026-11-22: US 119 waited ~20 h behind Louisiana).

    A staff single-bill run (run_single_bill_full) never comes here: it already goes first, unchanged.
    No awaits between a check and its update, so asyncio's single thread needs no lock.
    """

    def __init__(self, slots: int):
        self._free = slots
        # priority -> {run_id: waiting futures in that run's order}; dict order (arrival) breaks ties between runs
        self._waiting: dict[int, dict[str, collections.deque]] = {PRIORITY_POLLED: {}, PRIORITY_OTHER: {}}
        self._tick = 0
        self._last_served: dict[str, int] = {}  # run_id -> when it last got a slot; dropped once it has none waiting

    def _served(self, run_id: str) -> None:
        self._tick += 1
        self._last_served[run_id] = self._tick

    def waiting(self) -> int:
        return sum(len(q) for runs in self._waiting.values() for q in runs.values())

    async def acquire(self, run_id: str, priority: int = PRIORITY_OTHER) -> None:
        if self._free > 0 and not self.waiting():
            self._free -= 1
            self._served(run_id)
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiting[priority].setdefault(run_id, collections.deque()).append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                self.release()  # the slot was handed over just as this bill was cancelled: pass it on
            raise

    def release(self) -> None:
        self._free += 1
        while self._free > 0:
            fut = self._next()
            if fut is None:
                break
            self._free -= 1
            fut.set_result(None)
        if not self.waiting():
            self._last_served.clear()  # nobody waiting: no turn to keep (and no entry left behind by a finished run)

    def _next(self):
        """The next waiting bill: highest priority first; within it the first bill of the run served longest ago. A
        cancelled waiter is skipped."""
        for priority in (PRIORITY_POLLED, PRIORITY_OTHER):
            runs = self._waiting[priority]
            while runs:
                run_id = min(runs, key=lambda r: self._last_served.get(r, 0))  # min keeps arrival order on ties
                queue = runs[run_id]
                fut = queue.popleft()
                if not queue:
                    del runs[run_id]
                if not fut.done():
                    if any(run_id in waiting for waiting in self._waiting.values()):
                        self._served(run_id)
                    else:
                        self._last_served.pop(run_id, None)  # its last bill: nothing left to be fair to
                    return fut
        return None

    def slot(self, run_id: str, priority: int = PRIORITY_OTHER):
        line = self

        class _Slot:
            async def __aenter__(self):
                await line.acquire(run_id, priority)

            async def __aexit__(self, *exc):
                line.release()
                return False

        return _Slot()


def get_legbot_line() -> LegBotLine:
    """The single, process-wide gate on how many _process_bill calls can be
    in flight across ALL run_legbot_pipeline invocations at once -- not just
    within one call.

    SYNC-72: before this, run_legbot_pipeline created a fresh
    asyncio.Semaphore(session_pipeline_concurrency) on every call, as a
    local variable -- never shared across calls. That correctly bounded any
    ONE call to session_pipeline_concurrency bills in flight, a clean 1:1
    match with CAMS's configured MLX worker count (2) when only one
    jurisdiction's run was active. But run_legbot_pipeline has callers that
    can overlap freely, so two jurisdictions' runs overlapping meant two
    separate semaphores, each independently admitting 2 bills, for a real
    total of 4 competing for CAMS's 2 actual workers.

    Confirmed live (US 119th Congress + MI running concurrently,
    2026-09-20): throughput collapsed from 136.7 bills/hr (US alone) to
    22.6 bills/hr combined, because CAMS's own KV-cache reuse depends on a
    bill's worker staying free until its own next follow-up call arrives.
    With more distinct bills in flight than there are workers, a bill's
    worker gets claimed by a competing bill first, forcing a full prefill
    every follow-up. SYNC-185 keeps that cap and only changes WHICH waiting
    bill gets a freed slot (see LegBotLine).

    Same lazy-singleton idiom as get_redis_store() (services/redis_store.py).
    Safe without a lock: no `await` between the check and the assignment.

    Deliberately does NOT gate run_single_bill_full or
    dispatch_and_record_bill_artifact: a staff member's single-bill request
    goes straight through, which is what puts it first (SYNC-185 confirmed
    and kept this).
    """
    global _legbot_line
    if _legbot_line is None:
        _legbot_line = LegBotLine(get_settings().session_pipeline_concurrency)
    return _legbot_line


# The recognized BillArtifact types this plan ships -- matches
# bill_artifact_generation.py's own _ARTIFACT_TYPE_TO_QUESTION_TYPE keys
# (8 types, including bill_topics -- SYNC-1) plus bill_changelog, which
# dispatches through a separate function (generate_and_store_bill_changelog)
# because it needs a prior version's text + diff, not a single bill_source.
# Renamed from ALL_8_ARTIFACT_TYPES (SYNC-38). The count had been wrong since
# bill_changelog joined -- there are nine members, not eight. The old comment
# here deferred the rename to SYNC-1's default-artifact-set flip; SYNC-1 closed
# 2026-08-15 without the rename happening, so the deferral had simply outlived
# the thing it was waiting on.
#
# The count is dropped rather than corrected to nine, so it cannot go stale the
# same way a third time.
#
# Nothing here auto-widens a caller's list to "every recognized type"; each
# caller names its own. The scrape-completion trigger's default
# (config.legbot_scrape_completion_trigger_artifact_types, SYNC-50) names all
# of them, bill_topics included; it applies only where that trigger is enabled,
# and a host's LEGBOT_SCRAPE_COMPLETION_TRIGGER_ARTIFACT_TYPES replaces it.
ALL_ARTIFACT_TYPES = frozenset({
    "bill_summary",
    "bill_pros_cons",
    "bill_vote_yes_frame",
    "bill_vote_no_frame",
    "bill_supporting_orgs",
    "bill_opposing_orgs",
    "bill_impact_analysis",
    "bill_topics",
    "bill_changelog",
})

# SYNC-38: the order these are dispatched in, which is a property of the
# pipeline rather than of however the caller wrote its request body.
#
# ALL_ARTIFACT_TYPES above stays a frozenset -- it exists to validate
# membership, and a set is the right shape for that. But a set has no order,
# so before this constant the dispatch order came from the caller's own
# artifact_types list, and therefore so did which bill's KV cache survived.
#
# The order encodes one rule: everything that can share a bill's prefilled
# cache goes before the one thing that cannot.
#
#   - The 8 standard types all render the same bill text and share its
#     prefix, so after the first call they are cache hits. Measured on VA
#     2026S1 (2026-08-27): 90-100% warm.
#   - concept_statements shares that prefix too, and is dispatched separately
#     (SYNC-31) rather than as an artifact_type -- see the dispatch below.
#   - bill_changelog cannot share it. It builds a two-input prompt (prior
#     version text + diff), so it always takes its own cache key. Running it
#     before concept_statements evicted the bill's cache and made concept
#     statements pay a full prefill: 3 warm out of 20 on that same run,
#     against 90-100% for everything else. It goes last, always.
ARTIFACT_DISPATCH_ORDER: tuple[str, ...] = (
    "bill_summary",
    "bill_pros_cons",
    "bill_vote_yes_frame",
    "bill_vote_no_frame",
    "bill_supporting_orgs",
    "bill_opposing_orgs",
    "bill_impact_analysis",
    "bill_topics",
    # last, and deliberately: its own cache key evicts the bill's.
    "bill_changelog",
)

# The one type above that cannot reuse a bill's prefilled cache. Named rather
# than written as a literal at the two places that care, so the reason lives
# in one place.
_OWN_CACHE_KEY_TYPES = frozenset({"bill_changelog"})

assert set(ARTIFACT_DISPATCH_ORDER) == ALL_ARTIFACT_TYPES, (
    "ARTIFACT_DISPATCH_ORDER and ALL_ARTIFACT_TYPES have drifted apart; a "
    "type recognised but never ordered would silently never dispatch"
)



def _peak_memory_mb() -> float:
    """Peak resident-set size for this process so far, in MB -- stdlib
    `resource` only, no new dependency for this basic metric. ru_maxrss'
    unit differs by platform: bytes on macOS/BSD, KB on Linux.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


async def _run_org_research(
    *,
    bill_openstates_id: str,
    gov_id: str,
    jurisdiction_iso2: str,
    session_code: str,
    version: dict | None,
    dry_run: bool,
    run_id: str,
    broker_api_base: str | None,
    broker_api_token: str | None,
) -> dict:
    """One bill's organisation research, as a self-contained unit of work.

    Lifted out of _process_bill unchanged (SYNC-37) so it can run as a task
    alongside the artifact loop instead of blocking it. It returns the three
    result fields it used to assign directly and the caller merges them, so
    the per-bill result dict keeps exactly the shape every downstream reader
    already expects.

    It still catches its own dispatch failures, as it always did -- a bill
    whose organisation research fails keeps its artifacts. That mattered when
    this was inline and matters more now: an exception escaping into a task
    nobody handles becomes an unhandled-exception warning and a silently lost
    result.
    """
    out = {
        "org_research_dispatched": False,
        "org_research_skipped_reason": None,
        "org_research_duration_seconds": None,
        # SYNC-140: the bill version this research used (LegBot never sees a version, so this is the only record of
        # it); None when nothing was researched
        "org_research_version_date": None,
        "org_research_version_note": None,
    }
    try:
        org_status = await get_bill_organization_positions_status(
            bill_openstates_id=bill_openstates_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except BrokerClientError as exc:
        out["org_research_skipped_reason"] = f"status_check_failed: {exc}"
        return out

    if org_status["has_rows"]:
        out["org_research_skipped_reason"] = "already_researched"
        return out
    if version is not None:
        out["org_research_version_date"] = version["version_date"]
        out["org_research_version_note"] = version["version_note"]
    if dry_run:
        out["org_research_dispatched"] = True
        return out
    if version is None:
        out["org_research_skipped_reason"] = "no_current_version_resolved"
        return out

    org_started = time.monotonic()
    try:
        await generate_and_store_bill_organization_positions(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction_iso2,
            session_code=session_code,
            version_date=version["version_date"],
            version_note=version["version_note"],
            gov_id=gov_id,
            bill_title=version["bill_title"],
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        out["org_research_dispatched"] = True
    except Exception as exc:
        logger.warning(
            "session_pipeline_org_research_dispatch_failed",
            run_id=run_id, gov_id=gov_id, error=str(exc),
        )
        out["org_research_skipped_reason"] = f"dispatch_failed: {exc}"
    finally:
        out["org_research_duration_seconds"] = round(time.monotonic() - org_started, 3)
    return out


async def _process_bill(
    candidate: dict,
    **kwargs,
) -> dict:
    """One bill, with its organisation-research task guaranteed cleaned up.

    SYNC-37 gave _process_bill_inner a task that outlives individual awaits,
    and a task needs an owner: one that runs on the normal return, on an
    exception, and on the whole run being cancelled mid-bill (which has
    happened, twice, under memory pressure). Rather than wrap 130 lines of
    existing, unchanged bill processing in a try block -- or split it into a
    helper taking fifteen parameters purely to give `finally` a body -- the
    ownership lives here, and the inner function hands the task up through
    `org_holder` the moment it creates one.
    """
    org_holder: dict = {}
    try:
        return await _process_bill_inner(candidate, org_holder=org_holder, **kwargs)
    finally:
        org_task = org_holder.get("task")
        if org_task is not None:
            # Consume it unconditionally, not only while it is still
            # pending. /pm-review caught the difference: a task that has
            # already finished *with an exception* is `done()`, and skipping
            # it there leaves that exception unretrieved -- an asyncio
            # warning at collection time and a result quietly discarded,
            # which is the exact failure this cleanup exists to prevent.
            if not org_task.done():
                org_task.cancel()
            try:
                await org_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                # Cannot displace whatever exception is already on its way
                # out: an `except` inside `finally` only suppresses what it
                # catches itself. So a bill that failed for its own reason
                # still reports that reason, and a failure belonging to the
                # org task is logged as its own event rather than
                # impersonating the bill's.
                logger.warning(
                    "session_pipeline_org_research_task_failed",
                    run_id=kwargs.get("run_id"),
                    gov_id=candidate.get("gov_id"),
                    error=str(exc),
                )


async def _changelog_fully_covered(
    *,
    jurisdiction: str,
    session_code: str,
    gov_id: str,
    broker_api_base: str | None,
    broker_api_token: str | None,
) -> bool:
    """SYNC-45: is every version that could hold a ``bill_changelog`` already
    holding one?

    ``get_bill_artifacts`` reports on a bill's *current latest* version only, so
    the per-bill gate below saw one ``bill_changelog`` row there and marked the
    type covered -- and ``generate_and_store_bill_changelog``'s own
    per-transition filter (SYNC-44 AC2) lives *inside* the function the gate had
    already decided not to call. Six FL 2026E bills are stuck in exactly that
    state: a changelog of the ``e1 -> er`` typesetting pass, and no changelog of
    the substantive ``Filed -> e1`` transition, permanently.

    A changelog belongs to the newer version of its pair, so the oldest
    classified version can never hold one and every later one can. That is
    answerable from BROKER-130's all-versions response alone -- no api-v3 read
    and no transition walk here, both of which the inner function already does.

    Why ``versions[1:]`` is the exact transition universe and not an
    approximation of it (/pm-review round 1 challenged this, correctly, because
    getting it wrong would make a partially-covered bill look done and the fix
    inert). Two contracts combine:

    * ``get_archived_version_transitions`` walks **api-v3's own array** and
      makes a transition for each version api-v3 already attached a diff to
      (its old side is the version that diff was taken against, SYNC-110, not
      always the entry before it; that does not change which versions are
      targets). It never classifies a stage itself. api-v3 orders that array
      unknown-stage entries first, then classifiable versions chronologically.
    * openstates-core gives an unknown-stage version
      ``diff_from_previous_version=None`` unconditionally -- it "never updates or
      reads a baseline at all" (``archive_bill_versions``). So no unclassified
      version can ever be a transition *target*.

    Together: a transition's target is always a classified version, and never the
    oldest classified one (its diff lineage has no predecessor, so it carries no
    diff either). ``versions[1:]`` over ddp-broker-py's stage-ordered classified
    list is therefore the same set, not a subset of it -- which is why
    ``unclassified_versions`` are excluded here even though the inner filter
    still consults them when deciding what NOT to rewrite. Those are different
    questions: "could this version need a changelog" versus "does this version
    already have one".

    One status subtlety, deliberately matching the inner filter rather than
    improving on it: coverage is judged by artifact *presence*, ignoring status.
    A ``failed`` changelog on a non-latest version therefore reads as covered and
    is never retried, even under ``retry_failed``. That gap is pre-existing in
    SYNC-44's own filter, and the two must agree -- a stricter rule here would
    dispatch a bill the inner filter then declines to act on, every run, forever.
    Fixing it means changing both together and belongs to SYNC-42's territory.

    Deliberately approximate in one direction, and only that one: a version with
    no archived diff cannot produce a changelog at all (SYNC-46), so a bill
    holding one of those never satisfies this check and is re-offered to the
    inner function on every run. That costs one api-v3 read and one broker read
    per run for such a bill, after which the inner filter returns
    ``not_applicable`` -- no model call, no write, no artifact regenerated,
    because SYNC-44's filter is what actually decides. The alternative
    imprecision is the current bug, where a missing changelog is never generated
    at all. Cheap repeated reads beat permanently missing content.

    Returns True (treat as covered, do not dispatch) if the read cannot answer,
    so a broker problem degrades to today's behaviour rather than to
    re-dispatching every bill in a session.
    """
    try:
        coverage = await get_bill_artifact_coverage_all_versions(
            jurisdiction=jurisdiction,
            session_code=session_code,
            gov_id=gov_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except BrokerClientError as exc:
        # Includes the explicit raise for a ddp-broker-py predating BROKER-130,
        # which is the case that matters most: treating "cannot tell" as "not
        # covered" there would re-offer every bill in the session on every run.
        logger.warning(
            "all-versions changelog coverage read failed -- treating "
            "bill_changelog as covered for this bill",
            gov_id=gov_id,
            error=str(exc),
        )
        return True

    if coverage is None:
        # No Bill/BillVersion at all for this identity. Nothing to be covered
        # for; leave the decision to the existing single-version gate.
        return True

    versions = coverage.get("versions") or []
    if len(versions) < 2:
        # Fewer than two classified versions means no transition exists, so
        # there is nothing a changelog could describe.
        return True

    # `versions` is oldest-first by ddp-broker-py's stage-aware ordering. Skip
    # the oldest: it has no predecessor, so it can never carry a changelog.
    # SYNC-115: a rejected changelog does not count as holding one.
    return all(
        artifact_counts_as_present((entry.get("artifacts") or {}).get("bill_changelog"))
        for entry in versions[1:]
    )


async def _process_bill_inner(
    candidate: dict,
    *,
    org_holder: dict,
    jurisdiction_iso2: str,
    session_code: str,
    artifact_types: list[str],
    include_org_research: bool,
    include_concept_statements: bool,
    dry_run: bool,
    retry_failed: bool,
    run_id: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
    request_id: str | None = None,
    redo: dict | None = None,
    include_policy_statement: bool = False,
) -> dict:
    gov_id = candidate["gov_id"]
    bill_openstates_id = candidate["bill_openstates_id"]
    bill_started = time.monotonic()
    result = {
        "gov_id": gov_id,
        "artifacts_generated": [],
        "artifacts_skipped_present": [],
        "artifacts_failed": [],
        "artifacts_skipped_failed_previously": [],
        "artifacts_not_applicable": [],
        "artifact_durations_seconds": {},
        "org_research_dispatched": False,
        "org_research_skipped_reason": None,
        "org_research_duration_seconds": None,
        "org_research_version_date": None,
        "org_research_version_note": None,
        "concept_statements_dispatched": False,
        "concept_statements_skipped_reason": None,
        "concept_statements_duration_seconds": None,
        "concept_statements_failed": False,
        "policy_statement_dispatched": False,
        "policy_statement_skipped_reason": None,
        "policy_statement_duration_seconds": None,
        "policy_statement_failed": False,
        "policy_statement_statements": 0,
        "policy_statement_matched": 0,
        "policy_statement_created": 0,
        "policy_statement_excluded": 0,
        "dependency_failure": None,  # SYNC-196: "jev", "broker", "legbot" or "other" when a dependency failed this bill
        "duration_seconds": None,
        "error": None,
    }

    # OPEN-275: the pre-dispatch replica-freshness check (plan §5/§7.7) -- gated behind an
    # explicit flag, default off (see that setting's own docstring for why: this requires live
    # RDS replication infrastructure that does not exist yet). Checked once per bill, before the
    # coverage read and before any dispatch, and reuses the exact same per-bill "isolated soft
    # failure" mechanism the coverage-check-failure branch right below already uses -- not a new
    # pattern. Deliberately does NOT write any BillArtifact row (unlike the "no archived bill
    # text" case inside generate_and_store_bill_artifact): a row recorded status="failed" only
    # gets retried when the caller explicitly passes retry_failed=True (SYNC-42), which would
    # turn transient replication lag into a bill stuck skipped-as-failed indefinitely. Skipping
    # here instead, with no row written, means the next scheduled run reconsiders this bill
    # exactly as if this attempt never happened.
    settings = get_settings()
    # OPEN-276 (plan §3.6): the publication itself has no per-jurisdiction filter -- once the
    # subscription is live, the local replica can hold rows for a jurisdiction OPEN-193 hasn't
    # actually finished migrating yet (e.g. a partial in-progress load), so trust is gated
    # here, at dispatch, not at the replication layer. Checked first (before the RDS
    # round-trip the freshness check needs) since it's a cheap local membership test -- no
    # reason to query RDS at all for a jurisdiction this dispatch won't trust regardless of
    # what it finds. Same skip-with-no-write posture as the freshness check below: a
    # jurisdiction not yet on the allowlist should be reconsidered on the next scheduled run
    # once it's added, not permanently recorded as failed.
    #
    # SYNC-186: no longer nested inside replica_freshness_check_enabled. The allowlist is the
    # manifest's `legbot_replica` (SYNC-142), the switch a human flips for LegBot; turning the
    # freshness check off must not send a state with LegBot off to LegBot. The archive hook and
    # trigger_scraper_session_pipeline refuse such a state before a run starts; this per-bill
    # check is the last of the three.
    allowlist = _legbot_allowed_codes(settings)
    if jurisdiction_iso2.upper() not in allowlist:
        logger.info(
            "session_pipeline_bill_skipped_jurisdiction_not_on_rds_replica_allowlist",
            jurisdiction_iso2=jurisdiction_iso2.upper(),
            gov_id=gov_id,
        )
        result["error"] = (
            f"jurisdiction_not_on_rds_replica_allowlist: {jurisdiction_iso2.upper()}"
        )
        result["duration_seconds"] = time.monotonic() - bill_started
        return result

    if settings.replica_freshness_check_enabled:
        # OPEN-289 follow-up (2026-09-13, Ramon's call): the RDS content-hash round-trip
        # itself is independently toggleable now -- see
        # replica_freshness_content_check_enabled's own docstring in config.py for why. The
        # allowlist gate above still applies regardless of this flag; this only skips the
        # per-bill RDS query on top of that, trusting logical replication's own mechanics.
        if settings.replica_freshness_content_check_enabled:
            freshness = await check_bill_version_freshness(bill_openstates_id)
            if not freshness.is_fresh:
                result["error"] = f"replica_not_fresh: {freshness.reason}"
                result["duration_seconds"] = time.monotonic() - bill_started
                return result
        else:
            # pm-review: a bypass with no visible trace invites exactly the "did we actually
            # check this?" confusion this flag's own docstring is trying to prevent -- log it
            # per bill (cheap, this path is already logging elsewhere) and record it on the
            # result so it shows up in the run's own output, not just the process log.
            logger.warning(
                "replica_freshness_content_check_bypassed",
                bill_openstates_id=bill_openstates_id,
                jurisdiction_iso2=jurisdiction_iso2.upper(),
            )
            result["replica_freshness_content_check_bypassed"] = True

    try:
        coverage = await get_bill_artifacts(
            jurisdiction=jurisdiction_iso2,
            session_code=session_code,
            gov_id=gov_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except BrokerClientError as exc:
        # A coverage-check failure for one bill shouldn't abort the batch --
        # isolated to this bill's own result entry.
        result["error"] = f"coverage_check_failed: {exc}"
        result["dependency_failure"] = "broker"
        result["duration_seconds"] = time.monotonic() - bill_started
        return result

    existing = coverage["artifacts"] if coverage else {}
    needs_dispatch = []
    for artifact_type in artifact_types:
        entry = existing.get(artifact_type)
        status = (entry or {}).get("status")
        if status is not None and not artifact_counts_as_present(entry):
            # SYNC-115: staff rejected this row, so it counts as missing and is
            # written again (before the failed branch: a rejection is deliberate
            # and needs no retry_failed).
            needs_dispatch.append(artifact_type)
        elif status == "failed":
            # SYNC-42: the only status retry_failed reaches. Before it, a row
            # recorded failed stayed failed forever -- a failure caused by a
            # bug or outage since fixed could only be recovered by deleting
            # rows in the broker by hand.
            if retry_failed:
                needs_dispatch.append(artifact_type)
            else:
                result["artifacts_skipped_failed_previously"].append(artifact_type)
        elif status is not None:
            # SYNC-45: bill_changelog is the one type that is per-TRANSITION
            # rather than per-bill, so a row on the *latest* version does not
            # mean the type is done. Escalate to the all-versions read only
            # here -- when the cheap single-version check already says
            # "covered", which is precisely where the miss hides. A bill whose
            # latest version has no changelog takes the else branch below and
            # costs no extra read at all.
            if artifact_type == "bill_changelog" and not await _changelog_fully_covered(
                jurisdiction=jurisdiction_iso2,
                session_code=session_code,
                gov_id=gov_id,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            ):
                needs_dispatch.append(artifact_type)
                continue
            # complete, pending, or processing -- has a row, don't retry.
            #
            # SYNC-42 deliberately leaves this branch alone, and keeps it a
            # separate branch rather than folding the two skip buckets
            # together, so that no later edit can widen retry_failed onto
            # `complete` by accident. `complete` must never be dispatched
            # under any flag value.
            #
            # pending/processing are stuck here the same way failed was, and
            # that is knowingly not fixed: `failed` is terminal and
            # unambiguous, while a pending row may be genuinely in flight
            # right now. Retrying those safely needs an age threshold and a
            # way to tell "stuck" from "still running", which is a different
            # design and its own ticket -- not a flag this one can widen.
            result["artifacts_skipped_present"].append(artifact_type)
        else:
            needs_dispatch.append(artifact_type)

    # SYNC-38: canonical order, not the caller's. See ARTIFACT_DISPATCH_ORDER.
    needs_dispatch.sort(key=ARTIFACT_DISPATCH_ORDER.index)
    cache_sharing = [t for t in needs_dispatch if t not in _OWN_CACHE_KEY_TYPES]
    own_cache_key = [t for t in needs_dispatch if t in _OWN_CACHE_KEY_TYPES]


    # Only resolve the bill's current version identity if something actually
    # needs it -- a bill fully covered for every requested artifact_type
    # (and already researched, or org research not requested) never needs
    # this extra lookup.
    version = None
    if needs_dispatch or include_org_research:
        version = await get_current_version_identity(bill_openstates_id)

    # SYNC-21 (PLAN-local-openstates-migration.md §3.6): make sure a Bill row
    # exists in ddp-broker-py before dispatching any real LegBot analysis for
    # this bill -- write_bill_artifact/write_bill_organization_position only
    # ever attach to an EXISTING Bill row, by design, so without this a real
    # dispatch's output is silently discarded for any bill Voatz/Webflow
    # hasn't already brought in. `version is not None` here already implies
    # needs_dispatch or include_org_research was true above (version is only
    # ever resolved in that case) -- this bill was never going to be
    # skipped, so `ensure` is worth attempting.
    #
    # get_archived_bill_text below is a NEW, additional call, purely as a
    # gate -- generate_and_store_bill_artifact's own internal per-artifact-
    # type archived-text check (_resolve_bill_source) is completely
    # unchanged and still runs exactly as it does today. A bill with version
    # metadata but no archived text yet (most jurisdictions besides FL,
    # today) must never trigger `ensure` or get a stub created for it --
    # placing this check any earlier (e.g. right after
    # get_current_version_identity, before confirming text is actually
    # archived) would do exactly that for most non-FL bills in a session.
    if not dry_run and version is not None:
        archived_text = await get_archived_bill_text(bill_openstates_id)
        if archived_text:
            try:
                await ensure_bill_exists(
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    gov_id=gov_id,
                    title=version["bill_title"],
                    chamber_classification=version["chamber_classification"],
                    jurisdiction_classification=version["jurisdiction_classification"],
                    bill_openstates_id=bill_openstates_id,
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                )
            except BrokerClientError as exc:
                # Prevents this bill's LegBot dispatch entirely (both
                # artifact generation and org research below) -- a stable,
                # named result category distinct from artifacts_failed's
                # per-artifact-type failures, so a systemic problem (the
                # broker endpoint itself down/misconfigured -- every bill in
                # the run would show this same reason) reads differently
                # from a handful of bills each individually lacking archived
                # text.
                logger.warning(
                    "session_pipeline_ensure_bill_failed",
                    run_id=run_id, gov_id=gov_id, error=str(exc),
                )
                result["error"] = f"ensure_failed: {exc}"
                result["dependency_failure"] = "broker"
                result["duration_seconds"] = round(time.monotonic() - bill_started, 3)
                return result

    # SYNC-37: organisation research starts here and is awaited at the very
    # end, instead of running between the artifact loop and concept
    # statements.
    #
    # It used to block. Traced live on the WA 2025-2026 run of 2026-08-27:
    # eight artifacts five seconds apart, every one a warm cache hit, then
    # ~45 seconds of nothing while this bill waited on a network call, then
    # concept_statements paying a full re-read because the idle worker had
    # been correctly given to another bill in the meantime. Six of seven
    # full-artifact bills paid 2-3 prefills instead of one.
    #
    # This is the latest point it can start: `version` has been resolved and
    # `ensure_bill_exists` has run, and both of those can still return early
    # above -- launching before them would leave a task to clean up on paths
    # that currently just return.
    #
    # No new concurrency machinery: run_legbot_pipeline already runs bills
    # concurrently under a Semaphore, and this is one more await on the same
    # loop.
    org_task: asyncio.Task | None = None
    if include_org_research:
        org_task = asyncio.create_task(_run_org_research(
            bill_openstates_id=bill_openstates_id,
            gov_id=gov_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
            version=version,
            dry_run=dry_run,
            run_id=run_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        ))
        # The wrapper owns cleanup, and it can only clean up a task it can
        # see -- this is the handoff.
        org_holder["task"] = org_task

    # SYNC-38: one dispatch body, invoked in two phases with concept
    # statements between them. A closure rather than a helper taking a dozen
    # parameters purely to relocate code that has not otherwise changed.
    #
    # `continue` became `return`: inside the old loop it meant 'skip to the
    # next artifact_type', which per-artifact is the same thing.
    async def _dispatch_artifact(artifact_type: str) -> None:
        if dry_run:
            result["artifacts_generated"].append(artifact_type)
            return  # next artifact_type
        if version is None:
            logger.warning(
                "session_pipeline_no_version_identity",
                run_id=run_id, gov_id=gov_id, artifact_type=artifact_type,
            )
            result["artifacts_failed"].append(artifact_type)
            return  # next artifact_type
        dispatch_started = time.monotonic()
        try:
            if artifact_type == "bill_changelog":
                artifact_result = await generate_and_store_bill_changelog(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    version_date=version["version_date"],
                    version_note=version["version_note"],
                    # AC2/BROKER-130: lets generate_and_store_bill_changelog
                    # skip transitions it has already generated a changelog
                    # for on a NON-latest version, which this caller's own
                    # coverage check above (get_bill_artifacts) cannot see --
                    # that call is scoped to the bill's current latest
                    # version only. gov_id is already on hand from this
                    # bill's own candidate listing.
                    gov_id=gov_id,
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                )
            else:
                artifact_result = await generate_and_store_bill_artifact(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction_iso2,
                    session_code=session_code,
                    version_date=version["version_date"],
                    version_note=version["version_note"],
                    artifact_type=artifact_type,
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                    # SYNC-140: only a single-type request carries an id (run_single_bill_full checks that), so
                    # this is the one task the id belongs to; every other pass sends nothing extra
                    **({"request_id": request_id, "redo": redo} if request_id is not None else {}),
                )
            # SYNC-24: a normal (non-raising) return doesn't mean success --
            # generate_and_store_bill_artifact/_changelog deliberately return
            # normally with a written status="failed" row for a legitimate
            # decline (insufficient_information, no_valid_topics), so the
            # actual returned status has to be inspected. Anything other
            # than "complete"/"not_applicable" -- including "failed" or a
            # missing/malformed status from an outdated mock -- is treated
            # as a failure rather than ever being counted as generated.
            #
            # SYNC-44 (OPEN-297): "not_applicable" started as bill_changelog-
            # only -- a bill with no version transition ready yet (its
            # earliest version, or a diff not archived yet) writes no row at
            # all, which is neither a generation nor a failure. See
            # generate_and_store_bill_changelog's own docstring for why this
            # replaced a `failed` row there. OPEN-297 extended the same
            # status to every other artifact_type's own "no archived bill
            # text yet" case (generate_and_store_bill_artifact) for the
            # identical reason -- it used to attempt a `failed` write with
            # no Bill row to attach to (ensure_bill_exists above never
            # creates one while archived_text is falsy), which
            # ddp-broker-py rejected with a confusing "No Bill exists"
            # error instead of the real, mundane cause.
            status = artifact_result.get("status")
            if status == "complete":
                result["artifacts_generated"].append(artifact_type)
            elif status == "not_applicable":
                result["artifacts_not_applicable"].append(artifact_type)
            else:
                result["artifacts_failed"].append(artifact_type)
        except Exception as exc:
            # Broad on purpose: LegBotDispatchError, BrokerClientError, and
            # bill_changelog's own ArchivedVersionMismatchError are all
            # "one artifact type's dispatch failed" from this orchestrator's
            # point of view -- no bill_changelog-specific handling needed,
            # per this design's own reasoning for why a version mismatch
            # can't cause a stuck batch (see the PLAN doc).
            logger.warning(
                "session_pipeline_artifact_dispatch_failed",
                run_id=run_id, gov_id=gov_id, artifact_type=artifact_type,
                error=str(exc),
            )
            result["artifacts_failed"].append(artifact_type)
        finally:
            result["artifact_durations_seconds"][artifact_type] = round(
                time.monotonic() - dispatch_started, 3
            )
            activity.progress(run_id)  # AGENTS-271: the no-progress alert's clock

    # Phase 1: everything that shares the bill's prefilled cache.
    for artifact_type in cache_sharing:
        await _dispatch_artifact(artifact_type)

    # SYNC-31: ConceptStatementSet generation, folded in from the now-retired
    # standalone concept_statement_dispatch.py weekly job (SYNC-32). No
    # `version` dependency at all -- unlike every other artifact_type/org
    # research above, a ConceptStatementSet row has no BillVersion FK, so
    # this never needs get_current_version_identity to have resolved
    # anything. Its own dedup check (get_concept_statement_statuses) is
    # deliberately coarser than get_bill_artifacts' -- "does a non-rejected
    # set exist at all," with no version-currency concept. SYNC-85: it used
    # to ask only "is there a *published* set" (carried over unchanged from
    # the retired standalone job), which never saw a pending one -- so with
    # review on, every re-run wrote another duplicate pending set per bill.
    # A pending or published set now skips (distinct reasons); a bill whose
    # only sets are rejected is still regenerated. There is also no "failed"
    # ConceptStatementSet status to skip on a later run -- insufficient_information (or no archived text at all)
    # means dispatch_and_store_concept_statements wrote nothing, recorded
    # here as concept_statements_skipped_reason="nothing_to_publish", not as
    # a member of artifacts_failed (which is BillArtifact-status-shaped).
    #
    # SYNC-52: concept_statements_failed added as a narrow, standalone fix -- distinct from
    # this ticket's own blocked full scope (folding concept_statements into ALL_ARTIFACT_TYPES
    # and the shared _dispatch_artifact path, which needs BROKER-133's schema change first).
    # Before this field existed, both branches below only wrote a description into
    # concept_statements_skipped_reason, so a real BrokerClientError/exception and a benign
    # business-logic skip (already_published, nothing_to_publish) were both just "not
    # dispatched" to any caller checking booleans rather than parsing the reason string.
    # concept_statements_failed is set only on the two genuine-exception paths below, so a
    # caller can tell "this needs attention" from "this bill legitimately has nothing to
    # publish" without string-matching concept_statements_skipped_reason's contents.
    if include_concept_statements:
        try:
            existing_concept_statuses = await get_concept_statement_statuses(
                gov_id=gov_id,
                jurisdiction_iso2=jurisdiction_iso2,
                session_code=session_code,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        except BrokerClientError as exc:
            result["concept_statements_skipped_reason"] = f"status_check_failed: {exc}"
            result["concept_statements_failed"] = True
        else:
            if "published" in existing_concept_statuses:
                result["concept_statements_skipped_reason"] = "already_published"
            elif "pending" in existing_concept_statuses:
                result["concept_statements_skipped_reason"] = "already_exists_pending"
            elif dry_run:
                result["concept_statements_dispatched"] = True
            else:
                concept_started = time.monotonic()
                try:
                    concept_result = await dispatch_and_store_concept_statements(
                        gov_id=gov_id,
                        bill_openstates_id=bill_openstates_id,
                        jurisdiction_iso2=jurisdiction_iso2,
                        session_code=session_code,
                        bill_source=candidate.get("live_url_fallback", ""),
                        broker_api_base=broker_api_base,
                        broker_api_token=broker_api_token,
                    )
                except Exception as exc:
                    logger.warning(
                        "session_pipeline_concept_statements_dispatch_failed",
                        run_id=run_id, gov_id=gov_id, error=str(exc),
                    )
                    result["concept_statements_skipped_reason"] = f"dispatch_failed: {exc}"
                    result["concept_statements_failed"] = True
                else:
                    if concept_result is None:
                        result["concept_statements_skipped_reason"] = "nothing_to_publish"
                    else:
                        result["concept_statements_dispatched"] = True
                finally:
                    result["concept_statements_duration_seconds"] = round(
                        time.monotonic() - concept_started, 3
                    )

    # SYNC-183: policy_statement, right after concept_statements (its input) and before bill_changelog's own cache key.
    # The broker's work list says what needs doing, so there is no coverage read here; a failure is isolated to this bill
    # (policy_statement_failed) the way concept_statements_failed is, and nothing is recorded as "failed" -- the
    # statements simply have no match row, so the next run offers them again (which is also all retry_failed would do).
    if include_policy_statement:
        policy_started = time.monotonic()
        try:
            policy_result = await dispatch_and_store_policy_statements(
                gov_id=gov_id,
                bill_openstates_id=bill_openstates_id,
                jurisdiction_iso2=jurisdiction_iso2,
                session_code=session_code,
                dry_run=dry_run,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        except Exception as exc:
            logger.warning(
                "session_pipeline_policy_statement_failed", run_id=run_id, gov_id=gov_id, error=str(exc),
            )
            result["policy_statement_skipped_reason"] = f"dispatch_failed: {exc}"
            result["policy_statement_failed"] = True
            result["dependency_failure"] = _failure_source(exc)
        else:
            result["policy_statement_skipped_reason"] = policy_result["skipped_reason"]
            result["policy_statement_dispatched"] = policy_result["skipped_reason"] is None
            result["policy_statement_statements"] = policy_result["statements"]
            for kind in ("matched", "created", "excluded"):
                result[f"policy_statement_{kind}"] = policy_result[kind]
        finally:
            result["policy_statement_duration_seconds"] = round(time.monotonic() - policy_started, 3)

    # Phase 2: bill_changelog last, after concept statements have used the
    # cache. Its two-input prompt takes its own key, so running it earlier is
    # what made concept_statements pay a full prefill (SYNC-38).
    #
    # This is deliberately NOT wrapped in a try/finally, though /pm-review
    # asked. Moving changelog after the concept block does create a path where
    # it never dispatches -- but only one, and it is not a path where the
    # answer matters. Every *contained* concept failure is caught above
    # (BrokerClientError on the status check, broad Exception on the dispatch),
    # and phase 2 still runs; there is a test for exactly that. The only escape
    # is an unexpected exception from get_concept_statement_statuses, which
    # propagates out of _process_bill, out of asyncio.gather (no
    # return_exceptions) and ends the entire run -- at which point whether this
    # one bill's changelog was written before everything stopped is not a
    # meaningful difference, and the next run's coverage check re-dispatches it
    # anyway. A try/finally awaiting inside a possibly-cancelled scope would be
    # real added risk in exchange for that.
    for artifact_type in own_cache_key:
        await _dispatch_artifact(artifact_type)

    # SYNC-37: collect the organisation research launched before the artifact
    # loop. By now it has almost always finished during work that used to
    # wait for it; if it has not, this is the same wait as before, just at
    # the end. `_run_org_research` handles its own failures and returns the
    # same three fields that used to be assigned inline, so the result dict
    # below is identical in shape and content to what it was.
    if org_task is not None:
        result.update(await org_task)

    result["duration_seconds"] = round(time.monotonic() - bill_started, 3)

    return result


async def run_legbot_pipeline(
    jurisdiction_iso2: str,
    session_code: str,
    artifact_types: list[str],
    include_org_research: bool,
    limit: int | None,
    *,
    include_concept_statements: bool,
    include_policy_statement: bool = False,
    retry_failed: bool,
    dry_run: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
    bill_candidates: list[dict] | None = None,
    started_by: str = "trigger",
) -> dict:
    """Fill in whatever's missing, for every bill in one jurisdiction/session.

    bill_candidates (SYNC-63): optional explicit list of bills to process,
    as an alternative to scanning the whole jurisdiction/session for
    candidates via list_current_session_bill_candidates. Each entry is a
    dict with the same shape that scan already returns: {"gov_id",
    "bill_openstates_id"} required and non-empty, "live_url_fallback"
    optional (defaults to "" -- the same default _process_bill_inner's own
    candidate.get already falls back to). None (the default) preserves
    this function's original session-wide-scan behavior for every existing
    caller unchanged. Deduped by bill_openstates_id (keeping the first
    occurrence of each), the same precedent list_current_session_bill_
    candidates' own SYNC-23 dedup already set for the session-scan path --
    a caller-supplied list has no equivalent protection otherwise, and two
    entries for the same bill would each run their own concurrent coverage
    check/dispatch. Every entry is trusted to actually belong to this
    call's own jurisdiction_iso2/session_code -- not independently
    re-verified per entry, the same trust model the session-scan path
    already has (nothing downstream of candidate resolution re-checks a
    candidate's jurisdiction membership either way).

    Root cause this exists to fix: the SYNC-56/60/61/62 incident chain
    (2026-09-10/11) traced back to a 947-bill bill_changelog backfill run by
    calling /trigger/legbot-analyze-bill (the on-demand, single-bill
    endpoint) once per bill from an external script, with no concurrency
    awareness at all -- send-side pacing controls nothing about how many
    dispatches are actually in flight at once, since each call's own poll
    loop runs for potentially minutes. Hundreds ended up genuinely
    concurrent regardless of the delay between sends, which is what
    overloaded CAMS's status-check endpoint. The session-wide scan this
    function already does was not a fit either -- it has no way to target
    "just these specific bills" (SYNC-56's own investigation hit this same
    limitation first; see feedback_ddp_sync_batch_endpoint_limit_scope).
    Every existing safety property this function already has -- bounded
    concurrency via session_pipeline_concurrency, coverage-aware skip
    logic, retry_failed, dry_run -- applies identically here, since
    bill_candidates only changes where the candidate list comes from, not
    anything about how each one is processed (same _process_bill_bounded
    loop below, unmodified).

    When provided, `limit` is NOT used to select or truncate which bills
    are considered -- every entry in bill_candidates is processed. (limit
    is still validated as > 0 below regardless, the same as always, so
    this function's existing "no parameter here has a silent default"
    posture doesn't grow a special case; callers in this mode can pass
    len(bill_candidates) or any other positive value.) `truncated` in the
    return value is always False in this mode -- there is no "more exist
    beyond what was considered" to report when the caller supplied the
    complete list themselves.

    Raises:
        ValueError: any bill_candidates entry is not a dict, or is missing
            a non-empty "gov_id" or "bill_openstates_id" -- checked up
            front, before any dispatch, so a malformed caller-supplied
            list fails loudly and early (and always with this documented
            exception type, even for a direct Python caller bypassing the
            HTTP endpoint's own Pydantic validation) rather than as a
            confusing per-bill KeyError/TypeError deep inside
            _process_bill_inner. (Also raised, unchanged, for the
            pre-existing jurisdiction_iso2/session_code/limit/artifact_types
            validation below, regardless of which candidate-sourcing mode
            is in play.)

    include_policy_statement (SYNC-183) adds the policy_statement step (policy_statement_dispatch.py) after concept_statements:
    the writer drafts a Policy per statement the broker's work list offers, Jev matches each against the broker's current
    Policy list, and the result is written through the broker's service route. Default False here only so the
    post-archive hook, which does not send it, is unchanged; the trigger and a caller choose it. SYNC-193: with it on,
    artifact_types may be empty (a run of the policy_statement step alone, no artifact generated).

    artifact_types, include_org_research, include_concept_statements,
    retry_failed, and limit all have NO default -- the caller must decide
    explicitly, for every parameter with real cost implications.

    retry_failed (SYNC-42) re-dispatches artifacts whose stored status is
    "failed", which without it are skipped forever: nothing in this pipeline
    ever retried a failed row, so a failure caused by a bug, an outage or a
    prompt change since fixed could only be cleared by deleting rows in the
    broker by hand. It reaches "failed" and nothing else. A "complete" row is
    never re-dispatched under any value of this flag.

    A retry REPLACES the failed row rather than adding one, but only while
    ddp-sync leaves model_version/prompt_version/prompt_hash unset -- which
    it does today, at every call site. ddp-broker-py resolves the row a write
    lands on with those two fields in the key (its unique_billartifact_
    generation index, and _existing_ai_row's explicit both-NULL branch). With
    them NULL a retry finds the failed row and updates it; the moment
    anything starts populating them, a retry would stop matching that row and
    create a parallel one instead, leaving a failed row and a complete row
    for the same version and type with nothing to say which is current. So:
    **these fields stay unset while retry_failed exists.** Populating them is
    not a free provenance improvement -- it changes what a retry means, and
    needs retry semantics decided first. One call can trigger up to
    11 real dispatches (9 artifacts + org research + concept statements) per
    bill. include_concept_statements is keyword-only (unlike the other
    three, which are positional-or-keyword) -- added after those three
    already had real positional callers (SYNC-9/SYNC-15); every caller must
    still pass it explicitly, just as an explicit keyword rather than a new
    required positional slot spliced into an existing argument order.

    broker_api_base/broker_api_token: optional per-call override threaded to
    every bill's _process_bill call. None (the default) preserves this
    function's existing behavior for its only real caller today (SYNC-9's
    batch pipeline, which always targets the one shared broker instance via
    settings) -- SYNC-15's single-bill full-run endpoint is the first caller
    to pass these explicitly.

    dry_run=True runs the same coverage checks as a real run but skips every
    dispatch call, returning the same result shape with nothing actually
    generated -- lets an operator see real scope before spending anything.
    Not a mandatory first step; just cheap to call.

    Raises:
        ValueError: jurisdiction_iso2/session_code empty, limit <= 0 (None = no limit), or
            artifact_types empty (unless include_policy_statement is true, SYNC-193)/contains an unrecognized type -- ordinary
            function-signature hygiene, checked before any lister/dispatch
            call.

    Basic performance metrics, added for real visibility during the first
    live runs against actual sessions -- wall-clock timing (stdlib `time`)
    and peak RSS (stdlib `resource`, no new dependency): per-dispatch and
    per-bill duration, plus one run-level duration/peak-memory pair.
    Informational only, not a new observability subsystem -- everything is
    plain fields on the same result dict/log events this design already
    specifies, not a separate metrics store.

    Bounded concurrency: up to `SyncSettings.session_pipeline_concurrency`
    (default 1 as of AGENTS-37, env `SESSION_PIPELINE_CONCURRENCY`) bills'
    own _process_bill calls run at once -- see that setting's own docstring
    and this module's own docstring for why the default isn't higher out of
    the box. `session_pipeline_bill_complete` is still logged per-bill, as
    each one actually finishes
    (interleaved under concurrency, not batched at the end).

    Returns:
        {
            "bills_considered": int,   # how many candidates were actually looked at
            "bills_processed": int,    # always equal to bills_considered today
                                        # (no early-abort reason exists)
            "truncated": bool,         # True if more bills exist beyond bills_considered
            "duration_seconds": float, # whole-run wall-clock time
            "peak_memory_mb": float,   # this process' peak RSS so far, at run end
            "results": [               # one entry per bill, in candidate order
                                        # (asyncio.gather preserves input order
                                        # regardless of completion order under
                                        # concurrency)
                {
                    "gov_id": str,
                    "artifacts_generated": [str],  # dispatch returned status="complete"
                    "artifacts_skipped_present": [str],
                    "artifacts_failed": [str],     # SYNC-24: covers BOTH a raised
                                                    # dispatch/persistence exception AND a
                                                    # normal return with a written
                                                    # status="failed" row -- e.g. LegBot
                                                    # legitimately declining via
                                                    # insufficient_information. Both are
                                                    # "this artifact_type produced no
                                                    # usable content" from this summary's
                                                    # point of view; distinguishing an
                                                    # outage from a legitimate decline
                                                    # requires reading the underlying
                                                    # BillArtifact row's own
                                                    # failure_stage/failure_reason.
                    "artifacts_skipped_failed_previously": [str],
                    "artifact_durations_seconds": {artifact_type: float},  # dispatched types only
                    "org_research_dispatched": bool,
                    "org_research_skipped_reason": str | None,
                    "org_research_duration_seconds": float | None,
                    "concept_statements_dispatched": bool,
                    "concept_statements_skipped_reason": str | None,
                    "concept_statements_duration_seconds": float | None,
                    "concept_statements_failed": bool,  # SYNC-52: "an isolated
                                                         # concept-statements failure was
                                                         # observed," not "an attempt was made" --
                                                         # False when include_concept_statements
                                                         # is False too, same as every other
                                                         # concept_statements_* field. True only
                                                         # for a genuine status-check/dispatch
                                                         # exception, never a benign skip
                                                         # (already_published,
                                                         # nothing_to_publish) -- lets a caller
                                                         # tell the two apart without parsing
                                                         # concept_statements_skipped_reason.
                    "policy_statement_dispatched": bool,  # SYNC-183: include_policy_statement only; a dry run
                    "policy_statement_skipped_reason": str | None,  # reports the work list's size, writes nothing
                    "policy_statement_duration_seconds": float | None,
                    "policy_statement_failed": bool,  # an isolated failure of this step, like concept_statements_failed
                    "policy_statement_statements": int,  # statements the broker's work list offered
                    "policy_statement_matched": int, "policy_statement_created": int, "policy_statement_excluded": int,
                    "duration_seconds": float,  # this bill's total processing time
                    "error": str | None,
                },
                ...
            ],
        }
    """
    if not jurisdiction_iso2:
        raise ValueError("jurisdiction_iso2 is required")
    if not session_code:
        raise ValueError("session_code is required")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be a positive integer, or None for no limit")
    # SYNC-193: policy_statement is switched by its own flag, not by an artifact type, so a run that only wants it names
    # no type. Without the flag an empty list is still refused (the same message as before).
    if not artifact_types and not include_policy_statement:
        raise ValueError("artifact_types must be non-empty")
    unrecognized = set(artifact_types) - ALL_ARTIFACT_TYPES
    if unrecognized:
        raise ValueError(f"Unrecognized artifact_types: {sorted(unrecognized)}")
    if bill_candidates is not None:
        # SYNC-63 /pm-review: fail loudly and early on a malformed
        # caller-supplied list, before any dispatch -- the alternative is a
        # confusing KeyError/TypeError deep inside _process_bill_inner for
        # whichever entry happens to be bad, on whichever bill the
        # semaphore gets to first. Checks a non-dict entry explicitly
        # (a bare `"gov_id" not in entry` on e.g. None or an int raises
        # TypeError, not the documented ValueError, for a direct Python
        # caller bypassing the HTTP endpoint's own Pydantic validation) and
        # treats an empty-string value the same as a missing key (an empty
        # gov_id/bill_openstates_id is exactly as useless to every
        # downstream call as an absent one).
        bad_entries = [
            i for i, entry in enumerate(bill_candidates)
            if not isinstance(entry, dict)
            or not entry.get("gov_id")
            or not entry.get("bill_openstates_id")
        ]
        if bad_entries:
            raise ValueError(
                "bill_candidates entries must be dicts with non-empty "
                f"'gov_id' and 'bill_openstates_id' -- bad at index/indices: {bad_entries}"
            )
        # SYNC-63 /pm-review: dedup by bill_openstates_id, same precedent as
        # list_current_session_bill_candidates's own SYNC-23 dedup for the
        # session-scan path -- a caller-supplied list has no equivalent
        # protection otherwise, and duplicate entries would each run their
        # own concurrent coverage check/dispatch for the same bill (wasted
        # work at best, a race against each other's write at worst). Keeps
        # the first occurrence of each id, logged so a caller's own list
        # isn't silently regenerated smaller than what they submitted.
        deduped: list[dict] = []
        seen_ids: set[str] = set()
        duplicates_dropped = 0
        for entry in bill_candidates:
            bill_id = entry["bill_openstates_id"]
            if bill_id in seen_ids:
                duplicates_dropped += 1
                continue
            seen_ids.add(bill_id)
            deduped.append(entry)
        if duplicates_dropped:
            logger.warning(
                "bill_candidates had duplicate bill_openstates_id entries -- "
                "kept first occurrence of each, dropped the rest",
                duplicates_dropped=duplicates_dropped,
            )
        bill_candidates = deduped

    run_id = str(uuid.uuid4())
    run_started = time.monotonic()
    # SYNC-178: visible to `ddp-sync status` while it runs (progress per bill, below), whoever started it.
    activity.start(run_id, "LegBot session pipeline", started_by=started_by, jurisdiction=jurisdiction_iso2,
                   session=session_code, detail="dry run" if dry_run else None)
    logger.info(
        "session_pipeline_run_start",
        run_id=run_id,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        artifact_types=list(artifact_types),
        include_org_research=include_org_research,
        include_concept_statements=include_concept_statements,
        include_policy_statement=include_policy_statement,
        limit=limit,
        dry_run=dry_run,
        bill_candidates_count=(len(bill_candidates) if bill_candidates is not None else None),
        peak_memory_mb=round(_peak_memory_mb(), 1),
    )

    try:
        if bill_candidates is not None:
            # SYNC-63: an explicit, caller-supplied list replaces the session-wide
            # scan entirely -- limit does not select or truncate here (every
            # supplied entry is processed), and there's no "more exist beyond
            # what was considered" for truncated to report, since the caller
            # already supplied their own complete list.
            candidates = list(bill_candidates)
            truncated = False
        else:
            # Request limit + 1 so truncated is actually computable: if more than
            # `limit` candidates come back, we know more exist beyond what's
            # considered, without misreporting a specific further count (a
            # limit + 1 probe only proves "more than limit exist"). SYNC-103: with no
            # limit, every candidate is considered and nothing is truncated.
            candidates = await list_current_session_bill_candidates(
                jurisdiction_iso2,
                session_code=session_code,
                limit=None if limit is None else limit + 1,
            )
            truncated = limit is not None and len(candidates) > limit
            if limit is not None:
                candidates = candidates[:limit]
    except BaseException as exc:  # SYNC-178: a run that ends here must not stay "running" in `ddp-sync status`
        activity.finish(run_id, "failed", error=type(exc).__name__)
        raise
    bills_considered = len(candidates)
    activity.set_total(run_id, bills_considered)

    # Bounded concurrency (2026-08-18, made process-wide by SYNC-72 on
    # 2026-09-20): several bills' own _process_bill calls run at once
    # instead of strictly one at a time, up to session_pipeline_concurrency
    # -- see this module's own docstring and that setting's docstring for
    # why a caller that only ever has one request in flight can never
    # exercise ddp-agents' AGENTS-33/34 MLX instance pool at all. A
    # semaphore, not a bare asyncio.gather over every candidate at once:
    # _process_bill() fires several non-MLX HTTP calls per bill (broker
    # coverage check, OpenStates lookups, ensure_bill_exists) that a
    # genuinely unbounded fan-out would send all at once for however many
    # bills a session has -- potentially thousands -- for no throughput
    # benefit once only a handful of MLX-LM instances can usefully be busy
    # at the same time.
    #
    # get_legbot_line() (SYNC-72, SYNC-185), not a local
    # asyncio.Semaphore(...) constructed here -- see that function's own
    # docstring for why a semaphore scoped to just this one call can't
    # actually bound anything once a second run_legbot_pipeline call (a
    # different jurisdiction's own scheduled batch, say) is in flight at
    # the same time.
    line = get_legbot_line()
    breaker = _RunBreaker(get_settings().policy_statement_stop_after_failures if include_policy_statement else 0)
    polled = await _polled_bill_ids(jurisdiction_iso2, broker_api_base, broker_api_token) if candidates else set()

    async def _process_bill_bounded(candidate: dict) -> dict:
        global _session_pipeline_active_holders
        bill_label = str(candidate.get("gov_id") or candidate.get("bill_openstates_id") or "?")
        priority = PRIORITY_POLLED if candidate.get("bill_openstates_id") in polled else PRIORITY_OTHER
        async with line.slot(run_id, priority):
            activity.bill_started(run_id, bill_label)
            # SYNC-79: log the real concurrent-holder count at the instant
            # this bill is admitted -- the only way to tell, from logs
            # alone, whether session_pipeline_concurrency=2 ever actually
            # granted 2 bills' worth of work in flight at once, versus real
            # concurrency silently staying at 1 for some other reason.
            #
            # active_holders is process-wide, the same as the semaphore
            # itself (SYNC-72) -- it is NOT scoped to this run_id. Two
            # overlapping jurisdiction runs sharing the one global budget
            # can each log active_holders=2 while holding only 1 of those
            # 2 slots themselves; that's expected, not a bug, and matches
            # get_legbot_line()'s own docstring.
            #
            # Both the increment and the decrement live inside this single
            # try/finally (started before the acquire log, not after it) so
            # a failure in the log call itself can never leave the counter
            # incremented forever with no matching decrement.
            try:
                _session_pipeline_active_holders += 1
                logger.info(
                    "session_pipeline_semaphore_acquired",
                    run_id=run_id,
                    active_holders=_session_pipeline_active_holders,
                    configured_concurrency=get_settings().session_pipeline_concurrency,
                )
                bill_result = await _process_or_skip(
                    breaker,
                    candidate,
                    jurisdiction_iso2=jurisdiction_iso2,
                    session_code=session_code,
                    artifact_types=artifact_types,
                    include_org_research=include_org_research,
                    include_concept_statements=include_concept_statements,
                    include_policy_statement=include_policy_statement,
                    dry_run=dry_run,
                    retry_failed=retry_failed,
                    run_id=run_id,
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                )
            finally:
                # Named "releasing", not "released" -- this still runs
                # inside the semaphore's own `async with` block, before its
                # __aexit__ actually frees the slot. No `await` separates
                # this from that real release, so no other task can ever
                # observe the gap, but the event name says what's true.
                _session_pipeline_active_holders -= 1
                logger.info(
                    "session_pipeline_semaphore_releasing",
                    run_id=run_id,
                    active_holders=_session_pipeline_active_holders,
                    configured_concurrency=get_settings().session_pipeline_concurrency,
                )
        # Outside the semaphore's `async with` -- release this bill's slot
        # for the next queued one as soon as the real work finishes, rather
        # than holding it through this log call too.
        logger.info("session_pipeline_bill_complete", run_id=run_id, **bill_result)
        activity.bill_finished(run_id, bill_label, _bill_outcome(bill_result))
        # SYNC-196: nothing here may `await` before the record. The slot was just released, so the next queued bill is already
        # woken; it runs as soon as this task yields, and must find the run already tripped (a test holds two bills in flight).
        if breaker.record(bill_result):
            logger.warning("session_pipeline_run_stopped_early", run_id=run_id, reason=breaker.reason,
                           first_failure=breaker.first_failure)
            await _alert_run_stopped(breaker, jurisdiction_iso2, session_code, run_id)
        return bill_result

    watchdog = asyncio.create_task(_watch_no_progress(run_id, jurisdiction_iso2, session_code))
    try:
        results = list(await asyncio.gather(*(_process_bill_bounded(c) for c in candidates)))
    except BaseException as exc:
        activity.finish(run_id, "failed", error=type(exc).__name__)
        raise
    finally:
        watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog  # gone before the run returns (a test checks no task is left behind)
    # SYNC-196: only a policy_statement run reports failures this way; every other run keeps `status` "completed" as before
    not_started = sum(1 for r in results if r.get("not_started")) if include_policy_statement else 0
    failed_bills = breaker.failed if include_policy_statement else 0
    if not include_policy_statement or (failed_bills == 0 and not_started == 0):
        status = "completed"
    elif breaker.tripped and failed_bills >= len(results) - not_started:
        status = "failed"  # stopped early and not one bill that started got through
    else:
        status = "partial"
    activity.finish(run_id, "done" if status == "completed" else status, error=breaker.reason)

    summary = {
        "bills_considered": bills_considered,
        "bills_processed": len(results) - not_started,
        "truncated": truncated,
        "duration_seconds": round(time.monotonic() - run_started, 3),
        "peak_memory_mb": round(_peak_memory_mb(), 1),
        "results": results,
    }
    if include_policy_statement:  # SYNC-196: the run-level failure report; other runs' summaries are unchanged
        summary.update(status=status, bills_failed=failed_bills, bills_not_started=not_started,
                       stopped_early=breaker.reason, first_failure=breaker.first_failure)
    logger.info(
        "session_pipeline_run_end",
        run_id=run_id,
        bills_considered=bills_considered,
        bills_processed=len(results) - not_started,
        truncated=truncated,
        duration_seconds=summary["duration_seconds"],
        peak_memory_mb=summary["peak_memory_mb"],
    )
    if include_policy_statement:
        logger.info("session_pipeline_policy_statement_run_status", run_id=run_id, status=status, bills_failed=failed_bills,
                    bills_not_started=not_started, stopped_early=breaker.reason, first_failure=breaker.first_failure)
    return summary


async def _polled_bill_ids(jurisdiction_iso2: str, broker_api_base: str | None, broker_api_token: str | None) -> set[str]:
    """SYNC-185: the run's polled bills, read once at its start. A broker that cannot answer marks none: the run goes
    on in plain turns, never fails for it."""
    try:
        polled = await list_polled_bill_ids(
            jurisdiction_iso2, broker_api_base=broker_api_base, broker_api_token=broker_api_token
        )
    except Exception as exc:  # noqa: BLE001 -- before the run's own try: anything here must not fail or strand the run
        logger.warning("session_pipeline_polled_bills_unavailable", jurisdiction=jurisdiction_iso2, error=str(exc))
        return set()
    logger.info("session_pipeline_polled_bills", jurisdiction=jurisdiction_iso2, polled=len(polled))
    return polled


async def run_single_bill_full(
    *,
    bill_openstates_id: str,
    jurisdiction_iso2: str,
    session_code: str,
    gov_id: str,
    bill_source: str,
    artifact_types: list[str] | None,
    include_org_research: bool,
    include_concept_statements: bool,
    include_policy_statement: bool = False,
    retry_failed: bool,
    dry_run: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
    request_id: str | None = None,
    redo: dict | None = None,
) -> dict:
    """Run every requested artifact type (default: all of
    ALL_ARTIFACT_TYPES) plus optional org research and concept-statement
    generation for ONE caller-specified bill, in a single call -- SYNC-15
    (include_concept_statements added SYNC-31).

    SYNC-140: an EMPTY artifact_types is allowed when include_org_research is true: organization research
    alone, no result type regenerated. With research off an empty list is still refused. (Research is still
    skipped as "already_researched" for a bill that has been researched before: that gate is per bill, not
    per version, and is unchanged here.)

    request_id / redo (SYNC-140; ddp-agents docs/reference/legbot-redo-contract.md): the caller's id for this
    request and, for a staff redo of one result, its reason and note. They belong to ONE LegBot task (LegBot
    keeps ids per bot, so one id cannot cover several types), so they need exactly one artifact type, and not
    bill_changelog, which is one task per version change. redo needs a request_id. A rejected row is already
    regenerated (SYNC-115); nothing here forces a present result to be redone.

    The single-bill counterpart to run_legbot_pipeline: that function lists
    candidates across a whole jurisdiction/session and loops _process_bill
    over each; this function skips the listing step entirely and calls
    _process_bill directly with one caller-built candidate, since the caller
    already knows exactly which bill they want. Reuses _process_bill's
    existing coverage-check skip logic unchanged -- this is not a
    force-regenerate mode, already-present artifacts are still skipped.

    artifact_types: None means "all of them" (ALL_ARTIFACT_TYPES) -- the
    one place in this module a missing value gets a real default rather than
    being rejected, since "run everything for this bill" is this function's
    whole reason to exist. An empty list is still rejected, same as
    run_legbot_pipeline, since that's never a meaningful request either way.

    gov_id: required, not derived from bill_openstates_id -- _process_bill's
    coverage check (get_bill_artifacts) is keyed by (jurisdiction, session,
    gov_id), not by bill_openstates_id alone, so this can't be skipped or
    inferred without an extra lookup this function deliberately avoids (the
    caller already has this from wherever they got bill_openstates_id).

    Raises:
        ValueError: bill_openstates_id/jurisdiction_iso2/session_code/gov_id/
            bill_source empty, or artifact_types contains an unrecognized
            type -- same signature-hygiene discipline as run_legbot_pipeline.

    Returns:
        _process_bill's own per-bill result dict (see run_legbot_pipeline's
        docstring for its shape) plus "run_id".
    """
    if not bill_openstates_id:
        raise ValueError("bill_openstates_id is required")
    if not jurisdiction_iso2:
        raise ValueError("jurisdiction_iso2 is required")
    if not session_code:
        raise ValueError("session_code is required")
    if not gov_id:
        raise ValueError("gov_id is required")
    if not bill_source:
        raise ValueError("bill_source is required")

    resolved_artifact_types = (
        list(ALL_ARTIFACT_TYPES) if artifact_types is None else artifact_types
    )
    if not resolved_artifact_types and not include_org_research:
        raise ValueError("artifact_types must be non-empty unless include_org_research is true")
    unrecognized = set(resolved_artifact_types) - ALL_ARTIFACT_TYPES
    if unrecognized:
        raise ValueError(f"Unrecognized artifact_types: {sorted(unrecognized)}")
    if redo is not None and request_id is None:
        raise ValueError("redo needs a request_id")
    if request_id is not None:
        if len(resolved_artifact_types) != 1:
            raise ValueError("request_id and redo belong to one LegBot task: send exactly one artifact type")
        if resolved_artifact_types == ["bill_changelog"]:
            raise ValueError(
                "request_id and redo are not supported for bill_changelog yet: it is generated once per "
                "version change, so one request id cannot cover it"
            )

    run_id = str(uuid.uuid4())
    logger.info(
        "single_bill_full_run_start",
        run_id=run_id,
        bill_openstates_id=bill_openstates_id,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        gov_id=gov_id,
        artifact_types=resolved_artifact_types,
        include_org_research=include_org_research,
        include_concept_statements=include_concept_statements,
        include_policy_statement=include_policy_statement,
        dry_run=dry_run,
        request_id=request_id,
        redo_reason=(redo or {}).get("reason"),
    )

    candidate = {
        "gov_id": gov_id,
        "bill_openstates_id": bill_openstates_id,
        "live_url_fallback": bill_source,
    }
    result = await _process_bill(
        candidate,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        artifact_types=resolved_artifact_types,
        include_org_research=include_org_research,
        include_concept_statements=include_concept_statements,
        include_policy_statement=include_policy_statement,
        dry_run=dry_run,
        retry_failed=retry_failed,
        run_id=run_id,
        broker_api_base=broker_api_base,
        broker_api_token=broker_api_token,
        **({"request_id": request_id, "redo": redo} if request_id is not None else {}),
    )
    result["run_id"] = run_id
    logger.info("single_bill_full_run_end", run_id=run_id, **{k: v for k, v in result.items() if k != "run_id"})
    return result




def _failure_source(exc: BaseException) -> str:
    """SYNC-196: which dependency a policy_statement failure came from, for the run's stop-early rule."""
    if isinstance(exc, JevError):
        return "jev"
    if isinstance(exc, BrokerClientError):
        return "broker"
    if isinstance(exc, LegBotDispatchError):
        return "legbot"
    return "other"


class _RunBreaker:
    """SYNC-196: the stop-early rule of one policy_statement run. A bill that fails on a dependency (`dependency_failure`)
    extends a streak of the same source; any bill without one resets it; `limit` in a row trips it (0 = never). Once tripped,
    bills not yet started are not started (`_process_or_skip`); bills already finished stay written, and a re-run offers
    only statements with no match row, so nothing is repeated or duplicated."""

    def __init__(self, limit: int):
        self.limit = limit
        self.source: str | None = None
        self.streak = 0
        self.failed = 0
        self.reason: str | None = None
        self.first_failure: str | None = None

    @property
    def tripped(self) -> bool:
        return self.reason is not None

    def record(self, bill_result: dict) -> bool:
        """Count one finished bill; True when this bill tripped the rule."""
        if bill_result.get("not_started"):
            return False
        source = bill_result.get("dependency_failure")
        if source is None:
            self.source, self.streak = None, 0
            return False
        self.failed += 1
        if self.first_failure is None:
            detail = bill_result.get("policy_statement_skipped_reason") or bill_result.get("error") or ""
            self.first_failure = f"{source}: {detail}"[:300]
        self.streak = self.streak + 1 if source == self.source else 1
        self.source = source
        if self.limit and self.streak >= self.limit and not self.tripped:
            self.reason = f"{source} failed {self.streak} bills in a row"
            return True
        return False


def _not_started_result(candidate: dict, reason: str | None) -> dict:
    """SYNC-196: the result of a bill the run never started because it had stopped early."""
    return {"gov_id": candidate.get("gov_id"), "bill_openstates_id": candidate.get("bill_openstates_id"),
            "not_started": True, "stopped_early": reason, "dependency_failure": None, "error": None}


async def _process_or_skip(breaker: _RunBreaker, candidate: dict, **kwargs) -> dict:
    if breaker.tripped:
        return _not_started_result(candidate, breaker.reason)
    return await _process_bill(candidate, **kwargs)


async def _alert_run_stopped(breaker: _RunBreaker, jurisdiction_iso2: str, session_code: str, run_id: str) -> None:
    """SYNC-196: one alert when a run stops early (the dedupe key is the state and session, as for the archive alerts).
    `post_alert` blocks, so it goes off the event loop; it must never fail the run."""
    text = (f"A policy_statement run for {jurisdiction_iso2.upper()} {session_code} stopped early: {breaker.reason}. "
            f"First failure: {breaker.first_failure}. Bills that finished are kept; re-run the same bills when it is back "
            f"(only statements without a match row are offered). Run {run_id}.")
    try:
        await asyncio.to_thread(post_alert, text, source="policy_statement.run_stopped",
                                dedupe_key=f"policy_statement_run_stopped:{jurisdiction_iso2.upper()}:{session_code}")
    except Exception as exc:  # noqa: BLE001
        logger.warning("session_pipeline_run_stopped_alert_failed", run_id=run_id, error=str(exc))


# AGENTS-271: on 2026-11-26 a Colima relink left two CAMS tasks at "running"; both bill slots waited on them, nothing was
# written for 29 min 43 s, and the run record's `updated_at` kept moving, so nothing looked wrong. Alert only: nothing is
# cancelled or restarted (ddp-sync's own 1,800 s inference timeout still ends the wait).
NO_PROGRESS_ALERT_SECONDS = 600
NO_PROGRESS_CHECK_SECONDS = 60


async def _watch_no_progress(run_id: str, jurisdiction_iso2: str, session_code: str, *,
                             alert_after: float = NO_PROGRESS_ALERT_SECONDS,
                             interval: float = NO_PROGRESS_CHECK_SECONDS) -> None:
    """One alert per quiet stretch when a run with bills in flight has finished no artifact and no bill for `alert_after`
    seconds. A run with nothing in flight (queued behind another run's bills, SYNC-185) is not quiet, it is waiting its turn.
    A later stretch, after progress resumed, alerts again. Never raises; ends when cancelled."""
    alerted_at = None  # the `last_progress_ts` the alert was for
    while True:
        await asyncio.sleep(interval)
        try:
            entry = activity.snapshot(run_id)
            if entry is None or not entry["current"]:
                continue
            quiet = activity.clock() - entry["last_progress_ts"]
            if quiet < alert_after or alerted_at == entry["last_progress_ts"]:
                continue
            text = (f"LegBot run {run_id} for {jurisdiction_iso2.upper()} {session_code} is still running but has written "
                    f"nothing for {int(quiet // 60)} min. Bills in flight: {', '.join(entry['current'])}. Likely cause: "
                    f"CAMS tasks stuck at 'running' (AGENTS-271). Nothing was stopped; ddp-sync's own 1,800 s timeout "
                    f"ends each wait.")
            logger.warning("session_pipeline_no_progress", run_id=run_id, quiet_seconds=int(quiet), bills=entry["current"])
            await asyncio.to_thread(post_alert, text, source="legbot.no_progress",
                                    dedupe_key=f"legbot_no_progress:{run_id}")
            alerted_at = entry["last_progress_ts"]  # only once it was sent: a failed send is tried again at the next check
        except Exception as exc:  # noqa: BLE001 -- an alert must never touch the run
            logger.warning("session_pipeline_no_progress_alert_failed", run_id=run_id, error=str(exc))


def _bill_outcome(bill_result: dict) -> str:
    """SYNC-178: one bill's result as `ddp-sync status` counts it: "failed" (an error or a failed result type),
    "generated" (LegBot wrote at least one result type), else "skipped" (nothing was needed)."""
    if bill_result.get("error") or bill_result.get("artifacts_failed") or bill_result.get("policy_statement_failed"):
        return "failed"
    if bill_result.get("artifacts_generated") or bill_result.get("policy_statement_statements"):
        return "generated"
    return "skipped"
