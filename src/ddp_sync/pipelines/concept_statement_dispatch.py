"""Phase 0.4 dispatch wiring — ddp-infra's PLAN-bill-concept-polling.md,
"0.4 — Dispatch wiring (ddp-sync)".

Wires LegBot's new `concept_statements` question type (ddp-agents PR #105,
config/tool-enum only, no LegBot code path changes) to the new
`ConceptStatementSet` model/endpoint (ddp-broker-py PR #247) — the same
generate-then-persist shape `bill_artifact_generation.py`'s
`generate_and_store_bill_artifact()` already established for BillArtifact's
8 existing types, but writing to a different, independent model. That's a
deliberate consequence of the plan's foundational decision (see
PLAN-bill-concept-polling.md's "Foundational question" section): concept
statements must exist for *any* bill `/explore` reaches (any scraped bill
in a tracked jurisdiction), not just the narrow Voatz-curated subset
`BillArtifact`/`BillVersion` are scoped to — so this module does not reuse
`write_bill_artifact` or its Bill-row dependency at all.

Two independent pieces:

- `dispatch_and_store_concept_statements()` — the per-bill unit. Mirrors
  `generate_and_store_bill_artifact()` closely: resolve `bill_source` via
  the same archived-text-first helper (`_resolve_bill_source`, reused
  directly rather than duplicated), dispatch, then write. Its
  `insufficient_information` handling is *stricter* than BillArtifact's:
  there is no "failed" `ConceptStatementSet` status to record a non-answer
  against (§0.3's status enum is `pending`/`published`/`rejected` only,
  all three meaning "a real set of statements exists, in some review
  state") — per LegBot's own PLAN-legbot.md §25 and this plan's Phase 0.1,
  a partial/empty statement list must never look like a real set. So
  `insufficient_information` short-circuits with *no create call at all*,
  not a placeholder row.

- `run_concept_statement_batch_job()` — Phase 0.4's own new work: a
  scheduled batch job (Gating Question 3, resolved 2026-07-30 by Ramon —
  not on-demand-on-first-view). "Which bills to iterate over" is an
  explicitly still-open gap the plan shares with other not-yet-built
  backlog jobs in this fleet (e.g. Phase 8's own "find bill versions
  needing X" step) — scoped here deliberately narrowly rather than left
  unbuilt:

  - Iterates *current-session* bills in *tracked* jurisdictions only.
    "Tracked" reuses sync_schedule.yaml's existing `active_jurisdictions`
    seed list (the same list bill_sync.py/legislator_sync already treat as
    this repo's canonical "what DDP actually covers" set) — passed in by
    the scheduler wrapper, not re-invented here. "Current session" reuses
    `OpenStatesSource.get_current_session_identifier()` inside
    `list_current_session_bill_candidates` (services/local_openstates_
    client.py) — the same session-resolution call bill_sync.py's own
    `is_current_session_async()` already makes.
  - Bill identity enumeration reads the *local* archive api-v3 instance's
    paginated bill list (same host/auth this module already needs for
    bill_source resolution), not the live, rate-limited v3.openstates.org
    API — see that function's own docstring for why.
  - Dedup, decided: a bill with an existing *published* ConceptStatementSet
    is skipped, checked via the existing `GET /api/concept-statements/`
    read endpoint (`get_concept_statement_set` — published-only, per
    §0.3's public-read rule). A bill with an existing *pending* or
    *rejected* set is **not** separately detected — no endpoint in this
    contract exposes "does any set of any status already exist," and
    building one is out of scope for this task (the public read endpoint
    is deliberately published-only). In practice: a bill still awaiting
    staff review can accumulate an extra `pending` row on a later
    scheduled run before it's reviewed — an accepted, reviewable, harmless
    admin-side duplicate (this task's own explicitly sanctioned fallback
    for this exact gap), not silently-wrong data. The check this module
    *does* perform covers the common steady-state case that actually
    matters for cost: a bill that already has a published, reviewed answer
    is never wastefully re-dispatched to LegBot on every subsequent run.
  - A configurable `max_bills_per_run` cap (config-driven, mirrors
    legislator_sync's own `max_legislators_per_run` convention exactly)
    bounds each run's real LegBot-dispatch + broker-write cost — this is a
    production job hitting a real compute-costing dispatch and a real
    write endpoint, not a free operation to run unbounded.
"""

from __future__ import annotations

from typing import Any

import structlog

from ddp_sync.pipelines.bill_artifact_generation import _resolve_bill_source
from ddp_sync.services.broker_client import (
    BrokerClientError,
    create_concept_statement_set,
    get_concept_statement_set,
)
from ddp_sync.services.legbot_client import LegBotDispatchError, dispatch_bill_question
from ddp_sync.services.local_openstates_client import list_current_session_bill_candidates

logger = structlog.get_logger()

# LegBot question type this module dispatches -- ddp-agents PR #105
# (config/legbot_questions.yaml + tools.py's DISPATCH_LEGBOT_TOOL enum).
_QUESTION_TYPE = "concept_statements"


async def dispatch_and_store_concept_statements(
    *,
    gov_id: str,
    bill_openstates_id: str,
    jurisdiction_iso2: str,
    session_code: str,
    bill_source: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict | None:
    """Dispatch LegBot's `concept_statements` question for one bill, then
    persist the result as a new `ConceptStatementSet` row.

    broker_api_base/broker_api_token: optional per-call override forwarded
    unchanged to create_concept_statement_set, same shape as every other
    broker write in this codebase (SYNC-10) -- None (the default) preserves
    this function's original behavior for its original caller (the now-
    retired standalone batch job, SYNC-32); session_pipeline_runner.py's
    consolidated path (SYNC-31) passes these through so this write lands on
    the same dev/prod broker instance its other artifact types do.

    Args:
        gov_id: the bill's short public identifier (e.g. "SJR 2F",
            `Bill.identifier`) -- this set's own identity key, matching
            every other `gov_id` in this plan (`BillPromotionRequest`,
            `ConceptStatementSet`, the public read/vote API) and fitting
            `ConceptStatementSet.gov_id`'s `max_length=20`. **Not** the
            OpenStates UUID -- fixed 2026-07-30 after live testing found
            every real dispatch failed this field's length constraint
            (a bare UUID is 36 characters) because this parameter and
            `bill_openstates_id` below used to be the same value.
        bill_openstates_id: bare OpenStates bill UUID (no "ocd-bill/"
            prefix), matching get_archived_bill_text's own convention --
            used only to check ddp-open-states' archive, never stored.
        jurisdiction_iso2: two-letter state code (e.g. "FL").
        session_code: the bill's legislative session identifier.
        bill_source: URL to the bill's PDF/HTML. No longer used as a
            live-fetch fallback for LegBot dispatch (_resolve_bill_source
            dropped that behavior entirely -- see its own docstring); kept
            here purely as the value stored as ConceptStatementSet.
            source_document_url, a denormalized citation of which document
            this set represents.

    Returns:
        The created ConceptStatementSet row as ddp-broker-py's API reports
        it, or None when LegBot reported insufficient_information, or when
        nothing is archived for this bill at all (no dispatch attempted in
        that case) -- there is nothing to publish either way (see this
        module's docstring), and no row is created at all (unlike
        BillArtifact's own "failed" status, which ConceptStatementSet has
        no equivalent of).

    Raises:
        LegBotDispatchError: LegBot dispatch failed to produce a usable
            answer -- propagates unchanged, never swallowed.
        BrokerClientError: ddp-broker-py rejected the write or was
            unreachable -- propagates unchanged; there's no
            ConceptStatementSet row to record a failure on (unlike
            BillArtifact's "failed" status write path).
    """
    resolved_bill_source = await _resolve_bill_source(bill_openstates_id)
    if resolved_bill_source is None:
        logger.info(
            "No archived bill text -- skipping concept_statements entirely, no dispatch",
            bill_openstates_id=bill_openstates_id,
            gov_id=gov_id,
        )
        return None

    dispatch_result = await dispatch_bill_question(resolved_bill_source, _QUESTION_TYPE)
    answer = dispatch_result["answer"]

    if answer.get("insufficient_information"):
        logger.info(
            "LegBot reported insufficient_information for concept_statements "
            "-- nothing to publish, skipping the create call entirely",
            gov_id=gov_id,
            jurisdiction_iso2=jurisdiction_iso2,
            session_code=session_code,
        )
        return None

    return await create_concept_statement_set(
        gov_id=gov_id,
        jurisdiction_iso2=jurisdiction_iso2,
        session_code=session_code,
        statements=answer["statements"],
        source_document_url=bill_source,
        model_name=dispatch_result.get("backend"),
        broker_api_base=broker_api_base,
        broker_api_token=broker_api_token,
    )


async def run_concept_statement_batch_job(
    config: dict[str, Any],
    *,
    jurisdictions: list[str] | None = None,
) -> dict[str, Any]:
    """Scheduled batch job (ddp-infra PLAN-bill-concept-polling.md, Gating
    Question 3): iterate current-session bills in tracked jurisdictions,
    dispatching + storing concept statements, up to `max_bills_per_run`
    bills total for this run.

    Args:
        config: this job's own `concept_statement_dispatch` YAML block
            (sync_schedule.yaml) -- reads `max_bills_per_run` (default 25).
        jurisdictions: tracked jurisdiction codes to iterate. Passed in
            explicitly (rather than read from YAML here) so this function
            stays independently testable -- the scheduler wrapper is
            responsible for resolving this from sync_schedule.yaml's
            top-level `active_jurisdictions` list (or this job's own
            override, if configured).

    Returns:
        A summary dict: {"considered", "skipped_existing_published",
        "dispatched", "created", "insufficient_information", "failed",
        "errors"} -- errors capped at the first 10, matching this repo's
        existing SyncBatchResult convention (bill_sync.py/legislator_sync.py).
    """
    max_bills = config.get("max_bills_per_run", 25)
    jurisdictions = jurisdictions or []

    considered = 0
    skipped_existing_published = 0
    dispatched = 0
    created = 0
    insufficient = 0
    failed = 0
    errors: list[str] = []

    for jurisdiction_iso2 in jurisdictions:
        if considered >= max_bills:
            break

        candidates = await list_current_session_bill_candidates(
            jurisdiction_iso2,
            limit=max_bills - considered,
        )

        for candidate in candidates:
            if considered >= max_bills:
                break
            considered += 1

            gov_id = candidate["gov_id"]
            bill_openstates_id = candidate["bill_openstates_id"]
            session_code = candidate["session_code"]
            bill_source = candidate["live_url_fallback"]

            try:
                existing = await get_concept_statement_set(
                    gov_id=gov_id,
                    jurisdiction_iso2=jurisdiction_iso2,
                    session_code=session_code,
                )
            except BrokerClientError as exc:
                failed += 1
                errors.append(f"{jurisdiction_iso2}/{gov_id}: read check failed: {exc}")
                continue

            if existing is not None:
                skipped_existing_published += 1
                continue

            dispatched += 1
            try:
                result = await dispatch_and_store_concept_statements(
                    gov_id=gov_id,
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction_iso2=jurisdiction_iso2,
                    session_code=session_code,
                    bill_source=bill_source,
                )
            except (LegBotDispatchError, BrokerClientError) as exc:
                failed += 1
                errors.append(f"{jurisdiction_iso2}/{gov_id}: {exc}")
                continue

            if result is None:
                insufficient += 1
            else:
                created += 1

    logger.info(
        "concept_statement_dispatch batch run complete",
        considered=considered,
        skipped_existing_published=skipped_existing_published,
        dispatched=dispatched,
        created=created,
        insufficient_information=insufficient,
        failed=failed,
    )
    return {
        "considered": considered,
        "skipped_existing_published": skipped_existing_published,
        "dispatched": dispatched,
        "created": created,
        "insufficient_information": insufficient,
        "failed": failed,
        "errors": errors[:10],
    }
