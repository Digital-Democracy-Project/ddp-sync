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

This module's only remaining piece is the per-bill unit:

- `dispatch_and_store_concept_statements()` — mirrors
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

SYNC-32: this module's own scheduled batch job (`run_concept_statement_
batch_job()`, Phase 0.4's original "which bills to iterate over" work,
Gating Question 3 resolved 2026-07-30 by Ramon) has been retired and
removed. `dispatch_and_store_concept_statements()` above is now called
directly by `session_pipeline_runner.py`'s own per-bill batch instead
(`include_concept_statements`, SYNC-31), reusing that pipeline's own
candidate enumeration, `ensure_bill_exists()` on-demand Bill-stub creation,
and coverage/dedup conventions rather than this module's own separate
copies of the same three jobs. The dedup rule this removed job implemented
-- skip a bill with an existing *published* `ConceptStatementSet`, checked
via `get_concept_statement_set` (published-only, per §0.3's public-read
rule; a bill with an existing *pending* or *rejected* set is not separately
detected, an accepted, reviewable, harmless admin-side duplicate, not
silently-wrong data) -- was carried over unchanged into
session_pipeline_runner.py's own equivalent check, not rewritten.
"""

from __future__ import annotations

import structlog

from ddp_sync.pipelines.bill_artifact_generation import _resolve_bill_source
from ddp_sync.services.broker_client import create_concept_statement_set
from ddp_sync.services.legbot_client import dispatch_bill_question

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
