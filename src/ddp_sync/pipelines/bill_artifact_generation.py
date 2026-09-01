"""Phase 8 write path — ddp-infra's PLAN-bill-document-provenance.md.

Connects two pieces that already existed separately but were never wired
together: LegBot dispatch (services/legbot_client.py, shipped 2026-07-21,
dispatch-and-return only) and the BillArtifact ledger (ddp-broker-py Phase 6,
merged 2026-07-26). This module is the plan's step 4: write LegBot's result
to ddp-broker-py.

Decided 2026-08-10 (Ramon): this module does NOT also ingest into Pinecone.
LegBot's output already lands in queryable BillArtifact rows VoteBot can
read directly -- re-embedding LegBot's own summary would duplicate the
original "embed the full bill document" design intent for a different
purpose (deep full-text query, not structured artifact lookup) without
serving either well. If VoteBot needs full-bill-text Pinecone search, that
belongs in a new task or an extension of ddp-open-states' bill archiver
(which already owns the full archived text this module's LegBot dispatch
also reads via bill_source resolution below) -- not duplicated here. The
Pinecone primitives (IngestionPipeline, DocumentMetadata, EmbeddingService)
are untouched and still cataloged in primitives.md; this module simply
doesn't call them.

bill_summary/bill_pros_cons/bill_vote_yes_frame/bill_vote_no_frame/
bill_supporting_orgs/bill_opposing_orgs/bill_impact_analysis all dispatch
via generate_and_store_bill_artifact, below -- a single caller-supplied
bill_source, one dispatch_bill_question call each.

bill_changelog is the 8th type, but doesn't fit that shape (it needs a
prior version's text plus a precomputed diff, not a single bill_source)
-- see generate_and_store_bill_changelog, its own function further down
this file, not a branch of generate_and_store_bill_artifact. Until this
was added (ddp-infra's "bill_changelog's missing BillArtifact write path",
approved 2026-08-01 after 5 rounds of /pm-review), bill_changelog's only
caller of dispatch_bill_changelog was bill_version.py's legacy
_generate_and_ingest_changelog, which writes to Pinecone only, never
BillArtifact -- that function is untouched by this change and still
serves whatever the Webflow-CMS sync flow needs it for.

Scope note: this module generates and stores ONE artifact for a caller-
supplied bill version. It does NOT implement Phase 8's step 1 ("find bill
versions that don't have the artifacts they need yet") — that requires
BillVersion rows to actually exist, which is Phase 4's job, not yet built.

bill_source resolution (added 2026-07-30, ddp-infra's "Real gap found
2026-07-29/30" design): before dispatching to LegBot, check whether
ddp-open-states already has archived text for this bill's latest version
(local_openstates_client.get_archived_bill_text, OPEN-13) and use it
directly if present -- skipping the live-fetch-and-re-extract LegBot would
otherwise do for a document ddp-open-states already extracted once. Falls
back to the caller-supplied bill_source (a live URL) unchanged when no
archived text is available, exactly as before this change. Scoped to the
7 single-version artifact types generate_and_store_bill_artifact handles
-- bill_source here is always resolved for a bill's *current* version, so
this never applies to generate_and_store_bill_changelog, which resolves
its own two inputs via get_archived_changelog_inputs instead (no live-
fetch fallback at all -- see that function's own docstring).
"""

from __future__ import annotations

import json

import structlog

from ddp_sync.services.broker_client import (
    BrokerClientError,
    get_bill_artifact_coverage_all_versions,
    write_bill_artifact,
)
from ddp_sync.services.legbot_client import (
    LegBotDispatchError,
    dispatch_bill_changelog,
    dispatch_bill_question,
)
from ddp_sync.services.local_openstates_client import (
    get_archived_bill_text,
    get_archived_version_transitions,
)

logger = structlog.get_logger()

# artifact_type -> LegBot question_type (config/legbot_questions.yaml, ddp-agents)
_ARTIFACT_TYPE_TO_QUESTION_TYPE = {
    "bill_summary": "summary_500char",
    "bill_pros_cons": "pros_cons",
    "bill_vote_yes_frame": "vote_yes_frame",
    "bill_vote_no_frame": "vote_no_frame",
    "bill_supporting_orgs": "supporting_orgs",
    "bill_opposing_orgs": "opposing_orgs",
    "bill_impact_analysis": "impact_analysis",
    "bill_topics": "bill_topics",
}

# artifact_types whose answer is already plain text under an answer["text"]
# key (config/legbot_questions.yaml's output_shape) — flattened identically.
_TEXT_ANSWER_ARTIFACT_TYPES = {"bill_summary", "bill_vote_yes_frame", "bill_vote_no_frame"}

# artifact_types whose answer is a single list under answer["org_types"]
# ([{type, reason}, ...], config/legbot_questions.yaml) — flattened identically.
_ORG_TYPES_ANSWER_ARTIFACT_TYPES = {"bill_supporting_orgs", "bill_opposing_orgs"}

# bill_topics' 33-name taxonomy (ddp-infra's PLAN-legbot.md §27, approved
# 2026-08-07; Priority Bill and Disney excluded) -- copied verbatim from
# ddp-agents' config/legbot_questions.yaml bill_topics entry. Maintenance
# rule: any future Webflow category change updates that YAML list and these
# two constants together.
_BILL_TOPICS_TAXONOMY = (
    "Animals", "Arts", "Business", "Civil Rights", "Criminal Justice",
    "Culture", "Drugs", "Economy", "Education", "Elections", "Employment",
    "Energy", "Environment", "Government", "Guns", "Housing", "Immigration",
    "International Relations", "LGBT", "Marriage", "Media", "Medical",
    "Military and Veterans", "National Security", "Natural Disasters",
    "Public Records", "Public Safety", "Social Welfare", "Sports",
    "State Parks", "Taxes", "Technology", "Transportation",
)
_BILL_TOPICS_MAX = 4  # matches the YAML's max_topics

_BILL_TOPICS_CANONICAL_BY_FOLD = {name.casefold(): name for name in _BILL_TOPICS_TAXONOMY}


class ArchivedVersionMismatchError(Exception):
    """Raised when get_archived_changelog_inputs resolved a different version as
    "latest" than the one the caller asked to generate a changelog for.

    Deliberately never accompanied by any BillArtifact write, successful or
    failed -- see generate_and_store_bill_changelog's own docstring for why.
    """


def _bullet_list(items: list) -> str:
    return "\n".join(f"- {item}" for item in items) if items else "- None"


def _bill_changelog_content_from_answer(
    answer: dict, *, old_version_note: str, new_version_note: str
) -> str:
    """Flatten LegBot's bill_changelog answer into BillArtifact.content.

    Reuses bill_version.py's own _generate_and_ingest_changelog template
    verbatim (bullet-list helper, section structure) -- ddp-next's real
    BillChangelog.tsx parses content as light Markdown (##/### headings, -
    bullets, **bold**), not JSON, confirmed by reading that component
    directly. old_version_note/new_version_note are caller-side context
    (the two versions being diffed), not part of LegBot's own answer --
    the legacy template also sourced them this way, not from the answer.
    One deliberate difference from that legacy template: no "## What
    Changed: {bill_title}" top-level heading -- generate_and_store_
    bill_changelog has no bill_title parameter (not part of the reviewed
    design), and BillChangelog.tsx already renders its own "What Changed"
    section label above this content, so repeating it here would just be
    redundant.
    """
    return f"""**From:** {old_version_note}
**To:** {new_version_note}

### Sections Added
{_bullet_list(answer.get("sections_added") or [])}

### Sections Removed
{_bullet_list(answer.get("sections_removed") or [])}

### Sections Modified
{_bullet_list(answer.get("sections_modified") or [])}

### Key Policy Implications
{answer.get("policy_implications") or "None noted."}"""


# SYNC-47: fields bill_changelog's own answer shape declares for what
# actually changed -- deliberately excludes policy_implications, which the
# model reliably fills with its own refusal rationale ("the diff does not
# contain the actual text of the new version...") even when it also sets
# insufficient_information=true, rather than with real analysis. Counting
# that field here would treat a refusal's own explanation as evidence the
# answer is usable -- the same shape of trap AGENTS-79's own detector had to
# guard against for source_support.
_CHANGELOG_STRUCTURAL_FIELDS = ("sections_added", "sections_removed", "sections_modified")


def _bill_changelog_has_structural_content(answer: dict) -> bool:
    """True when bill_changelog's answer has real added/removed/modified
    content, regardless of what insufficient_information says (SYNC-47).

    Confirmed live 2026-08-31: 3 of 73 real bill_changelog calls set
    insufficient_information=true while sections_added/sections_removed
    were genuinely populated (Committee-substitute/Governor's-recommendation
    transitions) -- CAMS's own legbot_insufficient_but_populated detector
    fired on all 3. Before this existed, that content was discarded
    wholesale (AGENTS-79); this is what makes it publishable instead.
    """
    return any(answer.get(field) for field in _CHANGELOG_STRUCTURAL_FIELDS)


def _canonicalize_bill_topic(raw_topic: str) -> str | None:
    """Match a raw topic string (trimmed, case-insensitive) to its canonical
    _BILL_TOPICS_TAXONOMY name, or None if it isn't a member.
    """
    return _BILL_TOPICS_CANONICAL_BY_FOLD.get(raw_topic.strip().casefold())


def _filter_bill_topics(answer: dict, *, bill_openstates_id: str) -> tuple[str, list[str]] | None:
    """Deterministic filter/flatten for bill_topics' answer shape (ddp-infra's
    PLAN-legbot.md §27), in the plan's specified order:

    1. canonicalize every raw topic (trimmed, case-insensitive); drop
       duplicates and non-members (each drop logged).
    2. if primary_topic is a valid member absent from the surviving topics,
       prepend it (logged).
    3. cap at _BILL_TOPICS_MAX, keeping the primary when one was established
       in step 2.
    4. if primary_topic itself was dropped as a non-member, promote the
       first surviving topic to primary (logged).

    Returns:
        (primary, topics) with primary always a member of topics, or None if
        zero topics survive (the caller records a failed artifact row for
        that case rather than writing empty/misleading content).
    """
    topics: list[str] = []
    seen: set[str] = set()
    for raw_topic in answer.get("topics") or []:
        canonical = _canonicalize_bill_topic(raw_topic)
        if canonical is None:
            logger.info(
                "bill_topics_dropped_non_member",
                bill_openstates_id=bill_openstates_id, raw_topic=raw_topic,
            )
            continue
        if canonical in seen:
            logger.info(
                "bill_topics_dropped_duplicate",
                bill_openstates_id=bill_openstates_id, topic=canonical,
            )
            continue
        seen.add(canonical)
        topics.append(canonical)

    raw_primary = answer.get("primary_topic")
    primary = _canonicalize_bill_topic(raw_primary) if raw_primary else None

    if primary is not None and primary not in topics:
        topics.insert(0, primary)
        logger.info(
            "bill_topics_primary_prepended",
            bill_openstates_id=bill_openstates_id, primary_topic=primary,
        )

    if len(topics) > _BILL_TOPICS_MAX:
        if primary is not None:
            topics = [primary] + [t for t in topics if t != primary][: _BILL_TOPICS_MAX - 1]
        else:
            topics = topics[:_BILL_TOPICS_MAX]
        logger.info(
            "bill_topics_truncated",
            bill_openstates_id=bill_openstates_id, kept_topics=topics,
        )

    if primary is None and raw_primary and topics:
        primary = topics[0]
        logger.info(
            "bill_topics_primary_promoted",
            bill_openstates_id=bill_openstates_id, promoted_topic=primary,
            dropped_primary_topic=raw_primary,
        )

    if not topics:
        return None

    return primary or topics[0], topics


def _bill_topics_content(primary: str, topics: list[str]) -> str:
    return f"**Primary:** {primary}\n\n{_bullet_list(topics)}"


def _content_from_answer(artifact_type: str, answer: dict) -> str:
    """Flatten LegBot's structured answer into BillArtifact.content.

    bill_summary/bill_vote_yes_frame/bill_vote_no_frame's answer is already
    plain text. pros_cons/supporting_orgs/opposing_orgs/bill_impact_analysis'
    answers are structured — stored as a JSON string rather than inventing a
    Markdown format the plan doesn't specify; a consumer rendering these
    artifact types should json.loads() the content back.
    """
    if artifact_type in _TEXT_ANSWER_ARTIFACT_TYPES:
        return answer["text"]
    if artifact_type == "bill_pros_cons":
        return json.dumps({"pros": answer["pros"], "cons": answer["cons"]})
    if artifact_type in _ORG_TYPES_ANSWER_ARTIFACT_TYPES:
        return json.dumps({"org_types": answer["org_types"]})
    if artifact_type == "bill_impact_analysis":
        return json.dumps({
            "affected_parties": answer["affected_parties"],
            "fiscal_or_programmatic_effects": answer["fiscal_or_programmatic_effects"],
            "effective_date": answer["effective_date"],
        })
    raise ValueError(f"Unsupported artifact_type for content extraction: {artifact_type}")


async def _resolve_bill_source(bill_openstates_id: str) -> str | None:
    """Resolve a bill's current text from ddp-open-states' own archive --
    the only source. No live-URL fallback anymore.

    Checks the local api-v3 instance for already-archived, already-extracted
    text for this bill's latest version (OPEN-13). Returns that text if
    present, so LegBot is always handed real, already-quality-verified
    content -- never a bare URL it would have to fetch and extract itself.

    Removed the previous live_url_fallback behavior (falling back to a
    caller-supplied URL when nothing was archived): LegBot has no business
    ever reaching out to the public internet on its own, and every hour of
    OPEN-48 data-quality work (OPEN-15/33/34/76/85 and whatever lands after)
    exists specifically to make ddp-open-states' Postgres archive the one
    trustworthy source of bill text -- a silent fallback to an unverified
    live fetch would undo that guarantee for exactly the bills that need it
    most (the ones not yet backfilled). Returns None when nothing is
    archived; every caller must skip/fail gracefully rather than dispatch
    with no real text, the same posture generate_and_store_bill_changelog's
    get_archived_changelog_inputs already has.
    """
    archived_text = await get_archived_bill_text(bill_openstates_id)
    if archived_text:
        logger.info(
            "Using ddp-open-states' archived bill text",
            bill_openstates_id=bill_openstates_id,
        )
        return archived_text
    return None


async def generate_and_store_bill_artifact(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    artifact_type: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Dispatch to LegBot, then persist the result to ddp-broker-py.

    No bill_source parameter -- removed along with _resolve_bill_source's
    live-URL fallback (see that function's own docstring). This function
    only ever hands LegBot text ddp-open-states has already archived and
    quality-verified; if nothing is archived, it records a failed row
    without ever dispatching, rather than accepting a caller-supplied URL
    it would otherwise fall back to.

    Does not touch Pinecone -- decoupled 2026-08-10, see this module's own
    docstring. A LegBot answer flagged insufficient_information is recorded
    as a failed row (failure_stage=generation), not silently dropped, per
    Phase 6's failure-tracking design.

    Genuinely unrecoverable failures — LegBot unreachable/timed out
    (LegBotDispatchError), or ddp-broker-py rejecting/unreachable
    (BrokerClientError) — propagate to the caller rather than being
    swallowed; there's no BillArtifact row to record them on in the second
    case, and no point creating a placeholder failed row from a dispatch
    that produced no answer at all in the first.

    broker_api_base/broker_api_token pass straight through to
    write_bill_artifact -- see that function's own docstring; None (the
    default) preserves this function's existing behavior for every caller
    that doesn't need per-call broker routing (SYNC-9's batch pipeline).

    Returns:
        The BillArtifact write response as ddp-broker-py's API reports it
        (today, just `id`/`created` -- that serializer doesn't echo `status`
        back), merged with this function's own authoritative `status`
        ("complete" or "failed") under the `status` key -- applied last, so
        it always wins over whatever the broker response itself contains.
        SYNC-24: callers (session_pipeline_runner.py's `_process_bill`) need
        to know which of the two actually happened without re-deriving it
        from failure_stage/failure_reason themselves.
    """
    if artifact_type not in _ARTIFACT_TYPE_TO_QUESTION_TYPE:
        raise ValueError(f"Unsupported artifact_type for Phase 8 dispatch: {artifact_type}")

    question_type = _ARTIFACT_TYPE_TO_QUESTION_TYPE[artifact_type]
    resolved_bill_source = await _resolve_bill_source(bill_openstates_id)
    if resolved_bill_source is None:
        logger.info(
            "No archived bill text -- recording a failed artifact",
            bill_openstates_id=bill_openstates_id,
            artifact_type=artifact_type,
        )
        broker_result = await write_bill_artifact(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=version_date,
            version_note=version_note,
            artifact_type=artifact_type,
            content="",
            status="failed",
            failure_stage="generation",
            failure_reason="no_archived_bill_text",
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        return {**broker_result, "status": "failed"}

    dispatch_result = await dispatch_bill_question(resolved_bill_source, question_type)
    answer = dispatch_result["answer"]
    model_name = dispatch_result.get("backend")

    if answer.get("insufficient_information"):
        logger.info(
            "LegBot reported insufficient_information — recording a failed artifact",
            bill_openstates_id=bill_openstates_id,
            artifact_type=artifact_type,
        )
        broker_result = await write_bill_artifact(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=version_date,
            version_note=version_note,
            artifact_type=artifact_type,
            content="",
            status="failed",
            failure_stage="generation",
            failure_reason="insufficient_information",
            model_name=model_name,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        return {**broker_result, "status": "failed"}

    if artifact_type == "bill_topics":
        filtered = _filter_bill_topics(answer, bill_openstates_id=bill_openstates_id)
        if filtered is None:
            logger.info(
                "bill_topics had zero valid topics -- recording a failed artifact",
                bill_openstates_id=bill_openstates_id,
            )
            broker_result = await write_bill_artifact(
                bill_openstates_id=bill_openstates_id,
                jurisdiction=jurisdiction,
                session_code=session_code,
                version_date=version_date,
                version_note=version_note,
                artifact_type=artifact_type,
                content="",
                status="failed",
                failure_stage="generation",
                failure_reason="no_valid_topics",
                model_name=model_name,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
            return {**broker_result, "status": "failed"}
        primary, topics = filtered
        content = _bill_topics_content(primary, topics)
    else:
        content = _content_from_answer(artifact_type, answer)

    broker_result = await write_bill_artifact(
        bill_openstates_id=bill_openstates_id,
        jurisdiction=jurisdiction,
        session_code=session_code,
        version_date=version_date,
        version_note=version_note,
        artifact_type=artifact_type,
        content=content,
        status="complete",
        model_name=model_name,
        # SYNC-43: AGENTS-80's source_support, recorded rather than dropped.
        # Only "inferred" marks anything -- see _validation_notes_for.
        source_support=answer.get("source_support"),
        broker_api_base=broker_api_base,
        broker_api_token=broker_api_token,
    )
    return {**broker_result, "status": "complete"}


async def _dispatch_and_write_changelog(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    old_bill_source: str,
    diff_source: str,
    old_version_date: str,
    old_version_note: str,
    broker_api_base: str | None,
    broker_api_token: str | None,
) -> dict:
    """Dispatch one already-resolved version transition to LegBot and persist
    it as a bill_changelog BillArtifact attached to version_date/version_note
    (the transition's newer/target version), diffed against old_version_date/
    old_version_note.

    SYNC-44: the write-side body shared by generate_and_store_bill_changelog's
    two callers below (the single latest transition, and its own full-history
    walk) -- both resolve WHICH transition(s) to generate differently, but
    dispatch and persist a single, already-resolved one identically. Neither
    caller wraps this in a try/except for LegBotDispatchError or
    BrokerClientError -- both propagate uncaught, same convention as
    generate_and_store_bill_artifact.
    """
    dispatch_result = await dispatch_bill_changelog(
        old_bill_source=old_bill_source,
        diff_source=diff_source,
    )
    answer = dispatch_result["answer"]
    model_name = dispatch_result.get("backend")

    if answer.get("insufficient_information"):
        # SYNC-47: a flagged answer that still carries real structural
        # content is published, not discarded -- see
        # _bill_changelog_has_structural_content's own docstring for the
        # live evidence this is responding to.
        if _bill_changelog_has_structural_content(answer):
            populated_fields = [
                field for field in _CHANGELOG_STRUCTURAL_FIELDS if answer.get(field)
            ]
            logger.warning(
                "AUDIT sync_bill_changelog_insufficient_but_populated -- "
                "publishing with a review marker instead of discarding (SYNC-47)",
                bill_openstates_id=bill_openstates_id,
                version_note=version_note,
                populated_fields=populated_fields,
            )
            content = _bill_changelog_content_from_answer(
                answer,
                old_version_note=old_version_note,
                new_version_note=version_note,
            )
            broker_result = await write_bill_artifact(
                bill_openstates_id=bill_openstates_id,
                jurisdiction=jurisdiction,
                session_code=session_code,
                version_date=version_date,
                version_note=version_note,
                artifact_type="bill_changelog",
                content=content,
                status="complete",
                model_name=model_name,
                insufficient_but_populated=True,
                compare_version_date=old_version_date,
                compare_version_note=old_version_note,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
            return {**broker_result, "status": "complete"}

        logger.info(
            "LegBot reported insufficient_information for bill_changelog -- "
            "recording a failed artifact",
            bill_openstates_id=bill_openstates_id,
            version_note=version_note,
        )
        broker_result = await write_bill_artifact(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=version_date,
            version_note=version_note,
            artifact_type="bill_changelog",
            content="",
            status="failed",
            failure_stage="generation",
            failure_reason=answer.get("reason", "insufficient_information"),
            model_name=model_name,
            compare_version_date=old_version_date,
            compare_version_note=old_version_note,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        return {**broker_result, "status": "failed"}

    content = _bill_changelog_content_from_answer(
        answer,
        old_version_note=old_version_note,
        new_version_note=version_note,
    )

    try:
        broker_result = await write_bill_artifact(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=version_date,
            version_note=version_note,
            artifact_type="bill_changelog",
            content=content,
            status="complete",
            model_name=model_name,
            source_support=answer.get("source_support"),  # SYNC-43
            compare_version_date=old_version_date,
            compare_version_note=old_version_note,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        return {**broker_result, "status": "complete"}
    except BrokerClientError:
        # Distinguishable from other write failures, per this design's own
        # review: includes the compare_version fields that were attempted,
        # so an operator can tell a compare_version FK-resolution failure
        # (api-v3 has archived a version ddp-broker-py's BillVersion table
        # hasn't synced yet -- rare) apart from any other rejection.
        logger.exception(
            "bill_changelog_write_failed",
            bill_openstates_id=bill_openstates_id,
            compare_version_date=old_version_date,
            compare_version_note=old_version_note,
        )
        raise


async def generate_and_store_bill_changelog(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    gov_id: str | None = None,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Dispatch bill_changelog to LegBot, then persist the result(s) to
    ddp-broker-py -- the 8th BillArtifact type, not part of
    generate_and_store_bill_artifact above because it needs a prior
    version's text plus a precomputed diff, not a single bill_source.

    Does not touch Pinecone -- decoupled 2026-08-10, see this module's own
    docstring.

    ddp-infra's PLAN-bill-document-provenance.md, "bill_changelog's missing
    BillArtifact write path" (approved 2026-08-01 after 5 rounds of
    /pm-review); SYNC-44 (2026-08-28) rewrote this to walk every version
    transition, not just the one immediately before the bill's current
    version.

    SYNC-44 bug this replaces: taking versions[-2]/versions[-1] unconditionally
    means a bill that has reached enrollment always gets a changelog for
    engrossed -> enrolled -- the typesetting-only step -- and never for the
    earlier transitions that actually carried policy content. Confirmed
    against FL 2026E: all six multi-version bills that produced a changelog
    described only whitespace/formatting, with zero genuinely new lines
    across all six.

    Fix: get_archived_version_transitions (services/local_openstates_client.py)
    resolves EVERY transition api-v3 has already archived a diff for, oldest
    -first, and every transition is dispatched and attached to its own
    (newer) BillVersion -- ddp-broker-py already supports this (spiked live
    against the dev broker: two bill_changelog rows coexisted on two
    different versions of the same bill, no schema change needed).

    Each transition's target version must exist as a write target before a
    later transition can name it as compare_version -- ddp-broker-py
    auto-creates a write's own target version but refuses to auto-create a
    referenced compare_version (confirmed live during this ticket's spike:
    "No BillVersion exists for bill=..., compare_version_note=... --
    compare_version must already be synced before it can be referenced").
    Processing oldest-first, after backfilling every version as a ledger
    -only row up front, satisfies this naturally: by the time a transition's
    own compare_version is referenced, either the backfill already created
    it (the very first transition's compare_version) or an earlier iteration
    of this same loop just wrote a real artifact against it.

    AC2 (2026-08-29, /pm-review on the first version of this fix): a bill
    with 3+ versions is only ever re-checked at the OUTER level (session_
    pipeline_runner.py's per-bill coverage check) against its CURRENT latest
    version -- so the moment a bill this pipeline already processed gains a
    new version, the outer gate opens again and this function would
    otherwise re-walk and re-dispatch EVERY transition, including ones that
    already have a complete changelog. Demonstrated live: calling this
    function twice against the same 3-version bill produced 4 LegBot
    dispatches for 2 real transitions. Re-writing an already-`complete`
    changelog isn't just wasted MLX time -- ddp-broker-py's BROKER-105
    revision path queues a fresh review for what the reviewer had already
    approved, so a bill that keeps gaining versions would keep churning its
    entire changelog history back through review, not just its newest hop.

    Fixed by BROKER-130 (merged 2026-08-29): `gov_id`, when provided, is
    used to read artifact coverage across EVERY version of the bill
    (get_bill_artifact_coverage_all_versions, `?versions=all` on the same
    status endpoint the outer single-version coverage check already calls)
    and skip any transition whose target version already carries a
    bill_changelog row of ANY status -- matching this design's existing
    "never regenerate, never overwrite" posture (see
    ArchivedVersionMismatchError below) rather than inventing a new,
    separate retry policy for non-latest versions. `gov_id` is optional
    because it identifies a bill by a different natural key
    (jurisdiction/session/gov_id) than every write in this function uses
    (bill_openstates_id) -- session_pipeline_runner.py's batch callers
    already have it on hand from their own candidate listing and pass it
    through; SYNC-10's on-demand single-bill endpoint (dispatch_and_record_
    bill_artifact) does not carry gov_id in its request body at all today,
    so it keeps this function's pre-BROKER-130 behavior (walk and dispatch
    every transition every call) unchanged -- appropriate for a one-shot,
    explicitly user-triggered dispatch rather than a recurring batch job,
    and not something this ticket's ACs ask to change.

    If every transition is already covered once filtered, this writes
    nothing and returns status="not_applicable" -- the bill is fully caught
    up, which is not a failure any more than its earliest version having
    nothing to diff against is (see below).

    SYNC-46 (2026-08-29): the version-mismatch guard below now compares the
    caller's version against the bill's true latest ARCHIVED version, not
    against transitions[-1]'s own target -- see that check's own comment
    for the live Utah case (SB 59: 6 versions, 5 real archived transitions,
    zero changelogs produced) this fixes. A version can be real and current
    while having no diff of its own; that is not staleness, and previously
    cost a bill its entire changelog history rather than degrading to
    "generate what is ready."

    A bill with no version transition ready yet -- either it has only one
    archived version ever (nothing precedes it to diff against, not a
    failure, just not yet applicable) or its very next transition's diff/
    prior text simply isn't archived -- writes nothing at all and returns
    status="not_applicable" (SYNC-44/AC4), replacing the blanket `failed`/
    "no_archived_changelog_inputs" row this used to write for both cases.
    That row was actively harmful once SYNC-42's retry_failed shipped: a
    bill's permanently-not-applicable first version looked identical to a
    real, retryable failure and got retried forever.

    Raises:
        ArchivedVersionMismatchError: the caller's version_date/version_note
            don't match the bill's TRUE LATEST ARCHIVED version (SYNC-46:
            not transitions[-1]'s own target, which can legitimately lag
            behind when the true latest has no diff of its own) -- a stale
            caller, or the bill has moved on to a newer version since the
            caller looked it up. Deliberately raises *without writing
            anything at all*, not
            a failed row: writing a failed row here would upsert via
            write_bill_artifact's own (bill_version, artifact_type,
            model_version, prompt_version) key, which resolves from this
            call's own (stale) version_date/version_note -- exactly the row
            a real, already-successful changelog for that version might
            already occupy. This is a caller/timing bug, not a "couldn't
            analyze this bill" outcome; there is no correct row to write for
            it, and the one thing that must never happen is silently
            overwriting a good row with a bad one. Safe to simply retry later
            with fresh version info -- since nothing was written, a future
            coverage check still sees this bill as missing bill_changelog and
            will dispatch it again.
        LegBotDispatchError: LegBot unreachable/timed out -- propagates
            uncaught, same convention as generate_and_store_bill_artifact.
        BrokerClientError: ddp-broker-py rejected a request or was
            unreachable -- propagates uncaught, same convention as
            generate_and_store_bill_artifact. Two distinct sources when
            `gov_id` is provided: the all-versions coverage read itself
            (including a deliberate raise if the target broker predates
            BROKER-130 and silently ignores `?versions=all` -- see
            get_bill_artifact_coverage_all_versions' own docstring for why
            that must fail loudly rather than look like "nothing covered
            yet"), and a write failing mid-walk, which leaves whichever
            earlier transitions in this same call already wrote
            successfully in place (each is its own natural-key upsert on its
            own version, so nothing is left half-written) -- a later run,
            once the bill's current latest version still shows no
            bill_changelog, re-walks from the start; re-dispatching an
            already-succeeded transition again is a harmless idempotent
            update to that same row, not a duplicate.

    Returns:
        The BillArtifact write response as ddp-broker-py's API reports it,
        merged with this function's own authoritative `status` ("complete",
        "failed", or "not_applicable") under the `status` key -- same
        enrichment as generate_and_store_bill_artifact's own return value;
        see that function's docstring for why (SYNC-24). Every transition is
        written for real regardless, but this returns the FIRST failing
        transition's own result if any failed, and only the last
        transition's result if every one succeeded -- an earlier version of
        this function returned whichever transition ran last unconditionally,
        which let one failed transition (e.g. LegBot reporting
        insufficient_information) hide behind a later, successful one and
        get reported as "complete" to session_pipeline_runner.py's caller,
        even though a real failed row was left behind with nothing left to
        revisit it (the outer coverage gate only ever looks at the bill's
        current latest version).
    """
    resolved = await get_archived_version_transitions(bill_openstates_id)

    if resolved is None:
        logger.info(
            "No archived version transition ready for bill_changelog yet -- "
            "not a failure, nothing to write",
            bill_openstates_id=bill_openstates_id,
        )
        return {"status": "not_applicable"}

    transitions = resolved["transitions"]

    # SYNC-46: compare against the bill's true latest ARCHIVED version --
    # api-v3's own versions[-1] (SYNC-16/OPEN-92), the same value
    # get_current_version_identity() resolves as "current" for this exact
    # caller -- not against transitions[-1]'s own target. Those two are NOT
    # always the same version: a version can be real and current while
    # having no diff of its own (its predecessor's text is byte-identical,
    # or an extraction gap), in which case it never becomes any
    # transition's target at all. The previous check compared against
    # transitions[-1] and raised in exactly that normal situation --
    # observed live on Utah 2026: SB 59 has 6 versions and 5 archived
    # transitions, all with real diffs, and produced zero changelogs
    # because "Enrolled" (real, current, no diff of its own) didn't match
    # transitions[-1]'s target ("Substitute #2", the last version WITH a
    # diff). Comparing against the true latest fixes this without weakening
    # the guard: a version that isn't even in the bill's archived list at
    # all -- the actual stale/moved-on case this guard exists for -- still
    # fails this comparison and still raises.
    true_latest_version = resolved["versions"][-1]

    if (
        true_latest_version.get("date", "") != version_date
        or true_latest_version.get("note", "") != version_note
    ):
        logger.warning(
            "bill_changelog_archived_version_mismatch",
            bill_openstates_id=bill_openstates_id,
            requested_version_date=version_date,
            requested_version_note=version_note,
            archived_latest_version_date=true_latest_version.get("date", ""),
            archived_latest_version_note=true_latest_version.get("note", ""),
        )
        raise ArchivedVersionMismatchError(
            f"Requested a bill_changelog for version_date={version_date!r}/"
            f"version_note={version_note!r}, but get_archived_version_transitions "
            f"resolved the bill's true latest archived version as "
            f"{true_latest_version.get('date', '')!r}/"
            f"{true_latest_version.get('note', '')!r} -- refusing to write a "
            "changelog under a stale or mismatched version identity."
        )

    # AC2/BROKER-130 (2026-08-29): skip any transition whose target version
    # already carries a bill_changelog, of any status -- never regenerate,
    # never overwrite. Matched by version_note alone: ddp-broker-py's
    # all-versions coverage response doesn't expose version_date per entry
    # (BillVersion.version_date is blank on over half of all rows anyway),
    # and version_note is the natural key that actually distinguishes a
    # bill's own versions from each other in practice. Accepted, documented
    # limitation (raised on /pm-review's second pass): if a single bill ever
    # has two distinct versions sharing an identical note, this could skip a
    # transition that genuinely still needs generating. Not observed in any
    # real bill this ticket's own investigation looked at, and the failure
    # mode if it ever happens is a missed changelog, not a corrupted or
    # wrongly-overwritten one -- the same category of imprecision this
    # module already accepts elsewhere (version_date itself being blank on
    # most real rows). Fixing it properly needs ddp-broker-py to expose a
    # stable per-version identifier in this response, a further BROKER
    # ticket, not more logic here.
    #
    # gov_id is optional -- see this function's own docstring for why. When
    # it's absent (SYNC-10's on-demand endpoint), this is a no-op and every
    # transition is dispatched every call, exactly as before this fix.
    if gov_id is not None:
        coverage = await get_bill_artifact_coverage_all_versions(
            jurisdiction=jurisdiction,
            session_code=session_code,
            gov_id=gov_id,
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        # .get(..., default) throughout, not direct indexing (/pm-review's
        # second-pass catch): a response missing a key this code didn't
        # explicitly ask ddp-broker-py to guarantee should degrade to "no
        # extra coverage found" for that piece, not crash this bill's whole
        # dispatch with a raw KeyError. The one shape violation that DOES
        # need to fail loudly -- a "found" response with no `versions` key
        # at all, meaning the broker predates BROKER-130 -- is already
        # handled inside get_bill_artifact_coverage_all_versions itself.
        already_covered_notes = {
            entry.get("version_note")
            for entry in (
                (coverage.get("versions") or []) + (coverage.get("unclassified_versions") or [])
            )
            if "bill_changelog" in (entry.get("artifacts") or {})
        } if coverage is not None else set()

        transitions = [
            t for t in transitions if t["new_version_note"] not in already_covered_notes
        ]
        if not transitions:
            logger.info(
                "Every archived transition already has a bill_changelog -- "
                "nothing new to write",
                bill_openstates_id=bill_openstates_id,
            )
            return {"status": "not_applicable"}

    # SYNC-26 follow-up: run_legbot_pipeline (this call's own caller) never
    # goes through check_and_reingest_version, so a bill this pipeline is
    # generating artifacts for can hit the exact compare_version-FK-
    # resolution 400 SYNC-26 fixed for the daily bill_version_check job --
    # confirmed live against FL 2026E, where run_legbot_pipeline was the
    # actual caller that surfaced the bug report.
    #
    # SYNC-28: the original version of this fix gated the backfill on "does
    # ddp-broker-py's ledger have ANY row at all for this bill" (skipping
    # entirely once one existed), reasoning that a bill with a `latest` row
    # was either genuinely fully synced already, or "already broken" in a
    # way only a separate ddp-broker-py-side management command should
    # touch. That reasoning missed a real, deterministic case: within a
    # single run_legbot_pipeline batch call, bill_changelog runs LAST among
    # a bill's artifact types, and ddp-broker-py's own BillArtifact write
    # endpoint auto-creates a bare BillVersion row for whichever version an
    # EARLIER sibling artifact type (bill_summary, bill_pros_cons, etc.)
    # just wrote -- always the bill's actual current version, never the
    # OLDER compare_version this function itself needs. The per-bill gate
    # saw that sibling-created row, concluded "already has a ledger row,"
    # and skipped backfilling the older version entirely -- so the write
    # below failed 100% of the time for every bill in this shape.
    #
    # Fix: call _backfill_missing_versions unconditionally instead of
    # gating on ledger state at all. Safe to do because
    # _backfill_missing_versions already (a) never re-writes
    # `latest_version`'s own natural key (so it can't touch/duplicate
    # whatever row a sibling artifact type -- or a real prior
    # check_and_reingest_version sync -- already created for the current
    # version), and (b) calls write_bill_version, itself an idempotent
    # natural-key upsert, for the compare_version row -- a no-op if that
    # row already exists, a real (if overdue) creation if it doesn't.
    # _backfill_missing_versions also never raises (best-effort, logs and
    # continues per its own docstring), so a broker hiccup here cannot
    # block the write below -- both properties already have their own
    # dedicated coverage in test_bill_version_history.py (respectively
    # test_backfill_excludes_latest_by_natural_key_not_object_identity and
    # test_backfill_one_bad_version_does_not_block_the_others), not
    # re-verified again here.
    #
    # Known, bounded, accepted trade-off (not a new risk this fix
    # introduces, but stated honestly rather than left implicit): if the
    # compare_version row genuinely has never existed before and is
    # created only now -- after a sibling artifact type's write already
    # created the current version's row moments earlier in this same
    # batch -- its created_at will be later than the current version's.
    # BillVersion.created_at is auto_now_add and ddp-broker-py's own
    # latest_bill_version resolves by -created_at, not version_date, so
    # this can cause get_latest_bill_version's next caller
    # (check_and_reingest_version, the daily live-poll job) to see the
    # older version as "latest" and redundantly re-check/re-ingest a
    # version it already has real data for. That's wasted work, not data
    # loss or a broken artifact -- and it is strictly better than this
    # function's own prior behavior, which failed outright, every time,
    # for every bill in this shape.
    #
    # SYNC-30/SYNC-44: pass resolved["versions"] -- the bill's COMPLETE
    # archived version list -- rather than a synthetic 2-element [old, new]
    # list. A bill with 3+ real versions (confirmed live, FL SB 2506E:
    # Filed -> e1 -> er) needs every older version backfilled as a ledger
    # -only row, not just the one immediately-previous compare_version a
    # single transition would need -- _backfill_missing_versions already
    # loops over every entry in `versions` other than `latest_version`
    # (SYNC-26); passing the real full list here, which
    # get_archived_version_transitions already fetched to resolve every
    # transition, costs no extra I/O.
    from ddp_sync.pipelines.bill_version import BillVersionSyncService

    latest_version_raw = resolved["versions"][-1]
    await BillVersionSyncService._backfill_missing_versions(
        bill_openstates_id=bill_openstates_id,
        jurisdiction_code=jurisdiction,
        session_code=session_code,
        versions=resolved["versions"],
        latest_version={
            "date": latest_version_raw.get("date", ""),
            "note": latest_version_raw.get("note", ""),
        },
        broker_api_base=broker_api_base,
        broker_api_token=broker_api_token,
    )

    # Oldest-first (see this function's own docstring on why order matters
    # here): each transition's target version must exist before the NEXT
    # transition can reference it as compare_version.
    # _backfill_missing_versions above already covers the very first
    # transition's compare_version as a ledger-only row; every later
    # transition's compare_version is a version an earlier iteration of this
    # same loop just wrote a real artifact against.
    #
    # /pm-review caught a real gap in an earlier version of this loop: it
    # kept only the LAST transition's result, so an older transition failing
    # (e.g. LegBot reports insufficient_information) while a later one
    # succeeds reported the whole call as "complete" -- silently hiding a
    # real failed row that nothing revisits afterward (the outer coverage
    # gate only ever looks at the bill's current latest version, which the
    # later, successful transition just made look fully covered). Every
    # transition still gets written for real regardless -- this only changes
    # what status is REPORTED back to the caller when one of them failed.
    first_failure: dict | None = None
    last_result: dict = {"status": "not_applicable"}
    for transition in transitions:
        last_result = await _dispatch_and_write_changelog(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=transition["new_version_date"],
            version_note=transition["new_version_note"],
            old_bill_source=transition["old_bill_source"],
            diff_source=transition["diff_source"],
            old_version_date=transition["old_version_date"],
            old_version_note=transition["old_version_note"],
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
        if first_failure is None and last_result.get("status") != "complete":
            first_failure = last_result
    return first_failure if first_failure is not None else last_result


async def dispatch_and_record_bill_artifact(
    *,
    bill_openstates_id: str,
    jurisdiction: str,
    session_code: str,
    version_date: str,
    version_note: str,
    bill_source: str,
    artifact_type: str,
    broker_api_base: str,
    broker_api_token: str,
) -> None:
    """Background-task wrapper for the on-demand single-bill endpoint
    (SYNC-10) -- ddp-infra's PLAN-bill-document-provenance.md, extended for
    on-demand dispatch triggered via ddp-next -> ddp-api -> ddp-sync.

    bill_source is accepted for API-shape stability (the public request
    body still requires it) but no longer passed through to
    generate_and_store_bill_artifact, which dropped its own bill_source
    parameter entirely once _resolve_bill_source stopped falling back to a
    live URL -- see that function's own docstring for why.

    Writes an initial `pending` BillArtifact row before dispatching to
    LegBot, then lets generate_and_store_bill_artifact/
    generate_and_store_bill_changelog update the *same* row (via
    write_bill_artifact's existing upsert-by-natural-key semantics) once a
    real answer comes back -- so ddp-next can poll ddp-broker-py and
    observe a real pending -> complete/failed transition, per this ticket's
    AC #3. The pending write and the eventual complete/failed write always
    target the same broker_api_base/broker_api_token pair passed in here,
    so the upsert resolves against one broker's own database throughout --
    never split across the dev/prod pair.

    Never raises: this runs as a fire-and-forget FastAPI BackgroundTasks
    callback with no caller left to propagate an exception to by the time
    it executes. LegBotDispatchError (LegBot unreachable/timed out),
    BrokerClientError (ddp-broker-py rejected the write or was
    unreachable), and ArchivedVersionMismatchError (bill_changelog only --
    normally raised *without* writing anything, to protect a possibly-good
    pre-existing row under the same natural key; that concern doesn't apply
    here, since the only row under this key is the pending placeholder this
    same call just wrote) are all caught and turned into a terminal
    `failed` row instead -- without one, the pending row would be stuck
    forever with nothing left to update it.
    """
    try:
        await write_bill_artifact(
            bill_openstates_id=bill_openstates_id,
            jurisdiction=jurisdiction,
            session_code=session_code,
            version_date=version_date,
            version_note=version_note,
            artifact_type=artifact_type,
            content="",
            status="pending",
            broker_api_base=broker_api_base,
            broker_api_token=broker_api_token,
        )
    except BrokerClientError as exc:
        # Nothing to update later -- the caller's poll will simply never
        # see a row for this bill+artifact_type, same as if this call had
        # never been dispatched at all.
        logger.exception(
            "dispatch_and_record_bill_artifact_pending_write_failed",
            bill_openstates_id=bill_openstates_id,
            artifact_type=artifact_type,
            error=str(exc),
        )
        return

    try:
        if artifact_type == "bill_changelog":
            changelog_result = await generate_and_store_bill_changelog(
                bill_openstates_id=bill_openstates_id,
                jurisdiction=jurisdiction,
                session_code=session_code,
                version_date=version_date,
                version_note=version_note,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
            if changelog_result.get("status") == "not_applicable":
                # SYNC-44: unlike session_pipeline_runner.py's batch caller
                # (which never writes a placeholder row and is happy to
                # leave nothing behind for a not-yet-applicable bill), this
                # caller already wrote a `pending` row above and ddp-next is
                # polling it -- leaving it pending forever would hang that
                # poll. There's no broker-side "not_applicable" status (only
                # pending/processing/complete/failed), so this resolves the
                # placeholder to `failed` with a reason string that reads
                # distinctly from a real generation failure -- this repo has
                # no scheduled retry sweep against this on-demand endpoint,
                # so unlike the batch path's own AC4 concern, there's no
                # retry_failed loop for this to get stuck in.
                await write_bill_artifact(
                    bill_openstates_id=bill_openstates_id,
                    jurisdiction=jurisdiction,
                    session_code=session_code,
                    version_date=version_date,
                    version_note=version_note,
                    artifact_type=artifact_type,
                    content="",
                    status="failed",
                    failure_stage="generation",
                    failure_reason="no_version_transition_available",
                    broker_api_base=broker_api_base,
                    broker_api_token=broker_api_token,
                )
        else:
            await generate_and_store_bill_artifact(
                bill_openstates_id=bill_openstates_id,
                jurisdiction=jurisdiction,
                session_code=session_code,
                version_date=version_date,
                version_note=version_note,
                artifact_type=artifact_type,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
    except (LegBotDispatchError, BrokerClientError, ArchivedVersionMismatchError) as exc:
        logger.warning(
            "dispatch_and_record_bill_artifact_dispatch_failed",
            bill_openstates_id=bill_openstates_id,
            artifact_type=artifact_type,
            error=str(exc),
        )
        try:
            await write_bill_artifact(
                bill_openstates_id=bill_openstates_id,
                jurisdiction=jurisdiction,
                session_code=session_code,
                version_date=version_date,
                version_note=version_note,
                artifact_type=artifact_type,
                content="",
                status="failed",
                failure_stage="dispatch_error",
                failure_reason=str(exc),
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        except BrokerClientError:
            logger.exception(
                "dispatch_and_record_bill_artifact_failed_write_also_failed",
                bill_openstates_id=bill_openstates_id,
                artifact_type=artifact_type,
            )
