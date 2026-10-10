"""Verify and store organization-position citations that came from somewhere other than
find_bill_positions (SYNC-100).

The first such source is DDP's own Slack #legislation history: the older Zapier process cited a
page for each organization position it recorded, and none of those links were ever verified.
This takes a batch of (bill, organization, position, citation_url) items and, for each one the
broker does not already hold a settled row for, runs verify_bill_position against the cited page
and stores the result -- through verify_and_store_position, the same code find_bill_positions
research uses after it finds an organization.

A bill is one unit of work, exactly as it is for find_bill_positions research: a bill that already
has organization-position rows (or a record that research ran) is skipped whole, the same
`get_bill_organization_positions_status` check session_pipeline_runner makes. So a caller sends
ALL of a bill's citations in one request; sending some now and some later would skip the later ones.

Serial on purpose. verify_bill_position is a metered Claude call that fetches the page itself, and
ddp-agents' AGENTS-74 citation corpus (bench/agents74-citation-corpus/README.md) measured four
concurrent fetches reporting nine unreadable citations that a serial rerun cut to one: the harness
was being rate limited and blaming the sites.
"""

from __future__ import annotations

import uuid

import structlog

from ddp_sync.pipelines.bill_organization_position_research import verify_and_store_position
from ddp_sync.services.broker_client import BrokerClientError, get_bill_organization_positions_status
from ddp_sync.services.local_openstates_client import get_current_version_identity

logger = structlog.get_logger()

# Stop the run after this many rate-limited answers in a row. The remaining items would each pay a
# dispatch to be rate limited again; they are reported as not attempted and a later run picks them up.
MAX_CONSECUTIVE_RETRYABLE = 3


def _match_key(org_name: str, position: str, citation_url: str) -> tuple[str, str, str]:
    return (org_name.strip().lower(), position, citation_url.strip())


async def verify_org_citations(
    items: list[dict],
    *,
    source: str,
    dry_run: bool,
    run_id: str | None = None,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Verify and store each item's citation, one whole bill at a time.

    items: dicts with bill_openstates_id, jurisdiction, session_code, gov_id, org_name, position
        ("support"/"oppose") and citation_url.
    source: recorded as the row's find_model_name, so a stored row says where its citation came
        from (e.g. "slack-legislation-zapier") rather than naming a LegBot model that never ran.
    dry_run: check each bill's status and version and count what a real run would dispatch, but
        call neither LegBot nor the write endpoint.

    The size of a run is the size of `items`: send whole bills, and no more of them than you want
    to pay for.

    Returns a summary: counts per outcome, the per-item list, and `partial_bills` -- bills where some
    citations were stored and others were not, because the run stopped being rate limited partway
    through. Such a bill now has rows, so a later run will skip it (the same gap find_bill_positions
    research has if it dies partway through a bill); it needs its leftover citations handled by hand.
    Outcomes are the ones verify_and_store_position reports ("written", "verification_failed",
    "broker_write_failed", "retryable") plus "bill_already_researched", "duplicate_in_request",
    "no_current_version", "broker_read_failed", "dry_run" and "not_attempted".
    """
    run_id = run_id or f"org-citation-verify-{uuid.uuid4().hex[:12]}"
    invocation_id = str(uuid.uuid4())

    by_bill: dict[str, list[dict]] = {}
    for item in items:
        by_bill.setdefault(item["bill_openstates_id"], []).append(item)

    results: list[dict] = []
    partial_bills: list[str] = []
    consecutive_retryable = 0
    stopped_early = False

    for bill_openstates_id, bill_items in by_bill.items():
        if stopped_early:
            results.extend(_outcome(item, "not_attempted") for item in bill_items)
            continue

        try:
            status = await get_bill_organization_positions_status(
                bill_openstates_id=bill_openstates_id,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        except BrokerClientError as exc:
            logger.warning("org_citation_verify_status_read_failed", run_id=run_id,
                           bill_openstates_id=bill_openstates_id, error=str(exc))
            results.extend(_outcome(item, "broker_read_failed") for item in bill_items)
            continue
        if status["has_rows"]:
            results.extend(_outcome(item, "bill_already_researched") for item in bill_items)
            continue

        version = await get_current_version_identity(bill_openstates_id)
        if version is None:
            results.extend(_outcome(item, "no_current_version") for item in bill_items)
            continue

        seen: set[tuple[str, str, str]] = set()
        stored = 0
        left_over = 0
        for item in bill_items:
            key = _match_key(item["org_name"], item["position"], item["citation_url"])
            # The same finding twice in one request would otherwise be verified and stored twice.
            if key in seen:
                results.append(_outcome(item, "duplicate_in_request"))
                continue
            seen.add(key)
            if stopped_early:
                results.append(_outcome(item, "not_attempted"))
                left_over += 1
                continue
            if dry_run:
                results.append(_outcome(item, "dry_run"))
                continue

            result = await verify_and_store_position(
                bill_openstates_id=bill_openstates_id,
                jurisdiction=item["jurisdiction"],
                session_code=item["session_code"],
                version_date=version["version_date"],
                version_note=version["version_note"],
                gov_id=item["gov_id"],
                bill_title=version["bill_title"],
                invocation_id=invocation_id,
                org_name=item["org_name"],
                position=item["position"],
                citation_url=item["citation_url"],
                find_model_name=source,
                skip_write_on_rate_limit=True,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
            results.append(_outcome(item, result["outcome"], position_id=result["position_id"]))
            logger.info("org_citation_verify_item", run_id=run_id, bill_openstates_id=bill_openstates_id,
                        org_name=item["org_name"], outcome=result["outcome"])

            if result["outcome"] == "retryable":
                left_over += 1
            elif result["position_id"] is not None:
                stored += 1
            consecutive_retryable = consecutive_retryable + 1 if result["outcome"] == "retryable" else 0
            if consecutive_retryable >= MAX_CONSECUTIVE_RETRYABLE:
                stopped_early = True
                logger.warning("org_citation_verify_stopped_rate_limited", run_id=run_id,
                               consecutive_retryable=consecutive_retryable)

        if stored and left_over:
            partial_bills.append(bill_openstates_id)
            logger.warning("org_citation_verify_partial_bill", run_id=run_id,
                           bill_openstates_id=bill_openstates_id, stored=stored, left_over=left_over)

    counts: dict[str, int] = {}
    for result in results:
        counts[result["outcome"]] = counts.get(result["outcome"], 0) + 1
    summary = {
        "run_id": run_id,
        "source": source,
        "dry_run": dry_run,
        "stopped_early": stopped_early,
        "partial_bills": partial_bills,
        "counts": counts,
        "results": results,
    }
    logger.info("org_citation_verify_summary", run_id=run_id, dry_run=dry_run,
                stopped_early=stopped_early, partial_bills=partial_bills, counts=counts)
    return summary


def _outcome(item: dict, outcome: str, *, position_id: int | None = None) -> dict:
    return {
        "bill_openstates_id": item["bill_openstates_id"],
        "org_name": item["org_name"],
        "position": item["position"],
        "citation_url": item["citation_url"],
        "outcome": outcome,
        "position_id": position_id,
    }
