"""Verify and store organization-position citations that came from somewhere other than
find_bill_positions (SYNC-100).

The first such source is DDP's own Slack #legislation history: the older Zapier process cited a
page for each organization position it recorded, and none of those links were ever verified.
This takes a batch of (bill, organization, position, citation_url) items and, for each one the
broker does not already hold a settled row for, runs verify_bill_position against the cited page
and stores the result -- through verify_and_store_position, the same code find_bill_positions
research uses after it finds an organization.

Serial on purpose. verify_bill_position is a metered Claude call that fetches the page itself, and
ddp-agents' AGENTS-74 citation corpus (bench/agents74-citation-corpus/README.md) measured four
concurrent fetches reporting nine unreadable citations that a serial rerun cut to one: the harness
was being rate limited and blaming the sites.
"""

from __future__ import annotations

import uuid

import structlog

from ddp_sync.pipelines.bill_organization_position_research import verify_and_store_position
from ddp_sync.services.broker_client import BrokerClientError, get_bill_organization_positions_existing
from ddp_sync.services.local_openstates_client import get_current_version_identity

logger = structlog.get_logger()

# A row the broker already holds counts as settled -- the item is skipped -- only when verification
# reached a verdict. A "pending" or failed row means it could not be checked, so a rerun tries again.
_SETTLED_VERDICTS = frozenset({"confirmed", "not_confirmed"})

# Stop the run after this many rate-limited answers in a row. The remaining items would each pay a
# dispatch to be rate limited again; they are reported as not attempted and a later run picks them up.
MAX_CONSECUTIVE_RETRYABLE = 3


def _match_key(org_name: str, position: str, citation_url: str) -> tuple[str, str, str]:
    return (org_name.strip().lower(), position, citation_url.strip())


def _settled_keys(existing_rows: list[dict]) -> set[tuple[str, str, str]]:
    return {
        _match_key(row["org_name"], row["position"], row["citation_url"])
        for row in existing_rows
        if row["status"] == "complete" and row["verification_verdict"] in _SETTLED_VERDICTS
    }


async def verify_org_citations(
    items: list[dict],
    *,
    source: str,
    dry_run: bool,
    limit: int | None = None,
    run_id: str | None = None,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Verify and store each item's citation, skipping what the broker already settled.

    items: dicts with bill_openstates_id, jurisdiction, session_code, gov_id, org_name, position
        ("support"/"oppose") and citation_url.
    source: recorded as the row's find_model_name, so a stored row says where its citation came
        from (e.g. "slack-legislation-zapier") rather than naming a LegBot model that never ran.
    dry_run: resolve each bill's version and existing rows and count what a real run would
        dispatch, but call neither LegBot nor the write endpoint.
    limit: dispatch at most this many items (skipped items do not count), so a pilot stays small.

    Returns a summary: counts per outcome plus the per-item list. Outcomes are the ones
    verify_and_store_position reports ("written", "verification_failed", "broker_write_failed",
    "retryable") plus "already_settled", "no_current_version", "broker_read_failed", "dry_run"
    and "not_attempted".
    """
    run_id = run_id or f"org-citation-verify-{uuid.uuid4().hex[:12]}"
    invocation_id = str(uuid.uuid4())

    by_bill: dict[str, list[dict]] = {}
    for item in items:
        by_bill.setdefault(item["bill_openstates_id"], []).append(item)

    results: list[dict] = []
    dispatched = 0
    consecutive_retryable = 0
    stopped_early = False

    for bill_openstates_id, bill_items in by_bill.items():
        if stopped_early or (limit is not None and dispatched >= limit):
            results.extend(_outcome(item, "not_attempted") for item in bill_items)
            continue

        try:
            existing = await get_bill_organization_positions_existing(
                bill_openstates_id=bill_openstates_id,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
            )
        except BrokerClientError as exc:
            logger.warning("org_citation_verify_existing_read_failed", run_id=run_id,
                           bill_openstates_id=bill_openstates_id, error=str(exc))
            results.extend(_outcome(item, "broker_read_failed") for item in bill_items)
            continue

        version = await get_current_version_identity(bill_openstates_id)
        if version is None:
            results.extend(_outcome(item, "no_current_version") for item in bill_items)
            continue

        settled = _settled_keys(existing)
        for item in bill_items:
            if _match_key(item["org_name"], item["position"], item["citation_url"]) in settled:
                results.append(_outcome(item, "already_settled"))
                continue
            if stopped_early or (limit is not None and dispatched >= limit):
                results.append(_outcome(item, "not_attempted"))
                continue
            if dry_run:
                dispatched += 1
                results.append(_outcome(item, "dry_run"))
                continue

            dispatched += 1
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

            consecutive_retryable = consecutive_retryable + 1 if result["outcome"] == "retryable" else 0
            if consecutive_retryable >= MAX_CONSECUTIVE_RETRYABLE:
                stopped_early = True
                logger.warning("org_citation_verify_stopped_rate_limited", run_id=run_id,
                               consecutive_retryable=consecutive_retryable)

    counts: dict[str, int] = {}
    for result in results:
        counts[result["outcome"]] = counts.get(result["outcome"], 0) + 1
    summary = {
        "run_id": run_id,
        "source": source,
        "dry_run": dry_run,
        "stopped_early": stopped_early,
        "counts": counts,
        "results": results,
    }
    logger.info("org_citation_verify_summary", run_id=run_id, dry_run=dry_run,
                stopped_early=stopped_early, counts=counts)
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
