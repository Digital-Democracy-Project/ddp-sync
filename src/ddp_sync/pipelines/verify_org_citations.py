"""Verify organization-position citations that came from somewhere other than find_bill_positions
and store the result (SYNC-100).

Given a citation -- an organization, the stance it is said to hold on a bill, and a page that is
said to show it -- this runs verify_bill_position against the page and writes the result to the
bill's records through verify_and_store_position, the same code find_bill_positions research uses
after it finds an organization. The broker's write is an upsert on (bill version, organization,
position, citation_url), so a citation that is already on the bill is updated, never duplicated.

Three callers are expected:
  - a manual batch (the Slack #legislation citations): a failed run is rerun with only the items
    it did not process, which the per-item results say;
  - a single citation a visitor flagged on the site, re-checked so a confirmed row can be demoted
    to not_confirmed and stop being shown;
  - a single citation a user or staff member submitted on a form, checked before it is added.

Serial on purpose. verify_bill_position is a metered Claude call that fetches the page itself, and
ddp-agents' AGENTS-74 citation corpus (bench/agents74-citation-corpus/README.md) measured four
concurrent fetches reporting nine unreadable citations that a serial rerun cut to one: the harness
was being rate limited and blaming the sites.

Cost of the upsert approach: nothing here asks the broker whether a citation was already checked, so
sending one that is already settled pays for the verification again. Send only what needs checking.
"""

from __future__ import annotations

import uuid

import structlog

from ddp_sync.pipelines.bill_organization_position_research import verify_and_store_position
from ddp_sync.services.local_openstates_client import get_current_version_identity

logger = structlog.get_logger()


def _match_key(item: dict) -> tuple[str, str, str, str]:
    """Same citation as far as the broker's upsert is concerned (org name case-insensitive)."""
    return (
        item["bill_openstates_id"],
        item["org_name"].strip().lower(),
        item["position"],
        item["citation_url"].strip(),
    )


async def verify_org_citations(
    items: list[dict],
    *,
    source: str,
    run_id: str | None = None,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Verify each item's citation and store the result, one item at a time.

    items: dicts with bill_openstates_id, jurisdiction, session_code, gov_id, org_name, position
        ("support"/"oppose") and citation_url. Each is checked against the bill's current version,
        the one the public read shows.
    source: recorded as the row's find_model_name when a row is created, so it says where the
        citation came from (e.g. "slack-legislation-zapier", "user-form") rather than naming a
        LegBot model that never ran. An existing row keeps the source it was first written with.

    Returns {"run_id", "source", "counts", "results"}. Each result carries the item and its
    "outcome": "written" (see its `result`: "created", "updated", or "unchanged" when the answer was
    inconclusive and an existing settled row was kept), "verification_failed" (a failed row was
    stored), "broker_write_failed", "retryable" (rate limited; nothing was stored, try again later),
    "no_current_version" (nothing was dispatched) or "duplicate_in_request" (the same citation
    earlier in this request; verified once). Two verdicts are reported, because they differ when an
    inconclusive re-check is refused permission to overwrite a settled row: `attempted_verdict` is
    what this check concluded, `verification_verdict` is the stored row's verdict as it now stands.
    """
    run_id = run_id or f"org-citation-verify-{uuid.uuid4().hex[:12]}"
    invocation_id = str(uuid.uuid4())

    versions: dict[str, dict | None] = {}
    seen: set[tuple[str, str, str, str]] = set()
    results: list[dict] = []

    for item in items:
        key = _match_key(item)
        if key in seen:
            results.append(_outcome(item, "duplicate_in_request"))
            continue
        seen.add(key)

        bill_openstates_id = item["bill_openstates_id"]
        if bill_openstates_id not in versions:
            versions[bill_openstates_id] = await get_current_version_identity(bill_openstates_id)
        version = versions[bill_openstates_id]
        if version is None:
            results.append(_outcome(item, "no_current_version"))
            continue

        stored = await verify_and_store_position(
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
        results.append(
            _outcome(
                item,
                stored["outcome"],
                position_id=stored["position_id"],
                result=stored["result"],
                verification_verdict=stored["verification_verdict"],
                attempted_verdict=stored["attempted_verdict"],
            )
        )
        logger.info("org_citation_verify_item", run_id=run_id, bill_openstates_id=bill_openstates_id,
                    org_name=item["org_name"], outcome=stored["outcome"], result=stored["result"],
                    verification_verdict=stored["verification_verdict"],
                    attempted_verdict=stored["attempted_verdict"])

    counts: dict[str, int] = {}
    for result in results:
        counts[result["outcome"]] = counts.get(result["outcome"], 0) + 1
    logger.info("org_citation_verify_summary", run_id=run_id, counts=counts)
    return {"run_id": run_id, "source": source, "counts": counts, "results": results}


def _outcome(
    item: dict,
    outcome: str,
    *,
    position_id: int | None = None,
    result: str | None = None,
    verification_verdict: str | None = None,
    attempted_verdict: str | None = None,
) -> dict:
    return {
        "bill_openstates_id": item["bill_openstates_id"],
        "org_name": item["org_name"],
        "position": item["position"],
        "citation_url": item["citation_url"],
        "outcome": outcome,
        "result": result,
        "verification_verdict": verification_verdict,
        "attempted_verdict": attempted_verdict,
        "position_id": position_id,
    }
