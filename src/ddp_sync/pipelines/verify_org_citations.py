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

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import structlog

from ddp_sync.pipelines.bill_organization_position_research import verify_and_store_position
from ddp_sync.services.local_openstates_client import get_current_version_identity
from ddp_sync.services.redis_store import get_redis_store

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
    progress: Callable[[int, dict[str, int]], Awaitable[None]] | None = None,
) -> dict:
    """Verify each item's citation and store the result, one item at a time.

    items: dicts with bill_openstates_id, jurisdiction, session_code, gov_id, org_name, position
        ("support"/"oppose") and citation_url. Each is checked against the bill's current version,
        the one the public read shows.
    source: recorded as the row's find_model_name when a row is created, so it says where the
        citation came from (e.g. "slack-legislation-zapier", "user-form") rather than naming a
        LegBot model that never ran. An existing row keeps the source it was first written with.
    progress: awaited after each item with (items handled so far, outcome counts so far), for a
        background run to record how far it has got.

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
    counts: dict[str, int] = {}

    async def _record(result: dict) -> None:
        results.append(result)
        counts[result["outcome"]] = counts.get(result["outcome"], 0) + 1
        if progress is not None:
            await progress(len(results), counts)

    for item in items:
        key = _match_key(item)
        if key in seen:
            await _record(_outcome(item, "duplicate_in_request"))
            continue
        seen.add(key)

        bill_openstates_id = item["bill_openstates_id"]
        if bill_openstates_id not in versions:
            versions[bill_openstates_id] = await get_current_version_identity(bill_openstates_id)
        version = versions[bill_openstates_id]
        if version is None:
            await _record(_outcome(item, "no_current_version"))
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
        await _record(
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


# One background run at a time per process: a second one waits its turn instead of starting beside it.
# verify_bill_position fetches pages through Claude, and the AGENTS-74 corpus found concurrent fetches
# get rate limited (see the module docstring), so two long runs would slow and blur each other.
_background_run_lock = asyncio.Lock()


def new_run_record(run_id: str, *, source: str, environment: str | None, total: int) -> dict:
    """The progress record for a background run that has been accepted and is waiting to start."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "run_id": run_id,
        "status": "queued",
        "source": source,
        "environment": environment,
        "total": total,
        "processed": 0,
        "counts": {},
        "started_at": None,
        "updated_at": now,
        "finished_at": None,
        "error": None,
    }


async def run_verify_org_citations_in_background(
    items: list[dict],
    *,
    source: str,
    run_id: str,
    environment: str | None,
    broker_api_base: str | None,
    broker_api_token: str | None,
) -> dict | None:
    """Run verify_org_citations for a 202 caller, recording progress in Redis after every item.

    Returns verify_org_citations' result (its per-item outcomes), or None when the run failed; the
    nightly Slack pull (SYNC-127) uses it to know which messages it has finished with.

    The record says how many items have been handled in order (`processed`), so a run that was
    interrupted (ddp-sync restarted) can be resumed by resending the list from that index: the
    broker's upsert makes a repeated item an update, not a duplicate. A record whose `updated_at`
    stops moving while status is "running" is a run that died with the process; nothing restarts it.
    """
    store = get_redis_store()
    record = new_run_record(run_id, source=source, environment=environment, total=len(items))

    async def _save() -> None:
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        await store.set_verify_run(run_id, record)

    async def _progress(processed: int, counts: dict[str, int]) -> None:
        record["processed"] = processed
        record["counts"] = dict(counts)
        await _save()

    await _save()
    result = None
    async with _background_run_lock:
        record["status"] = "running"
        record["started_at"] = datetime.now(timezone.utc).isoformat()
        await _save()
        try:
            result = await verify_org_citations(
                items,
                source=source,
                run_id=run_id,
                broker_api_base=broker_api_base,
                broker_api_token=broker_api_token,
                progress=_progress,
            )
        except Exception as exc:
            logger.exception("org_citation_verify_run_failed", run_id=run_id)
            record["status"] = "failed"
            record["error"] = f"{type(exc).__name__}: {exc}"[:300]
        else:
            record["status"] = "finished"
        record["finished_at"] = datetime.now(timezone.utc).isoformat()
        await _save()
    return result
