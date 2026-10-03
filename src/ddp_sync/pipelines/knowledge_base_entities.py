"""SYNC-91: embed organizations into the NEW `ddp-knowledge-base` index.

PLAN-enterprise-search.md 5.6. One document per organization, written through the SAME
`KnowledgeBaseEmbedder` the bill hook uses (`embed_entity`: content-hash cache, surplus-chunk
cleanup, the refusal to ever target `votebot-large`), so nothing here talks to Pinecone directly and
the legacy write path is untouched.

* `organization:{broker_org_id}`: read from ddp-broker-py's public `/api/organizations/` (BROKER-144),
  never from Webflow. Organizations have no ocd id. An organization with no description and no
  policy focus areas has nothing to search on but its name, so it is skipped, not embedded.

Legislators are deliberately NOT embedded (SYNC-94, Ramon 2026-10-02): their structured facts (party,
chamber, district, contact details, links) come from api-v3 directly, and DDP has no narrative
biography to embed. Revisit only if one is ever produced.

Organizations are not per jurisdiction and have no archive, so they run from the manual trigger
`POST /trigger/knowledge-base-entities/organizations`. The run is idempotent: an unchanged entity
costs one cache read and writes nothing. Failures are logged, never raised; a run that left anything
undone says so (`knowledge_base_entities_incomplete`) and the next run repairs it. Do NOT embed LegBot
output here.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog

from ddp_sync.config import SyncSettings
from ddp_sync.ingestion.metadata import DocumentMetadata
from ddp_sync.pipelines.knowledge_base_embedding import KnowledgeBaseEmbedder
from ddp_sync.services import broker_client
from ddp_sync.services.broker_client import BrokerClientError

logger = structlog.get_logger()

DOCUMENT_TYPE_ORGANIZATION = "organization"
ORG_PAGE_SIZE = 200  # the broker's StandardPagination maximum


def organization_key(organization_id: int | str) -> str:
    return f"organization:{organization_id}"


def organization_document(org: dict) -> tuple[str, str, DocumentMetadata] | None:
    """`(key, content, metadata)` for one broker organization detail, or None when it has no
    description and no policy focus areas (nothing to search on but the name)."""
    org_id = org.get("id")
    name = org.get("name") or ""
    description = (org.get("description") or "").strip()
    focus = (org.get("policy_focus_areas") or "").strip()
    if org_id is None or not name or not (description or focus):
        return None

    parts = [f"# {name}"]
    if org.get("org_type"):
        parts.append(f"**Type:** {org['org_type']}")
    if org.get("website"):
        parts.append(f"**Website:** {org['website']}")
    aka = org.get("also_known_as")
    if aka:
        parts.append("**Also known as:** " + (", ".join(aka) if isinstance(aka, list) else str(aka)))
    if description:
        parts.append("## About\n" + description)
    if focus:
        parts.append("## Policy focus areas\n" + focus)
    for label, field in (("Funding", "funding"), ("Affiliates", "affiliates")):
        if (org.get(field) or "").strip():
            parts.append(f"## {label}\n{org[field].strip()}")
    parent = org.get("parent")
    if isinstance(parent, dict) and parent.get("name"):
        parts.append(f"**Parent organization:** {parent['name']}")
    chapters = [c.get("name") for c in (org.get("chapters") or []) if isinstance(c, dict) and c.get("name")]
    if chapters:
        parts.append("**Chapters:** " + ", ".join(chapters))

    key = organization_key(org_id)
    metadata = DocumentMetadata(
        document_id=key,
        document_type=DOCUMENT_TYPE_ORGANIZATION,
        source="ddp-broker",
        title=name,
        url=org.get("website") or None,
        extra={
            "broker_org_id": str(org_id),
            "slug": org.get("slug"),
            "organization_type": org.get("org_type") or "",
            "website": org.get("website") or "",
            "ddp_url": org.get("url") or "",
        },
    )
    return key, "\n\n".join(parts), metadata


def _totals(entity: str, **extra: Any) -> dict[str, Any]:
    return {"entity": entity, "listed": 0, "written": 0, "unchanged": 0, "would_write": 0,
            "skipped_no_content": 0, "failed": 0, "complete": False, **extra}


def _record(totals: dict[str, Any], outcome: str, key: str) -> None:
    if outcome.startswith("undone:"):
        totals["failed"] += 1
        logger.warning("knowledge_base_entity_undone", entity=totals["entity"], key=key,
                       reason=outcome.removeprefix("undone:"))
    else:
        totals[outcome] += 1


def _finish(totals: dict[str, Any], complete: bool) -> dict[str, Any]:
    totals["complete"] = complete and totals["failed"] == 0
    (logger.info if totals["complete"] else logger.warning)(
        "knowledge_base_entities_run" if totals["complete"] else "knowledge_base_entities_incomplete",
        **totals,
    )
    return totals


async def embed_organizations(
    *,
    settings: SyncSettings,
    embedder: KnowledgeBaseEmbedder | None = None,
    dry_run: bool = False,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict[str, Any]:
    """Embed every public organization ddp-broker-py lists. A dry run does everything but write:
    it reads each organization's detail (the descriptive text is only there) so the counts of
    would-write, unchanged and skipped are real, which costs one broker read per organization.
    Never raises: a broker that predates BROKER-144 answers 404 and the run reports incomplete."""
    totals = _totals("organizations")
    ids: list[Any] = []
    complete = True
    page = 1
    try:
        while True:
            body = await broker_client.list_organizations(
                page=page, page_size=ORG_PAGE_SIZE,
                broker_api_base=broker_api_base, broker_api_token=broker_api_token,
            )
            results = body.get("results")
            if not isinstance(results, list):
                raise BrokerClientError("organization list has no results")
            ids.extend(o["id"] for o in results if isinstance(o, dict) and o.get("id") is not None)
            if not body.get("next") or not results:
                break
            page += 1
    except BrokerClientError as e:
        complete = False
        logger.warning("knowledge_base_entities_list_failed", entity="organizations", error=str(e))
    totals["listed"] = len(ids)

    embedder = embedder or KnowledgeBaseEmbedder(settings)
    for org_id in ids:
        key = organization_key(org_id)
        try:
            org = await broker_client.get_organization(
                org_id, broker_api_base=broker_api_base, broker_api_token=broker_api_token
            )
            document = organization_document(org)
            if document is None:
                totals["skipped_no_content"] += 1
                continue
            key, content, metadata = document
            outcome = await embedder.embed_entity(key, content, metadata, dry_run=dry_run)
        except Exception as e:  # noqa: BLE001 -- a failed read or a malformed record must not stop the run
            outcome = f"undone:{e}"
        _record(totals, outcome, key)
    return _finish(totals, complete)


async def run_knowledge_base_entities(
    *,
    settings: SyncSettings,
    dry_run: bool = True,
    run_id: str | None = None,
) -> dict[str, Any]:
    """The manual trigger's body: embed the organizations ddp-broker-py lists. There is no entity
    argument any more (SYNC-94 left organizations as the only kind), so a stale caller cannot ask
    for another kind and silently get organizations."""
    run_id = run_id or f"kb-entities-organizations-{uuid.uuid4().hex[:12]}"
    logger.info("knowledge_base_entities_start", run_id=run_id, entity="organizations", dry_run=dry_run)
    runs = [await embed_organizations(settings=settings, dry_run=dry_run)]
    ok = all(r["complete"] for r in runs)
    (logger.info if ok else logger.warning)(
        "knowledge_base_entities_done", run_id=run_id, entity="organizations", dry_run=dry_run, complete=ok
    )
    return {"run_id": run_id, "complete": ok, "runs": runs}
