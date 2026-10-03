"""SYNC-91: organizations in the new knowledge-base index (legislators are not embedded, SYNC-94).

Mocked api-v3, broker, Redis and Pinecone throughout; nothing here can reach a real index. The
embedding is the real `KnowledgeBaseEmbedder` (so these documents go through the same code and
cache as the bills) with a recording pipeline. Covers the ticket's list: canonical ids, the broker
(not Webflow) as the organization source, the legacy index never targeted, and a re-run writing
nothing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import SyncSettings
from ddp_sync.pipelines import knowledge_base_embedding as kb
from ddp_sync.pipelines import knowledge_base_entities as ent
from ddp_sync.pipelines.openstates_archive import _maybe_embed_knowledge_base
from ddp_sync.services import broker_client
from ddp_sync.services.broker_client import BrokerClientError
from tests.test_knowledge_base_embedding import (
    FakePipeline,
    FakeRedis,
    _settings,
)

pytestmark = pytest.mark.asyncio

def _org(oid, name="Florida Education Association", description="A union of educators.",
         focus="Public education funding", **extra):
    return {"id": oid, "name": name, "slug": name.lower().replace(" ", "-"), "org_type": "Union",
            "website": "https://fea.example", "url": f"/organizations/{oid}", "description": description,
            "policy_focus_areas": focus, "funding": "", "affiliates": "", "email": "", "contact_page_url": "",
            "also_known_as": ["FEA"], "parent": None, "chapters": [], **extra}


def _embedder(redis=None, pipeline=None):
    redis, pipeline = redis or FakeRedis(), pipeline or FakePipeline()
    return kb.KnowledgeBaseEmbedder(_settings(), pipeline=pipeline, redis_store=redis), redis, pipeline


def _events(logs):
    return [e["event"] for e in logs]


# --- organization documents -------------------------------------------------------------------------------


async def test_the_key_helper():
    assert ent.organization_key(42) == "organization:42"


async def test_organization_document_key_metadata_and_content():
    key, content, meta = ent.organization_document(_org(
        7, funding="Member dues.", affiliates="NEA",
        parent={"id": 1, "name": "National Education Association"},
        chapters=[{"id": 2, "name": "FEA Miami"}],
    ))
    assert key == "organization:7" == meta.document_id  # broker id, not organization-<webflow id>
    assert meta.document_type == "organization" and meta.source == "ddp-broker" and meta.title == "Florida Education Association"
    assert meta.extra["broker_org_id"] == "7" and meta.extra["organization_type"] == "Union"
    assert meta.extra["ddp_url"] == "/organizations/7"
    assert "webflow_id" not in meta.extra
    for expected in ("A union of educators.", "Public education funding", "FEA", "Member dues.", "NEA",
                     "National Education Association", "FEA Miami"):
        assert expected in content


async def test_an_organization_with_nothing_to_search_on_but_its_name_is_skipped():
    assert ent.organization_document(_org(8, description="", focus="")) is None
    assert ent.organization_document(_org(8, description="  ", focus=None)) is None
    assert ent.organization_document({"id": None, "name": "x", "description": "d"}) is None
    assert ent.organization_document(_org(8, description="", focus="Clean water")) is not None


# --- embedding organizations ------------------------------------------------------------------------------


def _patch_broker(pages, details):
    """pages: list of list-page dicts in order; details: {id: org dict | Exception}."""
    async def detail(oid, **_):
        value = details[oid]
        if isinstance(value, Exception):
            raise value
        return value

    return (
        patch.object(ent.broker_client, "list_organizations", new=AsyncMock(side_effect=pages)),
        patch.object(ent.broker_client, "get_organization", new=AsyncMock(side_effect=detail)),
    )


def _page(ids, more=False):
    return {"count": len(ids), "next": "http://broker/next" if more else None, "previous": None,
            "results": [{"id": i, "name": f"Org {i}"} for i in ids]}


async def test_embeds_every_broker_organization_across_pages_and_a_rerun_writes_nothing():
    emb, _, pipe = _embedder()
    details = {1: _org(1, "Org 1"), 2: _org(2, "Org 2"), 3: _org(3, "Org 3")}
    lst, det = _patch_broker([_page([1, 2], more=True), _page([3])], details)
    with lst as listing, det:
        first = await ent.embed_organizations(settings=_settings(), embedder=emb)
        written = list(pipe.keys)
    assert sorted(written) == ["organization:1", "organization:2", "organization:3"]
    assert (first["listed"], first["written"], first["complete"]) == (3, 3, True)
    assert [c.kwargs["page"] for c in listing.await_args_list] == [1, 2]
    assert listing.await_args_list[0].kwargs["page_size"] == 200

    lst, det = _patch_broker([_page([1, 2], more=True), _page([3])], details)
    with lst, det:
        second = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert pipe.keys == written and (second["written"], second["unchanged"]) == (0, 3)


async def test_contentless_organizations_are_skipped_and_counted():
    emb, _, pipe = _embedder()
    details = {1: _org(1), 2: _org(2, description="", focus="")}
    lst, det = _patch_broker([_page([1, 2])], details)
    with lst, det:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert pipe.keys == ["organization:1"] and totals["skipped_no_content"] == 1 and totals["complete"] is True


async def test_a_broker_without_the_organization_api_is_incomplete_and_writes_nothing():
    """A broker that predates BROKER-144 answers 404 on the list."""
    emb, _, pipe = _embedder()
    lst = patch.object(ent.broker_client, "list_organizations",
                       new=AsyncMock(side_effect=BrokerClientError("rejected the organization read (404)")))
    with lst, capture_logs() as logs:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert totals["listed"] == 0 and totals["complete"] is False and pipe.keys == []
    assert "knowledge_base_entities_list_failed" in _events(logs)
    assert "knowledge_base_entities_incomplete" in _events(logs)


async def test_a_detail_read_failure_is_counted_and_the_rest_continue():
    emb, _, pipe = _embedder()
    details = {1: BrokerClientError("404 not public yet"), 2: _org(2)}
    lst, det = _patch_broker([_page([1, 2])], details)
    with lst, det, capture_logs() as logs:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert pipe.keys == ["organization:2"] and (totals["failed"], totals["written"], totals["complete"]) == (1, 1, False)
    assert "knowledge_base_entity_undone" in _events(logs)


async def test_an_organization_dry_run_reads_details_for_real_counts_but_writes_nothing():
    emb, redis, pipe = _embedder()
    details = {1: _org(1), 2: _org(2, description="", focus=""), 3: _org(3)}
    lst, det = _patch_broker([_page([1, 2, 3])], details)
    with lst, det as detail:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb, dry_run=True)
    assert (totals["listed"], totals["would_write"], totals["skipped_no_content"], totals["written"]) == (3, 2, 1, 0)
    assert totals["complete"] is True and detail.await_count == 3
    assert pipe.keys == [] and redis.versions == {}


async def test_a_partial_organization_listing_still_embeds_the_pages_it_got_but_is_incomplete():
    emb, _, pipe = _embedder()
    pages = [_page([1], more=True), BrokerClientError("page 2 failed")]
    lst, det = _patch_broker(pages, {1: _org(1)})
    with lst, det, capture_logs() as logs:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert pipe.keys == ["organization:1"] and totals["complete"] is False
    assert "knowledge_base_entities_list_failed" in _events(logs)


# --- the hook ---------------------------------------------------------------------------------------------


_CFG = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"]}}


def _hook_settings():
    return SyncSettings(knowledge_base_index_name="ddp-knowledge-base", cams_api_token="t",
                        local_openstates_api_base="http://local", local_openstates_api_key="lk")


async def test_the_post_archive_hook_embeds_bills_only_and_no_legislators():
    """SYNC-94: legislator profiles are not embedded, so the hook has nothing to run after its bills."""
    bills = AsyncMock()
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=_hook_settings()), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=bills):
        await _maybe_embed_knowledge_base("fl", MagicMock(), _CFG)
    bills.assert_awaited_once()
    assert not hasattr(ent, "embed_legislators")  # nothing exists that could embed them


# --- review additions: metadata in the digest, per-entity isolation, cache and chunk edge cases ----------


async def test_a_metadata_only_change_rewrites_once_and_is_then_unchanged():
    emb, _, pipe = _embedder()
    lst, det = _patch_broker([_page([7])], {7: _org(7)})
    with lst, det:
        await ent.embed_organizations(settings=_settings(), embedder=emb)
    renamed_slug = _org(7, url="/organizations/7-new")  # same text, different ddp_url metadata
    lst, det = _patch_broker([_page([7])], {7: renamed_slug})
    with lst, det:
        changed = await ent.embed_organizations(settings=_settings(), embedder=emb)
    lst, det = _patch_broker([_page([7])], {7: renamed_slug})
    with lst, det:
        settled = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert pipe.keys == ["organization:7", "organization:7"]  # initial write, then exactly one rewrite
    assert (changed["written"], settled["written"], settled["unchanged"]) == (1, 0, 1)


async def test_a_malformed_organization_is_counted_failed_and_the_rest_continue():
    emb, _, pipe = _embedder()
    broken = _org(1, chapters=["not-a-dict-is-fine"], parent="also-fine")  # tolerated, not a crash
    lst, det = _patch_broker([_page([1, 2])], {1: broken, 2: None})  # None: organization_document(None) raises
    with lst, det:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert pipe.keys == ["organization:1"] and (totals["failed"], totals["written"]) == (1, 1)


async def test_a_failed_write_is_counted_warned_and_retried_next_run():
    pipe = FakePipeline()
    pipe.fail_keys = {"organization:1"}
    emb, redis, _ = _embedder(pipeline=pipe)
    lst, det = _patch_broker([_page([1, 2])], {1: _org(1), 2: _org(2)})
    with lst, det, capture_logs() as logs:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert (totals["written"], totals["failed"], totals["complete"]) == (1, 1, False)
    assert "knowledge_base_entity_undone" in _events(logs)
    assert await redis.get_bill_version("organization:1") is None  # cache not advanced past a failure

    pipe.fail_keys = set()
    lst, det = _patch_broker([_page([1, 2])], {1: _org(1), 2: _org(2)})
    with lst, det:
        again = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert (again["written"], again["unchanged"], again["complete"]) == (1, 1, True)


async def test_a_cache_write_failure_after_a_good_ingest_is_undone_and_rewritten_next_time():
    class _NoWrite(FakeRedis):
        async def set_bill_version(self, key, data):
            return False

    emb, _, pipe = _embedder(redis=_NoWrite())
    lst, det = _patch_broker([_page([1])], {1: _org(1)})
    with lst, det:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb)
    lst, det = _patch_broker([_page([1])], {1: _org(1)})
    with lst, det:
        again = await ent.embed_organizations(settings=_settings(), embedder=emb)
    assert (totals["failed"], again["failed"]) == (1, 1) and len(pipe.keys) == 2  # the documented caveat


async def test_a_shrinking_entity_deletes_its_surplus_chunks():
    emb, redis, _ = _embedder()
    long_org = _org(5, description="x" * 700)  # chunks in the fake: one per 100 characters
    lst, det = _patch_broker([_page([5])], {5: long_org})
    with lst, det:
        await ent.embed_organizations(settings=_settings(), embedder=emb)
    first_chunks = redis.versions["organization:5"]["chunks"]
    vs = MagicMock()
    vs.delete = AsyncMock()
    lst, det = _patch_broker([_page([5])], {5: _org(5, description="x" * 200)})
    with lst, det, patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        await ent.embed_organizations(settings=_settings(), embedder=emb)
    vs.delete.assert_awaited_once()
    deleted = vs.delete.await_args.kwargs["ids"]
    assert deleted[0].startswith("organization:5-chunk-") and len(deleted) == first_chunks - redis.versions["organization:5"]["chunks"]


class _Settings:
    ddp_broker_api_base = "http://broker"
    ddp_broker_api_token = "tok"


def _patch_httpx(response=None, exc=None):
    client = AsyncMock()
    client.get = AsyncMock(return_value=response, side_effect=exc)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.broker_client.httpx.AsyncClient", return_value=cm), client


def _resp(status=200, body=None, text=""):
    r = MagicMock()
    r.status_code, r.text = status, text
    r.json.return_value = body
    return r


async def test_list_organizations_reads_a_page_with_the_bearer_token():
    patcher, client = _patch_httpx(_resp(body={"results": [], "next": None}))
    with patch("ddp_sync.services.broker_client.get_settings", return_value=_Settings()), patcher:
        body = await broker_client.list_organizations(page=3, page_size=200)
    assert body == {"results": [], "next": None}
    call = client.get.await_args
    assert call.args[0] == "http://broker/api/organizations/"
    assert call.kwargs["params"] == {"page": 3, "page_size": 200}
    assert call.kwargs["headers"]["Authorization"] == "Bearer tok"


async def test_get_organization_reads_the_detail_route():
    patcher, client = _patch_httpx(_resp(body={"id": 7, "name": "X"}))
    with patch("ddp_sync.services.broker_client.get_settings", return_value=_Settings()), patcher:
        assert (await broker_client.get_organization(7))["id"] == 7
    assert client.get.await_args.args[0] == "http://broker/api/organizations/7/"
    assert client.get.await_args.kwargs["params"] is None


@pytest.mark.parametrize("response, exc, match", [
    (_resp(404, text="Not Found"), None, "404"),
    (_resp(200, body=["not", "a", "dict"]), None, "unexpected"),
    (None, httpx.ConnectError("refused"), "unreachable"),
])
async def test_organization_reads_raise_brokerclienterror_not_raw_errors(response, exc, match):
    patcher, _ = _patch_httpx(response, exc)
    with patch("ddp_sync.services.broker_client.get_settings", return_value=_Settings()), patcher, \
         pytest.raises(BrokerClientError, match=match):
        await broker_client.list_organizations()


async def test_organization_reads_need_a_configured_broker_base():
    class _None(_Settings):
        ddp_broker_api_base = ""

    with patch("ddp_sync.services.broker_client.get_settings", return_value=_None()), \
         pytest.raises(BrokerClientError, match="DDP_BROKER_API_BASE"):
        await broker_client.get_organization(1)


async def test_non_json_organization_body_is_a_brokerclienterror():
    bad = MagicMock()
    bad.status_code = 200
    bad.json.side_effect = ValueError("no json")
    patcher, _ = _patch_httpx(bad)
    with patch("ddp_sync.services.broker_client.get_settings", return_value=_Settings()), patcher, \
         pytest.raises(BrokerClientError, match="non-JSON"):
        await broker_client.list_organizations()


# --- the trigger route -------------------------------------------------------------------------------------


def _client():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return TestClient(app)


def _route(path, *, settings=None):
    settings = settings or SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", ddp_broker_api_base="http://broker",
    )
    run = AsyncMock()
    with patch("ddp_sync.config.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.knowledge_base_entities.run_knowledge_base_entities", new=run):
        resp = _client().post(path)
    return resp, run


async def test_route_defaults_to_a_dry_run_of_organizations():
    resp, run = _route("/trigger/knowledge-base-entities/organizations")
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["entity"] == "organizations" and body["dry_run"] is True
    assert body["run_id"].startswith("kb-entities-organizations-dry-")
    args, kwargs = run.await_args
    assert args == () and kwargs["dry_run"] is True and kwargs["run_id"] == body["run_id"]


async def test_route_runs_organizations_for_real_when_asked():
    resp, run = _route("/trigger/knowledge-base-entities/organizations?dry_run=false")
    assert resp.status_code == 202 and run.await_args.args == ()
    assert run.await_args.kwargs["dry_run"] is False


async def test_route_still_accepts_the_removed_jurisdiction_parameter_and_runs_organizations_only():
    """A stale caller that still sends `jurisdiction` gets the same organization job, not an error."""
    resp, run = _route("/trigger/knowledge-base-entities/organizations?jurisdiction=fl&dry_run=false")
    assert resp.status_code == 202 and set(run.await_args.kwargs) == {"settings", "dry_run", "run_id"}


async def test_route_rejects_an_unknown_entity_and_legislators_are_no_longer_one():
    for entity in ("policies", "legislators"):  # SYNC-94: legislator profiles are not embedded
        # Settings with no index and no broker: the entity is rejected first, so it is a 404, not a 503.
        resp, run = _route(f"/trigger/knowledge-base-entities/{entity}", settings=SyncSettings())
        assert resp.status_code == 404 and "Unknown entity" in resp.text
        run.assert_not_awaited()


async def test_route_refuses_an_unset_or_legacy_index_and_a_missing_broker_base():
    resp, run = _route("/trigger/knowledge-base-entities/organizations", settings=SyncSettings())
    assert resp.status_code == 503
    resp, run = _route("/trigger/knowledge-base-entities/organizations",
                       settings=SyncSettings(knowledge_base_index_name="votebot-large", ddp_broker_api_base="http://broker"))
    assert resp.status_code == 503 and "legacy index" in resp.text
    resp, run = _route("/trigger/knowledge-base-entities/organizations", settings=SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", ddp_broker_api_base=""))
    assert resp.status_code == 503 and "DDP_BROKER_API_BASE" in resp.text
    run.assert_not_awaited()


async def test_the_runner_reports_completeness():
    for complete in (True, False):
        orgs = AsyncMock(return_value={"complete": complete})
        with patch.object(ent, "embed_organizations", new=orgs):
            out = await ent.run_knowledge_base_entities(settings=_settings(), dry_run=True)
        assert out["complete"] is complete and orgs.await_args.kwargs["dry_run"] is True
