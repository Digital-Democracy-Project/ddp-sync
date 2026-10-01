"""SYNC-91: legislators and organizations in the new knowledge-base index.

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
from ddp_sync.services import local_openstates_client as osc
from ddp_sync.services.broker_client import BrokerClientError
from tests.test_knowledge_base_embedding import (
    FakePipeline,
    FakeRedis,
    _settings,
    _votes_formatter,
)

pytestmark = pytest.mark.asyncio

U1, U2 = "11111111-aaaa-bbbb-cccc-000000000001", "22222222-aaaa-bbbb-cccc-000000000002"


def _person(uid, name, chamber="upper", district="5", party="Republican", email=None):
    return {
        "id": f"ocd-person/{uid}", "name": name, "party": party, "email": email,
        "current_role": {"title": "Senator", "org_classification": chamber, "district": district},
        "offices": [{"classification": "capitol", "address": "400 S Monroe", "voice": "850-555-0100"}],
        "links": [{"note": "Website", "url": "https://example.test/" + uid[:4]}],
    }


def _org(oid, name="Florida Education Association", description="A union of educators.",
         focus="Public education funding", **extra):
    return {"id": oid, "name": name, "slug": name.lower().replace(" ", "-"), "org_type": "Union",
            "website": "https://fea.example", "url": f"/organizations/{oid}", "description": description,
            "policy_focus_areas": focus, "funding": "", "affiliates": "", "email": "", "contact_page_url": "",
            "also_known_as": ["FEA"], "parent": None, "chapters": [], **extra}


def _embedder(redis=None, pipeline=None):
    redis, pipeline = redis or FakeRedis(), pipeline or FakePipeline()
    return kb.KnowledgeBaseEmbedder(_settings(), pipeline=pipeline, votes_formatter=_votes_formatter,
                                    redis_store=redis), redis, pipeline


def _events(logs):
    return [e["event"] for e in logs]


def _patch_people(result):
    return patch.object(ent.local_openstates_client, "list_people", new=AsyncMock(return_value=result))


# --- legislator documents ---------------------------------------------------------------------------


async def test_legislator_document_uses_the_bare_uuid_and_the_legacy_metadata_shape():
    from ddp_sync.ingestion.sources.openstates import OpenStatesSource

    key, content, meta = ent.legislator_document(_person(U1, "Jane Roe"), "fl", OpenStatesSource(_settings()))
    assert key == f"legislator-{U1}" == meta.document_id  # not legislator-ocd-person/<uuid>
    assert meta.document_type == "legislator" and meta.legislator_id == U1
    assert meta.jurisdiction == "FL" and meta.title == "Jane Roe"
    assert meta.extra["chamber"] == "upper" and meta.extra["district"] == "5"
    assert "Jane Roe" in content and "Senate" in content


async def test_a_person_without_an_id_or_content_is_skipped():
    from ddp_sync.ingestion.sources.openstates import OpenStatesSource

    src = OpenStatesSource(_settings())
    assert ent.legislator_document({"name": "No Id"}, "fl", src) is None
    assert ent.legislator_document({"id": f"ocd-person/{U1}"}, "fl", src) is not None  # name alone still has content


async def test_the_key_helpers():
    assert ent.legislator_key(f"ocd-person/{U1}") == ent.legislator_key(U1) == f"legislator-{U1}"
    assert ent.organization_key(42) == "organization:42"


# --- embedding legislators ----------------------------------------------------------------------------


async def _embed_leg(people, *, complete=True, redis=None, pipeline=None, dry_run=False):
    emb, redis, pipe = _embedder(redis, pipeline)
    with _patch_people((people, complete) if people is not None else None), capture_logs() as logs:
        totals = await ent.embed_legislators("fl", settings=_settings(), api_base="http://api",
                                             api_key="k", embedder=emb, dry_run=dry_run)
    return totals, redis, pipe, logs


async def test_embeds_each_legislator_once_under_its_canonical_id_and_a_rerun_writes_nothing():
    people = [_person(U1, "Jane Roe"), _person(U2, "John Doe", chamber="lower")]
    emb, _, pipe = _embedder()
    with _patch_people((people, True)):
        first = await ent.embed_legislators("fl", settings=_settings(), api_base="x", embedder=emb)
        written = list(pipe.keys)
        second = await ent.embed_legislators("fl", settings=_settings(), api_base="x", embedder=emb)
    assert sorted(written) == sorted([f"legislator-{U1}", f"legislator-{U2}"])
    assert (first["listed"], first["written"], first["unchanged"], first["complete"]) == (2, 2, 0, True)
    assert pipe.keys == written  # the rerun wrote nothing
    assert (second["written"], second["unchanged"], second["complete"]) == (0, 2, True)


async def test_only_the_changed_legislator_is_rewritten():
    emb, _, pipe = _embedder()
    with _patch_people(([_person(U1, "Jane Roe"), _person(U2, "John Doe")], True)):
        await ent.embed_legislators("fl", settings=_settings(), api_base="x", embedder=emb)
    before = len(pipe.keys)
    with _patch_people(([_person(U1, "Jane Roe"), _person(U2, "John Doe", district="9")], True)):
        totals = await ent.embed_legislators("fl", settings=_settings(), api_base="x", embedder=emb)
    assert pipe.keys[before:] == [f"legislator-{U2}"] and (totals["written"], totals["unchanged"]) == (1, 1)


async def test_listing_failure_is_incomplete_and_writes_nothing():
    totals, _, pipe, logs = await _embed_leg(None)
    assert totals["complete"] is False and pipe.keys == []
    assert "knowledge_base_entities_incomplete" in _events(logs)


async def test_a_truncated_listing_embeds_what_it_has_but_is_incomplete():
    totals, _, _, logs = await _embed_leg([_person(U1, "Jane Roe")], complete=False)
    assert totals["written"] == 1 and totals["complete"] is False
    assert "knowledge_base_entities_incomplete" in _events(logs)


async def test_a_failed_write_is_counted_warned_and_retried_next_run():
    pipe = FakePipeline()
    pipe.fail_keys = {f"legislator-{U1}"}
    emb, redis, _ = _embedder(pipeline=pipe)
    with _patch_people(([_person(U1, "Jane Roe"), _person(U2, "John Doe")], True)), capture_logs() as logs:
        totals = await ent.embed_legislators("fl", settings=_settings(), api_base="x", embedder=emb)
    assert (totals["written"], totals["failed"], totals["complete"]) == (1, 1, False)
    assert "knowledge_base_entity_undone" in _events(logs)
    assert await redis.get_bill_version(f"legislator-{U1}") is None  # cache not advanced past a failure

    pipe.fail_keys = set()
    with _patch_people(([_person(U1, "Jane Roe"), _person(U2, "John Doe")], True)):
        again = await ent.embed_legislators("fl", settings=_settings(), api_base="x", embedder=emb)
    assert (again["written"], again["unchanged"], again["complete"]) == (1, 1, True)


async def test_a_legislator_dry_run_counts_and_writes_nothing_not_even_the_cache():
    totals, redis, pipe, _ = await _embed_leg([_person(U1, "Jane Roe"), _person(U2, "John Doe")], dry_run=True)
    assert totals["would_write"] == 2 and totals["written"] == 0
    assert pipe.keys == [] and redis.versions == {}


async def test_the_new_index_setting_is_required():
    with _patch_people(([_person(U1, "Jane Roe")], True)), pytest.raises(ValueError):
        await ent.embed_legislators("fl", settings=SyncSettings(), api_base="x")
    with _patch_people(([_person(U1, "Jane Roe")], True)), pytest.raises(ValueError):
        await ent.embed_legislators("fl", settings=SyncSettings(knowledge_base_index_name="votebot-large"), api_base="x")


# --- organization documents -------------------------------------------------------------------------------


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


async def test_an_organization_dry_run_lists_but_never_fetches_details_or_writes():
    emb, redis, pipe = _embedder()
    lst, det = _patch_broker([_page([1, 2, 3])], {})
    with lst, det as detail:
        totals = await ent.embed_organizations(settings=_settings(), embedder=emb, dry_run=True)
    assert totals["listed"] == 3 and totals["complete"] is True
    detail.assert_not_awaited()
    assert pipe.keys == [] and redis.versions == {}


# --- the hook ---------------------------------------------------------------------------------------------


_CFG = {"knowledge_base_embedding": {"enabled": True, "jurisdictions": ["fl"]}}


def _hook_settings():
    return SyncSettings(knowledge_base_index_name="ddp-knowledge-base", cams_api_token="t",
                        local_openstates_api_base="http://local", local_openstates_api_key="lk")


async def test_the_post_archive_hook_embeds_the_jurisdictions_legislators_after_its_bills():
    order = []
    bills = AsyncMock(side_effect=lambda *a, **k: order.append("bills"))
    legs = AsyncMock(side_effect=lambda *a, **k: order.append("legislators"))
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=_hook_settings()), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=bills), \
         patch("ddp_sync.pipelines.knowledge_base_entities.embed_legislators", new=legs):
        await _maybe_embed_knowledge_base("fl", MagicMock(), _CFG)
    assert order == ["bills", "legislators"]
    assert legs.await_args.args == ("fl",)
    assert (legs.await_args.kwargs["api_base"], legs.await_args.kwargs["api_key"]) == ("http://local", "lk")


async def test_a_legislator_failure_in_the_hook_is_logged_and_never_raised():
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=_hook_settings()), \
         patch("ddp_sync.pipelines.knowledge_base_embedding.embed_archived_bills", new=AsyncMock()), \
         patch("ddp_sync.pipelines.knowledge_base_entities.embed_legislators",
               new=AsyncMock(side_effect=RuntimeError("boom"))), capture_logs() as logs:
        await _maybe_embed_knowledge_base("fl", MagicMock(), _CFG)
    assert "knowledge_base_legislators_failed" in _events(logs)


async def test_the_hook_embeds_no_legislators_when_it_is_not_enrolled_or_the_index_is_unset():
    legs = AsyncMock()
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=_hook_settings()), \
         patch("ddp_sync.pipelines.knowledge_base_entities.embed_legislators", new=legs):
        await _maybe_embed_knowledge_base("fl", MagicMock(), None)
        await _maybe_embed_knowledge_base("va", MagicMock(), _CFG)
    with patch("ddp_sync.pipelines.openstates_archive.get_settings", return_value=SyncSettings()), \
         patch("ddp_sync.pipelines.knowledge_base_entities.embed_legislators", new=legs):
        await _maybe_embed_knowledge_base("fl", MagicMock(), _CFG)
    legs.assert_not_awaited()


# --- api-v3 people listing and the broker reads -------------------------------------------------------------


async def test_list_people_pages_through_with_the_includes_and_the_50_cap():
    pages = [
        {"results": [{"id": "ocd-person/a"}, {"id": "ocd-person/b"}], "pagination": {"max_page": 2}},
        {"results": [{"id": "ocd-person/c"}], "pagination": {"max_page": 2}},
    ]
    get = AsyncMock(side_effect=pages)
    with patch.object(osc, "_get_json_with_retry", get):
        out = await osc.list_people("FL", api_base="http://api", api_key="k")
    assert out == ([{"id": "ocd-person/a"}, {"id": "ocd-person/b"}, {"id": "ocd-person/c"}], True)
    url, params, headers, _ = get.await_args_list[0].args
    assert url == "http://api/people" and headers == {"x-api-key": "k"}
    assert ("jurisdiction", "fl") in params and ("per_page", "50") in params
    assert {v for k, v in params if k == "include"} == {"other_names", "links", "offices"}


async def test_list_people_failure_semantics():
    page1 = {"results": [{"id": "ocd-person/a"}], "pagination": {"max_page": 9}}
    with patch.object(osc, "_get_json_with_retry", AsyncMock(side_effect=[page1, None])):
        assert await osc.list_people("fl", api_base="x") == ([{"id": "ocd-person/a"}], False)
    with patch.object(osc, "_get_json_with_retry", AsyncMock(return_value=None)):
        assert await osc.list_people("fl", api_base="x") is None
    assert await osc.list_people("fl", api_base="") is None


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


def _scheduler(jurisdictions=("fl", "us")):
    sched = MagicMock()
    sched._sync_config = {"openstates_archive": {"knowledge_base_embedding": {
        "enabled": False, "jurisdictions": list(jurisdictions)}}}
    return sched


def _route(path, *, settings=None, scheduler=None):
    settings = settings or SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", ddp_broker_api_base="http://broker",
        local_openstates_api_base="http://local", local_openstates_api_key="lk",
        rds_openstates_api_base="http://rds", rds_openstates_api_key="rk",
    )
    run = AsyncMock()
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler or _scheduler()), \
         patch("ddp_sync.config.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.openstates_archive._mac_capable", return_value=True), \
         patch("ddp_sync.pipelines.knowledge_base_entities.run_knowledge_base_entities", new=run):
        resp = _client().post(path)
    return resp, run


async def test_route_defaults_to_a_dry_run_of_every_enrolled_jurisdictions_legislators():
    resp, run = _route("/trigger/knowledge-base-entities/legislators")
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["dry_run"] is True and body["jurisdictions"] == ["fl", "us"]
    assert body["run_id"].startswith("kb-entities-legislators-dry-")
    args, kwargs = run.await_args
    assert args == ("legislators", ["fl", "us"]) and kwargs["dry_run"] is True
    assert (kwargs["api_base"], kwargs["api_key"]) == ("http://local", "lk")  # the Mac read split
    assert kwargs["run_id"] == body["run_id"]


async def test_route_runs_one_jurisdiction_for_real():
    resp, run = _route("/trigger/knowledge-base-entities/legislators?jurisdiction=US&dry_run=false")
    assert resp.status_code == 202 and run.await_args.args == ("legislators", ["us"])
    assert run.await_args.kwargs["dry_run"] is False


async def test_route_organizations_need_no_jurisdiction():
    resp, run = _route("/trigger/knowledge-base-entities/organizations?dry_run=false")
    assert resp.status_code == 202 and run.await_args.args == ("organizations", [])


async def test_route_rejects_an_unknown_entity_and_an_unenrolled_jurisdiction():
    resp, run = _route("/trigger/knowledge-base-entities/policies")
    assert resp.status_code == 404 and "Unknown entity" in resp.text
    resp, run = _route("/trigger/knowledge-base-entities/legislators?jurisdiction=ca")
    assert resp.status_code == 404 and "not enrolled" in resp.text
    run.assert_not_awaited()


async def test_route_refuses_an_unset_or_legacy_index_and_a_missing_read_path():
    resp, run = _route("/trigger/knowledge-base-entities/legislators", settings=SyncSettings())
    assert resp.status_code == 503
    resp, run = _route("/trigger/knowledge-base-entities/legislators",
                       settings=SyncSettings(knowledge_base_index_name="votebot-large", local_openstates_api_base="http://x"))
    assert resp.status_code == 503 and "legacy index" in resp.text
    resp, run = _route("/trigger/knowledge-base-entities/legislators", settings=SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", local_openstates_api_base=""))
    assert resp.status_code == 503 and "read path" in resp.text
    resp, run = _route("/trigger/knowledge-base-entities/organizations", settings=SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", ddp_broker_api_base=""))
    assert resp.status_code == 503 and "DDP_BROKER_API_BASE" in resp.text
    run.assert_not_awaited()


async def test_the_runner_runs_each_jurisdiction_and_reports_completeness():
    ok = {"complete": True}
    bad = {"complete": False}
    leg = AsyncMock(side_effect=[ok, bad])
    with patch.object(ent, "embed_legislators", new=leg):
        out = await ent.run_knowledge_base_entities(
            "legislators", ["fl", "us"], settings=_settings(), api_base="http://api", dry_run=False, run_id="r1")
    assert [c.args[0] for c in leg.await_args_list] == ["fl", "us"]
    assert out["complete"] is False and len(out["runs"]) == 2
    orgs = AsyncMock(return_value=ok)
    with patch.object(ent, "embed_organizations", new=orgs):
        out = await ent.run_knowledge_base_entities("organizations", [], settings=_settings(), api_base="", dry_run=True)
    assert out["complete"] is True and orgs.await_args.kwargs["dry_run"] is True
