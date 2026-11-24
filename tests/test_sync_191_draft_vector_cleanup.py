"""SYNC-191: the clean-up of knowledge-base vectors for drafts a bill has since replaced (OPEN-861 / OPEN-870).

api-v3 names the drafts (the ledger without and with `exclude_introduced_drafts`); the Redis record says which vectors
exist. Pinecone is a fake that records the exact ids it is asked to delete.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines import knowledge_base_draft_cleanup as cleanup
from ddp_sync.pipelines import knowledge_base_embedding as kb

from .test_knowledge_base_embedding import FakeRedis

pytestmark = pytest.mark.asyncio

DRAFT = "0b1c2d3e-0000-0000-0000-0000000000d1"
REAL = "0b1c2d3e-0000-0000-0000-0000000000a1"
NEVER = "0b1c2d3e-0000-0000-0000-0000000000e1"  # a draft with no record: nothing was embedded
SETTINGS = SyncSettings(knowledge_base_index_name="ddp-knowledge-base")


def _record(**docs):
    return {"schema": kb.CACHE_SCHEMA, "documents": docs}


def _world(extra_ledger=None, kept_complete=True):
    """Ledger without the option lists DRAFT, NEVER and REAL; with it, REAL only."""
    redis = FakeRedis()
    redis.versions[DRAFT] = _record(**{"7": {"chunks": 3, "diff_chunks": 1}, "8": {"chunks": 2}})
    redis.versions[REAL] = _record(**{"9": {"chunks": 4}})
    full = ({DRAFT: {"7": None, "8": None}, NEVER: {"5": None}, REAL: {"9": None}}, True)
    kept = ({REAL: {"9": None}}, kept_complete)

    async def ledger(jurisdiction, *, api_base, api_key="", exclude_introduced_drafts=True):
        return kept if exclude_introduced_drafts else full

    return redis, ledger


async def _run(redis, ledger, *, apply, vs=None, **kw):
    vs = vs or MagicMock(delete=AsyncMock())
    embedder = kb.KnowledgeBaseEmbedder(SETTINGS, pipeline=MagicMock(), redis_store=redis)
    with patch.object(cleanup.local_openstates_client, "list_embedding_ledger", ledger), \
         patch("ddp_sync.services.vector_store.VectorStoreService", return_value=vs):
        result = await cleanup.remove_introduced_draft_vectors(
            "mt", api_base="http://api", embedder=embedder, redis=redis, apply=apply, **kw)
    return result, vs


async def test_a_dry_run_counts_what_it_would_remove_and_changes_nothing():
    redis, ledger = _world()
    before = {k: dict(v) for k, v in redis.versions.items()}
    result, vs = await _run(redis, ledger, apply=False)
    assert result["status"] == "done" and result["dry_run"] is True
    assert result["drafts"] == 2 and result["with_vectors"] == 1  # NEVER has no record
    assert (result["documents"], result["vectors"]) == (2, 6)
    assert (result["removed_documents"], result["removed_vectors"]) == (0, 0)
    vs.delete.assert_not_awaited()
    assert redis.versions == before


async def test_apply_deletes_exactly_the_recorded_ids_of_the_draft_and_empties_its_record():
    redis, ledger = _world()
    result, vs = await _run(redis, ledger, apply=True)
    assert result["status"] == "done" and (result["removed_documents"], result["removed_vectors"]) == (2, 6)
    deleted = sorted(i for c in vs.delete.await_args_list for i in c.kwargs["ids"])
    assert deleted == sorted(
        [f"bill-text:{DRAFT}:7-chunk-{i}" for i in range(3)]
        + [f"bill-version-diff:{DRAFT}:7-chunk-0"] + [f"bill-text:{DRAFT}:8-chunk-{i}" for i in range(2)]
    )
    assert redis.versions[DRAFT]["documents"] == {} and DRAFT in redis.persistent_keys
    assert redis.versions[REAL] == _record(**{"9": {"chunks": 4}})  # a real bill is never touched
    assert NEVER not in redis.versions


async def test_a_failed_delete_leaves_the_record_whole_so_a_rerun_retries():
    redis, ledger = _world()
    vs = MagicMock(delete=AsyncMock(side_effect=RuntimeError("pinecone down")))
    result, _ = await _run(redis, ledger, apply=True, vs=vs)
    assert result["status"] == "partial" and result["removed_documents"] == 0
    assert set(redis.versions[DRAFT]["documents"]) == {"7", "8"}


async def test_a_record_another_writer_changed_is_left_alone():
    redis, ledger = _world()
    read = redis.get_bill_versions

    async def changing(ids):
        out = await read(ids)
        redis.versions[DRAFT] = _record(**{"7": {"chunks": 9}})  # changed after the dry read
        return out

    redis.get_bill_versions = changing
    result, vs = await _run(redis, ledger, apply=True)
    assert result["status"] == "partial" and result["bills"][0]["error"] == "record_changed"
    vs.delete.assert_not_awaited()


async def test_an_incomplete_or_missing_ledger_deletes_nothing():
    redis, ledger = _world(kept_complete=False)
    result, vs = await _run(redis, ledger, apply=True)
    assert result["error"] == "ledger_incomplete"
    result, vs = await _run(redis, AsyncMock(return_value=None), apply=True)
    assert result["error"] == "ledger_unavailable"
    vs.delete.assert_not_awaited()


async def test_an_older_api_v3_that_ignores_the_option_finds_no_drafts():
    redis, _ = _world()
    same = ({DRAFT: {"7": None}, REAL: {"9": None}}, True)
    result, vs = await _run(redis, AsyncMock(return_value=same), apply=True)
    assert result["drafts"] == 0
    vs.delete.assert_not_awaited()


async def test_more_documents_than_the_orphan_cap_is_refused_in_both_modes():
    redis, ledger = _world()
    with patch.object(cleanup, "MAX_ORPHAN_DOCUMENTS_PER_RUN", 1):
        result, vs = await _run(redis, ledger, apply=True)
        assert result["status"] == "refused"
    vs.delete.assert_not_awaited()


def _route(path, *, settings=None):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    settings = settings or SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", local_openstates_api_base="http://local",
        local_openstates_api_key="lk",
    )
    sched = MagicMock()
    sched._sync_config = {"openstates_archive": {"knowledge_base_embedding": {"jurisdictions": ["fl"]}}}
    run = AsyncMock()
    with patch("ddp_sync.scheduler.get_scheduler", return_value=sched), \
         patch("ddp_sync.config.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.openstates_archive._mac_capable", return_value=True), \
         patch.object(cleanup, "remove_introduced_draft_vectors", new=run):
        resp = TestClient(app).post(path)
    return resp, run


async def test_the_route_is_a_dry_run_unless_told_otherwise_and_refuses_what_it_cannot_run():
    resp, run = _route("/trigger/knowledge-base-draft-cleanup/FL")
    assert resp.status_code == 202 and resp.json()["dry_run"] is True
    assert run.await_args.kwargs["apply"] is False and run.await_args.args == ("fl",)
    resp, run = _route("/trigger/knowledge-base-draft-cleanup/fl?dry_run=false")
    assert run.await_args.kwargs["apply"] is True
    resp, run = _route("/trigger/knowledge-base-draft-cleanup/ca")
    assert resp.status_code == 404
    resp, run = _route("/trigger/knowledge-base-draft-cleanup/fl", settings=SyncSettings())
    assert resp.status_code == 503
    run.assert_not_awaited()
