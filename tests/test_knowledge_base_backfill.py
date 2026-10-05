"""SYNC-90: the staged, resumable knowledge-base backfill (PLAN-enterprise-search.md 5.6).

Mocked Pinecone, Redis and api-v3 throughout; nothing here can reach a real index. The embedding
itself is the real `KnowledgeBaseEmbedder` (so the backfill and the live hook share one code path)
with a recording pipeline. Covers the ticket's list: a dry run reports counts per stage, each stage
embeds exactly its slice, resumable from a checkpoint, a re-run writes nothing, a document another
writer changed is never overwritten, throttling around the archive window, and the trigger route.
"""

from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router
from ddp_sync.config import SyncSettings
from ddp_sync.pipelines import knowledge_base_backfill as bf
from ddp_sync.pipelines import knowledge_base_embedding as kb
from tests.test_knowledge_base_embedding import (
    DIFF_B,
    DIFF_C,
    TEXT_A,
    TEXT_B,
    TEXT_C,
    FakePipeline,
    FakeRedis,
    _settings,
    _version,
)

pytestmark = pytest.mark.asyncio

A, B, C = "aaa00000-0000-0000-0000-000000000001", "bbb00000-0000-0000-0000-000000000002", "ccc00000-0000-0000-0000-000000000003"
_VOTES = [{"start_date": "2026-02-01"}]


def _mk(ocd, session, versions, votes=None):
    return {
        "id": f"ocd-bill/{ocd}", "identifier": "HB 1", "title": "A bill", "session": session,
        "jurisdiction": {"name": "Florida"}, "sources": [{"url": "https://leg.example/x"}],
        "versions": versions, "votes": votes or [],
    }


def _world():
    return {
        A: _mk(A, "2026", [
            _version(1, "Filed", "introduced", 0, TEXT_A),
            _version(2, "Amended", "amendment", 1, TEXT_B, diff=DIFF_B),
            _version(3, "Enrolled", "enacted", 2, TEXT_C, diff=DIFF_C),
        ], _VOTES),
        B: _mk(B, "2025", [
            _version(4, "Filed", "introduced", 0, TEXT_A + "b"),
            _version(5, "Enrolled", "enacted", 1, TEXT_B + "b", diff=DIFF_B + "b"),
        ]),
        C: _mk(C, "2026", [
            _version(6, "Mystery", "unknown", None, TEXT_A + "c", unknown=True),
            _version(7, "Filed", "introduced", 0, TEXT_B + "c"),
        ]),
    }


LISTING = {"2026": [A, C], None: [A, B, C]}


def _t(ocd, n):
    return kb.text_document_key(ocd, n)


def _d(ocd, n):
    return kb.diff_document_key(ocd, n)


class _Client:
    def __init__(self):
        self.kv: dict = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    async def get(self, key):
        return self.kv.get(key)

    async def expire(self, key, ttl):
        return True

    async def delete(self, key):
        self.kv.pop(key, None)


class BfRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.checkpoints: dict = {}
        self._client = _Client()

    async def get_kb_backfill_checkpoint(self, jurisdiction, stage):
        return copy.deepcopy(self.checkpoints.get((jurisdiction.lower(), stage)))

    async def set_kb_backfill_checkpoint(self, jurisdiction, stage, data):
        self.checkpoints[(jurisdiction.lower(), stage)] = copy.deepcopy(data)
        return True

    async def delete_kb_backfill_checkpoint(self, jurisdiction, stage):
        self.checkpoints.pop((jurisdiction.lower(), stage), None)
        return True


class Env:
    """One test's world: fake Redis/pipeline/api-v3, wired into the backfill module."""

    def __init__(self, world=None, listing=None, session="2026"):
        self.redis, self.pipe = BfRedis(), FakePipeline()
        self.world = world if world is not None else _world()
        self.listing = listing if listing is not None else LISTING
        self.session = session
        self.fetched: list[str] = []
        self.fetch_hook = None

    async def _list(self, jurisdiction, *, since, api_base, api_key="", session=None):
        value = self.listing.get(session)
        return None if value is None else (list(value), True)

    async def _fetch(self, ocd, *, api_base, api_key=""):
        self.fetched.append(ocd)
        if self.fetch_hook:
            self.fetch_hook(ocd)
        return self.world.get(ocd)

    def embedder(self, settings):
        return kb.KnowledgeBaseEmbedder(settings, pipeline=self.pipe,
                                        redis_store=self.redis)

    async def run(self, stages, *, dry_run=False, restart=False, config=None, run_id="r1"):
        source = MagicMock()
        source.get_current_session_identifier = AsyncMock(return_value=self.session)
        with patch.object(bf, "get_redis_store", return_value=self.redis), \
             patch.object(bf.local_openstates_client, "list_touched_bill_ids", new=self._list), \
             patch.object(bf.local_openstates_client, "fetch_bill_for_embedding", new=self._fetch), \
             patch.object(bf, "OpenStatesSource", return_value=source), \
             patch.object(bf, "KnowledgeBaseEmbedder", side_effect=self.embedder), \
             patch.object(bf, "_utcnow", return_value=datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)):
            return await bf.run_knowledge_base_backfill(
                "fl", stages, settings=_settings(), api_base="http://api", api_key="k",
                config=config, dry_run=dry_run, restart=restart, run_id=run_id,
            )


# --- each stage embeds exactly its slice ----------------------------------------------------------


@pytest.mark.parametrize("stage, expected", [
    ("current", {_t(A, 3), _t(C, 7)}),  # current-session bills' current versions; unknown is never current
    ("diffs", {_d(A, 2), _d(A, 3), _d(B, 5)}),
    ("prior-sessions", {_t(B, 5)}),  # the prior-session bill's current version
    ("history", {_t(A, 1), _t(A, 2), _t(A, 3), _t(B, 4), _t(B, 5), _t(C, 6), _t(C, 7)}),
])
async def test_each_stage_embeds_exactly_its_slice(stage, expected):
    env = Env()
    result = await env.run([stage])
    assert set(env.pipe.keys) == expected and len(env.pipe.keys) == len(expected)
    assert result["status"] == "complete"
    assert env.redis.checkpoints[("fl", stage)]["done"] is True


async def test_all_stages_in_order_write_every_document_exactly_once():
    env = Env()
    result = await env.run(None)
    assert [r["stage"] for r in result["stages"]] == list(bf.DEFAULT_STAGES)
    assert result["status"] == "complete"
    assert len(env.pipe.keys) == len(set(env.pipe.keys))  # history skipped what the earlier stages wrote
    # Text only: version diffs are not embedded (Ramon, 2026-10-05), so no diff document is ever written.
    assert set(env.pipe.keys) == {_t(A, 1), _t(A, 2), _t(A, 3), _t(B, 4), _t(B, 5), _t(C, 6), _t(C, 7)}


async def test_a_default_run_never_includes_the_diffs_stage():
    assert bf.DEFAULT_STAGES == ("current", "prior-sessions", "history")
    assert "diffs" not in bf.DEFAULT_STAGES and "diffs" in bf.STAGES


async def test_there_is_no_votes_stage_and_every_stage_has_a_scope():
    """SYNC-94: votes are not embedded, so a no-stage run can never write a votes document."""
    assert bf.STAGES == ("current", "diffs", "prior-sessions", "history")
    assert set(bf.STAGE_SCOPES) == set(bf.STAGES)


async def test_a_leftover_votes_checkpoint_and_legacy_votes_totals_do_not_disturb_a_run():
    """Checkpoints are keyed per stage, so one written for the removed `votes` stage is simply never
    read; and a surviving stage's checkpoint may still carry a `votes` total from before SYNC-94."""
    env = Env()
    stale = {"last_bill_id": A, "failed_ids": [], "done": False, "totals": {"votes": 7}}
    env.redis.checkpoints[("fl", "votes")] = dict(stale)
    env.redis.checkpoints[("fl", "diffs")] = {"last_bill_id": "", "failed_ids": [], "done": False,
                                              "totals": {"documents": 0, "votes": 0}}
    result = await env.run(None)
    assert result["status"] == "complete" and [r["stage"] for r in result["stages"]] == list(bf.DEFAULT_STAGES)
    assert env.redis.checkpoints[("fl", "votes")] == stale  # untouched, never consulted
    assert env.redis.checkpoints[("fl", "diffs")]["done"] is False  # a default run does not touch diffs


async def test_stage_selection_uses_the_session_filter_not_a_detail_fetch_per_bill():
    env = Env()
    await env.run(["current"])
    assert env.fetched == [A, C]  # only current-session bills were ever fetched (B was not)
    env = Env()
    await env.run(["prior-sessions"])
    assert env.fetched == [B]


# --- idempotent and resumable ---------------------------------------------------------------------


async def test_rerunning_writes_nothing_and_a_finished_stage_does_not_even_fetch():
    env = Env()
    await env.run(None)
    written, fetched = len(env.pipe.keys), len(env.fetched)
    again = await env.run(None)
    assert len(env.pipe.keys) == written and len(env.fetched) == fetched
    assert all(r["status"] == "already_complete" for r in again["stages"])


async def test_restart_rewalks_but_still_writes_nothing_new():
    env = Env()
    await env.run(["history"])
    written = len(env.pipe.keys)
    env.fetched.clear()
    result = await env.run(["history"], restart=True)
    assert env.fetched == sorted([A, B, C])  # walked again
    assert len(env.pipe.keys) == written  # the version cache skipped every document
    assert result["status"] == "complete"


async def test_interrupted_run_resumes_from_its_checkpoint():
    env = Env()

    def interrupt(ocd):
        if ocd == C:
            raise asyncio.CancelledError()

    env.fetch_hook = interrupt
    with patch.object(bf, "CHECKPOINT_EVERY", 1), pytest.raises(asyncio.CancelledError):
        await env.run(["history"])
    assert env.redis.checkpoints[("fl", "history")]["last_bill_id"] == B
    assert not env.redis.checkpoints[("fl", "history")]["done"]
    assert bf.lock_key("fl") not in env.redis._client.kv  # released even though the run was cut short

    env.fetch_hook, env.fetched = None, []
    result = await env.run(["history"])
    assert env.fetched == [C]  # only what was left
    assert result["status"] == "complete"
    assert {_t(A, 1), _t(B, 4), _t(C, 6)} <= set(env.pipe.keys)


async def test_a_failed_bill_is_retried_first_and_keeps_the_stage_open():
    env = Env()
    env.pipe.fail_keys = {_t(B, 5)}
    first = await env.run(["prior-sessions"])
    assert first["status"] == "incomplete"
    cp = env.redis.checkpoints[("fl", "prior-sessions")]
    assert cp["failed_ids"] == [B] and cp["done"] is False

    env.pipe.fail_keys, env.fetched = set(), []
    second = await env.run(["prior-sessions"])
    assert env.fetched == [B]
    assert second["status"] == "complete"
    cp = env.redis.checkpoints[("fl", "prior-sessions")]
    assert cp["failed_ids"] == [] and cp["done"] is True


async def test_an_interrupted_retry_keeps_the_failures_it_had_not_retried_yet():
    """The reviewer's case: checkpoint {last=C, failed=[A,B]}, interrupted while retrying B. The
    failure list must survive the interrupt, or B sits below `last`, is never walked again, and the
    stage finishes done with a bill silently undone."""
    env = Env()
    env.redis.checkpoints[("fl", "history")] = {
        "last_bill_id": C, "failed_ids": [A, B], "done": False, "totals": {},
    }

    def interrupt(ocd):
        if ocd == B:
            raise asyncio.CancelledError()

    env.fetch_hook = interrupt
    with patch.object(bf, "CHECKPOINT_EVERY", 1), pytest.raises(asyncio.CancelledError):
        await env.run(["history"])
    saved = env.redis.checkpoints[("fl", "history")]
    assert saved["failed_ids"] == [B] and saved["done"] is False  # A was retried; B is still owed

    env.fetch_hook, env.fetched = None, []
    result = await env.run(["history"])
    assert env.fetched == [B]  # not skipped, not forgotten
    assert result["status"] == "complete"
    assert env.redis.checkpoints[("fl", "history")]["failed_ids"] == []


async def test_a_carried_over_failure_that_fails_again_stays_on_the_list_exactly_once():
    env = Env()
    env.redis.checkpoints[("fl", "history")] = {"last_bill_id": C, "failed_ids": [B], "done": False, "totals": {}}
    env.pipe.fail_keys = {_t(B, 4)}
    result = await env.run(["history"])
    assert result["status"] == "incomplete"
    assert env.redis.checkpoints[("fl", "history")]["failed_ids"] == [B]


async def test_a_malformed_blackout_window_fails_fast_before_taking_the_lock():
    env = Env()
    cfg = {"knowledge_base_embedding": {"backfill": {"blackout_start_utc": "late", "blackout_end_utc": "07:00"}}}
    result = await env.run(["diffs"], config=cfg)
    assert result["status"] == "error" and result["error"] == "bad_blackout_config"
    assert env.pipe.keys == [] and env.fetched == [] and env.redis._client.kv == {}


async def test_a_broken_stage_aborts_instead_of_walking_the_whole_corpus():
    env = Env()
    env.pipe.fail_keys = {_t(A, 1), _t(A, 2), _t(A, 3), _t(B, 4), _t(B, 5), _t(C, 6), _t(C, 7)}
    with patch.object(bf, "MAX_FAILED_BILLS", 1), capture_logs() as logs:
        result = await env.run(["history", "diffs"])
    assert [r["status"] for r in result["stages"]] == ["aborted"]  # the next stage never started
    assert env.fetched == [A, B]  # stopped once more than 1 bill had failed
    assert any(e["event"] == "knowledge_base_backfill_aborted" for e in logs)
    assert result["status"] == "incomplete"


async def test_restart_forgets_the_stored_checkpoint_before_any_bill_work():
    env = Env(listing={None: None, "2026": None})  # the run dies at listing, before touching a bill
    env.redis.checkpoints[("fl", "diffs")] = {"last_bill_id": C, "failed_ids": [], "done": True, "totals": {}}
    result = await env.run(["diffs"], restart=True)
    assert result["stages"][0]["status"] == "error"
    assert ("fl", "diffs") not in env.redis.checkpoints  # a crash now cannot resurrect the old position


async def test_an_api_v3_without_the_archive_ids_aborts_the_stage_instead_of_walking_it():
    """Until OPEN-315 deploys the new api-v3 fields, every version arrives without archived_document_id."""
    world = _world()
    for bill in world.values():
        for v in bill["versions"]:
            v.pop("archived_document_id")
    env = Env(world=world)
    with patch.object(bf, "MAX_FAILED_BILLS", 1):
        result = await env.run(["history"])
    assert result["stages"][0]["status"] == "aborted"
    assert env.pipe.keys == []  # nothing half-written to the index
    assert env.redis.checkpoints[("fl", "history")]["done"] is False


async def test_totals_accumulate_across_resumed_runs_and_report_chars():
    env = Env()
    env.pipe.fail_keys = {_t(B, 5)}
    await env.run(["history"])
    env.pipe.fail_keys = set()
    await env.run(["history"])
    totals = env.redis.checkpoints[("fl", "history")]["totals"]
    # 6 written the first time; a bill with a failed document keeps its cache un-advanced (SYNC-83's
    # all-or-nothing rule), so B's retry rewrites its first document too: 6 + 2 = 8, never lost.
    assert totals["documents"] == 8 and totals["chars"] > 0 and totals["failed_bills"] == 1
    assert totals["bills"] == 4  # B was processed twice


# --- failures that must not be silent -------------------------------------------------------------


async def test_listing_failure_is_an_error_and_advances_nothing():
    env = Env(listing={None: None, "2026": None})
    with capture_logs() as logs:
        result = await env.run(["diffs", "history"])
    assert result["stages"] == [{"stage": "diffs", "status": "error", "error": "listing_failed"}]
    assert env.redis.checkpoints == {} and env.pipe.keys == []
    assert any(e["event"] == "knowledge_base_backfill_failed" for e in logs)


async def test_unknown_current_session_is_an_error_not_a_guess():
    env = Env(session=None)
    result = await env.run(["current"])
    assert result["stages"][0]["status"] == "error" and env.pipe.keys == []


async def test_redis_down_refuses_a_real_run():
    env = Env()
    env.redis.is_available = False
    result = await env.run(["diffs"])
    assert result["error"] == "redis_unavailable" and env.pipe.keys == []


# --- dry run --------------------------------------------------------------------------------------


async def test_dry_run_reports_counts_per_stage_and_writes_nothing():
    env = Env()
    result = await env.run(None, dry_run=True)
    counts = {r["stage"]: (r["bills_in_stage"], r["bills_remaining"]) for r in result["stages"]}
    assert counts == {"current": (2, 2), "prior-sessions": (1, 1), "history": (3, 3)}
    assert env.pipe.keys == [] and env.fetched == []
    assert env.redis.checkpoints == {} and env.redis._client.kv == {}  # no lock, no checkpoint


async def test_dry_run_counts_only_what_a_checkpoint_has_left():
    env = Env()
    env.redis.checkpoints[("fl", "diffs")] = {"last_bill_id": A, "failed_ids": [], "done": False, "totals": {}}
    result = await env.run(["diffs"], dry_run=True)
    assert result["stages"][0]["bills_remaining"] == 2


# --- throttling around the archive window ---------------------------------------------------------


@pytest.mark.parametrize("hour, minute, left", [
    (4, 44, 0), (4, 45, 135 * 60), (5, 0, 120 * 60), (6, 59, 60), (7, 0, 0), (12, 0, 0),
])
async def test_blackout_window_default_is_0445_to_0700_utc(hour, minute, left):
    now = datetime(2026, 10, 1, hour, minute, tzinfo=timezone.utc)
    assert bf._blackout_seconds_left(now, None) == left


async def test_blackout_window_is_configurable_and_may_cross_midnight():
    cfg = {"knowledge_base_embedding": {"backfill": {"blackout_start_utc": "23:00", "blackout_end_utc": "01:00"}}}
    assert bf._blackout_seconds_left(datetime(2026, 10, 1, 23, 30, tzinfo=timezone.utc), cfg) == 90 * 60
    assert bf._blackout_seconds_left(datetime(2026, 10, 2, 0, 30, tzinfo=timezone.utc), cfg) == 30 * 60
    assert bf._blackout_seconds_left(datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc), cfg) == 0


async def test_a_run_inside_the_window_sleeps_until_it_ends_then_proceeds():
    env = Env()
    slept: list[float] = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    source = MagicMock()
    source.get_current_session_identifier = AsyncMock(return_value="2026")
    inside = datetime(2026, 10, 1, 5, 0, tzinfo=timezone.utc)
    with patch.object(bf, "get_redis_store", return_value=env.redis), \
         patch.object(bf.local_openstates_client, "list_touched_bill_ids", new=env._list), \
         patch.object(bf.local_openstates_client, "fetch_bill_for_embedding", new=env._fetch), \
         patch.object(bf, "OpenStatesSource", return_value=source), \
         patch.object(bf, "KnowledgeBaseEmbedder", side_effect=env.embedder), \
         patch.object(bf, "_utcnow", return_value=inside), patch.object(bf, "_sleep", new=fake_sleep):
        result = await bf.run_knowledge_base_backfill("fl", ["diffs"], settings=_settings(),
                                                      api_base="http://api", dry_run=False, run_id="r1")
    assert slept == [120 * 60.0] * 3  # once per bill, until 07:00
    assert result["status"] == "complete"


# --- one run per jurisdiction ---------------------------------------------------------------------


async def test_a_held_lock_rejects_a_second_run_and_a_normal_run_releases_it():
    env = Env()
    env.redis._client.kv[bf.lock_key("fl")] = "someone-else"
    result = await env.run(["diffs"])
    assert result["status"] == "already_running" and result["current_run_id"] == "someone-else"
    assert env.pipe.keys == []
    assert env.redis._client.kv[bf.lock_key("fl")] == "someone-else"  # not released by the loser

    env2 = Env()
    await env2.run(["diffs"])
    assert bf.lock_key("fl") not in env2.redis._client.kv


async def test_the_lock_is_released_when_setup_raises():
    env = Env()
    source = MagicMock()
    with patch.object(bf, "get_redis_store", return_value=env.redis), \
         patch.object(bf, "OpenStatesSource", return_value=source), \
         patch.object(bf, "KnowledgeBaseEmbedder", side_effect=ValueError("index unset")), \
         pytest.raises(ValueError):
        await bf.run_knowledge_base_backfill("fl", ["diffs"], settings=SyncSettings(), api_base="x",
                                             dry_run=False, run_id="r1")
    assert bf.lock_key("fl") not in env.redis._client.kv


# --- the trigger route ----------------------------------------------------------------------------


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


def _route(path, *, settings=None, scheduler=None, holder=None):
    settings = settings or SyncSettings(knowledge_base_index_name="ddp-knowledge-base",
                                        local_openstates_api_base="http://local", local_openstates_api_key="lk",
                                        rds_openstates_api_base="http://rds", rds_openstates_api_key="rk")
    run = AsyncMock()
    with patch("ddp_sync.scheduler.get_scheduler", return_value=scheduler or _scheduler()), \
         patch("ddp_sync.config.get_settings", return_value=settings), \
         patch("ddp_sync.pipelines.openstates_archive._mac_capable", return_value=True), \
         patch("ddp_sync.pipelines.knowledge_base_backfill.lock_holder", new=AsyncMock(return_value=holder)), \
         patch("ddp_sync.pipelines.knowledge_base_backfill.run_knowledge_base_backfill", new=run):
        resp = _client().post(path)
    return resp, run


async def test_route_defaults_to_a_dry_run_of_every_stage():
    resp, run = _route("/trigger/knowledge-base-backfill/fl")
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["dry_run"] is True and body["stages"] == list(bf.DEFAULT_STAGES)
    assert body["run_id"].startswith("fl-kb-backfill-dry-")
    run.assert_awaited_once()
    args, kwargs = run.await_args
    assert args == ("fl", None)
    assert kwargs["dry_run"] is True and kwargs["restart"] is False and kwargs["run_id"] == body["run_id"]
    assert (kwargs["api_base"], kwargs["api_key"]) == ("http://local", "lk")  # the Mac read split


async def test_route_runs_one_stage_for_real_and_passes_restart():
    resp, run = _route("/trigger/knowledge-base-backfill/fl?stage=diffs&dry_run=false&restart=true")
    assert resp.status_code == 202
    assert resp.json()["stages"] == ["diffs"] and resp.json()["run_id"].startswith("fl-kb-backfill-run-")
    args, kwargs = run.await_args
    assert args == ("fl", ["diffs"]) and kwargs["dry_run"] is False and kwargs["restart"] is True


async def test_route_rejects_unknown_stage_and_unenrolled_jurisdiction():
    resp, run = _route("/trigger/knowledge-base-backfill/fl?stage=nope")
    assert resp.status_code == 404 and "Unknown stage" in resp.text
    resp, run = _route("/trigger/knowledge-base-backfill/fl?stage=votes")  # SYNC-94: no longer a stage
    assert resp.status_code == 404 and "Unknown stage" in resp.text and not run.await_count
    resp, run = _route("/trigger/knowledge-base-backfill/ca")
    assert resp.status_code == 404 and "not enrolled" in resp.text
    run.assert_not_awaited()


async def test_route_refuses_the_legacy_index_with_a_503_not_a_silent_background_failure():
    resp, run = _route("/trigger/knowledge-base-backfill/fl", settings=SyncSettings(
        knowledge_base_index_name="votebot-large", local_openstates_api_base="http://local"))
    assert resp.status_code == 503 and "legacy index" in resp.text
    run.assert_not_awaited()


async def test_route_needs_the_index_setting_and_a_read_path():
    resp, run = _route("/trigger/knowledge-base-backfill/fl", settings=SyncSettings())
    assert resp.status_code == 503 and "KNOWLEDGE_BASE_INDEX_NAME" in resp.text
    resp, run = _route("/trigger/knowledge-base-backfill/fl", settings=SyncSettings(
        knowledge_base_index_name="ddp-knowledge-base", local_openstates_api_base=""))
    assert resp.status_code == 503 and "read path" in resp.text
    run.assert_not_awaited()


async def test_route_409s_a_real_run_while_one_is_running_but_not_a_dry_run():
    resp, run = _route("/trigger/knowledge-base-backfill/fl?dry_run=false", holder="run-123")
    assert resp.status_code == 409 and "run-123" in resp.text
    run.assert_not_awaited()
    resp, run = _route("/trigger/knowledge-base-backfill/fl?dry_run=true", holder="run-123")
    assert resp.status_code == 202


async def test_checked_in_yaml_has_the_blackout_window():
    from pathlib import Path

    import yaml

    cfg = yaml.safe_load((Path(__file__).parent.parent / "config" / "sync_schedule.yaml").read_text())
    block = cfg["openstates_archive"]["knowledge_base_embedding"]
    assert block["enabled"] is True  # on since 2026-10-05; the per-host gate is KNOWLEDGE_BASE_INDEX_NAME
    assert (block["backfill"]["blackout_start_utc"], block["backfill"]["blackout_end_utc"]) == ("04:45", "07:00")
    assert bf._blackout_seconds_left(datetime(2026, 10, 1, 5, 0, tzinfo=timezone.utc), cfg["openstates_archive"]) > 0
