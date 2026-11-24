"""OPEN-855: after each state's load, ddp-sync alerts in #automation-errors when a roll-call voter name matches no one
(a new name, or a session's unmatched share rising), listing the top names with counts. The count comes from
ddp-open-states' unmatched-voters.py; names the manifest accepts never alert; a clean state stays silent."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines import unmatched_voter_alert as uva
from ddp_sync.pipelines.openstates_scrape import PipelineFailed, run_state_pipeline

CFG = {"unmatched_voter_alert": {"enabled": True}}


def _state(names=(), sessions=None):
    """unmatched-voters.py's JSON: names are (session, name, count[, accepted])."""
    entries = [
        {"session": n[0], "name": n[1], "count": n[2], "accepted": n[3] if len(n) > 3 else False} for n in names
    ]
    if sessions is None:
        sessions = {}
        for e in entries:
            sessions.setdefault(e["session"], {"total": 1000, "unresolved": 0})["unresolved"] += e["count"]
        sessions = sessions or {"2026": {"total": 1000, "unresolved": 0}}
    return {"state": "me", "sessions": sessions, "names": entries}


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, nx=False, ex=None):
        self.store[key] = value
        return True


@pytest.fixture
def redis():
    fake = _FakeRedis()
    with patch("ddp_sync.services.redis_store.get_redis_store", return_value=SimpleNamespace(_client=fake)):
        yield fake


@pytest.fixture
def alert():
    with patch.object(uva, "post_alert", return_value=True) as mock:
        yield mock


async def _check(state, jurisdiction="me", config=CFG):
    with patch.object(uva, "read_unmatched", return_value=state) as read:
        await uva.maybe_alert_unmatched_voters(jurisdiction, config)
    return read


# ── the rule ───────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_planted_unmatched_name_alerts_with_its_name_and_count(redis, alert):
    await _check(_state([("132R2", "PUGH of Portland", 812)]))
    alert.assert_called_once()
    text = alert.call_args.args[0]
    assert '"PUGH of Portland": 812 votes in 132R2 (new)' in text
    assert "ME " in text
    assert alert.call_args.kwargs["source"] == "openstates_scrape.unmatched_voters"


@pytest.mark.asyncio
async def test_a_clean_state_stays_silent(redis, alert):
    await _check(_state())
    alert.assert_not_called()
    assert "ddp:unmatched_voters:me" in redis.store  # remembered, so the next run compares with it


@pytest.mark.asyncio
async def test_the_same_names_at_the_same_share_do_not_alert_again(redis, alert):
    await _check(_state([("2026", "Lewis C", 441)]))
    alert.reset_mock()
    await _check(_state([("2026", "Lewis C", 441)]))
    alert.assert_not_called()


@pytest.mark.asyncio
async def test_a_new_name_alerts_and_only_it_is_marked_new(redis, alert):
    await _check(_state([("2026", "Lewis C", 441)]))
    alert.reset_mock()
    await _check(_state([("2026", "Lewis C", 441), ("2026", "POPE, JR.", 109)]))
    text = alert.call_args.args[0]
    assert '"POPE, JR.": 109 votes in 2026 (new)' in text
    assert '"Lewis C": 441 votes in 2026\n' in text
    assert "1 new name(s)" in text


@pytest.mark.asyncio
async def test_one_name_over_several_sessions_is_one_line_with_its_total(redis, alert):
    await _check(_state([("132S1", "PUGH of Portland", 539), ("132R2", "PUGH of Portland", 273)]))
    text = alert.call_args.args[0]
    assert "1 new name(s)" in text
    assert '"PUGH of Portland": 812 votes in 132R2, 132S1 (new)' in text


@pytest.mark.asyncio
async def test_a_small_new_name_is_listed_ahead_of_bigger_old_ones(redis, alert):
    old = [("2026", f"OLD{i}", 500 + i) for i in range(12)]
    await _check(_state(old))
    alert.reset_mock()
    await _check(_state([*old, ("2026", "NEWBIE", 3)]))
    text = alert.call_args.args[0]
    assert '"NEWBIE": 3 votes in 2026 (new)' in text
    assert text.count("votes in") == 10  # still at most TOP_NAMES lines


@pytest.mark.asyncio
async def test_a_rising_share_with_no_new_name_alerts(redis, alert):
    await _check(_state([("2026", "Lewis C", 10)], {"2026": {"total": 1000, "unresolved": 10}}))
    alert.reset_mock()
    await _check(_state([("2026", "Lewis C", 30)], {"2026": {"total": 1100, "unresolved": 30}}))
    text = alert.call_args.args[0]
    assert "Session 2026: unmatched share rose from 1.00% to 2.73%" in text


@pytest.mark.asyncio
async def test_a_falling_share_is_silent(redis, alert):
    await _check(_state([("2026", "Lewis C", 30)], {"2026": {"total": 1000, "unresolved": 30}}))
    alert.reset_mock()
    await _check(_state([("2026", "Lewis C", 30)], {"2026": {"total": 2000, "unresolved": 30}}))
    alert.assert_not_called()


@pytest.mark.asyncio
async def test_an_accepted_name_never_alerts_nor_counts_in_the_share(redis, alert):
    await _check(_state([("2026", "Mr. Speaker", 50, True)], {"2026": {"total": 1000, "unresolved": 50}}))
    alert.assert_not_called()
    await _check(_state([("2026", "Mr. Speaker", 90, True)], {"2026": {"total": 1000, "unresolved": 90}}))
    alert.assert_not_called()


@pytest.mark.asyncio
async def test_an_undelivered_alert_is_tried_again_next_run(redis, alert):
    alert.return_value = False
    await _check(_state([("2026", "Lewis C", 441)]))
    assert "ddp:unmatched_voters:me" not in redis.store
    alert.return_value = True
    await _check(_state([("2026", "Lewis C", 441)]))
    assert alert.call_count == 2


@pytest.mark.asyncio
async def test_off_by_default_reads_nothing(redis, alert):
    read = await _check(_state([("2026", "Lewis C", 441)]), config={})
    read.assert_not_called()
    alert.assert_not_called()


@pytest.mark.asyncio
async def test_usa_is_checked_as_us(redis, alert):
    read = await _check(_state(), jurisdiction="usa")
    assert read.call_args.args[0] == "us"


@pytest.mark.asyncio
async def test_a_failed_read_never_raises(redis, alert):
    with patch.object(uva, "read_unmatched", side_effect=RuntimeError("unmatched-voters.py exited 2")):
        await uva.maybe_alert_unmatched_voters("me", CFG)
    alert.assert_not_called()


# ── running the script ─────────────────────────────────────────────────────────────


def test_read_unmatched_runs_the_script_with_the_openstates_python_and_the_rds_login():
    runner = MagicMock(return_value=subprocess.CompletedProcess([], 0, stdout=json.dumps(_state()), stderr=""))
    with patch.object(uva, "resolve_rds_database_url", return_value=("postgresql://u:pw@rds/db", None)):
        assert uva.read_unmatched("me", "/opt/ddp-open-states", runner=runner) == _state()
    cmd = runner.call_args.args[0]
    assert cmd == [uva.OPENSTATES_PYTHON, "/opt/ddp-open-states/unmatched-voters.py", "me"]
    assert runner.call_args.kwargs["env"]["DATABASE_URL"] == "postgresql://u:pw@rds/db"
    assert runner.call_args.kwargs["cwd"] == "/opt/ddp-open-states"


def test_read_unmatched_raises_on_a_failed_script_without_the_login():
    runner = MagicMock(return_value=subprocess.CompletedProcess([], 2, stdout="", stderr="could not read me"))
    with patch.object(uva, "resolve_rds_database_url", return_value=("postgresql://u:pw@rds/db", None)), \
            pytest.raises(RuntimeError) as err:
        uva.read_unmatched("me", "/opt/ddp-open-states", runner=runner)
    assert "exited 2" in str(err.value)
    assert "pw@" not in str(err.value)


def test_read_unmatched_refuses_without_an_rds_login():
    runner = MagicMock()
    with patch.object(uva, "resolve_rds_database_url", return_value=(None, "no secret")), pytest.raises(RuntimeError):
        uva.read_unmatched("me", "/opt/ddp-open-states", runner=runner)
    runner.assert_not_called()


# ── through run_state_pipeline ───────────────────────────────────────────────────────


def _scrape(success: bool):
    return patch(
        "ddp_sync.pipelines.openstates_scrape._run_scrape_impl",
        new=AsyncMock(return_value={"success": success, "jurisdiction": "me"}),
    )


async def _pipeline(archive):
    with patch("ddp_sync.pipelines.openstates_scrape._check_sustained_blocking", new=AsyncMock()), patch(
        "ddp_sync.pipelines.openstates_scrape._write_flow_status", new=AsyncMock()
    ), patch("ddp_sync.pipelines.openstates_archive.archive_with_retry", new=archive):
        return await run_state_pipeline("me", CFG)


@pytest.mark.asyncio
async def test_the_check_runs_after_a_successful_load_and_before_the_archive(redis, alert):
    order = []

    async def _archive(j, cfg):
        order.append("archive")
        return True

    with _scrape(True), patch.object(uva, "read_unmatched", side_effect=lambda c, r: order.append("check") or _state()):
        result = await _pipeline(_archive)
    assert result["success"] is True
    assert order == ["check", "archive"]


@pytest.mark.asyncio
async def test_a_failed_scrape_is_not_checked(redis, alert):
    with _scrape(False), patch.object(uva, "read_unmatched") as read, pytest.raises(PipelineFailed):
        await _pipeline(AsyncMock(return_value=True))
    read.assert_not_called()


@pytest.mark.asyncio
async def test_a_broken_check_never_changes_the_run(redis, alert):
    with _scrape(True), patch.object(uva, "read_unmatched", side_effect=FileNotFoundError("no script")):
        result = await _pipeline(AsyncMock(return_value=True))
    assert result["success"] is True
