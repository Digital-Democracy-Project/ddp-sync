"""SYNC-185: the Mac's LegBot slots are handed out bill by bill, fairly across runs, polled bills first; a staff
single-bill run never waits for them."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines import session_pipeline_runner
from ddp_sync.pipelines.session_pipeline_runner import (
    PRIORITY_OTHER,
    PRIORITY_POLLED,
    LegBotLine,
    run_legbot_pipeline,
    run_single_bill_full,
)
from ddp_sync.services import broker_client
from ddp_sync.services.broker_client import BrokerClientError

_RUNNER = "ddp_sync.pipelines.session_pipeline_runner"


@pytest.fixture(autouse=True)
def _fresh_line():
    session_pipeline_runner._legbot_line = None
    yield
    session_pipeline_runner._legbot_line = None


class _Bills:
    """Bills that hold their slot until let go, recording the order they started in."""

    def __init__(self, line):
        self.line, self.started, self.done = line, [], {}

    def queue(self, run_id, name, priority=PRIORITY_OTHER):
        self.done[name] = asyncio.Event()

        async def bill():
            async with self.line.slot(run_id, priority):
                self.started.append(name)
                await self.done[name].wait()

        return asyncio.create_task(bill())

    async def finish(self, name):
        self.done[name].set()
        for _ in range(5):
            await asyncio.sleep(0)


async def _settle():
    for _ in range(5):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_a_small_run_queued_behind_a_large_one_starts_before_the_large_one_finishes():
    bills = _Bills(LegBotLine(2))
    tasks = [bills.queue("large", f"L{i}") for i in range(10)] + [bills.queue("small", f"S{i}") for i in range(2)]
    await _settle()
    assert bills.started == ["L0", "L1"]
    await bills.finish("L0")
    await bills.finish("L1")
    # The two freed slots go one to each run, the small run first (the large run had both slots): not both to the
    # large run.
    assert bills.started[2:] == ["S0", "L2"]
    await bills.finish("S0")
    assert bills.started[-1] == "S1"  # served longer ago than the large run
    for name in list(bills.done):
        bills.done[name].set()
    await asyncio.gather(*tasks)
    assert sorted(bills.started) == sorted(bills.done)
    assert bills.line._free == 2 and bills.line._last_served == {}  # nothing left behind once the runs end


@pytest.mark.asyncio
async def test_turns_rotate_over_three_runs_and_each_runs_bills_keep_their_order():
    bills = _Bills(LegBotLine(1))
    tasks = [bills.queue(run, f"{run}{i}") for run in ("A", "B", "C") for i in range(3)]
    await _settle()
    for _ in range(8):
        await bills.finish(bills.started[-1])
    assert bills.started == ["A0", "B0", "C0", "A1", "B1", "C1", "A2", "B2", "C2"]
    await bills.finish("C2")
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_a_polled_bill_queued_after_a_thousand_unpolled_bills_starts_next():
    bills = _Bills(LegBotLine(2))
    tasks = [bills.queue("large", f"L{i}") for i in range(1000)]
    tasks.append(bills.queue("later", "POLLED", PRIORITY_POLLED))
    await _settle()
    assert bills.started == ["L0", "L1"]
    await bills.finish("L0")
    assert bills.started[2] == "POLLED"
    for name in list(bills.done):
        bills.done[name].set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_never_more_than_the_slots_and_a_cancelled_bill_gives_its_turn_away():
    line = LegBotLine(2)
    bills = _Bills(line)
    tasks = {name: bills.queue(run, name) for run, name in (("A", "A0"), ("A", "A1"), ("B", "B0"), ("C", "C0"))}
    await _settle()
    tasks["B0"].cancel()  # cancelled while waiting: skipped
    await _settle()
    await bills.finish("A0")
    assert bills.started == ["A0", "A1", "C0"]
    assert line._free == 0
    await bills.finish("A1")
    await bills.finish("C0")
    assert line._free == 2 and line.waiting() == 0


@pytest.mark.asyncio
async def test_a_slot_handed_to_a_bill_cancelled_at_that_moment_is_passed_on():
    line = LegBotLine(1)
    await line.acquire("A")
    loser = asyncio.create_task(line.acquire("B"))
    winner = asyncio.create_task(line.acquire("C"))
    await _settle()
    line.release()          # hands the slot to B's waiting future ...
    loser.cancel()          # ... and B is cancelled before it runs
    await _settle()
    assert winner.done() and not winner.cancelled()  # C got the slot B could not use
    assert line._free == 0
    line.release()
    assert line._free == 1


# --- through run_legbot_pipeline ---------------------------------------------------------------------------------


def _candidates(prefix, n):
    return [{"gov_id": f"{prefix} {i}", "bill_openstates_id": f"{prefix}-{i}", "live_url_fallback": ""} for i in range(n)]


def _settings():
    return SimpleNamespace(session_pipeline_concurrency=2, replica_freshness_check_enabled=False)


def _run(state, candidates):
    return run_legbot_pipeline(state, "2026", ["bill_summary"], False, None, include_concept_statements=False,
                               retry_failed=False, dry_run=True, bill_candidates=candidates)


@pytest.mark.asyncio
async def test_two_runs_share_the_slots_and_the_small_run_ends_first():
    order = []

    async def _bill(candidate, **kwargs):
        order.append(candidate["gov_id"])
        await asyncio.sleep(0.001)
        return {"gov_id": candidate["gov_id"], "bill_openstates_id": candidate["bill_openstates_id"]}

    with patch(f"{_RUNNER}._process_bill", new=_bill), patch(f"{_RUNNER}.get_settings", return_value=_settings()):
        large = asyncio.create_task(_run("la", _candidates("LA", 40)))
        await asyncio.sleep(0)
        small = asyncio.create_task(_run("us", _candidates("US", 3)))
        summary = await small
        assert not large.done(), "the small run waited for the whole large run"
        assert summary["bills_processed"] == 3 and len(summary["results"]) == 3  # its end-of-run summary is whole
        await large
    assert order.index("US 2") < order.index("LA 39")


@pytest.mark.asyncio
async def test_the_runs_polled_bills_come_from_the_broker_and_go_first(monkeypatch):
    order = []
    asked = []

    async def _polled(jurisdiction, **kwargs):
        asked.append(jurisdiction)
        return {"FL-30"}

    async def _bill(candidate, **kwargs):
        order.append(candidate["gov_id"])
        await asyncio.sleep(0.001)
        return {}

    monkeypatch.setattr(session_pipeline_runner, "list_polled_bill_ids", _polled)
    with patch(f"{_RUNNER}._process_bill", new=_bill), patch(f"{_RUNNER}.get_settings", return_value=_settings()):
        await _run("fl", _candidates("FL", 31))
    assert asked == ["fl"]
    assert order.index("FL 30") <= 2  # queued last, started with the first free slot


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [BrokerClientError("ddp-broker-py unreachable"), KeyError("openstates_id")])
async def test_a_broker_that_cannot_answer_marks_nothing_and_the_run_goes_on(monkeypatch, error):
    async def _down(*args, **kwargs):
        raise error

    monkeypatch.setattr(session_pipeline_runner, "list_polled_bill_ids", _down)
    with patch(f"{_RUNNER}._process_bill", new=AsyncMock(return_value={})), \
            patch(f"{_RUNNER}.get_settings", return_value=_settings()):
        summary = await _run("fl", _candidates("FL", 4))
    assert summary["bills_processed"] == 4


@pytest.mark.asyncio
async def test_a_staff_single_bill_run_does_not_wait_while_the_line_is_full():
    with patch(f"{_RUNNER}.get_settings", return_value=_settings()):
        line = session_pipeline_runner.get_legbot_line()
        await line.acquire("busy-1")
        await line.acquire("busy-2")
        assert line._free == 0
        with patch(f"{_RUNNER}._process_bill", new=AsyncMock(return_value={"gov_id": "HB 1"})) as bill:
            result = await asyncio.wait_for(run_single_bill_full(
                bill_openstates_id="ocd-bill/1", jurisdiction_iso2="fl", session_code="2026", gov_id="HB 1",
                bill_source="https://example.org/hb1", artifact_types=["bill_summary"], include_org_research=False,
                include_concept_statements=False, retry_failed=False), timeout=1)
    bill.assert_awaited_once()
    assert result["gov_id"] == "HB 1" and line._free == 0


# --- the broker read -----------------------------------------------------------------------------------------------


def _broker(pages):
    """An httpx.AsyncClient stand-in answering GET /api/bills/ with the given page bodies (or status codes) in turn."""
    client = MagicMock()
    answers = []
    for body in pages:
        resp = MagicMock()
        resp.status_code, resp.text = (body, "nope") if isinstance(body, int) else (200, "")
        resp.json.return_value = body
        answers.append(resp)
    client.get = AsyncMock(side_effect=answers)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("ddp_sync.services.broker_client.httpx.AsyncClient", return_value=cm), client


@pytest.mark.asyncio
async def test_the_polled_bills_are_read_page_by_page_by_number():
    page1 = {"count": 201, "next": "http://localhost/api/bills/?page=2", "results":
             [{"openstates_id": f"ocd-bill/{i}"} for i in range(200)]}
    page2 = {"count": 201, "next": None, "results": [{"openstates_id": "ocd-bill/200"}, {"openstates_id": ""}]}
    patcher, client = _broker([page1, page2])
    with patcher:
        ids = await broker_client.list_polled_bill_ids("us", broker_api_base="http://broker", broker_api_token="t")
    assert ids == {f"ocd-bill/{i}" for i in range(201)}
    calls = client.get.await_args_list
    assert [c.args[0] for c in calls] == ["http://broker/api/bills/", "http://broker/api/bills/"]  # never `next`
    assert [c.kwargs["params"] for c in calls] == [
        {"has_poll": "true", "jurisdiction": "US", "page": p, "page_size": 200} for p in (1, 2)]
    assert calls[0].kwargs["headers"] == {"Authorization": "Bearer t"}


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", [500, {"results": "x", "count": 1}, {"detail": "no"}])
async def test_a_bad_polled_bill_answer_is_a_broker_error(answer):
    patcher, _ = _broker([answer])
    with patcher, pytest.raises(BrokerClientError):
        await broker_client.list_polled_bill_ids("us", broker_api_base="http://broker", broker_api_token="t")


@pytest.mark.asyncio
async def test_more_polled_bills_than_the_page_limit_is_an_error_not_a_partial_list(monkeypatch):
    monkeypatch.setattr(broker_client, "_POLLED_MAX_PAGES", 2)
    page = {"count": 1000, "results": [{"openstates_id": f"ocd-bill/{i}"} for i in range(200)]}
    patcher, client = _broker([page, page])
    with patcher, pytest.raises(BrokerClientError, match="more polled bills"):
        await broker_client.list_polled_bill_ids("us", broker_api_base="http://broker", broker_api_token="t")
    assert client.get.await_count == 2


@pytest.mark.asyncio
async def test_a_bill_cancelled_while_working_frees_its_slot_once():
    line = LegBotLine(1)
    started = asyncio.Event()

    async def bill():
        async with line.slot("A"):
            started.set()
            await asyncio.sleep(10)

    working = asyncio.create_task(bill())
    await started.wait()
    waiting = asyncio.create_task(line.acquire("B"))
    await _settle()
    working.cancel()
    await _settle()
    assert waiting.done() and line._free == 0  # the slot went to B, once
    line.release()
    assert line._free == 1
