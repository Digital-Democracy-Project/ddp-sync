"""SYNC-199: the duplicate judge in the policy_statement step.

A draft Jev scores under the cutoff but with close Policies goes to LegBot's `resolve_policy` (AGENTS-272); the answer files
the draft under an existing Policy, creates it with a "replaces" marker, or creates it as before (BROKER-610 stores the
judge's answer and does the merge). LegBot and the broker's write are STAND-INS here, built to the ticket's stated contracts:
neither AGENTS-272 nor BROKER-610 was merged when this was written.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from ddp_sync.config import SyncSettings, _judge_shortlist
from ddp_sync.pipelines import policy_statement_dispatch as psd
from ddp_sync.pipelines import session_pipeline_runner as spr
from ddp_sync.services.legbot_client import LegBotDispatchError
from tests.test_sync_183_policy_statement import (  # noqa: F401  (pipeline is a fixture reused below)
    _CAND,
    FakeBroker,
    FakeJev,
    JevError,
    _answer,
    _concepts,
    _draft,
    _work,
    pipeline,
    run_step,
)

JUDGE = {"judge_id": "resolve_policy:c3:abcdef0123456789", "prompt_version": "c3", "prompt_hash": "abcdef0123456789",
         "model_name": "claude-haiku", "reason": "same position", "unusable": False}


def _pol(i, label=None):
    return {"id": i, "label": label or f"P{i}", "canonical_statement": "", "definition": "", "exclusions": "", "concept_id": 1}


class JudgeBroker(FakeBroker):
    """The BROKER-610 write as the ticket states it: accepts `judge` and, for a created ending, `replaces`; answers with
    `created_policy_id`. `with_id=False` models an answer that lacks it."""

    def __init__(self, *a, with_id=True, **k):
        super().__init__(*a, **k)
        self.with_id = with_id

    async def write(self, payload, **kw):
        if "replaces" in payload:
            assert payload["resolution"] == "created" and payload["judge"]["survivor"] == "draft"
            assert isinstance(payload["replaces"], int) and payload["replaces"] == payload["judge"]["choice"]
        response = await super().write(payload, **kw)
        response["created_policy_id"] = self.policies[-1]["id"] if payload["resolution"] == "created" and self.with_id else None
        return response


@pytest.fixture
def judged(run_step, monkeypatch):
    """judged(broker, jev, answers, drafts, shortlist=5, floor=0.3) -> (result, resolve_policy mock). `answers` is a list
    of replies (or exceptions) in order, or a callable (draft, candidates) -> reply."""

    async def go(broker, jev, answers, drafts, *, shortlist=5, floor=0.30):
        if callable(answers):
            async def stand_in(draft, candidates, **kw):
                return answers(draft, candidates)
            resolve = AsyncMock(side_effect=stand_in)
        else:
            resolve = AsyncMock(side_effect=answers)
        monkeypatch.setattr(psd, "dispatch_resolve_policy", resolve)
        # run_step builds its settings with the 183 module's SyncSettings name: carry the judge values through it
        monkeypatch.setattr("tests.test_sync_183_policy_statement.SyncSettings", lambda: SyncSettings(
            policy_statement_judge_shortlist=shortlist, policy_statement_judge_floor=floor))
        result, _ = await run_step(broker, _answer(drafts), jev)
        return result, resolve

    return go


def _reply(choice, survivor, **kw):
    return {"answer": {"choice": choice, "survivor": survivor, **JUDGE, **kw}, "backend": "claude"}


def _close(draft, scores):
    """A Jev table giving `draft` the stated score against each Policy `P<id>`."""
    return {(psd.draft_query(draft), f"P{i}"): s for i, s in scores.items()}


# ---- judge off -----------------------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_with_the_shortlist_at_zero_nothing_is_judged_and_the_write_is_as_before(judged):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a"]), [_pol(1), _pol(2)], _concepts())
    result, resolve = await judged(broker, FakeJev(_close(d, {1: 0.6, 2: 0.5})), [], [d], shortlist=0)
    resolve.assert_not_awaited()
    assert result == {"statements": 1, "matched": 0, "created": 1, "excluded": 0, "skipped_reason": None}
    assert "judge" not in broker.writes[0] and "replaces" not in broker.writes[0]


# ---- the three answers ---------------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_existing_files_the_draft_under_the_chosen_policy_with_the_judges_answer(judged):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a"]), [_pol(1), _pol(2), _pol(3)], _concepts())
    jev = FakeJev(_close(d, {1: 0.2, 2: 0.6, 3: 0.5}))
    result, resolve = await judged(broker, jev, [_reply(3, "existing")], [d])
    (shown_draft, shown), _ = resolve.await_args
    assert [c["id"] for c in shown] == [2, 3]  # best first, 1 is under the 0.30 floor
    assert shown_draft == {"label": "Draft label", "statement": "Draft statement.", "definition": "Draft definition.", "exclusions": "Not that."}
    assert set(shown[0]) == {"id", "label", "canonical_statement", "definition", "exclusions"}
    (w,) = broker.writes
    assert (w["resolution"], w["policy_id"], w["created"]) == ("matched", 3, None)
    assert "replaces" not in w and w["judge"]["choice"] == 3 and w["judge"]["survivor"] == "existing"
    assert w["judge"]["candidates"] == [2, 3] and w["judge"]["judge_id"] == JUDGE["judge_id"] and w["judge"]["unusable"] is False
    assert w["scores"]["best_policy_id"] == 2  # Jev's own best is kept in the scores
    assert result["judged"] == 1 and result["judge_filed"] == 1 and result["judge_replaced"] == 0 and result["judge_none"] == 0
    assert result["matched"] == 1 and result["created"] == 0
    assert broker.concept_reads == 0  # no new Policy, so no Concept pick


@pytest.mark.asyncio
async def test_draft_creates_the_policy_and_marks_the_one_it_replaces(judged):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a"]), [_pol(1), _pol(2)], _concepts())
    result, _ = await judged(broker, FakeJev(_close(d, {1: 0.6, 2: 0.4})), [_reply(2, "draft")], [d])
    (w,) = broker.writes
    assert w["resolution"] == "created" and w["created"]["label"] == "Draft label" and w["replaces"] == 2
    assert w["judge"]["survivor"] == "draft" and w["judge"]["choice"] == 2
    assert (result["judged"], result["judge_filed"], result["judge_replaced"], result["judge_none"], result["created"]) == (1, 0, 1, 0, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("reply", [_reply(None, None), _reply(None, None, unusable=True, reason="")])
async def test_none_or_an_unusable_reply_creates_as_today_and_carries_the_answer(judged, reply):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a"]), [_pol(1)], _concepts())
    result, _ = await judged(broker, FakeJev(_close(d, {1: 0.6})), [reply], [d])
    (w,) = broker.writes
    assert w["resolution"] == "created" and "replaces" not in w
    assert w["judge"]["choice"] is None and w["judge"]["survivor"] is None and w["judge"]["unusable"] is reply["answer"]["unusable"]
    assert (result["judged"], result["judge_none"], result["created"]) == (1, 1, 1)


@pytest.mark.asyncio
async def test_a_draft_with_no_policy_at_the_floor_is_not_judged(judged):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a"]), [_pol(1)], _concepts())
    result, resolve = await judged(broker, FakeJev(_close(d, {1: 0.29})), [], [d])
    resolve.assert_not_awaited()
    assert result["created"] == 1 and result["judged"] == 0 and "judge" not in broker.writes[0]


@pytest.mark.asyncio
async def test_the_floor_is_inclusive_and_a_match_over_the_cutoff_is_never_judged(judged):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a"]), [_pol(1)], _concepts())
    _, resolve = await judged(broker, FakeJev(_close(d, {1: 0.30})), [_reply(None, None)], [d])
    assert resolve.await_count == 1
    broker2 = JudgeBroker(_work(["a"]), [_pol(1)], _concepts())
    result, resolve2 = await judged(broker2, FakeJev(_close(d, {1: 0.7})), [], [d])
    resolve2.assert_not_awaited()
    assert broker2.writes[0]["resolution"] == "matched" and "judge" not in broker2.writes[0] and result["judged"] == 0


@pytest.mark.asyncio
async def test_not_a_position_is_excluded_and_not_judged(judged):
    broker = JudgeBroker(_work(["a"]), [_pol(1)], _concepts())
    _, resolve = await judged(broker, FakeJev(), [], [_draft(n=1, not_a_position=True)])
    resolve.assert_not_awaited()
    assert broker.writes[0]["resolution"] == "excluded"


@pytest.mark.asyncio
async def test_at_most_five_candidates_and_the_setting_lowers_it(judged):
    d = _draft(n=1)
    pols = [_pol(i) for i in range(1, 9)]
    scores = {i: 0.3 + i / 100 for i in range(1, 9)}
    broker = JudgeBroker(_work(["a"]), pols, _concepts())
    _, resolve = await judged(broker, FakeJev(_close(d, scores)), [_reply(None, None)], [d])
    assert [c["id"] for c in resolve.await_args.args[1]] == [8, 7, 6, 5, 4]
    broker2 = JudgeBroker(_work(["a"]), pols, _concepts())
    _, resolve2 = await judged(broker2, FakeJev(_close(d, scores)), [_reply(None, None)], [d], shortlist=2)
    assert [c["id"] for c in resolve2.await_args.args[1]] == [8, 7]


@pytest.mark.asyncio
async def test_the_shortlist_comes_from_the_same_jev_calls(judged):
    d = _draft(n=1)
    jev = FakeJev(_close(d, {1: 0.6, 2: 0.5}))
    broker = JudgeBroker(_work(["a"]), [_pol(1), _pol(2)], _concepts())
    await judged(broker, jev, [_reply(None, None)], [d])
    off_jev = FakeJev(_close(d, {1: 0.6, 2: 0.5}))
    broker2 = JudgeBroker(_work(["a"]), [_pol(1), _pol(2)], _concepts())
    await judged(broker2, off_jev, [], [d], shortlist=0)
    assert len(jev.calls) == len(off_jev.calls)  # the judge adds no Jev call


# ---- failures ------------------------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_failed_judge_call_fails_the_bill_writes_nothing_for_that_draft_and_counts_as_legbot(judged):
    d1, d2 = _draft(n=1, label="One"), _draft(n=2, label="Two")
    table = {**_close(d1, {1: 0.6}), **_close(d2, {1: 0.6})}
    broker = JudgeBroker(_work(["a", "b"]), [_pol(1)], _concepts())
    with pytest.raises(LegBotDispatchError) as raised:
        await judged(broker, FakeJev(table), [_reply(None, None), LegBotDispatchError("timed out")], [d1, d2])
    assert [w["statement_index"] for w in broker.writes] == [0]  # the draft whose judge failed has no row: offered again
    assert spr._failure_source(raised.value) == "legbot"


@pytest.mark.asyncio
@pytest.mark.parametrize("reply", [_reply(99, "existing"), _reply(1, None), _reply(1, "both"), _reply([1], "existing"),
                                   _reply(True, "existing"), {"backend": "claude"}, {"answer": "choice 1"}, {"answer": None}])
async def test_an_answer_naming_a_policy_it_was_not_shown_or_no_survivor_fails_instead_of_reading_as_none(judged, reply):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a"]), [_pol(1)], _concepts())
    with pytest.raises(LegBotDispatchError):
        await judged(broker, FakeJev(_close(d, {1: 0.6})), [reply], [d])
    assert broker.writes == []


# ---- two drafts of one bill ----------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_two_near_identical_drafts_in_one_bill_do_not_both_create_a_policy(judged):
    d1, d2 = _draft(n=1, label="Same"), _draft(n=2, label="Same")
    broker = JudgeBroker(_work(["a", "b"]), [_pol(1)], _concepts())
    jev = FakeJev({(psd.draft_query(d1), "P1"): 0.5})

    seen = []

    def judge(draft, candidates):
        seen.append([c["id"] for c in candidates])
        new = [c for c in candidates if c["id"] > 9000]
        return _reply(new[0]["id"], "existing") if new else _reply(None, None)

    # the second draft reads the first's new Policy as close (its text is the first draft's label, statement, definition)
    created_text = psd.policy_text({"label": "Same", "canonical_statement": "Draft statement.", "definition": "Draft definition.",
                                    "exclusions": "Not that."})
    jev.table[(psd.draft_query(d2), created_text)] = 0.55
    result, resolve = await judged(broker, jev, judge, [d1, d2])
    assert [w["resolution"] for w in broker.writes] == ["created", "matched"]
    assert broker.writes[1]["policy_id"] == broker.policies[-1]["id"] == 9001
    assert seen[0] == [1] and 9001 in seen[1]
    assert (result["created"], result["matched"], result["judged"], result["judge_filed"]) == (1, 1, 2, 1)


@pytest.mark.asyncio
async def test_a_replacing_draft_takes_the_replaced_policy_out_of_the_bills_list(judged):
    d1, d2 = _draft(n=1, label="Same"), _draft(n=2, label="Same")
    broker = JudgeBroker(_work(["a", "b"]), [_pol(1)], _concepts())
    jev = FakeJev({(psd.draft_query(d1), "P1"): 0.5, (psd.draft_query(d2), "P1"): 0.5})
    _, resolve = await judged(broker, jev, [_reply(1, "draft"), _reply(None, None)], [d1, d2])
    # d2 sees neither P1 (replaced) nor, with no score for it, the new Policy: nothing at the floor, so no second judge call
    assert resolve.await_count == 1


@pytest.mark.asyncio
async def test_an_answer_without_the_new_policys_id_is_handled_and_the_list_is_left_alone(judged):
    d1, d2 = _draft(n=1, label="Same"), _draft(n=2, label="Same")
    broker = JudgeBroker(_work(["a", "b"]), [_pol(1)], _concepts(), with_id=False)
    jev = FakeJev({(psd.draft_query(d1), "P1"): 0.5, (psd.draft_query(d2), "P1"): 0.5})
    result, resolve = await judged(broker, jev, [_reply(None, None), _reply(None, None)], [d1, d2])
    assert [c["id"] for c in resolve.await_args_list[1].args[1]] == [1]  # the second draft sees only the original list
    assert result["created"] == 2  # and both create: the duplicate is the broker's to catch (BROKER-610)


@pytest.mark.asyncio
async def test_the_judge_off_path_does_not_grow_the_in_bill_list(judged):
    d = _draft(n=1)
    broker = JudgeBroker(_work(["a", "b"]), [], _concepts())
    jev = FakeJev()
    await judged(broker, jev, [], [d, {**d, "n": 2}], shortlist=0)
    assert [w["scores"]["list_size"] for w in broker.writes] == [0, 0]


def test_created_policy_id_reads_the_ticket_shape_and_the_existing_one():
    assert psd._created_policy_id({"created_policy_id": 7}) == 7
    assert psd._created_policy_id({"created": {"concept_id": 1, "policy_id": 8}}) == 8
    assert psd._created_policy_id({"created": {"policy_id": None}}) is None
    assert psd._created_policy_id({"created_policy_id": True}) is None and psd._created_policy_id(None) is None


# ---- settings, client and summary ----------------------------------------------------------------------------------

def test_the_judge_settings_defaults_and_fail_safe_parsing(monkeypatch):
    s = SyncSettings()
    assert (s.policy_statement_judge_shortlist, s.policy_statement_judge_floor) == (5, 0.30)
    assert _judge_shortlist(None) == 5 and _judge_shortlist("") == 5 and _judge_shortlist("0") == 0 and _judge_shortlist("3") == 3
    assert _judge_shortlist("abc") == 5 and _judge_shortlist("-1") == 5 and _judge_shortlist("9") == 5


@pytest.mark.asyncio
async def test_dispatch_resolve_policy_sends_the_stated_payload(monkeypatch):
    from ddp_sync.services import legbot_client

    sent = AsyncMock(return_value={"answer": {}, "backend": "claude"})
    monkeypatch.setattr(legbot_client, "_dispatch_and_await", sent)
    await legbot_client.dispatch_resolve_policy({"label": "l"}, [{"id": 1}])
    assert sent.await_args.args[0] == {"question_type": "resolve_policy", "caller": "ddp_sync", "draft": {"label": "l"},
                                       "candidates": [{"id": 1}]}
    assert sent.await_args.kwargs["question_type"] == "resolve_policy"


@pytest.mark.asyncio
async def test_the_judge_counts_reach_the_bills_result(pipeline):
    run, step = pipeline
    step.return_value = {"statements": 3, "matched": 1, "created": 2, "excluded": 0, "judged": 3, "judge_filed": 1,
                         "judge_replaced": 1, "judge_none": 1, "skipped_reason": None}
    r = (await run(include_policy_statement=True))["results"][0]
    assert (r["policy_statement_judged"], r["policy_statement_judge_filed"], r["policy_statement_judge_replaced"],
            r["policy_statement_judge_none"]) == (3, 1, 1, 1)
    step.return_value = {"statements": 1, "matched": 0, "created": 1, "excluded": 0, "skipped_reason": None}
    r = (await run(include_policy_statement=True))["results"][0]
    assert r["policy_statement_judged"] == 0  # judge off: the step's result has no judge keys


@pytest.mark.asyncio
async def test_the_cross_chunk_winner_is_always_in_the_shortlist_first():
    """Two chunks of Policies: the second chunk's best raw score is lower than six scores in the first chunk, but the final
    comparison of the chunk winners picks it. The judge must still be shown it, first."""
    items = [_pol(i) for i in range(1, 202)]  # ids 1..200 are chunk one, 201 is chunk two alone
    query = "q"

    class TwoStageJev(FakeJev):
        async def score(self, text, candidates):
            self.calls.append((text, list(candidates)))
            if len(candidates) == 2:  # the final comparison of the two chunk winners
                return [0.1, 0.9]
            if len(candidates) == 1:  # chunk two: a lone Policy
                return [0.35]
            return [0.9 - i / 1000 for i in range(len(candidates))]  # chunk one: P1 best ... descending

    best, score, ranked = await psd.rank_items(TwoStageJev(), query, items, psd.policy_text)
    assert (best, score) == (201, 0.9) and ranked[0] == (0.9, 201)
    assert len({i for _, i in ranked}) == 201  # nothing dropped or doubled
    shown = psd._judge_candidates(ranked, items, 5, 0.30)
    assert shown[0]["id"] == 201 and len(shown) == 5
