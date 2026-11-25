"""SYNC-183: the policy_statement step of the LegBot session pipeline.

Everything outside this repo is faked: LegBot (`dispatch_bill_question`), the broker's policy-matching routes and Jev. The one
real thing under test is ddp-sync's own logic: what text Jev is shown, the cutoff, the choice of Concept, what is written, and
how the step sits in `run_legbot_pipeline`.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from ddp_sync.config import SyncSettings
from ddp_sync.pipelines import policy_statement_dispatch as psd
from ddp_sync.pipelines.session_pipeline_runner import run_legbot_pipeline
from ddp_sync.services import broker_client, jev_client
from ddp_sync.services.broker_client import BrokerClientError
from ddp_sync.services.jev_client import JevError, JevScorer
from ddp_sync.services.legbot_client import LegBotDispatchError, dispatch_bill_question

CUTOFF = 0.7
_FIXTURE = Path(__file__).parent / "fixtures" / "policy_statement_scale100.json.gz"


# --------------------------------------------------------------------------------------------------------------------
# The text Jev is shown (ddp-agents scripts/policy_walk_whole_list.py, variant "exclusions")
# --------------------------------------------------------------------------------------------------------------------

def test_policy_text_has_the_exclusions_suffix_only_when_there_are_exclusions():
    p = {"label": "L", "canonical_statement": "S", "definition": "D", "exclusions": "X"}
    assert psd.policy_text(p) == "L: S: D Does not cover: X"
    assert psd.policy_text({**p, "exclusions": ""}) == "L: S: D"
    assert psd.policy_text({"label": "L", "canonical_statement": "", "definition": "D"}) == "L: D"


def test_concept_text_is_label_definition_and_exclusions():
    c = {"label": "L", "definition": "D", "exclusions": "X"}
    assert psd.concept_text(c) == "L: D Does not cover: X"
    assert psd.concept_text({"label": "L", "definition": "D", "exclusions": ""}) == "L: D"


def test_draft_query_is_label_statement_definition_and_subject():
    d = {"label": "L", "statement": "S", "definition": "D", "subject": "Sub"}
    assert psd.draft_query(d) == "L: S D (subject: Sub)"
    assert psd.draft_query({**d, "subject": ""}) == "L: S D"
    assert psd.draft_query({"label": "L", "statement": "S"}) == "L: S"


# --------------------------------------------------------------------------------------------------------------------
# The whole-list comparison
# --------------------------------------------------------------------------------------------------------------------

class FakeJev:
    """Scores a candidate from a table keyed on its text, or on (query, candidate text); default 0.0. Records every call."""

    name = "typesafe:fake"

    def __init__(self, table=None):
        self.table = table or {}
        self.calls: list[tuple[str, list[str]]] = []
        self.input_tokens = 0
        self.cost_usd = 0.0

    async def score(self, text, candidates):
        self.calls.append((text, list(candidates)))
        return [self.table.get((text, c), self.table.get(c, 0.0)) for c in candidates]


def _policies(n, start=1):
    return [{"id": start + i, "label": f"P{start + i}", "canonical_statement": "", "definition": "", "exclusions": ""}
            for i in range(n)]


@pytest.mark.asyncio
async def test_best_of_asks_in_chunks_of_200_then_compares_the_winners():
    items = _policies(450)
    jev = FakeJev({"P37": 0.4, "P250": 0.9, "P401": 0.5})
    best, score = await psd.best_of(jev, "q", items, psd.policy_text)
    assert (best, score) == (250, 0.9)
    sizes = [len(c) for _, c in jev.calls]
    assert sorted(sizes[:3]) == [50, 200, 200]  # the three chunks (order among them is not fixed)
    assert sizes[3] == 3  # then the three chunk winners against each other
    assert sorted(jev.calls[3][1]) == ["P250", "P37", "P401"]


@pytest.mark.asyncio
async def test_best_of_with_one_chunk_is_one_call_and_its_score():
    jev = FakeJev({"P3": 0.8})
    assert await psd.best_of(jev, "q", _policies(5), psd.policy_text) == (3, 0.8)
    assert len(jev.calls) == 1


@pytest.mark.asyncio
async def test_best_of_empty_list_is_none_and_asks_nothing():
    jev = FakeJev()
    assert await psd.best_of(jev, "q", [], psd.policy_text) == (None, 0.0)
    assert jev.calls == []


@pytest.mark.asyncio
async def test_best_of_tie_goes_to_the_larger_id_like_the_measured_script():
    jev = FakeJev({"P1": 0.5, "P2": 0.5})
    assert await psd.best_of(jev, "q", _policies(2), psd.policy_text) == (2, 0.5)


# --------------------------------------------------------------------------------------------------------------------
# The cutoff
# --------------------------------------------------------------------------------------------------------------------

def _draft(**kw):
    return {"n": 1, "not_a_position": False, "subject": "s", "label": "Draft label", "statement": "Draft statement.",
            "definition": "Draft definition.", "exclusions": "Not that.", **kw}


@pytest.mark.asyncio
async def test_match_draft_cutoff_is_inclusive_and_below_it_is_a_new_policy():
    pol = _policies(3)
    q = psd.draft_query(_draft())
    for score, expected in ((0.7, "matched"), (0.7001, "matched"), (0.6999, "created"), (0.0, "created")):
        jev = FakeJev({"P2": score})
        resolution, best, got = await psd.match_draft(jev, _draft(), pol, CUTOFF)
        assert (resolution, got) == (expected, score) and jev.calls[0][0] == q
        assert best == (2 if score else 3)  # all-zero ties go to the larger id


@pytest.mark.asyncio
async def test_match_draft_uses_the_cutoff_it_is_given():
    jev = FakeJev({"P2": 0.8})
    assert (await psd.match_draft(jev, _draft(), _policies(3), 0.9))[0] == "created"
    assert (await psd.match_draft(jev, _draft(), _policies(3), 0.8))[0] == "matched"


@pytest.mark.asyncio
async def test_match_draft_not_a_position_is_excluded_without_asking_jev():
    jev = FakeJev()
    assert await psd.match_draft(jev, _draft(not_a_position=True), _policies(3), CUTOFF) == ("excluded", None, 0.0)
    assert jev.calls == []


@pytest.mark.asyncio
async def test_an_empty_policy_list_means_a_new_policy():
    resolution, best, score = await psd.match_draft(FakeJev(), _draft(), [], CUTOFF)
    assert (resolution, best, score) == ("created", None, 0.0)


def test_the_cutoff_is_a_setting_with_the_measured_default(monkeypatch):
    assert SyncSettings().policy_statement_match_cutoff == 0.7
    from ddp_sync.config import _unit_interval
    assert _unit_interval(None, default=0.7, name="X") == 0.7
    assert _unit_interval("0.85", default=0.7, name="X") == 0.85
    for bad in ("abc", "1.5", "-0.1"):
        with pytest.raises(ValueError, match="X"):
            _unit_interval(bad, default=0.7, name="X")


# --------------------------------------------------------------------------------------------------------------------
# Reproduction: the saved Gemma drafts of ddp-agents bench/data/scale100 against the 533 saved Policies
# --------------------------------------------------------------------------------------------------------------------

def _load_fixture():
    with gzip.open(_FIXTURE, "rt") as fh:
        return json.load(fh)


class ReplayJev:
    """Answers with the scores a saved Jev run recorded. A saved run keeps, per draft, the best Policy of each 200-Policy chunk
    with its score and the best of those winners with its score, so a chunk call is answered with the chunk winner's saved
    score (0 for the rest) and the final call with the saved best's score (the other winners strictly below it)."""

    name = "typesafe:replay"
    input_tokens = 0
    cost_usd = 0.0

    def __init__(self, policies, cases, run):
        self.id_of_text = {psd.policy_text(p): p["id"] for p in policies}
        self.case_of_query = {c["query"]: c for c in cases if c["query"]}
        self.run = run
        self.policy_ids = [p["id"] for p in policies]

    async def score(self, text, candidates):
        saved = self.case_of_query[text][self.run]
        ids = [self.id_of_text[c] for c in candidates]
        winners = {w: s for w, s in saved["chunk_winners"]}
        if all(i in winners for i in ids) and set(ids) == set(winners):  # the final call: the winners against each other
            return [saved["p"] if i == saved["policy"] else min(winners[i], saved["p"] - 1e-9) for i in ids]
        chunk_winner = next(w for w in winners if w in ids)  # a chunk call
        return [winners[chunk_winner] if i == chunk_winner else 0.0 for i in ids]


def _saved_decision(whole):
    if whole.get("excluded"):
        return ("excluded", None)
    return ("matched", whole["policy"]) if whole["p"] >= CUTOFF else ("created", None)


@pytest.mark.asyncio
async def test_reproduces_the_saved_decisions_of_the_scale100_runs():
    fx = _load_fixture()
    policies, cases = fx["policies"], fx["cases"]
    assert len(policies) == 533 and len(cases) == 495
    # The texts Jev is shown are the saved script's, character for character (hash made with that script's own logic).
    assert hashlib.sha256("\n".join(psd.policy_text(p) for p in policies).encode()).hexdigest() == fx["policy_texts_sha256"]
    assert all(psd.draft_query(c["draft"]) == c["query"] for c in cases if not c["draft"]["not_a_position"])

    mismatches = {"run1": 0, "run2": 0}
    decided = {"matched": 0, "created": 0, "excluded": 0}
    replay = ReplayJev(policies, cases, "run1")
    for c in cases:
        resolution, best, _ = await psd.match_draft(replay, c["draft"], policies, CUTOFF)
        decided[resolution] += 1
        got = (resolution, best if resolution == "matched" else None)
        for run in ("run1", "run2"):
            mismatches[run] += got != _saved_decision(c[run])
    # Replaying run 1 gives run 1 exactly; the second saved run differs only by Jev's own run-to-run noise (2 to 4 of 495).
    assert mismatches["run1"] == 0
    assert mismatches["run2"] <= 5
    assert decided == {"matched": 24, "created": 384, "excluded": 87}


@pytest.mark.asyncio
async def test_reproduction_notices_a_cutoff_that_moved():
    fx = _load_fixture()
    policies, cases = fx["policies"], fx["cases"]
    replay = ReplayJev(policies, cases, "run1")
    off = 0
    for c in cases:
        resolution, best, _ = await psd.match_draft(replay, c["draft"], policies, 0.5)
        off += (resolution, best if resolution == "matched" else None) != _saved_decision(c["run1"])
    assert off > 5


# --------------------------------------------------------------------------------------------------------------------
# The per-bill step with LegBot, the broker and Jev faked
# --------------------------------------------------------------------------------------------------------------------

def _broker_would_accept(p):
    """The broker's own checks of a draft write, copied from ddp-broker-py PR #832 `common/policy_matching.write_draft` and
    `_statement` (so a payload this side builds that the broker would refuse with a 400 fails here)."""
    assert p["resolution"] in ("matched", "created", "excluded")
    assert isinstance(p["concept_statement_set_id"], int) and isinstance(p["statement_index"], int)
    assert isinstance(p["draft"], dict) and isinstance(p["scores"], dict) and isinstance(p.get("reason") or "", str)
    assert len(p["model_name"]) <= 100 and len(p["prompt_version"]) <= 50 and len(p["prompt_hash"]) <= 64
    if p["resolution"] == "matched":
        assert isinstance(p["policy_id"], int) and p["created"] is None
    elif p["resolution"] == "created":
        c = p["created"]
        assert p["policy_id"] is None and isinstance(c, dict) and isinstance(c["concept_id"], int)
        assert all(isinstance(c[k], str) and c[k].strip() for k in ("label", "canonical_statement", "definition"))
        assert isinstance(c.get("exclusions") or "", str) and len(c["label"].strip()) <= 200
    else:
        assert p["policy_id"] is None and p["created"] is None and p["reason"].strip()


class FakeBroker:
    """The three broker reads and the write. A `created` write adds a Policy to the list the next read returns."""

    def __init__(self, work, policies, concepts):
        self.work, self.policies, self.concepts = work, policies, concepts
        self.writes: list[dict] = []
        self.policy_reads = 0
        self.concept_reads = 0
        self.work_kwargs: dict | None = None

    async def list_work(self, **kwargs):
        self.work_kwargs = kwargs
        return list(self.work)

    async def list_policies(self, **kw):
        self.policy_reads += 1
        return list(self.policies)

    async def list_concepts(self, **kw):
        self.concept_reads += 1
        return list(self.concepts)

    async def write(self, payload, **kw):
        _broker_would_accept(payload)
        self.writes.append(payload)
        if payload["resolution"] == "created":
            self.policies.append({"id": 9000 + len(self.writes), "concept_id": payload["created"]["concept_id"],
                                  "canonical_statement": payload["created"]["canonical_statement"],
                                  **{k: payload["created"][k] for k in ("label", "definition", "exclusions")}})
        # the broker's real response shape (BROKER-598 `_response`)
        return {"match": {"id": len(self.writes), "concept_statement_set_id": payload["concept_statement_set_id"],
                          "statement_index": payload["statement_index"], "resolution": payload["resolution"],
                          "policy_id": payload["policy_id"], "accepted": payload["resolution"] != "excluded"},
                "created": {"concept_id": None, "policy_id": None}, "replayed": False}


def _work(texts, gov_id="HB 1", session="2026", set_id=7, indices=None):
    return [{"concept_statement_set_id": set_id, "statement_index": (indices or range(len(texts)))[i], "statement_text": t,
             "jurisdiction": "FL", "session": session, "gov_id": gov_id, "why": "new"} for i, t in enumerate(texts)]


def _answer(drafts, **kw):
    return {"answer": {"drafts": drafts, "source_support": "direct", "insufficient_information": False,
                       "judge_id": "policy_statement:p1r1:0123456789abcdef", "prompt_version": "1",
                       "model_name": "mlx-community/gemma-4-26b-a4b-it-8bit", **kw}, "backend": "mlx"}


def _concepts(n=3):
    return [{"id": 100 + i, "label": f"C{i}", "definition": f"cd{i}", "exclusions": "", "topic_id": 1} for i in range(n)]


@pytest.fixture
def run_step(monkeypatch):
    """run_step(broker, legbot_result, jev, **kwargs) -> (result, dispatch mock). Everything outside ddp-sync is replaced."""

    async def run(broker, legbot, jev, *, dry_run=False, gov_id="HB 1", source="archived text", no_key=False):
        dispatch = AsyncMock(side_effect=legbot if isinstance(legbot, list) else None,
                             return_value=None if isinstance(legbot, list) else legbot)
        monkeypatch.setattr(psd, "get_settings", lambda: SyncSettings())
        monkeypatch.setattr(psd, "list_policy_statement_work", broker.list_work)
        monkeypatch.setattr(psd, "list_policy_matching_policies", broker.list_policies)
        monkeypatch.setattr(psd, "list_policy_matching_concepts", broker.list_concepts)
        monkeypatch.setattr(psd, "write_policy_statement_match", broker.write)
        monkeypatch.setattr(psd, "dispatch_bill_question", dispatch)
        monkeypatch.setattr(psd, "_resolve_bill_source", AsyncMock(return_value=source))

        class _Jev:
            def __init__(self):
                if no_key:
                    raise JevError("TYPESAFE_API_KEY is not set")

            async def __aenter__(self):
                return jev

            async def __aexit__(self, *a):
                return False

        monkeypatch.setattr(psd, "JevScorer", _Jev)
        result = await psd.dispatch_and_store_policy_statements(
            gov_id=gov_id, bill_openstates_id="ocd-bill/x", jurisdiction_iso2="FL", session_code="2026", dry_run=dry_run)
        return result, dispatch

    return run


@pytest.mark.asyncio
async def test_matched_excluded_and_new_are_written_with_the_scores_and_the_writers_judge_id(run_step):
    pol = [{"id": 5, "label": "Ban X", "canonical_statement": "Ban X.", "definition": "def", "exclusions": "",
            "concept_id": 1, "definition_version": 1}]
    broker = FakeBroker(_work(["s one", "s two", "s three"], indices=[0, 2, 3]), pol, _concepts())
    d1 = _draft(n=1, label="Ban X", statement="Ban X now.", definition="Bans X.", subject="X")
    d2 = _draft(n=2, not_a_position=True)
    d3 = _draft(n=3, label="Fund Y", statement="Fund Y.", definition="Funds Y.", subject="Y")
    jev = FakeJev({(psd.draft_query(d1), psd.policy_text(pol[0])): 0.9,
                   (psd.draft_query(d3), psd.concept_text(_concepts()[1])): 0.6})
    legbot = _answer([d3, d1, d2])  # out of order on purpose: matched by `n`, not by position

    result, dispatch = await run_step(broker, legbot, jev)

    assert dispatch.await_args.kwargs["statements"] == ["s one", "s two", "s three"]
    assert dispatch.await_args.args == ("archived text", "policy_statement")
    assert result == {"statements": 3, "matched": 1, "created": 1, "excluded": 1, "skipped_reason": None}
    w1, w2, w3 = broker.writes
    assert (w1["resolution"], w1["policy_id"], w1["created"], w1["statement_index"]) == ("matched", 5, None, 0)
    assert w1["scores"] == {"policy": 0.9, "best_policy_id": 5, "concept": None, "cutoff": 0.7, "list_size": 1}
    assert (w2["resolution"], w2["statement_index"], w2["reason"]) == ("excluded", 2, "not a position")
    assert w2["scores"]["policy"] == 0.0 and w2["created"] is None
    assert (w3["resolution"], w3["statement_index"], w3["policy_id"]) == ("created", 3, None)
    assert w3["created"] == {"label": "Fund Y", "canonical_statement": "Fund Y.", "definition": "Funds Y.",
                             "exclusions": "Not that.", "concept_id": 101}
    assert w3["scores"]["concept"] == 0.6 and w3["scores"]["policy"] == 0.0
    for w in broker.writes:
        assert (w["concept_statement_set_id"], w["model_name"], w["prompt_hash"], w["prompt_version"], w["rubric_version"]) == (
            7, "typesafe:fake", "policy_statement:p1r1:0123456789abcdef", "1", "policy_statement:v1")
        # the writer's model and prompt version ride in the stored draft (the broker's prompt_hash has no model name)
        assert w["draft"]["writer"] == {"model_name": "mlx-community/gemma-4-26b-a4b-it-8bit", "prompt_version": "1"}
        assert w["statement_text"] in ("s one", "s two", "s three") and w["draft"]["n"] in (1, 2, 3)
    assert broker.work_kwargs["rubric_version"] == "policy_statement:v1" and "prompt_hash" not in broker.work_kwargs
    assert (broker.work_kwargs["gov_id"], broker.work_kwargs["jurisdiction_iso2"], broker.work_kwargs["session_code"]) == (
        "HB 1", "FL", "2026")


@pytest.mark.asyncio
async def test_the_concept_list_is_read_only_when_a_draft_needs_one_and_only_once(run_step):
    pol = [{"id": 5, "label": "Ban X", "canonical_statement": "", "definition": "", "exclusions": ""}]
    broker = FakeBroker(_work(["a", "b", "c"]), pol, _concepts())
    jev = FakeJev({"Ban X": 0.1})  # under the cutoff: all three drafts are new Policies
    await run_step(broker, _answer([_draft(n=1), _draft(n=2), _draft(n=3)]), jev)
    assert broker.concept_reads == 1 and broker.policy_reads == 1  # one read of each per bill, however many drafts need a Concept

    broker2 = FakeBroker(_work(["a"]), pol, _concepts())
    await run_step(broker2, _answer([_draft(n=1, not_a_position=True)]), FakeJev())
    assert broker2.concept_reads == 0


@pytest.mark.asyncio
async def test_the_policy_list_is_read_again_for_each_bill_so_a_new_policy_is_a_candidate_for_the_next(run_step):
    broker = FakeBroker(_work(["a"]), [], _concepts())
    draft = _draft(n=1, label="New thing", statement="New thing.", definition="It is new.", subject="n")
    jev = FakeJev()
    first, _ = await run_step(broker, _answer([draft]), jev)
    assert first["created"] == 1 and broker.writes[0]["scores"]["list_size"] == 0
    created_text = psd.policy_text(broker.policies[0])
    jev.table[created_text] = 0.88  # the next bill's identical draft is close to the Policy the first bill just created
    broker.work = _work(["b"], gov_id="HB 2")
    second, _ = await run_step(broker, _answer([draft]), jev, gov_id="HB 2")
    assert broker.policy_reads == 2
    assert second["matched"] == 1 and broker.writes[1]["policy_id"] == broker.policies[0]["id"]
    assert broker.writes[1]["scores"]["list_size"] == 1


@pytest.mark.asyncio
async def test_nothing_to_do_dispatches_nothing(run_step):
    broker = FakeBroker([], [], _concepts())
    result, dispatch = await run_step(broker, _answer([]), FakeJev())
    assert result["skipped_reason"] == "nothing_to_do" and result["statements"] == 0
    dispatch.assert_not_awaited()
    assert broker.writes == [] and broker.policy_reads == 0


@pytest.mark.asyncio
async def test_dry_run_reads_the_work_list_and_calls_nothing_else(run_step):
    broker = FakeBroker(_work(["a", "b"]), _policies(2), _concepts())
    jev = FakeJev()
    result, dispatch = await run_step(broker, _answer([]), jev, dry_run=True)
    assert result == {"statements": 2, "matched": 0, "created": 0, "excluded": 0, "skipped_reason": None}
    dispatch.assert_not_awaited()
    assert jev.calls == [] and broker.writes == [] and broker.policy_reads == 0


@pytest.mark.asyncio
async def test_no_archived_text_is_a_skip_not_a_failure(run_step):
    broker = FakeBroker(_work(["a"]), [], _concepts())
    result, dispatch = await run_step(broker, _answer([]), FakeJev(), source=None)
    assert result["skipped_reason"] == "no_archived_text"
    dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_insufficient_information_writes_nothing(run_step):
    broker = FakeBroker(_work(["a"]), [], _concepts())
    legbot = {"answer": {"insufficient_information": True, "drafts": []}, "backend": "mlx"}
    result, _ = await run_step(broker, legbot, FakeJev())
    assert result["skipped_reason"] == "insufficient_information" and broker.writes == []


@pytest.mark.parametrize("drafts", [
    [_draft(n=1)],  # one missing
    [_draft(n=1), _draft(n=1)],  # one repeated, one missing
    [_draft(n=1), _draft(n=2), _draft(n=3)],  # one extra
])
@pytest.mark.asyncio
async def test_a_writer_answer_that_does_not_cover_every_statement_once_writes_nothing(run_step, drafts):
    broker = FakeBroker(_work(["a", "b"]), _policies(1), _concepts())
    with pytest.raises(LegBotDispatchError):
        await run_step(broker, _answer(drafts), FakeJev())
    assert broker.writes == []


@pytest.mark.asyncio
async def test_a_failed_legbot_task_is_a_failed_bill_and_writes_nothing(run_step):
    """LegBot fails the whole task on a malformed answer, on a backend error, and on a bill too big even for Claude."""
    broker = FakeBroker(_work(["a"]), _policies(1), _concepts())
    with pytest.raises(LegBotDispatchError, match="ended with status=failed"):
        await run_step(broker, [LegBotDispatchError("LegBot task t ended with status=failed")], FakeJev())
    assert broker.writes == [] and broker.policy_reads == 0


@pytest.mark.asyncio
async def test_a_judge_id_longer_than_the_brokers_prompt_hash_fails_the_bill_before_any_write(run_step):
    # the OLD id format (model name inside it, AGENTS-262 before its fix): a safety net in case an id over 64 ever arrives
    long_id = "policy_statement:mlx-community/gemma-4-26b-a4b-it-8bit:p1r1:0123abcd"
    assert len(long_id) == 68
    assert len("policy_statement:p1r1:0123456789abcdef") <= 64  # the compact format AGENTS-262 now builds
    broker = FakeBroker(_work(["a", "b"]), _policies(1), _concepts())
    jev = FakeJev()
    with pytest.raises(LegBotDispatchError, match="judge_id is 68 characters"):
        await run_step(broker, _answer([_draft(n=1), _draft(n=2)], judge_id=long_id), jev)
    assert broker.writes == [] and jev.calls == []
    ok = "x" * 64
    result, _ = await run_step(FakeBroker(_work(["a"]), _policies(1), _concepts()), _answer([_draft(n=1)], judge_id=ok), FakeJev())
    assert result["created"] == 1


@pytest.mark.asyncio
async def test_a_label_the_broker_would_refuse_fails_the_bill_before_any_write(run_step):
    broker = FakeBroker(_work(["a", "b"]), _policies(1), _concepts())
    drafts = [_draft(n=1), _draft(n=2, label="L" * 201)]
    with pytest.raises(LegBotDispatchError, match=r"\[2\]"):
        await run_step(broker, _answer(drafts), FakeJev())
    assert broker.writes == []
    # a draft that is not a position carries no label and is not checked
    ok, _ = await run_step(FakeBroker(_work(["a"]), _policies(1), _concepts()),
                           _answer([_draft(n=1, not_a_position=True, label="L" * 500)]), FakeJev())
    assert ok["excluded"] == 1


@pytest.mark.asyncio
async def test_a_work_list_with_another_bills_rows_is_refused(run_step):
    broker = FakeBroker(_work(["a"], gov_id="HB 999"), [], _concepts())
    with pytest.raises(BrokerClientError, match="did not honour gov_id="):
        await run_step(broker, _answer([_draft(n=1)]), FakeJev())
    assert broker.writes == []


@pytest.mark.asyncio
async def test_a_missing_jev_key_fails_the_bill_and_writes_nothing(run_step):
    broker = FakeBroker(_work(["a"]), _policies(1), _concepts())
    with pytest.raises(JevError, match="TYPESAFE_API_KEY"):
        legbot_probe = _answer([_draft(n=1)])
        await run_step(broker, legbot_probe, FakeJev(), no_key=True)
    assert broker.writes == []


@pytest.mark.asyncio
async def test_a_jev_failure_propagates_and_earlier_statements_stay_written(run_step):
    class FlakyJev(FakeJev):
        async def score(self, text, candidates):
            if "second" in text:
                raise JevError("Jev answered 500")
            return await super().score(text, candidates)

    broker = FakeBroker(_work(["a", "b"]), _policies(1), _concepts())
    drafts = [_draft(n=1, label="first", statement="x"), _draft(n=2, label="second", statement="y")]
    with pytest.raises(JevError):
        await run_step(broker, _answer(drafts), FlakyJev())
    assert [w["statement_index"] for w in broker.writes] == [0]


@pytest.mark.asyncio
async def test_a_broker_conflict_on_the_write_propagates(run_step):
    broker = FakeBroker(_work(["a"]), _policies(1), _concepts())
    broker.write = AsyncMock(side_effect=BrokerClientError("rejected the policy statement match write (409)"))
    with pytest.raises(BrokerClientError, match="409"):
        await run_step(broker, _answer([_draft(n=1)]), FakeJev())


@pytest.mark.asyncio
async def test_a_new_policy_with_no_concept_to_go_under_fails_rather_than_guessing(run_step):
    broker = FakeBroker(_work(["a"]), _policies(1), [])
    with pytest.raises(BrokerClientError, match="Concept list is empty"):
        await run_step(broker, _answer([_draft(n=1)]), FakeJev())
    assert broker.writes == []


@pytest.mark.asyncio
async def test_more_than_forty_statements_go_to_the_writer_in_batches_and_keep_their_order(run_step):
    texts = [f"s{i}" for i in range(45)]
    broker = FakeBroker(_work(texts), [], _concepts())
    batches = [_answer([_draft(n=i + 1, label=f"L{i}") for i in range(40)]), _answer([_draft(n=i + 1, label=f"M{i}") for i in range(5)])]
    result, dispatch = await run_step(broker, batches, FakeJev())
    assert [len(c.kwargs["statements"]) for c in dispatch.await_args_list] == [40, 5]
    assert result["created"] == 45
    assert [w["draft"]["label"] for w in broker.writes][38:42] == ["L38", "L39", "M0", "M1"]
    assert [w["statement_index"] for w in broker.writes] == list(range(45))


# --------------------------------------------------------------------------------------------------------------------
# The step inside run_legbot_pipeline
# --------------------------------------------------------------------------------------------------------------------

_CAND = {"gov_id": "HB 1", "bill_openstates_id": "ocd-bill/x", "live_url_fallback": "u"}


@pytest.fixture
def pipeline(monkeypatch, legbot_stand_in_manifest):
    """run(include_policy_statement=..., **kw) with the per-bill collaborators replaced; returns (summary, step mock)."""
    from ddp_sync.pipelines import session_pipeline_runner as spr

    spr._legbot_line = None
    step = AsyncMock(return_value={"statements": 2, "matched": 1, "created": 1, "excluded": 0, "skipped_reason": None})
    monkeypatch.setattr(spr, "dispatch_and_store_policy_statements", step)
    monkeypatch.setattr(spr, "get_archived_bill_text", AsyncMock(return_value=None))
    monkeypatch.setattr(spr, "_polled_bill_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(spr, "get_bill_artifacts", AsyncMock(return_value=None))
    monkeypatch.setattr(spr, "get_concept_statement_statuses", AsyncMock(return_value=["published"]))
    monkeypatch.setattr(spr, "get_current_version_identity", AsyncMock(return_value={
        "version_date": "2026-06-01", "version_note": "c1", "bill_title": "t", "chamber_classification": "lower",
        "jurisdiction_classification": "state"}))
    monkeypatch.setattr(spr, "generate_and_store_bill_artifact", AsyncMock(return_value={"id": 1, "status": "complete"}))
    monkeypatch.setattr(spr, "list_current_session_bill_candidates", AsyncMock(side_effect=AssertionError("no scan")))

    async def run(**kw):
        kw.setdefault("bill_candidates", [_CAND])
        return await run_legbot_pipeline("fl", "2026F", ["bill_summary"], False, 10, include_concept_statements=False,
                                         retry_failed=False, **kw)

    return run, step


@pytest.mark.asyncio
async def test_the_step_does_not_run_unless_asked(pipeline):
    run, step = pipeline
    summary = await run()
    step.assert_not_awaited()
    r = summary["results"][0]
    assert r["policy_statement_dispatched"] is False and r["policy_statement_failed"] is False


@pytest.mark.asyncio
async def test_the_step_runs_for_the_listed_bills_only_and_reports_its_counts(pipeline):
    run, step = pipeline
    summary = await run(include_policy_statement=True, bill_candidates=[_CAND, {**_CAND, "gov_id": "HB 2", "bill_openstates_id": "ocd-bill/y"}])
    assert step.await_count == 2 and {c.kwargs["gov_id"] for c in step.await_args_list} == {"HB 1", "HB 2"}
    assert step.await_args.kwargs["dry_run"] is False and step.await_args.kwargs["session_code"] == "2026F"
    r = summary["results"][0]
    assert (r["policy_statement_dispatched"], r["policy_statement_failed"], r["policy_statement_skipped_reason"]) == (True, False, None)
    assert (r["policy_statement_statements"], r["policy_statement_matched"], r["policy_statement_created"]) == (2, 1, 1)


@pytest.mark.asyncio
async def test_dry_run_is_passed_to_the_step(pipeline):
    run, step = pipeline
    await run(include_policy_statement=True, dry_run=True)
    assert step.await_args.kwargs["dry_run"] is True


@pytest.mark.asyncio
async def test_a_skip_is_not_a_failure_and_a_failure_is_isolated_to_its_bill(pipeline):
    run, step = pipeline
    step.side_effect = [
        {"statements": 0, "matched": 0, "created": 0, "excluded": 0, "skipped_reason": "nothing_to_do"},
        LegBotDispatchError("writer failed"),
    ]
    summary = await run(include_policy_statement=True, bill_candidates=[_CAND, {**_CAND, "gov_id": "HB 2", "bill_openstates_id": "ocd-bill/y"}])
    skipped, failed = sorted(summary["results"], key=lambda r: r["gov_id"])
    assert (skipped["policy_statement_dispatched"], skipped["policy_statement_failed"], skipped["policy_statement_skipped_reason"]) == (
        False, False, "nothing_to_do")
    assert failed["policy_statement_failed"] is True and "writer failed" in failed["policy_statement_skipped_reason"]
    assert failed["error"] is None  # the bill's other work is untouched
    assert summary["bills_processed"] == 2


@pytest.fixture
def run_empty(pipeline, monkeypatch):
    """SYNC-193: run_legbot_pipeline with artifact_types=[] on the `pipeline` fixture's stand-ins; returns (call, step, artifact mock)."""
    from ddp_sync.pipelines import session_pipeline_runner as spr

    _, step = pipeline
    artifact = AsyncMock(return_value={"id": 1, "status": "complete"})
    monkeypatch.setattr(spr, "generate_and_store_bill_artifact", artifact)

    async def call(**kw):
        return await run_legbot_pipeline("fl", "2026F", [], False, 10, include_concept_statements=False,
                                         retry_failed=False, bill_candidates=[_CAND], **kw)

    return call, step, artifact


@pytest.mark.asyncio
async def test_an_empty_artifact_list_runs_the_policy_step_alone_when_it_is_on(run_empty):
    call, step, artifact = run_empty
    summary = await call(include_policy_statement=True)
    step.assert_awaited_once()
    artifact.assert_not_awaited()
    r = summary["results"][0]
    assert (r["policy_statement_dispatched"], r["policy_statement_failed"], r["error"]) == (True, False, None)
    assert r["artifacts_generated"] == [] and r["artifacts_failed"] == []


@pytest.mark.asyncio
async def test_an_empty_artifact_list_is_still_refused_when_the_policy_step_is_off(run_empty):
    call, step, artifact = run_empty
    with pytest.raises(ValueError, match="^artifact_types must be non-empty$"):
        await call()
    with pytest.raises(ValueError, match="^artifact_types must be non-empty$"):
        await call(include_policy_statement=False)
    step.assert_not_awaited()
    artifact.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_empty_artifact_list_with_the_step_on_still_checks_the_other_inputs(run_empty):
    call, _, _ = run_empty
    with pytest.raises(ValueError, match="limit must be a positive integer"):
        await run_legbot_pipeline("fl", "2026F", [], False, 0, include_concept_statements=False, retry_failed=False,
                                  bill_candidates=[_CAND], include_policy_statement=True)


@pytest.mark.asyncio
async def test_an_empty_artifact_list_dry_run_reports_the_bills_it_would_handle(run_empty):
    call, step, artifact = run_empty
    summary = await call(include_policy_statement=True, dry_run=True)
    assert step.await_args.kwargs["dry_run"] is True and summary["bills_processed"] == 1
    artifact.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_listed_artifact_with_the_step_on_behaves_as_before(pipeline):
    run, step = pipeline
    summary = await run(include_policy_statement=True)
    step.assert_awaited_once()
    assert summary["results"][0]["artifacts_generated"] == ["bill_summary"]


@pytest.mark.asyncio
async def test_through_run_legbot_pipeline_a_policy_created_for_one_bill_is_matched_by_the_next(pipeline, monkeypatch):
    """The two bills run one after the other (session_pipeline_concurrency 1): bill 1's write lands before bill 2 reads the list."""
    from types import SimpleNamespace

    from ddp_sync.pipelines import session_pipeline_runner as spr

    run, _ = pipeline
    monkeypatch.setattr(spr, "dispatch_and_store_policy_statements", psd.dispatch_and_store_policy_statements)
    monkeypatch.setattr(spr, "get_settings", lambda: SimpleNamespace(session_pipeline_concurrency=1, replica_freshness_check_enabled=False))
    monkeypatch.setattr(psd, "get_settings", lambda: SyncSettings())
    events: list[str] = []
    broker = FakeBroker([], [], _concepts())
    draft = _draft(n=1, label="New thing", statement="New thing.", definition="It is new.", subject="n")
    jev = FakeJev()

    async def list_work(*, gov_id, **kw):
        return _work(["s"], gov_id=gov_id, session="2026F", set_id=7 if gov_id == "HB 1" else 8)

    async def list_policies(**kw):
        events.append(f"read:{len(broker.policies)}")
        if broker.policies:
            jev.table[psd.policy_text(broker.policies[0])] = 0.9
        return list(broker.policies)

    async def write(payload, **kw):
        events.append(f"write:{payload['resolution']}")
        return await FakeBroker.write(broker, payload)

    monkeypatch.setattr(psd, "list_policy_statement_work", list_work)
    monkeypatch.setattr(psd, "list_policy_matching_policies", list_policies)
    monkeypatch.setattr(psd, "list_policy_matching_concepts", broker.list_concepts)
    monkeypatch.setattr(psd, "write_policy_statement_match", write)
    monkeypatch.setattr(psd, "dispatch_bill_question", AsyncMock(return_value=_answer([draft])))
    monkeypatch.setattr(psd, "_resolve_bill_source", AsyncMock(return_value="src"))
    monkeypatch.setattr(psd, "JevScorer", lambda: _Ctx(jev))

    await run(include_policy_statement=True, bill_candidates=[_CAND, {**_CAND, "gov_id": "HB 2", "bill_openstates_id": "ocd-bill/y"}])
    assert events == ["read:0", "write:created", "read:1", "write:matched"]


class _Ctx:
    def __init__(self, jev):
        self.jev = jev

    async def __aenter__(self):
        return self.jev

    async def __aexit__(self, *a):
        return False


# --------------------------------------------------------------------------------------------------------------------
# The trigger
# --------------------------------------------------------------------------------------------------------------------

def _trigger(extra):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from ddp_sync.api.auth import api_key_auth
    from ddp_sync.api.routes.triggers import router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "t"
    body = {"jurisdiction_iso2": "fl", "session_code": "2026F", "artifact_types": ["bill_summary"], "include_org_research": False,
            "include_concept_statements": False, "retry_failed": False, "limit": 10, **extra}
    with patch("ddp_sync.pipelines.scraper_triggered_legbot.trigger_scraper_session_pipeline",
               new=AsyncMock(return_value={"success": True, "results": []})) as mock:
        response = TestClient(app).post("/trigger/bill-artifact-generation", json=body)
    return response, mock


def test_the_trigger_passes_include_policy_statement_and_defaults_it_to_false():
    response, mock = _trigger({"include_policy_statement": True, "dry_run": True,
                               "bill_candidates": [{"gov_id": "HB 1", "bill_openstates_id": "ocd-bill/x"}]})
    assert response.status_code == 200
    kw = mock.await_args.kwargs
    assert kw["include_policy_statement"] is True and kw["dry_run"] is True and len(kw["bill_candidates"]) == 1

    response, mock = _trigger({})  # the post-archive hook sends no such key
    assert response.status_code == 200 and mock.await_args.kwargs["include_policy_statement"] is False


def test_the_trigger_accepts_an_empty_artifact_list_and_passes_it_through():
    """SYNC-193: the request model does not refuse [] (the pipeline decides, and only with the policy step off)."""
    response, mock = _trigger({"artifact_types": [], "include_policy_statement": True, "dry_run": True,
                               "bill_candidates": [{"gov_id": "HB 1", "bill_openstates_id": "ocd-bill/x"}]})
    assert response.status_code == 200
    assert mock.await_args.kwargs["artifact_types"] == [] and mock.await_args.kwargs["include_policy_statement"] is True


# --------------------------------------------------------------------------------------------------------------------
# The clients
# --------------------------------------------------------------------------------------------------------------------

_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _mock_httpx(monkeypatch, module, handler):
    monkeypatch.setattr(
        module.httpx, "AsyncClient",
        lambda *a, **k: _REAL_ASYNC_CLIENT(*a, **{**k, "transport": httpx.MockTransport(handler)}),
    )


@pytest.mark.asyncio
async def test_jev_scorer_sends_the_measured_request_and_reads_the_probabilities(monkeypatch):
    seen = {}

    def handler(request):
        seen["url"], seen["auth"], seen["body"] = str(request.url), request.headers["authorization"], json.loads(request.content)
        return httpx.Response(200, json={"answers": {"place": {"probabilities": {"o0": 0.1, "o1": 0.75, "none": 0.15}}},
                                         "usage": {"input_tokens": 1000}})

    _mock_httpx(monkeypatch, jev_client, handler)
    async with JevScorer(api_key="k", model_id="jev-latest") as jev:
        scores = await jev.score("the draft", ["first", "second"])
        assert scores == [0.1, 0.75] and jev.input_tokens == 1000 and jev.cost_usd == pytest.approx(0.000042)
        assert jev.name == "typesafe:jev-latest"
    assert seen["url"] == "https://api.typesafe.ai/v1/systemone" and seen["auth"] == "Bearer k"
    body = seen["body"]
    assert body["model"] == "jev-latest" and body["state"] == "the draft"
    q = body["questions"]["place"]
    assert q["type"] == "choice" and q["criteria"] == {"o0": "first", "o1": "second", "none": "None of the other options fits the statement."}
    assert "policy area or policy family" in q["instructions"]


@pytest.mark.asyncio
async def test_jev_scorer_waits_and_retries_a_429_and_fails_on_other_errors(monkeypatch):
    calls = []
    sleeps = []

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"retry-after": "3"})
        return httpx.Response(200, json={"answers": {"place": {"probabilities": {"o0": 0.5}}}})

    _mock_httpx(monkeypatch, jev_client, handler)
    monkeypatch.setattr(jev_client.asyncio, "sleep", AsyncMock(side_effect=lambda s: sleeps.append(s)))
    async with JevScorer(api_key="k", model_id="m") as jev:
        assert await jev.score("t", ["a"]) == [0.5]
    assert len(calls) == 2 and sleeps == [3.0]

    _mock_httpx(monkeypatch, jev_client, lambda request: httpx.Response(500, text="boom"))
    async with JevScorer(api_key="k", model_id="m") as jev:
        with pytest.raises(JevError, match="500"):
            await jev.score("t", ["a"])
    _mock_httpx(monkeypatch, jev_client, lambda request: httpx.Response(200, json={"answers": {}}))
    async with JevScorer(api_key="k", model_id="m") as jev:
        with pytest.raises(JevError, match="unusable"):
            await jev.score("t", ["a"])


@pytest.mark.asyncio
async def test_jev_scorer_needs_a_key_and_refuses_more_options_than_jev_takes(monkeypatch):
    with pytest.raises(JevError, match="TYPESAFE_API_KEY"):
        JevScorer(api_key="")
    async with JevScorer(api_key="k", model_id="m") as jev:
        with pytest.raises(ValueError, match="255"):
            await jev.score("t", ["x"] * 255)


def test_the_jev_key_is_read_from_either_env_name(monkeypatch):
    from ddp_sync.config import _load_from_env

    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.setenv("LEGBOT_TYPESAFE_API_KEY", "from-cams-name")
    assert _load_from_env()["typesafe_api_key"] == "from-cams-name"
    monkeypatch.setenv("TYPESAFE_API_KEY", "from-sdk-name")
    assert _load_from_env()["typesafe_api_key"] == "from-sdk-name"
    monkeypatch.setenv("POLICY_STATEMENT_MATCH_CUTOFF", "0.8")
    assert _load_from_env()["policy_statement_match_cutoff"] == 0.8


class _Settings:
    ddp_broker_api_base = "http://broker"
    ddp_broker_api_token = "tok"


@pytest.mark.asyncio
async def test_broker_lists_page_until_next_after_is_null_and_send_the_filters(monkeypatch):
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path, dict(request.url.params), request.headers["authorization"]))
        after = int(request.url.params["after"])
        return httpx.Response(200, json={"results": [{"id": after + 1}], "next_after": after + 1 if after < 2 else None})

    _mock_httpx(monkeypatch, broker_client, handler)
    monkeypatch.setattr(broker_client, "get_settings", lambda: _Settings())
    rows = await broker_client.list_policy_matching_policies()
    assert [r["id"] for r in rows] == [1, 2, 3]
    assert [s[2]["after"] for s in seen] == ["0", "1", "2"] and seen[0][1] == "/api/policy-matching/policies/"
    assert seen[0][3] == "Bearer tok" and seen[0][2]["limit"] == "200"

    seen.clear()
    await broker_client.list_policy_statement_work(gov_id="HB 1", jurisdiction_iso2="fl", session_code="2026F", rubric_version="policy_statement:v1")
    assert seen[0][1] == "/api/policy-matching/work/"
    # a concept statement set has no Open States id: a bill is jurisdiction + gov_id + session (BROKER-598)
    assert (seen[0][2]["gov_id"], seen[0][2]["jurisdiction"], seen[0][2]["session"], seen[0][2]["rubric_version"]) == (
        "HB 1", "FL", "2026F", "policy_statement:v1")
    assert "bill" not in seen[0][2]
    assert "prompt_hash" not in seen[0][2]
    seen.clear()
    await broker_client.list_policy_matching_concepts()
    assert seen[0][1] == "/api/policy-matching/concepts/"


@pytest.mark.asyncio
async def test_broker_write_posts_the_payload_and_raises_on_any_error(monkeypatch):
    seen = {}

    def handler(request):
        seen["path"], seen["body"] = request.url.path, json.loads(request.content)
        return httpx.Response(seen.get("status", 201), json={"resolution": "matched"}, text=None if seen.get("status", 201) < 400 else "conflict")

    _mock_httpx(monkeypatch, broker_client, handler)
    monkeypatch.setattr(broker_client, "get_settings", lambda: _Settings())
    assert await broker_client.write_policy_statement_match({"a": 1}) == {"resolution": "matched"}
    assert seen == {"path": "/api/policy-matching/drafts/", "body": {"a": 1}}
    seen["status"] = 409
    with pytest.raises(BrokerClientError, match="409"):
        await broker_client.write_policy_statement_match({"a": 1})
    monkeypatch.setattr(broker_client, "get_settings", lambda: type("S", (), {"ddp_broker_api_base": "", "ddp_broker_api_token": ""})())
    with pytest.raises(BrokerClientError, match="DDP_BROKER_API_BASE"):
        await broker_client.list_policy_matching_concepts()


@pytest.mark.asyncio
async def test_dispatch_bill_question_sends_statements_for_policy_statement_only():
    with patch("ddp_sync.services.legbot_client._dispatch_and_await", new=AsyncMock(return_value={"answer": {}})) as mock:
        await dispatch_bill_question("src", "policy_statement", statements=["a", "b"])
        assert mock.await_args.args[0] == {"bill_source": "src", "question_type": "policy_statement", "caller": "ddp_sync",
                                           "statements": ["a", "b"]}
        await dispatch_bill_question("src", "bill_summary")
        assert "statements" not in mock.await_args.args[0]
