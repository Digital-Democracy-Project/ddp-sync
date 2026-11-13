"""SYNC-150: person-like organizations go to the person resolver (BROKER-441 broker, AGENTS-220 LegBot), and the
evaluation gate that must pass before act mode is switched on for people. The broker and LegBot are faked."""

from __future__ import annotations

import json
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.bill_organization_position_research import (
    generate_and_store_bill_organization_positions,
)
from ddp_sync.pipelines.person_resolution_gate import (
    CASES_PATH,
    load_cases,
    run_offline,
)
from ddp_sync.services.broker_client import BrokerClientError
from ddp_sync.services.legbot_client import LegBotDispatchError

_P = "ddp_sync.pipelines.bill_organization_position_research"
_BILL = {
    "bill_openstates_id": "8d71a94e-0000-0000-0000-000000000001", "jurisdiction": "FL", "session_code": "2026",
    "version_date": "2026-01-05", "version_note": "Introduced", "gov_id": "HB123", "bill_title": "An act relating to tests",
}
_RECORD = {"name": "Sen. Carlos Guillermo Smith", "cleaned_name": "Carlos Guillermo Smith",
           "org_type": "Public official (Florida State Senator)", "description": "Florida Senate District 17.",
           "website": "https://www.flsenate.gov/Senators/S17", "signals": ["title", "type", "site"], "state_hint": ["FL"]}
_CANDIDATES = [{"id": "rep:1214", "name": "Carlos Smith", "jurisdiction": "FL", "chamber": "upper", "district": "17"},
               {"id": "os:9f0c", "name": "Carla Smith", "jurisdiction": "OH", "chamber": "lower", "district": "3"}]


def _person_shortlist(attempt_id=700, candidates=None):
    return {"attempt_id": attempt_id, "candidates": _CANDIDATES if candidates is None else candidates,
            "input": _RECORD, "reason": ""}


_NOT_A_PERSON = {"attempt_id": None, "candidates": [], "input": None, "reason": "the record does not look like a person"}


def _person_answer(choice, reason="Florida Senate district 17"):
    return {"answer": {"choice": choice, "reason": reason, "model": "claude:claude-haiku-4-5", "prompt_version": "1",
                       "prompt_hash": "abc", "insufficient_information": False}, "backend": "claude"}


@pytest.fixture(autouse=True)
def _research_defaults():
    with patch(f"{_P}._resolve_bill_source", new=AsyncMock(return_value="BILL TEXT")), patch(
        f"{_P}.write_bill_organization_research_run", new=AsyncMock(return_value={"id": 1})
    ):
        yield


async def _run(*, person_mode="suggest", org_mode="suggest", person_shortlist=None, person=None, person_report=None,
               person_act=None, write=None, about_enabled=True):
    mocks = SimpleNamespace(
        person_shortlist=person_shortlist or AsyncMock(return_value=_person_shortlist()),
        person=person or AsyncMock(return_value=_person_answer("rep:1214")),
        person_report=person_report or AsyncMock(return_value={"attempt_id": 700, "recorded": True}),
        person_act=person_act or AsyncMock(return_value={"acted": False, "reason": "act mode is off"}),
        org_shortlist=AsyncMock(return_value={"attempt_id": 900, "candidates": [{"id": 41, "name": "Org"}]}),
        org=AsyncMock(return_value={"answer": {"choice": None, "reason": "x"}, "backend": "claude"}),
        org_report=AsyncMock(return_value={}),
        org_act=AsyncMock(return_value={"acted": False, "reason": "off", "organization_id": None}),
        about=AsyncMock(return_value={"answer": {"description": "An organization."}, "backend": "claude"}),
    )
    write = write or {"id": 1, "result": "created", "verification_verdict": "confirmed", "organization_id": 10888,
                      "organization_needs_content": True}
    positions = {"positions": [{"org_name": "Sen. Carlos Guillermo Smith", "position": "support",
                                "citation_url": "https://a.invalid", "citation_excerpt": "x"}],
                 "insufficient_information": False}
    with patch(f"{_P}.get_settings", return_value=SimpleNamespace(
        org_research_max_organizations=500, organization_resolution_mode=org_mode, person_resolution_mode=person_mode,
        organization_content_enabled=about_enabled,
    )), patch(f"{_P}.dispatch_bill_question", new=AsyncMock(return_value={"answer": positions, "backend": "openai"})), patch(
        f"{_P}.dispatch_bill_position_verification",
        new=AsyncMock(return_value={"answer": {"verdict": "confirmed", "insufficient_information": False,
                                               "content_looks_incomplete": False, "explanation": "ok"}, "backend": "claude"}),
    ), patch(f"{_P}.write_bill_organization_position", new=AsyncMock(return_value=write)), patch(
        f"{_P}.get_person_resolution_shortlist", new=mocks.person_shortlist
    ), patch(f"{_P}.dispatch_resolve_person", new=mocks.person), patch(
        f"{_P}.report_person_resolution_answer", new=mocks.person_report
    ), patch(f"{_P}.request_person_resolution_act", new=mocks.person_act), patch(
        f"{_P}.get_organization_resolution_shortlist", new=mocks.org_shortlist
    ), patch(f"{_P}.dispatch_resolve_organization", new=mocks.org), patch(
        f"{_P}.report_organization_resolution_answer", new=mocks.org_report
    ), patch(f"{_P}.request_organization_resolution_act", new=mocks.org_act), patch(
        f"{_P}.dispatch_describe_organization", new=mocks.about
    ), patch(f"{_P}.write_organization_content", new=AsyncMock(return_value={"id": 10888, "written": ["description"]})):
        results = await generate_and_store_bill_organization_positions(**_BILL)
    return results[0], mocks


# ---- routing ------------------------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_person_like_stub_goes_to_the_person_resolver_and_never_to_the_organization_one():
    result, m = await _run()

    assert m.person_shortlist.await_args.kwargs["organization_id"] == 10888
    assert m.person.await_args.args == (_RECORD, _CANDIDATES)  # the broker's input and shortlist, unchanged
    assert m.person_report.await_args.kwargs == {
        "attempt_id": 700, "model": "claude:claude-haiku-4-5", "resolver_prompt_rev": "1", "choice": "rep:1214",
        "reason": "Florida Senate district 17", "broker_api_base": None, "broker_api_token": None,
    }
    m.person_act.assert_not_awaited()  # suggest mode
    m.org_shortlist.assert_not_awaited()
    m.org.assert_not_awaited()
    assert result["person_resolution"]["outcome"] == "suggested" and "resolution" not in result
    assert result["position_id"] == 1  # the position stays as written


@pytest.mark.asyncio
async def test_a_record_the_broker_does_not_take_as_a_person_continues_to_the_organization_resolver():
    result, m = await _run(person_shortlist=AsyncMock(return_value=_NOT_A_PERSON))

    m.person.assert_not_awaited()
    m.person_report.assert_not_awaited()
    m.org_shortlist.assert_awaited_once()
    assert result["person_resolution"]["outcome"] == "not_eligible"
    assert result["person_resolution"]["reason"] == "the record does not look like a person"
    assert result["resolution"]["outcome"] == "null_answer"


@pytest.mark.asyncio
async def test_with_the_person_step_off_nothing_is_asked_of_it_and_the_organization_step_is_unchanged():
    result, m = await _run(person_mode="off")

    m.person_shortlist.assert_not_awaited()
    m.org_shortlist.assert_awaited_once()
    assert "person_resolution" not in result


@pytest.mark.asyncio
async def test_the_person_step_runs_when_the_organization_step_is_off():
    result, m = await _run(org_mode="off", person_shortlist=AsyncMock(return_value=_NOT_A_PERSON))

    m.person_shortlist.assert_awaited_once()
    m.org_shortlist.assert_not_awaited()
    assert result["person_resolution"]["outcome"] == "not_eligible" and "resolution" not in result


@pytest.mark.asyncio
async def test_only_a_newly_created_confirmed_stub_is_offered():
    for write in (
        {"id": 1, "result": "updated", "verification_verdict": "confirmed", "organization_id": 10888, "organization_needs_content": False},
        {"id": 1, "result": "created", "verification_verdict": "not_confirmed", "organization_id": 10888, "organization_needs_content": False},
    ):
        result, m = await _run(write=write)
        m.person_shortlist.assert_not_awaited()
        assert "person_resolution" not in result


# ---- modes and answers --------------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_act_mode_requests_the_act_for_a_named_legislator_and_never_writes_a_link_itself():
    result, m = await _run(person_mode="act", person_act=AsyncMock(return_value={"acted": True, "reason": ""}))

    m.person_act.assert_awaited_once()
    assert m.person_act.await_args.kwargs["attempt_id"] == 700
    assert result["person_resolution"]["outcome"] == "acted"
    assert result["organization_id"] == 10888  # the position still belongs to the record; the broker linked the record


@pytest.mark.asyncio
async def test_a_refused_act_is_a_normal_outcome():
    result, _ = await _run(person_mode="act")

    assert result["person_resolution"]["outcome"] == "act_refused"


@pytest.mark.asyncio
async def test_a_null_answer_is_reported_and_never_acted_on():
    result, m = await _run(person_mode="act", person=AsyncMock(return_value=_person_answer(None, "a council member")))

    assert m.person_report.await_args.kwargs["choice"] is None
    m.person_act.assert_not_awaited()
    assert result["person_resolution"]["outcome"] == "null_answer"


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["rep:999", 1214, ["rep:1214"], True])
async def test_a_choice_outside_the_shortlist_is_reported_as_null_and_never_acted_on(bad):
    result, m = await _run(person_mode="act", person=AsyncMock(return_value=_person_answer(bad)))

    assert m.person_report.await_args.kwargs["choice"] is None
    assert "not in the shortlist" in m.person_report.await_args.kwargs["reason"]
    m.person_act.assert_not_awaited()
    assert result["person_resolution"]["outcome"] == "null_answer"


@pytest.mark.asyncio
async def test_an_empty_shortlist_completes_the_attempt_without_asking_the_model():
    result, m = await _run(person_shortlist=AsyncMock(return_value=_person_shortlist(candidates=[])))

    m.person.assert_not_awaited()
    assert m.person_report.await_args.kwargs["choice"] is None
    m.org_shortlist.assert_not_awaited()  # still a person: not sent to the organization resolver
    assert result["person_resolution"]["outcome"] == "no_candidates"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, outcome, reported, org_asked",
    [
        ({"person_shortlist": AsyncMock(side_effect=BrokerClientError("503"))}, "shortlist_failed", 0, True),
        ({"person": AsyncMock(side_effect=LegBotDispatchError("chose an id not in the list"))}, "dispatch_failed", 0, False),
        ({"person_report": AsyncMock(side_effect=BrokerClientError("409"))}, "report_failed", 1, False),
        ({"person_act": AsyncMock(side_effect=BrokerClientError("500"))}, "act_failed", 1, False),
        # pm-review: an unexpected error before any attempt is known falls through to the organization resolver;
        # one after the attempt opened keeps the record on the person path.
        ({"person_shortlist": AsyncMock(side_effect=RuntimeError("boom"))}, "shortlist_failed", 0, True),
        ({"person_shortlist": AsyncMock(return_value="not a dict")}, "shortlist_failed", 0, True),
        ({"person": AsyncMock(side_effect=RuntimeError("boom"))}, "failed", 0, False),
    ],
)
async def test_a_failure_leaves_the_stub_and_never_stops_the_pipeline(kwargs, outcome, reported, org_asked):
    result, m = await _run(person_mode="act", **kwargs)

    assert result["person_resolution"]["outcome"] == outcome
    assert result["position_id"] == 1 and result["organization_id"] == 10888
    assert m.person_report.await_count == reported
    assert m.org_shortlist.await_count == (1 if org_asked else 0)


# ---- no About text for a person -----------------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("mode, act", [("suggest", None), ("act", {"acted": True, "reason": ""}), ("act", None)])
async def test_a_record_the_model_named_a_legislator_gets_no_organization_about_text(mode, act):
    kwargs = {"person_act": AsyncMock(return_value=act)} if act else {}
    result, m = await _run(person_mode=mode, **kwargs)

    m.about.assert_not_awaited()
    assert result["organization_needs_content"] is False and "about_content" not in result


@pytest.mark.asyncio
async def test_a_null_answer_or_a_non_person_still_gets_its_about_text():
    for kwargs in ({"person": AsyncMock(return_value=_person_answer(None))},
                   {"person_shortlist": AsyncMock(return_value=_NOT_A_PERSON)}):
        _, m = await _run(**kwargs)
        m.about.assert_awaited_once()


# ---- the evaluation gate ------------------------------------------------------------------------------------------

def _oracle(loaded):
    """A resolver that answers every case with its label (the best possible model)."""
    expected = {json.dumps(c["organization"], sort_keys=True): c["expected"] for c in loaded["cases"]}

    async def ask(organization, candidates):
        return {"answer": {"choice": expected[json.dumps(organization, sort_keys=True)]}}
    return ask


def test_the_frozen_fixture_holds_the_known_cases_and_every_expected_id_is_in_its_shortlist():
    loaded = load_cases()
    by_id = {c["case_id"]: c for c in loaded["cases"]}
    assert {k: by_id[f"broker-{k}"]["expected"] for k in (10888, 10799, 11068, 13996, 10301)} == {
        10888: "rep:1214", 10799: "rep:1094", 11068: "rep:1184", 13996: None, 10301: None}
    assert by_id["broker-14005"]["expected"] == "rep:940"  # Ramon 2026-11-13: a member's office is that member
    assert {c["shortlist_source"] for c in loaded["cases"]} == {"broker", "search", "synthetic"}
    assert loaded["verdicts_source"] == "provisional"


@pytest.mark.asyncio
async def test_even_a_perfect_model_cannot_pass_while_the_broker_returns_empty_shortlists_for_records_that_should_link():
    """The finding this gate records today (2026-11-13): the broker's shortlist is empty for 11115 and the three
    "Office of ..." records, so they can only be null, and recall falls short whatever the model does."""
    loaded = load_cases()
    run = await run_offline(loaded, ask=_oracle(loaded))
    report = run.gate()

    assert report.wrong_matches == []
    missed = {r.case_id for r in run.results if r.kind == "positive" and r.answered_org_id is None}
    assert missed == {"broker-11115", "broker-14005", "broker-14934", "broker-14999"}
    assert report.recall < 0.80 and not run.passed()
    assert "should have linked, answered null: " in run.render()


@pytest.mark.asyncio
async def test_a_single_wrong_link_fails_the_gate_and_a_provisional_fixture_is_never_a_pass():
    loaded = load_cases()
    loaded["cases"] = [c for c in loaded["cases"] if c["candidates"]]  # only cases the broker can shortlist
    perfect = await run_offline(loaded, ask=_oracle(loaded))
    assert perfect.gate().scored_pass and not perfect.passed()  # provisional labels
    loaded["verdicts_source"] = "reviewed"
    assert (await run_offline(loaded, ask=_oracle(loaded))).passed()

    oracle = _oracle(loaded)

    async def one_wrong(organization, candidates):
        if organization["name"] == "Rep. Smith":
            return {"answer": {"choice": "rep:1214"}}  # a bare surname linked to a Smith
        return await oracle(organization, candidates)
    run = await run_offline(loaded, ask=one_wrong)
    assert [r.case_id for r in run.gate().wrong_matches] == ["synthetic-surname-no-state"] and not run.passed()


@pytest.mark.asyncio
async def test_a_case_legbot_did_not_answer_makes_the_run_incomplete():
    loaded = load_cases()
    loaded["verdicts_source"] = "reviewed"
    loaded["cases"] = [c for c in loaded["cases"] if c["candidates"]]
    oracle = _oracle(loaded)

    async def flaky(organization, candidates):
        if organization["name"] == "Sen. Joe Gruters":
            raise LegBotDispatchError("timeout")
        return await oracle(organization, candidates)
    run = await run_offline(loaded, ask=flaky)
    assert run.errors and run.errors[0][0] == "broker-11060" and not run.passed()


@pytest.mark.asyncio
async def test_an_empty_shortlist_is_null_without_asking_the_model():
    loaded = load_cases()
    ask = AsyncMock(side_effect=_oracle(loaded))
    run = await run_offline(loaded, ask=ask)
    assert ask.await_count == sum(1 for c in loaded["cases"] if c["candidates"])
    assert {r.case_id: r.tier for r in run.results}["broker-10301"] == "no_candidates"


def test_the_shadow_gate_uses_the_person_thresholds(tmp_path):
    attempts = tmp_path / "attempts.jsonl"
    attempts.write_text("\n".join(json.dumps({"attempt_id": i, "choice": f"rep:{i}" if i <= 5 else None,
                                              "input_name": f"Rep. {i}", "eligible": True}) for i in range(1, 11)))
    reviews = tmp_path / "reviews.json"
    reviews.write_text(json.dumps({str(i): "right" for i in range(1, 6)}))
    script = str(CASES_PATH.parents[2] / "scripts" / "person_resolution_gate.py")
    ok = subprocess.run([sys.executable, script, "shadow", str(attempts), str(reviews)], capture_output=True, text=True, check=False)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "need >= 10" in ok.stdout and "need >= 5" in ok.stdout

    reviews.write_text(json.dumps({**{str(i): "right" for i in range(1, 5)}, "5": "wrong"}))
    wrong = subprocess.run([sys.executable, script, "shadow", str(attempts), str(reviews)], capture_output=True, text=True, check=False)
    assert wrong.returncode == 1 and "reviewed wrong: 1" in wrong.stdout
