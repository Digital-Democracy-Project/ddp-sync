"""SYNC-98: the two release gates, including the acceptance check that a broken resolver is rejected."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from ddp_sync.pipelines.org_resolution_gates import (
    EXACT,
    NEGATIVE,
    POSITIVE,
    Attempt,
    CaseResult,
    Groups,
    evaluate_offline_gate,
    evaluate_shadow_gate,
)


def _case(case_id, kind, expected, answered, *, tier="model", expected_group="auto", answered_group="auto", **kw):
    """A CaseResult where organization N is in group "gN" unless a group is given."""
    if expected_group == "auto":
        expected_group = None if expected is None else f"g{expected}"
    if answered_group == "auto":
        answered_group = None if answered is None else f"g{answered}"
    return CaseResult(
        case_id=case_id, kind=kind, expected_org_id=expected, answered_org_id=answered, tier=tier,
        expected_group_id=expected_group, answered_group_id=answered_group, **kw,
    )


def _fixture(answer):
    """10 positives (orgs 1..10), 3 negatives and 2 exact cases; `answer(case_id, expected)` is the resolver."""
    cases = []
    for i in range(1, 11):
        cases.append((f"p{i}", POSITIVE, i))
    for i in range(1, 4):
        cases.append((f"n{i}", NEGATIVE, None))
    for i in range(1, 3):
        cases.append((f"e{i}", EXACT, i))
    return [_case(cid, kind, exp, answer(cid, exp)) for cid, kind, exp in cases]


def test_a_perfect_resolver_passes():
    report = evaluate_offline_gate(_fixture(lambda cid, exp: exp))

    assert report.passed and report.recall == 1.0 and report.wrong_matches == []


def test_a_resolver_that_always_answers_null_fails_the_recall_part():
    report = evaluate_offline_gate(_fixture(lambda cid, exp: None))

    assert not report.passed
    assert report.wrong_matches == [] and report.recall == 0.0


def test_one_wrong_match_fails_even_with_perfect_recall():
    """The 'Better Ballot NC into SC' case: a negative answered with the neighbouring organization."""
    report = evaluate_offline_gate(_fixture(lambda cid, exp: 99 if cid == "n1" else exp))

    assert not report.passed
    assert [r.case_id for r in report.wrong_matches] == ["n1"]
    assert report.recall == 1.0


def test_a_wrong_organization_on_a_positive_is_wrong_not_just_a_miss():
    report = evaluate_offline_gate(_fixture(lambda cid, exp: 99 if cid == "p3" else exp))

    assert not report.passed and [r.case_id for r in report.wrong_matches] == ["p3"]


def test_recall_threshold_boundary_and_nulls_count_only_against_recall():
    two_nulls = evaluate_offline_gate(_fixture(lambda cid, exp: None if cid in ("p1", "p2") else exp))
    three_nulls = evaluate_offline_gate(_fixture(lambda cid, exp: None if cid in ("p1", "p2", "p3") else exp))

    assert two_nulls.recall == pytest.approx(0.8) and two_nulls.passed
    assert not three_nulls.passed and three_nulls.wrong_matches == []


def test_exact_cases_are_reported_but_not_counted_in_recall():
    report = evaluate_offline_gate(_fixture(lambda cid, exp: None if cid.startswith("e") else exp))

    assert report.passed and (report.exact_cases, report.exact_correct) == (2, 0) and report.recall == 1.0


def test_malformed_cases_and_duplicate_attempts_are_rejected_not_scored():
    with pytest.raises(ValueError):
        _case("x", "positve", 1, 1)                # a typo in the kind would drop out of recall
    with pytest.raises(ValueError):
        _case("x", POSITIVE, None, None)           # a positive needs an expected organization
    with pytest.raises(ValueError):
        _case("x", NEGATIVE, 5, None)              # a negative must not have one
    with pytest.raises(ValueError):
        _case("x", POSITIVE, 1, None, expected_group=None)   # ... nor a missing expected group
    with pytest.raises(ValueError):
        _case("x", POSITIVE, 1, 1, answered_group=None)      # an answer without its group is not scorable
    with pytest.raises(ValueError):
        evaluate_shadow_gate([Attempt(1, 5), Attempt(1, 6)], {1: "right"})


def test_an_empty_run_or_a_run_without_positives_cannot_pass():
    assert not evaluate_offline_gate([]).passed
    assert not evaluate_offline_gate([_case("n", NEGATIVE, None, None)]).passed


def test_the_report_breaks_results_down_by_tier():
    results = [
        _case("a", POSITIVE, 1, 1, tier="alias"),
        _case("b", POSITIVE, 2, 2, tier="model"),
        _case("c", POSITIVE, 3, None, tier="none"),
    ]
    report = evaluate_offline_gate(results)

    assert report.by_tier["alias"] == {"cases": 1, "correct": 1}
    assert report.by_tier["none"] == {"cases": 1}
    assert "by tier" in report.render()


def _attempts(eligible, non_null):
    return [
        Attempt(attempt_id=i, choice=(100 + i) if i <= non_null else None, input_name=f"org {i}")
        for i in range(1, eligible + 1)
    ]


def test_shadow_gate_needs_volume_and_every_suggestion_reviewed_and_none_wrong():
    attempts = _attempts(200, 30)
    reviews = {i: "right" for i in range(1, 31)}

    assert evaluate_shadow_gate(attempts, reviews).passed
    assert not evaluate_shadow_gate(_attempts(199, 30), reviews).passed          # too few eligible
    assert not evaluate_shadow_gate(_attempts(200, 29), {i: "right" for i in range(1, 30)}).passed  # too few non-null
    unreviewed = evaluate_shadow_gate(attempts, {i: "right" for i in range(1, 30)})
    assert not unreviewed.passed and [a.attempt_id for a in unreviewed.awaiting_review] == [30]
    wrong = evaluate_shadow_gate(attempts, {**reviews, 7: "wrong"})
    assert not wrong.passed and [a.attempt_id for a in wrong.wrong] == [7]


def test_shadow_gate_ignores_ineligible_attempts_and_unknown_review_words():
    attempts = _attempts(200, 30) + [Attempt(attempt_id=999, choice=5, eligible=False)]

    assert evaluate_shadow_gate(attempts, {i: "right" for i in range(1, 31)}).passed
    assert not evaluate_shadow_gate(attempts, {i: "ok" for i in range(1, 31)}).passed


def _loaded(**over):
    """A tiny group-aware fixture: p1 expects organization 1; organization 5 is a second record of group g1
    (a stub), 6 is a chapter of 1 (its own group), 7 is unrelated."""
    loaded = {
        "seed_version": 2, "verdicts_source": "reviewed", "seed_sha256": "abc", "organizations": 4,
        "org_groups": {"1": "g1", "2": "g2", "3": "g3", "4": "g4", "5": "g1", "6": "g6", "7": "g7"},
        "cases": [
            {"case_id": "p1", "kind": "positive", "expected_org_id": 1, "expected_group_id": "g1"},
            {"case_id": "n1", "kind": "negative", "expected_org_id": None, "expected_group_id": None},
        ],
    }
    loaded.update(over)
    return loaded


def _run_command(tmp_path, rows, loaded, *extra):
    results, loaded_path = tmp_path / "results.jsonl", tmp_path / "loaded.json"
    results.write_text("\n".join(json.dumps(r) for r in rows))
    loaded_path.write_text(json.dumps(loaded))
    return subprocess.run(
        [sys.executable, "scripts/org_resolution_gates.py", "offline", str(results), "--loaded", str(loaded_path), *extra],
        capture_output=True, text=True, env={"PYTHONPATH": "src", "PATH": ""},
    )


def test_the_command_exits_non_zero_on_a_failed_gate(tmp_path):
    rows = [
        {"case_id": "p1", "answered_org_id": 1, "tier": "model"},
        {"case_id": "n1", "answered_org_id": 7, "tier": "model"},
    ]

    done = _run_command(tmp_path, rows, _loaded())

    assert done.returncode == 1 and "FAIL" in done.stdout and "WRONG n1" in done.stdout


def test_the_command_passes_when_another_record_of_the_right_organization_is_chosen(tmp_path):
    rows = [{"case_id": "p1", "answered_org_id": 5, "tier": "model"}, {"case_id": "n1", "answered_org_id": None}]

    done = _run_command(tmp_path, rows, _loaded(), "--report-out", str(tmp_path / "report.json"))

    assert done.returncode == 0 and "offline gate: PASS" in done.stdout
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["passed"] is True and report["other_record_picks"] == 1 and report["other_record_stub_picks"] == 1
    assert (report["seed_sha256"], report["seed_version"], report["verdicts_source"], report["scoring_version"]) == (
        "abc", 2, "reviewed", "2",
    )


def test_the_command_refuses_an_old_fixture_and_results_for_unknown_cases(tmp_path):
    rows = [{"case_id": "p1", "answered_org_id": 1, "tier": "model"}]
    old = {k: v for k, v in _loaded().items() if k not in ("org_groups", "seed_version")}

    refused = _run_command(tmp_path, rows, old)
    unknown = _run_command(tmp_path, [{"case_id": "zzz", "answered_org_id": 1}], _loaded())

    assert refused.returncode == 2 and "no org_groups" in refused.stderr and "BROKER-206" in refused.stderr
    assert unknown.returncode == 2 and "zzz" in unknown.stderr


def test_a_provisional_fixture_is_never_a_gate_result(tmp_path):
    rows = [{"case_id": "p1", "answered_org_id": 1, "tier": "model"}, {"case_id": "n1", "answered_org_id": None}]

    done = _run_command(tmp_path, rows, _loaded(verdicts_source="provisional"))
    missing = _run_command(tmp_path, rows, {k: v for k, v in _loaded().items() if k != "verdicts_source"})

    for run in (done, missing):  # a fixture that does not say it is reviewed counts as provisional
        assert run.returncode == 1 and "PROVISIONAL" in run.stdout and "would PASS" in run.stdout


# --- SYNC-109: group-aware scoring ---------------------------------------------------------------------

def test_another_record_of_the_same_organization_is_correct_and_reported_separately():
    results = [
        _case("p1", POSITIVE, 1, 5, answered_group="g1", answered_is_stub=True),   # a duplicate stub of org 1
        _case("p2", POSITIVE, 2, 8, answered_group="g2"),                          # a duplicate reviewed record
        _case("p3", POSITIVE, 3, 3),
        _case("n1", NEGATIVE, None, None),
    ]

    report = evaluate_offline_gate(results)

    assert report.passed and report.wrong_matches == [] and report.recall == 1.0
    assert (report.other_record_picks, report.other_record_stub_picks) == (2, 1)
    assert "chose another record of the same organization (not errors): 2 (of which stubs: 1)" in report.render()


def test_a_record_of_a_different_group_is_wrong_and_a_chapter_is_not_its_parent():
    """Group g1 is the national organization; its chapter has its own group, so choosing the parent for the
    chapter's spelling (or the reverse) is a wrong match even though the names overlap."""
    chapter_to_parent = _case("c1", POSITIVE, 6, 1, expected_group="g6", answered_group="g1")
    parent_to_chapter = _case("c2", POSITIVE, 1, 6, expected_group="g1", answered_group="g6")
    other_org = _case("p1", POSITIVE, 1, 9, expected_group="g1", answered_group="g9")

    report = evaluate_offline_gate([chapter_to_parent, parent_to_chapter, other_org])

    assert not report.passed and [r.case_id for r in report.wrong_matches] == ["c1", "c2", "p1"]
    assert report.other_record_picks == 0


def test_a_null_on_a_positive_is_a_miss_not_a_wrong_match_and_a_negative_is_wrong_on_any_answer():
    report = evaluate_offline_gate(
        [_case("p1", POSITIVE, 1, None), _case("n1", NEGATIVE, None, 5, answered_group="g1")]
    )

    assert [r.case_id for r in report.wrong_matches] == ["n1"]
    assert (report.positives, report.positives_correct, report.other_record_picks) == (1, 0, 0)


def test_groups_refuses_an_old_fixture_and_labels_each_answer_with_its_group():
    with pytest.raises(ValueError, match="no org_groups"):
        Groups.from_loaded({"seed_version": 1, "cases": []})
    with pytest.raises(ValueError, match="BROKER-206"):
        Groups.from_loaded({"cases": [], "org_groups": {"1": "g1"}})  # groups without the version

    groups = Groups.from_loaded(_loaded())
    case = _loaded()["cases"][0]
    sibling = groups.result(case, answered_org_id=5, tier="model")
    unmapped = groups.result(case, answered_org_id=999, tier="model")
    null = groups.result(case, answered_org_id=None, tier="none")

    assert (sibling.answered_group_id, sibling.answered_is_stub, sibling.is_correct) == ("g1", True, True)
    assert unmapped.is_wrong and unmapped.answered_group_id == "unmapped:999"   # outside the map: never equal
    assert null.answered_group_id is None and not null.is_wrong


def test_the_stub_flag_uses_the_loaders_list_when_it_has_one():
    listed = Groups.from_loaded(_loaded(stub_org_ids=[7]))
    by_order = Groups.from_loaded(_loaded())   # organizations = 4: ids after 4 are the per-case stubs

    assert (listed.is_stub(7), listed.is_stub(5)) == (True, False)
    assert (by_order.is_stub(5), by_order.is_stub(4)) == (True, False)


def test_provisional_and_the_report_json_carry_the_fixture_identity():
    results = [_case("p1", POSITIVE, 1, 1), _case("n1", NEGATIVE, None, None)]
    groups = Groups.from_loaded(_loaded(verdicts_source="provisional"))

    report = evaluate_offline_gate(results, meta=groups.meta)

    assert report.scored_pass and not report.passed and report.provisional
    assert report.to_dict()["passed"] is False and report.to_dict()["verdicts_source"] == "provisional"
    assert report.render().startswith("offline gate: PROVISIONAL")
