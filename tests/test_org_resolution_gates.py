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
    evaluate_offline_gate,
    evaluate_shadow_gate,
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
    return [
        CaseResult(case_id=cid, kind=kind, expected_org_id=exp, answered_org_id=answer(cid, exp), tier="model")
        for cid, kind, exp in cases
    ]


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


def test_an_empty_run_or_a_run_without_positives_cannot_pass():
    assert not evaluate_offline_gate([]).passed
    assert not evaluate_offline_gate([CaseResult("n", NEGATIVE, None, None)]).passed


def test_the_report_breaks_results_down_by_tier():
    results = [
        CaseResult("a", POSITIVE, 1, 1, tier="alias"),
        CaseResult("b", POSITIVE, 2, 2, tier="model"),
        CaseResult("c", POSITIVE, 3, None, tier="none"),
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


def test_the_command_exits_non_zero_on_a_failed_gate(tmp_path):
    rows = [
        {"case_id": "p1", "kind": "positive", "expected_org_id": 1, "answered_org_id": 1, "tier": "model"},
        {"case_id": "n1", "kind": "negative", "expected_org_id": None, "answered_org_id": 7, "tier": "model"},
    ]
    results = tmp_path / "results.jsonl"
    results.write_text("\n".join(json.dumps(r) for r in rows))

    done = subprocess.run(
        [sys.executable, "scripts/org_resolution_gates.py", "offline", str(results)],
        capture_output=True, text=True, env={"PYTHONPATH": "src", "PATH": ""},
    )

    assert done.returncode == 1 and "FAIL" in done.stdout and "WRONG n1" in done.stdout
