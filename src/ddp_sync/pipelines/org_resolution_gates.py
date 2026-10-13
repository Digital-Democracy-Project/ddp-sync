"""The two release gates for organization resolution act mode (SYNC-98, PLAN-notifications-alert-engine.md 6.7.6).

Pure scoring, no I/O: given the answers a resolver produced, decide whether the gate passes. The resolver
under test (the broker shortlist plus LegBot's resolve_organization, SYNC-97) is not in this module; a run
of it produces `CaseResult` rows, and the shadow gate reads the broker's attempt records.

Offline gate (a frozen fixture, one case per variant spelling), both parts required:
  1. zero wrong matches across EVERY case, held-out or not. A wrong match is any non-null answer that is
     not the case's expected organization, including a non-null answer on a case that should stay a stub;
  2. at least `MIN_RECALL` of the positive cases resolved to the correct organization. A null on a positive
     counts only against this share, so a resolver that always answers null cannot pass.
`exact` cases (the canonical spelling itself) are reported but not counted toward recall.

Shadow gate (suggest mode on real traffic): at least `MIN_ELIGIBLE` eligible invocations and at least
`MIN_NON_NULL` non-null suggestions, every non-null suggestion reviewed by a person, none wrong.

Re-run both after any change that can alter which organization is chosen: the prompt, the model, the
shortlist logic, the cleaned-name key, the candidate filters, or the broker checks.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

MIN_RECALL = 0.80  # proposed in the plan; Ramon sets the real threshold
MIN_ELIGIBLE = 200
MIN_NON_NULL = 30

POSITIVE, NEGATIVE, EXACT = "positive", "negative", "exact"


@dataclass(frozen=True)
class CaseResult:
    """One labeled case and what the resolver answered.

    expected_org_id: the canonical organization a positive/exact case must resolve to; None for a negative
    (the spelling must stay a stub). answered_org_id: None when the resolver answered null.
    tier: which tier answered ("exact", "alias", "model", or "none" for a null).
    """

    case_id: str
    kind: str  # POSITIVE, NEGATIVE or EXACT
    expected_org_id: int | None
    answered_org_id: int | None
    tier: str = "none"
    held_out: bool = False

    def __post_init__(self):
        # A malformed label must not quietly drop out of the recall denominator.
        if self.kind not in (POSITIVE, NEGATIVE, EXACT):
            raise ValueError(f"case {self.case_id}: unknown kind {self.kind!r}")
        if (self.kind == NEGATIVE) != (self.expected_org_id is None):
            raise ValueError(f"case {self.case_id}: a negative has no expected organization, the others need one")

    @property
    def is_wrong(self) -> bool:
        return self.answered_org_id is not None and self.answered_org_id != self.expected_org_id

    @property
    def is_correct_positive(self) -> bool:
        return self.kind == POSITIVE and self.answered_org_id == self.expected_org_id


@dataclass
class OfflineGateReport:
    passed: bool
    wrong_matches: list[CaseResult]
    positives: int
    positives_correct: int
    recall: float | None
    min_recall: float
    by_tier: dict[str, dict[str, int]] = field(default_factory=dict)
    exact_cases: int = 0
    exact_correct: int = 0

    def render(self) -> str:
        lines = [
            f"offline gate: {'PASS' if self.passed else 'FAIL'}",
            f"  wrong matches: {len(self.wrong_matches)} (must be 0)",
            "  recall: "
            + ("n/a (no positive cases)" if self.recall is None else f"{self.recall:.1%}")
            + f" ({self.positives_correct}/{self.positives} positives; must be >= {self.min_recall:.0%})",
            f"  exact-spelling cases (not in recall): {self.exact_correct}/{self.exact_cases} correct",
            "  by tier that answered: "
            + ", ".join(f"{tier}={counts}" for tier, counts in sorted(self.by_tier.items())),
        ]
        lines += [
            f"  WRONG {r.case_id}: expected {r.expected_org_id}, answered {r.answered_org_id} (tier {r.tier})"
            for r in self.wrong_matches
        ]
        return "\n".join(lines)


def evaluate_offline_gate(results: list[CaseResult], *, min_recall: float = MIN_RECALL) -> OfflineGateReport:
    """Score a full run over the labeled set. An empty run, or one with no positives, cannot pass."""
    positives = [r for r in results if r.kind == POSITIVE]
    exact = [r for r in results if r.kind == EXACT]
    wrong = [r for r in results if r.is_wrong]
    correct = sum(1 for r in positives if r.is_correct_positive)
    recall = (correct / len(positives)) if positives else None
    by_tier: dict[str, Counter] = {}
    for r in results:
        by_tier.setdefault(r.tier, Counter())["cases"] += 1
        if r.is_wrong:
            by_tier[r.tier]["wrong"] += 1
        elif r.answered_org_id is not None:
            by_tier[r.tier]["correct"] += 1
    return OfflineGateReport(
        passed=bool(positives) and not wrong and recall >= min_recall,
        wrong_matches=wrong,
        positives=len(positives),
        positives_correct=correct,
        recall=recall,
        min_recall=min_recall,
        by_tier={tier: dict(counts) for tier, counts in by_tier.items()},
        exact_cases=len(exact),
        exact_correct=sum(1 for r in exact if r.answered_org_id == r.expected_org_id),
    )


@dataclass(frozen=True)
class Attempt:
    """One resolution attempt as the broker recorded it (BROKER-191). `eligible` attempts are those the
    broker opened for a pipeline-created stub; `choice` is the model's reported answer (None = null)."""

    attempt_id: int
    choice: int | None
    input_name: str = ""
    eligible: bool = True


@dataclass
class ShadowGateReport:
    passed: bool
    eligible: int
    non_null: int
    awaiting_review: list[Attempt]
    wrong: list[Attempt]
    non_null_attempts: list[Attempt]

    def render(self) -> str:
        return "\n".join(
            [
                f"shadow gate: {'PASS' if self.passed else 'FAIL'}",
                f"  eligible invocations: {self.eligible} (need >= {MIN_ELIGIBLE})",
                f"  non-null suggestions: {self.non_null} (need >= {MIN_NON_NULL})",
                f"  awaiting review: {len(self.awaiting_review)} (must be 0)",
                f"  reviewed wrong: {len(self.wrong)} (must be 0)",
            ]
            + [f"  - attempt {a.attempt_id}: {a.input_name!r} -> organization {a.choice}" for a in self.non_null_attempts]
        )


def evaluate_shadow_gate(
    attempts: list[Attempt],
    reviews: dict[int, str],
    *,
    min_eligible: int = MIN_ELIGIBLE,
    min_non_null: int = MIN_NON_NULL,
) -> ShadowGateReport:
    """Score suggest-mode traffic. `reviews` maps attempt id to a person's verdict, "right" or "wrong";
    any other value counts as not yet reviewed."""
    ids = [a.attempt_id for a in attempts]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate attempt ids: one review could cover several suggestions")
    eligible = [a for a in attempts if a.eligible]
    non_null = [a for a in eligible if a.choice is not None]
    awaiting = [a for a in non_null if reviews.get(a.attempt_id) not in ("right", "wrong")]
    wrong = [a for a in non_null if reviews.get(a.attempt_id) == "wrong"]
    return ShadowGateReport(
        passed=len(eligible) >= min_eligible and len(non_null) >= min_non_null and not awaiting and not wrong,
        eligible=len(eligible),
        non_null=len(non_null),
        awaiting_review=awaiting,
        wrong=wrong,
        non_null_attempts=non_null,
    )
