"""The two release gates for organization resolution act mode (SYNC-98, PLAN-notifications-alert-engine.md 6.7.6).

Pure scoring, no I/O: given the answers a resolver produced, decide whether the gate passes. The resolver
under test (the broker shortlist plus LegBot's resolve_organization, SYNC-97) is not in this module; a run
of it produces `CaseResult` rows, and the shadow gate reads the broker's attempt records.

Offline gate (a frozen fixture, one case per variant spelling), both parts required:
  1. zero wrong matches across EVERY case, held-out or not. A wrong match is any non-null answer that is
     not in the case's expected GROUP (SYNC-109: the records of one real organization share a group, so
     choosing another record of the right organization is correct; a chapter and its parent are different
     groups, so choosing the parent is wrong), including a non-null answer on a case that should stay a stub;
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
SCORING_VERSION = "2"  # 1 = scored by organization id (SYNC-98); 2 = scored by group (SYNC-109)
MIN_ELIGIBLE = 200
MIN_NON_NULL = 30

POSITIVE, NEGATIVE, EXACT = "positive", "negative", "exact"


@dataclass(frozen=True)
class CaseResult:
    """One labeled case and what the resolver answered.

    expected_org_id / expected_group_id: the organization record the key names, and the group (every record of
    that real organization) a positive/exact case may resolve to; None for a negative (the spelling must stay a
    stub). answered_org_id: None when the resolver answered null; answered_group_id is then None too.
    answered_is_stub: the answered record is one of the per-case stubs (reported, never scored).
    tier: which tier answered ("exact", "alias", "model", or "none" for a null).
    """

    case_id: str
    kind: str  # POSITIVE, NEGATIVE or EXACT
    expected_org_id: int | None
    answered_org_id: int | None
    tier: str = "none"
    held_out: bool = False
    expected_group_id: str | None = None
    answered_group_id: str | None = None
    answered_is_stub: bool = False

    def __post_init__(self):
        # A malformed label must not quietly drop out of the recall denominator.
        if self.kind not in (POSITIVE, NEGATIVE, EXACT):
            raise ValueError(f"case {self.case_id}: unknown kind {self.kind!r}")
        if (self.kind == NEGATIVE) != (self.expected_org_id is None):
            raise ValueError(f"case {self.case_id}: a negative has no expected organization, the others need one")
        if (self.kind == NEGATIVE) != (self.expected_group_id is None):
            raise ValueError(f"case {self.case_id}: a negative has no expected group, the others need one")
        if (self.answered_org_id is None) != (self.answered_group_id is None):
            raise ValueError(f"case {self.case_id}: an answer needs its group (and a null answer has none)")

    @property
    def is_wrong(self) -> bool:
        return self.answered_org_id is not None and (
            self.expected_group_id is None or self.answered_group_id != self.expected_group_id
        )

    @property
    def is_correct(self) -> bool:
        return self.answered_org_id is not None and not self.is_wrong

    @property
    def is_correct_positive(self) -> bool:
        return self.kind == POSITIVE and self.is_correct

    @property
    def is_other_record(self) -> bool:
        """Correct, but a different record of the organization than the key's: a duplicate, not an error."""
        return self.is_correct and self.answered_org_id != self.expected_org_id


@dataclass(frozen=True)
class Groups:
    """The group labels of a loaded evaluation fixture (BROKER-206, `seed_version` 2): which real organization
    every broker record belongs to, so scoring compares organizations, not records."""

    org_groups: dict[str, str]
    organizations: int
    seed_version: int
    verdicts_source: str
    seed_sha256: str = ""
    stub_org_ids: frozenset[int] | None = None

    @classmethod
    def from_loaded(cls, loaded: dict) -> "Groups":
        """Refuses an old (version 1) fixture: scoring it by organization id would silently re-introduce the
        duplicate-record errors this version removes."""
        if loaded.get("seed_version") != 2 or not isinstance(loaded.get("org_groups"), dict):
            raise ValueError(
                "this loaded.json has no org_groups (an old, version 1 evaluation fixture that scores by "
                "organization id): rebuild the seed and loaded.json with the group-aware seed (BROKER-206) "
                "before scoring"
            )
        stubs = loaded.get("stub_org_ids")
        return cls(
            org_groups={str(k): v for k, v in loaded["org_groups"].items()},
            organizations=int(loaded.get("organizations", 0)),
            seed_version=2,
            verdicts_source=loaded.get("verdicts_source") or "provisional",
            seed_sha256=loaded.get("seed_sha256", ""),
            stub_org_ids=frozenset(stubs) if stubs is not None else None,
        )

    def is_stub(self, org_id: int) -> bool:
        """The loader lists the stubs when it can; otherwise rely on its creation order: the reviewed organizations
        take the first `organizations` ids of the fresh scratch database and the per-case stubs come after."""
        if self.stub_org_ids is not None:
            return org_id in self.stub_org_ids
        return org_id > self.organizations

    def group_of(self, org_id: int) -> str:
        # An id outside the map cannot equal a real group, so it scores as a wrong match.
        return self.org_groups.get(str(org_id), f"unmapped:{org_id}")

    def result(self, case: dict, *, answered_org_id: int | None, tier: str) -> CaseResult:
        return CaseResult(
            case_id=case["case_id"], kind=case["kind"], expected_org_id=case["expected_org_id"],
            answered_org_id=answered_org_id, tier=tier, held_out=bool(case.get("held_out")),
            expected_group_id=case.get("expected_group_id"),
            answered_group_id=None if answered_org_id is None else self.group_of(answered_org_id),
            answered_is_stub=answered_org_id is not None and self.is_stub(answered_org_id),
        )

    @property
    def meta(self) -> dict:
        return {
            "seed_sha256": self.seed_sha256, "seed_version": self.seed_version,
            "verdicts_source": self.verdicts_source, "scoring_version": SCORING_VERSION,
        }


@dataclass
class OfflineGateReport:
    scored_pass: bool  # what the numbers say, ignoring whether the verdicts behind the fixture are reviewed
    wrong_matches: list[CaseResult]
    positives: int
    positives_correct: int
    recall: float | None
    min_recall: float
    by_tier: dict[str, dict[str, int]] = field(default_factory=dict)
    exact_cases: int = 0
    exact_correct: int = 0
    other_record_picks: int = 0  # correct answers that chose another record of the organization (not errors)
    other_record_stub_picks: int = 0  # ... of which the other record was a per-case stub
    meta: dict = field(default_factory=dict)

    @property
    def provisional(self) -> bool:
        """A fixture whose group verdicts a person has not reviewed is never a gate result."""
        return bool(self.meta) and self.meta.get("verdicts_source") != "reviewed"

    @property
    def passed(self) -> bool:
        return self.scored_pass and not self.provisional

    def render(self) -> str:
        if self.provisional:
            verdict = (
                f"PROVISIONAL (verdicts not reviewed, so not a gate result; the numbers would "
                f"{'PASS' if self.scored_pass else 'FAIL'})"
            )
        else:
            verdict = "PASS" if self.passed else "FAIL"
        lines = [
            f"offline gate: {verdict}",
            f"  wrong matches: {len(self.wrong_matches)} (must be 0)",
            "  recall: "
            + ("n/a (no positive cases)" if self.recall is None else f"{self.recall:.1%}")
            + f" ({self.positives_correct}/{self.positives} positives; must be >= {self.min_recall:.0%})",
            f"  exact-spelling cases (not in recall): {self.exact_correct}/{self.exact_cases} correct",
            f"  correct answers that chose another record of the same organization (not errors): "
            f"{self.other_record_picks} (of which stubs: {self.other_record_stub_picks})",
            "  by tier that answered: "
            + ", ".join(f"{tier}={counts}" for tier, counts in sorted(self.by_tier.items())),
        ]
        if self.meta:
            lines.append(
                "  fixture: " + ", ".join(f"{k}={v}" for k, v in self.meta.items() if k != "seed_sha256")
            )
        lines += [
            f"  WRONG {r.case_id}: expected group {r.expected_group_id}, answered {r.answered_org_id} "
            f"(group {r.answered_group_id}, tier {r.tier})"
            for r in self.wrong_matches
        ]
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed, "scored_pass": self.scored_pass, "provisional": self.provisional,
            "wrong_matches": [r.case_id for r in self.wrong_matches],
            "positives": self.positives, "positives_correct": self.positives_correct, "recall": self.recall,
            "min_recall": self.min_recall, "exact_cases": self.exact_cases, "exact_correct": self.exact_correct,
            "other_record_picks": self.other_record_picks, "other_record_stub_picks": self.other_record_stub_picks,
            "by_tier": self.by_tier, **self.meta,
        }


def evaluate_offline_gate(
    results: list[CaseResult], *, min_recall: float = MIN_RECALL, meta: dict | None = None
) -> OfflineGateReport:
    """Score a full run over the labeled set. An empty run, or one with no positives, cannot pass.
    `meta` is the fixture's identity (`Groups.meta`); with it, a fixture that is not `reviewed` is PROVISIONAL."""
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
    other = [r for r in results if r.is_other_record]
    return OfflineGateReport(
        scored_pass=bool(positives) and not wrong and recall >= min_recall,
        wrong_matches=wrong,
        positives=len(positives),
        positives_correct=correct,
        recall=recall,
        min_recall=min_recall,
        by_tier={tier: dict(counts) for tier, counts in by_tier.items()},
        exact_cases=len(exact),
        exact_correct=sum(1 for r in exact if r.is_correct),
        other_record_picks=len(other),
        other_record_stub_picks=sum(1 for r in other if r.answered_is_stub),
        meta=dict(meta or {}),
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
