"""The evaluation gate for the person resolver before act mode is switched on for people (SYNC-150; BROKER-441,
AGENTS-220). The scoring is the organization gate's (`org_resolution_gates`, SYNC-98), reused, not copied.

Offline gate: a frozen fixture of the broker's own shortlists (`tests/fixtures/person_resolution_cases.json`, read on
the dev copy without opening attempts) and labeled answers. `run_offline` asks LegBot `resolve_person` about each
case's shortlist exactly as the pipeline does (`dispatch_resolve_person`); a case with an empty shortlist is a null
with no model call, as in the pipeline. Nothing is written to any broker. Scored by `evaluate_offline_gate`: zero
wrong links over every case and at least `MIN_RECALL` of the should-link cases linked. A case whose candidate id is
the expected one is correct; there are no duplicate records of a legislator to group, so each id is its own group.
A fixture whose labels are not `reviewed` is PROVISIONAL (never a gate result), as for organizations.

Shadow gate: `suggest` mode on real traffic (the broker's backfill, BROKER-441, and new stubs), scored by
`evaluate_shadow_gate` with the person thresholds below: every non-null suggestion read by a person, none wrong.

Re-run both after any change to the prompt or model (AGENTS-220), or to the broker's candidate check or shortlist.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from ddp_sync.pipelines.org_resolution_gates import (
    MIN_RECALL,
    NEGATIVE,
    POSITIVE,
    CaseResult,
    OfflineGateReport,
    evaluate_offline_gate,
)
from ddp_sync.services.legbot_client import LegBotDispatchError, dispatch_resolve_person

CASES_PATH = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "person_resolution_cases.json"

# Proposed, for Ramon to set: the whole table offers about a dozen records (12 of 5,679 on the dev copy, 2026-11-13),
# so the organization gate's 200 / 30 can never be reached for people.
PERSON_MIN_ELIGIBLE = 10
PERSON_MIN_NON_NULL = 5


def case_result(case: dict, answered: str | None, *, tier: str) -> CaseResult:
    """One labeled case as the organization gate's `CaseResult`: the expected candidate id is its own group."""
    expected = case["expected"]
    return CaseResult(
        case_id=case["case_id"], kind=POSITIVE if expected else NEGATIVE,
        expected_org_id=expected, answered_org_id=answered, tier=tier,
        expected_group_id=expected, answered_group_id=answered,
    )


@dataclass
class OfflineRun:
    results: list[CaseResult]
    meta: dict
    errors: list[tuple[str, str]] = field(default_factory=list)  # (case_id, why): the run is incomplete

    def gate(self, *, min_recall: float = MIN_RECALL) -> OfflineGateReport:
        return evaluate_offline_gate(self.results, min_recall=min_recall, meta=self.meta)

    def passed(self, *, min_recall: float = MIN_RECALL) -> bool:
        return not self.errors and self.gate(min_recall=min_recall).passed

    def render(self, *, min_recall: float = MIN_RECALL) -> str:
        lines = [f"cases: {len(self.results)}; fixture frozen {self.meta.get('frozen', '?')}"]
        if self.errors:
            lines.append(
                f"INCOMPLETE: {len(self.errors)} case(s) LegBot did not answer (counted as null; the run cannot pass): "
                + ", ".join(f"{case} [{why}]" for case, why in self.errors)
            )
        missed = [r.case_id for r in self.results if r.kind == POSITIVE and r.answered_org_id is None]
        if missed:
            lines.append("should have linked, answered null: " + ", ".join(missed))
        lines += [self.gate(min_recall=min_recall).render(), f"person gate: {'PASS' if self.passed(min_recall=min_recall) else 'FAIL'}"]
        return "\n".join(lines)


def load_cases(path: Path = CASES_PATH) -> dict:
    loaded = json.loads(Path(path).read_text())
    ids = [case["case_id"] for case in loaded["cases"]]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{path}: a case id appears twice")
    for case in loaded["cases"]:
        offered = {c["id"] for c in case["candidates"]}
        if case["expected"] is not None and offered and case["expected"] not in offered:
            raise ValueError(f"{path}: case {case['case_id']} expects {case['expected']}, which its shortlist does not hold")
    return loaded


async def run_offline(loaded: dict, *, ask=dispatch_resolve_person) -> OfflineRun:
    """Ask LegBot about every case and score the answers. Writes nothing anywhere."""
    run = OfflineRun(results=[], meta={"verdicts_source": loaded.get("verdicts_source") or "provisional",
                                       "frozen": loaded.get("frozen", "")})
    for case in loaded["cases"]:
        if not case["candidates"]:
            run.results.append(case_result(case, None, tier="no_candidates"))
            continue
        try:
            answer = (await ask(case["organization"], case["candidates"]))["answer"]
        except LegBotDispatchError as exc:
            run.errors.append((case["case_id"], str(exc)[:200]))
            run.results.append(case_result(case, None, tier="none"))
            continue
        choice = answer.get("choice")
        if choice is not None and choice not in {c["id"] for c in case["candidates"]}:
            run.errors.append((case["case_id"], f"choice {choice!r} is not in the shortlist"))
            choice = None
        run.results.append(case_result(case, choice, tier="model" if choice is not None else "none"))
    return run
