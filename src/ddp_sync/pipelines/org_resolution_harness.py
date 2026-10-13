"""Run the whole organization resolver over the labeled cases (SYNC-98, PLAN-notifications-alert-engine.md 6.7.6).

The cases were loaded into a THROWAWAY broker by `manage.py load_resolution_eval_fixture` (ddp-broker-py), which
wrote the file this reads. For a case whose name already resolves in the broker (exact or alias tier) the answer
is already in the file. For every other case this runs the SAME resolve step the pipeline runs
(`resolve_organization_for_position`, SYNC-97) in suggest mode: the broker builds the shortlist, LegBot's
`resolve_organization` chooses or answers null, and the broker records the attempt (which holds the model and
prompt version). The results are scored by `org_resolution_gates.evaluate_offline_gate`, by group: the file must
be a group-aware (version 2) fixture, and an old one is refused (SYNC-109).

It writes attempts to the broker it is pointed at, so the CLI insists the broker is declared throwaway.

A case the resolver could not finish (the broker or LegBot failed) is counted as an error, not as a null: a run
with any error is incomplete and cannot pass the gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ddp_sync.pipelines.bill_organization_position_research import resolve_organization_for_position
from ddp_sync.pipelines.org_resolution_gates import CaseResult, Groups, OfflineGateReport, evaluate_offline_gate

# resolve_organization_for_position outcomes that mean the resolver ANSWERED (a choice or a null).
_ANSWERED = {"suggested", "null_answer", "no_candidates"}


@dataclass
class HarnessRun:
    results: list[CaseResult]
    groups: Groups
    errors: list[tuple[str, str]] = field(default_factory=list)  # (case_id, outcome)
    seed_sha256: str = ""
    skipped_by_loader: int = 0
    limit: int | None = None  # a smoke run: scored a subset, so it is never a gate result

    def gate(self, *, min_recall: float) -> OfflineGateReport:
        return evaluate_offline_gate(self.results, min_recall=min_recall, meta=self.groups.meta)

    def passed(self, *, min_recall: float) -> bool:
        return self.limit is None and not self.errors and self.gate(min_recall=min_recall).passed

    def render(self, *, min_recall: float) -> str:
        report = self.gate(min_recall=min_recall)
        kinds = {kind: sum(r.kind == kind for r in self.results) for kind in ("positive", "negative", "exact")}
        header = [
            f"seed sha256: {self.seed_sha256 or '(unknown)'}; cases scored: {len(self.results)} {kinds}; "
            f"skipped by the loader: {self.skipped_by_loader}; held out: {sum(r.held_out for r in self.results)}",
        ]
        if self.limit is not None:
            header.append(
                f"INCOMPLETE: --limit {self.limit} scored only a subset of the cases; a smoke run is never a gate result"
            )
        if self.errors:
            header.append(
                f"INCOMPLETE: {len(self.errors)} case(s) the resolver could not finish (counted as null; the run "
                "cannot pass): " + ", ".join(f"{case} [{outcome}]" for case, outcome in self.errors[:10])
                + (" ..." if len(self.errors) > 10 else "")
            )
        verdict = "PASS" if self.passed(min_recall=min_recall) else "FAIL"
        return "\n".join(header + [report.render(), f"harness: {verdict}"])


async def run_harness(
    loaded: dict,
    *,
    broker_api_base: str,
    broker_api_token: str,
    limit: int | None = None,
    resolve=resolve_organization_for_position,
    progress=None,
) -> HarnessRun:
    """Score every case in `loaded`. `limit` runs only the first N cases that need the model (a smoke run).
    Raises ValueError for a loaded file that is not a group-aware (version 2) fixture."""
    groups = Groups.from_loaded(loaded)
    run = HarnessRun(
        results=[], groups=groups, seed_sha256=loaded.get("seed_sha256", ""),
        skipped_by_loader=len(loaded.get("skipped", [])), limit=limit,
    )
    asked = 0
    for case in loaded["cases"]:
        if case.get("tier"):  # answered before the model: exact or alias
            run.results.append(groups.result(case, answered_org_id=case["answered_org_id"], tier=case["tier"]))
            continue
        if limit is not None and asked >= limit:
            continue
        asked += 1
        resolution = await resolve(
            position_id=case["position_id"], org_name=case["input_name"],
            citation_url=case.get("citation_url", ""), citation_excerpt=case.get("citation_excerpt", ""),
            mode="suggest", broker_api_base=broker_api_base, broker_api_token=broker_api_token,
        )
        if resolution["outcome"] in _ANSWERED:
            choice = resolution["choice"]
            run.results.append(
                groups.result(case, answered_org_id=choice, tier="model" if choice is not None else "none")
            )
        else:
            run.errors.append((case["case_id"], resolution["outcome"]))
            run.results.append(groups.result(case, answered_org_id=None, tier="none"))
        if progress:
            progress(asked, case["case_id"], resolution["outcome"])
    return run
