"""SYNC-98: the end-to-end harness (the broker and LegBot are replaced by a fake resolver)."""

from __future__ import annotations

import json
import subprocess
import sys
from unittest.mock import AsyncMock

import pytest

from ddp_sync.pipelines.org_resolution_harness import run_harness


def _loaded():
    """10 positives (orgs 1..10), 2 negatives, 1 exact answered by the broker before the model, 1 alias."""
    cases = []
    for i in range(1, 11):
        cases.append({"case_id": f"p{i}", "kind": "positive", "held_out": i > 7, "expected_org_id": i,
                      "expected_group_id": f"g{i}",
                      "input_name": f"Variant {i}", "citation_url": "https://c.invalid", "citation_excerpt": "x",
                      "tier": None, "answered_org_id": None, "position_id": 100 + i})
    for i in (1, 2):
        cases.append({"case_id": f"n{i}", "kind": "negative", "held_out": False, "expected_org_id": None,
                      "expected_group_id": None, "input_name": f"Chapter {i}", "tier": None, "answered_org_id": None, "position_id": 200 + i})
    cases.append({"case_id": "e1", "kind": "exact", "held_out": False, "expected_org_id": 1, "expected_group_id": "g1",
                  "input_name": "Org 1",
                  "tier": "exact", "answered_org_id": 1, "position_id": None})
    cases.append({"case_id": "a1", "kind": "positive", "held_out": False, "expected_org_id": 11,
                  "expected_group_id": "g11", "input_name": "Org 11",
                  "tier": "alias", "answered_org_id": 11, "position_id": None})
    # Organizations 1..11 are the reviewed ones (their own groups); 50 is a per-case stub that is a second record
    # of organization 1's group, 51 a chapter of organization 1 (a group of its own).
    org_groups = {**{str(i): f"g{i}" for i in range(1, 12)}, "50": "g1", "51": "g51"}
    return {"seed_sha256": "abc", "seed_version": 2, "verdicts_source": "reviewed", "organizations": 11,
            "org_groups": org_groups, "skipped": [{"case_id": "dup"}], "cases": cases}


def _resolver(answers):
    """answers: position_id -> choice (None = null); a missing id is an error."""
    async def resolve(*, position_id, **kwargs):
        if position_id not in answers:
            return {"position_id": position_id, "outcome": "dispatch_failed", "choice": None}
        choice = answers[position_id]
        return {"position_id": position_id, "outcome": "suggested" if choice else "null_answer", "choice": choice}
    return AsyncMock(side_effect=resolve)


def _perfect():
    return {**{100 + i: i for i in range(1, 11)}, 201: None, 202: None}


async def _run(resolver, loaded=None, **kwargs):
    return await run_harness(
        loaded or _loaded(), broker_api_base="http://b", broker_api_token="t", resolve=resolver, **kwargs
    )


@pytest.mark.asyncio
async def test_a_perfect_resolver_passes_and_cases_answered_by_the_broker_skip_the_model():
    resolver = _resolver(_perfect())

    run = await _run(resolver)

    assert run.passed(min_recall=0.8) and run.errors == []
    assert resolver.await_count == 12  # the exact and alias cases were not sent to the resolver
    by_id = {r.case_id: r for r in run.results}
    assert (by_id["e1"].tier, by_id["a1"].tier, by_id["p1"].tier, by_id["n1"].tier) == ("exact", "alias", "model", "none")
    kwargs = resolver.await_args_list[0].kwargs
    assert (kwargs["mode"], kwargs["broker_api_base"], kwargs["broker_api_token"]) == ("suggest", "http://b", "t")
    assert kwargs["org_name"] == "Variant 1" and kwargs["citation_url"] == "https://c.invalid"
    assert "seed sha256: abc" in run.render(min_recall=0.8)


@pytest.mark.asyncio
async def test_an_always_null_resolver_fails_the_recall_part():
    run = await _run(_resolver({**{100 + i: None for i in range(1, 11)}, 201: None, 202: None}))

    assert not run.passed(min_recall=0.8)
    assert run.gate(min_recall=0.8).wrong_matches == []


@pytest.mark.asyncio
async def test_one_wrong_match_fails_the_gate_even_with_perfect_recall():
    """A chapter spelling merged into its neighbour, the Better Ballot NC into SC case."""
    answers = _perfect()
    answers[201] = 5

    run = await _run(_resolver(answers))

    report = run.gate(min_recall=0.8)
    assert not run.passed(min_recall=0.8) and [r.case_id for r in report.wrong_matches] == ["n1"]
    assert report.recall == 1.0


@pytest.mark.asyncio
async def test_a_case_the_resolver_could_not_finish_is_an_error_and_the_run_cannot_pass():
    answers = _perfect()
    del answers[103]  # LegBot failed on one case

    run = await _run(_resolver(answers))

    assert run.errors == [("p3", "dispatch_failed")]
    assert run.gate(min_recall=0.8).passed  # 9 of 10 positives resolved, none wrong ...
    assert not run.passed(min_recall=0.8)   # ... but an incomplete run does not pass
    assert "INCOMPLETE" in run.render(min_recall=0.8)


@pytest.mark.asyncio
async def test_limit_runs_only_the_first_n_model_cases():
    resolver = _resolver(_perfect())

    run = await _run(resolver, limit=3)

    assert resolver.await_count == 3
    assert len(run.results) == 3 + 2  # plus the exact and alias cases
    # a perfect subset is still not a gate result
    assert not run.passed(min_recall=0.8) and "INCOMPLETE: --limit 3" in run.render(min_recall=0.8)


@pytest.mark.asyncio
async def test_choosing_another_record_of_the_right_organization_passes_and_is_counted_separately():
    """SYNC-109: the model picked a duplicate stub of organization 1 for p1; that is not a wrong match."""
    answers = _perfect()
    answers[101] = 50

    run = await _run(_resolver(answers))

    report = run.gate(min_recall=0.8)
    assert run.passed(min_recall=0.8) and report.wrong_matches == []
    assert (report.other_record_picks, report.other_record_stub_picks) == (1, 1)


@pytest.mark.asyncio
async def test_choosing_the_parent_for_a_chapter_is_still_a_wrong_match():
    loaded = _loaded()
    loaded["cases"][0].update({"expected_org_id": 51, "expected_group_id": "g51"})   # p1 is the chapter's spelling
    answers = _perfect()
    answers[101] = 1                                                                 # ... and the model chose the parent

    run = await _run(_resolver(answers), loaded)

    assert not run.passed(min_recall=0.8) and [r.case_id for r in run.gate(min_recall=0.8).wrong_matches] == ["p1"]


@pytest.mark.asyncio
async def test_an_old_fixture_is_refused_and_a_provisional_one_is_never_a_gate_result():
    old = {k: v for k, v in _loaded().items() if k not in ("org_groups", "seed_version")}
    with pytest.raises(ValueError, match="no org_groups"):
        await _run(_resolver(_perfect()), old)

    run = await _run(_resolver(_perfect()), {**_loaded(), "verdicts_source": "provisional"})

    assert run.gate(min_recall=0.8).scored_pass and not run.passed(min_recall=0.8)
    assert "PROVISIONAL" in run.render(min_recall=0.8) and "harness: FAIL" in run.render(min_recall=0.8)


def test_the_command_refuses_an_old_fixture_before_it_asks_the_broker_anything(tmp_path):
    loaded = tmp_path / "loaded.json"
    loaded.write_text(json.dumps({k: v for k, v in _loaded().items() if k not in ("org_groups", "seed_version")}))

    done = subprocess.run(
        [sys.executable, "scripts/org_resolution_harness.py", str(loaded), "--broker-base", "http://127.0.0.1:9",
         "--broker-is-throwaway"],
        capture_output=True, text=True, env={"PYTHONPATH": "src", "PATH": "", "DDP_EVAL_BROKER_TOKEN": "t"},
    )

    assert done.returncode == 2 and "no org_groups" in done.stderr   # no connection error: it never got that far


def test_the_command_refuses_a_broker_that_does_not_hold_the_fixture(tmp_path):
    """The throwaway flag is a claim; the public organization count has to match the fixture's."""
    import http.server
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # a real broker: thousands of organizations
            body = json.dumps({"count": 5239, "results": []}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    loaded = tmp_path / "loaded.json"
    loaded.write_text(json.dumps({**_loaded(), "organizations": 4887}))
    try:
        done = subprocess.run(
            [sys.executable, "scripts/org_resolution_harness.py", str(loaded), "--broker-base",
             f"http://127.0.0.1:{server.server_port}", "--broker-is-throwaway"],
            capture_output=True, text=True, env={"PYTHONPATH": "src", "PATH": "", "DDP_EVAL_BROKER_TOKEN": "t"},
        )
    finally:
        server.shutdown()

    assert done.returncode == 2 and "does not look like the throwaway broker" in done.stderr


def test_the_command_refuses_a_broker_not_declared_throwaway_and_a_missing_token(tmp_path):
    loaded = tmp_path / "loaded.json"
    loaded.write_text(json.dumps(_loaded()))
    base = [sys.executable, "scripts/org_resolution_harness.py", str(loaded), "--broker-base", "http://b"]
    env = {"PYTHONPATH": "src", "PATH": "", "DDP_EVAL_BROKER_TOKEN": "t"}

    refused = subprocess.run(base, capture_output=True, text=True, env=env)
    assert refused.returncode == 2 and "--broker-is-throwaway" in refused.stderr

    no_token = subprocess.run(
        base + ["--broker-is-throwaway"], capture_output=True, text=True, env={"PYTHONPATH": "src", "PATH": ""}
    )
    assert no_token.returncode == 2 and "DDP_EVAL_BROKER_TOKEN" in no_token.stderr
