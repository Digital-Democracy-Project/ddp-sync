"""SYNC-157 part 2: ddp-sync keeps no list of states of its own; it reads ddp-open-states' jurisdictions.yaml.

Ramon: "we need one singular list that everybody follows" (2026-11-02); "every service must read from the canonical
manifest of jurisdictions" (2026-11-10). Two guards:

  * the REAL config/sync_schedule.yaml has no list or map keyed by state codes anywhere except `openstates_scrape.timing`
    (WHEN a state runs, decided to stay in the scheduler; the drift check limits it to scraped states);
  * no module in src/ defines a module- or class-level table (dict, set, list, tuple) whose keys or members are two or
    more state codes, except the reference table below, which covers every state rather than listing the ones we serve (the 50-state session
    calendar in services/legislative_calendar.py is built without such a literal, so it needs no allowance).
"""

from __future__ import annotations

import ast
import pathlib

import yaml

REPO = pathlib.Path(__file__).resolve().parent.parent
CODES = set("al ak az ar ca co ct de fl ga hi id il in ia ks ky la me md ma mi mn ms mo mt ne nv nh nj nm ny nc nd oh ok "  # noqa: SIM905
            "or pa ri sc sd tn tx ut vt va wa wv wi wy us usa dc pr".split())

# path in sync_schedule.yaml that may be keyed by state (decided in SYNC-152 / the one-pipeline plan)
ALLOWED_YAML = {("openstates_scrape", "timing")}

# module path -> table name -> why it is not a list of the states we serve (SYNC-157, for Ramon to accept)
ALLOWED_TABLES = {
    # reference data covering every state, not "the states we serve"
    "src/ddp_sync/pipelines/voatz_brevo.py": "every state's name to its code (reference data)",
    # the old Webflow site's collection ids; goes with Webflow (Dec 2026). Not a manifest field: CLAUDE.md forbids
    # building anything new around Webflow.
    "src/ddp_sync/pipelines/bill_sync.py": "BillSyncService.JURISDICTION_MAP (Webflow collection ids, removed with Webflow)",
}


def _state_keyed(node) -> bool:
    if isinstance(node, dict):
        return sum(str(k).lower() in CODES for k in node) >= 2 or any(str(k).lower() in CODES for k in node) and len(node) == 1
    if isinstance(node, list):
        return len(node) >= 1 and all(isinstance(x, str) and x.lower() in CODES for x in node)
    return False


def _yaml_offenders(data, path=()):
    found = []
    if isinstance(data, dict):
        if path not in ALLOWED_YAML and data and _state_keyed(data):
            found.append(".".join(path))
        for k, v in data.items():
            if path + (k,) not in ALLOWED_YAML:
                found += _yaml_offenders(v, path + (k,))
    elif isinstance(data, list) and _state_keyed(data):
        found.append(".".join(path))
    return found


def test_the_real_schedule_has_no_list_or_map_keyed_by_state_outside_timing():
    data = yaml.safe_load((REPO / "config" / "sync_schedule.yaml").read_text())
    assert _yaml_offenders(data) == []
    assert isinstance(data["openstates_scrape"]["timing"], dict)  # the one allowed map is still there


def test_the_yaml_guard_catches_a_list_put_back():
    assert _yaml_offenders({"active_jurisdictions": ["FL", "VA"]}) == ["active_jurisdictions"]
    assert _yaml_offenders({"openstates_scrape": {"x": {"jurisdictions_excluded": ["mi"]}}}) == [
        "openstates_scrape.x.jurisdictions_excluded"]
    assert _yaml_offenders({"a": {"by_state": {"ma": 86400}}}) == ["a.by_state"]
    assert _yaml_offenders({"openstates_scrape": {"timing": {"fl": {}, "wa": {}}}}) == []


def _strings(node) -> list[str]:
    if isinstance(node, ast.Dict):
        return [n.value for n in (*node.keys, *node.values) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    if isinstance(node, (ast.Set, ast.List, ast.Tuple)):
        return [n.value for n in node.elts if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    if isinstance(node, ast.Call) and getattr(node.func, "id", "") in ("frozenset", "set", "tuple", "list") and node.args:
        return _strings(node.args[0])
    return []


def _tables(path: pathlib.Path):
    tree = ast.parse(path.read_text())
    bodies = [tree.body, *[c.body for c in tree.body if isinstance(c, ast.ClassDef)]]
    for body in bodies:
        for node in body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                names = [t.id for t in targets if isinstance(t, ast.Name)]
                # distinct states ("usa" and "us" are one: the federal spelling map is not a list of states)
                states = {("us" if s.lower() == "usa" else s.lower()) for s in _strings(node.value) if s.lower() in CODES}
                if len(states) >= 2:
                    yield node.lineno, names


def test_no_module_keeps_a_table_of_states():
    offenders = []
    for path in sorted((REPO / "src").rglob("*.py")):
        rel = str(path.relative_to(REPO))
        if rel in ALLOWED_TABLES:
            continue
        offenders += [f"{rel}:{line} {names}" for line, names in _tables(path)]
    assert offenders == [], f"per-state tables (read the manifest instead, SYNC-157): {offenders}"


def test_the_code_guard_catches_a_table_put_back(tmp_path):
    bad = tmp_path / "bad.py"
    bad.write_text('SCRAPE_TIMEOUT_S = {"fl": 1, "ma": 2, "default": 3}\nclass X:\n    STATES = frozenset({"pa", "ne"})\n')
    assert [names for _, names in _tables(bad)] == [["SCRAPE_TIMEOUT_S"], ["STATES"]]


def test_the_allowed_tables_still_exist():
    """An allowance for a file that no longer has such a table should be removed, not left to excuse a new one."""
    for rel in ALLOWED_TABLES:
        assert list(_tables(REPO / rel)), rel
