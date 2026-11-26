"""SYNC-197: fake archive records the test suite once leaked into the live in-flight Hash, and the guard against more.

On 2026-11-24 about 18:40 to 18:45 EST, test runs wrote eleven `mi-archive-*` records (task ids `arn:task/archive` and
`arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc`) into the live Redis; ddp-sync then tried to resume them at every restart.
These tests pin the purge that removes exactly those, and the session guard that stops a test from reaching the live store.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import redis

from ddp_sync.pipelines import inflight_fargate_jobs as ifj
from ddp_sync.pipelines.inflight_fargate_jobs import (
    INFLIGHT_KEY,
    is_leaked_test_archive_record,
    purge_leaked_test_archive_records,
)

REAL_ARN = "arn:aws:ecs:us-east-1:123456789012:task/ddp-scrapers/0123456789abcdef0123456789abcdef"


def _archive(run_id="mi-archive-04bcec1410b7", task_arn="arn:task/archive", **extra):
    return {"kind": "archive", "run_id": run_id, "jurisdiction": "mi", "task_arn": task_arn, "fargate_cfg": {"cluster": "c"},
            "archive_started_at": "2026-11-24T23:40:18+00:00", "attempt": 1, "hooks_done": [], **extra}


# ── what counts as a leaked test record ────────────────────────────────────────────────────────


@pytest.mark.parametrize("task_arn", [
    "arn:task/archive",                                          # seven of the eleven live records
    "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc",             # four of the eleven
    "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc123",          # the other test files' id
])
def test_the_test_suites_made_up_task_ids_are_leaked_records(task_arn):
    assert is_leaked_test_archive_record("mi-archive-04bcec1410b7", _archive(task_arn=task_arn))


@pytest.mark.parametrize(("run_id", "record"), [
    ("mi-archive-04bcec1410b7", _archive(task_arn=REAL_ARN)),                                   # a real task
    ("mi-archive-04bcec1410b7", _archive(task_arn="arn:aws:ecs:us-east-1:123456789012:task/ddp-scrapers/abc")),  # real account, id "abc"
    ("mi-archive-04bcec1410b7", _archive(task_arn="arn:aws:ecs:us-east-1:1:task/other-cluster/abc")),           # not the tests' cluster
    ("mi-archive-04bcec1410b7", {k: v for k, v in _archive().items() if k != "kind"}),          # a scrape record has no kind
    ("mi-archive-04bcec1410b7", _archive(kind="people_refresh")),                               # another kind, even with a fake id
    ("mi-1234", _archive()),                                                                    # not an archive run id
    ("mi-archive-04bcec1410b7", {"kind": "archive", "task_arn": None}),
    ("mi-archive-04bcec1410b7", {"kind": "archive"}),
], ids=["a real task", "real account, task abc", "other cluster", "no kind", "other kind", "other run id", "no task", "empty"])
def test_nothing_else_is_ever_judged_leaked(run_id, record):
    assert not is_leaked_test_archive_record(run_id, record)


# ── the purge ──────────────────────────────────────────────────────────────────────────────────


class _Hash:
    """The three Redis calls the purge makes, on one in-memory Hash."""

    def __init__(self, fields: dict[str, str], *, fail_hdel=False, fail_hgetall=False):
        self.fields, self.fail_hdel, self.fail_hgetall = dict(fields), fail_hdel, fail_hgetall

    def hgetall(self, key):
        assert key == INFLIGHT_KEY
        if self.fail_hgetall:
            raise RuntimeError("read failed")
        return dict(self.fields)

    def hget(self, key, field):
        return self.fields.get(field)

    def hdel(self, key, field):
        if self.fail_hdel:
            raise RuntimeError("delete failed")
        return 1 if self.fields.pop(field, None) is not None else 0


def _store():
    leaked = {f"mi-archive-{i:012x}": json.dumps(_archive(f"mi-archive-{i:012x}")) for i in range(3)}
    real = {"mi-archive-aaaaaaaaaaaa": json.dumps(_archive("mi-archive-aaaaaaaaaaaa", task_arn=REAL_ARN))}
    scrape = {"run-scrape-1": json.dumps({"jurisdiction": "mi", "task_arn": "arn:aws:ecs:us-east-1:1:task/ddp-scrapers/abc123"})}
    junk = {"run-junk": "{not json"}
    return leaked, {**real, **scrape, **junk}


def _purge(client, **kwargs):
    with patch.object(ifj, "_get_client", return_value=client):
        return purge_leaked_test_archive_records(**kwargs)


def test_a_dry_run_lists_everything_and_deletes_nothing():
    leaked, kept = _store()
    client = _Hash({**leaked, **kept})
    before = dict(client.fields)

    report = _purge(client)

    assert client.fields == before
    assert [i["run_id"] for i in report["leaked"]] == sorted(leaked)
    assert sorted(i["run_id"] for i in report["kept"]) == sorted(kept)
    assert (report["records"], report["deleted"], report["error"]) == (6, [], None)


def test_apply_deletes_the_leaked_records_and_leaves_every_other_record_exactly_as_it_was():
    leaked, kept = _store()
    client = _Hash({**leaked, **kept})

    report = _purge(client, apply=True)

    assert client.fields == kept  # byte for byte: the real archive, the scrape record and the unreadable one
    assert sorted(i["run_id"] for i in report["deleted"]) == sorted(leaked)
    assert report["changed"] == [] and report["error"] is None


def test_a_leaked_record_that_changed_since_it_was_judged_is_left_alone():
    leaked, kept = _store()
    client = _Hash({**leaked, **kept})
    changed = sorted(leaked)[1]
    real_hget = client.hget

    def hget_after_a_change(key, field):
        if field == changed:  # something rewrote this record between the read and the delete
            client.fields[field] = json.dumps(_archive(field, task_arn=REAL_ARN))
        return real_hget(key, field)

    client.hget = hget_after_a_change
    report = _purge(client, apply=True)

    assert [i["run_id"] for i in report["changed"]] == [changed]
    assert changed in client.fields and json.loads(client.fields[changed])["task_arn"] == REAL_ARN
    assert sorted(i["run_id"] for i in report["deleted"]) == sorted(set(leaked) - {changed})


def test_a_record_that_vanished_before_the_delete_is_not_reported_as_deleted():
    leaked, kept = _store()
    client = _Hash({**leaked, **kept})
    gone = min(leaked)
    real_hdel = client.hdel
    client.hdel = lambda key, field: (client.fields.pop(gone, None), 0)[1] if field == gone else real_hdel(key, field)
    report = _purge(client, apply=True)
    assert [i["run_id"] for i in report["changed"]] == [gone]
    assert gone not in [i["run_id"] for i in report["deleted"]] and len(report["deleted"]) == 2


def test_a_failed_delete_stops_the_purge_and_says_so():
    leaked, kept = _store()
    client = _Hash({**leaked, **kept}, fail_hdel=True)
    report = _purge(client, apply=True)
    assert report["error"].startswith("delete_failed") and report["deleted"] == []
    assert client.fields == {**leaked, **kept}


def test_no_redis_means_an_error_and_no_exception():
    report = _purge(None, apply=True)
    assert report["error"] == "redis_unavailable" and report["records"] == 0


def test_a_failed_read_deletes_nothing():
    leaked, kept = _store()
    client = _Hash({**leaked, **kept}, fail_hgetall=True)
    report = _purge(client, apply=True)
    assert report["error"].startswith("read_failed") and client.fields == {**leaked, **kept}


# ── the command ────────────────────────────────────────────────────────────────────────────────


def _script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "purge_leaked_inflight_records.py"
    spec = importlib.util.spec_from_file_location("purge_leaked_inflight_records", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_script(monkeypatch, capsys, args, client):
    script = _script()
    monkeypatch.setattr(sys, "argv", ["purge_leaked_inflight_records.py", *args])
    with patch.object(ifj, "_get_client", return_value=client):
        code = script.main()
    return code, capsys.readouterr()


def test_the_command_looks_by_default_and_prints_each_record_with_its_verdict(monkeypatch, capsys):
    leaked, kept = _store()
    client = _Hash({**leaked, **kept})
    code, out = _run_script(monkeypatch, capsys, [], client)
    assert code == 0 and "DRY RUN, nothing deleted. 3 LEAKED, 3 KEPT" in out.out
    assert out.out.count("LEAKED  mi-archive-") == 3 and out.out.count("KEPT") >= 3
    assert "mi-archive-aaaaaaaaaaaa" in out.out and client.fields == {**leaked, **kept}


def test_the_command_deletes_only_with_apply_and_reports_what_it_did(monkeypatch, capsys):
    leaked, kept = _store()
    client = _Hash({**leaked, **kept})
    code, out = _run_script(monkeypatch, capsys, ["--apply"], client)
    assert code == 0 and "Deleted 3 of 3 LEAKED" in out.out and client.fields == kept


def test_the_command_exits_1_when_a_delete_fails_part_way(monkeypatch, capsys):
    leaked, kept = _store()
    code, out = _run_script(monkeypatch, capsys, ["--apply"], _Hash({**leaked, **kept}, fail_hdel=True))
    assert code == 1 and "Stopped early: delete_failed" in out.err


def test_the_command_fails_loudly_when_redis_cannot_be_read(monkeypatch, capsys):
    code, out = _run_script(monkeypatch, capsys, ["--apply"], None)
    assert code == 1 and "Nothing was touched" in out.err


# ── the guard: a test can never reach the live store ───────────────────────────────────────────


def test_a_test_can_never_open_a_real_sync_redis_connection():
    with pytest.raises(AssertionError, match="SYNC-197"):
        redis.Redis.from_url("redis://localhost:6379/0")


def test_a_worker_thread_that_outlives_its_tests_fixtures_cannot_reach_the_live_store(monkeypatch):
    """monkeypatch gives the module's `_client` back as None at teardown. A thread still running then calls `_get_client()`;
    it must not be handed the real Redis (up on the Mac) and write a record there."""
    monkeypatch.setattr(ifj, "_client", None)

    assert ifj._get_client() is None  # asserted first: nothing is written if this fails
    ifj.record_archive_started("mi-archive-000000000000", "mi", "arn:task/archive", {}, "2026-11-26T00:00:00+00:00", 1)
    assert ifj.list_inflight() == {}


def test_every_test_starts_with_an_empty_in_memory_store():
    assert ifj.list_inflight() == {}
    ifj.record_archive_started("mi-archive-000000000001", "mi", "arn:task/archive", {}, "2026-11-26T00:00:00+00:00", 1)
    assert list(ifj.list_inflight()) == ["mi-archive-000000000001"]  # held in memory; the next test starts empty again
