"""SYNC-189: a ddp-sync restart during a local load kills cloud_loader.py before its `finally`, so the run's S3
`_import_lock` (SourceLock, 24 h) stayed behind and the resumed run's own load was refused with exit 90 (Oklahoma 2026,
2026-11-23, prod note 98fbc47). The resumed run now releases that lock first, only when its holder is this run_id.

The fake loader below acquires the lock the way ddp-open-states' `SourceLock.acquire` does: refused (exit 90) while the
lock is live, whoever holds it; taken once it has expired. FakeS3 enforces IfMatch (tests/test_sync_167_stopped_run_lock).
"""

from __future__ import annotations

import json
import time
from unittest.mock import patch

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from tests.test_cloud_scrape_trigger import (
    FakeEcsClient,
    FakeRedisClient,
    FakeSubprocessResult,
    _fargate_config,
    _stopped_response,
)
from tests.test_sync_167_stopped_run_lock import BUCKET, FakeS3

RUN_ID = "ok-b13efe4b9c89"
IMPORT_LOCK = "prod/ok_2026/_import_lock"


def _record(**fargate):
    return {
        "jurisdiction": "ok",
        "session_arg": "session=2026",
        "task_arn": "arn:task/collection",
        "fargate_cfg": _fargate_config(memory_bucket=BUCKET, memory_prefix="prod", **fargate)["cloud_path"]["fargate"],
        "openstates_root": "/fake/root",
    }


def _loader(s3, loads):
    """cloud_loader.py's import lock, as SourceLock.acquire decides it: a live lock refuses, an expired one is taken."""

    def run(cmd, env):
        lock = s3.lock(IMPORT_LOCK)
        if time.time() <= lock["expires_at"]:
            return FakeSubprocessResult(returncode=90, stderr=b"ERROR: another import of ok_2026 already holds the lock")
        s3.seed_lock(IMPORT_LOCK, holder=cmd[3], now=time.time())
        loads.append(cmd)
        return FakeSubprocessResult(returncode=0)

    return run


def _resume(s3, record, subprocess_runner, ecs=None):
    ecs = ecs or FakeEcsClient(describe_responses=[_stopped_response(exit_code=0)])
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3),
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/x", "")),
        patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        return cst.resume_inflight_fargate_job(RUN_ID, record, ecs_client=ecs, subprocess_runner=subprocess_runner)


def test_after_a_restart_mid_load_the_resumed_run_takes_the_lock_and_loads():
    s3 = FakeS3()
    s3.seed_lock(IMPORT_LOCK, holder=RUN_ID, now=time.time())  # the killed loader's lock, live for 24 h
    loads = []

    result = _resume(s3, _record(), _loader(s3, loads))

    assert result["success"] is True, result
    assert loads == [["python3", "/fake/root/cloud_loader.py", "ok", RUN_ID, "session=2026"]]
    assert s3.puts[0] == {"Key": IMPORT_LOCK, "IfMatch": '"etag-1"'}  # released conditionally on the lock it read


def test_a_lock_held_by_another_live_run_is_never_touched():
    s3 = FakeS3()
    s3.seed_lock(IMPORT_LOCK, holder="ok-0123456789ab", now=time.time())
    before = s3.objects[IMPORT_LOCK]
    loads = []

    result = _resume(s3, _record(), _loader(s3, loads))

    assert result["success"] is False
    assert "exited 90" in result["error"]
    assert s3.puts == []
    assert s3.objects[IMPORT_LOCK] == before
    assert loads == []


def test_the_collection_and_archive_locks_are_left_to_their_own_tasks():
    """A restart does not kill Fargate tasks: the collection's `_lock` and an archive's `_archive_lock` belong to
    tasks that are still running (or ended and released them), so the resume releases only the local load's lock."""
    s3 = FakeS3()
    s3.seed_lock(IMPORT_LOCK, holder=RUN_ID, now=time.time())
    s3.seed_lock("prod/ok/_lock", holder=RUN_ID, now=time.time())
    s3.seed_lock("prod/ok/_archive_lock", holder="ok-archive-0123456789ab", now=time.time())
    collection, archive = s3.objects["prod/ok/_lock"], s3.objects["prod/ok/_archive_lock"]

    _resume(s3, _record(), _loader(s3, []))

    assert {p["Key"] for p in s3.puts} == {IMPORT_LOCK}
    assert s3.objects["prod/ok/_lock"] == collection
    assert s3.objects["prod/ok/_archive_lock"] == archive


def test_a_resumed_fargate_load_keeps_its_lock():
    """`load_on_fargate`: the load task outlives the restart and is waited on, so its lock is never released here."""
    s3 = FakeS3()
    s3.seed_lock(IMPORT_LOCK, holder=RUN_ID, now=time.time())
    record = {**_record(load_on_fargate=True), "load_task_arn": "arn:task/load"}
    ecs = FakeEcsClient(describe_responses=[_stopped_response(exit_code=0)])

    result = _resume(s3, record, subprocess_runner=None, ecs=ecs)

    assert result["success"] is True, result
    assert ecs.describe_calls[0]["tasks"] == ["arn:task/load"]
    assert s3.puts == []


def test_a_fresh_run_never_reads_the_lock():
    """Only a resumed run releases: a fresh run_id cannot hold a lock yet, so nothing changes for it."""
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_stopped_response(exit_code=0)],
    )
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client") as s3_client,
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/x", "")),
        patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()),
        patch("ddp_sync.pipelines.cloud_scrape_trigger.post_alert"),
    ):
        result = cst._finish_after_launch(
            "ok", "session=2026", RUN_ID, "/fake/root", _record()["fargate_cfg"], "arn:task/1", ecs,
            lambda cmd, env: FakeSubprocessResult(returncode=0), "ok session=2026", logs_client=object(),
            load_task_arn=None,
        )
    assert result["success"] is True, result
    s3_client.assert_not_called()  # the release swallows errors, so count the calls rather than fail on one


def test_the_lock_is_released_even_when_the_rds_lookup_fails():
    """pm-review: the resume clears its in-flight record whatever happens, so a release after a failing preflight
    would leave the lock to its TTL and block the state's next fresh run."""
    s3 = FakeS3()
    s3.seed_lock(IMPORT_LOCK, holder=RUN_ID, now=time.time())
    ecs = FakeEcsClient(describe_responses=[_stopped_response(exit_code=0)])
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3),
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=(None, "secret unreadable")),
        patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = cst.resume_inflight_fargate_job(RUN_ID, _record(), ecs_client=ecs, subprocess_runner=None)

    assert result["success"] is False
    assert s3.lock(IMPORT_LOCK)["expires_at"] < time.time()


def test_a_missing_lock_does_not_stop_the_load():
    s3 = FakeS3()
    loads = []

    def loader(cmd, env):
        loads.append(cmd)
        return FakeSubprocessResult(returncode=0)

    result = _resume(s3, _record(), loader)

    assert result["success"] is True, result
    assert len(loads) == 1
    assert s3.puts == []


def test_lock_body_matches_sourcelock_format():
    s3 = FakeS3()
    s3.seed_lock(IMPORT_LOCK, holder=RUN_ID, now=time.time())
    with patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3):
        detail = cst._release_run_lock("ok_2026", RUN_ID, "_import_lock", {"memory_bucket": BUCKET})
    lock = json.loads(s3.objects[IMPORT_LOCK][0])
    assert detail == f"lock {IMPORT_LOCK} released"
    assert set(lock) == {"holder", "acquired_at", "expires_at"}
    assert lock["holder"] == RUN_ID and lock["expires_at"] < time.time()
