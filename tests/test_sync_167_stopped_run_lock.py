"""SYNC-167: when ddp-sync stops a Fargate task (a stalled scrape, an archive past its limit), it releases the S3 run
lock that task took, but only when the lock's holder is that run, so the state's next run is not refused for 24 h.

The lock object and body are ddp-open-states' cloud_collector.py `SourceLock`'s: `<prefix>/<source>/<suffix>`, JSON
`{holder, acquired_at, expires_at}`, released by writing it already expired with `IfMatch` on the ETag. The fake S3
below enforces `IfMatch` the way S3 does, so a release that skipped the condition or used a stale ETag would fail here.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from ddp_sync.pipelines import openstates_archive as oa
from tests.test_cloud_scrape_trigger import (
    FakeClock,
    FakeEcsClient,
    FakeLogsClient,
    FakeSubprocessResult,
    _fargate_config,
    _running_response,
    _stopped_response,
)

BUCKET = "ddp-openstates-scraper-memory"


class PreconditionFailed(Exception):
    pass


class FakeS3:
    def __init__(self):
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.puts: list[dict] = []
        self._n = 0

    def seed_lock(self, key, holder, expires_in=3600.0, now=1_800_000_000.0):
        self._store(key, json.dumps({"holder": holder, "acquired_at": now, "expires_at": now + expires_in}).encode())

    def _store(self, key, body):
        self._n += 1
        self.objects[key] = (body, f'"etag-{self._n}"')

    def get_object(self, Bucket, Key):
        assert Bucket == BUCKET
        body, etag = self.objects[Key]

        class _Body:
            def read(self_inner):
                return body

        return {"Body": _Body(), "ETag": etag}

    def put_object(self, Bucket, Key, Body, IfMatch=None):
        assert Bucket == BUCKET
        self.puts.append({"Key": Key, "IfMatch": IfMatch})
        if IfMatch is not None and self.objects.get(Key, (None, None))[1] != IfMatch:
            raise PreconditionFailed(Key)
        self._store(Key, Body)
        return {"ETag": self.objects[Key][1]}

    def lock(self, key):
        return json.loads(self.objects[key][0])


def _stalled_scrape(s3, clock, jurisdiction="pa", run_id="pa-bfb6773bf3ba", ecs=None, **fargate):
    # Four polls RUNNING (stopped as stalled on the 4th), then ECS reports the stopped task STOPPED.
    ecs = ecs or FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[*[_running_response()] * 4, _stopped_response(exit_code=None)],
    )
    logs = FakeLogsClient(clock, lag_s=None)  # no log line at all: stopped as stalled on the 4th poll
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.time", clock),
        patch("ddp_sync.pipelines.cloud_scrape_trigger._generate_run_id", return_value=run_id),
        patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3),
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=("postgresql://rds/x", "")),
        patch("ddp_sync.pipelines.cloud_scrape_trigger.post_alert"),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure") as failure_alert,
    ):
        result = cst.run_cloud_scrape(
            jurisdiction, None, "/fake/root", _fargate_config(**fargate), ecs_client=ecs, logs_client=logs,
            subprocess_runner=lambda cmd, env: FakeSubprocessResult(returncode=0),
        )
    return result, ecs, failure_alert


def test_stalled_scrape_releases_its_own_lock():
    clock = FakeClock(step_s=1000)
    s3 = FakeS3()
    s3.seed_lock("prod/pa/_lock", holder="pa-bfb6773bf3ba", now=clock.now)

    result, ecs, failure_alert = _stalled_scrape(s3, clock, memory_bucket=BUCKET, memory_prefix="prod")

    assert result["success"] is False
    assert len(ecs.stop_task_calls) == 1
    lock = s3.lock("prod/pa/_lock")
    assert lock["holder"] == "pa-bfb6773bf3ba"
    assert lock["expires_at"] < clock.now  # expired: the next run's SourceLock.acquire reclaims it
    assert s3.puts == [{"Key": "prod/pa/_lock", "IfMatch": '"etag-1"'}]  # conditional on the lock it read
    assert "lock prod/pa/_lock released" in failure_alert.call_args.args[1]


def test_stalled_scrape_leaves_another_runs_lock_alone():
    clock = FakeClock(step_s=1000)
    s3 = FakeS3()
    s3.seed_lock("prod/pa/_lock", holder="pa-5e8e74d5aa86", now=clock.now)
    before = s3.objects["prod/pa/_lock"]

    _, ecs, failure_alert = _stalled_scrape(s3, clock, memory_bucket=BUCKET, memory_prefix="prod")

    assert len(ecs.stop_task_calls) == 1
    assert s3.puts == []
    assert s3.objects["prod/pa/_lock"] == before
    assert "lock left alone: held by pa-5e8e74d5aa86" in failure_alert.call_args.args[1]


def test_a_lock_that_changed_between_read_and_write_is_not_overwritten():
    """The release is conditional on the ETag it read: a run that took the lock in between keeps it."""
    s3 = FakeS3()
    s3.seed_lock("prod/pa/_lock", holder="pa-old")
    real_get = s3.get_object

    def get_then_race(**kw):
        resp = real_get(**kw)
        s3.seed_lock("prod/pa/_lock", holder="pa-new")  # a new run reclaims it right after our read
        return resp

    s3.get_object = get_then_race
    ecs = FakeEcsClient(describe_responses=[_stopped_response()])
    cfg = {"cluster": "c", "memory_bucket": BUCKET}
    with patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3):
        detail = cst._release_stopped_run_lock(ecs, "arn:task/1", "pa", "pa-old", "_lock", cfg)

    assert detail.startswith("lock not released")
    assert s3.lock("prod/pa/_lock")["holder"] == "pa-new"


def test_s3_failure_never_raises_and_the_stop_is_still_reported():
    class BrokenS3:
        def get_object(self, **_kw):
            raise RuntimeError("AccessDenied")

    clock = FakeClock(step_s=1000)
    result, ecs, failure_alert = _stalled_scrape(BrokenS3(), clock, memory_bucket=BUCKET)

    assert result["success"] is False
    assert len(ecs.stop_task_calls) == 1
    assert "stalled" in failure_alert.call_args.args[1]
    assert "lock not released" in failure_alert.call_args.args[1]


def test_no_memory_bucket_configured_skips_the_release_without_touching_s3(monkeypatch):
    monkeypatch.delenv("MEMORY_BUCKET", raising=False)
    with patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", side_effect=AssertionError("no S3 call")):
        detail = cst._release_stopped_run_lock(None, "arn:task/1", "pa", "pa-1", "_lock", {})
    assert detail == "lock not released: no memory_bucket configured"


def test_hard_stop_wait_releases_the_lock_it_was_given():
    """The wait without a logs client (archive, backfill) stops at max_wait_seconds; with run_lock it releases too."""
    s3 = FakeS3()
    s3.seed_lock("prod/mi/_archive_lock", holder="mi-archive-1")
    ecs = FakeEcsClient(describe_responses=[_running_response(), _stopped_response(exit_code=None)])
    fargate = _fargate_config(max_wait_seconds=0, memory_bucket=BUCKET)["cloud_path"]["fargate"]

    with patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3):
        exit_code, detail = cst._wait_for_task_stop(
            "arn:task/1", fargate, ecs, run_lock=("mi", "mi-archive-1", "_archive_lock")
        )

    assert exit_code is None
    assert len(ecs.stop_task_calls) == 1
    assert s3.lock("prod/mi/_archive_lock")["expires_at"] < s3.lock("prod/mi/_archive_lock")["acquired_at"]
    assert "released" in detail


def test_a_task_that_stops_by_itself_never_touches_the_lock():
    """Only a stop ddp-sync makes releases: a task that ends on its own released its lock in its own `finally`."""
    ecs = FakeEcsClient(describe_responses=[_stopped_response(exit_code=1)])
    fargate = _fargate_config(memory_bucket=BUCKET)["cloud_path"]["fargate"]
    with patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", side_effect=AssertionError("no S3 call")):
        exit_code, _ = cst._wait_for_task_stop("arn:task/1", fargate, ecs, run_lock=("pa", "pa-1", "_lock"))
    assert exit_code == 1


@pytest.mark.asyncio
async def test_archive_stopped_at_its_limit_releases_its_archive_lock(monkeypatch):
    """The archive path: cloud_archiver.py's `_archive_lock`, holder = the archive run_id ddp-sync passes as RUN_ID."""
    monkeypatch.setattr(oa, "resolve_rds_database_url", lambda: ("postgresql://rds/openstates", ""))

    class _Uuid:
        hex = "abcdef0123456789"

    monkeypatch.setattr(oa.uuid, "uuid4", lambda: _Uuid())
    s3 = FakeS3()
    s3.seed_lock("prod/mi/_archive_lock", holder="mi-archive-abcdef012345")
    ecs = FakeEcsClient(
        run_task_response={"tasks": [{"taskArn": "arn:task/a"}], "failures": []},
        describe_responses=[_running_response(), _stopped_response(exit_code=None)],
    )
    cfg = _fargate_config(max_wait_seconds=0, memory_bucket=BUCKET, memory_prefix="prod")["cloud_path"]["fargate"]

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3),
        patch.object(oa, "_alert_archive_failure"),
    ):
        result = await oa._run_archive_fargate("mi", config={"cloud_path": {"fargate": cfg}}, ecs_client=ecs)

    assert result["success"] is False
    run_id = ecs.run_task_calls[0]["overrides"]["containerOverrides"][0]["environment"][0]["value"]
    assert run_id == "mi-archive-abcdef012345"
    assert len(ecs.stop_task_calls) == 1
    assert s3.puts == [{"Key": "prod/mi/_archive_lock", "IfMatch": '"etag-1"'}]


# ── pm-review: release only after a stop that worked, and only once ECS says STOPPED ─────────────────────────────


def test_a_failed_stop_request_releases_nothing():
    """stop_task failed: the run may still be going, so its lock must stay."""

    class StopFails(FakeEcsClient):
        def stop_task(self, **kwargs):
            raise RuntimeError("AccessDeniedException")

    clock = FakeClock(step_s=1000)
    s3 = FakeS3()
    s3.seed_lock("prod/pa/_lock", holder="pa-bfb6773bf3ba", now=clock.now)
    ecs = StopFails(
        run_task_response={"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
        describe_responses=[_running_response()] * 4,
    )

    _, _, failure_alert = _stalled_scrape(s3, clock, ecs=ecs, memory_bucket=BUCKET)

    assert s3.puts == []
    assert "stop_task also failed" in failure_alert.call_args.args[1]
    assert "released" not in failure_alert.call_args.args[1]


def test_a_task_still_running_after_the_stop_request_keeps_its_lock():
    """ECS never reports STOPPED within the confirm window: the lock is left to its TTL rather than shared."""
    clock = FakeClock(step_s=60)
    s3 = FakeS3()
    s3.seed_lock("prod/mi/_archive_lock", holder="mi-archive-1", now=clock.now)
    ecs = FakeEcsClient(describe_responses=[_running_response()] * 10)
    fargate = _fargate_config(max_wait_seconds=0, memory_bucket=BUCKET)["cloud_path"]["fargate"]

    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.time", clock),
        patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3),
    ):
        exit_code, detail = cst._wait_for_task_stop(
            "arn:task/1", fargate, ecs, run_lock=("mi", "mi-archive-1", "_archive_lock")
        )

    assert exit_code is None
    assert s3.puts == []
    assert "lock not released: task not STOPPED after 150s" in detail


def test_the_lock_is_released_only_after_ecs_reports_stopped():
    """RUNNING twice after the stop request, then STOPPED: the release waits for STOPPED (no put before it)."""
    clock = FakeClock(step_s=5)
    s3 = FakeS3()
    s3.seed_lock("prod/pa/_lock", holder="pa-1", now=clock.now)
    stopped_seen = []

    class Ecs(FakeEcsClient):
        def describe_tasks(self, **kwargs):
            resp = super().describe_tasks(**kwargs)
            stopped_seen.append(resp["tasks"][0]["lastStatus"] == "STOPPED")
            return resp

    ecs = Ecs(describe_responses=[_running_response(), _running_response(), _stopped_response(exit_code=None)])
    real_put = s3.put_object

    def put_checking_order(**kw):
        assert stopped_seen and stopped_seen[-1], "released before ECS reported STOPPED"
        return real_put(**kw)

    s3.put_object = put_checking_order
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.time", clock),
        patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3),
    ):
        detail = cst._release_stopped_run_lock(
            ecs, "arn:task/1", "pa", "pa-1", "_lock", {"cluster": "c", "memory_bucket": BUCKET}
        )

    assert detail == "lock prod/pa/_lock released"
    assert len(ecs.describe_calls) == 3


def test_a_describe_failure_after_the_stop_keeps_the_lock():
    """pm-review round 2: if ECS cannot be asked whether the task stopped, nothing is released."""

    class DescribeFails(FakeEcsClient):
        def describe_tasks(self, **kwargs):
            raise RuntimeError("ThrottlingException")

    s3 = FakeS3()
    s3.seed_lock("prod/pa/_lock", holder="pa-1")
    with patch("ddp_sync.pipelines.cloud_scrape_trigger._s3_client", return_value=s3):
        detail = cst._release_stopped_run_lock(
            DescribeFails(), "arn:task/1", "pa", "pa-1", "_lock", {"cluster": "c", "memory_bucket": BUCKET}
        )

    assert detail == "lock not released (ThrottlingException)"
    assert s3.puts == []
