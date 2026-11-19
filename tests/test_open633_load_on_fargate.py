"""OPEN-633: the cloud load runs as a Fargate task of the scrapers image when the state's manifest says
`scrape.load_on_fargate: true`, and (D2) the load and archive tasks can read the RDS login from their own task
definition (`cloud_path.fargate.rds_from_task_secret`) instead of being passed DATABASE_URL.

Reuses test_cloud_scrape_trigger.py's fakes, so these read like the collection's own Fargate tests."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from ddp_sync.pipelines import openstates_archive as oa
from tests import sync157_manifest
from tests.test_cloud_scrape_trigger import (
    FakeEcsClient,
    FakeRedisClient,
    _fargate_config,
    _running_response,
    _stopped_response,
)

RDS = ("postgresql://loader:s3cret@db.example.internal:5432/openstates", "")


class TwoTaskEcs(FakeEcsClient):
    """run_task hands out one ARN per launch (collection first, then the load)."""

    def __init__(self, arns, **kwargs):
        super().__init__(**kwargs)
        self._arns = list(arns)

    def run_task(self, **kwargs):
        self.run_task_calls.append(kwargs)
        if self._run_task_error:
            raise self._run_task_error
        return {"tasks": [{"taskArn": self._arns.pop(0)}], "failures": []}


def _overrides(call):
    o = call["overrides"]["containerOverrides"][0]
    return o["command"], {e["name"]: e["value"] for e in o["environment"]}


@pytest.fixture(autouse=True)
def _manifest(tmp_path, monkeypatch):
    """fl loads on Fargate; every other state keeps the field false, and `al` has no such field at all (a manifest
    from before OPEN-633)."""
    changes = {f"{c}.scrape.load_on_fargate": False for c in sync157_manifest.MANIFEST if c != "al"}
    changes["fl.scrape.load_on_fargate"] = True
    sync157_manifest.use(tmp_path, monkeypatch, **changes)


def _local_runner_must_not_run(cmd, env):
    raise AssertionError("the local cloud_loader.py subprocess ran although the state loads on Fargate")


def _scrape(ecs, *, session_arg="session=2026", alerts=None, **fargate):
    with (
        patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=RDS),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure",
              side_effect=lambda label, error, duration: (alerts if alerts is not None else []).append(error)),
        patch("time.sleep"),
    ):
        return cst.run_cloud_scrape(
            "fl", session_arg, "/fake/root", _fargate_config(**fargate), ecs_client=ecs,
            subprocess_runner=_local_runner_must_not_run,
        )


# ── which states ─────────────────────────────────────────────────────────────────────────────


def test_only_a_state_whose_manifest_says_true_loads_on_fargate():
    config = _fargate_config()
    assert cst._fargate_config(config, "fl")["load_on_fargate"] is True
    assert cst._fargate_config(config, "ut")["load_on_fargate"] is False
    assert cst._fargate_config(config, "al")["load_on_fargate"] is False  # field missing: today's local load
    assert "load_on_fargate" not in config["cloud_path"]["fargate"]  # the shared dict is not changed


# ── the load task ────────────────────────────────────────────────────────────────────────────


def test_the_load_runs_as_its_own_task_of_the_scrapers_image_after_the_collection():
    ecs = TwoTaskEcs(["arn:task/collect", "arn:task/load"],
                     describe_responses=[_stopped_response(exit_code=0), _running_response(),
                                         _stopped_response(exit_code=0)])
    redis = FakeRedisClient()
    seen = {}
    real_wait = cst._wait_for_task_stop

    def watching_wait(task_arn, *args, **kwargs):
        if task_arn == "arn:task/load":
            seen["record"] = dict(cst.inflight_fargate_jobs.list_inflight())
            seen["max_wait"] = args[0]["max_wait_seconds"]
            seen["run_lock"] = kwargs["run_lock"]
        return real_wait(task_arn, *args, **kwargs)

    with patch.object(cst.inflight_fargate_jobs, "_client", redis), \
         patch("ddp_sync.pipelines.cloud_scrape_trigger._wait_for_task_stop", watching_wait):
        result = _scrape(ecs, load_timeout_seconds=7200)

    assert result["success"] is True, result
    run_id = result["cloud_run_id"]
    assert len(ecs.run_task_calls) == 2
    command, env = _overrides(ecs.run_task_calls[1])
    assert command == ["fl", run_id, "session=2026"]
    assert env["RUNNER_SCRIPT"] == "cloud_loader.py"
    assert env["RUN_ID"] == run_id
    assert env["DATABASE_URL"] == RDS[0]  # rds_from_task_secret is off: passed exactly as today's local load gets it
    assert ecs.describe_calls[-1]["tasks"] == ["arn:task/load"]
    # while the load was waited on, the in-flight record named the load task (a restart then waits for it)
    assert seen["record"][run_id]["load_task_arn"] == "arn:task/load"
    assert seen["max_wait"] == 7200
    assert seen["run_lock"] == ("fl_2026", run_id, "_import_lock")
    assert cst.inflight_fargate_jobs.list_inflight() == {}


def test_with_the_task_secret_on_no_connection_string_is_resolved_or_passed():
    ecs = TwoTaskEcs(["arn:task/collect", "arn:task/load"],
                     describe_responses=[_stopped_response(exit_code=0), _stopped_response(exit_code=0)])
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()), \
         patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url",
               side_effect=AssertionError("resolved although the task reads its own secret")), \
         patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"):
        result = cst.run_cloud_scrape(
            "fl", None, "/fake/root", _fargate_config(rds_from_task_secret=True), ecs_client=ecs,
            subprocess_runner=_local_runner_must_not_run,
        )
    assert result["success"] is True, result
    command, env = _overrides(ecs.run_task_calls[1])
    assert command == ["fl", result["cloud_run_id"]]
    assert "DATABASE_URL" not in env


def test_a_failed_load_task_is_a_load_failure_and_never_shows_the_connection_string():
    ecs = TwoTaskEcs(["arn:task/collect", "arn:task/load"],
                     describe_responses=[_stopped_response(exit_code=0),
                                         _stopped_response(exit_code=1, container_reason=f"bad url {RDS[0]}")])
    alerts = []
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()):
        result = _scrape(ecs, alerts=alerts)
    assert result["success"] is False
    assert result["error"].startswith("load_failed: load task exited 1 (task_arn=arn:task/load)")
    assert "s3cret" not in json.dumps(result) and "s3cret" not in " ".join(alerts)
    assert "postgresql://<redacted>" in result["error"]


def test_a_load_task_that_cannot_start_is_reported_without_the_connection_string():
    class LoadLaunchFails(TwoTaskEcs):
        def run_task(self, **kwargs):
            if self.run_task_calls:
                self.run_task_calls.append(kwargs)
                raise RuntimeError(f"InvalidParameterException: override DATABASE_URL={RDS[0]} rejected")
            return super().run_task(**kwargs)

    ecs = LoadLaunchFails(["arn:task/collect"], describe_responses=[_stopped_response(exit_code=0)])
    alerts = []
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()):
        result = _scrape(ecs, alerts=alerts)
    assert result["success"] is False
    assert "load task did not start" in result["error"]
    assert "s3cret" not in json.dumps(result) and "s3cret" not in " ".join(alerts)
    assert "db.example.internal" not in result["error"]  # the address is cut too, not only the password


def test_a_load_past_its_ceiling_is_stopped_and_only_then_its_import_lock_released():
    ecs = TwoTaskEcs(["arn:task/collect", "arn:task/load"],
                     describe_responses=[_stopped_response(exit_code=0), _running_response()])
    released = []
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()), \
         patch("ddp_sync.pipelines.cloud_scrape_trigger._release_stopped_run_lock",
               side_effect=lambda ecs_client, arn, *lock: released.append((arn, *lock[:3])) or "lock released"):
        result = _scrape(ecs, load_timeout_seconds=0)
    assert result["success"] is False
    assert ecs.stop_task_calls and ecs.stop_task_calls[0]["task"] == "arn:task/load"
    assert released == [("arn:task/load", "fl_2026", result["cloud_run_id"], "_import_lock")]


def test_a_failed_stop_request_releases_no_lock():
    class StopFails(TwoTaskEcs):
        def stop_task(self, **kwargs):
            raise RuntimeError("AccessDenied")

    ecs = StopFails(["arn:task/collect", "arn:task/load"],
                    describe_responses=[_stopped_response(exit_code=0), _running_response()])
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()), \
         patch("ddp_sync.pipelines.cloud_scrape_trigger._release_stopped_run_lock",
               side_effect=AssertionError("released a lock after a failed stop")):
        result = _scrape(ecs, load_timeout_seconds=0)
    assert result["success"] is False


# ── a ddp-sync restart ───────────────────────────────────────────────────────────────────────


def _record(**extra):
    cfg = {**_fargate_config()["cloud_path"]["fargate"], "load_on_fargate": True}
    return {"jurisdiction": "fl", "session_arg": "session=2026", "task_arn": "arn:task/collect",
            "fargate_cfg": cfg, "openstates_root": "/fake/root", **extra}


def test_a_restart_during_the_load_waits_for_that_load_task_and_starts_no_second_one():
    ecs = TwoTaskEcs([], describe_responses=[_stopped_response(exit_code=0)])
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()):
        result = cst.resume_inflight_fargate_job(
            "fl-0123456789ab", _record(load_task_arn="arn:task/load"), ecs_client=ecs,
            subprocess_runner=_local_runner_must_not_run,
        )
    assert result["success"] is True, result
    assert ecs.run_task_calls == []
    assert [c["tasks"] for c in ecs.describe_calls] == [["arn:task/load"]]  # not the finished collection


def test_a_restart_before_the_load_task_was_recorded_starts_the_load_once():
    """The window the design names: the task started but its ARN was not yet recorded. The resume finds no load ARN,
    so it launches the load (once); a first load still running would make this one refuse on its import lock."""
    ecs = TwoTaskEcs(["arn:task/load-2"],
                     describe_responses=[_stopped_response(exit_code=0), _stopped_response(exit_code=0)])
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()), \
         patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=RDS):
        result = cst.resume_inflight_fargate_job(
            "fl-0123456789ab", _record(), ecs_client=ecs, subprocess_runner=_local_runner_must_not_run,
        )
    assert result["success"] is True, result
    assert len(ecs.run_task_calls) == 1
    assert _overrides(ecs.run_task_calls[0])[0] == ["fl", "fl-0123456789ab", "session=2026"]


def test_a_job_launched_for_the_local_load_resumes_with_the_local_load():
    """The choice is made at launch and stored in the record: a manifest flipped meanwhile changes nothing."""
    ecs = TwoTaskEcs([], describe_responses=[_stopped_response(exit_code=0)])
    ran = []
    record = _record()
    record["fargate_cfg"]["load_on_fargate"] = False
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()), \
         patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=RDS):
        result = cst.resume_inflight_fargate_job(
            "fl-0123456789ab", record, ecs_client=ecs,
            subprocess_runner=lambda cmd, env: ran.append(cmd) or type("R", (), {"returncode": 0, "stderr": b""})(),
        )
    assert result["success"] is True
    assert ecs.run_task_calls == []
    assert ran and ran[0][1] == "/fake/root/cloud_loader.py"


# ── the loader's lock key ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("state", "session_arg", "key"), [
    ("fl", None, "fl"),
    ("fl", "session=2026", "fl_2026"),
    ("usa", "session=119 chamber=lower", "usa_119_lower"),
    ("usa", "chamber=upper session=119", "usa_119_upper"),
    ("ut", "session=2026 a=1 b=2", "ut_2026_1_2"),
])
def test_the_lock_key_is_the_one_cloud_loader_takes(state, session_arg, key):
    """Pinned to ddp-open-states' derive_scrape_key(source, parse_kv_args(...)): the docstring's own examples
    (`fl_2026`, the USA chamber split) plus the sorted-extras rule."""
    assert cst.loader_lock_key(state, session_arg) == key


# ── D2 for the archive task ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_archive_task_passes_no_connection_string_with_the_task_secret_on(monkeypatch):
    monkeypatch.setattr(oa, "resolve_rds_database_url",
                        lambda: (_ for _ in ()).throw(AssertionError("resolved although the task reads its secret")))
    ecs = TwoTaskEcs(["arn:task/archive"], describe_responses=[_stopped_response(exit_code=0)])
    cfg = {**_fargate_config()["cloud_path"]["fargate"], "rds_from_task_secret": True, "max_wait_seconds": 5}
    result = await oa._run_archive_fargate("mi", config={"cloud_path": {"fargate": cfg}}, ecs_client=ecs)
    assert result["success"] is True
    command, env = _overrides(ecs.run_task_calls[0])
    assert command == ["mi"]
    assert env["RUNNER_SCRIPT"] == "cloud_archiver.py"
    assert "DATABASE_URL" not in env


def test_redact_dsn_cuts_every_connection_string():
    text = f"a {RDS[0]} b postgres://u:p@h/db c"
    assert oa.redact_dsn(text) == "a postgresql://<redacted> b postgresql://<redacted> c"
