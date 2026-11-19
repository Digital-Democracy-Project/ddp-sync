"""OPEN-639: only the load and archive tasks run from the database task definition (`task_definition_db`), the one
definition that may carry the RDS login; collection keeps `task_definition` and never gets a connection string. A
missing setting refuses the launch instead of falling back to the collection definition."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ddp_sync.pipelines import cloud_scrape_trigger as cst
from ddp_sync.pipelines import openstates_archive as oa
from tests.test_cloud_scrape_trigger import (
    FakeRedisClient,
    _fargate_config,
    _stopped_response,
)
from tests.test_open633_load_on_fargate import (  # noqa: F401 (fixture)
    RDS,
    TwoTaskEcs,
    _manifest,
    _overrides,
)


def _scrape(ecs, **fargate):
    with patch.object(cst.inflight_fargate_jobs, "_client", FakeRedisClient()), \
         patch("ddp_sync.pipelines.cloud_scrape_trigger.resolve_rds_database_url", return_value=RDS), \
         patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"), patch("time.sleep"):
        return cst.run_cloud_scrape("fl", "session=2026", "/fake/root", _fargate_config(**fargate), ecs_client=ecs,
                                    subprocess_runner=lambda cmd, env: None)


def test_collection_runs_on_the_collection_definition_and_the_load_on_the_db_one():
    ecs = TwoTaskEcs(["arn:task/collect", "arn:task/load"],
                     describe_responses=[_stopped_response(exit_code=0), _stopped_response(exit_code=0)])
    assert _scrape(ecs)["success"] is True
    collect, load = ecs.run_task_calls
    assert collect["taskDefinition"] == "ddp-scraper-prototype"
    assert load["taskDefinition"] == "ddp-scraper-prototype-db"
    _, env = _overrides(collect)
    assert not [k for k in env if k == "DATABASE_URL" or k.startswith("RDS_")]  # collection never gets the login
    assert _overrides(load)[1]["RUNNER_SCRIPT"] == "cloud_loader.py"


def test_a_load_without_the_db_definition_refuses_and_never_uses_the_collection_one():
    ecs = TwoTaskEcs(["arn:task/collect", "arn:task/load"], describe_responses=[_stopped_response(exit_code=0)])
    result = _scrape(ecs, task_definition_db=None)
    assert result["success"] is False
    assert "task_definition_db is not set" in result["error"]
    assert [c["taskDefinition"] for c in ecs.run_task_calls] == ["ddp-scraper-prototype"]  # the collection only


@pytest.mark.asyncio
async def test_the_archive_runs_on_the_db_definition(monkeypatch):
    monkeypatch.setattr(oa, "resolve_rds_database_url", lambda: RDS)
    ecs = TwoTaskEcs(["arn:task/archive"], describe_responses=[_stopped_response(exit_code=0)])
    cfg = {**_fargate_config()["cloud_path"]["fargate"], "max_wait_seconds": 5}
    result = await oa._run_archive_fargate("mi", config={"cloud_path": {"fargate": cfg}}, ecs_client=ecs)
    assert result["success"] is True
    assert ecs.run_task_calls[0]["taskDefinition"] == "ddp-scraper-prototype-db"


@pytest.mark.asyncio
async def test_an_archive_without_the_db_definition_refuses_without_launching(monkeypatch):
    monkeypatch.setattr(oa, "resolve_rds_database_url", lambda: RDS)
    monkeypatch.setattr(oa, "_alert_archive_failure", lambda *a, **k: None)
    ecs = TwoTaskEcs(["arn:task/archive"])
    cfg = {**_fargate_config()["cloud_path"]["fargate"], "max_wait_seconds": 5}
    cfg.pop("task_definition_db")
    result = await oa._run_archive_fargate("mi", config={"cloud_path": {"fargate": cfg}}, ecs_client=ecs)
    assert result["success"] is False
    assert "task_definition_db is not set" in result["error"]
    assert ecs.run_task_calls == []


def test_the_real_schedule_names_the_db_definition_for_both_blocks():
    import yaml

    with open("config/sync_schedule.yaml") as f:
        data = yaml.safe_load(f)
    for block in ("openstates_scrape", "openstates_archive"):
        fargate = data[block]["cloud_path"]["fargate"]
        assert fargate["task_definition"] == "ddp-scrapers"
        assert fargate["task_definition_db"] == "ddp-scrapers-db"
