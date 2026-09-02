"""Scheduler wiring tests for SYNC-51's per-task .env flags.

Light-touch, matching test_scheduler_bio_sync.py's own established convention for this
module (no other broad coverage exists) -- these pin that each flag actually gates its own
job(s), that unrelated jobs are untouched by one flag, and that a flag with an existing
(shared, checked-in) YAML gate ANDs with it rather than replacing it.
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ddp_sync.scheduler import UpdateScheduler


def _settings(**overrides) -> MagicMock:
    """A settings MagicMock with every SYNC-51 flag explicitly True (the documented
    default), overridden per-test. Explicit rather than relying on MagicMock's own
    attribute-access-returns-truthy behavior -- these tests are exactly the ones that
    need to tell True apart from False, not just "truthy"."""
    settings = MagicMock()
    settings.sync_interval_minutes = 30
    settings.bill_sync_enabled = True
    settings.legislator_sync_enabled = True
    settings.legislator_bio_sync_enabled = True
    settings.organization_sync_enabled = True
    settings.voatz_sync_enabled = True
    settings.webflow_batch_enabled = True
    settings.votebot_eval_enabled = True
    settings.api_health_check_enabled = True
    settings.openstates_scrape_enabled = True
    settings.openstates_archive_enabled = True
    settings.mi_cookie_publish_enabled = True
    settings.session_pipeline_batch_enabled = True
    for key, value in overrides.items():
        setattr(settings, key, value)
    return settings


def _scheduler_with_yaml(yaml_text: str, **settings_overrides) -> UpdateScheduler:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(textwrap.dedent(yaml_text))
        path = Path(f.name)
    return UpdateScheduler(settings=_settings(**settings_overrides), config_path=path)


@pytest.mark.asyncio
async def test_bill_sync_enabled_false_skips_the_job_that_had_no_yaml_gate_at_all():
    """daily_bill_sync had zero gate before SYNC-51 -- the env flag is the only thing
    that can ever skip it."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        """,
        bill_sync_enabled=False,
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "daily_bill_sync" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_bill_sync_enabled_default_true_registers_the_job():
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "daily_bill_sync" in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_voatz_sync_enabled_false_skips_only_voatz_jobs_not_webflow():
    """The two flags inside _register_ddp_api_jobs() must be independent -- a host
    opting out of Voatz must not lose Webflow, and vice versa."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        """,
        voatz_sync_enabled=False,
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "voatz_user_sync" not in ids
        assert "voatz_full_sync" not in ids
        assert "webflow_fill_session_code" in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_webflow_batch_enabled_false_skips_only_webflow_jobs_not_voatz():
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        """,
        webflow_batch_enabled=False,
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "webflow_fill_session_code" not in ids
        assert "webflow_bill_org_sync" not in ids
        assert "voatz_user_sync" in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_mi_cookie_publish_enabled_false_overrides_yaml_enabled_true():
    """The env flag must AND with the existing YAML gate, not be ignored in its
    presence -- this is the actual SYNC-51 scenario: a shared, checked-in
    sync_schedule.yaml says enabled: true for every deployment, but one host's own
    .env opts out."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        openstates_scrape:
          enabled: true
          mi_cookie_publish:
            enabled: true
            interval_hours: 6
        """,
        mi_cookie_publish_enabled=False,
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert not any("mi_cookie" in job_id for job_id in ids)
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_mi_cookie_publish_enabled_true_still_requires_yaml_enabled_true():
    """The reverse direction of the AND: the env flag defaulting True must not make
    mi_cookie_publish run when the shared YAML itself has it disabled (the documented
    default, OPEN-188 -- disabled until explicitly turned on)."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        openstates_scrape:
          enabled: true
          mi_cookie_publish:
            enabled: false
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert not any("mi_cookie" in job_id for job_id in ids)
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_session_pipeline_batch_enabled_false_overrides_yaml_enabled_true():
    """Independent review, round 1: this job was originally missed entirely -- it has no
    cross-host overlap lock of its own (run_scheduled_session_pipeline() calls
    run_legbot_pipeline() directly, not through SYNC-48's overlap-safe wrapper), so this
    env flag is the only lever available to stop it firing on two colocated hosts at once."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        session_pipeline_batch:
          enabled: true
          jurisdiction_iso2: "us-fl"
          session_code: "2026"
          artifact_types: ["bill_summary"]
          limit: 10
        """,
        session_pipeline_batch_enabled=False,
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "session_pipeline_batch" not in ids
    finally:
        sched.stop()


@pytest.mark.asyncio
async def test_session_pipeline_batch_enabled_true_still_requires_yaml_enabled_true():
    """The reverse direction of the AND: the env flag defaulting True must not make
    session_pipeline_batch run when the shared YAML itself has it disabled (the documented
    default -- ddp-infra's Phase 8 concurrency cap/prioritization isn't live yet)."""
    sched = _scheduler_with_yaml(
        """
        bill_sync:
          sync_time_utc: "04:00"
        session_pipeline_batch:
          enabled: false
        """
    )
    sched.start()
    try:
        ids = {j.id for j in sched.scheduler.get_jobs()}
        assert "session_pipeline_batch" not in ids
    finally:
        sched.stop()
