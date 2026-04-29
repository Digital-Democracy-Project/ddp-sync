"""Tests for the /trigger/legislator-bio-sync endpoint (Phase 1 step 5).

Pins the round-7 ALB-timeout safety gate (503 + Retry-After when the
congress-legislators source isn't warmed) plus param validation, audit-only
short-circuit, and the happy-path that wires options through to the
orchestrator and serializes the report.

These are HTTP-level tests via FastAPI's TestClient; the orchestrator
itself has its own unit coverage in tests/test_legislator_bio_foundation.py
and end-to-end smoke (run during step 4 development).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router


@dataclass
class _FakeSource:
    """Stand-in for CongressLegislatorsSource.warmed-flag inspection only."""

    _warmed: bool = False


@dataclass
class _FakeReport:
    """Match BioSyncReport shape (asdict-able)."""

    cms_items_seen: int = 0
    items_resolved_via_openstates: int = 0
    items_resolved_via_bioguide_fallback: int = 0
    would_patch: list = field(default_factory=list)
    would_create: list = field(default_factory=list)
    potential_merges: list = field(default_factory=list)
    upstream_orphans: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    aborted: bool = False
    abort_reason: str | None = None


def _make_app(*, warmed: bool):
    app = FastAPI()
    app.state.congress_legislators = _FakeSource(_warmed=warmed)
    app.include_router(router)
    # Bypass auth for the test client
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    return app


def test_returns_503_with_retry_after_when_source_not_warmed():
    """Round-7 ALB-timeout safety gate."""
    app = _make_app(warmed=False)
    client = TestClient(app)
    resp = client.post("/trigger/legislator-bio-sync")
    assert resp.status_code == 503
    assert resp.headers.get("retry-after") == "60"
    body = resp.json()
    assert "warming up" in body["detail"].lower()


def test_returns_503_when_app_state_missing():
    """If app.state.congress_legislators isn't set up at all (misconfigured
    deployment) we should also gate, not crash."""
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"
    client = TestClient(app)
    resp = client.post("/trigger/legislator-bio-sync")
    assert resp.status_code == 503


def test_invalid_target_rejected_as_400():
    app = _make_app(warmed=True)
    client = TestClient(app)
    resp = client.post("/trigger/legislator-bio-sync?target=does-not-exist")
    assert resp.status_code == 400
    assert "target" in resp.json()["detail"].lower()


def test_invalid_audit_only_rejected_as_400():
    app = _make_app(warmed=True)
    client = TestClient(app)
    resp = client.post("/trigger/legislator-bio-sync?audit_only=Z")
    assert resp.status_code == 400
    assert "audit" in resp.json()["detail"].lower()


def test_invalid_historical_since_rejected_as_400():
    app = _make_app(warmed=True)
    client = TestClient(app)
    resp = client.post(
        "/trigger/legislator-bio-sync?historical_since=not-a-date"
    )
    assert resp.status_code == 400


def test_audit_only_a_runs_audit_and_returns_report():
    """Step 6: audit_only=A invokes audit_federal_join_keys and returns the report."""
    app = _make_app(warmed=True)
    client = TestClient(app)

    from ddp_sync.pipelines.legislator_bio import AuditEntry, AuditReport
    fake_report = AuditReport(
        audit_name="A",
        total_scanned=535,
        flagged_count=2,
        flagged=[
            AuditEntry(webflow_id="w-1", slug="missing-1", name="Missing One",
                       chamber="Senate"),
            AuditEntry(webflow_id="w-2", slug="missing-2", name="Missing Two",
                       chamber="House"),
        ],
    )
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.audit_federal_join_keys = AsyncMock(return_value=fake_report)
        instance.audit_state_join_keys = AsyncMock()  # should NOT be called
        resp = client.post("/trigger/legislator-bio-sync?audit_only=A")

    assert resp.status_code == 200
    body = resp.json()
    assert body["audit_name"] == "A"
    assert body["total_scanned"] == 535
    assert body["flagged_count"] == 2
    assert len(body["flagged"]) == 2
    assert body["flagged"][0]["webflow_id"] == "w-1"
    instance.audit_federal_join_keys.assert_awaited_once()
    instance.audit_state_join_keys.assert_not_called()


def test_audit_only_c_runs_audit_and_passes_jurisdiction():
    """audit_only=C with jurisdiction filters by that state code."""
    app = _make_app(warmed=True)
    client = TestClient(app)

    from ddp_sync.pipelines.legislator_bio import AuditReport
    fake_report = AuditReport(
        audit_name="C",
        total_scanned=160,
        flagged_count=3,
        jurisdiction="FL",
    )
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.audit_state_join_keys = AsyncMock(return_value=fake_report)
        resp = client.post(
            "/trigger/legislator-bio-sync?audit_only=c&jurisdiction=FL"
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["audit_name"] == "C"
    assert body["jurisdiction"] == "FL"
    assert body["flagged_count"] == 3
    instance.audit_state_join_keys.assert_awaited_once_with(jurisdiction="FL")


def test_audit_only_c_with_no_jurisdiction_passes_none():
    app = _make_app(warmed=True)
    client = TestClient(app)
    from ddp_sync.pipelines.legislator_bio import AuditReport
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.audit_state_join_keys = AsyncMock(
            return_value=AuditReport(audit_name="C", total_scanned=0, flagged_count=0)
        )
        resp = client.post("/trigger/legislator-bio-sync?audit_only=C")
    assert resp.status_code == 200
    instance.audit_state_join_keys.assert_awaited_once_with(jurisdiction=None)


def test_audit_only_exception_returns_500():
    """Unhandled exception during audit → HTTP 500 with detail."""
    app = _make_app(warmed=True)
    client = TestClient(app)
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.audit_federal_join_keys = AsyncMock(
            side_effect=RuntimeError("audit broke")
        )
        resp = client.post("/trigger/legislator-bio-sync?audit_only=A")
    assert resp.status_code == 500
    assert "audit broke" in resp.json()["detail"]


def test_happy_path_invokes_orchestrator_and_returns_report():
    """Wires options through to LegislatorBioPipeline.run() and serializes
    the resulting BioSyncReport as JSON."""
    app = _make_app(warmed=True)
    client = TestClient(app)

    fake_report = _FakeReport(
        cms_items_seen=2,
        items_resolved_via_openstates=1,
        items_resolved_via_bioguide_fallback=1,
        would_patch=[{"webflow_id": "w-1", "name": "Rick", "changed_fields": ["x"]}],
    )

    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.run = AsyncMock(return_value=fake_report)
        resp = client.post(
            "/trigger/legislator-bio-sync"
            "?dry_run=true&jurisdiction=us&limit=5"
            "&historical_since=2024-01-01"
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cms_items_seen"] == 2
    assert body["items_resolved_via_openstates"] == 1
    assert body["items_resolved_via_bioguide_fallback"] == 1
    assert len(body["would_patch"]) == 1
    assert body["would_patch"][0]["webflow_id"] == "w-1"

    # Verify orchestrator was constructed with the pre-warmed source
    MockPipeline.assert_called_once()
    kwargs = MockPipeline.call_args.kwargs
    assert "congress" in kwargs
    # And run() got the right options
    instance.run.assert_awaited_once()
    options = instance.run.call_args.args[0]
    assert options.dry_run is True
    assert options.jurisdiction == "us"
    assert options.limit == 5
    assert options.historical_since.isoformat() == "2024-01-01"


def test_happy_path_default_options():
    """Defaults: dry_run=False, target=all, limit=0, historical_since=2023-01-01."""
    app = _make_app(warmed=True)
    client = TestClient(app)
    fake_report = _FakeReport()

    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.run = AsyncMock(return_value=fake_report)
        resp = client.post("/trigger/legislator-bio-sync")

    assert resp.status_code == 200
    options = instance.run.call_args.args[0]
    assert options.dry_run is False
    assert options.target == "all"
    assert options.limit == 0
    assert options.historical_since.isoformat() == "2023-01-01"
    assert options.auto_create is False


def test_orchestrator_exception_returns_500():
    app = _make_app(warmed=True)
    client = TestClient(app)
    with patch(
        "ddp_sync.pipelines.legislator_bio.LegislatorBioPipeline"
    ) as MockPipeline:
        instance = MockPipeline.return_value
        instance.run = AsyncMock(side_effect=RuntimeError("upstream broke"))
        resp = client.post("/trigger/legislator-bio-sync")
    assert resp.status_code == 500
    assert "upstream broke" in resp.json()["detail"]
