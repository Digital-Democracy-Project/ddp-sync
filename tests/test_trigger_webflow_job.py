"""SYNC-113: the retired Webflow organization merge cannot be started by hand either.

Mirrors test_trigger_grantbot_scrape_funders.py's TestClient shape.
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ddp_sync.api.auth import api_key_auth
from ddp_sync.api.routes.triggers import router


def test_merge_duplicate_orgs_is_an_unknown_job():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[api_key_auth] = lambda: "test-token"

    resp = TestClient(app).post("/trigger/webflow/merge-duplicate-orgs")

    assert resp.status_code == 404
    assert "Unknown job: merge-duplicate-orgs" in resp.json()["detail"]
