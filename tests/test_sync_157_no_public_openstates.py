"""SYNC-157 part 1: ddp-sync never reaches the public OpenStates API.

Every OpenStates read goes to this host's own api-v3 (`services.openstates_route.api_v3`): the RDS-backed one on a host
with no CAMS access and an RDS base (EC2), else the local one (the Mac). A state the jurisdiction manifest does not
serve is refused (and alerted once), never fetched from anywhere else. These tests replace the per-caller routing tests
of SYNC-6/8/93, whose subject (a per-host list that sent every other state to v3.openstates.org) is deleted.
"""

from __future__ import annotations

import pathlib
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from ddp_sync.config import SyncSettings
from ddp_sync.ingestion.sources.openstates import OpenStatesSource
from ddp_sync.pipelines.bill_sync import BillSyncService
from ddp_sync.pipelines.legislator_sync import LegislatorSyncService
from ddp_sync.services import jurisdiction_enrollment as je
from ddp_sync.services import openstates_route
from ddp_sync.services.openstates_people import OpenStatesPeopleClient
from ddp_sync.services.openstates_route import NoOpenStatesRoute, api_v3
from ddp_sync.sync.federal_legislator_cache import FederalLegislatorCache

REPO = pathlib.Path(__file__).resolve().parent.parent
LOCAL, RDS = "http://localhost:8002", "http://10.0.0.11:8002"
# pa, ne and la: the three live states the old EC2 routing list lacked on 2026-11-12 (they went to the public API)
SERVED = ["us", "fl", "wa", "va", "mi", "ma", "ut", "az", "nc", "pa", "ne", "la", "al"]


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    je._cache.clear()
    je._logged.clear()
    openstates_route._alerted.clear()
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump({c: {"name": c.upper(), "status": "live"} for c in SERVED}))
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path))
    return tmp_path


@pytest.fixture
def alerts():
    with patch("ddp_sync.slack_alerts.post_alert") as post:
        yield post


def mac(**kw):
    return SyncSettings(openai_api_key="test", cams_api_token="cams", local_openstates_api_base=LOCAL, local_openstates_api_key="local-key",
                        rds_openstates_api_base=RDS, rds_openstates_api_key="rds-key", **kw)


def ec2(**kw):
    return SyncSettings(openai_api_key="test", cams_api_token="", local_openstates_api_base=LOCAL, local_openstates_api_key="local-key",
                        rds_openstates_api_base=RDS, rds_openstates_api_key="rds-key", **kw)


# --- the one route -------------------------------------------------------------------------------------------------


def test_the_mac_reads_its_local_api_and_ec2_the_rds_one():
    assert api_v3(mac(), "pa") == (LOCAL, "local-key")
    assert api_v3(ec2(), "pa") == (RDS, "rds-key")


@pytest.mark.parametrize("code", ["pa", "ne", "la", "PA", "usa", "us"])
def test_every_served_state_including_the_three_that_fell_through_goes_to_our_api(code):
    assert api_v3(ec2(), code) == (RDS, "rds-key")


def test_a_state_the_manifest_does_not_serve_is_refused_and_alerted_once(alerts):
    for _ in range(2):
        with pytest.raises(NoOpenStatesRoute, match="not in the jurisdiction manifest"):
            api_v3(ec2(), "tx")
    assert alerts.call_count == 1  # once per process, not once per call


def test_a_host_with_no_api_of_its_own_is_refused_not_sent_to_the_public_api(alerts):
    bare = SyncSettings(cams_api_token="", local_openstates_api_base="", rds_openstates_api_base="")
    with pytest.raises(NoOpenStatesRoute, match="no api-v3 of its own"):
        api_v3(bare, "fl")
    assert alerts.call_count == 1


@pytest.mark.parametrize("base", ["https://v3.openstates.org", "https://V3.OpenStates.ORG/", "HTTPS://V3.OPENSTATES.ORG"])
def test_a_base_pointing_at_the_public_host_is_refused_in_any_casing(alerts, base):
    with pytest.raises(NoOpenStatesRoute, match="v3.openstates.org"):
        api_v3(SyncSettings(cams_api_token="x", local_openstates_api_base=base), "fl")


def test_an_unreadable_manifest_still_uses_our_own_api(tmp_path, monkeypatch):
    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    assert api_v3(ec2(), "pa") == (RDS, "rds-key")  # our api only holds our data; nothing public is reached


def test_a_call_with_no_state_goes_to_our_api():
    assert api_v3(ec2()) == (RDS, "rds-key")


def test_the_deleted_settings_are_gone():
    fields = set(SyncSettings.__dataclass_fields__)
    assert not fields & {"openstates_api_base", "openstates_api_key", "ddp_openstates_jurisdictions",
                         "legbot_rds_replica_jurisdiction_allowlist"}


# --- every caller ----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("make", [
    lambda s: BillSyncService(s), lambda s: LegislatorSyncService(s), lambda s: OpenStatesSource(s),
    lambda s: OpenStatesPeopleClient(s), lambda s: FederalLegislatorCache(s),
])
def test_every_caller_routes_to_our_api_and_refuses_an_unserved_state(make, alerts):
    caller = make(ec2())
    for code in ("pa", "ne", "la", "fl"):
        base, key, _ = caller._get_api_base_and_key(code)
        assert (base, key) == (RDS, "rds-key")
    with pytest.raises(NoOpenStatesRoute):
        caller._get_api_base_and_key("tx")


def _patch_async_client(module, client):
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch(f"{module}.httpx.AsyncClient", return_value=cm)


def _response(data):
    resp = MagicMock(status_code=200)
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


@pytest.mark.asyncio
async def test_lookups_by_opaque_id_go_to_our_api_not_the_public_one():
    source = OpenStatesSource(ec2())
    client = AsyncMock()
    client.get.return_value = _response({"id": "ocd-bill/1", "title": "A bill", "jurisdiction": {"id": "va"}})
    with _patch_async_client("ddp_sync.ingestion.sources.openstates", client):
        await source.fetch_bill("ocd-bill/1")
    url, kwargs = client.get.call_args
    assert url[0] == f"{RDS}/bills/ocd-bill/1" and kwargs["headers"]["X-API-Key"] == "rds-key"

    client = AsyncMock()
    client.get.return_value = _response({"results": [{"id": "ocd-person/1", "name": "Someone"}]})
    with _patch_async_client("ddp_sync.ingestion.sources.openstates", client):
        await source.fetch_legislator_by_id("ocd-person/1")
    url, kwargs = client.get.call_args
    assert url[0] == f"{RDS}/people" and kwargs["headers"]["X-API-Key"] == "rds-key"


@pytest.mark.asyncio
async def test_a_missing_key_sends_no_key_header():
    source = OpenStatesSource(SyncSettings(cams_api_token="x", local_openstates_api_base=LOCAL, local_openstates_api_key=""))
    client = AsyncMock()
    client.get.return_value = _response({"id": "ocd-bill/1", "title": "A bill", "jurisdiction": {"id": "va"}})
    with _patch_async_client("ddp_sync.ingestion.sources.openstates", client):
        await source.fetch_bill("ocd-bill/1")
    assert "X-API-Key" not in client.get.call_args[1]["headers"]


# --- the guard: nothing builds a URL on the public host --------------------------------------------------------------


def test_no_code_or_script_names_the_public_host():
    """Only openstates_route.py may name it (to refuse it). Comments explaining history are fine; a string is not."""
    offenders = []
    for path in [*(REPO / "src").rglob("*.py"), *(REPO / "scripts").rglob("*.py"), *(REPO / "infrastructure").rglob("*")]:
        if not path.is_file() or path.name == "openstates_route.py":
            continue
        for n, line in enumerate(path.read_text(errors="ignore").splitlines(), 1):
            code = line.split("#", 1)[0]
            if re.search(r"""["'][^"']*v3\.openstates\.org""", code):
                offenders.append(f"{path.relative_to(REPO)}:{n}")
    assert offenders == [], f"public OpenStates host named in code: {offenders}"


def test_no_env_or_secret_mapping_names_the_deleted_settings():
    text = (REPO / "infrastructure" / "render-env.sh").read_text()
    for name in ("OPENSTATES_API_KEY", "OPENSTATES_API_BASE", "DDP_OPENSTATES_JURISDICTIONS"):
        assert not re.search(rf'["\s]{name}["\s]', text), name
