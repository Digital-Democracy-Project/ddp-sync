"""SYNC-200 (OPEN-896, OPEN-929): the legislature's own summary of a bill goes to LegBot as an extra, labelled input.

ddp-sync holds no state code for this: the yes/no is the manifest's `enrollment.legbot_send_abstract` (absent = false),
read through the shared manifest reader and looked up by the artifact's jurisdiction. The summary is its own payload key
(`legislature_summary`), never inside `bill_source`, and only for the seven single-version artifact types. These tests
use fakes only (no network, no CAMS, no LegBot). The manifest fixtures below name states as DATA; the production code
does not.
"""

from __future__ import annotations

import contextlib
import copy
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ddp_sync.pipelines import bill_artifact_generation as gen
from ddp_sync.pipelines.bill_artifact_generation import (
    _LEGISLATURE_SUMMARY_ARTIFACT_TYPES,
)
from ddp_sync.services import jurisdiction_enrollment as je
from ddp_sync.services import local_openstates_client as lo
from ddp_sync.services.legbot_client import dispatch_bill_question
from tests import sync157_manifest

SEVEN = ["bill_summary", "bill_impact_analysis", "bill_pros_cons", "bill_vote_yes_frame", "bill_vote_no_frame",
         "bill_supporting_orgs", "bill_opposing_orgs"]
SUMMARY = "Directs a hospital to adopt a nurse staffing plan."
TEXT = "BILL TEXT"
KWARGS = {
    "bill_openstates_id": "8d71a94e-0000-0000-0000-000000000001",
    "jurisdiction": "OR",
    "session_code": "2026R1",
    "version_date": "2026-01-05",
    "version_note": "Introduced",
}


def _manifest(on=("or",), off=("fl",)):
    """The stand-in manifest with the flag true for `on`, false for `off`, and ABSENT for every other state."""
    manifest = copy.deepcopy(sync157_manifest.MANIFEST)
    for code, entry in manifest.items():
        entry["enrollment"].pop("legbot_send_abstract", None)
    manifest.setdefault("or", copy.deepcopy(manifest["fl"]))
    for code in on:
        manifest[code]["enrollment"]["legbot_send_abstract"] = True
    for code in off:
        manifest[code]["enrollment"]["legbot_send_abstract"] = False
    return manifest


@pytest.fixture
def flag_manifest(tmp_path, monkeypatch):
    return sync157_manifest.use(tmp_path, monkeypatch, manifest=_manifest())


# ── the manifest reader ───────────────────────────────────────────────────────────


def test_the_reader_reads_the_key_from_a_manifest_that_has_it(flag_manifest):
    assert je.is_enrolled("legbot_send_abstract", "or", flag_manifest) is True
    assert je.is_enrolled("legbot_send_abstract", "OR", flag_manifest) is True  # looked up case-insensitively
    assert je.is_enrolled("legbot_send_abstract", "fl", flag_manifest) is False  # explicitly false
    assert je.is_enrolled("legbot_send_abstract", "wa", flag_manifest) is False  # absent = false
    assert je.manifest_codes("legbot_send_abstract", flag_manifest) == ["or"]


def test_a_manifest_without_the_key_anywhere_answers_false_not_unknown(tmp_path, monkeypatch):
    manifest = _manifest(on=(), off=())
    root = sync157_manifest.use(tmp_path, monkeypatch, manifest=manifest)
    assert je.manifest_codes("legbot_send_abstract", root) == []  # an answer (nobody), not None


def test_a_non_boolean_value_is_not_a_yes(tmp_path, monkeypatch):
    manifest = _manifest(on=(), off=())
    manifest["or"]["enrollment"]["legbot_send_abstract"] = "true"  # a string, not a yaml bool
    root = sync157_manifest.use(tmp_path, monkeypatch, manifest=manifest)
    assert je.is_enrolled("legbot_send_abstract", "or", root) is False


def test_a_missing_manifest_is_a_no(tmp_path, monkeypatch):
    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    assert gen._sends_legislature_summary("bill_summary", "OR") is False


def test_the_seven_types_are_exactly_these():
    assert sorted(_LEGISLATURE_SUMMARY_ARTIFACT_TYPES) == sorted(SEVEN)
    assert "bill_topics" not in _LEGISLATURE_SUMMARY_ARTIFACT_TYPES


# ── the payload ───────────────────────────────────────────────────────────────────


async def _payload(**kwargs):
    with patch("ddp_sync.services.legbot_client._dispatch_and_await", new=AsyncMock(return_value={})) as send:
        await dispatch_bill_question(TEXT, "summary_500char", **kwargs)
    return send.await_args.args[0]


@pytest.mark.asyncio
async def test_payload_carries_the_summary_as_its_own_key_not_inside_bill_source():
    payload = await _payload(legislature_summary=f"  {SUMMARY}  ")
    assert payload["legislature_summary"] == SUMMARY
    assert payload["bill_source"] == TEXT  # unchanged: the latest version's text only
    assert SUMMARY not in payload["bill_source"]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", "   "])
async def test_payload_is_byte_identical_to_today_without_a_summary(value):
    assert await _payload(legislature_summary=value) == await _payload()
    assert await _payload() == {"bill_source": TEXT, "question_type": "summary_500char", "caller": "ddp_sync"}


# ── the local api-v3 read ─────────────────────────────────────────────────────────


class _Settings:
    local_openstates_api_base = "http://api.test"
    local_openstates_api_key = "k"


def _client(response=None, error=None):
    client = MagicMock()
    client.get = AsyncMock(return_value=response, side_effect=error)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return client, patch.object(lo.httpx, "AsyncClient", return_value=cm)


def _response(body, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json = MagicMock(return_value=body)
    return resp


BODY = {
    "versions": [{"links": [{"raw_text": "OLD"}]}, {"links": [{"raw_text": TEXT}]}],
    "abstracts": [{"abstract": "Some chamber digest.", "note": "house abstract"},
                  {"abstract": f"  {SUMMARY}  ", "note": "summary"}],
}


@pytest.mark.asyncio
async def test_one_read_asks_for_versions_and_abstracts_together_and_takes_the_summary_note():
    client, cm = _client(_response(BODY))
    with patch.object(lo, "get_settings", return_value=_Settings()), cm:
        result = await lo.get_archived_bill_text_and_summary("abc")
    assert result == (TEXT, SUMMARY)
    assert client.get.await_count == 1
    assert client.get.await_args.kwargs["params"] == {"include": ["versions", "abstracts"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("abstracts", [None, [], [{"abstract": "digest", "note": "house abstract"}],
                                       [{"abstract": "digest", "note": "Summary"}],  # the note must be exactly `summary`
                                       [{"abstract": "   ", "note": "summary"}]])
async def test_no_summary_note_or_an_empty_one_means_no_summary(abstracts):
    _, cm = _client(_response({**BODY, "abstracts": abstracts}))
    with patch.object(lo, "get_settings", return_value=_Settings()), cm:
        assert await lo.get_archived_bill_text_and_summary("abc") == (TEXT, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["404", "500", "unreachable"])
async def test_any_failure_answers_nothing_and_never_raises(kind):
    if kind == "unreachable":
        _, cm = _client(error=httpx.ConnectError("down"))
    else:
        _, cm = _client(_response({}, status=int(kind)))
    with patch.object(lo, "get_settings", return_value=_Settings()), cm:
        assert await lo.get_archived_bill_text_and_summary("abc") == (None, None)


@pytest.mark.asyncio
async def test_the_old_text_read_still_asks_for_versions_only():
    client, cm = _client(_response(BODY))
    with patch.object(lo, "get_settings", return_value=_Settings()), cm:
        assert await lo.get_archived_bill_text("abc") == TEXT
    assert client.get.await_args.kwargs["params"] == {"include": "versions"}


# ── the artifact pipeline ─────────────────────────────────────────────────────────


async def _generate(artifact_type, *, jurisdiction="OR", summary=SUMMARY):
    answer = {"answer": {"text": "ok", "insufficient_information": False, "org_types": [], "frame": "x"},
              "backend": "mlx"}
    with patch.object(gen, "get_archived_bill_text_and_summary", new=AsyncMock(return_value=(TEXT, summary))) as both, \
            patch.object(gen, "get_archived_bill_text", new=AsyncMock(return_value=TEXT)) as plain, \
            patch.object(gen, "dispatch_bill_question", new=AsyncMock(return_value=answer)) as dispatch, \
            patch.object(gen, "write_bill_artifact", new=AsyncMock(return_value={"id": 1})), \
            contextlib.suppress(Exception):  # the content step after dispatch is not under test; the dispatch call is
        await gen.generate_and_store_bill_artifact(**{**KWARGS, "jurisdiction": jurisdiction},
                                                   artifact_type=artifact_type)
    return dispatch, both, plain


@pytest.mark.asyncio
@pytest.mark.parametrize("artifact_type", SEVEN)
async def test_flag_true_and_an_abstract_sends_the_summary_for_each_of_the_seven(flag_manifest, artifact_type):
    dispatch, both, plain = await _generate(artifact_type)
    dispatch.assert_awaited_once()
    assert dispatch.await_args.args == (TEXT, gen._ARTIFACT_TYPE_TO_QUESTION_TYPE[artifact_type])
    assert dispatch.await_args.kwargs["legislature_summary"] == SUMMARY
    both.assert_awaited_once()
    plain.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("jurisdiction", ["FL", "WA"])  # explicitly false, and absent from the manifest key
async def test_flag_false_or_absent_leaves_the_call_exactly_as_today(flag_manifest, jurisdiction):
    dispatch, both, plain = await _generate("bill_summary", jurisdiction=jurisdiction)
    assert dispatch.await_args.args == (TEXT, "summary_500char")
    assert dispatch.await_args.kwargs == {"topic_list": None}  # what the call sent before SYNC-200
    both.assert_not_awaited()
    plain.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("summary", [None, ""])
async def test_flag_true_but_no_abstract_leaves_the_call_exactly_as_today(flag_manifest, summary):
    dispatch, _, _ = await _generate("bill_summary", summary=summary)
    assert dispatch.await_args.args == (TEXT, "summary_500char")
    assert dispatch.await_args.kwargs == {"topic_list": None}


@pytest.mark.asyncio
async def test_bill_topics_never_gets_the_summary_even_with_the_flag_true(flag_manifest):
    with patch.object(gen, "_bill_topics_taxonomy",
                      new=AsyncMock(return_value={"version": "v", "max_topics": 3, "topics": ["A"]})):
        dispatch, both, plain = await _generate("bill_topics")
    assert "legislature_summary" not in dispatch.await_args.kwargs
    both.assert_not_awaited()
    plain.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_unreadable_manifest_fails_closed(tmp_path, monkeypatch):
    je._cache.clear()
    monkeypatch.setenv("OPENSTATES_ROOT", str(tmp_path / "nowhere"))
    dispatch, both, _ = await _generate("bill_summary")
    assert "legislature_summary" not in dispatch.await_args.kwargs
    both.assert_not_awaited()


# ── the other callers and the changelog ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_resolve_bill_source_still_returns_text_only(flag_manifest):
    with patch.object(gen, "get_archived_bill_text", new=AsyncMock(return_value=TEXT)):
        assert await gen._resolve_bill_source("abc") == TEXT


@pytest.mark.asyncio
async def test_concept_statements_are_unchanged_with_the_flag_true(flag_manifest):
    from ddp_sync.pipelines.concept_statement_dispatch import (
        dispatch_and_store_concept_statements,
    )

    answer = {"answer": {"statements": ["S."], "insufficient_information": False}, "backend": "mlx"}
    with patch.object(gen, "get_archived_bill_text", new=AsyncMock(return_value=TEXT)), \
            patch("ddp_sync.pipelines.concept_statement_dispatch.dispatch_bill_question",
                  new=AsyncMock(return_value=answer)) as dispatch, \
            patch("ddp_sync.pipelines.concept_statement_dispatch.create_concept_statement_set",
                  new=AsyncMock(return_value={"id": 1})):
        await dispatch_and_store_concept_statements(gov_id="HB 1", bill_openstates_id="abc", jurisdiction_iso2="OR",
                                                    session_code="2026R1", bill_source="u")
    dispatch.assert_awaited_once_with(TEXT, "concept_statements")


def test_the_changelog_and_the_other_dispatchers_do_not_know_the_field():
    import pathlib

    src = pathlib.Path(gen.__file__).parent
    for name in ("policy_statement_dispatch.py", "bill_organization_position_research.py",
                 "concept_statement_dispatch.py"):
        assert "legislature_summary" not in (src / name).read_text(), name
    changelog = gen.generate_and_store_bill_changelog
    assert "legislature_summary" not in changelog.__code__.co_names + changelog.__code__.co_varnames
