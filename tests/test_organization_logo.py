"""SYNC-161: the logo step -- the acceptance rule on recorded judge_logo answers, the website's candidates, picture preparation,
one organization's search end to end (fake network, fake broker, recorded judge answers), the pipeline step and the backfill.

The judge answers below are in AGENTS-240's answer shape ({is_logo, text_seen, shows_org, shows_parent, names_other_org,
reason}) and mirror the 2026-11-14 test's cases: a wrong picture, a real logo given a mismatched name, a white logo, a
symbol-only logo, and a chapter whose site shows the parent's logo.
"""

import io
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from PIL import Image

from ddp_sync.pipelines import organization_logo as ol
from ddp_sync.pipelines.bill_organization_position_research import (
    resolve_and_describe_organizations,
)

_L = "ddp_sync.pipelines.organization_logo"
_REAL_RESOLVE = ol._resolve  # saved before the fake DNS fixture replaces it
_P = "ddp_sync.pipelines.bill_organization_position_research"

# recorded judge_logo answers (AGENTS-240 shape)
WRONG_PICTURE = {"is_logo": False, "text_seen": "", "shows_org": False, "shows_parent": False, "names_other_org": False,
                 "reason": "a landscape photo"}
SPONSOR = {"is_logo": True, "text_seen": "Acme Bank", "shows_org": False, "shows_parent": False, "names_other_org": True,
           "reason": "the logo of Acme Bank, a sponsor"}
OWN = {"is_logo": True, "text_seen": "Sierra Club", "shows_org": True, "shows_parent": False, "names_other_org": False,
       "reason": "the name in the logo"}
SYMBOL_ONLY = {"is_logo": True, "text_seen": "", "shows_org": False, "shows_parent": False, "names_other_org": False,
               "reason": "a mark with no words"}
PARENT_NAME = {"is_logo": True, "text_seen": "AARP", "shows_org": False, "shows_parent": True, "names_other_org": False,
               "reason": "the parent's name"}


def png(width=300, height=120, color=(20, 90, 200, 255), mode="RGBA", fmt="PNG"):
    out = io.BytesIO()
    Image.new(mode, (width, height), color).save(out, fmt)
    return out.getvalue()


PRIVATE = {"intranet.example": "10.0.0.5", "metadata.example": "169.254.169.254", "localhost": "127.0.0.1"}


@pytest.fixture(autouse=True)
def fake_dns():
    """No real DNS in tests: every host is a public address except the ones named in PRIVATE."""
    import ipaddress

    async def resolve(host):
        try:
            return [str(ipaddress.ip_address(host))]  # an address resolves to itself, as real DNS does
        except ValueError:
            return [PRIVATE.get(host, "93.184.216.34")]
    with patch(f"{_L}._resolve", new=resolve):
        yield


# ---- the acceptance rule ----

@pytest.mark.parametrize("answer,kind,has_parent,expected", [
    (WRONG_PICTURE, "img", False, (False, False)),
    (SPONSOR, "img", False, (False, False)),            # a real logo, but another organization's
    (OWN, "og_image", False, (True, False)),
    (SYMBOL_ONLY, "img", False, (True, False)),         # declared by the site: allowed (Ramon, 2026-11-15)
    (SYMBOL_ONLY, "schema_org", False, (True, False)),
    (SYMBOL_ONLY, "apple_touch_icon", False, (False, False)),  # not declared as the logo: needs the name
    (SYMBOL_ONLY, "og_image", False, (False, False)),
    (PARENT_NAME, "img", True, (True, True)),           # a chapter site showing the parent's name
    (PARENT_NAME, "img", False, (False, False)),        # no parent set: it does not show this organization
])
def test_accept_logo(answer, kind, has_parent, expected):
    assert ol.accept_logo(answer, kind=kind, has_parent=has_parent)[:2] == expected


def test_a_mismatched_name_is_turned_down_even_when_the_picture_is_a_logo():
    mismatched = {**OWN, "shows_org": False, "names_other_org": True, "reason": "shows Sierra Club, not this name"}
    assert ol.accept_logo(mismatched, kind="img", has_parent=False)[0] is False


# ---- the website's candidates ----

PAGE = """<html><head>
<link rel="apple-touch-icon" href="/apple.png">
<meta property="og:image" content="https://cdn.example/banner.jpg">
<script type="application/ld+json">{"@context": "https://schema.org", "@graph": [{"@type": "Organization",
  "logo": {"@type": "ImageObject", "url": "/schema-logo.png"}}]}</script>
</head><body>
<img src="/photo.jpg" alt="volunteers">
<a class="site-logo" href="/"><img src="/img/header.png" alt="Home"></a>
<img src="/sponsors/acme-logo.png" alt="Acme Bank">
<img src="data:image/png;base64,AAAA" class="logo">
</body></html>"""


def test_candidates_are_taken_in_the_tested_order():
    found = ol.website_candidates(PAGE, "https://www.sierraclub.org/")
    assert [(c.kind, c.url) for c in found] == [
        ("img", "https://www.sierraclub.org/img/header.png"),        # labelled by its link's class
        ("img", "https://www.sierraclub.org/sponsors/acme-logo.png"),  # "logo" in the file name: the judge decides
        ("schema_org", "https://www.sierraclub.org/schema-logo.png"),
        ("apple_touch_icon", "https://www.sierraclub.org/apple.png"),
        ("og_image", "https://cdn.example/banner.jpg"),
    ]


def test_names_and_hosts():
    assert ol.clean_name("Sierra Club (national) Inc.") == "Sierra Club"
    assert ol.clean_name("Acme Holdings, LLC") == "Acme Holdings"
    assert ol.site_host("https://WWW.SierraClub.org/about") == "sierraclub.org"
    assert ol.site_host("sierraclub.org") == "sierraclub.org"
    assert ol.site_host("") == ""


# ---- picture preparation ----

def test_a_white_logo_is_shown_to_the_judge_on_grey_and_stored_as_it_is():
    white = png(color=(255, 255, 255, 255))
    canvas = Image.new("RGBA", (300, 120), (0, 0, 0, 0))
    canvas.paste(Image.open(io.BytesIO(white)).crop((0, 0, 100, 40)), (100, 40))
    out = io.BytesIO()
    canvas.save(out, "PNG")
    picture = ol.prepare_picture(out.getvalue())
    judged = Image.open(io.BytesIO(picture.judge_png)).convert("RGB")
    assert judged.getpixel((0, 0)) == ol.GREY            # see-through corner drawn on grey
    assert judged.getpixel((150, 60)) == (255, 255, 255)  # the white mark stays white, now visible
    assert picture.data == out.getvalue() and picture.content_type == "image/png"


def test_a_tiny_icon_is_not_judged_and_an_icon_file_is_stored_as_png():
    assert ol.prepare_picture(png(68, 68)) is None
    ico = io.BytesIO()
    Image.new("RGBA", (256, 256), (10, 20, 30, 255)).save(ico, "ICO", sizes=[(256, 256)])
    picture = ol.prepare_picture(ico.getvalue())
    assert picture.content_type == "image/png" and picture.data[:8] == b"\x89PNG\r\n\x1a\n"
    with pytest.raises(ol.LogoFetchError):
        ol.prepare_picture(b"<html>not a picture</html>")


# ---- one organization, end to end ----

def _state(**overrides):
    state = {"id": 7, "name": "Sierra Club", "website": "https://www.sierraclub.org/", "parent": None,
             "logo": {"url": None, "hidden": False}, "needs_logo": True}
    state.update(overrides)
    return state


def _network(routes):
    """A fake web: {url-prefix: (status, content-type, body)}; Wikidata answers no match unless a route says otherwise."""
    def handler(request):
        url = str(request.url)
        for prefix, (status, content_type, body) in routes.items():
            if url.startswith(prefix):
                data = body if isinstance(body, bytes) else (json.dumps(body) if not isinstance(body, str) else body).encode()
                return httpx.Response(status, headers={"content-type": content_type}, content=data)
        if "wikidata.org" in url:
            return httpx.Response(200, json={"search": []})
        return httpx.Response(404)
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)


async def _run(state, routes, judge_answers=(), force=False):
    judge = AsyncMock(side_effect=[{"answer": a, "backend": "mlx"} for a in judge_answers])
    write = AsyncMock(return_value={})
    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(return_value=state)), \
            patch(f"{_L}.write_organization_logo", new=write), patch(f"{_L}.dispatch_judge_logo", new=judge):
        async with _network(routes) as client:
            result = await ol.find_and_store_organization_logo(organization_id=7, org_name="Sierra Club", client=client,
                                                               force=force)
    return result, write, judge


@pytest.mark.asyncio
async def test_a_commons_logo_matched_by_website_is_used_without_asking_the_judge():
    routes = {
        "https://www.wikidata.org/w/api.php?action=wbsearchentities": (200, "application/json",
                                                                       {"search": [{"id": "Q1"}, {"id": "Q2"}]}),
        "https://www.wikidata.org/w/api.php?action=wbgetentities": (200, "application/json", {"entities": {
            "Q1": {"claims": {"P856": [{"mainsnak": {"datavalue": {"value": "https://sierra.example/"}}}]}},  # another site
            "Q2": {"claims": {
                "P856": [{"mainsnak": {"datavalue": {"value": "https://sierraclub.org"}}}],
                "P154": [{"mainsnak": {"datavalue": {"value": "Sierra Club logo.svg"}}}]}}}}),
        "https://commons.wikimedia.org/w/api.php": (200, "application/json", {"query": {"pages": {"1": {"imageinfo": [{
            "url": "https://upload.example/Sierra.svg", "thumburl": "https://upload.example/512px-Sierra.png",
            "descriptionurl": "https://commons.wikimedia.org/wiki/File:Sierra_Club_logo.svg",
            "extmetadata": {"LicenseShortName": {"value": "CC BY-SA 4.0"}, "AttributionRequired": {"value": "true"},
                            "Artist": {"value": "<a href='x'>Jane Doe</a>"}}}]}}}}),
        "https://upload.example/512px-Sierra.png": (200, "image/png", png()),
    }
    result, write, judge = await _run(_state(), routes)
    assert result["outcome"] == "commons"
    judge.assert_not_awaited()
    kwargs = write.await_args.kwargs
    assert kwargs["action"] == "set"
    assert kwargs["fields"]["source"] == "commons"
    assert kwargs["fields"]["license"] == "CC BY-SA 4.0"
    assert kwargs["fields"]["credit"] == "Jane Doe, CC BY-SA 4.0"
    assert kwargs["fields"]["source_url"] == "https://commons.wikimedia.org/wiki/File:Sierra_Club_logo.svg"


@pytest.mark.asyncio
async def test_a_sponsor_logo_and_a_wrong_picture_are_turned_down_and_the_sites_own_logo_is_taken():
    page = """<img class="logo" src="/sponsors/acme-logo.png"><img id="logo-photo" src="/photo-logo.jpg">
    <script type="application/ld+json">{"logo": "/brand.png"}</script>"""
    routes = {
        "https://www.sierraclub.org/sponsors/acme-logo.png": (200, "image/png", png()),
        "https://www.sierraclub.org/photo-logo.jpg": (200, "image/jpeg", png(mode="RGB", color=(1, 2, 3), fmt="JPEG")),
        "https://www.sierraclub.org/brand.png": (200, "image/png", png(color=(9, 9, 9, 255))),
        "https://www.sierraclub.org/": (200, "text/html", page),
    }
    result, write, judge = await _run(_state(), routes, [SPONSOR, WRONG_PICTURE, OWN])
    assert (result["outcome"], result["source"], result["rejected"]) == ("website", "schema_org", 2)
    kwargs = write.await_args.kwargs
    assert kwargs["fields"]["source_url"] == "https://www.sierraclub.org/brand.png"
    assert kwargs["fields"]["is_parents"] is False
    assert [r["why"] for r in kwargs["fields"]["check"]["rejected"]] == ["names another organization", "not a logo"]
    assert judge.await_count == 3
    assert judge.await_args.kwargs["candidate_kind"] == "schema_org"


@pytest.mark.asyncio
async def test_a_chapter_with_nothing_of_its_own_uses_its_parents_logo():
    page = '<link rel="apple-touch-icon" href="/icon.png">'
    routes = {"https://www.sierraclub.org/icon.png": (200, "image/png", png(200, 200)),
              "https://www.sierraclub.org/": (200, "text/html", page)}
    state = _state(parent={"id": 1, "name": "Sierra Club National", "has_logo": True})
    result, write, judge = await _run(state, routes, [SYMBOL_ONLY])  # an icon with no words: not enough
    assert result["outcome"] == "parent"
    assert write.await_args.kwargs["action"] == "parent"
    assert judge.await_args.kwargs["parent_name"] == "Sierra Club National"


@pytest.mark.asyncio
async def test_a_chapter_site_logo_showing_the_parents_name_is_stored_as_the_parents():
    page = '<img alt="AARP logo" src="/aarp.png">'
    routes = {"https://www.sierraclub.org/aarp.png": (200, "image/png", png()),
              "https://www.sierraclub.org/": (200, "text/html", page)}
    state = _state(parent={"id": 1, "name": "AARP", "has_logo": False})
    result, write, _ = await _run(state, routes, [PARENT_NAME])
    assert result["outcome"] == "website"
    assert write.await_args.kwargs["fields"]["is_parents"] is True


@pytest.mark.asyncio
async def test_a_chapters_own_logo_found_later_wins_over_one_showing_the_parents_name():
    page = '<img alt="AARP logo" src="/aarp.png"><script type="application/ld+json">{"logo": "/own.png"}</script>'
    routes = {"https://www.sierraclub.org/aarp.png": (200, "image/png", png()),
              "https://www.sierraclub.org/own.png": (200, "image/png", png(color=(5, 5, 5, 255))),
              "https://www.sierraclub.org/": (200, "text/html", page)}
    state = _state(parent={"id": 1, "name": "AARP", "has_logo": True})
    result, write, judge = await _run(state, routes, [PARENT_NAME, OWN])
    assert (result["outcome"], result["source"]) == ("website", "schema_org")
    assert write.await_args.kwargs["fields"]["is_parents"] is False
    assert judge.await_count == 2


@pytest.mark.asyncio
async def test_svg_and_tiny_candidates_are_skipped_and_nothing_found_is_recorded():
    page = '<img class="logo" src="/logo.svg"><link rel="apple-touch-icon" href="/tiny.png">'
    routes = {"https://www.sierraclub.org/logo.svg": (200, "image/svg+xml", b"<svg xmlns='http://www.w3.org/2000/svg'/>"),
              "https://www.sierraclub.org/tiny.png": (200, "image/png", png(68, 68)),
              "https://www.sierraclub.org/": (200, "text/html", page)}
    result, write, judge = await _run(_state(), routes)
    assert result["outcome"] == "none"
    assert result["skipped"] == {"svg": 1, "small": 1}
    judge.assert_not_awaited()
    assert write.await_args.kwargs["action"] == "none"


@pytest.mark.asyncio
async def test_an_organization_that_needs_no_logo_or_whose_logo_staff_hid_is_not_looked_at():
    for state, force in ((_state(needs_logo=False), False), (_state(logo={"hidden": True}, needs_logo=False), True)):
        result, write, _ = await _run(state, {}, force=force)  # even the refresh never looks at a hidden logo
        assert result["outcome"] == "not_needed"
        write.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_broker_failure_stays_with_the_organization():
    from ddp_sync.services.broker_client import BrokerClientError

    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(side_effect=BrokerClientError("down"))):
        result = await ol.find_and_store_organization_logo(organization_id=7, org_name="Sierra Club")
    assert result["outcome"] == "state_failed"
    routes = {"https://www.sierraclub.org/": (500, "text/html", "")}
    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(return_value=_state())), \
            patch(f"{_L}.write_organization_logo", new=AsyncMock(side_effect=BrokerClientError("409 hidden"))):
        async with _network(routes) as client:
            result = await ol.find_and_store_organization_logo(organization_id=7, org_name="Sierra Club", client=client)
    assert result["outcome"] == "write_failed"
    assert result["skipped"] == {"site_unreachable": 1}


# ---- the pipeline step ----

def _settings(enabled):
    return SimpleNamespace(organization_resolution_mode="off", person_resolution_mode="off",
                           organization_content_enabled=False, organization_logo_enabled=enabled)


def _results():
    return [
        {"org_name": "A", "verification_verdict": "confirmed", "organization_id": 1, "organization_needs_logo": True},
        {"org_name": "A again", "verification_verdict": "confirmed", "organization_id": 1, "organization_needs_logo": True},
        {"org_name": "B", "verification_verdict": "not_confirmed", "organization_id": 2, "organization_needs_logo": True},
        {"org_name": "C", "verification_verdict": "confirmed", "organization_id": 3, "organization_needs_logo": False},
    ]


@pytest.mark.asyncio
async def test_the_step_looks_once_per_confirmed_organization_that_needs_a_logo_when_switched_on():
    find = AsyncMock(return_value={"organization_id": 1, "outcome": "website", "source": "img", "rejected": 1, "skipped": {}})
    for enabled, calls in ((True, 1), (False, 0)):
        find.reset_mock()
        results = _results()
        with patch(f"{_P}.get_settings", return_value=_settings(enabled)), \
                patch(f"{_P}.find_and_store_organization_logo", new=find):
            await resolve_and_describe_organizations(results, citations={}, bill_openstates_id="b", invocation_id="i")
        assert find.await_count == calls
        if enabled:
            assert find.await_args.kwargs["organization_id"] == 1
            assert results[0]["logo"]["outcome"] == "website"


# ---- the backfill ----

@pytest.mark.asyncio
async def test_the_backfill_calls_the_same_function_and_a_failing_write_cannot_loop():
    queue = AsyncMock(side_effect=[
        {"count": 3, "results": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]},
        {"count": 2, "results": [{"id": 2, "name": "B"}, {"id": 3, "name": "C"}]},   # B's write failed: still listed
        {"count": 1, "results": [{"id": 2, "name": "B"}]},
    ])
    outcomes = iter(["commons", "write_failed", "none"])

    async def find(**kwargs):
        return {"organization_id": kwargs["organization_id"], "outcome": next(outcomes), "source": None, "rejected": 0,
                "skipped": {}}

    with patch("ddp_sync.services.broker_client.list_organizations_needing_logo", new=queue), \
            patch(f"{_L}.find_and_store_organization_logo", side_effect=find) as mock_find:
        report = await ol.backfill_organization_logos(page_size=2, pause_seconds=0)
    assert [c.kwargs["organization_id"] for c in mock_find.call_args_list] == [1, 2, 3]
    assert report["looked"] == 3 and report["due"] == 3
    assert report["outcomes"] == {"commons": 1, "write_failed": 1, "none": 1}


@pytest.mark.asyncio
async def test_a_dry_run_reads_one_page_and_writes_nothing_and_the_refresh_forces_a_look():
    queue = AsyncMock(return_value={"count": 9, "results": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]})
    find = AsyncMock(return_value={"organization_id": 1, "outcome": "none", "source": None, "rejected": 0, "skipped": {}})
    with patch("ddp_sync.services.broker_client.list_organizations_needing_logo", new=queue), \
            patch(f"{_L}.find_and_store_organization_logo", new=find):
        await ol.backfill_organization_logos(dry_run=True, older_than_days=180, pause_seconds=0)
    assert queue.await_count == 1
    assert queue.await_args.kwargs["older_than_days"] == 180
    assert all(c.kwargs["dry_run"] and c.kwargs["force"] for c in find.await_args_list)


# ---- safety: only public addresses, bounded pictures, never raises ----

@pytest.mark.asyncio
async def test_a_page_cannot_send_the_finder_to_a_private_address_directly_or_by_redirect():
    page = ('<img class="logo" src="http://metadata.example/latest/meta-data/">'
            '<img class="logo" src="http://127.0.0.1:8000/admin.png">'
            '<link rel="apple-touch-icon" href="/redirected.png">')
    routes = {"https://www.sierraclub.org/redirected.png": (302, "text/plain", ""),
              "http://metadata.example/": (200, "image/png", png()),
              "http://127.0.0.1": (200, "image/png", png()),
              "https://www.sierraclub.org/": (200, "text/html", page)}
    seen = []

    def handler(request):
        seen.append(str(request.url))
        url = str(request.url)
        if url == "https://www.sierraclub.org/redirected.png":
            return httpx.Response(302, headers={"location": "http://intranet.example/logo.png"})
        for prefix, (status, content_type, body) in routes.items():
            if url.startswith(prefix):
                data = body if isinstance(body, bytes) else body.encode()
                return httpx.Response(status, headers={"content-type": content_type}, content=data)
        return httpx.Response(200, json={"search": []})

    judge = AsyncMock()
    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(return_value=_state())), \
            patch(f"{_L}.write_organization_logo", new=AsyncMock()), patch(f"{_L}.dispatch_judge_logo", new=judge):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await ol.find_and_store_organization_logo(organization_id=7, org_name="Sierra Club", client=client)
    assert not any(host in url for url in seen for host in ("metadata.example", "127.0.0.1", "intranet.example"))
    assert result["skipped"] == {"unreadable": 3}
    judge.assert_not_awaited()
    assert result["outcome"] == "none"


@pytest.mark.asyncio
async def test_a_website_on_a_private_address_is_not_read():
    with pytest.raises(ol.LogoFetchError):
        async with _network({}) as client:
            await ol._fetch(client, "http://intranet.example/")
    with pytest.raises(ol.LogoFetchError):
        async with _network({}) as client:
            await ol._fetch(client, "file:///etc/passwd")


def test_a_picture_that_would_unpack_too_large_is_refused_before_decoding():
    big = png(6000, 5000, mode="L", color=0)  # 30 million pixels, a small file
    assert len(big) < ol.MAX_DOWNLOAD_BYTES
    with pytest.raises(ol.LogoFetchError):
        ol.prepare_picture(big)


@pytest.mark.asyncio
async def test_the_step_never_raises_on_a_strange_broker_answer_or_an_unexpected_error():
    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(return_value={"needs_logo": True, "website": 5})):
        result = await ol.find_and_store_organization_logo(organization_id=7, org_name="X")
    assert result["outcome"] in ("failed", "none", "write_failed")
    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(side_effect=RuntimeError("boom"))):
        result = await ol.find_and_store_organization_logo(organization_id=7, org_name="X")
    assert result["outcome"] == "failed"


@pytest.mark.asyncio
async def test_a_redirect_to_a_public_picture_is_followed():
    def handler(request):
        url = str(request.url)
        if url == "https://www.sierraclub.org/":
            return httpx.Response(200, headers={"content-type": "text/html"},
                                  content=b'<img class="logo" src="/old-logo.png">')
        if url == "https://www.sierraclub.org/old-logo.png":
            return httpx.Response(301, headers={"location": "https://cdn.example/new-logo.png"})
        if url == "https://cdn.example/new-logo.png":
            return httpx.Response(200, headers={"content-type": "image/png"}, content=png())
        return httpx.Response(200, json={"search": []})

    judge = AsyncMock(return_value={"answer": OWN, "backend": "mlx"})
    write = AsyncMock()
    with patch(f"{_L}.get_organization_logo_state", new=AsyncMock(return_value=_state())), \
            patch(f"{_L}.write_organization_logo", new=write), patch(f"{_L}.dispatch_judge_logo", new=judge):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await ol.find_and_store_organization_logo(organization_id=7, org_name="Sierra Club", client=client)
    assert result["outcome"] == "website"
    assert write.await_args.kwargs["image"] == png()


@pytest.mark.asyncio
async def test_the_real_resolver_answers_and_localhost_is_refused():
    addresses = await _REAL_RESOLVE("localhost")  # the event loop's own getaddrinfo, no network needed
    assert addresses and all(a.split("%")[0] in ("127.0.0.1", "::1") for a in addresses)
    with patch(f"{_L}._resolve", new=_REAL_RESOLVE), pytest.raises(ol.LogoFetchError):
        await ol._check_public("http://localhost:8000/logo.png")
