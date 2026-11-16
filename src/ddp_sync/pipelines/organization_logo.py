"""Organization logos (SYNC-161): find each organization's logo and check it automatically, as step 3 of the organization
research pipeline (`resolve_and_describe_organizations`, after the About step). The broker stores and serves it (BROKER-502);
LegBot's `judge_logo` (AGENTS-240) says what a candidate image shows; the website shows it (NEXT-216).

There is no staff approval step (Ramon, 2026-11-14). The order to look in, per organization:

1. Wikidata/Commons: an item found by the organization's name whose official website (P856) has the SAME host as the broker's
   `website`; its logo (P154) from Commons with the license and credit. Matched by website, so not sent to the judge.
2. The organization's own website, in the order that worked in the 2026-11-14 test: an `<img>` the page labels as its logo,
   the `schema.org` logo in the page's JSON-LD, the `apple-touch-icon`, the `og:image` (last: usually a banner). Each must pass
   `judge_logo` and `accept_logo`.
3. A chapter with nothing of its own uses its parent's logo (Ramon, 2026-11-15: "chapters use the parent logo if that's all
   we have"); a chapter-site logo showing the parent's name counts the same way.
4. Nothing: no logo (the website shows initials).

Plain rules before the model is asked: a raster under 128 px on its longest side is skipped; an image with see-through parts
is drawn on mid-grey for the judge (white logos); downloads are capped; one request per site at a time, with a timeout. An SVG
from a website (SYNC-163) is drawn once by `svg_drawing.draw_for_judge` (resvg, its outside references removed first, in its
own process, 512 px, on grey) only for the judge; when accepted, the ORIGINAL SVG goes to the broker, which cleans it
(BROKER-503). Where `resvg-py` is not installed it is skipped as before (counted as `svg`). A Commons SVG is fetched as
Commons' own PNG rendering.
"""

from __future__ import annotations

import asyncio
import html
import io
import ipaddress
import json
import re
import socket
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import httpx
import structlog

from ddp_sync.services.broker_client import (
    BrokerClientError,
    get_organization_logo_state,
    write_organization_logo,
)
from ddp_sync.pipelines import svg_drawing
from ddp_sync.services.legbot_client import (
    LegBotDispatchError,
    LegBotUnavailable,
    dispatch_judge_logo,
)

logger = structlog.get_logger()

MIN_LONGEST_SIDE = 128           # px: tiny icons pass the model (the AAP's 68 px icon did, 2026-11-14)
MAX_DOWNLOAD_BYTES = 3_000_000   # a page or a picture; larger is not read
REQUEST_TIMEOUT_SECONDS = 15.0
GREY = (128, 128, 128)
USER_AGENT = "DDP-logo-finder/1.0"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
COMMONS_RENDER_WIDTH = 512
WEBSITE_KINDS = ("img", "schema_org", "apple_touch_icon", "og_image")
STORED_LONGEST_SIDE = 1024      # px: the stored copy is at most this wide or tall
JUDGED_LONGEST_SIDE = 768       # px: the PNG the judge sees (LegBot refuses over 1024 px a side or 2 MB; AGENTS-240)
MAX_JUDGED = 8                   # judge calls per organization: a wall of sponsor logos must not mean 40 model calls
MAX_PIXELS = 25_000_000          # a small file can unpack into a huge picture: refuse before decoding
MAX_REDIRECTS = 5
# SYNC-166: when LegBot cannot judge at all (memory full, vision server busy) the catch-up run waits and asks again about
# the same organization; it gives up after DEFERRED_LIMIT_SECONDS of waiting in one run (run it again: the list is the
# progress). Bill research never waits.
DEFERRED_WAIT_SECONDS = 300
DEFERRED_LIMIT_SECONDS = 4 * 3600

_site_locks: dict[str, asyncio.Lock] = {}


class LogoFetchError(Exception):
    """A page or picture could not be read (down, blocked, too big, not a picture)."""


class _JudgeUnavailable(Exception):
    """LegBot could not judge now (SYNC-166): ends this organization's search with nothing written."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass
class Candidate:
    kind: str   # commons, img, schema_org, apple_touch_icon, og_image
    url: str


@dataclass
class Picture:
    data: bytes          # what the broker gets: a re-encoded PNG, or the original SVG (the broker cleans it, BROKER-503)
    content_type: str
    judge_png: bytes     # what the judge sees (see-through parts on grey)
    width: int
    height: int


@dataclass
class Search:
    rejected: list[dict] = field(default_factory=list)   # {"kind", "url", "why", "answer"?}
    skipped: Counter = field(default_factory=Counter)    # small, svg, unreadable, over_cap
    judged: int = 0


# ---- names and hosts ----

def clean_name(name: str) -> str:
    """The name to search Wikidata with: parentheticals and a trailing LLC / Inc. removed."""
    text = re.sub(r"\s*\([^)]*\)", "", name or "").strip()
    text = re.sub(r"[,\s]+(llc|l\.l\.c\.|inc\.?|incorporated)$", "", text, flags=re.IGNORECASE).strip()
    return text


def site_host(url: str) -> str:
    """The host of a website address, lower case, without a leading `www.`; "" when there is none."""
    if not url:
        return ""
    if "//" not in url:
        url = "http://" + url
    return (urlparse(url).hostname or "").lower().removeprefix("www.")


# ---- the acceptance rule (the judge only reports facts; AGENTS-240) ----

def accept_logo(answer: dict, *, has_parent: bool) -> tuple[bool, bool, str]:
    """(accepted, is_parents, why) for one judged candidate.

    Never: not a logo, or it names another organization. Accepted as the organization's own when it shows the organization's
    name, initials or emblem; as the parent's when a parent is set and it shows the parent's (a chapter-site logo showing the
    parent's name counts as "all we have", Ramon 2026-11-15). Strict: a symbol-only image (no name or initials of the
    organization or the parent) is never accepted, wherever it came from (Ramon, 2026-11-15 pop-up "Strict: need the name",
    after AGENTS-240's re-test: allowing symbols added 4 real logos and 4 wrong ones out of 72). The judge's `shows_org` also
    counts "an emblem that clearly belongs to it", so the rule is enforced here: no words read (`text_seen` empty) is never
    accepted (the 2026-11-15 dev trial accepted two wordless images that way, one of them another organization's icon)."""
    if not answer.get("is_logo"):
        return False, False, "not a logo"
    if answer.get("names_other_org"):
        return False, False, "names another organization"
    if not str(answer.get("text_seen") or "").strip():
        return False, False, "no name or initials in the image"
    if answer.get("shows_org"):
        return True, False, "shows the organization"
    if has_parent and answer.get("shows_parent"):
        return True, True, "shows the parent organization"
    return False, False, "does not show the organization"


# ---- the website's candidates ----

def website_candidates(page_html: str, page_url: str) -> list[Candidate]:
    """The site's logo candidates, in the order to try: labelled `<img>`s, the JSON-LD `logo`, the apple-touch-icon, the
    og:image. Addresses are made absolute; a repeat is kept once (first place wins)."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(page_html or "", "html.parser")
    found: list[Candidate] = []

    def add(kind, url):
        url = (url or "").strip()
        if not url or url.startswith("data:"):
            return
        absolute = urljoin(page_url, url)
        if absolute.startswith(("http://", "https://")) and absolute not in {c.url for c in found}:
            found.append(Candidate(kind, absolute))

    for img in soup.find_all("img"):
        label = " ".join(
            [" ".join(img.get("class") or []), img.get("id") or "", img.get("alt") or "", img.get("src") or ""]
            + [" ".join(parent.get("class") or []) + " " + (parent.get("id") or "") for parent in img.parents
               if getattr(parent, "name", None) in ("a", "div", "span", "figure", "picture")][:2]
        ).lower()
        if "logo" in label:
            src = img.get("src") or ""
            lazy = img.get("data-src") or img.get("data-lazy-src") or img.get("data-original")
            add("img", lazy if (not src or src.startswith("data:")) and lazy else src)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except ValueError:
            continue
        for logo in _json_ld_logos(data):
            add("schema_org", logo)
    for link in soup.find_all("link"):
        rel = " ".join(link.get("rel") or []).lower()
        if "apple-touch-icon" in rel:
            add("apple_touch_icon", link.get("href"))
    for meta in soup.find_all("meta"):
        if (meta.get("property") or meta.get("name") or "").lower() == "og:image":
            add("og_image", meta.get("content"))
    order = {kind: n for n, kind in enumerate(WEBSITE_KINDS)}
    return sorted(found, key=lambda c: order[c.kind])  # stable: page order kept within a kind


def _json_ld_logos(data) -> list[str]:
    out = []
    if isinstance(data, list):
        for item in data:
            out += _json_ld_logos(item)
    elif isinstance(data, dict):
        logo = data.get("logo")
        if isinstance(logo, str):
            out.append(logo)
        elif isinstance(logo, dict):
            out.append(logo.get("url") or logo.get("contentUrl") or "")
        for key in ("@graph", "publisher", "organization"):
            if key in data:
                out += _json_ld_logos(data[key])
    return [url for url in out if url]


# ---- fetching ----

async def _resolve(host: str) -> list[str]:
    """The addresses `host` resolves to (a seam for the tests)."""
    infos = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return [info[4][0] for info in infos]


async def _check_public(url: str) -> str:
    """The public address to connect to for `url`, or LogoFetchError. `url` must be http(s) and its host must resolve only
    to public internet addresses. The pages and pictures come from organizations' websites and from addresses those pages
    name, and this runs on the Mac, which reaches private services over WireGuard: loopback, private, link-local (cloud
    metadata) and reserved addresses are refused. The caller connects to the address returned here, never to a second
    lookup of the name (a name that answers a public address first and a private one next cannot get through)."""
    parts = urlparse(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise LogoFetchError(f"not a web address: {url}")
    try:
        addresses = await _resolve(parts.hostname)
    except OSError as exc:
        raise LogoFetchError(f"cannot resolve {parts.hostname}") from exc
    if not addresses or not all(ipaddress.ip_address(a.split("%")[0]).is_global for a in addresses):
        raise LogoFetchError(f"not a public address: {parts.hostname}")
    return addresses[0].split("%")[0]


def _pinned(url: str, address: str) -> tuple[str, dict, dict]:
    """(url with the checked address in place of the name, headers, extensions): the request goes to `address`, says the
    real name in `Host`, and TLS checks the certificate against the real name (`sni_hostname`)."""
    parts = urlparse(url)
    host = f"[{address}]" if ":" in address else address
    netloc = f"{host}:{parts.port}" if parts.port else host
    headers = {"Host": parts.netloc.rsplit("@", 1)[-1]}
    extensions = {"sni_hostname": parts.hostname} if parts.scheme == "https" else {}
    return parts._replace(netloc=netloc).geturl(), headers, extensions


async def _fetch(client: httpx.AsyncClient, url: str) -> tuple[bytes, str]:
    """(body, content type) of `url`, at most MAX_DOWNLOAD_BYTES, one request per site at a time. Every address, the first
    and each redirect's, must be public and is connected to as checked (`_check_public`, `_pinned`); redirects are followed
    here, at most MAX_REDIRECTS. The whole download, not only each read, must finish within REQUEST_TIMEOUT_SECONDS."""
    for _ in range(MAX_REDIRECTS + 1):
        address = await _check_public(url)
        target, headers, extensions = _pinned(url, address)
        lock = _site_locks.setdefault(site_host(url), asyncio.Lock())
        async with lock:
            try:
                async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
                    async with client.stream("GET", target, headers=headers, extensions=extensions,
                                             follow_redirects=False, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                        if resp.is_redirect:
                            location = resp.headers.get("location")
                            if not location:
                                raise LogoFetchError(f"redirect without an address: {url}")
                            url = urljoin(url, location)
                            continue
                        if resp.status_code >= 400:
                            raise LogoFetchError(f"{resp.status_code} for {url}")
                        try:
                            declared = int(resp.headers.get("content-length") or 0)
                        except ValueError:  # a malformed or doubled header: the streaming cap below still holds
                            declared = 0
                        if declared > MAX_DOWNLOAD_BYTES:
                            raise LogoFetchError(f"too big: {url}")
                        body = bytearray()
                        async for chunk in resp.aiter_bytes():
                            body += chunk
                            if len(body) > MAX_DOWNLOAD_BYTES:
                                raise LogoFetchError(f"too big: {url}")
                        return bytes(body), resp.headers.get("content-type", "")
            except TimeoutError as exc:
                raise LogoFetchError(f"took longer than {REQUEST_TIMEOUT_SECONDS:g} s: {url}") from exc
            except httpx.HTTPError as exc:
                raise LogoFetchError(f"{type(exc).__name__} for {url}") from exc
    raise LogoFetchError(f"too many redirects: {url}")


def is_svg(data: bytes, content_type: str) -> bool:
    return "svg" in (content_type or "").lower() or b"<svg" in data[:2000].lower()


def prepare_picture(data: bytes) -> Picture | None:
    """The picture to store and the PNG the judge sees, or None when it is too small to judge (under MIN_LONGEST_SIDE).
    Raises LogoFetchError for something that is not a readable raster picture. A format the broker does not store (an
    icon file, a BMP) is stored as PNG; one with see-through parts is drawn on mid-grey for the judge."""
    from PIL import Image, UnidentifiedImageError

    try:
        image = Image.open(io.BytesIO(data))  # reads the header only
        width, height = image.size
        if width * height > MAX_PIXELS:
            raise LogoFetchError("picture too large to unpack")
        image.load()
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError) as exc:
        raise LogoFetchError("not a readable picture") from exc
    if max(width, height) < MIN_LONGEST_SIDE:
        return None
    rgba = image.convert("RGBA")
    # what the broker stores is drawn again here, never the site's own bytes (no metadata or appended data kept), as a PNG at
    # most STORED_LONGEST_SIDE wide or tall (a logo needs no more, and a re-encoded photo stays within the broker's cap)
    stored_image = rgba.copy()
    stored_image.thumbnail((STORED_LONGEST_SIDE, STORED_LONGEST_SIDE))
    out = io.BytesIO()
    stored_image.save(out, "PNG", optimize=True)
    stored, content_type = out.getvalue(), "image/png"
    # the judge's copy: at most JUDGED_LONGEST_SIDE (LegBot refuses a larger one), see-through parts drawn on mid-grey
    judged_image = rgba.copy()
    judged_image.thumbnail((JUDGED_LONGEST_SIDE, JUDGED_LONGEST_SIDE))
    backdrop = Image.new("RGBA", judged_image.size, GREY + (255,))
    backdrop.alpha_composite(judged_image)
    judged = io.BytesIO()
    backdrop.convert("RGB").save(judged, "PNG")
    return Picture(stored, content_type, judged.getvalue(), width, height)


# ---- Wikidata / Commons ----

async def commons_logo(client: httpx.AsyncClient, org_name: str, website: str) -> dict | None:
    """{"url", "source_url", "license", "credit"} of the Commons logo of the Wikidata item for this organization whose
    official website has the same host as `website`, or None. A name-only match is never enough."""
    host = site_host(website)
    if not host:
        return None
    search = (await _json(client, WIKIDATA_API, {
        "action": "wbsearchentities", "search": clean_name(org_name), "language": "en", "type": "item", "limit": 7,
        "format": "json"})).get("search", [])
    ids = [item["id"] for item in search if item.get("id")]
    if not ids:
        return None
    entities = (await _json(client, WIKIDATA_API, {
        "action": "wbgetentities", "ids": "|".join(ids), "props": "claims", "format": "json"})).get("entities", {})
    for item_id in ids:
        claims = entities.get(item_id, {}).get("claims", {})
        if host not in {site_host(value) for value in _claim_values(claims, "P856")}:
            continue
        files = _claim_values(claims, "P154")
        if not files:
            return None
        return await _commons_file(client, files[0])
    return None


def _claim_values(claims: dict, prop: str) -> list:
    values = []
    for claim in claims.get(prop, []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(value, str):
            values.append(value)
    return values


async def _commons_file(client: httpx.AsyncClient, file_name: str) -> dict | None:
    pages = (await _json(client, COMMONS_API, {
        "action": "query", "titles": f"File:{file_name}", "prop": "imageinfo", "iiprop": "url|extmetadata",
        "iiurlwidth": COMMONS_RENDER_WIDTH, "format": "json"})).get("query", {}).get("pages", {})
    for page in pages.values():
        info = (page.get("imageinfo") or [{}])[0]
        url = info.get("thumburl") or info.get("url")  # thumburl: a PNG rendering of an SVG
        if not url:
            return None
        meta = info.get("extmetadata", {})
        license_name = _plain(meta.get("LicenseShortName", {}).get("value", ""))
        credit = ""
        if str(meta.get("AttributionRequired", {}).get("value", "")).lower() == "true":
            artist = _plain(meta.get("Artist", {}).get("value", "")) or "Wikimedia Commons"
            credit = f"{artist}, {license_name}" if license_name else artist
        return {"url": url, "source_url": info.get("descriptionurl") or url,
                "license": license_name or "unknown", "credit": credit}
    return None


def _plain(text: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text or ""))).strip()


async def _json(client: httpx.AsyncClient, url: str, params: dict) -> dict:
    try:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        body = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise LogoFetchError(f"{url}: {exc}") from exc
    return body if isinstance(body, dict) else {}


# ---- one organization ----

async def find_and_store_organization_logo(
    *,
    organization_id: int,
    org_name: str,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
    client: httpx.AsyncClient | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    """Find, check and store one organization's logo (SYNC-161). Never raises; a failure stays with this organization.

    Reads the broker's state at that moment (website, parent, logo, whether one is needed), runs the search order, asks
    `judge_logo` about each website candidate, applies `accept_logo`, and writes the result (`set`, `parent` or `none`).
    `dry_run` writes nothing. `force` (the refresh) looks even when the broker says no logo is needed yet, except for a logo
    staff hid, which is never looked for.

    Returns {"organization_id", "outcome", "source", "rejected", "skipped"}: outcome `commons` / `website` / `parent` / `none`
    / `not_needed` / `state_failed` / `write_failed` / `failed` / `deferred`; `source` the candidate kind used; `rejected`
    how many candidates the rule turned down; `skipped` the counts of candidates not judged (small, svg, unreadable,
    judge_failed).

    `deferred` (SYNC-166, with `deferred_reason` memory_full or server_busy): LegBot could not judge at all, so the search
    stopped at once (no more downloads or judgements) and NOTHING was written: no `none`, no `parent`. The organization
    stays on the broker's logo list, and that is the pause. Any other judge failure is counted as `judge_failed` and the
    search goes on.
    """
    result = {"organization_id": organization_id, "outcome": "failed", "source": None, "rejected": 0, "skipped": {}}
    try:
        return await _find_and_store(organization_id, org_name, result, client=client, dry_run=dry_run, force=force,
                                     broker={"broker_api_base": broker_api_base, "broker_api_token": broker_api_token})
    except Exception as exc:  # noqa: BLE001 -- the step must never fail the bill or the position
        logger.warning("organization logo step raised", organization_id=organization_id, error=str(exc))
        result["outcome"] = "failed"
        return result


async def _find_and_store(organization_id, org_name, result, *, client, dry_run, force, broker) -> dict:
    try:
        state = await get_organization_logo_state(organization_id, **broker)
    except BrokerClientError as exc:
        logger.warning("organization logo state read failed", organization_id=organization_id, error=str(exc))
        result["outcome"] = "state_failed"
        return result
    if (state.get("logo") or {}).get("hidden") or not (state.get("needs_logo") or force):
        result["outcome"] = "not_needed"
        return result
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT})
    try:
        write = await _search(client, state, org_name or state.get("name") or "", result)
    except _JudgeUnavailable as exc:
        result.update(outcome="deferred", deferred_reason=exc.code, source=None)
        logger.info("organization logo deferred", organization_id=organization_id, reason=exc.code)
        return result
    finally:
        if own_client:
            await client.aclose()
    if dry_run:
        return result
    try:
        await write_organization_logo(organization_id=organization_id, **write, **broker)
    except BrokerClientError as exc:
        logger.warning("organization logo write failed", organization_id=organization_id, error=str(exc))
        result["outcome"] = "write_failed"
    return result


async def _search(client: httpx.AsyncClient, state: dict, org_name: str, result: dict) -> dict:
    """Run the search order; fill `result` and return the write to make (keyword arguments of write_organization_logo)."""
    search = Search()
    website = state.get("website") or ""
    parent = state.get("parent") or None

    try:
        commons = await commons_logo(client, org_name, website)
    except LogoFetchError as exc:
        commons = None
        search.skipped["commons_unreachable"] += 1
        logger.info("commons lookup failed", organization_id=state.get("id"), error=str(exc))
    if commons:
        try:
            data, _ = await _fetch(client, commons["url"])
            picture = prepare_picture(data)
        except LogoFetchError:
            picture = None
        if picture is not None:
            result.update(outcome="commons", source="commons", skipped=dict(search.skipped))
            return {"action": "set", "image": picture.data, "image_content_type": picture.content_type, "fields": {
                "source": "commons", "source_url": commons["source_url"], "license": commons["license"],
                "credit": commons["credit"], "check": {"matched_by": "website", "url": commons["url"]}}}

    if website:
        try:
            page, _ = await _fetch(client, website)
            candidates = website_candidates(page.decode("utf-8", "replace"), website)
        except LogoFetchError as exc:
            candidates = []
            search.skipped["site_unreachable"] += 1
            logger.info("organization website unreadable", organization_id=state.get("id"), error=str(exc))
        # the chapter's own logo first (Ramon, 2026-11-15): a logo showing the parent's name is kept as the fallback and
        # the search goes on; the organization's own logo, when found, wins
        fallback = None
        for candidate in candidates:
            accepted = await _judge_candidate(client, candidate, org_name, parent, search)
            if accepted is None:
                continue
            if accepted[2] and fallback is None:
                fallback = (candidate, accepted)
                continue
            if not accepted[2]:
                fallback = (candidate, accepted)
                break
        if fallback is not None:
            candidate, (picture, answer, is_parents) = fallback
            result.update(outcome="website", source=candidate.kind, rejected=len(search.rejected),
                          skipped=dict(search.skipped))
            return {"action": "set", "image": picture.data, "image_content_type": picture.content_type, "fields": {
                "source": candidate.kind, "source_url": candidate.url, "is_parents": is_parents,
                "check": {**answer, "rejected": search.rejected}}}

    result.update(rejected=len(search.rejected), skipped=dict(search.skipped))
    if parent and parent.get("has_logo"):
        result.update(outcome="parent", source="parent")
        return {"action": "parent", "fields": {"check": {"rejected": search.rejected}}}
    result.update(outcome="none")
    return {"action": "none", "fields": {"check": {"rejected": search.rejected}}}


async def _judge_candidate(client, candidate: Candidate, org_name: str, parent: dict | None, search: Search):
    """(picture, answer, is_parents) when the candidate is accepted, else None (recorded in `search`). At most MAX_JUDGED
    candidates per organization are tried (fetched, and judged when readable); the rest are counted as `over_cap`."""
    if search.judged >= MAX_JUDGED:
        search.skipped["over_cap"] += 1
        return None
    search.judged += 1  # counted when tried, so a wall of unreadable images is capped too
    try:
        data, content_type = await _fetch(client, candidate.url)
    except LogoFetchError:
        search.skipped["unreadable"] += 1
        return None
    if is_svg(data, content_type):
        # SYNC-163: drawn once for the judge only; the original SVG is what is sent when it is accepted
        if not svg_drawing.renderer_installed():  # until resvg-py is installed (a release step), skipped as before
            search.skipped["svg"] += 1
            return None
        try:
            drawn = await svg_drawing.draw_for_judge(data)
        except svg_drawing.SVGUnreadable as exc:
            search.skipped["unreadable"] += 1
            logger.info("svg logo candidate not drawn", url=candidate.url, error=str(exc))
            return None
        picture = Picture(data, "image/svg+xml", drawn, int.from_bytes(drawn[16:20], "big"),
                          int.from_bytes(drawn[20:24], "big"))
    else:
        try:
            picture = prepare_picture(data)
        except LogoFetchError:
            search.skipped["unreadable"] += 1
            return None
    if picture is None:
        search.skipped["small"] += 1
        return None
    try:
        dispatched = await dispatch_judge_logo(picture.judge_png, org_name, parent_name=(parent or {}).get("name"),
                                               candidate_kind=candidate.kind, source_url=candidate.url)
    except LegBotUnavailable as exc:
        raise _JudgeUnavailable(exc.code) from exc
    except LegBotDispatchError as exc:
        search.skipped["judge_failed"] += 1
        logger.warning("judge_logo dispatch failed", url=candidate.url, error=str(exc))
        return None
    answer = dispatched.get("answer") or {}
    accepted, is_parents, why = accept_logo(answer, has_parent=bool(parent))
    if not accepted:
        search.rejected.append({"kind": candidate.kind, "url": candidate.url, "why": why,
                                "text_seen": answer.get("text_seen"), "reason": answer.get("reason")})
        return None
    record = {key: answer.get(key) for key in ("is_logo", "text_seen", "shows_org", "shows_parent", "names_other_org", "reason")}
    record.update(why=why, model=answer.get("model") or dispatched.get("model"), backend=dispatched.get("backend"))
    return picture, record, is_parents


def summarize(outcomes: list[dict]) -> dict:
    """Counts for the `organization_logo_summary` log line and the backfill's report."""
    return {
        "outcomes": dict(Counter(o["outcome"] for o in outcomes)),
        "sources": dict(Counter(o["source"] for o in outcomes if o.get("source"))),
        "rejected": sum(o.get("rejected", 0) for o in outcomes),
        "skipped": dict(sum((Counter(o.get("skipped") or {}) for o in outcomes), Counter())),
    }


# ---- the backfill and refresh (the same function, for organizations already in the broker) ----

async def backfill_organization_logos(
    *,
    limit: int | None = None,
    older_than_days: int | None = None,
    dry_run: bool = False,
    pause_seconds: float = 1.0,
    page_size: int = 50,
    deferred_wait_seconds: float = DEFERRED_WAIT_SECONDS,
    deferred_limit_seconds: float = DEFERRED_LIMIT_SECONDS,
    broker_api_base: str | None = None,
    broker_api_token: str | None = None,
) -> dict:
    """Look for the logo of every organization the broker lists as due (`GET /api/organizations/logo-queue/`: never checked
    first, then the oldest check), through `find_and_store_organization_logo`, the same function the pipeline step uses.

    Resumable: the broker's list is the progress (an organization written drops out of it), so a stopped run is just run
    again. Rate-limited: `pause_seconds` between organizations, and one request per site at a time. `older_than_days` is the
    refresh: it also re-checks every logo checked longer ago, so a chapter on its parent's logo moves to its own when one
    appears (the broker re-checks a borrowed logo after 14 days anyway). `dry_run` looks but writes nothing, and reads one
    page only (nothing leaves the list). An organization seen once is not looked at again in the same run, so a write that
    keeps failing cannot loop.

    SYNC-166: an organization `deferred` (LegBot cannot judge now: memory full or its vision server busy) is asked about
    again after `deferred_wait_seconds`, and again, until LegBot answers; after `deferred_limit_seconds` of waiting in all
    the run stops and says so (`stopped`). Each pause and each resume is logged.

    Returns `summarize(...)` of every organization looked at, plus `looked`, `due` (the broker's count at the start),
    `waited_seconds`, and `stopped` ("judge_unavailable" when it gave up waiting, else None).
    """
    broker = {"broker_api_base": broker_api_base, "broker_api_token": broker_api_token}
    from ddp_sync.services.broker_client import list_organizations_needing_logo

    seen: set[int] = set()
    outcomes: list[dict] = []
    due = None
    waited = 0.0
    stopped = None
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT}) as client:
        while limit is None or len(outcomes) < limit:
            size = page_size if limit is None else min(page_size, limit - len(outcomes))
            if dry_run:  # one read only (a dry run writes nothing, so nothing leaves the list): up to --limit, at most 500
                size = min(500, limit or page_size)
            # ask past the ones already seen this run (a failed write stays on the list), within the broker's 500 cap
            page = await list_organizations_needing_logo(limit=min(500, size + len(seen)),
                                                         older_than_days=older_than_days, **broker)
            if due is None:
                due = page.get("count")
            fresh = [row for row in page.get("results", []) if row.get("id") not in seen]
            if not fresh:
                break
            for row in fresh[:size]:
                seen.add(row["id"])
                while True:
                    outcome = await find_and_store_organization_logo(
                        organization_id=row["id"], org_name=row.get("name") or "", client=client, dry_run=dry_run,
                        force=older_than_days is not None, **broker)
                    if outcome["outcome"] != "deferred":
                        break
                    if waited + deferred_wait_seconds > deferred_limit_seconds:
                        stopped = "judge_unavailable"
                        logger.warning("organization_logo_backfill_stopped", organization_id=row["id"],
                                       reason=outcome.get("deferred_reason"), waited_seconds=waited)
                        break
                    logger.info("organization_logo_backfill_paused", organization_id=row["id"],
                                reason=outcome.get("deferred_reason"), wait_seconds=deferred_wait_seconds,
                                waited_seconds=waited)
                    await asyncio.sleep(deferred_wait_seconds)
                    waited += deferred_wait_seconds
                    logger.info("organization_logo_backfill_resumed", organization_id=row["id"], waited_seconds=waited)
                if stopped:
                    break
                outcomes.append(outcome)
                logger.info("organization_logo_backfill_item", dry_run=dry_run, **{k: v for k, v in outcome.items()
                                                                                    if k != "skipped"})
                if pause_seconds:
                    await asyncio.sleep(pause_seconds)
            if dry_run or stopped:
                break
    report = {"looked": len(outcomes), "due": due, "waited_seconds": waited, "stopped": stopped, **summarize(outcomes)}
    logger.info("organization_logo_backfill_summary", dry_run=dry_run, **report)
    return report
