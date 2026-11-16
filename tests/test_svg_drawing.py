"""SYNC-163: website SVG logo candidates are drawn once, only for LegBot's judge_logo, safely: no local file, no address,
no entity, a fixed output size, a time limit; a bad file is unreadable, never a crash."""

import asyncio
import io
import os

import pytest
from PIL import Image

from ddp_sync.pipelines import svg_drawing as sd

pytestmark = pytest.mark.skipif(not sd.renderer_installed(), reason="resvg-py is not installed (pip install '.[svg-logos]')")

NS = 'xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"'


def draw(svg: str, **kw) -> Image.Image:
    return Image.open(io.BytesIO(asyncio.run(sd.draw_for_judge(svg.encode(), **kw)))).convert("RGB")


def has_red(img: Image.Image) -> bool:
    return any(px == (255, 0, 0) for px in img.getdata())


def test_a_white_logo_on_a_transparent_background_is_drawn_on_grey():
    img = draw(f'<svg {NS} viewBox="0 0 20 10"><rect x="10" width="10" height="10" fill="#fff"/></svg>')
    assert img.size == (512, 256)
    assert img.getpixel((10, 10)) == (128, 128, 128)     # the see-through half is the grey backdrop
    assert img.getpixel((500, 10)) == (255, 255, 255)    # the white mark stays white, now visible


def test_a_tall_logo_is_drawn_at_most_512_high_and_a_tiny_viewbox_is_drawn_large():
    assert draw(f'<svg {NS} viewBox="0 0 10 30"><rect width="10" height="30"/></svg>').size == (171, 512)
    assert max(draw(f'<svg {NS} viewBox="0 0 2 1"><rect width="2" height="1"/></svg>').size) == 512


def test_a_local_file_named_inside_the_svg_is_never_read(tmp_path):
    red = tmp_path / "red.png"
    Image.new("RGB", (40, 40), (255, 0, 0)).save(red)
    for href in (str(red), f"file://{red}", os.path.relpath(red)):
        img = draw(f'<svg {NS} viewBox="0 0 40 40"><rect width="2" height="2" fill="#00f"/>'
                   f'<image href="{href}" width="40" height="40"/><image xlink:href="{href}" width="40" height="40"/></svg>')
        assert not has_red(img), href


def embedded_png_svg(color=(255, 0, 0)) -> bytes:
    """Like dow.com's logo: an SVG whose only content is a PNG embedded in it."""
    import base64

    data = io.BytesIO()
    Image.new("RGB", (40, 20), color).save(data, "PNG")
    href = "data:image/png;base64," + base64.b64encode(data.getvalue()).decode()
    return f'<svg {NS} viewBox="0 0 40 20"><image xlink:href="{href}" width="40" height="20"/></svg>'.encode()


def test_an_svg_that_only_wraps_an_embedded_picture_draws_nothing_and_is_not_judged():
    """SYNC-168: the judge must see what the broker stores, and the broker's cleaner drops embedded pictures; Dow
    Chemical's SVG was judged on its embedded PNG and stored blank."""
    safe, _ = sd.for_drawing(embedded_png_svg())
    assert b"data:image" not in safe and b"<image" in safe and b"href" not in safe
    with pytest.raises(sd.SVGUnreadable, match="draws nothing"):
        asyncio.run(sd.draw_for_judge(embedded_png_svg()))


def test_an_svg_with_real_vector_content_draws_as_before():
    img = draw(f'<svg {NS} viewBox="0 0 40 20"><path d="M0 0 H20 V20 H0 Z" fill="#c00"/>'
               f'<text x="22" y="15" font-size="12" fill="#000">AB</text></svg>')
    assert img.size == (512, 256) and has_red_ish(img) and img.getbbox() is not None


def has_red_ish(img: Image.Image) -> bool:
    return any(r > 150 and g < 60 and b < 60 for r, g, b in img.getdata())


def test_a_hash_link_on_an_image_goes_but_stays_on_use():
    """SYNC-169: resvg reads `<image href="#x">` as a file path; on `<use>` a `#` link is a real fragment."""
    safe, _ = sd.for_drawing(f'<svg {NS} viewBox="0 0 10 10"><defs><rect id="x" width="5" height="5"/></defs>'
                             f'<image href="#x" width="5" height="5"/><image xlink:href="#x" width="5" height="5"/>'
                             f'<use href="#x"/></svg>'.encode())
    text = safe.decode()
    assert text.count('href="#x"') == 1 and "<use" in text and 'use href="#x"' in text


def test_an_address_on_a_private_network_is_never_fetched():
    import http.server
    import threading

    hits = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            self.send_response(404)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/x.png"
        draw(f'<svg {NS} viewBox="0 0 10 10"><style>@import url({url});</style><image href="{url}" width="10" height="10"/>'
             f'<rect width="10" height="10" fill="url({url}#p)"/><rect width="1" height="1" fill="#00f"/></svg>')
    finally:
        server.shutdown()
    assert hits == []


def test_a_huge_viewbox_is_drawn_at_the_fixed_size():
    assert draw(f'<svg {NS} viewBox="0 0 100000000 100000000" width="100000000" height="100000000">'
                f'<rect width="100000000" height="100000000"/></svg>').size == (512, 512)


@pytest.mark.parametrize("data", [
    b"<svg><rect></svg>",                                                                           # malformed
    b'<?xml version="1.0"?><!DOCTYPE svg [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;">]><svg xmlns="http://www.w3.org/2000/svg">&b;</svg>',
    b'<?xml version="1.0"?><!DOCTYPE svg [<!ENTITY x SYSTEM "file:///etc/passwd">]><svg xmlns="http://www.w3.org/2000/svg">&x;</svg>',
    b'<html xmlns="http://www.w3.org/1999/xhtml"/>',                                                # not an SVG
    b"\x89PNG not an svg at all",
])
def test_a_bad_file_is_unreadable_not_a_crash(data):
    with pytest.raises(sd.SVGUnreadable):
        asyncio.run(sd.draw_for_judge(data))


def test_a_drawing_that_takes_too_long_is_stopped():
    with pytest.raises(sd.SVGUnreadable, match="longer than"):
        asyncio.run(sd.draw_for_judge(f'<svg {NS} viewBox="0 0 1 1"><rect width="1" height="1"/></svg>'.encode(), timeout=0.001))


def test_outside_css_addresses_and_imports_are_removed_before_drawing():
    safe, _ = sd.for_drawing(f'<svg {NS} viewBox="0 0 10 10"><style>@import url(https://evil.example/a.css); '
                             f'rect {{ fill: url(/etc/x.svg#p) }}</style><rect style="fill:url(file:///tmp/p.svg#q)" '
                             f'fill="url(https://evil.example/f.svg#r)" width="10" height="10"/>'
                             f'<rect fill="url(#ok)" width="1" height="1"/></svg>'.encode())
    text = safe.decode()
    for gone in ("@import", "evil.example", "/etc/x.svg", "file:///tmp"):
        assert gone not in text
    assert "url(#ok)" in text


def test_a_drawing_that_runs_too_long_is_killed(monkeypatch):
    killed = []

    class Stuck:
        returncode = None

        async def communicate(self, _input):
            await asyncio.sleep(3600)

        def kill(self):
            killed.append(True)

        async def wait(self):
            return -9

    async def fake_exec(*_args, **_kwargs):
        return Stuck()

    monkeypatch.setattr(sd.asyncio, "create_subprocess_exec", fake_exec)
    with pytest.raises(sd.SVGUnreadable, match="longer than"):
        asyncio.run(sd.draw_for_judge(f'<svg {NS} viewBox="0 0 1 1"/>'.encode(), timeout=0.05))
    assert killed == [True]


@pytest.mark.parametrize("body", [
    r'<style>rect { fill: u\72l(https://evil.example/a.svg#p) }</style>',
    '<style>rect { fill: url/**/(https://evil.example/a.svg#p) }</style>',
    r'<style>@\69mport "https://evil.example/a.css";</style>',
    r'<rect style="fill: u\72l(https://evil.example/b.svg#q)" width="1"/>',
    r'<rect fill="u\72l(https://evil.example/c.svg#r)" width="1"/>',
    '<g xml:base="https://evil.example/x.svg"><use href="#p"/></g>',
    '<style><g/>rect { fill: url(https://evil.example/e.svg#t) }</style>',
])
def test_escapes_comments_at_rules_and_xml_base_cannot_hide_an_address(body):
    safe, _ = sd.for_drawing(f'<svg {NS} viewBox="0 0 10 10">{body}</svg>'.encode())
    assert b"evil.example" not in safe


def test_a_deeply_nested_svg_is_unreadable_not_a_crash():
    with pytest.raises(sd.SVGUnreadable):
        sd.for_drawing(b'<svg xmlns="http://www.w3.org/2000/svg">' + b"<g>" * 5000 + b"</g>" * 5000 + b"</svg>")


def test_a_comment_hidden_address_in_any_attribute_is_removed():
    safe, _ = sd.for_drawing(f'<svg {NS} viewBox="0 0 10 10"><rect fill="u/**/rl(/etc/x.svg#p)" filter="url/**/(file:///tmp/f.svg#q)" '
                             f'width="10" height="10"/></svg>'.encode())
    assert b"/etc/x.svg" not in safe and b"file:///tmp" not in safe


def test_every_embedded_picture_is_dropped_large_or_small():
    import base64
    big = io.BytesIO()
    Image.new("L", (6000, 5000), 0).save(big, "PNG")  # 30 million pixels in a small file
    small = io.BytesIO()
    Image.new("RGB", (4, 4), (255, 0, 0)).save(small, "PNG")
    href = lambda b: "data:image/png;base64," + base64.b64encode(b.getvalue()).decode()
    safe, _ = sd.for_drawing(f'<svg {NS} viewBox="0 0 4 4"><image href="{href(big)}" width="4" height="4"/>'
                             f'<image href="{href(small)}" width="4" height="4"/></svg>'.encode())
    assert b"data:image" not in safe



# ---- SYNC-169: the drawing child is isolated and never left running ----

def _png(width, height, color=(200, 0, 0)):
    out = io.BytesIO()
    Image.new("RGB", (width, height), color).save(out, "PNG")
    return out.getvalue()


def _fake_child(monkeypatch, png=None, hang=False):
    """Replace the child process; record how it was started."""
    seen = {}

    class Child:
        returncode = None

        async def communicate(self, _input):
            seen["cwd_listing"] = os.listdir(seen["kwargs"]["cwd"])
            if hang:
                await asyncio.sleep(3600)
            self.returncode = 0
            return png, b""

        def kill(self):
            seen["killed"] = seen.get("killed", 0) + 1

        async def wait(self):
            self.returncode = -9
            return -9

    async def fake_exec(*args, **kwargs):
        seen["args"], seen["kwargs"] = args, kwargs
        return Child()

    monkeypatch.setattr(sd.asyncio, "create_subprocess_exec", fake_exec)
    return seen


SIMPLE = f'<svg {NS} viewBox="0 0 1 1"><rect width="1" height="1"/></svg>'.encode()


def test_the_child_gets_only_path_home_and_lang(monkeypatch):
    monkeypatch.setenv("CAMS_API_TOKEN", "secret-value")
    monkeypatch.setenv("LANG", "en_US.UTF-8")
    seen = _fake_child(monkeypatch, png=_png(10, 10))
    asyncio.run(sd.draw_for_judge(SIMPLE))
    assert set(seen["kwargs"]["env"]) <= {"PATH", "HOME", "LANG"}
    assert "secret-value" not in str(seen["kwargs"]["env"])


def test_the_child_runs_isolated_on_this_file_in_an_empty_folder(monkeypatch):
    seen = _fake_child(monkeypatch, png=_png(10, 10))
    asyncio.run(sd.draw_for_judge(SIMPLE))
    assert seen["args"][1] == "-I" and seen["args"][2] == sd.__file__
    assert seen["cwd_listing"] == []                      # the working folder is empty while the child runs
    assert seen["args"][5] == seen["kwargs"]["cwd"]        # and it is resvg's resource folder too
    assert not os.path.exists(seen["kwargs"]["cwd"])       # removed afterwards


def test_a_drawing_larger_than_512_is_refused(monkeypatch):
    _fake_child(monkeypatch, png=_png(600, 10))
    with pytest.raises(sd.SVGUnreadable, match="over 512"):
        asyncio.run(sd.draw_for_judge(SIMPLE))


def test_a_cancelled_drawing_kills_the_child(monkeypatch):
    seen = _fake_child(monkeypatch, hang=True)

    async def go():
        task = asyncio.create_task(sd.draw_for_judge(SIMPLE))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(go())
    assert seen.get("killed") == 1
