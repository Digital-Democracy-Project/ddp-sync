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
        img = draw(f'<svg {NS} viewBox="0 0 40 40"><image href="{href}" width="40" height="40"/>'
                   f'<image xlink:href="{href}" width="40" height="40"/></svg>')
        assert not has_red(img), href
    # an embedded raster picture is fine: it is in the file itself
    data = io.BytesIO()
    Image.new("RGB", (4, 4), (255, 0, 0)).save(data, "PNG")
    import base64
    embedded = "data:image/png;base64," + base64.b64encode(data.getvalue()).decode()
    assert has_red(draw(f'<svg {NS} viewBox="0 0 4 4"><image href="{embedded}" width="4" height="4"/></svg>'))


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
             f'<rect width="10" height="10" fill="url({url}#p)"/></svg>')
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


def test_an_embedded_picture_that_would_unpack_huge_is_dropped(monkeypatch):
    import base64
    big = io.BytesIO()
    Image.new("L", (6000, 5000), 0).save(big, "PNG")  # 30 million pixels in a small file
    small = io.BytesIO()
    Image.new("RGB", (4, 4), (255, 0, 0)).save(small, "PNG")
    href = lambda b: "data:image/png;base64," + base64.b64encode(b.getvalue()).decode()  # noqa: E731
    safe, _ = sd.for_drawing(f'<svg {NS} viewBox="0 0 4 4"><image href="{href(big)}" width="4" height="4"/>'
                             f'<image href="{href(small)}" width="4" height="4"/></svg>'.encode())
    assert safe.count(b"data:image/png") == 1                                  # only the small one is kept
    assert sd._small_raster("data:image/png;base64,not-a-picture") is False
